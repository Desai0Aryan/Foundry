"""Post-run auditor: proves the platform's guarantees held, from the database alone.

It never trusts the code under test to grade itself with its own counters. Every check is an
independent SQL query over the persisted state (jobs, audit trail, outbox, effect ledger and
the external-effects sink). Run it after a load test, a chaos run, or in production:

    foundry audit                     # everything
    foundry audit --user <uuid>       # one tenant / one test run
    foundry audit --allow-active      # jobs may still be in flight

Checks
------
no_stuck_jobs         every job reached a terminal state (nothing queued/running/retrying)
state_machine         every audit-trail transition is legal and chained (no gaps, no teleports)
single_outcome        no job succeeded twice, or changed state after succeeding / cancelling
terminal_integrity    succeeded jobs carry a result, dead jobs an error, no job holds a lease
attempt_budget        no job ran more attempts than max_attempts
effects_exactly_once  every succeeded job has exactly one ledger row and one external effect,
                      and was never delivered more times than it had attempts
no_lost_jobs          every job the client was acknowledged for exists (when ids are supplied)
outbox_drained        nothing waiting to be published, nothing stuck unroutable
dlq_consistent        every dead job produced a dead-letter notice
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

import asyncpg

from .domain import ACTIVE_STATUSES, TRANSITIONS, JobStatus

SAMPLE = 5  # violations shown per check


@dataclass
class Check:
    name: str
    passed: bool
    detail: str
    violations: list[str] = field(default_factory=list)


@dataclass
class AuditReport:
    checks: list[Check]
    jobs_audited: int
    stats: dict[str, Any]

    @property
    def passed(self) -> bool:
        return all(c.passed for c in self.checks)

    def render(self, color: bool = True) -> str:
        g, r, d, b, x = (
            ("\033[32m", "\033[31m", "\033[2m", "\033[1m", "\033[0m") if color else ("",) * 5
        )
        lines = [f"{b}Audit of {self.jobs_audited} jobs{x}"]
        for c in self.checks:
            mark = f"{g}PASS{x}" if c.passed else f"{r}FAIL{x}"
            lines.append(f"  {mark}  {c.name:<22} {d}{c.detail}{x}")
            lines.extend(f"          {r}- {v}{x}" for v in c.violations[:SAMPLE])
            if len(c.violations) > SAMPLE:
                lines.append(f"          {r}... and {len(c.violations) - SAMPLE} more{x}")
        verdict = (
            f"{g}ALL INVARIANTS HELD{x}" if self.passed else f"{r}INVARIANT VIOLATIONS FOUND{x}"
        )
        lines.append(f"  {b}{verdict}{x}")
        return "\n".join(lines)

    def as_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "jobs_audited": self.jobs_audited,
            "stats": self.stats,
            "checks": [
                {
                    "name": c.name,
                    "passed": c.passed,
                    "detail": c.detail,
                    "violations": c.violations[:20],
                }
                for c in self.checks
            ],
        }


def _scope(user_id: UUID | None, alias: str = "j") -> tuple[str, list[Any]]:
    return (f"AND {alias}.user_id = $1", [user_id]) if user_id else ("", [])


async def run_audit(
    pool: asyncpg.Pool,
    *,
    user_id: UUID | None = None,
    expected_job_ids: set[UUID] | None = None,
    require_drained: bool = True,
) -> AuditReport:
    scope, args = _scope(user_id)
    checks: list[Check] = []
    async with pool.acquire() as conn:
        total = await conn.fetchval(f"SELECT count(*) FROM jobs j WHERE true {scope}", *args)
        by_status = {
            r["status"]: r["n"]
            for r in await conn.fetch(
                f"SELECT status, count(*) AS n FROM jobs j WHERE true {scope} GROUP BY status",
                *args,
            )
        }

        # 1 ---- nothing stuck -------------------------------------------------------------
        active = [s.value for s in ACTIVE_STATUSES]
        stuck = await conn.fetch(
            f"SELECT j.id, j.status, j.attempts FROM jobs j WHERE j.status = ANY(${len(args) + 1}) {scope}",
            *args, active,
        )  # fmt: skip
        if require_drained:
            checks.append(
                Check(
                    "no_stuck_jobs",
                    not stuck,
                    f"{len(stuck)} job(s) not in a terminal state"
                    if stuck
                    else "every job reached a terminal state",
                    [f"{r['id']} is {r['status']} after {r['attempts']} attempt(s)" for r in stuck],
                )
            )

        # 2 ---- state machine ---------------------------------------------------------------
        events = await conn.fetch(
            f"""
            SELECT e.job_id, e.id, e.from_status, e.to_status,
                   lag(e.to_status) OVER (PARTITION BY e.job_id ORDER BY e.id) AS prev
              FROM job_events e JOIN jobs j ON j.id = e.job_id
             WHERE true {scope}
            """,
            *args,
        )
        bad: list[str] = []
        first_seen: set[UUID] = set()
        for e in events:
            frm, to, prev = e["from_status"], e["to_status"], e["prev"]
            if e["job_id"] not in first_seen:
                first_seen.add(e["job_id"])
                if frm is not None or to != "queued":
                    bad.append(f"{e['job_id']} starts with {frm} -> {to}, expected submission")
                continue
            if frm != prev:
                bad.append(f"{e['job_id']} event #{e['id']}: from={frm} but job was {prev}")
            elif not (frm == to == "running") and JobStatus(to) not in TRANSITIONS[JobStatus(frm)]:
                bad.append(f"{e['job_id']} illegal transition {frm} -> {to}")
        checks.append(
            Check(
                "state_machine",
                not bad,
                f"{len(events)} transitions, all legal and chained"
                if not bad
                else f"{len(bad)} bad",
                bad,
            )
        )

        # 3 ---- single outcome --------------------------------------------------------------
        multi = await conn.fetch(
            f"""
            SELECT e.job_id, count(*) AS n
              FROM job_events e JOIN jobs j ON j.id = e.job_id
             WHERE e.to_status = 'succeeded' {scope}
             GROUP BY e.job_id HAVING count(*) > 1
            """,
            *args,
        )
        after = await conn.fetch(
            f"""
            SELECT DISTINCT e.job_id
              FROM job_events e JOIN jobs j ON j.id = e.job_id
              JOIN job_events t ON t.job_id = e.job_id AND t.id < e.id
             WHERE t.to_status IN ('succeeded', 'cancelled') {scope}
            """,
            *args,
        )
        v = [f"{r['job_id']} succeeded {r['n']} times" for r in multi]
        v += [f"{r['job_id']} changed state after a final outcome" for r in after]
        checks.append(
            Check(
                "single_outcome",
                not v,
                "each job has exactly one final outcome" if not v else f"{len(v)} bad",
                v,
            )
        )

        # 4 ---- terminal integrity ----------------------------------------------------------
        rows = await conn.fetch(
            f"""
            SELECT j.id, j.status, j.result IS NULL AS no_result, j.error IS NULL AS no_error,
                   j.finished_at IS NULL AS no_finish, j.lease_owner
              FROM jobs j
             WHERE (   (j.status = 'succeeded' AND (j.result IS NULL OR j.finished_at IS NULL))
                    OR (j.status = 'dead'      AND (j.error IS NULL  OR j.finished_at IS NULL))
                    OR (j.status IN ('succeeded','dead','cancelled','queued','retry_scheduled')
                        AND j.lease_owner IS NOT NULL)
                    OR (j.status = 'running' AND j.lease_owner IS NULL)) {scope}
            """,
            *args,
        )
        v = [f"{r['id']} ({r['status']}) has inconsistent result/error/lease fields" for r in rows]
        checks.append(
            Check(
                "terminal_integrity",
                not v,
                "results, errors and leases consistent" if not v else f"{len(v)} bad",
                v,
            )
        )

        # 5 ---- attempt budget --------------------------------------------------------------
        rows = await conn.fetch(
            f"SELECT j.id, j.attempts, j.max_attempts FROM jobs j WHERE j.attempts > j.max_attempts {scope}",
            *args,
        )
        v = [f"{r['id']} ran {r['attempts']} attempts, budget {r['max_attempts']}" for r in rows]
        checks.append(
            Check(
                "attempt_budget",
                not v,
                "no job exceeded max_attempts" if not v else f"{len(v)} bad",
                v,
            )
        )

        # 6 ---- effects exactly once --------------------------------------------------------
        rows = await conn.fetch(
            f"""
            SELECT j.id, j.attempts,
                   (SELECT count(*) FROM effect_ledger l WHERE l.job_id = j.id)       AS ledger,
                   (SELECT count(*) FROM external_effects x WHERE x.job_id = j.id)    AS external,
                   (SELECT COALESCE(sum(x.deliveries), 0) FROM external_effects x
                     WHERE x.job_id = j.id)                                           AS deliveries
              FROM jobs j
             WHERE j.status = 'succeeded' AND j.type = 'demo.sleep' {scope}
            """,
            *args,
        )
        v = []
        absorbed = 0
        for r in rows:
            if r["ledger"] != 1:
                v.append(f"{r['id']} has {r['ledger']} ledger rows, expected 1")
            if r["external"] != 1:
                v.append(f"{r['id']} has {r['external']} external effects, expected 1")
            if r["deliveries"] > r["attempts"]:
                v.append(
                    f"{r['id']} delivered {r['deliveries']}x in {r['attempts']} attempt(s): "
                    "one attempt ran twice"
                )
            absorbed += max(0, r["deliveries"] - 1)
        checks.append(
            Check(
                "effects_exactly_once",
                not v,
                f"{len(rows)} succeeded jobs: 1 effect each; {absorbed} duplicate deliveries absorbed"
                if not v
                else f"{len(v)} bad",
                v,
            )
        )
        stats_absorbed = absorbed

        # 7 ---- no lost jobs ----------------------------------------------------------------
        if expected_job_ids is not None:
            expected = {str(x) for x in expected_job_ids}
            present = {
                str(r["id"])
                for r in await conn.fetch(
                    "SELECT id FROM jobs WHERE id = ANY($1::uuid[])", list(expected)
                )
            }
            missing = expected - present
            checks.append(
                Check(
                    "no_lost_jobs",
                    not missing,
                    f"all {len(expected)} acknowledged jobs exist"
                    if not missing
                    else f"{len(missing)} acknowledged job(s) vanished",
                    [str(m) for m in sorted(missing, key=str)],
                )
            )

        # 8 ---- outbox ----------------------------------------------------------------------
        oscope = "AND o.job_id IN (SELECT id FROM jobs j WHERE j.user_id = $1)" if user_id else ""
        backlog = await conn.fetchval(
            f"SELECT count(*) FROM outbox o WHERE o.published_at IS NULL AND o.attempts < 10 {oscope}",
            *args,
        )
        stuck_out = await conn.fetchval(
            f"SELECT count(*) FROM outbox o WHERE o.published_at IS NULL AND o.attempts >= 10 {oscope}",
            *args,
        )
        ok = stuck_out == 0 and (backlog == 0 or not require_drained)
        checks.append(
            Check(
                "outbox_drained",
                ok,
                f"{backlog} unpublished, {stuck_out} stuck"
                if not ok
                else "every message was published",
            )
        )

        # 9 ---- dead-letter consistency -----------------------------------------------------
        rows = await conn.fetch(
            f"""
            SELECT j.id FROM jobs j
             WHERE j.status = 'dead'
               AND NOT EXISTS (SELECT 1 FROM outbox o WHERE o.job_id = j.id AND o.routing_key = 'dead')
               {scope}
            """,
            *args,
        )
        v = [f"{r['id']} is dead but produced no dead-letter notice" for r in rows]
        checks.append(
            Check(
                "dlq_consistent",
                not v,
                f"{by_status.get('dead', 0)} dead job(s), all dead-lettered"
                if not v
                else f"{len(v)} bad",
                v,
            )
        )

        recovered = await conn.fetchval(
            f"""
            SELECT count(DISTINCT e.job_id) FROM job_events e JOIN jobs j ON j.id = e.job_id
             WHERE e.detail->>'reason' = 'lease_expired' {scope}
            """,
            *args,
        )
        multi_attempt = await conn.fetchval(
            f"SELECT count(*) FROM jobs j WHERE j.attempts > 1 {scope}",
            *args,
        )

    return AuditReport(
        checks=checks,
        jobs_audited=total,
        stats={
            "by_status": by_status,
            "recovered_from_expired_lease": recovered,
            "jobs_with_multiple_attempts": multi_attempt,
            "duplicate_deliveries_absorbed": stats_absorbed,
        },
    )
