"""Every job state transition after submission, in one place.

Correctness rests on two ideas:

1. **Leases.** A worker claims a job by flipping ``queued -> running`` with a conditional
   UPDATE (so exactly one claimant wins) and stamps a lease deadline. It must keep
   extending that lease with heartbeats. If it dies, the reaper sees the expired lease and
   re-schedules the job.

2. **Fencing.** ``attempts`` doubles as a fencing token. Every write a worker makes is
   guarded by ``status = 'running' AND lease_owner = me AND attempts = my_attempt``. A
   "zombie" worker that was presumed dead (GC pause, network partition) wakes up holding
   an old token and its writes simply match zero rows. It can never overwrite the result
   of the attempt that replaced it.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import Any, Literal, NamedTuple
from uuid import UUID

import asyncpg

from . import repository as repo
from .backoff import backoff_delay
from .config import Settings

log = logging.getLogger("foundry.lifecycle")

FailOutcome = Literal["retry", "dead", "cancelled", "lost"]


class Completion(NamedTuple):
    """Truthy proof that a result was recorded, plus the user-visible latency."""

    end_to_end_seconds: float


def make_error(kind: str, message: str, attempt: int, *, retryable: bool) -> dict[str, Any]:
    return {
        "type": kind,
        "message": message[:2000],
        "attempt": attempt,
        "retryable": retryable,
        "at": datetime.now(UTC).isoformat(timespec="milliseconds"),
    }


_OWNED = "id = $1 AND status = 'running' AND lease_owner = $2 AND attempts = $3"


# ------------------------------------------------------------------------------- claim
async def claim_job(
    pool: asyncpg.Pool, job_id: UUID, worker_id: str, lease_seconds: float
) -> asyncpg.Record | None:
    """Atomically take a queued job. Returns None if it is gone, cancelled or already taken."""
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            """
            UPDATE jobs
               SET status = 'running', attempts = attempts + 1, lease_owner = $2,
                   lease_expires_at = now() + make_interval(secs => $3),
                   started_at = COALESCE(started_at, now()), next_run_at = NULL
             WHERE id = $1 AND status = 'queued'
            RETURNING *, extract(epoch FROM now() - queued_at)::float8 AS queue_wait_seconds
            """,
            job_id, worker_id, float(lease_seconds),
        )  # fmt: skip
        if row is not None:
            await repo.add_event(
                conn, job_id, "queued", "running", {"worker": worker_id, "attempt": row["attempts"]}
            )
        return row


# --------------------------------------------------------------------------- completion
async def complete_job(
    pool: asyncpg.Pool, job_id: UUID, worker_id: str, attempt: int, result: Any
) -> Completion | None:
    """A Completion if the result was recorded; None if this attempt lost its lease."""
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            f"""
            UPDATE jobs
               SET status = 'succeeded', result = $4, error = NULL, finished_at = now(),
                   lease_owner = NULL, lease_expires_at = NULL
             WHERE {_OWNED}
            RETURNING extract(epoch FROM finished_at - created_at)::float8 AS e2e
            """,  # noqa: S608
            job_id, worker_id, attempt, result,
        )  # fmt: skip
        if row is None:
            return None
        await repo.add_event(
            conn, job_id, "running", "succeeded", {"worker": worker_id, "attempt": attempt}
        )
        return Completion(float(row["e2e"]))


async def _settle_failure(
    conn: asyncpg.Connection,
    row: asyncpg.Record,
    error: dict[str, Any],
    retryable: bool,
    settings: Settings,
    reason: str,
) -> FailOutcome:
    job_id, attempts = row["id"], row["attempts"]
    detail = {"reason": reason, "attempt": attempts, "error": error["type"]}

    if row["cancel_requested"]:
        await conn.execute(
            "UPDATE jobs SET status = 'cancelled', finished_at = now(), error = $2, "
            "lease_owner = NULL, lease_expires_at = NULL WHERE id = $1",
            job_id, error,
        )  # fmt: skip
        await repo.add_event(conn, job_id, "running", "cancelled", {**detail, "reason": "user"})
        return "cancelled"

    if retryable and attempts < row["max_attempts"]:
        delay = backoff_delay(attempts, settings.backoff_base_seconds, settings.backoff_cap_seconds)
        await conn.execute(
            "UPDATE jobs SET status = 'retry_scheduled', "
            "next_run_at = now() + make_interval(secs => $2), error = $3, "
            "lease_owner = NULL, lease_expires_at = NULL WHERE id = $1",
            job_id, delay, error,
        )  # fmt: skip
        await repo.add_event(
            conn, job_id, "running", "retry_scheduled", {**detail, "retry_in_s": round(delay, 2)}
        )
        return "retry"

    await conn.execute(
        "UPDATE jobs SET status = 'dead', finished_at = now(), error = $2, "
        "lease_owner = NULL, lease_expires_at = NULL WHERE id = $1",
        job_id, error,
    )  # fmt: skip
    await repo.add_event(conn, job_id, "running", "dead", detail)
    await repo.add_outbox(
        conn,
        job_id,
        "dead",
        {
            "job_id": str(job_id),
            "user_id": str(row["user_id"]),
            "type": row["type"],
            "attempts": attempts,
            "error": error,
        },
    )
    return "dead"


async def fail_job(
    pool: asyncpg.Pool,
    *,
    job_id: UUID,
    worker_id: str,
    attempt: int,
    error: dict[str, Any],
    retryable: bool,
    settings: Settings,
    reason: str = "handler_failed",
) -> FailOutcome:
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            f"SELECT * FROM jobs WHERE {_OWNED} FOR UPDATE",  # noqa: S608
            job_id, worker_id, attempt,
        )  # fmt: skip
        if row is None:
            return "lost"
        return await _settle_failure(conn, row, error, retryable, settings, reason)


async def cancel_running(pool: asyncpg.Pool, job_id: UUID, worker_id: str, attempt: int) -> bool:
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            f"""
            UPDATE jobs SET status = 'cancelled', finished_at = now(),
                   lease_owner = NULL, lease_expires_at = NULL
             WHERE {_OWNED} RETURNING id
            """,  # noqa: S608
            job_id, worker_id, attempt,
        )  # fmt: skip
        if row is None:
            return False
        await repo.add_event(
            conn, job_id, "running", "cancelled", {"reason": "user", "worker": worker_id}
        )
        return True


async def release_job(pool: asyncpg.Pool, job_id: UUID, worker_id: str, attempt: int) -> bool:
    """Graceful shutdown: hand an unfinished job straight back to the queue.

    The interrupted attempt is not charged against ``max_attempts``: it was our decision
    to stop, not the job's failure.
    """
    async with pool.acquire() as conn, conn.transaction():
        row = await conn.fetchrow(
            f"""
            UPDATE jobs SET status = 'queued', attempts = attempts - 1, queued_at = now(),
                   lease_owner = NULL, lease_expires_at = NULL
             WHERE {_OWNED} RETURNING id, type, priority
            """,  # noqa: S608
            job_id, worker_id, attempt,
        )  # fmt: skip
        if row is None:
            return False
        await repo.add_event(
            conn, job_id, "running", "queued", {"reason": "worker_shutdown", "worker": worker_id}
        )
        await repo.add_outbox(
            conn,
            job_id,
            row["type"],
            {"job_id": str(job_id), "type": row["type"], "priority": row["priority"]},
        )
        return True


# -------------------------------------------------------------------------- heartbeat
async def heartbeat(
    pool: asyncpg.Pool, worker_id: str, owned: list[tuple[UUID, int]], lease_seconds: float
) -> dict[UUID, bool]:
    """Extend leases. Returns {job_id: cancel_requested} for jobs we still own.

    A job missing from the result means the lease was lost: stop working on it.
    """
    if not owned:
        return {}
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            UPDATE jobs j
               SET lease_expires_at = now() + make_interval(secs => $2)
              FROM unnest($3::uuid[], $4::int[]) AS t(id, attempt)
             WHERE j.id = t.id AND j.attempts = t.attempt
               AND j.status = 'running' AND j.lease_owner = $1
            RETURNING j.id, j.cancel_requested
            """,
            worker_id, float(lease_seconds), [o[0] for o in owned], [o[1] for o in owned],
        )  # fmt: skip
    return {r["id"]: r["cancel_requested"] for r in rows}


async def upsert_worker(
    pool: asyncpg.Pool,
    *,
    worker_id: str,
    host: str,
    pid: int,
    concurrency: int,
    active: int,
    processed: int,
    status: str = "running",
) -> None:
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO workers (id, host, pid, concurrency, active_jobs, processed_total, status)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (id) DO UPDATE
               SET last_heartbeat_at = now(), active_jobs = EXCLUDED.active_jobs,
                   processed_total = EXCLUDED.processed_total, status = EXCLUDED.status
            """,
            worker_id, host, pid, concurrency, active, processed, status,
        )  # fmt: skip


# ------------------------------------------------------------------- reaper operations
async def reap_expired(pool: asyncpg.Pool, settings: Settings, limit: int) -> dict[str, int]:
    """Recover jobs whose worker stopped heartbeating (crash, OOM-kill, node loss)."""
    counts = {"retry": 0, "dead": 0, "cancelled": 0}
    async with pool.acquire() as conn, conn.transaction():
        rows = await conn.fetch(
            """
            SELECT * FROM jobs
             WHERE status = 'running' AND lease_expires_at < now()
             ORDER BY lease_expires_at LIMIT $1
               FOR UPDATE SKIP LOCKED
            """,
            limit,
        )
        for row in rows:
            error = make_error(
                "lease_expired",
                f"worker {row['lease_owner']} stopped heartbeating",
                row["attempts"],
                retryable=True,
            )
            outcome = await _settle_failure(conn, row, error, True, settings, "lease_expired")
            counts[outcome] += 1
            log.warning(
                "recovered job from expired lease",
                extra={"job_id": str(row["id"]), "worker": row["lease_owner"], "outcome": outcome},
            )
    return counts


async def promote_due(pool: asyncpg.Pool, limit: int) -> int:
    """retry_scheduled -> queued once the backoff delay has elapsed."""
    async with pool.acquire() as conn, conn.transaction():
        rows = await conn.fetch(
            """
            SELECT id, type, priority FROM jobs
             WHERE status = 'retry_scheduled' AND next_run_at <= now()
             ORDER BY next_run_at LIMIT $1
               FOR UPDATE SKIP LOCKED
            """,
            limit,
        )
        for row in rows:
            await conn.execute(
                "UPDATE jobs SET status = 'queued', next_run_at = NULL, queued_at = now() "
                "WHERE id = $1",
                row["id"],
            )
            await repo.add_event(
                conn, row["id"], "retry_scheduled", "queued", {"reason": "backoff_elapsed"}
            )
            await repo.add_outbox(
                conn,
                row["id"],
                row["type"],
                {"job_id": str(row["id"]), "type": row["type"], "priority": row["priority"]},
            )
        return len(rows)


async def redrive_stale(pool: asyncpg.Pool, after_seconds: float, limit: int) -> int:
    """Safety net: re-enqueue queued jobs whose message may have been lost (broker wiped).

    Harmless when wrong: a duplicate message is dropped by the claim step.
    """
    async with pool.acquire() as conn, conn.transaction():
        rows = await conn.fetch(
            """
            SELECT j.id, j.type, j.priority FROM jobs j
             WHERE j.status = 'queued'
               AND NOT EXISTS (
                     SELECT 1 FROM outbox o
                      WHERE o.job_id = j.id
                        AND (o.published_at IS NULL
                             OR o.published_at > now() - make_interval(secs => $1)))
             ORDER BY j.created_at LIMIT $2
               FOR UPDATE OF j SKIP LOCKED
            """,
            float(after_seconds), limit,
        )  # fmt: skip
        for row in rows:
            await repo.add_outbox(
                conn,
                row["id"],
                row["type"],
                {"job_id": str(row["id"]), "type": row["type"], "priority": row["priority"]},
            )
        return len(rows)


async def cleanup(pool: asyncpg.Pool, outbox_retention_seconds: int) -> dict[str, int]:
    async with pool.acquire() as conn:
        out = await conn.execute(
            "DELETE FROM outbox WHERE published_at < now() - make_interval(secs => $1)",
            float(outbox_retention_seconds),
        )
        wk = await conn.execute(
            "DELETE FROM workers WHERE last_heartbeat_at < now() - interval '1 hour'"
        )
    return {"outbox": int(out.split()[-1]), "workers": int(wk.split()[-1])}
