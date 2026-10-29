"""The auditor must catch real violations, or a green audit proves nothing."""

from uuid import UUID, uuid4

import pytest

from foundry import lifecycle
from foundry.audit import run_audit
from foundry.handlers import JobContext
from tests.conftest import make_user, submit

pytestmark = pytest.mark.integration


class Env:
    def __init__(self, client, auth, app):
        self.client, self.auth, self.app = client, auth, app
        self.pool, self.settings = app.state.pool, app.state.settings
        self.n = 0

    async def new(self, **kw) -> UUID:
        self.n += 1
        return UUID(await submit(self.client, self.auth, payload={"seconds": self.n}, **kw))

    async def execute(self, sql, *args):
        async with self.pool.acquire() as c:
            return await c.execute(sql, *args)

    async def succeed(self, job_id, worker="w1", attempt=1):
        """Run one attempt to success: external delivery + ledger + completion."""
        claimed = await lifecycle.claim_job(self.pool, job_id, worker, 30)
        assert claimed["attempts"] == attempt
        ctx = JobContext(job_id, attempt, worker, self.pool)
        await ctx.deliver("demo-effect")
        await ctx.effect("demo-effect")
        assert await lifecycle.complete_job(self.pool, job_id, worker, attempt, {"ok": 1})

    async def crash_then_succeed(self, job_id):
        """Attempt 1 delivers then its worker dies; attempt 2 re-delivers and completes."""
        await lifecycle.claim_job(self.pool, job_id, "doomed", 30)
        await JobContext(job_id, 1, "doomed", self.pool).deliver("demo-effect")
        await self.execute(
            "UPDATE jobs SET lease_expires_at = now() - interval '1 s' WHERE id=$1", job_id
        )
        await lifecycle.reap_expired(self.pool, self.settings, 10)
        await self.execute(
            "UPDATE jobs SET next_run_at = now() - interval '1 s' WHERE id=$1", job_id
        )
        await lifecycle.promote_due(self.pool, 10)
        await self.succeed(job_id, "rescuer", attempt=2)

    async def publish_all(self):
        await self.execute("UPDATE outbox SET published_at = now() WHERE published_at IS NULL")

    async def audit(self, **kw):
        return await run_audit(self.pool, **kw)


@pytest.fixture
async def env(client, auth, app):
    return Env(client, auth, app)


def failed(report) -> set[str]:
    return {c.name for c in report.checks if not c.passed}


async def clean_world(env: Env):
    ok = await env.new()
    await env.succeed(ok)
    crashed = await env.new()
    await env.crash_then_succeed(crashed)
    dead = await env.new(max_attempts=1)
    await lifecycle.claim_job(env.pool, dead, "w", 30)
    await lifecycle.fail_job(
        env.pool, job_id=dead, worker_id="w", attempt=1, retryable=True, settings=env.settings,
        error=lifecycle.make_error("t", "x", 1, retryable=True),
    )  # fmt: skip
    cancelled = await env.new()
    await env.client.post(f"/v1/jobs/{cancelled}/cancel", headers=env.auth)
    await env.publish_all()
    return {"ok": ok, "crashed": crashed, "dead": dead, "cancelled": cancelled}


# ----------------------------------------------------------------------- the happy path
async def test_a_healthy_system_passes_every_check(env):
    jobs = await clean_world(env)
    report = await env.audit(expected_job_ids=set(jobs.values()))
    assert report.passed, report.render(color=False)
    assert report.jobs_audited == 4
    assert report.stats["recovered_from_expired_lease"] == 1
    assert report.stats["duplicate_deliveries_absorbed"] == 1  # the crash's re-delivery
    names = {c.name for c in report.checks}
    assert {"no_stuck_jobs", "state_machine", "single_outcome", "effects_exactly_once",
            "no_lost_jobs", "outbox_drained", "dlq_consistent"} <= names  # fmt: skip
    assert "ALL INVARIANTS HELD" in report.render(color=False)


async def test_an_empty_database_passes(env):
    assert (await env.audit()).passed


# --------------------------------------------------------------- every violation is caught
async def test_catches_stuck_jobs_unless_work_is_still_in_flight(env):
    await clean_world(env)
    await env.new()  # left queued
    assert failed(await env.audit()) == {"no_stuck_jobs", "outbox_drained"}
    assert (await env.audit(require_drained=False)).passed


async def test_catches_illegal_transition(env):
    jobs = await clean_world(env)
    await env.execute(
        "INSERT INTO job_events (job_id, from_status, to_status) VALUES ($1, 'succeeded', 'running')",
        jobs["ok"],
    )
    r = await env.audit()
    assert "state_machine" in failed(r) and "single_outcome" in failed(r)


async def test_catches_a_broken_event_chain(env):
    jobs = await clean_world(env)
    await env.execute(
        "INSERT INTO job_events (job_id, from_status, to_status) VALUES ($1, 'queued', 'running')",
        jobs["ok"],  # the job was 'succeeded', not 'queued'
    )
    assert "state_machine" in failed(await env.audit())


async def test_catches_a_job_that_succeeded_twice(env):
    jobs = await clean_world(env)
    await env.execute(
        "INSERT INTO job_events (job_id, from_status, to_status) VALUES ($1, 'running', 'succeeded')",
        jobs["ok"],
    )
    assert "single_outcome" in failed(await env.audit())


async def test_catches_inconsistent_terminal_state(env):
    jobs = await clean_world(env)
    await env.execute("UPDATE jobs SET result = NULL WHERE id = $1", jobs["ok"])
    assert "terminal_integrity" in failed(await env.audit())


async def test_catches_a_lease_leaked_on_a_finished_job(env):
    jobs = await clean_world(env)
    await env.execute("UPDATE jobs SET lease_owner = 'ghost' WHERE id = $1", jobs["ok"])
    assert "terminal_integrity" in failed(await env.audit())


async def test_catches_attempt_budget_overrun(env):
    jobs = await clean_world(env)
    await env.execute("UPDATE jobs SET attempts = 99 WHERE id = $1", jobs["ok"])
    assert "attempt_budget" in failed(await env.audit())


async def test_catches_one_attempt_delivering_twice(env):
    """The actual double-execution bug: two workers running the SAME attempt number."""
    jobs = await clean_world(env)
    await env.execute("UPDATE external_effects SET deliveries = 2 WHERE job_id = $1", jobs["ok"])
    r = await env.audit()
    assert "effects_exactly_once" in failed(r)
    assert "one attempt ran twice" in "\n".join(
        next(c for c in r.checks if c.name == "effects_exactly_once").violations
    )


async def test_redelivery_across_attempts_is_fine_but_extra_ones_are_not(env):
    jobs = await clean_world(env)  # crashed job: 2 attempts, 2 deliveries -> fine
    assert (await env.audit()).passed
    await env.execute(
        "UPDATE external_effects SET deliveries = 3 WHERE job_id = $1", jobs["crashed"]
    )
    assert "effects_exactly_once" in failed(await env.audit())


async def test_catches_a_missing_or_duplicated_effect(env):
    jobs = await clean_world(env)
    await env.execute("DELETE FROM external_effects WHERE job_id = $1", jobs["ok"])
    assert "effects_exactly_once" in failed(await env.audit())
    await env.execute("DELETE FROM effect_ledger WHERE job_id = $1", jobs["crashed"])
    r = await env.audit()
    assert len(next(c for c in r.checks if c.name == "effects_exactly_once").violations) == 2


async def test_catches_lost_acknowledged_jobs(env):
    jobs = await clean_world(env)
    ghost = uuid4()
    r = await env.audit(expected_job_ids={*jobs.values(), ghost})
    assert failed(r) == {"no_lost_jobs"}
    assert str(ghost) in next(c for c in r.checks if c.name == "no_lost_jobs").violations


async def test_catches_unpublished_and_stuck_outbox_rows(env):
    jobs = await clean_world(env)
    await env.execute(
        "INSERT INTO outbox (job_id, routing_key, payload) VALUES ($1, 'demo.sleep', '{}')",
        jobs["ok"],
    )
    assert failed(await env.audit()) == {"outbox_drained"}
    assert (await env.audit(require_drained=False)).passed  # a backlog is fine while running
    await env.execute("UPDATE outbox SET attempts = 10 WHERE published_at IS NULL")
    assert "outbox_drained" in failed(await env.audit(require_drained=False))  # stuck never is


async def test_catches_a_dead_job_with_no_dead_letter_notice(env):
    jobs = await clean_world(env)
    await env.execute("DELETE FROM outbox WHERE job_id = $1 AND routing_key = 'dead'", jobs["dead"])
    assert failed(await env.audit()) == {"dlq_consistent"}


async def test_audit_can_be_scoped_to_one_user(client, env):
    jobs = await clean_world(env)
    other_user, other_auth = await make_user(client, "other@example.com")
    stray = UUID(await submit(client, other_auth))  # left queued: a violation, but not ours
    me = (await client.get("/v1/me", headers=env.auth)).json()["id"]
    assert not (await env.audit()).passed
    scoped = await env.audit(user_id=UUID(me), expected_job_ids=set(jobs.values()))
    assert scoped.passed and scoped.jobs_audited == 4
    assert not (await env.audit(user_id=UUID(other_user["id"]))).passed
    assert stray


async def test_report_renders_and_serialises(env):
    jobs = await clean_world(env)
    await env.execute("UPDATE jobs SET attempts = 99 WHERE id = $1", jobs["ok"])
    r = await env.audit()
    text = r.render(color=False)
    assert "FAIL  attempt_budget" in text and "INVARIANT VIOLATIONS FOUND" in text
    d = r.as_dict()
    assert d["passed"] is False and d["jobs_audited"] == 4
    assert any(c["name"] == "attempt_budget" and not c["passed"] for c in d["checks"])
