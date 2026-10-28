"""State-machine, lease and fencing behaviour, tested directly against PostgreSQL."""

import asyncio
from uuid import UUID

import pytest

from foundry import lifecycle
from foundry.handlers import JobContext
from tests.conftest import ADMIN, make_user, sleep_job, submit

pytestmark = pytest.mark.integration


async def row(app, job_id):
    async with app.state.pool.acquire() as conn:
        return await conn.fetchrow("SELECT * FROM jobs WHERE id = $1", UUID(str(job_id)))


async def events(app, job_id):
    async with app.state.pool.acquire() as conn:
        rs = await conn.fetch(
            "SELECT to_status, detail FROM job_events WHERE job_id=$1 ORDER BY id",
            UUID(str(job_id)),
        )
    return [(r["to_status"], r["detail"]) for r in rs]


async def expire_lease(app, job_id):
    async with app.state.pool.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET lease_expires_at = now() - interval '1 second' WHERE id=$1",
            UUID(str(job_id)),
        )


async def make_due(app, job_id):
    async with app.state.pool.acquire() as conn:
        await conn.execute(
            "UPDATE jobs SET next_run_at = now() - interval '1 second' WHERE id=$1",
            UUID(str(job_id)),
        )


def P(app):
    return app.state.pool, app.state.settings


# ----------------------------------------------------------------------------- claim
async def test_claim_is_exclusive_under_contention(client, auth, app):
    pool, _ = P(app)
    job_id = UUID(await submit(client, auth))
    results = await asyncio.gather(
        *(lifecycle.claim_job(pool, job_id, f"w{i}", 30) for i in range(12))
    )
    winners = [r for r in results if r is not None]
    assert len(winners) == 1
    job = await row(app, job_id)
    assert job["status"] == "running" and job["attempts"] == 1
    assert job["lease_owner"] == winners[0]["lease_owner"]
    assert job["lease_expires_at"] is not None


async def test_cancelled_or_unknown_jobs_cannot_be_claimed(client, auth, app):
    pool, _ = P(app)
    job_id = UUID(await submit(client, auth))
    await client.post(f"/v1/jobs/{job_id}/cancel", headers=auth)
    assert await lifecycle.claim_job(pool, job_id, "w", 30) is None
    assert await lifecycle.claim_job(pool, UUID(int=0), "w", 30) is None


async def test_successful_completion(client, auth, app):
    pool, _ = P(app)
    job_id = UUID(await submit(client, auth))
    claimed = await lifecycle.claim_job(pool, job_id, "w1", 30)
    assert await lifecycle.complete_job(pool, job_id, "w1", claimed["attempts"], {"ok": True})
    job = await row(app, job_id)
    assert job["status"] == "succeeded" and job["result"] == {"ok": True}
    assert job["lease_owner"] is None and job["finished_at"] is not None
    assert [e[0] for e in await events(app, job_id)] == ["queued", "running", "succeeded"]
    # a second completion attempt (e.g. duplicate delivery) is refused
    assert not await lifecycle.complete_job(pool, job_id, "w1", 1, {"ok": "again"})


# -------------------------------------------------------------------------- fencing
async def test_zombie_worker_cannot_overwrite_the_replacement_attempt(client, auth, app):
    pool, settings = P(app)
    job_id = UUID(await submit(client, auth, max_attempts=3))

    a = await lifecycle.claim_job(pool, job_id, "worker-A", 30)  # attempt 1
    await expire_lease(app, job_id)  # A "freezes": GC pause, partition, stopped VM
    reaped = await lifecycle.reap_expired(pool, settings, 10)
    assert reaped["retry"] == 1
    await make_due(app, job_id)
    assert await lifecycle.promote_due(pool, 10) == 1
    b = await lifecycle.claim_job(pool, job_id, "worker-B", 30)  # attempt 2
    assert (a["attempts"], b["attempts"]) == (1, 2)

    # A wakes up and tries to report. Every write is fenced out.
    assert not await lifecycle.complete_job(pool, job_id, "worker-A", 1, {"from": "A"})
    err = lifecycle.make_error("x", "late", 1, retryable=True)
    assert (
        await lifecycle.fail_job(
            pool,
            job_id=job_id,
            worker_id="worker-A",
            attempt=1,
            error=err,
            retryable=True,
            settings=settings,
        )
        == "lost"
    )
    assert not await lifecycle.cancel_running(pool, job_id, "worker-A", 1)
    assert not await lifecycle.release_job(pool, job_id, "worker-A", 1)
    assert (await lifecycle.heartbeat(pool, "worker-A", [(job_id, 1)], 30)) == {}

    assert await lifecycle.complete_job(pool, job_id, "worker-B", 2, {"from": "B"})
    job = await row(app, job_id)
    assert job["status"] == "succeeded" and job["result"] == {"from": "B"} and job["attempts"] == 2


async def test_same_worker_with_stale_attempt_number_is_fenced_too(client, auth, app):
    pool, settings = P(app)
    job_id = UUID(await submit(client, auth))
    await lifecycle.claim_job(pool, job_id, "w", 30)
    await expire_lease(app, job_id)
    await lifecycle.reap_expired(pool, settings, 10)
    await make_due(app, job_id)
    await lifecycle.promote_due(pool, 10)
    await lifecycle.claim_job(pool, job_id, "w", 30)  # same worker id, attempt 2
    assert not await lifecycle.complete_job(pool, job_id, "w", 1, {})
    assert await lifecycle.complete_job(pool, job_id, "w", 2, {})


# ----------------------------------------------------------------- retries and DLQ
async def test_retry_with_backoff_then_dead_letter(client, auth, app):
    pool, settings = P(app)
    job_id = UUID(await submit(client, auth, max_attempts=2))

    await lifecycle.claim_job(pool, job_id, "w", 30)
    err = lifecycle.make_error("TransientError", "boom", 1, retryable=True)
    out = await lifecycle.fail_job(
        pool, job_id=job_id, worker_id="w", attempt=1, error=err, retryable=True, settings=settings
    )
    assert out == "retry"
    job = await row(app, job_id)
    assert job["status"] == "retry_scheduled" and job["error"]["message"] == "boom"
    async with pool.acquire() as conn:
        delay = await conn.fetchval(
            "SELECT extract(epoch FROM next_run_at - now()) FROM jobs WHERE id=$1", job_id
        )
    assert 0.0 < delay <= settings.backoff_base_seconds  # attempt 1 -> ceiling == base

    assert await lifecycle.promote_due(pool, 10) == 0  # not due yet
    await make_due(app, job_id)
    assert await lifecycle.promote_due(pool, 10) == 1
    await lifecycle.claim_job(pool, job_id, "w", 30)
    err2 = lifecycle.make_error("TransientError", "boom again", 2, retryable=True)
    out = await lifecycle.fail_job(
        pool, job_id=job_id, worker_id="w", attempt=2, error=err2, retryable=True, settings=settings
    )
    assert out == "dead"

    job = await row(app, job_id)
    assert job["status"] == "dead" and job["attempts"] == 2 and job["finished_at"] is not None
    assert [e[0] for e in await events(app, job_id)] == [
        "queued",
        "running",
        "retry_scheduled",
        "queued",
        "running",
        "dead",
    ]
    async with pool.acquire() as conn:
        dead_msgs = await conn.fetch(
            "SELECT payload FROM outbox WHERE job_id=$1 AND routing_key='dead'", job_id
        )
    assert len(dead_msgs) == 1 and dead_msgs[0]["payload"]["attempts"] == 2


async def test_backoff_grows_between_attempts(client, auth, app):
    pool, settings = P(app)
    job_id = UUID(await submit(client, auth, max_attempts=6))
    ceilings = []
    for attempt in range(1, 5):
        await lifecycle.claim_job(pool, job_id, "w", 30)
        err = lifecycle.make_error("t", "x", attempt, retryable=True)
        await lifecycle.fail_job(
            pool,
            job_id=job_id,
            worker_id="w",
            attempt=attempt,
            error=err,
            retryable=True,
            settings=settings,
        )
        async with pool.acquire() as conn:
            delay = await conn.fetchval(
                "SELECT extract(epoch FROM next_run_at - now()) FROM jobs WHERE id=$1", job_id
            )
        ceilings.append(delay)
        await make_due(app, job_id)
        await lifecycle.promote_due(pool, 10)
    # base 0.2, cap 1.0 -> ceilings 0.2, 0.4, 0.8, 1.0 ; floors are half of those
    assert 0.1 <= ceilings[0] <= 0.2 and 0.2 <= ceilings[1] <= 0.4
    assert 0.4 <= ceilings[2] <= 0.8 and 0.5 <= ceilings[3] <= 1.0


async def test_permanent_error_skips_retries(client, auth, app):
    pool, settings = P(app)
    job_id = UUID(await submit(client, auth, max_attempts=5))
    await lifecycle.claim_job(pool, job_id, "w", 30)
    err = lifecycle.make_error("permanent", "corrupt file", 1, retryable=False)
    assert (
        await lifecycle.fail_job(
            pool,
            job_id=job_id,
            worker_id="w",
            attempt=1,
            error=err,
            retryable=False,
            settings=settings,
        )
        == "dead"
    )
    assert (await row(app, job_id))["attempts"] == 1


async def test_failure_of_cancel_requested_job_becomes_cancelled(client, auth, app):
    pool, settings = P(app)
    job_id = UUID(await submit(client, auth))
    await lifecycle.claim_job(pool, job_id, "w", 30)
    assert (await client.post(f"/v1/jobs/{job_id}/cancel", headers=auth)).status_code == 202
    err = lifecycle.make_error("t", "x", 1, retryable=True)
    assert (
        await lifecycle.fail_job(
            pool,
            job_id=job_id,
            worker_id="w",
            attempt=1,
            error=err,
            retryable=True,
            settings=settings,
        )
        == "cancelled"
    )
    assert (await row(app, job_id))["status"] == "cancelled"


# ------------------------------------------------------------------------ the reaper
async def test_reaper_recovers_only_expired_leases(client, auth, app):
    pool, settings = P(app)
    live = UUID(await submit(client, auth, payload={"seconds": 1}))
    dead = UUID(await submit(client, auth, payload={"seconds": 2}))
    await lifecycle.claim_job(pool, live, "alive-worker", 30)
    await lifecycle.claim_job(pool, dead, "crashed-worker", 30)
    await expire_lease(app, dead)
    out = await lifecycle.reap_expired(pool, settings, 10)
    assert out == {"retry": 1, "dead": 0, "cancelled": 0}
    assert (await row(app, live))["status"] == "running"
    j = await row(app, dead)
    assert j["status"] == "retry_scheduled" and j["lease_owner"] is None
    detail = (await events(app, dead))[-1][1]
    assert detail["reason"] == "lease_expired" and j["error"]["type"] == "lease_expired"


async def test_reaper_dead_letters_when_attempts_exhausted(client, auth, app):
    pool, settings = P(app)
    job_id = UUID(await submit(client, auth, max_attempts=1))
    await lifecycle.claim_job(pool, job_id, "w", 30)
    await expire_lease(app, job_id)
    assert (await lifecycle.reap_expired(pool, settings, 10))["dead"] == 1
    assert (await row(app, job_id))["status"] == "dead"


async def test_concurrent_reapers_do_not_double_process(client, auth, app):
    pool, settings = P(app)
    ids = [UUID(await submit(client, auth, payload={"seconds": i})) for i in range(8)]
    for i in ids:
        await lifecycle.claim_job(pool, i, "w", 30)
        await expire_lease(app, i)
    results = await asyncio.gather(*(lifecycle.reap_expired(pool, settings, 100) for _ in range(5)))
    assert sum(r["retry"] for r in results) == 8
    for i in ids:  # exactly one lease_expired event each
        assert sum(1 for e in await events(app, i) if e[1].get("reason") == "lease_expired") == 1


async def test_promote_creates_outbox_message_for_due_retries(client, auth, app):
    pool, settings = P(app)
    job_id = UUID(await submit(client, auth))
    await lifecycle.claim_job(pool, job_id, "w", 30)
    err = lifecycle.make_error("t", "x", 1, retryable=True)
    await lifecycle.fail_job(
        pool, job_id=job_id, worker_id="w", attempt=1, error=err, retryable=True, settings=settings
    )
    await make_due(app, job_id)
    await lifecycle.promote_due(pool, 10)
    async with pool.acquire() as conn:
        n = await conn.fetchval(
            "SELECT count(*) FROM outbox WHERE job_id=$1 AND routing_key='demo.sleep'", job_id
        )
    assert n == 2  # the original submission + the retry


async def test_redrive_requeues_jobs_whose_message_may_be_lost(client, auth, app):
    pool, _ = P(app)
    job_id = UUID(await submit(client, auth))
    assert await lifecycle.redrive_stale(pool, 60, 10) == 0  # unpublished outbox row pending
    async with pool.acquire() as conn:
        await conn.execute("UPDATE outbox SET published_at = now() - interval '2 hours'")
    assert await lifecycle.redrive_stale(pool, 60, 10) == 1
    assert await lifecycle.redrive_stale(pool, 60, 10) == 0  # fresh unpublished row again
    async with pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM outbox WHERE job_id=$1", job_id) == 2


# ------------------------------------------------- heartbeat, release, cancellation
async def test_heartbeat_extends_lease_and_reports_cancellation(client, auth, app):
    pool, _ = P(app)
    job_id = UUID(await submit(client, auth))
    await lifecycle.claim_job(pool, job_id, "w", 5)
    await expire_lease(app, job_id)
    alive = await lifecycle.heartbeat(pool, "w", [(job_id, 1)], 30)
    assert alive == {job_id: False}
    assert (await row(app, job_id))["lease_expires_at"] > (await row(app, job_id))["updated_at"]
    await client.post(f"/v1/jobs/{job_id}/cancel", headers=auth)
    assert await lifecycle.heartbeat(pool, "w", [(job_id, 1)], 30) == {job_id: True}
    assert await lifecycle.cancel_running(pool, job_id, "w", 1)
    assert (await row(app, job_id))["status"] == "cancelled"


async def test_release_on_shutdown_does_not_burn_an_attempt(client, auth, app):
    pool, _ = P(app)
    job_id = UUID(await submit(client, auth, max_attempts=2))
    await lifecycle.claim_job(pool, job_id, "w", 30)
    assert await lifecycle.release_job(pool, job_id, "w", 1)
    job = await row(app, job_id)
    assert job["status"] == "queued" and job["attempts"] == 0 and job["lease_owner"] is None
    again = await lifecycle.claim_job(pool, job_id, "w2", 30)
    assert again["attempts"] == 1


# ------------------------------------------------------------------- effect ledger
async def test_effect_ledger_applies_once_and_counts_suppressed_duplicates(client, auth, app):
    pool, _ = P(app)
    job_id = UUID(await submit(client, auth))
    first = JobContext(job_id, 1, "A", pool)
    retry = JobContext(job_id, 2, "B", pool)
    assert await first.effect("charge-card") is True
    assert await retry.effect("charge-card") is False
    assert await retry.effect("charge-card") is False
    assert await retry.effect("send-email") is True  # different effect, independent
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT effect_key, attempt, worker_id, suppressed FROM effect_ledger WHERE job_id=$1 ORDER BY effect_key",
            job_id,
        )
    assert [(r["effect_key"], r["attempt"], r["worker_id"], r["suppressed"]) for r in rows] == [
        ("charge-card", 1, "A", 2),
        ("send-email", 2, "B", 0),
    ]
    assert first.idempotency_key("charge-card") == f"{job_id}:charge-card"


# ------------------------------------------------------------------------ replay API
async def make_dead(client, auth, app, **kw):
    pool, settings = P(app)
    job_id = UUID(await submit(client, auth, max_attempts=1, **kw))
    await lifecycle.claim_job(pool, job_id, "w", 30)
    err = lifecycle.make_error("t", "x", 1, retryable=True)
    await lifecycle.fail_job(
        pool, job_id=job_id, worker_id="w", attempt=1, error=err, retryable=True, settings=settings
    )
    return job_id


async def test_replay_dead_job(client, auth, app):
    job_id = await make_dead(client, auth, app)
    r = await client.post(f"/v1/jobs/{job_id}/replay", headers=auth)
    assert r.status_code == 202
    body = r.json()
    assert body["status"] == "queued" and body["attempts"] == 0 and body["error"] is None
    assert [e[0] for e in await events(app, job_id)][-2:] == ["dead", "queued"]
    again = await client.post(f"/v1/jobs/{job_id}/replay", headers=auth)
    assert again.status_code == 409 and again.json()["code"] == "not_replayable"


async def test_replay_is_owner_only_and_dead_only(client, auth, app):
    job_id = await make_dead(client, auth, app)
    _, other = await make_user(client, email="mallory@example.com")
    assert (await client.post(f"/v1/jobs/{job_id}/replay", headers=other)).status_code == 404
    live = await submit(client, auth, payload={"seconds": 9})
    assert (await client.post(f"/v1/jobs/{live}/replay", headers=auth)).status_code == 409


async def test_replay_respects_deduplication(client, auth, app):
    dead = await make_dead(client, auth, app, deduplicate=True, payload={"seconds": 3})
    twin = await client.post(
        "/v1/jobs", json=sleep_job(deduplicate=True, payload={"seconds": 3}), headers=auth
    )
    assert twin.status_code == 201  # the slot was free once the first job died
    r = await client.post(f"/v1/jobs/{dead}/replay", headers=auth)
    assert r.status_code == 409 and r.json()["code"] == "duplicate_active_job"
    assert (await row(app, dead))["status"] == "dead"  # untouched


# ------------------------------------------------------------------------ admin API
async def test_admin_stats_and_workers(client, auth, app):
    pool, _ = P(app)
    await submit(client, auth, payload={"seconds": 1})
    dead = await make_dead(client, auth, app, payload={"seconds": 2})
    await lifecycle.upsert_worker(
        pool, worker_id="w-1", host="h", pid=1, concurrency=4, active=2, processed=7
    )
    stats = (await client.get("/v1/admin/stats", headers=ADMIN)).json()
    assert stats["queue_depth"] == 1 and stats["dead_letter_count"] == 1
    assert stats["outbox_backlog"] >= 1 and stats["oldest_queued_age_seconds"] >= 0
    workers = (await client.get("/v1/admin/workers", headers=ADMIN)).json()
    assert (
        workers[0]["id"] == "w-1"
        and workers[0]["alive"] is True
        and workers[0]["processed_total"] == 7
    )
    assert (await client.get("/v1/admin/stats")).status_code == 403
    assert dead is not None
