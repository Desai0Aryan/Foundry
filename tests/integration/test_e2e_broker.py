"""End to end: API -> outbox -> RabbitMQ -> workers -> PostgreSQL, against a real broker."""

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
import uuid
from pathlib import Path
from uuid import UUID

import aio_pika
import orjson
import pytest
import pytest_asyncio

from foundry import broker
from foundry.reaper import Reaper
from foundry.relay import OutboxRelay
from foundry.worker import Worker
from tests.conftest import BROKER_URL, TEST_DSN, make_settings, submit, wait_until

pytestmark = [pytest.mark.integration, pytest.mark.broker]
ROOT = Path(__file__).resolve().parents[2]


@pytest_asyncio.fixture
async def stack(app):
    """A unique broker namespace plus helpers to run relay / reaper / workers in-process."""
    settings = make_settings(broker_prefix=f"ftest_{uuid.uuid4().hex[:8]}")
    try:
        conn = await broker.connect(settings)
    except Exception as exc:
        pytest.skip(f"RabbitMQ not reachable at {BROKER_URL}: {exc}")
    channel = await conn.channel()
    await broker.declare_topology(channel, settings)
    pool = app.state.pool

    stop = asyncio.Event()
    tasks: list[asyncio.Task] = []
    workers: list[Worker] = []

    class Stack:
        def __init__(self) -> None:
            self.settings, self.pool, self.channel = settings, pool, channel

        async def start_relay(self):
            relay = OutboxRelay(pool, settings)
            await relay.start()
            tasks.append(asyncio.create_task(relay.run(stop)))
            return relay

        async def start_reaper(self):
            reaper = Reaper(pool, settings)
            tasks.append(asyncio.create_task(reaper.run(stop)))
            return reaper

        async def start_worker(self, name: str, **overrides):
            s = make_settings(broker_prefix=settings.broker_prefix, **overrides)
            w = Worker(pool, s, worker_id=name)
            await w.start()
            beat = asyncio.create_task(w._heartbeat_loop())
            workers.append(w)
            tasks.append(beat)
            return w

        async def status(self, job_id):
            async with pool.acquire() as c:
                return await c.fetchval("SELECT status FROM jobs WHERE id=$1", UUID(str(job_id)))

        async def job(self, job_id):
            async with pool.acquire() as c:
                return await c.fetchrow("SELECT * FROM jobs WHERE id=$1", UUID(str(job_id)))

        async def wait_status(self, job_id, wanted, timeout=15):
            async def check():
                return (await self.status(job_id)) in wanted

            await wait_until(check, timeout)
            return await self.job(job_id)

    yield Stack()

    stop.set()
    for w in workers:
        with contextlib.suppress(Exception):
            await w.stop()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    with contextlib.suppress(Exception):
        await broker.delete_topology(channel, settings)
    await conn.close()


async def test_jobs_flow_through_broker_and_execute_exactly_once(client, auth, app, stack):
    await stack.start_relay()
    await stack.start_reaper()
    w1 = await stack.start_worker("w1")
    w2 = await stack.start_worker("w2")

    ids = [await submit(client, auth, payload={"seconds": 0.2}) for _ in range(24)]

    async def all_done():
        async with app.state.pool.acquire() as c:
            return await c.fetchval("SELECT count(*) FROM jobs WHERE status='succeeded'") == 24

    await wait_until(all_done, 30)

    async with app.state.pool.acquire() as c:
        rows = await c.fetch("SELECT id, attempts, result FROM jobs")
        ledger = await c.fetch("SELECT job_id, suppressed FROM effect_ledger")
        unpublished = await c.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NULL")
    assert len(rows) == 24 and all(r["attempts"] == 1 for r in rows)
    assert len(ledger) == 24 and len({r["job_id"] for r in ledger}) == 24
    assert all(r["suppressed"] == 0 for r in ledger)
    assert unpublished == 0
    assert w1.processed > 0 and w2.processed > 0  # work was shared across the fleet
    assert w1.processed + w2.processed == 24
    assert {str(r["id"]) for r in rows} == set(ids)


async def test_priority_jobs_overtake_a_backlog(client, auth, app, stack):
    # Fill the queue first (no consumers yet), then start one single-slot worker.
    low = [
        await submit(client, auth, priority=0, payload={"seconds": 0.05 + i / 1000})
        for i in range(6)
    ]
    high = await submit(client, auth, priority=9, payload={"seconds": 0.07})
    relay = await stack.start_relay()
    await wait_until(lambda: _published(app, 7), 10)
    await stack.start_worker("solo", worker_concurrency=1)
    await stack.wait_status(high, {"succeeded"})
    async with app.state.pool.acquire() as c:
        started = await c.fetch(
            "SELECT job_id FROM job_events WHERE to_status='running' ORDER BY id"
        )
    # With one slot and a 6-job low-priority backlog, the urgent job must start within the
    # first two (the first may already be in flight before the broker reorders).
    assert high in {str(started[0]["job_id"]), str(started[1]["job_id"])}
    assert relay.published_total >= 7
    assert len(low) == 6


async def _published(app, n):
    async with app.state.pool.acquire() as c:
        return await c.fetchval("SELECT count(*) FROM outbox WHERE published_at IS NOT NULL") >= n


async def test_failures_retry_with_backoff_then_land_in_dead_letter_queue(client, auth, app, stack):
    await stack.start_relay()
    await stack.start_reaper()
    await stack.start_worker("w1")
    job_id = await submit(
        client, auth, payload={"seconds": 0, "fail_probability": 1.0}, max_attempts=3
    )
    job = await stack.wait_status(job_id, {"dead"})

    assert job["attempts"] == 3 and job["error"]["type"] == "TransientError"
    path = [r["to_status"] for r in await _events(app, job_id)]
    assert path == [
        "queued",
        "running",
        "retry_scheduled",
        "queued",
        "running",
        "retry_scheduled",
        "queued",
        "running",
        "dead",
    ]

    # the application-level DLQ received exactly one message describing the dead job
    async def dlq_message():
        return (
            await stack.channel.get_queue(
                broker.Names(stack.settings.broker_prefix).dead_queue, ensure=False
            )
            and None
        )

    q = await stack.channel.get_queue(broker.Names(stack.settings.broker_prefix).dead_queue)
    msg = None
    for _ in range(50):
        msg = await q.get(fail=False)
        if msg:
            break
        await asyncio.sleep(0.1)
    assert msg is not None

    body = orjson.loads(msg.body)
    assert body["job_id"] == str(job_id) and body["attempts"] == 3
    await msg.ack()

    # ...and a replay brings it back (it will fail again, proving it ran)
    r = await client.post(f"/v1/jobs/{job_id}/replay", headers=auth)
    assert r.status_code == 202
    again = await stack.wait_status(job_id, {"dead"}, timeout=20)
    assert again["attempts"] == 3


async def _events(app, job_id):
    async with app.state.pool.acquire() as c:
        return await c.fetch(
            "SELECT to_status FROM job_events WHERE job_id=$1 ORDER BY id", UUID(str(job_id))
        )


async def test_timeout_is_enforced_and_retried(client, auth, app, stack):
    await stack.start_relay()
    await stack.start_reaper()
    await stack.start_worker("w1")
    job_id = await submit(client, auth, payload={"seconds": 30}, timeout_seconds=1, max_attempts=2)
    job = await stack.wait_status(job_id, {"dead"}, timeout=20)
    assert job["attempts"] == 2 and job["error"]["type"] == "timeout"
    assert "exceeded 1s" in job["error"]["message"]
    async with app.state.pool.acquire() as c:
        assert (
            await c.fetchval("SELECT count(*) FROM effect_ledger") == 0
        )  # never reached its effect


async def test_cancelling_a_running_job_interrupts_the_worker(client, auth, app, stack):
    await stack.start_relay()
    await stack.start_worker("w1")
    job_id = await submit(client, auth, payload={"seconds": 30})
    await stack.wait_status(job_id, {"running"})
    r = await client.post(f"/v1/jobs/{job_id}/cancel", headers=auth)
    assert r.status_code == 202
    job = await stack.wait_status(job_id, {"cancelled"}, timeout=5)
    assert job["lease_owner"] is None and job["attempts"] == 1


async def test_graceful_shutdown_hands_unfinished_jobs_back_without_burning_attempts(
    client, auth, app, stack
):
    await stack.start_relay()
    w1 = await stack.start_worker("w1", shutdown_grace_seconds=0.2)
    job_id = await submit(client, auth, payload={"seconds": 30})
    await stack.wait_status(job_id, {"running"})
    await w1.stop()  # SIGTERM equivalent
    job = await stack.job(job_id)
    assert job["status"] == "queued" and job["attempts"] == 0
    # a second worker picks it up and finishes it normally
    await stack.start_worker("w2")
    await stack.wait_status(job_id, {"running"})
    assert (await stack.job(job_id))["attempts"] == 1


async def test_poison_message_is_dead_lettered_by_the_broker(client, auth, app, stack):
    await stack.start_worker("w1")
    n = broker.Names(stack.settings.broker_prefix)
    exchange = await stack.channel.get_exchange(n.jobs_exchange)
    await exchange.publish(aio_pika.Message(b"this is not json"), routing_key="demo.sleep")
    await exchange.publish(aio_pika.Message(b'{"job_id": "not-a-uuid"}'), routing_key="demo.sleep")
    q = await stack.channel.get_queue(n.poison_queue)
    got = []
    for _ in range(50):
        m = await q.get(fail=False)
        if m:
            got.append(m.body)
            await m.ack()
        if len(got) == 2:
            break
        await asyncio.sleep(0.1)
    assert sorted(got) == [b"this is not json", b'{"job_id": "not-a-uuid"}']


async def test_unroutable_outbox_row_is_not_lost_and_does_not_block_others(
    client, auth, app, stack
):
    async with app.state.pool.acquire() as c:
        owner = await c.fetchval(
            "INSERT INTO users (email,name,max_concurrent_jobs,daily_job_quota,rate_limit_per_sec) VALUES ('x@y.z','x',1,1,1) RETURNING id"
        )
        jid = await c.fetchval(
            "INSERT INTO jobs (user_id,type,payload,payload_hash,request_hash,max_attempts,timeout_seconds) VALUES ($1,'nope','{}','a','b',1,1) RETURNING id",
            owner,
        )
        await c.execute(
            "INSERT INTO outbox (job_id, routing_key, payload) VALUES ($1,'nope','{}')", jid
        )
    ok = await submit(client, auth, payload={"seconds": 0.05})
    await stack.start_relay()
    await stack.start_worker("w1")
    await stack.wait_status(ok, {"succeeded"})

    async def recorded():
        async with app.state.pool.acquire() as c:
            return await c.fetchval("SELECT attempts FROM outbox WHERE routing_key='nope'") >= 1

    await wait_until(recorded, 10)
    async with app.state.pool.acquire() as c:
        row = await c.fetchrow(
            "SELECT published_at, last_error FROM outbox WHERE routing_key='nope'"
        )
    assert row["published_at"] is None and row["last_error"]


async def test_lost_broker_messages_are_rebuilt_from_postgres(client, auth, app, stack):
    """Wipe the queue (simulating broker data loss) - the redrive safety net re-enqueues."""
    relay = await stack.start_relay()
    job_id = await submit(client, auth, payload={"seconds": 0.05})
    await wait_until(lambda: _published(app, 1), 10)
    q = await stack.channel.get_queue(broker.Names(stack.settings.broker_prefix).jobs_queue)
    await q.purge()  # message gone, job still 'queued' in PostgreSQL
    async with app.state.pool.acquire() as c:
        await c.execute("UPDATE outbox SET published_at = now() - interval '1 hour'")
    from foundry import lifecycle

    assert await lifecycle.redrive_stale(app.state.pool, 60, 10) == 1
    await stack.start_worker("w1")
    await stack.wait_status(job_id, {"succeeded"})
    assert relay.published_total >= 2


# ------------------------------------------------------------------ the crash test
async def test_kill_dash_9_mid_job_recovers_without_duplicate_side_effects(
    client, auth, app, stack
):
    """The headline guarantee, with a real OS process and a real SIGKILL."""
    await stack.start_relay()
    await stack.start_reaper()

    env = {
        **os.environ,
        "PYTHONPATH": str(ROOT / "src"),
        "FOUNDRY_DATABASE_URL": TEST_DSN,
        "FOUNDRY_BROKER_URL": BROKER_URL,
        "FOUNDRY_BROKER_PREFIX": stack.settings.broker_prefix,
        "FOUNDRY_LEASE_SECONDS": "3",
        "FOUNDRY_HEARTBEAT_SECONDS": "0.5",
        "FOUNDRY_BACKOFF_BASE_SECONDS": "0.2",
        "FOUNDRY_LOG_LEVEL": "WARNING",
        "FOUNDRY_ENVIRONMENT": "test",
    }
    doomed = subprocess.Popen([sys.executable, "-m", "foundry.cli", "worker"], env=env, cwd=ROOT)
    try:
        job_id = await submit(client, auth, payload={"seconds": 3}, max_attempts=3)
        job = await stack.wait_status(job_id, {"running"}, timeout=20)
        victim = job["lease_owner"]
        assert victim and f"-{doomed.pid}-" in victim  # it really is the subprocess holding it

        doomed.send_signal(signal.SIGKILL)  # no cleanup, no goodbye
        doomed.wait(timeout=5)
        assert await stack.status(job_id) == "running"  # nobody told the database

        await stack.start_worker("rescuer")
        final = await stack.wait_status(job_id, {"succeeded"}, timeout=30)
    finally:
        if doomed.poll() is None:
            doomed.kill()

    assert final["attempts"] == 2 and final["result"]["worker"] == "rescuer"
    reasons = [r["detail"].get("reason") async for r in _aiter(await _events_full(app, job_id))]
    assert "lease_expired" in reasons
    async with app.state.pool.acquire() as c:
        ledger = await c.fetch("SELECT * FROM effect_ledger WHERE job_id=$1", UUID(str(job_id)))
    assert len(ledger) == 1 and ledger[0]["attempt"] == 2  # applied once, by the surviving attempt


async def _events_full(app, job_id):
    async with app.state.pool.acquire() as c:
        return await c.fetch(
            "SELECT to_status, detail FROM job_events WHERE job_id=$1 ORDER BY id",
            UUID(str(job_id)),
        )


async def _aiter(items):
    for i in items:
        yield i


# ------------------------------------------------------------------------------ metrics
def _sample(name: str, **labels) -> float:
    from prometheus_client import REGISTRY

    return REGISTRY.get_sample_value(name, labels) or 0.0


async def test_worker_relay_and_reaper_emit_metrics(client, auth, app, stack):
    names = {
        "ok": ("foundry_jobs_finished_total", {"type": "demo.sleep", "outcome": "succeeded"}),
        "retry": ("foundry_jobs_finished_total", {"type": "demo.sleep", "outcome": "retry"}),
        "dead": ("foundry_jobs_finished_total", {"type": "demo.sleep", "outcome": "dead"}),
        "exec": ("foundry_job_execution_seconds_count", {"type": "demo.sleep"}),
        "wait": ("foundry_job_queue_wait_seconds_count", {"type": "demo.sleep"}),
        "e2e": ("foundry_job_end_to_end_seconds_count", {"type": "demo.sleep"}),
        "e2e_sum": ("foundry_job_end_to_end_seconds_sum", {"type": "demo.sleep"}),
        "published": ("foundry_outbox_published_total", {}),
        "promoted": ("foundry_retries_promoted_total", {}),
    }
    base = {k: _sample(n, **lbl) for k, (n, lbl) in names.items()}
    delta = lambda k: _sample(names[k][0], **names[k][1]) - base[k]  # noqa: E731

    await stack.start_relay()
    await stack.start_reaper()
    await stack.start_worker("w1")
    ok_ids = [await submit(client, auth, payload={"seconds": 0.1}) for _ in range(5)]
    bad = await submit(
        client, auth, payload={"seconds": 0, "fail_probability": 1.0}, max_attempts=2
    )
    for jid in ok_ids:
        await stack.wait_status(jid, {"succeeded"})
    await stack.wait_status(bad, {"dead"})

    async def converged():  # metrics are bumped just after the DB commit; give them a moment
        return (
            delta("ok") == 5
            and delta("dead") == 1
            and delta("published") >= 8
            and delta("e2e") == 5
        )

    await wait_until(converged, 10)
    assert delta("ok") == 5 and delta("retry") == 1 and delta("dead") == 1
    assert delta("exec") == 7  # 5 successes + 2 failed attempts
    assert delta("wait") == 7  # every claim records how long the job waited
    assert delta("e2e") == 5  # end-to-end is observed for successes only
    assert delta("e2e_sum") >= 5 * 0.1  # can't be faster than the work itself
    assert delta("promoted") == 1
    assert delta("published") >= 8  # 6 submissions + 1 retry + 1 dead-letter notice


async def test_reaper_counts_recovered_jobs(client, auth, app, stack):
    from foundry import lifecycle

    before = _sample("foundry_reaper_recovered_total", outcome="retry")
    job_id = UUID(await submit(client, auth))
    await lifecycle.claim_job(app.state.pool, job_id, "doomed", 30)
    async with app.state.pool.acquire() as c:
        await c.execute("UPDATE jobs SET lease_expires_at = now() - interval '1 second'")
    stats = await Reaper(app.state.pool, stack.settings).tick()
    assert stats["recovered"] == 1
    assert _sample("foundry_reaper_recovered_total", outcome="retry") - before == 1
