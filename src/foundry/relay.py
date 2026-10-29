"""Outbox relay: moves "please run this job" messages from PostgreSQL to RabbitMQ.

Rows are claimed with ``FOR UPDATE SKIP LOCKED`` (so several relays can run side by side
without fighting over rows), published with publisher confirms, and only then marked
published, all inside one transaction. If the relay dies between the broker's confirm and
the commit, the row is published again later. That is the "at-least-once" in the design.
Duplicates are harmless because workers must win the claim UPDATE to do any work.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging

import aio_pika
import asyncpg
import orjson

from . import broker, metrics
from .config import Settings
from .runtime import install_stop_signals, sleep_or_stop

log = logging.getLogger("foundry.relay")
MAX_PUBLISH_ATTEMPTS = 10


def is_infrastructure_error(exc: BaseException) -> bool:
    """Anything except a failure caused by the message itself is the broker's problem.

    Allow-list the message-specific errors (unroutable, unserialisable) rather than
    enumerating connection errors: aio-pika, aiormq and the stdlib each have their own
    hierarchy (e.g. ChannelInvalidStateError) and a missed one would park work forever.
    """
    return not isinstance(exc, UnroutableError | ValueError | TypeError)


class UnroutableError(Exception):
    """The broker accepted the message but no queue is bound to its routing key."""


class OutboxRelay:
    def __init__(self, pool: asyncpg.Pool, settings: Settings) -> None:
        self.pool = pool
        self.settings = settings
        self.published_total = 0
        self._wake = asyncio.Event()
        self._topology: broker.Topology | None = None
        self._connection = None

    async def start(self) -> None:
        self._connection = await broker.connect(self.settings)
        channel = await self._connection.channel(publisher_confirms=True)
        self._topology = await broker.declare_topology(channel, self.settings)

    async def close(self) -> None:
        if self._connection is not None:
            await self._connection.close()

    def _message(self, row: asyncpg.Record) -> aio_pika.Message:
        payload = row["payload"]
        return aio_pika.Message(
            body=orjson.dumps(payload),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            priority=int(payload.get("priority", 5)),
            message_id=str(row["job_id"]),
            headers={"outbox_id": row["id"]},
        )

    async def _publish(self, row: asyncpg.Record) -> None:
        assert self._topology is not None
        exchange = (
            self._topology.dlx
            if row["routing_key"] == broker.DEAD_ROUTING_KEY
            else self._topology.jobs_exchange
        )
        # mandatory=True: the broker hands unroutable messages back. aio-pika surfaces that as
        # a returned message (not an exception), so we must check, or we would mark the row
        # published and silently lose the job.
        confirmation = await exchange.publish(
            self._message(row), routing_key=row["routing_key"], mandatory=True
        )
        if confirmation is not None and hasattr(confirmation, "delivery"):
            raise UnroutableError(f"no queue bound for routing key {row['routing_key']!r}")

    async def drain_once(self) -> int:
        """Publish one batch. Returns the number of rows published."""
        async with self.pool.acquire() as conn, conn.transaction():
            rows = await conn.fetch(
                """
                SELECT id, job_id, routing_key, payload FROM outbox
                 WHERE published_at IS NULL AND attempts < $2
                 ORDER BY id LIMIT $1
                   FOR UPDATE SKIP LOCKED
                """,
                self.settings.outbox_batch_size, MAX_PUBLISH_ATTEMPTS,
            )  # fmt: skip
            if not rows:
                return 0
            results = await asyncio.gather(
                *(self._publish(r) for r in rows), return_exceptions=True
            )
            ok = [r["id"] for r, res in zip(rows, results, strict=True) if res is None]
            bad = [
                (r["id"], repr(res))
                for r, res in zip(rows, results, strict=True)
                if res is not None
            ]
            infra_failed = {
                r["id"]
                for r, res in zip(rows, results, strict=True)
                if res is not None and is_infrastructure_error(res)
            }
            if ok:
                await conn.execute("UPDATE outbox SET published_at = now() WHERE id = ANY($1)", ok)
            for outbox_id, err in bad:
                # Only message-specific failures count toward the poison cap. A broker outage
                # is not the message's fault: it keeps retrying until the broker is back.
                counts = 0 if outbox_id in infra_failed else 1
                await conn.execute(
                    "UPDATE outbox SET attempts = attempts + $3, last_error = $2 WHERE id = $1",
                    outbox_id, err[:500], counts,
                )  # fmt: skip
                metrics.OUTBOX_FAILURES.inc()
                log.error("publish failed", extra={"outbox_id": outbox_id, "error": err[:200]})
        self.published_total += len(ok)
        metrics.OUTBOX_PUBLISHED.inc(len(ok))
        if bad and not ok:
            raise ConnectionError(f"all {len(bad)} publishes failed: {bad[0][1]}")
        return len(ok)

    async def run(self, stop: asyncio.Event) -> None:
        listener = await self.pool.acquire()
        on_notify = lambda *_: self._wake.set()  # noqa: E731
        try:
            await listener.add_listener("foundry_outbox", on_notify)
            log.info("relay started")
            while not stop.is_set():
                try:
                    published = await self.drain_once()
                except Exception:
                    log.exception("relay batch failed; backing off")
                    if await sleep_or_stop(stop, 1.0):
                        break
                    continue
                if published >= self.settings.outbox_batch_size:
                    continue  # more work waiting
                self._wake.clear()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(
                        self._wake.wait(), timeout=self.settings.outbox_poll_seconds
                    )
        finally:
            with contextlib.suppress(Exception):
                await listener.remove_listener("foundry_outbox", on_notify)
            await self.pool.release(listener)
            log.info("relay stopped", extra={"published_total": self.published_total})


async def run_relay(settings: Settings, pool: asyncpg.Pool) -> None:
    stop = asyncio.Event()
    install_stop_signals(stop)
    relay = OutboxRelay(pool, settings)
    await relay.start()
    try:
        await relay.run(stop)
    finally:
        await relay.close()
