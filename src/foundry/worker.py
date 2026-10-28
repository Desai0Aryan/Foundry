"""Worker: consumes job messages and executes handlers under a lease.

Lifecycle of one message
------------------------
1. claim   : conditional UPDATE queued -> running (+ lease). Lost the race / job cancelled /
             job finished already?  -> ack and drop the (duplicate) message.
2. execute : handler runs under ``asyncio.timeout(job.timeout_seconds)``; a background
             heartbeat keeps extending the lease and watches for cancellation.
3. settle  : succeeded | retry_scheduled | dead, each guarded by the fencing token.
4. ack     : only AFTER the database transition committed.

If the process is killed at any point, nothing is lost: the unacked message is redelivered
(and dropped by step 1 if the job is still leased) and the reaper reclaims the job when the
lease expires.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import socket
import uuid
from uuid import UUID

import asyncpg
import orjson
from aio_pika.abc import AbstractIncomingMessage

from . import broker, lifecycle, metrics
from .config import Settings
from .handlers import HANDLERS, JobContext
from .jobs_errors import PermanentError
from .runtime import install_stop_signals

log = logging.getLogger("foundry.worker")


class Worker:
    def __init__(
        self, pool: asyncpg.Pool, settings: Settings, worker_id: str | None = None
    ) -> None:
        self.pool = pool
        self.settings = settings
        self.id = worker_id or f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"
        self.processed = 0
        self._tasks: set[asyncio.Task[None]] = set()
        # job_id -> (attempt, running task). Cancelled by heartbeat / shutdown.
        self._active: dict[UUID, tuple[int, asyncio.Task[None]]] = {}
        self._cancel_reason: dict[UUID, str] = {}
        # Jobs whose handler has finished and whose outcome is being written to PostgreSQL.
        # They must never be interrupted: cancelling mid-transaction would roll the result back.
        self._settling: set[UUID] = set()
        self._connection = None
        self._channel = None
        self._queue = None
        self._consumer_tag: str | None = None
        self._status = "running"

    # ------------------------------------------------------------------ startup/shutdown
    async def start(self) -> None:
        self._connection = await broker.connect(self.settings)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self.settings.worker_concurrency)
        topology = await broker.declare_topology(self._channel, self.settings)
        self._queue = topology.jobs_queue
        await self._beat()
        self._consumer_tag = await self._queue.consume(self._on_message)
        log.info("worker started", extra={"worker": self.id})

    async def stop(self) -> None:
        """Graceful: stop taking work, let running jobs finish, hand back the rest."""
        if self._status != "running":
            return  # already stopping or stopped: idempotent
        self._status = "draining"
        if self._queue is not None and self._consumer_tag is not None:
            with contextlib.suppress(Exception):
                await self._queue.cancel(self._consumer_tag)
        pending = {t for t in self._tasks if not t.done()}
        if pending:
            _, still = await asyncio.wait(pending, timeout=self.settings.shutdown_grace_seconds)
            for job_id in list(self._active):
                self._interrupt(job_id, "shutdown")
            if still:
                await asyncio.wait(still, timeout=10)
        self._status = "stopped"
        with contextlib.suppress(Exception):
            await self._beat()
        if self._connection is not None:
            with contextlib.suppress(Exception):
                await self._connection.close()
        log.info("worker stopped", extra={"worker": self.id, "processed": self.processed})

    async def run(self, stop: asyncio.Event) -> None:
        await self.start()
        beat_task = asyncio.create_task(self._heartbeat_loop(), name="heartbeat")
        try:
            await stop.wait()
        finally:
            beat_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await beat_task
            await self.stop()

    # ------------------------------------------------------------------------- heartbeat
    async def _beat(self) -> None:
        await lifecycle.upsert_worker(
            self.pool,
            worker_id=self.id,
            host=socket.gethostname(),
            pid=os.getpid(),
            concurrency=self.settings.worker_concurrency,
            active=len(self._active),
            processed=self.processed,
            status=self._status,
        )

    def _interrupt(self, job_id: UUID, reason: str) -> None:
        entry = self._active.get(job_id)
        if entry is not None and job_id not in self._cancel_reason:
            self._cancel_reason[job_id] = reason
            entry[1].cancel()

    async def _heartbeat_once(self) -> None:
        owned = [(jid, attempt) for jid, (attempt, _) in self._active.items()]
        alive = await lifecycle.heartbeat(self.pool, self.id, owned, self.settings.lease_seconds)
        for jid, _ in owned:
            # The query was in flight while jobs kept running. A job that finished, or is
            # writing its outcome right now, is absent from `alive` for a good reason: it
            # is NOT a lost lease. (A synchronous check: nothing can change between it
            # and the interrupt below, because there is no await in between.)
            if jid in self._settling or jid not in self._active:
                continue
            if jid not in alive:
                log.warning("lease lost; abandoning job", extra={"job_id": str(jid)})
                metrics.LEASE_LOST.inc()
                self._interrupt(jid, "lease_lost")
            elif alive[jid]:
                self._interrupt(jid, "cancel_requested")
        await self._beat()

    async def _heartbeat_loop(self) -> None:
        while True:
            await asyncio.sleep(self.settings.heartbeat_seconds)
            try:
                await self._heartbeat_once()
            except Exception:
                # Transient DB trouble: keep trying. If we stay silent past the lease the
                # reaper will rescue our jobs and fencing will neutralise our late writes.
                metrics.HEARTBEAT_FAILURES.inc()
                log.exception("heartbeat failed")

    # ------------------------------------------------------------------------- messages
    async def _on_message(self, message: AbstractIncomingMessage) -> None:
        task = asyncio.create_task(self._handle(message))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    async def _handle(self, message: AbstractIncomingMessage) -> None:
        try:
            body = orjson.loads(message.body)
            job_id = UUID(body["job_id"])
        except (orjson.JSONDecodeError, KeyError, ValueError, TypeError):
            log.error("poison message; dead-lettering", extra={"message_id": message.message_id})
            await self._settle_message(message, "reject")
            return

        try:
            job = await lifecycle.claim_job(self.pool, job_id, self.id, self.settings.lease_seconds)
        except (asyncpg.PostgresError, OSError):
            log.exception("claim failed; requeueing message")
            await asyncio.sleep(1)
            await self._settle_message(message, "requeue")
            return

        if job is None:  # duplicate, cancelled, or already finished: nothing to do
            metrics.DUPLICATE_DELIVERIES.inc()
            await self._settle_message(message, "ack")
            return
        metrics.JOB_QUEUE_WAIT.labels(job["type"]).observe(max(0.0, job["queue_wait_seconds"]))

        attempt = job["attempts"]
        runner = asyncio.create_task(self._run_job(job), name=f"job-{job_id}")
        self._active[job_id] = (attempt, runner)
        metrics.WORKER_ACTIVE.set(len(self._active))
        try:
            await asyncio.wait({runner})  # never raises, even if `runner` was cancelled
        finally:
            self._active.pop(job_id, None)
            self._cancel_reason.pop(job_id, None)
            self._settling.discard(job_id)
            metrics.WORKER_ACTIVE.set(len(self._active))
        self.processed += 1
        await self._settle_message(message, "ack")

    @staticmethod
    async def _settle_message(message: AbstractIncomingMessage, how: str) -> None:
        try:
            if how == "ack":
                await message.ack()
            elif how == "reject":
                await message.reject(requeue=False)
            else:
                await message.nack(requeue=True)
        except Exception:
            # Channel died. The broker redelivers; the claim step makes that harmless.
            log.warning("could not settle message", extra={"how": how})

    # ------------------------------------------------------------------------ execution
    async def _run_job(self, job: asyncpg.Record) -> None:
        job_id, attempt = job["id"], job["attempts"]
        log_extra = {
            "job_id": str(job_id),
            "type": job["type"],
            "attempt": attempt,
            "worker": self.id,
        }
        handler = HANDLERS.get(job["type"])
        ctx = JobContext(job_id, attempt, self.id, self.pool, self.settings.sim_speed)
        started = asyncio.get_running_loop().time()
        try:
            if handler is None:
                raise PermanentError(f"no handler registered for {job['type']!r}")
            async with asyncio.timeout(job["timeout_seconds"]):
                result = await handler(ctx, job["payload"])
        except asyncio.CancelledError:
            self._settling.add(job_id)
            reason = self._cancel_reason.get(job_id, "unknown")
            log.info("job interrupted", extra={**log_extra, "reason": reason})
            self._observe_execution(job["type"], started)
            await self._on_interrupted(job["type"], job_id, attempt, reason)
            return  # swallowed on purpose: the supervising coroutine just waits for us
        except TimeoutError:
            self._settling.add(job_id)
            self._observe_execution(job["type"], started)
            await self._on_failure(
                job, attempt, "timeout", f"exceeded {job['timeout_seconds']}s timeout", True
            )
            return
        except PermanentError as exc:
            self._settling.add(job_id)
            self._observe_execution(job["type"], started)
            await self._on_failure(job, attempt, "permanent", str(exc), False)
            return
        except Exception as exc:
            self._settling.add(job_id)
            log.warning("job failed", extra={**log_extra, "error": repr(exc)})
            self._observe_execution(job["type"], started)
            await self._on_failure(job, attempt, type(exc).__name__, str(exc) or repr(exc), True)
            return

        self._settling.add(job_id)
        self._observe_execution(job["type"], started)
        try:
            recorded = await lifecycle.complete_job(self.pool, job_id, self.id, attempt, result)
        except Exception:
            log.exception(
                "could not record result; lease will expire and job will retry", extra=log_extra
            )
            return
        elapsed = asyncio.get_running_loop().time() - started
        if recorded:
            metrics.JOBS_FINISHED.labels(job["type"], "succeeded").inc()
            metrics.JOB_END_TO_END.labels(job["type"]).observe(recorded.end_to_end_seconds)
            log.info("job succeeded", extra={**log_extra, "seconds": round(elapsed, 3)})
        else:
            metrics.JOBS_FINISHED.labels(job["type"], "lost").inc()
            log.warning("result discarded: lease lost to a newer attempt", extra=log_extra)

    async def _on_failure(
        self, job: asyncpg.Record, attempt: int, kind: str, message: str, retryable: bool
    ) -> None:
        error = lifecycle.make_error(kind, message, attempt, retryable=retryable)
        try:
            outcome = await lifecycle.fail_job(
                self.pool,
                job_id=job["id"],
                worker_id=self.id,
                attempt=attempt,
                error=error,
                retryable=retryable,
                settings=self.settings,
            )
            metrics.JOBS_FINISHED.labels(job["type"], outcome).inc()
            log.info(
                "job failure settled",
                extra={"job_id": str(job["id"]), "outcome": outcome, "kind": kind},
            )
        except Exception:
            log.exception(
                "could not record failure; lease will expire", extra={"job_id": str(job["id"])}
            )

    @staticmethod
    def _observe_execution(job_type: str, started: float) -> None:
        elapsed = asyncio.get_running_loop().time() - started
        metrics.JOB_EXECUTION.labels(job_type).observe(elapsed)

    async def _on_interrupted(self, job_type: str, job_id: UUID, attempt: int, reason: str) -> None:
        try:
            if reason == "cancel_requested":
                if await lifecycle.cancel_running(self.pool, job_id, self.id, attempt):
                    metrics.JOBS_FINISHED.labels(job_type, "cancelled").inc()
            elif reason == "shutdown":
                if await lifecycle.release_job(self.pool, job_id, self.id, attempt):
                    metrics.JOBS_FINISHED.labels(job_type, "released").inc()
            else:
                # lease_lost: someone else owns the job now; writing anything would be wrong.
                metrics.JOBS_FINISHED.labels(job_type, "lost").inc()
        except Exception:
            log.exception("could not settle interrupted job", extra={"job_id": str(job_id)})


async def run_worker(settings: Settings, pool: asyncpg.Pool) -> None:
    stop = asyncio.Event()
    install_stop_signals(stop)
    await Worker(pool, settings).run(stop)
