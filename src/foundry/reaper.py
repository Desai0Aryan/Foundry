"""Reaper: the part of the system that makes failures boring.

Every tick it (1) recovers jobs whose worker stopped heartbeating, (2) promotes retries
whose backoff elapsed, and occasionally (3) re-enqueues queued jobs whose message may have
been lost and (4) prunes old outbox rows. It is stateless and uses SKIP LOCKED, so running
several replicas is safe.
"""

from __future__ import annotations

import asyncio
import logging
import time

import asyncpg

from . import lifecycle, metrics
from .config import Settings
from .runtime import install_stop_signals, sleep_or_stop

log = logging.getLogger("foundry.reaper")


class Reaper:
    def __init__(self, pool: asyncpg.Pool, settings: Settings) -> None:
        self.pool = pool
        self.settings = settings
        self._last_redrive = 0.0
        self._last_cleanup = 0.0

    async def tick(self) -> dict[str, int]:
        s = self.settings
        reaped = await lifecycle.reap_expired(self.pool, s, s.reaper_batch_size)
        promoted = await lifecycle.promote_due(self.pool, s.reaper_batch_size)
        for outcome, n in reaped.items():
            if n:
                metrics.REAPER_RECOVERED.labels(outcome).inc(n)
        metrics.RETRIES_PROMOTED.inc(promoted)
        redriven = 0
        now = time.monotonic()
        if now - self._last_redrive > 30:
            self._last_redrive = now
            redriven = await lifecycle.redrive_stale(
                self.pool, s.redrive_after_seconds, s.reaper_batch_size
            )
            metrics.REDRIVEN.inc(redriven)
        if now - self._last_cleanup > 60:
            self._last_cleanup = now
            await lifecycle.cleanup(self.pool, s.outbox_retention_seconds)
        return {
            "recovered": sum(reaped.values()),
            "dead_from_lease": reaped["dead"],
            "promoted": promoted,
            "redriven": redriven,
        }

    async def run(self, stop: asyncio.Event) -> None:
        log.info("reaper started")
        while not stop.is_set():
            try:
                stats = await self.tick()
                if any(stats.values()):
                    log.info("reaper tick", extra=stats)
            except Exception:
                log.exception("reaper tick failed")
            if await sleep_or_stop(stop, self.settings.reaper_interval_seconds):
                break
        log.info("reaper stopped")


async def run_reaper(settings: Settings, pool: asyncpg.Pool) -> None:
    stop = asyncio.Event()
    install_stop_signals(stop)
    await Reaper(pool, settings).run(stop)
