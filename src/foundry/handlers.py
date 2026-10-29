"""Job handlers.

The handlers below *simulate* heavy work (conversion, transcoding, inference) with
deterministic results and realistic timing, so the platform's reliability machinery can be
demonstrated and tested without gigabytes of media. To plug in real work, replace the body
of a handler. The contract stays the same:

* return a JSON-serialisable result, or raise ``PermanentError`` / ``TransientError``;
* be safe to run more than once: wrap every side effect in ``ctx.effect(key)`` and pass
  ``ctx.idempotency_key(key)`` to any external system that supports it.
"""

from __future__ import annotations

import asyncio
import hashlib
import random
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any
from uuid import UUID

import asyncpg

from .jobs_errors import TransientError


@dataclass
class JobContext:
    job_id: UUID
    attempt: int
    worker_id: str
    pool: asyncpg.Pool
    sim_speed: float = 1.0

    def idempotency_key(self, name: str) -> str:
        """Stable across attempts: hand this to external APIs so *they* can deduplicate too."""
        return f"{self.job_id}:{name}"

    async def effect(self, key: str) -> bool:
        """Record that an externally visible side effect is being applied.

        Returns True the first time and False on every later call (a retry, a re-delivered
        message, a zombie worker): the caller must skip the effect when it gets False.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO effect_ledger (job_id, effect_key, attempt, worker_id)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (job_id, effect_key)
                DO UPDATE SET suppressed = effect_ledger.suppressed + 1
                RETURNING (xmax = 0) AS inserted
                """,
                self.job_id, key, self.attempt, self.worker_id,
            )  # fmt: skip
        return bool(row["inserted"])

    async def deliver(self, effect_key: str) -> bool:
        """Call an external system, passing a stable idempotency key.

        The receiver (``external_effects``) applies the effect once per key and counts every
        delivery. Returns True if this delivery was the first. This is the standard way to get
        effectively-once behaviour out of at-least-once execution: a crash right after the call
        and before the job is marked done means the retry delivers again, and the receiver
        absorbs it.
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO external_effects
                    (idempotency_key, job_id, effect_key, first_attempt, first_worker)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (idempotency_key)
                DO UPDATE SET deliveries = external_effects.deliveries + 1, last_at = now()
                RETURNING (xmax = 0) AS first
                """,
                self.idempotency_key(effect_key), self.job_id, effect_key, self.attempt, self.worker_id,
            )  # fmt: skip
        return bool(row["first"])

    async def work(self, seconds: float) -> None:
        await asyncio.sleep(seconds / self.sim_speed)


Handler = Callable[[JobContext, dict[str, Any]], Awaitable[dict[str, Any]]]


def _digest(*parts: object) -> int:
    return int.from_bytes(hashlib.sha256("|".join(map(str, parts)).encode()).digest()[:8], "big")


async def demo_sleep(ctx: JobContext, p: dict[str, Any]) -> dict[str, Any]:
    await asyncio.sleep(p["seconds"])  # exact: the demos rely on predictable durations
    if p["fail_probability"] and random.random() < p["fail_probability"]:  # noqa: S311
        raise TransientError("injected failure")
    key = p.get("side_effect_key") or "demo-effect"
    first_delivery = await ctx.deliver(key)  # the "external call"; a crash after it is survivable
    applied = await ctx.effect(key)  # local ledger: recorded atomically in PostgreSQL
    return {
        "slept_seconds": p["seconds"],
        "effect_applied": applied,
        "first_delivery": first_delivery,
        "worker": ctx.worker_id,
    }


async def document_convert(ctx: JobContext, p: dict[str, Any]) -> dict[str, Any]:
    await ctx.work(1.0 + (_digest(p["source_url"]) % 100) / 100)
    if p["ocr"]:
        await ctx.work(1.0)
    pages = 1 + _digest(p["source_url"], "pages") % 80
    applied = await ctx.effect("artifact")
    return {
        "simulated": True,
        "output_url": f"s3://foundry-artifacts/{ctx.job_id}.{p['output_format']}",
        "pages": pages,
        "ocr": p["ocr"],
        "artifact_written": applied,
    }


async def video_transcode(ctx: JobContext, p: dict[str, Any]) -> dict[str, Any]:
    segments = 6
    for _ in range(segments):  # progress in small steps so cancellation lands promptly
        await ctx.work(0.7)
    applied = await ctx.effect("artifact")
    return {
        "simulated": True,
        "output_url": f"s3://foundry-artifacts/{ctx.job_id}-{p['preset']}.mp4",
        "codec": p["codec"],
        "segments": segments,
        "artifact_written": applied,
    }


async def ml_inference(ctx: JobContext, p: dict[str, Any]) -> dict[str, Any]:
    outputs = []
    for item in p["inputs"]:
        await ctx.work(0.05)
        score = (_digest(p["model"], item) % 10_000) / 10_000
        outputs.append({"label": "positive" if score >= 0.5 else "negative", "score": score})
    applied = await ctx.effect("predictions")
    return {"simulated": True, "model": p["model"], "outputs": outputs, "persisted": applied}


HANDLERS: dict[str, Handler] = {
    "demo.sleep": demo_sleep,
    "document.convert": document_convert,
    "video.transcode": video_transcode,
    "ml.inference": ml_inference,
}
