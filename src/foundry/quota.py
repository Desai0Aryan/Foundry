"""Per-user quotas.

Daily submission quota
    Fast path: an atomic Redis counter per (user, UTC day). If the counter is missing (first
    request of the day, or Redis was flushed) it is *seeded from PostgreSQL*, which holds the
    real job rows, so a Redis restart never hands out a fresh quota. If Redis is down the
    check falls back to counting rows in PostgreSQL (slower, still correct).

    Only jobs that are actually *created* consume quota. The slot is reserved before the insert
    and refunded when the request turns out to be an idempotent replay, a dedup hit, or fails.

Concurrent-jobs limit
    Enforced inside the submission transaction under a per-user advisory lock (see
    ``service.submit_job``), because it must be exact, not approximate.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from uuid import UUID

import asyncpg
import redis.asyncio as aioredis
from redis.exceptions import RedisError

from . import metrics
from .config import Settings
from .errors import ApiError

log = logging.getLogger("foundry.quota")

# returns {code, used}: code 1 = reserved, 0 = over limit, -1 = counter missing (seed it)
RESERVE_LUA = """
local v = redis.call('GET', KEYS[1])
if not v then return {-1, 0} end
v = tonumber(v)
if v >= tonumber(ARGV[1]) then return {0, v} end
return {1, redis.call('INCR', KEYS[1])}
"""
REFUND_LUA = """
local v = redis.call('GET', KEYS[1])
if v and tonumber(v) > 0 then return redis.call('DECR', KEYS[1]) end
return 0
"""


def utc_day_start(now: datetime | None = None) -> datetime:
    now = now or datetime.now(UTC)
    return now.replace(hour=0, minute=0, second=0, microsecond=0)


@dataclass(frozen=True)
class Reservation:
    key: str | None  # None => nothing to refund (fallback path or quota disabled)


@dataclass(frozen=True)
class QuotaUsage:
    limit: int
    used: int
    resets_at: datetime

    @property
    def remaining(self) -> int:
        return max(0, self.limit - self.used)

    def headers(self) -> dict[str, str]:
        return {
            "X-Daily-Quota-Limit": str(self.limit),
            "X-Daily-Quota-Remaining": str(self.remaining),
        }


class QuotaManager:
    def __init__(
        self, redis: aioredis.Redis | None, pool: asyncpg.Pool, settings: Settings
    ) -> None:
        self.redis = redis
        self.pool = pool
        self.settings = settings
        self._reserve = redis.register_script(RESERVE_LUA) if redis else None
        self._refund = redis.register_script(REFUND_LUA) if redis else None

    @staticmethod
    def _key(user_id: UUID, day: datetime) -> str:
        return f"foundry:quota:{user_id}:{day:%Y%m%d}"

    async def _db_count(self, user_id: UUID, since: datetime) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval(
                "SELECT count(*) FROM jobs WHERE user_id = $1 AND created_at >= $2", user_id, since
            )

    async def used_today(self, user_id: UUID) -> int:
        day = utc_day_start()
        if self.redis is not None:
            try:
                v = await self.redis.get(self._key(user_id, day))
                if v is not None:
                    return int(v)
            except (RedisError, OSError):
                pass
        return await self._db_count(user_id, day)

    async def usage(self, user_id: UUID, limit: int) -> QuotaUsage:
        day = utc_day_start()
        return QuotaUsage(limit, await self.used_today(user_id), day + timedelta(days=1))

    def _exceeded(self, limit: int, resets_at: datetime) -> ApiError:
        metrics.LIMIT_REJECTIONS.labels("daily_quota").inc()
        retry = max(1, int((resets_at - datetime.now(UTC)).total_seconds()))
        return ApiError(
            429,
            "quota_exceeded",
            "Daily job quota exceeded",
            f"Your plan allows {limit} jobs per UTC day; "
            f"the quota resets at {resets_at:%H:%M} UTC.",
            headers={
                "Retry-After": str(retry),
                "X-Daily-Quota-Limit": str(limit),
                "X-Daily-Quota-Remaining": "0",
            },
        )

    async def reserve(self, user_id: UUID, limit: int) -> tuple[Reservation, QuotaUsage]:
        """Claim one unit of today's quota or raise 429 ``quota_exceeded``."""
        day = utc_day_start()
        resets_at = day + timedelta(days=1)
        if not self.settings.quota_enabled:
            return Reservation(None), QuotaUsage(limit, 0, resets_at)

        if self.redis is not None and self._reserve is not None:
            key = self._key(user_id, day)
            try:
                code, used = await self._reserve(keys=[key], args=[limit])
                if int(code) == -1:  # cold counter: seed from the source of truth, then retry
                    seeded = await self._db_count(user_id, day)
                    await self.redis.set(key, seeded, ex=60 * 60 * 48, nx=True)
                    code, used = await self._reserve(keys=[key], args=[limit])
                if int(code) == 0:
                    raise self._exceeded(limit, resets_at)
                return Reservation(key), QuotaUsage(limit, int(used), resets_at)
            except (RedisError, OSError) as exc:
                log.error("quota counter unavailable; using PostgreSQL", extra={"error": repr(exc)})

        used = await self._db_count(user_id, day)
        if used >= limit:
            raise self._exceeded(limit, resets_at)
        return Reservation(None), QuotaUsage(limit, used + 1, resets_at)

    async def refund(self, reservation: Reservation) -> None:
        if reservation.key is None or self._refund is None:
            return
        try:
            await self._refund(keys=[reservation.key])
        except (RedisError, OSError):
            log.warning("could not refund quota; counter may over-count until reseeded")
