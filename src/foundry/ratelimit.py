"""Distributed rate limiting: a token bucket evaluated atomically inside Redis.

Why a token bucket: it allows short bursts (a client that was idle can send a small batch)
while enforcing the sustained rate, and it needs only two numbers of state per user.

Why a Lua script: refill + check + take is one atomic step, so any number of API replicas
can share a bucket with no races. The script reads the clock from Redis itself (TIME), so
replicas with skewed clocks cannot cheat or starve each other.

Failure policy: if Redis is unavailable the limiter *fails open* by default (and counts it in
``foundry_rate_limit_decisions_total{result="error_open"}``). A rate limiter is a protection
mechanism, not a source of truth; it should not become the reason the API is down. Set
``FOUNDRY_RATE_LIMIT_FAIL_OPEN=false`` to fail closed instead.
"""

from __future__ import annotations

import logging
import math
from dataclasses import dataclass

import redis.asyncio as aioredis
from redis.exceptions import RedisError

from . import metrics
from .config import Settings

log = logging.getLogger("foundry.ratelimit")

TOKEN_BUCKET_LUA = """
local key      = KEYS[1]
local rate     = tonumber(ARGV[1])
local capacity = tonumber(ARGV[2])
local cost     = tonumber(ARGV[3])

local t   = redis.call('TIME')
local now = tonumber(t[1]) + tonumber(t[2]) / 1000000

local data   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])
if tokens == nil then
  tokens = capacity
  ts = now
end

tokens = math.min(capacity, tokens + math.max(0, now - ts) * rate)

local allowed = 0
local retry_after = 0
if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after = (cost - tokens) / rate
end

redis.call('HSET', key, 'tokens', tostring(tokens), 'ts', tostring(now))
redis.call('PEXPIRE', key, math.ceil(capacity / rate * 1000) + 1000)
return {allowed, tostring(tokens), tostring(retry_after)}
"""

AUTH_FAIL_LUA = """
local n = redis.call('INCR', KEYS[1])
if n == 1 then redis.call('EXPIRE', KEYS[1], tonumber(ARGV[1])) end
return n
"""


@dataclass(frozen=True)
class Decision:
    allowed: bool
    limit: int  # bucket capacity (max burst)
    remaining: int
    reset_seconds: int  # until the bucket is full again
    retry_after: int  # seconds to wait (only meaningful when not allowed)

    def headers(self) -> dict[str, str]:
        h = {
            "RateLimit-Limit": str(self.limit),
            "RateLimit-Remaining": str(self.remaining),
            "RateLimit-Reset": str(self.reset_seconds),
        }
        if not self.allowed:
            h["Retry-After"] = str(max(1, self.retry_after))
        return h


class RateLimiter:
    def __init__(self, redis: aioredis.Redis | None, settings: Settings) -> None:
        self.redis = redis
        self.settings = settings
        self._bucket = redis.register_script(TOKEN_BUCKET_LUA) if redis else None
        self._auth_fail = redis.register_script(AUTH_FAIL_LUA) if redis else None

    @property
    def active(self) -> bool:
        return bool(self.settings.rate_limit_enabled and self.redis is not None)

    def capacity_for(self, rate: float) -> int:
        return max(1, math.floor(rate * self.settings.rate_limit_burst_seconds))

    async def check(self, user_id: str, rate: float, cost: int = 1) -> Decision | None:
        """Returns None when limiting is switched off."""
        if not self.active:
            return None
        capacity = self.capacity_for(rate)
        try:
            allowed, tokens, retry = await self._bucket(  # type: ignore[misc]
                keys=[f"foundry:rl:{user_id}"], args=[rate, capacity, cost]
            )
        except (RedisError, OSError) as exc:
            fail_open = self.settings.rate_limit_fail_open
            metrics.RATE_LIMIT_DECISIONS.labels("error_open" if fail_open else "error_closed").inc()
            log.error(
                "rate limiter unavailable", extra={"error": repr(exc), "fail_open": fail_open}
            )
            return Decision(fail_open, capacity, capacity, 0, 1)
        tokens_f = float(tokens)
        decision = Decision(
            allowed=bool(int(allowed)),
            limit=capacity,
            remaining=max(0, int(tokens_f)),
            reset_seconds=math.ceil(max(0.0, capacity - tokens_f) / rate),
            retry_after=math.ceil(float(retry)),
        )
        metrics.RATE_LIMIT_DECISIONS.labels("allowed" if decision.allowed else "limited").inc()
        return decision

    async def record_auth_failure(self, client_ip: str) -> bool:
        """Count a bad-credential attempt. True if this client is now over the limit.

        Only *invalid* credentials are ever throttled: a valid key from the same NAT'd IP
        keeps working, so an attacker cannot lock legitimate users out.
        """
        if not self.active or not client_ip:
            return False
        try:
            n = await self._auth_fail(keys=[f"foundry:authfail:{client_ip}"], args=[60])  # type: ignore[misc]
        except (RedisError, OSError):
            return False
        return int(n) > self.settings.auth_fail_limit_per_minute
