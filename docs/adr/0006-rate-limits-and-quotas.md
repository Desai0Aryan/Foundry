# ADR 0006 — Rate limiting, quotas and concurrency limits

**Status:** accepted

## Context
Three different protections are easy to conflate, but they bound different things:

| Protection | Bounds | Enforced by | Exactness |
|---|---|---|---|
| Rate limit | requests / second (sustained, with a burst) | Redis token bucket | exact, atomic |
| Daily quota | jobs *created* per UTC day | Redis counter, seeded from PostgreSQL | exact |
| Concurrency limit | jobs queued/running at once | PostgreSQL, in the submit transaction | exact |

## Decisions
* **Token bucket in a Lua script.** Refill, check and take are one atomic Redis step, so any
  number of API replicas share a bucket without races. The script reads time from Redis
  (`TIME`), so replica clock skew is irrelevant. Capacity = `rate x burst_seconds` (default 2 s).
  Standard `RateLimit-*` and `Retry-After` headers tell clients exactly when to retry.
* **Fail open.** If Redis is unreachable the limiter allows requests and counts the event
  (`foundry_rate_limit_decisions_total{result="error_open"}`, alertable). A protection layer
  must not become the outage. `FOUNDRY_RATE_LIMIT_FAIL_OPEN=false` flips it.
* **Quota is reserved, then refunded.** The slot is taken *before* inserting and returned if
  the request turns out to be an idempotent replay, a dedup hit, or fails. Invalid requests
  (422) are rejected before reserving, so they never cost quota.
* **Retries of successful requests are never punished.** An over-quota client retrying a
  request that already created a job (same `Idempotency-Key`, or an identical active dedup job)
  gets the original job back, not a 429.
* **Redis is a cache, PostgreSQL is the truth.** A missing counter (restart, eviction, flush)
  is re-seeded from `count(jobs today)`, so losing Redis never grants a fresh quota. If Redis
  is down the quota check falls back to counting rows.
* **Concurrency limit under a per-user advisory lock.** Counting then inserting is a race, so
  submissions for one user are serialized with `pg_advisory_xact_lock`, the insert is made,
  active jobs are counted (including the new row), and the transaction rolls back if the limit is
  exceeded. Other users are unaffected. Cost: one user's submissions are serial (about a millisecond each).
* **Credential-guessing throttle** counts only *invalid* credentials per client IP. Valid keys
  from the same IP (NAT) are never blocked, so an attacker cannot lock real users out.

## Consequences
+ Limits are correct under concurrency, across replicas, and across Redis failures.
+ Plan changes (`PATCH /v1/admin/users/{id}`) take effect on the next request: limits are read
  per request, not cached in tokens.
- The daily quota resets at UTC midnight for everyone (simple and predictable; not per-user rolling).
- A per-IP guard keyed on `request.client.host` needs `--proxy-headers` behind a load balancer
  (the Dockerfile sets it) and a trusted proxy in front.
