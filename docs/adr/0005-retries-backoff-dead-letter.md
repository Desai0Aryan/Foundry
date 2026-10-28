# ADR 0005 — Retries, timeouts and dead-lettering

**Status:** accepted

## Decision
* **Where retries live.** Retry state is a row state (`retry_scheduled` + `next_run_at`), not
  a broker TTL queue. The reaper promotes due retries back onto the queue. This keeps the
  schedule queryable ("when will job X run next?"), survives broker loss, and needs no
  per-delay queues.
* **Backoff.** Equal-jitter exponential: ceiling `min(cap, base * 2^(attempt-1))`, delay drawn
  from `[ceiling/2, ceiling]`. Growth is guaranteed; synchronized retry storms are not.
* **Timeouts.** Each job has `timeout_seconds`, enforced with `asyncio.timeout` around the
  handler. A timeout is a retryable failure (`error.type = "timeout"`).
* **Error taxonomy.** `PermanentError` skips straight to dead (retrying cannot help).
  `TransientError` and any unexpected exception retry until `max_attempts`.
* **Dead-letter queue.** Exhausted jobs get `status = 'dead'` in PostgreSQL (authoritative,
  with the last error and full event trail) and a message on the `dead` queue via the outbox
  (for alerting). `POST /v1/jobs/{id}/replay` returns a dead job to the queue with a fresh
  attempt budget. Broker-level dead-lettering separately captures unparseable "poison"
  messages in `poison`.
* **Cancellation** is cooperative: queued jobs cancel instantly; running jobs are told via the
  heartbeat and interrupted promptly.
* **Graceful shutdown** (SIGTERM): stop consuming, drain until `shutdown_grace_seconds`, then
  return unfinished jobs to the queue *without* charging an attempt.

## Consequences
+ Every outcome is explainable from `GET /v1/jobs/{id}/events`.
- Retry latency has a floor of the reaper interval (default 2 s).
