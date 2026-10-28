# ADR 0004 — Leases, heartbeats and fencing tokens for crash recovery

**Status:** accepted

## Context
Workers crash (OOM-kill, node loss, `kill -9`) or stall (GC pause, network partition) in the
middle of a job. We need to (a) notice, (b) re-run the job elsewhere, and (c) make sure a
worker that was *wrongly* presumed dead cannot corrupt the outcome when it wakes up.

## Decision
* **Lease.** Claiming a job is a conditional `UPDATE ... WHERE status = 'queued'`, which stamps
  `lease_owner` and `lease_expires_at`. Exactly one claimant can win, however many messages
  or workers race for it.
* **Heartbeat.** The worker extends the leases of all its running jobs every
  `heartbeat_seconds` (validated to be < half of `lease_seconds`). The same statement returns
  `cancel_requested`, so cancellation needs no extra round trip.
* **Reaper.** Expired leases are rescheduled (backoff) or dead-lettered if attempts are
  exhausted. It uses `FOR UPDATE SKIP LOCKED`, so multiple reapers cooperate.
* **Fencing.** `attempts` is a fencing token. Every worker write is guarded by
  `status = 'running' AND lease_owner = me AND attempts = my_attempt`. A zombie's write
  matches zero rows and is discarded, so it can never overwrite the replacement attempt.
* **Ack after commit.** The broker message is acked only after the database transition
  commits. A crash in between causes a redelivery that the claim step drops.

## Consequences
+ Recovery time after a hard crash ≈ `lease_seconds` + reaper interval + retry backoff.
+ No leader election, no distributed lock service: PostgreSQL row locks are the arbiter.
+ Slow-but-alive jobs are safe as long as heartbeats continue (they run on a separate task).
- Handlers can run more than once for the same job (at-least-once execution). Side effects
  MUST go through the effect ledger / idempotency keys. See `JobContext.effect`.
- Tuning trade-off: short leases recover fast but risk false positives on a stalled event loop.
