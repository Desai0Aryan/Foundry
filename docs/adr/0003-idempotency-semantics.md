# ADR 0003 — Idempotency and deduplication semantics

**Status:** accepted

## Decision
* **Idempotency-Key** (client-supplied, scoped per user): unique index on `(user_id, key)`.
  The request is fingerprinted (`request_hash` over the *normalized* type, payload and options).
  Same key + same fingerprint → replay the original job (`200`, `Idempotent-Replayed: true`).
  Same key + different fingerprint → `422 idempotency_key_reuse`.
* **Deduplication** (opt-in `deduplicate: true`): partial unique index on `(user_id, dedup_key)`
  limited to *active* statuses. Identical in-flight work collapses; finished work does not block
  a deliberate re-run.
* Both are enforced by database constraints, not by check-then-insert code, so they are race-free.
  On a unique violation we read the winner inside a savepoint-protected transaction.

## Consequences
+ Safe client retries after timeouts and network failures.
+ Normalization means `{"seconds":1}` and `{"seconds":1.0,"fail_probability":0}` are the same request.
- A replay returns the job's *current* state, not a stored snapshot of the first response.
