# ADR 0008: Prove correctness with an auditor and chaos, don't just claim it

**Status:** accepted (Part 4)

## Context
"Exactly-once" is not achievable end to end. What is achievable is at-least-once delivery plus
an idempotent receiver, and *evidence* that the combination held.

## Decision
- External effects go through a receiver keyed by `job_id:effect_key` that counts deliveries.
- `foundry audit` checks invariants from SQL alone (no stuck jobs, legal chained transitions,
  single outcome, attempt budget, one effect per succeeded job, no lost jobs, outbox drained,
  DLQ consistent). It has negative tests: each check is shown to fail on a corrupted database.
- `foundry chaos` sends real signals (KILL/STOP/TERM) and breaks infra during an open-loop
  load test, then runs the audit. Exit code is the verdict, so CI can gate on it.
- The load generator is open-loop and uses Idempotency-Keys so retries are safe.

## Consequences
The harness found real bugs the unit tests missed (relay poison cap counting broker outages;
a missing graceful-release transition). Chaos tests are slow, so they run under `-m chaos`.
