# ADR 0002 — Transactional outbox for publishing work

**Status:** accepted

## Context
"Insert job, then publish message" is a dual write. A crash between the two leaves a job that
will never run (or a message for a job that does not exist).

## Decision
Submission inserts the job, its audit event and an `outbox` row in **one transaction**. A relay
process publishes unpublished rows using `SELECT … FOR UPDATE SKIP LOCKED` and marks them
published after the broker confirms (publisher confirms). Delivery is therefore at-least-once.

## Consequences
+ A committed job always eventually produces a message.
+ Multiple relays can run concurrently without double-publishing the same row for long.
- Duplicate messages are possible by design; consumers must be idempotent (claim step + effect ledger).
