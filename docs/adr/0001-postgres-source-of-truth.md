# ADR 0001 — PostgreSQL is the source of truth; the broker is a hint

**Status:** accepted

## Context
Job state must survive broker restarts, worker crashes and lost messages. Brokers give
delivery guarantees, but not durable, queryable, transactional *state*.

## Decision
All job state (status, attempts, leases, results) lives in PostgreSQL. RabbitMQ only tells
workers "there may be work". A worker must *claim* a job with a conditional `UPDATE` before
doing anything; a message for a job that is cancelled, finished, or already claimed is simply
acked and dropped.

## Consequences
+ Losing the broker loses nothing: unpublished outbox rows and expired leases are re-driven.
+ Every state transition is auditable and testable with plain SQL.
+ Users can query job state without touching the broker.
- PostgreSQL is on the hot path; it must be sized and indexed for the claim/update rate.
