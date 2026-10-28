# ADR 0007 — Metrics: event counters from processes, system gauges from PostgreSQL

**Status:** accepted

## Decision
* **Event metrics** (counters, histograms) are emitted by the process that sees the event: API
  (requests, submissions, limits), workers (outcomes, execution/queue-wait/end-to-end latency),
  relay, reaper. Prometheus scrapes every replica and aggregates (`sum(rate(...))`).
  Workers/relay/reaper expose `:9100/:9101/:9102`; the API serves `/metrics`.
* **System gauges** (queue depth, backlog age, outbox lag, workers alive) are computed from
  PostgreSQL when `/metrics` is scraped (cached 2 s). A dead worker cannot report that it is dead,
  and a queue that no process is draining still needs a depth, so the database is the only
  trustworthy source.
* **Three latencies**, because one number hides the cause: *queue wait* (eligible -> claimed),
  *execution* (per attempt), *end-to-end* (submitted -> succeeded, including retries). The
  user-visible SLO is end-to-end p95.
* **Route templates, not URLs**, as HTTP labels (`/v1/jobs/{job_id}`), so cardinality is bounded.
* **Failure rate** is defined per *attempt* (`retry|dead` over all finished attempts); `dead` has
  its own counter and alert.
* Alerts live in `ops/prometheus/alerts.yml`; every dashboard query is executed against a real
  Prometheus by `scripts/validate_dashboard.py`.

## Consequences
+ Dashboards stay correct as workers come and go.
- API replicas each export the same system gauges: always aggregate with `max()`, not `sum()`.
- Run API replicas, not `uvicorn --workers N` (per-process registries would need multiprocess mode).
