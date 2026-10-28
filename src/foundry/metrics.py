"""Prometheus metrics: what the platform exposes, and how to read it.

Two sources, on purpose:

* **Event metrics** (counters/histograms) are emitted by the process that observes the event:
  API (requests, submissions, limits), workers (job outcomes and latencies), relay, reaper.
  Prometheus scrapes every replica and aggregates with ``sum(rate(...))``.
* **System gauges** (queue depth, backlog age, workers alive) are derived from PostgreSQL, the
  source of truth, whenever the API's ``/metrics`` is scraped. They stay correct however many
  workers exist or die, so use ``max()`` across API replicas.

Latency definitions (all in seconds):
    queue_wait  : eligible-to-run -> claimed by a worker
    execution   : handler start -> finish (per attempt)
    end_to_end  : submitted -> succeeded (what a user experiences; includes retries)
"""

from __future__ import annotations

import asyncio
import time

import asyncpg
from prometheus_client import Counter, Gauge, Histogram

JOB_BUCKETS = (0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300, 600, 1800)
HTTP_BUCKETS = (0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5)
ALL_STATUSES = ("queued", "running", "retry_scheduled", "succeeded", "dead", "cancelled")

# --- API ---------------------------------------------------------------------------------
HTTP_REQUESTS = Counter(
    "foundry_http_requests_total", "HTTP requests", ["method", "route", "status"]
)
HTTP_LATENCY = Histogram(
    "foundry_http_request_duration_seconds",
    "HTTP latency",
    ["method", "route"],
    buckets=HTTP_BUCKETS,
)
SUBMISSIONS = Counter(
    "foundry_job_submissions_total", "Job submissions by outcome", ["outcome"]
)  # created | replayed | deduplicated
RATE_LIMIT_DECISIONS = Counter(
    "foundry_rate_limit_decisions_total", "Rate limiter decisions", ["result"]
)  # allowed | limited | error_open | error_closed
AUTH_FAILURES = Counter("foundry_auth_failures_total", "Rejected credentials", ["result"])
LIMIT_REJECTIONS = Counter(
    "foundry_limit_rejections_total", "Requests rejected by a quota", ["kind"]
)  # daily_quota | concurrency

# --- workers -----------------------------------------------------------------------------
JOBS_FINISHED = Counter(
    "foundry_jobs_finished_total",
    "Job attempts that ended, by outcome",
    ["type", "outcome"],
)  # succeeded | retry | dead | cancelled | lost | released
JOB_EXECUTION = Histogram(
    "foundry_job_execution_seconds",
    "Handler execution time per attempt",
    ["type"],
    buckets=JOB_BUCKETS,
)
JOB_QUEUE_WAIT = Histogram(
    "foundry_job_queue_wait_seconds", "Time from eligible to claimed", ["type"], buckets=JOB_BUCKETS
)
JOB_END_TO_END = Histogram(
    "foundry_job_end_to_end_seconds", "Submit-to-success latency", ["type"], buckets=JOB_BUCKETS
)
WORKER_ACTIVE = Gauge("foundry_worker_active_jobs", "Jobs this worker is running now")
LEASE_LOST = Counter("foundry_worker_lease_lost_total", "Jobs abandoned after losing the lease")
HEARTBEAT_FAILURES = Counter("foundry_worker_heartbeat_failures_total", "Failed heartbeats")
DUPLICATE_DELIVERIES = Counter(
    "foundry_duplicate_deliveries_total", "Messages dropped because the job was not claimable"
)

# --- relay / reaper ----------------------------------------------------------------------
OUTBOX_PUBLISHED = Counter("foundry_outbox_published_total", "Outbox rows published to the broker")
OUTBOX_FAILURES = Counter("foundry_outbox_publish_failures_total", "Outbox publish failures")
REAPER_RECOVERED = Counter(
    "foundry_reaper_recovered_total", "Jobs recovered from expired leases", ["outcome"]
)
RETRIES_PROMOTED = Counter("foundry_retries_promoted_total", "Retries moved back onto the queue")
REDRIVEN = Counter("foundry_redriven_total", "Queued jobs re-enqueued by the safety net")

# --- system gauges (derived from PostgreSQL at scrape time) -------------------------------
JOBS_BY_STATUS = Gauge("foundry_jobs", "Jobs currently in each status", ["status"])
QUEUE_OLDEST_AGE = Gauge(
    "foundry_queue_oldest_age_seconds", "Age of the oldest job waiting in the queue"
)
OUTBOX_BACKLOG = Gauge("foundry_outbox_backlog", "Outbox rows not yet published")
OUTBOX_STUCK = Gauge("foundry_outbox_stuck", "Outbox rows that exhausted publish attempts")
OUTBOX_LAG = Gauge("foundry_outbox_lag_seconds", "Age of the oldest unpublished outbox row")
WORKERS_ALIVE = Gauge("foundry_workers_alive", "Workers with a recent heartbeat")
WORKER_SLOTS = Gauge("foundry_worker_slots", "Concurrency slots across live workers", ["state"])

_last_refresh = 0.0
_lock_state: dict[str, object] = {"loop": None, "lock": None}


def _refresh_lock() -> asyncio.Lock:
    """One lock per running event loop (tests create many loops; production has one)."""
    loop = asyncio.get_running_loop()
    if _lock_state["loop"] is not loop:
        _lock_state["loop"], _lock_state["lock"] = loop, asyncio.Lock()
    return _lock_state["lock"]  # type: ignore[return-value]


async def refresh_system_gauges(pool: asyncpg.Pool, max_age_seconds: float = 2.0) -> None:
    """Re-query PostgreSQL (at most once per `max_age_seconds`, however often we are scraped)."""
    global _last_refresh
    if time.monotonic() - _last_refresh < max_age_seconds:
        return
    async with _refresh_lock():
        if time.monotonic() - _last_refresh < max_age_seconds:
            return
        async with pool.acquire() as conn:
            by_status = {
                r["status"]: r["n"]
                for r in await conn.fetch("SELECT status, count(*) AS n FROM jobs GROUP BY status")
            }
            row = await conn.fetchrow(
                """
                SELECT
                  (SELECT COALESCE(extract(epoch FROM now() - min(queued_at)), 0)
                     FROM jobs WHERE status = 'queued')                          AS oldest_queued,
                  (SELECT count(*) FROM outbox
                    WHERE published_at IS NULL AND attempts < 10)                AS backlog,
                  (SELECT count(*) FROM outbox
                    WHERE published_at IS NULL AND attempts >= 10)               AS stuck,
                  (SELECT COALESCE(extract(epoch FROM now() - min(created_at)), 0)
                     FROM outbox WHERE published_at IS NULL AND attempts < 10)   AS lag,
                  w.alive, w.slots, w.used
                  FROM (SELECT count(*) AS alive,
                               COALESCE(sum(concurrency), 0) AS slots,
                               COALESCE(sum(active_jobs), 0) AS used
                          FROM workers
                         WHERE status = 'running'
                           AND last_heartbeat_at > now() - interval '30 seconds') w
                """
            )
        for status in ALL_STATUSES:
            JOBS_BY_STATUS.labels(status).set(by_status.get(status, 0))
        QUEUE_OLDEST_AGE.set(float(row["oldest_queued"]))
        OUTBOX_BACKLOG.set(row["backlog"])
        OUTBOX_STUCK.set(row["stuck"])
        OUTBOX_LAG.set(float(row["lag"]))
        WORKERS_ALIVE.set(row["alive"])
        WORKER_SLOTS.labels("total").set(int(row["slots"]))
        WORKER_SLOTS.labels("busy").set(int(row["used"]))
        _last_refresh = time.monotonic()


def start_exporter(port: int | None) -> None:
    """Expose /metrics from a non-HTTP process (worker, relay, reaper)."""
    if port:
        from prometheus_client import start_http_server

        start_http_server(port)
