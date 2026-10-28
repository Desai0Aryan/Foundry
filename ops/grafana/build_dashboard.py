#!/usr/bin/env python3
"""Generate ops/grafana/dashboards/foundry.json.

The dashboard is code so that every panel's PromQL is reviewable, diffable and testable
(scripts/validate_dashboard.py runs each query against a live Prometheus).
"""

from __future__ import annotations

import json
from pathlib import Path

DS = {"type": "prometheus", "uid": "foundry-prom"}
R = "$__rate_interval"
_id = 0


def _panel(
    kind: str,
    title: str,
    targets: list[tuple[str, str]],
    x: int,
    y: int,
    w: int,
    h: int,
    unit: str = "short",
    desc: str = "",
    **extra,
) -> dict:
    global _id
    _id += 1
    return {
        "id": _id,
        "type": kind,
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "targets": [
            {"refId": chr(65 + i), "expr": expr, "legendFormat": legend, "datasource": DS}
            for i, (expr, legend) in enumerate(targets)
        ],
        "fieldConfig": {"defaults": {"unit": unit, **extra.pop("defaults", {})}, "overrides": []},
        **extra,
    }


def stat(title, expr, x, y, unit="short", w=4, h=4, desc="", thresholds=None, color="value"):
    steps = thresholds or [{"color": "green", "value": None}]
    return _panel(
        "stat",
        title,
        [(expr, "")],
        x,
        y,
        w,
        h,
        unit,
        desc,
        defaults={"thresholds": {"mode": "absolute", "steps": steps}, "noValue": "0"},
        options={
            "colorMode": color,
            "graphMode": "area",
            "reduceOptions": {"calcs": ["lastNotNull"]},
        },
    )


def ts(title, targets, x, y, w=12, h=8, unit="short", desc="", stack=False, **kw):
    custom = {"lineWidth": 2, "fillOpacity": 25 if stack else 8, "showPoints": "never"}
    if stack:
        custom["stacking"] = {"mode": "normal"}
    return _panel(
        "timeseries",
        title,
        targets,
        x,
        y,
        w,
        h,
        unit,
        desc,
        defaults={"custom": custom, **kw.pop("defaults", {})},
        options={
            "legend": {"displayMode": "list", "placement": "bottom"},
            "tooltip": {"mode": "multi"},
        },
    )


def row(title, y):
    global _id
    _id += 1
    return {
        "id": _id,
        "type": "row",
        "title": title,
        "collapsed": False,
        "gridPos": {"x": 0, "y": y, "w": 24, "h": 1},
        "panels": [],
    }


def q(p: float, metric: str, by: str = "") -> str:
    grp = f"le, {by}" if by else "le"
    return f"histogram_quantile({p}, sum by ({grp}) (rate({metric}_bucket[{R}])))"


FAIL = f'sum(rate(foundry_jobs_finished_total{{outcome=~"retry|dead"}}[{R}])) / sum(rate(foundry_jobs_finished_total{{outcome=~"succeeded|retry|dead"}}[{R}]))'
RED = [
    {"color": "green", "value": None},
    {"color": "orange", "value": 1},
    {"color": "red", "value": 10},
]

panels = [
    row("At a glance", 0),
    stat(
        "Queue depth",
        'max(foundry_jobs{status="queued"})',
        0,
        1,
        desc="Jobs waiting for a worker (from PostgreSQL).",
    ),
    stat("Running", 'max(foundry_jobs{status="running"})', 4, 1),
    stat(
        "Dead-letter jobs",
        'max(foundry_jobs{status="dead"})',
        8,
        1,
        thresholds=RED,
        color="background",
        desc="Jobs that exhausted retries. Inspect and replay via the API.",
    ),
    stat(
        "Workers alive",
        "max(foundry_workers_alive)",
        12,
        1,
        thresholds=[{"color": "red", "value": None}, {"color": "green", "value": 1}],
        color="background",
    ),
    stat(
        "Throughput",
        f'sum(rate(foundry_jobs_finished_total{{outcome="succeeded"}}[{R}]))',
        16,
        1,
        "ops",
        desc="Successful jobs per second.",
    ),
    stat(
        "p95 end-to-end",
        q(0.95, "foundry_job_end_to_end_seconds"),
        20,
        1,
        "s",
        desc="Submit-to-success latency, 95th percentile (includes queueing and retries).",
    ),
    row("Throughput, failures and latency", 5),
    ts(
        "Throughput by job type",
        [
            (
                f'sum by (type) (rate(foundry_jobs_finished_total{{outcome="succeeded"}}[{R}]))',
                "{{type}}",
            )
        ],
        0,
        6,
        unit="ops",
        stack=True,
    ),
    ts(
        "Attempt outcomes",
        [(f"sum by (outcome) (rate(foundry_jobs_finished_total[{R}]))", "{{outcome}}")],
        12,
        6,
        unit="ops",
        stack=True,
        desc="retry = failed but will be retried; dead = retries exhausted; lost = fenced out after losing a lease.",
    ),
    ts(
        "Attempt failure rate",
        [(FAIL, "failed attempts / all attempts")],
        0,
        14,
        unit="percentunit",
        desc="Share of attempts that failed (and were retried or dead-lettered).",
    ),
    ts(
        "Latency percentiles",
        [
            (q(0.5, "foundry_job_end_to_end_seconds"), "end-to-end p50"),
            (q(0.95, "foundry_job_end_to_end_seconds"), "end-to-end p95"),
            (q(0.99, "foundry_job_end_to_end_seconds"), "end-to-end p99"),
            (q(0.95, "foundry_job_execution_seconds"), "execution p95"),
            (q(0.95, "foundry_job_queue_wait_seconds"), "queue wait p95"),
        ],
        12,
        14,
        unit="s",
        desc="Where time goes: waiting in the queue vs. executing vs. the whole user-visible latency.",
    ),
    row("Queue and fleet", 22),
    ts("Jobs by status", [("max by (status) (foundry_jobs)", "{{status}}")], 0, 23, stack=True),
    ts(
        "Oldest queued job",
        [("max(foundry_queue_oldest_age_seconds)", "age")],
        8,
        23,
        w=8,
        unit="s",
        desc="Backlog age is the best autoscaling signal: it rises as soon as capacity is short.",
    ),
    ts(
        "Worker slots",
        [
            ('max(foundry_worker_slots{state="total"})', "total"),
            ('max(foundry_worker_slots{state="busy"})', "busy"),
        ],
        16,
        23,
        w=8,
        desc="Concurrency slots across live workers. Busy ~ total means add workers.",
    ),
    ts(
        "Outbox (PostgreSQL -> RabbitMQ)",
        [
            ("max(foundry_outbox_backlog)", "unpublished rows"),
            ("max(foundry_outbox_stuck)", "stuck rows"),
        ],
        0,
        31,
        w=8,
    ),
    ts(
        "Outbox lag",
        [("max(foundry_outbox_lag_seconds)", "oldest unpublished row")],
        8,
        31,
        w=8,
        unit="s",
    ),
    ts(
        "Submissions",
        [(f"sum by (outcome) (rate(foundry_job_submissions_total[{R}]))", "{{outcome}}")],
        16,
        31,
        w=8,
        unit="ops",
        desc="created = new job; replayed = idempotent retry; deduplicated = merged with an active identical job.",
    ),
    row("Reliability events", 39),
    ts(
        "Jobs recovered from crashed workers",
        [
            (
                "sum by (outcome) (increase(foundry_reaper_recovered_total[$__interval]))",
                "{{outcome}}",
            )
        ],
        0,
        40,
        w=8,
        desc="Expired leases the reaper rescued. A spike right after a node loss is the system working.",
    ),
    ts(
        "Duplicate deliveries dropped",
        [
            (f"sum(rate(foundry_duplicate_deliveries_total[{R}]))", "dropped messages/s"),
            (f"sum(rate(foundry_worker_lease_lost_total[{R}]))", "leases lost/s"),
        ],
        8,
        40,
        w=8,
        unit="ops",
        desc="Harmless by design: the claim step discards messages for jobs that are not claimable.",
    ),
    ts(
        "Retries promoted / redriven",
        [
            (f"sum(rate(foundry_retries_promoted_total[{R}]))", "retries promoted/s"),
            (f"sum(rate(foundry_redriven_total[{R}]))", "redriven/s"),
        ],
        16,
        40,
        w=8,
        unit="ops",
    ),
    row("API, rate limits and quotas", 48),
    ts(
        "Requests by status class",
        [(f"sum by (status) (rate(foundry_http_requests_total[{R}]))", "{{status}}")],
        0,
        49,
        unit="reqps",
        stack=True,
    ),
    ts(
        "API latency p95 by route",
        [(q(0.95, "foundry_http_request_duration_seconds", "route"), "{{route}}")],
        12,
        49,
        unit="s",
    ),
    ts(
        "Rate limiter decisions",
        [(f"sum by (result) (rate(foundry_rate_limit_decisions_total[{R}]))", "{{result}}")],
        0,
        57,
        w=8,
        unit="ops",
        desc="limited = HTTP 429. error_open/error_closed = Redis trouble.",
    ),
    ts(
        "Quota and concurrency rejections",
        [(f"sum by (kind) (rate(foundry_limit_rejections_total[{R}]))", "{{kind}}")],
        8,
        57,
        w=8,
        unit="ops",
    ),
    ts(
        "Rejected credentials",
        [(f"sum by (result) (rate(foundry_auth_failures_total[{R}]))", "{{result}}")],
        16,
        57,
        w=8,
        unit="ops",
        desc="throttled = a client guessing keys was cut off (429).",
    ),
]

dashboard = {
    "uid": "foundry-overview",
    "title": "Foundry: job platform",
    "tags": ["foundry"],
    "timezone": "browser",
    "schemaVersion": 39,
    "version": 1,
    "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"},
    "editable": True,
    "graphTooltip": 1,
    "panels": panels,
    "templating": {"list": []},
    "annotations": {"list": []},
}

if __name__ == "__main__":
    out = Path(__file__).parent / "dashboards" / "foundry.json"
    out.write_text(json.dumps(dashboard, indent=2) + "\n")
    print(f"wrote {out} ({sum(p['type'] != 'row' for p in panels)} panels)")
