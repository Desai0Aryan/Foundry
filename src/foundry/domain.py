"""Core domain rules: the job state machine and per-tier limits."""

from __future__ import annotations

import hashlib
from enum import StrEnum
from typing import Any

import orjson


class JobStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    RETRY_SCHEDULED = "retry_scheduled"
    SUCCEEDED = "succeeded"
    DEAD = "dead"  # retries exhausted -> dead-letter
    CANCELLED = "cancelled"


ACTIVE_STATUSES = frozenset({JobStatus.QUEUED, JobStatus.RUNNING, JobStatus.RETRY_SCHEDULED})
TERMINAL_STATUSES = frozenset({JobStatus.SUCCEEDED, JobStatus.DEAD, JobStatus.CANCELLED})

# The ONLY legal transitions. Every UPDATE in the codebase is guarded by this table
# (and by a status predicate in SQL), so illegal transitions are impossible by design.
TRANSITIONS: dict[JobStatus, frozenset[JobStatus]] = {
    JobStatus.QUEUED: frozenset({JobStatus.RUNNING, JobStatus.CANCELLED}),
    JobStatus.RUNNING: frozenset(
        {
            JobStatus.SUCCEEDED,
            JobStatus.RETRY_SCHEDULED,  # failed or lease expired, attempts remain
            JobStatus.DEAD,  # failed, attempts exhausted
            JobStatus.CANCELLED,
        }
    ),
    JobStatus.RETRY_SCHEDULED: frozenset({JobStatus.QUEUED, JobStatus.CANCELLED}),
    JobStatus.SUCCEEDED: frozenset(),
    JobStatus.DEAD: frozenset({JobStatus.QUEUED}),  # manual replay from the DLQ
    JobStatus.CANCELLED: frozenset(),
}


def can_transition(src: JobStatus, dst: JobStatus) -> bool:
    return dst in TRANSITIONS[src]


# (max concurrent active jobs, daily submissions, sustained requests/sec)
TIER_LIMITS: dict[str, tuple[int, int, float]] = {
    "free": (5, 500, 5.0),
    "pro": (25, 10_000, 50.0),
    "enterprise": (100, 1_000_000, 500.0),
}


def canonical_json(value: Any) -> bytes:
    """Deterministic JSON: sorted keys, no whitespace. Equal data => equal bytes."""
    return orjson.dumps(value, option=orjson.OPT_SORT_KEYS)


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()
