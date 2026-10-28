"""Job submission and cancellation: where idempotency, dedup and atomicity are enforced.

Guarantees (all enforced by PostgreSQL constraints, so they hold under any concurrency):

* Idempotency-Key: the same key + same request => the original job, never a second one.
  The same key + a *different* request => 422 (client bug), never a silent mix-up.
* Deduplication (opt-in): an identical request while an equal job is still active
  collapses onto that job. Once it finishes, the slot frees and a new job may be created.
* Atomicity: job row + audit event + outbox message commit in ONE transaction, so a job
  can never exist without its "run me" message (the classic dual-write bug).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal
from uuid import UUID

import asyncpg
from pydantic import ValidationError

from . import repository as repo
from .domain import JobStatus, canonical_json, sha256_hex
from .errors import ApiError, not_found
from .jobtypes import REGISTRY
from .schemas import JobCreate

Outcome = Literal["created", "replayed", "deduplicated"]
_MAX_ATTEMPTS_BEFORE_GIVING_UP = 4


@dataclass(frozen=True)
class SubmitResult:
    job: asyncpg.Record
    outcome: Outcome


@dataclass(frozen=True)
class PreparedJob:
    type: str
    payload: dict
    priority: int
    max_attempts: int
    timeout_seconds: int
    payload_hash: str
    request_hash: str
    dedup_key: str | None


def prepare_job(body: JobCreate, max_payload_bytes: int) -> PreparedJob:
    """Validate against the job type's schema and normalize into a canonical form."""
    spec = REGISTRY[body.type]
    try:
        model = spec.payload_model.model_validate(body.payload)
    except ValidationError as exc:
        errors = [
            {"loc": ["payload", *e["loc"]], "msg": e["msg"], "type": e["type"]}
            for e in exc.errors(include_url=False, include_context=False, include_input=False)
        ]
        raise ApiError(
            422, "invalid_payload", "Invalid payload", f"Payload invalid for {body.type}.",
            errors=errors,
        ) from exc  # fmt: skip
    payload = model.model_dump(mode="json")
    if len(canonical_json(payload)) > max_payload_bytes:
        raise ApiError(
            413, "payload_too_large", "Payload too large",
            f"Payload must be at most {max_payload_bytes} bytes.",
        )  # fmt: skip

    max_attempts = body.max_attempts or spec.default_max_attempts
    timeout = body.timeout_seconds or spec.default_timeout_seconds
    payload_hash = sha256_hex(canonical_json({"type": body.type, "payload": payload}))
    request_hash = sha256_hex(
        canonical_json(
            {
                "type": body.type,
                "payload": payload,
                "priority": body.priority,
                "max_attempts": max_attempts,
                "timeout_seconds": timeout,
                "deduplicate": body.deduplicate,
            }
        )
    )
    return PreparedJob(
        type=body.type,
        payload=payload,
        priority=body.priority,
        max_attempts=max_attempts,
        timeout_seconds=timeout,
        payload_hash=payload_hash,
        request_hash=request_hash,
        dedup_key=payload_hash if body.deduplicate else None,
    )


async def submit_job(
    pool: asyncpg.Pool, user_id: UUID, job: PreparedJob, idempotency_key: str | None
) -> SubmitResult:
    for _ in range(_MAX_ATTEMPTS_BEFORE_GIVING_UP):
        async with pool.acquire() as conn, conn.transaction():
            try:
                # Nested transaction == SAVEPOINT: a unique violation rolls back only the
                # INSERT, leaving the outer transaction usable so we can read the winner.
                async with conn.transaction():
                    row = await repo.insert_job(
                        conn,
                        user_id=user_id,
                        type_=job.type,
                        payload=job.payload,
                        payload_hash=job.payload_hash,
                        request_hash=job.request_hash,
                        idempotency_key=idempotency_key,
                        dedup_key=job.dedup_key,
                        priority=job.priority,
                        max_attempts=job.max_attempts,
                        timeout_seconds=job.timeout_seconds,
                    )
            except asyncpg.UniqueViolationError as exc:
                if exc.constraint_name == repo.UQ_IDEMPOTENCY and idempotency_key:
                    existing = await repo.get_by_idempotency_key(conn, user_id, idempotency_key)
                    if existing is None:  # winner rolled back between our insert and read
                        continue
                    if existing["request_hash"] != job.request_hash:
                        raise ApiError(
                            422,
                            "idempotency_key_reuse",
                            "Idempotency key reused with a different request",
                            "This Idempotency-Key was already used with different parameters.",
                        ) from exc
                    return SubmitResult(existing, "replayed")
                if exc.constraint_name == repo.UQ_DEDUP and job.dedup_key:
                    existing = await repo.get_active_by_dedup_key(conn, user_id, job.dedup_key)
                    if existing is None:  # it finished a moment ago; the slot is free now
                        continue
                    return SubmitResult(existing, "deduplicated")
                raise

            await repo.add_event(
                conn, row["id"], None, JobStatus.QUEUED.value, {"reason": "submitted"}
            )
            await repo.add_outbox(
                conn,
                row["id"],
                routing_key=job.type,
                payload={
                    "job_id": str(row["id"]),
                    "type": job.type,
                    "priority": job.priority,
                },
            )
            return SubmitResult(row, "created")
    raise ApiError(
        503, "contention", "Please retry", "Could not submit job due to contention.",
        headers={"Retry-After": "1"},
    )  # fmt: skip


async def cancel_job(
    pool: asyncpg.Pool, user_id: UUID, job_id: UUID
) -> tuple[asyncpg.Record, bool]:
    """Returns (job, immediate). A running job can only be *asked* to stop (cooperative)."""
    async with pool.acquire() as conn, conn.transaction():
        row = await repo.lock_job(conn, user_id, job_id)
        if row is None:
            raise not_found("Job")
        status = JobStatus(row["status"])
        if status in (JobStatus.QUEUED, JobStatus.RETRY_SCHEDULED):
            updated = await repo.set_job_cancelled(conn, job_id)
            await repo.add_event(conn, job_id, status.value, "cancelled", {"reason": "user"})
            return updated, True
        if status is JobStatus.RUNNING:
            updated = await repo.request_cancel(conn, job_id)
            await repo.add_event(conn, job_id, status.value, "running", {"cancel_requested": True})
            return updated, False
        raise ApiError(
            409, "not_cancellable", "Job cannot be cancelled",
            f"Job is already {status.value}.",
        )  # fmt: skip
