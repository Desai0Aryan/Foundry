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

from . import metrics
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


async def has_existing_job(
    pool: asyncpg.Pool, user_id: UUID, job: PreparedJob, idempotency_key: str | None
) -> bool:
    """Would this submission resolve to an existing job (replay / dedup) instead of a new one?

    Used so that an over-quota user can still safely retry a request that already succeeded.
    """
    async with pool.acquire() as conn:
        if idempotency_key and await repo.get_by_idempotency_key(conn, user_id, idempotency_key):
            return True
        if job.dedup_key:
            return await repo.get_active_by_dedup_key(conn, user_id, job.dedup_key) is not None
    return False


async def submit_job(
    pool: asyncpg.Pool,
    user_id: UUID,
    job: PreparedJob,
    idempotency_key: str | None,
    max_concurrent: int | None = None,
) -> SubmitResult:
    for _ in range(_MAX_ATTEMPTS_BEFORE_GIVING_UP):
        async with pool.acquire() as conn, conn.transaction():
            if max_concurrent is not None:
                # Serialize this user's submissions so the concurrency limit is exact, not
                # "approximately right under a burst". Other users are unaffected.
                await conn.execute(
                    "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", str(user_id)
                )
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

            if max_concurrent is not None:
                active = await repo.count_active(conn, user_id)  # includes the row just inserted
                if active > max_concurrent:
                    metrics.LIMIT_REJECTIONS.labels("concurrency").inc()
                    raise ApiError(  # raising rolls the INSERT back
                        429, "concurrency_limit", "Too many active jobs",
                        f"Your plan allows {max_concurrent} jobs queued or running at once.",
                        headers={"Retry-After": "5"},
                    )  # fmt: skip
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


async def replay_job(pool: asyncpg.Pool, user_id: UUID, job_id: UUID) -> asyncpg.Record:
    """Dead-letter replay: give a dead job a fresh attempt budget and put it back on the queue."""
    async with pool.acquire() as conn, conn.transaction():
        row = await repo.lock_job(conn, user_id, job_id)
        if row is None:
            raise not_found("Job")
        if row["status"] != JobStatus.DEAD.value:
            raise ApiError(
                409, "not_replayable", "Job cannot be replayed",
                f"Only dead jobs can be replayed; this one is {row['status']}.",
            )  # fmt: skip
        try:
            async with conn.transaction():
                updated = await conn.fetchrow(
                    "UPDATE jobs SET status = 'queued', attempts = 0, result = NULL, "
                    "queued_at = now(), error = NULL, started_at = NULL, finished_at = NULL, "
                    "next_run_at = NULL, "
                    "cancel_requested = false WHERE id = $1 RETURNING *",
                    job_id,
                )  # fmt: skip
        except asyncpg.UniqueViolationError as exc:
            raise ApiError(
                409, "duplicate_active_job", "An identical job is already active",
                "Replay would violate deduplication: an equal job is still queued or running.",
            ) from exc  # fmt: skip
        await repo.add_event(conn, job_id, "dead", "queued", {"reason": "replayed"})
        await repo.add_outbox(
            conn,
            job_id,
            row["type"],
            {"job_id": str(job_id), "type": row["type"], "priority": row["priority"]},
        )
        return updated
