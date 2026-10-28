from __future__ import annotations

import re
from typing import Annotated
from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, Header, Query, Response

from ... import metrics, service
from ... import repository as repo
from ...config import Settings
from ...domain import JobStatus
from ...errors import ApiError, not_found
from ...jobtypes import REGISTRY
from ...quota import QuotaManager
from ...schemas import JobCreate, JobEventOut, JobOut, JobPage, MeOut, UsageOut
from ..deps import AuthUser, get_pool, get_quotas, get_settings, limited_user

router = APIRouter(prefix="/v1", tags=["jobs"])

_IDEMPOTENCY_RE = re.compile(r"^[\x21-\x7e]{1,255}$")  # printable ASCII, no spaces


@router.get("/me", response_model=MeOut, summary="Who am I, and what are my limits?")
async def me(user: AuthUser = Depends(limited_user)) -> MeOut:
    return MeOut(
        id=user.id,
        email=user.email,
        name=user.name,
        tier=user.tier,
        max_concurrent_jobs=user.max_concurrent_jobs,
        daily_job_quota=user.daily_job_quota,
        rate_limit_per_sec=user.rate_limit_per_sec,
    )


@router.get("/job-types", summary="Discover supported job types and their payload schemas")
async def job_types() -> list[dict]:
    return [
        {
            "type": s.name,
            "description": s.description,
            "default_timeout_seconds": s.default_timeout_seconds,
            "default_max_attempts": s.default_max_attempts,
            "payload_schema": s.payload_model.model_json_schema(),
        }
        for s in REGISTRY.values()
    ]


@router.post(
    "/jobs",
    response_model=JobOut,
    status_code=201,
    summary="Submit a job",
    responses={200: {"description": "Idempotent replay or deduplicated onto an active job"}},
)
async def submit_job(
    body: JobCreate,
    response: Response,
    user: AuthUser = Depends(limited_user),
    pool: asyncpg.Pool = Depends(get_pool),
    settings: Settings = Depends(get_settings),
    quotas: QuotaManager = Depends(get_quotas),
    idempotency_key: Annotated[str | None, Header(alias="Idempotency-Key")] = None,
) -> JobOut:
    if idempotency_key is not None and not _IDEMPOTENCY_RE.match(idempotency_key):
        raise ApiError(
            400, "invalid_idempotency_key", "Invalid Idempotency-Key",
            "Must be 1-255 printable ASCII characters without spaces.",
        )  # fmt: skip
    prepared = service.prepare_job(body, settings.max_payload_bytes)

    # Reserve today's quota only for requests that could create a job. An over-quota user may
    # still safely retry a request that already succeeded (idempotent replay / dedup hit).
    reservation, usage = None, None
    try:
        reservation, usage = await quotas.reserve(user.id, user.daily_job_quota)
    except ApiError as exc:
        if exc.code != "quota_exceeded" or not await service.has_existing_job(
            pool, user.id, prepared, idempotency_key
        ):
            raise
    try:
        result = await service.submit_job(
            pool, user.id, prepared, idempotency_key, max_concurrent=user.max_concurrent_jobs
        )
    except BaseException:
        if reservation is not None:
            await quotas.refund(reservation)
        raise
    if result.outcome != "created" and reservation is not None:
        await quotas.refund(reservation)  # replays and dedup hits are free
        usage = None
    metrics.SUBMISSIONS.labels(result.outcome).inc()
    if usage is not None:
        for k, v in usage.headers().items():
            response.headers[k] = v
    if result.outcome == "replayed":
        response.status_code = 200
        response.headers["Idempotent-Replayed"] = "true"
    elif result.outcome == "deduplicated":
        response.status_code = 200
        response.headers["X-Deduplicated"] = "true"
    response.headers["Location"] = f"/v1/jobs/{result.job['id']}"
    return JobOut.model_validate(dict(result.job))


@router.get("/jobs", response_model=JobPage, summary="List your jobs (newest first)")
async def list_jobs(
    status: JobStatus | None = None,
    limit: int = Query(default=50, ge=1, le=200),
    cursor: str | None = None,
    user: AuthUser = Depends(limited_user),
    pool: asyncpg.Pool = Depends(get_pool),
) -> JobPage:
    decoded = None
    if cursor:
        try:
            decoded = repo.decode_cursor(cursor)
        except ValueError as exc:
            raise ApiError(400, "invalid_cursor", "Invalid cursor") from exc
    async with pool.acquire() as conn:
        rows, next_cursor = await repo.list_jobs(
            conn, user.id, status=status.value if status else None, limit=limit, cursor=decoded
        )
    return JobPage(items=[JobOut.model_validate(dict(r)) for r in rows], next_cursor=next_cursor)


@router.get("/jobs/{job_id}", response_model=JobOut, summary="Get a job")
async def get_job(
    job_id: UUID, user: AuthUser = Depends(limited_user), pool: asyncpg.Pool = Depends(get_pool)
) -> JobOut:
    async with pool.acquire() as conn:
        row = await repo.get_job(conn, user.id, job_id)
    if row is None:
        raise not_found("Job")
    return JobOut.model_validate(dict(row))


@router.get(
    "/jobs/{job_id}/events",
    response_model=list[JobEventOut],
    summary="Audit trail of a job's state transitions",
)
async def job_events(
    job_id: UUID, user: AuthUser = Depends(limited_user), pool: asyncpg.Pool = Depends(get_pool)
) -> list[JobEventOut]:
    async with pool.acquire() as conn:
        if await repo.get_job(conn, user.id, job_id) is None:
            raise not_found("Job")
        rows = await repo.list_events(conn, job_id)
    return [JobEventOut.model_validate(dict(r)) for r in rows]


@router.post(
    "/jobs/{job_id}/cancel",
    response_model=JobOut,
    summary="Cancel a job",
    responses={202: {"description": "Running job: cancellation requested (cooperative)"}},
)
async def cancel_job(
    job_id: UUID,
    response: Response,
    user: AuthUser = Depends(limited_user),
    pool: asyncpg.Pool = Depends(get_pool),
) -> JobOut:
    row, immediate = await service.cancel_job(pool, user.id, job_id)
    if not immediate:
        response.status_code = 202
    return JobOut.model_validate(dict(row))


@router.post(
    "/jobs/{job_id}/replay",
    response_model=JobOut,
    status_code=202,
    summary="Replay a dead (dead-lettered) job with a fresh attempt budget",
)
async def replay_job(
    job_id: UUID, user: AuthUser = Depends(limited_user), pool: asyncpg.Pool = Depends(get_pool)
) -> JobOut:
    row = await service.replay_job(pool, user.id, job_id)
    return JobOut.model_validate(dict(row))


@router.get("/usage", response_model=UsageOut, summary="Your limits and current consumption")
async def usage(
    user: AuthUser = Depends(limited_user),
    pool: asyncpg.Pool = Depends(get_pool),
    quotas: QuotaManager = Depends(get_quotas),
    settings: Settings = Depends(get_settings),
) -> UsageOut:
    q = await quotas.usage(user.id, user.daily_job_quota)
    async with pool.acquire() as conn:
        active = await repo.count_active(conn, user.id)
    burst = max(1, int(user.rate_limit_per_sec * settings.rate_limit_burst_seconds))
    return UsageOut(
        tier=user.tier,
        rate_limit={"requests_per_second": user.rate_limit_per_sec, "burst": burst},
        daily_quota={
            "limit": q.limit, "used": q.used, "remaining": q.remaining,
            "resets_at": q.resets_at.isoformat(),
        },
        concurrency={
            "limit": user.max_concurrent_jobs, "active": active,
            "remaining": max(0, user.max_concurrent_jobs - active),
        },
    )  # fmt: skip
