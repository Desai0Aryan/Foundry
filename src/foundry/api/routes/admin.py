from __future__ import annotations

from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, Header

from ... import repository as repo
from ...config import Settings
from ...domain import TIER_LIMITS
from ...errors import ApiError, not_found
from ...schemas import MeOut, UserCreate, UserLimitsUpdate, UserWithKey
from ...security import constant_time_equals, generate_api_key
from ..deps import get_pool, get_settings


async def require_admin(
    x_admin_token: str | None = Header(default=None), settings: Settings = Depends(get_settings)
) -> None:
    expected = settings.admin_token.get_secret_value()
    if not x_admin_token or not constant_time_equals(x_admin_token, expected):
        raise ApiError(403, "forbidden", "Forbidden", "Admin token required.")


router = APIRouter(prefix="/v1/admin", tags=["admin"], dependencies=[Depends(require_admin)])


def _me(row: asyncpg.Record) -> MeOut:
    return MeOut(
        id=row["id"],
        email=row["email"],
        name=row["name"],
        tier=row["tier"],
        max_concurrent_jobs=row["max_concurrent_jobs"],
        daily_job_quota=row["daily_job_quota"],
        rate_limit_per_sec=float(row["rate_limit_per_sec"]),
    )


@router.post("/users", response_model=UserWithKey, status_code=201, summary="Create a user + key")
async def create_user(
    body: UserCreate,
    pool: asyncpg.Pool = Depends(get_pool),
    settings: Settings = Depends(get_settings),
) -> UserWithKey:
    concurrent, daily, rps = TIER_LIMITS[body.tier]
    key = generate_api_key(settings.api_key_pepper.get_secret_value())
    try:
        async with pool.acquire() as conn, conn.transaction():
            user = await repo.create_user(
                conn,
                email=str(body.email).lower(),
                name=body.name,
                tier=body.tier,
                max_concurrent_jobs=concurrent,
                daily_job_quota=daily,
                rate_limit_per_sec=rps,
            )
            await repo.insert_api_key(
                conn, user_id=user["id"], prefix=key.prefix, secret_hash=key.secret_hash,
                label="initial",
            )  # fmt: skip
    except asyncpg.UniqueViolationError as exc:
        raise ApiError(409, "email_taken", "Email already registered") from exc
    return UserWithKey(user=_me(user), api_key=key.token)


@router.post(
    "/users/{user_id}/api-keys",
    status_code=201,
    summary="Issue an additional API key (key rotation)",
)
async def issue_key(
    user_id: UUID,
    pool: asyncpg.Pool = Depends(get_pool),
    settings: Settings = Depends(get_settings),
) -> dict[str, str]:
    key = generate_api_key(settings.api_key_pepper.get_secret_value())
    async with pool.acquire() as conn, conn.transaction():
        if await repo.get_user(conn, user_id) is None:
            raise not_found("User")
        await repo.insert_api_key(
            conn, user_id=user_id, prefix=key.prefix, secret_hash=key.secret_hash, label="rotated"
        )
    return {"api_key": key.token}


@router.get("/workers", summary="Worker fleet: liveness, load and throughput")
async def workers(pool: asyncpg.Pool = Depends(get_pool)) -> list[dict]:
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            """
            SELECT id, host, pid, status, concurrency, active_jobs, processed_total,
                   started_at, last_heartbeat_at,
                   (status <> 'stopped' AND last_heartbeat_at > now() - interval '30 seconds')
                       AS alive
              FROM workers ORDER BY started_at DESC LIMIT 200
            """
        )
    return [dict(r) for r in rows]


@router.get("/stats", summary="Queue depth, dead-letter count and backlog age")
async def stats(pool: asyncpg.Pool = Depends(get_pool)) -> dict:
    async with pool.acquire() as conn:
        by_status = {
            r["status"]: r["n"]
            for r in await conn.fetch("SELECT status, count(*) AS n FROM jobs GROUP BY status")
        }
        outbox_backlog = await conn.fetchval(
            "SELECT count(*) FROM outbox WHERE published_at IS NULL AND attempts < 10"
        )
        outbox_stuck = await conn.fetchval(
            "SELECT count(*) FROM outbox WHERE published_at IS NULL AND attempts >= 10"
        )
        oldest = await conn.fetchval(
            "SELECT extract(epoch FROM now() - min(created_at)) FROM jobs WHERE status = 'queued'"
        )
    return {
        "jobs_by_status": by_status,
        "queue_depth": by_status.get("queued", 0),
        "dead_letter_count": by_status.get("dead", 0),
        "outbox_backlog": outbox_backlog,
        "outbox_stuck": outbox_stuck,
        "oldest_queued_age_seconds": float(oldest) if oldest is not None else 0.0,
    }


@router.get("/effects", summary="Side-effect ledger summary (duplicate-suppression proof)")
async def effects(pool: asyncpg.Pool = Depends(get_pool)) -> dict:
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT count(*) AS effects, count(DISTINCT job_id) AS jobs, "
            "COALESCE(sum(suppressed), 0) AS suppressed FROM effect_ledger"
        )
    return {
        "effects_applied": row["effects"],
        "jobs_with_effects": row["jobs"],
        "duplicate_applications_suppressed": int(row["suppressed"]),
    }


@router.patch("/users/{user_id}", response_model=MeOut, summary="Change a user's tier or limits")
async def update_limits(
    user_id: UUID, body: UserLimitsUpdate, pool: asyncpg.Pool = Depends(get_pool)
) -> MeOut:
    changes = body.model_dump(exclude_none=True)
    if "tier" in changes and not {
        "max_concurrent_jobs",
        "daily_job_quota",
        "rate_limit_per_sec",
    } & set(changes):
        concurrent, daily, rps = TIER_LIMITS[changes["tier"]]  # a tier change implies its defaults
        changes.update(
            max_concurrent_jobs=concurrent, daily_job_quota=daily, rate_limit_per_sec=rps
        )
    if not changes:
        raise ApiError(400, "empty_update", "Nothing to update")
    cols = list(changes)
    assignments = ", ".join(f"{c} = ${i + 2}" for i, c in enumerate(cols))
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            f"UPDATE users SET {assignments} WHERE id = $1 RETURNING *",  # noqa: S608 - cols are a fixed schema allowlist
            user_id, *changes.values(),
        )  # fmt: skip
    if row is None:
        raise not_found("User")
    return _me(row)
