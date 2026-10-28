from __future__ import annotations

from uuid import UUID

import asyncpg
from fastapi import APIRouter, Depends, Header

from ... import repository as repo
from ...config import Settings
from ...domain import TIER_LIMITS
from ...errors import ApiError, not_found
from ...schemas import MeOut, UserCreate, UserWithKey
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
