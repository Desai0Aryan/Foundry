from __future__ import annotations

from dataclasses import dataclass
from uuid import UUID

import asyncpg
from fastapi import Header, Request

from .. import repository as repo
from ..config import Settings
from ..errors import unauthorized
from ..security import hash_secret, parse_api_key, verify_secret

_DUMMY_HASH = hash_secret("x", "y")  # burn equal CPU when the prefix is unknown


@dataclass(frozen=True)
class AuthUser:
    id: UUID
    email: str
    name: str
    tier: str
    max_concurrent_jobs: int
    daily_job_quota: int
    rate_limit_per_sec: float
    key_id: UUID


def get_pool(request: Request) -> asyncpg.Pool:
    return request.app.state.pool


def get_settings(request: Request) -> Settings:
    return request.app.state.settings


def _extract_token(authorization: str | None, x_api_key: str | None) -> str | None:
    if authorization:
        scheme, _, value = authorization.partition(" ")
        if scheme.lower() == "bearer" and value:
            return value.strip()
    return x_api_key


async def current_user(
    request: Request,
    authorization: str | None = Header(default=None),
    x_api_key: str | None = Header(default=None),
) -> AuthUser:
    token = _extract_token(authorization, x_api_key)
    parsed = parse_api_key(token) if token else None
    if parsed is None:
        raise unauthorized()
    prefix, secret = parsed
    pepper = request.app.state.settings.api_key_pepper.get_secret_value()

    pool: asyncpg.Pool = request.app.state.pool
    async with pool.acquire() as conn:
        row = await repo.find_key_with_user(conn, prefix)
        valid = verify_secret(secret, pepper, row["secret_hash"] if row else _DUMMY_HASH)
        if row is None or not valid or row["revoked_at"] is not None:
            raise unauthorized()
        await repo.touch_key(conn, row["key_id"])

    user = AuthUser(
        id=row["id"],
        email=row["email"],
        name=row["name"],
        tier=row["tier"],
        max_concurrent_jobs=row["max_concurrent_jobs"],
        daily_job_quota=row["daily_job_quota"],
        rate_limit_per_sec=float(row["rate_limit_per_sec"]),
        key_id=row["key_id"],
    )
    request.state.user = user
    return user
