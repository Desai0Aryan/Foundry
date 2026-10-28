from __future__ import annotations

import asyncio

import asyncpg
from fastapi import APIRouter, Depends, Response

from ..deps import get_pool

router = APIRouter(tags=["health"])


@router.get("/healthz", summary="Liveness probe")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}


@router.get("/readyz", summary="Readiness probe (checks PostgreSQL)")
async def readyz(response: Response, pool: asyncpg.Pool = Depends(get_pool)) -> dict[str, str]:
    try:
        async with asyncio.timeout(2):
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
    except Exception:
        response.status_code = 503
        return {"status": "unavailable", "database": "down"}
    return {"status": "ready", "database": "up"}
