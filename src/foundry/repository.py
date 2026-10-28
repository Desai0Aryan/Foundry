"""All SQL lives here. Plain asyncpg + hand-written queries: no ORM magic to hide behind."""

from __future__ import annotations

import base64
import binascii
from datetime import datetime
from typing import Any
from uuid import UUID

import asyncpg

from .domain import ACTIVE_STATUSES

UQ_IDEMPOTENCY = "uq_jobs_idempotency"
UQ_DEDUP = "uq_jobs_dedup_active"

_ACTIVE = [s.value for s in ACTIVE_STATUSES]


# --------------------------------------------------------------------------- users / keys
async def create_user(
    conn: asyncpg.Connection,
    *,
    email: str,
    name: str,
    tier: str,
    max_concurrent_jobs: int,
    daily_job_quota: int,
    rate_limit_per_sec: float,
) -> asyncpg.Record:
    return await conn.fetchrow(
        """
        INSERT INTO users (email, name, tier, max_concurrent_jobs, daily_job_quota,
                           rate_limit_per_sec)
        VALUES ($1, $2, $3, $4, $5, $6) RETURNING *
        """,
        email, name, tier, max_concurrent_jobs, daily_job_quota, rate_limit_per_sec,
    )  # fmt: skip


async def get_user(conn: asyncpg.Connection, user_id: UUID) -> asyncpg.Record | None:
    return await conn.fetchrow("SELECT * FROM users WHERE id = $1", user_id)


async def insert_api_key(
    conn: asyncpg.Connection, *, user_id: UUID, prefix: str, secret_hash: str, label: str
) -> asyncpg.Record:
    return await conn.fetchrow(
        "INSERT INTO api_keys (user_id, prefix, secret_hash, label) "
        "VALUES ($1, $2, $3, $4) RETURNING *",
        user_id, prefix, secret_hash, label,
    )  # fmt: skip


async def find_key_with_user(conn: asyncpg.Connection, prefix: str) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """
        SELECT k.id AS key_id, k.secret_hash, k.revoked_at,
               u.id, u.email, u.name, u.tier, u.max_concurrent_jobs,
               u.daily_job_quota, u.rate_limit_per_sec
          FROM api_keys k JOIN users u ON u.id = k.user_id
         WHERE k.prefix = $1
        """,
        prefix,
    )


async def touch_key(conn: asyncpg.Connection, key_id: UUID) -> None:
    """Throttled to one write per minute per key, so auth never becomes a write hotspot."""
    await conn.execute(
        "UPDATE api_keys SET last_used_at = now() WHERE id = $1 "
        "AND (last_used_at IS NULL OR last_used_at < now() - interval '1 minute')",
        key_id,
    )


# --------------------------------------------------------------------------------- jobs
async def insert_job(
    conn: asyncpg.Connection,
    *,
    user_id: UUID,
    type_: str,
    payload: dict[str, Any],
    payload_hash: str,
    request_hash: str,
    idempotency_key: str | None,
    dedup_key: str | None,
    priority: int,
    max_attempts: int,
    timeout_seconds: int,
) -> asyncpg.Record:
    return await conn.fetchrow(
        """
        INSERT INTO jobs (user_id, type, payload, payload_hash, request_hash, idempotency_key,
                          dedup_key, priority, max_attempts, timeout_seconds)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING *
        """,
        user_id, type_, payload, payload_hash, request_hash, idempotency_key,
        dedup_key, priority, max_attempts, timeout_seconds,
    )  # fmt: skip


async def get_job(conn: asyncpg.Connection, user_id: UUID, job_id: UUID) -> asyncpg.Record | None:
    return await conn.fetchrow("SELECT * FROM jobs WHERE id = $1 AND user_id = $2", job_id, user_id)


async def get_by_idempotency_key(
    conn: asyncpg.Connection, user_id: UUID, key: str
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        "SELECT * FROM jobs WHERE user_id = $1 AND idempotency_key = $2", user_id, key
    )


async def get_active_by_dedup_key(
    conn: asyncpg.Connection, user_id: UUID, dedup_key: str
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        "SELECT * FROM jobs WHERE user_id = $1 AND dedup_key = $2 AND status = ANY($3::text[])",
        user_id, dedup_key, _ACTIVE,
    )  # fmt: skip


def encode_cursor(created_at: datetime, job_id: UUID) -> str:
    raw = f"{created_at.isoformat()}|{job_id}".encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_cursor(cursor: str) -> tuple[datetime, UUID]:
    try:
        padded = cursor + "=" * (-len(cursor) % 4)
        ts, _, ident = base64.urlsafe_b64decode(padded).decode().partition("|")
        return datetime.fromisoformat(ts), UUID(ident)
    except (ValueError, binascii.Error, UnicodeDecodeError) as exc:
        raise ValueError("invalid cursor") from exc


async def list_jobs(
    conn: asyncpg.Connection,
    user_id: UUID,
    *,
    status: str | None,
    limit: int,
    cursor: tuple[datetime, UUID] | None,
) -> tuple[list[asyncpg.Record], str | None]:
    """Keyset pagination: stable under concurrent inserts, O(log n) at any depth."""
    after_ts, after_id = cursor if cursor else (None, None)
    rows = await conn.fetch(
        """
        SELECT * FROM jobs
         WHERE user_id = $1
           AND ($2::text IS NULL OR status = $2)
           AND ($3::timestamptz IS NULL OR (created_at, id) < ($3::timestamptz, $4::uuid))
         ORDER BY created_at DESC, id DESC
         LIMIT $5
        """,
        user_id, status, after_ts, after_id, limit + 1,
    )  # fmt: skip
    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        next_cursor = encode_cursor(last["created_at"], last["id"])
    return rows, next_cursor


async def add_event(
    conn: asyncpg.Connection,
    job_id: UUID,
    from_status: str | None,
    to_status: str,
    detail: dict[str, Any] | None = None,
) -> None:
    await conn.execute(
        "INSERT INTO job_events (job_id, from_status, to_status, detail) VALUES ($1,$2,$3,$4)",
        job_id, from_status, to_status, detail or {},
    )  # fmt: skip


async def list_events(conn: asyncpg.Connection, job_id: UUID) -> list[asyncpg.Record]:
    return await conn.fetch(
        "SELECT id, at, from_status, to_status, detail FROM job_events "
        "WHERE job_id = $1 ORDER BY id",
        job_id,
    )


async def add_outbox(
    conn: asyncpg.Connection, job_id: UUID, routing_key: str, payload: dict[str, Any]
) -> None:
    await conn.execute(
        "INSERT INTO outbox (job_id, routing_key, payload) VALUES ($1, $2, $3)",
        job_id, routing_key, payload,
    )  # fmt: skip


async def lock_job(conn: asyncpg.Connection, user_id: UUID, job_id: UUID) -> asyncpg.Record | None:
    return await conn.fetchrow(
        "SELECT * FROM jobs WHERE id = $1 AND user_id = $2 FOR UPDATE", job_id, user_id
    )


async def set_job_cancelled(conn: asyncpg.Connection, job_id: UUID) -> asyncpg.Record:
    return await conn.fetchrow(
        "UPDATE jobs SET status = 'cancelled', finished_at = now(), next_run_at = NULL "
        "WHERE id = $1 RETURNING *",
        job_id,
    )


async def request_cancel(conn: asyncpg.Connection, job_id: UUID) -> asyncpg.Record:
    return await conn.fetchrow(
        "UPDATE jobs SET cancel_requested = true WHERE id = $1 RETURNING *", job_id
    )
