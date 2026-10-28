from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field

from .domain import JobStatus
from .jobtypes import JobType


class JobCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: JobType
    payload: dict[str, Any]
    priority: int = Field(default=5, ge=0, le=9, description="0 = lowest, 9 = highest")
    max_attempts: int | None = Field(default=None, ge=1, le=20)
    timeout_seconds: int | None = Field(default=None, ge=1, le=3600)
    deduplicate: bool = Field(
        default=False,
        description="Collapse onto an identical *active* job of yours instead of creating one.",
    )


class JobOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    type: str
    status: JobStatus
    priority: int
    payload: dict[str, Any]
    attempts: int
    max_attempts: int
    timeout_seconds: int
    idempotency_key: str | None
    result: Any | None
    error: Any | None
    cancel_requested: bool
    next_run_at: datetime | None
    created_at: datetime
    updated_at: datetime
    started_at: datetime | None
    finished_at: datetime | None


class JobPage(BaseModel):
    items: list[JobOut]
    next_cursor: str | None


class JobEventOut(BaseModel):
    id: int
    at: datetime
    from_status: str | None
    to_status: str
    detail: dict[str, Any]


class MeOut(BaseModel):
    id: UUID
    email: str
    name: str
    tier: str
    max_concurrent_jobs: int
    daily_job_quota: int
    rate_limit_per_sec: float


class UserCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    email: EmailStr
    name: str = Field(min_length=1, max_length=200)
    tier: str = Field(default="free", pattern="^(free|pro|enterprise)$")


class UserWithKey(BaseModel):
    user: MeOut
    api_key: str = Field(description="Shown exactly once. Store it securely.")
