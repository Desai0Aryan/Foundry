from __future__ import annotations

import asyncio
import os

import asyncpg
import httpx
import pytest
import pytest_asyncio

from foundry.api.app import create_app
from foundry.config import Settings
from foundry.db import create_pool, run_migrations

TEST_DSN = os.environ.get(
    "FOUNDRY_TEST_DATABASE_URL", "postgresql://postgres@localhost:5433/foundry_test"
)
BROKER_URL = os.environ.get("FOUNDRY_TEST_BROKER_URL", "amqp://guest:guest@localhost:5672/")
ADMIN = {"X-Admin-Token": "test-admin"}


def make_settings(**overrides) -> Settings:
    base = {
        "database_url": TEST_DSN,
        "auto_migrate": False,
        "admin_token": "test-admin",
        "api_key_pepper": "test-pepper",
        "log_json": False,
        "log_level": "WARNING",
        "environment": "test",
        "db_pool_max": 30,
        "broker_url": BROKER_URL,
        # fast timings so recovery behaviour can be tested in seconds
        "lease_seconds": 3.0,
        "heartbeat_seconds": 0.5,
        "backoff_base_seconds": 0.2,
        "backoff_cap_seconds": 1.0,
        "reaper_interval_seconds": 0.2,
        "outbox_poll_seconds": 0.2,
        "worker_concurrency": 4,
        "shutdown_grace_seconds": 5.0,
    }
    return Settings(**{**base, **overrides})


async def _reset_schema() -> None:
    conn = await asyncpg.connect(TEST_DSN)
    try:
        await conn.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
    finally:
        await conn.close()
    pool = await create_pool(make_settings())
    try:
        await run_migrations(pool)
    finally:
        await pool.close()


@pytest.fixture(scope="session")
def _schema() -> None:
    try:
        asyncio.run(_reset_schema())
    except (OSError, asyncpg.PostgresError) as exc:
        pytest.skip(f"PostgreSQL not reachable at {TEST_DSN}: {exc}")


@pytest_asyncio.fixture
async def app(_schema):
    application = create_app(make_settings())
    async with application.router.lifespan_context(application):
        async with application.state.pool.acquire() as conn:
            await conn.execute(
                "TRUNCATE users, api_keys, jobs, job_events, outbox, effect_ledger, workers CASCADE"
            )
        yield application


@pytest_asyncio.fixture
async def client(app):
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


async def make_user(client: httpx.AsyncClient, email: str = "ada@example.com", tier: str = "pro"):
    r = await client.post(
        "/v1/admin/users", json={"email": email, "name": "Test User", "tier": tier}, headers=ADMIN
    )
    assert r.status_code == 201, r.text
    body = r.json()
    return body["user"], {"Authorization": f"Bearer {body['api_key']}"}


@pytest_asyncio.fixture
async def auth(client):
    _, headers = await make_user(client)
    return headers


def sleep_job(**overrides):
    body = {"type": "demo.sleep", "payload": {"seconds": 0.1}}
    body.update(overrides)
    return body


async def submit(client: httpx.AsyncClient, auth: dict, **overrides) -> str:
    r = await client.post("/v1/jobs", json=sleep_job(**overrides), headers=auth)
    assert r.status_code in (200, 201), r.text
    return r.json()["id"]


async def wait_until(predicate, timeout: float = 15.0, interval: float = 0.05):
    """Poll an async predicate until it returns a truthy value, or fail loudly."""
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        value = await predicate()
        if value:
            return value
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError("condition not met within timeout")
        await asyncio.sleep(interval)
