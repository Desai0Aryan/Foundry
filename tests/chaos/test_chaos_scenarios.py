"""End-to-end chaos: real processes, real signals, then the auditor decides.

Run with:  pytest -m chaos   (needs PostgreSQL, Redis and RabbitMQ; see conftest)
"""

import os
import subprocess
import sys

import asyncpg
import pytest

from tests.conftest import BROKER_URL, TEST_DSN

pytestmark = pytest.mark.chaos

DB = "foundry_chaos_test"


@pytest.fixture(scope="module")
def chaos_env():
    import asyncio

    admin_dsn = TEST_DSN.rsplit("/", 1)[0] + "/postgres"

    async def recreate():
        c = await asyncpg.connect(admin_dsn)
        try:
            await c.execute(f"DROP DATABASE IF EXISTS {DB}")
            await c.execute(f"CREATE DATABASE {DB}")
        finally:
            await c.close()

    asyncio.run(recreate())
    return {
        **os.environ,
        "PYTHONPATH": "src",
        "FOUNDRY_DATABASE_URL": TEST_DSN.rsplit("/", 1)[0] + f"/{DB}",
        "FOUNDRY_BROKER_URL": BROKER_URL,
        "FOUNDRY_REDIS_URL": os.environ.get("FOUNDRY_TEST_REDIS_URL", "redis://localhost:6379/0"),
    }


@pytest.mark.parametrize("scenario", ["worker-crash", "worker-freeze", "rolling-restart"])
def test_invariants_hold_under_fault(scenario, chaos_env):
    out = subprocess.run(
        [sys.executable, "-m", "foundry.cli", "chaos", scenario, "--duration", "12", "--rate", "6",
         "--job-seconds", "0.8", "--lease", "3"],
        env=chaos_env, capture_output=True, text=True, timeout=240,
    )  # fmt: skip
    assert out.returncode == 0, out.stdout[-3000:] + out.stderr[-1500:]
    assert "ALL INVARIANTS HELD" in out.stdout
