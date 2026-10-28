"""foundry CLI:  migrate | create-user | worker | relay | reaper"""

from __future__ import annotations

import argparse
import asyncio

from . import repository as repo
from .config import get_settings
from .db import create_pool, run_migrations
from .domain import TIER_LIMITS
from .logging_config import configure_logging
from .security import generate_api_key


async def _migrate() -> None:
    pool = await create_pool(get_settings())
    try:
        applied = await run_migrations(pool)
        print("applied:", ", ".join(applied) if applied else "nothing to do")
    finally:
        await pool.close()


async def _create_user(email: str, name: str, tier: str) -> None:
    settings = get_settings()
    pool = await create_pool(settings)
    try:
        await run_migrations(pool)
        concurrent, daily, rps = TIER_LIMITS[tier]
        key = generate_api_key(settings.api_key_pepper.get_secret_value())
        async with pool.acquire() as conn, conn.transaction():
            user = await repo.create_user(
                conn, email=email.lower(), name=name, tier=tier,
                max_concurrent_jobs=concurrent, daily_job_quota=daily, rate_limit_per_sec=rps,
            )  # fmt: skip
            await repo.insert_api_key(
                conn, user_id=user["id"], prefix=key.prefix, secret_hash=key.secret_hash,
                label="cli",
            )  # fmt: skip
        print(f"user_id: {user['id']}\napi_key: {key.token}")
    finally:
        await pool.close()


async def _service(name: str) -> None:
    settings = get_settings()
    configure_logging(settings.log_level, settings.log_json)
    pool = await create_pool(settings)
    try:
        await run_migrations(pool)  # idempotent and advisory-locked, so replicas can race
        if name == "worker":
            from .worker import run_worker

            await run_worker(settings, pool)
        elif name == "relay":
            from .relay import run_relay

            await run_relay(settings, pool)
        else:
            from .reaper import run_reaper

            await run_reaper(settings, pool)
    finally:
        await pool.close()


def main() -> None:
    parser = argparse.ArgumentParser(prog="foundry")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="apply database migrations")
    sub.add_parser("worker", help="run a worker (scale by running more)")
    sub.add_parser("relay", help="run the outbox relay (PostgreSQL -> RabbitMQ)")
    sub.add_parser("reaper", help="run the reaper (lease recovery, retry promotion)")
    cu = sub.add_parser("create-user", help="create a user and print their API key once")
    cu.add_argument("--email", required=True)
    cu.add_argument("--name", required=True)
    cu.add_argument("--tier", choices=sorted(TIER_LIMITS), default="free")
    args = parser.parse_args()
    if args.cmd == "migrate":
        asyncio.run(_migrate())
    elif args.cmd in ("worker", "relay", "reaper"):
        asyncio.run(_service(args.cmd))
    else:
        asyncio.run(_create_user(args.email, args.name, args.tier))


if __name__ == "__main__":
    main()
