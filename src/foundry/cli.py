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
    from . import metrics

    default_ports = {"worker": 9100, "relay": 9101, "reaper": 9102}
    port = settings.metrics_port if settings.metrics_port is not None else default_ports[name]
    metrics.start_exporter(port)  # 0 disables
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


async def _audit(user: str | None, allow_active: bool, as_json: bool) -> int:
    import json
    from uuid import UUID

    from .audit import run_audit

    pool = await create_pool(get_settings())
    try:
        report = await run_audit(
            pool, user_id=UUID(user) if user else None, require_drained=not allow_active
        )
    finally:
        await pool.close()
    print(json.dumps(report.as_dict(), indent=2, default=str) if as_json else report.render())
    return 0 if report.passed else 1


def _loadtest(args: argparse.Namespace) -> int:
    import json

    from .tools.loadgen import LoadConfig, render_report, run_load

    cfg = LoadConfig(
        api=args.api, admin_token=args.admin_token, rate=args.rate, duration=args.duration,
        job_seconds=args.job_seconds, fail_probability=args.fail_prob,
    )  # fmt: skip
    report = asyncio.run(run_load(cfg))
    print(render_report(report))
    if args.output:
        with open(args.output, "w") as fh:
            json.dump(report, fh, indent=2)
        print(f"\nfull report written to {args.output}")
    if report["submit"]["gave_up"] or not report["jobs"]["drained"]:
        return 1
    return 0


def _chaos(args: argparse.Namespace) -> int:
    import json

    from .tools.chaos import SCENARIOS, render_chaos, run_chaos

    if args.scenario not in SCENARIOS:
        raise SystemExit(f"unknown scenario; choose from: {', '.join(SCENARIOS)}")
    res = asyncio.run(
        run_chaos(
            args.scenario, rate=args.rate, duration=args.duration, job_seconds=args.job_seconds,
            workers=args.workers, lease=args.lease, manage_fleet=not args.external_fleet,
        )
    )  # fmt: skip
    print(render_chaos(res))
    if args.output:
        data = {**res, "audit": res["audit"].as_dict()}
        data["load"] = {k: v for k, v in res["load"].items() if k != "acknowledged_job_ids"}
        with open(args.output, "w") as fh:
            json.dump(data, fh, indent=2, default=str)
    return 0 if res["audit"].passed and not res["load"]["submit"]["gave_up"] else 1


def main() -> None:
    parser = argparse.ArgumentParser(prog="foundry")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("migrate", help="apply database migrations")
    sub.add_parser("worker", help="run a worker (scale by running more)")
    sub.add_parser("relay", help="run the outbox relay (PostgreSQL -> RabbitMQ)")
    sub.add_parser("reaper", help="run the reaper (lease recovery, retry promotion)")
    lt = sub.add_parser("loadtest", help="open-loop load test against a running API")
    lt.add_argument("--api", default="http://localhost:8000")
    lt.add_argument("--admin-token", default="dev-admin-token-change-me")
    lt.add_argument("--rate", type=float, default=20, help="jobs per second")
    lt.add_argument("--duration", type=float, default=30, help="seconds of sending")
    lt.add_argument("--job-seconds", type=float, default=0.5, help="mean work per job")
    lt.add_argument("--fail-prob", type=float, default=0.0, help="injected failure probability")
    lt.add_argument("--output", help="write the full JSON report here")
    ch = sub.add_parser("chaos", help="load test while killing/freezing processes, then audit")
    ch.add_argument("scenario", help="worker-crash | worker-freeze | rolling-restart | ...")
    ch.add_argument("--rate", type=float, default=15)
    ch.add_argument("--duration", type=float, default=40)
    ch.add_argument("--job-seconds", type=float, default=1.0)
    ch.add_argument("--workers", type=int, default=3)
    ch.add_argument("--lease", type=float, default=4, help="lease seconds (short = fast recovery)")
    ch.add_argument(
        "--external-fleet", action="store_true", help="don't spawn processes (compose/k8s)"
    )
    ch.add_argument("--output")
    au = sub.add_parser("audit", help="verify the platform's invariants from the database")
    au.add_argument("--user", help="restrict to one user id")
    au.add_argument("--allow-active", action="store_true", help="jobs may still be in flight")
    au.add_argument("--json", action="store_true")
    cu = sub.add_parser("create-user", help="create a user and print their API key once")
    cu.add_argument("--email", required=True)
    cu.add_argument("--name", required=True)
    cu.add_argument("--tier", choices=sorted(TIER_LIMITS), default="free")
    args = parser.parse_args()
    if args.cmd == "migrate":
        asyncio.run(_migrate())
    elif args.cmd == "chaos":
        raise SystemExit(_chaos(args))
    elif args.cmd == "loadtest":
        raise SystemExit(_loadtest(args))
    elif args.cmd == "audit":
        raise SystemExit(asyncio.run(_audit(args.user, args.allow_active, args.json)))
    elif args.cmd in ("worker", "relay", "reaper"):
        asyncio.run(_service(args.cmd))
    else:
        asyncio.run(_create_user(args.email, args.name, args.tier))


if __name__ == "__main__":
    main()
