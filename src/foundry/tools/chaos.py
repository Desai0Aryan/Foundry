"""Chaos harness: run a local fleet of real OS processes, hurt it during a load test, then
prove from the database alone that nothing was lost or duplicated.

Faults are real signals sent to real processes:
  SIGKILL  worker dies mid-job (no cleanup at all)
  SIGSTOP  worker freezes past its lease, then SIGCONT: a "zombie" that wakes up and tries
           to finish a job that was already re-assigned (fencing must reject it)
  SIGTERM  rolling restart (graceful drain, no attempt burned)
Infra outages use overridable commands (FOUNDRY_CHAOS_STOP_<NAME> / _START_<NAME>), which
default to ``docker compose stop|start <name>``.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import shlex
import signal
import subprocess
import sys
import time
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from ..audit import run_audit
from ..config import get_settings
from ..db import create_pool, run_migrations
from .loadgen import LoadConfig, LoadRun, render_report


class Fleet:
    def __init__(
        self, run_dir: Path, workers: int, api_port: int, lease: float, redrive: float
    ) -> None:
        self.run_dir, self.n_workers, self.api_port = run_dir, workers, api_port
        run_dir.mkdir(parents=True, exist_ok=True)
        self.procs: dict[str, subprocess.Popen] = {}
        self.env = {
            **os.environ,
            "FOUNDRY_LEASE_SECONDS": str(lease),
            "FOUNDRY_HEARTBEAT_SECONDS": str(max(0.5, lease / 3)),
            "FOUNDRY_BACKOFF_BASE_SECONDS": "1",
            "FOUNDRY_REDRIVE_AFTER_SECONDS": str(redrive),
            "FOUNDRY_REAPER_INTERVAL_SECONDS": "1",
            "FOUNDRY_LOG_JSON": "true",
        }

    def start(self, name: str, *args: str, extra: dict[str, str] | None = None) -> None:
        log = open(self.run_dir / f"{name}.log", "ab")  # noqa: SIM115
        self.procs[name] = subprocess.Popen(
            [sys.executable, "-m", *args], env={**self.env, **(extra or {})}, stdout=log, stderr=log
        )

    def start_worker(self, i: int) -> None:
        self.start(
            f"worker{i}", "foundry.cli", "worker", extra={"FOUNDRY_METRICS_PORT": str(9110 + i)}
        )

    def up(self) -> None:
        self.start(
            "api",
            "uvicorn",
            "foundry.api.app:app_factory",
            "--factory",
            "--port",
            str(self.api_port),
        )
        self.start("relay", "foundry.cli", "relay", extra={"FOUNDRY_METRICS_PORT": "9101"})
        self.start("reaper", "foundry.cli", "reaper", extra={"FOUNDRY_METRICS_PORT": "9102"})
        for i in range(1, self.n_workers + 1):
            self.start_worker(i)

    def signal(self, name: str, sig: int) -> int:
        p = self.procs[name]
        p.send_signal(sig)
        return p.pid

    def alive(self, name: str) -> bool:
        return self.procs[name].poll() is None

    def busiest_worker(self) -> str:
        return next((n for n in self.procs if n.startswith("worker") and self.alive(n)), "worker1")

    def down(self) -> None:
        for p in self.procs.values():
            if p.poll() is None:
                with contextlib.suppress(ProcessLookupError):
                    p.send_signal(signal.SIGCONT)  # a stopped process cannot handle SIGTERM
                    p.terminate()
        for p in self.procs.values():
            with contextlib.suppress(subprocess.TimeoutExpired):
                p.wait(timeout=10)
            if p.poll() is None:
                p.kill()


def infra(name: str, action: str) -> None:
    cmd = (
        os.environ.get(f"FOUNDRY_CHAOS_{action.upper()}_{name.upper()}")
        or f"docker compose {action} {name}"
    )
    subprocess.run(shlex.split(cmd), check=True, capture_output=True, timeout=60)


# ------------------------------------------------------------------------- scenarios
Log = Callable[[str], None]
Scenario = Callable[[Fleet, Log, float], Awaitable[None]]


async def worker_crash(fleet: Fleet, log: Log, at: float) -> None:
    """SIGKILL a worker mid-job; a replacement joins later, like an orchestrator would do."""
    await asyncio.sleep(at)
    w = fleet.busiest_worker()
    log(f"SIGKILL {w} (pid {fleet.signal(w, signal.SIGKILL)}) while it holds jobs")
    await asyncio.sleep(4)
    fleet.start_worker(int(w.removeprefix("worker")))
    log(f"{w} replaced by a fresh process")


async def worker_freeze(fleet: Fleet, log: Log, at: float) -> None:
    """Zombie: freeze past the lease so jobs are re-assigned, then wake it up."""
    await asyncio.sleep(at)
    w = fleet.busiest_worker()
    lease = float(fleet.env["FOUNDRY_LEASE_SECONDS"])
    log(f"SIGSTOP {w}: frozen for {lease * 2.5:.0f}s (lease is {lease:g}s)")
    fleet.signal(w, signal.SIGSTOP)
    await asyncio.sleep(lease * 2.5)
    log(f"SIGCONT {w}: zombie wakes up and tries to finish stale work")
    fleet.signal(w, signal.SIGCONT)


async def rolling_restart(fleet: Fleet, log: Log, at: float) -> None:
    """Graceful deploy: SIGTERM each worker in turn and start its replacement."""
    await asyncio.sleep(at)
    for i in range(1, fleet.n_workers + 1):
        name = f"worker{i}"
        log(f"SIGTERM {name} (graceful drain)")
        fleet.signal(name, signal.SIGTERM)
        for _ in range(300):
            if not fleet.alive(name):
                break
            await asyncio.sleep(0.1)
        fleet.start_worker(i)
        log(f"{name} restarted")
        await asyncio.sleep(1.5)


async def control_plane_crash(fleet: Fleet, log: Log, at: float) -> None:
    """Kill the relay and the reaper. Work must wait in PostgreSQL, then flow when they return."""
    await asyncio.sleep(at)
    log("SIGKILL relay and reaper")
    fleet.signal("relay", signal.SIGKILL)
    fleet.signal("reaper", signal.SIGKILL)
    await asyncio.sleep(6)
    fleet.start("relay", "foundry.cli", "relay", extra={"FOUNDRY_METRICS_PORT": "9101"})
    fleet.start("reaper", "foundry.cli", "reaper", extra={"FOUNDRY_METRICS_PORT": "9102"})
    log("relay and reaper restarted")


async def broker_outage(fleet: Fleet, log: Log, at: float) -> None:
    """RabbitMQ disappears for 8s. PostgreSQL is the source of truth, so nothing is lost."""
    await asyncio.sleep(at)
    log("RabbitMQ stopped")
    await asyncio.to_thread(infra, "rabbitmq", "stop")
    await asyncio.sleep(8)
    await asyncio.to_thread(infra, "rabbitmq", "start")
    log("RabbitMQ started again")


async def kitchen_sink(fleet: Fleet, log: Log, at: float) -> None:
    await asyncio.gather(
        worker_crash(fleet, log, at),
        worker_freeze(fleet, log, at + 6),
        control_plane_crash(fleet, log, at + 14),
        broker_outage(fleet, log, at + 24),
    )


SCENARIOS: dict[str, Scenario] = {
    "worker-crash": worker_crash,
    "worker-freeze": worker_freeze,
    "rolling-restart": rolling_restart,
    "control-plane-crash": control_plane_crash,
    "broker-outage": broker_outage,
    "kitchen-sink": kitchen_sink,
}


# -------------------------------------------------------------------------- the run
async def run_chaos(
    scenario: str,
    *,
    rate: float = 15,
    duration: float = 40,
    job_seconds: float = 1.0,
    workers: int = 3,
    lease: float = 4,
    fault_at: float = 8,
    api_port: int = 8000,
    run_dir: str = ".run/chaos",
    manage_fleet: bool = True,
) -> dict[str, Any]:
    settings = get_settings()
    pool = await create_pool(settings)
    await run_migrations(pool)
    fleet = Fleet(Path(run_dir), workers, api_port, lease, redrive=lease * 3)
    t0 = time.monotonic()
    timeline: list[tuple[float, str]] = []

    def log(msg: str) -> None:
        timeline.append((time.monotonic() - t0, msg))
        print(f"\r  \033[33m[{timeline[-1][0]:6.1f}s] {msg}\033[0m" + " " * 30, flush=True)

    try:
        if manage_fleet:
            fleet.up()
        api = f"http://localhost:{api_port}"
        import httpx

        async with httpx.AsyncClient(timeout=1) as hc:
            for _ in range(150):
                try:
                    if (await hc.get(f"{api}/healthz")).status_code == 200:
                        break
                except httpx.HTTPError:
                    await asyncio.sleep(0.2)
        await asyncio.sleep(2)  # let workers register
        cfg = LoadConfig(
            api=api, rate=rate, duration=duration, job_seconds=job_seconds, on_event=log
        )
        cfg.drain_timeout = 240
        run = LoadRun(cfg)
        fault = asyncio.create_task(SCENARIOS[scenario](fleet, log, fault_at))
        report = await run.run()
        await fault
        audit = await run_audit(
            pool, user_id=report["user_id"], expected_job_ids=set(report["acknowledged_job_ids"])
        )
    finally:
        if manage_fleet:
            fleet.down()
        await pool.close()
    return {"scenario": scenario, "timeline": timeline, "load": report, "audit": audit}


def render_chaos(res: dict[str, Any], color: bool = True) -> str:
    return "\n\n".join(
        [
            f"Scenario: {res['scenario']}",
            render_report(res["load"], color),
            res["audit"].render(color),
        ]
    )
