"""Open-loop HTTP load generator for Foundry.

Why open-loop: a closed-loop tool ("N users, each waits for a response before sending the
next") quietly slows down when the system slows down, so it under-reports latency during
exactly the incidents you care about ("coordinated omission"). This generator sends job
number *i* at ``t0 + i / rate`` whether or not earlier requests finished, and measures each
latency from that *intended* send time.

Clients are realistic: every logical job has an Idempotency-Key and is retried with
exponential backoff on network errors, 5xx and 429, which is also what makes it safe to run
during chaos: a retried submission can never create a second job.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
import random
import time
import uuid
from dataclasses import dataclass, field
from typing import Any

import httpx


def percentile(values: list[float], p: float) -> float:
    """Linear-interpolated percentile (p in 0..100). Empty input -> 0."""
    if not values:
        return 0.0
    xs = sorted(values)
    k = (len(xs) - 1) * p / 100
    lo, hi = math.floor(k), math.ceil(k)
    return xs[lo] if lo == hi else xs[lo] + (xs[hi] - xs[lo]) * (k - lo)


def summarize(values: list[float]) -> dict[str, float]:
    return {
        "count": len(values),
        "min": min(values, default=0.0),
        "p50": percentile(values, 50),
        "p95": percentile(values, 95),
        "p99": percentile(values, 99),
        "max": max(values, default=0.0),
        "mean": sum(values) / len(values) if values else 0.0,
    }


@dataclass
class LoadConfig:
    api: str = "http://localhost:8000"
    admin_token: str = "dev-admin-token-change-me"  # noqa: S105
    rate: float = 20.0  # jobs per second, constant arrival rate
    duration: float = 30.0  # seconds of sending
    job_seconds: float = 0.5  # mean duration of each demo.sleep job
    fail_probability: float = 0.0
    max_attempts: int = 5
    max_in_flight: int = 1000
    client_retry_deadline: float = 90.0  # a client gives up on one submission after this long
    request_timeout: float = 10.0
    drain_timeout: float = 180.0
    wait_for_drain: bool = True
    user_id: str | None = None  # reuse an existing user (needs api_key too)
    api_key: str | None = None
    progress: bool = True
    transport: Any = None  # httpx transport, for tests against an in-process app
    on_event: Any = None  # callback(str) for the chaos harness timeline


@dataclass
class Submission:
    index: int
    key: str
    intended: float
    job_id: str | None = None
    latency: float | None = None  # first-attempt-success latency from the intended send time
    total_latency: float | None = None  # intended -> acknowledged, including retries
    retries: int = 0
    error: str | None = None
    statuses: list[int] = field(default_factory=list)


class LoadRun:
    def __init__(self, cfg: LoadConfig) -> None:
        self.cfg = cfg
        self.run_id = uuid.uuid4().hex[:8]
        self.subs: list[Submission] = []
        self.samples: list[dict[str, Any]] = []
        self.status_counts: dict[str, int] = {}
        self.user_id: str | None = cfg.user_id
        self.auth: dict[str, str] = {}
        self._t0 = 0.0

    def log(self, msg: str) -> None:
        if self.cfg.on_event:
            self.cfg.on_event(msg)

    # ------------------------------------------------------------------------- setup
    async def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            base_url=self.cfg.api,
            timeout=self.cfg.request_timeout,
            transport=self.cfg.transport,
            limits=httpx.Limits(max_connections=self.cfg.max_in_flight + 20),
        )

    async def _setup_user(self, c: httpx.AsyncClient) -> None:
        if self.cfg.api_key:
            self.auth = {"Authorization": f"Bearer {self.cfg.api_key}"}
            return
        admin = {"X-Admin-Token": self.cfg.admin_token}
        r = await c.post(
            "/v1/admin/users",
            headers=admin,
            json={
                "email": f"loadtest-{self.run_id}@example.com",
                "name": "Load test",
                "tier": "enterprise",
            },
        )
        r.raise_for_status()
        body = r.json()
        self.user_id = body["user"]["id"]
        self.auth = {"Authorization": f"Bearer {body['api_key']}"}
        # a load test measures the platform, not its own rate limit: lift the plan limits
        r = await c.patch(
            f"/v1/admin/users/{self.user_id}",
            headers=admin,
            json={
                "rate_limit_per_sec": 100_000,
                "daily_job_quota": 100_000_000,
                "max_concurrent_jobs": 100_000,
            },
        )
        r.raise_for_status()

    # ----------------------------------------------------------------------- sending
    async def _submit(self, c: httpx.AsyncClient, sub: Submission, sem: asyncio.Semaphore) -> None:
        await asyncio.sleep(max(0.0, sub.intended - time.monotonic()))
        payload = {
            "seconds": max(0.0, random.uniform(0.5, 1.5) * self.cfg.job_seconds),
            "fail_probability": self.cfg.fail_probability,
        }
        body = {"type": "demo.sleep", "payload": payload, "max_attempts": self.cfg.max_attempts}
        headers = {**self.auth, "Idempotency-Key": sub.key}
        deadline = sub.intended + self.cfg.client_retry_deadline
        backoff = 0.2
        async with sem:
            while True:
                try:
                    r = await c.post("/v1/jobs", json=body, headers=headers)
                    sub.statuses.append(r.status_code)
                    self.status_counts[str(r.status_code)] = (
                        self.status_counts.get(str(r.status_code), 0) + 1
                    )
                    if r.status_code in (200, 201):
                        now = time.monotonic()
                        sub.job_id = r.json()["id"]
                        sub.total_latency = now - sub.intended
                        if sub.retries == 0:
                            sub.latency = sub.total_latency
                        return
                    if r.status_code not in (429, 500, 502, 503, 504):
                        sub.error = f"HTTP {r.status_code}: {r.text[:120]}"
                        return  # a client error will not get better by retrying
                except (httpx.TransportError, httpx.TimeoutException) as exc:
                    sub.statuses.append(0)
                    self.status_counts["network_error"] = (
                        self.status_counts.get("network_error", 0) + 1
                    )
                    sub.error = type(exc).__name__
                if time.monotonic() + backoff > deadline:
                    sub.error = sub.error or "retry deadline exceeded"
                    return
                sub.retries += 1
                await asyncio.sleep(backoff * random.uniform(0.5, 1.0))
                backoff = min(backoff * 2, 2.0)

    # ----------------------------------------------------------------------- sampling
    async def _sample(self, c: httpx.AsyncClient, stop: asyncio.Event) -> None:
        admin = {"X-Admin-Token": self.cfg.admin_token}
        while not stop.is_set():
            try:
                u = (await c.get("/v1/usage", headers=self.auth)).json()
                st = (await c.get("/v1/admin/stats", headers=admin)).json()
                ws = (await c.get("/v1/admin/workers", headers=admin)).json()
                alive = [w for w in ws if w["alive"]]
                self.samples.append(
                    {
                        "t": round(time.monotonic() - self._t0, 2),
                        "active": u["concurrency"]["active"],
                        "queue_depth": st["queue_depth"],
                        "by_status": st["jobs_by_status"],
                        "oldest_queued_s": st["oldest_queued_age_seconds"],
                        "workers_alive": len(alive),
                        "busy_slots": sum(w["active_jobs"] for w in alive),
                        "slots": sum(w["concurrency"] for w in alive),
                    }
                )
                if self.cfg.progress:
                    acked = sum(1 for s in self.subs if s.job_id)
                    print(
                        f"\r  t={self.samples[-1]['t']:6.1f}s  acked={acked:>6}  queue={st['queue_depth']:>5}"
                        f"  running={st['jobs_by_status'].get('running', 0):>3}"
                        f"  workers={len(alive)}   ",
                        end="",
                        flush=True,
                    )
            except Exception:
                pass
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=1.0)

    # ------------------------------------------------------------------------ the run
    async def run(self) -> dict[str, Any]:
        cfg = self.cfg
        async with await self._client() as c:
            await self._setup_user(c)
            n = int(cfg.rate * cfg.duration)
            self._t0 = time.monotonic() + 0.5
            sem = asyncio.Semaphore(cfg.max_in_flight)
            self.subs = [
                Submission(i, f"lt-{self.run_id}-{i}", self._t0 + i / cfg.rate) for i in range(n)
            ]
            stop = asyncio.Event()
            sampler = asyncio.create_task(self._sample(c, stop))
            self.log(f"load started: {cfg.rate:g} jobs/s for {cfg.duration:g}s ({n} jobs)")
            await asyncio.gather(*(self._submit(c, s, sem) for s in self.subs))
            send_end = time.monotonic()
            self.log("all submissions acknowledged")

            drained = True
            if cfg.wait_for_drain:
                drained = False
                deadline = time.monotonic() + cfg.drain_timeout
                while time.monotonic() < deadline:
                    try:
                        u = (await c.get("/v1/usage", headers=self.auth)).json()
                        if u["concurrency"]["active"] == 0:
                            drained = True
                            break
                    except Exception:
                        pass
                    await asyncio.sleep(0.5)
            drain_end = time.monotonic()
            stop.set()
            await sampler
            if cfg.progress:
                print()
            jobs = await self._fetch_jobs(c)
        return self._report(jobs, send_end, drain_end, drained)

    async def _fetch_jobs(self, c: httpx.AsyncClient) -> list[dict[str, Any]]:
        jobs, cursor = [], None
        for _ in range(100_000):
            params: dict[str, Any] = {"limit": 200}
            if cursor:
                params["cursor"] = cursor
            r = await c.get("/v1/jobs", params=params, headers=self.auth)
            r.raise_for_status()
            page = r.json()
            jobs += page["items"]
            cursor = page["next_cursor"]
            if not cursor:
                break
        return jobs

    # ---------------------------------------------------------------------- reporting
    def _report(
        self, jobs: list[dict[str, Any]], send_end: float, drain_end: float, drained: bool
    ) -> dict[str, Any]:
        from datetime import datetime

        def ts(v: str | None) -> float | None:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp() if v else None

        cfg = self.cfg
        acked = [s for s in self.subs if s.job_id]
        failed_subs = [s for s in self.subs if not s.job_id]
        by_status: dict[str, int] = {}
        e2e, start_delay, attempts = [], [], {}
        first_created, last_finished = None, None
        for j in jobs:
            by_status[j["status"]] = by_status.get(j["status"], 0) + 1
            attempts[str(j["attempts"])] = attempts.get(str(j["attempts"]), 0) + 1
            created, started, finished = (
                ts(j["created_at"]),
                ts(j["started_at"]),
                ts(j["finished_at"]),
            )
            first_created = created if first_created is None else min(first_created, created)
            if j["status"] == "succeeded" and finished and created:
                e2e.append(finished - created)
                last_finished = finished if last_finished is None else max(last_finished, finished)
            if started and created:
                start_delay.append(started - created)
        succeeded = by_status.get("succeeded", 0)
        busy = [s["busy_slots"] / s["slots"] for s in self.samples if s["slots"]]
        send_seconds = max(1e-9, send_end - self._t0)
        window = (
            (last_finished - first_created)
            if (last_finished and first_created)
            else drain_end - self._t0
        )
        return {
            "run_id": self.run_id,
            "user_id": self.user_id,
            "config": {
                "rate": cfg.rate,
                "duration": cfg.duration,
                "job_seconds": cfg.job_seconds,
                "fail_probability": cfg.fail_probability,
                "max_attempts": cfg.max_attempts,
            },
            "submit": {
                "planned": len(self.subs),
                "acknowledged": len(acked),
                "gave_up": len(failed_subs),
                "achieved_rate": len(acked) / send_seconds,
                "latency_first_try": summarize([s.latency for s in acked if s.latency is not None]),
                "latency_incl_retries": summarize(
                    [s.total_latency for s in acked if s.total_latency is not None]
                ),
                "retried_submissions": sum(1 for s in self.subs if s.retries),
                "total_retries": sum(s.retries for s in self.subs),
                "http_status_counts": self.status_counts,
                "errors": sorted({s.error for s in failed_subs if s.error}),
            },
            "jobs": {
                "total_in_system": len(jobs),
                "by_status": by_status,
                "drained": drained,
                "attempts_histogram": attempts,
                "end_to_end_seconds": summarize(e2e),
                "wait_for_first_start_seconds": summarize(start_delay),
                "throughput_jobs_per_s": succeeded / window if window > 0 else 0.0,
            },
            "capacity": {
                "peak_queue_depth": max((s["queue_depth"] for s in self.samples), default=0),
                "peak_oldest_queued_s": max(
                    (s["oldest_queued_s"] for s in self.samples), default=0.0
                ),
                "mean_worker_utilization": sum(busy) / len(busy) if busy else 0.0,
                "max_workers_alive": max((s["workers_alive"] for s in self.samples), default=0),
                "min_workers_alive": min((s["workers_alive"] for s in self.samples), default=0),
            },
            "acknowledged_job_ids": [s.job_id for s in acked],
            "samples": self.samples,
        }


async def run_load(cfg: LoadConfig) -> dict[str, Any]:
    return await LoadRun(cfg).run()


def render_report(r: dict[str, Any], color: bool = True) -> str:
    b, d, g, y, x = (
        ("\033[1m", "\033[2m", "\033[32m", "\033[33m", "\033[0m") if color else ("",) * 5
    )
    s, j, cap, cfg = r["submit"], r["jobs"], r["capacity"], r["config"]
    lt, e2, w = s["latency_first_try"], j["end_to_end_seconds"], j["wait_for_first_start_seconds"]
    ms = lambda v: f"{v * 1000:,.0f} ms"  # noqa: E731
    sec = lambda v: f"{v:,.2f} s"  # noqa: E731
    st = " ".join(f"{k}={v}" for k, v in sorted(j["by_status"].items()))
    lines = [
        f"{b}Load test report{x} {d}(run {r['run_id']}){x}",
        f"  target ............. {cfg['rate']:g} jobs/s for {cfg['duration']:g}s, "
        f"~{cfg['job_seconds']:g}s of work per job, {cfg['fail_probability']:.0%} injected failures",
        f"  {b}submission{x}",
        f"    acknowledged ..... {s['acknowledged']}/{s['planned']}  (achieved {s['achieved_rate']:.1f}/s)"
        + (f"  {y}gave up: {s['gave_up']}{x}" if s["gave_up"] else ""),
        f"    latency .......... p50 {ms(lt['p50'])}   p95 {g}{ms(lt['p95'])}{x}   p99 {ms(lt['p99'])}   max {ms(lt['max'])}"
        f"  {d}(from intended send time){x}",
        f"    client retries ... {s['total_retries']} across {s['retried_submissions']} submissions  "
        f"{d}http: {json.dumps(s['http_status_counts'])}{x}",
        f"  {b}processing{x}",
        f"    outcome .......... {st}   {d}(drained: {j['drained']}){x}",
        f"    throughput ....... {j['throughput_jobs_per_s']:.1f} jobs/s completed",
        f"    end-to-end ....... p50 {sec(e2['p50'])}   p95 {g}{sec(e2['p95'])}{x}   p99 {sec(e2['p99'])}   max {sec(e2['max'])}",
        f"    wait to start .... p50 {sec(w['p50'])}   p95 {sec(w['p95'])}",
        f"    attempts ......... {json.dumps(j['attempts_histogram'])}",
        f"  {b}capacity{x}",
        f"    peak queue depth . {cap['peak_queue_depth']}   oldest waiting {sec(cap['peak_oldest_queued_s'])}",
        f"    worker use ....... {cap['mean_worker_utilization']:.0%} of slots on average, "
        f"workers alive {cap['min_workers_alive']}..{cap['max_workers_alive']}",
    ]
    return "\n".join(lines)
