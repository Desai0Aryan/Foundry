#!/usr/bin/env python3
"""Part 2 demo: submit a batch, SIGKILL a busy worker mid-flight, watch every job recover.

Works against `docker compose up` (kills the worker's container) or `make local-up`
(kills the worker's process). Uses only the standard library.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import signal
import socket
import subprocess
import sys
import time
import urllib.request

BOLD, DIM, GREEN, RED, YELLOW, RESET = (
    "\033[1m",
    "\033[2m",
    "\033[32m",
    "\033[31m",
    "\033[33m",
    "\033[0m",
)


def call(method: str, url: str, headers: dict, body: dict | None = None):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    for k, v in {**headers, "Content-Type": "application/json"}.items():
        req.add_header(k, v)
    with urllib.request.urlopen(req, timeout=15) as resp:
        return json.loads(resp.read() or b"null")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default=os.environ.get("API", "http://localhost:8000"))
    ap.add_argument(
        "--admin-token", default=os.environ.get("FOUNDRY_ADMIN_TOKEN", "dev-admin-token-change-me")
    )
    ap.add_argument("--jobs", type=int, default=40)
    ap.add_argument("--seconds", type=float, default=4.0, help="duration of each job")
    ap.add_argument("--timeout", type=float, default=120)
    args = ap.parse_args()
    admin = {"X-Admin-Token": args.admin_token}

    before = call("GET", f"{args.api}/v1/admin/effects", admin)
    print(f"{BOLD}1) Create a demo user{RESET}")
    created = call(
        "POST",
        f"{args.api}/v1/admin/users",
        admin,
        {
            "email": f"crash-demo-{random.randrange(10**6)}@example.com",
            "name": "Crash demo",
            "tier": "enterprise",
        },
    )
    auth = {"Authorization": f"Bearer {created['api_key']}"}

    print(
        f"{BOLD}2) Submit {args.jobs} jobs ({args.seconds}s each, every one has a side effect){RESET}"
    )
    ids = []
    for i in range(args.jobs):
        job = call(
            "POST",
            f"{args.api}/v1/jobs",
            {**auth, "Idempotency-Key": f"crash-demo-{time.time_ns()}-{i}"},
            {"type": "demo.sleep", "payload": {"seconds": args.seconds}},
        )
        ids.append(job["id"])

    print(f"{BOLD}3) Wait until workers are busy, then SIGKILL one{RESET}")
    victim = None
    deadline = time.time() + 30
    while time.time() < deadline and victim is None:
        busy = [
            w
            for w in call("GET", f"{args.api}/v1/admin/workers", admin)
            if w["alive"] and w["active_jobs"] > 0
        ]
        if busy:
            victim = max(busy, key=lambda w: w["active_jobs"])
        else:
            time.sleep(0.3)
    if victim is None:
        print(f"{RED}no busy worker found: are workers and the relay running?{RESET}")
        return 1
    where = "process" if victim["host"] == socket.gethostname() else "container"
    print(
        f"   {RED}killing worker {victim['id']} ({where}, {victim['active_jobs']} jobs in flight){RESET}"
    )
    if where == "process":
        os.kill(victim["pid"], signal.SIGKILL)
    else:
        subprocess.run(["docker", "kill", victim["host"]], check=True, capture_output=True)

    print(
        f"{BOLD}4) Watch recovery{RESET}  {DIM}(lease expiry -> reaper -> backoff -> another worker){RESET}"
    )
    start, last = time.time(), None
    while time.time() - start < args.timeout:
        jobs = [call("GET", f"{args.api}/v1/jobs/{i}", auth) for i in ids]
        counts: dict[str, int] = {}
        for j in jobs:
            counts[j["status"]] = counts.get(j["status"], 0) + 1
        line = "   " + "  ".join(f"{k}={v}" for k, v in sorted(counts.items()))
        if line != last:
            print(f"{DIM}[{time.time() - start:5.1f}s]{RESET}{line}")
            last = line
        if counts.get("succeeded", 0) == len(ids):
            break
        time.sleep(1)
    else:
        print(f"{RED}timed out waiting for recovery{RESET}")
        return 1

    recovered = [j for j in jobs if j["attempts"] > 1]
    effects = call("GET", f"{args.api}/v1/admin/effects", admin)
    print(f"\n{BOLD}Result{RESET}")
    print(f"   jobs submitted ............. {len(ids)}")
    print(f"   jobs succeeded ............. {GREEN}{counts['succeeded']}{RESET}")
    print(
        f"   recovered after the crash .. {YELLOW}{len(recovered)}{RESET} (ran a 2nd attempt on another worker)"
    )
    applied = effects["effects_applied"] - before["effects_applied"]
    jobs_hit = effects["jobs_with_effects"] - before["jobs_with_effects"]
    print(f"   side effects applied ....... {applied}  (one per job)")
    print(f"   duplicate effects .......... {GREEN}{applied - jobs_hit}{RESET}")
    ok = counts["succeeded"] == len(ids) and applied == jobs_hit == len(ids)
    print(
        f"\n{GREEN if ok else RED}{'PASS: every job completed exactly once.' if ok else 'FAIL'}{RESET}"
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
