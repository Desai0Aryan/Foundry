#!/usr/bin/env python3
"""Part 3 demo: rate limiting, quotas, concurrency limits, and live metrics.

python scripts/demo_part3.py            # guided tour of the limits
python scripts/demo_part3.py --traffic 40   # 40 s of mixed traffic to light up the dashboard
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BOLD, DIM, GREEN, RED, YELLOW, RESET = (
    "\033[1m",
    "\033[2m",
    "\033[32m",
    "\033[31m",
    "\033[33m",
    "\033[0m",
)


def call(method, url, headers=None, body=None):
    """Returns (status, headers, json) without raising on 4xx/5xx."""
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    for k, v in {"Content-Type": "application/json", **(headers or {})}.items():
        req.add_header(k, v)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read()
            return (
                r.status,
                {k.lower(): v for k, v in r.headers.items()},
                json.loads(raw) if raw else None,
            )
    except urllib.error.HTTPError as e:
        raw = e.read()
        return (
            e.code,
            {k.lower(): v for k, v in e.headers.items()},
            json.loads(raw) if raw else None,
        )


def make_user(api, admin, tier="pro", **limits):
    st, _, body = call(
        "POST",
        f"{api}/v1/admin/users",
        admin,
        {"email": f"demo-{random.randrange(10**8)}@example.com", "name": "Demo", "tier": tier},
    )
    assert st == 201, body
    if limits:
        call("PATCH", f"{api}/v1/admin/users/{body['user']['id']}", admin, limits)
    return body["user"], {"Authorization": f"Bearer {body['api_key']}"}


def tour(api, admin):
    print(f"{BOLD}1) Rate limiting: token bucket in Redis{RESET}")
    user, auth = make_user(api, admin, rate_limit_per_sec=5)
    print("   limit: 5 req/s, burst 10. Firing 25 requests as fast as possible...")
    codes, last = [], None
    for _ in range(25):
        st, h, _ = call("GET", f"{api}/v1/me", auth)
        codes.append(st)
        if st == 429:
            last = h
    print(
        f"   {GREEN}{codes.count(200)} allowed{RESET}, {RED}{codes.count(429)} rejected (429){RESET}"
    )
    if last:
        print(
            f"   {DIM}Retry-After={last.get('retry-after')}  RateLimit-Limit={last.get('ratelimit-limit')}  "
            f"RateLimit-Remaining={last.get('ratelimit-remaining')}{RESET}"
        )
    time.sleep(1.2)
    st, h, _ = call("GET", f"{api}/v1/me", auth)
    print(
        f"   after 1.2 s the bucket has refilled -> HTTP {st}, RateLimit-Remaining={h.get('ratelimit-remaining')}"
    )

    print(f"\n{BOLD}2) Daily quota: only created jobs count{RESET}")
    user, auth = make_user(
        api, admin, rate_limit_per_sec=1000, daily_job_quota=3, max_concurrent_jobs=100
    )
    key = {"Idempotency-Key": "demo-quota-1"}
    for i in range(3):
        st, h, _ = call(
            "POST",
            f"{api}/v1/jobs",
            {**auth, **(key if i == 0 else {})},
            {"type": "demo.sleep", "payload": {"seconds": 0.1 + i}},
        )
        print(
            f"   job {i + 1}: HTTP {st}  X-Daily-Quota-Remaining={h.get('x-daily-quota-remaining')}"
        )
    st, h, body = call(
        "POST", f"{api}/v1/jobs", auth, {"type": "demo.sleep", "payload": {"seconds": 9}}
    )
    print(
        f"   job 4 (new):     {RED}HTTP {st} {body['code']}{RESET}  Retry-After={h.get('retry-after')}s"
    )
    st, h, _ = call(
        "POST",
        f"{api}/v1/jobs",
        {**auth, **key},
        {"type": "demo.sleep", "payload": {"seconds": 0.1}},
    )
    print(
        f"   job 1 retried:   {GREEN}HTTP {st}{RESET}  Idempotent-Replayed={h.get('idempotent-replayed')}"
        f"  {DIM}(a retry of work that already succeeded is never punished){RESET}"
    )

    print(f"\n{BOLD}3) Concurrent-jobs limit: exact even under a burst{RESET}")
    user, auth = make_user(
        api, admin, rate_limit_per_sec=1000, daily_job_quota=1000, max_concurrent_jobs=3
    )

    def submit(i):
        return call(
            "POST", f"{api}/v1/jobs", auth, {"type": "demo.sleep", "payload": {"seconds": 5 + i}}
        )[0]

    with ThreadPoolExecutor(12) as pool:
        codes = list(pool.map(submit, range(12)))
    print(
        f"   12 simultaneous submissions, limit 3 -> {GREEN}{codes.count(201)} accepted{RESET}, "
        f"{RED}{codes.count(429)} rejected{RESET}"
    )
    st, _, u = call("GET", f"{api}/v1/usage", auth)
    print(
        f"   /v1/usage: {json.dumps(u['concurrency'])}  {json.dumps(u['daily_quota'] | {'resets_at': '...'})}"
    )

    print(f"\n{BOLD}4) Credential guessing is throttled, real users are not{RESET}")
    codes = [
        call("GET", f"{api}/v1/me", {"Authorization": f"Bearer fnd_deadbeef_guess{i}"})[0]
        for i in range(40)
    ]
    print(f"   40 bad keys -> {codes.count(401)} x 401, then {RED}{codes.count(429)} x 429{RESET}")
    st, _, _ = call("GET", f"{api}/v1/me", auth)
    print(f"   a valid key from the same address still works -> HTTP {st}")

    print(f"\n{BOLD}5) Metrics{RESET}")
    req = urllib.request.Request(f"{api}/metrics")
    text = urllib.request.urlopen(req, timeout=10).read().decode()
    for line in text.splitlines():
        if line.startswith(
            (
                "foundry_jobs{",
                "foundry_workers_alive",
                "foundry_queue_oldest",
                "foundry_rate_limit_decisions_total",
                "foundry_limit_rejections_total",
                "foundry_job_submissions_total",
            )
        ):
            print(f"   {line}")


def traffic(api, admin, seconds):
    """Mixed workload: successes, flaky jobs, permanent failures, 429s, quota hits, bad keys."""
    print(f"{BOLD}Generating {seconds}s of mixed traffic...{RESET}")
    _, auth = make_user(api, admin, tier="enterprise")
    _, tight = make_user(
        api, admin, rate_limit_per_sec=2, daily_job_quota=50, max_concurrent_jobs=100
    )
    end, n = time.time() + seconds, 0
    kinds = ["ok"] * 6 + ["flaky"] * 2 + ["fail"]
    while time.time() < end:
        kind = random.choice(kinds)
        payload = {
            "seconds": random.uniform(0.2, 1.5),
            "fail_probability": {"ok": 0, "flaky": 0.5, "fail": 1}[kind],
        }
        call(
            "POST",
            f"{api}/v1/jobs",
            {**auth, "Idempotency-Key": f"t-{time.time_ns()}"},
            {"type": "demo.sleep", "payload": payload, "max_attempts": 3},
        )
        if n % 3 == 0:
            call("GET", f"{api}/v1/me", tight)  # drives 429s on the tight user
        if n % 10 == 0:
            call("GET", f"{api}/v1/me", {"Authorization": "Bearer fnd_badbad00_nope"})
        n += 1
        time.sleep(0.08)
    print(f"   submitted {n} jobs")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--api", default=os.environ.get("API", "http://localhost:8000"))
    ap.add_argument(
        "--admin-token", default=os.environ.get("FOUNDRY_ADMIN_TOKEN", "dev-admin-token-change-me")
    )
    ap.add_argument("--traffic", type=int, metavar="SECONDS")
    a = ap.parse_args()
    admin = {"X-Admin-Token": a.admin_token}
    traffic(a.api, admin, a.traffic) if a.traffic else tour(a.api, admin)
    return 0


if __name__ == "__main__":
    sys.exit(main())
