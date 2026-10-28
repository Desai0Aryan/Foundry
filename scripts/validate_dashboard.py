#!/usr/bin/env python3
"""Run every Grafana panel query against a live Prometheus and report errors / empty panels.

    python scripts/validate_dashboard.py --prometheus http://localhost:9090

A query that Prometheus rejects is a bug; a query that returns no series is only a warning
(the metric may not have been emitted yet, e.g. no failures so far).
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.parse
import urllib.request
from pathlib import Path

DASHBOARD = Path(__file__).resolve().parent.parent / "ops/grafana/dashboards/foundry.json"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prometheus", default="http://localhost:9090")
    ap.add_argument("--window", default="1m", help="substituted for $__rate_interval / $__interval")
    args = ap.parse_args()

    dash = json.loads(DASHBOARD.read_text())
    bad = empty = ok = 0
    for panel in dash["panels"]:
        for t in panel.get("targets", []):
            expr = (
                t["expr"]
                .replace("$__rate_interval", args.window)
                .replace("$__interval", args.window)
            )
            url = f"{args.prometheus}/api/v1/query?" + urllib.parse.urlencode({"query": expr})
            with urllib.request.urlopen(url, timeout=10) as resp:
                body = json.loads(resp.read())
            if body["status"] != "success":
                bad += 1
                print(f"  ERROR  {panel['title']!r}: {body.get('error')}")
            elif not body["data"]["result"]:
                empty += 1
                print(f"  empty  {panel['title']!r}")
            else:
                ok += 1
                print(f"  ok     {panel['title']!r}: {len(body['data']['result'])} series")
    print(f"\n{ok} ok, {empty} empty, {bad} errors")
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
