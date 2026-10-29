#!/usr/bin/env bash
# The portfolio demo: load test + kill a worker + zombie + broker outage, then prove it.
# Needs PostgreSQL, Redis and RabbitMQ reachable via the FOUNDRY_* env vars.
set -euo pipefail
cd "$(dirname "$0")/.."
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
python3 -m foundry.cli migrate
echo "== 1/2  worker SIGKILLed mid-load"
python3 -m foundry.cli chaos worker-crash --duration 30 --rate 10
echo "== 2/2  everything at once"
python3 -m foundry.cli chaos kitchen-sink --duration 50 --rate 8
