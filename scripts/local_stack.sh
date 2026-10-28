#!/usr/bin/env bash
# Run the whole platform as local processes against an existing PostgreSQL + RabbitMQ.
#   FOUNDRY_DATABASE_URL, FOUNDRY_BROKER_URL  point at your services (defaults below)
#   WORKERS=3                                 number of worker processes
set -euo pipefail
cd "$(dirname "$0")/.."
RUN_DIR=${RUN_DIR:-.run}
WORKERS=${WORKERS:-3}
export PYTHONPATH="$PWD/src${PYTHONPATH:+:$PYTHONPATH}"
export FOUNDRY_DATABASE_URL=${FOUNDRY_DATABASE_URL:-postgresql://foundry:foundry@localhost:5432/foundry}
export FOUNDRY_BROKER_URL=${FOUNDRY_BROKER_URL:-amqp://guest:guest@localhost:5672/}
export FOUNDRY_REDIS_URL=${FOUNDRY_REDIS_URL:-redis://localhost:6379/0}
export FOUNDRY_LEASE_SECONDS=${FOUNDRY_LEASE_SECONDS:-6}
export FOUNDRY_HEARTBEAT_SECONDS=${FOUNDRY_HEARTBEAT_SECONDS:-2}
export FOUNDRY_BACKOFF_BASE_SECONDS=${FOUNDRY_BACKOFF_BASE_SECONDS:-1}
export FOUNDRY_LOG_JSON=${FOUNDRY_LOG_JSON:-true}

start() { # name, command...
  local name=$1; shift
  nohup "$@" >"$RUN_DIR/$name.log" 2>&1 &
  echo $! >"$RUN_DIR/$name.pid"
}

case "${1:-up}" in
  up)
    mkdir -p "$RUN_DIR"
    python3 -m foundry.cli migrate
    start api python3 -m uvicorn foundry.api.app:app_factory --factory --port "${PORT:-8000}"
    start relay python3 -m foundry.cli relay
    start reaper python3 -m foundry.cli reaper
    # each local worker needs its own metrics port (containers get one namespace each)
    for i in $(seq "$WORKERS"); do
      FOUNDRY_METRICS_PORT=$((9109 + i)) start "worker$i" python3 -m foundry.cli worker
    done
    echo "started api, relay, reaper and $WORKERS workers (logs in $RUN_DIR/)"
    echo "metrics: api :${PORT:-8000}/metrics  workers :9110+  relay :9101  reaper :9102"
    ;;
  down)
    for f in "$RUN_DIR"/*.pid; do
      [ -e "$f" ] || continue
      kill "$(cat "$f")" 2>/dev/null || true
      rm -f "$f"
    done
    echo "stopped"
    ;;
  *) echo "usage: $0 up|down" >&2; exit 2 ;;
esac
