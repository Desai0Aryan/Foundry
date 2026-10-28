#!/usr/bin/env bash
# Part 1 walkthrough: create a user, submit jobs, prove idempotency and dedup.
set -euo pipefail
API=${API:-http://localhost:8000}
ADMIN=${FOUNDRY_ADMIN_TOKEN:-dev-admin-token-change-me}
bold() { printf '\n\033[1m%s\033[0m\n' "$*"; }

bold "1) Create a user (the API key is shown exactly once)"
RESP=$(curl -sf -X POST "$API/v1/admin/users" -H "X-Admin-Token: $ADMIN" \
  -H 'Content-Type: application/json' \
  -d "{\"email\":\"demo-$RANDOM@example.com\",\"name\":\"Demo\",\"tier\":\"pro\"}")
KEY=$(echo "$RESP" | python3 -c 'import sys,json; print(json.load(sys.stdin)["api_key"])')
AUTH="Authorization: Bearer $KEY"
echo "$RESP" | python3 -m json.tool | sed 's/"api_key": ".*"/"api_key": "fnd_********"/'

bold "2) Submit a job with an Idempotency-Key  ->  201 Created"
curl -si -X POST "$API/v1/jobs" -H "$AUTH" -H 'Idempotency-Key: demo-order-1' \
  -H 'Content-Type: application/json' \
  -d '{"type":"demo.sleep","payload":{"seconds":2}}' | sed -n '1p;/^[Ii]dempotent/p;/^{/p'

bold "3) Retry the exact same request (network hiccup)  ->  200 + Idempotent-Replayed"
curl -si -X POST "$API/v1/jobs" -H "$AUTH" -H 'Idempotency-Key: demo-order-1' \
  -H 'Content-Type: application/json' \
  -d '{"type":"demo.sleep","payload":{"seconds":2}}' | sed -n '1p;/^[Ii]dempotent/p;/^{/p'

bold "4) Same key, different body  ->  422 idempotency_key_reuse"
curl -s -X POST "$API/v1/jobs" -H "$AUTH" -H 'Idempotency-Key: demo-order-1' \
  -H 'Content-Type: application/json' \
  -d '{"type":"demo.sleep","payload":{"seconds":99}}' | python3 -m json.tool

bold "5) 20 concurrent identical submissions with dedup  ->  exactly one job"
for i in $(seq 20); do
  curl -s -o /dev/null -w '%{http_code}\n' -X POST "$API/v1/jobs" -H "$AUTH" \
    -H 'Content-Type: application/json' \
    -d '{"type":"demo.sleep","payload":{"seconds":5},"deduplicate":true}' &
done | sort | uniq -c
wait

bold "6) Your jobs"
curl -s "$API/v1/jobs?limit=5" -H "$AUTH" | python3 -c '
import sys, json
for j in json.load(sys.stdin)["items"]:
    print(j["id"], j["type"], j["status"])'
