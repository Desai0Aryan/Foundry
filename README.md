<div align="center">

# ⚒️ Foundry

**A production-style distributed job processing platform.**
Submit long-running jobs over REST. A fleet of workers executes them reliably — surviving crashes, retries, and duplicate deliveries without duplicate side effects.

![python](https://img.shields.io/badge/python-3.11%2B-3776AB?logo=python&logoColor=white)
![fastapi](https://img.shields.io/badge/FastAPI-async-009688?logo=fastapi&logoColor=white)
![postgres](https://img.shields.io/badge/PostgreSQL-16-4169E1?logo=postgresql&logoColor=white)
![redis](https://img.shields.io/badge/Redis-7-DC382D?logo=redis&logoColor=white)
![rabbitmq](https://img.shields.io/badge/RabbitMQ-3.13-FF6600?logo=rabbitmq&logoColor=white)
![docker](https://img.shields.io/badge/Docker-ready-2496ED?logo=docker&logoColor=white)

</div>

---

## Why this exists

Most "AI demos" are a model behind an endpoint. Real systems are mostly **failure handling**: a worker dies mid-job, a client retries a request, a message is delivered twice, a queue backs up. Foundry is built around those problems first.

> **Demo goal:** run a load test, `kill -9` a worker mid-processing, and watch every job recover — with zero duplicate side effects.

## Build status

| Part | Scope | Status |
|-----:|-------|:------:|
| 1 | Schema, auth, REST API, idempotency, dedup, transactional outbox, Docker stack | ✅ done |
| 2 | Workers, RabbitMQ, retries + exponential backoff, timeouts, dead-letter queue, lease reaper, crash recovery | ✅ **done** |
| 3 | Rate limiting, per-user quotas, Prometheus metrics, Grafana dashboards | ⏳ |
| 4 | Load tests, chaos / worker-crash recovery tests, exactly-once-effect proof | ⏳ |
| 5 | Kubernetes manifests, CI polish, architecture docs, final demo | ⏳ |

## Architecture

```mermaid
flowchart LR
    C([Client]) -->|"POST /v1/jobs<br/>Idempotency-Key"| API[FastAPI API]
    API -->|"one transaction"| PG[(PostgreSQL<br/>source of truth)]
    PG -. "jobs + events + outbox" .-> PG
    PG -->|"outbox relay<br/>(publisher confirms)"| MQ{{RabbitMQ<br/>priority queue}}
    MQ --> W1[Worker 1]
    MQ --> W2[Worker 2]
    MQ --> W3[Worker N]
    W1 & W2 & W3 -->|"claim · heartbeat · fenced result"| PG
    RP[Reaper] -->|"expired leases → retry<br/>due retries → queue"| PG
    MQ -. "exhausted jobs" .-> DLQ{{Dead-letter queue}}
    API -. "rate limit / quotas (Part 3)" .-> R[(Redis)]
```

**Key idea:** PostgreSQL owns *state*; the broker only carries *hints that work exists*. If the broker loses every message, the outbox and the lease reaper rebuild them. If a worker dies, its lease expires and another worker takes over.

## How a job survives a crash

```
queued  ──claim(lease)──▶ running ──▶ succeeded
                             │  ╲
              handler error  │   ╲ worker dies / stalls  (lease expires)
              or timeout     ▼    ▼
                       retry_scheduled ──backoff──▶ queued ──▶ …
                             │ attempts exhausted
                             ▼
                            dead  ──replay──▶ queued
```

Real audit trail from the crash demo (`GET /v1/jobs/{id}/events`), where worker `…-3473` was `kill -9`'d mid-job:

```
queued           submitted
running          worker …-3473-96977e   attempt 1      ← killed here, no goodbye
retry_scheduled  lease_expired          attempt 1      ← reaper noticed after the lease ran out
queued           backoff_elapsed
running          worker …-3475-86cd24   attempt 2      ← a different worker took over
succeeded        worker …-3475-86cd24   attempt 2
```

Three mechanisms make this safe (see [ADR 0004](docs/adr/0004-leases-and-fencing.md)):
**leases** (a dead worker's jobs are reclaimed), **fencing tokens** (a "zombie" worker's late writes are rejected by the database), and an **effect ledger** (a side effect keyed by `(job, effect)` can be applied only once, however many times the job runs).

## What Part 2 adds

- **Worker fleet** (`foundry worker`): RabbitMQ consumers with prefetch-based concurrency, heartbeats, cooperative cancellation, `asyncio.timeout` per job, graceful SIGTERM drain.
- **Outbox relay** (`foundry relay`): `LISTEN/NOTIFY`-woken, `SKIP LOCKED` batches, publisher confirms, detection of unroutable messages (found by a test — aio-pika returns them instead of raising).
- **Reaper** (`foundry reaper`): lease recovery, retry promotion, lost-message redrive, housekeeping.
- **Retries** with equal-jitter exponential backoff, per-job `max_attempts` and `timeout_seconds`, `PermanentError` vs `TransientError`.
- **Dead-letter queue**: `dead` status + `dead` broker queue + poison-message queue + `POST /v1/jobs/{id}/replay`.
- **Priorities** (0–9) honoured by the broker queue; **worker registry** and **queue stats** under `/v1/admin`.

> The handlers simulate heavy work (deterministic results, realistic timing) so the reliability machinery can be demonstrated anywhere. Swap in real conversion/transcoding/inference inside `handlers.py`; the contract (`ctx.effect`, `PermanentError`/`TransientError`) stays the same.

## What Part 1 gives you

- **Authenticated REST API** — API keys stored as `HMAC-SHA256(pepper, secret)`, constant-time compare, key rotation, tier-based limits.
- **Idempotent submission** — `Idempotency-Key` header. Same key + same request → the original job. Same key + *different* request → `422`. Enforced by a unique index, so it holds under 25 concurrent retries (tested).
- **Opt-in deduplication** — `"deduplicate": true` collapses identical *active* jobs; the slot frees automatically when the job finishes (partial unique index).
- **Transactional outbox** — the job row, its audit event, and its "run me" message commit atomically. No dual-write bug.
- **Strict state machine** — `queued → running → succeeded | retry_scheduled | dead | cancelled`, encoded once and tested.
- **Payload validation + SSRF guard** — typed schemas per job type, normalized so equivalent requests hash equally.
- **Keyset pagination**, per-job **audit trail**, RFC 9457 `problem+json` errors, request IDs, structured JSON logs.
- **Schema migrations** with an advisory lock, safe for many replicas starting at once.

## Quickstart

```bash
docker compose up --build -d          # Postgres, Redis, RabbitMQ, API, relay, reaper, 3 workers
open http://localhost:8000/docs        # interactive OpenAPI
make demo                              # idempotency + dedup walkthrough
make crash-demo                        # SIGKILL a busy worker, watch all jobs recover
make scale N=8                         # resize the worker fleet
# RabbitMQ management UI: http://localhost:15672  (guest / guest)
```

Or step by step:

```bash
# 1. create a user (admin token from docker-compose / .env)
curl -s -X POST localhost:8000/v1/admin/users \
  -H "X-Admin-Token: dev-admin-token-change-me" -H 'Content-Type: application/json' \
  -d '{"email":"you@example.com","name":"You","tier":"pro"}'
# -> {"user": {...}, "api_key": "fnd_ab12cd34_..."}   (shown once)

# 2. submit a job, safely retryable
curl -i -X POST localhost:8000/v1/jobs \
  -H "Authorization: Bearer $KEY" -H "Idempotency-Key: invoice-2026-10-26" \
  -H 'Content-Type: application/json' \
  -d '{"type":"video.transcode","payload":{"source_url":"https://example.com/in.mp4","preset":"1080p"},"priority":7}'

# 3. watch it
curl -s localhost:8000/v1/jobs/$JOB_ID        -H "Authorization: Bearer $KEY"
curl -s localhost:8000/v1/jobs/$JOB_ID/events -H "Authorization: Bearer $KEY"
```

## API surface

| Method & path | Purpose |
|---|---|
| `POST /v1/jobs` | Submit a job (`Idempotency-Key`, `deduplicate`, `priority`, `max_attempts`, `timeout_seconds`) |
| `GET /v1/jobs` | List your jobs, newest first, keyset-paginated, `?status=` filter |
| `GET /v1/jobs/{id}` · `/events` | Job state · audit trail of every transition |
| `POST /v1/jobs/{id}/cancel` | Immediate for queued jobs, cooperative (`202`) for running ones |
| `POST /v1/jobs/{id}/replay` | Re-run a dead-lettered job with a fresh attempt budget |
| `GET /v1/job-types` | Discover job types and their JSON-Schema payloads |
| `GET /v1/me` | Your tier and limits |
| `POST /v1/admin/users` · `/users/{id}/api-keys` | Provision users, rotate keys (admin token) |
| `GET /v1/admin/workers` · `/stats` · `/effects` | Fleet liveness · queue depth & DLQ size · side-effect ledger summary (admin) |
| `GET /healthz` · `/readyz` | Liveness · readiness (checks PostgreSQL) |

Job types: `document.convert`, `video.transcode`, `ml.inference`, `demo.sleep` (synthetic, for load/chaos tests).

## Development

```bash
make install        # editable install + dev tools
make infra          # just the backing services
make dev            # API with auto-reload
make test           # 111 tests: unit + integration (real PostgreSQL + RabbitMQ, no mocks)
make local-up       # run everything as local processes (no Docker for the apps)
make lint
```

Integration tests need PostgreSQL (`FOUNDRY_TEST_DATABASE_URL`, default
`postgresql://postgres@localhost:5433/foundry_test`) and, for the end-to-end/crash tests,
RabbitMQ (`FOUNDRY_TEST_BROKER_URL`, default `amqp://guest:guest@localhost:5672/`). Each run uses
its own uniquely-named exchanges and queues. Tests skip cleanly if a service is unreachable.
The crash test spawns a real worker process and `SIGKILL`s it mid-job.

## Project layout

```
src/foundry/
  api/            FastAPI app, routes, auth dependency
  service.py      submission / cancellation / replay: idempotency + dedup + atomicity
  lifecycle.py    every post-submission transition: claim, fenced complete/fail, reaper ops
  worker.py       RabbitMQ consumer, heartbeats, timeouts, graceful shutdown
  relay.py        transactional-outbox relay (PostgreSQL -> RabbitMQ)
  reaper.py       lease recovery, retry promotion, redrive
  broker.py       exchange / queue / DLX topology
  handlers.py     job handlers + JobContext (effect ledger)
  backoff.py      exponential backoff with jitter
  repository.py   every SQL statement lives here
  domain.py       state machine, tier limits, canonical hashing
  jobtypes.py     payload schemas + per-type defaults (extend here)
  security.py     API key generation and verification
  migrations/     ordered SQL migrations
docs/adr/         architecture decision records
tests/            unit + integration
```

## Design decisions

Short ADRs in [`docs/adr`](docs/adr): PostgreSQL as the source of truth, the transactional outbox, idempotency semantics, leases + fencing, and retries / DLQ.
