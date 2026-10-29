# Performance & resilience results

All numbers below were measured on a **2-vCPU sandbox** that ran *everything* on one box:
PostgreSQL, Redis, RabbitMQ, the API, the relay, the reaper, 3 workers **and the load generator**.
They are a floor, not a benchmark of the design; treat them as "it behaves sanely under
contention", and re-run on your own hardware with `make loadtest` / `make chaos`.

## Method

`foundry loadtest` is **open-loop**: job *i* is sent at `t0 + i/rate` regardless of whether
earlier requests finished, and latency is measured from that *intended* send time. A
closed-loop tool slows down when the system slows down and hides exactly the latency you want
to see (coordinated omission). Every submission carries an `Idempotency-Key` and is retried
with backoff, so a retry can never create a second job.

## Throughput ramp (jobs of ~20 ms of work, 3 workers, 15 s each)

| offered rate | acknowledged | submit p95 | end-to-end p95 | completed | verdict |
|---:|---:|---:|---:|---:|---|
| 50/s  | 750/750   | 13 ms  | 40 ms  | 50.0/s | idle (7% worker use) |
| 100/s | 1500/1500 | 63 ms  | 80 ms  | 99.9/s | healthy |
| 200/s | 3000/3000 | 34 s   | 300 ms | 62.8/s | **saturated**: the API (single process, sharing 2 vCPUs with the generator) tops out near 65 submissions/s; the open-loop generator reports the queueing honestly instead of hiding it |

Workers were never the bottleneck (8-18% slot use). Scale the API with more replicas
(`docker compose up --scale api=N`, or the HPA in Part 5) and workers with `--scale worker=N`.
After every run `foundry audit` reported **all invariants held**.

## Fault injection (`foundry chaos <scenario>`)

Each scenario runs a load test against a fleet of real OS processes, injects the fault, waits
for the queue to drain, then audits the database.

| scenario | fault | result |
|---|---|---|
| `worker-crash` | `SIGKILL` a worker holding jobs | 200/200 succeeded, 2 jobs ran a second attempt, 0 duplicate effects |
| `worker-freeze` | `SIGSTOP` for 2.5x the lease, then `SIGCONT` (a zombie) | 200/200 succeeded; the zombie finished work it no longer owned: **fencing rejected its writes**, the idempotent receiver absorbed 1 duplicate delivery, 1 effect per job |
| `rolling-restart` | `SIGTERM` every worker in turn | 200/200 succeeded, **every job on attempt 1** (graceful release burns no attempts) |
| `kitchen-sink` | worker kill + zombie freeze + relay/reaper kill + **RabbitMQ outage**, overlapping, 50 s | 400/400 succeeded (394 on attempt 1, 5 on 2, 1 on 3), 3 duplicate deliveries absorbed, 0 duplicate effects, all invariants held |

### Bugs the chaos harness found (and the fixes)

1. **Relay parked work forever during a broker outage.** The poison-message cap
   (`attempts >= 10`) counted *connection* failures, so ten failed publishes while RabbitMQ was
   down permanently stranded 9 jobs. Now only message-specific errors (unroutable,
   unserialisable) count; everything else is treated as infrastructure and retried. The rule is
   an allow-list because aio-pika, aiormq and the stdlib each have their own exception trees.
2. Heartbeat/settle race (Part 3) and a missing `running -> queued` state-machine edge
   (found by the auditor) are covered in the ADRs.

## What "no duplicate side effects" means here

Delivery is **at-least-once** (a zombie or a crash after the effect can repeat work), and the
external effect is an **idempotent receiver** keyed by `job_id:effect_key`. The audit proves,
from SQL alone, that every succeeded job has exactly one effect row and that
`deliveries <= attempts`; the "duplicate deliveries absorbed" figure is the number of times the
receiver deduplicated a repeat.
