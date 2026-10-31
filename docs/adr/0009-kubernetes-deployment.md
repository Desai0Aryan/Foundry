# ADR 0009: Kubernetes deployment model

**Status:** accepted (Part 5)

## Decision
- Kustomize base + `dev` (in-cluster throwaway deps) and `prod` (managed deps) overlays.
- Workers are disposable: `maxUnavailable: 1`, PDB, 60 s grace period (> the 40 s drain grace),
  scale-down limited to 2 pods/min. SIGKILL is also fine; leases recover the jobs.
- Relay and reaper run 2 replicas each for availability; `SKIP LOCKED` makes that safe.
- API readiness checks PostgreSQL; rolling updates use `maxUnavailable: 0` and a `preStop` sleep.
- Pods run non-root, read-only root FS, no capabilities, seccomp `RuntimeDefault`; namespace
  enforces the `restricted` Pod Security level; NetworkPolicy is default-deny.
- Autoscaling by CPU out of the box; `extras/` has a KEDA ScaledObject that scales on backlog
  per worker slot (a better signal for IO-bound jobs). DB-derived gauges are exported by every
  API replica, so queries aggregate with `max()`, not `sum()`.

## Verification
`scripts/validate_k8s.py` renders every overlay with kustomize and validates each resource
against the Kubernetes 1.30 schema (strict). It has **not** been applied to a live cluster.
