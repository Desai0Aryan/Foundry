-- Foundry schema v1: PostgreSQL is the single source of truth for job state.
-- The message broker only carries *hints* that work exists; it never owns state.

CREATE TABLE users (
    id                   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    email                text        NOT NULL UNIQUE,
    name                 text        NOT NULL,
    tier                 text        NOT NULL DEFAULT 'free'
                         CHECK (tier IN ('free', 'pro', 'enterprise')),
    max_concurrent_jobs  integer     NOT NULL CHECK (max_concurrent_jobs > 0),
    daily_job_quota      integer     NOT NULL CHECK (daily_job_quota > 0),
    rate_limit_per_sec   numeric(10,2) NOT NULL CHECK (rate_limit_per_sec > 0),
    created_at           timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE api_keys (
    id           uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id      uuid        NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    prefix       text        NOT NULL UNIQUE,      -- public lookup handle, not secret
    secret_hash  text        NOT NULL,             -- HMAC-SHA256(pepper, secret)
    label        text        NOT NULL DEFAULT 'default',
    created_at   timestamptz NOT NULL DEFAULT now(),
    last_used_at timestamptz,
    revoked_at   timestamptz
);
CREATE INDEX api_keys_user_idx ON api_keys (user_id);

CREATE TABLE jobs (
    id               uuid PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id          uuid        NOT NULL REFERENCES users (id) ON DELETE CASCADE,
    type             text        NOT NULL,
    status           text        NOT NULL DEFAULT 'queued'
                     CHECK (status IN ('queued', 'running', 'retry_scheduled',
                                       'succeeded', 'dead', 'cancelled')),
    priority         smallint    NOT NULL DEFAULT 5 CHECK (priority BETWEEN 0 AND 9),
    payload          jsonb       NOT NULL,
    payload_hash     char(64)    NOT NULL,         -- sha256(type + canonical payload)
    request_hash     char(64)    NOT NULL,         -- fingerprint for idempotency checks
    idempotency_key  text,
    dedup_key        text,
    attempts         integer     NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    max_attempts     integer     NOT NULL CHECK (max_attempts BETWEEN 1 AND 20),
    timeout_seconds  integer     NOT NULL CHECK (timeout_seconds BETWEEN 1 AND 3600),
    result           jsonb,
    error            jsonb,
    cancel_requested boolean     NOT NULL DEFAULT false,
    lease_owner      text,
    lease_expires_at timestamptz,
    next_run_at      timestamptz,
    created_at       timestamptz NOT NULL DEFAULT now(),
    updated_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,
    finished_at      timestamptz
);

-- Idempotency: one logical request per (user, key), enforced by the database.
CREATE UNIQUE INDEX uq_jobs_idempotency
    ON jobs (user_id, idempotency_key) WHERE idempotency_key IS NOT NULL;

-- Deduplication: at most one *active* job per (user, dedup_key).
-- Once a job reaches a terminal state the slot frees up automatically.
CREATE UNIQUE INDEX uq_jobs_dedup_active
    ON jobs (user_id, dedup_key)
    WHERE dedup_key IS NOT NULL AND status IN ('queued', 'running', 'retry_scheduled');

CREATE INDEX jobs_user_created_idx ON jobs (user_id, created_at DESC, id DESC);
CREATE INDEX jobs_user_status_idx  ON jobs (user_id, status);
CREATE INDEX jobs_due_idx          ON jobs (next_run_at) WHERE status = 'retry_scheduled';
CREATE INDEX jobs_lease_idx        ON jobs (lease_expires_at) WHERE status = 'running';

CREATE FUNCTION touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END $$;

CREATE TRIGGER jobs_touch_updated_at BEFORE UPDATE ON jobs
    FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- Append-only audit trail of every state transition.
CREATE TABLE job_events (
    id          bigserial PRIMARY KEY,
    job_id      uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    at          timestamptz NOT NULL DEFAULT now(),
    from_status text,
    to_status   text        NOT NULL,
    detail      jsonb       NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX job_events_job_idx ON job_events (job_id, id);

-- Transactional outbox: the job row and its "please run me" message commit atomically.
-- A relay (Part 2) publishes unpublished rows to the broker with at-least-once delivery.
CREATE TABLE outbox (
    id           bigserial PRIMARY KEY,
    job_id       uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    routing_key  text        NOT NULL,
    payload      jsonb       NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT now(),
    published_at timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- Effect ledger: workers record external side effects here with a unique key so a
-- retried or re-delivered job can never apply the same effect twice (Part 2 / Part 4).
CREATE TABLE effect_ledger (
    job_id     uuid        NOT NULL REFERENCES jobs (id) ON DELETE CASCADE,
    effect_key text        NOT NULL,
    attempt    integer     NOT NULL,
    worker_id  text        NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (job_id, effect_key)
);
