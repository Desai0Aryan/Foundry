-- Part 2: worker registry, outbox hardening, effect-suppression accounting.

CREATE TABLE workers (
    id                text PRIMARY KEY,
    host              text        NOT NULL,
    pid               integer     NOT NULL,
    started_at        timestamptz NOT NULL DEFAULT now(),
    last_heartbeat_at timestamptz NOT NULL DEFAULT now(),
    concurrency       integer     NOT NULL,
    active_jobs       integer     NOT NULL DEFAULT 0,
    processed_total   bigint      NOT NULL DEFAULT 0,
    status            text        NOT NULL DEFAULT 'running'
                      CHECK (status IN ('running', 'draining', 'stopped'))
);
CREATE INDEX workers_heartbeat_idx ON workers (last_heartbeat_at);

-- A relay that cannot publish a row (e.g. unroutable) must not spin on it forever.
ALTER TABLE outbox
    ADD COLUMN attempts   integer NOT NULL DEFAULT 0,
    ADD COLUMN last_error text;
DROP INDEX outbox_unpublished_idx;
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL AND attempts < 10;
CREATE INDEX outbox_published_idx   ON outbox (published_at) WHERE published_at IS NOT NULL;
CREATE INDEX outbox_job_idx         ON outbox (job_id);

-- Wake the relay immediately instead of waiting for its poll interval.
CREATE FUNCTION notify_outbox() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify('foundry_outbox', '');
    RETURN NULL;
END $$;

CREATE TRIGGER outbox_notify AFTER INSERT ON outbox
    FOR EACH STATEMENT EXECUTE FUNCTION notify_outbox();

-- How many times a duplicate execution tried to re-apply an effect and was suppressed.
ALTER TABLE effect_ledger ADD COLUMN suppressed integer NOT NULL DEFAULT 0;
