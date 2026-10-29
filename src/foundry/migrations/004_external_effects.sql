-- Part 4: a stand-in for "the outside world" (a payment API, an email provider, an object store).
--
-- Real external systems cannot take part in our database transactions, so exactly-once is
-- impossible; the production pattern is at-least-once delivery + an idempotent receiver. This
-- table plays the receiver: it dedups by Idempotency-Key (like Stripe) and records how many
-- times each effect was *delivered*, which is what lets the audit prove that retries and
-- crashes were absorbed instead of duplicated.
--
-- Deliberately has NO foreign key to jobs: it is independent of our system's state.
CREATE TABLE external_effects (
    idempotency_key text PRIMARY KEY,
    job_id          uuid        NOT NULL,
    effect_key      text        NOT NULL,
    first_attempt   integer     NOT NULL,
    first_worker    text        NOT NULL,
    deliveries      integer     NOT NULL DEFAULT 1,
    first_at        timestamptz NOT NULL DEFAULT now(),
    last_at         timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX external_effects_job_idx ON external_effects (job_id);
