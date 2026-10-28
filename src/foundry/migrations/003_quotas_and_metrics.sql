-- Part 3: per-user limits and latency metrics.

-- When the job last became eligible to run. Queue-wait latency = claim time - queued_at.
-- (created_at is wrong for retries and replays; updated_at changes on every write.)
ALTER TABLE jobs ADD COLUMN queued_at timestamptz NOT NULL DEFAULT now();
UPDATE jobs SET queued_at = created_at;

-- Fast "how many active jobs does this user have?" for the concurrency limit.
CREATE INDEX jobs_active_by_user_idx ON jobs (user_id)
    WHERE status IN ('queued', 'running', 'retry_scheduled');
