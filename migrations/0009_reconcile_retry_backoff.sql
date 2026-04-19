ALTER TABLE environment_reconcile_preparations
ADD COLUMN IF NOT EXISTS failure_count INTEGER NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_environment_reconcile_preparations_retry
ON environment_reconcile_preparations(status, next_attempt_at);

ALTER TABLE environment_run_plans
ADD COLUMN IF NOT EXISTS failure_count INTEGER NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_environment_run_plans_retry
ON environment_run_plans(project_id, environment_id, status, next_attempt_at);
