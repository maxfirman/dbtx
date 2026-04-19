ALTER TABLE environment_reconcile_preparations
ADD COLUMN IF NOT EXISTS input_fingerprint TEXT;

CREATE INDEX IF NOT EXISTS idx_environment_reconcile_preparations_input
ON environment_reconcile_preparations(project_id, environment_id, kind, input_fingerprint);

ALTER TABLE environment_run_plans
ADD COLUMN IF NOT EXISTS input_fingerprint TEXT;

CREATE INDEX IF NOT EXISTS idx_environment_run_plans_input
ON environment_run_plans(project_id, environment_id, reason, input_fingerprint, created_at DESC);
