ALTER TABLE environment_run_plans
ADD COLUMN IF NOT EXISTS superseded_by_plan_id UUID REFERENCES environment_run_plans(plan_id) ON DELETE SET NULL,
ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0,
ADD COLUMN IF NOT EXISTS first_blocked_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_blocked_at TIMESTAMPTZ,
ADD COLUMN IF NOT EXISTS last_checked_at TIMESTAMPTZ;

CREATE INDEX IF NOT EXISTS idx_environment_run_plans_live_scope
ON environment_run_plans(project_id, environment_id, status, created_at DESC);

CREATE TABLE IF NOT EXISTS environment_reconcile_leases (
    environment_id BIGINT PRIMARY KEY REFERENCES environments(id) ON DELETE CASCADE,
    owner TEXT NOT NULL,
    leased_until TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_environment_reconcile_leases_owner CHECK (btrim(owner) <> '')
);
