CREATE TABLE IF NOT EXISTS environment_actual_state (
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    last_attempted_run_id UUID REFERENCES runs(run_id) ON DELETE SET NULL,
    last_attempted_commit_sha TEXT,
    last_attempted_at TIMESTAMPTZ,
    last_successful_run_id UUID REFERENCES runs(run_id) ON DELETE SET NULL,
    last_successful_commit_sha TEXT,
    last_successful_at TIMESTAMPTZ,
    last_admitted_plan_id UUID,
    last_completed_plan_id UUID,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, environment_id)
);

CREATE TABLE IF NOT EXISTS source_state_events (
    id BIGSERIAL PRIMARY KEY,
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT REFERENCES environments(id) ON DELETE CASCADE,
    source_key TEXT NOT NULL,
    provider TEXT NOT NULL,
    state_version TEXT,
    payload JSONB NOT NULL DEFAULT '{}'::jsonb,
    observed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_source_state_events_source_key CHECK (btrim(source_key) <> ''),
    CONSTRAINT chk_source_state_events_provider CHECK (btrim(provider) <> '')
);

CREATE INDEX IF NOT EXISTS idx_source_state_events_scope_observed
ON source_state_events(project_id, environment_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS environment_run_plans (
    plan_id UUID PRIMARY KEY,
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    status TEXT NOT NULL,
    reason TEXT NOT NULL,
    target_git_branch TEXT,
    target_git_commit_sha TEXT,
    baseline_run_id UUID REFERENCES runs(run_id) ON DELETE SET NULL,
    selection_spec TEXT,
    selected_resources JSONB NOT NULL DEFAULT '[]'::jsonb,
    resource_count INTEGER NOT NULL DEFAULT 0,
    blocked_by_invocation_id UUID REFERENCES invocations(invocation_id) ON DELETE SET NULL,
    admitted_invocation_id UUID REFERENCES invocations(invocation_id) ON DELETE SET NULL,
    source_event_id BIGINT REFERENCES source_state_events(id) ON DELETE SET NULL,
    error TEXT,
    admitted_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    CONSTRAINT chk_environment_run_plans_status CHECK (
        status IN ('planned', 'blocked', 'admitted', 'superseded', 'canceled', 'completed', 'failed')
    ),
    CONSTRAINT chk_environment_run_plans_reason CHECK (
        reason IN ('code_change', 'source_state_change', 'manual_retry', 'manual_release')
    )
);

CREATE INDEX IF NOT EXISTS idx_environment_run_plans_scope_created
ON environment_run_plans(project_id, environment_id, created_at DESC);

ALTER TABLE invocations
ADD COLUMN IF NOT EXISTS plan_id UUID REFERENCES environment_run_plans(plan_id) ON DELETE SET NULL;

ALTER TABLE environment_actual_state
DROP CONSTRAINT IF EXISTS environment_actual_state_last_admitted_plan_id_fkey,
DROP CONSTRAINT IF EXISTS environment_actual_state_last_completed_plan_id_fkey;

ALTER TABLE environment_actual_state
ADD CONSTRAINT environment_actual_state_last_admitted_plan_id_fkey
FOREIGN KEY (last_admitted_plan_id) REFERENCES environment_run_plans(plan_id) ON DELETE SET NULL,
ADD CONSTRAINT environment_actual_state_last_completed_plan_id_fkey
FOREIGN KEY (last_completed_plan_id) REFERENCES environment_run_plans(plan_id) ON DELETE SET NULL;
