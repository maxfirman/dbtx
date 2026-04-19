CREATE TABLE IF NOT EXISTS environment_source_state_status (
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    source_key TEXT NOT NULL,
    latest_satisfied_event_id BIGINT NOT NULL REFERENCES source_state_events(id) ON DELETE CASCADE,
    latest_satisfied_state_version TEXT,
    latest_satisfied_observed_at TIMESTAMPTZ NOT NULL,
    last_satisfied_run_id UUID REFERENCES runs(run_id) ON DELETE SET NULL,
    last_satisfied_plan_id UUID REFERENCES environment_run_plans(plan_id) ON DELETE SET NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, environment_id, source_key),
    CONSTRAINT chk_environment_source_state_status_source_key CHECK (btrim(source_key) <> '')
);

CREATE INDEX IF NOT EXISTS idx_environment_source_state_status_scope
ON environment_source_state_status(project_id, environment_id, source_key);
