CREATE TABLE IF NOT EXISTS invocation_selected_resources (
    invocation_id UUID NOT NULL REFERENCES invocations(invocation_id) ON DELETE CASCADE,
    run_id UUID REFERENCES runs(run_id) ON DELETE CASCADE,
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    unique_id TEXT NOT NULL,
    resource_type TEXT,
    selected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    node_started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    close_reason TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (invocation_id, unique_id),
    CONSTRAINT chk_invocation_selected_resources_unique_id CHECK (btrim(unique_id) <> ''),
    CONSTRAINT chk_invocation_selected_resources_close_reason CHECK (
        close_reason IS NULL
        OR close_reason IN (
            'completed',
            'invocation_succeeded',
            'invocation_failed',
            'invocation_canceled'
        )
    )
);

CREATE INDEX IF NOT EXISTS idx_invocation_selected_resources_open_unique_id
ON invocation_selected_resources(project_id, environment_id, unique_id)
WHERE finished_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_invocation_selected_resources_open_invocation
ON invocation_selected_resources(invocation_id)
WHERE finished_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_invocation_selected_resources_history
ON invocation_selected_resources(unique_id, selected_at DESC);
