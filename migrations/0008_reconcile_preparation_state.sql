CREATE TABLE IF NOT EXISTS environment_reconcile_preparations (
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    kind TEXT NOT NULL,
    target_git_commit_sha TEXT,
    status TEXT NOT NULL,
    invocation_id UUID REFERENCES invocations(invocation_id) ON DELETE SET NULL,
    error TEXT,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, environment_id, kind),
    CONSTRAINT chk_environment_reconcile_preparations_kind
        CHECK (kind IN ('target_manifest')),
    CONSTRAINT chk_environment_reconcile_preparations_status
        CHECK (status IN ('running', 'succeeded', 'failed')),
    CONSTRAINT chk_environment_reconcile_preparations_target_commit
        CHECK (target_git_commit_sha IS NULL OR btrim(target_git_commit_sha) <> '')
);

CREATE INDEX IF NOT EXISTS idx_environment_reconcile_preparations_status
ON environment_reconcile_preparations(status, updated_at DESC);
