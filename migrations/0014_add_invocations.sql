CREATE TABLE IF NOT EXISTS invocations (
    id BIGSERIAL PRIMARY KEY,
    invocation_id UUID NOT NULL UNIQUE,
    run_id UUID NULL REFERENCES runs(run_id) ON DELETE SET NULL,
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    command TEXT NOT NULL,
    execution_mode TEXT NOT NULL,
    worker_queue TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'running',
    exit_code INTEGER NULL,
    error TEXT NULL,
    execution_spec JSONB NULL,
    promote_base_manifest BOOLEAN NOT NULL DEFAULT FALSE,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    claimed_at TIMESTAMPTZ NULL,
    last_heartbeat_at TIMESTAMPTZ NULL,
    completed_at TIMESTAMPTZ NULL,
    claimed_by TEXT NULL,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    CONSTRAINT chk_invocations_execution_mode CHECK (execution_mode IN ('server', 'local')),
    CONSTRAINT chk_invocations_status CHECK (status IN ('running', 'succeeded', 'failed', 'canceled')),
    CONSTRAINT chk_invocations_worker_queue CHECK (btrim(worker_queue) <> '')
);

CREATE INDEX IF NOT EXISTS idx_invocations_claim
    ON invocations (status, execution_mode, worker_queue, started_at, invocation_id);

CREATE INDEX IF NOT EXISTS idx_invocations_run_id
    ON invocations (run_id);
