CREATE TABLE IF NOT EXISTS projects (
    id BIGSERIAL PRIMARY KEY,
    slug TEXT NOT NULL UNIQUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS environments (
    id BIGSERIAL PRIMARY KEY,
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    slug TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(project_id, slug)
);

CREATE TABLE IF NOT EXISTS runs (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL UNIQUE,
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    dbt_invocation_id UUID,
    command TEXT NOT NULL,
    args JSONB NOT NULL,
    is_full_graph_run BOOLEAN NOT NULL DEFAULT FALSE,
    dbt_version TEXT,
    started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at TIMESTAMPTZ,
    exit_code INTEGER,
    terminal_status TEXT
);

CREATE TABLE IF NOT EXISTS run_events (
    id BIGSERIAL PRIMARY KEY,
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    sequence_no BIGINT NOT NULL,
    event_name TEXT,
    event_code TEXT,
    unique_id TEXT,
    payload JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(run_id, sequence_no)
);

CREATE TABLE IF NOT EXISTS node_executions (
    run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    unique_id TEXT NOT NULL,
    resource_type TEXT,
    node_name TEXT,
    node_path TEXT,
    materialized TEXT,
    status TEXT,
    relation_database TEXT,
    relation_schema TEXT,
    relation_alias TEXT,
    relation_name TEXT,
    checksum TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    execution_time_seconds DOUBLE PRECISION,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, unique_id)
);

CREATE TABLE IF NOT EXISTS manifest_snapshots (
    run_id UUID PRIMARY KEY REFERENCES runs(run_id) ON DELETE CASCADE,
    manifest JSONB NOT NULL,
    manifest_size_bytes BIGINT NOT NULL,
    checksum TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS manifest_nodes (
    run_id UUID NOT NULL REFERENCES manifest_snapshots(run_id) ON DELETE CASCADE,
    unique_id TEXT NOT NULL,
    resource_type TEXT,
    name TEXT,
    package_name TEXT,
    original_file_path TEXT,
    tags JSONB NOT NULL,
    fqn JSONB NOT NULL,
    config JSONB NOT NULL,
    checksum TEXT,
    database_name TEXT,
    schema_name TEXT,
    alias TEXT,
    relation_name TEXT,
    PRIMARY KEY (run_id, unique_id)
);

CREATE TABLE IF NOT EXISTS manifest_edges (
    run_id UUID NOT NULL REFERENCES manifest_snapshots(run_id) ON DELETE CASCADE,
    parent_unique_id TEXT NOT NULL,
    child_unique_id TEXT NOT NULL,
    PRIMARY KEY (run_id, parent_unique_id, child_unique_id)
);

CREATE TABLE IF NOT EXISTS current_node_state (
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    unique_id TEXT NOT NULL,
    last_run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    status TEXT,
    resource_type TEXT,
    node_name TEXT,
    node_path TEXT,
    materialized TEXT,
    relation_database TEXT,
    relation_schema TEXT,
    relation_alias TEXT,
    relation_name TEXT,
    checksum TEXT,
    started_at TIMESTAMPTZ,
    finished_at TIMESTAMPTZ,
    execution_time_seconds DOUBLE PRECISION,
    last_success_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, environment_id, unique_id)
);

CREATE TABLE IF NOT EXISTS promoted_manifest_meta (
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    source_run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    base_manifest JSONB NOT NULL,
    promoted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, environment_id)
);

CREATE TABLE IF NOT EXISTS promoted_manifest_nodes (
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    unique_id TEXT NOT NULL,
    source_run_id UUID NOT NULL REFERENCES runs(run_id) ON DELETE CASCADE,
    checksum TEXT,
    raw_node JSONB NOT NULL,
    promoted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, environment_id, unique_id)
);

CREATE INDEX IF NOT EXISTS idx_runs_project_env ON runs(project_id, environment_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_run_events_run ON run_events(run_id, sequence_no);
CREATE INDEX IF NOT EXISTS idx_node_executions_run ON node_executions(run_id);
CREATE INDEX IF NOT EXISTS idx_promoted_manifest_nodes_scope ON promoted_manifest_nodes(project_id, environment_id);
