CREATE TABLE IF NOT EXISTS projects (
    id BIGSERIAL PRIMARY KEY,
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

ALTER TABLE projects
ADD COLUMN IF NOT EXISTS git_repo_url TEXT,
ADD COLUMN IF NOT EXISTS default_branch TEXT,
ADD COLUMN IF NOT EXISTS project_root TEXT,
ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE environments
ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'persistent',
ADD COLUMN IF NOT EXISTS baseline_environment_id BIGINT NULL REFERENCES environments(id) ON DELETE SET NULL,
ADD COLUMN IF NOT EXISTS git_ref TEXT,
ADD COLUMN IF NOT EXISTS pr_number INTEGER,
ADD COLUMN IF NOT EXISTS protected BOOLEAN NOT NULL DEFAULT FALSE,
ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'active',
ADD COLUMN IF NOT EXISTS schema_prefix TEXT,
ADD COLUMN IF NOT EXISTS metadata JSONB NOT NULL DEFAULT '{}'::jsonb;

CREATE TABLE IF NOT EXISTS environment_seeds (
    id BIGSERIAL PRIMARY KEY,
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    target_environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    source_environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE RESTRICT,
    seed_type TEXT NOT NULL,
    source_run_id UUID NULL REFERENCES runs(run_id) ON DELETE SET NULL,
    seeded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_environment_seeds_target
ON environment_seeds(target_environment_id, id DESC);

ALTER TABLE projects
ADD COLUMN IF NOT EXISTS project_id TEXT,
ADD COLUMN IF NOT EXISTS project_name TEXT;

UPDATE projects
SET project_id = COALESCE(project_id, 'prj_' || lpad(id::text, 8, '0')),
    project_name = COALESCE(project_name, project_id);

ALTER TABLE projects
ALTER COLUMN project_id SET NOT NULL;

ALTER TABLE projects
ALTER COLUMN project_name SET NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_projects_project_id
ON projects(project_id);

ALTER TABLE environments
ADD COLUMN IF NOT EXISTS git_branch TEXT,
ADD COLUMN IF NOT EXISTS git_commit_sha TEXT,
ADD COLUMN IF NOT EXISTS immutable BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE runs
ADD COLUMN IF NOT EXISTS git_branch TEXT,
ADD COLUMN IF NOT EXISTS git_commit_sha TEXT,
ADD COLUMN IF NOT EXISTS git_repo_url TEXT,
ADD COLUMN IF NOT EXISTS project_root TEXT,
ADD COLUMN IF NOT EXISTS project_name TEXT,
ADD COLUMN IF NOT EXISTS project_ref TEXT;

CREATE TABLE IF NOT EXISTS environment_versions (
    id BIGSERIAL PRIMARY KEY,
    environment_id BIGINT NOT NULL REFERENCES environments(id) ON DELETE CASCADE,
    project_id BIGINT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    recorded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    reason TEXT NOT NULL,
    git_branch TEXT,
    git_commit_sha TEXT,
    kind TEXT NOT NULL,
    immutable BOOLEAN NOT NULL,
    baseline_environment_id BIGINT NULL REFERENCES environments(id) ON DELETE SET NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb
);

CREATE INDEX IF NOT EXISTS idx_environment_versions_environment
ON environment_versions(environment_id, id DESC);

ALTER TABLE environments
DROP COLUMN IF EXISTS git_ref,
DROP COLUMN IF EXISTS protected;

ALTER TABLE environments
DROP CONSTRAINT IF EXISTS chk_environments_kind,
DROP CONSTRAINT IF EXISTS chk_environments_status,
DROP CONSTRAINT IF EXISTS chk_environments_commit_sha;

ALTER TABLE environments
ADD CONSTRAINT chk_environments_kind
CHECK (kind IN ('persistent', 'ephemeral', 'commit')),
ADD CONSTRAINT chk_environments_status
CHECK (status IN ('active', 'archived', 'failed', 'deleting')),
ADD CONSTRAINT chk_environments_commit_sha
CHECK (kind <> 'commit' OR git_commit_sha IS NOT NULL);

ALTER TABLE environment_versions
DROP CONSTRAINT IF EXISTS chk_environment_versions_reason,
DROP CONSTRAINT IF EXISTS chk_environment_versions_kind;

ALTER TABLE environment_versions
ADD CONSTRAINT chk_environment_versions_reason
CHECK (reason IN ('created', 'updated', 'seeded', 'released', 'rolled_back')),
ADD CONSTRAINT chk_environment_versions_kind
CHECK (kind IN ('persistent', 'ephemeral', 'commit'));

ALTER TABLE projects
DROP COLUMN IF EXISTS slug;

ALTER TABLE projects
ADD COLUMN IF NOT EXISTS adapter_type TEXT,
ADD COLUMN IF NOT EXISTS default_profile JSONB NOT NULL DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS default_profile_secrets JSONB NOT NULL DEFAULT '{}'::jsonb;

ALTER TABLE environments
ADD COLUMN IF NOT EXISTS schema_name TEXT,
ADD COLUMN IF NOT EXISTS profile_overrides JSONB NOT NULL DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS profile_override_secrets JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE environments
SET schema_name = COALESCE(schema_name, schema_prefix, 'main');

ALTER TABLE environments
ALTER COLUMN schema_name SET NOT NULL;

ALTER TABLE environments
ADD COLUMN IF NOT EXISTS adapter_type TEXT,
ADD COLUMN IF NOT EXISTS threads INTEGER,
ADD COLUMN IF NOT EXISTS profile_config JSONB NOT NULL DEFAULT '{}'::jsonb,
ADD COLUMN IF NOT EXISTS profile_secrets JSONB NOT NULL DEFAULT '{}'::jsonb;

UPDATE environments e
SET
    adapter_type = COALESCE(e.adapter_type, p.adapter_type, 'duckdb'),
    schema_name = COALESCE(
        e.schema_name,
        e.schema_prefix,
        p.default_profile ->> 'schema',
        'main'
    ),
    threads = COALESCE(
        e.threads,
        NULLIF(p.default_profile ->> 'threads', '')::INTEGER
    ),
    profile_config = CASE
        WHEN e.profile_config = '{}'::jsonb THEN
            COALESCE((p.default_profile - 'schema' - 'threads'), '{}'::jsonb)
        ELSE e.profile_config
    END,
    profile_secrets = CASE
        WHEN e.profile_secrets = '{}'::jsonb THEN
            COALESCE(p.default_profile_secrets, '{}'::jsonb)
        ELSE e.profile_secrets
    END
FROM projects p
WHERE p.id = e.project_id;

ALTER TABLE environments
ALTER COLUMN adapter_type SET NOT NULL,
ALTER COLUMN schema_name SET NOT NULL;

ALTER TABLE environments
DROP COLUMN IF EXISTS schema_prefix;

ALTER TABLE projects
DROP COLUMN IF EXISTS adapter_type,
DROP COLUMN IF EXISTS default_profile,
DROP COLUMN IF EXISTS default_profile_secrets;

ALTER TABLE environments
DROP COLUMN IF EXISTS profile_overrides,
DROP COLUMN IF EXISTS profile_override_secrets;

ALTER TABLE environments
ADD COLUMN IF NOT EXISTS target_name TEXT;

UPDATE environments
SET target_name = COALESCE(target_name, slug)
WHERE target_name IS NULL;

ALTER TABLE environments
ALTER COLUMN target_name SET NOT NULL;

ALTER TABLE environments
DROP CONSTRAINT IF EXISTS chk_environments_target_name;

ALTER TABLE environments
ADD CONSTRAINT chk_environments_target_name
CHECK (btrim(target_name) <> '');

ALTER TABLE runs
DROP COLUMN IF EXISTS dbt_invocation_id;

ALTER TABLE runs
ADD COLUMN IF NOT EXISTS execution_mode TEXT NOT NULL DEFAULT 'server';

ALTER TABLE environments
ADD COLUMN IF NOT EXISTS worker_queue TEXT;

UPDATE environments
SET worker_queue = 'generic'
WHERE worker_queue IS NULL OR btrim(worker_queue) = '';

ALTER TABLE environments
ALTER COLUMN worker_queue SET NOT NULL;

ALTER TABLE environments
DROP CONSTRAINT IF EXISTS chk_environments_worker_queue;

ALTER TABLE environments
ADD CONSTRAINT chk_environments_worker_queue
CHECK (btrim(worker_queue) <> '');

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

ALTER TABLE invocations
ADD COLUMN IF NOT EXISTS claim_deadline_at TIMESTAMPTZ NULL;

CREATE INDEX IF NOT EXISTS idx_invocations_claim_deadline
    ON invocations (claim_deadline_at)
    WHERE status = 'running' AND claimed_by IS NULL;

ALTER TABLE invocations
ADD COLUMN IF NOT EXISTS next_event_sequence BIGINT NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS invocation_events (
    id BIGSERIAL PRIMARY KEY,
    invocation_id UUID NOT NULL REFERENCES invocations(invocation_id) ON DELETE CASCADE,
    sequence_no BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    event_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    UNIQUE (invocation_id, sequence_no)
);

CREATE INDEX IF NOT EXISTS idx_invocation_events_invocation_sequence
    ON invocation_events (invocation_id, sequence_no);

ALTER TABLE invocations
ADD COLUMN lease_token UUID;

ALTER TABLE invocations
ADD CONSTRAINT invocations_claim_lease_consistency CHECK (
    (
        status = 'running'
        AND (
            (claimed_by IS NULL AND lease_token IS NULL)
            OR (claimed_by IS NOT NULL AND lease_token IS NOT NULL)
        )
    )
    OR (
        status <> 'running'
        AND lease_token IS NULL
    )
);

ALTER TABLE invocations
ADD COLUMN cancel_requested_at TIMESTAMPTZ;

ALTER TABLE environments
ADD COLUMN IF NOT EXISTS profile_name TEXT;

UPDATE environments e
SET profile_name = COALESCE(e.profile_name, p.project_name)
FROM projects p
WHERE p.id = e.project_id
  AND e.profile_name IS NULL;

ALTER TABLE environments
ALTER COLUMN profile_name SET NOT NULL;

ALTER TABLE environments
DROP CONSTRAINT IF EXISTS chk_environments_profile_name;

ALTER TABLE environments
ADD CONSTRAINT chk_environments_profile_name
CHECK (btrim(profile_name) <> '');

ALTER TABLE projects
ADD COLUMN IF NOT EXISTS mode TEXT NOT NULL DEFAULT 'local';

UPDATE projects p
SET mode = 'remote'
WHERE EXISTS (
    SELECT 1
    FROM environments e
    WHERE e.project_id = p.id
      AND e.git_commit_sha IS NOT NULL
);

ALTER TABLE projects
DROP CONSTRAINT IF EXISTS chk_projects_mode;

ALTER TABLE projects
ADD CONSTRAINT chk_projects_mode
CHECK (mode IN ('local', 'remote')),
ADD CONSTRAINT chk_projects_remote_metadata
CHECK (mode <> 'remote' OR (git_repo_url IS NOT NULL AND project_root IS NOT NULL));

ALTER TABLE environments
DROP CONSTRAINT IF EXISTS chk_environments_kind,
DROP CONSTRAINT IF EXISTS chk_environments_commit_sha,
DROP COLUMN IF EXISTS kind,
DROP COLUMN IF EXISTS immutable;

ALTER TABLE environment_versions
DROP CONSTRAINT IF EXISTS chk_environment_versions_kind,
DROP COLUMN IF EXISTS kind,
DROP COLUMN IF EXISTS immutable;
