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
