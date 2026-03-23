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
