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
