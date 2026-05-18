-- Remove the projects.mode column and related constraints.
-- All projects now require git_repo_url and project_root.

ALTER TABLE projects
DROP CONSTRAINT IF EXISTS chk_projects_mode,
DROP CONSTRAINT IF EXISTS chk_projects_remote_metadata;

ALTER TABLE projects
DROP COLUMN IF EXISTS mode;

-- Backfill any NULL values before adding NOT NULL constraints.
UPDATE projects SET git_repo_url = '' WHERE git_repo_url IS NULL;
UPDATE projects SET project_root = '.' WHERE project_root IS NULL;
UPDATE projects SET default_branch = 'main' WHERE default_branch IS NULL;

ALTER TABLE projects
ALTER COLUMN git_repo_url SET NOT NULL,
ALTER COLUMN project_root SET NOT NULL,
ALTER COLUMN default_branch SET NOT NULL;
