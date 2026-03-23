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
CHECK (reason IN ('created', 'updated', 'seeded')),
ADD CONSTRAINT chk_environment_versions_kind
CHECK (kind IN ('persistent', 'ephemeral', 'commit'));
