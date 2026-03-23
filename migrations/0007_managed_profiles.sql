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
