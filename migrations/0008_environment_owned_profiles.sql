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
