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
