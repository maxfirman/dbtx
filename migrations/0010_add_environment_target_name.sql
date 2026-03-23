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
