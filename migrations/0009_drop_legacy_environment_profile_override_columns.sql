ALTER TABLE environments
DROP COLUMN IF EXISTS profile_overrides,
DROP COLUMN IF EXISTS profile_override_secrets;
