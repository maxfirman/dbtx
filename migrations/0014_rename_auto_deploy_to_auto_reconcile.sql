-- Rename auto_deploy to auto_reconcile

ALTER TABLE environments RENAME COLUMN auto_deploy TO auto_reconcile;
ALTER TABLE environment_onboarding_drafts RENAME COLUMN auto_deploy TO auto_reconcile;
ALTER TABLE environment_versions RENAME COLUMN auto_deploy TO auto_reconcile;
