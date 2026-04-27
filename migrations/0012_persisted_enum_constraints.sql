ALTER TABLE environments
DROP CONSTRAINT IF EXISTS chk_environments_status;

ALTER TABLE environments
ADD CONSTRAINT chk_environments_status
CHECK (status IN ('active', 'archived', 'failed', 'deleting'));

ALTER TABLE invocations
DROP CONSTRAINT IF EXISTS chk_invocations_execution_mode,
DROP CONSTRAINT IF EXISTS chk_invocations_status;

ALTER TABLE invocations
ADD CONSTRAINT chk_invocations_execution_mode
CHECK (execution_mode IN ('server', 'local')),
ADD CONSTRAINT chk_invocations_status
CHECK (status IN ('running', 'succeeded', 'failed', 'canceled'));

ALTER TABLE workers
DROP CONSTRAINT IF EXISTS chk_workers_execution_mode;

ALTER TABLE workers
ADD CONSTRAINT chk_workers_execution_mode
CHECK (execution_mode IN ('server', 'local'));

ALTER TABLE project_onboarding_drafts
DROP CONSTRAINT IF EXISTS chk_project_onboarding_drafts_status;

ALTER TABLE project_onboarding_drafts
ADD CONSTRAINT chk_project_onboarding_drafts_status
CHECK (status IN ('draft', 'loading_git', 'ready', 'validating', 'validated', 'failed'));

ALTER TABLE environment_onboarding_drafts
DROP CONSTRAINT IF EXISTS chk_environment_onboarding_drafts_status;

ALTER TABLE environment_onboarding_drafts
ADD CONSTRAINT chk_environment_onboarding_drafts_status
CHECK (status IN ('draft', 'loading_git', 'ready', 'validating', 'validated', 'failed'));

ALTER TABLE environment_run_plans
DROP CONSTRAINT IF EXISTS chk_environment_run_plans_status;

ALTER TABLE environment_run_plans
ADD CONSTRAINT chk_environment_run_plans_status
CHECK (status IN ('planned', 'blocked', 'admitted', 'completed', 'failed', 'canceled', 'superseded'));

ALTER TABLE environment_reconcile_preparations
DROP CONSTRAINT IF EXISTS chk_environment_reconcile_preparations_status;

ALTER TABLE environment_reconcile_preparations
ADD CONSTRAINT chk_environment_reconcile_preparations_status
CHECK (status IN ('running', 'succeeded', 'failed'));
