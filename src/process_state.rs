//! Shared process state used by both the server and reconciler binaries.
use crate::api::{InvocationCommandApi, InvocationExecutionModeApi, InvocationExecutionSpecApi};
use crate::config::RuntimeConfig;
use crate::db::{CreateInvocationInput, Db, PreparationStatus};
use crate::error::{AppError, AppResult};
use crate::execution::{ExecutionCompletion, invocation_claim_deadline_at};
use crate::invocation_runtime::{
    InvocationManager, InvocationPersistence, InvocationRecorder, started_invocation_event,
};
use crate::reconciler::auto_admit_blocked_plans_for_environment;
use crate::services::{
    InvocationCommand, InvocationService, code_change_input_fingerprint_for_baseline,
    target_manifest_input_fingerprint,
};
use chrono::Utc;
use tokio::time::{Instant, sleep};
use tracing::info;
use uuid::Uuid;

/// Core process state shared across binaries (server, reconciler).
///
/// Holds the database handle and invocation event infrastructure.
/// The server uses this directly as its axum state; the reconciler
/// constructs one without needing any HTTP-server machinery.
#[derive(Clone)]
pub struct ProcessState {
    pub(crate) db: Db,
    #[allow(dead_code)]
    runtime_config: RuntimeConfig,
    pub(crate) invocations: InvocationManager,
}

impl ProcessState {
    pub fn new(db: Db, runtime_config: RuntimeConfig) -> Self {
        Self {
            db,
            runtime_config,
            invocations: InvocationManager::default(),
        }
    }

    pub(crate) fn db(&self) -> &Db {
        &self.db
    }

    pub(crate) async fn bootstrap_invocation_started(
        &self,
        invocation_id: Uuid,
        persistence: Option<InvocationPersistence>,
    ) -> AppResult<()> {
        let runtime = self
            .invocations
            .get_or_create(invocation_id, persistence)
            .await;
        let started_event = started_invocation_event();
        let sequence = self
            .db
            .append_invocation_event(invocation_id, &started_event)
            .await?;
        runtime.push_event(sequence, started_event).await;
        Ok(())
    }

    /// Complete an invocation and apply all post-terminal reactions:
    /// persist completion, auto-admit blocked plans, schedule cleanup.
    pub(crate) async fn complete_invocation(
        &self,
        invocation_id: Uuid,
        worker_id: &str,
        lease_token: Uuid,
        completion: ExecutionCompletion,
    ) -> AppResult<()> {
        let persistence = self
            .db
            .get_invocation_persistence(invocation_id, Some(worker_id), Some(lease_token))
            .await?;
        let runtime = self.invocations.get_or_create(invocation_id, None).await;
        let recorder = InvocationRecorder::new(self.db.clone(), invocation_id, runtime);
        recorder
            .complete(worker_id, lease_token, completion)
            .await?;
        if let (Some(project_id), Some(environment_id)) =
            (persistence.project_id, persistence.environment_id)
        {
            let admitted =
                auto_admit_blocked_plans_for_environment(self, project_id, environment_id).await?;
            if admitted > 0 {
                info!(
                    invocation_id = %invocation_id,
                    project_id,
                    environment_id,
                    admitted,
                    "auto-admitted blocked reconciliation plans"
                );
            }
        }
        self.invocations.schedule_cleanup(invocation_id);
        Ok(())
    }

    // --- Invocation startup ---

    pub(crate) async fn start_prepared_invocation(
        &self,
        invocation_id: Uuid,
        command: InvocationCommandApi,
        plan_id: Option<Uuid>,
        prepared: crate::services::LocalExecutionPrepared,
    ) -> AppResult<Uuid> {
        let project_id = prepared.project_id;
        let environment_id = prepared.environment_id;
        let project_draft_id = prepared.project_draft_id;
        let environment_draft_id = prepared.environment_draft_id;
        let worker_queue = prepared.worker_queue.clone();
        let command_service: InvocationCommand = command.into();
        let start = prepared.into_invocation_start(command);
        self.db
            .create_invocation(CreateInvocationInput {
                invocation_id,
                plan_id,
                run_id: start.persistence.as_ref().map(|p| p.run_id),
                project_id,
                environment_id,
                project_draft_id,
                environment_draft_id,
                command: command_service.as_str().to_string(),
                execution_mode: start.execution_mode,
                worker_queue,
                execution_spec: Some(start.execution_spec),
                promote_base_manifest: start
                    .persistence
                    .as_ref()
                    .map(|p| p.promote_base_manifest)
                    .unwrap_or(false),
                updates_actual_state: start
                    .persistence
                    .as_ref()
                    .map(|p| p.updates_actual_state)
                    .unwrap_or(false),
                claim_deadline_at: Some(invocation_claim_deadline_at(start.execution_mode)),
            })
            .await?;
        self.bootstrap_invocation_started(invocation_id, start.persistence)
            .await?;
        Ok(invocation_id)
    }

    pub(crate) async fn start_project_draft_validation_invocation(
        &self,
        prepared: crate::services::ProjectDraftValidationPrepared,
    ) -> AppResult<Uuid> {
        self.start_draft_invocation(
            prepared.invocation_id,
            InvocationCommand::ProjectValidate,
            prepared.worker_queue,
            InvocationExecutionSpecApi::ProjectValidation {
                repo_url: prepared.spec.repo_url,
                project_root: prepared.spec.project_root,
            },
            DraftAttachment::Project(prepared.draft.id),
        )
        .await
    }

    pub(crate) async fn start_environment_draft_prepare_invocation(
        &self,
        prepared: crate::services::EnvironmentDraftCreatePrepared,
    ) -> AppResult<Uuid> {
        self.start_draft_invocation(
            prepared.invocation_id,
            InvocationCommand::EnvironmentPrepare,
            prepared.worker_queue,
            InvocationExecutionSpecApi::EnvironmentPrepare {
                repo_url: prepared.spec.repo_url,
                selected_branch: prepared.spec.selected_branch,
            },
            DraftAttachment::Environment(prepared.draft.id),
        )
        .await
    }

    pub(crate) async fn start_environment_draft_validation_invocation(
        &self,
        prepared: crate::services::EnvironmentDraftValidationPrepared,
    ) -> AppResult<Uuid> {
        self.start_draft_invocation(
            prepared.invocation_id,
            InvocationCommand::EnvironmentValidate,
            prepared.worker_queue,
            InvocationExecutionSpecApi::EnvironmentValidate {
                repo_url: prepared.spec.repo_url,
                commit_sha: prepared.spec.commit_sha,
                project_root: prepared.spec.project_root,
                selected_branch: prepared.spec.selected_branch,
                profiles_yml: prepared.spec.profiles_yml,
            },
            DraftAttachment::Environment(prepared.draft.id),
        )
        .await
    }

    async fn start_draft_invocation(
        &self,
        invocation_id: Uuid,
        command: InvocationCommand,
        worker_queue: String,
        execution_spec: InvocationExecutionSpecApi,
        attachment: DraftAttachment,
    ) -> AppResult<Uuid> {
        let (project_draft_id, environment_draft_id) = match &attachment {
            DraftAttachment::Project(id) => (Some(*id), None),
            DraftAttachment::Environment(id) => (None, Some(*id)),
        };
        self.db
            .create_invocation(CreateInvocationInput {
                invocation_id,
                plan_id: None,
                run_id: None,
                project_id: None,
                environment_id: None,
                project_draft_id,
                environment_draft_id,
                command: command.as_str().to_string(),
                execution_mode: InvocationExecutionModeApi::Server,
                worker_queue,
                execution_spec: Some(execution_spec),
                promote_base_manifest: false,
                updates_actual_state: false,
                claim_deadline_at: Some(invocation_claim_deadline_at(
                    InvocationExecutionModeApi::Server,
                )),
            })
            .await?;
        match attachment {
            DraftAttachment::Project(draft_id) => {
                self.db
                    .attach_project_draft_invocation(draft_id, invocation_id)
                    .await?;
            }
            DraftAttachment::Environment(draft_id) => {
                self.db
                    .attach_environment_draft_invocation(draft_id, invocation_id)
                    .await?;
            }
        }
        self.bootstrap_invocation_started(invocation_id, None)
            .await?;
        Ok(invocation_id)
    }

    // --- Manifest preparation ---

    /// Ensure a target manifest exists for the environment's desired commit.
    /// Blocks until the manifest prepare invocation completes or times out.
    pub(crate) async fn ensure_target_manifest_for_reconcile(
        &self,
        project_id: &str,
        environment_slug: &str,
    ) -> AppResult<()> {
        let environment = self
            .db
            .get_environment(project_id, environment_slug)
            .await?;
        let desired_commit_sha = environment
            .git_commit_sha
            .clone()
            .ok_or(AppError::ReconciliationRequiresCommitSha)?;
        let baseline_run_id = self
            .db
            .get_environment_actual_state(&environment.project_ref, &environment.slug)
            .await?
            .last_successful_run_id;
        let input_fingerprint = target_manifest_input_fingerprint(
            &code_change_input_fingerprint_for_baseline(&desired_commit_sha, baseline_run_id),
        );
        if self
            .db
            .latest_manifest_run_id_for_commit(
                environment.project_id,
                environment.id,
                &desired_commit_sha,
            )
            .await?
            .is_some()
        {
            return Ok(());
        }
        if self
            .db
            .has_active_manifest_prepare_for_commit(
                environment.project_id,
                environment.id,
                &desired_commit_sha,
            )
            .await?
        {
            return Err(AppError::ReconciliationInProgress);
        }
        if self
            .db
            .get_environment_reconcile_preparation_by_scope(environment.project_id, environment.id)
            .await?
            .filter(|preparation| {
                preparation.kind == "target_manifest"
                    && preparation.input_fingerprint.as_deref()
                        == Some(input_fingerprint.as_str())
                    && preparation.status == PreparationStatus::Failed
                    && preparation
                        .next_attempt_at
                        .map(|next_attempt_at| next_attempt_at > Utc::now())
                        .unwrap_or(false)
            })
            .is_some()
        {
            return Err(AppError::ReconciliationInProgress);
        }

        let invocation_id = Uuid::new_v4();
        let prepared = InvocationService::new(&self.db)
            .prepare_remote_manifest_capture(invocation_id, project_id, environment_slug)
            .await?;
        self.start_prepared_invocation(
            invocation_id,
            InvocationCommandApi::ManifestPrepare,
            None,
            prepared,
        )
        .await?;
        self.db
            .mark_manifest_prepare_running(
                environment.project_id,
                environment.id,
                &input_fingerprint,
                &desired_commit_sha,
                invocation_id,
            )
            .await?;
        wait_for_terminal_invocation(self, invocation_id, std::time::Duration::from_secs(120))
            .await?;
        let status = self.db.get_invocation_status(invocation_id).await?;
        match status.status {
            crate::api::InvocationLifecycleStatus::Succeeded => {}
            crate::api::InvocationLifecycleStatus::Failed
            | crate::api::InvocationLifecycleStatus::Canceled => {
                return Err(AppError::Internal(status.error.unwrap_or_else(|| {
                    "manifest prepare invocation failed".to_string()
                })));
            }
            crate::api::InvocationLifecycleStatus::Running => {
                return Err(AppError::Internal(
                    "manifest prepare invocation did not reach a terminal state".to_string(),
                ));
            }
        }

        if self
            .db
            .latest_manifest_run_id_for_commit(
                environment.project_id,
                environment.id,
                &desired_commit_sha,
            )
            .await?
            .is_none()
        {
            return Err(AppError::Internal(
                "manifest prepare finished without persisting a manifest snapshot".to_string(),
            ));
        }

        Ok(())
    }
}

enum DraftAttachment {
    Project(Uuid),
    Environment(Uuid),
}

async fn wait_for_terminal_invocation(
    state: &ProcessState,
    invocation_id: Uuid,
    timeout: std::time::Duration,
) -> AppResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let status = state.db.get_invocation_status(invocation_id).await?;
        if !matches!(
            status.status,
            crate::api::InvocationLifecycleStatus::Running
        ) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(AppError::TimedOut(format!(
                "timed out waiting for invocation {invocation_id}"
            )));
        }
        sleep(std::time::Duration::from_millis(250)).await;
    }
}

impl crate::services::InvocationStarter for ProcessState {
    async fn start_prepared_invocation(
        &self,
        invocation_id: Uuid,
        command: InvocationCommandApi,
        plan_id: Option<Uuid>,
        prepared: crate::services::LocalExecutionPrepared,
    ) -> AppResult<Uuid> {
        self.start_prepared_invocation(invocation_id, command, plan_id, prepared)
            .await
    }
}
