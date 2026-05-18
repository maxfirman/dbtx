//! Invocation lifecycle: start, record, complete, and post-terminal reactions.
//!
//! Concentrates all invocation state transitions (queued → running → terminal → side effects)
//! behind a single module. Callers get "start this invocation" or "complete this invocation
//! and handle all consequences" without knowing about InvocationRecorder, InvocationManager,
//! or the completion dispatch table.

use crate::api::{InvocationCommandApi, InvocationExecutionModeApi, InvocationExecutionSpecApi};
use crate::db::{CreateInvocationInput, Db};
use crate::error::AppResult;
use crate::execution::{ExecutionCompletion, invocation_claim_deadline_at};
use crate::invocation_runtime::{
    InvocationManager, InvocationPersistence, InvocationRecorder, started_invocation_event,
};
use crate::services::InvocationCommand;
use tracing::info;
use uuid::Uuid;

/// Manages the full invocation lifecycle: creation, event streaming, and completion.
#[derive(Clone)]
pub(crate) struct InvocationLifecycle {
    db: Db,
    pub(crate) invocations: InvocationManager,
}

impl InvocationLifecycle {
    pub(crate) fn new(db: Db) -> Self {
        Self {
            db,
            invocations: InvocationManager::default(),
        }
    }

    /// Bootstrap the in-memory runtime for a started invocation and emit the started event.
    pub(crate) async fn bootstrap_started(
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
    pub(crate) async fn complete(
        &self,
        invocation_id: Uuid,
        worker_id: &str,
        lease_token: Uuid,
        completion: ExecutionCompletion,
        admit_blocked: impl AsyncAdmitBlocked,
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
            let admitted = admit_blocked.admit(project_id, environment_id).await?;
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

    /// Start a prepared invocation (normal execution path).
    pub(crate) async fn start_prepared(
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
        let command_service = command;
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
        self.bootstrap_started(invocation_id, start.persistence)
            .await?;
        Ok(invocation_id)
    }

    /// Start a project draft validation invocation.
    pub(crate) async fn start_project_draft_validation(
        &self,
        prepared: crate::services::ProjectDraftValidationPrepared,
    ) -> AppResult<Uuid> {
        self.start_draft(
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

    /// Start an environment draft prepare invocation.
    pub(crate) async fn start_environment_draft_prepare(
        &self,
        prepared: crate::services::EnvironmentDraftCreatePrepared,
    ) -> AppResult<Uuid> {
        self.start_draft(
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

    /// Start an environment draft validation invocation.
    pub(crate) async fn start_environment_draft_validation(
        &self,
        prepared: crate::services::EnvironmentDraftValidationPrepared,
    ) -> AppResult<Uuid> {
        self.start_draft(
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

    async fn start_draft(
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
        self.bootstrap_started(invocation_id, None).await?;
        Ok(invocation_id)
    }
}

enum DraftAttachment {
    Project(Uuid),
    Environment(Uuid),
}

/// Trait for post-completion blocked plan admission.
/// Allows InvocationLifecycle to remain independent of the reconciler module.
pub(crate) trait AsyncAdmitBlocked: Send + Sync {
    fn admit(
        &self,
        project_id: i64,
        environment_id: i64,
    ) -> impl std::future::Future<Output = AppResult<usize>> + Send;
}
