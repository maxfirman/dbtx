//! Shared process state used by both the server and reconciler binaries.
use crate::api::InvocationCommandApi;
use crate::config::RuntimeConfig;
use crate::db::Db;
use crate::error::{AppError, AppResult};
use crate::execution::ExecutionCompletion;
use crate::invocation_lifecycle::{AsyncAdmitBlocked, InvocationLifecycle};
use crate::invocation_runtime::{InvocationManager, InvocationPersistence};
use crate::reconciler::auto_admit_blocked_plans_for_environment;
use tokio::time::{Instant, sleep};

/// Core process state shared across binaries (server, reconciler).
///
/// Holds the database handle and invocation lifecycle infrastructure.
/// The server uses this directly as its axum state; the reconciler
/// constructs one without needing any HTTP-server machinery.
#[derive(Clone)]
pub struct ProcessState {
    pub(crate) db: Db,
    #[allow(dead_code)]
    runtime_config: RuntimeConfig,
    pub(crate) lifecycle: InvocationLifecycle,
}

/// Adapter that uses ProcessState for post-completion blocked plan admission.
struct ProcessStateAdmitter<'a>(&'a ProcessState);

impl AsyncAdmitBlocked for ProcessStateAdmitter<'_> {
    async fn admit(&self, project_id: i64, environment_id: i64) -> AppResult<usize> {
        auto_admit_blocked_plans_for_environment(self.0, project_id, environment_id).await
    }
}

impl ProcessState {
    pub fn new(db: Db, runtime_config: RuntimeConfig) -> Self {
        let lifecycle = InvocationLifecycle::new(db.clone());
        Self {
            db,
            runtime_config,
            lifecycle,
        }
    }

    pub(crate) fn db(&self) -> &Db {
        &self.db
    }

    pub(crate) fn invocations(&self) -> &InvocationManager {
        &self.lifecycle.invocations
    }

    pub(crate) async fn bootstrap_invocation_started(
        &self,
        invocation_id: uuid::Uuid,
        persistence: Option<InvocationPersistence>,
    ) -> AppResult<()> {
        self.lifecycle
            .bootstrap_started(invocation_id, persistence)
            .await
    }

    /// Complete an invocation and apply all post-terminal reactions.
    pub(crate) async fn complete_invocation(
        &self,
        invocation_id: uuid::Uuid,
        worker_id: &str,
        lease_token: uuid::Uuid,
        completion: ExecutionCompletion,
    ) -> AppResult<()> {
        self.lifecycle
            .complete(
                invocation_id,
                worker_id,
                lease_token,
                completion,
                ProcessStateAdmitter(self),
            )
            .await
    }

    // --- Invocation startup ---

    pub(crate) async fn start_prepared_invocation(
        &self,
        invocation_id: uuid::Uuid,
        command: InvocationCommandApi,
        plan_id: Option<uuid::Uuid>,
        prepared: crate::services::LocalExecutionPrepared,
    ) -> AppResult<uuid::Uuid> {
        self.lifecycle
            .start_prepared(invocation_id, command, plan_id, prepared)
            .await
    }

    pub(crate) async fn start_project_draft_validation_invocation(
        &self,
        prepared: crate::services::ProjectDraftValidationPrepared,
    ) -> AppResult<uuid::Uuid> {
        self.lifecycle.start_project_draft_validation(prepared).await
    }

    pub(crate) async fn start_environment_draft_prepare_invocation(
        &self,
        prepared: crate::services::EnvironmentDraftCreatePrepared,
    ) -> AppResult<uuid::Uuid> {
        self.lifecycle.start_environment_draft_prepare(prepared).await
    }

    pub(crate) async fn start_environment_draft_validation_invocation(
        &self,
        prepared: crate::services::EnvironmentDraftValidationPrepared,
    ) -> AppResult<uuid::Uuid> {
        self.lifecycle
            .start_environment_draft_validation(prepared)
            .await
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
        let outcome = crate::manifest_preparation::ensure_manifest_preparation(
            &self.db,
            &environment,
            crate::manifest_preparation::ProcessStateStarter(self),
        )
        .await?;
        match outcome {
            crate::manifest_preparation::ManifestPreparationOutcome::AlreadyAvailable => Ok(()),
            crate::manifest_preparation::ManifestPreparationOutcome::InProgress => {
                Err(AppError::ReconciliationInProgress)
            }
            crate::manifest_preparation::ManifestPreparationOutcome::Started(invocation_id) => {
                wait_for_terminal_invocation(
                    self,
                    invocation_id,
                    std::time::Duration::from_secs(120),
                )
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
                            "manifest prepare invocation did not reach a terminal state"
                                .to_string(),
                        ));
                    }
                }
                if self
                    .db
                    .latest_manifest_run_id_for_commit(
                        environment.project_id,
                        environment.id,
                        environment.git_commit_sha.as_deref().unwrap_or_default(),
                    )
                    .await?
                    .is_none()
                {
                    return Err(AppError::Internal(
                        "manifest prepare finished without persisting a manifest snapshot"
                            .to_string(),
                    ));
                }
                Ok(())
            }
        }
    }
}

async fn wait_for_terminal_invocation(
    state: &ProcessState,
    invocation_id: uuid::Uuid,
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
        invocation_id: uuid::Uuid,
        command: InvocationCommandApi,
        plan_id: Option<uuid::Uuid>,
        prepared: crate::services::LocalExecutionPrepared,
    ) -> AppResult<uuid::Uuid> {
        self.start_prepared_invocation(invocation_id, command, plan_id, prepared)
            .await
    }
}
