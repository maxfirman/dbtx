//! Shared process state used by both the server and reconciler binaries.
use crate::config::RuntimeConfig;
use crate::db::Db;
use crate::error::AppResult;
use crate::execution::ExecutionCompletion;
use crate::invocation_runtime::{
    InvocationManager, InvocationPersistence, InvocationRecorder, started_invocation_event,
};
use crate::reconciler::auto_admit_blocked_plans_for_environment;
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
}

impl crate::services::InvocationStarter for ProcessState {
    async fn start_prepared_invocation(
        &self,
        invocation_id: Uuid,
        command: crate::api::InvocationCommandApi,
        plan_id: Option<Uuid>,
        prepared: crate::services::LocalExecutionPrepared,
    ) -> AppResult<Uuid> {
        crate::invocation_bootstrap::start_prepared_invocation(
            self,
            invocation_id,
            command,
            plan_id,
            prepared,
        )
        .await
    }
}
