//! Manifest preparation: idempotent decision logic for ensuring a target manifest exists.
//!
//! Both the synchronous (server/UI) and asynchronous (reconciler) callers share
//! the same preparation decision. They differ only in how they react to `Started`.

use crate::api::InvocationCommandApi;
use crate::db::{Db, EnvironmentRecord, PreparationStatus};
use crate::error::{AppError, AppResult};
use crate::services::{InvocationService, PreparationKind, ReconcileInputIdentity};
use chrono::Utc;
use uuid::Uuid;

/// Outcome of the manifest preparation decision.
#[derive(Debug)]
pub(crate) enum ManifestPreparationOutcome {
    /// Manifest already exists for the desired commit — nothing to do.
    AlreadyAvailable,
    /// A preparation is already in progress or in backoff — caller should wait or skip.
    InProgress,
    /// A new manifest prepare invocation was started.
    Started(Uuid),
}

/// Resolve the manifest preparation state for an environment and, if needed,
/// start a new manifest prepare invocation. Returns the outcome so callers
/// can decide whether to block, skip, or proceed.
pub(crate) async fn ensure_manifest_preparation(
    db: &Db,
    environment: &EnvironmentRecord,
    start_invocation: impl AsyncStartInvocation,
) -> AppResult<ManifestPreparationOutcome> {
    let desired_commit_sha = environment
        .git_commit_sha
        .clone()
        .ok_or(AppError::ReconciliationRequiresCommitSha)?;
    let baseline_run_id = db
        .get_environment_actual_state(&environment.project_ref, &environment.slug)
        .await?
        .last_successful_run_id;
    let identity = ReconcileInputIdentity::code_change(&desired_commit_sha, baseline_run_id);
    let input_fingerprint = identity.target_manifest_preparation_fingerprint();

    // Already have a manifest for this commit
    if db
        .latest_manifest_run_id_for_commit(
            environment.project_id,
            environment.id,
            &desired_commit_sha,
        )
        .await?
        .is_some()
    {
        return Ok(ManifestPreparationOutcome::AlreadyAvailable);
    }

    // Active preparation already running
    if db
        .has_active_manifest_prepare_for_commit(
            environment.project_id,
            environment.id,
            &desired_commit_sha,
        )
        .await?
    {
        return Ok(ManifestPreparationOutcome::InProgress);
    }

    // Failed preparation still in backoff
    if db
        .get_environment_reconcile_preparation_by_scope(environment.project_id, environment.id)
        .await?
        .filter(|preparation| {
            preparation.kind == PreparationKind::TargetManifest.as_str()
                && preparation.input_fingerprint.as_deref() == Some(input_fingerprint.as_str())
                && preparation.status == PreparationStatus::Failed
                && preparation
                    .next_attempt_at
                    .map(|next_attempt_at| next_attempt_at > Utc::now())
                    .unwrap_or(false)
        })
        .is_some()
    {
        return Ok(ManifestPreparationOutcome::InProgress);
    }

    // Start a new manifest prepare invocation
    let invocation_id = Uuid::new_v4();
    let prepared = InvocationService::new(db)
        .prepare_remote_manifest_capture(invocation_id, &environment.project_ref, &environment.slug)
        .await?;
    start_invocation.start(invocation_id, prepared).await?;
    db.mark_manifest_prepare_running(
        environment.project_id,
        environment.id,
        &input_fingerprint,
        &desired_commit_sha,
        invocation_id,
    )
    .await?;

    Ok(ManifestPreparationOutcome::Started(invocation_id))
}

/// Trait for starting a manifest prepare invocation.
/// Allows the shared logic to remain independent of ProcessState.
pub(crate) trait AsyncStartInvocation {
    fn start(
        &self,
        invocation_id: Uuid,
        prepared: crate::services::LocalExecutionPrepared,
    ) -> impl std::future::Future<Output = AppResult<Uuid>> + Send;
}

/// Adapter that uses ProcessState to start invocations.
pub(crate) struct ProcessStateStarter<'a>(pub(crate) &'a super::process_state::ProcessState);

impl AsyncStartInvocation for ProcessStateStarter<'_> {
    async fn start(
        &self,
        invocation_id: Uuid,
        prepared: crate::services::LocalExecutionPrepared,
    ) -> AppResult<Uuid> {
        self.0
            .start_prepared_invocation(
                invocation_id,
                InvocationCommandApi::ManifestPrepare,
                None,
                prepared,
            )
            .await
    }
}
