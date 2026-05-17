//! Invocation lifecycle bootstrapping: creation, claim deadlines, and prepared invocation startup.
use crate::api::{InvocationCommandApi, InvocationExecutionModeApi, InvocationExecutionSpecApi};
use crate::db::{CreateInvocationInput, PreparationStatus};
use crate::error::{AppError, AppResult};
use crate::server::AppState;
use crate::services::{
    InvocationCommand, InvocationService, code_change_input_fingerprint_for_baseline,
    target_manifest_input_fingerprint,
};
use chrono::{Duration, Utc};
use tokio::time::{Instant, sleep};
use uuid::Uuid;

pub fn invocation_claim_deadline_at(
    execution_mode: InvocationExecutionModeApi,
) -> chrono::DateTime<Utc> {
    Utc::now()
        + Duration::from_std(crate::execution::claim_startup_timeout(execution_mode))
            .expect("timeout fits chrono duration")
}

pub async fn start_project_draft_validation_invocation(
    state: &AppState,
    prepared: crate::services::ProjectDraftValidationPrepared,
) -> AppResult<Uuid> {
    start_draft_invocation(
        state,
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

pub async fn start_environment_draft_prepare_invocation(
    state: &AppState,
    prepared: crate::services::EnvironmentDraftCreatePrepared,
) -> AppResult<Uuid> {
    start_draft_invocation(
        state,
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

pub async fn start_environment_draft_validation_invocation(
    state: &AppState,
    prepared: crate::services::EnvironmentDraftValidationPrepared,
) -> AppResult<Uuid> {
    start_draft_invocation(
        state,
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

enum DraftAttachment {
    Project(Uuid),
    Environment(Uuid),
}

async fn start_draft_invocation(
    state: &AppState,
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
    state
        .db()
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
            state
                .db()
                .attach_project_draft_invocation(draft_id, invocation_id)
                .await?;
        }
        DraftAttachment::Environment(draft_id) => {
            state
                .db()
                .attach_environment_draft_invocation(draft_id, invocation_id)
                .await?;
        }
    }
    state
        .bootstrap_invocation_started(invocation_id, None)
        .await?;
    Ok(invocation_id)
}

pub async fn start_prepared_invocation(
    state: &AppState,
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
    let start = prepared.into_invocation_start(command);
    state
        .db()
        .create_invocation(CreateInvocationInput {
            invocation_id,
            plan_id,
            run_id: start.persistence.as_ref().map(|p| p.run_id),
            project_id,
            environment_id,
            project_draft_id,
            environment_draft_id,
            command: map_command_to_service(command).as_str().to_string(),
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
    state
        .bootstrap_invocation_started(invocation_id, start.persistence)
        .await?;
    Ok(invocation_id)
}

fn map_command_to_service(command: InvocationCommandApi) -> InvocationCommand {
    command.into()
}

pub async fn ensure_target_manifest_for_reconcile(
    state: &AppState,
    project_id: &str,
    environment_slug: &str,
) -> AppResult<()> {
    let environment = state
        .db()
        .get_environment(project_id, environment_slug)
        .await?;
    let desired_commit_sha = environment
        .git_commit_sha
        .clone()
        .ok_or(AppError::ReconciliationRequiresCommitSha)?;
    let baseline_run_id = state
        .db()
        .get_environment_actual_state(&environment.project_ref, &environment.slug)
        .await?
        .last_successful_run_id;
    let input_fingerprint = target_manifest_input_fingerprint(
        &code_change_input_fingerprint_for_baseline(&desired_commit_sha, baseline_run_id),
    );
    if state
        .db()
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
    if state
        .db()
        .has_active_manifest_prepare_for_commit(
            environment.project_id,
            environment.id,
            &desired_commit_sha,
        )
        .await?
    {
        return Err(AppError::ReconciliationInProgress);
    }
    if state
        .db()
        .get_environment_reconcile_preparation_by_scope(environment.project_id, environment.id)
        .await?
        .filter(|preparation| {
            preparation.kind == "target_manifest"
                && preparation.input_fingerprint.as_deref() == Some(input_fingerprint.as_str())
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
    let prepared = InvocationService::new(state.db())
        .prepare_remote_manifest_capture(invocation_id, project_id, environment_slug)
        .await?;
    start_prepared_invocation(
        state,
        invocation_id,
        InvocationCommandApi::ManifestPrepare,
        None,
        prepared,
    )
    .await?;
    state
        .db()
        .mark_manifest_prepare_running(
            environment.project_id,
            environment.id,
            &input_fingerprint,
            &desired_commit_sha,
            invocation_id,
        )
        .await?;
    wait_for_terminal_invocation(state, invocation_id, std::time::Duration::from_secs(120)).await?;
    let status = state.db().get_invocation_status(invocation_id).await?;
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

    if state
        .db()
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

async fn wait_for_terminal_invocation(
    state: &AppState,
    invocation_id: Uuid,
    timeout: std::time::Duration,
) -> AppResult<()> {
    let deadline = Instant::now() + timeout;
    loop {
        let status = state.db().get_invocation_status(invocation_id).await?;
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

#[cfg(test)]
mod tests {
    use super::{invocation_claim_deadline_at, map_command_to_service};
    use crate::api::InvocationCommandApi;
    use crate::api::InvocationExecutionModeApi;
    use crate::services::InvocationCommand;
    use chrono::Utc;

    #[test]
    fn claim_deadline_is_in_the_future() {
        let now = Utc::now();
        let deadline = invocation_claim_deadline_at(InvocationExecutionModeApi::Server);
        assert!(deadline > now);
    }

    #[test]
    fn server_deadline_is_longer_than_local() {
        let server = invocation_claim_deadline_at(InvocationExecutionModeApi::Server);
        let local = invocation_claim_deadline_at(InvocationExecutionModeApi::Local);
        assert!(server > local);
    }

    #[test]
    fn map_command_to_service_maps_all_variants() {
        assert_eq!(
            map_command_to_service(InvocationCommandApi::Build).as_str(),
            InvocationCommand::Build.as_str()
        );
        assert_eq!(
            map_command_to_service(InvocationCommandApi::Run).as_str(),
            InvocationCommand::Run.as_str()
        );
        assert_eq!(
            map_command_to_service(InvocationCommandApi::Ls).as_str(),
            InvocationCommand::Ls.as_str()
        );
        assert_eq!(
            map_command_to_service(InvocationCommandApi::Test).as_str(),
            InvocationCommand::Test.as_str()
        );
        assert_eq!(
            map_command_to_service(InvocationCommandApi::Seed).as_str(),
            InvocationCommand::Seed.as_str()
        );
        assert_eq!(
            map_command_to_service(InvocationCommandApi::Release).as_str(),
            InvocationCommand::Release.as_str()
        );
        assert_eq!(
            map_command_to_service(InvocationCommandApi::ProjectValidate).as_str(),
            InvocationCommand::ProjectValidate.as_str()
        );
        assert_eq!(
            map_command_to_service(InvocationCommandApi::EnvironmentPrepare).as_str(),
            InvocationCommand::EnvironmentPrepare.as_str()
        );
        assert_eq!(
            map_command_to_service(InvocationCommandApi::EnvironmentValidate).as_str(),
            InvocationCommand::EnvironmentValidate.as_str()
        );
        assert_eq!(
            map_command_to_service(InvocationCommandApi::ManifestPrepare).as_str(),
            InvocationCommand::ManifestPrepare.as_str()
        );
    }
}
