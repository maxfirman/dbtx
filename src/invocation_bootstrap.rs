use crate::api::{InvocationExecutionModeApi, InvocationExecutionSpecApi};
use crate::db::CreateInvocationInput;
use crate::error::AppResult;
use crate::server::AppState;
use crate::services::InvocationCommand;
use chrono::{Duration, Utc};
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
    let invocation_id = prepared.invocation_id;
    state
        .db()
        .create_invocation(CreateInvocationInput {
            invocation_id,
            run_id: None,
            project_id: None,
            environment_id: None,
            project_draft_id: Some(prepared.draft.id),
            environment_draft_id: None,
            command: InvocationCommand::ProjectValidate.as_str().to_string(),
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: prepared.worker_queue,
            execution_spec: Some(InvocationExecutionSpecApi::ProjectValidation {
                repo_url: prepared.spec.repo_url,
                project_root: prepared.spec.project_root,
            }),
            promote_base_manifest: false,
            claim_deadline_at: Some(invocation_claim_deadline_at(
                InvocationExecutionModeApi::Server,
            )),
        })
        .await?;
    state.bootstrap_invocation_started(invocation_id, None).await?;
    Ok(invocation_id)
}

pub async fn start_environment_draft_prepare_invocation(
    state: &AppState,
    prepared: crate::services::EnvironmentDraftCreatePrepared,
) -> AppResult<Uuid> {
    let invocation_id = prepared.invocation_id;
    state
        .db()
        .create_invocation(CreateInvocationInput {
            invocation_id,
            run_id: None,
            project_id: None,
            environment_id: None,
            project_draft_id: None,
            environment_draft_id: Some(prepared.draft.id),
            command: InvocationCommand::EnvironmentPrepare.as_str().to_string(),
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: prepared.worker_queue,
            execution_spec: Some(InvocationExecutionSpecApi::EnvironmentPrepare {
                repo_url: prepared.spec.repo_url,
                selected_branch: prepared.spec.selected_branch,
            }),
            promote_base_manifest: false,
            claim_deadline_at: Some(invocation_claim_deadline_at(
                InvocationExecutionModeApi::Server,
            )),
        })
        .await?;
    state
        .db()
        .attach_environment_draft_invocation(prepared.draft.id, invocation_id)
        .await?;
    state.bootstrap_invocation_started(invocation_id, None).await?;
    Ok(invocation_id)
}

pub async fn start_environment_draft_validation_invocation(
    state: &AppState,
    prepared: crate::services::EnvironmentDraftValidationPrepared,
) -> AppResult<Uuid> {
    let invocation_id = prepared.invocation_id;
    state
        .db()
        .create_invocation(CreateInvocationInput {
            invocation_id,
            run_id: None,
            project_id: None,
            environment_id: None,
            project_draft_id: None,
            environment_draft_id: Some(prepared.draft.id),
            command: InvocationCommand::EnvironmentValidate.as_str().to_string(),
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: prepared.worker_queue,
            execution_spec: Some(InvocationExecutionSpecApi::EnvironmentValidate {
                repo_url: prepared.spec.repo_url,
                commit_sha: prepared.spec.commit_sha,
                project_root: prepared.spec.project_root,
                selected_branch: prepared.spec.selected_branch,
                profiles_yml: prepared.spec.profiles_yml,
            }),
            promote_base_manifest: false,
            claim_deadline_at: Some(invocation_claim_deadline_at(
                InvocationExecutionModeApi::Server,
            )),
        })
        .await?;
    state
        .db()
        .attach_environment_draft_invocation(prepared.draft.id, invocation_id)
        .await?;
    state.bootstrap_invocation_started(invocation_id, None).await?;
    Ok(invocation_id)
}
