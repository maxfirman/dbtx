use crate::api::{
    InvocationCommandApi, InvocationExecutionModeApi, InvocationExecutionSpecApi,
};
use crate::db::CreateInvocationInput;
use crate::error::{AppError, AppResult};
use crate::server::AppState;
use crate::services::{InvocationCommand, InvocationService};
use chrono::{Duration, Utc};
use tokio::time::{sleep, Instant};
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
            plan_id: None,
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
            updates_actual_state: false,
            claim_deadline_at: Some(invocation_claim_deadline_at(
                InvocationExecutionModeApi::Server,
            )),
        })
        .await?;
    state
        .db()
        .attach_project_draft_invocation(prepared.draft.id, invocation_id)
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
            plan_id: None,
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
            updates_actual_state: false,
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
            plan_id: None,
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
            updates_actual_state: false,
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

pub async fn start_prepared_invocation(
    state: &AppState,
    invocation_id: Uuid,
    command: InvocationCommandApi,
    plan_id: Option<Uuid>,
    prepared: crate::services::LocalExecutionPrepared,
) -> AppResult<Uuid> {
    let execution_spec = match prepared.spec {
        crate::services::PreparedExecutionSpec::Local(spec) => InvocationExecutionSpecApi::Local {
            command,
            args: spec
                .args
                .into_iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect(),
            project_dir: spec.project_dir.display().to_string(),
            profiles_yml: spec.profiles_yml,
            state_manifest: spec.state_manifest,
        },
        crate::services::PreparedExecutionSpec::Remote(spec) => InvocationExecutionSpecApi::Remote {
            command,
            args: spec
                .args
                .into_iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect(),
            repo_url: spec.repo_url,
            commit_sha: spec.commit_sha,
            project_root: spec.project_root,
            profiles_yml: spec.profiles_yml,
            state_manifest: spec.state_manifest,
        },
        crate::services::PreparedExecutionSpec::ReleaseValidation(spec) => {
            InvocationExecutionSpecApi::ReleaseValidation {
                repo_url: spec.repo_url,
                git_ref: spec.git_ref,
                git_commit_sha: spec.git_commit_sha,
                git_branch: spec.git_branch,
            }
        }
        crate::services::PreparedExecutionSpec::ProjectValidation(spec) => {
            InvocationExecutionSpecApi::ProjectValidation {
                repo_url: spec.repo_url,
                project_root: spec.project_root,
            }
        }
        crate::services::PreparedExecutionSpec::EnvironmentPrepare(spec) => {
            InvocationExecutionSpecApi::EnvironmentPrepare {
                repo_url: spec.repo_url,
                selected_branch: spec.selected_branch,
            }
        }
        crate::services::PreparedExecutionSpec::EnvironmentValidate(spec) => {
            InvocationExecutionSpecApi::EnvironmentValidate {
                repo_url: spec.repo_url,
                commit_sha: spec.commit_sha,
                project_root: spec.project_root,
                selected_branch: spec.selected_branch,
                profiles_yml: spec.profiles_yml,
            }
        }
    };
    let execution_mode = match &execution_spec {
        InvocationExecutionSpecApi::Local { .. } => InvocationExecutionModeApi::Local,
        _ => InvocationExecutionModeApi::Server,
    };
    let persistence = prepared.persistence.map(|p| crate::invocation_runtime::InvocationPersistence {
        run_id: p.run_id,
        project_id: p.project_id,
        environment_id: p.environment_id,
        promote_base_manifest: p.promote_base_manifest,
        updates_actual_state: p.updates_actual_state,
    });
    state
        .db()
        .create_invocation(CreateInvocationInput {
            invocation_id,
            plan_id,
            run_id: persistence.as_ref().map(|p| p.run_id),
            project_id: prepared.project_id,
            environment_id: prepared.environment_id,
            project_draft_id: prepared.project_draft_id,
            environment_draft_id: prepared.environment_draft_id,
            command: map_command_to_service(command).as_str().to_string(),
            execution_mode,
            worker_queue: prepared.worker_queue.clone(),
            execution_spec: Some(execution_spec),
            promote_base_manifest: persistence
                .as_ref()
                .map(|p| p.promote_base_manifest)
                .unwrap_or(false),
            updates_actual_state: persistence
                .as_ref()
                .map(|p| p.updates_actual_state)
                .unwrap_or(false),
            claim_deadline_at: Some(invocation_claim_deadline_at(execution_mode)),
        })
        .await?;
    state.bootstrap_invocation_started(invocation_id, persistence).await?;
    Ok(invocation_id)
}

fn map_command_to_service(command: InvocationCommandApi) -> InvocationCommand {
    match command {
        InvocationCommandApi::Build => InvocationCommand::Build,
        InvocationCommandApi::Run => InvocationCommand::Run,
        InvocationCommandApi::Ls => InvocationCommand::Ls,
        InvocationCommandApi::Test => InvocationCommand::Test,
        InvocationCommandApi::Seed => InvocationCommand::Seed,
        InvocationCommandApi::Release => InvocationCommand::Release,
        InvocationCommandApi::ProjectValidate => InvocationCommand::ProjectValidate,
        InvocationCommandApi::EnvironmentPrepare => InvocationCommand::EnvironmentPrepare,
        InvocationCommandApi::EnvironmentValidate => InvocationCommand::EnvironmentValidate,
        InvocationCommandApi::ManifestPrepare => InvocationCommand::ManifestPrepare,
    }
}

pub async fn ensure_target_manifest_for_reconcile(
    state: &AppState,
    project_id: &str,
    environment_slug: &str,
) -> AppResult<()> {
    let environment = state.db().get_environment(project_id, environment_slug).await?;
    let desired_commit_sha = environment.git_commit_sha.clone().ok_or_else(|| {
        AppError::Io(std::io::Error::other(
            "reconciliation requires a desired git commit sha",
        ))
    })?;
    if state
        .db()
        .latest_manifest_run_id_for_commit(environment.project_id, environment.id, &desired_commit_sha)
        .await?
        .is_some()
    {
        return Ok(());
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
    wait_for_terminal_invocation(state, invocation_id, std::time::Duration::from_secs(120)).await?;
    let status = state.db().get_invocation_status(invocation_id).await?;
    match status.status {
        crate::api::InvocationLifecycleStatus::Succeeded => {}
        crate::api::InvocationLifecycleStatus::Failed
        | crate::api::InvocationLifecycleStatus::Canceled => {
            return Err(AppError::Io(std::io::Error::other(
                status
                    .error
                    .unwrap_or_else(|| "manifest prepare invocation failed".to_string()),
            )));
        }
        crate::api::InvocationLifecycleStatus::Running => {
            return Err(AppError::Io(std::io::Error::other(
                "manifest prepare invocation did not reach a terminal state",
            )));
        }
    }

    if state
        .db()
        .latest_manifest_run_id_for_commit(environment.project_id, environment.id, &desired_commit_sha)
        .await?
        .is_none()
    {
        return Err(AppError::Io(std::io::Error::other(
            "manifest prepare finished without persisting a manifest snapshot",
        )));
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
        if !matches!(status.status, crate::api::InvocationLifecycleStatus::Running) {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("timed out waiting for invocation {invocation_id}"),
            )));
        }
        sleep(std::time::Duration::from_millis(250)).await;
    }
}
