//! CLI runtime handlers for project, environment, invocation, worker, and queue subcommands.
use crate::api;
use crate::api::{
    InvocationCancelStateApi, InvocationExecutionModeApi, InvocationLifecycleStatus,
    InvocationListApiRequest,
};
use crate::cli::{
    EnvironmentCommand, InvocationCommand as InvocationCliCommand, ProjectCommand, QueueCommand,
    WorkerCommand,
};
use crate::cli_output::{
    print_environment, print_environment_version, print_invocation, print_project,
    print_project_create_start, print_queue, print_release_already_released, print_release_failure,
    print_release_start, print_release_success, print_worker, render_invocation_event,
    render_project_validation_event, render_release_event,
};
use crate::cli_workflow::{stream_and_wait_for_invocation, wait_for_invocation_completion};
use crate::client;
use crate::config::{self, resolve_service_url};
use crate::error::{AppError, AppResult};
use crate::services::InvocationCommand;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;

pub async fn handle_project_command(
    command: ProjectCommand,
    service_url_override: Option<String>,
) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let client = daemon_client(&current_dir, service_url_override)?;
    match command {
        ProjectCommand::Create {
            git_repo_url,
            project_root,
        } => {
            print_project_create_start(&git_repo_url, &project_root);
            let draft = client
                .project_draft_create(api::ProjectDraftCreateApiRequest {
                    git_repo_url,
                    project_root,
                })
                .await?
                .draft;
            let validation = client.project_draft_validate(draft.id).await?;
            let status = stream_and_wait_for_invocation(
                &client,
                validation.invocation_id,
                render_project_validation_event,
            )
            .await?;
            if !matches!(status.status, api::InvocationLifecycleStatus::Succeeded) {
                eprintln!(
                    "project validation failed: {}",
                    status.error.as_deref().unwrap_or("validation failed")
                );
                return Err(AppError::SilentExit(1));
            }
            let draft = client.project_draft_get(draft.id).await?.draft;
            let project = client.project_draft_confirm(draft.id).await?.project;
            print_project(&project);
        }
        ProjectCommand::Update {
            project,
            git_repo_url,
            project_root,
        } => {
            let project = client
                .project_update(
                    &project,
                    api::ProjectUpdateApiRequest {
                        git_repo_url,
                        project_root,
                    },
                )
                .await?;
            print_project(&project.project);
        }
        ProjectCommand::List => {
            for project in client.project_list().await?.projects {
                print_project(&project);
            }
        }
        ProjectCommand::Show { project } => {
            let project = client.project_show_by_id(&project).await?.project;
            print_project(&project);
        }
    }
    Ok(())
}

pub async fn handle_environment_command(
    command: EnvironmentCommand,
    service_url_override: Option<String>,
) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let client = daemon_client(&current_dir, service_url_override)?;
    match command {
        EnvironmentCommand::List { project } => {
            for environment in client.environment_list(&project).await?.environments {
                print_environment(&environment);
            }
        }
        EnvironmentCommand::Show { project, slug } => {
            let environment = client
                .environment_show_by_id(&project, &slug)
                .await?
                .environment;
            print_environment(&environment);
        }
        EnvironmentCommand::Release {
            project,
            slug,
            git_branch,
            git_commit_sha,
            git_ref,
        } => {
            let current_environment = client
                .environment_show_by_id(&project, &slug)
                .await?
                .environment;
            let current_commit_sha = current_environment.git_commit_sha.clone();
            print_release_start(
                &project,
                &slug,
                git_ref.as_deref(),
                git_commit_sha.as_deref(),
            );
            if git_commit_sha.is_none() == git_ref.is_none() {
                return Err(AppError::InvalidInput(
                    "provide exactly one of --git-commit-sha or --git-ref".to_string(),
                ));
            }
            if let Some(candidate_sha) = git_commit_sha.as_deref()
                && current_commit_sha.as_deref() == Some(candidate_sha)
            {
                print_release_already_released(&project, &slug, candidate_sha);
                return Ok(());
            }
            let release_args = build_release_validation_args(
                git_branch.clone(),
                git_commit_sha.clone(),
                git_ref.clone(),
            );
            let response = client
                .invocation_create(api::InvocationCreateApiRequest {
                    command: api::InvocationCommandApi::Release,
                    args: release_args,
                    project_id: Some(project.clone()),
                    environment_slug: Some(slug.clone()),
                })
                .await?;
            let status = stream_and_wait_for_invocation(
                &client,
                response.invocation_id,
                render_release_event,
            )
            .await?;
            if !matches!(status.status, api::InvocationLifecycleStatus::Succeeded) {
                print_release_failure(
                    &project,
                    &slug,
                    status
                        .error
                        .as_deref()
                        .unwrap_or("release validation failed"),
                );
                return Err(AppError::SilentExit(1));
            }
            let environment = client
                .environment_show_by_id(&project, &slug)
                .await?
                .environment;
            if current_commit_sha == environment.git_commit_sha {
                print_release_already_released(
                    &project,
                    &slug,
                    environment.git_commit_sha.as_deref().unwrap_or(""),
                );
            } else {
                print_release_success(&project, &slug, environment.git_commit_sha.as_deref());
            }
        }
        EnvironmentCommand::History { project, slug } => {
            for version in client.environment_history(&project, &slug).await?.versions {
                print_environment_version(&version);
            }
        }
        EnvironmentCommand::Rollback {
            project,
            slug,
            version_id,
        } => {
            let environment = client
                .environment_rollback(
                    &project,
                    &slug,
                    api::EnvironmentRollbackApiRequest { version_id },
                )
                .await?
                .environment;
            print_environment(&environment);
        }
        EnvironmentCommand::Pause { project, slug } => {
            let environment = client.environment_pause(&project, &slug).await?.environment;
            println!("Paused automatic reconciliation for {}/{}", project, slug);
            print_environment(&environment);
        }
        EnvironmentCommand::Resume { project, slug } => {
            let environment = client
                .environment_resume(&project, &slug)
                .await?
                .environment;
            println!("Resumed automatic reconciliation for {}/{}", project, slug);
            print_environment(&environment);
        }
    }
    Ok(())
}

pub async fn handle_invocation_command(
    command: InvocationCliCommand,
    service_url_override: Option<String>,
) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let client = daemon_client(&current_dir, service_url_override)?;
    match command {
        InvocationCliCommand::List {
            status,
            execution_mode,
            worker_queue,
            claimed_by,
            cancel_state,
            limit,
        } => {
            for invocation in client
                .invocation_list(InvocationListApiRequest {
                    status: parse_invocation_status_filter(status.as_deref())?,
                    execution_mode: parse_execution_mode_filter(execution_mode.as_deref())?,
                    worker_queue,
                    claimed_by,
                    cancel_state: parse_cancel_state_filter(cancel_state.as_deref())?,
                    limit,
                })
                .await?
                .invocations
            {
                print_invocation(&invocation);
            }
        }
        InvocationCliCommand::Show { invocation_id } => {
            let invocation_id = uuid::Uuid::parse_str(&invocation_id)
                .map_err(|err| AppError::InvalidInput(format!("invalid invocation id: {err}")))?;
            let invocation = client.invocation_status(invocation_id).await?;
            print_invocation(&invocation);
        }
        InvocationCliCommand::Cancel {
            invocation_id,
            wait,
        } => {
            let invocation_id = uuid::Uuid::parse_str(&invocation_id)
                .map_err(|err| AppError::InvalidInput(format!("invalid invocation id: {err}")))?;
            client
                .invocation_cancel(invocation_id, api::InvocationCancelApiRequest::default())
                .await?;
            eprintln!("requested cancellation for invocation {invocation_id}");
            let invocation = if wait {
                wait_for_invocation_completion(&client, invocation_id).await?
            } else {
                client.invocation_status(invocation_id).await?
            };
            print_invocation(&invocation);
        }
        InvocationCliCommand::Cleanup { older_than_hours } => {
            if older_than_hours <= 0 {
                return Err(AppError::InvalidInput(
                    "--older-than-hours must be greater than 0".to_string(),
                ));
            }
            let response = client
                .invocation_cleanup(api::InvocationCleanupApiRequest {
                    older_than_seconds: older_than_hours * 3600,
                })
                .await?;
            println!("deleted {} invocation(s)", response.deleted);
        }
    }
    Ok(())
}

pub async fn handle_worker_command(
    command: WorkerCommand,
    service_url_override: Option<String>,
) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let client = daemon_client(&current_dir, service_url_override)?;
    match command {
        WorkerCommand::List => {
            for worker in client.worker_list().await?.workers {
                print_worker(&worker);
            }
        }
    }
    Ok(())
}

pub async fn handle_queue_command(
    command: QueueCommand,
    service_url_override: Option<String>,
) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let client = daemon_client(&current_dir, service_url_override)?;
    match command {
        QueueCommand::List => {
            for queue in client.queue_list().await?.queues {
                print_queue(&queue);
            }
        }
    }
    Ok(())
}

pub async fn invoke_via_daemon(
    service_url: String,
    command: InvocationCommand,
    args: Vec<OsString>,
    ctx: &config::InvocationContext,
) -> AppResult<()> {
    if matches!(command, InvocationCommand::Ls) || command.persists_state() {
        return invoke_via_local_worker(service_url, command, args, ctx).await;
    }
    let response = create_invocation(service_url.clone(), command, args, ctx).await?;
    let client = client::DaemonClient::new(service_url.clone());
    let status =
        stream_and_wait_for_invocation(&client, response.invocation_id, render_invocation_event)
            .await?;
    match status.exit_code.unwrap_or(1) {
        0 => Ok(()),
        code => {
            if let Some(error) = status.error {
                Err(AppError::Internal(error))
            } else {
                Err(AppError::DbtFailed(code))
            }
        }
    }
}

fn daemon_client(
    current_dir: &Path,
    service_url_override: Option<String>,
) -> AppResult<client::DaemonClient> {
    let service_url = resolve_service_url(service_url_override, Some(current_dir))?
        .ok_or(AppError::MissingServiceUrl)?;
    Ok(client::DaemonClient::new(service_url))
}

async fn invoke_via_local_worker(
    service_url: String,
    command: InvocationCommand,
    args: Vec<OsString>,
    ctx: &config::InvocationContext,
) -> AppResult<()> {
    let response = create_invocation(service_url.clone(), command, args, ctx).await?;

    let worker_path = resolve_worker_binary_path()?;
    let status = StdCommand::new(worker_path)
        .arg("--service-url")
        .arg(&service_url)
        .arg("--execution-mode")
        .arg("local")
        .arg("--once")
        .arg("--queue")
        .arg(&response.worker_queue)
        .env(
            "RUST_LOG",
            std::env::var("DBTX_ONE_SHOT_WORKER_LOG").unwrap_or_else(|_| "dbtx=warn".to_string()),
        )
        .status()?;
    match status.code().unwrap_or(1) {
        0 => Ok(()),
        code => {
            let client = client::DaemonClient::new(service_url);
            let invocation = client.invocation_status(response.invocation_id).await?;
            if let Some(error) = invocation.error {
                Err(AppError::Internal(error))
            } else {
                Err(AppError::DbtFailed(code))
            }
        }
    }
}

fn resolve_worker_binary_path() -> AppResult<PathBuf> {
    if let Ok(path) = std::env::var("DBTX_WORKER_PATH") {
        return Ok(PathBuf::from(path));
    }
    let current_exe = std::env::current_exe()?;
    let current_dir = current_exe.parent().ok_or_else(|| {
        AppError::Internal("failed to resolve current binary directory".to_string())
    })?;
    let worker_name = if cfg!(windows) {
        "dbtx-worker.exe"
    } else {
        "dbtx-worker"
    };
    Ok(current_dir.join(worker_name))
}

async fn create_invocation(
    service_url: String,
    command: InvocationCommand,
    args: Vec<OsString>,
    ctx: &config::InvocationContext,
) -> AppResult<api::InvocationCreateResponse> {
    let client = client::DaemonClient::new(service_url);

    // Resolve project by git repo
    let git_state = crate::dbt_utils::read_git_state(&ctx.project_dir);
    let repo_url = git_state
        .repo_url
        .ok_or(AppError::RemoteProjectRequiresGitRepo)?;
    let repo_root = crate::dbt_utils::git_repo_root(&ctx.project_dir)?;
    let project_root =
        crate::services::relative_project_root(&repo_root, &ctx.project_dir.canonicalize()?);

    let project = client
        .project_resolve(&repo_url, &project_root)
        .await?
        .project
        .project;

    // Resolve local environment
    let target_name = ctx.target_name.clone().unwrap_or_else(|| "dev".to_string());
    let local_profile = crate::profile::LocalTargetProfile::from_local_project(
        &ctx.project_dir,
        Some(&target_name),
    )?;
    let machine_id = crate::services::local_machine_scope()?;

    let env = client
        .environment_local_upsert(
            &project.project_id,
            api::LocalEnvironmentUpsertApiRequest {
                target_name: local_profile.target_name,
                machine_id,
                adapter_type: local_profile.adapter_type,
                schema_name: local_profile.schema_name,
            },
        )
        .await?;

    // Create invocation
    client
        .invocation_create(api::InvocationCreateApiRequest {
            command: match command {
                InvocationCommand::Build => api::InvocationCommandApi::Build,
                InvocationCommand::Run => api::InvocationCommandApi::Run,
                InvocationCommand::Ls => api::InvocationCommandApi::Ls,
                InvocationCommand::Test => api::InvocationCommandApi::Test,
                InvocationCommand::Seed => api::InvocationCommandApi::Seed,
                InvocationCommand::Release => api::InvocationCommandApi::Release,
                InvocationCommand::ProjectValidate => api::InvocationCommandApi::ProjectValidate,
                InvocationCommand::EnvironmentPrepare => {
                    api::InvocationCommandApi::EnvironmentPrepare
                }
                InvocationCommand::EnvironmentValidate => {
                    api::InvocationCommandApi::EnvironmentValidate
                }
                InvocationCommand::ManifestPrepare => api::InvocationCommandApi::ManifestPrepare,
            },
            args: args
                .into_iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect(),
            project_id: Some(project.project_id),
            environment_slug: Some(env.environment_slug),
        })
        .await
}

fn build_release_validation_args(
    git_branch: Option<String>,
    git_commit_sha: Option<String>,
    git_ref: Option<String>,
) -> Vec<String> {
    let mut args = Vec::new();
    if let Some(git_branch) = git_branch {
        args.push("--git-branch".to_string());
        args.push(git_branch);
    }
    if let Some(git_commit_sha) = git_commit_sha {
        args.push("--git-commit-sha".to_string());
        args.push(git_commit_sha);
    }
    if let Some(git_ref) = git_ref {
        args.push("--git-ref".to_string());
        args.push(git_ref);
    }
    args
}

fn parse_invocation_status_filter(
    value: Option<&str>,
) -> AppResult<Option<InvocationLifecycleStatus>> {
    match value {
        None => Ok(None),
        Some("running") => Ok(Some(InvocationLifecycleStatus::Running)),
        Some("succeeded") => Ok(Some(InvocationLifecycleStatus::Succeeded)),
        Some("failed") => Ok(Some(InvocationLifecycleStatus::Failed)),
        Some("canceled") => Ok(Some(InvocationLifecycleStatus::Canceled)),
        Some(other) => Err(AppError::InvalidInput(format!(
            "invalid invocation status filter: {other}"
        ))),
    }
}

fn parse_execution_mode_filter(
    value: Option<&str>,
) -> AppResult<Option<InvocationExecutionModeApi>> {
    match value {
        None => Ok(None),
        Some("server") => Ok(Some(InvocationExecutionModeApi::Server)),
        Some("local") => Ok(Some(InvocationExecutionModeApi::Local)),
        Some(other) => Err(AppError::InvalidInput(format!(
            "invalid execution mode filter: {other}"
        ))),
    }
}

fn parse_cancel_state_filter(value: Option<&str>) -> AppResult<Option<InvocationCancelStateApi>> {
    match value {
        None => Ok(None),
        Some("none") => Ok(Some(InvocationCancelStateApi::None)),
        Some("requested") => Ok(Some(InvocationCancelStateApi::Requested)),
        Some("completed") => Ok(Some(InvocationCancelStateApi::Completed)),
        Some(other) => Err(AppError::InvalidInput(format!(
            "invalid cancel state filter: {other}"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::build_release_validation_args;

    #[test]
    fn release_validation_args_support_git_ref_resolution() {
        let args = build_release_validation_args(
            Some("main".to_string()),
            None,
            Some("preview".to_string()),
        );
        assert_eq!(args, vec!["--git-branch", "main", "--git-ref", "preview"]);
    }

    #[test]
    fn release_validation_args_support_commit_sha_resolution() {
        let args = build_release_validation_args(
            Some("main".to_string()),
            Some("0123456789abcdef0123456789abcdef01234567".to_string()),
            None,
        );
        assert_eq!(
            args,
            vec![
                "--git-branch",
                "main",
                "--git-commit-sha",
                "0123456789abcdef0123456789abcdef01234567",
            ]
        );
    }
}
