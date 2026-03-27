use clap::Parser;
use dbtx::api;
use dbtx::api::{
    InvocationCancelStateApi, InvocationExecutionModeApi, InvocationLifecycleStatus,
    InvocationListApiRequest,
};
use dbtx::cli::{
    Cli, Command, EnvironmentCommand, InvocationCommand as InvocationCliCommand, ProjectCommand,
    QueueCommand, StateCommand, WorkerCommand,
};
use dbtx::cli_output::{
    print_environment, print_environment_version, print_invocation, print_migration_summary,
    print_project, print_project_create_start, print_queue, print_release_already_released,
    print_release_failure, print_release_start, print_release_success, print_worker,
    render_invocation_event, render_project_validation_event, render_release_event,
};
use dbtx::client;
use dbtx::config::{self, resolve_service_url};
use dbtx::error::{AppError, AppResult};
use dbtx::services::InvocationCommand;
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        if !matches!(err, AppError::SilentExit(_)) {
            eprintln!("error: {err}");
        }
        std::process::exit(err.exit_code());
    }
}

async fn run() -> AppResult<()> {
    let cli = Cli::parse();

    match cli.command {
        Command::State(state_command) => match state_command {
            StateCommand::Migrate => {
                let current_dir = std::env::current_dir()?;
                let client = daemon_client(&current_dir, cli.service_url)?;
                let response = client.migrate().await?;
                print_migration_summary(&response.applied);
            }
        },
        Command::Project(project_command) => {
            handle_project_command(project_command, cli.service_url).await?
        }
        Command::Environment(environment_command) => {
            handle_environment_command(environment_command, cli.service_url).await?
        }
        Command::Invocation(invocation_command) => {
            handle_invocation_command(invocation_command, cli.service_url).await?
        }
        Command::Worker(worker_command) => {
            handle_worker_command(worker_command, cli.service_url).await?
        }
        Command::Queue(queue_command) => {
            handle_queue_command(queue_command, cli.service_url).await?
        }
        Command::Build { args } => {
            handle_persisting_command(InvocationCommand::Build, args, cli.service_url).await?
        }
        Command::Run { args } => {
            handle_persisting_command(InvocationCommand::Run, args, cli.service_url).await?
        }
        Command::Ls { args } => handle_passthrough_command(args, cli.service_url).await?,
        Command::Test { args } => {
            handle_persisting_command(InvocationCommand::Test, args, cli.service_url).await?
        }
        Command::Seed { args } => {
            handle_persisting_command(InvocationCommand::Seed, args, cli.service_url).await?
        }
    }

    Ok(())
}

fn is_help_request(args: &[OsString]) -> bool {
    args.iter().any(|arg| arg == "--help" || arg == "-h")
}

fn exit_with_dbt_help(subcommand: &str) -> AppResult<()> {
    let dbt_path = std::env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string());
    let status = StdCommand::new(dbt_path)
        .arg(subcommand)
        .arg("--help")
        .status()?;
    std::process::exit(status.code().unwrap_or(0));
}

async fn handle_persisting_command(
    command: InvocationCommand,
    args: Vec<OsString>,
    service_url_override: Option<String>,
) -> AppResult<()> {
    if is_help_request(&args) {
        exit_with_dbt_help(command.as_str())?;
    }
    let ctx = config::InvocationContext::from_args(&args, false)?;
    let project_dir = ctx.project_dir.clone();
    let service_url = resolve_service_url(service_url_override, Some(&project_dir))?
        .ok_or(AppError::MissingServiceUrl)?;
    invoke_via_daemon(service_url, command, args, &ctx).await?;
    Ok(())
}

async fn handle_passthrough_command(
    args: Vec<OsString>,
    service_url_override: Option<String>,
) -> AppResult<()> {
    if is_help_request(&args) {
        exit_with_dbt_help(InvocationCommand::Ls.as_str())?;
    }
    let ctx = config::InvocationContext::from_args(&args, false)?;
    let project_dir = ctx.project_dir.clone();
    let service_url = resolve_service_url(service_url_override, Some(&project_dir))?
        .ok_or(AppError::MissingServiceUrl)?;
    invoke_via_daemon(service_url, InvocationCommand::Ls, args, &ctx).await?;
    Ok(())
}

async fn handle_project_command(
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
            client
                .stream_invocation_events(validation.invocation_id, render_project_validation_event)
                .await?;
            let status = wait_for_invocation_completion(&client, validation.invocation_id).await?;
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

async fn handle_environment_command(
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
                return Err(AppError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "provide exactly one of --git-commit-sha or --git-ref",
                )));
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
                    current_dir: None,
                    project_id: Some(project.clone()),
                    environment_slug: Some(slug.clone()),
                })
                .await?;
            client
                .stream_invocation_events(response.invocation_id, render_release_event)
                .await?;
            let status = wait_for_invocation_completion(&client, response.invocation_id).await?;
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
    }
    Ok(())
}

async fn handle_invocation_command(
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
            let invocation_id = uuid::Uuid::parse_str(&invocation_id).map_err(|err| {
                AppError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid invocation id: {err}"),
                ))
            })?;
            let invocation = client.invocation_status(invocation_id).await?;
            print_invocation(&invocation);
        }
        InvocationCliCommand::Cancel {
            invocation_id,
            wait,
        } => {
            let invocation_id = uuid::Uuid::parse_str(&invocation_id).map_err(|err| {
                AppError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("invalid invocation id: {err}"),
                ))
            })?;
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
                return Err(AppError::Io(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "--older-than-hours must be greater than 0",
                )));
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

async fn handle_worker_command(
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

async fn handle_queue_command(
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

fn daemon_client(
    current_dir: &Path,
    service_url_override: Option<String>,
) -> AppResult<client::DaemonClient> {
    let service_url = resolve_service_url(service_url_override, Some(current_dir))?
        .ok_or(AppError::MissingServiceUrl)?;
    Ok(client::DaemonClient::new(service_url))
}

async fn invoke_via_daemon(
    service_url: String,
    command: InvocationCommand,
    args: Vec<OsString>,
    ctx: &config::InvocationContext,
) -> AppResult<()> {
    if matches!(command, InvocationCommand::Ls) || command.persists_state() {
        return invoke_via_local_worker(service_url, command, args, ctx).await;
    }
    let response = create_invocation(
        service_url.clone(),
        command,
        args,
        ctx,
    )
    .await?;
    let client = client::DaemonClient::new(service_url.clone());
    client
        .stream_invocation_events(response.invocation_id, render_invocation_event)
        .await?;
    let status = wait_for_invocation_completion(&client, response.invocation_id).await?;
    match status.exit_code.unwrap_or(1) {
        0 => Ok(()),
        code => {
            if let Some(error) = status.error {
                Err(AppError::Io(std::io::Error::other(error)))
            } else {
                Err(AppError::DbtFailed(code))
            }
        }
    }
}

async fn wait_for_invocation_completion(
    client: &client::DaemonClient,
    invocation_id: uuid::Uuid,
) -> AppResult<api::InvocationStatusResponse> {
    loop {
        let status = client.invocation_status(invocation_id).await?;
        if !matches!(status.status, api::InvocationLifecycleStatus::Running) {
            return Ok(status);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

async fn invoke_via_local_worker(
    service_url: String,
    command: InvocationCommand,
    args: Vec<OsString>,
    ctx: &config::InvocationContext,
) -> AppResult<()> {
    let response = create_invocation(service_url.clone(), command, args, ctx).await?;
    let _ = command;
    let _ = ctx;

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
                Err(AppError::Io(std::io::Error::other(error)))
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
        AppError::Io(std::io::Error::other(
            "failed to resolve current binary directory",
        ))
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
                InvocationCommand::EnvironmentPrepare => api::InvocationCommandApi::EnvironmentPrepare,
                InvocationCommand::EnvironmentValidate => api::InvocationCommandApi::EnvironmentValidate,
            },
            args: args
                .into_iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect(),
            current_dir: Some(ctx.project_dir.display().to_string()),
            project_id: None,
            environment_slug: Some(ctx.target_name.clone().unwrap_or_default()),
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
        Some(other) => Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid invocation status filter: {other}"),
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
        Some(other) => Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid execution mode filter: {other}"),
        ))),
    }
}

fn parse_cancel_state_filter(value: Option<&str>) -> AppResult<Option<InvocationCancelStateApi>> {
    match value {
        None => Ok(None),
        Some("none") => Ok(Some(InvocationCancelStateApi::None)),
        Some("requested") => Ok(Some(InvocationCancelStateApi::Requested)),
        Some("completed") => Ok(Some(InvocationCancelStateApi::Completed)),
        Some(other) => Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("invalid cancel state filter: {other}"),
        ))),
    }
}

#[cfg(test)]
mod tests {
    use dbtx::error::AppError;
    use dbtx::services::{
        infer_local_project_defaults, infer_remote_project_defaults,
        read_dbt_project_name_from_root, relative_project_root, validate_remote_project_root,
    };
    use std::process::Command;
    use tempfile::TempDir;

    #[test]
    fn infers_project_defaults_from_current_repo_and_dbt_project() {
        let temp = TempDir::new().expect("temp dir");
        let repo_root = temp.path();
        let project_root = repo_root.join("analytics");
        std::fs::create_dir_all(&project_root).expect("project root");
        std::fs::write(
            project_root.join("dbt_project.yml"),
            "name: jaffle_shop_project\n",
        )
        .expect("dbt project");

        run_git_cmd(["init"], repo_root);
        run_git_cmd(
            ["remote", "add", "origin", "git@github.com:example/repo.git"],
            repo_root,
        );

        let inferred =
            infer_local_project_defaults(&project_root, None, None, None).expect("inferred");

        assert_eq!(inferred.project_name, "jaffle_shop_project");
        assert_eq!(
            inferred.git_repo_url.as_deref(),
            Some("git@github.com:example/repo.git")
        );
        assert_eq!(
            inferred.project_root.as_deref(),
            Some(
                project_root
                    .canonicalize()
                    .expect("canonical")
                    .to_string_lossy()
                    .as_ref()
            )
        );
        assert_eq!(inferred.default_branch, None);
    }

    #[test]
    fn rejects_non_dbt_project_root() {
        let temp = TempDir::new().expect("temp dir");
        let err = read_dbt_project_name_from_root(temp.path()).expect_err("should fail");
        assert!(matches!(err, AppError::NotDbtProjectRoot));
    }

    #[test]
    fn relative_project_root_uses_dot_for_repo_root() {
        let temp = TempDir::new().expect("temp dir");
        assert_eq!(relative_project_root(temp.path(), temp.path()), ".");
    }

    #[test]
    fn infers_remote_project_defaults_stably_from_repo_metadata() {
        let temp_a = TempDir::new().expect("temp dir");
        let temp_b = TempDir::new().expect("temp dir");
        let project_root_a = temp_a.path().join("analytics");
        let project_root_b = temp_b.path().join("analytics");
        std::fs::create_dir_all(&project_root_a).expect("project root a");
        std::fs::create_dir_all(&project_root_b).expect("project root b");
        std::fs::write(
            project_root_a.join("dbt_project.yml"),
            "name: jaffle_shop_project\n",
        )
        .expect("dbt project a");
        std::fs::write(
            project_root_b.join("dbt_project.yml"),
            "name: jaffle_shop_project\n",
        )
        .expect("dbt project b");

        run_git_cmd(["init"], temp_a.path());
        run_git_cmd(["init"], temp_b.path());
        run_git_cmd(
            ["remote", "add", "origin", "git@github.com:example/repo.git"],
            temp_a.path(),
        );
        run_git_cmd(
            ["remote", "add", "origin", "git@github.com:example/repo.git"],
            temp_b.path(),
        );

        let inferred_a =
            infer_remote_project_defaults(&project_root_a, None, None, None).expect("remote a");
        let inferred_b =
            infer_remote_project_defaults(&project_root_b, None, None, None).expect("remote b");

        assert_eq!(inferred_a.project_id, inferred_b.project_id);
        assert_eq!(inferred_a.project_root.as_deref(), Some("analytics"));
        assert_eq!(inferred_b.project_root.as_deref(), Some("analytics"));
    }

    #[test]
    fn rejects_invalid_remote_project_root() {
        assert!(matches!(
            validate_remote_project_root("/tmp/analytics"),
            Err(AppError::InvalidRemoteProjectRoot(_))
        ));
        assert!(matches!(
            validate_remote_project_root("../analytics"),
            Err(AppError::InvalidRemoteProjectRoot(_))
        ));
        assert!(validate_remote_project_root(".").is_ok());
        assert!(validate_remote_project_root("analytics").is_ok());
    }

    fn run_git_cmd<const N: usize>(args: [&str; N], cwd: &std::path::Path) {
        let output = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .expect("git command");
        assert!(
            output.status.success(),
            "git failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }
}
