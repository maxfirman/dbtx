use clap::Parser;
use dbtx::api;
use dbtx::api::{
    InvocationCancelStateApi, InvocationExecutionModeApi, InvocationLifecycleStatus,
    InvocationListApiRequest, QueueStatusResponse, WorkerStatusResponse,
};
use dbtx::cli::{
    Cli, Command, EnvironmentCommand, InvocationCommand as InvocationCliCommand, ProjectCommand,
    QueueCommand, StateCommand, WorkerCommand,
};
use dbtx::client;
use dbtx::config::{self, resolve_service_url};
use dbtx::db::{self, EnvironmentRecord, EnvironmentVersionRecord, ProjectRecord};
use dbtx::error::{AppError, AppResult};
use dbtx::services::InvocationCommand;
use std::ffi::OsString;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err}");
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

#[derive(Clone, Copy)]
enum CliStyle {
    Green,
    Yellow,
    Cyan,
    Bold,
    Dim,
}

fn print_migration_summary(applied: &[db::AppliedMigration]) {
    let use_color = should_use_color();
    println!(
        "{}",
        style(
            "dbtx migrations",
            &[CliStyle::Cyan, CliStyle::Bold],
            use_color
        )
    );
    if applied.is_empty() {
        println!(
            "{}",
            style("  No pending migrations.", &[CliStyle::Dim], use_color)
        );
        return;
    }

    for migration in applied {
        println!(
            "  {} {}",
            style("Applied", &[CliStyle::Green, CliStyle::Bold], use_color),
            style(
                &format!("{} {}", migration.version, migration.description),
                &[CliStyle::Bold],
                use_color,
            )
        );
    }
    println!(
        "{}",
        style(
            &format!("  {} migration(s) applied.", applied.len()),
            &[CliStyle::Yellow, CliStyle::Bold],
            use_color,
        )
    );
}

fn should_use_color() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if matches!(std::env::var("TERM").ok().as_deref(), Some("dumb")) {
        return false;
    }
    if matches!(std::env::var("CLICOLOR_FORCE").ok().as_deref(), Some("1")) {
        return true;
    }
    std::io::stdout().is_terminal()
}

fn style(input: &str, styles: &[CliStyle], use_color: bool) -> String {
    if !use_color || styles.is_empty() {
        return input.to_string();
    }
    let prefix = styles
        .iter()
        .map(|style| match style {
            CliStyle::Green => "32",
            CliStyle::Yellow => "33",
            CliStyle::Cyan => "36",
            CliStyle::Bold => "1",
            CliStyle::Dim => "2",
        })
        .collect::<Vec<_>>()
        .join(";");
    format!("\u{1b}[{prefix}m{input}\u{1b}[0m")
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
        ProjectCommand::Init {
            mode,
            git_repo_url,
            project_root,
            default_branch,
            force,
        } => {
            let project = client
                .project_init(api::ProjectInitApiRequest {
                    current_dir: current_dir.display().to_string(),
                    mode,
                    git_repo_url,
                    project_root,
                    default_branch,
                    force,
                })
                .await?
                .project;
            print_project(&project);
        }
        ProjectCommand::Update {
            mode,
            git_repo_url,
            project_root,
            default_branch,
        } => {
            let project_id = client
                .project_show_with_context(&current_dir, None)
                .await?
                .project
                .project_id;
            let project = client
                .project_update(
                    &project_id,
                    api::ProjectUpdateApiRequest {
                        current_dir: current_dir.display().to_string(),
                        mode,
                        git_repo_url,
                        project_root,
                        default_branch,
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
            let project = if let Some(project_id) = project.clone() {
                client.project_show_by_id(&project_id).await?.project
            } else {
                client
                    .project_show_with_context(&current_dir, project)
                    .await?
                    .project
            };
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
        EnvironmentCommand::Create {
            project,
            slug,
            target,
            baseline,
            git_branch,
            git_commit_sha,
            pr_number,
            status,
            worker_queue,
            schema_name,
        } => {
            let environment = client
                .environment_create(api::EnvironmentCreateApiRequest {
                    current_dir: current_dir.display().to_string(),
                    project,
                    slug,
                    target,
                    baseline,
                    git_branch,
                    git_commit_sha,
                    pr_number,
                    status,
                    worker_queue,
                    schema_name,
                })
                .await?
                .environment;
            print_environment(&environment);
        }
        EnvironmentCommand::Update {
            project,
            slug,
            baseline,
            git_branch,
            git_commit_sha,
            pr_number,
            status,
            adapter_type,
            worker_queue,
            schema_name,
            threads,
        } => {
            let environment = client
                .environment_update(
                    &project,
                    &slug,
                    api::EnvironmentUpdateApiRequest {
                        current_dir: current_dir.display().to_string(),
                        project: project.clone(),
                        slug: slug.clone(),
                        baseline,
                        git_branch,
                        git_commit_sha,
                        pr_number,
                        status,
                        adapter_type,
                        worker_queue,
                        schema_name,
                        threads,
                    },
                )
                .await?
                .environment;
            print_environment(&environment);
        }
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
        } => {
            let environment = client
                .environment_release(
                    &project,
                    &slug,
                    api::EnvironmentReleaseApiRequest {
                        current_dir: current_dir.display().to_string(),
                        project: project.clone(),
                        slug: slug.clone(),
                        git_branch,
                        git_commit_sha,
                    },
                )
                .await?
                .environment;
            print_environment(&environment);
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
                    api::EnvironmentRollbackApiRequest {
                        current_dir: current_dir.display().to_string(),
                        project: project.clone(),
                        slug: slug.clone(),
                        version_id,
                    },
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
        InvocationExecutionModeApi::Server,
        None,
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

fn render_invocation_event(event: api::InvocationEvent) {
    match event.stream.as_deref() {
        Some("stderr") => {
            if let Some(text) = event.text {
                eprintln!("{text}");
            }
        }
        _ => {
            if let Some(text) = event.text {
                println!("{text}");
            }
        }
    }
    if event.event_type == "invocation.completed"
        && let Some(error) = event.error
    {
        eprintln!("{error}");
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
    let queue = format!("local-{}", uuid::Uuid::new_v4().simple());
    let response = create_invocation(
        service_url.clone(),
        command,
        args,
        ctx,
        InvocationExecutionModeApi::Local,
        Some(queue.clone()),
    )
    .await?;
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
        .arg(queue)
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
    execution_mode: InvocationExecutionModeApi,
    worker_queue: Option<String>,
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
            },
            args: args
                .into_iter()
                .map(|value| value.to_string_lossy().into_owned())
                .collect(),
            current_dir: Some(ctx.project_dir.display().to_string()),
            project_id: None,
            environment_slug: Some(ctx.target_name.clone().unwrap_or_default()),
            execution_mode,
            worker_queue,
        })
        .await
}

fn print_project(project: &ProjectRecord) {
    println!(
        "project id={} project_id={} project_name={} mode={} git_repo_url={} default_branch={} project_root={} metadata={}",
        project.id,
        project.project_id,
        project.project_name,
        project.mode,
        project.git_repo_url.as_deref().unwrap_or(""),
        project.default_branch.as_deref().unwrap_or(""),
        project.project_root.as_deref().unwrap_or(""),
        project.metadata,
    );
}

fn print_environment(environment: &EnvironmentRecord) {
    println!(
        "environment id={} project_pk={} project_id={} project={} slug={} profile_name={} target_name={} baseline_id={} baseline={} git_branch={} git_commit_sha={} pr_number={} status={} adapter_type={} worker_queue={} schema_name={} threads={} profile_config={} metadata={}",
        environment.id,
        environment.project_id,
        environment.project_ref,
        environment.project_name,
        environment.slug,
        environment.profile_name,
        environment.target_name,
        environment
            .baseline_environment_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
        environment
            .baseline_environment_slug
            .as_deref()
            .unwrap_or(""),
        environment.git_branch.as_deref().unwrap_or(""),
        environment.git_commit_sha.as_deref().unwrap_or(""),
        environment
            .pr_number
            .map(|value| value.to_string())
            .unwrap_or_default(),
        environment.status,
        environment.adapter_type,
        environment.worker_queue,
        environment.schema_name,
        environment
            .threads
            .map(|v| v.to_string())
            .unwrap_or_default(),
        environment.profile_config,
        environment.metadata,
    );
}

fn print_environment_version(version: &EnvironmentVersionRecord) {
    println!(
        "environment_version id={} environment_id={} project_id={} recorded_at={} reason={} git_branch={} git_commit_sha={} baseline_environment_id={} metadata={}",
        version.id,
        version.environment_id,
        version.project_id,
        version.recorded_at.to_rfc3339(),
        version.reason,
        version.git_branch.as_deref().unwrap_or(""),
        version.git_commit_sha.as_deref().unwrap_or(""),
        version
            .baseline_environment_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
        version.metadata,
    );
}

fn print_invocation(invocation: &api::InvocationStatusResponse) {
    println!(
        "invocation id={} mode={:?} worker_queue={} worker_health={:?} cancel_state={:?} status={:?} exit_code={} claimed_by={} claimed_at={} last_heartbeat_at={} cancel_requested_at={} started_at={} completed_at={} cancel_requested={} error={}",
        invocation.invocation_id,
        invocation.execution_mode,
        invocation.worker_queue,
        invocation.worker_health,
        invocation.cancel_state,
        invocation.status,
        invocation
            .exit_code
            .map(|value| value.to_string())
            .unwrap_or_default(),
        invocation.claimed_by.as_deref().unwrap_or(""),
        invocation
            .claimed_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
        invocation
            .last_heartbeat_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
        invocation
            .cancel_requested_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
        invocation.started_at.to_rfc3339(),
        invocation
            .completed_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
        invocation.cancel_requested,
        invocation.error.as_deref().unwrap_or(""),
    );
}

fn print_worker(worker: &WorkerStatusResponse) {
    println!(
        "worker id={} mode={:?} worker_queue={} claimed_invocation_count={} last_heartbeat_at={} health={:?}",
        worker.worker_id,
        worker.execution_mode,
        worker.worker_queue,
        worker.claimed_invocation_count,
        worker
            .last_heartbeat_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
        worker.health,
    );
}

fn print_queue(queue: &QueueStatusResponse) {
    println!(
        "queue worker_queue={} mode={:?} pending_count={} claimed_count={} stale_claim_count={} oldest_pending_at={}",
        queue.worker_queue,
        queue.execution_mode,
        queue.pending_count,
        queue.claimed_count,
        queue.stale_claim_count,
        queue
            .oldest_pending_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
    );
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
