use clap::Parser;
use dbtx::api;
use dbtx::api::InvocationExecutionModeApi;
use dbtx::cli::{Cli, Command, EnvironmentCommand, ProjectCommand, StateCommand};
use dbtx::client;
use dbtx::config::{self, resolve_service_url};
use dbtx::db::{self, EnvironmentRecord, ProjectRecord};
use dbtx::error::{AppError, AppResult};
use dbtx::services::InvocationCommand;
use std::ffi::OsString;
use std::io::IsTerminal;
use std::path::Path;
use std::process::Command as StdCommand;
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;

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
            git_repo_url,
            project_root,
            default_branch,
            force,
        } => {
            let project = client
                .project_init(api::ProjectInitApiRequest {
                    current_dir: current_dir.display().to_string(),
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
            git_repo_url,
            project_root,
            default_branch,
        } => {
            let project_id =
                config::read_dbtx_project_id(&current_dir)?.ok_or(AppError::ProjectIdMissing)?;
            let project = client
                .project_update(
                    &project_id,
                    api::ProjectUpdateApiRequest {
                        current_dir: current_dir.display().to_string(),
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
            kind,
            baseline,
            git_branch,
            git_commit_sha,
            pr_number,
            immutable,
            status,
            schema_name,
        } => {
            let environment = client
                .environment_create(api::EnvironmentCreateApiRequest {
                    current_dir: current_dir.display().to_string(),
                    project,
                    slug,
                    target,
                    kind,
                    baseline,
                    git_branch,
                    git_commit_sha,
                    pr_number,
                    immutable,
                    status,
                    schema_name,
                })
                .await?
                .environment;
            print_environment(&environment);
        }
        EnvironmentCommand::Update {
            project,
            slug,
            kind,
            baseline,
            git_branch,
            git_commit_sha,
            pr_number,
            immutable,
            status,
            adapter_type,
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
                        kind,
                        baseline,
                        git_branch,
                        git_commit_sha,
                        pr_number,
                        immutable,
                        status,
                        adapter_type,
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
    )
    .await?;
    let client = client::DaemonClient::new(service_url);
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
    let response = create_invocation(
        service_url.clone(),
        command,
        args,
        ctx,
        InvocationExecutionModeApi::Local,
    )
    .await?;
    let client = client::DaemonClient::new(service_url);
    let claimed = client.invocation_claim(response.invocation_id).await?;
    let spec = claimed.execution_spec;
    let profiles_dir = write_profiles_dir(&spec.profiles_yml)?;
    let state_dir = write_state_dir(spec.state_manifest.as_ref())?;

    let mut dbt_args: Vec<OsString> = spec.args.into_iter().map(Into::into).collect();
    if let Some(state_dir) = state_dir.as_ref() {
        dbt_args.push("--state".into());
        dbt_args.push(state_dir.path().as_os_str().to_os_string());
    }
    dbt_args.push("--profiles-dir".into());
    dbt_args.push(profiles_dir.path().as_os_str().to_os_string());

    let mut child =
        TokioCommand::new(std::env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string()))
            .arg(command.as_str())
            .args(&dbt_args)
            .current_dir(&ctx.project_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stdout")))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stderr")))?;

    let mut stdout_reader = BufReader::new(stdout).lines();
    let stderr_handle = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        let mut lines = Vec::new();
        while let Some(line) = reader.next_line().await? {
            lines.push(line);
        }
        Result::<Vec<String>, std::io::Error>::Ok(lines)
    });
    let mut dbt_version: Option<String> = None;

    while let Some(line) = stdout_reader.next_line().await? {
        if command.persists_state()
            && let Some(event) = dbtx::event::LogEvent::parse(&line)
        {
            if dbt_version.is_none() && event.info.name == "MainReportVersion" {
                dbt_version = event
                    .data
                    .get("version")
                    .and_then(serde_json::Value::as_str)
                    .map(ToString::to_string);
            }
            if let Some(rendered) = event.render_text_line() {
                println!("{rendered}");
            }
            client
                .invocation_append_events(
                    response.invocation_id,
                    api::InvocationEventBatchApiRequest {
                        events: vec![dbtx::execution::ExecutionEvent {
                            kind: dbtx::execution::ExecutionEventKind::DbtLog,
                            occurred_at: chrono::Utc::now(),
                            text: event.render_text_line(),
                            raw_line: Some(line),
                            dbt_event_name: Some(event.info.name.clone()),
                            node_unique_id: event
                                .data
                                .get("node_info")
                                .and_then(|value| value.get("unique_id"))
                                .and_then(|value| value.as_str())
                                .map(ToString::to_string),
                            level: Some(event.info.level.clone()),
                            error: None,
                        }],
                    },
                )
                .await?;
        } else {
            println!("{line}");
            client
                .invocation_append_events(
                    response.invocation_id,
                    api::InvocationEventBatchApiRequest {
                        events: vec![dbtx::execution::ExecutionEvent {
                            kind: dbtx::execution::ExecutionEventKind::StdoutLine,
                            occurred_at: chrono::Utc::now(),
                            text: Some(line.clone()),
                            raw_line: Some(line),
                            dbt_event_name: None,
                            node_unique_id: None,
                            level: None,
                            error: None,
                        }],
                    },
                )
                .await?;
        }
    }

    let status = child.wait().await?;
    for line in stderr_handle.await.map_err(|err| {
        AppError::Io(std::io::Error::other(format!("stderr task failed: {err}")))
    })?? {
        eprintln!("{line}");
        client
            .invocation_append_events(
                response.invocation_id,
                api::InvocationEventBatchApiRequest {
                    events: vec![dbtx::execution::ExecutionEvent {
                        kind: dbtx::execution::ExecutionEventKind::StderrLine,
                        occurred_at: chrono::Utc::now(),
                        text: Some(line.clone()),
                        raw_line: Some(line),
                        dbt_event_name: None,
                        node_unique_id: None,
                        level: None,
                        error: None,
                    }],
                },
            )
            .await?;
    }

    let exit_code = status.code().unwrap_or(1);
    let manifest = if command.persists_state() {
        let manifest_path = ctx.target_path.join("manifest.json");
        std::fs::read_to_string(&manifest_path)
            .ok()
            .and_then(|content| serde_json::from_str(&content).ok())
    } else {
        None
    };
    client
        .invocation_complete(
            response.invocation_id,
            api::InvocationCompleteApiRequest {
                completion: dbtx::execution::ExecutionCompletion {
                    status: if status.success() {
                        api::InvocationLifecycleStatus::Succeeded
                    } else {
                        api::InvocationLifecycleStatus::Failed
                    },
                    exit_code,
                    error: (!status.success())
                        .then(|| format!("dbt invocation failed with exit code {exit_code}")),
                    dbt_version,
                    manifest,
                },
            },
        )
        .await?;

    if status.success() {
        Ok(())
    } else {
        Err(AppError::DbtFailed(exit_code))
    }
}

async fn create_invocation(
    service_url: String,
    command: InvocationCommand,
    args: Vec<OsString>,
    ctx: &config::InvocationContext,
    execution_mode: InvocationExecutionModeApi,
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
            current_dir: ctx.project_dir.display().to_string(),
            environment_slug: ctx.environment_slug.clone(),
            execution_mode,
        })
        .await
}

fn write_profiles_dir(profiles_yml: &str) -> AppResult<TempDir> {
    let temp_dir = TempDir::new()?;
    std::fs::write(temp_dir.path().join("profiles.yml"), profiles_yml)?;
    Ok(temp_dir)
}

fn write_state_dir(state_manifest: Option<&serde_json::Value>) -> AppResult<Option<TempDir>> {
    let Some(state_manifest) = state_manifest else {
        return Ok(None);
    };
    let temp_dir = TempDir::new()?;
    std::fs::write(
        temp_dir.path().join("manifest.json"),
        serde_json::to_vec(state_manifest)?,
    )?;
    Ok(Some(temp_dir))
}

fn print_project(project: &ProjectRecord) {
    println!(
        "project id={} project_id={} project_name={} git_repo_url={} default_branch={} project_root={} metadata={}",
        project.id,
        project.project_id,
        project.project_name,
        project.git_repo_url.as_deref().unwrap_or(""),
        project.default_branch.as_deref().unwrap_or(""),
        project.project_root.as_deref().unwrap_or(""),
        project.metadata,
    );
}

fn print_environment(environment: &EnvironmentRecord) {
    println!(
        "environment id={} project_pk={} project_id={} project={} slug={} target_name={} kind={} baseline_id={} baseline={} git_branch={} git_commit_sha={} pr_number={} immutable={} status={} adapter_type={} schema_name={} threads={} profile_config={} metadata={}",
        environment.id,
        environment.project_id,
        environment.project_ref,
        environment.project_name,
        environment.slug,
        environment.target_name,
        environment.kind,
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
        environment.immutable,
        environment.status,
        environment.adapter_type,
        environment.schema_name,
        environment
            .threads
            .map(|v| v.to_string())
            .unwrap_or_default(),
        environment.profile_config,
        environment.metadata,
    );
}

#[cfg(test)]
mod tests {
    use dbtx::config::{read_dbtx_project_id, write_dbtx_toml};
    use dbtx::error::AppError;
    use dbtx::services::{
        infer_project_defaults, read_dbt_project_name_from_root, relative_project_root,
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

        let inferred = infer_project_defaults(&project_root, None, None, None).expect("inferred");

        assert_eq!(inferred.project_name, "jaffle_shop_project");
        assert_eq!(inferred.git_repo_url, "git@github.com:example/repo.git");
        assert_eq!(inferred.project_root, "analytics");
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
    fn write_and_read_dbtx_project_id_round_trip() {
        let temp = TempDir::new().expect("temp dir");
        std::fs::write(
            temp.path().join("dbt_project.yml"),
            "name: sample\nversion: '1.0'\n",
        )
        .expect("dbt project");

        write_dbtx_toml(temp.path(), Some("prj_123"), false).expect("write project id");
        assert_eq!(
            read_dbtx_project_id(temp.path()).expect("read project id"),
            Some("prj_123".to_string())
        );
    }

    #[test]
    fn write_dbtx_project_id_overwrites_existing_nested_value() {
        let temp = TempDir::new().expect("temp dir");
        std::fs::write(temp.path().join("dbt_project.yml"), "name: sample\n").expect("dbt project");
        write_dbtx_toml(temp.path(), Some("prj_old"), false).expect("initial config");
        write_dbtx_toml(temp.path(), Some("prj_new"), true).expect("overwrite project id");
        assert_eq!(
            read_dbtx_project_id(temp.path()).expect("read project id"),
            Some("prj_new".to_string())
        );
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
