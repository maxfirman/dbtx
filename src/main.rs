mod cli;
mod config;
mod db;
mod error;
mod event;
mod manifest;
mod profile;
mod services;

use clap::Parser;
use cli::{Cli, Command, EnvironmentCommand, ProjectCommand, StateCommand};
use config::RuntimeConfig;
use db::{Db, EnvironmentRecord, ProjectRecord};
use error::AppResult;
use services::{
    EnvironmentCreateRequest, EnvironmentService, EnvironmentUpdateRequest, InvocationCommand,
    InvocationObserver, InvocationRequest, InvocationService, ProjectInitRequest, ProjectService,
    ProjectUpdateRequest,
};
use std::ffi::OsString;
use std::io::IsTerminal;
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
                let config = RuntimeConfig::resolve(cli.database_url, Some(&current_dir))?;
                let db = connect_db(&config).await?;
                let applied = db.migrate().await?;
                print_migration_summary(&applied);
            }
        },
        Command::Project(project_command) => {
            handle_project_command(project_command, cli.database_url).await?
        }
        Command::Environment(environment_command) => {
            handle_environment_command(environment_command, cli.database_url).await?
        }
        Command::Build { args } => {
            handle_persisting_command(InvocationCommand::Build, args, cli.database_url).await?
        }
        Command::Run { args } => {
            handle_persisting_command(InvocationCommand::Run, args, cli.database_url).await?
        }
        Command::Ls { args } => handle_passthrough_command(args, cli.database_url).await?,
        Command::Test { args } => {
            handle_persisting_command(InvocationCommand::Test, args, cli.database_url).await?
        }
        Command::Seed { args } => {
            handle_persisting_command(InvocationCommand::Seed, args, cli.database_url).await?
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
    database_url_override: Option<String>,
) -> AppResult<()> {
    if is_help_request(&args) {
        exit_with_dbt_help(command.as_str())?;
    }
    let ctx = config::InvocationContext::from_args(&args, false)?;
    let project_dir = ctx.project_dir;
    let config = RuntimeConfig::resolve(database_url_override, Some(&project_dir))?;
    let db = connect_db(&config).await?;
    let service = InvocationService::new(&db);
    let mut observer = TerminalInvocationObserver;
    service
        .invoke(
            InvocationRequest {
                command,
                args,
                config,
            },
            &mut observer,
        )
        .await?;
    Ok(())
}

async fn handle_passthrough_command(
    args: Vec<OsString>,
    database_url_override: Option<String>,
) -> AppResult<()> {
    if is_help_request(&args) {
        exit_with_dbt_help(InvocationCommand::Ls.as_str())?;
    }
    let ctx = config::InvocationContext::from_args(&args, false)?;
    let project_dir = ctx.project_dir;
    let config = RuntimeConfig::resolve(database_url_override, Some(&project_dir))?;
    let db = connect_db(&config).await?;
    let service = InvocationService::new(&db);
    let mut observer = TerminalInvocationObserver;
    service
        .invoke(
            InvocationRequest {
                command: InvocationCommand::Ls,
                args,
                config,
            },
            &mut observer,
        )
        .await?;
    Ok(())
}

async fn connect_db(config: &RuntimeConfig) -> AppResult<Db> {
    Db::connect(&config.database_url).await
}

async fn handle_project_command(
    command: ProjectCommand,
    database_url_override: Option<String>,
) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = RuntimeConfig::resolve(database_url_override, Some(&current_dir))?;
    let db = connect_db(&config).await?;
    let service = ProjectService::new(&db);
    match command {
        ProjectCommand::Init {
            git_repo_url,
            project_root,
            default_branch,
            force,
        } => {
            let project = service
                .init(ProjectInitRequest {
                    current_dir,
                    git_repo_url,
                    project_root,
                    default_branch,
                    force,
                    database_url: config.database_url.clone(),
                })
                .await?;
            print_project(&project);
        }
        ProjectCommand::Update {
            git_repo_url,
            project_root,
            default_branch,
        } => {
            let project = service
                .update(ProjectUpdateRequest {
                    current_dir,
                    git_repo_url,
                    project_root,
                    default_branch,
                })
                .await?;
            print_project(&project);
        }
        ProjectCommand::List => {
            for project in service.list().await? {
                print_project(&project);
            }
        }
        ProjectCommand::Show { project } => {
            let project = service.show(&current_dir, project).await?;
            print_project(&project);
        }
    }
    Ok(())
}

async fn handle_environment_command(
    command: EnvironmentCommand,
    database_url_override: Option<String>,
) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = RuntimeConfig::resolve(database_url_override, Some(&current_dir))?;
    let db = connect_db(&config).await?;
    let service = EnvironmentService::new(&db);
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
            let environment = service
                .create(EnvironmentCreateRequest {
                    current_dir,
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
                .await?;
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
            let environment = service
                .update(EnvironmentUpdateRequest {
                    current_dir,
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
                })
                .await?;
            print_environment(&environment);
        }
        EnvironmentCommand::List { project } => {
            for environment in service.list(&current_dir, project).await? {
                print_environment(&environment);
            }
        }
        EnvironmentCommand::Show { project, slug } => {
            let environment = service.show(&current_dir, project, slug).await?;
            print_environment(&environment);
        }
    }
    Ok(())
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

struct TerminalInvocationObserver;

impl InvocationObserver for TerminalInvocationObserver {
    fn stdout_line(&mut self, line: &str) {
        println!("{line}");
    }

    fn stderr_line(&mut self, line: &str) {
        eprintln!("{line}");
    }
}

#[cfg(test)]
mod tests {
    use crate::config::{read_dbtx_project_id, write_dbtx_toml};
    use crate::error::AppError;
    use crate::services::{
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

        write_dbtx_toml(
            temp.path(),
            Some("prj_123"),
            Some("postgres://example/dbtx"),
            false,
        )
        .expect("write project id");
        assert_eq!(
            read_dbtx_project_id(temp.path()).expect("read project id"),
            Some("prj_123".to_string())
        );
    }

    #[test]
    fn write_dbtx_project_id_overwrites_existing_nested_value() {
        let temp = TempDir::new().expect("temp dir");
        std::fs::write(temp.path().join("dbt_project.yml"), "name: sample\n").expect("dbt project");
        write_dbtx_toml(
            temp.path(),
            Some("prj_old"),
            Some("postgres://example/dbtx"),
            false,
        )
        .expect("initial config");
        write_dbtx_toml(
            temp.path(),
            Some("prj_new"),
            Some("postgres://example/dbtx"),
            true,
        )
        .expect("overwrite project id");
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
