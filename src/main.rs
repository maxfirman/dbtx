mod cli;
mod config;
mod db;
mod error;
mod event;
mod manifest;

use clap::Parser;
use cli::{Cli, Command, EnvironmentCommand, ProjectCommand, StateCommand};
use config::{RuntimeConfig, read_dbtx_project_id, write_dbtx_toml};
use db::{
    CreateEnvironmentInput, CreateProjectInput, Db, EnvironmentRecord, ProjectRecord,
    UpdateEnvironmentInput,
};
use error::{AppError, AppResult};
use std::ffi::OsString;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::Command as StdCommand;
use uuid::Uuid;

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
            handle_persisting_command("build", args, cli.database_url).await?
        }
        Command::Run { args } => handle_persisting_command("run", args, cli.database_url).await?,
        Command::Ls { args } => handle_passthrough_command("ls", args, cli.database_url).await?,
        Command::Test { args } => handle_persisting_command("test", args, cli.database_url).await?,
        Command::Seed { args } => handle_persisting_command("seed", args, cli.database_url).await?,
        Command::Replay { run_id } => {
            let current_dir = std::env::current_dir()?;
            let config = RuntimeConfig::resolve(cli.database_url, Some(&current_dir))?;
            let db = connect_db(&config).await?;
            let updated = db.replay_projection(run_id).await?;
            println!("Rebuilt current state for {updated} nodes.");
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
    subcommand: &str,
    args: Vec<OsString>,
    database_url_override: Option<String>,
) -> AppResult<()> {
    if is_help_request(&args) {
        exit_with_dbt_help(subcommand)?;
    }
    let ctx = config::InvocationContext::from_args(&args, false)?;
    let project_dir = ctx.project_dir;
    let config = RuntimeConfig::resolve(database_url_override, Some(&project_dir))?;
    let db = connect_initialized_db(&config).await?;
    db.persisting_invocation(subcommand, &config, &args).await
}

async fn handle_passthrough_command(
    subcommand: &str,
    args: Vec<OsString>,
    database_url_override: Option<String>,
) -> AppResult<()> {
    if is_help_request(&args) {
        exit_with_dbt_help(subcommand)?;
    }
    let ctx = config::InvocationContext::from_args(&args, false)?;
    let project_dir = ctx.project_dir;
    let config = RuntimeConfig::resolve(database_url_override, Some(&project_dir))?;
    let db = connect_db(&config).await?;
    db.ls_invocation(&config, &args).await
}

async fn connect_db(config: &RuntimeConfig) -> AppResult<Db> {
    Db::connect(&config.database_url).await
}

async fn connect_initialized_db(config: &RuntimeConfig) -> AppResult<Db> {
    let db = connect_db(config).await?;
    db.init().await?;
    Ok(db)
}

async fn handle_project_command(
    command: ProjectCommand,
    database_url_override: Option<String>,
) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let config = RuntimeConfig::resolve(database_url_override, Some(&current_dir))?;
    let db = connect_initialized_db(&config).await?;
    match command {
        ProjectCommand::Init {
            git_repo_url,
            project_root,
            default_branch,
            force,
        } => {
            let inferred = infer_project_defaults(
                git_repo_url.as_deref(),
                project_root.as_deref(),
                default_branch.as_deref(),
            )?;
            let existing_project_id = read_dbtx_project_id(&current_dir)?;
            if let Some(existing_project_id) = existing_project_id.as_deref()
                && !force
            {
                return Err(AppError::ProjectIdAlreadyConfigured(
                    existing_project_id.to_string(),
                ));
            }
            let project_id = format!("prj_{}", Uuid::new_v4().simple());
            write_dbtx_toml(
                &current_dir,
                Some(&project_id),
                Some(&config.database_url),
                force,
            )?;
            let input = CreateProjectInput {
                project_id,
                project_name: inferred.project_name,
                git_repo_url: inferred.git_repo_url,
                default_branch: inferred.default_branch,
                project_root: inferred.project_root,
            };
            let project = if let Some(existing_project_id) = existing_project_id.as_deref() {
                db.reinitialize_project_id(existing_project_id, input)
                    .await?
            } else {
                db.create_project(input).await?
            };
            db.ensure_default_environment(&project.project_id).await?;
            print_project(&project);
        }
        ProjectCommand::Update {
            git_repo_url,
            project_root,
            default_branch,
        } => {
            let project_id =
                read_dbtx_project_id(&current_dir)?.ok_or(AppError::ProjectIdMissing)?;
            let inferred = infer_project_defaults(
                git_repo_url.as_deref(),
                project_root.as_deref(),
                default_branch.as_deref(),
            )?;
            let project = db
                .create_project(CreateProjectInput {
                    project_id,
                    project_name: inferred.project_name,
                    git_repo_url: inferred.git_repo_url,
                    default_branch: inferred.default_branch,
                    project_root: inferred.project_root,
                })
                .await?;
            print_project(&project);
        }
        ProjectCommand::List => {
            for project in db.list_projects().await? {
                print_project(&project);
            }
        }
        ProjectCommand::Show { project } => {
            let project = match project {
                Some(project) => project,
                None => {
                    let current_dir = std::env::current_dir()?;
                    read_dbtx_project_id(&current_dir)?.ok_or(AppError::ProjectIdMissing)?
                }
            };
            let project = db.get_project_by_project_id(&project).await?;
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
    let db = connect_initialized_db(&config).await?;
    match command {
        EnvironmentCommand::Create {
            project,
            slug,
            kind,
            baseline,
            git_branch,
            git_commit_sha,
            pr_number,
            immutable,
            status,
            schema_prefix,
        } => {
            let environment = db
                .create_environment(CreateEnvironmentInput {
                    project,
                    slug,
                    kind,
                    baseline_slug: baseline,
                    git_branch,
                    git_commit_sha,
                    pr_number,
                    immutable,
                    status,
                    schema_prefix,
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
            schema_prefix,
        } => {
            let environment = db
                .update_environment(UpdateEnvironmentInput {
                    project,
                    slug,
                    kind,
                    baseline_slug: baseline,
                    git_branch,
                    git_commit_sha,
                    pr_number,
                    immutable,
                    status,
                    schema_prefix,
                })
                .await?;
            print_environment(&environment);
        }
        EnvironmentCommand::List { project } => {
            for environment in db.list_environments(&project).await? {
                print_environment(&environment);
            }
        }
        EnvironmentCommand::Show { project, slug } => {
            let environment = db.get_environment(&project, &slug).await?;
            print_environment(&environment);
        }
        EnvironmentCommand::SeedFrom {
            project,
            target,
            source,
            seed_type,
        } => {
            db.seed_environment_from(&project, &target, &source, &seed_type)
                .await?;
            println!(
                "Seeded environment '{}' from '{}' in project '{}' via '{}'.",
                target, source, project, seed_type
            );
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
        "environment id={} project_pk={} project_id={} project={} slug={} kind={} baseline_id={} baseline={} git_branch={} git_commit_sha={} pr_number={} immutable={} status={} schema_prefix={} metadata={}",
        environment.id,
        environment.project_id,
        environment.project_ref,
        environment.project_name,
        environment.slug,
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
        environment.schema_prefix.as_deref().unwrap_or(""),
        environment.metadata,
    );
}

#[derive(Debug, PartialEq, Eq)]
struct InferredProjectInput {
    project_name: String,
    git_repo_url: String,
    default_branch: Option<String>,
    project_root: String,
}

fn infer_project_defaults(
    git_repo_url: Option<&str>,
    project_root: Option<&str>,
    default_branch: Option<&str>,
) -> AppResult<InferredProjectInput> {
    let current_dir = std::env::current_dir()?;
    let project_name = read_dbt_project_name_from_root(&current_dir)?;
    let repo_root = git_repo_root(&current_dir)?;

    Ok(InferredProjectInput {
        project_name,
        git_repo_url: git_repo_url
            .map(ToString::to_string)
            .or_else(|| git_remote_origin_url(&repo_root).ok())
            .ok_or(AppError::GitRemoteNotFound)?,
        default_branch: default_branch.map(ToString::to_string),
        project_root: project_root
            .map(ToString::to_string)
            .unwrap_or(relative_project_root(&repo_root, &current_dir)),
    })
}

fn read_dbt_project_name_from_root(project_root: &Path) -> AppResult<String> {
    let yaml = read_dbt_project_yaml(project_root)?;
    yaml.get("name")
        .and_then(serde_yaml::Value::as_str)
        .map(ToString::to_string)
        .or_else(|| {
            project_root
                .file_name()
                .map(|name| name.to_string_lossy().into_owned())
        })
        .ok_or(AppError::NotDbtProjectRoot)
}

fn git_repo_root(current_dir: &Path) -> AppResult<PathBuf> {
    let output = run_git(["rev-parse", "--show-toplevel"], current_dir)?;
    Ok(PathBuf::from(output))
}

fn git_remote_origin_url(repo_root: &Path) -> AppResult<String> {
    run_git(["config", "--get", "remote.origin.url"], repo_root)
        .map_err(|_| AppError::GitRemoteNotFound)
}

fn relative_project_root(repo_root: &Path, project_root: &Path) -> String {
    match project_root.strip_prefix(repo_root) {
        Ok(path) if path.as_os_str().is_empty() => ".".to_string(),
        Ok(path) => path.to_string_lossy().into_owned(),
        Err(_) => project_root.to_string_lossy().into_owned(),
    }
}

fn read_dbt_project_yaml(project_root: &Path) -> AppResult<serde_yaml::Value> {
    let path = project_root.join("dbt_project.yml");
    if !path.is_file() {
        return Err(AppError::NotDbtProjectRoot);
    }
    let content = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&content)?)
}

fn run_git<const N: usize>(args: [&str; N], cwd: &Path) -> AppResult<String> {
    let output = StdCommand::new("git")
        .args(args)
        .current_dir(cwd)
        .output()?;
    if !output.status.success() {
        return Err(AppError::GitRepoNotFound);
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        return Err(AppError::GitRepoNotFound);
    }
    Ok(stdout)
}

#[cfg(test)]
mod tests {
    use super::{infer_project_defaults, read_dbt_project_name_from_root, relative_project_root};
    use crate::config::{read_dbtx_project_id, write_dbtx_toml};
    use crate::error::AppError;
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

        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&project_root).expect("set cwd");
        let inferred = infer_project_defaults(None, None, None).expect("inferred");
        std::env::set_current_dir(original_dir).expect("restore cwd");

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
