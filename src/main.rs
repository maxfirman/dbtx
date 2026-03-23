mod cli;
mod config;
mod db;
mod error;
mod event;
mod manifest;

use clap::Parser;
use cli::{Cli, Command, EnvironmentCommand, ProjectCommand, StateCommand};
use config::RuntimeConfig;
use db::{CreateEnvironmentInput, CreateProjectInput, Db, EnvironmentRecord, ProjectRecord};
use error::{AppError, AppResult};
use std::ffi::OsString;
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
            StateCommand::Init { database_url } => {
                let _db =
                    connect_initialized_db(&RuntimeConfig::from_optional_database_url(database_url)?)
                        .await?;
                println!("Initialized dbtx database schema.");
            }
        },
        Command::Project(project_command) => handle_project_command(project_command).await?,
        Command::Environment(environment_command) => {
            handle_environment_command(environment_command).await?
        }
        Command::Build { args } => handle_persisting_command("build", args).await?,
        Command::Run { args } => handle_persisting_command("run", args).await?,
        Command::Ls { args } => handle_passthrough_command("ls", args).await?,
        Command::Test { args } => handle_persisting_command("test", args).await?,
        Command::Seed { args } => handle_persisting_command("seed", args).await?,
        Command::Replay { run_id } => {
            let config = RuntimeConfig::from_env()?;
            let db = connect_db(&config).await?;
            let updated = db.replay_projection(run_id).await?;
            println!("Rebuilt current state for {updated} nodes.");
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

async fn handle_persisting_command(subcommand: &str, args: Vec<OsString>) -> AppResult<()> {
    if is_help_request(&args) {
        exit_with_dbt_help(subcommand)?;
    }
    let config = RuntimeConfig::from_env()?;
    let db = connect_initialized_db(&config).await?;
    db.persisting_invocation(subcommand, &config, &args).await
}

async fn handle_passthrough_command(subcommand: &str, args: Vec<OsString>) -> AppResult<()> {
    if is_help_request(&args) {
        exit_with_dbt_help(subcommand)?;
    }
    let config = RuntimeConfig::from_env()?;
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

async fn handle_project_command(command: ProjectCommand) -> AppResult<()> {
    let config = RuntimeConfig::from_env()?;
    let db = connect_initialized_db(&config).await?;
    match command {
        ProjectCommand::Init {
            slug,
            git_repo_url,
            project_root,
            default_branch,
            force,
        } => {
            let current_dir = std::env::current_dir()?;
            let inferred = infer_project_defaults(
                slug.as_deref(),
                git_repo_url.as_deref(),
                project_root.as_deref(),
                default_branch.as_deref(),
            )?;
            let existing_project_id = read_dbtx_project_id(&current_dir)?;
            if let Some(existing_project_id) = existing_project_id.as_deref()
                && !force
            {
                return Err(AppError::ProjectIdAlreadyConfigured(existing_project_id.to_string()));
            }
            let project_id = format!("prj_{}", Uuid::new_v4().simple());
            write_dbtx_project_id(&current_dir, &project_id, force)?;
            let input = CreateProjectInput {
                project_id,
                project_name: inferred.project_name,
                slug: inferred.slug,
                git_repo_url: inferred.git_repo_url,
                default_branch: inferred.default_branch,
                project_root: inferred.project_root,
            };
            let project = if let Some(existing_project_id) = existing_project_id.as_deref() {
                db.reinitialize_project_id(existing_project_id, input).await?
            } else {
                db.create_project(input).await?
            };
            print_project(&project);
        }
        ProjectCommand::Update {
            slug,
            git_repo_url,
            project_root,
            default_branch,
        } => {
            let current_dir = std::env::current_dir()?;
            let project_id = read_dbtx_project_id(&current_dir)?.ok_or(AppError::ProjectIdMissing)?;
            let inferred = infer_project_defaults(
                slug.as_deref(),
                git_repo_url.as_deref(),
                project_root.as_deref(),
                default_branch.as_deref(),
            )?;
            let project = db
                .create_project(CreateProjectInput {
                    project_id,
                    project_name: inferred.project_name,
                    slug: inferred.slug,
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
            let project = db.get_project_by_identifier(&project).await?;
            print_project(&project);
        }
    }
    Ok(())
}

async fn handle_environment_command(command: EnvironmentCommand) -> AppResult<()> {
    let config = RuntimeConfig::from_env()?;
    let db = connect_initialized_db(&config).await?;
    match command {
        EnvironmentCommand::Create {
            project,
            slug,
            kind,
            baseline,
            git_ref,
            pr_number,
            protected,
            status,
            schema_prefix,
        } => {
            let environment = db
                .create_environment(CreateEnvironmentInput {
                    project,
                    slug,
                    kind,
                    baseline_slug: baseline,
                    git_ref,
                    pr_number,
                    protected,
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
        "project id={} project_id={} project_name={} slug={} git_repo_url={} default_branch={} project_root={} metadata={}",
        project.id,
        project.project_id,
        project.project_name,
        project.slug,
        project.git_repo_url.as_deref().unwrap_or(""),
        project.default_branch.as_deref().unwrap_or(""),
        project.project_root.as_deref().unwrap_or(""),
        project.metadata,
    );
}

fn print_environment(environment: &EnvironmentRecord) {
    println!(
        "environment id={} project_pk={} project_id={} project={} slug={} kind={} baseline_id={} baseline={} git_ref={} pr_number={} protected={} status={} schema_prefix={} metadata={}",
        environment.id,
        environment.project_id,
        environment.project_ref,
        environment.project_slug,
        environment.slug,
        environment.kind,
        environment
            .baseline_environment_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
        environment.baseline_environment_slug.as_deref().unwrap_or(""),
        environment.git_ref.as_deref().unwrap_or(""),
        environment
            .pr_number
            .map(|value| value.to_string())
            .unwrap_or_default(),
        environment.protected,
        environment.status,
        environment.schema_prefix.as_deref().unwrap_or(""),
        environment.metadata,
    );
}

#[derive(Debug, PartialEq, Eq)]
struct InferredProjectInput {
    project_name: String,
    slug: String,
    git_repo_url: String,
    default_branch: Option<String>,
    project_root: String,
}

fn infer_project_defaults(
    slug: Option<&str>,
    git_repo_url: Option<&str>,
    project_root: Option<&str>,
    default_branch: Option<&str>,
) -> AppResult<InferredProjectInput> {
    let current_dir = std::env::current_dir()?;
    let project_name = read_dbt_project_name_from_root(&current_dir)?;
    let repo_root = git_repo_root(&current_dir)?;

    Ok(InferredProjectInput {
        project_name: project_name.clone(),
        slug: slug
            .map(ToString::to_string)
            .unwrap_or_else(|| project_name.replace(' ', "-")),
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
    let path = project_root.join("dbt_project.yml");
    if !path.is_file() {
        return Err(AppError::NotDbtProjectRoot);
    }

    let content = std::fs::read_to_string(path)?;
    for line in content.lines() {
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("name:") {
            let value = rest.trim().trim_matches('"').trim_matches('\'');
            if !value.is_empty() {
                return Ok(value.to_string());
            }
        }
    }

    project_root
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
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

fn read_dbtx_project_id(project_root: &Path) -> AppResult<Option<String>> {
    let path = project_root.join("dbt_project.yml");
    if !path.is_file() {
        return Err(AppError::NotDbtProjectRoot);
    }

    let content = std::fs::read_to_string(path)?;
    let mut in_vars = false;
    let mut vars_indent = 0usize;
    let mut in_dbtx = false;
    let mut dbtx_indent = 0usize;

    for line in content.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let indent = line.len() - line.trim_start().len();
        if in_dbtx && indent <= dbtx_indent {
            in_dbtx = false;
        }
        if in_vars && indent <= vars_indent && trimmed != "vars:" {
            in_vars = false;
            in_dbtx = false;
        }

        if trimmed == "vars:" {
            in_vars = true;
            vars_indent = indent;
            in_dbtx = false;
            continue;
        }

        if in_vars && trimmed == "dbtx:" {
            in_dbtx = true;
            dbtx_indent = indent;
            continue;
        }

        if in_dbtx
            && let Some(rest) = trimmed.strip_prefix("project_id:")
        {
            let value = rest.trim().trim_matches('"').trim_matches('\'');
            if !value.is_empty() {
                return Ok(Some(value.to_string()));
            }
        }
    }

    Ok(None)
}

fn write_dbtx_project_id(project_root: &Path, project_id: &str, force: bool) -> AppResult<()> {
    let path = project_root.join("dbt_project.yml");
    if !path.is_file() {
        return Err(AppError::NotDbtProjectRoot);
    }

    let existing = read_dbtx_project_id(project_root)?;
    if let Some(existing) = existing
        && !force
    {
        return Err(AppError::ProjectIdAlreadyConfigured(existing));
    }

    let mut lines = std::fs::read_to_string(&path)?
        .lines()
        .map(ToString::to_string)
        .collect::<Vec<_>>();

    if replace_existing_project_id(&mut lines, project_id)
        || insert_into_existing_dbtx_block(&mut lines, project_id)
        || insert_into_existing_vars_block(&mut lines, project_id)
    {
        std::fs::write(path, format!("{}\n", lines.join("\n")))?;
        return Ok(());
    }

    if !lines.is_empty() && !lines.last().is_some_and(String::is_empty) {
        lines.push(String::new());
    }
    lines.push("vars:".to_string());
    lines.push("  dbtx:".to_string());
    lines.push(format!("    project_id: {project_id}"));
    std::fs::write(path, format!("{}\n", lines.join("\n")))?;
    Ok(())
}

fn replace_existing_project_id(lines: &mut [String], project_id: &str) -> bool {
    let mut in_vars = false;
    let mut vars_indent = 0usize;
    let mut in_dbtx = false;
    let mut dbtx_indent = 0usize;

    for line in lines.iter_mut() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        let indent = line.len() - line.trim_start().len();
        if in_dbtx && indent <= dbtx_indent {
            in_dbtx = false;
        }
        if in_vars && indent <= vars_indent && trimmed != "vars:" {
            in_vars = false;
            in_dbtx = false;
        }
        if trimmed == "vars:" {
            in_vars = true;
            vars_indent = indent;
            in_dbtx = false;
            continue;
        }
        if in_vars && trimmed == "dbtx:" {
            in_dbtx = true;
            dbtx_indent = indent;
            continue;
        }
        if in_dbtx && trimmed.starts_with("project_id:") {
            *line = format!("{}project_id: {project_id}", " ".repeat(indent));
            return true;
        }
    }

    false
}

fn insert_into_existing_dbtx_block(lines: &mut Vec<String>, project_id: &str) -> bool {
    let mut in_vars = false;
    let mut vars_indent = 0usize;
    let mut dbtx_index = None;
    let mut dbtx_indent = 0usize;

    for idx in 0..lines.len() {
        let trimmed = lines[idx].trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let indent = lines[idx].len() - lines[idx].trim_start().len();
        if dbtx_index.is_some()
            && indent <= dbtx_indent
        {
            lines.insert(idx, format!("{}project_id: {project_id}", " ".repeat(dbtx_indent + 2)));
            return true;
        }
        if in_vars && indent <= vars_indent && trimmed != "vars:" {
            in_vars = false;
            dbtx_index = None;
        }
        if trimmed == "vars:" {
            in_vars = true;
            vars_indent = indent;
            dbtx_index = None;
            continue;
        }
        if in_vars && trimmed == "dbtx:" {
            dbtx_index = Some(idx);
            dbtx_indent = indent;
        }
    }

    if let Some(start_idx) = dbtx_index {
        lines.insert(
            start_idx + 1,
            format!("{}project_id: {project_id}", " ".repeat(dbtx_indent + 2)),
        );
        return true;
    }

    false
}

fn insert_into_existing_vars_block(lines: &mut Vec<String>, project_id: &str) -> bool {
    for idx in 0..lines.len() {
        if lines[idx].trim() == "vars:" {
            let vars_indent = lines[idx].len() - lines[idx].trim_start().len();
            let mut insert_at = idx + 1;
            while insert_at < lines.len() {
                let next = lines[insert_at].trim();
                if !next.is_empty() && !next.starts_with('#') {
                    let indent = lines[insert_at].len() - lines[insert_at].trim_start().len();
                    if indent <= vars_indent {
                        break;
                    }
                }
                insert_at += 1;
            }
            lines.insert(insert_at, format!("{}dbtx:", " ".repeat(vars_indent + 2)));
            lines.insert(
                insert_at + 1,
                format!("{}project_id: {project_id}", " ".repeat(vars_indent + 4)),
            );
            return true;
        }
    }
    false
}

fn run_git<const N: usize>(args: [&str; N], cwd: &Path) -> AppResult<String> {
    let output = StdCommand::new("git").args(args).current_dir(cwd).output()?;
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
    use super::{
        infer_project_defaults, read_dbt_project_name_from_root, read_dbtx_project_id,
        relative_project_root, replace_existing_project_id, write_dbtx_project_id,
    };
    use crate::error::AppError;
    use std::process::Command;
    use tempfile::TempDir;

    #[test]
    fn infers_project_defaults_from_current_repo_and_dbt_project() {
        let temp = TempDir::new().expect("temp dir");
        let repo_root = temp.path();
        let project_root = repo_root.join("analytics");
        std::fs::create_dir_all(&project_root).expect("project root");
        std::fs::write(project_root.join("dbt_project.yml"), "name: jaffle_shop_project\n")
            .expect("dbt project");

        run_git_cmd(["init"], repo_root);
        run_git_cmd(["remote", "add", "origin", "git@github.com:example/repo.git"], repo_root);

        let original_dir = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&project_root).expect("set cwd");
        let inferred = infer_project_defaults(None, None, None, None).expect("inferred");
        std::env::set_current_dir(original_dir).expect("restore cwd");

        assert_eq!(inferred.project_name, "jaffle_shop_project");
        assert_eq!(inferred.slug, "jaffle_shop_project");
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

        write_dbtx_project_id(temp.path(), "prj_123", false).expect("write project id");
        assert_eq!(
            read_dbtx_project_id(temp.path()).expect("read project id"),
            Some("prj_123".to_string())
        );
    }

    #[test]
    fn replace_existing_project_id_in_vars_block() {
        let mut lines = vec![
            "name: sample".to_string(),
            "vars:".to_string(),
            "  dbtx:".to_string(),
            "    project_id: prj_old".to_string(),
        ];
        assert!(replace_existing_project_id(&mut lines, "prj_new"));
        assert_eq!(lines[3], "    project_id: prj_new");
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
