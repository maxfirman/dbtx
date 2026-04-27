use clap::Parser;
use dbtx::cli::{Cli, Command, StateCommand};
use dbtx::cli_entry::{
    execute_state_migrate, handle_passthrough_command, handle_persisting_command,
};
use dbtx::cli_runtime::{
    handle_environment_command, handle_invocation_command, handle_project_command,
    handle_queue_command, handle_worker_command,
};
use dbtx::error::{AppError, AppResult};
use dbtx::services::InvocationCommand;

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
            StateCommand::Migrate => execute_state_migrate(cli.service_url).await?,
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
