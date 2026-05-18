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
        read_dbt_project_name_from_root, relative_project_root, validate_remote_project_root,
    };
    use tempfile::TempDir;

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
}
