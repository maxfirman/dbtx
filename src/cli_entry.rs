//! CLI entry point handlers for state migration and dbt command dispatch.
use crate::cli_output::print_migration_summary;
use crate::cli_runtime::invoke_via_daemon;
use crate::client;
use crate::config::{self, resolve_service_url};
use crate::error::{AppError, AppResult};
use crate::services::InvocationCommand;
use std::ffi::OsString;
use std::process::Command as StdCommand;

pub async fn execute_state_migrate(service_url_override: Option<String>) -> AppResult<()> {
    let current_dir = std::env::current_dir()?;
    let service_url = resolve_service_url(service_url_override, Some(&current_dir))?
        .ok_or(AppError::MissingServiceUrl)?;
    let client = client::DaemonClient::new(service_url);
    let response = client.migrate().await?;
    print_migration_summary(&response.applied);
    Ok(())
}

pub async fn handle_persisting_command(
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
    invoke_via_daemon(service_url, command, args, &ctx).await
}

pub async fn handle_passthrough_command(
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
    invoke_via_daemon(service_url, InvocationCommand::Ls, args, &ctx).await
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
