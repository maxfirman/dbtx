mod cli;
mod config;
mod db;
mod error;
mod event;
mod manifest;

use clap::Parser;
use cli::{Cli, Command, StateCommand};
use config::RuntimeConfig;
use db::Db;
use error::AppResult;
use std::ffi::OsString;
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
            StateCommand::Init { database_url } => {
                let _db =
                    connect_initialized_db(&RuntimeConfig::from_optional_database_url(database_url)?)
                        .await?;
                println!("Initialized dbtx database schema.");
            }
        },
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
