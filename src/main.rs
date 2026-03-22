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
                let config = RuntimeConfig::from_optional_database_url(database_url)?;
                let db = Db::connect(&config.database_url).await?;
                db.init().await?;
                println!("Initialized dbtx database schema.");
            }
        },
        Command::Build { args } => {
            if is_help_request(&args) {
                exit_with_dbt_help("build")?;
            }
            let config = RuntimeConfig::from_env()?;
            let db = Db::connect(&config.database_url).await?;
            db.init().await?;
            db.persisting_invocation("build", &config, &args).await?;
        }
        Command::Run { args } => {
            if is_help_request(&args) {
                exit_with_dbt_help("run")?;
            }
            let config = RuntimeConfig::from_env()?;
            let db = Db::connect(&config.database_url).await?;
            db.init().await?;
            db.persisting_invocation("run", &config, &args).await?;
        }
        Command::Ls { args } => {
            if is_help_request(&args) {
                exit_with_dbt_help("ls")?;
            }
            let config = RuntimeConfig::from_env()?;
            let db = Db::connect(&config.database_url).await?;
            db.ls_invocation(&config, &args).await?;
        }
        Command::Test { args } => {
            if is_help_request(&args) {
                exit_with_dbt_help("test")?;
            }
            let config = RuntimeConfig::from_env()?;
            let db = Db::connect(&config.database_url).await?;
            db.init().await?;
            db.persisting_invocation("test", &config, &args).await?;
        }
        Command::Seed { args } => {
            if is_help_request(&args) {
                exit_with_dbt_help("seed")?;
            }
            let config = RuntimeConfig::from_env()?;
            let db = Db::connect(&config.database_url).await?;
            db.init().await?;
            db.persisting_invocation("seed", &config, &args).await?;
        }
        Command::Replay { run_id } => {
            let config = RuntimeConfig::from_env()?;
            let db = Db::connect(&config.database_url).await?;
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
