use clap::Parser;
use dbtx::config::{RuntimeConfig, resolve_database_url};
use dbtx::db::Db;
use dbtx::error::AppResult;
use dbtx::process_state::ProcessState;
use dbtx::reconciler::{self, ReconcilerConfig};
use std::sync::Once;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "dbtx-reconciler")]
#[command(about = "Run the dbtx reconciliation daemon")]
struct ReconcilerCli {
    #[arg(long)]
    database_url: Option<String>,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err}");
        std::process::exit(err.exit_code());
    }
}

async fn run() -> AppResult<()> {
    init_service_logging();
    let cli = ReconcilerCli::parse();
    let current_dir = std::env::current_dir()?;
    let config = RuntimeConfig::from_database_url(resolve_database_url(
        cli.database_url,
        Some(&current_dir),
    )?);
    let db = Db::connect(&config.database_url).await?;
    reconciler::run(ProcessState::new(db, config), ReconcilerConfig::from_env()).await
}

fn init_service_logging() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let filter = EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| EnvFilter::new("dbtx=info,tower_http=info"));
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .compact()
            .init();
    });
}
