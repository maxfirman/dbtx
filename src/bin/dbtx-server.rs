use clap::Parser;
use dbtx::config::{RuntimeConfig, resolve_database_url};
use dbtx::db::Db;
use dbtx::error::AppResult;
use dbtx::server;
use std::sync::Once;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "dbtx-server")]
#[command(about = "Run the dbtx local server")]
struct ServerCli {
    #[arg(long)]
    database_url: Option<String>,
    #[arg(long, default_value = "127.0.0.1:8585")]
    listen: String,
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
    let cli = ServerCli::parse();
    let current_dir = std::env::current_dir()?;
    let config = RuntimeConfig::from_database_url(resolve_database_url(
        cli.database_url,
        Some(&current_dir),
    )?);
    let db = Db::connect(&config.database_url).await?;
    server::serve(&cli.listen, server::AppState::new(db, config)).await
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
