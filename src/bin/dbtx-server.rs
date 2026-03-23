#[path = "../api.rs"]
mod api;
#[path = "../config.rs"]
mod config;
#[path = "../db.rs"]
mod db;
#[path = "../error.rs"]
mod error;
#[path = "../event.rs"]
mod event;
#[path = "../manifest.rs"]
mod manifest;
#[path = "../profile.rs"]
mod profile;
#[path = "../services.rs"]
mod services;
#[path = "../server.rs"]
mod server;

use clap::Parser;
use config::RuntimeConfig;
use db::Db;
use error::AppResult;
use std::sync::Once;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "dbtx-server")]
#[command(about = "Run the dbtx local daemon")]
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
    let config = RuntimeConfig::resolve(cli.database_url, None, Some(&current_dir))?;
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
