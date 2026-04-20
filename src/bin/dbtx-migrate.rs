use clap::Parser;
use dbtx::cli_output::print_migration_summary;
use dbtx::config::resolve_database_url;
use dbtx::db::Db;
use dbtx::error::AppResult;
use std::sync::Once;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "dbtx-migrate")]
#[command(about = "Apply dbtx database migrations directly against PostgreSQL")]
struct MigrateCli {
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
    init_logging();
    let cli = MigrateCli::parse();
    let current_dir = std::env::current_dir()?;
    let database_url = resolve_database_url(cli.database_url, Some(&current_dir))?;
    let db = Db::connect(&database_url).await?;
    let applied = db.migrate().await?;
    info!(applied = applied.len(), "applied database migrations");
    print_migration_summary(&applied);
    Ok(())
}

fn init_logging() {
    static INIT: Once = Once::new();
    INIT.call_once(|| {
        let filter =
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("dbtx=info"));
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_target(false)
            .compact()
            .init();
    });
}
