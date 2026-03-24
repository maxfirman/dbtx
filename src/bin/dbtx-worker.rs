use clap::Parser;
use dbtx::api::{InvocationClaimApiRequest, InvocationClaimNextApiRequest, InvocationExecutionModeApi};
use dbtx::client::DaemonClient;
use dbtx::config::resolve_service_url;
use dbtx::error::{AppError, AppResult};
use dbtx::worker;
use uuid::Uuid;
use std::sync::Once;
use std::time::Duration;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

#[derive(Debug, Parser)]
#[command(name = "dbtx-worker")]
#[command(about = "Run a dbtx execution worker")]
struct WorkerCli {
    #[arg(long)]
    service_url: Option<String>,
    #[arg(long, default_value = "server")]
    execution_mode: WorkerExecutionMode,
    #[arg(long)]
    queue: Option<String>,
    #[arg(long)]
    invocation_id: Option<Uuid>,
    #[arg(long)]
    once: bool,
    #[arg(long, default_value_t = 1000)]
    poll_interval_ms: u64,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum WorkerExecutionMode {
    Server,
    Local,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("error: {err}");
        std::process::exit(err.exit_code());
    }
}

async fn run() -> AppResult<()> {
    init_worker_logging();
    let cli = WorkerCli::parse();
    let current_dir = std::env::current_dir()?;
    let service_url = resolve_service_url(cli.service_url, Some(&current_dir))?
        .ok_or(AppError::MissingServiceUrl)?;
    let client = DaemonClient::new(service_url);
    let worker_id = format!(
        "worker-{}-{}",
        std::env::var("HOSTNAME")
            .or_else(|_| std::env::var("HOST"))
            .unwrap_or_else(|_| "unknown".to_string()),
        std::process::id()
    );
    let execution_mode = match cli.execution_mode {
        WorkerExecutionMode::Server => InvocationExecutionModeApi::Server,
        WorkerExecutionMode::Local => InvocationExecutionModeApi::Local,
    };
    let poll_interval = Duration::from_millis(cli.poll_interval_ms);

    if let Some(invocation_id) = cli.invocation_id {
        let claim = client
            .invocation_claim(
                invocation_id,
                InvocationClaimApiRequest {
                    worker_id: worker_id.clone(),
                },
            )
            .await?;
        info!(invocation_id = %claim.invocation_id, ?execution_mode, "claimed invocation directly");
        return worker::execute_claimed_invocation(&client, claim, Some(invocation_id)).await;
    }

    loop {
        match client
            .invocation_claim_next(InvocationClaimNextApiRequest {
                execution_mode: Some(execution_mode),
                worker_id: worker_id.clone(),
                worker_queue: cli.queue.clone(),
            })
            .await?
        {
            Some(claim) => {
                info!(invocation_id = %claim.invocation_id, ?execution_mode, "claimed invocation");
                if let Err(err) = worker::execute_claimed_invocation(&client, claim, None).await {
                    warn!(error = %err, "worker invocation failed");
                    if cli.once {
                        return Err(err);
                    }
                } else if cli.once {
                    return Ok(());
                }
            }
            None => {
                if cli.once {
                    return Err(AppError::Io(std::io::Error::other(
                        "no invocation available for worker",
                    )));
                }
                tokio::time::sleep(poll_interval).await;
            }
        }
    }
}

fn init_worker_logging() {
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
