use clap::{Parser, Subcommand};
use std::ffi::OsString;
use uuid::Uuid;

#[derive(Debug, Parser)]
#[command(name = "dbtx")]
#[command(about = "dbt-compatible wrapper with state persistence")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    #[command(about = "Initialize the dbtx database schema")]
    Init {
        #[arg(long, env = "DBTX_DATABASE_URL")]
        database_url: Option<String>,
    },
    #[command(
        about = "Run dbt and persist execution state",
        trailing_var_arg = true,
        allow_hyphen_values = true
    )]
    Run {
        #[arg()]
        args: Vec<OsString>,
    },
    #[command(
        about = "List dbt nodes using reconstructed state",
        trailing_var_arg = true,
        allow_hyphen_values = true
    )]
    Ls {
        #[arg()]
        args: Vec<OsString>,
    },
    #[command(about = "Rebuild current-state projections from persisted facts")]
    Replay {
        #[arg(long)]
        run_id: Uuid,
    },
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command};
    use clap::Parser;

    #[test]
    fn run_accepts_passthrough_args() {
        let cli = Cli::parse_from(["dbtx", "run", "--target", "prod", "--select", "orders+"]);
        match cli.command {
            Command::Run { args } => {
                let args: Vec<String> = args
                    .into_iter()
                    .map(|value| value.to_string_lossy().into_owned())
                    .collect();
                assert_eq!(args, vec!["--target", "prod", "--select", "orders+"]);
            }
            _ => panic!("expected run command"),
        }
    }

    #[test]
    fn ls_accepts_passthrough_args() {
        let cli = Cli::parse_from(["dbtx", "ls", "--output", "json", "--select", "orders+"]);
        match cli.command {
            Command::Ls { args } => {
                let args: Vec<String> = args
                    .into_iter()
                    .map(|value| value.to_string_lossy().into_owned())
                    .collect();
                assert_eq!(args, vec!["--output", "json", "--select", "orders+"]);
            }
            _ => panic!("expected ls command"),
        }
    }
}
