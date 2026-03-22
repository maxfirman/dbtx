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
    #[command(subcommand)]
    State(StateCommand),
    #[command(
        about = "Build dbt resources and persist execution state",
        trailing_var_arg = true,
        allow_hyphen_values = true
    )]
    Build {
        #[arg()]
        args: Vec<OsString>,
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
    #[command(
        about = "Execute dbt tests and persist execution state",
        trailing_var_arg = true,
        allow_hyphen_values = true
    )]
    Test {
        #[arg()]
        args: Vec<OsString>,
    },
    #[command(
        about = "Load dbt seeds and persist execution state",
        trailing_var_arg = true,
        allow_hyphen_values = true
    )]
    Seed {
        #[arg()]
        args: Vec<OsString>,
    },
    #[command(about = "Rebuild current-state projections from persisted facts")]
    Replay {
        #[arg(long)]
        run_id: Uuid,
    },
}

#[derive(Debug, Subcommand)]
pub enum StateCommand {
    #[command(about = "Initialize the dbtx database schema")]
    Init {
        #[arg(long, env = "DBTX_DATABASE_URL")]
        database_url: Option<String>,
    },
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command, StateCommand};
    use clap::Parser;

    #[test]
    fn state_init_accepts_database_url() {
        let cli = Cli::parse_from(["dbtx", "state", "init", "--database-url", "postgres://example"]);
        match cli.command {
            Command::State(StateCommand::Init { database_url }) => {
                assert_eq!(database_url.as_deref(), Some("postgres://example"));
            }
            _ => panic!("expected state init command"),
        }
    }

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
    fn build_accepts_passthrough_args() {
        let cli = Cli::parse_from(["dbtx", "build", "--target", "prod", "--select", "orders+"]);
        match cli.command {
            Command::Build { args } => {
                let args: Vec<String> = args
                    .into_iter()
                    .map(|value| value.to_string_lossy().into_owned())
                    .collect();
                assert_eq!(args, vec!["--target", "prod", "--select", "orders+"]);
            }
            _ => panic!("expected build command"),
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

    #[test]
    fn test_accepts_passthrough_args() {
        let cli = Cli::parse_from(["dbtx", "test", "--select", "orders"]);
        match cli.command {
            Command::Test { args } => {
                let args: Vec<String> = args
                    .into_iter()
                    .map(|value| value.to_string_lossy().into_owned())
                    .collect();
                assert_eq!(args, vec!["--select", "orders"]);
            }
            _ => panic!("expected test command"),
        }
    }

    #[test]
    fn seed_accepts_passthrough_args() {
        let cli = Cli::parse_from(["dbtx", "seed", "--full-refresh"]);
        match cli.command {
            Command::Seed { args } => {
                let args: Vec<String> = args
                    .into_iter()
                    .map(|value| value.to_string_lossy().into_owned())
                    .collect();
                assert_eq!(args, vec!["--full-refresh"]);
            }
            _ => panic!("expected seed command"),
        }
    }
}
