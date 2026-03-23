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
    #[command(subcommand)]
    Project(ProjectCommand),
    #[command(subcommand)]
    Environment(EnvironmentCommand),
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

#[derive(Debug, Subcommand)]
pub enum ProjectCommand {
    #[command(about = "Initialize the current dbt project and write vars.dbtx.project_id")]
    Init {
        #[arg(long)]
        git_repo_url: Option<String>,
        #[arg(long)]
        project_root: Option<String>,
        #[arg(long)]
        default_branch: Option<String>,
        #[arg(long)]
        force: bool,
    },
    #[command(about = "Update the registered dbtx project to match the current repo state")]
    Update {
        #[arg(long)]
        git_repo_url: Option<String>,
        #[arg(long)]
        project_root: Option<String>,
        #[arg(long)]
        default_branch: Option<String>,
    },
    #[command(about = "List registered dbtx projects")]
    List,
    #[command(about = "Show one registered dbtx project")]
    Show {
        #[arg(long)]
        project: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub enum EnvironmentCommand {
    #[command(about = "Create a registered dbtx environment")]
    Create {
        #[arg(long)]
        project: String,
        #[arg(long)]
        slug: String,
        #[arg(long, default_value = "persistent")]
        kind: String,
        #[arg(long)]
        baseline: Option<String>,
        #[arg(long)]
        git_branch: Option<String>,
        #[arg(long)]
        git_commit_sha: Option<String>,
        #[arg(long)]
        pr_number: Option<i32>,
        #[arg(long)]
        immutable: bool,
        #[arg(long, default_value = "active")]
        status: String,
        #[arg(long)]
        schema_prefix: Option<String>,
    },
    #[command(about = "Update a registered dbtx environment")]
    Update {
        #[arg(long)]
        project: String,
        #[arg(long)]
        slug: String,
        #[arg(long)]
        kind: Option<String>,
        #[arg(long)]
        baseline: Option<String>,
        #[arg(long)]
        git_branch: Option<String>,
        #[arg(long)]
        git_commit_sha: Option<String>,
        #[arg(long)]
        pr_number: Option<i32>,
        #[arg(long)]
        immutable: bool,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        schema_prefix: Option<String>,
    },
    #[command(about = "List registered dbtx environments for a project")]
    List {
        #[arg(long)]
        project: String,
    },
    #[command(about = "Show one registered dbtx environment")]
    Show {
        #[arg(long)]
        project: String,
        #[arg(long)]
        slug: String,
    },
    #[command(about = "Seed one environment's active state from another")]
    SeedFrom {
        #[arg(long)]
        project: String,
        #[arg(long)]
        target: String,
        #[arg(long)]
        source: String,
        #[arg(long, default_value = "clone")]
        seed_type: String,
    },
}

#[cfg(test)]
mod tests {
    use super::{Cli, Command, EnvironmentCommand, ProjectCommand, StateCommand};
    use clap::Parser;

    #[test]
    fn state_init_accepts_database_url() {
        let cli = Cli::parse_from([
            "dbtx",
            "state",
            "init",
            "--database-url",
            "postgres://example",
        ]);
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

    #[test]
    fn project_init_parses() {
        let cli = Cli::parse_from([
            "dbtx",
            "project",
            "init",
            "--git-repo-url",
            "https://github.com/example/repo.git",
            "--project-root",
            "analytics",
        ]);
        match cli.command {
            Command::Project(ProjectCommand::Init {
                git_repo_url,
                project_root,
                default_branch,
                force,
            }) => {
                assert_eq!(
                    git_repo_url.as_deref(),
                    Some("https://github.com/example/repo.git")
                );
                assert_eq!(project_root.as_deref(), Some("analytics"));
                assert_eq!(default_branch.as_deref(), None);
                assert!(!force);
            }
            _ => panic!("expected project init command"),
        }
    }

    #[test]
    fn project_update_parses() {
        let cli = Cli::parse_from(["dbtx", "project", "update", "--default-branch", "main"]);
        match cli.command {
            Command::Project(ProjectCommand::Update {
                git_repo_url,
                project_root,
                default_branch,
            }) => {
                assert!(git_repo_url.is_none());
                assert!(project_root.is_none());
                assert_eq!(default_branch.as_deref(), Some("main"));
            }
            _ => panic!("expected project update command"),
        }
    }

    #[test]
    fn environment_seed_from_parses() {
        let cli = Cli::parse_from([
            "dbtx",
            "environment",
            "seed-from",
            "--project",
            "jaffle",
            "--target",
            "pr-123",
            "--source",
            "staging",
        ]);
        match cli.command {
            Command::Environment(EnvironmentCommand::SeedFrom {
                project,
                target,
                source,
                seed_type,
            }) => {
                assert_eq!(project, "jaffle");
                assert_eq!(target, "pr-123");
                assert_eq!(source, "staging");
                assert_eq!(seed_type, "clone");
            }
            _ => panic!("expected environment seed-from command"),
        }
    }

    #[test]
    fn project_show_allows_omitted_project_flag() {
        let cli = Cli::parse_from(["dbtx", "project", "show"]);
        match cli.command {
            Command::Project(ProjectCommand::Show { project }) => {
                assert!(project.is_none());
            }
            _ => panic!("expected project show command"),
        }
    }

    #[test]
    fn environment_update_parses() {
        let cli = Cli::parse_from([
            "dbtx",
            "environment",
            "update",
            "--project",
            "prj_123",
            "--slug",
            "ci-main",
            "--git-branch",
            "main",
            "--git-commit-sha",
            "abc123",
            "--immutable",
        ]);
        match cli.command {
            Command::Environment(EnvironmentCommand::Update {
                project,
                slug,
                kind,
                baseline,
                git_branch,
                git_commit_sha,
                immutable,
                ..
            }) => {
                assert_eq!(project, "prj_123");
                assert_eq!(slug, "ci-main");
                assert!(kind.is_none());
                assert!(baseline.is_none());
                assert_eq!(git_branch.as_deref(), Some("main"));
                assert_eq!(git_commit_sha.as_deref(), Some("abc123"));
                assert!(immutable);
            }
            _ => panic!("expected environment update command"),
        }
    }
}
