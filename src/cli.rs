use clap::{Parser, Subcommand};
use std::ffi::OsString;
#[derive(Debug, Parser)]
#[command(name = "dbtx")]
#[command(about = "dbt-compatible wrapper with state persistence")]
pub struct Cli {
    #[arg(long, global = true)]
    pub service_url: Option<String>,
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
    #[command(subcommand)]
    Invocation(InvocationCommand),
    #[command(subcommand)]
    Worker(WorkerCommand),
    #[command(subcommand)]
    Queue(QueueCommand),
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
}

#[derive(Debug, Subcommand)]
pub enum StateCommand {
    #[command(about = "Apply dbtx database migrations")]
    Migrate,
}

#[derive(Debug, Subcommand)]
pub enum ProjectCommand {
    #[command(about = "Initialize the current dbt project and write dbtx.toml")]
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
        project: Option<String>,
        #[arg(long)]
        slug: Option<String>,
        #[arg(long)]
        target: Option<String>,
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
        worker_queue: Option<String>,
        #[arg(long)]
        schema_name: Option<String>,
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
        adapter_type: Option<String>,
        #[arg(long)]
        worker_queue: Option<String>,
        #[arg(long)]
        schema_name: Option<String>,
        #[arg(long)]
        threads: Option<i32>,
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
}

#[derive(Debug, Subcommand)]
pub enum InvocationCommand {
    #[command(about = "List active and recent invocations")]
    List {
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        execution_mode: Option<String>,
        #[arg(long)]
        worker_queue: Option<String>,
        #[arg(long)]
        claimed_by: Option<String>,
        #[arg(long)]
        cancel_state: Option<String>,
        #[arg(long)]
        limit: Option<i64>,
    },
    #[command(about = "Show one invocation")]
    Show {
        #[arg(long)]
        invocation_id: String,
    },
    #[command(about = "Request cancellation for one invocation")]
    Cancel {
        #[arg(long)]
        invocation_id: String,
        #[arg(long)]
        wait: bool,
    },
    #[command(about = "Delete old terminal invocations and their invocation events")]
    Cleanup {
        #[arg(long)]
        older_than_hours: i64,
    },
}

#[derive(Debug, Subcommand)]
pub enum WorkerCommand {
    #[command(about = "List active workers derived from current invocation ownership")]
    List,
}

#[derive(Debug, Subcommand)]
pub enum QueueCommand {
    #[command(about = "List execution queues and backlog state")]
    List,
}

#[cfg(test)]
mod tests {
    use super::{
        Cli, Command, EnvironmentCommand, InvocationCommand, ProjectCommand, QueueCommand,
        StateCommand, WorkerCommand,
    };
    use clap::Parser;

    #[test]
    fn state_migrate_accepts_service_url() {
        let cli = Cli::parse_from([
            "dbtx",
            "--service-url",
            "http://127.0.0.1:8585",
            "state",
            "migrate",
        ]);
        assert_eq!(cli.service_url.as_deref(), Some("http://127.0.0.1:8585"));
        match cli.command {
            Command::State(StateCommand::Migrate) => {}
            _ => panic!("expected state migrate command"),
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
    fn invocation_cancel_parses() {
        let cli = Cli::parse_from([
            "dbtx",
            "invocation",
            "cancel",
            "--invocation-id",
            "123e4567-e89b-12d3-a456-426614174000",
            "--wait",
        ]);
        match cli.command {
            Command::Invocation(InvocationCommand::Cancel {
                invocation_id,
                wait,
            }) => {
                assert_eq!(invocation_id, "123e4567-e89b-12d3-a456-426614174000");
                assert!(wait);
            }
            _ => panic!("expected invocation cancel command"),
        }
    }

    #[test]
    fn invocation_list_filters_parse() {
        let cli = Cli::parse_from([
            "dbtx",
            "invocation",
            "list",
            "--status",
            "running",
            "--execution-mode",
            "local",
            "--worker-queue",
            "generic",
            "--claimed-by",
            "worker-1",
            "--cancel-state",
            "requested",
            "--limit",
            "25",
        ]);
        match cli.command {
            Command::Invocation(InvocationCommand::List {
                status,
                execution_mode,
                worker_queue,
                claimed_by,
                cancel_state,
                limit,
            }) => {
                assert_eq!(status.as_deref(), Some("running"));
                assert_eq!(execution_mode.as_deref(), Some("local"));
                assert_eq!(worker_queue.as_deref(), Some("generic"));
                assert_eq!(claimed_by.as_deref(), Some("worker-1"));
                assert_eq!(cancel_state.as_deref(), Some("requested"));
                assert_eq!(limit, Some(25));
            }
            _ => panic!("expected invocation list command"),
        }
    }

    #[test]
    fn worker_list_parses() {
        let cli = Cli::parse_from(["dbtx", "worker", "list"]);
        match cli.command {
            Command::Worker(WorkerCommand::List) => {}
            _ => panic!("expected worker list command"),
        }
    }

    #[test]
    fn queue_list_parses() {
        let cli = Cli::parse_from(["dbtx", "queue", "list"]);
        match cli.command {
            Command::Queue(QueueCommand::List) => {}
            _ => panic!("expected queue list command"),
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

    #[test]
    fn invocation_cleanup_parses() {
        let cli = Cli::parse_from(["dbtx", "invocation", "cleanup", "--older-than-hours", "24"]);
        match cli.command {
            Command::Invocation(InvocationCommand::Cleanup { older_than_hours }) => {
                assert_eq!(older_than_hours, 24);
            }
            _ => panic!("expected invocation cleanup command"),
        }
    }
}
