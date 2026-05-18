//! CLI output formatting for invocations, projects, environments, workers, and queues.
use crate::api;
use crate::db::{self, EnvironmentRecord, EnvironmentVersionRecord, ProjectRecord};
use std::io::IsTerminal;

#[derive(Clone, Copy)]
enum CliStyle {
    Green,
    Yellow,
    Cyan,
    Bold,
    Dim,
}

pub fn print_migration_summary(applied: &[db::AppliedMigration]) {
    let use_color = should_use_color();
    println!(
        "{}",
        style(
            "dbtx migrations",
            &[CliStyle::Cyan, CliStyle::Bold],
            use_color
        )
    );
    if applied.is_empty() {
        println!(
            "{}",
            style("  No pending migrations.", &[CliStyle::Dim], use_color)
        );
        return;
    }

    for migration in applied {
        println!(
            "  {} {}",
            style("Applied", &[CliStyle::Green, CliStyle::Bold], use_color),
            style(
                &format!("{} {}", migration.version, migration.description),
                &[CliStyle::Bold],
                use_color,
            )
        );
    }
    println!(
        "{}",
        style(
            &format!("  {} migration(s) applied.", applied.len()),
            &[CliStyle::Yellow, CliStyle::Bold],
            use_color,
        )
    );
}

pub fn render_invocation_event(event: api::InvocationEvent) {
    match event.stream.as_deref() {
        Some("stderr") => {
            if let Some(text) = event.text {
                eprintln!("{text}");
            }
        }
        _ => {
            if let Some(text) = event.text {
                println!("{text}");
            }
        }
    }
    if event.event_type == "invocation.completed"
        && let Some(error) = event.error
    {
        eprintln!("{error}");
    }
}

pub fn render_release_event(event: api::InvocationEvent) {
    let use_color = should_use_color();
    match event.event_type.as_str() {
        "invocation.started" => {
            println!(
                "{}",
                style(
                    "  Preparing release validation…",
                    &[CliStyle::Dim],
                    use_color
                )
            );
        }
        "invocation.completed" => {}
        _ => {
            if let Some(text) = event.text {
                let text = text.trim();
                if text.is_empty() {
                    return;
                }
                let bullet = style("  •", &[CliStyle::Cyan, CliStyle::Bold], use_color);
                let body = match event.stream.as_deref() {
                    Some("stderr") => style(text, &[CliStyle::Yellow], use_color),
                    _ => style(text, &[], use_color),
                };
                println!("{bullet} {body}");
            }
        }
    }
}

pub fn render_project_validation_event(event: api::InvocationEvent) {
    let use_color = should_use_color();
    if let Some(text) = event.text {
        let text = text.trim();
        if text.is_empty() {
            return;
        }
        let bullet = style("  •", &[CliStyle::Cyan, CliStyle::Bold], use_color);
        println!("{bullet} {}", style(text, &[], use_color));
    }
}

pub fn print_project_create_start(repo_url: &str, project_root: &str) {
    let use_color = should_use_color();
    println!(
        "{}",
        style(
            "dbtx project create",
            &[CliStyle::Cyan, CliStyle::Bold],
            use_color
        )
    );
    println!(
        "  {} {}",
        style("Repository", &[CliStyle::Dim], use_color),
        style(repo_url, &[CliStyle::Bold], use_color),
    );
    println!(
        "  {} {}",
        style("Project root", &[CliStyle::Dim], use_color),
        style(project_root, &[CliStyle::Bold], use_color),
    );
}

pub fn print_release_start(
    project: &str,
    slug: &str,
    git_ref: Option<&str>,
    git_commit_sha: Option<&str>,
) {
    let use_color = should_use_color();
    println!(
        "{}",
        style("dbtx release", &[CliStyle::Cyan, CliStyle::Bold], use_color)
    );
    let target = git_ref
        .map(|value| format!("ref {}", style(value, &[CliStyle::Bold], use_color)))
        .or_else(|| {
            git_commit_sha
                .map(|value| format!("commit {}", style(value, &[CliStyle::Bold], use_color)))
        })
        .unwrap_or_else(|| "unknown target".to_string());
    println!(
        "  {} {}  {} {}",
        style("Project", &[CliStyle::Dim], use_color),
        style(project, &[CliStyle::Bold], use_color),
        style("Environment", &[CliStyle::Dim], use_color),
        style(slug, &[CliStyle::Bold], use_color),
    );
    println!(
        "  {} {target}",
        style("Target", &[CliStyle::Dim], use_color)
    );
}

pub fn print_release_success(project: &str, slug: &str, git_commit_sha: Option<&str>) {
    let use_color = should_use_color();
    let resolved = git_commit_sha.unwrap_or("");
    println!(
        "{} {} {} {} {} {}",
        style("✅", &[], use_color),
        style(
            "Release succeeded.",
            &[CliStyle::Green, CliStyle::Bold],
            use_color
        ),
        style(project, &[CliStyle::Bold], use_color),
        style("/", &[CliStyle::Dim], use_color),
        style(slug, &[CliStyle::Bold], use_color),
        style(
            &format!("-> {resolved}"),
            &[CliStyle::Green, CliStyle::Bold],
            use_color
        ),
    );
}

pub fn print_release_already_released(project: &str, slug: &str, git_commit_sha: &str) {
    let use_color = should_use_color();
    println!(
        "{} {} {} {} {} {}",
        style("✅", &[], use_color),
        style(
            "Version already released.",
            &[CliStyle::Cyan, CliStyle::Bold],
            use_color
        ),
        style(project, &[CliStyle::Bold], use_color),
        style("/", &[CliStyle::Dim], use_color),
        style(slug, &[CliStyle::Bold], use_color),
        style(
            &format!("-> {git_commit_sha}"),
            &[CliStyle::Cyan, CliStyle::Bold],
            use_color
        ),
    );
}

pub fn print_release_failure(project: &str, slug: &str, error: &str) {
    let use_color = should_use_color();
    eprintln!(
        "{} {} {} {}",
        style("❌", &[], use_color),
        style(
            "Release failed.",
            &[CliStyle::Yellow, CliStyle::Bold],
            use_color
        ),
        style(&format!("{project}/{slug}"), &[CliStyle::Bold], use_color),
        style(error, &[CliStyle::Yellow], use_color),
    );
}

pub fn print_project(project: &ProjectRecord) {
    println!(
        "project id={} project_id={} project_name={} git_repo_url={} default_branch={} project_root={} metadata={}",
        project.id,
        project.project_id,
        project.project_name,
        project.git_repo_url,
        project.default_branch,
        project.project_root,
        project.metadata,
    );
}

pub fn print_environment(environment: &EnvironmentRecord) {
    println!(
        "environment id={} project_pk={} project_id={} project={} slug={} profile_name={} target_name={} baseline_id={} baseline={} git_branch={} git_commit_sha={} pr_number={} status={} adapter_type={} worker_queue={} schema_name={} threads={} profile_config={} metadata={}",
        environment.id,
        environment.project_id,
        environment.project_ref,
        environment.project_name,
        environment.slug,
        environment.profile_name,
        environment.target_name,
        environment
            .baseline_environment_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
        environment
            .baseline_environment_slug
            .as_deref()
            .unwrap_or(""),
        environment.git_branch.as_deref().unwrap_or(""),
        environment.git_commit_sha.as_deref().unwrap_or(""),
        environment
            .pr_number
            .map(|value| value.to_string())
            .unwrap_or_default(),
        environment.status,
        environment.adapter_type,
        environment.worker_queue,
        environment.schema_name,
        environment
            .threads
            .map(|v| v.to_string())
            .unwrap_or_default(),
        environment.profile_config,
        environment.metadata,
    );
}

pub fn print_environment_version(version: &EnvironmentVersionRecord) {
    println!(
        "environment_version id={} environment_id={} project_id={} recorded_at={} reason={} git_branch={} git_commit_sha={} baseline_environment_id={} metadata={}",
        version.id,
        version.environment_id,
        version.project_id,
        version.recorded_at.to_rfc3339(),
        version.reason,
        version.git_branch.as_deref().unwrap_or(""),
        version.git_commit_sha.as_deref().unwrap_or(""),
        version
            .baseline_environment_id
            .map(|value| value.to_string())
            .unwrap_or_default(),
        version.metadata,
    );
}

pub fn print_invocation(invocation: &api::InvocationStatusResponse) {
    println!(
        "invocation id={} mode={:?} worker_queue={} worker_health={:?} cancel_state={:?} status={:?} exit_code={} claimed_by={} claimed_at={} last_heartbeat_at={} cancel_requested_at={} started_at={} completed_at={} cancel_requested={} error={}",
        invocation.invocation_id,
        invocation.execution_mode,
        invocation.worker_queue,
        invocation.worker_health,
        invocation.cancel_state,
        invocation.status,
        invocation
            .exit_code
            .map(|value| value.to_string())
            .unwrap_or_default(),
        invocation.claimed_by.as_deref().unwrap_or(""),
        invocation
            .claimed_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
        invocation
            .last_heartbeat_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
        invocation
            .cancel_requested_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
        invocation.started_at.to_rfc3339(),
        invocation
            .completed_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
        invocation.cancel_requested,
        invocation.error.as_deref().unwrap_or(""),
    );
}

pub fn print_worker(worker: &api::WorkerStatusResponse) {
    println!(
        "worker id={} mode={:?} worker_queues={} claimed_invocation_count={} last_heartbeat_at={} health={:?}",
        worker.worker_id,
        worker.execution_mode,
        worker.worker_queues.join(","),
        worker.claimed_invocation_count,
        worker
            .last_heartbeat_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
        worker.health,
    );
}

pub fn print_queue(queue: &api::QueueStatusResponse) {
    println!(
        "queue worker_queue={} mode={:?} pending_count={} claimed_count={} stale_claim_count={} oldest_pending_at={}",
        queue.worker_queue,
        queue.execution_mode,
        queue.pending_count,
        queue.claimed_count,
        queue.stale_claim_count,
        queue
            .oldest_pending_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_default(),
    );
}

fn should_use_color() -> bool {
    if std::env::var_os("NO_COLOR").is_some() {
        return false;
    }
    if matches!(std::env::var("TERM").ok().as_deref(), Some("dumb")) {
        return false;
    }
    if matches!(std::env::var("CLICOLOR_FORCE").ok().as_deref(), Some("1")) {
        return true;
    }
    std::io::stdout().is_terminal()
}

fn style(input: &str, styles: &[CliStyle], use_color: bool) -> String {
    if !use_color || styles.is_empty() {
        return input.to_string();
    }
    let prefix = styles
        .iter()
        .map(|style| match style {
            CliStyle::Green => "32",
            CliStyle::Yellow => "33",
            CliStyle::Cyan => "36",
            CliStyle::Bold => "1",
            CliStyle::Dim => "2",
        })
        .collect::<Vec<_>>()
        .join(";");
    format!("\u{1b}[{prefix}m{input}\u{1b}[0m")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn style_returns_plain_text_when_color_disabled() {
        assert_eq!(style("hello", &[CliStyle::Green], false), "hello");
    }

    #[test]
    fn style_returns_plain_text_when_no_styles() {
        assert_eq!(style("hello", &[], true), "hello");
    }

    #[test]
    fn style_wraps_with_ansi_codes_when_enabled() {
        let result = style("hello", &[CliStyle::Green], true);
        assert_eq!(result, "\u{1b}[32mhello\u{1b}[0m");
    }

    #[test]
    fn style_combines_multiple_codes() {
        let result = style("hello", &[CliStyle::Cyan, CliStyle::Bold], true);
        assert_eq!(result, "\u{1b}[36;1mhello\u{1b}[0m");
    }

    #[test]
    fn style_all_variants_produce_expected_codes() {
        assert!(style("x", &[CliStyle::Green], true).contains("32"));
        assert!(style("x", &[CliStyle::Yellow], true).contains("33"));
        assert!(style("x", &[CliStyle::Cyan], true).contains("36"));
        assert!(style("x", &[CliStyle::Bold], true).contains("1m"));
        assert!(style("x", &[CliStyle::Dim], true).contains("2m"));
    }

    #[test]
    fn print_project_formats_all_fields() {
        let project = ProjectRecord {
            id: 1,
            project_id: "prj_abc".to_string(),
            project_name: "my_project".to_string(),
            git_repo_url: "https://github.com/org/repo.git".to_string(),
            default_branch: "main".to_string(),
            project_root: ".".to_string(),
            metadata: serde_json::json!({}),
        };
        // Should not panic
        print_project(&project);
    }

    #[test]
    fn print_environment_formats_all_fields() {
        let env = EnvironmentRecord {
            id: 1,
            project_id: 1,
            project_ref: "prj_abc".to_string(),
            project_name: "my_project".to_string(),
            slug: "production".to_string(),
            profile_name: "default".to_string(),
            target_name: "prod".to_string(),
            baseline_environment_id: None,
            baseline_environment_slug: None,
            git_branch: Some("main".to_string()),
            git_commit_sha: Some("abc123def456".to_string()),
            use_latest_commit: false,
            pr_number: None,
            status: db::EnvironmentStatus::Active,
            auto_reconcile: true,
            immutable: false,
            adapter_type: "postgres".to_string(),
            worker_queue: "generic".to_string(),
            schema_name: "public".to_string(),
            threads: Some(4),
            profile_config: serde_json::json!({}),
            profile_secrets: serde_json::json!({}),
            metadata: serde_json::json!({}),
        };
        print_environment(&env);
    }

    #[test]
    fn print_environment_handles_none_fields() {
        let env = EnvironmentRecord {
            id: 2,
            project_id: 1,
            project_ref: "prj_abc".to_string(),
            project_name: "my_project".to_string(),
            slug: "dev".to_string(),
            profile_name: "default".to_string(),
            target_name: "dev".to_string(),
            baseline_environment_id: Some(1),
            baseline_environment_slug: Some("production".to_string()),
            git_branch: None,
            git_commit_sha: None,
            use_latest_commit: false,
            pr_number: Some(42),
            status: db::EnvironmentStatus::Active,
            auto_reconcile: false,
            immutable: false,
            adapter_type: "duckdb".to_string(),
            worker_queue: "local".to_string(),
            schema_name: "main".to_string(),
            threads: None,
            profile_config: serde_json::json!({}),
            profile_secrets: serde_json::json!({}),
            metadata: serde_json::json!({}),
        };
        print_environment(&env);
    }

    #[test]
    fn print_environment_version_formats_correctly() {
        let version = EnvironmentVersionRecord {
            id: 1,
            environment_id: 1,
            project_id: 1,
            recorded_at: chrono::Utc::now(),
            reason: "release".to_string(),
            git_branch: Some("main".to_string()),
            git_commit_sha: Some("abc123".to_string()),
            use_latest_commit: false,
            auto_reconcile: true,
            immutable: false,
            baseline_environment_id: None,
            metadata: serde_json::json!({}),
        };
        print_environment_version(&version);
    }

    #[test]
    fn print_invocation_formats_all_fields() {
        let inv = api::InvocationStatusResponse {
            invocation_id: uuid::Uuid::nil(),
            execution_mode: api::InvocationExecutionModeApi::Server,
            worker_queue: "generic".to_string(),
            worker_health: api::InvocationWorkerHealthApi::Claimed,
            cancel_state: api::InvocationCancelStateApi::None,
            status: api::InvocationLifecycleStatus::Succeeded,
            exit_code: Some(0),
            claimed_by: Some("worker-1".to_string()),
            claimed_at: Some(chrono::Utc::now()),
            last_heartbeat_at: Some(chrono::Utc::now()),
            cancel_requested_at: None,
            started_at: chrono::Utc::now(),
            completed_at: Some(chrono::Utc::now()),
            cancel_requested: false,
            error: None,
        };
        print_invocation(&inv);
    }

    #[test]
    fn print_invocation_handles_none_fields() {
        let inv = api::InvocationStatusResponse {
            invocation_id: uuid::Uuid::nil(),
            execution_mode: api::InvocationExecutionModeApi::Local,
            worker_queue: "local".to_string(),
            worker_health: api::InvocationWorkerHealthApi::Unclaimed,
            cancel_state: api::InvocationCancelStateApi::None,
            status: api::InvocationLifecycleStatus::Running,
            exit_code: None,
            claimed_by: None,
            claimed_at: None,
            last_heartbeat_at: None,
            cancel_requested_at: None,
            started_at: chrono::Utc::now(),
            completed_at: None,
            cancel_requested: false,
            error: Some("something went wrong".to_string()),
        };
        print_invocation(&inv);
    }

    #[test]
    fn print_worker_formats_correctly() {
        let worker = api::WorkerStatusResponse {
            worker_id: "worker-1".to_string(),
            execution_mode: api::InvocationExecutionModeApi::Server,
            worker_queues: vec!["generic".to_string(), "analytics".to_string()],
            claimed_invocation_count: 1,
            last_heartbeat_at: Some(chrono::Utc::now()),
            health: api::InvocationWorkerHealthApi::Claimed,
        };
        print_worker(&worker);
    }

    #[test]
    fn print_queue_formats_correctly() {
        let queue = api::QueueStatusResponse {
            worker_queue: "generic".to_string(),
            execution_mode: api::InvocationExecutionModeApi::Server,
            pending_count: 5,
            claimed_count: 2,
            stale_claim_count: 0,
            oldest_pending_at: Some(chrono::Utc::now()),
        };
        print_queue(&queue);
    }

    #[test]
    fn print_queue_handles_none_oldest_pending() {
        let queue = api::QueueStatusResponse {
            worker_queue: "local".to_string(),
            execution_mode: api::InvocationExecutionModeApi::Local,
            pending_count: 0,
            claimed_count: 0,
            stale_claim_count: 0,
            oldest_pending_at: None,
        };
        print_queue(&queue);
    }

    #[test]
    fn print_migration_summary_empty() {
        print_migration_summary(&[]);
    }

    #[test]
    fn print_migration_summary_with_migrations() {
        let migrations = vec![
            db::AppliedMigration {
                version: 1,
                description: "initial schema".to_string(),
            },
            db::AppliedMigration {
                version: 2,
                description: "add environments".to_string(),
            },
        ];
        print_migration_summary(&migrations);
    }

    fn test_event(event_type: &str, stream: Option<&str>, text: Option<&str>, error: Option<&str>) -> api::InvocationEvent {
        api::InvocationEvent {
            event_type: event_type.to_string(),
            stream: stream.map(|s| s.to_string()),
            text: text.map(|s| s.to_string()),
            dbt_event_name: None,
            node_unique_id: None,
            level: None,
            exit_code: None,
            error: error.map(|s| s.to_string()),
            timestamp: chrono::Utc::now(),
        }
    }

    #[test]
    fn render_invocation_event_stdout() {
        render_invocation_event(test_event("log", Some("stdout"), Some("hello world"), None));
    }

    #[test]
    fn render_invocation_event_stderr() {
        render_invocation_event(test_event("log", Some("stderr"), Some("warning"), None));
    }

    #[test]
    fn render_invocation_event_completed_with_error() {
        render_invocation_event(test_event("invocation.completed", None, None, Some("failed to run")));
    }

    #[test]
    fn render_release_event_started() {
        render_release_event(test_event("invocation.started", None, None, None));
    }

    #[test]
    fn render_release_event_completed_is_silent() {
        render_release_event(test_event("invocation.completed", None, None, None));
    }

    #[test]
    fn render_release_event_log_stdout() {
        render_release_event(test_event("log", Some("stdout"), Some("resolving commit"), None));
    }

    #[test]
    fn render_release_event_log_stderr() {
        render_release_event(test_event("log", Some("stderr"), Some("warning: slow"), None));
    }

    #[test]
    fn render_release_event_empty_text_skipped() {
        render_release_event(test_event("log", None, Some("   "), None));
    }

    #[test]
    fn render_project_validation_event_with_text() {
        render_project_validation_event(test_event("log", None, Some("validating project structure"), None));
    }

    #[test]
    fn render_project_validation_event_empty_text_skipped() {
        render_project_validation_event(test_event("log", None, Some("  "), None));
    }

    #[test]
    fn print_release_start_with_ref() {
        print_release_start("prj_abc", "production", Some("v1.0"), None);
    }

    #[test]
    fn print_release_start_with_commit() {
        print_release_start("prj_abc", "production", None, Some("abc123"));
    }

    #[test]
    fn print_release_start_with_neither() {
        print_release_start("prj_abc", "production", None, None);
    }

    #[test]
    fn print_release_success_formats() {
        print_release_success("prj_abc", "production", Some("abc123"));
    }

    #[test]
    fn print_release_success_no_sha() {
        print_release_success("prj_abc", "production", None);
    }

    #[test]
    fn print_release_already_released_formats() {
        print_release_already_released("prj_abc", "production", "abc123");
    }

    #[test]
    fn print_release_failure_formats() {
        print_release_failure("prj_abc", "production", "commit not found");
    }

    #[test]
    fn print_project_create_start_formats() {
        print_project_create_start("https://github.com/org/repo.git", ".");
    }
}
