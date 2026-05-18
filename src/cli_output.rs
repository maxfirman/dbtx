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
        project.default_branch.as_deref().unwrap_or(""),
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
