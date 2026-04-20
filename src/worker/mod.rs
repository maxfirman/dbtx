//! Worker execution runtime: dbt process management, git worktree materialization, and event streaming.

pub(crate) mod git;
mod validation;

use crate::api::{
    InvocationClaimResponse, InvocationCommandApi, InvocationCompleteApiRequest,
    InvocationEventBatchApiRequest, InvocationExecutionModeApi, InvocationExecutionSpecApi,
    InvocationHeartbeatApiRequest, InvocationLifecycleStatus,
};
use crate::client::DaemonClient;
use crate::db::validate_remote_project_root;
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use git::{ensure_git_worktree, git_cache_root, short_hash};
use serde_yaml::Value as YamlValue;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use tracing::{info, warn};
use validation::{
    execute_environment_prepare, execute_environment_validation, execute_project_validation,
    execute_release_validation,
};

const DBTX_SELECTED_RESOURCES_HOOK: &str = "{{ dbtx__log_selected_resources() }}";
const DBTX_SELECTED_RESOURCES_MACRO_FILE: &str = "_dbtx_selected_resources.sql";
const DBTX_SELECTED_RESOURCES_MACRO: &str = r#"{% macro dbtx__log_selected_resources() %}
  {% if execute %}
    {% set payload = {"selected_resources": selected_resources} %}
    {% do log("DBTX_SELECTED_RESOURCES::" ~ (payload | tojson), info=True) %}
  {% endif %}
{% endmacro %}
"#;

pub async fn execute_claimed_invocation(
    client: &DaemonClient,
    claim: InvocationClaimResponse,
    expected_invocation_id: Option<uuid::Uuid>,
) -> AppResult<()> {
    if let Some(expected) = expected_invocation_id
        && claim.invocation_id != expected
    {
        return Err(AppError::Internal(format!(
            "claimed unexpected invocation {}, expected {}",
            claim.invocation_id, expected
        )));
    }

    let spec = claim.execution_spec.clone();
    if matches!(spec, InvocationExecutionSpecApi::ReleaseValidation { .. }) {
        return execute_release_validation(client, claim, &spec).await;
    }
    if matches!(spec, InvocationExecutionSpecApi::ProjectValidation { .. }) {
        return execute_project_validation(client, claim, &spec).await;
    }
    if matches!(spec, InvocationExecutionSpecApi::EnvironmentPrepare { .. }) {
        return execute_environment_prepare(client, claim, &spec).await;
    }
    if matches!(spec, InvocationExecutionSpecApi::EnvironmentValidate { .. }) {
        return execute_environment_validation(client, claim, &spec).await;
    }
    let command_name = spec.command();
    let pretty_terminal_output = claim.execution_mode == InvocationExecutionModeApi::Local;
    let project_dir = match materialize_execution_project_dir(&spec).await {
        Ok(project_dir) => project_dir,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };
    info!(
        invocation_id = %claim.invocation_id,
        worker_id = %claim.worker_id,
        command = ?command_name,
        project_dir = %project_dir.display(),
        "starting claimed invocation execution"
    );
    if let Err(err) = prepare_runtime_project_for_execution(&spec, command_name, &project_dir).await {
        report_setup_failure(client, &claim, &err.to_string()).await?;
        return Err(err);
    }
    let profiles_dir = match write_profiles_dir(spec.profiles_yml()) {
        Ok(profiles_dir) => profiles_dir,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };
    let state_dir = match write_state_dir(spec.state_manifest()) {
        Ok(state_dir) => state_dir,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };

    let mut dbt_args: Vec<std::ffi::OsString> = spec.args().iter().cloned().map(Into::into).collect();
    if let Some(state_dir) = state_dir.as_ref() {
        dbt_args.push("--state".into());
        dbt_args.push(state_dir.path().as_os_str().to_os_string());
    }
    dbt_args.push("--profiles-dir".into());
    dbt_args.push(profiles_dir.path().as_os_str().to_os_string());

    let command = map_command(command_name);
    let dbt_path = std::env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string());
    let mut dbt_child = match crate::dbt_runner::DbtChild::spawn(&dbt_path, command, &dbt_args, &project_dir) {
        Ok(child) => child,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };

    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(2));
    let mut dbt_version: Option<String> = None;
    let mut cancel_requested = false;

    loop {
        tokio::select! {
            line = dbt_child.stdout_lines.next_line() => {
                let Some(line) = line? else { break; };
                if persists_state(command_name)
                    && let Some(event) = LogEvent::parse(&line)
                {
                    if dbt_version.is_none() && event.info.name == "MainReportVersion" {
                        dbt_version = event.data.get("version")
                            .and_then(serde_json::Value::as_str)
                            .map(ToString::to_string);
                    }
                    emit_dbt_log_output(pretty_terminal_output, claim.invocation_id, &claim.worker_id, &event);
                    send_event(client, &claim, crate::execution::ExecutionEvent {
                        kind: crate::execution::ExecutionEventKind::DbtLog,
                        occurred_at: chrono::Utc::now(),
                        text: event.render_text_line(),
                        raw_line: Some(line),
                        dbt_event_name: Some(event.info.name.clone()),
                        node_unique_id: event.data.get("node_info")
                            .and_then(|v| v.get("unique_id"))
                            .and_then(|v| v.as_str())
                            .map(ToString::to_string),
                        level: Some(event.info.level.clone()),
                        error: None,
                    }).await?;
                } else {
                    emit_stream_output(pretty_terminal_output, claim.invocation_id, &claim.worker_id, "stdout", &line);
                    send_event(client, &claim, crate::execution::ExecutionEvent {
                        kind: crate::execution::ExecutionEventKind::StdoutLine,
                        occurred_at: chrono::Utc::now(),
                        text: Some(line.clone()),
                        raw_line: Some(line),
                        dbt_event_name: None,
                        node_unique_id: None,
                        level: None,
                        error: None,
                    }).await?;
                }
            }
            _ = heartbeat.tick() => {
                let hb = client.invocation_heartbeat(claim.invocation_id, InvocationHeartbeatApiRequest {
                    worker_id: claim.worker_id.clone(),
                    lease_token: claim.lease_token,
                }).await?;
                if hb.cancel_requested {
                    warn!(invocation_id = %claim.invocation_id, worker_id = %claim.worker_id, "cancel requested by control plane");
                    cancel_requested = true;
                    dbt_child.start_kill();
                }
            }
        }
    }

    let result = dbt_child.wait().await?;
    for line in &result.stderr_lines {
        emit_stream_output(pretty_terminal_output, claim.invocation_id, &claim.worker_id, "stderr", line);
        send_event(client, &claim, crate::execution::ExecutionEvent {
            kind: crate::execution::ExecutionEventKind::StderrLine,
            occurred_at: chrono::Utc::now(),
            text: Some(line.clone()),
            raw_line: Some(line.clone()),
            dbt_event_name: None,
            node_unique_id: None,
            level: None,
            error: None,
        }).await?;
    }

    let exit_code = if cancel_requested { 130 } else { result.exit_code };
    let manifest = if persists_state(command_name) {
        let manifest_path = project_dir.join("target").join("manifest.json");
        std::fs::read_to_string(&manifest_path).ok().and_then(|c| serde_json::from_str(&c).ok())
    } else {
        None
    };

    client.invocation_complete(claim.invocation_id, InvocationCompleteApiRequest {
        worker_id: claim.worker_id.clone(),
        lease_token: claim.lease_token,
        completion: crate::execution::ExecutionCompletion {
            status: if cancel_requested { InvocationLifecycleStatus::Canceled }
                else if exit_code != 0 { InvocationLifecycleStatus::Failed }
                else { InvocationLifecycleStatus::Succeeded },
            exit_code,
            error: if cancel_requested { Some("invocation canceled".to_string()) }
                else if exit_code == 0 { None }
                else { Some(format!("dbt invocation failed with exit code {exit_code}")) },
            dbt_version,
            manifest,
            result: None,
        },
    }).await?;

    info!(invocation_id = %claim.invocation_id, worker_id = %claim.worker_id, exit_code, canceled = cancel_requested, "finished claimed invocation execution");

    if cancel_requested { Err(AppError::InvocationCanceled) }
    else if exit_code == 0 { Ok(()) }
    else { Err(AppError::DbtFailed(exit_code)) }
}

async fn report_setup_failure(client: &DaemonClient, claim: &InvocationClaimResponse, error_message: &str) -> AppResult<()> {
    client.invocation_complete(claim.invocation_id, InvocationCompleteApiRequest {
        worker_id: claim.worker_id.clone(),
        lease_token: claim.lease_token,
        completion: crate::execution::ExecutionCompletion {
            status: InvocationLifecycleStatus::Failed,
            exit_code: 1,
            error: Some(error_message.to_string()),
            dbt_version: None,
            manifest: None,
            result: None,
        },
    }).await
}

fn emit_dbt_log_output(pretty_terminal_output: bool, invocation_id: uuid::Uuid, worker_id: &str, event: &LogEvent) {
    let rendered = event.render_text_line();
    if pretty_terminal_output {
        if let Some(rendered) = rendered { println!("{rendered}"); }
        return;
    }
    info!(
        invocation_id = %invocation_id, worker_id = %worker_id,
        event_type = "dbt.log", dbt_event_name = %event.info.name, level = %event.info.level,
        node_unique_id = event.data.get("node_info").and_then(|v| v.get("unique_id")).and_then(|v| v.as_str()).unwrap_or(""),
        text = rendered.as_deref().unwrap_or(""),
        "worker invocation event"
    );
}

fn emit_stream_output(pretty_terminal_output: bool, invocation_id: uuid::Uuid, worker_id: &str, stream: &'static str, line: &str) {
    if pretty_terminal_output {
        match stream { "stderr" => eprintln!("{line}"), _ => println!("{line}") }
        return;
    }
    info!(
        invocation_id = %invocation_id, worker_id = %worker_id,
        event_type = if stream == "stderr" { "stderr.line" } else { "stdout.line" },
        stream = %stream, text = %line, "worker invocation event"
    );
}

async fn send_event(client: &DaemonClient, claim: &InvocationClaimResponse, event: crate::execution::ExecutionEvent) -> AppResult<()> {
    client.invocation_append_events(claim.invocation_id, InvocationEventBatchApiRequest {
        worker_id: claim.worker_id.clone(),
        lease_token: claim.lease_token,
        events: vec![event],
    }).await
}

impl InvocationExecutionSpecApi {
    fn command(&self) -> InvocationCommandApi {
        match self {
            Self::Local { command, .. } | Self::Remote { command, .. } => *command,
            Self::ReleaseValidation { .. } => InvocationCommandApi::Release,
            Self::ProjectValidation { .. } => InvocationCommandApi::ProjectValidate,
            Self::EnvironmentPrepare { .. } => InvocationCommandApi::EnvironmentPrepare,
            Self::EnvironmentValidate { .. } => InvocationCommandApi::EnvironmentValidate,
        }
    }

    fn args(&self) -> &[String] {
        match self {
            Self::Local { args, .. } | Self::Remote { args, .. } => args,
            _ => &[],
        }
    }

    fn profiles_yml(&self) -> &str {
        match self {
            Self::Local { profiles_yml, .. } | Self::Remote { profiles_yml, .. } => profiles_yml,
            Self::EnvironmentValidate { profiles_yml, .. } => profiles_yml,
            _ => "",
        }
    }

    fn state_manifest(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Local { state_manifest, .. } | Self::Remote { state_manifest, .. } => state_manifest.as_ref(),
            _ => None,
        }
    }
}

async fn materialize_execution_project_dir(spec: &InvocationExecutionSpecApi) -> AppResult<PathBuf> {
    match spec {
        InvocationExecutionSpecApi::Local { project_dir, .. } => Ok(PathBuf::from(project_dir)),
        InvocationExecutionSpecApi::Remote { repo_url, commit_sha, project_root, .. } => {
            let worktree_root = ensure_git_worktree(repo_url, commit_sha).await?;
            validate_remote_project_root(project_root)?;
            let project_dir = if project_root == "." || project_root.is_empty() { worktree_root } else { worktree_root.join(project_root) };
            if !project_dir.join("dbt_project.yml").is_file() { return Err(AppError::NotDbtProjectRoot); }
            Ok(project_dir)
        }
        InvocationExecutionSpecApi::ReleaseValidation { .. } => Err(AppError::UnsupportedLocalExecution("release".to_string())),
        InvocationExecutionSpecApi::ProjectValidation { .. } => Err(AppError::UnsupportedLocalExecution("project_validate".to_string())),
        InvocationExecutionSpecApi::EnvironmentPrepare { .. } => Err(AppError::UnsupportedLocalExecution("environment_prepare".to_string())),
        InvocationExecutionSpecApi::EnvironmentValidate { .. } => Err(AppError::UnsupportedLocalExecution("environment_validate".to_string())),
    }
}

async fn prepare_runtime_project_for_execution(spec: &InvocationExecutionSpecApi, command: InvocationCommandApi, project_dir: &Path) -> AppResult<()> {
    if !persists_state(command) { return Ok(()); }
    let InvocationExecutionSpecApi::Remote { repo_url, .. } = spec else { return Ok(()); };
    patch_remote_runtime_project(repo_url, project_dir).await
}

async fn patch_remote_runtime_project(repo_url: &str, project_dir: &Path) -> AppResult<()> {
    let cache_root = git_cache_root()?;
    let repo_hash = short_hash(repo_url);
    let _repo_lock = git::acquire_repo_lock(&cache_root, &repo_hash).await?;
    patch_runtime_project_yaml(project_dir)?;
    Ok(())
}

fn patch_runtime_project_yaml(project_dir: &Path) -> AppResult<()> {
    let project_path = project_dir.join("dbt_project.yml");
    let mut project_yaml: YamlValue = serde_yaml::from_str(&std::fs::read_to_string(&project_path)?)?;
    ensure_on_run_start_hook(&mut project_yaml)?;
    let macro_file = ensure_selected_resources_macro_file(project_dir, &project_yaml)?;
    std::fs::write(project_path, serde_yaml::to_string(&project_yaml)?)?;
    std::fs::write(macro_file, DBTX_SELECTED_RESOURCES_MACRO)?;
    Ok(())
}

fn ensure_on_run_start_hook(project_yaml: &mut YamlValue) -> AppResult<()> {
    let mapping = project_yaml.as_mapping_mut().ok_or_else(|| AppError::Internal("dbt_project.yml must be a YAML mapping".to_string()))?;
    let key = YamlValue::String("on-run-start".to_string());
    let hook = YamlValue::String(DBTX_SELECTED_RESOURCES_HOOK.to_string());
    let updated = match mapping.remove(&key) {
        Some(YamlValue::Sequence(mut seq)) => {
            if !seq.iter().any(|v| v.as_str() == Some(DBTX_SELECTED_RESOURCES_HOOK)) { seq.push(hook); }
            YamlValue::Sequence(seq)
        }
        Some(YamlValue::String(existing)) => {
            if existing == DBTX_SELECTED_RESOURCES_HOOK { YamlValue::String(existing) }
            else { YamlValue::Sequence(vec![YamlValue::String(existing), hook]) }
        }
        Some(YamlValue::Null) | None => YamlValue::Sequence(vec![hook]),
        Some(_) => return Err(AppError::Internal("unsupported on-run-start hook shape in dbt_project.yml".to_string())),
    };
    mapping.insert(key, updated);
    Ok(())
}

fn ensure_selected_resources_macro_file(project_dir: &Path, project_yaml: &YamlValue) -> AppResult<PathBuf> {
    let macro_root = project_yaml.get("macro-paths").and_then(first_yaml_path).unwrap_or_else(|| PathBuf::from("macros"));
    let macro_dir = project_dir.join(macro_root);
    std::fs::create_dir_all(&macro_dir)?;
    Ok(macro_dir.join(DBTX_SELECTED_RESOURCES_MACRO_FILE))
}

fn first_yaml_path(value: &YamlValue) -> Option<PathBuf> {
    match value {
        YamlValue::Sequence(seq) => seq.iter().find_map(|item| item.as_str().filter(|p| !p.trim().is_empty())).map(PathBuf::from),
        YamlValue::String(path) if !path.trim().is_empty() => Some(PathBuf::from(path)),
        _ => None,
    }
}

fn persists_state(command: InvocationCommandApi) -> bool {
    !matches!(command, InvocationCommandApi::Ls | InvocationCommandApi::Release | InvocationCommandApi::ProjectValidate | InvocationCommandApi::EnvironmentPrepare | InvocationCommandApi::EnvironmentValidate)
}

fn map_command(command: InvocationCommandApi) -> &'static str {
    match command {
        InvocationCommandApi::Build => "build",
        InvocationCommandApi::Run => "run",
        InvocationCommandApi::Ls => "ls",
        InvocationCommandApi::Test => "test",
        InvocationCommandApi::Seed => "seed",
        InvocationCommandApi::Release => "release",
        InvocationCommandApi::ProjectValidate => "project_validate",
        InvocationCommandApi::EnvironmentPrepare => "environment_prepare",
        InvocationCommandApi::EnvironmentValidate => "environment_validate",
        InvocationCommandApi::ManifestPrepare => "parse",
    }
}

fn write_profiles_dir(profiles_yml: &str) -> AppResult<TempDir> {
    let temp_dir = TempDir::new()?;
    std::fs::write(temp_dir.path().join("profiles.yml"), profiles_yml)?;
    Ok(temp_dir)
}

fn write_state_dir(state_manifest: Option<&serde_json::Value>) -> AppResult<Option<TempDir>> {
    let Some(state_manifest) = state_manifest else { return Ok(None); };
    let temp_dir = TempDir::new()?;
    std::fs::write(temp_dir.path().join("manifest.json"), serde_json::to_vec(state_manifest)?)?;
    Ok(Some(temp_dir))
}

#[cfg(test)]
mod tests {
    use super::{
        DBTX_SELECTED_RESOURCES_HOOK, DBTX_SELECTED_RESOURCES_MACRO_FILE,
        git::ensure_git_worktree_in, patch_runtime_project_yaml,
        git::resolve_remote_git_target,
    };
    use crate::db::validate_remote_project_root;
    use std::path::Path;
    use std::process::Command;
    use tempfile::TempDir;

    #[test]
    fn rejects_invalid_remote_project_roots() {
        assert!(validate_remote_project_root(".").is_ok());
        assert!(validate_remote_project_root("analytics").is_ok());
        assert!(validate_remote_project_root("../analytics").is_err());
        assert!(validate_remote_project_root("/tmp/analytics").is_err());
    }

    #[tokio::test]
    async fn ensure_git_worktree_is_safe_under_concurrency() {
        let repo = TempDir::new().expect("repo dir");
        let cache = TempDir::new().expect("cache dir");
        std::fs::write(repo.path().join("dbt_project.yml"), "name: proj\n").expect("dbt project");
        run_git(["init"], repo.path());
        run_git(["config", "user.email", "test@example.com"], repo.path());
        run_git(["config", "user.name", "Test User"], repo.path());
        run_git(["add", "dbt_project.yml"], repo.path());
        run_git(["commit", "-m", "init"], repo.path());
        let commit_sha = git_output(["rev-parse", "HEAD"], repo.path());

        let (first, second) = tokio::join!(
            ensure_git_worktree_in(cache.path(), repo.path().to_str().expect("repo str"), &commit_sha),
            ensure_git_worktree_in(cache.path(), repo.path().to_str().expect("repo str"), &commit_sha)
        );
        let first = first.expect("first worktree");
        let second = second.expect("second worktree");
        assert_eq!(first, second);
        assert!(first.join("dbt_project.yml").is_file());
    }

    #[tokio::test]
    async fn resolve_remote_git_target_fails_for_missing_ref() {
        let repo = TempDir::new().expect("repo dir");
        run_git(["init"], repo.path());
        run_git(["config", "user.email", "test@example.com"], repo.path());
        run_git(["config", "user.name", "Test User"], repo.path());
        std::fs::write(repo.path().join("README.md"), "hello").expect("readme");
        run_git(["add", "README.md"], repo.path());
        run_git(["commit", "-m", "init"], repo.path());

        let error = resolve_remote_git_target(repo.path().to_str().expect("repo str"), Some("missing-branch"), None)
            .await.expect_err("expected missing ref error");
        assert!(error.to_string().contains("ref missing-branch is not available from remote repository"));
    }

    #[tokio::test]
    async fn resolve_remote_git_target_fails_for_missing_commit() {
        let repo = TempDir::new().expect("repo dir");
        run_git(["init"], repo.path());
        run_git(["config", "user.email", "test@example.com"], repo.path());
        run_git(["config", "user.name", "Test User"], repo.path());
        std::fs::write(repo.path().join("README.md"), "hello").expect("readme");
        run_git(["add", "README.md"], repo.path());
        run_git(["commit", "-m", "init"], repo.path());

        let error = resolve_remote_git_target(repo.path().to_str().expect("repo str"), None, Some("deadbeef"))
            .await.expect_err("expected missing commit error");
        assert!(error.to_string().contains("commit deadbeef is not available from remote repository"));
    }

    #[test]
    fn patch_runtime_project_yaml_appends_selected_resources_hook() {
        let project = TempDir::new().expect("project dir");
        std::fs::create_dir_all(project.path().join("custom_macros")).expect("macro dir");
        std::fs::write(
            project.path().join("dbt_project.yml"),
            "name: demo\nmacro-paths: [custom_macros]\non-run-start: [\"{{ existing_hook() }}\"]\n",
        ).expect("dbt project");

        patch_runtime_project_yaml(project.path()).expect("patch project");
        patch_runtime_project_yaml(project.path()).expect("patch project idempotently");

        let patched = std::fs::read_to_string(project.path().join("dbt_project.yml")).expect("patched yaml");
        assert!(patched.contains("existing_hook"));
        assert!(patched.contains(DBTX_SELECTED_RESOURCES_HOOK));
        assert_eq!(patched.matches(DBTX_SELECTED_RESOURCES_HOOK).count(), 1);
        assert!(project.path().join("custom_macros").join(DBTX_SELECTED_RESOURCES_MACRO_FILE).is_file());
    }

    fn run_git<const N: usize>(args: [&str; N], cwd: &Path) {
        let output = Command::new("git").args(args).current_dir(cwd).output().expect("git command");
        assert!(output.status.success(), "git failed\nstdout:\n{}\nstderr:\n{}", String::from_utf8_lossy(&output.stdout), String::from_utf8_lossy(&output.stderr));
    }

    fn git_output<const N: usize>(args: [&str; N], cwd: &Path) -> String {
        let output = Command::new("git").args(args).current_dir(cwd).output().expect("git output");
        assert!(output.status.success(), "git failed");
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }
}
