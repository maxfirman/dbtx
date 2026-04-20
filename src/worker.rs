//! Worker execution runtime: dbt process management, git worktree materialization, and event streaming.
use crate::api::{
    InvocationClaimResponse, InvocationCommandApi, InvocationCompleteApiRequest,
    InvocationEventBatchApiRequest, InvocationExecutionModeApi, InvocationExecutionSpecApi,
    InvocationHeartbeatApiRequest, InvocationLifecycleStatus,
};
use crate::client::DaemonClient;
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use crate::services::read_dbt_project_name_from_root;
use serde_yaml::Value as YamlValue;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};
use tempfile::TempDir;
use tokio::process::Command as TokioCommand;
use tracing::{info, warn};

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
    if let Err(err) = prepare_runtime_project_for_execution(&spec, command_name, &project_dir).await
    {
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

    let mut dbt_args: Vec<OsString> = spec.args().iter().cloned().map(Into::into).collect();
    if let Some(state_dir) = state_dir.as_ref() {
        dbt_args.push("--state".into());
        dbt_args.push(state_dir.path().as_os_str().to_os_string());
    }
    dbt_args.push("--profiles-dir".into());
    dbt_args.push(profiles_dir.path().as_os_str().to_os_string());

    let command = map_command(command_name);
    let dbt_path = std::env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string());
    let mut dbt_child = match crate::dbt_runner::DbtChild::spawn(
        &dbt_path,
        command,
        &dbt_args,
        &project_dir,
    ) {
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
                        dbt_version = event
                            .data
                            .get("version")
                            .and_then(serde_json::Value::as_str)
                            .map(ToString::to_string);
                    }
                    emit_dbt_log_output(
                        pretty_terminal_output,
                        claim.invocation_id,
                        &claim.worker_id,
                        &event,
                    );
                    send_event(client, &claim, crate::execution::ExecutionEvent {
                        kind: crate::execution::ExecutionEventKind::DbtLog,
                        occurred_at: chrono::Utc::now(),
                        text: event.render_text_line(),
                        raw_line: Some(line),
                        dbt_event_name: Some(event.info.name.clone()),
                        node_unique_id: event
                            .data
                            .get("node_info")
                            .and_then(|value| value.get("unique_id"))
                            .and_then(|value| value.as_str())
                            .map(ToString::to_string),
                        level: Some(event.info.level.clone()),
                        error: None,
                    }).await?;
                } else {
                    emit_stream_output(
                        pretty_terminal_output,
                        claim.invocation_id,
                        &claim.worker_id,
                        "stdout",
                        &line,
                    );
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
                let hb = client
                    .invocation_heartbeat(
                        claim.invocation_id,
                        InvocationHeartbeatApiRequest {
                            worker_id: claim.worker_id.clone(),
                            lease_token: claim.lease_token,
                        },
                    )
                    .await?;
                if hb.cancel_requested {
                    warn!(
                        invocation_id = %claim.invocation_id,
                        worker_id = %claim.worker_id,
                        "cancel requested by control plane"
                    );
                    cancel_requested = true;
                    dbt_child.start_kill();
                }
            }
        }
    }

    let result = dbt_child.wait().await?;
    for line in &result.stderr_lines {
        emit_stream_output(
            pretty_terminal_output,
            claim.invocation_id,
            &claim.worker_id,
            "stderr",
            line,
        );
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

    let exit_code = if cancel_requested {
        130
    } else {
        result.exit_code
    };
    let manifest = if persists_state(command_name) {
        let manifest_path = project_dir.join("target").join("manifest.json");
        std::fs::read_to_string(&manifest_path)
            .ok()
            .and_then(|content| serde_json::from_str(&content).ok())
    } else {
        None
    };

    client
        .invocation_complete(
            claim.invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id.clone(),
                lease_token: claim.lease_token,
                completion: crate::execution::ExecutionCompletion {
                    status: if cancel_requested {
                        InvocationLifecycleStatus::Canceled
                    } else if exit_code != 0 {
                        InvocationLifecycleStatus::Failed
                    } else {
                        InvocationLifecycleStatus::Succeeded
                    },
                    exit_code,
                    error: if cancel_requested {
                        Some("invocation canceled".to_string())
                    } else if exit_code == 0 {
                        None
                    } else {
                        Some(format!("dbt invocation failed with exit code {exit_code}"))
                    },
                    dbt_version,
                    manifest,
                    result: None,
                },
            },
        )
        .await?;

    info!(
        invocation_id = %claim.invocation_id,
        worker_id = %claim.worker_id,
        exit_code,
        canceled = cancel_requested,
        "finished claimed invocation execution"
    );

    if cancel_requested {
        Err(AppError::InvocationCanceled)
    } else if exit_code == 0 {
        Ok(())
    } else {
        Err(AppError::DbtFailed(exit_code))
    }
}

async fn report_setup_failure(
    client: &DaemonClient,
    claim: &InvocationClaimResponse,
    error_message: &str,
) -> AppResult<()> {
    client
        .invocation_complete(
            claim.invocation_id,
            InvocationCompleteApiRequest {
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
            },
        )
        .await
}

fn emit_dbt_log_output(
    pretty_terminal_output: bool,
    invocation_id: uuid::Uuid,
    worker_id: &str,
    event: &LogEvent,
) {
    let rendered = event.render_text_line();
    if pretty_terminal_output {
        if let Some(rendered) = rendered {
            println!("{rendered}");
        }
        return;
    }
    info!(
        invocation_id = %invocation_id,
        worker_id = %worker_id,
        event_type = "dbt.log",
        dbt_event_name = %event.info.name,
        level = %event.info.level,
        node_unique_id = event
            .data
            .get("node_info")
            .and_then(|value| value.get("unique_id"))
            .and_then(|value| value.as_str())
            .unwrap_or(""),
        text = rendered.as_deref().unwrap_or(""),
        "worker invocation event"
    );
}

fn emit_stream_output(
    pretty_terminal_output: bool,
    invocation_id: uuid::Uuid,
    worker_id: &str,
    stream: &'static str,
    line: &str,
) {
    if pretty_terminal_output {
        match stream {
            "stderr" => eprintln!("{line}"),
            _ => println!("{line}"),
        }
        return;
    }
    info!(
        invocation_id = %invocation_id,
        worker_id = %worker_id,
        event_type = if stream == "stderr" { "stderr.line" } else { "stdout.line" },
        stream = %stream,
        text = %line,
        "worker invocation event"
    );
}

/// Send a single execution event to the server for the given invocation.
async fn send_event(
    client: &DaemonClient,
    claim: &InvocationClaimResponse,
    event: crate::execution::ExecutionEvent,
) -> AppResult<()> {
    client
        .invocation_append_events(
            claim.invocation_id,
            InvocationEventBatchApiRequest {
                worker_id: claim.worker_id.clone(),
                lease_token: claim.lease_token,
                events: vec![event],
            },
        )
        .await
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
            Self::ReleaseValidation { .. } => &[],
            Self::ProjectValidation { .. } => &[],
            Self::EnvironmentPrepare { .. } => &[],
            Self::EnvironmentValidate { .. } => &[],
        }
    }

    fn profiles_yml(&self) -> &str {
        match self {
            Self::Local { profiles_yml, .. } | Self::Remote { profiles_yml, .. } => profiles_yml,
            Self::ReleaseValidation { .. } => "",
            Self::ProjectValidation { .. } => "",
            Self::EnvironmentPrepare { .. } => "",
            Self::EnvironmentValidate { profiles_yml, .. } => profiles_yml,
        }
    }

    fn state_manifest(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Local { state_manifest, .. } | Self::Remote { state_manifest, .. } => {
                state_manifest.as_ref()
            }
            Self::ReleaseValidation { .. } => None,
            Self::ProjectValidation { .. } => None,
            Self::EnvironmentPrepare { .. } => None,
            Self::EnvironmentValidate { .. } => None,
        }
    }
}

async fn execute_release_validation(
    client: &DaemonClient,
    claim: InvocationClaimResponse,
    spec: &InvocationExecutionSpecApi,
) -> AppResult<()> {
    let InvocationExecutionSpecApi::ReleaseValidation {
        repo_url,
        git_ref,
        git_commit_sha,
        git_branch,
    } = spec
    else {
        return Err(AppError::Internal("release validation requires release spec".to_string()));
    };

    send_worker_event(
        client,
        &claim,
        crate::execution::ExecutionEventKind::StdoutLine,
        format!("Validating release target against {repo_url}"),
    )
    .await?;
    if let Some(git_ref) = git_ref {
        send_worker_event(
            client,
            &claim,
            crate::execution::ExecutionEventKind::StdoutLine,
            format!("Resolving git ref {git_ref}"),
        )
        .await?;
    } else if let Some(git_commit_sha) = git_commit_sha {
        send_worker_event(
            client,
            &claim,
            crate::execution::ExecutionEventKind::StdoutLine,
            format!("Checking commit {git_commit_sha}"),
        )
        .await?;
    }
    let resolved_commit_sha =
        match resolve_remote_git_target(repo_url, git_ref.as_deref(), git_commit_sha.as_deref())
            .await
        {
            Ok(resolved_commit_sha) => resolved_commit_sha,
            Err(err) => {
                report_setup_failure(client, &claim, &err.to_string()).await?;
                return Err(err);
            }
        };
    send_worker_event(
        client,
        &claim,
        crate::execution::ExecutionEventKind::StdoutLine,
        format!("Resolved release target to commit {resolved_commit_sha}"),
    )
    .await?;
    client
        .invocation_complete(
            claim.invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id.clone(),
                lease_token: claim.lease_token,
                completion: crate::execution::ExecutionCompletion {
                    status: InvocationLifecycleStatus::Succeeded,
                    exit_code: 0,
                    error: None,
                    dbt_version: None,
                    manifest: None,
                    result: Some(json!({
                        "resolved_commit_sha": resolved_commit_sha,
                        "git_branch": git_branch,
                    })),
                },
            },
        )
        .await?;
    Ok(())
}

async fn execute_project_validation(
    client: &DaemonClient,
    claim: InvocationClaimResponse,
    spec: &InvocationExecutionSpecApi,
) -> AppResult<()> {
    let InvocationExecutionSpecApi::ProjectValidation {
        repo_url,
        project_root,
    } = spec
    else {
        return Err(AppError::Internal("project validation requires project validation spec".to_string()));
    };

    send_worker_event(
        client,
        &claim,
        crate::execution::ExecutionEventKind::StdoutLine,
        format!("Validating project repository {repo_url}"),
    )
    .await?;
    send_worker_event(
        client,
        &claim,
        crate::execution::ExecutionEventKind::StdoutLine,
        format!("Checking project path {project_root}"),
    )
    .await?;

    let default_branch = match resolve_remote_default_branch(repo_url).await {
        Ok(branch) => branch,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };

    send_worker_event(
        client,
        &claim,
        crate::execution::ExecutionEventKind::StdoutLine,
        format!("Resolved default branch {default_branch}"),
    )
    .await?;

    let default_commit =
        match resolve_remote_git_target(repo_url, Some(&default_branch), None).await {
            Ok(commit) => commit,
            Err(err) => {
                report_setup_failure(client, &claim, &err.to_string()).await?;
                return Err(err);
            }
        };
    let repo_checkout = match ensure_git_worktree(repo_url, &default_commit).await {
        Ok(path) => path,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };
    let project_dir = repo_checkout.join(project_root);
    let project_name = match read_dbt_project_name_from_root(&project_dir) {
        Ok(name) => name,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };

    send_worker_event(
        client,
        &claim,
        crate::execution::ExecutionEventKind::StdoutLine,
        format!("Found dbt project {project_name}"),
    )
    .await?;

    client
        .invocation_complete(
            claim.invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id.clone(),
                lease_token: claim.lease_token,
                completion: crate::execution::ExecutionCompletion {
                    status: InvocationLifecycleStatus::Succeeded,
                    exit_code: 0,
                    error: None,
                    dbt_version: None,
                    manifest: None,
                    result: Some(json!({
                        "project_name": project_name,
                        "default_branch": default_branch,
                    })),
                },
            },
        )
        .await?;
    Ok(())
}

async fn execute_environment_prepare(
    client: &DaemonClient,
    claim: InvocationClaimResponse,
    spec: &InvocationExecutionSpecApi,
) -> AppResult<()> {
    let InvocationExecutionSpecApi::EnvironmentPrepare {
        repo_url,
        selected_branch,
    } = spec
    else {
        return Err(AppError::Internal("environment prepare requires environment prepare spec".to_string()));
    };

    send_worker_event(
        client,
        &claim,
        crate::execution::ExecutionEventKind::StdoutLine,
        format!("Loading branches for {repo_url}"),
    )
    .await?;

    let default_branch = match resolve_remote_default_branch(repo_url).await {
        Ok(branch) => branch,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };
    let active_branch = selected_branch
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default_branch.clone());

    send_worker_event(
        client,
        &claim,
        crate::execution::ExecutionEventKind::StdoutLine,
        format!("Resolved branch {active_branch}"),
    )
    .await?;

    let branches = match list_remote_branches(repo_url).await {
        Ok(branches) => branches,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };
    let latest_commit_sha = match resolve_remote_git_target(repo_url, Some(&active_branch), None).await {
        Ok(commit) => commit,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };
    let commits = match list_recent_branch_commits(repo_url, &active_branch, 50).await {
        Ok(commits) => commits,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };

    client
        .invocation_complete(
            claim.invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id.clone(),
                lease_token: claim.lease_token,
                completion: crate::execution::ExecutionCompletion {
                    status: InvocationLifecycleStatus::Succeeded,
                    exit_code: 0,
                    error: None,
                    dbt_version: None,
                    manifest: None,
                    result: Some(json!({
                        "default_branch": default_branch,
                        "selected_branch": active_branch,
                        "latest_commit_sha": latest_commit_sha,
                        "branches": branches,
                        "commits": commits,
                    })),
                },
            },
        )
        .await?;
    Ok(())
}

async fn execute_environment_validation(
    client: &DaemonClient,
    claim: InvocationClaimResponse,
    spec: &InvocationExecutionSpecApi,
) -> AppResult<()> {
    let InvocationExecutionSpecApi::EnvironmentValidate {
        repo_url,
        commit_sha,
        project_root,
        selected_branch,
        profiles_yml,
    } = spec
    else {
        return Err(AppError::Internal("environment validation requires environment validate spec".to_string()));
    };

    send_worker_event(
        client,
        &claim,
        crate::execution::ExecutionEventKind::StdoutLine,
        format!("Checking commit {commit_sha}"),
    )
    .await?;

    let repo_checkout = match ensure_git_worktree(repo_url, commit_sha).await {
        Ok(path) => path,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };
    validate_remote_project_root(project_root)?;
    let project_dir = if project_root == "." || project_root.is_empty() {
        repo_checkout
    } else {
        repo_checkout.join(project_root)
    };
    if !project_dir.join("dbt_project.yml").is_file() {
        let err = AppError::NotDbtProjectRoot;
        report_setup_failure(client, &claim, &err.to_string()).await?;
        return Err(err);
    }
    let profiles_dir = match write_profiles_dir(profiles_yml) {
        Ok(dir) => dir,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };

    run_validation_command(client, &claim, &project_dir, profiles_dir.path(), "deps").await?;
    run_validation_command(client, &claim, &project_dir, profiles_dir.path(), "debug").await?;
    run_validation_command(client, &claim, &project_dir, profiles_dir.path(), "compile").await?;

    client
        .invocation_complete(
            claim.invocation_id,
            InvocationCompleteApiRequest {
                worker_id: claim.worker_id.clone(),
                lease_token: claim.lease_token,
                completion: crate::execution::ExecutionCompletion {
                    status: InvocationLifecycleStatus::Succeeded,
                    exit_code: 0,
                    error: None,
                    dbt_version: None,
                    manifest: None,
                    result: Some(json!({
                        "resolved_commit_sha": commit_sha,
                        "selected_branch": selected_branch,
                    })),
                },
            },
        )
        .await?;
    Ok(())
}

async fn send_worker_event(
    client: &DaemonClient,
    claim: &InvocationClaimResponse,
    kind: crate::execution::ExecutionEventKind,
    text: String,
) -> AppResult<()> {
    emit_stream_output(
        claim.execution_mode == InvocationExecutionModeApi::Local,
        claim.invocation_id,
        &claim.worker_id,
        "stdout",
        &text,
    );
    send_event(client, claim, crate::execution::ExecutionEvent {
        kind,
        occurred_at: chrono::Utc::now(),
        text: Some(text),
        raw_line: None,
        dbt_event_name: None,
        node_unique_id: None,
        level: None,
        error: None,
    }).await
}

async fn materialize_execution_project_dir(
    spec: &InvocationExecutionSpecApi,
) -> AppResult<PathBuf> {
    match spec {
        InvocationExecutionSpecApi::Local { project_dir, .. } => Ok(PathBuf::from(project_dir)),
        InvocationExecutionSpecApi::Remote {
            repo_url,
            commit_sha,
            project_root,
            ..
        } => {
            let worktree_root = ensure_git_worktree(repo_url, commit_sha).await?;
            validate_remote_project_root(project_root)?;
            let project_dir = if project_root == "." || project_root.is_empty() {
                worktree_root
            } else {
                worktree_root.join(project_root)
            };
            if !project_dir.join("dbt_project.yml").is_file() {
                return Err(AppError::NotDbtProjectRoot);
            }
            Ok(project_dir)
        }
        InvocationExecutionSpecApi::ReleaseValidation { .. } => {
            Err(AppError::UnsupportedLocalExecution("release".to_string()))
        }
        InvocationExecutionSpecApi::ProjectValidation { .. } => Err(
            AppError::UnsupportedLocalExecution("project_validate".to_string()),
        ),
        InvocationExecutionSpecApi::EnvironmentPrepare { .. } => Err(
            AppError::UnsupportedLocalExecution("environment_prepare".to_string()),
        ),
        InvocationExecutionSpecApi::EnvironmentValidate { .. } => Err(
            AppError::UnsupportedLocalExecution("environment_validate".to_string()),
        ),
    }
}

async fn prepare_runtime_project_for_execution(
    spec: &InvocationExecutionSpecApi,
    command: InvocationCommandApi,
    project_dir: &Path,
) -> AppResult<()> {
    if !persists_state(command) {
        return Ok(());
    }

    let InvocationExecutionSpecApi::Remote { repo_url, .. } = spec else {
        return Ok(());
    };

    patch_remote_runtime_project(repo_url, project_dir).await
}

async fn patch_remote_runtime_project(repo_url: &str, project_dir: &Path) -> AppResult<()> {
    let cache_root = git_cache_root()?;
    let repo_hash = short_hash(repo_url);
    let _repo_lock = acquire_repo_lock(&cache_root, &repo_hash).await?;
    patch_runtime_project_yaml(project_dir)?;
    Ok(())
}

fn patch_runtime_project_yaml(project_dir: &Path) -> AppResult<()> {
    let project_path = project_dir.join("dbt_project.yml");
    let mut project_yaml: YamlValue =
        serde_yaml::from_str(&std::fs::read_to_string(&project_path)?)?;

    ensure_on_run_start_hook(&mut project_yaml)?;
    let macro_file = ensure_selected_resources_macro_file(project_dir, &project_yaml)?;

    std::fs::write(project_path, serde_yaml::to_string(&project_yaml)?)?;
    std::fs::write(macro_file, DBTX_SELECTED_RESOURCES_MACRO)?;
    Ok(())
}

fn ensure_on_run_start_hook(project_yaml: &mut YamlValue) -> AppResult<()> {
    let mapping = project_yaml.as_mapping_mut().ok_or_else(|| {
        AppError::Internal("dbt_project.yml must be a YAML mapping".to_string())
    })?;
    let key = YamlValue::String("on-run-start".to_string());
    let hook = YamlValue::String(DBTX_SELECTED_RESOURCES_HOOK.to_string());
    let updated = match mapping.remove(&key) {
        Some(YamlValue::Sequence(mut sequence)) => {
            if !sequence
                .iter()
                .any(|value| value.as_str() == Some(DBTX_SELECTED_RESOURCES_HOOK))
            {
                sequence.push(hook);
            }
            YamlValue::Sequence(sequence)
        }
        Some(YamlValue::String(existing)) => {
            if existing == DBTX_SELECTED_RESOURCES_HOOK {
                YamlValue::String(existing)
            } else {
                YamlValue::Sequence(vec![YamlValue::String(existing), hook])
            }
        }
        Some(YamlValue::Null) | None => YamlValue::Sequence(vec![hook]),
        Some(_) => {
            return Err(AppError::Internal(
                "unsupported on-run-start hook shape in dbt_project.yml".to_string(),
            ));
        }
    };
    mapping.insert(key, updated);
    Ok(())
}

fn ensure_selected_resources_macro_file(
    project_dir: &Path,
    project_yaml: &YamlValue,
) -> AppResult<PathBuf> {
    let macro_root = project_yaml
        .get("macro-paths")
        .and_then(first_yaml_path)
        .unwrap_or_else(|| PathBuf::from("macros"));
    let macro_dir = project_dir.join(macro_root);
    std::fs::create_dir_all(&macro_dir)?;
    Ok(macro_dir.join(DBTX_SELECTED_RESOURCES_MACRO_FILE))
}

fn first_yaml_path(value: &YamlValue) -> Option<PathBuf> {
    match value {
        YamlValue::Sequence(sequence) => sequence
            .iter()
            .find_map(|item| item.as_str().filter(|path| !path.trim().is_empty()))
            .map(PathBuf::from),
        YamlValue::String(path) if !path.trim().is_empty() => Some(PathBuf::from(path)),
        _ => None,
    }
}

async fn ensure_git_worktree(repo_url: &str, commit_sha: &str) -> AppResult<PathBuf> {
    let cache_root = git_cache_root()?;
    ensure_git_worktree_in(&cache_root, repo_url, commit_sha).await
}

async fn resolve_remote_git_target(
    repo_url: &str,
    git_ref: Option<&str>,
    git_commit_sha: Option<&str>,
) -> AppResult<String> {
    let cache_root = git_cache_root()?;
    let repo_hash = short_hash(repo_url);
    let mirror_dir = cache_root.join("mirrors").join(format!("{repo_hash}.git"));
    tokio::fs::create_dir_all(mirror_dir.parent().expect("mirror parent")).await?;
    let _repo_lock = acquire_repo_lock(&cache_root, &repo_hash).await?;
    ensure_git_mirror(repo_url, &mirror_dir).await?;

    match (git_ref, git_commit_sha) {
        (Some(git_ref), None) => resolve_git_ref(&mirror_dir, git_ref, repo_url).await,
        (None, Some(git_commit_sha)) => {
            ensure_commit_exists_in_mirror(repo_url, &mirror_dir, git_commit_sha).await?;
            Ok(git_commit_sha.to_string())
        }
        _ => Err(AppError::InvalidInput(
            "provide exactly one of git_ref or git_commit_sha".to_string(),
        )),
    }
}

async fn list_remote_branches(repo_url: &str) -> AppResult<Vec<String>> {
    let cache_root = git_cache_root()?;
    let repo_hash = short_hash(repo_url);
    let mirror_dir = cache_root.join("mirrors").join(format!("{repo_hash}.git"));
    tokio::fs::create_dir_all(mirror_dir.parent().expect("mirror parent")).await?;
    let _repo_lock = acquire_repo_lock(&cache_root, &repo_hash).await?;
    ensure_git_mirror(repo_url, &mirror_dir).await?;
    let output = run_git_capture_with_git_dir(
        &mirror_dir,
        ["for-each-ref", "--format=%(refname:strip=2)", "refs/heads"],
    )
    .await?;
    let mut branches: Vec<String> = String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.trim().is_empty() && *line != "HEAD")
        .map(|line| line.trim().to_string())
        .collect();
    branches.sort();
    branches.dedup();
    Ok(branches)
}

async fn list_recent_branch_commits(
    repo_url: &str,
    branch: &str,
    limit: usize,
) -> AppResult<Vec<serde_json::Value>> {
    let cache_root = git_cache_root()?;
    let repo_hash = short_hash(repo_url);
    let mirror_dir = cache_root.join("mirrors").join(format!("{repo_hash}.git"));
    tokio::fs::create_dir_all(mirror_dir.parent().expect("mirror parent")).await?;
    let _repo_lock = acquire_repo_lock(&cache_root, &repo_hash).await?;
    ensure_git_mirror(repo_url, &mirror_dir).await?;
    let reference = format!("refs/heads/{branch}");
    let format = "%H%x1f%h%x1f%s%x1f%cI";
    let output = run_git_capture_with_git_dir(
        &mirror_dir,
        ["log", "--max-count", &limit.to_string(), "--format", format, &reference],
    )
    .await?;
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut parts = line.split('\u{1f}');
            json!({
                "sha": parts.next().unwrap_or_default(),
                "short_sha": parts.next().unwrap_or_default(),
                "summary": parts.next().unwrap_or_default(),
                "committed_at": parts.next().unwrap_or_default(),
            })
        })
        .collect())
}

async fn resolve_remote_default_branch(repo_url: &str) -> AppResult<String> {
    let output = TokioCommand::new("git")
        .args(["ls-remote", "--symref", repo_url, "HEAD"])
        .output()
        .await?;
    if !output.status.success() {
        return Err(AppError::Internal(format!(
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr).trim())));
    }
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some(rest) = line.strip_prefix("ref: refs/heads/")
            && let Some((branch, _)) = rest.split_once('\t')
        {
            return Ok(branch.to_string());
        }
    }
    Err(AppError::Internal(format!(
        "git failed: could not determine default branch for {repo_url}"
    )))
}

async fn run_validation_command(
    client: &DaemonClient,
    claim: &InvocationClaimResponse,
    project_dir: &Path,
    profiles_dir: &Path,
    command: &str,
) -> AppResult<()> {
    send_worker_event(
        client,
        claim,
        crate::execution::ExecutionEventKind::StdoutLine,
        format!("Running dbt {command}"),
    )
    .await?;
    let output = TokioCommand::new(std::env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string()))
        .arg(command)
        .arg("--profiles-dir")
        .arg(profiles_dir)
        .current_dir(project_dir)
        .output()
        .await?;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        send_worker_event(
            client,
            claim,
            crate::execution::ExecutionEventKind::StdoutLine,
            line.to_string(),
        )
        .await?;
    }
    for line in String::from_utf8_lossy(&output.stderr).lines() {
        send_worker_event(
            client,
            claim,
            crate::execution::ExecutionEventKind::StderrLine,
            line.to_string(),
        )
        .await?;
    }
    if !output.status.success() {
        let err = AppError::Internal(format!(
            "dbt {command} failed with exit code {}",
            output.status.code().unwrap_or(1)));
        report_setup_failure(client, claim, &err.to_string()).await?;
        return Err(err);
    }
    Ok(())
}

async fn resolve_git_ref(git_dir: &Path, git_ref: &str, repo_url: &str) -> AppResult<String> {
    for candidate in [
        git_ref.to_string(),
        format!("refs/heads/{git_ref}"),
        format!("refs/tags/{git_ref}"),
    ] {
        if let Ok(resolved) = rev_parse_commit(git_dir, &candidate).await {
            return Ok(resolved);
        }
    }
    Err(AppError::Internal(format!(
        "git failed: ref {git_ref} is not available from remote repository {repo_url}"
    )))
}

async fn rev_parse_commit(git_dir: &Path, reference: &str) -> AppResult<String> {
    let mut command = TokioCommand::new("git");
    command.env("GIT_DIR", git_dir);
    command.args(["rev-parse", "--verify", &format!("{reference}^{{commit}}")]);
    let output = command.output().await?;
    if !output.status.success() {
        return Err(AppError::Internal(
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

async fn ensure_git_worktree_in(
    cache_root: &Path,
    repo_url: &str,
    commit_sha: &str,
) -> AppResult<PathBuf> {
    let repo_hash = short_hash(repo_url);
    let mirror_dir = cache_root.join("mirrors").join(format!("{repo_hash}.git"));
    let worktree_dir = cache_root
        .join("worktrees")
        .join(&repo_hash)
        .join(commit_sha);
    let usage_file = cache_root
        .join("usage")
        .join(&repo_hash)
        .join(format!("{commit_sha}.touch"));

    tokio::fs::create_dir_all(mirror_dir.parent().expect("mirror parent")).await?;
    tokio::fs::create_dir_all(worktree_dir.parent().expect("worktree parent")).await?;
    tokio::fs::create_dir_all(usage_file.parent().expect("usage parent")).await?;

    let _repo_lock = acquire_repo_lock(cache_root, &repo_hash).await?;

    ensure_git_mirror(repo_url, &mirror_dir).await?;

    ensure_commit_exists_in_mirror(repo_url, &mirror_dir, commit_sha).await?;

    if !worktree_dir.exists() {
        info!(
            repo_url = %repo_url,
            commit_sha = %commit_sha,
            worktree = %worktree_dir.display(),
            "creating git worktree"
        );
        run_git_with_git_dir(
            &mirror_dir,
            [
                "worktree",
                "add",
                "--detach",
                worktree_dir.to_string_lossy().as_ref(),
                commit_sha,
            ],
        )
        .await?;
    } else {
        info!(
            repo_url = %repo_url,
            commit_sha = %commit_sha,
            worktree = %worktree_dir.display(),
            "reusing git worktree"
        );
    }

    tokio::fs::write(&usage_file, b"").await?;
    prune_git_cache(cache_root, &repo_hash, commit_sha, &mirror_dir).await?;

    Ok(worktree_dir)
}

async fn ensure_git_mirror(repo_url: &str, mirror_dir: &Path) -> AppResult<()> {
    if !mirror_dir.exists() {
        info!(repo_url = %repo_url, mirror = %mirror_dir.display(), "creating git mirror cache");
        run_git(
            None,
            [
                "clone",
                "--mirror",
                repo_url,
                mirror_dir.to_string_lossy().as_ref(),
            ],
        )
        .await?;
    } else {
        info!(repo_url = %repo_url, mirror = %mirror_dir.display(), "refreshing git mirror cache");
        run_git_with_git_dir(mirror_dir, ["remote", "set-url", "origin", repo_url]).await?;
        run_git_with_git_dir(mirror_dir, ["fetch", "--prune", "origin"]).await?;
    }
    Ok(())
}

fn git_cache_root() -> AppResult<PathBuf> {
    if let Ok(value) = std::env::var("DBTX_GIT_CACHE_DIR")
        && !value.trim().is_empty()
    {
        return Ok(PathBuf::from(value));
    }
    if let Ok(home) = std::env::var("HOME")
        && !home.trim().is_empty()
    {
        return Ok(PathBuf::from(home).join(".cache").join("dbtx").join("git"));
    }
    Ok(std::env::temp_dir().join("dbtx").join("git"))
}

async fn run_git_with_git_dir<const N: usize>(
    git_dir: &std::path::Path,
    args: [&str; N],
) -> AppResult<()> {
    let output = run_git_capture_with_git_dir(git_dir, args).await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(AppError::Internal(format!(
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr).trim())))
    }
}

async fn run_git_capture_with_git_dir<const N: usize>(
    git_dir: &std::path::Path,
    args: [&str; N],
) -> AppResult<std::process::Output> {
    let mut command = TokioCommand::new("git");
    command.env("GIT_DIR", git_dir);
    command.args(args);
    command.output().await.map_err(AppError::from)
}

async fn ensure_commit_exists_in_mirror(
    repo_url: &str,
    git_dir: &std::path::Path,
    commit_sha: &str,
) -> AppResult<()> {
    let mut command = TokioCommand::new("git");
    command.env("GIT_DIR", git_dir);
    command.args(["cat-file", "-e", &format!("{commit_sha}^{{commit}}")]);
    let output = command.output().await?;
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
    if stderr.contains("Not a valid object name") {
        return Err(AppError::Internal(format!(
            "git failed: commit {commit_sha} is not available from remote repository {repo_url}; has it been pushed?"
        )));
    }
    Err(AppError::Internal(format!(
        "git failed: {stderr}"
    )))
}

async fn run_git<const N: usize>(cwd: Option<&std::path::Path>, args: [&str; N]) -> AppResult<()> {
    let mut command = TokioCommand::new("git");
    command.args(args);
    if let Some(cwd) = cwd {
        command.current_dir(cwd);
    }
    let output = command.output().await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(AppError::Internal(format!(
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr).trim())))
    }
}

fn short_hash(input: &str) -> String {
    let digest = Sha256::digest(input.as_bytes());
    format!("{digest:x}").chars().take(20).collect()
}

fn validate_remote_project_root(project_root: &str) -> AppResult<()> {
    let path = Path::new(project_root);
    if path.is_absolute()
        || path
            .components()
            .any(|component| matches!(component, Component::ParentDir))
    {
        Err(AppError::InvalidRemoteProjectRoot(project_root.to_string()))
    } else {
        Ok(())
    }
}

struct RepoLock {
    path: PathBuf,
}

impl Drop for RepoLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

async fn acquire_repo_lock(cache_root: &Path, repo_hash: &str) -> AppResult<RepoLock> {
    let lock_dir = cache_root.join("locks");
    tokio::fs::create_dir_all(&lock_dir).await?;
    let lock_path = lock_dir.join(format!("{repo_hash}.lock"));
    let started = std::time::Instant::now();
    loop {
        match tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&lock_path)
            .await
        {
            Ok(_) => return Ok(RepoLock { path: lock_path }),
            Err(err) if err.kind() == std::io::ErrorKind::AlreadyExists => {
                if let Ok(metadata) = tokio::fs::metadata(&lock_path).await
                    && let Ok(modified) = metadata.modified()
                    && modified.elapsed().unwrap_or_default() > std::time::Duration::from_secs(300)
                {
                    let _ = tokio::fs::remove_file(&lock_path).await;
                    continue;
                }
                if started.elapsed() > std::time::Duration::from_secs(30) {
                    return Err(AppError::Internal(format!(
                        "timed out acquiring git cache lock for {repo_hash}"
                    )));
                }
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
            Err(err) => return Err(AppError::Io(err)),
        }
    }
}

async fn prune_git_cache(
    cache_root: &Path,
    repo_hash: &str,
    current_commit_sha: &str,
    mirror_dir: &Path,
) -> AppResult<()> {
    let ttl_hours = std::env::var("DBTX_GIT_CACHE_TTL_HOURS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(24 * 7);
    let ttl = std::time::Duration::from_secs(ttl_hours.saturating_mul(3600));
    let usage_root = cache_root.join("usage").join(repo_hash);
    let worktree_root = cache_root.join("worktrees").join(repo_hash);
    let mut removed_any = false;

    let Ok(mut entries) = tokio::fs::read_dir(&usage_root).await else {
        return Ok(());
    };
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let Some(stem) = path.file_stem().and_then(|value| value.to_str()) else {
            continue;
        };
        if stem == current_commit_sha {
            continue;
        }
        let metadata = entry.metadata().await?;
        let modified = metadata.modified().unwrap_or(std::time::SystemTime::now());
        if modified.elapsed().unwrap_or_default() <= ttl {
            continue;
        }
        let worktree_dir = worktree_root.join(stem);
        if worktree_dir.exists() {
            let _ = tokio::fs::remove_dir_all(&worktree_dir).await;
            removed_any = true;
        }
        let _ = tokio::fs::remove_file(&path).await;
    }

    if removed_any {
        let _ = run_git_with_git_dir(mirror_dir, ["worktree", "prune"]).await;
    }

    Ok(())
}

fn persists_state(command: InvocationCommandApi) -> bool {
    !matches!(
        command,
        InvocationCommandApi::Ls
            | InvocationCommandApi::Release
            | InvocationCommandApi::ProjectValidate
            | InvocationCommandApi::EnvironmentPrepare
            | InvocationCommandApi::EnvironmentValidate
    )
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
    let Some(state_manifest) = state_manifest else {
        return Ok(None);
    };
    let temp_dir = TempDir::new()?;
    std::fs::write(
        temp_dir.path().join("manifest.json"),
        serde_json::to_vec(state_manifest)?,
    )?;
    Ok(Some(temp_dir))
}

#[cfg(test)]
mod tests {
    use super::{
        DBTX_SELECTED_RESOURCES_HOOK, DBTX_SELECTED_RESOURCES_MACRO_FILE,
        ensure_git_worktree_in, patch_runtime_project_yaml, resolve_remote_git_target,
        validate_remote_project_root,
    };
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
            ensure_git_worktree_in(
                cache.path(),
                repo.path().to_str().expect("repo str"),
                &commit_sha
            ),
            ensure_git_worktree_in(
                cache.path(),
                repo.path().to_str().expect("repo str"),
                &commit_sha
            )
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

        let error = resolve_remote_git_target(
            repo.path().to_str().expect("repo str"),
            Some("missing-branch"),
            None,
        )
        .await
        .expect_err("expected missing ref error");
        assert!(
            error
                .to_string()
                .contains("ref missing-branch is not available from remote repository")
        );
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

        let error = resolve_remote_git_target(
            repo.path().to_str().expect("repo str"),
            None,
            Some("deadbeef"),
        )
        .await
        .expect_err("expected missing commit error");
        assert!(
            error
                .to_string()
                .contains("commit deadbeef is not available from remote repository")
        );
    }

    #[test]
    fn patch_runtime_project_yaml_appends_selected_resources_hook() {
        let project = TempDir::new().expect("project dir");
        std::fs::create_dir_all(project.path().join("custom_macros")).expect("macro dir");
        std::fs::write(
            project.path().join("dbt_project.yml"),
            "name: demo\nmacro-paths: [custom_macros]\non-run-start: [\"{{ existing_hook() }}\"]\n",
        )
        .expect("dbt project");

        patch_runtime_project_yaml(project.path()).expect("patch project");
        patch_runtime_project_yaml(project.path()).expect("patch project idempotently");

        let patched =
            std::fs::read_to_string(project.path().join("dbt_project.yml")).expect("patched yaml");
        assert!(patched.contains("existing_hook"));
        assert!(patched.contains(DBTX_SELECTED_RESOURCES_HOOK));
        assert_eq!(patched.matches(DBTX_SELECTED_RESOURCES_HOOK).count(), 1);
        assert!(
            project
                .path()
                .join("custom_macros")
                .join(DBTX_SELECTED_RESOURCES_MACRO_FILE)
                .is_file()
        );
    }

    fn run_git<const N: usize>(args: [&str; N], cwd: &Path) {
        let output = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .expect("git command");
        assert!(
            output.status.success(),
            "git failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
    }

    fn git_output<const N: usize>(args: [&str; N], cwd: &Path) -> String {
        let output = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .output()
            .expect("git output");
        assert!(output.status.success(), "git failed");
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }
}
