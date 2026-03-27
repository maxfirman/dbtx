use crate::api::{
    InvocationClaimResponse, InvocationCommandApi, InvocationCompleteApiRequest,
    InvocationEventBatchApiRequest, InvocationExecutionModeApi, InvocationExecutionSpecApi,
    InvocationHeartbeatApiRequest, InvocationLifecycleStatus,
};
use crate::client::DaemonClient;
use crate::services::read_dbt_project_name_from_root;
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
use serde_json::json;
use sha2::{Digest, Sha256};
use std::ffi::OsString;
use std::path::{Component, Path, PathBuf};
use tempfile::TempDir;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command as TokioCommand;
use tracing::{info, warn};

pub async fn execute_claimed_invocation(
    client: &DaemonClient,
    claim: InvocationClaimResponse,
    expected_invocation_id: Option<uuid::Uuid>,
) -> AppResult<()> {
    if let Some(expected) = expected_invocation_id
        && claim.invocation_id != expected
    {
        return Err(AppError::Io(std::io::Error::other(format!(
            "claimed unexpected invocation {}, expected {}",
            claim.invocation_id, expected
        ))));
    }

    let spec = claim.execution_spec.clone();
    if matches!(spec, InvocationExecutionSpecApi::ReleaseValidation { .. }) {
        return execute_release_validation(client, claim, &spec).await;
    }
    if matches!(spec, InvocationExecutionSpecApi::ProjectValidation { .. }) {
        return execute_project_validation(client, claim, &spec).await;
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
    let mut child = match TokioCommand::new(
        std::env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string()),
    )
    .arg(command)
    .args(&dbt_args)
    .current_dir(&project_dir)
    .stdout(std::process::Stdio::piped())
    .stderr(std::process::Stdio::piped())
    .spawn()
    {
        Ok(child) => child,
        Err(err) => {
            let error = AppError::Io(err);
            report_setup_failure(client, &claim, &error.to_string()).await?;
            return Err(error);
        }
    };

    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stdout")))?;
    let stderr = child
        .stderr
        .take()
        .ok_or_else(|| AppError::Io(std::io::Error::other("missing child stderr")))?;

    let mut stdout_reader = BufReader::new(stdout).lines();
    let stderr_handle = tokio::spawn(async move {
        let mut reader = BufReader::new(stderr).lines();
        let mut lines = Vec::new();
        while let Some(line) = reader.next_line().await? {
            lines.push(line);
        }
        Result::<Vec<String>, std::io::Error>::Ok(lines)
    });

    let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(2));
    let mut dbt_version: Option<String> = None;
    let mut cancel_requested = false;

    loop {
        tokio::select! {
            line = stdout_reader.next_line() => {
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
                    client
                        .invocation_append_events(
                            claim.invocation_id,
                            InvocationEventBatchApiRequest {
                                worker_id: claim.worker_id.clone(),
                                lease_token: claim.lease_token,
                                events: vec![crate::execution::ExecutionEvent {
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
                                }],
                            },
                        )
                        .await?;
                } else {
                    emit_stream_output(
                        pretty_terminal_output,
                        claim.invocation_id,
                        &claim.worker_id,
                        "stdout",
                        &line,
                    );
                    client
                        .invocation_append_events(
                            claim.invocation_id,
                            InvocationEventBatchApiRequest {
                                worker_id: claim.worker_id.clone(),
                                lease_token: claim.lease_token,
                                events: vec![crate::execution::ExecutionEvent {
                                    kind: crate::execution::ExecutionEventKind::StdoutLine,
                                    occurred_at: chrono::Utc::now(),
                                    text: Some(line.clone()),
                                    raw_line: Some(line),
                                    dbt_event_name: None,
                                    node_unique_id: None,
                                    level: None,
                                    error: None,
                                }],
                            },
                        )
                        .await?;
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
                    let _ = child.start_kill();
                }
            }
        }
    }

    let status = child.wait().await?;
    for line in stderr_handle.await.map_err(|err| {
        AppError::Io(std::io::Error::other(format!("stderr task failed: {err}")))
    })?? {
        emit_stream_output(
            pretty_terminal_output,
            claim.invocation_id,
            &claim.worker_id,
            "stderr",
            &line,
        );
        client
            .invocation_append_events(
                claim.invocation_id,
                InvocationEventBatchApiRequest {
                    worker_id: claim.worker_id.clone(),
                    lease_token: claim.lease_token,
                    events: vec![crate::execution::ExecutionEvent {
                        kind: crate::execution::ExecutionEventKind::StderrLine,
                        occurred_at: chrono::Utc::now(),
                        text: Some(line.clone()),
                        raw_line: Some(line),
                        dbt_event_name: None,
                        node_unique_id: None,
                        level: None,
                        error: None,
                    }],
                },
            )
            .await?;
    }

    let exit_code = if cancel_requested {
        130
    } else {
        status.code().unwrap_or(1)
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
                    } else if !status.success() {
                        InvocationLifecycleStatus::Failed
                    } else {
                        InvocationLifecycleStatus::Succeeded
                    },
                    exit_code,
                    error: if cancel_requested {
                        Some("invocation canceled".to_string())
                    } else if status.success() {
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
        Err(AppError::Io(std::io::Error::other("invocation canceled")))
    } else if status.success() {
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

impl InvocationExecutionSpecApi {
    fn command(&self) -> InvocationCommandApi {
        match self {
            Self::Local { command, .. } | Self::Remote { command, .. } => *command,
            Self::ReleaseValidation { .. } => InvocationCommandApi::Release,
            Self::ProjectValidation { .. } => InvocationCommandApi::ProjectValidate,
        }
    }

    fn args(&self) -> &[String] {
        match self {
            Self::Local { args, .. } | Self::Remote { args, .. } => args,
            Self::ReleaseValidation { .. } => &[],
            Self::ProjectValidation { .. } => &[],
        }
    }

    fn profiles_yml(&self) -> &str {
        match self {
            Self::Local { profiles_yml, .. } | Self::Remote { profiles_yml, .. } => profiles_yml,
            Self::ReleaseValidation { .. } => "",
            Self::ProjectValidation { .. } => "",
        }
    }

    fn state_manifest(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Local { state_manifest, .. } | Self::Remote { state_manifest, .. } => {
                state_manifest.as_ref()
            }
            Self::ReleaseValidation { .. } => None,
            Self::ProjectValidation { .. } => None,
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
        unreachable!("release validation requires release spec");
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
        unreachable!("project validation requires project validation spec");
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

    let default_commit = match resolve_remote_git_target(repo_url, Some(&default_branch), None).await {
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
    client
        .invocation_append_events(
            claim.invocation_id,
            InvocationEventBatchApiRequest {
                worker_id: claim.worker_id.clone(),
                lease_token: claim.lease_token,
                events: vec![crate::execution::ExecutionEvent {
                    kind,
                    occurred_at: chrono::Utc::now(),
                    text: Some(text),
                    raw_line: None,
                    dbt_event_name: None,
                    node_unique_id: None,
                    level: None,
                    error: None,
                }],
            },
        )
        .await
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
        InvocationExecutionSpecApi::ProjectValidation { .. } => {
            Err(AppError::UnsupportedLocalExecution("project_validate".to_string()))
        }
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
        _ => Err(AppError::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "provide exactly one of git_ref or git_commit_sha",
        ))),
    }
}

async fn resolve_remote_default_branch(repo_url: &str) -> AppResult<String> {
    let output = TokioCommand::new("git")
        .args(["ls-remote", "--symref", repo_url, "HEAD"])
        .output()
        .await?;
    if !output.status.success() {
        return Err(AppError::Io(std::io::Error::other(format!(
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))));
    }
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        if let Some(rest) = line.strip_prefix("ref: refs/heads/")
            && let Some((branch, _)) = rest.split_once('\t')
        {
            return Ok(branch.to_string());
        }
    }
    Err(AppError::Io(std::io::Error::other(format!(
        "git failed: could not determine default branch for {repo_url}"
    ))))
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
    Err(AppError::Io(std::io::Error::other(format!(
        "git failed: ref {git_ref} is not available from remote repository {repo_url}"
    ))))
}

async fn rev_parse_commit(git_dir: &Path, reference: &str) -> AppResult<String> {
    let mut command = TokioCommand::new("git");
    command.env("GIT_DIR", git_dir);
    command.args(["rev-parse", "--verify", &format!("{reference}^{{commit}}")]);
    let output = command.output().await?;
    if !output.status.success() {
        return Err(AppError::Io(std::io::Error::other(
            String::from_utf8_lossy(&output.stderr).trim().to_string(),
        )));
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
    let mut command = TokioCommand::new("git");
    command.env("GIT_DIR", git_dir);
    command.args(args);
    let output = command.output().await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(AppError::Io(std::io::Error::other(format!(
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))))
    }
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
        return Err(AppError::Io(std::io::Error::other(format!(
            "git failed: commit {commit_sha} is not available from remote repository {repo_url}; has it been pushed?"
        ))));
    }
    Err(AppError::Io(std::io::Error::other(format!(
        "git failed: {stderr}"
    ))))
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
        Err(AppError::Io(std::io::Error::other(format!(
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        ))))
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
                    return Err(AppError::Io(std::io::Error::other(format!(
                        "timed out acquiring git cache lock for {repo_hash}"
                    ))));
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
        InvocationCommandApi::Ls | InvocationCommandApi::Release | InvocationCommandApi::ProjectValidate
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
    use super::{ensure_git_worktree_in, resolve_remote_git_target, validate_remote_project_root};
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
