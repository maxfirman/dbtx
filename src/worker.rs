use crate::api::{
    InvocationClaimResponse, InvocationCommandApi, InvocationCompleteApiRequest,
    InvocationEventBatchApiRequest, InvocationExecutionSpecApi, InvocationHeartbeatApiRequest,
    InvocationLifecycleStatus,
};
use crate::client::DaemonClient;
use crate::error::{AppError, AppResult};
use crate::event::LogEvent;
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

    let spec = claim.execution_spec;
    let command_name = spec.command();
    let project_dir = materialize_execution_project_dir(&spec).await?;
    info!(
        invocation_id = %claim.invocation_id,
        worker_id = %claim.worker_id,
        command = ?command_name,
        project_dir = %project_dir.display(),
        "starting claimed invocation execution"
    );
    let profiles_dir = write_profiles_dir(spec.profiles_yml())?;
    let state_dir = write_state_dir(spec.state_manifest())?;

    let mut dbt_args: Vec<OsString> = spec.args().iter().cloned().map(Into::into).collect();
    if let Some(state_dir) = state_dir.as_ref() {
        dbt_args.push("--state".into());
        dbt_args.push(state_dir.path().as_os_str().to_os_string());
    }
    dbt_args.push("--profiles-dir".into());
    dbt_args.push(profiles_dir.path().as_os_str().to_os_string());

    let command = map_command(command_name);
    let mut child =
        TokioCommand::new(std::env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string()))
            .arg(command)
            .args(&dbt_args)
            .current_dir(&project_dir)
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()?;

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
                    if let Some(rendered) = event.render_text_line() {
                        println!("{rendered}");
                    }
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
                    println!("{line}");
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
        eprintln!("{line}");
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

impl InvocationExecutionSpecApi {
    fn command(&self) -> InvocationCommandApi {
        match self {
            Self::Local { command, .. } | Self::Remote { command, .. } => *command,
        }
    }

    fn args(&self) -> &[String] {
        match self {
            Self::Local { args, .. } | Self::Remote { args, .. } => args,
        }
    }

    fn profiles_yml(&self) -> &str {
        match self {
            Self::Local { profiles_yml, .. } | Self::Remote { profiles_yml, .. } => profiles_yml,
        }
    }

    fn state_manifest(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Local { state_manifest, .. } | Self::Remote { state_manifest, .. } => {
                state_manifest.as_ref()
            }
        }
    }
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
    }
}

async fn ensure_git_worktree(repo_url: &str, commit_sha: &str) -> AppResult<PathBuf> {
    let cache_root = git_cache_root()?;
    ensure_git_worktree_in(&cache_root, repo_url, commit_sha).await
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
        run_git_with_git_dir(&mirror_dir, ["remote", "set-url", "origin", repo_url]).await?;
        run_git_with_git_dir(&mirror_dir, ["fetch", "--prune", "origin"]).await?;
    }

    run_git_with_git_dir(
        &mirror_dir,
        ["cat-file", "-e", &format!("{commit_sha}^{{commit}}")],
    )
    .await?;

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
                    && modified
                        .elapsed()
                        .unwrap_or_default()
                        > std::time::Duration::from_secs(300)
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
    !matches!(command, InvocationCommandApi::Ls)
}

fn map_command(command: InvocationCommandApi) -> &'static str {
    match command {
        InvocationCommandApi::Build => "build",
        InvocationCommandApi::Run => "run",
        InvocationCommandApi::Ls => "ls",
        InvocationCommandApi::Test => "test",
        InvocationCommandApi::Seed => "seed",
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
    use super::{ensure_git_worktree_in, validate_remote_project_root};
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
