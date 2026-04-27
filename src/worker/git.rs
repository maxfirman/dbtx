//! Git mirror, worktree, and cache management for the worker execution plane.

use crate::error::{AppError, AppResult};
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use tokio::process::Command as TokioCommand;
use tracing::info;

pub(crate) async fn ensure_git_worktree(repo_url: &str, commit_sha: &str) -> AppResult<PathBuf> {
    let cache_root = git_cache_root()?;
    ensure_git_worktree_in(&cache_root, repo_url, commit_sha).await
}

pub(crate) async fn resolve_remote_git_target(
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

pub(crate) async fn list_remote_branches(repo_url: &str) -> AppResult<Vec<String>> {
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

pub(crate) async fn list_recent_branch_commits(
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
        [
            "log",
            "--max-count",
            &limit.to_string(),
            "--format",
            format,
            &reference,
        ],
    )
    .await?;
    Ok(String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| {
            let mut parts = line.split('\u{1f}');
            serde_json::json!({
                "sha": parts.next().unwrap_or_default(),
                "short_sha": parts.next().unwrap_or_default(),
                "summary": parts.next().unwrap_or_default(),
                "committed_at": parts.next().unwrap_or_default(),
            })
        })
        .collect())
}

pub(crate) async fn resolve_remote_default_branch(repo_url: &str) -> AppResult<String> {
    let output = TokioCommand::new("git")
        .args(["ls-remote", "--symref", repo_url, "HEAD"])
        .output()
        .await?;
    if !output.status.success() {
        return Err(AppError::Internal(format!(
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )));
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

pub(super) async fn ensure_git_worktree_in(
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

pub(super) fn git_cache_root() -> AppResult<PathBuf> {
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

async fn run_git_with_git_dir<const N: usize>(git_dir: &Path, args: [&str; N]) -> AppResult<()> {
    let output = run_git_capture_with_git_dir(git_dir, args).await?;
    if output.status.success() {
        Ok(())
    } else {
        Err(AppError::Internal(format!(
            "git failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

async fn run_git_capture_with_git_dir<const N: usize>(
    git_dir: &Path,
    args: [&str; N],
) -> AppResult<std::process::Output> {
    let mut command = TokioCommand::new("git");
    command.env("GIT_DIR", git_dir);
    command.args(args);
    command.output().await.map_err(AppError::from)
}

async fn ensure_commit_exists_in_mirror(
    repo_url: &str,
    git_dir: &Path,
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
    Err(AppError::Internal(format!("git failed: {stderr}")))
}

async fn run_git<const N: usize>(cwd: Option<&Path>, args: [&str; N]) -> AppResult<()> {
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
            String::from_utf8_lossy(&output.stderr).trim()
        )))
    }
}

pub(super) fn short_hash(input: &str) -> String {
    let digest = Sha256::digest(input.as_bytes());
    format!("{digest:x}").chars().take(20).collect()
}

pub(super) struct RepoLock {
    path: PathBuf,
}

impl Drop for RepoLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub(super) async fn acquire_repo_lock(cache_root: &Path, repo_hash: &str) -> AppResult<RepoLock> {
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
