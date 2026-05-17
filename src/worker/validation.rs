//! Validation execution: release, project, environment prepare, and environment validate.

use super::git::{
    ensure_git_worktree, list_recent_branch_commits, list_remote_branches,
    resolve_remote_default_branch, resolve_remote_git_target,
};
use super::{
    emit_stream_output, report_setup_failure, send_event, session::WorkerInvocationSession,
    write_profiles_dir,
};
use crate::api::{InvocationClaimResponse, InvocationExecutionModeApi, InvocationExecutionSpecApi};
use crate::client::DaemonClient;
use crate::db::validate_remote_project_root;
use crate::error::{AppError, AppResult};
use crate::services::read_dbt_project_name_from_root;
use serde_json::{Value, json};
use std::path::Path;
use tokio::process::Command as TokioCommand;

pub(super) async fn execute_release_validation(
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
        return Err(AppError::Internal(
            "release validation requires release spec".to_string(),
        ));
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
            Ok(resolved) => resolved,
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
    complete_success(
        client,
        &claim,
        Some(json!({
            "resolved_commit_sha": resolved_commit_sha,
            "git_branch": git_branch,
        })),
    )
    .await?;
    Ok(())
}

pub(super) async fn execute_project_validation(
    client: &DaemonClient,
    claim: InvocationClaimResponse,
    spec: &InvocationExecutionSpecApi,
) -> AppResult<()> {
    let InvocationExecutionSpecApi::ProjectValidation {
        repo_url,
        project_root,
    } = spec
    else {
        return Err(AppError::Internal(
            "project validation requires project validation spec".to_string(),
        ));
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

    complete_success(
        client,
        &claim,
        Some(json!({
            "project_name": project_name,
            "default_branch": default_branch,
        })),
    )
    .await?;
    Ok(())
}

pub(super) async fn execute_environment_prepare(
    client: &DaemonClient,
    claim: InvocationClaimResponse,
    spec: &InvocationExecutionSpecApi,
) -> AppResult<()> {
    let InvocationExecutionSpecApi::EnvironmentPrepare {
        repo_url,
        selected_branch,
    } = spec
    else {
        return Err(AppError::Internal(
            "environment prepare requires environment prepare spec".to_string(),
        ));
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
    let latest_commit_sha =
        match resolve_remote_git_target(repo_url, Some(&active_branch), None).await {
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

    complete_success(
        client,
        &claim,
        Some(json!({
            "default_branch": default_branch,
            "selected_branch": active_branch,
            "latest_commit_sha": latest_commit_sha,
            "branches": branches,
            "commits": commits,
        })),
    )
    .await?;
    Ok(())
}

pub(super) async fn execute_environment_validation(
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
        return Err(AppError::Internal(
            "environment validation requires environment validate spec".to_string(),
        ));
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

    complete_success(
        client,
        &claim,
        Some(json!({
            "resolved_commit_sha": commit_sha,
            "selected_branch": selected_branch,
        })),
    )
    .await?;
    Ok(())
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
    let output =
        TokioCommand::new(std::env::var("DBTX_DBT_PATH").unwrap_or_else(|_| "dbt".to_string()))
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
            output.status.code().unwrap_or(1)
        ));
        report_setup_failure(client, claim, &err.to_string()).await?;
        return Err(err);
    }
    Ok(())
}

async fn complete_success(
    client: &DaemonClient,
    claim: &InvocationClaimResponse,
    result: Option<Value>,
) -> AppResult<()> {
    WorkerInvocationSession::new(client, claim)
        .complete(crate::execution::ExecutionCompletion {
            status: crate::api::InvocationLifecycleStatus::Succeeded,
            exit_code: 0,
            error: None,
            dbt_version: None,
            manifest: None,
            result,
        })
        .await
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
    send_event(
        client,
        claim,
        crate::execution::ExecutionEvent {
            kind,
            occurred_at: chrono::Utc::now(),
            text: Some(text),
            raw_line: None,
            dbt_event_name: None,
            node_unique_id: None,
            level: None,
            error: None,
        },
    )
    .await
}
