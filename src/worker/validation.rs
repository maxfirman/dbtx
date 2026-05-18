//! Validation execution: release, project, environment prepare, and environment validate.
//!
//! All validation functions share a common lifecycle:
//! 1. Extract spec fields (fail if wrong variant)
//! 2. Perform fallible async work, reporting failures to the control plane
//! 3. Send progress events
//! 4. Complete with a result JSON
//!
//! The `try_or_fail` helper concentrates the "try this, or mark the invocation failed" pattern.

use super::git::{
    ensure_git_worktree, list_recent_branch_commits, list_remote_branches,
    resolve_remote_default_branch, resolve_remote_git_target,
};
use super::{
    emit_stream_output, report_setup_failure, run_worker_dbt_process, send_event,
    session::WorkerInvocationSession, write_profiles_dir,
};
use crate::api::{InvocationClaimResponse, InvocationExecutionModeApi, InvocationExecutionSpecApi};
use crate::client::DaemonClient;
use crate::db::validate_remote_project_root;
use crate::error::{AppError, AppResult};
use crate::services::read_dbt_project_name_from_root;
use serde_json::{Value, json};
use std::ffi::OsString;
use std::path::Path;

/// Try a fallible operation; on error, report failure to the control plane and propagate.
async fn try_or_fail<T>(
    client: &DaemonClient,
    claim: &InvocationClaimResponse,
    result: AppResult<T>,
) -> AppResult<T> {
    match result {
        Ok(value) => Ok(value),
        Err(err) => {
            report_setup_failure(client, claim, &err.to_string()).await?;
            Err(err)
        }
    }
}

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
    let resolved_commit_sha = try_or_fail(
        client,
        &claim,
        resolve_remote_git_target(repo_url, git_ref.as_deref(), git_commit_sha.as_deref()).await,
    )
    .await?;
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

    let default_branch =
        try_or_fail(client, &claim, resolve_remote_default_branch(repo_url).await).await?;
    send_worker_event(
        client,
        &claim,
        crate::execution::ExecutionEventKind::StdoutLine,
        format!("Resolved default branch {default_branch}"),
    )
    .await?;

    let default_commit = try_or_fail(
        client,
        &claim,
        resolve_remote_git_target(repo_url, Some(&default_branch), None).await,
    )
    .await?;
    let repo_checkout =
        try_or_fail(client, &claim, ensure_git_worktree(repo_url, &default_commit).await).await?;
    let project_dir = repo_checkout.join(project_root);
    let project_name =
        try_or_fail(client, &claim, read_dbt_project_name_from_root(&project_dir)).await?;
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

    let default_branch =
        try_or_fail(client, &claim, resolve_remote_default_branch(repo_url).await).await?;
    let active_branch = resolve_active_branch(selected_branch, &default_branch);
    send_worker_event(
        client,
        &claim,
        crate::execution::ExecutionEventKind::StdoutLine,
        format!("Resolved branch {active_branch}"),
    )
    .await?;

    let branches =
        try_or_fail(client, &claim, list_remote_branches(repo_url).await).await?;
    let latest_commit_sha = try_or_fail(
        client,
        &claim,
        resolve_remote_git_target(repo_url, Some(&active_branch), None).await,
    )
    .await?;
    let commits = try_or_fail(
        client,
        &claim,
        list_recent_branch_commits(repo_url, &active_branch, 50).await,
    )
    .await?;

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

    let repo_checkout =
        try_or_fail(client, &claim, ensure_git_worktree(repo_url, commit_sha).await).await?;
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
    let profiles_dir = try_or_fail(client, &claim, write_profiles_dir(profiles_yml)).await?;

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
    let args = vec![
        OsString::from("--profiles-dir"),
        profiles_dir.as_os_str().to_os_string(),
    ];
    let session = WorkerInvocationSession::new(client, claim);
    let exec_result =
        run_worker_dbt_process(client, claim, command, &args, project_dir, false).await?;

    if exec_result.cancel_requested {
        session.complete_canceled().await?;
        return Err(AppError::InvocationCanceled);
    }
    if exec_result.child_result.exit_code != 0 {
        let err = AppError::Internal(format!(
            "dbt {command} failed with exit code {}",
            exec_result.child_result.exit_code
        ));
        session.complete_failed(&err.to_string()).await?;
        return Err(err);
    }
    Ok(())
}

fn resolve_active_branch(selected_branch: &Option<String>, default_branch: &str) -> String {
    selected_branch
        .clone()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| default_branch.to_string())
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

#[cfg(test)]
mod tests {
    use super::resolve_active_branch;
    use crate::api::InvocationExecutionSpecApi;
    use crate::error::AppError;

    /// Validates that the spec matches the expected variant for release validation.
    fn validate_release_spec(spec: &InvocationExecutionSpecApi) -> Result<(), AppError> {
        match spec {
            InvocationExecutionSpecApi::ReleaseValidation { .. } => Ok(()),
            _ => Err(AppError::Internal(
                "release validation requires release spec".to_string(),
            )),
        }
    }

    /// Validates that the spec matches the expected variant for project validation.
    fn validate_project_spec(spec: &InvocationExecutionSpecApi) -> Result<(), AppError> {
        match spec {
            InvocationExecutionSpecApi::ProjectValidation { .. } => Ok(()),
            _ => Err(AppError::Internal(
                "project validation requires project validation spec".to_string(),
            )),
        }
    }

    #[test]
    fn release_spec_validation_accepts_correct_variant() {
        let spec = InvocationExecutionSpecApi::ReleaseValidation {
            repo_url: "https://github.com/org/repo.git".to_string(),
            git_ref: Some("v1.0".to_string()),
            git_commit_sha: None,
            git_branch: Some("main".to_string()),
        };
        assert!(validate_release_spec(&spec).is_ok());
    }

    #[test]
    fn release_spec_validation_rejects_wrong_variant() {
        let spec = InvocationExecutionSpecApi::ProjectValidation {
            repo_url: "https://github.com/org/repo.git".to_string(),
            project_root: ".".to_string(),
        };
        assert!(validate_release_spec(&spec).is_err());
    }

    #[test]
    fn project_spec_validation_accepts_correct_variant() {
        let spec = InvocationExecutionSpecApi::ProjectValidation {
            repo_url: "https://github.com/org/repo.git".to_string(),
            project_root: ".".to_string(),
        };
        assert!(validate_project_spec(&spec).is_ok());
    }

    #[test]
    fn project_spec_validation_rejects_wrong_variant() {
        let spec = InvocationExecutionSpecApi::ReleaseValidation {
            repo_url: "https://github.com/org/repo.git".to_string(),
            git_ref: None,
            git_commit_sha: Some("abc123".to_string()),
            git_branch: None,
        };
        assert!(validate_project_spec(&spec).is_err());
    }

    #[test]
    fn resolve_active_branch_uses_selected_when_present() {
        assert_eq!(
            resolve_active_branch(&Some("feature/x".to_string()), "main"),
            "feature/x"
        );
    }

    #[test]
    fn resolve_active_branch_falls_back_to_default() {
        assert_eq!(resolve_active_branch(&None, "main"), "main");
    }

    #[test]
    fn resolve_active_branch_ignores_empty_selected() {
        assert_eq!(resolve_active_branch(&Some("".to_string()), "main"), "main");
    }

    #[test]
    fn resolve_active_branch_ignores_whitespace_selected() {
        assert_eq!(
            resolve_active_branch(&Some("   ".to_string()), "main"),
            "main"
        );
    }
}
