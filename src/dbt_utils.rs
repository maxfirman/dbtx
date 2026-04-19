//! Utility functions for dbt project interaction, git state, and process spawning.
//!
//! These were previously in db.rs but have no database dependency.

use crate::db::{EnvironmentRecord, GitState};
use crate::error::{AppError, AppResult};
use crate::manifest::ReconstructedManifest;
use crate::profile::{EnvironmentProfileRecord, GeneratedProfiles, resolve_runtime_profile};
use std::ffi::OsString;
use std::path::Path;
use std::process::Stdio;
use tokio::process::{Child, Command};
use uuid::Uuid;

pub(crate) fn append_invocation_id(mut args: Vec<OsString>, run_id: Uuid) -> Vec<OsString> {
    args.push("--invocation-id".into());
    args.push(run_id.to_string().into());
    args
}

pub(crate) fn append_state_dir(
    mut args: Vec<OsString>,
    reconstructed_manifest: Option<&ReconstructedManifest>,
) -> Vec<OsString> {
    if let Some(reconstructed_manifest) = reconstructed_manifest {
        args.push("--state".into());
        args.push(
            reconstructed_manifest
                .temp_dir
                .path()
                .as_os_str()
                .to_os_string(),
        );
    }
    args
}

pub(crate) fn append_profiles_dir(
    mut args: Vec<OsString>,
    generated_profiles: &GeneratedProfiles,
) -> Vec<OsString> {
    args.push("--profiles-dir".into());
    args.push(
        generated_profiles
            .temp_dir
            .path()
            .as_os_str()
            .to_os_string(),
    );
    args
}

pub(crate) fn spawn_dbt_child(
    dbt_path: &str,
    subcommand: &str,
    args: &[OsString],
    project_dir: &Path,
) -> AppResult<Child> {
    let child = Command::new(dbt_path)
        .arg(subcommand)
        .args(args)
        .current_dir(project_dir)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    Ok(child)
}

pub(crate) fn read_dbt_project_name(project_dir: &Path) -> String {
    read_dbt_project_yaml(project_dir)
        .ok()
        .and_then(|yaml| {
            yaml.get("name")
                .and_then(serde_yaml::Value::as_str)
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| {
            project_dir
                .file_name()
                .unwrap_or_default()
                .to_string_lossy()
                .into_owned()
        })
}

fn read_dbt_project_yaml(project_dir: &Path) -> AppResult<serde_yaml::Value> {
    let path = project_dir.join("dbt_project.yml");
    if !path.is_file() {
        return Err(AppError::NotDbtProjectRoot);
    }
    let content = std::fs::read_to_string(path)?;
    Ok(serde_yaml::from_str(&content)?)
}

pub(crate) fn git_repo_root(current_dir: &Path) -> AppResult<std::path::PathBuf> {
    let output = run_git(["rev-parse", "--show-toplevel"], current_dir)?;
    Ok(output.into())
}

fn git_remote_origin_url(repo_root: &Path) -> AppResult<String> {
    run_git(["config", "--get", "remote.origin.url"], repo_root)
        .map_err(|_| AppError::GitRemoteNotFound)
}

pub(crate) fn read_git_state(project_dir: &Path) -> GitState {
    let repo_root = git_repo_root(project_dir).ok();
    let repo_url = repo_root
        .as_deref()
        .and_then(|root| git_remote_origin_url(root).ok());
    let branch = repo_root.as_deref().and_then(|root| {
        run_git(["rev-parse", "--abbrev-ref", "HEAD"], root)
            .ok()
            .filter(|value| value != "HEAD")
    });
    let commit_sha = repo_root
        .as_deref()
        .and_then(|root| run_git(["rev-parse", "HEAD"], root).ok());
    GitState {
        branch,
        commit_sha,
        repo_url,
    }
}

pub(crate) fn build_generated_profiles(
    _project_dir: &Path,
    environment: &EnvironmentRecord,
) -> AppResult<GeneratedProfiles> {
    let resolved = resolve_runtime_profile(
        &environment.profile_name,
        &environment.target_name,
        &EnvironmentProfileRecord {
            adapter_type: environment.adapter_type.clone(),
            schema_name: environment.schema_name.clone(),
            threads: environment.threads,
            profile_config: environment.profile_config.clone(),
            profile_secrets: environment.profile_secrets.clone(),
        },
    )?;
    resolved.generate()
}

fn run_git<const N: usize>(args: [&str; N], cwd: &Path) -> AppResult<String> {
    let output = std::process::Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()?;
    if !output.status.success() {
        return Err(AppError::GitRepoNotFound);
    }
    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() {
        return Err(AppError::GitRepoNotFound);
    }
    Ok(stdout)
}
