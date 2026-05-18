//! Worker execution runtime: dbt process management, git worktree materialization, and event streaming.

mod dbt_execution;
mod dbt_process;
pub(crate) mod git;
mod session;
mod validation;

use crate::api::{InvocationClaimResponse, InvocationCommandApi, InvocationExecutionSpecApi};
use crate::client::DaemonClient;
use crate::db::validate_remote_project_root;
use crate::error::{AppError, AppResult};
use dbt_execution::complete_worker_dbt_invocation;
use dbt_process::{command_persists_state, run_worker_dbt_process};
use git::{ensure_git_worktree, git_cache_root, short_hash};
use serde_yaml::Value as YamlValue;
use session::WorkerInvocationSession;
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
    let _runtime_guard =
        match prepare_runtime_project_for_execution(&spec, command_name, &project_dir).await {
            Ok(guard) => guard,
            Err(err) => {
                report_setup_failure(client, &claim, &err.to_string()).await?;
                return Err(err);
            }
        };
    let is_local = matches!(spec, InvocationExecutionSpecApi::Local { .. });
    let profiles_dir = if !is_local {
        match write_profiles_dir(spec.profiles_yml()) {
            Ok(profiles_dir) => Some(profiles_dir),
            Err(err) => {
                report_setup_failure(client, &claim, &err.to_string()).await?;
                return Err(err);
            }
        }
    } else {
        None
    };
    let state_dir = match write_state_dir(spec.state_manifest()) {
        Ok(state_dir) => state_dir,
        Err(err) => {
            report_setup_failure(client, &claim, &err.to_string()).await?;
            return Err(err);
        }
    };

    let mut dbt_args: Vec<std::ffi::OsString> =
        spec.args().iter().cloned().map(Into::into).collect();
    if let Some(state_dir) = state_dir.as_ref() {
        dbt_args.push("--state".into());
        dbt_args.push(state_dir.path().as_os_str().to_os_string());
    }
    if let Some(profiles_dir) = profiles_dir.as_ref() {
        dbt_args.push("--profiles-dir".into());
        dbt_args.push(profiles_dir.path().as_os_str().to_os_string());
    }

    let command = map_command(command_name);
    let exec_result = run_worker_dbt_process(
        client,
        &claim,
        command,
        &dbt_args,
        &project_dir,
        command_persists_state(command_name),
    )
    .await?;

    let manifest = if command_persists_state(command_name) {
        let manifest_path = project_dir.join("target").join("manifest.json");
        std::fs::read_to_string(&manifest_path)
            .ok()
            .and_then(|c| serde_json::from_str(&c).ok())
    } else {
        None
    };
    let terminal = complete_worker_dbt_invocation(
        exec_result.child_result,
        exec_result.cancel_requested,
        exec_result.dbt_version,
        manifest,
    );
    let app_result = terminal.as_result();
    let exit_code = terminal.exit_code;

    let session = WorkerInvocationSession::new(client, &claim);
    session.complete(terminal.completion).await?;

    info!(invocation_id = %claim.invocation_id, worker_id = %claim.worker_id, exit_code, canceled = exec_result.cancel_requested, "finished claimed invocation execution");
    app_result
}

async fn report_setup_failure(
    client: &DaemonClient,
    claim: &InvocationClaimResponse,
    error_message: &str,
) -> AppResult<()> {
    WorkerInvocationSession::new(client, claim)
        .complete_failed(error_message)
        .await
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
        invocation_id = %invocation_id, worker_id = %worker_id,
        event_type = if stream == "stderr" { "stderr.line" } else { "stdout.line" },
        stream = %stream, text = %line, "worker invocation event"
    );
}

async fn send_event(
    client: &DaemonClient,
    claim: &InvocationClaimResponse,
    event: crate::execution::ExecutionEvent,
) -> AppResult<()> {
    WorkerInvocationSession::new(client, claim)
        .append_event(event)
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
            _ => &[],
        }
    }

    fn profiles_yml(&self) -> &str {
        match self {
            Self::Remote { profiles_yml, .. } => profiles_yml,
            Self::EnvironmentValidate { profiles_yml, .. } => profiles_yml,
            _ => "",
        }
    }

    fn state_manifest(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Local { state_manifest, .. } | Self::Remote { state_manifest, .. } => {
                state_manifest.as_ref()
            }
            _ => None,
        }
    }
}

async fn materialize_execution_project_dir(
    spec: &InvocationExecutionSpecApi,
) -> AppResult<PathBuf> {
    match spec {
        InvocationExecutionSpecApi::Local { args, .. } => {
            let args = args.iter().cloned().map(Into::into).collect::<Vec<_>>();
            let ctx = crate::config::InvocationContext::from_args(&args, false)?;
            Ok(ctx.project_dir)
        }
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
) -> AppResult<Option<RuntimeProjectGuard>> {
    if !persists_state(command) {
        return Ok(None);
    }
    match spec {
        InvocationExecutionSpecApi::Remote { repo_url, .. } => {
            patch_remote_runtime_project(repo_url, project_dir).await?;
            Ok(None)
        }
        InvocationExecutionSpecApi::Local { .. } => {
            let guard = patch_local_runtime_project(project_dir)?;
            Ok(Some(guard))
        }
        _ => Ok(None),
    }
}

/// Guard that restores the original `dbt_project.yml` and removes the macro file on drop.
pub(crate) struct RuntimeProjectGuard {
    project_path: PathBuf,
    original_content: String,
    macro_file: PathBuf,
}

impl Drop for RuntimeProjectGuard {
    fn drop(&mut self) {
        if let Err(err) = std::fs::write(&self.project_path, &self.original_content) {
            warn!(path = %self.project_path.display(), error = %err, "failed to restore dbt_project.yml");
        }
        let _ = std::fs::remove_file(&self.macro_file);
    }
}

fn patch_local_runtime_project(project_dir: &Path) -> AppResult<RuntimeProjectGuard> {
    let project_path = project_dir.join("dbt_project.yml");
    let original_content = std::fs::read_to_string(&project_path)?;
    let mut project_yaml: YamlValue = serde_yaml::from_str(&original_content)?;
    ensure_on_run_start_hook(&mut project_yaml)?;
    let macro_file = ensure_selected_resources_macro_file(project_dir, &project_yaml)?;
    std::fs::write(&project_path, serde_yaml::to_string(&project_yaml)?)?;
    std::fs::write(&macro_file, DBTX_SELECTED_RESOURCES_MACRO)?;
    Ok(RuntimeProjectGuard {
        project_path,
        original_content,
        macro_file,
    })
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
    let mut project_yaml: YamlValue =
        serde_yaml::from_str(&std::fs::read_to_string(&project_path)?)?;
    ensure_on_run_start_hook(&mut project_yaml)?;
    let macro_file = ensure_selected_resources_macro_file(project_dir, &project_yaml)?;
    std::fs::write(project_path, serde_yaml::to_string(&project_yaml)?)?;
    std::fs::write(macro_file, DBTX_SELECTED_RESOURCES_MACRO)?;
    Ok(())
}

fn ensure_on_run_start_hook(project_yaml: &mut YamlValue) -> AppResult<()> {
    let mapping = project_yaml
        .as_mapping_mut()
        .ok_or_else(|| AppError::Internal("dbt_project.yml must be a YAML mapping".to_string()))?;
    let key = YamlValue::String("on-run-start".to_string());
    let hook = YamlValue::String(DBTX_SELECTED_RESOURCES_HOOK.to_string());
    let updated = match mapping.remove(&key) {
        Some(YamlValue::Sequence(mut seq)) => {
            if !seq
                .iter()
                .any(|v| v.as_str() == Some(DBTX_SELECTED_RESOURCES_HOOK))
            {
                seq.push(hook);
            }
            YamlValue::Sequence(seq)
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
        YamlValue::Sequence(seq) => seq
            .iter()
            .find_map(|item| item.as_str().filter(|p| !p.trim().is_empty()))
            .map(PathBuf::from),
        YamlValue::String(path) if !path.trim().is_empty() => Some(PathBuf::from(path)),
        _ => None,
    }
}

fn persists_state(command: InvocationCommandApi) -> bool {
    command_persists_state(command)
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
        git::ensure_git_worktree_in, git::resolve_remote_git_target, patch_runtime_project_yaml,
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

    #[test]
    fn local_runtime_project_guard_restores_on_drop() {
        use super::patch_local_runtime_project;

        let project = TempDir::new().expect("project dir");
        let original = "name: demo\n";
        std::fs::write(project.path().join("dbt_project.yml"), original).expect("dbt project");

        {
            let guard = patch_local_runtime_project(project.path()).expect("patch local project");
            // While guard is alive, file is patched
            let patched = std::fs::read_to_string(project.path().join("dbt_project.yml"))
                .expect("read patched");
            assert!(patched.contains(DBTX_SELECTED_RESOURCES_HOOK));
            assert!(
                project
                    .path()
                    .join("macros")
                    .join(DBTX_SELECTED_RESOURCES_MACRO_FILE)
                    .is_file()
            );
            drop(guard);
        }

        // After guard drops, file is restored
        let restored =
            std::fs::read_to_string(project.path().join("dbt_project.yml")).expect("read restored");
        assert_eq!(restored, original);
        assert!(
            !project
                .path()
                .join("macros")
                .join(DBTX_SELECTED_RESOURCES_MACRO_FILE)
                .exists()
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
            String::from_utf8_lossy(&output.stderr)
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

    #[test]
    fn map_command_returns_expected_dbt_subcommands() {
        use super::map_command;
        use crate::api::InvocationCommandApi;
        assert_eq!(map_command(InvocationCommandApi::Build), "build");
        assert_eq!(map_command(InvocationCommandApi::Run), "run");
        assert_eq!(map_command(InvocationCommandApi::Ls), "ls");
        assert_eq!(map_command(InvocationCommandApi::Test), "test");
        assert_eq!(map_command(InvocationCommandApi::Seed), "seed");
        assert_eq!(map_command(InvocationCommandApi::ManifestPrepare), "parse");
    }

    #[test]
    fn persists_state_is_true_for_data_commands() {
        use super::persists_state;
        use crate::api::InvocationCommandApi;
        assert!(persists_state(InvocationCommandApi::Build));
        assert!(persists_state(InvocationCommandApi::Run));
        assert!(persists_state(InvocationCommandApi::Test));
        assert!(persists_state(InvocationCommandApi::Seed));
        assert!(persists_state(InvocationCommandApi::ManifestPrepare));
    }

    #[test]
    fn persists_state_is_false_for_non_data_commands() {
        use super::persists_state;
        use crate::api::InvocationCommandApi;
        assert!(!persists_state(InvocationCommandApi::Ls));
        assert!(!persists_state(InvocationCommandApi::Release));
        assert!(!persists_state(InvocationCommandApi::ProjectValidate));
        assert!(!persists_state(InvocationCommandApi::EnvironmentPrepare));
        assert!(!persists_state(InvocationCommandApi::EnvironmentValidate));
    }

    #[test]
    fn short_hash_is_deterministic() {
        let a = super::git::short_hash("https://github.com/example/repo.git");
        let b = super::git::short_hash("https://github.com/example/repo.git");
        assert_eq!(a, b);
        assert_eq!(a.len(), 20);
    }

    #[test]
    fn short_hash_differs_for_different_inputs() {
        let a = super::git::short_hash("https://github.com/example/repo-a.git");
        let b = super::git::short_hash("https://github.com/example/repo-b.git");
        assert_ne!(a, b);
    }

    #[test]
    fn git_cache_root_respects_env_override() {
        unsafe { std::env::set_var("DBTX_GIT_CACHE_DIR", "/tmp/test-git-cache") };
        let root = super::git::git_cache_root().expect("cache root");
        assert_eq!(root, std::path::PathBuf::from("/tmp/test-git-cache"));
        unsafe { std::env::remove_var("DBTX_GIT_CACHE_DIR") };
    }

    #[test]
    fn local_spec_profiles_yml_is_empty() {
        use crate::api::{InvocationCommandApi, InvocationExecutionSpecApi};
        let spec = InvocationExecutionSpecApi::Local {
            command: InvocationCommandApi::Build,
            args: vec![],
            state_manifest: None,
        };
        assert!(spec.profiles_yml().is_empty());
        assert!(matches!(spec, InvocationExecutionSpecApi::Local { .. }));
    }

    #[tokio::test]
    async fn local_spec_materializes_project_dir_from_args() {
        use crate::api::{InvocationCommandApi, InvocationExecutionSpecApi};

        let project = TempDir::new().expect("project dir");
        let spec = InvocationExecutionSpecApi::Local {
            command: InvocationCommandApi::Build,
            args: vec![
                "--project-dir".to_string(),
                project.path().to_string_lossy().into_owned(),
            ],
            state_manifest: None,
        };

        let resolved = super::materialize_execution_project_dir(&spec)
            .await
            .expect("materialize local project dir");
        assert_eq!(resolved, project.path());
    }

    #[test]
    fn remote_spec_profiles_yml_is_populated() {
        use crate::api::{InvocationCommandApi, InvocationExecutionSpecApi};
        let spec = InvocationExecutionSpecApi::Remote {
            command: InvocationCommandApi::Build,
            args: vec![],
            repo_url: "git@github.com:org/repo.git".to_string(),
            commit_sha: "abc123".to_string(),
            project_root: ".".to_string(),
            profiles_yml: "dbtx:\n  target: prod\n".to_string(),
            state_manifest: None,
        };
        assert!(!spec.profiles_yml().is_empty());
        assert!(!matches!(spec, InvocationExecutionSpecApi::Local { .. }));
    }

    #[test]
    fn prepare_runtime_project_patches_local_spec() {
        use super::patch_local_runtime_project;

        let project = TempDir::new().expect("project dir");
        std::fs::write(
            project.path().join("dbt_project.yml"),
            "name: test_project\n",
        )
        .expect("write dbt_project.yml");

        let guard = patch_local_runtime_project(project.path()).expect("patch");
        let patched =
            std::fs::read_to_string(project.path().join("dbt_project.yml")).expect("read");
        assert!(patched.contains(DBTX_SELECTED_RESOURCES_HOOK));

        // Macro file exists
        assert!(
            project
                .path()
                .join("macros")
                .join(DBTX_SELECTED_RESOURCES_MACRO_FILE)
                .is_file()
        );

        drop(guard);

        // Restored
        let restored =
            std::fs::read_to_string(project.path().join("dbt_project.yml")).expect("read");
        assert_eq!(restored, "name: test_project\n");
    }
}
