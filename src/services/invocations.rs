//! Invocation preparation, execution, and lifecycle management.

use super::*;

pub struct InvocationService<'a> {
    db: &'a Db,
}

pub fn target_manifest_input_fingerprint(target_git_commit_sha: &str) -> String {
    format!("target_manifest:{target_git_commit_sha}")
}

pub fn code_change_input_fingerprint_for_baseline(
    desired_commit_sha: &str,
    baseline_run_id: Option<Uuid>,
) -> String {
    match baseline_run_id {
        Some(baseline_run_id) => code_change_input_fingerprint(desired_commit_sha, baseline_run_id),
        None => format!("code_change:{desired_commit_sha}:initial"),
    }
}

pub fn code_change_input_fingerprint(desired_commit_sha: &str, baseline_run_id: Uuid) -> String {
    format!("code_change:{desired_commit_sha}:{baseline_run_id}")
}

pub fn source_state_change_input_fingerprint(source_event_ids: &[i64]) -> String {
    let mut event_ids = source_event_ids.to_vec();
    event_ids.sort_unstable();
    event_ids.dedup();
    let joined = event_ids
        .into_iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("source_state_change:{joined}")
}

impl<'a> InvocationService<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    pub async fn prepare_remote_execution(
        &self,
        run_id: Uuid,
        command: InvocationCommand,
        args: Vec<OsString>,
        project_id: &str,
        environment_slug: &str,
    ) -> AppResult<LocalExecutionPrepared> {
        let project = self.db.get_project_by_project_id(project_id).await?;
        let environment = self
            .db
            .get_environment(project_id, environment_slug)
            .await?;

        let repo_url = project.git_repo_url.clone();
        let project_root = project.project_root.clone();
        let commit_sha = environment.git_commit_sha.clone().ok_or_else(|| {
            AppError::RemoteExecutionRequiresCommitSha(
                project.project_id.clone(),
                environment.slug.clone(),
            )
        })?;

        let inject_json_logging = command.persists_state();
        let fake_project_dir = PathBuf::from("/");
        let ctx =
            InvocationContext::from_args_in_dir(&args, inject_json_logging, &fake_project_dir)?;
        let project_name = project.project_name.clone();

        let reconstructed_manifest = self
            .db
            .load_reconstructed_manifest(project.id, environment.id)
            .await?
            .or(if ctx.wants_state_modified {
                Some(
                    ReconstructedManifest::write_empty_state(
                        &project_name,
                        &environment.adapter_type,
                    )
                    .await?,
                )
            } else {
                None
            });
        let state_manifest = if let Some(reconstructed_manifest) = reconstructed_manifest.as_ref() {
            let path = reconstructed_manifest.temp_dir.path().join("manifest.json");
            let content = tokio::fs::read_to_string(path).await?;
            Some(serde_json::from_str(&content)?)
        } else {
            None
        };
        let generated_profiles = build_generated_profiles(Path::new("."), &environment)?;
        let profiles_yml =
            tokio::fs::read_to_string(generated_profiles.temp_dir.path().join("profiles.yml"))
                .await?;
        let dbt_args = if command.persists_state() {
            append_invocation_id(ctx.dbt_args, run_id)
        } else {
            ctx.dbt_args
        };
        let persistence = if command.persists_state() {
            let args_json = Value::Array(
                dbt_args
                    .iter()
                    .map(|value| Value::String(value.to_string_lossy().into_owned()))
                    .collect(),
            );
            let git_state = GitState {
                branch: environment.git_branch.clone(),
                commit_sha: Some(commit_sha.clone()),
                repo_url: Some(repo_url.clone()),
            };
            self.db
                .insert_run_started(RunStart {
                    run_id,
                    project: &project,
                    environment: &environment,
                    subcommand: command.as_str(),
                    args_json,
                    is_full_graph_run: ctx.is_full_graph_run,
                    execution_mode: ExecutionMode::Server,
                    git_state: &git_state,
                })
                .await?;
            Some(LocalExecutionPersistence {
                run_id,
                project_id: project.id,
                environment_id: environment.id,
                subcommand: command.as_str().to_string(),
                promote_base_manifest: ctx.is_full_graph_run,
                updates_actual_state: true,
            })
        } else {
            None
        };

        Ok(LocalExecutionPrepared {
            spec: PreparedExecutionSpec::Remote(RemoteExecutionSpec {
                command,
                args: dbt_args,
                repo_url,
                commit_sha,
                project_root,
                profiles_yml,
                state_manifest,
            }),
            persistence,
            worker_queue: environment.worker_queue.clone(),
            project_id: Some(project.id),
            environment_id: Some(environment.id),
            project_draft_id: None,
            environment_draft_id: None,
        })
    }

    pub async fn prepare_remote_manifest_capture(
        &self,
        run_id: Uuid,
        project_id: &str,
        environment_slug: &str,
    ) -> AppResult<LocalExecutionPrepared> {
        let mut prepared = self
            .prepare_remote_execution(
                run_id,
                InvocationCommand::ManifestPrepare,
                Vec::new(),
                project_id,
                environment_slug,
            )
            .await?;
        prepared.worker_queue = validation_worker_queue_from_env(
            std::env::var("DBTX_VALIDATION_QUEUE").ok().as_deref(),
        );
        if let Some(persistence) = prepared.persistence.as_mut() {
            persistence.subcommand = InvocationCommand::ManifestPrepare.as_str().to_string();
            persistence.promote_base_manifest = false;
            persistence.updates_actual_state = false;
        }
        Ok(prepared)
    }

    pub async fn prepare_release_validation(
        &self,
        args: Vec<OsString>,
        project_id: &str,
        environment_slug: &str,
    ) -> AppResult<LocalExecutionPrepared> {
        let project = self.db.get_project_by_project_id(project_id).await?;
        let environment = self
            .db
            .get_environment(project_id, environment_slug)
            .await?;

        let repo_url = project.git_repo_url.clone();
        let target = parse_release_target_args(&args)?;

        Ok(LocalExecutionPrepared {
            spec: PreparedExecutionSpec::ReleaseValidation(ReleaseValidationSpec {
                repo_url,
                git_ref: target.git_ref,
                git_commit_sha: target.git_commit_sha,
                git_branch: target.git_branch,
            }),
            persistence: None,
            worker_queue: environment.worker_queue.clone(),
            project_id: Some(project.id),
            environment_id: Some(environment.id),
            project_draft_id: None,
            environment_draft_id: None,
        })
    }

    pub async fn prepare_local_execution(
        &self,
        run_id: Uuid,
        command: InvocationCommand,
        args: Vec<OsString>,
        project: &ProjectRecord,
        environment: &EnvironmentRecord,
    ) -> AppResult<LocalExecutionPrepared> {
        if matches!(command, InvocationCommand::Release) {
            return Err(AppError::UnsupportedLocalExecution("release".to_string()));
        }

        let inject_json_logging = command.persists_state();
        let ctx = InvocationContext::from_args(&args, inject_json_logging)?;

        let reconstructed_manifest = self
            .db
            .load_reconstructed_manifest(project.id, environment.id)
            .await?
            .or(if ctx.wants_state_modified {
                Some(
                    ReconstructedManifest::write_empty_state(
                        &project.project_name,
                        &environment.adapter_type,
                    )
                    .await?,
                )
            } else {
                None
            });
        let state_manifest = if let Some(reconstructed_manifest) = reconstructed_manifest.as_ref() {
            let path = reconstructed_manifest.temp_dir.path().join("manifest.json");
            let content = tokio::fs::read_to_string(path).await?;
            Some(serde_json::from_str(&content)?)
        } else {
            None
        };

        let mut dbt_args = ctx.dbt_args;
        if command.persists_state() {
            dbt_args = append_invocation_id(dbt_args, run_id);
        }

        let persistence = if command.persists_state() {
            let args_json = Value::Array(
                dbt_args
                    .iter()
                    .map(|value| Value::String(value.to_string_lossy().into_owned()))
                    .collect(),
            );
            let git_state = read_git_state(&ctx.project_dir);
            self.db
                .insert_run_started(RunStart {
                    run_id,
                    project,
                    environment,
                    subcommand: command.as_str(),
                    args_json,
                    is_full_graph_run: ctx.is_full_graph_run,
                    execution_mode: ExecutionMode::Local,
                    git_state: &git_state,
                })
                .await?;
            Some(LocalExecutionPersistence {
                run_id,
                project_id: project.id,
                environment_id: environment.id,
                subcommand: command.as_str().to_string(),
                promote_base_manifest: ctx.is_full_graph_run,
                updates_actual_state: true,
            })
        } else {
            None
        };

        Ok(LocalExecutionPrepared {
            spec: PreparedExecutionSpec::Local(LocalExecutionSpec {
                command,
                args: dbt_args,
                state_manifest,
            }),
            persistence,
            worker_queue: environment.worker_queue.clone(),
            project_id: Some(project.id),
            environment_id: Some(environment.id),
            project_draft_id: None,
            environment_draft_id: None,
        })
    }
}

// --- Watermark manifest resolution ---

/// Plan reason indicating the invocation was triggered by a source state change.
const PLAN_REASON_SOURCE_STATE_CHANGE: &str = "source_state_change";

/// Pre-fetched plan data used for watermark resolution.
#[derive(Debug, Clone)]
pub struct WatermarkPlanContext {
    pub reason: String,
    pub baseline_run_id: Option<Uuid>,
    pub target_git_commit_sha: Option<String>,
    pub metadata: Value,
}

/// The inputs needed to resolve a watermark manifest run_id.
#[derive(Debug, Clone)]
pub struct WatermarkResolutionInput {
    pub command: String,
    pub project_id: Option<i64>,
    pub environment_id: Option<i64>,
    pub plan: Option<WatermarkPlanContext>,
    pub run_commit_sha: Option<String>,
}

/// The resolution strategy determined by the pure decision function.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WatermarkResolution {
    /// Command is not watermarkable or no scope — no watermark.
    None,
    /// Use a specific known run_id directly.
    Resolved(Option<Uuid>),
    /// Look up the latest manifest for a specific commit.
    LookupByCommit(String),
    /// Fall back to the latest manifest for the environment.
    LatestManifest,
}

/// Determines which watermark manifest to use based on pre-fetched context.
///
/// This is a pure function: all DB lookups happen before/after this call.
/// Returns a `WatermarkResolution` indicating what the caller should do.
pub fn resolve_watermark_strategy(input: &WatermarkResolutionInput) -> WatermarkResolution {
    if !is_watermarkable_command(&input.command) {
        return WatermarkResolution::None;
    }
    if input.project_id.is_none() || input.environment_id.is_none() {
        return WatermarkResolution::None;
    }

    if let Some(plan) = &input.plan {
        if plan.reason == PLAN_REASON_SOURCE_STATE_CHANGE {
            return WatermarkResolution::Resolved(plan.baseline_run_id);
        }
        if let Some(run_id) = plan
            .metadata
            .get("target_manifest_run_id")
            .and_then(Value::as_str)
            .and_then(|value| Uuid::parse_str(value).ok())
        {
            return WatermarkResolution::Resolved(Some(run_id));
        }
        if let Some(commit_sha) = &plan.target_git_commit_sha {
            return WatermarkResolution::LookupByCommit(commit_sha.clone());
        }
        return WatermarkResolution::Resolved(plan.baseline_run_id);
    }

    if let Some(commit_sha) = &input.run_commit_sha {
        return WatermarkResolution::LookupByCommit(commit_sha.clone());
    }

    WatermarkResolution::LatestManifest
}

/// Returns true if the command produces state that should be watermarked.
pub fn is_watermarkable_command(command: &str) -> bool {
    matches!(
        command.split_whitespace().next().unwrap_or(""),
        "build" | "run" | "test" | "seed"
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dbt_utils::append_invocation_id;

    #[test]
    fn release_command_does_not_persist_state() {
        assert!(!InvocationCommand::Release.persists_state());
    }

    #[test]
    fn ls_command_does_not_persist_state() {
        assert!(!InvocationCommand::Ls.persists_state());
    }

    #[test]
    fn build_command_persists_state() {
        assert!(InvocationCommand::Build.persists_state());
    }

    #[test]
    fn run_command_persists_state() {
        assert!(InvocationCommand::Run.persists_state());
    }

    #[test]
    fn test_command_persists_state() {
        assert!(InvocationCommand::Test.persists_state());
    }

    #[test]
    fn seed_command_persists_state() {
        assert!(InvocationCommand::Seed.persists_state());
    }

    #[test]
    fn append_invocation_id_adds_flag_to_args() {
        let args = vec![OsString::from("--select"), OsString::from("orders")];
        let run_id = Uuid::nil();
        let result = append_invocation_id(args, run_id);
        assert_eq!(result.len(), 4);
        assert_eq!(result[2], OsString::from("--invocation-id"));
        assert_eq!(
            result[3],
            OsString::from("00000000-0000-0000-0000-000000000000")
        );
    }

    #[test]
    fn invocation_context_detects_full_graph_run() {
        let args: Vec<OsString> = vec![];
        let ctx = InvocationContext::from_args_in_dir(&args, false, Path::new("/tmp"))
            .expect("parse context");
        assert!(ctx.is_full_graph_run);
    }

    #[test]
    fn invocation_context_detects_selective_run() {
        let args = vec![OsString::from("--select"), OsString::from("orders+")];
        let ctx = InvocationContext::from_args_in_dir(&args, false, Path::new("/tmp"))
            .expect("parse context");
        assert!(!ctx.is_full_graph_run);
    }

    #[test]
    fn invocation_context_injects_json_logging_for_persisting_commands() {
        let args: Vec<OsString> = vec![OsString::from("--select"), OsString::from("orders")];
        let ctx = InvocationContext::from_args_in_dir(&args, true, Path::new("/tmp"))
            .expect("parse context");
        let args_str: Vec<String> = ctx
            .dbt_args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(args_str.contains(&"--log-format".to_string()));
        assert!(args_str.contains(&"json".to_string()));
        assert!(args_str.contains(&"--write-json".to_string()));
    }

    #[test]
    fn invocation_context_skips_json_logging_for_non_persisting_commands() {
        let args: Vec<OsString> = vec![OsString::from("--select"), OsString::from("orders")];
        let ctx = InvocationContext::from_args_in_dir(&args, false, Path::new("/tmp"))
            .expect("parse context");
        let args_str: Vec<String> = ctx
            .dbt_args
            .iter()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert!(!args_str.contains(&"--log-format".to_string()));
    }

    #[test]
    fn invocation_context_rejects_user_state_flag() {
        let args = vec![OsString::from("--state"), OsString::from("/tmp/state")];
        let result = InvocationContext::from_args_in_dir(&args, false, Path::new("/tmp"));
        assert!(result.is_err());
    }

    #[test]
    fn invocation_context_rejects_profiles_dir_flag() {
        let args = vec![
            OsString::from("--profiles-dir"),
            OsString::from("/tmp/profiles"),
        ];
        let result = InvocationContext::from_args_in_dir(&args, false, Path::new("/tmp"));
        assert!(result.is_err());
    }

    #[test]
    fn invocation_context_detects_state_modified_selector() {
        let args = vec![
            OsString::from("--select"),
            OsString::from("state:modified+"),
        ];
        let ctx = InvocationContext::from_args_in_dir(&args, true, Path::new("/tmp"))
            .expect("parse context");
        assert!(ctx.wants_state_modified);
    }

    #[test]
    fn invocation_context_no_state_modified_without_selector() {
        let args = vec![OsString::from("--select"), OsString::from("orders+")];
        let ctx = InvocationContext::from_args_in_dir(&args, true, Path::new("/tmp"))
            .expect("parse context");
        assert!(!ctx.wants_state_modified);
    }

    #[test]
    fn command_from_api_roundtrip() {
        use crate::api::InvocationCommandApi;
        let cases = [
            (InvocationCommandApi::Build, InvocationCommand::Build),
            (InvocationCommandApi::Run, InvocationCommand::Run),
            (InvocationCommandApi::Ls, InvocationCommand::Ls),
            (InvocationCommandApi::Test, InvocationCommand::Test),
            (InvocationCommandApi::Seed, InvocationCommand::Seed),
            (InvocationCommandApi::Release, InvocationCommand::Release),
        ];
        for (api_cmd, expected) in cases {
            let converted: InvocationCommand = api_cmd.into();
            assert_eq!(converted.as_str(), expected.as_str());
        }
    }

    // --- Watermark resolution tests ---

    fn base_input() -> WatermarkResolutionInput {
        WatermarkResolutionInput {
            command: "build".to_string(),
            project_id: Some(1),
            environment_id: Some(1),
            plan: None,
            run_commit_sha: None,
        }
    }

    #[test]
    fn watermark_non_watermarkable_command_returns_none() {
        let input = WatermarkResolutionInput {
            command: "ls".to_string(),
            ..base_input()
        };
        assert_eq!(
            resolve_watermark_strategy(&input),
            WatermarkResolution::None
        );
    }

    #[test]
    fn watermark_no_project_scope_returns_none() {
        let input = WatermarkResolutionInput {
            project_id: None,
            ..base_input()
        };
        assert_eq!(
            resolve_watermark_strategy(&input),
            WatermarkResolution::None
        );
    }

    #[test]
    fn watermark_no_environment_scope_returns_none() {
        let input = WatermarkResolutionInput {
            environment_id: None,
            ..base_input()
        };
        assert_eq!(
            resolve_watermark_strategy(&input),
            WatermarkResolution::None
        );
    }

    #[test]
    fn watermark_source_state_change_plan_uses_baseline() {
        let baseline = Uuid::new_v4();
        let input = WatermarkResolutionInput {
            plan: Some(WatermarkPlanContext {
                reason: "source_state_change".to_string(),
                baseline_run_id: Some(baseline),
                target_git_commit_sha: Some("abc123".to_string()),
                metadata: Value::Null,
            }),
            ..base_input()
        };
        assert_eq!(
            resolve_watermark_strategy(&input),
            WatermarkResolution::Resolved(Some(baseline))
        );
    }

    #[test]
    fn watermark_source_state_change_plan_without_baseline_returns_none() {
        let input = WatermarkResolutionInput {
            plan: Some(WatermarkPlanContext {
                reason: "source_state_change".to_string(),
                baseline_run_id: None,
                target_git_commit_sha: None,
                metadata: Value::Null,
            }),
            ..base_input()
        };
        assert_eq!(
            resolve_watermark_strategy(&input),
            WatermarkResolution::Resolved(None)
        );
    }

    #[test]
    fn watermark_plan_with_target_manifest_run_id_in_metadata() {
        let target_run = Uuid::new_v4();
        let input = WatermarkResolutionInput {
            plan: Some(WatermarkPlanContext {
                reason: "code_change".to_string(),
                baseline_run_id: Some(Uuid::new_v4()),
                target_git_commit_sha: Some("abc123".to_string()),
                metadata: serde_json::json!({
                    "target_manifest_run_id": target_run.to_string()
                }),
            }),
            ..base_input()
        };
        assert_eq!(
            resolve_watermark_strategy(&input),
            WatermarkResolution::Resolved(Some(target_run))
        );
    }

    #[test]
    fn watermark_plan_without_metadata_uses_commit_lookup() {
        let input = WatermarkResolutionInput {
            plan: Some(WatermarkPlanContext {
                reason: "code_change".to_string(),
                baseline_run_id: Some(Uuid::new_v4()),
                target_git_commit_sha: Some("deadbeef".to_string()),
                metadata: Value::Null,
            }),
            ..base_input()
        };
        assert_eq!(
            resolve_watermark_strategy(&input),
            WatermarkResolution::LookupByCommit("deadbeef".to_string())
        );
    }

    #[test]
    fn watermark_plan_without_commit_or_metadata_uses_baseline() {
        let baseline = Uuid::new_v4();
        let input = WatermarkResolutionInput {
            plan: Some(WatermarkPlanContext {
                reason: "code_change".to_string(),
                baseline_run_id: Some(baseline),
                target_git_commit_sha: None,
                metadata: Value::Null,
            }),
            ..base_input()
        };
        assert_eq!(
            resolve_watermark_strategy(&input),
            WatermarkResolution::Resolved(Some(baseline))
        );
    }

    #[test]
    fn watermark_run_commit_sha_triggers_commit_lookup() {
        let input = WatermarkResolutionInput {
            run_commit_sha: Some("cafebabe".to_string()),
            ..base_input()
        };
        assert_eq!(
            resolve_watermark_strategy(&input),
            WatermarkResolution::LookupByCommit("cafebabe".to_string())
        );
    }

    #[test]
    fn watermark_no_plan_no_run_falls_back_to_latest_manifest() {
        let input = base_input();
        assert_eq!(
            resolve_watermark_strategy(&input),
            WatermarkResolution::LatestManifest
        );
    }

    #[test]
    fn watermark_is_watermarkable_command_accepts_data_commands() {
        for command in ["build", "run", "test", "seed"] {
            assert!(is_watermarkable_command(command));
        }
    }

    #[test]
    fn watermark_is_watermarkable_command_uses_first_token() {
        assert!(is_watermarkable_command("build --select orders+"));
        assert!(is_watermarkable_command("run --full-refresh"));
        assert!(!is_watermarkable_command("ls --select orders+"));
    }

    #[test]
    fn watermark_is_watermarkable_command_rejects_non_data_commands() {
        for command in ["", "ls", "release", "environment-validate"] {
            assert!(!is_watermarkable_command(command));
        }
    }
}
