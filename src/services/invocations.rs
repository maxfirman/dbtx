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

        if project.mode != "remote" {
            return Err(AppError::RemoteExecutionRequiresRemoteProject(
                project.project_id.clone(),
                project.mode.clone(),
            ));
        }
        let repo_url = project.git_repo_url.clone().ok_or_else(|| {
            AppError::RemoteExecutionRequiresGitRepoUrl(project.project_id.clone())
        })?;
        let project_root = project.project_root.clone().ok_or_else(|| {
            AppError::RemoteExecutionRequiresProjectRoot(project.project_id.clone())
        })?;
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

        if project.mode != "remote" {
            return Err(AppError::RemoteExecutionRequiresRemoteProject(
                project.project_id.clone(),
                project.mode.clone(),
            ));
        }
        let repo_url = project.git_repo_url.clone().ok_or_else(|| {
            AppError::RemoteExecutionRequiresGitRepoUrl(project.project_id.clone())
        })?;
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
}
