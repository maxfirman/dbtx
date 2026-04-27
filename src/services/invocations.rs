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

struct InvocationScope<'a> {
    ctx: InvocationContext,
    project: &'a ProjectRecord,
    environment: &'a EnvironmentRecord,
    git_state: &'a GitState,
}

impl<'a> InvocationService<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    pub async fn invoke<O: InvocationObserver>(
        &self,
        request: InvocationRequest,
        observer: &mut O,
    ) -> AppResult<InvocationResult> {
        self.db.require_current_schema().await?;
        let inject_json_logging = request.command.persists_state();
        let current_dir = request
            .current_dir
            .clone()
            .unwrap_or(std::env::current_dir()?);
        let ctx =
            InvocationContext::from_args_in_dir(&request.args, inject_json_logging, &current_dir)?;
        let git_state = read_git_state(&ctx.project_dir);
        let (project, environment) = self
            .resolve_local_project_and_environment(&ctx.project_dir, ctx.target_name.as_deref())
            .await?;
        match request.command {
            InvocationCommand::Ls => {
                self.invoke_ls(
                    &request,
                    InvocationScope {
                        ctx,
                        project: &project,
                        environment: &environment,
                        git_state: &git_state,
                    },
                    observer,
                )
                .await
            }
            _ => {
                self.invoke_persisting(
                    request.command.as_str(),
                    request.execution_mode,
                    &request.config,
                    InvocationScope {
                        ctx,
                        project: &project,
                        environment: &environment,
                        git_state: &git_state,
                    },
                    observer,
                )
                .await
            }
        }
    }

    pub async fn prepare_local_execution(
        &self,
        run_id: Uuid,
        request: InvocationRequest,
    ) -> AppResult<LocalExecutionPrepared> {
        self.db.require_current_schema().await?;
        let inject_json_logging = request.command.persists_state();
        let current_dir = request
            .current_dir
            .clone()
            .unwrap_or(std::env::current_dir()?);
        let ctx =
            InvocationContext::from_args_in_dir(&request.args, inject_json_logging, &current_dir)?;
        let git_state = read_git_state(&ctx.project_dir);
        let (project, environment) = self
            .resolve_local_project_and_environment(&ctx.project_dir, ctx.target_name.as_deref())
            .await?;
        let reconstructed_manifest = self
            .db
            .load_reconstructed_manifest(project.id, environment.id)
            .await?
            .or(if ctx.wants_state_modified {
                Some(
                    ReconstructedManifest::write_empty_state(
                        &read_dbt_project_name(&ctx.project_dir),
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
        let generated_profiles = build_generated_profiles(&ctx.project_dir, &environment)?;
        let profiles_yml =
            tokio::fs::read_to_string(generated_profiles.temp_dir.path().join("profiles.yml"))
                .await?;
        let dbt_args = if request.command.persists_state() {
            append_invocation_id(ctx.dbt_args, run_id)
        } else {
            ctx.dbt_args
        };
        let persistence = if request.command.persists_state() {
            let args_json = Value::Array(
                dbt_args
                    .iter()
                    .map(|value| Value::String(value.to_string_lossy().into_owned()))
                    .collect(),
            );
            self.db
                .insert_run_started(RunStart {
                    run_id,
                    project: &project,
                    environment: &environment,
                    subcommand: request.command.as_str(),
                    args_json,
                    is_full_graph_run: ctx.is_full_graph_run,
                    execution_mode: request.execution_mode,
                    git_state: &git_state,
                })
                .await?;
            Some(LocalExecutionPersistence {
                run_id,
                project_id: project.id,
                environment_id: environment.id,
                subcommand: request.command.as_str().to_string(),
                promote_base_manifest: ctx.is_full_graph_run,
                updates_actual_state: true,
            })
        } else {
            None
        };

        Ok(LocalExecutionPrepared {
            spec: PreparedExecutionSpec::Local(LocalExecutionSpec {
                command: request.command,
                args: dbt_args,
                project_dir: ctx.project_dir.clone(),
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

    pub async fn prepare_remote_execution(
        &self,
        run_id: Uuid,
        command: InvocationCommand,
        args: Vec<OsString>,
        project_id: &str,
        environment_slug: &str,
    ) -> AppResult<LocalExecutionPrepared> {
        self.db.require_current_schema().await?;
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
        self.db.require_current_schema().await?;
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

    async fn resolve_local_project_and_environment(
        &self,
        project_dir: &Path,
        target_override: Option<&str>,
    ) -> AppResult<(ProjectRecord, EnvironmentRecord)> {
        let project = self.load_or_create_inferred_project(project_dir).await?;
        let local_profile = LocalTargetProfile::from_local_project(project_dir, target_override)?;
        let profile_secrets = local_profile.encrypted_secrets()?;
        let worker_queue = infer_local_worker_queue(project_dir)?;
        let environment = self
            .db
            .upsert_local_environment(LocalEnvironmentUpsertInput {
                project: &project,
                profile_name: &local_profile.profile_name,
                target_name: &local_profile.target_name,
                adapter_type: &local_profile.adapter_type,
                worker_queue: &worker_queue,
                schema_name: &local_profile.schema_name,
                threads: local_profile.threads,
                profile_config: &local_profile.profile_config,
                profile_secrets: &profile_secrets,
            })
            .await?;
        Ok((project, environment))
    }

    async fn load_or_create_inferred_project(
        &self,
        project_dir: &Path,
    ) -> AppResult<ProjectRecord> {
        let project_input = infer_local_project_defaults(project_dir, None, None, None)?;
        match self
            .db
            .get_project_by_project_id(&project_input.project_id)
            .await
        {
            Ok(project) => Ok(project),
            Err(AppError::ProjectIdNotFound(_)) => {
                self.db
                    .upsert_project(CreateProjectInput {
                        project_id: project_input.project_id,
                        project_name: project_input.project_name,
                        mode: project_input.mode,
                        git_repo_url: project_input.git_repo_url,
                        default_branch: project_input.default_branch,
                        project_root: project_input.project_root,
                    })
                    .await
            }
            Err(err) => Err(err),
        }
    }

    async fn invoke_persisting<O: InvocationObserver>(
        &self,
        subcommand: &str,
        execution_mode: ExecutionMode,
        config: &RuntimeConfig,
        scope: InvocationScope<'_>,
        observer: &mut O,
    ) -> AppResult<InvocationResult> {
        let InvocationScope {
            ctx,
            project,
            environment,
            git_state,
        } = scope;
        let run_id = Uuid::new_v4();
        let reconstructed_manifest = self
            .db
            .load_reconstructed_manifest(project.id, environment.id)
            .await?
            .or(if ctx.wants_state_modified {
                Some(
                    ReconstructedManifest::write_empty_state(
                        &read_dbt_project_name(&ctx.project_dir),
                        &environment.adapter_type,
                    )
                    .await?,
                )
            } else {
                None
            });
        let generated_profiles = build_generated_profiles(&ctx.project_dir, environment)?;
        let dbt_args = append_invocation_id(
            append_profiles_dir(
                append_state_dir(ctx.dbt_args.clone(), reconstructed_manifest.as_ref()),
                &generated_profiles,
            ),
            run_id,
        );
        let args_json = Value::Array(
            dbt_args
                .iter()
                .map(|value| Value::String(value.to_string_lossy().into_owned()))
                .collect(),
        );
        self.db
            .insert_run_started(RunStart {
                run_id,
                project,
                environment,
                subcommand,
                args_json,
                is_full_graph_run: ctx.is_full_graph_run,
                execution_mode,
                git_state,
            })
            .await?;

        let mut dbt_child = match crate::dbt_runner::DbtChild::spawn(
            &config.dbt_path,
            subcommand,
            &dbt_args,
            &ctx.project_dir,
        ) {
            Ok(child) => child,
            Err(err) => {
                self.db
                    .mark_run_finished(run_id, None, 1, "wrapper_failed")
                    .await?;
                return Err(err);
            }
        };

        let mut sequence_no: i64 = 0;
        let mut dbt_version: Option<String> = None;
        while let Some(line) = dbt_child.stdout_lines.next_line().await? {
            sequence_no += 1;
            if let Some(event) = LogEvent::parse(&line) {
                let rendered = event.render_text_line();
                observer.dbt_log(&event, rendered.as_deref());
                if dbt_version.is_none() && event.info.name == "MainReportVersion" {
                    dbt_version = event
                        .data
                        .get("version")
                        .and_then(Value::as_str)
                        .map(ToString::to_string);
                }
                self.db
                    .persist_log_event(
                        None,
                        run_id,
                        project.id,
                        environment.id,
                        sequence_no,
                        &event,
                    )
                    .await?;
            } else {
                observer.stdout_line(&line);
                self.db.persist_raw_line(run_id, sequence_no, &line).await?;
            }
        }

        let dbt_result = dbt_child.wait().await?;
        for line in &dbt_result.stderr_lines {
            observer.stderr_line(line);
        }

        let manifest_path = ctx.target_path.join("manifest.json");
        let manifest_result = ManifestSnapshot::from_path(&manifest_path).await;
        let terminal_status = if dbt_result.exit_code == 0 {
            "success"
        } else {
            "failed"
        };
        let exit_code = dbt_result.exit_code;

        self.db
            .finalize_run(RunFinalization {
                run_id,
                project_id: project.id,
                environment_id: environment.id,
                subcommand,
                dbt_version: dbt_version.as_deref(),
                exit_code,
                terminal_status,
                manifest: manifest_result.ok().as_ref(),
                promote_base_manifest: ctx.is_full_graph_run && exit_code == 0,
            })
            .await?;

        let result = InvocationResult { exit_code };
        if exit_code == 0 {
            Ok(result)
        } else {
            Err(AppError::DbtFailed(exit_code))
        }
    }

    async fn invoke_ls<O: InvocationObserver>(
        &self,
        request: &InvocationRequest,
        scope: InvocationScope<'_>,
        observer: &mut O,
    ) -> AppResult<InvocationResult> {
        let InvocationScope {
            ctx,
            project,
            environment,
            ..
        } = scope;
        let reconstructed_manifest = self
            .db
            .load_reconstructed_manifest(project.id, environment.id)
            .await?
            .or(if ctx.wants_state_modified {
                Some(
                    ReconstructedManifest::write_empty_state(
                        &read_dbt_project_name(&ctx.project_dir),
                        &environment.adapter_type,
                    )
                    .await?,
                )
            } else {
                None
            });
        let generated_profiles = build_generated_profiles(&ctx.project_dir, environment)?;
        let dbt_args = append_profiles_dir(
            append_state_dir(ctx.dbt_args.clone(), reconstructed_manifest.as_ref()),
            &generated_profiles,
        );

        let mut dbt_child = crate::dbt_runner::DbtChild::spawn(
            &request.config.dbt_path,
            InvocationCommand::Ls.as_str(),
            &dbt_args,
            &ctx.project_dir,
        )?;

        let mut stdout_lines = Vec::new();
        while let Some(line) = dbt_child.stdout_lines.next_line().await? {
            stdout_lines.push(line);
        }
        let dbt_result = dbt_child.wait().await?;

        for line in &stdout_lines {
            observer.stdout_line(line);
        }
        for line in &dbt_result.stderr_lines {
            observer.stderr_line(line);
        }

        let exit_code = dbt_result.exit_code;
        let result = InvocationResult { exit_code };
        if exit_code == 0 {
            Ok(result)
        } else {
            Err(AppError::DbtFailed(exit_code))
        }
    }
}
