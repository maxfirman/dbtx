//! Environment reconciliation, planning, and release management.

use super::*;

pub struct EnvironmentService<'a> {
    db: &'a Db,
}

#[derive(Debug, Clone)]
pub struct EnvironmentPlanAdmitPrepared {
    pub plan: EnvironmentRunPlanRecord,
    pub invocation_id: Option<Uuid>,
    pub prepared: Option<LocalExecutionPrepared>,
}

impl<'a> EnvironmentService<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    async fn acquire_reconcile_lease(&self, environment_id: i64, owner: &str) -> AppResult<()> {
        if self
            .db
            .acquire_environment_reconcile_lease(environment_id, owner, RECONCILE_LEASE_DURATION)
            .await?
        {
            Ok(())
        } else {
            Err(AppError::ReconciliationInProgress)
        }
    }

    pub async fn create_draft(&self, project: String) -> AppResult<EnvironmentDraftRecord> {
        let project = self.db.get_project_by_project_id(&project).await?;
        if project.mode != "remote" {
            return Err(AppError::RemoteExecutionRequiresRemoteProject(
                project.project_id,
                project.mode,
            ));
        }
        self.db
            .create_environment_draft(CreateEnvironmentDraftInput {
                project_id: project.id,
                default_branch: project.default_branch,
            })
            .await
    }

    pub async fn get_draft(&self, draft_id: Uuid) -> AppResult<EnvironmentDraftRecord> {
        self.db.get_environment_draft(draft_id).await
    }

    pub async fn update_draft(
        &self,
        draft_id: Uuid,
        request: EnvironmentDraftUpdateRequest,
    ) -> AppResult<EnvironmentDraftRecord> {
        self.db
            .update_environment_draft(
                draft_id,
                UpdateEnvironmentDraftInput {
                    slug: request.slug,
                    git_branch: request.git_branch,
                    git_commit_sha: request.git_commit_sha,
                    use_latest_commit: request.use_latest_commit,
                    auto_reconcile: request.auto_reconcile,
                    immutable: request.immutable,
                    adapter_type: Some(request.adapter_type),
                    schema_name: Some(request.schema_name),
                    threads: request.threads,
                    profile_config: request.profile_config,
                    profile_secrets: request.profile_secrets,
                },
            )
            .await
    }

    pub async fn prepare_draft_git_metadata(
        &self,
        draft_id: Uuid,
    ) -> AppResult<EnvironmentDraftCreatePrepared> {
        let invocation_id = Uuid::new_v4();
        let draft = self.db.mark_environment_draft_loading_git(draft_id).await?;
        let project = self.db.get_project_by_id(draft.project_id).await?;
        let repo_url = project.git_repo_url.ok_or_else(|| {
            AppError::RemoteExecutionRequiresGitRepoUrl(project.project_id.clone())
        })?;
        Ok(EnvironmentDraftCreatePrepared {
            draft,
            invocation_id,
            spec: EnvironmentPrepareSpec {
                repo_url,
                selected_branch: None,
            },
            worker_queue: validation_worker_queue(),
        })
    }

    pub async fn refresh_draft_branch(
        &self,
        draft_id: Uuid,
        request: EnvironmentDraftUpdateRequest,
    ) -> AppResult<EnvironmentDraftCreatePrepared> {
        let draft = self.update_draft(draft_id, request).await?;
        let invocation_id = Uuid::new_v4();
        let draft = self.db.mark_environment_draft_loading_git(draft.id).await?;
        let project = self.db.get_project_by_id(draft.project_id).await?;
        let repo_url = project.git_repo_url.ok_or_else(|| {
            AppError::RemoteExecutionRequiresGitRepoUrl(project.project_id.clone())
        })?;
        Ok(EnvironmentDraftCreatePrepared {
            draft: draft.clone(),
            invocation_id,
            spec: EnvironmentPrepareSpec {
                repo_url,
                selected_branch: draft.git_branch.clone(),
            },
            worker_queue: validation_worker_queue(),
        })
    }

    pub async fn prepare_draft_validation(
        &self,
        draft_id: Uuid,
        request: EnvironmentDraftUpdateRequest,
    ) -> AppResult<EnvironmentDraftValidationPrepared> {
        let draft = self.update_draft(draft_id, request).await?;
        validate_environment_profile(
            draft.adapter_type.as_deref().unwrap_or_default(),
            draft.schema_name.as_deref().unwrap_or_default(),
            draft.threads,
            &draft.profile_config,
            &crate::profile::decrypt_json(&draft.profile_secrets)?,
            false,
        )?;
        let invocation_id = Uuid::new_v4();
        let draft = self.db.mark_environment_draft_validating(draft.id).await?;
        let project = self.db.get_project_by_id(draft.project_id).await?;
        let repo_url = project.git_repo_url.ok_or_else(|| {
            AppError::RemoteExecutionRequiresGitRepoUrl(project.project_id.clone())
        })?;
        let project_root = project.project_root.ok_or_else(|| {
            AppError::RemoteExecutionRequiresProjectRoot(project.project_id.clone())
        })?;
        let profile_record = EnvironmentProfileRecord {
            adapter_type: draft.adapter_type.clone().ok_or_else(|| {
                AppError::InvalidProfileConfig("adapter type is required".to_string())
            })?,
            schema_name: draft
                .schema_name
                .clone()
                .ok_or_else(|| AppError::InvalidProfileConfig("schema is required".to_string()))?,
            threads: draft.threads,
            profile_config: draft.profile_config.clone(),
            profile_secrets: draft.profile_secrets.clone(),
        };
        let profile_name = project.project_name.clone();
        let resolved = resolve_runtime_profile(&profile_name, &draft.slug, &profile_record)?;
        let generated = resolved.generate()?;
        let profiles_yml = std::fs::read_to_string(generated.temp_dir.path().join("profiles.yml"))?;
        let commit_sha = draft.git_commit_sha.clone().ok_or_else(|| {
            AppError::RemoteExecutionRequiresCommitSha(
                project.project_id.clone(),
                draft.slug.clone(),
            )
        })?;
        let selected_branch = draft.git_branch.clone();
        Ok(EnvironmentDraftValidationPrepared {
            draft,
            invocation_id,
            spec: EnvironmentValidationSpec {
                repo_url,
                commit_sha,
                project_root,
                selected_branch,
                profiles_yml,
            },
            worker_queue: validation_worker_queue(),
        })
    }

    pub async fn confirm_draft(&self, draft_id: Uuid) -> AppResult<EnvironmentRecord> {
        self.db.confirm_environment_draft(draft_id).await
    }

    pub async fn release(
        &self,
        request: EnvironmentReleaseRequest,
    ) -> AppResult<EnvironmentRecord> {
        let project = self.db.get_project_by_project_id(&request.project).await?;
        if project.mode != "remote" {
            return Err(AppError::RemoteExecutionRequiresRemoteProject(
                project.project_id,
                project.mode,
            ));
        }
        let git_commit_sha = request.git_commit_sha.ok_or_else(|| {
            AppError::InvalidReleaseTarget(
                "public release API requires git_commit_sha; use worker-validated release flow to resolve refs"
                    .to_string(),
            )
        })?;
        self.db
            .release_environment(EnvironmentReleaseInput {
                project: project.project_id,
                slug: request.slug,
                git_branch: request.git_branch,
                git_commit_sha,
            })
            .await
    }

    pub async fn history(
        &self,
        project: String,
        slug: String,
    ) -> AppResult<Vec<EnvironmentVersionRecord>> {
        self.db.list_environment_versions(&project, &slug).await
    }

    pub async fn rollback(
        &self,
        request: EnvironmentRollbackRequest,
    ) -> AppResult<EnvironmentRecord> {
        let project = self.db.get_project_by_project_id(&request.project).await?;
        if project.mode != "remote" {
            return Err(AppError::RemoteExecutionRequiresRemoteProject(
                project.project_id,
                project.mode,
            ));
        }
        self.db
            .rollback_environment_to_version(&project.project_id, &request.slug, request.version_id)
            .await
    }

    pub async fn list(&self, project: String) -> AppResult<Vec<EnvironmentRecord>> {
        self.db.list_environments(&project).await
    }

    pub async fn show(&self, project: String, slug: String) -> AppResult<EnvironmentRecord> {
        self.db.get_environment(&project, &slug).await
    }

    pub async fn actual_state(
        &self,
        project: String,
        slug: String,
    ) -> AppResult<EnvironmentActualStateRecord> {
        self.db.get_environment_actual_state(&project, &slug).await
    }

    pub async fn create_source_state_event(
        &self,
        request: SourceStateEventCreateRequest,
    ) -> AppResult<SourceStateEventRecord> {
        self.db
            .create_source_state_event(SourceStateEventCreateInput {
                project: request.project,
                environment_slug: request.slug,
                source_key: request.source_key,
                provider: request.provider,
                state_version: request.state_version,
                observed_at: request.observed_at,
                payload: request.payload,
            })
            .await
    }

    pub async fn list_plans(
        &self,
        project: String,
        slug: String,
    ) -> AppResult<Vec<EnvironmentRunPlanRecord>> {
        self.db.list_environment_run_plans(&project, &slug).await
    }

    pub async fn get_plan(&self, plan_id: Uuid) -> AppResult<EnvironmentRunPlanRecord> {
        self.db.get_environment_run_plan(plan_id).await
    }

    pub async fn reconcile(
        &self,
        project: String,
        slug: String,
    ) -> AppResult<EnvironmentRunPlanRecord> {
        let environment = self.db.get_environment(&project, &slug).await?;
        let lease_owner = format!("reconcile:{}", Uuid::new_v4());
        self.acquire_reconcile_lease(environment.id, &lease_owner)
            .await?;
        let result = async {
            let actual_state = self
                .db
                .get_environment_actual_state(&project, &slug)
                .await?;
            let source_events = self
                .db
                .list_unsatisfied_source_state_events(environment.project_id, environment.id)
                .await?;
            let draft = crate::services::planning::derive_environment_plan(
                self.db,
                &environment,
                &actual_state,
                &source_events,
            )
            .await?;

            if draft.selected_resources.is_empty() {
                if draft.code_drift
                    && let Some(sha) = &environment.git_commit_sha
                {
                    self.db
                        .advance_environment_actual_state_commit(
                            environment.project_id,
                            environment.id,
                            sha,
                        )
                        .await?;
                }
                return Err(AppError::ReconciliationEmptyPlan);
            }
            if let Some(plan) = self
                .db
                .find_equivalent_live_environment_run_plan(EquivalentPlanLookup {
                    project_id: environment.project_id,
                    environment_id: environment.id,
                    reason: draft.reason,
                    input_fingerprint: &draft.input_fingerprint,
                    target_git_branch: environment.git_branch.as_deref(),
                    target_git_commit_sha: environment.git_commit_sha.as_deref(),
                    baseline_run_id: draft.baseline_run_id,
                    selection_spec: draft.selection_spec.as_deref(),
                    selected_resources: &draft.selected_resources,
                })
                .await?
            {
                return Ok(plan);
            }

            let created = self
                .db
                .create_environment_run_plan(CreateEnvironmentRunPlanInput {
                    environment: &environment,
                    reason: draft.reason,
                    input_fingerprint: &draft.input_fingerprint,
                    baseline_run_id: draft.baseline_run_id,
                    selection_spec: draft.selection_spec.as_deref(),
                    selected_resources: &draft.selected_resources,
                    source_event_id: draft.source_event_id,
                    metadata: draft.metadata,
                })
                .await?;
            self.db
                .supersede_pending_environment_run_plans(
                    environment.project_id,
                    environment.id,
                    created.plan_id,
                )
                .await?;
            Ok(created)
        }
        .await;
        let _ = self
            .db
            .release_environment_reconcile_lease(environment.id, &lease_owner)
            .await;
        result
    }

    pub async fn admit_plan(
        &self,
        invocation_id: Uuid,
        plan_id: Uuid,
    ) -> AppResult<EnvironmentPlanAdmitPrepared> {
        let plan = self.db.get_environment_run_plan(plan_id).await?;
        let environment_id = plan.environment_id;
        let lease_owner = format!("admit:{}", invocation_id);
        self.acquire_reconcile_lease(environment_id, &lease_owner)
            .await?;
        let result = async {
            if !plan.status.is_admissible() {
                return Err(AppError::PlanNotAdmissible(
                    plan_id.to_string(),
                    plan.status.to_string(),
                ));
            }
            let plan = crate::services::planning::replan_pending_plan(self.db, plan).await?;
            if plan.status == PlanStatus::Completed {
                return Ok(EnvironmentPlanAdmitPrepared {
                    plan,
                    invocation_id: None,
                    prepared: None,
                });
            }
            let blockers = self.db.list_active_conflicting_invocations(plan_id).await?;
            if let Some(blocking_invocation_id) = blockers.first().copied() {
                let blocked = self
                    .db
                    .mark_environment_run_plan_blocked(
                        plan_id,
                        Some(blocking_invocation_id),
                        "plan is blocked by active resource overlap",
                    )
                    .await?;
                return Ok(EnvironmentPlanAdmitPrepared {
                    plan: blocked,
                    invocation_id: None,
                    prepared: None,
                });
            }

            let project = self.db.get_project_by_id(plan.project_id).await?;
            let environment = self.db.get_environment_by_id(plan.environment_id).await?;
            let mut args: Vec<OsString> = Vec::new();
            if !plan.selected_resources.is_empty() {
                args.push("--select".into());
                for resource in &plan.selected_resources {
                    args.push(resource.into());
                }
            }
            let prepared = InvocationService::new(self.db)
                .prepare_remote_execution(
                    invocation_id,
                    InvocationCommand::Build,
                    args,
                    &project.project_id,
                    &environment.slug,
                )
                .await?;
            Ok(EnvironmentPlanAdmitPrepared {
                plan,
                invocation_id: Some(invocation_id),
                prepared: Some(prepared),
            })
        }
        .await;
        let _ = self
            .db
            .release_environment_reconcile_lease(environment_id, &lease_owner)
            .await;
        result
    }

    /// Admit a plan and start its invocation in one call.
    ///
    /// This is the unified workflow that replaces the old three-step dance
    /// (service.admit_plan → start_prepared_invocation → mark_admitted).
    pub async fn admit_and_start_plan(
        &self,
        starter: &impl super::InvocationStarter,
        plan_id: Uuid,
    ) -> AppResult<super::EnvironmentPlanAdmission> {
        let invocation_id = Uuid::new_v4();
        let prepared = self.admit_plan(invocation_id, plan_id).await?;
        let Some(prepared_invocation) = prepared.prepared else {
            return Ok(super::EnvironmentPlanAdmission {
                plan: prepared.plan,
                invocation_id: None,
            });
        };
        starter
            .start_prepared_invocation(
                invocation_id,
                crate::api::InvocationCommandApi::Build,
                Some(plan_id),
                prepared_invocation,
            )
            .await?;
        let plan = self
            .db
            .mark_environment_run_plan_admitted(plan_id, invocation_id)
            .await?;
        Ok(super::EnvironmentPlanAdmission {
            plan,
            invocation_id: Some(invocation_id),
        })
    }
}
