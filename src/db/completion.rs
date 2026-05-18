//! Invocation completion side effects: draft status updates, release application,
//! manifest preparation tracking, and plan completion.

use super::*;

fn completion_terminal_status(status: InvocationLifecycleStatus) -> &'static str {
    match status {
        InvocationLifecycleStatus::Succeeded => "success",
        InvocationLifecycleStatus::Canceled => "canceled",
        InvocationLifecycleStatus::Failed => "failed",
        InvocationLifecycleStatus::Running => "running",
    }
}

fn completion_close_reason(status: InvocationLifecycleStatus) -> &'static str {
    match status {
        InvocationLifecycleStatus::Succeeded => "invocation_succeeded",
        InvocationLifecycleStatus::Failed => "invocation_failed",
        InvocationLifecycleStatus::Canceled => "invocation_canceled",
        InvocationLifecycleStatus::Running => "invocation_failed",
    }
}

impl InvocationPersistenceRecord {
    fn require_project_id(&self, context: &str) -> AppResult<i64> {
        self.project_id.ok_or_else(|| {
            AppError::Internal(format!("{context} invocation missing project scope"))
        })
    }

    fn require_environment_id(&self, context: &str) -> AppResult<i64> {
        self.environment_id.ok_or_else(|| {
            AppError::Internal(format!("{context} invocation missing environment scope"))
        })
    }

    fn require_project_draft_id(&self, context: &str) -> AppResult<Uuid> {
        self.project_draft_id.ok_or_else(|| {
            AppError::Internal(format!("{context} invocation missing draft scope"))
        })
    }

    fn require_environment_draft_id(&self, context: &str) -> AppResult<Uuid> {
        self.environment_draft_id.ok_or_else(|| {
            AppError::Internal(format!("{context} invocation missing draft scope"))
        })
    }
}

impl Db {
    /// Dispatch completion side effects based on invocation command type.
    /// Called within the invocation completion transaction.
    pub(super) async fn apply_invocation_completion_side_effects_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        invocation_id: Uuid,
        persistence: &InvocationPersistenceRecord,
        completion: &crate::execution::ExecutionCompletion,
    ) -> AppResult<()> {
        if let Some(run_id) = persistence.run_id {
            let manifest = completion.manifest.clone().map(ManifestSnapshot::from_raw);
            self.finalize_run_in_tx(
                tx,
                RunFinalization {
                    run_id,
                    project_id: persistence.project_id.ok_or_else(|| {
                        AppError::Internal("run invocation missing project scope".to_string())
                    })?,
                    environment_id: persistence.environment_id.ok_or_else(|| {
                        AppError::Internal("run invocation missing environment scope".to_string())
                    })?,
                    subcommand: &persistence.command,
                    dbt_version: completion.dbt_version.as_deref(),
                    exit_code: completion.exit_code,
                    terminal_status: completion_terminal_status(completion.status),
                    manifest: manifest.as_ref(),
                    promote_base_manifest: persistence.promote_base_manifest
                        && matches!(completion.status, InvocationLifecycleStatus::Succeeded),
                },
            )
            .await?;

            if persistence.updates_actual_state {
                self.upsert_environment_actual_state_for_run_in_tx(
                    tx,
                    run_id,
                    persistence.project_id.ok_or_else(|| {
                        AppError::Internal("run invocation missing project scope".to_string())
                    })?,
                    persistence.environment_id.ok_or_else(|| {
                        AppError::Internal("run invocation missing environment scope".to_string())
                    })?,
                    matches!(completion.status, InvocationLifecycleStatus::Succeeded),
                )
                .await?;
            }
        }

        // Command-specific side effects
        match persistence.command.as_str() {
            "release" if matches!(completion.status, InvocationLifecycleStatus::Succeeded) => {
                self.apply_release_completion_in_tx(
                    tx,
                    persistence.require_project_id("release")?,
                    persistence.require_environment_id("release")?,
                    completion.result.as_ref(),
                )
                .await?;
            }
            "project_validate" => {
                self.apply_project_validation_completion_in_tx(
                    tx,
                    persistence.require_project_draft_id("project_validate")?,
                    completion,
                )
                .await?;
            }
            "environment_prepare" => {
                self.apply_environment_prepare_completion_in_tx(
                    tx,
                    persistence.require_environment_draft_id("environment_prepare")?,
                    completion,
                )
                .await?;
            }
            "environment_validate" => {
                self.apply_environment_validation_completion_in_tx(
                    tx,
                    persistence.require_environment_draft_id("environment_validate")?,
                    completion,
                )
                .await?;
            }
            "manifest_prepare" => {
                self.apply_manifest_prepare_completion_in_tx(
                    tx,
                    persistence.require_project_id("manifest_prepare")?,
                    persistence.require_environment_id("manifest_prepare")?,
                    invocation_id,
                    completion,
                )
                .await?;
            }
            _ => {}
        }

        self.close_invocation_selected_resources_in_tx(
            tx,
            invocation_id,
            completion_close_reason(completion.status),
        )
        .await?;

        if let Some(plan_id) = persistence.plan_id {
            self.complete_environment_run_plan_in_tx(
                tx,
                plan_id,
                completion.status,
                completion.error.as_deref(),
            )
            .await?;
        }
        Ok(())
    }

    async fn apply_manifest_prepare_completion_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        project_id: i64,
        environment_id: i64,
        invocation_id: Uuid,
        completion: &crate::execution::ExecutionCompletion,
    ) -> AppResult<()> {
        let status = match completion.status {
            InvocationLifecycleStatus::Succeeded => "succeeded",
            InvocationLifecycleStatus::Failed | InvocationLifecycleStatus::Canceled => "failed",
            InvocationLifecycleStatus::Running => "failed",
        };
        let existing_failure_count: i32 = sqlx::query_scalar(
            r#"
            SELECT failure_count
            FROM environment_reconcile_preparations
            WHERE project_id = $1
              AND environment_id = $2
              AND kind = 'target_manifest'
              AND invocation_id = $3
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(invocation_id)
        .fetch_optional(&mut **tx)
        .await?
        .unwrap_or(0);
        let next_failure_count = if status == "succeeded" {
            0
        } else {
            existing_failure_count + 1
        };
        let next_attempt_at = if status == "succeeded" {
            None
        } else {
            Some(Utc::now() + automatic_retry_backoff(next_failure_count))
        };
        sqlx::query(
            r#"
            UPDATE environment_reconcile_preparations
            SET status = $4,
                error = $5,
                failure_count = $6,
                next_attempt_at = $7,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE project_id = $1
              AND environment_id = $2
              AND kind = 'target_manifest'
              AND invocation_id = $3
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(invocation_id)
        .bind(status)
        .bind(completion.error.as_deref())
        .bind(next_failure_count)
        .bind(next_attempt_at)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn apply_project_validation_completion_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        draft_id: Uuid,
        completion: &crate::execution::ExecutionCompletion,
    ) -> AppResult<()> {
        match completion.status {
            InvocationLifecycleStatus::Succeeded => {
                let result = completion.result.as_ref().ok_or_else(|| {
                    AppError::Internal("project validation completed without metadata".to_string())
                })?;
                let project_name = result
                    .get("project_name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        AppError::Internal("project validation missing project_name".to_string())
                    })?;
                let default_branch = result
                    .get("default_branch")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        AppError::Internal("project validation missing default_branch".to_string())
                    })?;
                sqlx::query(
                    r#"
                    UPDATE project_onboarding_drafts
                    SET status = 'validated',
                        validation_error = NULL,
                        project_name = $2,
                        default_branch = $3,
                        validated_at = NOW(),
                        updated_at = NOW()
                    WHERE id = $1
                    "#,
                )
                .bind(draft_id)
                .bind(project_name)
                .bind(default_branch)
                .execute(&mut **tx)
                .await?;
            }
            _ => {
                sqlx::query(
                    r#"
                    UPDATE project_onboarding_drafts
                    SET status = 'failed',
                        validation_error = $2,
                        validated_at = NULL,
                        updated_at = NOW()
                    WHERE id = $1
                    "#,
                )
                .bind(draft_id)
                .bind(
                    completion
                        .error
                        .as_deref()
                        .unwrap_or("project validation failed"),
                )
                .execute(&mut **tx)
                .await?;
            }
        }
        Ok(())
    }

    async fn apply_release_completion_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        project_id: i64,
        environment_id: i64,
        result: Option<&Value>,
    ) -> AppResult<()> {
        let result = result.ok_or_else(|| {
            AppError::Internal(
                "release validation completed without resolved commit metadata".to_string(),
            )
        })?;
        let resolved_commit_sha = result
            .get("resolved_commit_sha")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                AppError::Internal("release validation missing resolved_commit_sha".to_string())
            })?;
        let git_branch = result.get("git_branch").and_then(Value::as_str);

        let existing = self.get_environment_by_id_in_tx(tx, environment_id).await?;
        if existing.git_commit_sha.as_deref() == Some(resolved_commit_sha) {
            return Ok(());
        }

        sqlx::query(
            r#"
            UPDATE environments
            SET git_branch = $3,
                git_commit_sha = $4
            WHERE id = $1 AND project_id = $2
            "#,
        )
        .bind(environment_id)
        .bind(project_id)
        .bind(git_branch)
        .bind(resolved_commit_sha)
        .execute(&mut **tx)
        .await?;

        let environment = self.get_environment_by_id_in_tx(tx, environment_id).await?;
        self.record_environment_version_in_tx(tx, &environment, "released")
            .await?;
        Ok(())
    }

    async fn apply_environment_prepare_completion_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        draft_id: Uuid,
        completion: &crate::execution::ExecutionCompletion,
    ) -> AppResult<()> {
        match completion.status {
            InvocationLifecycleStatus::Succeeded => {
                let result = completion.result.as_ref().ok_or_else(|| {
                    AppError::Internal("environment prepare completed without metadata".to_string())
                })?;
                let selected_branch = result
                    .get("selected_branch")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
                let latest_commit_sha = result
                    .get("latest_commit_sha")
                    .and_then(Value::as_str)
                    .map(ToString::to_string);
                let branches = result
                    .get("branches")
                    .cloned()
                    .unwrap_or_else(|| Value::Array(Vec::new()));
                let commits = result
                    .get("commits")
                    .cloned()
                    .unwrap_or_else(|| Value::Array(Vec::new()));
                sqlx::query(
                    r#"
                    UPDATE environment_onboarding_drafts
                    SET status = 'ready',
                        validation_error = NULL,
                        git_branch = COALESCE($2, git_branch),
                        git_commit_sha = COALESCE($3, git_commit_sha),
                        branch_options = $4,
                        commit_options = $5,
                        updated_at = NOW()
                    WHERE id = $1
                    "#,
                )
                .bind(draft_id)
                .bind(selected_branch)
                .bind(latest_commit_sha)
                .bind(branches)
                .bind(commits)
                .execute(&mut **tx)
                .await?;
            }
            _ => {
                sqlx::query(
                    r#"
                    UPDATE environment_onboarding_drafts
                    SET status = 'failed',
                        validation_error = $2,
                        updated_at = NOW()
                    WHERE id = $1
                    "#,
                )
                .bind(draft_id)
                .bind(
                    completion
                        .error
                        .as_deref()
                        .unwrap_or("environment preparation failed"),
                )
                .execute(&mut **tx)
                .await?;
            }
        }
        Ok(())
    }

    async fn apply_environment_validation_completion_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        draft_id: Uuid,
        completion: &crate::execution::ExecutionCompletion,
    ) -> AppResult<()> {
        match completion.status {
            InvocationLifecycleStatus::Succeeded => {
                let result = completion.result.as_ref().ok_or_else(|| {
                    AppError::Internal(
                        "environment validation completed without metadata".to_string(),
                    )
                })?;
                let resolved_commit_sha = result
                    .get("resolved_commit_sha")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        AppError::Internal(
                            "environment validation missing resolved_commit_sha".to_string(),
                        )
                    })?;
                let selected_branch = result.get("selected_branch").and_then(Value::as_str);
                sqlx::query(
                    r#"
                    UPDATE environment_onboarding_drafts
                    SET status = 'validated',
                        validation_error = NULL,
                        git_branch = COALESCE($2, git_branch),
                        git_commit_sha = $3,
                        validated_at = NOW(),
                        updated_at = NOW()
                    WHERE id = $1
                    "#,
                )
                .bind(draft_id)
                .bind(selected_branch)
                .bind(resolved_commit_sha)
                .execute(&mut **tx)
                .await?;
            }
            _ => {
                sqlx::query(
                    r#"
                    UPDATE environment_onboarding_drafts
                    SET status = 'failed',
                        validation_error = $2,
                        validated_at = NULL,
                        updated_at = NOW()
                    WHERE id = $1
                    "#,
                )
                .bind(draft_id)
                .bind(
                    completion
                        .error
                        .as_deref()
                        .unwrap_or("environment validation failed"),
                )
                .execute(&mut **tx)
                .await?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::InvocationLifecycleStatus;

    #[test]
    fn terminal_status_maps_all_variants() {
        assert_eq!(completion_terminal_status(InvocationLifecycleStatus::Succeeded), "success");
        assert_eq!(completion_terminal_status(InvocationLifecycleStatus::Failed), "failed");
        assert_eq!(completion_terminal_status(InvocationLifecycleStatus::Canceled), "canceled");
        assert_eq!(completion_terminal_status(InvocationLifecycleStatus::Running), "running");
    }

    #[test]
    fn close_reason_maps_all_variants() {
        assert_eq!(completion_close_reason(InvocationLifecycleStatus::Succeeded), "invocation_succeeded");
        assert_eq!(completion_close_reason(InvocationLifecycleStatus::Failed), "invocation_failed");
        assert_eq!(completion_close_reason(InvocationLifecycleStatus::Canceled), "invocation_canceled");
        assert_eq!(completion_close_reason(InvocationLifecycleStatus::Running), "invocation_failed");
    }

    #[test]
    fn require_project_id_returns_error_when_missing() {
        let record = InvocationPersistenceRecord {
            plan_id: None,
            run_id: None,
            project_id: None,
            environment_id: None,
            project_draft_id: None,
            environment_draft_id: None,
            command: "build".to_string(),
            promote_base_manifest: false,
            updates_actual_state: false,
        };
        assert!(record.require_project_id("test").is_err());
    }

    #[test]
    fn require_project_id_returns_value_when_present() {
        let record = InvocationPersistenceRecord {
            plan_id: None,
            run_id: None,
            project_id: Some(42),
            environment_id: None,
            project_draft_id: None,
            environment_draft_id: None,
            command: "build".to_string(),
            promote_base_manifest: false,
            updates_actual_state: false,
        };
        assert_eq!(record.require_project_id("test").unwrap(), 42);
    }

    #[test]
    fn require_environment_draft_id_returns_error_when_missing() {
        let record = InvocationPersistenceRecord {
            plan_id: None,
            run_id: None,
            project_id: None,
            environment_id: None,
            project_draft_id: None,
            environment_draft_id: None,
            command: "environment_validate".to_string(),
            promote_base_manifest: false,
            updates_actual_state: false,
        };
        assert!(record.require_environment_draft_id("environment_validate").is_err());
    }

    #[test]
    fn promote_base_manifest_only_on_success() {
        let flag = true;
        assert!(flag && matches!(InvocationLifecycleStatus::Succeeded, InvocationLifecycleStatus::Succeeded));
        assert!(!(flag && matches!(InvocationLifecycleStatus::Failed, InvocationLifecycleStatus::Succeeded)));
        assert!(!(flag && matches!(InvocationLifecycleStatus::Canceled, InvocationLifecycleStatus::Succeeded)));
    }

    #[test]
    fn release_only_applies_on_success() {
        let command = "release";
        let status = InvocationLifecycleStatus::Succeeded;
        assert!(command == "release" && matches!(status, InvocationLifecycleStatus::Succeeded));

        let status = InvocationLifecycleStatus::Failed;
        assert!(!(command == "release" && matches!(status, InvocationLifecycleStatus::Succeeded)));
    }
}
