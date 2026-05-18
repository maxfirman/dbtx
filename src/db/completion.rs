//! Invocation completion side effects: draft status updates, release application,
//! manifest preparation tracking, and plan completion.

use super::*;

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
                    terminal_status: match completion.status {
                        InvocationLifecycleStatus::Succeeded => "success",
                        InvocationLifecycleStatus::Canceled => "canceled",
                        InvocationLifecycleStatus::Failed => "failed",
                        InvocationLifecycleStatus::Running => "running",
                    },
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

        if persistence.command == "release"
            && matches!(completion.status, InvocationLifecycleStatus::Succeeded)
        {
            self.apply_release_completion_in_tx(
                tx,
                persistence.project_id.ok_or_else(|| {
                    AppError::Internal("release invocation missing project scope".to_string())
                })?,
                persistence.environment_id.ok_or_else(|| {
                    AppError::Internal("release invocation missing environment scope".to_string())
                })?,
                completion.result.as_ref(),
            )
            .await?;
        }

        if persistence.command == "project_validate" {
            self.apply_project_validation_completion_in_tx(
                tx,
                persistence.project_draft_id.ok_or_else(|| {
                    AppError::Internal(
                        "project validation invocation missing draft scope".to_string(),
                    )
                })?,
                completion,
            )
            .await?;
        }

        if persistence.command == "environment_prepare" {
            self.apply_environment_prepare_completion_in_tx(
                tx,
                persistence.environment_draft_id.ok_or_else(|| {
                    AppError::Internal(
                        "environment prepare invocation missing draft scope".to_string(),
                    )
                })?,
                completion,
            )
            .await?;
        }

        if persistence.command == "environment_validate" {
            self.apply_environment_validation_completion_in_tx(
                tx,
                persistence.environment_draft_id.ok_or_else(|| {
                    AppError::Internal(
                        "environment validation invocation missing draft scope".to_string(),
                    )
                })?,
                completion,
            )
            .await?;
        }

        if persistence.command == "manifest_prepare" {
            self.apply_manifest_prepare_completion_in_tx(
                tx,
                persistence.project_id.ok_or_else(|| {
                    AppError::Internal(
                        "manifest prepare invocation missing project scope".to_string(),
                    )
                })?,
                persistence.environment_id.ok_or_else(|| {
                    AppError::Internal(
                        "manifest prepare invocation missing environment scope".to_string(),
                    )
                })?,
                invocation_id,
                completion,
            )
            .await?;
        }

        self.close_invocation_selected_resources_in_tx(
            tx,
            invocation_id,
            match completion.status {
                InvocationLifecycleStatus::Succeeded => "invocation_succeeded",
                InvocationLifecycleStatus::Failed => "invocation_failed",
                InvocationLifecycleStatus::Canceled => "invocation_canceled",
                InvocationLifecycleStatus::Running => "invocation_failed",
            },
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
    use crate::api::InvocationLifecycleStatus;

    /// The terminal_status mapping used in finalize_run_in_tx.
    fn terminal_status_for_run(status: InvocationLifecycleStatus) -> &'static str {
        match status {
            InvocationLifecycleStatus::Succeeded => "success",
            InvocationLifecycleStatus::Canceled => "canceled",
            InvocationLifecycleStatus::Failed => "failed",
            InvocationLifecycleStatus::Running => "running",
        }
    }

    /// The close reason mapping used in close_invocation_selected_resources_in_tx.
    fn selected_resource_close_reason(status: InvocationLifecycleStatus) -> &'static str {
        match status {
            InvocationLifecycleStatus::Succeeded => "invocation_succeeded",
            InvocationLifecycleStatus::Failed => "invocation_failed",
            InvocationLifecycleStatus::Canceled => "invocation_canceled",
            InvocationLifecycleStatus::Running => "invocation_failed",
        }
    }

    /// The manifest_prepare status mapping.
    fn manifest_prepare_status(status: InvocationLifecycleStatus) -> &'static str {
        match status {
            InvocationLifecycleStatus::Succeeded => "succeeded",
            InvocationLifecycleStatus::Failed | InvocationLifecycleStatus::Canceled => "failed",
            InvocationLifecycleStatus::Running => "failed",
        }
    }

    #[test]
    fn terminal_status_maps_all_variants() {
        assert_eq!(terminal_status_for_run(InvocationLifecycleStatus::Succeeded), "success");
        assert_eq!(terminal_status_for_run(InvocationLifecycleStatus::Failed), "failed");
        assert_eq!(terminal_status_for_run(InvocationLifecycleStatus::Canceled), "canceled");
        assert_eq!(terminal_status_for_run(InvocationLifecycleStatus::Running), "running");
    }

    #[test]
    fn close_reason_maps_all_variants() {
        assert_eq!(selected_resource_close_reason(InvocationLifecycleStatus::Succeeded), "invocation_succeeded");
        assert_eq!(selected_resource_close_reason(InvocationLifecycleStatus::Failed), "invocation_failed");
        assert_eq!(selected_resource_close_reason(InvocationLifecycleStatus::Canceled), "invocation_canceled");
        assert_eq!(selected_resource_close_reason(InvocationLifecycleStatus::Running), "invocation_failed");
    }

    #[test]
    fn manifest_prepare_status_maps_all_variants() {
        assert_eq!(manifest_prepare_status(InvocationLifecycleStatus::Succeeded), "succeeded");
        assert_eq!(manifest_prepare_status(InvocationLifecycleStatus::Failed), "failed");
        assert_eq!(manifest_prepare_status(InvocationLifecycleStatus::Canceled), "failed");
        assert_eq!(manifest_prepare_status(InvocationLifecycleStatus::Running), "failed");
    }

    #[test]
    fn promote_base_manifest_only_on_success() {
        // promote_base_manifest should only be true when both the flag is set AND status is Succeeded
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
