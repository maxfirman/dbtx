//! Reconciliation plans, leases, source state, preparation, and manifest planning.

use super::*;

impl Db {
    pub(crate) async fn get_environment_actual_state(
        &self,
        project: &str,
        environment_slug: &str,
    ) -> AppResult<EnvironmentActualStateRecord> {
        let environment = self.get_environment(project, environment_slug).await?;
        let row = sqlx::query(
            r#"
            SELECT
                project_id,
                environment_id,
                last_attempted_run_id,
                last_attempted_commit_sha,
                last_attempted_at,
                last_successful_run_id,
                last_successful_commit_sha,
                last_successful_at,
                last_admitted_plan_id,
                last_completed_plan_id,
                updated_at
            FROM environment_actual_state
            WHERE project_id = $1
              AND environment_id = $2
            "#,
        )
        .bind(environment.project_id)
        .bind(environment.id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row
            .as_ref()
            .map(environment_actual_state_from_row)
            .unwrap_or(EnvironmentActualStateRecord {
                project_id: environment.project_id,
                environment_id: environment.id,
                last_attempted_run_id: None,
                last_attempted_commit_sha: None,
                last_attempted_at: None,
                last_successful_run_id: None,
                last_successful_commit_sha: None,
                last_successful_at: None,
                last_admitted_plan_id: None,
                last_completed_plan_id: None,
                updated_at: Utc::now(),
            }))
    }

    pub(crate) async fn get_environment_reconcile_preparation(
        &self,
        project: &str,
        environment_slug: &str,
    ) -> AppResult<Option<EnvironmentReconcilePreparationRecord>> {
        let environment = self.get_environment(project, environment_slug).await?;
        self.get_environment_reconcile_preparation_by_scope(environment.project_id, environment.id)
            .await
    }

    pub(crate) async fn get_environment_reconcile_preparation_by_scope(
        &self,
        project_id: i64,
        environment_id: i64,
    ) -> AppResult<Option<EnvironmentReconcilePreparationRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                project_id,
                environment_id,
                kind,
                input_fingerprint,
                target_git_commit_sha,
                status,
                invocation_id,
                error,
                failure_count,
                next_attempt_at,
                started_at,
                completed_at,
                updated_at
            FROM environment_reconcile_preparations
            WHERE project_id = $1
              AND environment_id = $2
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(environment_reconcile_preparation_from_row))
    }

    pub(crate) async fn create_source_state_event(
        &self,
        input: SourceStateEventCreateInput,
    ) -> AppResult<SourceStateEventRecord> {
        let environment = self
            .get_environment(&input.project, &input.environment_slug)
            .await?;
        let row = sqlx::query(
            r#"
            INSERT INTO source_state_events (
                project_id, environment_id, source_key, provider, state_version, payload, observed_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, COALESCE($7, NOW()))
            RETURNING id, project_id, environment_id, source_key, provider, state_version, payload, observed_at, created_at
            "#,
        )
        .bind(environment.project_id)
        .bind(environment.id)
        .bind(input.source_key)
        .bind(input.provider)
        .bind(input.state_version)
        .bind(sqlx::types::Json(input.payload))
        .bind(input.observed_at)
        .fetch_one(&self.pool)
        .await?;
        Ok(source_state_event_from_row(&row))
    }

    pub(crate) async fn list_environment_run_plans(
        &self,
        project: &str,
        environment_slug: &str,
    ) -> AppResult<Vec<EnvironmentRunPlanRecord>> {
        let environment = self.get_environment(project, environment_slug).await?;
        self.list_environment_run_plans_by_scope(environment.project_id, environment.id)
            .await
    }

    pub(crate) async fn list_environment_run_plans_by_scope(
        &self,
        project_id: i64,
        environment_id: i64,
    ) -> AppResult<Vec<EnvironmentRunPlanRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT
                plan_id, project_id, environment_id, status, reason, input_fingerprint, target_git_branch,
                target_git_commit_sha, baseline_run_id, selection_spec, selected_resources,
                resource_count, superseded_by_plan_id, retry_count, blocked_by_invocation_id,
                admitted_invocation_id, source_event_id, error, failure_count, next_attempt_at,
                first_blocked_at,
                last_blocked_at, last_checked_at, admitted_at, completed_at, created_at,
                updated_at, metadata
            FROM environment_run_plans
            WHERE project_id = $1
              AND environment_id = $2
            ORDER BY created_at DESC, plan_id DESC
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(environment_run_plan_from_row).collect())
    }

    pub(crate) async fn list_blocked_environment_run_plan_ids(
        &self,
        project_id: i64,
        environment_id: i64,
    ) -> AppResult<Vec<Uuid>> {
        sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT plan_id
            FROM environment_run_plans
            WHERE project_id = $1
              AND environment_id = $2
              AND status = 'blocked'
            ORDER BY created_at ASC, plan_id ASC
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub(crate) async fn list_blocked_environment_scopes(
        &self,
    ) -> AppResult<Vec<(i64, i64)>> {
        let rows = sqlx::query(
            r#"
            SELECT DISTINCT project_id, environment_id
            FROM environment_run_plans
            WHERE status = 'blocked'
            ORDER BY project_id ASC, environment_id ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get("project_id"), row.get("environment_id")))
            .collect())
    }

    pub(crate) async fn get_environment_run_plan(
        &self,
        plan_id: Uuid,
    ) -> AppResult<EnvironmentRunPlanRecord> {
        let row = sqlx::query(
            r#"
            SELECT
                plan_id, project_id, environment_id, status, reason, input_fingerprint, target_git_branch,
                target_git_commit_sha, baseline_run_id, selection_spec, selected_resources,
                resource_count, superseded_by_plan_id, retry_count, blocked_by_invocation_id,
                admitted_invocation_id, source_event_id, error, failure_count, next_attempt_at,
                first_blocked_at,
                last_blocked_at, last_checked_at, admitted_at, completed_at, created_at,
                updated_at, metadata
            FROM environment_run_plans
            WHERE plan_id = $1
            "#,
        )
        .bind(plan_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::PlanNotFound(plan_id.to_string()))?;
        Ok(environment_run_plan_from_row(&row))
    }

    pub(crate) async fn create_environment_run_plan(
        &self,
        input: CreateEnvironmentRunPlanInput<'_>,
    ) -> AppResult<EnvironmentRunPlanRecord> {
        let row = sqlx::query(
            r#"
            INSERT INTO environment_run_plans (
                plan_id, project_id, environment_id, status, reason, input_fingerprint, target_git_branch,
                target_git_commit_sha, baseline_run_id, selection_spec, selected_resources,
                resource_count, source_event_id, metadata
            )
            VALUES (
                $1, $2, $3, 'planned', $4, $5, $6,
                $7, $8, $9, $10,
                $11, $12, $13
            )
            RETURNING
                plan_id, project_id, environment_id, status, reason, input_fingerprint, target_git_branch,
                target_git_commit_sha, baseline_run_id, selection_spec, selected_resources,
                resource_count, superseded_by_plan_id, retry_count, blocked_by_invocation_id,
                admitted_invocation_id, source_event_id, error, failure_count, next_attempt_at,
                first_blocked_at,
                last_blocked_at, last_checked_at, admitted_at, completed_at, created_at,
                updated_at, metadata
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(input.environment.project_id)
        .bind(input.environment.id)
        .bind(input.reason)
        .bind(input.input_fingerprint)
        .bind(input.environment.git_branch.as_deref())
        .bind(input.environment.git_commit_sha.as_deref())
        .bind(input.baseline_run_id)
        .bind(input.selection_spec)
        .bind(sqlx::types::Json(input.selected_resources))
        .bind(input.selected_resources.len() as i32)
        .bind(input.source_event_id)
        .bind(sqlx::types::Json(input.metadata))
        .fetch_one(&self.pool)
        .await?;
        Ok(environment_run_plan_from_row(&row))
    }

    pub(crate) async fn find_equivalent_live_environment_run_plan(
        &self,
        lookup: EquivalentPlanLookup<'_>,
    ) -> AppResult<Option<EnvironmentRunPlanRecord>> {
        let row = sqlx::query(
            r#"
            SELECT
                plan_id, project_id, environment_id, status, reason, input_fingerprint, target_git_branch,
                target_git_commit_sha, baseline_run_id, selection_spec, selected_resources,
                resource_count, superseded_by_plan_id, retry_count, blocked_by_invocation_id,
                admitted_invocation_id, source_event_id, error, failure_count, next_attempt_at,
                first_blocked_at,
                last_blocked_at, last_checked_at, admitted_at, completed_at, created_at,
                updated_at, metadata
            FROM environment_run_plans
            WHERE project_id = $1
              AND environment_id = $2
              AND status IN ('planned', 'blocked')
              AND reason = $3
              AND input_fingerprint = $4
              AND target_git_branch IS NOT DISTINCT FROM $5
              AND target_git_commit_sha IS NOT DISTINCT FROM $6
              AND baseline_run_id IS NOT DISTINCT FROM $7
              AND selection_spec IS NOT DISTINCT FROM $8
              AND selected_resources = $9
            ORDER BY created_at DESC, plan_id DESC
            LIMIT 1
            "#,
        )
        .bind(lookup.project_id)
        .bind(lookup.environment_id)
        .bind(lookup.reason)
        .bind(lookup.input_fingerprint)
        .bind(lookup.target_git_branch)
        .bind(lookup.target_git_commit_sha)
        .bind(lookup.baseline_run_id)
        .bind(lookup.selection_spec)
        .bind(sqlx::types::Json(lookup.selected_resources))
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.as_ref().map(environment_run_plan_from_row))
    }

    pub(crate) async fn supersede_pending_environment_run_plans(
        &self,
        project_id: i64,
        environment_id: i64,
        superseded_by_plan_id: Uuid,
    ) -> AppResult<u64> {
        let result = sqlx::query(
            r#"
            UPDATE environment_run_plans
            SET status = 'superseded',
                superseded_by_plan_id = $3,
                error = 'superseded by newer reconciliation plan',
                updated_at = NOW()
            WHERE project_id = $1
              AND environment_id = $2
              AND status IN ('planned', 'blocked')
              AND plan_id <> $3
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(superseded_by_plan_id)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub(crate) async fn acquire_environment_reconcile_lease(
        &self,
        environment_id: i64,
        owner: &str,
        lease_duration: std::time::Duration,
    ) -> AppResult<bool> {
        let lease_interval = chrono::Duration::from_std(lease_duration)
            .unwrap_or_else(|_| chrono::Duration::seconds(30));
        let leased_until = Utc::now() + lease_interval;
        let row = sqlx::query(
            r#"
            INSERT INTO environment_reconcile_leases (environment_id, owner, leased_until, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (environment_id) DO UPDATE
            SET owner = EXCLUDED.owner,
                leased_until = EXCLUDED.leased_until,
                updated_at = NOW()
            WHERE environment_reconcile_leases.leased_until < NOW()
               OR environment_reconcile_leases.owner = EXCLUDED.owner
            RETURNING owner
            "#,
        )
        .bind(environment_id)
        .bind(owner)
        .bind(leased_until)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.is_some())
    }

    pub(crate) async fn release_environment_reconcile_lease(
        &self,
        environment_id: i64,
        owner: &str,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            DELETE FROM environment_reconcile_leases
            WHERE environment_id = $1
              AND owner = $2
            "#,
        )
        .bind(environment_id)
        .bind(owner)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn mark_environment_run_plan_blocked(
        &self,
        plan_id: Uuid,
        blocked_by_invocation_id: Option<Uuid>,
        error: &str,
    ) -> AppResult<EnvironmentRunPlanRecord> {
        let row = sqlx::query(
            r#"
            UPDATE environment_run_plans
            SET status = 'blocked',
                blocked_by_invocation_id = $2,
                error = $3,
                retry_count = retry_count + 1,
                next_attempt_at = NULL,
                first_blocked_at = COALESCE(first_blocked_at, NOW()),
                last_blocked_at = NOW(),
                last_checked_at = NOW(),
                updated_at = NOW()
            WHERE plan_id = $1
            RETURNING
                plan_id, project_id, environment_id, status, reason, input_fingerprint, target_git_branch,
                target_git_commit_sha, baseline_run_id, selection_spec, selected_resources,
                resource_count, superseded_by_plan_id, retry_count, blocked_by_invocation_id,
                admitted_invocation_id, source_event_id, error, failure_count, next_attempt_at,
                first_blocked_at,
                last_blocked_at, last_checked_at, admitted_at, completed_at, created_at,
                updated_at, metadata
            "#,
        )
        .bind(plan_id)
        .bind(blocked_by_invocation_id)
        .bind(error)
        .fetch_one(&self.pool)
        .await?;
        Ok(environment_run_plan_from_row(&row))
    }

    pub(crate) async fn mark_environment_run_plan_admitted(
        &self,
        plan_id: Uuid,
        invocation_id: Uuid,
    ) -> AppResult<EnvironmentRunPlanRecord> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            WITH current_plan AS (
                SELECT *
                FROM environment_run_plans
                WHERE plan_id = $1
                FOR UPDATE
            ),
            admitted AS (
                UPDATE environment_run_plans plan
                SET status = 'admitted',
                    admitted_invocation_id = $2,
                    superseded_by_plan_id = NULL,
                    blocked_by_invocation_id = NULL,
                    error = NULL,
                    next_attempt_at = NULL,
                    last_checked_at = NOW(),
                    admitted_at = NOW(),
                    updated_at = NOW()
                FROM current_plan
                WHERE plan.plan_id = current_plan.plan_id
                  AND current_plan.status IN ('planned', 'blocked', 'admitted')
                RETURNING
                    plan.plan_id, plan.project_id, plan.environment_id, plan.status, plan.reason,
                    plan.input_fingerprint, plan.target_git_branch, plan.target_git_commit_sha,
                    plan.baseline_run_id, plan.selection_spec, plan.selected_resources,
                    plan.resource_count, plan.superseded_by_plan_id, plan.retry_count,
                    plan.blocked_by_invocation_id, plan.admitted_invocation_id,
                    plan.source_event_id, plan.error, plan.failure_count, plan.next_attempt_at,
                    plan.first_blocked_at, plan.last_blocked_at, plan.last_checked_at,
                    plan.admitted_at, plan.completed_at, plan.created_at, plan.updated_at,
                    plan.metadata
            )
            SELECT
                plan_id, project_id, environment_id, status, reason, input_fingerprint, target_git_branch,
                target_git_commit_sha, baseline_run_id, selection_spec, selected_resources,
                resource_count, superseded_by_plan_id, retry_count, blocked_by_invocation_id,
                admitted_invocation_id, source_event_id, error, failure_count, next_attempt_at,
                first_blocked_at,
                last_blocked_at, last_checked_at, admitted_at, completed_at, created_at,
                updated_at, metadata
            FROM admitted
            UNION ALL
            SELECT
                plan_id, project_id, environment_id, status, reason, input_fingerprint, target_git_branch,
                target_git_commit_sha, baseline_run_id, selection_spec, selected_resources,
                resource_count, superseded_by_plan_id, retry_count, blocked_by_invocation_id,
                admitted_invocation_id, source_event_id, error, failure_count, next_attempt_at,
                first_blocked_at,
                last_blocked_at, last_checked_at, admitted_at, completed_at, created_at,
                updated_at, metadata
            FROM current_plan
            WHERE NOT EXISTS (SELECT 1 FROM admitted)
            "#,
        )
        .bind(plan_id)
        .bind(invocation_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| AppError::PlanNotFound(plan_id.to_string()))?;
        sqlx::query(
            r#"
            INSERT INTO environment_actual_state (
                project_id, environment_id, last_admitted_plan_id, updated_at
            )
            SELECT project_id, environment_id, $1, NOW()
            FROM environment_run_plans
            WHERE plan_id = $1
            ON CONFLICT (project_id, environment_id) DO UPDATE SET
                last_admitted_plan_id = EXCLUDED.last_admitted_plan_id,
                updated_at = NOW()
            "#,
        )
        .bind(plan_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(environment_run_plan_from_row(&row))
    }

    pub(crate) async fn update_environment_run_plan_selection(
        &self,
        plan_id: Uuid,
        selection_spec: Option<&str>,
        selected_resources: &[String],
        metadata: Value,
    ) -> AppResult<EnvironmentRunPlanRecord> {
        let row = sqlx::query(
            r#"
            UPDATE environment_run_plans
            SET selection_spec = $2,
                selected_resources = $3,
                resource_count = $4,
                metadata = $5,
                next_attempt_at = NULL,
                last_checked_at = NOW(),
                updated_at = NOW()
            WHERE plan_id = $1
            RETURNING
                plan_id, project_id, environment_id, status, reason, input_fingerprint, target_git_branch,
                target_git_commit_sha, baseline_run_id, selection_spec, selected_resources,
                resource_count, superseded_by_plan_id, retry_count, blocked_by_invocation_id,
                admitted_invocation_id, source_event_id, error, failure_count, next_attempt_at,
                first_blocked_at,
                last_blocked_at, last_checked_at, admitted_at, completed_at, created_at,
                updated_at, metadata
            "#,
        )
        .bind(plan_id)
        .bind(selection_spec)
        .bind(sqlx::types::Json(selected_resources))
        .bind(selected_resources.len() as i32)
        .bind(sqlx::types::Json(metadata))
        .fetch_one(&self.pool)
        .await?;
        Ok(environment_run_plan_from_row(&row))
    }

    pub(crate) async fn mark_environment_run_plan_completed_noop(
        &self,
        plan_id: Uuid,
        error: &str,
        metadata: Value,
    ) -> AppResult<EnvironmentRunPlanRecord> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            UPDATE environment_run_plans
            SET status = 'completed',
                selected_resources = '[]'::jsonb,
                resource_count = 0,
                blocked_by_invocation_id = NULL,
                error = $2,
                failure_count = 0,
                next_attempt_at = NULL,
                metadata = $3,
                last_checked_at = NOW(),
                completed_at = NOW(),
                updated_at = NOW()
            WHERE plan_id = $1
            RETURNING
                plan_id, project_id, environment_id, status, reason, input_fingerprint, target_git_branch,
                target_git_commit_sha, baseline_run_id, selection_spec, selected_resources,
                resource_count, superseded_by_plan_id, retry_count, blocked_by_invocation_id,
                admitted_invocation_id, source_event_id, error, failure_count, next_attempt_at, first_blocked_at,
                last_blocked_at, last_checked_at, admitted_at, completed_at, created_at,
                updated_at, metadata
            "#,
        )
        .bind(plan_id)
        .bind(error)
        .bind(sqlx::types::Json(metadata))
        .fetch_one(&mut *tx)
        .await?;
        sqlx::query(
            r#"
            INSERT INTO environment_actual_state (
                project_id, environment_id, last_completed_plan_id, updated_at
            )
            SELECT project_id, environment_id, $1, NOW()
            FROM environment_run_plans
            WHERE plan_id = $1
            ON CONFLICT (project_id, environment_id) DO UPDATE SET
                last_completed_plan_id = EXCLUDED.last_completed_plan_id,
                updated_at = NOW()
            "#,
        )
        .bind(plan_id)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(environment_run_plan_from_row(&row))
    }

    pub(crate) async fn list_active_conflicting_invocations(
        &self,
        plan_id: Uuid,
    ) -> AppResult<Vec<Uuid>> {
        let rows = sqlx::query_scalar::<_, Uuid>(
            r#"
            WITH plan_resources AS (
                SELECT plan.plan_id, plan.project_id, plan.environment_id, sel.unique_id
                FROM environment_run_plans plan
                JOIN LATERAL jsonb_array_elements_text(plan.selected_resources) sel(unique_id) ON TRUE
                WHERE plan.plan_id = $1
            ),
            active_resource_conflicts AS (
                SELECT DISTINCT isr.invocation_id
                FROM plan_resources pr
                JOIN invocation_selected_resources isr
                  ON isr.project_id = pr.project_id
                 AND isr.environment_id = pr.environment_id
                 AND isr.unique_id = pr.unique_id
                 AND isr.finished_at IS NULL
            ),
            admitted_plan_conflicts AS (
                SELECT DISTINCT other.admitted_invocation_id AS invocation_id
                FROM plan_resources pr
                JOIN environment_run_plans other
                  ON other.project_id = pr.project_id
                 AND other.environment_id = pr.environment_id
                 AND other.plan_id <> pr.plan_id
                 AND other.status = 'admitted'
                 AND other.admitted_invocation_id IS NOT NULL
                JOIN invocations inv
                  ON inv.invocation_id = other.admitted_invocation_id
                 AND inv.status = 'running'
                 AND inv.completed_at IS NULL
                JOIN LATERAL jsonb_array_elements_text(other.selected_resources) other_sel(unique_id)
                  ON other_sel.unique_id = pr.unique_id
            )
            SELECT DISTINCT invocation_id
            FROM (
                SELECT invocation_id FROM active_resource_conflicts
                UNION
                SELECT invocation_id FROM admitted_plan_conflicts
            ) conflicts
            ORDER BY invocation_id
            "#,
        )
        .bind(plan_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub(crate) async fn conflicting_resources_for_plan_and_invocation(
        &self,
        plan_id: Uuid,
        invocation_id: Uuid,
        sample_limit: i64,
    ) -> AppResult<(i64, Vec<String>)> {
        let row = sqlx::query(
            r#"
            WITH plan_resources AS (
                SELECT plan.plan_id, plan.project_id, plan.environment_id, sel.unique_id
                FROM environment_run_plans plan
                JOIN LATERAL jsonb_array_elements_text(plan.selected_resources) sel(unique_id) ON TRUE
                WHERE plan.plan_id = $1
            ),
            active_resource_conflicts AS (
                SELECT DISTINCT isr.unique_id
                FROM plan_resources pr
                JOIN invocation_selected_resources isr
                  ON isr.project_id = pr.project_id
                 AND isr.environment_id = pr.environment_id
                 AND isr.unique_id = pr.unique_id
                 AND isr.invocation_id = $2
                 AND isr.finished_at IS NULL
            ),
            admitted_plan_conflicts AS (
                SELECT DISTINCT other_sel.unique_id
                FROM plan_resources pr
                JOIN environment_run_plans other
                  ON other.project_id = pr.project_id
                 AND other.environment_id = pr.environment_id
                 AND other.plan_id <> pr.plan_id
                 AND other.status = 'admitted'
                 AND other.admitted_invocation_id = $2
                JOIN invocations inv
                  ON inv.invocation_id = other.admitted_invocation_id
                 AND inv.status = 'running'
                 AND inv.completed_at IS NULL
                JOIN LATERAL jsonb_array_elements_text(other.selected_resources) other_sel(unique_id)
                  ON other_sel.unique_id = pr.unique_id
            ),
            overlap AS (
                SELECT unique_id FROM active_resource_conflicts
                UNION
                SELECT unique_id FROM admitted_plan_conflicts
            )
            SELECT
                COUNT(*)::BIGINT AS overlap_count,
                COALESCE(
                    ARRAY(SELECT unique_id FROM overlap ORDER BY unique_id ASC LIMIT $3),
                    ARRAY[]::TEXT[]
                ) AS overlapping_resources
            FROM overlap
            "#,
        )
        .bind(plan_id)
        .bind(invocation_id)
        .bind(sample_limit)
        .fetch_one(&self.pool)
        .await?;
        Ok((
            row.get::<i64, _>("overlap_count"),
            row.get::<Vec<String>, _>("overlapping_resources"),
        ))
    }

    pub(crate) async fn list_manifest_node_unique_ids(
        &self,
        run_id: Uuid,
    ) -> AppResult<Vec<String>> {
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT unique_id
            FROM manifest_nodes
            WHERE run_id = $1
            ORDER BY unique_id ASC
            "#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub(crate) async fn latest_manifest_run_id_for_commit(
        &self,
        project_id: i64,
        environment_id: i64,
        commit_sha: &str,
    ) -> AppResult<Option<Uuid>> {
        sqlx::query_scalar::<_, Uuid>(
            r#"
            SELECT r.run_id
            FROM runs r
            JOIN manifest_snapshots ms ON ms.run_id = r.run_id
            WHERE r.project_id = $1
              AND r.environment_id = $2
              AND r.git_commit_sha = $3
            ORDER BY r.id DESC
            LIMIT 1
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(commit_sha)
        .fetch_optional(&self.pool)
        .await
        .map_err(Into::into)
    }

    pub(crate) async fn has_active_manifest_prepare_for_commit(
        &self,
        project_id: i64,
        environment_id: i64,
        commit_sha: &str,
    ) -> AppResult<bool> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM invocations i
                JOIN runs r ON r.run_id = i.run_id
                WHERE i.project_id = $1
                  AND i.environment_id = $2
                  AND i.command = 'manifest_prepare'
                  AND i.status = 'running'
                  AND i.completed_at IS NULL
                  AND r.git_commit_sha = $3
            )
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(commit_sha)
        .fetch_one(&self.pool)
        .await?;
        Ok(exists)
    }

    pub(crate) async fn mark_manifest_prepare_running(
        &self,
        project_id: i64,
        environment_id: i64,
        input_fingerprint: &str,
        target_git_commit_sha: &str,
        invocation_id: Uuid,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            INSERT INTO environment_reconcile_preparations (
                project_id,
                environment_id,
                kind,
                input_fingerprint,
                target_git_commit_sha,
                status,
                invocation_id,
                error,
                failure_count,
                next_attempt_at,
                started_at,
                completed_at,
                updated_at
            )
            VALUES ($1, $2, 'target_manifest', $3, $4, 'running', $5, NULL, 0, NULL, NOW(), NULL, NOW())
            ON CONFLICT (project_id, environment_id, kind) DO UPDATE SET
                input_fingerprint = EXCLUDED.input_fingerprint,
                target_git_commit_sha = EXCLUDED.target_git_commit_sha,
                status = EXCLUDED.status,
                invocation_id = EXCLUDED.invocation_id,
                error = NULL,
                next_attempt_at = NULL,
                started_at = NOW(),
                completed_at = NULL,
                updated_at = NOW()
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(input_fingerprint)
        .bind(target_git_commit_sha)
        .bind(invocation_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn load_planning_manifest_nodes(
        &self,
        run_id: Uuid,
    ) -> AppResult<Vec<PlanningManifestNodeRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT unique_id, resource_type, checksum
            FROM manifest_nodes
            WHERE run_id = $1
            ORDER BY unique_id ASC
            "#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| PlanningManifestNodeRecord {
                unique_id: row.get("unique_id"),
                resource_type: row.get("resource_type"),
                checksum: row.get("checksum"),
            })
            .collect())
    }

    pub(crate) async fn load_manifest_edges(
        &self,
        run_id: Uuid,
    ) -> AppResult<Vec<(String, String)>> {
        let rows = sqlx::query(
            r#"
            SELECT parent_unique_id, child_unique_id
            FROM manifest_edges
            WHERE run_id = $1
            ORDER BY parent_unique_id ASC, child_unique_id ASC
            "#,
        )
        .bind(run_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| (row.get("parent_unique_id"), row.get("child_unique_id")))
            .collect())
    }

    pub(crate) async fn load_current_node_state_for_planning(
        &self,
        project_id: i64,
        environment_id: i64,
    ) -> AppResult<Vec<CurrentNodeStatePlanningRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT unique_id, checksum, last_success_at
            FROM current_node_state
            WHERE project_id = $1
              AND environment_id = $2
            ORDER BY unique_id ASC
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| CurrentNodeStatePlanningRecord {
                unique_id: row.get("unique_id"),
                checksum: row.get("checksum"),
                last_success_at: row.get("last_success_at"),
            })
            .collect())
    }

    pub(crate) async fn list_unsatisfied_source_state_events(
        &self,
        project_id: i64,
        environment_id: i64,
    ) -> AppResult<Vec<SourceStateEventRecord>> {
        let rows = sqlx::query(
            r#"
            WITH latest_unsatisfied AS (
                SELECT DISTINCT ON (e.source_key)
                    e.id,
                    e.project_id,
                    e.environment_id,
                    e.source_key,
                    e.provider,
                    e.state_version,
                    e.payload,
                    e.observed_at,
                    e.created_at
                FROM source_state_events e
                LEFT JOIN environment_source_state_status s
                  ON s.project_id = e.project_id
                 AND s.environment_id = e.environment_id
                 AND s.source_key = e.source_key
                WHERE e.project_id = $1
                  AND e.environment_id = $2
                  AND (s.latest_satisfied_event_id IS NULL OR e.id > s.latest_satisfied_event_id)
                ORDER BY e.source_key ASC, e.id DESC
            )
            SELECT id, project_id, environment_id, source_key, provider, state_version, payload, observed_at, created_at
            FROM latest_unsatisfied
            ORDER BY observed_at ASC, id ASC
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(source_state_event_from_row).collect())
    }

    pub(crate) async fn are_source_state_events_satisfied(
        &self,
        project_id: i64,
        environment_id: i64,
        source_event_ids: &[i64],
    ) -> AppResult<bool> {
        if source_event_ids.is_empty() {
            return Ok(true);
        }
        let unsatisfied = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM source_state_events e
            LEFT JOIN environment_source_state_status s
              ON s.project_id = e.project_id
             AND s.environment_id = e.environment_id
             AND s.source_key = e.source_key
            WHERE e.project_id = $1
              AND e.environment_id = $2
              AND e.id = ANY($3::BIGINT[])
              AND (s.latest_satisfied_event_id IS NULL OR e.id > s.latest_satisfied_event_id)
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(source_event_ids)
        .fetch_one(&self.pool)
        .await?;
        Ok(unsatisfied == 0)
    }

    pub(crate) async fn list_downstream_manifest_node_unique_ids(
        &self,
        run_id: Uuid,
        source_keys: &[String],
    ) -> AppResult<Vec<String>> {
        if source_keys.is_empty() {
            return Ok(Vec::new());
        }
        sqlx::query_scalar::<_, String>(
            r#"
            WITH RECURSIVE reachable(unique_id) AS (
                SELECT unnest($2::TEXT[])
                UNION
                SELECT me.child_unique_id
                FROM manifest_edges me
                JOIN reachable r ON r.unique_id = me.parent_unique_id
                WHERE me.run_id = $1
            )
            SELECT DISTINCT mn.unique_id
            FROM reachable r
            JOIN manifest_nodes mn
              ON mn.run_id = $1
             AND mn.unique_id = r.unique_id
            ORDER BY mn.unique_id ASC
            "#,
        )
        .bind(run_id)
        .bind(source_keys)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Advance the environment actual state commit SHA without requiring a run.
    /// Used when a code change produces an empty diff (e.g. noop rollback).
    pub(crate) async fn advance_environment_actual_state_commit(
        &self,
        project_id: i64,
        environment_id: i64,
        commit_sha: &str,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            INSERT INTO environment_actual_state (project_id, environment_id, last_successful_commit_sha, updated_at)
            VALUES ($1, $2, $3, NOW())
            ON CONFLICT (project_id, environment_id) DO UPDATE SET
                last_successful_commit_sha = $3,
                updated_at = NOW()
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(commit_sha)
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}
