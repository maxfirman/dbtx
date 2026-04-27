//! Invocation lifecycle: creation, claiming, heartbeats, cancellation, events, completion, workers, and queues.

use super::*;

impl Db {
    pub(crate) async fn create_invocation(
        &self,
        input: CreateInvocationInput,
    ) -> AppResult<InvocationStatusResponse> {
        let row = sqlx::query(
            r#"
            INSERT INTO invocations (
                invocation_id, plan_id, run_id, project_id, environment_id, project_draft_id, environment_draft_id,
                command, execution_mode, worker_queue, status, execution_spec, promote_base_manifest, updates_actual_state, claim_deadline_at
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'running', $11, $12, $13, $14)
            RETURNING invocation_id, execution_mode, worker_queue, status, exit_code, error,
                started_at, claimed_at, last_heartbeat_at, cancel_requested_at, completed_at,
                cancel_requested, claimed_by
            "#,
        )
        .bind(input.invocation_id)
        .bind(input.plan_id)
        .bind(input.run_id)
        .bind(input.project_id)
        .bind(input.environment_id)
        .bind(input.project_draft_id)
        .bind(input.environment_draft_id)
        .bind(&input.command)
        .bind(match input.execution_mode {
            InvocationExecutionModeApi::Server => "server",
            InvocationExecutionModeApi::Local => "local",
        })
        .bind(&input.worker_queue)
        .bind(input.execution_spec.as_ref().map(sqlx::types::Json))
        .bind(input.promote_base_manifest)
        .bind(input.updates_actual_state)
        .bind(input.claim_deadline_at)
        .fetch_one(&self.pool)
        .await?;
        Ok(invocation_status_from_row(&row))
    }

    pub(crate) async fn list_invocations(
        &self,
        filter: InvocationListApiRequest,
    ) -> AppResult<Vec<InvocationStatusResponse>> {
        let rows = sqlx::query(
            r#"
            SELECT invocation_id, execution_mode, worker_queue, status, exit_code, error,
                started_at, claimed_at, last_heartbeat_at, cancel_requested_at, completed_at,
                cancel_requested, claimed_by
            FROM invocations
            WHERE ($1::TEXT IS NULL OR status = $1)
              AND ($2::TEXT IS NULL OR execution_mode = $2)
              AND ($3::TEXT IS NULL OR worker_queue = $3)
              AND ($4::TEXT IS NULL OR claimed_by = $4)
              AND (
                $5::TEXT IS NULL
                OR ($5 = 'none' AND status <> 'canceled' AND cancel_requested = FALSE)
                OR ($5 = 'requested' AND status = 'running' AND cancel_requested = TRUE)
                OR ($5 = 'completed' AND status = 'canceled')
              )
            ORDER BY started_at DESC, invocation_id DESC
            LIMIT COALESCE($6, 100)
            "#,
        )
        .bind(filter.status.map(invocation_status_to_db))
        .bind(filter.execution_mode.map(|mode| match mode {
            InvocationExecutionModeApi::Server => "server",
            InvocationExecutionModeApi::Local => "local",
        }))
        .bind(filter.worker_queue)
        .bind(filter.claimed_by)
        .bind(filter.cancel_state.map(|state| match state {
            InvocationCancelStateApi::None => "none",
            InvocationCancelStateApi::Requested => "requested",
            InvocationCancelStateApi::Completed => "completed",
        }))
        .bind(filter.limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(invocation_status_from_row).collect())
    }

    pub(crate) async fn list_invocations_filtered(
        &self,
        filters: InvocationListFilters<'_>,
        limit: i64,
        offset: i64,
    ) -> AppResult<Vec<InvocationStatusResponse>> {
        let rows = sqlx::query(
            r#"
            SELECT invocation_id, execution_mode, worker_queue, status, exit_code, error,
                started_at, claimed_at, last_heartbeat_at, cancel_requested_at, completed_at,
                cancel_requested, claimed_by
            FROM invocations
            WHERE (
                cardinality($1::TEXT[]) = 0
                OR ('queued' = ANY($1) AND status = 'running' AND claimed_by IS NULL)
                OR ('running' = ANY($1) AND status = 'running' AND claimed_by IS NOT NULL AND cancel_requested = FALSE)
                OR ('cancelling' = ANY($1) AND status = 'running' AND claimed_by IS NOT NULL AND cancel_requested = TRUE)
                OR ('succeeded' = ANY($1) AND status = 'succeeded')
                OR ('failed' = ANY($1) AND status = 'failed')
                OR ('canceled' = ANY($1) AND status = 'canceled')
            )
              AND (cardinality($2::TEXT[]) = 0 OR execution_mode = ANY($2))
              AND (cardinality($3::TEXT[]) = 0 OR worker_queue = ANY($3))
              AND (cardinality($4::TEXT[]) = 0 OR claimed_by = ANY($4))
            ORDER BY started_at DESC, invocation_id DESC
            LIMIT $5
            OFFSET $6
            "#,
        )
        .bind(filters.display_statuses)
        .bind(filters.execution_modes)
        .bind(filters.worker_queues)
        .bind(filters.claimed_bys)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(invocation_status_from_row).collect())
    }

    pub(crate) async fn count_invocations_filtered(
        &self,
        filters: InvocationListFilters<'_>,
    ) -> AppResult<i64> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM invocations
            WHERE (
                cardinality($1::TEXT[]) = 0
                OR ('queued' = ANY($1) AND status = 'running' AND claimed_by IS NULL)
                OR ('running' = ANY($1) AND status = 'running' AND claimed_by IS NOT NULL AND cancel_requested = FALSE)
                OR ('cancelling' = ANY($1) AND status = 'running' AND claimed_by IS NOT NULL AND cancel_requested = TRUE)
                OR ('succeeded' = ANY($1) AND status = 'succeeded')
                OR ('failed' = ANY($1) AND status = 'failed')
                OR ('canceled' = ANY($1) AND status = 'canceled')
            )
              AND (cardinality($2::TEXT[]) = 0 OR execution_mode = ANY($2))
              AND (cardinality($3::TEXT[]) = 0 OR worker_queue = ANY($3))
              AND (cardinality($4::TEXT[]) = 0 OR claimed_by = ANY($4))
            "#,
        )
        .bind(filters.display_statuses)
        .bind(filters.execution_modes)
        .bind(filters.worker_queues)
        .bind(filters.claimed_bys)
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    pub(crate) async fn list_worker_filter_options(&self) -> AppResult<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            r#"
            SELECT value
            FROM (
                SELECT DISTINCT worker_id AS value FROM workers
                UNION
                SELECT DISTINCT claimed_by AS value
                FROM invocations
                WHERE claimed_by IS NOT NULL
            ) options
            ORDER BY value ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub(crate) async fn get_invocation_status(
        &self,
        invocation_id: Uuid,
    ) -> AppResult<InvocationStatusResponse> {
        let row = sqlx::query(
            r#"
            SELECT invocation_id, execution_mode, worker_queue, status, exit_code, error,
                started_at, claimed_at, last_heartbeat_at, cancel_requested_at, completed_at,
                cancel_requested, claimed_by
            FROM invocations
            WHERE invocation_id = $1
            "#,
        )
        .bind(invocation_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::InvocationNotFound(invocation_id.to_string())
        })?;
        Ok(invocation_status_from_row(&row))
    }

    pub(crate) async fn list_workers(&self) -> AppResult<Vec<WorkerStatusResponse>> {
        let worker_rows = sqlx::query(
            r#"
            SELECT worker_id, execution_mode, worker_queue, first_seen_at, last_seen_at
            FROM workers
            ORDER BY worker_id ASC, worker_queue ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        let claimed_rows = sqlx::query(
            r#"
            SELECT execution_mode, worker_queue, claimed_by, claimed_at, last_heartbeat_at, cancel_requested, status, started_at
            FROM invocations
            WHERE status = 'running'
              AND claimed_by IS NOT NULL
            ORDER BY claimed_by ASC, execution_mode ASC, worker_queue ASC, started_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let registry = worker_rows
            .into_iter()
            .map(worker_registry_read_model_from_row)
            .collect::<Vec<_>>();
        let mut claimed_counts: BTreeMap<String, i64> = BTreeMap::new();
        let mut active_health: BTreeMap<String, InvocationWorkerHealthApi> = BTreeMap::new();
        for row in claimed_rows {
            let model = invocation_read_model_from_row(&row);
            let model_health = compute_worker_health_from_model(&model);
            if let Some(worker_id) = model.claimed_by {
                *claimed_counts.entry(worker_id.clone()).or_insert(0) += 1;
                let entry = active_health
                    .entry(worker_id)
                    .or_insert(InvocationWorkerHealthApi::Claimed);
                if matches!(model_health, InvocationWorkerHealthApi::Stale) {
                    *entry = InvocationWorkerHealthApi::Stale;
                }
            }
        }

        let mut grouped: BTreeMap<String, Vec<WorkerRegistryReadModel>> = BTreeMap::new();
        for worker in registry {
            grouped.entry(worker.worker_id.clone()).or_default().push(worker);
        }

        Ok(grouped
            .into_iter()
            .map(|(worker_id, registrations)| {
                let execution_mode = registrations
                    .first()
                    .map(|worker| worker.execution_mode)
                    .unwrap_or(InvocationExecutionModeApi::Server);
                let claimed_invocation_count =
                    claimed_counts.get(&worker_id).copied().unwrap_or_default();
                let last_seen_at = registrations
                    .iter()
                    .map(|worker| worker.last_seen_at)
                    .max()
                    .unwrap_or_else(Utc::now);
                let worker_queues = registrations
                    .iter()
                    .map(|worker| worker.worker_queue.clone())
                    .collect::<Vec<_>>();
                let health = active_health.get(&worker_id).copied().unwrap_or_else(|| {
                    compute_worker_registry_health(
                        &registrations[0],
                        claimed_invocation_count,
                        last_seen_at,
                    )
                });
                WorkerStatusResponse {
                    worker_id,
                    execution_mode,
                    worker_queues,
                    claimed_invocation_count,
                    last_heartbeat_at: Some(last_seen_at),
                    health,
                }
            })
            .collect())
    }

    pub(crate) async fn list_queues(&self) -> AppResult<Vec<QueueStatusResponse>> {
        let rows = sqlx::query(
            r#"
            SELECT execution_mode, worker_queue, claimed_by, claimed_at, last_heartbeat_at, cancel_requested, status, started_at
            FROM invocations
            WHERE status = 'running'
            ORDER BY execution_mode ASC, worker_queue ASC, started_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut grouped: BTreeMap<(String, String), Vec<InvocationReadModel>> = BTreeMap::new();
        for row in rows {
            let model = invocation_read_model_from_row(&row);
            let mode = invocation_mode_value(model.execution_mode).to_string();
            let queue = model.worker_queue.clone();
            grouped.entry((mode, queue)).or_default().push(model);
        }

        let env_rows = sqlx::query(
            r#"
            SELECT DISTINCT
                CASE
                    WHEN p.mode = 'remote' THEN 'server'
                    ELSE 'local'
                END AS execution_mode,
                e.worker_queue
            FROM environments e
            JOIN projects p ON p.id = e.project_id
            ORDER BY execution_mode ASC, e.worker_queue ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        for row in env_rows {
            let mode = row.get::<String, _>("execution_mode");
            let queue: String = row.get("worker_queue");
            grouped.entry((mode, queue)).or_default();
        }

        let worker_rows = sqlx::query(
            r#"
            SELECT execution_mode, worker_queue
            FROM workers
            ORDER BY execution_mode ASC, worker_queue ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        for row in worker_rows {
            let mode = row.get::<String, _>("execution_mode");
            let queue: String = row.get("worker_queue");
            grouped.entry((mode, queue)).or_default();
        }

        Ok(grouped
            .into_iter()
            .map(|((execution_mode, worker_queue), models)| {
                let pending_count = models.iter().filter(|m| m.claimed_by.is_none()).count() as i64;
                let claimed_count = models.iter().filter(|m| m.claimed_by.is_some()).count() as i64;
                let stale_claim_count = models
                    .iter()
                    .filter(|m| {
                        m.claimed_by.is_some()
                            && matches!(
                                compute_worker_health_from_model(m),
                                InvocationWorkerHealthApi::Stale
                            )
                    })
                    .count() as i64;
                let oldest_pending_at = models
                    .iter()
                    .filter(|m| m.claimed_by.is_none())
                    .map(|m| m.started_at)
                    .min();
                QueueStatusResponse {
                    worker_queue,
                    execution_mode: execution_mode_from_db(&execution_mode),
                    pending_count,
                    claimed_count,
                    stale_claim_count,
                    oldest_pending_at,
                }
            })
            .collect())
    }

    pub(crate) async fn upsert_worker_registration(
        &self,
        worker_id: &str,
        execution_mode: InvocationExecutionModeApi,
        worker_queue: &str,
    ) -> AppResult<()> {
        let execution_mode = match execution_mode {
            InvocationExecutionModeApi::Server => "server",
            InvocationExecutionModeApi::Local => "local",
        };
        sqlx::query(
            r#"
            INSERT INTO workers (worker_id, execution_mode, worker_queue, first_seen_at, last_seen_at)
            VALUES ($1, $2, $3, NOW(), NOW())
            ON CONFLICT (worker_id, worker_queue) DO UPDATE
            SET execution_mode = EXCLUDED.execution_mode,
                last_seen_at = NOW()
            "#,
        )
        .bind(worker_id)
        .bind(execution_mode)
        .bind(worker_queue)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(crate) async fn sync_worker_registrations(
        &self,
        worker_id: &str,
        execution_mode: InvocationExecutionModeApi,
        worker_queues: &[String],
    ) -> AppResult<()> {
        let execution_mode = match execution_mode {
            InvocationExecutionModeApi::Server => "server",
            InvocationExecutionModeApi::Local => "local",
        };
        let mut tx = self.pool.begin().await?;
        sqlx::query(
            r#"
            DELETE FROM workers
            WHERE worker_id = $1
              AND (execution_mode <> $2 OR NOT (worker_queue = ANY($3)))
            "#,
        )
        .bind(worker_id)
        .bind(execution_mode)
        .bind(worker_queues)
        .execute(&mut *tx)
        .await?;
        for worker_queue in worker_queues {
            sqlx::query(
                r#"
                INSERT INTO workers (worker_id, execution_mode, worker_queue, first_seen_at, last_seen_at)
                VALUES ($1, $2, $3, NOW(), NOW())
                ON CONFLICT (worker_id, worker_queue) DO UPDATE
                SET execution_mode = EXCLUDED.execution_mode,
                    last_seen_at = NOW()
                "#,
            )
            .bind(worker_id)
            .bind(execution_mode)
            .bind(worker_queue)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(())
    }

    pub(crate) async fn claim_next_invocation(
        &self,
        worker_id: &str,
        execution_mode: Option<InvocationExecutionModeApi>,
        worker_queues: &[String],
    ) -> AppResult<Option<InvocationClaimResponse>> {
        if let Some(mode) = execution_mode {
            self.sync_worker_registrations(worker_id, mode, worker_queues)
                .await?;
        }
        let mut tx = self.pool.begin().await?;
        let lease_token = Uuid::new_v4();
        let row = sqlx::query(
            r#"
            WITH next_invocation AS (
                SELECT invocation_id
                FROM invocations
                WHERE status = 'running'
                  AND execution_spec IS NOT NULL
                  AND ($1::TEXT IS NULL OR execution_mode = $1)
                  AND worker_queue = ANY($2)
                  AND (claim_deadline_at IS NULL OR claim_deadline_at >= NOW())
                  AND claimed_by IS NULL
                ORDER BY started_at ASC, invocation_id ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
            UPDATE invocations inv
            SET claimed_by = $3,
                lease_token = $4,
                claimed_at = NOW(),
                last_heartbeat_at = NOW()
            FROM next_invocation
            WHERE inv.invocation_id = next_invocation.invocation_id
            RETURNING inv.invocation_id, inv.lease_token, inv.execution_mode, inv.execution_spec
            "#,
        )
        .bind(execution_mode.map(|mode| match mode {
            InvocationExecutionModeApi::Server => "server",
            InvocationExecutionModeApi::Local => "local",
        }))
        .bind(worker_queues)
        .bind(worker_id)
        .bind(lease_token)
        .fetch_optional(&mut *tx)
        .await?;
        tx.commit().await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let execution_spec: sqlx::types::Json<InvocationExecutionSpecApi> =
            row.get("execution_spec");
        Ok(Some(InvocationClaimResponse {
            invocation_id: row.get("invocation_id"),
            worker_id: worker_id.to_string(),
            lease_token: row.get("lease_token"),
            execution_mode: execution_mode_from_db(&row.get::<String, _>("execution_mode")),
            execution_spec: execution_spec.0,
        }))
    }

    pub(crate) async fn heartbeat_invocation(
        &self,
        invocation_id: Uuid,
        worker_id: &str,
        lease_token: Uuid,
    ) -> AppResult<bool> {
        let row = sqlx::query(
            r#"
            UPDATE invocations
            SET last_heartbeat_at = NOW()
            WHERE invocation_id = $1
              AND claimed_by = $2
              AND lease_token = $3
              AND status = 'running'
            RETURNING cancel_requested, execution_mode, worker_queue
            "#,
        )
        .bind(invocation_id)
        .bind(worker_id)
        .bind(lease_token)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(AppError::InvocationOwnershipMismatch);
        };
        let execution_mode = execution_mode_from_db(&row.get::<String, _>("execution_mode"));
        let worker_queue: String = row.get("worker_queue");
        self.upsert_worker_registration(worker_id, execution_mode, &worker_queue)
            .await?;
        Ok(row.get("cancel_requested"))
    }

    pub(crate) async fn request_cancel_invocation(
        &self,
        invocation_id: Uuid,
    ) -> AppResult<Option<InvocationCancellationRecord>> {
        let row = sqlx::query(
            r#"
            UPDATE invocations
            SET cancel_requested = CASE
                    WHEN status = 'running' THEN TRUE
                    ELSE cancel_requested
                END,
                cancel_requested_at = CASE
                    WHEN status = 'running' THEN COALESCE(cancel_requested_at, NOW())
                    ELSE cancel_requested_at
                END,
                status = CASE
                    WHEN status = 'running' AND claimed_by IS NULL THEN 'canceled'
                    ELSE status
                END,
                exit_code = CASE
                    WHEN status = 'running' AND claimed_by IS NULL THEN 130
                    ELSE exit_code
                END,
                error = CASE
                    WHEN status = 'running' AND claimed_by IS NULL THEN 'invocation canceled'
                    ELSE error
                END,
                completed_at = CASE
                    WHEN status = 'running' AND claimed_by IS NULL THEN NOW()
                    ELSE completed_at
                END
            WHERE invocation_id = $1
            RETURNING status, exit_code, error, claimed_by
            "#,
        )
        .bind(invocation_id)
        .fetch_optional(&self.pool)
        .await?;
        let Some(row) = row else {
            return Err(AppError::InvocationNotFound(invocation_id.to_string()));
        };
        let status_str: String = row.get("status");
        if status_str == InvocationLifecycleStatus::Canceled.to_string()
            && row.get::<Option<String>, _>("claimed_by").is_none()
        {
            return Ok(Some(InvocationCancellationRecord {
                invocation_id,
                status: InvocationLifecycleStatus::Canceled,
                exit_code: row.get("exit_code"),
                error: row.get("error"),
            }));
        }
        Ok(None)
    }

    pub(crate) async fn reconcile_timed_out_invocations(
        &self,
        local_heartbeat_timeout: std::time::Duration,
        server_heartbeat_timeout: std::time::Duration,
    ) -> AppResult<Vec<TimedOutInvocationRecord>> {
        let mut tx = self.pool.begin().await?;
        let local_stale_at = Utc::now()
            - chrono::Duration::from_std(local_heartbeat_timeout)
                .unwrap_or_else(|_| chrono::Duration::seconds(15));
        let server_stale_at = Utc::now()
            - chrono::Duration::from_std(server_heartbeat_timeout)
                .unwrap_or_else(|_| chrono::Duration::seconds(60));
        let mut timed_out = Vec::new();

        let unclaimed_rows = sqlx::query(
            r#"
            UPDATE invocations
            SET status = 'failed',
                exit_code = 1,
                error = 'worker did not claim invocation before startup deadline',
                completed_at = NOW(),
                lease_token = NULL
            WHERE status = 'running'
              AND claimed_by IS NULL
              AND claim_deadline_at IS NOT NULL
              AND claim_deadline_at < NOW()
            RETURNING invocation_id, status, exit_code, error
            "#,
        )
        .fetch_all(&mut *tx)
        .await?;
        timed_out.extend(
            unclaimed_rows
                .into_iter()
                .map(timed_out_invocation_from_row),
        );

        let claimed_rows = sqlx::query(
            r#"
            UPDATE invocations
            SET status = 'failed',
                exit_code = 1,
                error = 'worker heartbeat timed out',
                completed_at = NOW(),
                lease_token = NULL
            WHERE status = 'running'
              AND claimed_by IS NOT NULL
              AND (
                (execution_mode = 'local' AND COALESCE(last_heartbeat_at, claimed_at, started_at) < $1)
                OR
                (execution_mode = 'server' AND COALESCE(last_heartbeat_at, claimed_at, started_at) < $2)
              )
            RETURNING invocation_id, status, exit_code, error
            "#,
        )
        .bind(local_stale_at)
        .bind(server_stale_at)
        .fetch_all(&mut *tx)
        .await?;
        timed_out.extend(claimed_rows.into_iter().map(timed_out_invocation_from_row));

        tx.commit().await?;
        Ok(timed_out)
    }

    pub(crate) async fn cleanup_terminal_invocations_older_than(
        &self,
        cutoff: chrono::DateTime<Utc>,
    ) -> AppResult<u64> {
        let result = sqlx::query(
            r#"
            DELETE FROM invocations
            WHERE status IN ('succeeded', 'failed', 'canceled')
              AND completed_at IS NOT NULL
              AND completed_at < $1
            "#,
        )
        .bind(cutoff)
        .execute(&self.pool)
        .await?;
        Ok(result.rows_affected())
    }

    pub(crate) async fn get_invocation_persistence(
        &self,
        invocation_id: Uuid,
        worker_id: Option<&str>,
        lease_token: Option<Uuid>,
    ) -> AppResult<InvocationPersistenceRecord> {
        let row = sqlx::query(
            r#"
            SELECT plan_id, run_id, project_id, environment_id, project_draft_id, environment_draft_id, command, promote_base_manifest, updates_actual_state
            FROM invocations
            WHERE invocation_id = $1
              AND ($2::TEXT IS NULL OR claimed_by = $2)
              AND ($3::UUID IS NULL OR lease_token = $3)
            "#,
        )
        .bind(invocation_id)
        .bind(worker_id)
        .bind(lease_token)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::InvocationNotFound(invocation_id.to_string())
        })?;
        Ok(InvocationPersistenceRecord {
            plan_id: row.get("plan_id"),
            run_id: row.get("run_id"),
            project_id: row.get("project_id"),
            environment_id: row.get("environment_id"),
            project_draft_id: row.get("project_draft_id"),
            environment_draft_id: row.get("environment_draft_id"),
            command: row.get("command"),
            promote_base_manifest: row.get("promote_base_manifest"),
            updates_actual_state: row.get("updates_actual_state"),
        })
    }

    pub(crate) async fn append_invocation_event(
        &self,
        invocation_id: Uuid,
        event: &InvocationEvent,
    ) -> AppResult<u64> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            UPDATE invocations
            SET next_event_sequence = next_event_sequence + 1
            WHERE invocation_id = $1
            RETURNING next_event_sequence
            "#,
        )
        .bind(invocation_id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| {
            AppError::InvocationNotFound(invocation_id.to_string())
        })?;
        let sequence_no: i64 = row.get("next_event_sequence");
        sqlx::query(
            r#"
            INSERT INTO invocation_events (
                invocation_id, sequence_no, occurred_at, event_type, payload
            )
            VALUES ($1, $2, $3, $4, $5)
            "#,
        )
        .bind(invocation_id)
        .bind(sequence_no)
        .bind(event.timestamp)
        .bind(&event.event_type)
        .bind(sqlx::types::Json(event))
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;
        Ok(sequence_no as u64)
    }

    pub(crate) async fn load_invocation_events_since(
        &self,
        invocation_id: Uuid,
        after_sequence: u64,
    ) -> AppResult<Vec<(u64, InvocationEvent)>> {
        let rows = sqlx::query(
            r#"
            SELECT sequence_no, payload
            FROM invocation_events
            WHERE invocation_id = $1
              AND sequence_no > $2
            ORDER BY sequence_no ASC
            "#,
        )
        .bind(invocation_id)
        .bind(after_sequence as i64)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let payload: sqlx::types::Json<InvocationEvent> = row.get("payload");
                (row.get::<i64, _>("sequence_no") as u64, payload.0)
            })
            .collect())
    }

    pub(crate) async fn complete_invocation(
        &self,
        invocation_id: Uuid,
        worker_id: &str,
        lease_token: Uuid,
        completion: &crate::execution::ExecutionCompletion,
    ) -> AppResult<()> {
        let mut tx = self.pool.begin().await?;
        let persistence = self
            .get_invocation_persistence_for_completion_in_tx(
                &mut tx,
                invocation_id,
                worker_id,
                lease_token,
            )
            .await?;

        self.apply_invocation_completion_side_effects_in_tx(
            &mut tx,
            invocation_id,
            &persistence,
            completion,
        )
        .await?;

        let result = sqlx::query(
            r#"
            UPDATE invocations
            SET status = $3,
                exit_code = $4,
                error = $5,
                completed_at = NOW(),
                lease_token = NULL
            WHERE invocation_id = $1
              AND claimed_by = $2
              AND lease_token = $6
              AND status = 'running'
            "#,
        )
        .bind(invocation_id)
        .bind(worker_id)
        .bind(invocation_status_to_db(completion.status))
        .bind(completion.exit_code)
        .bind(completion.error.as_deref())
        .bind(lease_token)
        .execute(&mut *tx)
        .await?;
        if result.rows_affected() != 1 {
            return Err(AppError::InvocationOwnershipMismatch);
        }

        tx.commit().await?;
        Ok(())
    }

    async fn get_invocation_persistence_for_completion_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        invocation_id: Uuid,
        worker_id: &str,
        lease_token: Uuid,
    ) -> AppResult<InvocationPersistenceRecord> {
        let row = sqlx::query(
            r#"
            SELECT plan_id, run_id, project_id, environment_id, project_draft_id, environment_draft_id, command, promote_base_manifest, updates_actual_state
            FROM invocations
            WHERE invocation_id = $1
              AND claimed_by = $2
              AND lease_token = $3
              AND status = 'running'
            FOR UPDATE
            "#,
        )
        .bind(invocation_id)
        .bind(worker_id)
        .bind(lease_token)
        .fetch_optional(&mut **tx)
        .await?;
        if let Some(row) = row {
            return Ok(InvocationPersistenceRecord {
                plan_id: row.get("plan_id"),
                run_id: row.get("run_id"),
                project_id: row.get("project_id"),
                environment_id: row.get("environment_id"),
                project_draft_id: row.get("project_draft_id"),
                environment_draft_id: row.get("environment_draft_id"),
                command: row.get("command"),
                promote_base_manifest: row.get("promote_base_manifest"),
                updates_actual_state: row.get("updates_actual_state"),
            });
        }

        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            SELECT EXISTS(
                SELECT 1
                FROM invocations
                WHERE invocation_id = $1
            )
            "#,
        )
        .bind(invocation_id)
        .fetch_one(&mut **tx)
        .await?;
        if exists {
            Err(AppError::InvocationOwnershipMismatch)
        } else {
            Err(AppError::InvocationNotFound(invocation_id.to_string()))
        }
    }

    pub(crate) async fn force_complete_invocation(
        &self,
        invocation_id: Uuid,
        completion: &crate::execution::ExecutionCompletion,
    ) -> AppResult<Option<(i64, i64)>> {
        let persistence = self.get_invocation_persistence(invocation_id, None, None).await?;
        let mut tx = self.pool.begin().await?;

        self.apply_invocation_completion_side_effects_in_tx(
            &mut tx,
            invocation_id,
            &persistence,
            completion,
        )
        .await?;

        sqlx::query(
            r#"
            UPDATE invocations
            SET status = $2,
                exit_code = $3,
                error = $4,
                completed_at = COALESCE(completed_at, NOW()),
                lease_token = NULL
            WHERE invocation_id = $1
            "#,
        )
        .bind(invocation_id)
        .bind(invocation_status_to_db(completion.status))
        .bind(completion.exit_code)
        .bind(completion.error.as_deref())
        .execute(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(persistence.project_id.zip(persistence.environment_id))
    }

    pub(crate) async fn get_invocation_lineage(
        &self,
        invocation_id: Uuid,
    ) -> AppResult<ModelLineageRecord> {
        let persistence = self.get_invocation_persistence(invocation_id, None, None).await?;
        let resources = self.get_invocation_timeline_resources(invocation_id).await?;
        let unique_ids: Vec<String> = resources.iter().map(|r| r.0.clone()).collect();

        if unique_ids.is_empty() {
            return Ok(ModelLineageRecord { nodes: Vec::new(), edges: Vec::new() });
        }

        let id_set: std::collections::HashSet<&str> = unique_ids.iter().map(|s| s.as_str()).collect();

        let edges = if let Some(run_id) = persistence.run_id {
            self.load_manifest_edges(run_id)
                .await
                .unwrap_or_default()
                .into_iter()
                .filter(|(s, t)| id_set.contains(s.as_str()) && id_set.contains(t.as_str()))
                .collect()
        } else {
            Vec::new()
        };

        let nodes = if let (Some(run_id), Some(project_id), Some(environment_id)) =
            (persistence.run_id, persistence.project_id, persistence.environment_id)
        {
            let rows = sqlx::query(
                r#"
                SELECT
                    mn.unique_id,
                    mn.name,
                    mn.resource_type,
                    mn.package_name,
                    mn.config,
                    cns.status,
                    cns.materialized
                FROM manifest_nodes mn
                LEFT JOIN current_node_state cns
                    ON cns.unique_id = mn.unique_id
                    AND cns.project_id = $2
                    AND cns.environment_id = $3
                WHERE mn.run_id = $1
                  AND mn.unique_id = ANY($4)
                "#,
            )
            .bind(run_id)
            .bind(project_id)
            .bind(environment_id)
            .bind(&unique_ids)
            .fetch_all(&self.pool)
            .await?;

            rows.into_iter()
                .map(|row| {
                    let config: Option<sqlx::types::Json<Value>> = row.get("config");
                    let materialized_from_config = config
                        .as_ref()
                        .and_then(|c| c.get("materialized").and_then(Value::as_str).map(String::from));
                    LineageNodeRecord {
                        unique_id: row.get("unique_id"),
                        name: row.get("name"),
                        resource_type: row.get("resource_type"),
                        package_name: row.get("package_name"),
                        status: row.get("status"),
                        materialized: row.get::<Option<String>, _>("materialized").or(materialized_from_config),
                    }
                })
                .collect()
        } else {
            // No manifest available — build minimal nodes from selected_resources
            resources
                .iter()
                .map(|r| LineageNodeRecord {
                    unique_id: r.0.clone(),
                    name: Some(r.0.rsplit('.').next().unwrap_or(&r.0).to_string()),
                    resource_type: r.1.clone(),
                    package_name: None,
                    status: match (&r.3, r.4.as_deref()) {
                        (Some(_), Some("completed")) => Some("success".to_string()),
                        (Some(_), _) => Some("error".to_string()),
                        _ => None,
                    },
                    materialized: None,
                })
                .collect()
        };

        Ok(ModelLineageRecord { nodes, edges })
    }

    pub(crate) async fn get_invocation_timeline_resources(
        &self,
        invocation_id: Uuid,
    ) -> AppResult<Vec<(String, Option<String>, Option<chrono::DateTime<Utc>>, Option<chrono::DateTime<Utc>>, Option<String>)>> {
        let rows = sqlx::query(
            r#"
            SELECT unique_id, resource_type, node_started_at, finished_at, close_reason
            FROM invocation_selected_resources
            WHERE invocation_id = $1
            "#,
        )
        .bind(invocation_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                (
                    row.get("unique_id"),
                    row.get("resource_type"),
                    row.get("node_started_at"),
                    row.get("finished_at"),
                    row.get("close_reason"),
                )
            })
            .collect())
    }

}
