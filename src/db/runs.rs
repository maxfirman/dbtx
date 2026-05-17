//! Run persistence, event logging, manifest management, node state, and finalization.

use super::*;

impl Db {
    pub(crate) async fn insert_run_started(&self, run: RunStart<'_>) -> AppResult<()> {
        sqlx::query(
            r#"
            INSERT INTO runs (
                run_id, project_id, environment_id, command, args, is_full_graph_run,
                execution_mode, git_branch, git_commit_sha, git_repo_url, project_root, project_name, project_ref
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            "#,
        )
        .bind(run.run_id)
        .bind(run.project.id)
        .bind(run.environment.id)
        .bind(run.subcommand)
        .bind(run.args_json)
        .bind(run.is_full_graph_run)
        .bind(match run.execution_mode {
            ExecutionMode::Server => "server",
            ExecutionMode::Local => "local",
        })
        .bind(run.git_state.branch.as_deref())
        .bind(run.git_state.commit_sha.as_deref())
        .bind(
            run.git_state
                .repo_url
                .as_deref()
                .or(run.project.git_repo_url.as_deref()),
        )
        .bind(run.project.project_root.as_deref())
        .bind(&run.project.project_name)
        .bind(&run.project.project_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn seed_environment_from_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        project: &ProjectRecord,
        target: &EnvironmentRecord,
        source: &EnvironmentRecord,
        seed_type: &str,
    ) -> AppResult<()> {
        sqlx::query(
            "DELETE FROM promoted_manifest_nodes WHERE project_id = $1 AND environment_id = $2",
        )
        .bind(project.id)
        .bind(target.id)
        .execute(&mut **tx)
        .await?;
        sqlx::query(
            "DELETE FROM promoted_manifest_meta WHERE project_id = $1 AND environment_id = $2",
        )
        .bind(project.id)
        .bind(target.id)
        .execute(&mut **tx)
        .await?;
        sqlx::query("DELETE FROM current_node_state WHERE project_id = $1 AND environment_id = $2")
            .bind(project.id)
            .bind(target.id)
            .execute(&mut **tx)
            .await?;

        sqlx::query(
            r#"
            INSERT INTO promoted_manifest_meta (project_id, environment_id, source_run_id, base_manifest, promoted_at)
            SELECT $1, $2, source_run_id, base_manifest, NOW()
            FROM promoted_manifest_meta
            WHERE project_id = $1 AND environment_id = $3
            "#,
        )
        .bind(project.id)
        .bind(target.id)
        .bind(source.id)
        .execute(&mut **tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO promoted_manifest_nodes (
                project_id, environment_id, unique_id, source_run_id, checksum, raw_node, promoted_at
            )
            SELECT $1, $2, unique_id, source_run_id, checksum, raw_node, NOW()
            FROM promoted_manifest_nodes
            WHERE project_id = $1 AND environment_id = $3
            "#,
        )
        .bind(project.id)
        .bind(target.id)
        .bind(source.id)
        .execute(&mut **tx)
        .await?;

        sqlx::query(
            r#"
            INSERT INTO current_node_state (
                project_id, environment_id, unique_id, last_run_id, status, resource_type,
                node_name, node_path, materialized, relation_database, relation_schema,
                relation_alias, relation_name, checksum, started_at, finished_at,
                execution_time_seconds, last_success_at, updated_at
            )
            SELECT
                $1, $2, unique_id, last_run_id, status, resource_type,
                node_name, node_path, materialized, relation_database, relation_schema,
                relation_alias, relation_name, checksum, started_at, finished_at,
                execution_time_seconds, last_success_at, NOW()
            FROM current_node_state
            WHERE project_id = $1 AND environment_id = $3
            "#,
        )
        .bind(project.id)
        .bind(target.id)
        .bind(source.id)
        .execute(&mut **tx)
        .await?;

        let source_run_id: Option<Uuid> = sqlx::query_scalar(
            "SELECT source_run_id FROM promoted_manifest_meta WHERE project_id = $1 AND environment_id = $2",
        )
        .bind(project.id)
        .bind(source.id)
        .fetch_optional(&mut **tx)
        .await?
        .flatten();

        sqlx::query(
            r#"
            INSERT INTO environment_seeds (
                project_id, target_environment_id, source_environment_id, seed_type, source_run_id, metadata
            )
            VALUES ($1, $2, $3, $4, $5, '{}'::jsonb)
            "#,
        )
        .bind(project.id)
        .bind(target.id)
        .bind(source.id)
        .bind(seed_type)
        .bind(source_run_id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    pub(crate) async fn upsert_local_environment(
        &self,
        input: LocalEnvironmentUpsertInput<'_>,
    ) -> AppResult<EnvironmentRecord> {
        let LocalEnvironmentUpsertInput {
            project,
            profile_name,
            target_name,
            adapter_type,
            worker_queue,
            schema_name,
            threads,
            profile_config,
            profile_secrets,
        } = input;
        validate_environment_profile(
            adapter_type,
            schema_name,
            threads,
            profile_config,
            profile_secrets,
            false,
        )?;
        let slug = format!("{profile_name}__{target_name}");
        let row = sqlx::query(
            r#"
            INSERT INTO environments (
                project_id, slug, profile_name, target_name, status, adapter_type,
                worker_queue, schema_name, threads, profile_config, profile_secrets
            )
            VALUES ($1, $2, $3, $4, 'active', $5, $6, $7, $8, $9, $10)
            ON CONFLICT (project_id, slug) DO UPDATE
            SET slug = EXCLUDED.slug,
                profile_name = EXCLUDED.profile_name,
                target_name = EXCLUDED.target_name,
                adapter_type = EXCLUDED.adapter_type,
                worker_queue = EXCLUDED.worker_queue,
                schema_name = EXCLUDED.schema_name,
                threads = EXCLUDED.threads,
                profile_config = EXCLUDED.profile_config,
                profile_secrets = EXCLUDED.profile_secrets
            RETURNING id
            "#,
        )
        .bind(project.id)
        .bind(&slug)
        .bind(profile_name)
        .bind(target_name)
        .bind(adapter_type)
        .bind(worker_queue)
        .bind(schema_name)
        .bind(threads)
        .bind(sqlx::types::Json(profile_config))
        .bind(sqlx::types::Json(profile_secrets))
        .fetch_one(&self.pool)
        .await?;
        let environment_id: i64 = row.get("id");
        self.get_environment_by_id(environment_id).await
    }

    pub(super) async fn finalize_run_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        finalization: RunFinalization<'_>,
    ) -> AppResult<()> {
        self.mark_run_finished_in_tx(
            tx,
            finalization.run_id,
            finalization.dbt_version,
            finalization.exit_code,
            finalization.terminal_status,
        )
        .await?;

        if let Some(manifest) = finalization.manifest {
            self.persist_manifest_in_tx(
                tx,
                finalization.run_id,
                finalization.project_id,
                finalization.environment_id,
                manifest,
            )
            .await?;
            if should_promote_manifest(finalization.subcommand) {
                self.promote_manifest_state_in_tx(
                    tx,
                    finalization.run_id,
                    finalization.project_id,
                    finalization.environment_id,
                    finalization.promote_base_manifest,
                )
                .await?;
            }
        }

        self.rebuild_current_state_up_to_in_tx(
            tx,
            finalization.project_id,
            finalization.environment_id,
            None,
        )
        .await?;
        Ok(())
    }

    pub(super) async fn record_environment_version(
        &self,
        environment: &EnvironmentRecord,
        reason: &str,
    ) -> AppResult<()> {
        let mut tx = self.pool.begin().await?;
        self.record_environment_version_in_tx(&mut tx, environment, reason)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    pub(super) async fn record_environment_version_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        environment: &EnvironmentRecord,
        reason: &str,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            INSERT INTO environment_versions (
                environment_id, project_id, reason, git_branch, git_commit_sha,
                use_latest_commit, auto_reconcile, immutable, baseline_environment_id, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
        )
        .bind(environment.id)
        .bind(environment.project_id)
        .bind(reason)
        .bind(environment.git_branch.as_deref())
        .bind(environment.git_commit_sha.as_deref())
        .bind(environment.use_latest_commit)
        .bind(environment.auto_reconcile)
        .bind(environment.immutable)
        .bind(environment.baseline_environment_id)
        .bind(sqlx::types::Json(serde_json::json!({
            "environment_slug": environment.slug,
            "target_name": environment.target_name,
            "environment_metadata": environment.metadata,
        })))
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    async fn mark_run_finished_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        run_id: Uuid,
        dbt_version: Option<&str>,
        exit_code: i32,
        terminal_status: &str,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            UPDATE runs
            SET dbt_version = COALESCE($2, dbt_version),
                finished_at = NOW(),
                exit_code = $3,
                terminal_status = $4
            WHERE run_id = $1
            "#,
        )
        .bind(run_id)
        .bind(dbt_version)
        .bind(exit_code)
        .bind(terminal_status)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(super) async fn get_environment_by_project_id(
        &self,
        project_id: i64,
        project_ref: &str,
        environment_slug: &str,
    ) -> AppResult<EnvironmentRecord> {
        let query = environment_query("WHERE e.project_id = $1 AND e.slug = $2");
        let row = sqlx::query(&query)
            .bind(project_id)
            .bind(environment_slug)
            .fetch_optional(&self.pool)
            .await?
            .ok_or_else(|| {
                AppError::EnvironmentNotFound(project_ref.to_string(), environment_slug.to_string())
            })?;
        environment_record_from_row(&row)
    }

    pub(crate) async fn get_environment_by_id(
        &self,
        environment_id: i64,
    ) -> AppResult<EnvironmentRecord> {
        let query = environment_query("WHERE e.id = $1");
        let row = sqlx::query(&query)
            .bind(environment_id)
            .fetch_one(&self.pool)
            .await?;
        environment_record_from_row(&row)
    }

    pub(super) async fn get_environment_by_id_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        environment_id: i64,
    ) -> AppResult<EnvironmentRecord> {
        let query = environment_query("WHERE e.id = $1");
        let row = sqlx::query(&query)
            .bind(environment_id)
            .fetch_one(&mut **tx)
            .await?;
        environment_record_from_row(&row)
    }

    pub(crate) async fn persist_log_event(
        &self,
        invocation_id: Option<Uuid>,
        run_id: Uuid,
        project_id: i64,
        environment_id: i64,
        sequence_no: i64,
        event: &LogEvent,
    ) -> AppResult<()> {
        let unique_id = event
            .normalized_node_event()
            .as_ref()
            .map(|node| node.unique_id.clone());

        sqlx::query(
            r#"
            INSERT INTO run_events (run_id, sequence_no, event_name, event_code, unique_id, payload)
            VALUES ($1, $2, $3, $4, $5, $6)
            "#,
        )
        .bind(run_id)
        .bind(sequence_no)
        .bind(null_if_empty(&event.info.name))
        .bind(null_if_empty(&event.info.code))
        .bind(unique_id.clone())
        .bind(sqlx::types::Json(&event))
        .execute(&self.pool)
        .await?;

        if let Some(invocation_id) = invocation_id
            && let Some(selected_resources) = event.selected_resources()
        {
            self.insert_invocation_selected_resources(
                invocation_id,
                run_id,
                project_id,
                environment_id,
                &selected_resources,
            )
            .await?;
        }

        if let Some(node) = event.normalized_node_event() {
            if let Some(invocation_id) = invocation_id {
                self.update_invocation_selected_resource_progress(invocation_id, &node)
                    .await?;
            }
            let promotable = node.status.as_deref().is_some_and(is_promotable_status);
            let promoted = |field: &Option<String>| -> Option<String> {
                if promotable { field.clone() } else { None }
            };

            sqlx::query(
                r#"
                INSERT INTO node_executions (
                    run_id, unique_id, resource_type, node_name, node_path, materialized, status,
                    relation_database, relation_schema, relation_alias, relation_name, checksum,
                    started_at, finished_at, execution_time_seconds, updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6, $7,
                    $8, $9, $10, $11, $12,
                    $13, $14, $15, NOW()
                )
                ON CONFLICT (run_id, unique_id) DO UPDATE SET
                    resource_type = COALESCE(EXCLUDED.resource_type, node_executions.resource_type),
                    node_name = COALESCE(EXCLUDED.node_name, node_executions.node_name),
                    node_path = COALESCE(EXCLUDED.node_path, node_executions.node_path),
                    materialized = COALESCE(EXCLUDED.materialized, node_executions.materialized),
                    status = COALESCE(EXCLUDED.status, node_executions.status),
                    relation_database = COALESCE(EXCLUDED.relation_database, node_executions.relation_database),
                    relation_schema = COALESCE(EXCLUDED.relation_schema, node_executions.relation_schema),
                    relation_alias = COALESCE(EXCLUDED.relation_alias, node_executions.relation_alias),
                    relation_name = COALESCE(EXCLUDED.relation_name, node_executions.relation_name),
                    checksum = COALESCE(EXCLUDED.checksum, node_executions.checksum),
                    started_at = COALESCE(EXCLUDED.started_at, node_executions.started_at),
                    finished_at = COALESCE(EXCLUDED.finished_at, node_executions.finished_at),
                    execution_time_seconds = COALESCE(EXCLUDED.execution_time_seconds, node_executions.execution_time_seconds),
                    updated_at = NOW()
                "#,
            )
            .bind(run_id)
            .bind(&node.unique_id)
            .bind(&node.resource_type)
            .bind(&node.node_name)
            .bind(&node.node_path)
            .bind(&node.materialized)
            .bind(&node.status)
            .bind(&node.relation_database)
            .bind(&node.relation_schema)
            .bind(&node.relation_alias)
            .bind(&node.relation_name)
            .bind(&node.node_checksum)
            .bind(node.started_at)
            .bind(node.finished_at)
            .bind(node.execution_time_seconds)
            .execute(&self.pool)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO current_node_state (
                    project_id, environment_id, unique_id, last_run_id, status, resource_type,
                    node_name, node_path, materialized, relation_database, relation_schema,
                    relation_alias, relation_name, checksum, started_at, finished_at,
                    execution_time_seconds, last_success_at, updated_at
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9, $10, $11,
                    $12, $13, $14, $15, $16,
                    $17, $18, NOW()
                )
                ON CONFLICT (project_id, environment_id, unique_id) DO UPDATE SET
                    last_run_id = EXCLUDED.last_run_id,
                    status = COALESCE(EXCLUDED.status, current_node_state.status),
                    resource_type = COALESCE(EXCLUDED.resource_type, current_node_state.resource_type),
                    node_name = COALESCE(EXCLUDED.node_name, current_node_state.node_name),
                    node_path = COALESCE(EXCLUDED.node_path, current_node_state.node_path),
                    materialized = COALESCE(EXCLUDED.materialized, current_node_state.materialized),
                    relation_database = COALESCE(EXCLUDED.relation_database, current_node_state.relation_database),
                    relation_schema = COALESCE(EXCLUDED.relation_schema, current_node_state.relation_schema),
                    relation_alias = COALESCE(EXCLUDED.relation_alias, current_node_state.relation_alias),
                    relation_name = COALESCE(EXCLUDED.relation_name, current_node_state.relation_name),
                    checksum = COALESCE(EXCLUDED.checksum, current_node_state.checksum),
                    started_at = COALESCE(EXCLUDED.started_at, current_node_state.started_at),
                    finished_at = COALESCE(EXCLUDED.finished_at, current_node_state.finished_at),
                    execution_time_seconds = COALESCE(EXCLUDED.execution_time_seconds, current_node_state.execution_time_seconds),
                    last_success_at = COALESCE(EXCLUDED.last_success_at, current_node_state.last_success_at),
                    updated_at = NOW()
                "#,
            )
            .bind(project_id)
            .bind(environment_id)
            .bind(&node.unique_id)
            .bind(run_id)
            .bind(&node.status)
            .bind(&node.resource_type)
            .bind(&node.node_name)
            .bind(&node.node_path)
            .bind(promoted(&node.materialized))
            .bind(promoted(&node.relation_database))
            .bind(promoted(&node.relation_schema))
            .bind(promoted(&node.relation_alias))
            .bind(promoted(&node.relation_name))
            .bind(promoted(&node.node_checksum))
            .bind(node.started_at)
            .bind(node.finished_at)
            .bind(node.execution_time_seconds)
            .bind(if promotable { node.finished_at } else { None })
            .execute(&self.pool)
            .await?;

            let watermark_manifest_run_id = match invocation_id {
                Some(invocation_id) => {
                    self.load_invocation_watermark_manifest_run_id(invocation_id)
                        .await?
                }
                None => None,
            };

            // Watermark tracking: compute candidates on node start, commit on success
            self.handle_node_watermark(
                invocation_id,
                run_id,
                project_id,
                environment_id,
                watermark_manifest_run_id,
                &node,
                promotable,
            )
            .await?;
        }

        Ok(())
    }

    /// Handle watermark computation for a node execution event.
    /// On node start: compute candidate watermarks from parents and store in staging table.
    /// On node finish (success): commit candidates to the primary watermark table.
    /// On node finish (failure): discard candidates.
    #[allow(clippy::too_many_arguments)]
    async fn handle_node_watermark(
        &self,
        invocation_id: Option<Uuid>,
        run_id: Uuid,
        project_id: i64,
        environment_id: i64,
        watermark_manifest_run_id: Option<Uuid>,
        node: &crate::event::NormalizedNodeEvent,
        promotable: bool,
    ) -> AppResult<()> {
        let is_start = node.started_at.is_some() && node.finished_at.is_none();
        let is_finish = node.finished_at.is_some();

        if is_start {
            let Some(manifest_run_id) = watermark_manifest_run_id else {
                return Ok(());
            };

            // Check if this node has any tracked ancestor sources
            let ancestor_sources = self
                .load_node_ancestor_sources(manifest_run_id, &node.unique_id)
                .await?;
            if ancestor_sources.is_empty() {
                return Ok(());
            }

            // Determine if this node is itself a source node
            let is_source_node = node
                .resource_type
                .as_deref()
                .is_some_and(|rt| rt == "source");

            let candidates = if is_source_node {
                // Source node: watermark is the latest event for this source
                match self
                    .load_latest_source_event_id(project_id, environment_id, &node.unique_id)
                    .await?
                {
                    Some(candidate) => vec![candidate],
                    None => Vec::new(),
                }
            } else {
                // Non-source node: watermark = MIN(parent watermarks) per source
                let parents = self
                    .load_node_parents(manifest_run_id, &node.unique_id)
                    .await?;
                if parents.is_empty() {
                    return Ok(());
                }
                self.load_parent_watermarks_min(project_id, environment_id, &parents)
                    .await?
            };

            if !candidates.is_empty() {
                self.insert_watermark_candidates(run_id, &node.unique_id, &candidates)
                    .await?;
            }
        } else if is_finish {
            if promotable {
                // Node succeeded: commit candidates to primary watermark table
                let candidates = self
                    .load_watermark_candidates(run_id, &node.unique_id)
                    .await?;
                if !candidates.is_empty() {
                    let advanced = self
                        .commit_node_watermarks(
                            project_id,
                            environment_id,
                            &node.unique_id,
                            run_id,
                            invocation_id,
                            &candidates,
                        )
                        .await?;
                    let advanced_source_keys = advanced
                        .into_iter()
                        .map(|candidate| candidate.source_key)
                        .collect::<Vec<_>>();
                    if let Some(manifest_run_id) = watermark_manifest_run_id {
                        self.advance_satisfied_source_events_from_watermarks(
                            project_id,
                            environment_id,
                            &advanced_source_keys,
                            manifest_run_id,
                        )
                        .await?;
                    }
                }
            }
            // Always clean up candidates (success or failure)
            self.delete_watermark_candidates(run_id, &node.unique_id)
                .await?;
        }

        Ok(())
    }

    async fn insert_invocation_selected_resources(
        &self,
        invocation_id: Uuid,
        run_id: Uuid,
        project_id: i64,
        environment_id: i64,
        selected_resources: &[String],
    ) -> AppResult<()> {
        if selected_resources.is_empty() {
            return Ok(());
        }

        sqlx::query(
            r#"
            INSERT INTO invocation_selected_resources (
                invocation_id,
                run_id,
                project_id,
                environment_id,
                unique_id,
                resource_type,
                selected_at,
                created_at,
                updated_at
            )
            SELECT
                $1,
                $2,
                $3,
                $4,
                unique_id,
                NULLIF(split_part(unique_id, '.', 1), ''),
                NOW(),
                NOW(),
                NOW()
            FROM unnest($5::text[]) AS unique_id
            ON CONFLICT (invocation_id, unique_id) DO UPDATE
            SET resource_type = COALESCE(
                    invocation_selected_resources.resource_type,
                    EXCLUDED.resource_type
                ),
                updated_at = NOW()
            "#,
        )
        .bind(invocation_id)
        .bind(run_id)
        .bind(project_id)
        .bind(environment_id)
        .bind(selected_resources)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    async fn update_invocation_selected_resource_progress(
        &self,
        invocation_id: Uuid,
        node: &crate::event::NormalizedNodeEvent,
    ) -> AppResult<()> {
        let close_reason = node.finished_at.map(|_| "completed");
        sqlx::query(
            r#"
            UPDATE invocation_selected_resources
            SET resource_type = COALESCE($3, resource_type),
                node_started_at = COALESCE($4, node_started_at),
                finished_at = COALESCE($5, finished_at),
                close_reason = COALESCE($6, close_reason),
                updated_at = NOW()
            WHERE invocation_id = $1
              AND unique_id = $2
              AND finished_at IS NULL
            "#,
        )
        .bind(invocation_id)
        .bind(&node.unique_id)
        .bind(node.resource_type.clone())
        .bind(node.started_at)
        .bind(node.finished_at)
        .bind(close_reason)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub(super) async fn close_invocation_selected_resources_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        invocation_id: Uuid,
        close_reason: &str,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            UPDATE invocation_selected_resources
            SET finished_at = COALESCE(finished_at, NOW()),
                close_reason = COALESCE(close_reason, $2),
                updated_at = NOW()
            WHERE invocation_id = $1
              AND finished_at IS NULL
            "#,
        )
        .bind(invocation_id)
        .bind(close_reason)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(super) async fn upsert_environment_actual_state_for_run_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        run_id: Uuid,
        project_id: i64,
        environment_id: i64,
        succeeded: bool,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            INSERT INTO environment_actual_state (
                project_id,
                environment_id,
                last_attempted_run_id,
                last_attempted_commit_sha,
                last_attempted_at,
                last_successful_run_id,
                last_successful_commit_sha,
                last_successful_at,
                updated_at
            )
            SELECT
                $2,
                $3,
                r.run_id,
                r.git_commit_sha,
                NOW(),
                CASE WHEN $4 THEN r.run_id ELSE NULL END,
                CASE WHEN $4 THEN r.git_commit_sha ELSE NULL END,
                CASE WHEN $4 THEN NOW() ELSE NULL END,
                NOW()
            FROM runs r
            WHERE r.run_id = $1
            ON CONFLICT (project_id, environment_id) DO UPDATE SET
                last_attempted_run_id = EXCLUDED.last_attempted_run_id,
                last_attempted_commit_sha = EXCLUDED.last_attempted_commit_sha,
                last_attempted_at = EXCLUDED.last_attempted_at,
                last_successful_run_id = COALESCE(EXCLUDED.last_successful_run_id, environment_actual_state.last_successful_run_id),
                last_successful_commit_sha = COALESCE(EXCLUDED.last_successful_commit_sha, environment_actual_state.last_successful_commit_sha),
                last_successful_at = COALESCE(EXCLUDED.last_successful_at, environment_actual_state.last_successful_at),
                updated_at = NOW()
            "#,
        )
        .bind(run_id)
        .bind(project_id)
        .bind(environment_id)
        .bind(succeeded)
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(super) async fn complete_environment_run_plan_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        plan_id: Uuid,
        invocation_status: InvocationLifecycleStatus,
        invocation_error: Option<&str>,
    ) -> AppResult<()> {
        let status = match invocation_status {
            InvocationLifecycleStatus::Succeeded => "completed",
            InvocationLifecycleStatus::Failed => "failed",
            InvocationLifecycleStatus::Canceled => "canceled",
            InvocationLifecycleStatus::Running => "failed",
        };
        let existing_failure_count: i32 = sqlx::query_scalar(
            r#"
            SELECT failure_count
            FROM environment_run_plans
            WHERE plan_id = $1
            "#,
        )
        .bind(plan_id)
        .fetch_optional(&mut **tx)
        .await?
        .unwrap_or(0);
        let next_failure_count = if status == "completed" {
            0
        } else {
            existing_failure_count + 1
        };
        let next_attempt_at = if status == "completed" {
            None
        } else {
            Some(Utc::now() + automatic_retry_backoff(next_failure_count))
        };
        sqlx::query(
            r#"
            UPDATE environment_run_plans
            SET status = $2,
                error = CASE WHEN $2 = 'completed' THEN NULL ELSE COALESCE($3, error) END,
                failure_count = $4,
                next_attempt_at = $5,
                completed_at = NOW(),
                updated_at = NOW()
            WHERE plan_id = $1
            "#,
        )
        .bind(plan_id)
        .bind(status)
        .bind(invocation_error)
        .bind(next_failure_count)
        .bind(next_attempt_at)
        .execute(&mut **tx)
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
        .execute(&mut **tx)
        .await?;
        Ok(())
    }

    pub(crate) async fn persist_raw_line(
        &self,
        run_id: Uuid,
        sequence_no: i64,
        line: &str,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            INSERT INTO run_events (run_id, sequence_no, event_name, event_code, unique_id, payload)
            VALUES ($1, $2, 'RawLine', NULL, NULL, $3)
            "#,
        )
        .bind(run_id)
        .bind(sequence_no)
        .bind(sqlx::types::Json(serde_json::json!({ "raw_line": line })))
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn persist_manifest_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        run_id: Uuid,
        project_id: i64,
        environment_id: i64,
        manifest: &ManifestSnapshot,
    ) -> AppResult<()> {
        let manifest_raw = serde_json::to_vec(&manifest.raw)?;
        let checksum = format!("{:x}", md5::compute(&manifest_raw));
        sqlx::query(
            r#"
            INSERT INTO manifest_snapshots (run_id, manifest, manifest_size_bytes, checksum)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (run_id) DO UPDATE SET
                manifest = EXCLUDED.manifest,
                manifest_size_bytes = EXCLUDED.manifest_size_bytes,
                checksum = EXCLUDED.checksum
            "#,
        )
        .bind(run_id)
        .bind(sqlx::types::Json(&manifest.raw))
        .bind(manifest_raw.len() as i64)
        .bind(checksum)
        .execute(&mut **tx)
        .await?;

        sqlx::query("DELETE FROM manifest_nodes WHERE run_id = $1")
            .bind(run_id)
            .execute(&mut **tx)
            .await?;
        sqlx::query("DELETE FROM manifest_edges WHERE run_id = $1")
            .bind(run_id)
            .execute(&mut **tx)
            .await?;

        for node in &manifest.nodes {
            sqlx::query(
                r#"
                INSERT INTO manifest_nodes (
                    run_id, unique_id, resource_type, name, package_name, original_file_path,
                    tags, fqn, config, checksum, database_name, schema_name, alias, relation_name
                )
                VALUES (
                    $1, $2, $3, $4, $5, $6,
                    $7, $8, $9, $10, $11, $12, $13, $14
                )
                "#,
            )
            .bind(run_id)
            .bind(&node.unique_id)
            .bind(&node.resource_type)
            .bind(&node.name)
            .bind(&node.package_name)
            .bind(&node.original_file_path)
            .bind(sqlx::types::Json(&node.tags))
            .bind(sqlx::types::Json(&node.fqn))
            .bind(sqlx::types::Json(&node.config))
            .bind(&node.checksum)
            .bind(&node.database_name)
            .bind(&node.schema_name)
            .bind(&node.alias)
            .bind(&node.relation_name)
            .execute(&mut **tx)
            .await?;
        }

        for edge in &manifest.edges {
            sqlx::query(
                r#"
                INSERT INTO manifest_edges (run_id, parent_unique_id, child_unique_id)
                VALUES ($1, $2, $3)
                "#,
            )
            .bind(run_id)
            .bind(&edge.parent_unique_id)
            .bind(&edge.child_unique_id)
            .execute(&mut **tx)
            .await?;
        }

        self.populate_node_ancestor_sources_in_tx(tx, run_id, project_id, environment_id)
            .await?;

        Ok(())
    }

    async fn promote_manifest_state_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        run_id: Uuid,
        project_id: i64,
        environment_id: i64,
        promote_base_manifest: bool,
    ) -> AppResult<()> {
        if promote_base_manifest {
            sqlx::query(
                r#"
                INSERT INTO promoted_manifest_meta (project_id, environment_id, source_run_id, base_manifest)
                SELECT $2, $3, $1, manifest
                FROM manifest_snapshots
                WHERE run_id = $1
                ON CONFLICT (project_id, environment_id) DO UPDATE SET
                    source_run_id = EXCLUDED.source_run_id,
                    base_manifest = EXCLUDED.base_manifest,
                    promoted_at = NOW()
                "#,
            )
            .bind(run_id)
            .bind(project_id)
            .bind(environment_id)
            .execute(&mut **tx)
            .await?;
        }

        sqlx::query(&format!(
            r#"
            INSERT INTO promoted_manifest_nodes (
                project_id, environment_id, unique_id, source_run_id, checksum, raw_node
            )
            SELECT
                $2,
                $3,
                ne.unique_id,
                ne.run_id,
                ne.checksum,
                ms.manifest -> 'nodes' -> ne.unique_id
            FROM node_executions ne
            JOIN manifest_snapshots ms ON ms.run_id = ne.run_id
            WHERE ne.run_id = $1
              AND ne.status IN ({})
              AND ms.manifest -> 'nodes' -> ne.unique_id IS NOT NULL
            ON CONFLICT (project_id, environment_id, unique_id) DO UPDATE SET
                source_run_id = EXCLUDED.source_run_id,
                checksum = EXCLUDED.checksum,
                raw_node = EXCLUDED.raw_node,
                promoted_at = NOW()
            "#,
            NodeExecutionStatus::PROMOTABLE_SQL
        ))
        .bind(run_id)
        .bind(project_id)
        .bind(environment_id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    async fn rebuild_current_state_up_to_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        project_id: i64,
        environment_id: i64,
        max_run_pk: Option<i64>,
    ) -> AppResult<u64> {
        // Use a sentinel timestamp to identify rows touched by this rebuild.
        // After upserting, any row with updated_at < rebuild_marker is stale and
        // gets cleaned up — avoiding the empty-table window that a DELETE-first
        // approach would expose to concurrent readers under READ COMMITTED.
        let rebuild_marker: chrono::DateTime<Utc> = sqlx::query_scalar("SELECT NOW()")
            .fetch_one(&mut **tx)
            .await?;

        // Upsert from node_executions: latest execution for status/timing,
        // latest successful execution for promoted fields (relation, checksum).
        let upserted = sqlx::query(&format!(
            r#"
            WITH latest_execution AS (
                SELECT DISTINCT ON (ne.unique_id)
                    r.project_id,
                    r.environment_id,
                    ne.unique_id,
                    ne.run_id,
                    ne.status,
                    ne.resource_type,
                    ne.node_name,
                    ne.node_path,
                    ne.started_at,
                    ne.finished_at,
                    ne.execution_time_seconds
                FROM node_executions ne
                JOIN runs r ON r.run_id = ne.run_id
                WHERE r.project_id = $1
                  AND r.environment_id = $2
                  AND ($3::BIGINT IS NULL OR r.id <= $3)
                ORDER BY ne.unique_id, r.id DESC
            ),
            latest_success AS (
                SELECT DISTINCT ON (ne.unique_id)
                    ne.unique_id,
                    ne.materialized,
                    ne.relation_database,
                    ne.relation_schema,
                    ne.relation_alias,
                    ne.relation_name,
                    ne.checksum,
                    ne.finished_at
                FROM node_executions ne
                JOIN runs r ON r.run_id = ne.run_id
                WHERE r.project_id = $1
                  AND r.environment_id = $2
                  AND ($3::BIGINT IS NULL OR r.id <= $3)
                  AND ne.status IN ({})
                ORDER BY ne.unique_id, r.id DESC
            )
            INSERT INTO current_node_state (
                project_id, environment_id, unique_id, last_run_id, status, resource_type,
                node_name, node_path, materialized, relation_database, relation_schema,
                relation_alias, relation_name, checksum, started_at, finished_at,
                execution_time_seconds, last_success_at, updated_at
            )
            SELECT
                le.project_id,
                le.environment_id,
                le.unique_id,
                le.run_id,
                le.status,
                le.resource_type,
                le.node_name,
                le.node_path,
                ls.materialized,
                ls.relation_database,
                ls.relation_schema,
                ls.relation_alias,
                ls.relation_name,
                ls.checksum,
                le.started_at,
                le.finished_at,
                le.execution_time_seconds,
                ls.finished_at,
                $4
            FROM latest_execution le
            LEFT JOIN latest_success ls ON ls.unique_id = le.unique_id
            ON CONFLICT (project_id, environment_id, unique_id) DO UPDATE SET
                last_run_id = EXCLUDED.last_run_id,
                status = EXCLUDED.status,
                resource_type = EXCLUDED.resource_type,
                node_name = EXCLUDED.node_name,
                node_path = EXCLUDED.node_path,
                materialized = EXCLUDED.materialized,
                relation_database = EXCLUDED.relation_database,
                relation_schema = EXCLUDED.relation_schema,
                relation_alias = EXCLUDED.relation_alias,
                relation_name = EXCLUDED.relation_name,
                checksum = EXCLUDED.checksum,
                started_at = EXCLUDED.started_at,
                finished_at = EXCLUDED.finished_at,
                execution_time_seconds = EXCLUDED.execution_time_seconds,
                last_success_at = EXCLUDED.last_success_at,
                updated_at = EXCLUDED.updated_at
            "#,
            NodeExecutionStatus::PROMOTABLE_SQL
        ))
        .bind(project_id)
        .bind(environment_id)
        .bind(max_run_pk)
        .bind(rebuild_marker)
        .execute(&mut **tx)
        .await?;

        // Backfill current_node_state from manifest_nodes for resources that were never
        // executed (e.g. sources, macros). This ensures all manifest resources appear in
        // the catalog, not just those with node_executions.
        sqlx::query(
            r#"
            WITH latest_manifest_run AS (
                SELECT r.run_id
                FROM runs r
                JOIN manifest_snapshots ms ON ms.run_id = r.run_id
                WHERE r.project_id = $1
                  AND r.environment_id = $2
                  AND ($3::BIGINT IS NULL OR r.id <= $3)
                ORDER BY r.id DESC
                LIMIT 1
            )
            INSERT INTO current_node_state (
                project_id, environment_id, unique_id, last_run_id,
                resource_type, node_name, node_path,
                relation_database, relation_schema, relation_alias, relation_name,
                checksum, updated_at
            )
            SELECT
                $1, $2, mn.unique_id, mn.run_id,
                mn.resource_type, mn.name, mn.original_file_path,
                mn.database_name, mn.schema_name, mn.alias, mn.relation_name,
                mn.checksum, $4
            FROM manifest_nodes mn
            JOIN latest_manifest_run lmr ON mn.run_id = lmr.run_id
            ON CONFLICT (project_id, environment_id, unique_id) DO UPDATE SET
                updated_at = EXCLUDED.updated_at
                WHERE current_node_state.status IS NULL
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(max_run_pk)
        .bind(rebuild_marker)
        .execute(&mut **tx)
        .await?;

        // Remove stale rows not touched by either upsert above. These are nodes
        // from previous runs that no longer appear in the execution history or
        // the current manifest.
        sqlx::query(
            r#"
            DELETE FROM current_node_state
            WHERE project_id = $1
              AND environment_id = $2
              AND updated_at < $3
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(rebuild_marker)
        .execute(&mut **tx)
        .await?;

        Ok(upserted.rows_affected())
    }

    pub(crate) async fn load_reconstructed_manifest(
        &self,
        project_id: i64,
        environment_id: i64,
    ) -> AppResult<Option<ReconstructedManifest>> {
        let base_row = sqlx::query(
            r#"
            SELECT
                pmm.project_id,
                pmm.environment_id,
                pmm.base_manifest
            FROM promoted_manifest_meta pmm
            WHERE pmm.project_id = $1
              AND pmm.environment_id = $2
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(base_row) = base_row else {
            return Ok(None);
        };

        let project_id: i64 = base_row.get("project_id");
        let environment_id: i64 = base_row.get("environment_id");
        let base_manifest: sqlx::types::Json<Value> = base_row.get("base_manifest");

        let promoted_nodes = sqlx::query(
            r#"
            SELECT
                unique_id,
                raw_node
            FROM promoted_manifest_nodes
            WHERE project_id = $1
              AND environment_id = $2
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .map(|row| {
            let unique_id: String = row.get("unique_id");
            let raw_node: sqlx::types::Json<Value> = row.get("raw_node");
            (unique_id, raw_node.0)
        })
        .collect::<BTreeMap<_, _>>();

        let reconstructed = ManifestSnapshot::reconstruct(base_manifest.0, &promoted_nodes);
        Ok(Some(ReconstructedManifest::write(&reconstructed).await?))
    }
}

impl Db {
    pub(super) async fn migration_versions(&self) -> AppResult<BTreeSet<i64>> {
        Ok(self
            .migration_rows()
            .await?
            .into_iter()
            .map(|migration| migration.version)
            .collect())
    }

    pub(super) async fn migration_rows(&self) -> AppResult<Vec<AppliedMigration>> {
        let rows =
            sqlx::query("SELECT version, description FROM _sqlx_migrations ORDER BY version")
                .fetch_all(&self.pool)
                .await;
        match rows {
            Ok(rows) => Ok(rows
                .into_iter()
                .map(|row| AppliedMigration {
                    version: row.get("version"),
                    description: row.get("description"),
                })
                .collect()),
            Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("42P01") => {
                Ok(Vec::new())
            }
            Err(err) => Err(AppError::Sqlx(err)),
        }
    }
}
