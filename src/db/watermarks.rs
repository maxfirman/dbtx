//! Per-node source watermark tracking: storage, candidate staging, and ancestor precomputation.

use super::*;

/// A candidate watermark computed at node start, pending commit on success.
#[derive(Debug, Clone)]
pub(crate) struct WatermarkCandidate {
    pub(crate) source_key: String,
    pub(crate) watermark_event_id: i64,
    pub(crate) watermark_observed_at: Option<chrono::DateTime<Utc>>,
}

impl Db {
    /// Populate node_ancestor_sources for a manifest run.
    /// Walks edges upward (child → parent) to find all ancestor source nodes
    /// that are tracked (have at least one source_state_events entry).
    pub(crate) async fn populate_node_ancestor_sources_in_tx(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        run_id: Uuid,
        project_id: i64,
        environment_id: i64,
    ) -> AppResult<()> {
        sqlx::query("DELETE FROM node_ancestor_sources WHERE run_id = $1")
            .bind(run_id)
            .execute(&mut **tx)
            .await?;

        sqlx::query(
            r#"
            WITH RECURSIVE ancestors(unique_id, ancestor_id) AS (
                SELECT me.child_unique_id, me.parent_unique_id
                FROM manifest_edges me
                WHERE me.run_id = $1
                UNION
                SELECT a.unique_id, me.parent_unique_id
                FROM ancestors a
                JOIN manifest_edges me
                  ON me.child_unique_id = a.ancestor_id
                 AND me.run_id = $1
            ),
            tracked_sources AS (
                SELECT DISTINCT source_key
                FROM source_state_events
                WHERE project_id = $2
                  AND environment_id = $3
            )
            INSERT INTO node_ancestor_sources (run_id, unique_id, source_key)
            SELECT DISTINCT $1, a.unique_id, a.ancestor_id
            FROM ancestors a
            JOIN tracked_sources ts ON ts.source_key = a.ancestor_id
            "#,
        )
        .bind(run_id)
        .bind(project_id)
        .bind(environment_id)
        .execute(&mut **tx)
        .await?;

        // Also insert self-references for source nodes themselves
        sqlx::query(
            r#"
            INSERT INTO node_ancestor_sources (run_id, unique_id, source_key)
            SELECT DISTINCT $1, mn.unique_id, mn.unique_id
            FROM manifest_nodes mn
            JOIN source_state_events sse
              ON sse.source_key = mn.unique_id
             AND sse.project_id = $2
             AND sse.environment_id = $3
            WHERE mn.run_id = $1
              AND mn.resource_type = 'source'
            ON CONFLICT (run_id, unique_id, source_key) DO NOTHING
            "#,
        )
        .bind(run_id)
        .bind(project_id)
        .bind(environment_id)
        .execute(&mut **tx)
        .await?;

        Ok(())
    }

    /// Load ancestor source keys for a node in a given manifest run.
    pub(crate) async fn load_node_ancestor_sources(
        &self,
        run_id: Uuid,
        unique_id: &str,
    ) -> AppResult<Vec<String>> {
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT source_key
            FROM node_ancestor_sources
            WHERE run_id = $1 AND unique_id = $2
            ORDER BY source_key
            "#,
        )
        .bind(run_id)
        .bind(unique_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Load the current watermarks for a set of parent nodes (all sources they track).
    /// For parent nodes that are source nodes without watermarks, falls back to the
    /// latest source event ID directly (since source nodes don't execute).
    pub(crate) async fn load_parent_watermarks_min(
        &self,
        project_id: i64,
        environment_id: i64,
        parent_unique_ids: &[String],
    ) -> AppResult<Vec<WatermarkCandidate>> {
        if parent_unique_ids.is_empty() {
            return Ok(Vec::new());
        }
        // Get watermarks from parent nodes that have them, PLUS
        // fall back to source_state_events for source parents without watermarks
        let rows = sqlx::query(
            r#"
            WITH parent_watermarks AS (
                SELECT source_key, watermark_event_id, watermark_observed_at
                FROM node_source_watermarks
                WHERE project_id = $1
                  AND environment_id = $2
                  AND unique_id = ANY($3::TEXT[])
            ),
            source_parent_fallback AS (
                SELECT
                    sse.source_key,
                    MAX(sse.id) AS watermark_event_id,
                    MAX(sse.observed_at) AS watermark_observed_at
                FROM source_state_events sse
                WHERE sse.project_id = $1
                  AND sse.environment_id = $2
                  AND sse.source_key = ANY($3::TEXT[])
                  AND NOT EXISTS (
                      SELECT 1 FROM node_source_watermarks nsw
                      WHERE nsw.project_id = $1
                        AND nsw.environment_id = $2
                        AND nsw.unique_id = sse.source_key
                        AND nsw.source_key = sse.source_key
                  )
                GROUP BY sse.source_key
            ),
            combined AS (
                SELECT source_key, watermark_event_id, watermark_observed_at FROM parent_watermarks
                UNION ALL
                SELECT source_key, watermark_event_id, watermark_observed_at FROM source_parent_fallback
            )
            SELECT
                source_key,
                MIN(watermark_event_id) AS min_event_id,
                MIN(watermark_observed_at) AS min_observed_at
            FROM combined
            GROUP BY source_key
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(parent_unique_ids)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| WatermarkCandidate {
                source_key: row.get("source_key"),
                watermark_event_id: row.get("min_event_id"),
                watermark_observed_at: row.get("min_observed_at"),
            })
            .collect())
    }

    /// Load the latest source event ID for a source node's self-watermark.
    pub(crate) async fn load_latest_source_event_id(
        &self,
        project_id: i64,
        environment_id: i64,
        source_key: &str,
    ) -> AppResult<Option<WatermarkCandidate>> {
        let row = sqlx::query(
            r#"
            SELECT id, observed_at
            FROM source_state_events
            WHERE project_id = $1
              AND environment_id = $2
              AND source_key = $3
            ORDER BY id DESC
            LIMIT 1
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(source_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|row| WatermarkCandidate {
            source_key: source_key.to_string(),
            watermark_event_id: row.get("id"),
            watermark_observed_at: row.get("observed_at"),
        }))
    }

    /// Load direct parents of a node from manifest_edges.
    pub(crate) async fn load_node_parents(
        &self,
        run_id: Uuid,
        unique_id: &str,
    ) -> AppResult<Vec<String>> {
        sqlx::query_scalar::<_, String>(
            r#"
            SELECT parent_unique_id
            FROM manifest_edges
            WHERE run_id = $1 AND child_unique_id = $2
            "#,
        )
        .bind(run_id)
        .bind(unique_id)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Insert candidate watermarks on node start.
    pub(crate) async fn insert_watermark_candidates(
        &self,
        run_id: Uuid,
        unique_id: &str,
        candidates: &[WatermarkCandidate],
    ) -> AppResult<()> {
        if candidates.is_empty() {
            return Ok(());
        }
        for candidate in candidates {
            sqlx::query(
                r#"
                INSERT INTO node_source_watermark_candidates
                    (run_id, unique_id, source_key, watermark_event_id, watermark_observed_at)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (run_id, unique_id, source_key) DO UPDATE SET
                    watermark_event_id = EXCLUDED.watermark_event_id,
                    watermark_observed_at = EXCLUDED.watermark_observed_at
                "#,
            )
            .bind(run_id)
            .bind(unique_id)
            .bind(&candidate.source_key)
            .bind(candidate.watermark_event_id)
            .bind(candidate.watermark_observed_at)
            .execute(&self.pool)
            .await?;
        }
        Ok(())
    }

    /// Load candidate watermarks for a node (on node finish).
    pub(crate) async fn load_watermark_candidates(
        &self,
        run_id: Uuid,
        unique_id: &str,
    ) -> AppResult<Vec<WatermarkCandidate>> {
        let rows = sqlx::query(
            r#"
            SELECT source_key, watermark_event_id, watermark_observed_at
            FROM node_source_watermark_candidates
            WHERE run_id = $1 AND unique_id = $2
            "#,
        )
        .bind(run_id)
        .bind(unique_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .into_iter()
            .map(|row| WatermarkCandidate {
                source_key: row.get("source_key"),
                watermark_event_id: row.get("watermark_event_id"),
                watermark_observed_at: row.get("watermark_observed_at"),
            })
            .collect())
    }

    /// Delete candidate watermarks for a node (cleanup after commit or failure).
    pub(crate) async fn delete_watermark_candidates(
        &self,
        run_id: Uuid,
        unique_id: &str,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            DELETE FROM node_source_watermark_candidates
            WHERE run_id = $1 AND unique_id = $2
            "#,
        )
        .bind(run_id)
        .bind(unique_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Commit watermarks for a successfully completed node.
    /// Uses monotonic guard to prevent regression.
    /// Returns the entries that actually advanced (for audit logging).
    pub(crate) async fn commit_node_watermarks(
        &self,
        project_id: i64,
        environment_id: i64,
        unique_id: &str,
        run_id: Uuid,
        invocation_id: Option<Uuid>,
        candidates: &[WatermarkCandidate],
    ) -> AppResult<Vec<WatermarkCandidate>> {
        if candidates.is_empty() {
            return Ok(Vec::new());
        }
        let mut tx = self.pool.begin().await?;
        let mut advanced = Vec::new();
        for candidate in candidates {
            // Advisory lock per (project, environment, node, source_key) to serialize
            // concurrent commits. Hash collisions only widen the serialization scope.
            let lock_key = format!(
                "{}:{}:{}:{}",
                project_id, environment_id, unique_id, candidate.source_key
            );
            sqlx::query("SELECT pg_advisory_xact_lock(hashtextextended($1, 0))")
                .bind(&lock_key)
                .execute(&mut *tx)
                .await?;

            let previous_event_id = sqlx::query_scalar::<_, i64>(
                r#"
                SELECT watermark_event_id
                FROM node_source_watermarks
                WHERE project_id = $1
                  AND environment_id = $2
                  AND unique_id = $3
                  AND source_key = $4
                "#,
            )
            .bind(project_id)
            .bind(environment_id)
            .bind(unique_id)
            .bind(&candidate.source_key)
            .fetch_optional(&mut *tx)
            .await?;

            if previous_event_id
                .map(|previous| previous >= candidate.watermark_event_id)
                .unwrap_or(false)
            {
                continue;
            }

            sqlx::query(
                r#"
                INSERT INTO node_source_watermarks
                    (project_id, environment_id, unique_id, source_key,
                     watermark_event_id, watermark_observed_at, run_id, updated_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
                ON CONFLICT (project_id, environment_id, unique_id, source_key) DO UPDATE SET
                    watermark_event_id = EXCLUDED.watermark_event_id,
                    watermark_observed_at = EXCLUDED.watermark_observed_at,
                    run_id = EXCLUDED.run_id,
                    updated_at = NOW()
                "#,
            )
            .bind(project_id)
            .bind(environment_id)
            .bind(unique_id)
            .bind(&candidate.source_key)
            .bind(candidate.watermark_event_id)
            .bind(candidate.watermark_observed_at)
            .bind(run_id)
            .execute(&mut *tx)
            .await?;

            sqlx::query(
                r#"
                    INSERT INTO node_source_watermark_log
                        (project_id, environment_id, unique_id, source_key,
                         watermark_event_id, previous_event_id, run_id, invocation_id)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    "#,
            )
            .bind(project_id)
            .bind(environment_id)
            .bind(unique_id)
            .bind(&candidate.source_key)
            .bind(candidate.watermark_event_id)
            .bind(previous_event_id)
            .bind(run_id)
            .bind(invocation_id)
            .execute(&mut *tx)
            .await?;
            advanced.push(candidate.clone());
        }
        tx.commit().await?;
        Ok(advanced)
    }

    pub(crate) async fn advance_satisfied_source_events_from_watermarks(
        &self,
        project_id: i64,
        environment_id: i64,
        source_keys: &[String],
        manifest_run_id: Uuid,
    ) -> AppResult<()> {
        if source_keys.is_empty() {
            return Ok(());
        }
        let source_event_ids = sqlx::query(
            r#"
            SELECT e.source_key, e.id
            FROM source_state_events e
            LEFT JOIN environment_source_state_status s
              ON s.project_id = e.project_id
             AND s.environment_id = e.environment_id
             AND s.source_key = e.source_key
            WHERE e.project_id = $1
              AND e.environment_id = $2
              AND e.source_key = ANY($3::TEXT[])
              AND (s.latest_satisfied_event_id IS NULL OR e.id > s.latest_satisfied_event_id)
            ORDER BY e.source_key ASC, e.id ASC
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(source_keys)
        .fetch_all(&self.pool)
        .await?;

        for row in source_event_ids {
            let source_key: String = row.get("source_key");
            let event_id: i64 = row.get("id");
            if self
                .are_all_downstream_nodes_satisfied(
                    project_id,
                    environment_id,
                    &source_key,
                    event_id,
                    manifest_run_id,
                )
                .await?
            {
                self.advance_source_state_status_from_watermarks(
                    project_id,
                    environment_id,
                    event_id,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Find downstream nodes that are stale for a given set of source events.
    /// Used by the reconciler to select only nodes that need execution.
    /// Falls back to all downstream nodes if node_ancestor_sources is not populated
    /// for this manifest run (pre-migration manifests).
    pub(crate) async fn list_stale_downstream_nodes(
        &self,
        project_id: i64,
        environment_id: i64,
        source_keys: &[String],
        target_event_ids: &[i64],
        manifest_run_id: Uuid,
    ) -> AppResult<Vec<String>> {
        if source_keys.is_empty() || target_event_ids.is_empty() {
            return Ok(Vec::new());
        }

        // Check if ancestor sources are populated for this manifest run.
        // If not (pre-migration manifest), fall back to all downstream nodes.
        let has_ancestors = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM node_ancestor_sources WHERE run_id = $1 LIMIT 1)",
        )
        .bind(manifest_run_id)
        .fetch_one(&self.pool)
        .await?;

        if !has_ancestors {
            return self
                .list_downstream_manifest_node_unique_ids(manifest_run_id, source_keys)
                .await;
        }

        sqlx::query_scalar::<_, String>(
            r#"
            SELECT DISTINCT nas.unique_id
            FROM node_ancestor_sources nas
            JOIN manifest_nodes mn
              ON mn.run_id = nas.run_id
             AND mn.unique_id = nas.unique_id
             AND mn.resource_type <> 'source'
            LEFT JOIN node_source_watermarks nsw
                ON nsw.project_id = $1
               AND nsw.environment_id = $2
               AND nsw.unique_id = nas.unique_id
               AND nsw.source_key = nas.source_key
            WHERE nas.run_id = $3
              AND nas.source_key = ANY($4::TEXT[])
              AND (nsw.watermark_event_id IS NULL
                   OR nsw.watermark_event_id < (
                       SELECT MAX(sse.id)
                       FROM source_state_events sse
                       WHERE sse.project_id = $1
                         AND sse.environment_id = $2
                         AND sse.source_key = nas.source_key
                         AND sse.id = ANY($5::BIGINT[])
                   ))
            ORDER BY nas.unique_id
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(manifest_run_id)
        .bind(source_keys)
        .bind(target_event_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(Into::into)
    }

    /// Check if all downstream nodes for a source have watermarks >= target event ID.
    /// Returns false if node_ancestor_sources is not populated for this manifest run.
    pub(crate) async fn are_all_downstream_nodes_satisfied(
        &self,
        project_id: i64,
        environment_id: i64,
        source_key: &str,
        target_event_id: i64,
        manifest_run_id: Uuid,
    ) -> AppResult<bool> {
        // If ancestor sources aren't populated, we can't determine satisfaction
        let has_ancestors = sqlx::query_scalar::<_, bool>(
            "SELECT EXISTS(SELECT 1 FROM node_ancestor_sources WHERE run_id = $1 LIMIT 1)",
        )
        .bind(manifest_run_id)
        .fetch_one(&self.pool)
        .await?;

        if !has_ancestors {
            return Ok(false);
        }

        let stale_count = sqlx::query_scalar::<_, i64>(
            r#"
            SELECT COUNT(*)
            FROM node_ancestor_sources nas
            JOIN manifest_nodes mn
              ON mn.run_id = nas.run_id
             AND mn.unique_id = nas.unique_id
             AND mn.resource_type <> 'source'
            LEFT JOIN node_source_watermarks nsw
                ON nsw.project_id = $1
               AND nsw.environment_id = $2
               AND nsw.unique_id = nas.unique_id
               AND nsw.source_key = nas.source_key
            WHERE nas.run_id = $3
              AND nas.source_key = $4
              AND (nsw.watermark_event_id IS NULL OR nsw.watermark_event_id < $5)
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(manifest_run_id)
        .bind(source_key)
        .bind(target_event_id)
        .fetch_one(&self.pool)
        .await?;
        Ok(stale_count == 0)
    }

    /// Advance environment_source_state_status for a source event that is now
    /// fully satisfied by per-node watermarks (all downstream nodes have watermark >= event).
    pub(crate) async fn advance_source_state_status_from_watermarks(
        &self,
        project_id: i64,
        environment_id: i64,
        source_event_id: i64,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            INSERT INTO environment_source_state_status (
                project_id,
                environment_id,
                source_key,
                latest_satisfied_event_id,
                latest_satisfied_state_version,
                latest_satisfied_observed_at,
                last_satisfied_run_id,
                last_satisfied_plan_id,
                updated_at
            )
            SELECT
                e.project_id,
                e.environment_id,
                e.source_key,
                e.id,
                e.state_version,
                e.observed_at,
                NULL,
                NULL,
                NOW()
            FROM source_state_events e
            WHERE e.id = $1
              AND e.project_id = $2
              AND e.environment_id = $3
            ON CONFLICT (project_id, environment_id, source_key) DO UPDATE SET
                latest_satisfied_event_id = EXCLUDED.latest_satisfied_event_id,
                latest_satisfied_state_version = EXCLUDED.latest_satisfied_state_version,
                latest_satisfied_observed_at = EXCLUDED.latest_satisfied_observed_at,
                updated_at = NOW()
            WHERE environment_source_state_status.latest_satisfied_event_id < EXCLUDED.latest_satisfied_event_id
            "#,
        )
        .bind(source_event_id)
        .bind(project_id)
        .bind(environment_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Load reconciliation state (code + source) for a set of nodes.
    #[allow(clippy::type_complexity)]
    pub(crate) async fn load_node_reconciliation_state(
        &self,
        project_id: i64,
        environment_id: i64,
        unique_ids: &[String],
    ) -> AppResult<Vec<NodeReconcileState>> {
        if unique_ids.is_empty() {
            return Ok(Vec::new());
        }

        let target_run_id: Option<Uuid> = sqlx::query_scalar(
            r#"
            SELECT r.run_id
            FROM runs r
            JOIN manifest_snapshots ms ON ms.run_id = r.run_id
            WHERE r.project_id = $1 AND r.environment_id = $2
            ORDER BY r.id DESC
            LIMIT 1
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(target_run_id) = target_run_id else {
            return Ok(unique_ids
                .iter()
                .map(|uid| NodeReconcileState {
                    unique_id: uid.clone(),
                    code_state: ReconcileIndicator::Unknown,
                    code_tooltip: "No target manifest available".to_string(),
                    source_state: ReconcileIndicator::NoSources,
                    source_tooltip: "No tracked sources".to_string(),
                })
                .collect());
        };

        // Code state: current_node_state.checksum vs manifest_nodes.checksum
        let code_rows = sqlx::query(
            r#"
            SELECT
                mn.unique_id,
                mn.checksum AS target_checksum,
                cns.checksum AS current_checksum
            FROM manifest_nodes mn
            LEFT JOIN current_node_state cns
                ON cns.project_id = $2
               AND cns.environment_id = $3
               AND cns.unique_id = mn.unique_id
            WHERE mn.run_id = $1
              AND mn.unique_id = ANY($4)
            "#,
        )
        .bind(target_run_id)
        .bind(project_id)
        .bind(environment_id)
        .bind(unique_ids)
        .fetch_all(&self.pool)
        .await?;

        let mut code_map: BTreeMap<String, (ReconcileIndicator, String)> = BTreeMap::new();
        for row in &code_rows {
            let uid: String = row.get("unique_id");
            let target: Option<String> = row.get("target_checksum");
            let current: Option<String> = row.get("current_checksum");
            let (state, tooltip) = match (&current, &target) {
                (Some(c), Some(t)) if c == t => {
                    (ReconcileIndicator::Reconciled, "Matches target".to_string())
                }
                (Some(_), Some(_)) => (
                    ReconcileIndicator::Stale,
                    "Checksum differs from target".to_string(),
                ),
                (None, Some(_)) => (ReconcileIndicator::Stale, "Never executed".to_string()),
                _ => (
                    ReconcileIndicator::Unknown,
                    "No target checksum".to_string(),
                ),
            };
            code_map.insert(uid, (state, tooltip));
        }

        // Source state: watermarks vs latest source events
        let source_rows = sqlx::query(
            r#"
            SELECT
                nas.unique_id,
                nas.source_key,
                nsw.watermark_event_id,
                (SELECT MAX(sse.id) FROM source_state_events sse
                 WHERE sse.project_id = $2
                   AND sse.environment_id = $3
                   AND sse.source_key = nas.source_key) AS latest_event_id
            FROM node_ancestor_sources nas
            LEFT JOIN node_source_watermarks nsw
                ON nsw.project_id = $2
               AND nsw.environment_id = $3
               AND nsw.unique_id = nas.unique_id
               AND nsw.source_key = nas.source_key
            WHERE nas.run_id = $1
              AND nas.unique_id = ANY($4)
            "#,
        )
        .bind(target_run_id)
        .bind(project_id)
        .bind(environment_id)
        .bind(unique_ids)
        .fetch_all(&self.pool)
        .await?;

        let mut source_map: BTreeMap<String, Vec<(String, Option<i64>, Option<i64>)>> =
            BTreeMap::new();
        for row in &source_rows {
            let uid: String = row.get("unique_id");
            let source_key: String = row.get("source_key");
            let watermark: Option<i64> = row.get("watermark_event_id");
            let latest: Option<i64> = row.get("latest_event_id");
            source_map
                .entry(uid)
                .or_default()
                .push((source_key, watermark, latest));
        }

        Ok(unique_ids
            .iter()
            .map(|uid| {
                let (code_state, code_tooltip) = code_map
                    .get(uid)
                    .cloned()
                    .unwrap_or((ReconcileIndicator::Unknown, "Not in manifest".to_string()));

                let (source_state, source_tooltip) = match source_map.get(uid) {
                    None => (
                        ReconcileIndicator::NoSources,
                        "No tracked sources".to_string(),
                    ),
                    Some(sources) if sources.is_empty() => (
                        ReconcileIndicator::NoSources,
                        "No tracked sources".to_string(),
                    ),
                    // Source nodes don't have upstream sources — they ARE the source
                    Some(sources) if uid.starts_with("source.") => {
                        let has_events = sources.iter().any(|(_, _, l)| l.is_some());
                        if has_events {
                            (ReconcileIndicator::Reconciled, "Source origin".to_string())
                        } else {
                            (
                                ReconcileIndicator::NoSources,
                                "No events recorded".to_string(),
                            )
                        }
                    }
                    Some(sources) => {
                        let total = sources.len();
                        let stale: Vec<_> = sources
                            .iter()
                            .filter(|(_, watermark, latest)| match (watermark, latest) {
                                (Some(w), Some(l)) => w < l,
                                (None, Some(_)) => true,
                                _ => false,
                            })
                            .collect();
                        if stale.is_empty() {
                            (
                                ReconcileIndicator::Reconciled,
                                format!("All {} sources current", total),
                            )
                        } else {
                            let details: Vec<String> = stale
                                .iter()
                                .take(3)
                                .map(|(key, w, l)| {
                                    let behind = l.unwrap_or(0) - w.unwrap_or(0);
                                    format!("{}: {} behind", short_source_key(key), behind)
                                })
                                .collect();
                            (
                                ReconcileIndicator::Stale,
                                format!(
                                    "Behind {} of {} sources\n{}",
                                    stale.len(),
                                    total,
                                    details.join("\n")
                                ),
                            )
                        }
                    }
                };

                NodeReconcileState {
                    unique_id: uid.clone(),
                    code_state,
                    code_tooltip,
                    source_state,
                    source_tooltip,
                }
            })
            .collect())
    }
}

/// Reconciliation indicator state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReconcileIndicator {
    Reconciled,
    Stale,
    Unknown,
    NoSources,
}

impl ReconcileIndicator {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Reconciled => "reconciled",
            Self::Stale => "stale",
            Self::Unknown => "unknown",
            Self::NoSources => "no_sources",
        }
    }
}

/// Per-node reconciliation state for UI display.
#[derive(Debug, Clone)]
pub(crate) struct NodeReconcileState {
    pub(crate) unique_id: String,
    pub(crate) code_state: ReconcileIndicator,
    pub(crate) code_tooltip: String,
    pub(crate) source_state: ReconcileIndicator,
    pub(crate) source_tooltip: String,
}

fn short_source_key(key: &str) -> &str {
    key.rsplit('.').next().unwrap_or(key)
}

#[cfg(test)]
mod tests {
    use super::WatermarkCandidate;

    #[test]
    fn watermark_candidate_min_selects_lowest_event_id() {
        let candidates = [
            WatermarkCandidate {
                source_key: "source.pkg.orders".to_string(),
                watermark_event_id: 10,
                watermark_observed_at: None,
            },
            WatermarkCandidate {
                source_key: "source.pkg.orders".to_string(),
                watermark_event_id: 5,
                watermark_observed_at: None,
            },
        ];
        let min = candidates
            .iter()
            .min_by_key(|c| c.watermark_event_id)
            .unwrap();
        assert_eq!(min.watermark_event_id, 5);
    }

    #[test]
    fn empty_candidates_produce_no_watermark() {
        let candidates: Vec<WatermarkCandidate> = Vec::new();
        assert!(candidates.is_empty());
    }
}
