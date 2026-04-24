//! Model-centric queries for the models UI views.

use super::*;

impl Db {
    /// List models for a given environment from current_node_state, filtered to resource_type = 'model'.
    pub(crate) async fn list_models_for_environment(
        &self,
        project_id: i64,
        environment_id: i64,
    ) -> AppResult<Vec<ModelSummaryRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT
                cns.unique_id,
                cns.node_name,
                cns.node_path,
                cns.resource_type,
                cns.status,
                cns.materialized,
                cns.relation_schema,
                cns.relation_database,
                cns.last_success_at,
                cns.finished_at,
                cns.execution_time_seconds,
                mn.package_name,
                mn.config
            FROM current_node_state cns
            LEFT JOIN LATERAL (
                SELECT mn2.package_name, mn2.config
                FROM manifest_nodes mn2
                JOIN runs r ON r.run_id = mn2.run_id
                WHERE mn2.unique_id = cns.unique_id
                  AND r.project_id = $1
                  AND r.environment_id = $2
                ORDER BY r.id DESC
                LIMIT 1
            ) mn ON true
            WHERE cns.project_id = $1
              AND cns.environment_id = $2
              AND cns.resource_type = 'model'
            ORDER BY cns.node_name ASC
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let config: Option<sqlx::types::Json<Value>> = row.get("config");
                let group = config
                    .as_ref()
                    .and_then(|c| c.get("group").and_then(Value::as_str).map(String::from));
                ModelSummaryRecord {
                    unique_id: row.get("unique_id"),
                    node_name: row.get("node_name"),
                    node_path: row.get("node_path"),
                    resource_type: row.get("resource_type"),
                    status: row.get("status"),
                    materialized: row.get("materialized"),
                    relation_schema: row.get("relation_schema"),
                    relation_database: row.get("relation_database"),
                    last_success_at: row.get("last_success_at"),
                    finished_at: row.get("finished_at"),
                    execution_time_seconds: row.get("execution_time_seconds"),
                    package_name: row.get("package_name"),
                    group,
                }
            })
            .collect())
    }

    /// Get model detail: latest manifest raw node JSON + promoted manifest raw node JSON.
    pub(crate) async fn get_model_detail(
        &self,
        project_id: i64,
        environment_id: i64,
        unique_id: &str,
    ) -> AppResult<ModelDetailRecord> {
        // Latest manifest node from the most recent run with a manifest
        let latest_raw: Option<Value> = sqlx::query_scalar(
            r#"
            SELECT ms.manifest -> 'nodes' -> $3
            FROM runs r
            JOIN manifest_snapshots ms ON ms.run_id = r.run_id
            WHERE r.project_id = $1
              AND r.environment_id = $2
            ORDER BY r.id DESC
            LIMIT 1
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(unique_id)
        .fetch_optional(&self.pool)
        .await?
        .flatten();

        // Promoted manifest node
        let promoted_raw: Option<sqlx::types::Json<Value>> = sqlx::query_scalar(
            r#"
            SELECT raw_node
            FROM promoted_manifest_nodes
            WHERE project_id = $1
              AND environment_id = $2
              AND unique_id = $3
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(unique_id)
        .fetch_optional(&self.pool)
        .await?;

        // Current node state
        let state_row = sqlx::query(
            r#"
            SELECT status, last_success_at, finished_at
            FROM current_node_state
            WHERE project_id = $1 AND environment_id = $2 AND unique_id = $3
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(unique_id)
        .fetch_optional(&self.pool)
        .await?;

        Ok(ModelDetailRecord {
            latest_manifest_node: latest_raw,
            promoted_manifest_node: promoted_raw.map(|j| j.0),
            status: state_row.as_ref().and_then(|r| r.get("status")),
            last_success_at: state_row.as_ref().and_then(|r| r.get("last_success_at")),
            finished_at: state_row.as_ref().and_then(|r| r.get("finished_at")),
        })
    }

    /// Get node execution history for a model.
    pub(crate) async fn get_model_node_executions(
        &self,
        project_id: i64,
        environment_id: i64,
        unique_id: &str,
        limit: i64,
    ) -> AppResult<Vec<ModelNodeExecutionRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT
                ne.run_id,
                ne.status,
                ne.started_at,
                ne.finished_at,
                ne.execution_time_seconds,
                r.git_commit_sha,
                r.command,
                i.invocation_id
            FROM node_executions ne
            JOIN runs r ON r.run_id = ne.run_id
            LEFT JOIN invocations i ON i.run_id = ne.run_id
            WHERE r.project_id = $1
              AND r.environment_id = $2
              AND ne.unique_id = $3
            ORDER BY r.id DESC
            LIMIT $4
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(unique_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| ModelNodeExecutionRecord {
                run_id: row.get("run_id"),
                invocation_id: row.get("invocation_id"),
                status: row.get("status"),
                started_at: row.get("started_at"),
                finished_at: row.get("finished_at"),
                execution_time_seconds: row.get("execution_time_seconds"),
                git_commit_sha: row.get("git_commit_sha"),
                command: row.get("command"),
            })
            .collect())
    }

    /// Get lineage (ancestors/descendants) for a model from manifest_edges.
    pub(crate) async fn get_model_lineage(
        &self,
        project_id: i64,
        environment_id: i64,
        unique_id: &str,
        depth: i32,
        direction: &str,
    ) -> AppResult<ModelLineageRecord> {
        // Find the latest run with a manifest for this environment
        let run_id: Option<Uuid> = sqlx::query_scalar(
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

        let Some(run_id) = run_id else {
            return Ok(ModelLineageRecord {
                nodes: Vec::new(),
                edges: Vec::new(),
            });
        };

        let include_ancestors = direction == "both" || direction == "ancestors";
        let include_descendants = direction == "both" || direction == "descendants";

        let mut node_ids: BTreeSet<String> = BTreeSet::new();
        let mut edges: Vec<(String, String)> = Vec::new();
        node_ids.insert(unique_id.to_string());

        // Ancestors (walk parent_unique_id -> child_unique_id where child = known)
        if include_ancestors {
            let ancestor_rows = sqlx::query(
                r#"
                WITH RECURSIVE ancestors AS (
                    SELECT parent_unique_id, child_unique_id, 1 AS depth
                    FROM manifest_edges
                    WHERE run_id = $1 AND child_unique_id = $2
                    UNION ALL
                    SELECT me.parent_unique_id, me.child_unique_id, a.depth + 1
                    FROM manifest_edges me
                    JOIN ancestors a ON me.child_unique_id = a.parent_unique_id
                    WHERE me.run_id = $1 AND a.depth < $3
                )
                SELECT DISTINCT parent_unique_id, child_unique_id FROM ancestors
                "#,
            )
            .bind(run_id)
            .bind(unique_id)
            .bind(depth)
            .fetch_all(&self.pool)
            .await?;

            for row in &ancestor_rows {
                let parent: String = row.get("parent_unique_id");
                let child: String = row.get("child_unique_id");
                node_ids.insert(parent.clone());
                node_ids.insert(child.clone());
                edges.push((parent, child));
            }
        }

        // Descendants (walk parent_unique_id -> child_unique_id where parent = known)
        if include_descendants {
            let descendant_rows = sqlx::query(
                r#"
                WITH RECURSIVE descendants AS (
                    SELECT parent_unique_id, child_unique_id, 1 AS depth
                    FROM manifest_edges
                    WHERE run_id = $1 AND parent_unique_id = $2
                    UNION ALL
                    SELECT me.parent_unique_id, me.child_unique_id, d.depth + 1
                    FROM manifest_edges me
                    JOIN descendants d ON me.parent_unique_id = d.child_unique_id
                    WHERE me.run_id = $1 AND d.depth < $3
                )
                SELECT DISTINCT parent_unique_id, child_unique_id FROM descendants
                "#,
            )
            .bind(run_id)
            .bind(unique_id)
            .bind(depth)
            .fetch_all(&self.pool)
            .await?;

            for row in &descendant_rows {
                let parent: String = row.get("parent_unique_id");
                let child: String = row.get("child_unique_id");
                node_ids.insert(parent.clone());
                node_ids.insert(child.clone());
                edges.push((parent, child));
            }
        }

        // Fetch node metadata for all discovered nodes
        let node_id_list: Vec<String> = node_ids.into_iter().collect();
        let nodes = if node_id_list.is_empty() {
            Vec::new()
        } else {
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
            .bind(&node_id_list)
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
        };

        // Deduplicate edges
        let mut seen = BTreeSet::new();
        let edges = edges
            .into_iter()
            .filter(|e| seen.insert((e.0.clone(), e.1.clone())))
            .collect();

        Ok(ModelLineageRecord { nodes, edges })
    }

    /// Get tests associated with a model from the latest manifest.
    pub(crate) async fn get_model_tests(
        &self,
        project_id: i64,
        environment_id: i64,
        model_unique_id: &str,
    ) -> AppResult<Vec<ModelTestRecord>> {
        let rows = sqlx::query(
            r#"
            SELECT
                mn.unique_id,
                mn.name,
                mn.config,
                cns.status,
                cns.finished_at,
                cns.last_success_at
            FROM manifest_nodes mn
            JOIN runs r ON r.run_id = mn.run_id
            LEFT JOIN current_node_state cns
                ON cns.unique_id = mn.unique_id
                AND cns.project_id = $1
                AND cns.environment_id = $2
            WHERE r.run_id = (
                SELECT r2.run_id
                FROM runs r2
                JOIN manifest_snapshots ms ON ms.run_id = r2.run_id
                WHERE r2.project_id = $1 AND r2.environment_id = $2
                ORDER BY r2.id DESC
                LIMIT 1
            )
            AND mn.resource_type = 'test'
            AND EXISTS (
                SELECT 1 FROM manifest_edges me
                WHERE me.run_id = mn.run_id
                  AND me.child_unique_id = mn.unique_id
                  AND me.parent_unique_id = $3
            )
            ORDER BY mn.name ASC
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(model_unique_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| {
                let config: Option<sqlx::types::Json<Value>> = row.get("config");
                let test_type = config
                    .as_ref()
                    .and_then(|c| c.get("test_metadata").and_then(|tm| tm.get("name")).and_then(Value::as_str).map(String::from))
                    .or_else(|| config.as_ref().and_then(|c| c.get("severity").and_then(Value::as_str).map(String::from)));
                ModelTestRecord {
                    unique_id: row.get("unique_id"),
                    name: row.get("name"),
                    test_type,
                    status: row.get("status"),
                    finished_at: row.get("finished_at"),
                    last_success_at: row.get("last_success_at"),
                }
            })
            .collect())
    }

    /// Get model history: runs where the model's checksum changed.
    pub(crate) async fn get_model_history(
        &self,
        project_id: i64,
        environment_id: i64,
        unique_id: &str,
    ) -> AppResult<Vec<ModelHistoryRecord>> {
        let rows = sqlx::query(
            r#"
            WITH ordered AS (
                SELECT
                    mn.run_id,
                    mn.checksum,
                    r.git_commit_sha,
                    r.git_repo_url,
                    r.started_at,
                    LAG(mn.checksum) OVER (ORDER BY r.id ASC) AS prev_checksum
                FROM manifest_nodes mn
                JOIN runs r ON r.run_id = mn.run_id
                WHERE r.project_id = $1
                  AND r.environment_id = $2
                  AND mn.unique_id = $3
                ORDER BY r.id ASC
            )
            SELECT run_id, checksum, prev_checksum, git_commit_sha, git_repo_url, started_at
            FROM ordered
            WHERE prev_checksum IS NULL OR checksum IS DISTINCT FROM prev_checksum
            ORDER BY started_at DESC
            "#,
        )
        .bind(project_id)
        .bind(environment_id)
        .bind(unique_id)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .into_iter()
            .map(|row| ModelHistoryRecord {
                run_id: row.get("run_id"),
                checksum: row.get("checksum"),
                prev_checksum: row.get("prev_checksum"),
                git_commit_sha: row.get("git_commit_sha"),
                git_repo_url: row.get("git_repo_url"),
                started_at: row.get("started_at"),
            })
            .collect())
    }

    /// Get raw_code for a model from a specific run's manifest snapshot.
    pub(crate) async fn get_model_history_raw_code(
        &self,
        run_id: Uuid,
        unique_id: &str,
    ) -> AppResult<Option<String>> {
        let raw: Option<Value> = sqlx::query_scalar(
            r#"
            SELECT ms.manifest -> 'nodes' -> $2 -> 'raw_code'
            FROM manifest_snapshots ms
            WHERE ms.run_id = $1
            "#,
        )
        .bind(run_id)
        .bind(unique_id)
        .fetch_optional(&self.pool)
        .await?
        .flatten();

        Ok(raw.and_then(|v| v.as_str().map(String::from)))
    }

}
