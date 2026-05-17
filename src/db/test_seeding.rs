//! Test seeding operations for environment state.
//!
//! These methods provide a clean interface for tests to seed environment
//! state (manifests, node state, actual state) without raw SQL. Both
//! projection tests and any future test that needs "an environment has
//! been deployed with this manifest" use this seam.

use super::*;

/// Input for seeding a successful deployment in tests.
pub struct SeedDeploymentInput<'a> {
    pub project_id: &'a str,
    pub environment_slug: &'a str,
    pub commit_sha: &'a str,
    pub nodes: &'a [(&'a str, &'a str)],
    pub edges: &'a [(&'a str, &'a str)],
}

impl Db {
    /// Seed a complete successful deployment: run, manifest, nodes, edges, and actual state.
    ///
    /// This is the test-facing equivalent of what happens when a worker completes
    /// a successful full-graph build invocation. It creates:
    /// - A completed run record
    /// - A manifest snapshot
    /// - Manifest nodes and edges
    /// - Current node state for each node
    /// - Environment actual state pointing to this run
    pub async fn seed_successful_deployment(&self, input: SeedDeploymentInput<'_>) -> AppResult<Uuid> {
        let row = sqlx::query(
            r#"
            SELECT p.id AS project_pk, p.project_name, p.project_root, p.git_repo_url,
                   e.id AS environment_pk
            FROM projects p
            JOIN environments e ON e.project_id = p.id
            WHERE p.project_id = $1 AND e.slug = $2
            "#,
        )
        .bind(input.project_id)
        .bind(input.environment_slug)
        .fetch_one(&self.pool)
        .await?;

        let project_pk: i64 = row.get("project_pk");
        let environment_pk: i64 = row.get("environment_pk");
        let project_name: String = row.get("project_name");
        let project_root: Option<String> = row.get("project_root");
        let git_repo_url: Option<String> = row.get("git_repo_url");
        let run_id = Uuid::new_v4();
        let successful_at = chrono::Utc::now() - chrono::Duration::hours(1);

        // Insert completed run
        sqlx::query(
            r#"
            INSERT INTO runs (
                run_id, project_id, environment_id, command, args, is_full_graph_run, execution_mode,
                git_branch, git_commit_sha, git_repo_url, project_root, project_name, project_ref,
                started_at, finished_at, exit_code, terminal_status
            )
            VALUES (
                $1, $2, $3, 'build', '[]'::jsonb, true, 'server',
                'main', $4, $5, $6, $7, $8,
                $9, $10, 0, 'succeeded'
            )
            "#,
        )
        .bind(run_id)
        .bind(project_pk)
        .bind(environment_pk)
        .bind(input.commit_sha)
        .bind(&git_repo_url)
        .bind(&project_root)
        .bind(&project_name)
        .bind(input.project_id)
        .bind(successful_at)
        .bind(successful_at)
        .execute(&self.pool)
        .await?;

        // Insert manifest snapshot
        sqlx::query(
            r#"
            INSERT INTO manifest_snapshots (run_id, manifest, manifest_size_bytes, checksum)
            VALUES ($1, '{}'::jsonb, 2, 'fixture-checksum')
            "#,
        )
        .bind(run_id)
        .execute(&self.pool)
        .await?;

        // Insert manifest nodes and current node state
        for (unique_id, resource_type) in input.nodes {
            let name = unique_id.rsplit('.').next().unwrap_or(unique_id);
            sqlx::query(
                r#"
                INSERT INTO manifest_nodes (
                    run_id, unique_id, resource_type, name, package_name, original_file_path,
                    tags, fqn, config, checksum, database_name, schema_name, alias, relation_name
                )
                VALUES (
                    $1, $2, $3, $4, 'pkg', '', '[]'::jsonb, '[]'::jsonb, '{}'::jsonb,
                    NULL, NULL, NULL, NULL, NULL
                )
                "#,
            )
            .bind(run_id)
            .bind(unique_id)
            .bind(resource_type)
            .bind(name)
            .execute(&self.pool)
            .await?;

            sqlx::query(
                r#"
                INSERT INTO current_node_state (
                    project_id, environment_id, unique_id, last_run_id, status, resource_type,
                    node_name, checksum, finished_at, last_success_at, updated_at
                )
                VALUES (
                    $1, $2, $3, $4, 'succeeded', $5,
                    $6, NULL, $7, $7, NOW()
                )
                ON CONFLICT (project_id, environment_id, unique_id) DO UPDATE
                SET last_run_id = EXCLUDED.last_run_id,
                    status = EXCLUDED.status,
                    resource_type = EXCLUDED.resource_type,
                    node_name = EXCLUDED.node_name,
                    finished_at = EXCLUDED.finished_at,
                    last_success_at = EXCLUDED.last_success_at,
                    updated_at = NOW()
                "#,
            )
            .bind(project_pk)
            .bind(environment_pk)
            .bind(unique_id)
            .bind(run_id)
            .bind(resource_type)
            .bind(name)
            .bind(successful_at)
            .execute(&self.pool)
            .await?;
        }

        // Insert manifest edges
        for (parent_unique_id, child_unique_id) in input.edges {
            sqlx::query(
                r#"
                INSERT INTO manifest_edges (run_id, parent_unique_id, child_unique_id)
                VALUES ($1, $2, $3)
                "#,
            )
            .bind(run_id)
            .bind(parent_unique_id)
            .bind(child_unique_id)
            .execute(&self.pool)
            .await?;
        }

        // Upsert environment actual state
        sqlx::query(
            r#"
            INSERT INTO environment_actual_state (
                project_id, environment_id,
                last_attempted_run_id, last_attempted_commit_sha, last_attempted_at,
                last_successful_run_id, last_successful_commit_sha, last_successful_at,
                updated_at
            )
            VALUES ($1, $2, $3, $4, $5, $3, $4, $5, $5)
            ON CONFLICT (project_id, environment_id) DO UPDATE
            SET last_attempted_run_id = EXCLUDED.last_attempted_run_id,
                last_attempted_commit_sha = EXCLUDED.last_attempted_commit_sha,
                last_attempted_at = EXCLUDED.last_attempted_at,
                last_successful_run_id = EXCLUDED.last_successful_run_id,
                last_successful_commit_sha = EXCLUDED.last_successful_commit_sha,
                last_successful_at = EXCLUDED.last_successful_at,
                updated_at = EXCLUDED.updated_at
            "#,
        )
        .bind(project_pk)
        .bind(environment_pk)
        .bind(run_id)
        .bind(input.commit_sha)
        .bind(successful_at)
        .execute(&self.pool)
        .await?;

        Ok(run_id)
    }
}
