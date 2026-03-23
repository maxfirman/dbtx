use serde_json::json;
use sqlx::{PgPool, Row};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use tempfile::TempDir;
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, runners::AsyncRunner},
};
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn replay_ignores_seed_and_test_for_promoted_manifest_state() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let ids = scope_ids(db.pool()).await;
    let run_id = Uuid::new_v4();
    let test_run_id = Uuid::new_v4();

    insert_run(
        db.pool(),
        RunInsert {
            id: 1,
            run_id,
            project_id: ids.project_id,
            environment_id: ids.environment_id,
            command: "run",
            is_full_graph_run: true,
            terminal_status: "success",
        },
    )
    .await;
    insert_manifest(
        db.pool(),
        run_id,
        &manifest_with_nodes([node("model.pkg.a", "old")]),
    )
    .await;
    insert_node_execution(
        db.pool(),
        run_id,
        "model.pkg.a",
        "model",
        "success",
        Some("old"),
    )
    .await;

    insert_run(
        db.pool(),
        RunInsert {
            id: 2,
            run_id: test_run_id,
            project_id: ids.project_id,
            environment_id: ids.environment_id,
            command: "test",
            is_full_graph_run: true,
            terminal_status: "success",
        },
    )
    .await;
    insert_manifest(
        db.pool(),
        test_run_id,
        &manifest_with_nodes([node("model.pkg.a", "test-only")]),
    )
    .await;
    insert_node_execution(
        db.pool(),
        test_run_id,
        "model.pkg.a",
        "test",
        "pass",
        Some("test-only"),
    )
    .await;

    run_replay(db.url(), test_run_id);

    let promoted_checksum: Option<String> = sqlx::query_scalar(
        "SELECT checksum FROM promoted_manifest_nodes WHERE project_id = $1 AND environment_id = $2 AND unique_id = 'model.pkg.a'",
    )
    .bind(ids.project_id)
    .bind(ids.environment_id)
    .fetch_one(db.pool())
    .await
    .expect("promoted checksum");
    let promoted_source_run: Uuid = sqlx::query_scalar(
        "SELECT source_run_id FROM promoted_manifest_meta WHERE project_id = $1 AND environment_id = $2",
    )
    .bind(ids.project_id)
    .bind(ids.environment_id)
    .fetch_one(db.pool())
    .await
    .expect("promoted meta source");

    assert_eq!(promoted_checksum.as_deref(), Some("old"));
    assert_eq!(promoted_source_run, run_id);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn replay_rebuilds_promoted_and_current_state_for_partial_progress() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let ids = scope_ids(db.pool()).await;
    let base_run_id = Uuid::new_v4();
    let partial_run_id = Uuid::new_v4();
    let failed_run_id = Uuid::new_v4();

    insert_run(
        db.pool(),
        RunInsert {
            id: 1,
            run_id: base_run_id,
            project_id: ids.project_id,
            environment_id: ids.environment_id,
            command: "run",
            is_full_graph_run: true,
            terminal_status: "success",
        },
    )
    .await;
    insert_manifest(
        db.pool(),
        base_run_id,
        &manifest_with_nodes([node("model.pkg.a", "old-a"), node("model.pkg.b", "old-b")]),
    )
    .await;
    insert_node_execution(
        db.pool(),
        base_run_id,
        "model.pkg.a",
        "model",
        "success",
        Some("old-a"),
    )
    .await;
    insert_node_execution(
        db.pool(),
        base_run_id,
        "model.pkg.b",
        "model",
        "success",
        Some("old-b"),
    )
    .await;

    insert_run(
        db.pool(),
        RunInsert {
            id: 2,
            run_id: partial_run_id,
            project_id: ids.project_id,
            environment_id: ids.environment_id,
            command: "run",
            is_full_graph_run: false,
            terminal_status: "success",
        },
    )
    .await;
    insert_manifest(
        db.pool(),
        partial_run_id,
        &manifest_with_nodes([node("model.pkg.a", "new-a"), node("model.pkg.b", "new-b")]),
    )
    .await;
    insert_node_execution(
        db.pool(),
        partial_run_id,
        "model.pkg.a",
        "model",
        "success",
        Some("new-a"),
    )
    .await;

    insert_run(
        db.pool(),
        RunInsert {
            id: 3,
            run_id: failed_run_id,
            project_id: ids.project_id,
            environment_id: ids.environment_id,
            command: "run",
            is_full_graph_run: false,
            terminal_status: "failed",
        },
    )
    .await;
    insert_manifest(
        db.pool(),
        failed_run_id,
        &manifest_with_nodes([
            node("model.pkg.a", "failed-a"),
            node("model.pkg.b", "failed-b"),
        ]),
    )
    .await;
    insert_node_execution(
        db.pool(),
        failed_run_id,
        "model.pkg.b",
        "model",
        "failed",
        Some("failed-b"),
    )
    .await;

    run_replay(db.url(), failed_run_id);

    let promoted = sqlx::query(
        "SELECT unique_id, checksum, source_run_id FROM promoted_manifest_nodes WHERE project_id = $1 AND environment_id = $2 ORDER BY unique_id",
    )
    .bind(ids.project_id)
    .bind(ids.environment_id)
    .fetch_all(db.pool())
    .await
    .expect("promoted nodes");

    assert_eq!(promoted.len(), 2);
    assert_eq!(promoted[0].get::<String, _>("unique_id"), "model.pkg.a");
    assert_eq!(
        promoted[0].get::<Option<String>, _>("checksum").as_deref(),
        Some("new-a")
    );
    assert_eq!(promoted[0].get::<Uuid, _>("source_run_id"), partial_run_id);
    assert_eq!(promoted[1].get::<String, _>("unique_id"), "model.pkg.b");
    assert_eq!(
        promoted[1].get::<Option<String>, _>("checksum").as_deref(),
        Some("old-b")
    );
    assert_eq!(promoted[1].get::<Uuid, _>("source_run_id"), base_run_id);

    let current = sqlx::query(
        "SELECT unique_id, last_run_id, status, checksum FROM current_node_state WHERE project_id = $1 AND environment_id = $2 ORDER BY unique_id",
    )
    .bind(ids.project_id)
    .bind(ids.environment_id)
    .fetch_all(db.pool())
    .await
    .expect("current state");

    assert_eq!(current.len(), 2);
    assert_eq!(current[0].get::<String, _>("unique_id"), "model.pkg.a");
    assert_eq!(current[0].get::<Uuid, _>("last_run_id"), partial_run_id);
    assert_eq!(current[0].get::<String, _>("status"), "success");
    assert_eq!(
        current[0].get::<Option<String>, _>("checksum").as_deref(),
        Some("new-a")
    );
    assert_eq!(current[1].get::<String, _>("unique_id"), "model.pkg.b");
    assert_eq!(current[1].get::<Uuid, _>("last_run_id"), failed_run_id);
    assert_eq!(current[1].get::<String, _>("status"), "failed");
    assert_eq!(
        current[1].get::<Option<String>, _>("checksum").as_deref(),
        Some("old-b")
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn project_and_environment_cli_round_trip() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");

    let output = run_dbtx_in_dir(db.url(), repo.project_dir(), &["project", "init"]);
    assert_success(&output);
    let project_id = read_project_id_from_dbt_project(repo.project_dir());

    let output = run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--slug",
            "staging",
            "--target",
            "dev",
            "--kind",
            "persistent",
        ],
    );
    assert_success(&output);

    let list_output = run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &["environment", "list", "--project", &project_id],
    );
    assert_success(&list_output);
    let stdout = String::from_utf8_lossy(&list_output.stdout);
    assert!(
        stdout.contains("slug=staging"),
        "unexpected stdout: {stdout}"
    );
    assert!(
        stdout.contains(&format!("project_id={project_id}")),
        "expected project id in stdout: {stdout}"
    );

    let project_row = sqlx::query(
        "SELECT project_id, project_name, git_repo_url, default_branch, project_root FROM projects WHERE project_id = $1",
    )
    .bind(&project_id)
    .fetch_one(db.pool())
    .await
    .expect("project row");
    assert_eq!(project_row.get::<String, _>("project_id"), project_id);
    assert_eq!(project_row.get::<String, _>("project_name"), "proj");
    assert_eq!(
        project_row
            .get::<Option<String>, _>("git_repo_url")
            .as_deref(),
        Some("https://example.com/repo.git")
    );

    let environment_row =
        sqlx::query("SELECT slug, kind, status FROM environments WHERE slug = 'staging'")
            .fetch_one(db.pool())
            .await
            .expect("environment row");
    assert_eq!(environment_row.get::<String, _>("slug"), "staging");
    assert_eq!(environment_row.get::<String, _>("kind"), "persistent");
    assert_eq!(environment_row.get::<String, _>("status"), "active");

    let duplicate_output = run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--project",
            &project_id,
            "--slug",
            "staging",
        ],
    );
    assert_failure(&duplicate_output);
    assert!(
        String::from_utf8_lossy(&duplicate_output.stderr).contains("already exists"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&duplicate_output.stderr)
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn environment_seed_from_copies_active_state_without_runs() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");

    assert_success(&run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &["project", "init"],
    ));
    let project_id = read_project_id_from_dbt_project(repo.project_dir());
    assert_success(&run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--project",
            &project_id,
            "--slug",
            "source",
        ],
    ));
    assert_success(&run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--project",
            &project_id,
            "--slug",
            "target",
            "--kind",
            "ephemeral",
            "--baseline",
            "source",
            "--git-branch",
            "main",
            "--pr-number",
            "123",
        ],
    ));

    let ids = project_environment_ids(db.pool(), &project_id, "source").await;
    let run_id = Uuid::new_v4();
    insert_run(
        db.pool(),
        RunInsert {
            id: 1,
            run_id,
            project_id: ids.project_id,
            environment_id: ids.environment_id,
            command: "run",
            is_full_graph_run: true,
            terminal_status: "success",
        },
    )
    .await;
    insert_manifest(
        db.pool(),
        run_id,
        &manifest_with_nodes([node("model.pkg.a", "seeded")]),
    )
    .await;
    insert_node_execution(
        db.pool(),
        run_id,
        "model.pkg.a",
        "model",
        "success",
        Some("seeded"),
    )
    .await;

    sqlx::query(
        "INSERT INTO promoted_manifest_meta (project_id, environment_id, source_run_id, base_manifest) SELECT $1, $2, $3, manifest FROM manifest_snapshots WHERE run_id = $3",
    )
    .bind(ids.project_id)
    .bind(ids.environment_id)
    .bind(run_id)
    .execute(db.pool())
    .await
    .expect("insert promoted meta");
    sqlx::query(
        "INSERT INTO promoted_manifest_nodes (project_id, environment_id, unique_id, source_run_id, checksum, raw_node) SELECT $1, $2, 'model.pkg.a', $3, 'seeded', manifest -> 'nodes' -> 'model.pkg.a' FROM manifest_snapshots WHERE run_id = $3",
    )
    .bind(ids.project_id)
    .bind(ids.environment_id)
    .bind(run_id)
    .execute(db.pool())
    .await
    .expect("insert promoted node");
    sqlx::query(
        r#"
        INSERT INTO current_node_state (
            project_id, environment_id, unique_id, last_run_id, status, resource_type, node_name,
            node_path, materialized, relation_database, relation_schema, relation_alias,
            relation_name, checksum, started_at, finished_at, execution_time_seconds,
            last_success_at, updated_at
        )
        VALUES ($1, $2, 'model.pkg.a', $3, 'success', 'model', 'a', 'models/a.sql', 'table',
                'warehouse', 'main', 'a', 'warehouse.main.a', 'seeded', NOW(), NOW(), 0.1, NOW(), NOW())
        "#,
    )
    .bind(ids.project_id)
    .bind(ids.environment_id)
    .bind(run_id)
    .execute(db.pool())
    .await
    .expect("insert current state");

    let seed_output = run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &[
            "environment",
            "seed-from",
            "--project",
            &project_id,
            "--target",
            "target",
            "--source",
            "source",
            "--seed-type",
            "clone",
        ],
    );
    assert_success(&seed_output);

    let target_ids = project_environment_ids(db.pool(), &project_id, "target").await;
    let promoted_node_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM promoted_manifest_nodes WHERE project_id = $1 AND environment_id = $2",
    )
    .bind(target_ids.project_id)
    .bind(target_ids.environment_id)
    .fetch_one(db.pool())
    .await
    .expect("promoted node count");
    let current_state_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM current_node_state WHERE project_id = $1 AND environment_id = $2",
    )
    .bind(target_ids.project_id)
    .bind(target_ids.environment_id)
    .fetch_one(db.pool())
    .await
    .expect("current state count");
    let seed_record_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM environment_seeds WHERE target_environment_id = $1",
    )
    .bind(target_ids.environment_id)
    .fetch_one(db.pool())
    .await
    .expect("seed records");
    let runs_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs")
        .fetch_one(db.pool())
        .await
        .expect("runs count");

    assert_eq!(promoted_node_count, 1);
    assert_eq!(current_state_count, 1);
    assert_eq!(seed_record_count, 1);
    assert_eq!(runs_count, 1);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn commit_environment_requires_commit_sha_and_records_version_history() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");

    assert_success(&run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &["project", "init"],
    ));
    let project_id = read_project_id_from_dbt_project(repo.project_dir());

    let missing_sha = run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--project",
            &project_id,
            "--slug",
            "ci-main",
            "--kind",
            "commit",
        ],
    );
    assert_failure(&missing_sha);
    assert!(
        String::from_utf8_lossy(&missing_sha.stderr).contains("require --git-commit-sha"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&missing_sha.stderr)
    );

    assert_success(&run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--project",
            &project_id,
            "--slug",
            "ci-main",
            "--kind",
            "commit",
            "--git-branch",
            "main",
            "--git-commit-sha",
            "abc123",
            "--immutable",
        ],
    ));

    let versions: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM environment_versions ev JOIN environments e ON e.id = ev.environment_id WHERE e.slug = 'ci-main'",
    )
    .fetch_one(db.pool())
    .await
    .expect("environment versions");
    assert_eq!(versions, 1);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn immutable_environment_rejects_identity_updates() {
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;
    let repo = TempProjectRepo::new("proj");

    assert_success(&run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &["project", "init"],
    ));
    let project_id = read_project_id_from_dbt_project(repo.project_dir());

    assert_success(&run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &[
            "environment",
            "create",
            "--project",
            &project_id,
            "--slug",
            "ci-main",
            "--kind",
            "commit",
            "--git-branch",
            "main",
            "--git-commit-sha",
            "abc123",
            "--immutable",
        ],
    ));

    let update = run_dbtx_in_dir(
        db.url(),
        repo.project_dir(),
        &[
            "environment",
            "update",
            "--project",
            &project_id,
            "--slug",
            "ci-main",
            "--git-commit-sha",
            "def456",
        ],
    );
    assert_failure(&update);
    assert!(
        String::from_utf8_lossy(&update.stderr).contains("immutable"),
        "unexpected stderr: {}",
        String::from_utf8_lossy(&update.stderr)
    );
}

struct ScopeIds {
    project_id: i64,
    environment_id: i64,
}

struct RunInsert<'a> {
    id: i64,
    run_id: Uuid,
    project_id: i64,
    environment_id: i64,
    command: &'a str,
    is_full_graph_run: bool,
    terminal_status: &'a str,
}

struct TestDatabase {
    url: String,
    pool: PgPool,
    _container: Option<ContainerAsync<Postgres>>,
}

impl TestDatabase {
    async fn new() -> Self {
        if let Ok(url) = std::env::var("DBTX_TEST_DATABASE_URL") {
            init_dbtx_schema(&url);
            let pool = PgPool::connect(&url)
                .await
                .expect("connect external test db");
            return Self {
                url,
                pool,
                _container: None,
            };
        }

        let container = Postgres::default()
            .with_db_name("dbtx")
            .with_user("dbtx")
            .with_password("dbtx")
            .start()
            .await
            .expect("start postgres container");

        let host = container.get_host().await.expect("postgres host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("postgres port");
        let url = format!("postgres://dbtx:dbtx@{host}:{port}/dbtx");
        init_dbtx_schema(&url);
        let pool = PgPool::connect(&url)
            .await
            .expect("connect testcontainer db");

        Self {
            url,
            pool,
            _container: Some(container),
        }
    }

    fn url(&self) -> &str {
        &self.url
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }
}

async fn scope_ids(pool: &PgPool) -> ScopeIds {
    let project_id: i64 = sqlx::query_scalar(
        "INSERT INTO projects (project_id, project_name, git_repo_url, default_branch, project_root) VALUES ('proj', 'proj', 'https://example.com/repo.git', 'main', '.') ON CONFLICT (project_id) DO UPDATE SET project_name = EXCLUDED.project_name RETURNING id",
    )
    .fetch_one(pool)
    .await
    .expect("project id");
    let environment_id: i64 = sqlx::query_scalar(
        "INSERT INTO environments (project_id, slug, adapter_type, schema_name, threads, profile_config, profile_secrets) VALUES ($1, 'dev', 'duckdb', 'main', 4, '{\"path\":\"warehouse.duckdb\"}'::jsonb, '{}'::jsonb) ON CONFLICT (project_id, slug) DO UPDATE SET slug = EXCLUDED.slug RETURNING id",
    )
    .bind(project_id)
    .fetch_one(pool)
    .await
    .expect("environment id");
    ScopeIds {
        project_id,
        environment_id,
    }
}

async fn project_environment_ids(
    pool: &PgPool,
    project_ref: &str,
    environment_slug: &str,
) -> ScopeIds {
    let row = sqlx::query(
        r#"
        SELECT p.id AS project_id, e.id AS environment_id
        FROM projects p
        JOIN environments e ON e.project_id = p.id
        WHERE p.project_id = $1 AND e.slug = $2
        "#,
    )
    .bind(project_ref)
    .bind(environment_slug)
    .fetch_one(pool)
    .await
    .expect("project/environment row");
    ScopeIds {
        project_id: row.get("project_id"),
        environment_id: row.get("environment_id"),
    }
}

async fn insert_run(pool: &PgPool, run: RunInsert<'_>) {
    sqlx::query(
        r#"
        INSERT INTO runs (
            id, run_id, project_id, environment_id, dbt_invocation_id, command, args,
            is_full_graph_run, started_at, finished_at, exit_code, terminal_status
        )
        VALUES ($1, $2, $3, $4, $2, $5, '[]'::jsonb, $6, NOW(), NOW(), 0, $7)
        "#,
    )
    .bind(run.id)
    .bind(run.run_id)
    .bind(run.project_id)
    .bind(run.environment_id)
    .bind(run.command)
    .bind(run.is_full_graph_run)
    .bind(run.terminal_status)
    .execute(pool)
    .await
    .expect("insert run");
}

async fn insert_manifest(pool: &PgPool, run_id: Uuid, manifest: &serde_json::Value) {
    sqlx::query(
        r#"
        INSERT INTO manifest_snapshots (run_id, manifest, manifest_size_bytes, checksum)
        VALUES ($1, $2, $3, $4)
        "#,
    )
    .bind(run_id)
    .bind(sqlx::types::Json(manifest))
    .bind(serde_json::to_vec(manifest).expect("manifest json").len() as i64)
    .bind(format!(
        "{:x}",
        md5::compute(serde_json::to_vec(manifest).expect("manifest bytes"))
    ))
    .execute(pool)
    .await
    .expect("insert manifest");
}

async fn insert_node_execution(
    pool: &PgPool,
    run_id: Uuid,
    unique_id: &str,
    resource_type: &str,
    status: &str,
    checksum: Option<&str>,
) {
    sqlx::query(
        r#"
        INSERT INTO node_executions (
            run_id, unique_id, resource_type, node_name, node_path, materialized, status,
            checksum, started_at, finished_at, updated_at
        )
        VALUES ($1, $2, $3, $4, $5, 'table', $6, $7, NOW(), NOW(), NOW())
        "#,
    )
    .bind(run_id)
    .bind(unique_id)
    .bind(resource_type)
    .bind(unique_id)
    .bind(format!("{unique_id}.sql"))
    .bind(status)
    .bind(checksum)
    .execute(pool)
    .await
    .expect("insert node execution");
}

fn node(unique_id: &str, checksum: &str) -> serde_json::Value {
    json!({
        "unique_id": unique_id,
        "resource_type": "model",
        "name": unique_id,
        "database": "warehouse",
        "schema": "main",
        "alias": unique_id,
        "relation_name": unique_id,
        "config": {"materialized": "table"},
        "checksum": {"checksum": checksum},
        "depends_on": {"nodes": []}
    })
}

fn manifest_with_nodes<const N: usize>(nodes: [serde_json::Value; N]) -> serde_json::Value {
    let nodes = nodes
        .into_iter()
        .map(|node| {
            let unique_id = node["unique_id"]
                .as_str()
                .expect("node unique_id")
                .to_string();
            (unique_id, node)
        })
        .collect::<serde_json::Map<_, _>>();
    json!({
        "metadata": {
            "dbt_schema_version": "https://schemas.getdbt.com/dbt/manifest/v12.json",
            "dbt_version": "2.0.0",
            "generated_at": "2026-01-01T00:00:00Z",
            "invocation_id": Uuid::nil(),
            "project_name": "proj",
            "project_id": "proj",
            "adapter_type": "duckdb",
            "env": {}
        },
        "nodes": nodes,
        "sources": {},
        "parent_map": {},
        "child_map": {},
        "macros": {},
        "docs": {},
        "exposures": {},
        "groups": {},
        "group_map": {},
        "metrics": {},
        "selectors": {},
        "semantic_models": {},
        "saved_queries": {},
        "unit_tests": {},
        "disabled": {},
        "functions": {}
    })
}

fn init_dbtx_schema(database_url: &str) {
    let output = Command::new(env!("CARGO_BIN_EXE_dbtx"))
        .args(["state", "migrate"])
        .env("DBTX_DATABASE_URL", database_url)
        .output()
        .expect("run dbtx migrate");
    assert!(
        output.status.success(),
        "dbtx init failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn run_replay(database_url: &str, run_id: Uuid) {
    let output = Command::new(env!("CARGO_BIN_EXE_dbtx"))
        .args(["replay", "--run-id", &run_id.to_string()])
        .env("DBTX_DATABASE_URL", database_url)
        .output()
        .expect("run replay");
    assert!(
        output.status.success(),
        "replay failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn run_dbtx_in_dir(database_url: &str, cwd: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_dbtx"))
        .args(args)
        .env("DBTX_DATABASE_URL", database_url)
        .env("DBTX_SECRET_KEY", "test-secret-key")
        .current_dir(cwd)
        .output()
        .expect("run dbtx in dir")
}

struct TempProjectRepo {
    _temp_dir: TempDir,
    project_dir: PathBuf,
}

impl TempProjectRepo {
    fn new(project_name: &str) -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        let project_dir = temp_dir.path().join("analytics");
        fs::create_dir_all(&project_dir).expect("create project dir");
        fs::write(
            project_dir.join("dbt_project.yml"),
            format!("name: {project_name}\nprofile: {project_name}\nversion: '1.0'\n"),
        )
        .expect("write dbt project");
        fs::write(
            project_dir.join("profiles.yml"),
            format!(
                "{project_name}:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: warehouse.duckdb\n      schema: main\n      threads: 4\n"
            ),
        )
        .expect("write profiles");
        git(&["init", "-b", "main"], temp_dir.path());
        git(
            &["config", "user.email", "dbtx@example.com"],
            temp_dir.path(),
        );
        git(&["config", "user.name", "dbtx"], temp_dir.path());
        git(
            &["remote", "add", "origin", "https://example.com/repo.git"],
            temp_dir.path(),
        );
        git(&["add", "."], temp_dir.path());
        git(&["commit", "-m", "initial"], temp_dir.path());
        Self {
            _temp_dir: temp_dir,
            project_dir,
        }
    }

    fn project_dir(&self) -> &Path {
        &self.project_dir
    }
}

fn read_project_id_from_dbt_project(project_dir: &Path) -> String {
    let content = fs::read_to_string(project_dir.join("dbtx.toml")).expect("read dbtx config");
    let config: toml::Value = toml::from_str(&content).expect("parse dbtx config");
    config
        .get("project")
        .and_then(toml::Value::as_table)
        .and_then(|table| table.get("id"))
        .and_then(toml::Value::as_str)
        .expect("project id")
        .to_string()
}

fn git(args: &[&str], cwd: &Path) {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("run git");
    assert!(
        output.status.success(),
        "git failed\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn assert_success(output: &std::process::Output) {
    assert!(
        output.status.success(),
        "command failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
        output.status.code(),
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn assert_failure(output: &std::process::Output) {
    assert!(
        !output.status.success(),
        "expected failure but command succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

async fn reset_db(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE environment_seeds, promoted_manifest_nodes, promoted_manifest_meta, current_node_state, manifest_edges, manifest_nodes, manifest_snapshots, node_executions, run_events, runs, environments, projects CASCADE",
    )
    .execute(pool)
    .await
    .expect("truncate db");
}
