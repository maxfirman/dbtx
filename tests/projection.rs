use serde_json::json;
use sqlx::{PgPool, Row};
use std::process::Command;
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{runners::AsyncRunner, ContainerAsync},
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
    insert_manifest(db.pool(), run_id, &manifest_with_nodes([node("model.pkg.a", "old")])).await;
    insert_node_execution(db.pool(), run_id, "model.pkg.a", "model", "success", Some("old")).await;

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
        &manifest_with_nodes([
            node("model.pkg.a", "old-a"),
            node("model.pkg.b", "old-b"),
        ]),
    )
    .await;
    insert_node_execution(db.pool(), base_run_id, "model.pkg.a", "model", "success", Some("old-a"))
        .await;
    insert_node_execution(db.pool(), base_run_id, "model.pkg.b", "model", "success", Some("old-b"))
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
        &manifest_with_nodes([
            node("model.pkg.a", "new-a"),
            node("model.pkg.b", "new-b"),
        ]),
    )
    .await;
    insert_node_execution(db.pool(), partial_run_id, "model.pkg.a", "model", "success", Some("new-a"))
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
    insert_node_execution(db.pool(), failed_run_id, "model.pkg.b", "model", "failed", Some("failed-b"))
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
    assert_eq!(promoted[0].get::<Option<String>, _>("checksum").as_deref(), Some("new-a"));
    assert_eq!(promoted[0].get::<Uuid, _>("source_run_id"), partial_run_id);
    assert_eq!(promoted[1].get::<String, _>("unique_id"), "model.pkg.b");
    assert_eq!(promoted[1].get::<Option<String>, _>("checksum").as_deref(), Some("old-b"));
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
    assert_eq!(current[0].get::<Option<String>, _>("checksum").as_deref(), Some("new-a"));
    assert_eq!(current[1].get::<String, _>("unique_id"), "model.pkg.b");
    assert_eq!(current[1].get::<Uuid, _>("last_run_id"), failed_run_id);
    assert_eq!(current[1].get::<String, _>("status"), "failed");
    assert_eq!(current[1].get::<Option<String>, _>("checksum").as_deref(), Some("old-b"));
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
            let pool = PgPool::connect(&url).await.expect("connect external test db");
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
        let pool = PgPool::connect(&url).await.expect("connect testcontainer db");

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
        "INSERT INTO projects (slug) VALUES ('proj') ON CONFLICT (slug) DO UPDATE SET slug = EXCLUDED.slug RETURNING id",
    )
    .fetch_one(pool)
    .await
    .expect("project id");
    let environment_id: i64 = sqlx::query_scalar(
        "INSERT INTO environments (project_id, slug) VALUES ($1, 'dev') ON CONFLICT (project_id, slug) DO UPDATE SET slug = EXCLUDED.slug RETURNING id",
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
    .bind(format!("{:x}", md5::compute(serde_json::to_vec(manifest).expect("manifest bytes"))))
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
        .args(["state", "init"])
        .env("DBTX_DATABASE_URL", database_url)
        .output()
        .expect("run dbtx init");
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

async fn reset_db(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE promoted_manifest_nodes, promoted_manifest_meta, current_node_state, manifest_edges, manifest_nodes, manifest_snapshots, node_executions, run_events, runs, environments, projects CASCADE",
    )
    .execute(pool)
    .await
    .expect("truncate db");
}
