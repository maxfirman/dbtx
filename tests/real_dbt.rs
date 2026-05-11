#![allow(clippy::await_holding_lock)]

use dbtx::api::{InvocationCommandApi, InvocationCreateApiRequest, InvocationLifecycleStatus};
use dbtx::client::DaemonClient;
use dbtx::services::{infer_local_project_defaults, infer_remote_project_defaults};
use serde_json::Value;
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};
use std::fs;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Output, Stdio};
use std::sync::{Mutex, OnceLock};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, runners::AsyncRunner},
};
use uuid::Uuid;

const ENVIRONMENT_SLUG: &str = "dev";

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_ls_opportunistically_creates_local_project_state() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();

    let output = run_dbtx(
        db.service_url(),
        &project,
        &[
            "ls",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
        ],
    );
    assert_success(&output);
    assert_eq!(
        project.project_id(),
        infer_local_project_defaults(project.path(), None, None, None)
            .expect("infer local project")
            .project_id
    );
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_run_persists_real_jaffle_state() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();
    project.init_dbtx_project(db.service_url());

    let output = run_dbtx(
        db.service_url(),
        &project,
        &[
            "run",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
            "--select",
            "stg_customers",
        ],
    );
    assert_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("dbt-fusion 2.0.0-preview."),
        "expected dbt version line in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("Loading ") && stdout.contains("profiles.yml"),
        "expected text-mode loading line in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("Succeeded ["),
        "expected model result line in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("Finished 'run' successfully"),
        "expected execution summary in stdout, got: {stdout}"
    );
    assert!(
        !stdout.contains("\"info\":"),
        "expected rendered text output rather than raw json, got: {stdout}"
    );

    let run_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs")
        .fetch_one(db.pool())
        .await
        .expect("run count");
    let manifest_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM manifest_snapshots")
        .fetch_one(db.pool())
        .await
        .expect("manifest count");
    let node_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM current_node_state")
        .fetch_one(db.pool())
        .await
        .expect("node count");
    let status: String = sqlx::query("SELECT terminal_status FROM runs ORDER BY id DESC LIMIT 1")
        .fetch_one(db.pool())
        .await
        .expect("run row")
        .get("terminal_status");
    let run_row = sqlx::query(
        "SELECT git_branch, git_commit_sha, git_repo_url, project_root, project_name, project_ref FROM runs ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(db.pool())
    .await
    .expect("run provenance row");

    assert_eq!(run_count, 1);
    assert_eq!(manifest_count, 1);
    assert_eq!(node_count, 1);
    assert_eq!(status, "success");
    assert_eq!(
        run_row.get::<Option<String>, _>("git_branch").as_deref(),
        Some("main")
    );
    assert_eq!(
        run_row
            .get::<Option<String>, _>("git_commit_sha")
            .as_deref(),
        Some(project.head_sha().as_str())
    );
    assert_eq!(
        run_row.get::<Option<String>, _>("git_repo_url").as_deref(),
        Some("https://example.com/jaffle_shop_project.git")
    );
    assert_eq!(
        run_row.get::<Option<String>, _>("project_root").as_deref(),
        Some(project.path_str())
    );
    assert_eq!(
        run_row.get::<Option<String>, _>("project_name").as_deref(),
        Some("jaffle_shop_project")
    );
    assert_eq!(
        run_row.get::<Option<String>, _>("project_ref").as_deref(),
        Some(project.project_id().as_str())
    );
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_run_respects_project_dir_when_called_outside_project_root() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();
    project.init_dbtx_project(db.service_url());
    let outside_dir = TempDir::new().expect("outside dir");

    let output = run_dbtx_in_cwd(
        db.service_url(),
        outside_dir.path(),
        ENVIRONMENT_SLUG,
        &[
            "run",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
            "--select",
            "stg_customers",
        ],
    );
    assert_success(&output);
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn remote_worker_executes_commit_pinned_invocation_from_git_cache() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();

    bootstrap_remote_project_and_env(
        db.pool(),
        &project,
        "remote",
        &project.head_sha(),
        project
            .path()
            .join("warehouse.duckdb")
            .to_string_lossy()
            .as_ref(),
    )
    .await;

    let client = DaemonClient::new(db.service_url().to_string());
    let invocation = client
        .invocation_create(InvocationCreateApiRequest {
            command: InvocationCommandApi::Run,
            args: vec!["--select".to_string(), "stg_customers".to_string()],
            current_dir: None,
            project_id: Some(project.remote_project_id()),
            environment_slug: Some("remote".to_string()),
        })
        .await
        .expect("create remote invocation");

    let git_cache = TempDir::new().expect("git cache dir");
    let worker = Command::new(env!("CARGO_BIN_EXE_dbtx-worker"))
        .args([
            "--service-url",
            db.service_url(),
            "--execution-mode",
            "server",
            "--queue",
            "generic",
            "--once",
        ])
        .env("DBTX_GIT_CACHE_DIR", git_cache.path())
        .output()
        .expect("run remote worker");
    assert_success(&worker);

    let status = wait_for_invocation_terminal(&client, invocation.invocation_id).await;
    assert!(matches!(
        status.status,
        InvocationLifecycleStatus::Succeeded
    ));
    assert_eq!(status.exit_code, Some(0));

    let run_row = sqlx::query(
        "SELECT git_repo_url, git_commit_sha, project_root, project_ref FROM runs ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(db.pool())
    .await
    .expect("remote run row");
    assert_eq!(
        run_row.get::<Option<String>, _>("git_repo_url").as_deref(),
        Some(project.path_str())
    );
    assert_eq!(
        run_row
            .get::<Option<String>, _>("git_commit_sha")
            .as_deref(),
        Some(project.head_sha().as_str())
    );
    assert_eq!(
        run_row.get::<Option<String>, _>("project_root").as_deref(),
        Some(".")
    );
    assert_eq!(
        run_row.get::<Option<String>, _>("project_ref").as_deref(),
        Some(project.remote_project_id().as_str())
    );

    let selected_rows = sqlx::query(
        r#"
        SELECT unique_id, resource_type, finished_at, close_reason
        FROM invocation_selected_resources
        WHERE invocation_id = $1
        ORDER BY unique_id
        "#,
    )
    .bind(invocation.invocation_id)
    .fetch_all(db.pool())
    .await
    .expect("selected resource rows");
    assert_eq!(selected_rows.len(), 1);
    assert_eq!(
        selected_rows[0].get::<String, _>("unique_id"),
        "model.jaffle_shop_project.stg_customers"
    );
    assert_eq!(
        selected_rows[0]
            .get::<Option<String>, _>("resource_type")
            .as_deref(),
        Some("model")
    );
    assert!(
        selected_rows[0]
            .get::<Option<chrono::DateTime<chrono::Utc>>, _>("finished_at")
            .is_some()
    );
    assert_eq!(
        selected_rows[0]
            .get::<Option<String>, _>("close_reason")
            .as_deref(),
        Some("completed")
    );

    let repo_hash = short_hash(project.path_str());
    let mirror_dir = git_cache
        .path()
        .join("mirrors")
        .join(format!("{repo_hash}.git"));
    let worktree_dir = git_cache
        .path()
        .join("worktrees")
        .join(repo_hash)
        .join(project.head_sha());
    assert!(
        mirror_dir.is_dir(),
        "expected mirror at {}",
        mirror_dir.display()
    );
    assert!(
        worktree_dir.is_dir(),
        "expected worktree at {}",
        worktree_dir.display()
    );
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_build_persists_real_jaffle_state() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.init_dbtx_project(db.service_url());

    let output = run_dbtx(
        db.service_url(),
        &project,
        &[
            "build",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
        ],
    );
    assert_success(&output);

    let status: String = sqlx::query("SELECT terminal_status FROM runs ORDER BY id DESC LIMIT 1")
        .fetch_one(db.pool())
        .await
        .expect("build run row")
        .get("terminal_status");
    let command: String = sqlx::query("SELECT command FROM runs ORDER BY id DESC LIMIT 1")
        .fetch_one(db.pool())
        .await
        .expect("build command row")
        .get("command");
    let model_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM current_node_state WHERE resource_type = 'model'")
            .fetch_one(db.pool())
            .await
            .expect("model count");

    assert_eq!(command, "build");
    assert_eq!(status, "success");
    assert!(
        model_count >= 6,
        "expected build to persist multiple models, got {model_count}"
    );
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_seed_persists_seed_state() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.init_dbtx_project(db.service_url());

    let output = run_dbtx(
        db.service_url(),
        &project,
        &[
            "seed",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
        ],
    );
    assert_success(&output);

    let command: String = sqlx::query("SELECT command FROM runs ORDER BY id DESC LIMIT 1")
        .fetch_one(db.pool())
        .await
        .expect("seed command row")
        .get("command");
    let seed_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM current_node_state WHERE resource_type = 'seed'")
            .fetch_one(db.pool())
            .await
            .expect("seed node count");
    let promoted_node_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM promoted_manifest_nodes")
            .fetch_one(db.pool())
            .await
            .expect("promoted node count");
    let promoted_meta_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM promoted_manifest_meta")
            .fetch_one(db.pool())
            .await
            .expect("promoted meta count");

    assert_eq!(command, "seed");
    assert_eq!(seed_count, 6);
    assert_eq!(promoted_node_count, 0);
    assert_eq!(promoted_meta_count, 0);
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_test_persists_test_state() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();
    project.init_dbtx_project(db.service_url());
    assert_success(&run_dbtx(
        db.service_url(),
        &project,
        &[
            "run",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
        ],
    ));
    let promoted_node_count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM promoted_manifest_nodes")
            .fetch_one(db.pool())
            .await
            .expect("promoted node count before test");
    let promoted_meta_source_before: Uuid =
        sqlx::query_scalar("SELECT source_run_id FROM promoted_manifest_meta LIMIT 1")
            .fetch_one(db.pool())
            .await
            .expect("promoted meta source before test");

    let output = run_dbtx(
        db.service_url(),
        &project,
        &[
            "test",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
            "--select",
            "stg_customers",
        ],
    );
    assert_success(&output);

    let command: String = sqlx::query("SELECT command FROM runs ORDER BY id DESC LIMIT 1")
        .fetch_one(db.pool())
        .await
        .expect("test command row")
        .get("command");
    let test_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM node_executions ne JOIN runs r ON r.run_id = ne.run_id WHERE r.command = 'test' AND ne.resource_type = 'test'",
    )
    .fetch_one(db.pool())
    .await
    .expect("test node count");
    let promoted_node_count_after: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM promoted_manifest_nodes")
            .fetch_one(db.pool())
            .await
            .expect("promoted node count after test");
    let promoted_meta_source_after: Uuid =
        sqlx::query_scalar("SELECT source_run_id FROM promoted_manifest_meta LIMIT 1")
            .fetch_one(db.pool())
            .await
            .expect("promoted meta source after test");

    assert_eq!(command, "test");
    assert!(
        test_count >= 1,
        "expected persisted test node executions, got {test_count}"
    );
    assert_eq!(promoted_node_count_after, promoted_node_count_before);
    assert_eq!(promoted_meta_source_after, promoted_meta_source_before);
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_ls_uses_real_project_and_does_not_write_runs() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();
    project.init_dbtx_project(db.service_url());
    assert_success(&run_dbtx(
        db.service_url(),
        &project,
        &[
            "run",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
            "--select",
            "stg_customers",
        ],
    ));

    let runs_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs")
        .fetch_one(db.pool())
        .await
        .expect("runs before");
    let events_before: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM run_events")
        .fetch_one(db.pool())
        .await
        .expect("events before");

    let target_manifest = project.path().join("target").join("manifest.json");
    if target_manifest.exists() {
        fs::remove_file(&target_manifest).expect("remove manifest");
    }

    let output = run_dbtx(
        db.service_url(),
        &project,
        &[
            "ls",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
            "--select",
            "stg_customers",
            "--output",
            "json",
        ],
    );
    assert_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("\"unique_id\":\"model.jaffle_shop_project.stg_customers\""),
        "expected model in stdout, got: {stdout}"
    );

    let runs_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs")
        .fetch_one(db.pool())
        .await
        .expect("runs after");
    let events_after: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM run_events")
        .fetch_one(db.pool())
        .await
        .expect("events after");

    assert_eq!(runs_after, runs_before);
    assert_eq!(events_after, events_before);
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_ls_without_prior_state_succeeds() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();
    project.init_dbtx_project(db.service_url());

    let output = run_dbtx(
        db.service_url(),
        &project,
        &[
            "ls",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
            "--select",
            "stg_customers",
            "--output",
            "json",
        ],
    );
    assert_success(&output);

    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("\"unique_id\":\"model.jaffle_shop_project.stg_customers\""),
        "expected model in stdout, got: {stdout}"
    );

    let runs_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs")
        .fetch_one(db.pool())
        .await
        .expect("runs count");
    let events_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM run_events")
        .fetch_one(db.pool())
        .await
        .expect("events count");

    assert_eq!(runs_count, 0);
    assert_eq!(events_count, 0);
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_ls_state_modified_without_prior_state_returns_all_node_types() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();
    project.init_dbtx_project(db.service_url());

    let modified = modified_unique_ids(db.service_url(), &project);
    assert!(
        modified.contains("model.jaffle_shop_project.stg_customers"),
        "expected clean-state modified selector to include models, got: {modified:?}"
    );
    assert!(
        modified.contains("seed.jaffle_shop_project.raw_customers"),
        "expected clean-state modified selector to include seeds, got: {modified:?}"
    );
    assert!(
        modified.contains("test.jaffle_shop_project.not_null_stg_customers_customer_id.e2cfb1f9aa"),
        "expected clean-state modified selector to include tests, got: {modified:?}"
    );

    let runs_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM runs")
        .fetch_one(db.pool())
        .await
        .expect("runs count");
    assert_eq!(runs_count, 0);
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_full_run_clears_state_modified() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();
    project.init_dbtx_project(db.service_url());

    let output = run_dbtx(
        db.service_url(),
        &project,
        &[
            "run",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
        ],
    );
    assert_success(&output);

    let modified = modified_unique_ids(db.service_url(), &project);
    assert!(
        modified.is_empty(),
        "expected state:modified to be empty after full run, got: {modified:?}"
    );
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_build_state_modified_clears_modified_set() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();
    project.init_dbtx_project(db.service_url());

    assert_success(&run_dbtx(
        db.service_url(),
        &project,
        &[
            "run",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
        ],
    ));

    project.append_to_file(
        "models/staging/stg_customers.sql",
        "\n-- dbtx build state:modified marker\n",
    );

    let modified_before = modified_unique_ids(db.service_url(), &project);
    assert!(
        modified_before.contains("model.jaffle_shop_project.stg_customers"),
        "expected modified model before state:modified build, got: {modified_before:?}"
    );

    let output = run_dbtx(
        db.service_url(),
        &project,
        &[
            "build",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
            "-s",
            "state:modified",
        ],
    );
    assert_success(&output);

    let modified_after = modified_unique_ids(db.service_url(), &project);
    assert!(
        modified_after.is_empty(),
        "expected state:modified to be empty after build -s state:modified, got: {modified_after:?}"
    );
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_ls_reports_modified_model_after_file_change() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();
    project.init_dbtx_project(db.service_url());

    assert_success(&run_dbtx(
        db.service_url(),
        &project,
        &[
            "run",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
        ],
    ));

    project.append_to_file(
        "models/staging/stg_customers.sql",
        "\n-- dbtx integration modified marker\n",
    );

    let modified = modified_unique_ids(db.service_url(), &project);
    assert!(
        modified.contains("model.jaffle_shop_project.stg_customers"),
        "expected stg_customers to be reported as modified, got: {modified:?}"
    );
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_failed_run_keeps_only_unsuccessful_modified_models() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();
    project.init_dbtx_project(db.service_url());

    assert_success(&run_dbtx(
        db.service_url(),
        &project,
        &[
            "run",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
        ],
    ));

    project.append_to_file(
        "models/staging/stg_orders.sql",
        "\n-- dbtx integration modified upstream marker\n",
    );
    project.replace_in_file(
        "models/marts/orders.sql",
        "select * from customer_order_count\n",
        "select *, missing_dbtx_column from customer_order_count\n",
    );

    let modified_before = modified_unique_ids(db.service_url(), &project);
    assert!(
        modified_before.contains("model.jaffle_shop_project.stg_orders"),
        "expected stg_orders to be modified before rerun, got: {modified_before:?}"
    );
    assert!(
        modified_before.contains("model.jaffle_shop_project.orders"),
        "expected orders to be modified before rerun, got: {modified_before:?}"
    );

    let failed_run = run_dbtx(
        db.service_url(),
        &project,
        &[
            "run",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
            "--select",
            "stg_orders orders",
        ],
    );
    assert_failure(&failed_run);

    let modified_after = modified_unique_ids(db.service_url(), &project);
    assert!(
        !modified_after.contains("model.jaffle_shop_project.stg_orders"),
        "expected successful upstream model to be removed from modified set, got: {modified_after:?}"
    );
    assert!(
        modified_after.contains("model.jaffle_shop_project.orders"),
        "expected failed downstream model to remain modified, got: {modified_after:?}"
    );
}

struct RealProject {
    temp_dir: TempDir,
}

impl RealProject {
    fn new() -> Self {
        let temp_dir = TempDir::new().expect("temp dir");
        copy_dir_all(&jaffle_fixture_dir(), temp_dir.path());
        clean_runtime_artifacts(temp_dir.path());
        git_cmd(["init", "-b", "main"], temp_dir.path());
        git_cmd(
            ["config", "user.email", "dbtx@example.com"],
            temp_dir.path(),
        );
        git_cmd(["config", "user.name", "dbtx"], temp_dir.path());
        git_cmd(
            [
                "remote",
                "add",
                "origin",
                "https://example.com/jaffle_shop_project.git",
            ],
            temp_dir.path(),
        );
        git_cmd(["add", "."], temp_dir.path());
        git_cmd(["commit", "-m", "initial"], temp_dir.path());
        Self { temp_dir }
    }

    fn path(&self) -> &Path {
        self.temp_dir.path()
    }

    fn path_str(&self) -> &str {
        self.path().to_str().expect("utf8 path")
    }

    fn project_id(&self) -> String {
        infer_local_project_defaults(self.path(), None, None, None)
            .expect("infer local project")
            .project_id
    }

    fn remote_project_id(&self) -> String {
        infer_remote_project_defaults(self.path(), None, None, None)
            .expect("infer remote project")
            .project_id
    }

    fn head_sha(&self) -> String {
        let output = Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(self.path())
            .output()
            .expect("git rev-parse");
        assert_success(&output);
        String::from_utf8_lossy(&output.stdout).trim().to_string()
    }

    fn seed(&self) {
        let mut command = Command::new("dbt");
        command.args([
            "seed",
            "--project-dir",
            self.path_str(),
            "--profiles-dir",
            self.path_str(),
        ]);
        command.current_dir(self.path());
        let output = command.output().expect("run dbt seed");
        assert_success(&output);
    }

    fn init_dbtx_project(&self, service_url: &str) -> String {
        let _ = service_url;
        self.project_id()
    }

    fn append_to_file(&self, relative_path: &str, content: &str) {
        let path = self.path().join(relative_path);
        let mut existing = fs::read_to_string(&path).expect("read file");
        existing.push_str(content);
        fs::write(path, existing).expect("write file");
    }

    fn replace_in_file(&self, relative_path: &str, from: &str, to: &str) {
        let path = self.path().join(relative_path);
        let existing = fs::read_to_string(&path).expect("read file");
        let updated = existing.replace(from, to);
        assert_ne!(
            existing, updated,
            "expected replacement to modify file {relative_path}"
        );
        fs::write(path, updated).expect("write file");
    }
}

async fn bootstrap_remote_project_and_env(
    pool: &PgPool,
    project: &RealProject,
    environment_slug: &str,
    commit_sha: &str,
    duckdb_path: &str,
) {
    let project_name = fs::read_to_string(project.path().join("dbt_project.yml"))
        .expect("read dbt_project")
        .lines()
        .find_map(|line| line.strip_prefix("name: ").map(str::to_string))
        .expect("project name");

    let project_row = sqlx::query(
        r#"
        INSERT INTO projects (project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata)
        VALUES ($1, $2, 'remote', $3, 'main', '.', '{}'::jsonb)
        ON CONFLICT (project_id) DO UPDATE
        SET project_name = EXCLUDED.project_name,
            mode = EXCLUDED.mode,
            git_repo_url = EXCLUDED.git_repo_url,
            default_branch = EXCLUDED.default_branch,
            project_root = EXCLUDED.project_root
        RETURNING id
        "#,
    )
    .bind(project.remote_project_id())
    .bind(&project_name)
    .bind(project.path_str())
    .fetch_one(pool)
    .await
    .expect("upsert remote project");
    let project_pk: i64 = project_row.get("id");

    let environment_row = sqlx::query(
        r#"
        INSERT INTO environments (
            project_id, slug, profile_name, target_name, git_branch, git_commit_sha,
            use_latest_commit, auto_reconcile, immutable, status, adapter_type, worker_queue,
            schema_name, threads, profile_config, profile_secrets, metadata
        )
        VALUES ($1, $2, $3, 'dev', 'main', $4, false, true, false, 'active', 'duckdb', 'generic',
                'main', 4, jsonb_build_object('path', $5::text), '{}'::jsonb, '{}'::jsonb)
        ON CONFLICT (project_id, slug) DO UPDATE
        SET git_branch = EXCLUDED.git_branch,
            git_commit_sha = EXCLUDED.git_commit_sha,
            profile_config = EXCLUDED.profile_config
        RETURNING id
        "#,
    )
    .bind(project_pk)
    .bind(environment_slug)
    .bind(&project_name)
    .bind(commit_sha)
    .bind(duckdb_path)
    .fetch_one(pool)
    .await
    .expect("upsert remote environment");
    let environment_pk: i64 = environment_row.get("id");

    sqlx::query(
        r#"
        INSERT INTO environment_versions (
            environment_id, project_id, reason, git_branch, git_commit_sha,
            use_latest_commit, auto_reconcile, immutable, baseline_environment_id, metadata
        )
        VALUES ($1, $2, 'created', 'main', $3, false, true, false, NULL, '{}'::jsonb)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(environment_pk)
    .bind(project_pk)
    .bind(commit_sha)
    .execute(pool)
    .await
    .expect("insert environment version");
}

struct TestDatabase {
    daemon: TestDaemon,
    pool: PgPool,
    _container: Option<ContainerAsync<Postgres>>,
}

impl TestDatabase {
    async fn new() -> Self {
        if let Ok(url) = std::env::var("DBTX_TEST_DATABASE_URL") {
            let daemon = TestDaemon::start(&url);
            init_dbtx_schema(daemon.service_url());
            let pool = PgPool::connect(&url)
                .await
                .expect("connect external test db");
            return Self {
                daemon,
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
        let daemon = TestDaemon::start(&url);
        init_dbtx_schema(daemon.service_url());
        let pool = PgPool::connect(&url)
            .await
            .expect("connect testcontainer db");

        Self {
            daemon,
            pool,
            _container: Some(container),
        }
    }

    fn service_url(&self) -> &str {
        self.daemon.service_url()
    }

    fn pool(&self) -> &PgPool {
        &self.pool
    }
}

struct TestDaemon {
    service_url: String,
    child: Child,
}

impl TestDaemon {
    fn start(database_url: &str) -> Self {
        let listen = next_listen_addr();
        let mut child = Command::new(env!("CARGO_BIN_EXE_dbtx-server"))
            .args(["--listen", &listen])
            .env("DBTX_DATABASE_URL", database_url)
            .env("DBTX_SECRET_KEY", "test-secret-key")
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("start dbtx-server");

        let service_url = format!("http://{listen}");
        wait_for_server(&service_url, &mut child);
        Self { service_url, child }
    }

    fn service_url(&self) -> &str {
        &self.service_url
    }
}

impl Drop for TestDaemon {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn next_listen_addr() -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral port");
    let addr = listener.local_addr().expect("local addr");
    drop(listener);
    addr.to_string()
}

fn wait_for_server(service_url: &str, child: &mut Child) {
    let deadline = Instant::now() + Duration::from_secs(30);
    let addr = service_url.trim_start_matches("http://");
    loop {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        if let Some(status) = child.try_wait().expect("poll dbtx-server") {
            panic!("dbtx-server exited early with status {status}");
        }
        if Instant::now() >= deadline {
            panic!("timed out waiting for dbtx-server at {service_url}");
        }
        thread::sleep(Duration::from_millis(50));
    }
}

fn init_dbtx_schema(service_url: &str) {
    let output = Command::new(env!("CARGO_BIN_EXE_dbtx"))
        .args(["state", "migrate"])
        .env("DBTX_SERVICE_URL", service_url)
        .output()
        .expect("run dbtx migrate");
    assert_success(&output);
}

async fn reset_db(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE environment_seeds, promoted_manifest_nodes, promoted_manifest_meta, current_node_state, manifest_edges, manifest_nodes, manifest_snapshots, node_executions, run_events, runs, environments, projects CASCADE",
    )
    .execute(pool)
    .await
    .expect("truncate db");
}

fn run_dbtx(service_url: &str, project: &RealProject, args: &[&str]) -> Output {
    run_dbtx_with_environment_slug(
        service_url,
        project,
        environment_slug_from_args(args).unwrap_or(ENVIRONMENT_SLUG),
        args,
    )
}

fn run_dbtx_with_environment_slug(
    service_url: &str,
    project: &RealProject,
    environment_slug: &str,
    args: &[&str],
) -> Output {
    run_dbtx_in_cwd(service_url, project.path(), environment_slug, args)
}

fn run_dbtx_in_cwd(service_url: &str, cwd: &Path, environment_slug: &str, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dbtx"));
    command.args(strip_profiles_dir_args(args));
    command.env("DBTX_SERVICE_URL", service_url);
    command.env("DBTX_ENVIRONMENT_SLUG", environment_slug);
    command.current_dir(cwd);
    command.output().expect("run dbtx")
}

fn strip_profiles_dir_args<'a>(args: &'a [&'a str]) -> Vec<&'a str> {
    let mut filtered = Vec::with_capacity(args.len());
    let mut idx = 0;
    while idx < args.len() {
        let current = args[idx];
        if current == "--profiles-dir" {
            idx += 2;
            continue;
        }
        if current.starts_with("--profiles-dir=") {
            idx += 1;
            continue;
        }
        filtered.push(current);
        idx += 1;
    }
    filtered
}

fn environment_slug_from_args<'a>(args: &'a [&'a str]) -> Option<&'a str> {
    let mut idx = 0;
    while idx < args.len() {
        if args[idx] == "--target" {
            return args.get(idx + 1).copied();
        }
        if let Some((flag, value)) = args[idx].split_once('=')
            && flag == "--target"
        {
            return Some(value);
        }
        idx += 1;
    }
    None
}

fn assert_success(output: &Output) {
    if !output.status.success() {
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "command failed with status {:?}\nstdout:\n{}\nstderr:\n{}",
            output.status.code(),
            stdout,
            stderr
        );
    }
}

fn assert_failure(output: &Output) {
    assert!(
        !output.status.success(),
        "expected command to fail but it succeeded\nstdout:\n{}\nstderr:\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr),
    );
}

fn modified_unique_ids(
    service_url: &str,
    project: &RealProject,
) -> std::collections::BTreeSet<String> {
    modified_unique_ids_for_env(service_url, project, ENVIRONMENT_SLUG)
}

fn modified_unique_ids_for_env(
    service_url: &str,
    project: &RealProject,
    environment_slug: &str,
) -> std::collections::BTreeSet<String> {
    listed_unique_ids_for_env_impl(
        service_url,
        project,
        environment_slug,
        &["-s", "state:modified", "--output", "json"],
    )
}

fn listed_unique_ids_for_env_impl(
    service_url: &str,
    project: &RealProject,
    environment_slug: &str,
    extra_args: &[&str],
) -> std::collections::BTreeSet<String> {
    let mut args = vec![
        "ls",
        "--project-dir",
        project.path_str(),
        "--profiles-dir",
        project.path_str(),
    ];
    args.extend_from_slice(extra_args);

    let output = run_dbtx_with_environment_slug(service_url, project, environment_slug, &args);
    assert_success(&output);

    String::from_utf8_lossy(&output.stdout)
        .lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|value| {
            value
                .get("unique_id")
                .and_then(Value::as_str)
                .map(ToString::to_string)
        })
        .collect()
}

async fn wait_for_invocation_terminal(
    client: &DaemonClient,
    invocation_id: Uuid,
) -> dbtx::api::InvocationStatusResponse {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        let status = client
            .invocation_status(invocation_id)
            .await
            .expect("invocation status");
        if !matches!(status.status, InvocationLifecycleStatus::Running) {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for invocation completion"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn short_hash(input: &str) -> String {
    let digest = Sha256::digest(input.as_bytes());
    format!("{digest:x}").chars().take(20).collect()
}

fn jaffle_fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/jaffle_shop_project")
        .canonicalize()
        .expect("jaffle fixture dir")
}

fn clean_runtime_artifacts(project_dir: &Path) {
    for entry in ["target", "logs", "warehouse.duckdb", "dbtx.toml"] {
        let path = project_dir.join(entry);
        if path.is_dir() {
            fs::remove_dir_all(path).expect("remove dir");
        } else if path.is_file() {
            fs::remove_file(path).expect("remove file");
        }
    }
}

fn copy_dir_all(src: &Path, dst: &Path) {
    fs::create_dir_all(dst).expect("create dst");
    for entry in fs::read_dir(src).expect("read src") {
        let entry = entry.expect("dir entry");
        if should_skip_copy(&entry.file_name().to_string_lossy()) {
            continue;
        }
        let file_type = entry.file_type().expect("file type");
        let target = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).expect("copy file");
        }
    }
}

fn should_skip_copy(name: &str) -> bool {
    matches!(name, "target" | "logs" | "warehouse.duckdb" | ".git")
}

fn git_cmd<const N: usize>(args: [&str; N], cwd: &Path) {
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

fn integration_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
