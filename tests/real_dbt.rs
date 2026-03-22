#![allow(clippy::await_holding_lock)]

use sqlx::{PgPool, Row};
use serde_json::Value;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Mutex, OnceLock};
use tempfile::TempDir;
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{runners::AsyncRunner, ContainerAsync},
};
use uuid::Uuid;

const PROJECT_SLUG: &str = "jaffle-it";
const ENVIRONMENT_SLUG: &str = "dev";

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

    let output = run_dbtx(
        db.url(),
        &project,
        &["run", "--project-dir", project.path_str(), "--profiles-dir", project.path_str(), "--select", "stg_customers"],
    );
    assert_success(&output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("dbt-fusion 2.0.0-preview."),
        "expected dbt version line in stdout, got: {stdout}"
    );
    assert!(
        stdout.contains("   Loading profiles.yml"),
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

    assert_eq!(run_count, 1);
    assert_eq!(manifest_count, 1);
    assert_eq!(node_count, 1);
    assert_eq!(status, "success");
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

    let output = run_dbtx(
        db.url(),
        &project,
        &["build", "--project-dir", project.path_str(), "--profiles-dir", project.path_str()],
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
    let model_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM current_node_state WHERE resource_type = 'model'",
    )
    .fetch_one(db.pool())
    .await
    .expect("model count");

    assert_eq!(command, "build");
    assert_eq!(status, "success");
    assert!(model_count >= 6, "expected build to persist multiple models, got {model_count}");
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

    let output = run_dbtx(
        db.url(),
        &project,
        &["seed", "--project-dir", project.path_str(), "--profiles-dir", project.path_str()],
    );
    assert_success(&output);

    let command: String = sqlx::query("SELECT command FROM runs ORDER BY id DESC LIMIT 1")
        .fetch_one(db.pool())
        .await
        .expect("seed command row")
        .get("command");
    let seed_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM current_node_state WHERE resource_type = 'seed'",
    )
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
    assert_success(&run_dbtx(
        db.url(),
        &project,
        &["run", "--project-dir", project.path_str(), "--profiles-dir", project.path_str()],
    ));
    let promoted_node_count_before: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM promoted_manifest_nodes")
            .fetch_one(db.pool())
            .await
            .expect("promoted node count before test");
    let promoted_meta_source_before: Uuid = sqlx::query_scalar(
        "SELECT source_run_id FROM promoted_manifest_meta LIMIT 1",
    )
    .fetch_one(db.pool())
    .await
    .expect("promoted meta source before test");

    let output = run_dbtx(
        db.url(),
        &project,
        &["test", "--project-dir", project.path_str(), "--profiles-dir", project.path_str(), "--select", "stg_customers"],
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
    let promoted_meta_source_after: Uuid = sqlx::query_scalar(
        "SELECT source_run_id FROM promoted_manifest_meta LIMIT 1",
    )
    .fetch_one(db.pool())
    .await
    .expect("promoted meta source after test");

    assert_eq!(command, "test");
    assert!(test_count >= 1, "expected persisted test node executions, got {test_count}");
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
    assert_success(&run_dbtx(
        db.url(),
        &project,
        &["run", "--project-dir", project.path_str(), "--profiles-dir", project.path_str(), "--select", "stg_customers"],
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
        db.url(),
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

    let output = run_dbtx(
        db.url(),
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

    let modified = modified_unique_ids(db.url(), &project);
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

    let output = run_dbtx(
        db.url(),
        &project,
        &["run", "--project-dir", project.path_str(), "--profiles-dir", project.path_str()],
    );
    assert_success(&output);

    let modified = modified_unique_ids(db.url(), &project);
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

    assert_success(&run_dbtx(
        db.url(),
        &project,
        &["run", "--project-dir", project.path_str(), "--profiles-dir", project.path_str()],
    ));

    project.append_to_file(
        "models/staging/stg_customers.sql",
        "\n-- dbtx build state:modified marker\n",
    );

    let modified_before = modified_unique_ids(db.url(), &project);
    assert!(
        modified_before.contains("model.jaffle_shop_project.stg_customers"),
        "expected modified model before state:modified build, got: {modified_before:?}"
    );

    let output = run_dbtx(
        db.url(),
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

    let modified_after = modified_unique_ids(db.url(), &project);
    assert!(
        modified_after.is_empty(),
        "expected state:modified to be empty after build -s state:modified, got: {modified_after:?}"
    );
}

#[tokio::test]
#[ignore = "requires local dbt fusion, duckdb, and docker"]
async fn dbtx_replay_rebuilds_real_current_state() {
    let _guard = integration_lock()
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let db = TestDatabase::new().await;
    reset_db(db.pool()).await;

    let project = RealProject::new();
    project.seed();
    assert_success(&run_dbtx(
        db.url(),
        &project,
        &["run", "--project-dir", project.path_str(), "--profiles-dir", project.path_str(), "--select", "stg_customers"],
    ));

    let run_id: uuid::Uuid = sqlx::query("SELECT run_id FROM runs ORDER BY id DESC LIMIT 1")
        .fetch_one(db.pool())
        .await
        .expect("run row")
        .get("run_id");

    sqlx::query("DELETE FROM current_node_state")
        .execute(db.pool())
        .await
        .expect("delete current state");

    let empty_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM current_node_state")
        .fetch_one(db.pool())
        .await
        .expect("empty count");
    assert_eq!(empty_count, 0);

    let output = run_dbtx(db.url(), &project, &["replay", "--run-id", &run_id.to_string()]);
    assert_success(&output);

    let rebuilt_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM current_node_state")
        .fetch_one(db.pool())
        .await
        .expect("rebuilt count");
    assert_eq!(rebuilt_count, 1);
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

    assert_success(&run_dbtx(
        db.url(),
        &project,
        &["run", "--project-dir", project.path_str(), "--profiles-dir", project.path_str()],
    ));

    project.append_to_file(
        "models/staging/stg_customers.sql",
        "\n-- dbtx integration modified marker\n",
    );

    let modified = modified_unique_ids(db.url(), &project);
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

    assert_success(&run_dbtx(
        db.url(),
        &project,
        &["run", "--project-dir", project.path_str(), "--profiles-dir", project.path_str()],
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

    let modified_before = modified_unique_ids(db.url(), &project);
    assert!(
        modified_before.contains("model.jaffle_shop_project.stg_orders"),
        "expected stg_orders to be modified before rerun, got: {modified_before:?}"
    );
    assert!(
        modified_before.contains("model.jaffle_shop_project.orders"),
        "expected orders to be modified before rerun, got: {modified_before:?}"
    );

    let failed_run = run_dbtx(
        db.url(),
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

    let modified_after = modified_unique_ids(db.url(), &project);
    assert!(
        !modified_after.contains("model.jaffle_shop_project.stg_orders"),
        "expected successful upstream model to be removed from modified set, got: {modified_after:?}"
    );
    assert!(
        modified_after.contains("model.jaffle_shop_project.orders"),
        "expected failed downstream model to remain modified, got: {modified_after:?}"
    );

    let failed_run_id: uuid::Uuid = sqlx::query("SELECT run_id FROM runs ORDER BY id DESC LIMIT 1")
        .fetch_one(db.pool())
        .await
        .expect("failed run row")
        .get("run_id");

    sqlx::query("DELETE FROM current_node_state")
        .execute(db.pool())
        .await
        .expect("delete current state before replay");

    let replay_output = run_dbtx(db.url(), &project, &["replay", "--run-id", &failed_run_id.to_string()]);
    assert_success(&replay_output);

    let modified_after_replay = modified_unique_ids(db.url(), &project);
    assert!(
        !modified_after_replay.contains("model.jaffle_shop_project.stg_orders"),
        "expected successful upstream model to stay out of modified set after replay, got: {modified_after_replay:?}"
    );
    assert!(
        modified_after_replay.contains("model.jaffle_shop_project.orders"),
        "expected failed downstream model to remain modified after replay, got: {modified_after_replay:?}"
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
        Self { temp_dir }
    }

    fn path(&self) -> &Path {
        self.temp_dir.path()
    }

    fn path_str(&self) -> &str {
        self.path().to_str().expect("utf8 path")
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

fn init_dbtx_schema(database_url: &str) {
    let output = Command::new(env!("CARGO_BIN_EXE_dbtx"))
        .args(["state", "init"])
        .env("DBTX_DATABASE_URL", database_url)
        .output()
        .expect("run dbtx init");
    assert_success(&output);
}

async fn reset_db(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE promoted_manifest_nodes, promoted_manifest_meta, current_node_state, manifest_edges, manifest_nodes, manifest_snapshots, node_executions, run_events, runs, environments, projects CASCADE",
    )
    .execute(pool)
    .await
    .expect("truncate db");
}

fn run_dbtx(database_url: &str, project: &RealProject, args: &[&str]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_dbtx"));
    command.args(args);
    command.env("DBTX_DATABASE_URL", database_url);
    command.env("DBTX_PROJECT_SLUG", PROJECT_SLUG);
    command.env("DBTX_ENVIRONMENT_SLUG", ENVIRONMENT_SLUG);
    command.current_dir(project.path());
    command.output().expect("run dbtx")
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

fn modified_unique_ids(database_url: &str, project: &RealProject) -> std::collections::BTreeSet<String> {
    let output = run_dbtx(
        database_url,
        project,
        &[
            "ls",
            "--project-dir",
            project.path_str(),
            "--profiles-dir",
            project.path_str(),
            "-s",
            "state:modified",
            "--output",
            "json",
        ],
    );
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

fn jaffle_fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/jaffle_shop_project")
        .canonicalize()
        .expect("jaffle fixture dir")
}

fn clean_runtime_artifacts(project_dir: &Path) {
    for entry in ["target", "logs", "warehouse.duckdb"] {
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
        let file_type = entry.file_type().expect("file type");
        let target = dst.join(entry.file_name());
        if file_type.is_dir() {
            copy_dir_all(&entry.path(), &target);
        } else {
            fs::copy(entry.path(), target).expect("copy file");
        }
    }
}

fn integration_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}
