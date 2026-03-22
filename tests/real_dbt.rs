use sqlx::{PgPool, Row};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::sync::{Mutex, OnceLock};
use tempfile::TempDir;
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{runners::AsyncRunner, ContainerAsync},
};

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
        .arg("init")
        .env("DBTX_DATABASE_URL", database_url)
        .output()
        .expect("run dbtx init");
    assert_success(&output);
}

async fn reset_db(pool: &PgPool) {
    sqlx::query(
        "TRUNCATE current_node_state, manifest_edges, manifest_nodes, manifest_snapshots, node_executions, run_events, runs, environments, projects CASCADE",
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
