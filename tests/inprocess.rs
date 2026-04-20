//! In-process integration tests using axum's tower::ServiceExt.
//! These run the server in the same process as the test, giving accurate coverage.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::InProcessClient;
use dbtx::config::RuntimeConfig;
use dbtx::db::Db;
use dbtx::server::{router, AppState};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sqlx::PgPool;
use std::sync::OnceLock;
use testcontainers_modules::{
    postgres::Postgres,
    testcontainers::{ContainerAsync, runners::AsyncRunner},
};
use tower::ServiceExt;

/// Shared Postgres container for all in-process tests.
static SHARED_PG: tokio::sync::OnceCell<SharedPg> = tokio::sync::OnceCell::const_new();

struct SharedPg {
    admin_url: String,
    _container: Option<ContainerAsync<Postgres>>,
}

async fn shared_pg() -> &'static SharedPg {
    SHARED_PG
        .get_or_init(|| async {
            if let Ok(url) = std::env::var("DBTX_TEST_DATABASE_URL") {
                let admin_url = url.rsplit_once('/').map(|(b, _)| format!("{b}/postgres")).unwrap_or(url);
                return SharedPg { admin_url, _container: None };
            }
            let container = Postgres::default()
                .with_db_name("dbtx_inproc_template")
                .with_user("dbtx")
                .with_password("dbtx")
                .start()
                .await
                .expect("start postgres");
            let host = container.get_host().await.expect("host");
            let port = container.get_host_port_ipv4(5432).await.expect("port");
            let template_url = format!("postgres://dbtx:dbtx@{host}:{port}/dbtx_inproc_template");
            let admin_url = format!("postgres://dbtx:dbtx@{host}:{port}/postgres");

            // Apply migrations to template
            let db = Db::connect(&template_url).await.expect("connect template");
            db.migrate().await.expect("migrate template");

            SharedPg { admin_url, _container: Some(container) }
        })
        .await
}

/// Create an isolated test database and return an app router + pool.
async fn test_app() -> (axum::Router, PgPool) {
    let pg = shared_pg().await;
    let db_name = format!("inproc_{}", uuid::Uuid::new_v4().simple());
    let admin_pool = PgPool::connect(&pg.admin_url).await.expect("admin connect");
    sqlx::query(&format!("CREATE DATABASE {db_name} TEMPLATE dbtx_inproc_template"))
        .execute(&admin_pool)
        .await
        .expect("create test db");
    let test_url = pg.admin_url.rsplit_once('/').map(|(b, _)| format!("{b}/{db_name}")).unwrap();
    let pool = PgPool::connect(&test_url).await.expect("connect test db");
    let db = Db::connect(&test_url).await.expect("connect app db");
    let config = RuntimeConfig::from_database_url(test_url);
    let state = AppState::new(db, config);
    let app = router(state);
    (app, pool)
}

// Helper to make JSON POST requests
async fn post_json(app: &axum::Router, path: &str, body: Value) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::post(path)
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&body).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

// Helper to make GET requests
async fn get_json(app: &axum::Router, path: &str) -> (StatusCode, Value) {
    let response = app
        .clone()
        .oneshot(
            Request::get(path)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    let value: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, value)
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn healthz_returns_ok() {
    let (app, _pool) = test_app().await;
    let (status, body) = get_json(&app, "/healthz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ok");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn readyz_returns_ready_with_current_schema() {
    let (app, _pool) = test_app().await;
    let (status, body) = get_json(&app, "/readyz").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "ready");
    assert_eq!(body["database"], "ok");
    assert_eq!(body["schema"], "ok");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn project_draft_lifecycle() {
    let (app, _pool) = test_app().await;

    // Create a project draft
    let (status, body) = post_json(&app, "/v1/project-drafts", json!({
        "git_repo_url": "https://github.com/example/repo.git",
        "project_root": "."
    }))
    .await;
    assert_eq!(status, StatusCode::OK);
    let draft_id = body["draft"]["id"].as_str().expect("draft id");

    // Get the draft
    let (status, body) = get_json(&app, &format!("/v1/project-drafts/{draft_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["draft"]["git_repo_url"], "https://github.com/example/repo.git");
    assert_eq!(body["draft"]["project_root"], ".");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn project_crud() {
    let (app, pool) = test_app().await;

    // Seed a project directly
    sqlx::query(
        "INSERT INTO projects (project_id, project_name, mode, git_repo_url, project_root, metadata) VALUES ($1, $2, 'remote', 'https://example.com/repo.git', '.', '{}'::jsonb)"
    )
    .bind("prj_test_1")
    .bind("test_project")
    .execute(&pool)
    .await
    .expect("seed project");

    // List projects
    let (status, body) = get_json(&app, "/v1/projects").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["projects"].as_array().unwrap().len(), 1);
    assert_eq!(body["projects"][0]["project_id"], "prj_test_1");

    // Get project
    let (status, body) = get_json(&app, "/v1/projects/prj_test_1").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["project"]["project_name"], "test_project");

    // Update project
    let (status, body) = post_json(&app, "/v1/projects/prj_test_1", json!({
        "git_repo_url": "https://example.com/new-repo.git"
    }))
    .await;
    // PATCH not POST - need to use the right method
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/v1/projects/prj_test_1")
                .header("content-type", "application/json")
                .body(Body::from(serde_json::to_vec(&json!({"git_repo_url": "https://example.com/new-repo.git"})).unwrap()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Delete project
    let response = app
        .clone()
        .oneshot(Request::delete("/v1/projects/prj_test_1").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Verify deleted
    let (status, _) = get_json(&app, "/v1/projects/prj_test_1").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_lifecycle_in_process() {
    let (app, pool) = test_app().await;

    // Seed project + environment
    sqlx::query(
        "INSERT INTO projects (project_id, project_name, mode, metadata) VALUES ('prj_local_1', 'demo', 'local', '{}'::jsonb)"
    ).execute(&pool).await.unwrap();
    sqlx::query(
        "INSERT INTO environments (project_id, slug, profile_name, target_name, adapter_type, worker_queue, schema_name, profile_config, profile_secrets, metadata) VALUES (1, 'dev', 'demo', 'dev', 'duckdb', 'generic', 'main', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb)"
    ).execute(&pool).await.unwrap();

    // Create a temp dbt project
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("dbt_project.yml"), "name: demo\nprofile: demo\n").unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: warehouse.duckdb\n      schema: main\n").unwrap();

    // Create invocation
    let (status, body) = post_json(&app, "/v1/invocations", json!({
        "command": "ls",
        "args": [],
        "current_dir": tmp.path().to_str().unwrap(),
        "project_id": null,
        "environment_slug": "dev"
    }))
    .await;
    assert_eq!(status, StatusCode::OK, "create invocation failed: {body}");
    let invocation_id = body["invocation_id"].as_str().expect("invocation_id");

    // Get invocation status
    let (status, body) = get_json(&app, &format!("/v1/invocations/{invocation_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "running");

    // List invocations
    let (status, body) = get_json(&app, "/v1/invocations").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["invocations"].as_array().unwrap().len() >= 1);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn not_found_returns_404() {
    let (app, _pool) = test_app().await;
    let (status, body) = get_json(&app, "/v1/projects/nonexistent").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(body["error"].as_str().unwrap().contains("not found"));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn reconcile_tick_endpoint_works() {
    let (app, _pool) = test_app().await;
    let (status, body) = post_json(&app, "/v1/reconcile/tick", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["planned"], 0);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn sweep_tick_endpoint_works() {
    let (app, _pool) = test_app().await;
    let (status, body) = post_json(&app, "/v1/reconcile/sweep", json!({})).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["admitted"], 0);
}
