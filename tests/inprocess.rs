//! In-process integration tests using axum's tower::ServiceExt.
//! These run the server in the same process as the test, giving accurate coverage.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use dbtx::config::RuntimeConfig;
use dbtx::db::Db;
use dbtx::server::{router, AppState};
use http_body_util::BodyExt;
use serde_json::{json, Value};
use sqlx::PgPool;
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
    let (_status, _body) = post_json(&app, "/v1/projects/prj_test_1", json!({
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

// --- HTML helpers ---

async fn get_html(app: &axum::Router, path: &str) -> (StatusCode, String) {
    let response = app.clone().oneshot(Request::get(path).body(Body::empty()).unwrap()).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

async fn post_form(app: &axum::Router, path: &str, form: &str) -> (StatusCode, String) {
    let response = app.clone().oneshot(
        Request::post(path)
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from(form.to_string())).unwrap()
    ).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

/// Seed a project + environment + invocation for UI tests
async fn seed_ui_test_data(pool: &PgPool) {
    sqlx::query("INSERT INTO projects (project_id, project_name, mode, git_repo_url, project_root, metadata) VALUES ('prj_ui', 'ui_project', 'remote', 'https://example.com/repo.git', '.', '{}'::jsonb)")
        .execute(pool).await.unwrap();
    sqlx::query("INSERT INTO environments (project_id, slug, profile_name, target_name, adapter_type, worker_queue, schema_name, git_branch, git_commit_sha, use_latest_commit, auto_deploy, immutable, profile_config, profile_secrets, metadata) VALUES (1, 'prod', 'ui_project', 'prod', 'duckdb', 'generic', 'main', 'main', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', true, true, false, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb)")
        .execute(pool).await.unwrap();
}

// --- UI handler tests ---

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_dashboard_renders() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, html) = get_html(&app, "/").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("dbtx"), "dashboard should contain dbtx branding");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_dashboard_summary_partial() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, _html) = get_html(&app, "/ui/dashboard/summary").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_dashboard_workers_partial() {
    let (app, _pool) = test_app().await;
    let (status, _html) = get_html(&app, "/ui/dashboard/workers").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_dashboard_queues_partial() {
    let (app, _pool) = test_app().await;
    let (status, _html) = get_html(&app, "/ui/dashboard/queues").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_projects_index_renders() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/projects").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("ui_project"));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_project_create_modal_renders() {
    let (app, _pool) = test_app().await;
    let (status, html) = get_html(&app, "/ui/projects/new").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("Create"));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_environment_detail_renders() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/projects/prj_ui/environments/prod").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("prod"));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_environment_panel_renders() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, _html) = get_html(&app, "/ui/projects/prj_ui/environments/prod/panel").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_invocations_index_renders() {
    let (app, _pool) = test_app().await;
    let (status, html) = get_html(&app, "/ui/invocations").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("Invocations"));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_invocations_table_renders() {
    let (app, _pool) = test_app().await;
    let (status, _html) = get_html(&app, "/ui/invocations/table").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_workers_index_renders() {
    let (app, _pool) = test_app().await;
    let (status, html) = get_html(&app, "/ui/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("Workers"));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_workers_table_renders() {
    let (app, _pool) = test_app().await;
    let (status, _html) = get_html(&app, "/ui/workers/table").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_queues_index_renders() {
    let (app, _pool) = test_app().await;
    let (status, html) = get_html(&app, "/ui/queues").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("Queues"));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_queues_table_renders() {
    let (app, _pool) = test_app().await;
    let (status, _html) = get_html(&app, "/ui/queues/table").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_environment_not_found_returns_error() {
    let (app, _pool) = test_app().await;
    let (status, _html) = get_html(&app, "/ui/projects/nonexistent/environments/nope").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_invocation_detail_not_found() {
    let (app, _pool) = test_app().await;
    let (status, _html) = get_html(&app, "/ui/invocations/00000000-0000-0000-0000-000000000000").await;
    assert!(status == StatusCode::NOT_FOUND || status == StatusCode::INTERNAL_SERVER_ERROR);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_project_delete_modal_renders() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/projects/prj_ui/delete").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("Delete") || html.contains("delete"));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_environment_create_modal_renders() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/projects/prj_ui/environments/new").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("Create") || html.contains("Environment"));
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_invocations_with_filters() {
    let (app, _pool) = test_app().await;
    let (status, _html) = get_html(&app, "/ui/invocations?status=running&page=1").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_workers_with_stale_toggle() {
    let (app, _pool) = test_app().await;
    let (status, _html) = get_html(&app, "/ui/workers?show_stale=true").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_dashboard_recent_invocations_partial() {
    let (app, _pool) = test_app().await;
    let (status, _html) = get_html(&app, "/ui/dashboard/recent-invocations").await;
    assert_eq!(status, StatusCode::OK);
}

// --- More API coverage tests ---

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn environment_release_and_history() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;

    // Release to a new commit
    let (status, _body) = post_json(&app, "/v1/projects/prj_ui/environments/prod/release", json!({
        "git_commit_sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    })).await;
    assert_eq!(status, StatusCode::OK);

    // Check history
    let (status, body) = get_json(&app, "/v1/projects/prj_ui/environments/prod/history").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["versions"].as_array().unwrap().len() >= 1);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn environment_actual_state_returns_default() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, _body) = get_json(&app, "/v1/projects/prj_ui/environments/prod/actual-state").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn environment_active_resources_empty() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, body) = get_json(&app, "/v1/projects/prj_ui/environments/prod/active-resources").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["resources"].as_array().unwrap().len(), 0);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn environment_plan_list_empty() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, body) = get_json(&app, "/v1/projects/prj_ui/environments/prod/plans").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["plans"].as_array().unwrap().len(), 0);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn worker_and_queue_list_empty() {
    let (app, _pool) = test_app().await;
    let (status, body) = get_json(&app, "/v1/workers").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["workers"].as_array().unwrap().len(), 0);

    let (status, body) = get_json(&app, "/v1/queues").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_claim_returns_none_when_empty() {
    let (app, _pool) = test_app().await;
    let (status, _body) = post_json(&app, "/v1/invocations/claim-next", json!({
        "worker_id": "w1",
        "worker_queues": ["generic"]
    })).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_full_lifecycle() {
    let (app, pool) = test_app().await;

    // Seed project + environment
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("dbt_project.yml"), "name: demo\nprofile: demo\n").unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: w.duckdb\n      schema: main\n").unwrap();

    // Create invocation via API
    let (status, body) = post_json(&app, "/v1/invocations", json!({
        "command": "ls",
        "args": [],
        "current_dir": tmp.path().to_str().unwrap(),
        "project_id": null,
        "environment_slug": null
    })).await;
    assert_eq!(status, StatusCode::OK, "create: {body}");
    let inv_id = body["invocation_id"].as_str().unwrap();

    // Claim
    let (status, body) = post_json(&app, "/v1/invocations/claim-next", json!({
        "execution_mode": "local",
        "worker_id": "w1",
        "worker_queues": [body["worker_queue"].as_str().unwrap()]
    })).await;
    assert_eq!(status, StatusCode::OK);
    let worker_id = body["worker_id"].as_str().unwrap().to_string();
    let lease_token = body["lease_token"].as_str().unwrap().to_string();

    // Heartbeat
    let (status, _) = post_json(&app, &format!("/v1/invocations/{inv_id}/heartbeat"), json!({
        "worker_id": worker_id,
        "lease_token": lease_token
    })).await;
    assert_eq!(status, StatusCode::OK);

    // Append events
    let (status, _) = post_json(&app, &format!("/v1/invocations/{inv_id}/events"), json!({
        "worker_id": worker_id,
        "lease_token": lease_token,
        "events": [{
            "kind": "StdoutLine",
            "occurred_at": "2026-01-01T00:00:00Z",
            "text": "hello",
            "raw_line": "hello",
            "dbt_event_name": null,
            "node_unique_id": null,
            "level": null,
            "error": null
        }]
    })).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Complete
    let (status, _) = post_json(&app, &format!("/v1/invocations/{inv_id}/complete"), json!({
        "worker_id": worker_id,
        "lease_token": lease_token,
        "completion": {
            "status": "succeeded",
            "exit_code": 0,
            "error": null,
            "dbt_version": "1.0.0",
            "manifest": null,
            "result": null
        }
    })).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Verify final status
    let (status, body) = get_json(&app, &format!("/v1/invocations/{inv_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "succeeded");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_cancel_unclaimed() {
    let (app, _pool) = test_app().await;

    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("dbt_project.yml"), "name: demo\nprofile: demo\n").unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: w.duckdb\n      schema: main\n").unwrap();

    let (_, body) = post_json(&app, "/v1/invocations", json!({
        "command": "ls",
        "args": [],
        "current_dir": tmp.path().to_str().unwrap(),
        "project_id": null,
        "environment_slug": null
    })).await;
    let inv_id = body["invocation_id"].as_str().unwrap();

    // Cancel unclaimed
    let (status, _) = post_json(&app, &format!("/v1/invocations/{inv_id}/cancel"), json!({})).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, body) = get_json(&app, &format!("/v1/invocations/{inv_id}")).await;
    assert_eq!(body["status"], "canceled");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn source_state_event_create() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;

    let (status, body) = post_json(&app, "/v1/projects/prj_ui/environments/prod/source-state-events", json!({
        "source_key": "source.raw_orders",
        "provider": "manual",
        "state_version": "v1",
        "observed_at": "2026-01-01T00:00:00Z",
        "payload": {"reason": "test"}
    })).await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["event"]["id"].is_number());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn environment_reconcile_requires_baseline() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;

    let (status, body) = post_json(&app, "/v1/projects/prj_ui/environments/prod/reconcile", json!({})).await;
    // Should fail — no baseline run exists
    assert_ne!(status, StatusCode::OK, "expected error: {body}");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_environment_release_post() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;

    let (status, _html) = post_form(&app, "/ui/projects/prj_ui/environments/prod/release",
        "git_commit_sha=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb").await;
    // Should redirect (302/303) or return HTML
    assert!(status.is_success() || status.is_redirection(), "release status: {status}");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_project_delete_post() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;

    let (status, _html) = post_form(&app, "/ui/projects/prj_ui/delete", "").await;
    // Should redirect or succeed
    assert!(status.is_success() || status.is_redirection(), "delete status: {status}");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_complete_with_manifest_persists_state() {
    let (app, pool) = test_app().await;

    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("dbt_project.yml"), "name: demo\nprofile: demo\n").unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: w.duckdb\n      schema: main\n").unwrap();

    // Create a build invocation (persists state)
    let (_, body) = post_json(&app, "/v1/invocations", json!({
        "command": "build",
        "args": [],
        "current_dir": tmp.path().to_str().unwrap(),
        "project_id": null,
        "environment_slug": null
    })).await;
    let inv_id = body["invocation_id"].as_str().unwrap().to_string();
    let wq = body["worker_queue"].as_str().unwrap().to_string();

    let (_, body) = post_json(&app, "/v1/invocations/claim-next", json!({
        "execution_mode": "local",
        "worker_id": "w1",
        "worker_queues": [wq]
    })).await;
    let worker_id = body["worker_id"].as_str().unwrap().to_string();
    let lease_token = body["lease_token"].as_str().unwrap().to_string();

    // Complete with a manifest
    let manifest = json!({
        "nodes": {
            "model.demo.orders": {
                "unique_id": "model.demo.orders",
                "resource_type": "model",
                "name": "orders",
                "package_name": "demo",
                "original_file_path": "models/orders.sql",
                "tags": [],
                "fqn": ["demo", "orders"],
                "config": {"materialized": "table"},
                "checksum": {"name": "sha256", "checksum": "abc123"},
                "database": "db",
                "schema": "main",
                "alias": "orders",
                "relation_name": "db.main.orders"
            }
        },
        "parent_map": {"model.demo.orders": []},
        "child_map": {"model.demo.orders": []}
    });

    let (status, _) = post_json(&app, &format!("/v1/invocations/{inv_id}/complete"), json!({
        "worker_id": worker_id,
        "lease_token": lease_token,
        "completion": {
            "status": "succeeded",
            "exit_code": 0,
            "error": null,
            "dbt_version": "1.0.0",
            "manifest": manifest,
            "result": null
        }
    })).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Verify manifest was persisted
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM manifest_nodes")
        .fetch_one(&pool).await.unwrap();
    assert!(count > 0, "manifest nodes should be persisted");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_with_dbt_log_events() {
    let (app, _pool) = test_app().await;

    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("dbt_project.yml"), "name: demo\nprofile: demo\n").unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: w.duckdb\n      schema: main\n").unwrap();

    let (_, body) = post_json(&app, "/v1/invocations", json!({
        "command": "build",
        "args": [],
        "current_dir": tmp.path().to_str().unwrap(),
        "project_id": null,
        "environment_slug": null
    })).await;
    let inv_id = body["invocation_id"].as_str().unwrap().to_string();
    let wq = body["worker_queue"].as_str().unwrap().to_string();

    let (_, body) = post_json(&app, "/v1/invocations/claim-next", json!({
        "execution_mode": "local",
        "worker_id": "w1",
        "worker_queues": [wq]
    })).await;
    let worker_id = body["worker_id"].as_str().unwrap().to_string();
    let lease_token = body["lease_token"].as_str().unwrap().to_string();

    // Send dbt log events
    let dbt_log = r#"{"info":{"name":"LogModelResult","code":"Q012","invocation_id":"abc","level":"info","msg":"Succeeded [table] model.demo.orders"},"data":{"node_info":{"unique_id":"model.demo.orders","resource_type":"model","node_name":"orders","node_path":"models/orders.sql","materialized":"table","node_status":"success","node_started_at":"2026-01-01T00:00:00Z","node_finished_at":"2026-01-01T00:00:01Z","node_relation":{"database":"db","schema":"main","alias":"orders","relation_name":"db.main.orders"},"node_checksum":"abc123"},"run_result":{"status":"success","execution_time":1.0}}}"#;

    let (status, _) = post_json(&app, &format!("/v1/invocations/{inv_id}/events"), json!({
        "worker_id": worker_id,
        "lease_token": lease_token,
        "events": [{
            "kind": "DbtLog",
            "occurred_at": "2026-01-01T00:00:01Z",
            "text": "Succeeded [table] model.demo.orders",
            "raw_line": dbt_log,
            "dbt_event_name": "LogModelResult",
            "node_unique_id": "model.demo.orders",
            "level": "info",
            "error": null
        }]
    })).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Complete
    let (status, _) = post_json(&app, &format!("/v1/invocations/{inv_id}/complete"), json!({
        "worker_id": worker_id,
        "lease_token": lease_token,
        "completion": {
            "status": "succeeded",
            "exit_code": 0,
            "error": null,
            "dbt_version": "1.0.0",
            "manifest": null,
            "result": null
        }
    })).await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_complete_failed_records_error() {
    let (app, _pool) = test_app().await;

    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(tmp.path().join("dbt_project.yml"), "name: demo\nprofile: demo\n").unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: w.duckdb\n      schema: main\n").unwrap();

    let (_, body) = post_json(&app, "/v1/invocations", json!({
        "command": "build",
        "args": [],
        "current_dir": tmp.path().to_str().unwrap(),
        "project_id": null,
        "environment_slug": null
    })).await;
    let inv_id = body["invocation_id"].as_str().unwrap().to_string();
    let wq = body["worker_queue"].as_str().unwrap().to_string();

    let (_, body) = post_json(&app, "/v1/invocations/claim-next", json!({
        "execution_mode": "local",
        "worker_id": "w1",
        "worker_queues": [wq]
    })).await;
    let worker_id = body["worker_id"].as_str().unwrap().to_string();
    let lease_token = body["lease_token"].as_str().unwrap().to_string();

    let (status, _) = post_json(&app, &format!("/v1/invocations/{inv_id}/complete"), json!({
        "worker_id": worker_id,
        "lease_token": lease_token,
        "completion": {
            "status": "failed",
            "exit_code": 1,
            "error": "compilation error",
            "dbt_version": null,
            "manifest": null,
            "result": null
        }
    })).await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, body) = get_json(&app, &format!("/v1/invocations/{inv_id}")).await;
    assert_eq!(body["status"], "failed");
    assert_eq!(body["error"], "compilation error");
}
