//! In-process integration tests using axum's tower::ServiceExt.
//! These run the server in the same process as the test, giving accurate coverage.

mod common;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use common::{connect_db_with_retry, connect_test_pool, register_testcontainer_cleanup};
use dbtx::config::RuntimeConfig;
use dbtx::process_state::ProcessState;
use dbtx::server::router;
use http_body_util::BodyExt;
use serde_json::{Value, json};
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
                let admin_url = url
                    .rsplit_once('/')
                    .map(|(b, _)| format!("{b}/postgres"))
                    .unwrap_or(url);
                return SharedPg {
                    admin_url,
                    _container: None,
                };
            }
            let container = Postgres::default()
                .with_db_name("dbtx_inproc_template")
                .with_user("dbtx")
                .with_password("dbtx")
                .start()
                .await
                .expect("start postgres");
            register_testcontainer_cleanup(container.id().to_string());
            let host = container.get_host().await.expect("host");
            let port = container.get_host_port_ipv4(5432).await.expect("port");
            let template_url = format!("postgres://dbtx:dbtx@{host}:{port}/dbtx_inproc_template");
            let admin_url = format!("postgres://dbtx:dbtx@{host}:{port}/postgres");

            // Apply migrations to template
            let db = connect_db_with_retry(&template_url, "connect template").await;
            db.migrate().await.expect("migrate template");

            SharedPg {
                admin_url,
                _container: Some(container),
            }
        })
        .await
}

/// Create an isolated test database and return an app router + pool.
async fn test_app() -> (axum::Router, PgPool) {
    let pg = shared_pg().await;
    let db_name = format!("inproc_{}", uuid::Uuid::new_v4().simple());
    let admin_pool = connect_test_pool(&pg.admin_url, "admin connect").await;
    sqlx::query(&format!(
        "CREATE DATABASE {db_name} TEMPLATE dbtx_inproc_template"
    ))
    .execute(&admin_pool)
    .await
    .expect("create test db");
    let test_url = pg
        .admin_url
        .rsplit_once('/')
        .map(|(b, _)| format!("{b}/{db_name}"))
        .unwrap();
    let pool = connect_test_pool(&test_url, "connect test db").await;
    let db = connect_db_with_retry(&test_url, "connect app db").await;
    let config = RuntimeConfig::from_database_url(test_url);
    let state = ProcessState::new(db, config);
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
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
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
    let (status, body) = post_json(
        &app,
        "/v1/project-drafts",
        json!({
            "git_repo_url": "https://github.com/example/repo.git",
            "project_root": "."
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let draft_id = body["draft"]["id"].as_str().expect("draft id");

    // Get the draft
    let (status, body) = get_json(&app, &format!("/v1/project-drafts/{draft_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(
        body["draft"]["git_repo_url"],
        "https://github.com/example/repo.git"
    );
    assert_eq!(body["draft"]["project_root"], ".");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn project_crud() {
    let (app, pool) = test_app().await;

    // Seed a project directly
    sqlx::query(
        "INSERT INTO projects (project_id, project_name, git_repo_url, default_branch, project_root, metadata) VALUES ($1, $2, 'https://example.com/repo.git', 'main', '.', '{}'::jsonb)"
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
    let (_status, _body) = post_json(
        &app,
        "/v1/projects/prj_test_1",
        json!({
            "git_repo_url": "https://example.com/new-repo.git"
        }),
    )
    .await;
    // PATCH not POST - need to use the right method
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("PATCH")
                .uri("/v1/projects/prj_test_1")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::to_vec(
                        &json!({"git_repo_url": "https://example.com/new-repo.git"}),
                    )
                    .unwrap(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // Delete project
    let response = app
        .clone()
        .oneshot(
            Request::delete("/v1/projects/prj_test_1")
                .body(Body::empty())
                .unwrap(),
        )
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
    seed_local_project(&pool).await;

    // Create a temp dbt project
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(
        tmp.path().join("dbt_project.yml"),
        "name: demo\nprofile: demo\n",
    )
    .unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: warehouse.duckdb\n      schema: main\n").unwrap();

    // Create invocation
    let (status, body) = post_json(
        &app,
        "/v1/invocations",
        json!({
            "command": "ls",
            "args": [],
            "project_id": "prj_local_1",
            "environment_slug": "dev"
        }),
    )
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
    assert!(!body["invocations"].as_array().unwrap().is_empty());
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
    let response = app
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

async fn post_form(app: &axum::Router, path: &str, form: &str) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(
            Request::post(path)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(form.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, String::from_utf8_lossy(&bytes).to_string())
}

/// Seed a project + environment + invocation for UI tests
async fn seed_ui_test_data(pool: &PgPool) {
    sqlx::query("INSERT INTO projects (project_id, project_name, git_repo_url, default_branch, project_root, metadata) VALUES ('prj_ui', 'ui_project', 'https://example.com/repo.git', 'main', '.', '{}'::jsonb)")
        .execute(pool).await.unwrap();
    sqlx::query("INSERT INTO environments (project_id, slug, profile_name, target_name, adapter_type, worker_queue, schema_name, git_branch, git_commit_sha, use_latest_commit, auto_reconcile, immutable, profile_config, profile_secrets, metadata) VALUES (1, 'prod', 'ui_project', 'prod', 'duckdb', 'generic', 'main', 'main', 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa', true, true, false, '{}'::jsonb, '{}'::jsonb, '{}'::jsonb)")
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
    assert!(
        html.contains("dbtx"),
        "dashboard should contain dbtx branding"
    );
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
    let (status, _html) =
        get_html(&app, "/ui/invocations/00000000-0000-0000-0000-000000000000").await;
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
    let (status, _body) = post_json(
        &app,
        "/v1/projects/prj_ui/environments/prod/release",
        json!({
            "git_commit_sha": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Check history
    let (status, body) = get_json(&app, "/v1/projects/prj_ui/environments/prod/history").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!body["versions"].as_array().unwrap().is_empty());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn environment_actual_state_returns_default() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, _body) =
        get_json(&app, "/v1/projects/prj_ui/environments/prod/actual-state").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn environment_active_resources_empty() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, body) = get_json(
        &app,
        "/v1/projects/prj_ui/environments/prod/active-resources",
    )
    .await;
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

    let (status, _body) = get_json(&app, "/v1/queues").await;
    assert_eq!(status, StatusCode::OK);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_claim_returns_none_when_empty() {
    let (app, _pool) = test_app().await;
    let (status, _body) = post_json(
        &app,
        "/v1/invocations/claim-next",
        json!({
            "worker_id": "w1",
            "worker_queues": ["generic"]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_full_lifecycle() {
    let (app, pool) = test_app().await;

    // Seed project + environment
    seed_local_project(&pool).await;
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(
        tmp.path().join("dbt_project.yml"),
        "name: demo\nprofile: demo\n",
    )
    .unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: w.duckdb\n      schema: main\n").unwrap();

    // Create invocation via API
    let (status, body) = post_json(
        &app,
        "/v1/invocations",
        json!({
            "command": "ls",
            "args": [],
            "project_id": "prj_local_1",
            "environment_slug": "dev"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "create: {body}");
    let inv_id = body["invocation_id"].as_str().unwrap();

    // Claim
    let (status, body) = post_json(
        &app,
        "/v1/invocations/claim-next",
        json!({
            "execution_mode": "local",
            "worker_id": "w1",
            "worker_queues": [body["worker_queue"].as_str().unwrap()]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let worker_id = body["worker_id"].as_str().unwrap().to_string();
    let lease_token = body["lease_token"].as_str().unwrap().to_string();

    // Heartbeat
    let (status, _) = post_json(
        &app,
        &format!("/v1/invocations/{inv_id}/heartbeat"),
        json!({
            "worker_id": worker_id,
            "lease_token": lease_token
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);

    // Append events
    let (status, _) = post_json(
        &app,
        &format!("/v1/invocations/{inv_id}/events"),
        json!({
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
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Complete
    let (status, _) = post_json(
        &app,
        &format!("/v1/invocations/{inv_id}/complete"),
        json!({
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
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Verify final status
    let (status, body) = get_json(&app, &format!("/v1/invocations/{inv_id}")).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["status"], "succeeded");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_cancel_unclaimed() {
    let (app, pool) = test_app().await;
    seed_local_project(&pool).await;

    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(
        tmp.path().join("dbt_project.yml"),
        "name: demo\nprofile: demo\n",
    )
    .unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: w.duckdb\n      schema: main\n").unwrap();

    let (_, body) = post_json(
        &app,
        "/v1/invocations",
        json!({
            "command": "ls",
            "args": [],
            "project_id": "prj_local_1",
            "environment_slug": "dev"
        }),
    )
    .await;
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

    let (status, body) = post_json(
        &app,
        "/v1/projects/prj_ui/environments/prod/source-state-events",
        json!({
            "source_key": "source.raw_orders",
            "provider": "manual",
            "state_version": "v1",
            "observed_at": "2026-01-01T00:00:00Z",
            "payload": {"reason": "test"}
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(body["event"]["id"].is_number());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn environment_reconcile_requires_baseline() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;

    let (status, body) = post_json(
        &app,
        "/v1/projects/prj_ui/environments/prod/reconcile",
        json!({}),
    )
    .await;
    // Should fail — no baseline run exists
    assert_ne!(status, StatusCode::OK, "expected error: {body}");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_environment_release_post() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;

    let (status, _html) = post_form(
        &app,
        "/ui/projects/prj_ui/environments/prod/release",
        "git_commit_sha=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )
    .await;
    // Should redirect (302/303) or return HTML
    assert!(
        status.is_success() || status.is_redirection(),
        "release status: {status}"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_project_delete_post() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;

    let (status, _html) = post_form(&app, "/ui/projects/prj_ui/delete", "").await;
    // Should redirect or succeed
    assert!(
        status.is_success() || status.is_redirection(),
        "delete status: {status}"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_complete_with_manifest_persists_state() {
    let (app, pool) = test_app().await;
    seed_local_project(&pool).await;

    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(
        tmp.path().join("dbt_project.yml"),
        "name: demo\nprofile: demo\n",
    )
    .unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: w.duckdb\n      schema: main\n").unwrap();

    // Create a build invocation (persists state)
    let (_, body) = post_json(
        &app,
        "/v1/invocations",
        json!({
            "command": "build",
            "args": [],
            "project_id": "prj_local_1",
            "environment_slug": "dev"
        }),
    )
    .await;
    let inv_id = body["invocation_id"].as_str().unwrap().to_string();
    let wq = body["worker_queue"].as_str().unwrap().to_string();

    let (_, body) = post_json(
        &app,
        "/v1/invocations/claim-next",
        json!({
            "execution_mode": "local",
            "worker_id": "w1",
            "worker_queues": [wq]
        }),
    )
    .await;
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

    let (status, _) = post_json(
        &app,
        &format!("/v1/invocations/{inv_id}/complete"),
        json!({
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
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Verify manifest was persisted
    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM manifest_nodes")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(count > 0, "manifest nodes should be persisted");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_with_dbt_log_events() {
    let (app, pool) = test_app().await;
    seed_local_project(&pool).await;

    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(
        tmp.path().join("dbt_project.yml"),
        "name: demo\nprofile: demo\n",
    )
    .unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: w.duckdb\n      schema: main\n").unwrap();

    let (_, body) = post_json(
        &app,
        "/v1/invocations",
        json!({
            "command": "build",
            "args": [],
            "project_id": "prj_local_1",
            "environment_slug": "dev"
        }),
    )
    .await;
    let inv_id = body["invocation_id"].as_str().unwrap().to_string();
    let wq = body["worker_queue"].as_str().unwrap().to_string();

    let (_, body) = post_json(
        &app,
        "/v1/invocations/claim-next",
        json!({
            "execution_mode": "local",
            "worker_id": "w1",
            "worker_queues": [wq]
        }),
    )
    .await;
    let worker_id = body["worker_id"].as_str().unwrap().to_string();
    let lease_token = body["lease_token"].as_str().unwrap().to_string();

    // Send dbt log events
    let dbt_log = r#"{"info":{"name":"LogModelResult","code":"Q012","invocation_id":"abc","level":"info","msg":"Succeeded [table] model.demo.orders"},"data":{"node_info":{"unique_id":"model.demo.orders","resource_type":"model","node_name":"orders","node_path":"models/orders.sql","materialized":"table","node_status":"success","node_started_at":"2026-01-01T00:00:00Z","node_finished_at":"2026-01-01T00:00:01Z","node_relation":{"database":"db","schema":"main","alias":"orders","relation_name":"db.main.orders"},"node_checksum":"abc123"},"run_result":{"status":"success","execution_time":1.0}}}"#;

    let (status, _) = post_json(
        &app,
        &format!("/v1/invocations/{inv_id}/events"),
        json!({
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
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Complete
    let (status, _) = post_json(
        &app,
        &format!("/v1/invocations/{inv_id}/complete"),
        json!({
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
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn invocation_complete_failed_records_error() {
    let (app, pool) = test_app().await;
    seed_local_project(&pool).await;

    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(
        tmp.path().join("dbt_project.yml"),
        "name: demo\nprofile: demo\n",
    )
    .unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: w.duckdb\n      schema: main\n").unwrap();

    let (_, body) = post_json(
        &app,
        "/v1/invocations",
        json!({
            "command": "build",
            "args": [],
            "project_id": "prj_local_1",
            "environment_slug": "dev"
        }),
    )
    .await;
    let inv_id = body["invocation_id"].as_str().unwrap().to_string();
    let wq = body["worker_queue"].as_str().unwrap().to_string();

    let (_, body) = post_json(
        &app,
        "/v1/invocations/claim-next",
        json!({
            "execution_mode": "local",
            "worker_id": "w1",
            "worker_queues": [wq]
        }),
    )
    .await;
    let worker_id = body["worker_id"].as_str().unwrap().to_string();
    let lease_token = body["lease_token"].as_str().unwrap().to_string();

    let (status, _) = post_json(
        &app,
        &format!("/v1/invocations/{inv_id}/complete"),
        json!({
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
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, body) = get_json(&app, &format!("/v1/invocations/{inv_id}")).await;
    assert_eq!(body["status"], "failed");
    assert_eq!(body["error"], "compilation error");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_catalog_renders_with_resource_type_filter() {
    let (app, pool) = test_app().await;
    seed_ui_test_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/catalog").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("Catalog"), "page should have Catalog heading");
    assert!(
        html.contains("Resource Type"),
        "page should have resource type filter"
    );
    assert!(
        html.contains("resource_type"),
        "page should have resource_type checkboxes"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn manifest_backfill_populates_sources_in_current_node_state() {
    let (app, pool) = test_app().await;
    seed_local_project(&pool).await;

    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(
        tmp.path().join("dbt_project.yml"),
        "name: demo\nprofile: demo\n",
    )
    .unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: w.duckdb\n      schema: main\n").unwrap();

    // Create invocation
    let (_, body) = post_json(
        &app,
        "/v1/invocations",
        json!({
            "command": "build",
            "args": [],
            "project_id": "prj_local_1",
            "environment_slug": "dev"
        }),
    )
    .await;
    let inv_id = body["invocation_id"].as_str().unwrap().to_string();
    let wq = body["worker_queue"].as_str().unwrap().to_string();

    // Claim
    let (_, body) = post_json(
        &app,
        "/v1/invocations/claim-next",
        json!({
            "execution_mode": "local",
            "worker_id": "w1",
            "worker_queues": [wq]
        }),
    )
    .await;
    let worker_id = body["worker_id"].as_str().unwrap().to_string();
    let lease_token = body["lease_token"].as_str().unwrap().to_string();

    // Send node execution event for the model only (not the source)
    let dbt_log = r#"{"info":{"name":"LogModelResult","code":"Q012","invocation_id":"abc","level":"info","msg":"OK model.demo.orders"},"data":{"node_info":{"unique_id":"model.demo.orders","resource_type":"model","node_name":"orders","node_path":"models/orders.sql","materialized":"table","node_status":"success","node_started_at":"2026-01-01T00:00:00Z","node_finished_at":"2026-01-01T00:00:01Z","node_relation":{"database":"db","schema":"main","alias":"orders","relation_name":"db.main.orders"},"node_checksum":"abc123"},"run_result":{"status":"success","execution_time":1.0}}}"#;
    let (status, _) = post_json(
        &app,
        &format!("/v1/invocations/{inv_id}/events"),
        json!({
            "worker_id": worker_id,
            "lease_token": lease_token,
            "events": [{
                "kind": "DbtLog",
                "occurred_at": "2026-01-01T00:00:01Z",
                "text": "OK model.demo.orders",
                "raw_line": dbt_log,
                "dbt_event_name": "LogModelResult",
                "node_unique_id": "model.demo.orders",
                "level": "info",
                "error": null
            }]
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Complete with manifest that includes both a model and a source
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
                "relation_name": "db.main.orders",
                "depends_on": {"nodes": ["source.demo.raw_orders"]}
            }
        },
        "sources": {
            "source.demo.raw_orders": {
                "unique_id": "source.demo.raw_orders",
                "resource_type": "source",
                "name": "raw_orders",
                "package_name": "demo",
                "original_file_path": "models/staging/__sources.yml",
                "tags": [],
                "fqn": ["demo", "raw_orders"],
                "config": {},
                "database": "db",
                "schema": "raw",
                "identifier": "raw_orders",
                "loader": "csv"
            }
        },
        "parent_map": {
            "model.demo.orders": ["source.demo.raw_orders"],
            "source.demo.raw_orders": []
        },
        "child_map": {
            "source.demo.raw_orders": ["model.demo.orders"],
            "model.demo.orders": []
        }
    });

    let (status, _) = post_json(
        &app,
        &format!("/v1/invocations/{inv_id}/complete"),
        json!({
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
        }),
    )
    .await;
    assert_eq!(status, StatusCode::NO_CONTENT);

    // Verify the model is in current_node_state (from node execution)
    let model_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM current_node_state WHERE unique_id = 'model.demo.orders'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        model_count, 1,
        "model should be in current_node_state from execution"
    );

    // Verify the source is ALSO in current_node_state (from manifest backfill)
    let source_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM current_node_state WHERE unique_id = 'source.demo.raw_orders'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        source_count, 1,
        "source should be in current_node_state from manifest backfill"
    );

    // Verify the source has the correct resource_type
    let source_rt: Option<String> = sqlx::query_scalar(
        "SELECT resource_type FROM current_node_state WHERE unique_id = 'source.demo.raw_orders'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(source_rt.as_deref(), Some("source"));
}

/// Helper: create a local invocation, claim it, return (invocation_id, worker_id, lease_token, worker_queue).
async fn seed_local_project(pool: &sqlx::PgPool) {
    sqlx::query("INSERT INTO projects (project_id, project_name, git_repo_url, default_branch, project_root, metadata) VALUES ('prj_local_1', 'demo', 'https://example.com/local.git', 'main', '.', '{}'::jsonb) ON CONFLICT DO NOTHING").execute(pool).await.unwrap();
    sqlx::query("INSERT INTO environments (project_id, slug, profile_name, target_name, adapter_type, worker_queue, schema_name, profile_config, profile_secrets, metadata) VALUES (1, 'dev', 'demo', 'dev', 'duckdb', 'generic', 'main', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb) ON CONFLICT DO NOTHING").execute(pool).await.unwrap();
}

async fn create_and_claim(
    app: &axum::Router,
    _project_dir: &std::path::Path,
) -> (String, String, String, String) {
    let (_, body) = post_json(
        app,
        "/v1/invocations",
        json!({
            "command": "build",
            "args": [],
            "project_id": "prj_local_1",
            "environment_slug": "dev"
        }),
    )
    .await;
    let inv_id = body["invocation_id"].as_str().unwrap().to_string();
    let wq = body["worker_queue"].as_str().unwrap().to_string();
    let (_, body) = post_json(
        app,
        "/v1/invocations/claim-next",
        json!({
            "execution_mode": "local",
            "worker_id": "w1",
            "worker_queues": [wq]
        }),
    )
    .await;
    let worker_id = body["worker_id"].as_str().unwrap().to_string();
    let lease_token = body["lease_token"].as_str().unwrap().to_string();
    (inv_id, worker_id, lease_token, wq)
}

/// Helper: send a single node execution event.
#[allow(clippy::too_many_arguments)]
async fn send_node_event(
    app: &axum::Router,
    inv_id: &str,
    worker_id: &str,
    lease_token: &str,
    unique_id: &str,
    status: &str,
    checksum: &str,
    relation_db: &str,
    relation_schema: &str,
    relation_name: &str,
) {
    let dbt_log = serde_json::to_string(&json!({
        "info": {"name": "LogModelResult", "code": "Q012", "invocation_id": "abc", "level": "info", "msg": ""},
        "data": {
            "node_info": {
                "unique_id": unique_id,
                "resource_type": "model",
                "node_name": unique_id.rsplit('.').next().unwrap_or(unique_id),
                "node_path": format!("models/{}.sql", unique_id.rsplit('.').next().unwrap_or(unique_id)),
                "materialized": "table",
                "node_status": status,
                "node_started_at": "2026-01-01T00:00:00Z",
                "node_finished_at": "2026-01-01T00:00:01Z",
                "node_relation": {
                    "database": relation_db,
                    "schema": relation_schema,
                    "alias": unique_id.rsplit('.').next().unwrap_or(unique_id),
                    "relation_name": relation_name
                },
                "node_checksum": checksum
            },
            "run_result": {"status": status, "execution_time": 1.0}
        }
    })).unwrap();
    let (s, _) = post_json(
        app,
        &format!("/v1/invocations/{inv_id}/events"),
        json!({
            "worker_id": worker_id,
            "lease_token": lease_token,
            "events": [{
                "kind": "DbtLog",
                "occurred_at": "2026-01-01T00:00:01Z",
                "text": "",
                "raw_line": dbt_log,
                "dbt_event_name": "LogModelResult",
                "node_unique_id": unique_id,
                "level": "info",
                "error": null
            }]
        }),
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
}

/// Helper: complete an invocation with a given status and optional manifest.
async fn complete_invocation(
    app: &axum::Router,
    inv_id: &str,
    worker_id: &str,
    lease_token: &str,
    status: &str,
    exit_code: i32,
    manifest: Option<Value>,
) {
    let (s, _) = post_json(
        app,
        &format!("/v1/invocations/{inv_id}/complete"),
        json!({
            "worker_id": worker_id,
            "lease_token": lease_token,
            "completion": {
                "status": status,
                "exit_code": exit_code,
                "error": null,
                "dbt_version": "1.0.0",
                "manifest": manifest,
                "result": null
            }
        }),
    )
    .await;
    assert_eq!(s, StatusCode::NO_CONTENT);
}

/// Helper: create a temp dir with minimal dbt project files.
fn temp_dbt_project() -> tempfile::TempDir {
    let tmp = tempfile::TempDir::new().unwrap();
    std::fs::write(
        tmp.path().join("dbt_project.yml"),
        "name: demo\nprofile: demo\n",
    )
    .unwrap();
    std::fs::write(tmp.path().join("profiles.yml"), "demo:\n  target: dev\n  outputs:\n    dev:\n      type: duckdb\n      path: w.duckdb\n      schema: main\n").unwrap();
    tmp
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn rebuild_populates_correct_field_values_after_successful_run() {
    let (app, pool) = test_app().await;
    seed_local_project(&pool).await;
    let tmp = temp_dbt_project();
    let (inv_id, worker_id, lease_token, _) = create_and_claim(&app, tmp.path()).await;

    send_node_event(
        &app,
        &inv_id,
        &worker_id,
        &lease_token,
        "model.demo.orders",
        "success",
        "checksum_abc",
        "my_db",
        "my_schema",
        "my_db.my_schema.orders",
    )
    .await;

    complete_invocation(
        &app,
        &inv_id,
        &worker_id,
        &lease_token,
        "succeeded",
        0,
        None,
    )
    .await;

    let row = sqlx::query(
        "SELECT status, checksum, relation_database, relation_schema, relation_name, \
         materialized, last_success_at, resource_type \
         FROM current_node_state WHERE unique_id = 'model.demo.orders'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    use sqlx::Row;
    assert_eq!(
        row.get::<Option<String>, _>("status").as_deref(),
        Some("success")
    );
    assert_eq!(
        row.get::<Option<String>, _>("checksum").as_deref(),
        Some("checksum_abc")
    );
    assert_eq!(
        row.get::<Option<String>, _>("relation_database").as_deref(),
        Some("my_db")
    );
    assert_eq!(
        row.get::<Option<String>, _>("relation_schema").as_deref(),
        Some("my_schema")
    );
    assert_eq!(
        row.get::<Option<String>, _>("relation_name").as_deref(),
        Some("my_db.my_schema.orders")
    );
    assert_eq!(
        row.get::<Option<String>, _>("materialized").as_deref(),
        Some("table")
    );
    assert_eq!(
        row.get::<Option<String>, _>("resource_type").as_deref(),
        Some("model")
    );
    assert!(
        row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_success_at")
            .is_some(),
        "last_success_at should be set for a successful run"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn rebuild_preserves_prior_success_state_after_failed_run() {
    let (app, pool) = test_app().await;
    seed_local_project(&pool).await;
    let tmp = temp_dbt_project();

    // --- Run 1: successful ---
    let (inv1, w1, lt1, _) = create_and_claim(&app, tmp.path()).await;
    send_node_event(
        &app,
        &inv1,
        &w1,
        &lt1,
        "model.demo.orders",
        "success",
        "good_checksum",
        "prod_db",
        "prod_schema",
        "prod_db.prod_schema.orders",
    )
    .await;
    complete_invocation(&app, &inv1, &w1, &lt1, "succeeded", 0, None).await;

    let row = sqlx::query(
        "SELECT checksum, relation_database, last_success_at \
         FROM current_node_state WHERE unique_id = 'model.demo.orders'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    use sqlx::Row;
    let success_at: Option<chrono::DateTime<chrono::Utc>> = row.get("last_success_at");
    assert_eq!(
        row.get::<Option<String>, _>("checksum").as_deref(),
        Some("good_checksum")
    );
    assert!(success_at.is_some());

    // --- Run 2: failed on the same node ---
    let (inv2, w2, lt2, _) = create_and_claim(&app, tmp.path()).await;
    send_node_event(
        &app,
        &inv2,
        &w2,
        &lt2,
        "model.demo.orders",
        "error",
        "bad_checksum",
        "other_db",
        "other_schema",
        "other_db.other_schema.orders",
    )
    .await;
    complete_invocation(&app, &inv2, &w2, &lt2, "failed", 1, None).await;

    let row = sqlx::query(
        "SELECT status, checksum, relation_database, last_success_at \
         FROM current_node_state WHERE unique_id = 'model.demo.orders'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();

    assert_eq!(
        row.get::<Option<String>, _>("status").as_deref(),
        Some("error"),
        "status should reflect the latest (failed) execution"
    );
    assert_eq!(
        row.get::<Option<String>, _>("checksum").as_deref(),
        Some("good_checksum"),
        "checksum should be preserved from the prior successful run"
    );
    assert_eq!(
        row.get::<Option<String>, _>("relation_database").as_deref(),
        Some("prod_db"),
        "relation_database should be preserved from the prior successful run"
    );
    assert_eq!(
        row.get::<Option<chrono::DateTime<chrono::Utc>>, _>("last_success_at"),
        success_at,
        "last_success_at should be preserved from the prior successful run"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn rebuild_accumulates_state_across_multiple_runs_with_different_nodes() {
    let (app, pool) = test_app().await;
    seed_local_project(&pool).await;
    let tmp = temp_dbt_project();

    // --- Run 1: build orders ---
    let (inv1, w1, lt1, _) = create_and_claim(&app, tmp.path()).await;
    send_node_event(
        &app,
        &inv1,
        &w1,
        &lt1,
        "model.demo.orders",
        "success",
        "orders_checksum",
        "db",
        "main",
        "db.main.orders",
    )
    .await;
    complete_invocation(&app, &inv1, &w1, &lt1, "succeeded", 0, None).await;

    // --- Run 2: build customers (different node) ---
    let (inv2, w2, lt2, _) = create_and_claim(&app, tmp.path()).await;
    send_node_event(
        &app,
        &inv2,
        &w2,
        &lt2,
        "model.demo.customers",
        "success",
        "customers_checksum",
        "db",
        "main",
        "db.main.customers",
    )
    .await;
    complete_invocation(&app, &inv2, &w2, &lt2, "succeeded", 0, None).await;

    let count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM current_node_state")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(
        count, 2,
        "both nodes from separate runs should be in current_node_state"
    );

    use sqlx::Row;
    let orders = sqlx::query(
        "SELECT status, checksum FROM current_node_state WHERE unique_id = 'model.demo.orders'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        orders.get::<Option<String>, _>("status").as_deref(),
        Some("success")
    );
    assert_eq!(
        orders.get::<Option<String>, _>("checksum").as_deref(),
        Some("orders_checksum")
    );

    let customers = sqlx::query(
        "SELECT status, checksum FROM current_node_state WHERE unique_id = 'model.demo.customers'",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        customers.get::<Option<String>, _>("status").as_deref(),
        Some("success")
    );
    assert_eq!(
        customers.get::<Option<String>, _>("checksum").as_deref(),
        Some("customers_checksum")
    );
}

// ── Malformed input tests ──────────────────────────────────────────────

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn project_draft_rejects_empty_body() {
    let (app, _pool) = test_app().await;
    let (status, _) = post_json(&app, "/v1/project-drafts", json!({})).await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn project_draft_rejects_missing_repo_url() {
    let (app, _pool) = test_app().await;
    let (status, _) = post_json(
        &app,
        "/v1/project-drafts",
        json!({
            "project_root": "."
        }),
    )
    .await;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn get_nonexistent_project_returns_404() {
    let (app, _pool) = test_app().await;
    let (status, _) = get_json(&app, "/v1/projects/prj_does_not_exist").await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn get_nonexistent_invocation_returns_404() {
    let (app, _pool) = test_app().await;
    let fake_id = uuid::Uuid::new_v4();
    let (status, _) = get_json(&app, &format!("/v1/invocations/{fake_id}")).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn source_state_event_rejects_nonexistent_environment() {
    let (app, _pool) = test_app().await;
    let (status, _) = post_json(
        &app,
        "/v1/projects/prj_missing/environments/dev/source-state-events",
        json!({
            "source_key": "raw.orders",
            "provider": "test"
        }),
    )
    .await;
    assert!(
        status == StatusCode::NOT_FOUND || status == StatusCode::UNPROCESSABLE_ENTITY,
        "expected 404 or 422, got {status}"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn reconcile_nonexistent_environment_returns_error() {
    let (app, _pool) = test_app().await;
    let (status, _) = post_json(
        &app,
        "/v1/projects/prj_missing/environments/dev/reconcile",
        json!({}),
    )
    .await;
    assert!(
        status == StatusCode::NOT_FOUND || status == StatusCode::INTERNAL_SERVER_ERROR,
        "expected error status, got {status}"
    );
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn project_resolve_returns_project_by_repo_url() {
    let (app, pool) = test_app().await;
    sqlx::query(
        "INSERT INTO projects (project_id, project_name, git_repo_url, default_branch, project_root, metadata) VALUES ('prj_remote_abc', 'analytics', 'git@github.com:org/repo.git', 'main', 'analytics', '{}'::jsonb)"
    ).execute(&pool).await.unwrap();

    let (status, body) = get_json(
        &app,
        "/v1/projects/resolve?git_repo_url=git%40github.com%3Aorg%2Frepo.git&project_root=analytics",
    )
    .await;
    assert_eq!(status, StatusCode::OK, "resolve: {body}");
    assert_eq!(body["project"]["project"]["project_id"], "prj_remote_abc");
    assert_eq!(body["project"]["project"]["project_name"], "analytics");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn project_resolve_returns_404_for_unknown_repo() {
    let (app, _pool) = test_app().await;

    let (status, _body) = get_json(
        &app,
        "/v1/projects/resolve?git_repo_url=git%40github.com%3Aunknown%2Frepo.git&project_root=.",
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn local_environment_upsert_creates_and_returns_environment() {
    let (app, pool) = test_app().await;
    sqlx::query(
        "INSERT INTO projects (project_id, project_name, git_repo_url, default_branch, project_root, metadata) VALUES ('prj_remote_xyz', 'demo', 'https://example.com/repo.git', 'main', '.', '{}'::jsonb)"
    ).execute(&pool).await.unwrap();

    let (status, body) = post_json(
        &app,
        "/v1/projects/prj_remote_xyz/environments/local",
        serde_json::json!({
            "target_name": "dev",
            "machine_id": "laptop-123",
            "adapter_type": "duckdb",
            "schema_name": "main"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "upsert: {body}");
    assert_eq!(body["environment_slug"], "local-laptop-123-dev");
    assert_eq!(body["worker_queue"], "local-laptop-123");

    // Calling again is idempotent
    let (status2, body2) = post_json(
        &app,
        "/v1/projects/prj_remote_xyz/environments/local",
        serde_json::json!({
            "target_name": "dev",
            "machine_id": "laptop-123",
            "adapter_type": "postgres",
            "schema_name": "public"
        }),
    )
    .await;
    assert_eq!(status2, StatusCode::OK);
    assert_eq!(body2["environment_slug"], "local-laptop-123-dev");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn execution_mode_derived_from_environment_commit_sha() {
    let (app, pool) = test_app().await;
    sqlx::query(
        "INSERT INTO projects (project_id, project_name, git_repo_url, default_branch, project_root, metadata) VALUES ('prj_mode_test', 'demo', 'https://example.com/repo.git', 'main', '.', '{}'::jsonb)"
    ).execute(&pool).await.unwrap();

    // Environment WITH git_commit_sha → server mode
    sqlx::query(
        "INSERT INTO environments (project_id, slug, profile_name, target_name, git_branch, git_commit_sha, adapter_type, worker_queue, schema_name, profile_config, profile_secrets, metadata) VALUES (1, 'prod', 'demo', 'prod', 'main', 'abc123def456', 'duckdb', 'generic', 'main', '{\"path\":\"warehouse.duckdb\"}'::jsonb, '{}'::jsonb, '{}'::jsonb)"
    ).execute(&pool).await.unwrap();

    // Environment WITHOUT git_commit_sha → local mode
    sqlx::query(
        "INSERT INTO environments (project_id, slug, profile_name, target_name, adapter_type, worker_queue, schema_name, profile_config, profile_secrets, metadata) VALUES (1, 'local-dev', 'demo', 'dev', 'duckdb', 'local-test', 'main', '{}'::jsonb, '{}'::jsonb, '{}'::jsonb)"
    ).execute(&pool).await.unwrap();

    // Server mode invocation
    let (status, body) = post_json(
        &app,
        "/v1/invocations",
        serde_json::json!({
            "command": "ls",
            "args": [],
            "project_id": "prj_mode_test",
            "environment_slug": "prod"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "server invocation: {body}");
    assert_eq!(body["execution_mode"], "server");

    // Local mode invocation
    let (status, body) = post_json(
        &app,
        "/v1/invocations",
        serde_json::json!({
            "command": "ls",
            "args": [],
            "project_id": "prj_mode_test",
            "environment_slug": "local-dev"
        }),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "local invocation: {body}");
    assert_eq!(body["execution_mode"], "local");
}

// --- Catalog UI tests ---

async fn seed_catalog_data(pool: &PgPool) {
    seed_ui_test_data(pool).await;
    // Add a run
    sqlx::query(
        "INSERT INTO runs (run_id, project_id, environment_id, command, args, terminal_status) VALUES ('11111111-1111-1111-1111-111111111111', 1, 1, 'build', '[]'::jsonb, 'success')"
    ).execute(pool).await.unwrap();
    // Add manifest snapshot (required FK for manifest_nodes)
    sqlx::query(
        "INSERT INTO manifest_snapshots (run_id, manifest, manifest_size_bytes) VALUES ('11111111-1111-1111-1111-111111111111', '{}'::jsonb, 100)"
    ).execute(pool).await.unwrap();
    // Add manifest nodes
    sqlx::query(
        "INSERT INTO manifest_nodes (run_id, unique_id, name, resource_type, package_name, original_file_path, tags, fqn, config, checksum) VALUES ('11111111-1111-1111-1111-111111111111', 'model.pkg.orders', 'orders', 'model', 'pkg', 'models/orders.sql', '[]'::jsonb, '[\"pkg\",\"orders\"]'::jsonb, '{\"materialized\": \"table\"}'::jsonb, 'abc123')"
    ).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO manifest_nodes (run_id, unique_id, name, resource_type, package_name, original_file_path, tags, fqn, config, checksum) VALUES ('11111111-1111-1111-1111-111111111111', 'source.pkg.raw_orders', 'raw_orders', 'source', 'pkg', 'models/sources.yml', '[]'::jsonb, '[\"pkg\",\"raw_orders\"]'::jsonb, '{}'::jsonb, 'def456')"
    ).execute(pool).await.unwrap();
    // Add current_node_state entries
    sqlx::query(
        "INSERT INTO current_node_state (project_id, environment_id, unique_id, last_run_id, node_name, node_path, resource_type, status, materialized) VALUES (1, 1, 'model.pkg.orders', '11111111-1111-1111-1111-111111111111', 'orders', 'models/orders.sql', 'model', 'success', 'table')"
    ).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO current_node_state (project_id, environment_id, unique_id, last_run_id, node_name, node_path, resource_type, status, materialized) VALUES (1, 1, 'source.pkg.raw_orders', '11111111-1111-1111-1111-111111111111', 'raw_orders', 'models/sources.yml', 'source', 'success', NULL)"
    ).execute(pool).await.unwrap();
    // Add promoted manifest nodes
    sqlx::query(
        "INSERT INTO promoted_manifest_nodes (project_id, environment_id, unique_id, source_run_id, checksum, raw_node) VALUES (1, 1, 'model.pkg.orders', '11111111-1111-1111-1111-111111111111', 'abc123', '{\"name\": \"orders\", \"unique_id\": \"model.pkg.orders\", \"resource_type\": \"model\", \"raw_code\": \"SELECT 1\", \"compiled_code\": \"SELECT 1\", \"description\": \"Orders model\", \"depends_on\": {\"nodes\": []}}'::jsonb)"
    ).execute(pool).await.unwrap();
    sqlx::query(
        "INSERT INTO promoted_manifest_nodes (project_id, environment_id, unique_id, source_run_id, checksum, raw_node) VALUES (1, 1, 'source.pkg.raw_orders', '11111111-1111-1111-1111-111111111111', 'def456', '{\"name\": \"raw_orders\", \"unique_id\": \"source.pkg.raw_orders\", \"resource_type\": \"source\", \"description\": \"Raw orders source\", \"depends_on\": {\"nodes\": []}}'::jsonb)"
    ).execute(pool).await.unwrap();
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_catalog_index_renders_models() {
    let (app, pool) = test_app().await;
    seed_catalog_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/catalog").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("orders"), "catalog should list the orders model");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_catalog_filters_by_resource_type() {
    let (app, pool) = test_app().await;
    seed_catalog_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/catalog?resource_type=source&project_id=prj_ui&environment_slug=prod").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("raw_orders"), "should show source");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_catalog_model_detail_renders() {
    let (app, pool) = test_app().await;
    seed_catalog_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/catalog/prj_ui/prod/model.pkg.orders").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("orders"), "detail page should show model name");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_catalog_model_tab_overview() {
    let (app, pool) = test_app().await;
    seed_catalog_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/catalog/prj_ui/prod/model.pkg.orders/tab?tab=overview").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!html.is_empty());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_catalog_model_tab_code() {
    let (app, pool) = test_app().await;
    seed_catalog_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/catalog/prj_ui/prod/model.pkg.orders/tab?tab=code").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!html.is_empty(), "code tab should render content");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_catalog_model_tab_lineage() {
    let (app, pool) = test_app().await;
    seed_catalog_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/catalog/prj_ui/prod/model.pkg.orders/tab?tab=lineage&depth=2&direction=both").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!html.is_empty());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_catalog_model_tab_history() {
    let (app, pool) = test_app().await;
    seed_catalog_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/catalog/prj_ui/prod/model.pkg.orders/tab?tab=history").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!html.is_empty());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_catalog_model_tab_invocations() {
    let (app, pool) = test_app().await;
    seed_catalog_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/catalog/prj_ui/prod/model.pkg.orders/tab?tab=invocations").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!html.is_empty());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_catalog_model_tab_tests() {
    let (app, pool) = test_app().await;
    seed_catalog_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/catalog/prj_ui/prod/model.pkg.orders/tab?tab=tests").await;
    assert_eq!(status, StatusCode::OK);
    assert!(!html.is_empty());
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_catalog_source_detail_renders() {
    let (app, pool) = test_app().await;
    seed_catalog_data(&pool).await;
    let (status, html) = get_html(&app, "/ui/catalog/prj_ui/prod/source.pkg.raw_orders").await;
    assert_eq!(status, StatusCode::OK);
    assert!(html.contains("raw_orders"), "source detail should show source name");
}

#[tokio::test]
#[ignore = "requires docker for postgres testcontainer"]
async fn ui_catalog_nonexistent_project_returns_error() {
    let (app, pool) = test_app().await;
    seed_catalog_data(&pool).await;
    let (status, _html) = get_html(&app, "/ui/catalog/prj_nonexistent/prod/model.pkg.orders").await;
    assert_ne!(status, StatusCode::OK);
}
