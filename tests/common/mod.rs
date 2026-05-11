//! In-process test client that wraps an axum Router with the same API as DaemonClient.
#![allow(dead_code)]

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use dbtx::api::*;
use dbtx::db::Db;
use dbtx::error::{AppError, AppResult};
use http_body_util::BodyExt;
use serde::Serialize;
use serde::de::DeserializeOwned;
use sqlx::PgPool;
use sqlx::postgres::PgPoolOptions;
use std::process::Command;
use std::sync::{Mutex, Once, OnceLock};
use tower::ServiceExt;
use uuid::Uuid;

pub const TEST_POOL_MAX_CONNECTIONS: u32 = 4;
pub const TEST_POOL_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

static TESTCONTAINER_CLEANUP_REGISTERED: Once = Once::new();
static TESTCONTAINER_IDS: OnceLock<Mutex<Vec<String>>> = OnceLock::new();

pub fn register_testcontainer_cleanup(container_id: impl Into<String>) {
    TESTCONTAINER_CLEANUP_REGISTERED.call_once(|| {
        // SAFETY: the cleanup function has C ABI, does not unwind, and only reads
        // process-global container IDs to remove test-only Docker containers.
        unsafe {
            libc::atexit(cleanup_testcontainers);
        }
    });

    TESTCONTAINER_IDS
        .get_or_init(|| Mutex::new(Vec::new()))
        .lock()
        .expect("testcontainer cleanup registry poisoned")
        .push(container_id.into());
}

extern "C" fn cleanup_testcontainers() {
    let Some(ids) = TESTCONTAINER_IDS.get() else {
        return;
    };
    let Ok(ids) = ids.lock() else {
        return;
    };
    if ids.is_empty() {
        return;
    }

    let _ = Command::new("docker")
        .arg("rm")
        .arg("-f")
        .arg("-v")
        .args(ids.iter().map(String::as_str))
        .output();
}

pub async fn connect_test_pool(database_url: &str, context: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(TEST_POOL_MAX_CONNECTIONS)
        .acquire_timeout(TEST_POOL_ACQUIRE_TIMEOUT)
        .connect(database_url)
        .await
        .unwrap_or_else(|err| panic!("{context}: {err}"))
}

pub async fn connect_db_with_retry(database_url: &str, context: &str) -> Db {
    let mut last_error = None;
    for attempt in 1..=5 {
        match Db::connect(database_url).await {
            Ok(db) => return db,
            Err(err) => {
                last_error = Some(err.to_string());
                tokio::time::sleep(std::time::Duration::from_millis(200 * attempt)).await;
            }
        }
    }
    panic!(
        "{context} after retries: {}",
        last_error.unwrap_or_else(|| "unknown error".to_string())
    );
}

#[derive(Clone)]
pub struct InProcessClient {
    app: Router,
}

impl InProcessClient {
    pub fn new(app: Router) -> Self {
        Self { app }
    }

    async fn request(&self, req: Request<Body>) -> AppResult<(StatusCode, Vec<u8>)> {
        let resp = self
            .app
            .clone()
            .oneshot(req)
            .await
            .map_err(|e| AppError::Internal(format!("request failed: {e}")))?;
        let status = resp.status();
        let bytes = resp
            .into_body()
            .collect()
            .await
            .map_err(|e| AppError::Internal(format!("body read: {e}")))?
            .to_bytes();
        Ok((status, bytes.to_vec()))
    }

    async fn get<T: DeserializeOwned>(&self, path: &str) -> AppResult<T> {
        let (s, b) = self
            .request(Request::get(path).body(Body::empty()).unwrap())
            .await?;
        parse(s, &b)
    }

    async fn post<B: Serialize, T: DeserializeOwned>(&self, path: &str, body: &B) -> AppResult<T> {
        let (s, b) = self
            .request(
                Request::post(path)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(body)?))
                    .unwrap(),
            )
            .await?;
        parse(s, &b)
    }

    async fn post_empty<T: DeserializeOwned>(&self, path: &str) -> AppResult<T> {
        let (s, b) = self
            .request(
                Request::post(path)
                    .header("content-type", "application/json")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await?;
        parse(s, &b)
    }

    async fn post_no_content<B: Serialize>(&self, path: &str, body: &B) -> AppResult<()> {
        let (s, b) = self
            .request(
                Request::post(path)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(body)?))
                    .unwrap(),
            )
            .await?;
        ensure_ok(s, &b)
    }

    async fn post_optional<B: Serialize, T: DeserializeOwned>(
        &self,
        path: &str,
        body: &B,
    ) -> AppResult<Option<T>> {
        let (s, b) = self
            .request(
                Request::post(path)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(body)?))
                    .unwrap(),
            )
            .await?;
        if s == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        ensure_ok(s, &b)?;
        Ok(Some(serde_json::from_slice(&b)?))
    }

    async fn patch<B: Serialize, T: DeserializeOwned>(&self, path: &str, body: &B) -> AppResult<T> {
        let (s, b) = self
            .request(
                Request::patch(path)
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(body)?))
                    .unwrap(),
            )
            .await?;
        parse(s, &b)
    }

    async fn delete<T: DeserializeOwned>(&self, path: &str) -> AppResult<T> {
        let (s, b) = self
            .request(Request::delete(path).body(Body::empty()).unwrap())
            .await?;
        parse(s, &b)
    }

    // --- Projects ---

    pub async fn project_list(&self) -> AppResult<ProjectsResponse> {
        self.get("/v1/projects").await
    }

    pub async fn project_show_by_id(&self, id: &str) -> AppResult<ProjectResponse> {
        self.get(&format!("/v1/projects/{id}")).await
    }

    pub async fn project_update(
        &self,
        id: &str,
        req: ProjectUpdateApiRequest,
    ) -> AppResult<ProjectResponse> {
        self.patch(&format!("/v1/projects/{id}"), &req).await
    }

    pub async fn project_delete(&self, id: &str) -> AppResult<ProjectDeleteResponse> {
        self.delete(&format!("/v1/projects/{id}")).await
    }

    pub async fn project_draft_create(
        &self,
        req: ProjectDraftCreateApiRequest,
    ) -> AppResult<ProjectDraftResponse> {
        self.post("/v1/project-drafts", &req).await
    }

    pub async fn project_draft_get(&self, id: Uuid) -> AppResult<ProjectDraftResponse> {
        self.get(&format!("/v1/project-drafts/{id}")).await
    }

    pub async fn project_draft_validate(
        &self,
        id: Uuid,
    ) -> AppResult<ProjectDraftValidateResponse> {
        self.post_empty(&format!("/v1/project-drafts/{id}/validate"))
            .await
    }

    pub async fn project_draft_confirm(&self, id: Uuid) -> AppResult<ProjectResponse> {
        self.post_empty(&format!("/v1/project-drafts/{id}/confirm"))
            .await
    }

    // --- Environments ---

    pub async fn environment_list(&self, project: &str) -> AppResult<EnvironmentsResponse> {
        self.get(&format!("/v1/projects/{project}/environments"))
            .await
    }

    pub async fn environment_draft_create(
        &self,
        project: &str,
    ) -> AppResult<EnvironmentDraftStartResponse> {
        self.post_empty(&format!("/v1/projects/{project}/environment-drafts"))
            .await
    }

    pub async fn environment_draft_get(&self, id: Uuid) -> AppResult<EnvironmentDraftResponse> {
        self.get(&format!("/v1/environment-drafts/{id}")).await
    }

    pub async fn environment_draft_refresh_branch(
        &self,
        id: Uuid,
        req: EnvironmentDraftUpdateApiRequest,
    ) -> AppResult<EnvironmentDraftStartResponse> {
        self.post(&format!("/v1/environment-drafts/{id}/branch"), &req)
            .await
    }

    pub async fn environment_draft_validate(
        &self,
        id: Uuid,
        req: EnvironmentDraftUpdateApiRequest,
    ) -> AppResult<EnvironmentDraftStartResponse> {
        self.post(&format!("/v1/environment-drafts/{id}/validate"), &req)
            .await
    }

    pub async fn environment_draft_confirm(&self, id: Uuid) -> AppResult<EnvironmentResponse> {
        self.post_empty(&format!("/v1/environment-drafts/{id}/confirm"))
            .await
    }

    pub async fn environment_release(
        &self,
        project: &str,
        slug: &str,
        req: EnvironmentReleaseApiRequest,
    ) -> AppResult<EnvironmentResponse> {
        self.post(
            &format!("/v1/projects/{project}/environments/{slug}/release"),
            &req,
        )
        .await
    }

    pub async fn environment_history(
        &self,
        project: &str,
        slug: &str,
    ) -> AppResult<EnvironmentVersionsResponse> {
        self.get(&format!(
            "/v1/projects/{project}/environments/{slug}/history"
        ))
        .await
    }

    pub async fn environment_active_resources(
        &self,
        project: &str,
        slug: &str,
        req: EnvironmentActiveResourcesApiRequest,
    ) -> AppResult<EnvironmentActiveResourcesResponse> {
        // GET with query params - encode manually
        let qs = serde_urlencoded::to_string(&req).unwrap_or_default();
        let path = if qs.is_empty() {
            format!("/v1/projects/{project}/environments/{slug}/active-resources")
        } else {
            format!("/v1/projects/{project}/environments/{slug}/active-resources?{qs}")
        };
        self.get(&path).await
    }

    pub async fn environment_actual_state(
        &self,
        project: &str,
        slug: &str,
    ) -> AppResult<EnvironmentActualStateResponse> {
        self.get(&format!(
            "/v1/projects/{project}/environments/{slug}/actual-state"
        ))
        .await
    }

    pub async fn environment_source_state_event_create(
        &self,
        project: &str,
        slug: &str,
        req: SourceStateEventCreateApiRequest,
    ) -> AppResult<SourceStateEventResponse> {
        self.post(
            &format!("/v1/projects/{project}/environments/{slug}/source-state-events"),
            &req,
        )
        .await
    }

    pub async fn environment_plan_list(
        &self,
        project: &str,
        slug: &str,
    ) -> AppResult<EnvironmentRunPlansResponse> {
        self.get(&format!("/v1/projects/{project}/environments/{slug}/plans"))
            .await
    }

    pub async fn environment_reconcile(
        &self,
        project: &str,
        slug: &str,
        req: EnvironmentReconcileApiRequest,
    ) -> AppResult<EnvironmentRunPlanResponse> {
        self.post(
            &format!("/v1/projects/{project}/environments/{slug}/reconcile"),
            &req,
        )
        .await
    }

    pub async fn environment_plan_get(
        &self,
        plan_id: Uuid,
    ) -> AppResult<EnvironmentRunPlanResponse> {
        self.get(&format!("/v1/plans/{plan_id}")).await
    }

    pub async fn environment_plan_admit(
        &self,
        plan_id: Uuid,
    ) -> AppResult<EnvironmentRunPlanResponse> {
        self.post_empty(&format!("/v1/plans/{plan_id}/admit")).await
    }

    pub async fn environment_rollback(
        &self,
        project: &str,
        slug: &str,
        req: EnvironmentRollbackApiRequest,
    ) -> AppResult<EnvironmentResponse> {
        self.post(
            &format!("/v1/projects/{project}/environments/{slug}/rollback"),
            &req,
        )
        .await
    }

    pub async fn environment_pause(
        &self,
        project: &str,
        slug: &str,
    ) -> AppResult<EnvironmentResponse> {
        self.post_empty(&format!("/v1/projects/{project}/environments/{slug}/pause"))
            .await
    }

    pub async fn environment_resume(
        &self,
        project: &str,
        slug: &str,
    ) -> AppResult<EnvironmentResponse> {
        self.post_empty(&format!(
            "/v1/projects/{project}/environments/{slug}/resume"
        ))
        .await
    }

    // --- Invocations ---

    pub async fn invocation_create(
        &self,
        req: InvocationCreateApiRequest,
    ) -> AppResult<InvocationCreateResponse> {
        self.post("/v1/invocations", &req).await
    }

    pub async fn invocation_status(&self, id: Uuid) -> AppResult<InvocationStatusResponse> {
        self.get(&format!("/v1/invocations/{id}")).await
    }

    pub async fn invocation_list(
        &self,
        req: InvocationListApiRequest,
    ) -> AppResult<InvocationsResponse> {
        let qs = serde_urlencoded::to_string(&req).unwrap_or_default();
        let path = if qs.is_empty() {
            "/v1/invocations".to_string()
        } else {
            format!("/v1/invocations?{qs}")
        };
        self.get(&path).await
    }

    pub async fn invocation_claim_next(
        &self,
        req: InvocationClaimNextApiRequest,
    ) -> AppResult<Option<InvocationClaimResponse>> {
        self.post_optional("/v1/invocations/claim-next", &req).await
    }

    pub async fn invocation_append_events(
        &self,
        id: Uuid,
        req: InvocationEventBatchApiRequest,
    ) -> AppResult<()> {
        self.post_no_content(&format!("/v1/invocations/{id}/events"), &req)
            .await
    }

    pub async fn invocation_complete(
        &self,
        id: Uuid,
        req: InvocationCompleteApiRequest,
    ) -> AppResult<()> {
        self.post_no_content(&format!("/v1/invocations/{id}/complete"), &req)
            .await
    }

    pub async fn invocation_heartbeat(
        &self,
        id: Uuid,
        req: InvocationHeartbeatApiRequest,
    ) -> AppResult<InvocationHeartbeatResponse> {
        self.post(&format!("/v1/invocations/{id}/heartbeat"), &req)
            .await
    }

    pub async fn invocation_cancel(
        &self,
        id: Uuid,
        req: InvocationCancelApiRequest,
    ) -> AppResult<()> {
        self.post_no_content(&format!("/v1/invocations/{id}/cancel"), &req)
            .await
    }

    // --- Operators ---

    pub async fn worker_list(&self) -> AppResult<WorkersResponse> {
        self.get("/v1/workers").await
    }

    pub async fn queue_list(&self) -> AppResult<QueuesResponse> {
        self.get("/v1/queues").await
    }

    pub async fn reconcile_tick(&self) -> AppResult<serde_json::Value> {
        self.post_empty("/v1/reconcile/tick").await
    }

    pub async fn sweep_tick(&self) -> AppResult<serde_json::Value> {
        self.post_empty("/v1/reconcile/sweep").await
    }
}

fn parse<T: DeserializeOwned>(status: StatusCode, bytes: &[u8]) -> AppResult<T> {
    ensure_ok(status, bytes)?;
    serde_json::from_slice(bytes).map_err(AppError::from)
}

fn ensure_ok(status: StatusCode, bytes: &[u8]) -> AppResult<()> {
    if status.is_success() {
        return Ok(());
    }
    let msg = serde_json::from_slice::<serde_json::Value>(bytes)
        .ok()
        .and_then(|v| {
            v.get("error")
                .and_then(|e| e.as_str())
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| String::from_utf8_lossy(bytes).to_string());
    Err(match status {
        StatusCode::PRECONDITION_FAILED => AppError::SchemaOutOfDate,
        _ => AppError::Internal(msg),
    })
}
