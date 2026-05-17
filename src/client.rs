//! HTTP client for communicating with the dbtx-server API.
use crate::api::{
    EnvironmentActiveResourcesApiRequest, EnvironmentActiveResourcesResponse,
    EnvironmentActualStateResponse, EnvironmentDraftResponse, EnvironmentDraftStartResponse,
    EnvironmentDraftUpdateApiRequest, EnvironmentReconcileApiRequest, EnvironmentReleaseApiRequest,
    EnvironmentResponse, EnvironmentRollbackApiRequest, EnvironmentRunPlanResponse,
    EnvironmentRunPlansResponse, EnvironmentVersionsResponse, EnvironmentsResponse,
    InvocationCancelApiRequest, InvocationClaimNextApiRequest, InvocationClaimResponse,
    InvocationCleanupApiRequest, InvocationCleanupResponse, InvocationCompleteApiRequest,
    InvocationCreateApiRequest, InvocationCreateResponse, InvocationEvent,
    InvocationEventBatchApiRequest, InvocationHeartbeatApiRequest, InvocationHeartbeatResponse,
    InvocationListApiRequest, InvocationStatusResponse, InvocationsResponse, MigrateResponse,
    ProjectDeleteResponse, ProjectDraftCreateApiRequest, ProjectDraftResponse,
    ProjectDraftValidateResponse, ProjectResponse, ProjectUpdateApiRequest, ProjectsResponse,
    QueuesResponse, SourceStateEventCreateApiRequest, SourceStateEventResponse, WorkersResponse,
};
use crate::error::{AppError, AppResult};
use futures_util::StreamExt;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use uuid::Uuid;

pub struct DaemonClient {
    base_url: String,
    http: reqwest::Client,
}

impl DaemonClient {
    pub fn new(base_url: String) -> Self {
        let http = reqwest::Client::builder()
            .connect_timeout(std::time::Duration::from_secs(5))
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .unwrap_or_else(|_| reqwest::Client::new());
        Self::with_http(base_url, http)
    }

    fn with_http(base_url: String, http: reqwest::Client) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http,
        }
    }

    pub async fn migrate(&self) -> AppResult<MigrateResponse> {
        self.send(self.http.post(self.url("/v1/state/migrate")))
            .await
    }

    pub async fn project_update(
        &self,
        project_id: &str,
        request: ProjectUpdateApiRequest,
    ) -> AppResult<ProjectResponse> {
        self.send(
            self.http
                .patch(self.url(&format!("/v1/projects/{project_id}")))
                .json(&request),
        )
        .await
    }

    pub async fn project_list(&self) -> AppResult<ProjectsResponse> {
        self.send(self.http.get(self.url("/v1/projects"))).await
    }

    pub async fn project_show_by_id(&self, project_id: &str) -> AppResult<ProjectResponse> {
        self.send(
            self.http
                .get(self.url(&format!("/v1/projects/{project_id}"))),
        )
        .await
    }

    pub async fn project_delete(&self, project_id: &str) -> AppResult<ProjectDeleteResponse> {
        self.send(
            self.http
                .delete(self.url(&format!("/v1/projects/{project_id}"))),
        )
        .await
    }

    pub async fn project_draft_create(
        &self,
        request: ProjectDraftCreateApiRequest,
    ) -> AppResult<ProjectDraftResponse> {
        self.send(
            self.http
                .post(self.url("/v1/project-drafts"))
                .json(&request),
        )
        .await
    }

    pub async fn project_draft_get(&self, draft_id: Uuid) -> AppResult<ProjectDraftResponse> {
        self.send(
            self.http
                .get(self.url(&format!("/v1/project-drafts/{draft_id}"))),
        )
        .await
    }

    pub async fn project_draft_validate(
        &self,
        draft_id: Uuid,
    ) -> AppResult<ProjectDraftValidateResponse> {
        self.send(
            self.http
                .post(self.url(&format!("/v1/project-drafts/{draft_id}/validate"))),
        )
        .await
    }

    pub async fn project_draft_confirm(&self, draft_id: Uuid) -> AppResult<ProjectResponse> {
        self.send(
            self.http
                .post(self.url(&format!("/v1/project-drafts/{draft_id}/confirm"))),
        )
        .await
    }

    pub async fn environment_draft_create(
        &self,
        project_id: &str,
    ) -> AppResult<EnvironmentDraftStartResponse> {
        self.send(
            self.http
                .post(self.url(&format!("/v1/projects/{project_id}/environment-drafts"))),
        )
        .await
    }

    pub async fn environment_draft_get(
        &self,
        draft_id: Uuid,
    ) -> AppResult<EnvironmentDraftResponse> {
        self.send(
            self.http
                .get(self.url(&format!("/v1/environment-drafts/{draft_id}"))),
        )
        .await
    }

    pub async fn environment_draft_refresh_branch(
        &self,
        draft_id: Uuid,
        request: EnvironmentDraftUpdateApiRequest,
    ) -> AppResult<EnvironmentDraftStartResponse> {
        self.send(
            self.http
                .post(self.url(&format!("/v1/environment-drafts/{draft_id}/branch")))
                .json(&request),
        )
        .await
    }

    pub async fn environment_draft_validate(
        &self,
        draft_id: Uuid,
        request: EnvironmentDraftUpdateApiRequest,
    ) -> AppResult<EnvironmentDraftStartResponse> {
        self.send(
            self.http
                .post(self.url(&format!("/v1/environment-drafts/{draft_id}/validate")))
                .json(&request),
        )
        .await
    }

    pub async fn environment_draft_confirm(
        &self,
        draft_id: Uuid,
    ) -> AppResult<EnvironmentResponse> {
        self.send(
            self.http
                .post(self.url(&format!("/v1/environment-drafts/{draft_id}/confirm"))),
        )
        .await
    }

    pub async fn environment_list(&self, project_id: &str) -> AppResult<EnvironmentsResponse> {
        self.send(
            self.http
                .get(self.url(&format!("/v1/projects/{project_id}/environments"))),
        )
        .await
    }

    pub async fn environment_show_by_id(
        &self,
        project_id: &str,
        slug: &str,
    ) -> AppResult<EnvironmentResponse> {
        self.send(
            self.http
                .get(self.url(&format!("/v1/projects/{project_id}/environments/{slug}"))),
        )
        .await
    }

    pub async fn environment_release(
        &self,
        project_id: &str,
        slug: &str,
        request: EnvironmentReleaseApiRequest,
    ) -> AppResult<EnvironmentResponse> {
        self.send(
            self.http
                .post(self.url(&format!(
                    "/v1/projects/{project_id}/environments/{slug}/release"
                )))
                .json(&request),
        )
        .await
    }

    pub async fn environment_history(
        &self,
        project_id: &str,
        slug: &str,
    ) -> AppResult<EnvironmentVersionsResponse> {
        self.send(self.http.get(self.url(&format!(
            "/v1/projects/{project_id}/environments/{slug}/history"
        ))))
        .await
    }

    pub async fn environment_active_resources(
        &self,
        project_id: &str,
        slug: &str,
        request: EnvironmentActiveResourcesApiRequest,
    ) -> AppResult<EnvironmentActiveResourcesResponse> {
        self.send(
            self.http
                .get(self.url(&format!(
                    "/v1/projects/{project_id}/environments/{slug}/active-resources"
                )))
                .query(&request),
        )
        .await
    }

    pub async fn environment_actual_state(
        &self,
        project_id: &str,
        slug: &str,
    ) -> AppResult<EnvironmentActualStateResponse> {
        self.send(self.http.get(self.url(&format!(
            "/v1/projects/{project_id}/environments/{slug}/actual-state"
        ))))
        .await
    }

    pub async fn environment_source_state_event_create(
        &self,
        project_id: &str,
        slug: &str,
        request: SourceStateEventCreateApiRequest,
    ) -> AppResult<SourceStateEventResponse> {
        self.send(
            self.http
                .post(self.url(&format!(
                    "/v1/projects/{project_id}/environments/{slug}/source-state-events"
                )))
                .json(&request),
        )
        .await
    }

    pub async fn environment_plan_list(
        &self,
        project_id: &str,
        slug: &str,
    ) -> AppResult<EnvironmentRunPlansResponse> {
        self.send(self.http.get(self.url(&format!(
            "/v1/projects/{project_id}/environments/{slug}/plans"
        ))))
        .await
    }

    pub async fn environment_reconcile(
        &self,
        project_id: &str,
        slug: &str,
        request: EnvironmentReconcileApiRequest,
    ) -> AppResult<EnvironmentRunPlanResponse> {
        self.send(
            self.http
                .post(self.url(&format!(
                    "/v1/projects/{project_id}/environments/{slug}/reconcile"
                )))
                .json(&request),
        )
        .await
    }

    pub async fn environment_plan_get(
        &self,
        plan_id: Uuid,
    ) -> AppResult<EnvironmentRunPlanResponse> {
        self.send(self.http.get(self.url(&format!("/v1/plans/{plan_id}"))))
            .await
    }

    pub async fn environment_plan_admit(
        &self,
        plan_id: Uuid,
    ) -> AppResult<EnvironmentRunPlanResponse> {
        self.send(
            self.http
                .post(self.url(&format!("/v1/plans/{plan_id}/admit"))),
        )
        .await
    }

    pub async fn environment_rollback(
        &self,
        project_id: &str,
        slug: &str,
        request: EnvironmentRollbackApiRequest,
    ) -> AppResult<EnvironmentResponse> {
        self.send(
            self.http
                .post(self.url(&format!(
                    "/v1/projects/{project_id}/environments/{slug}/rollback"
                )))
                .json(&request),
        )
        .await
    }

    pub async fn environment_pause(
        &self,
        project_id: &str,
        slug: &str,
    ) -> AppResult<EnvironmentResponse> {
        self.send(self.http.post(self.url(&format!(
            "/v1/projects/{project_id}/environments/{slug}/pause"
        ))))
        .await
    }

    pub async fn environment_resume(
        &self,
        project_id: &str,
        slug: &str,
    ) -> AppResult<EnvironmentResponse> {
        self.send(self.http.post(self.url(&format!(
            "/v1/projects/{project_id}/environments/{slug}/resume"
        ))))
        .await
    }

    pub async fn invocation_create(
        &self,
        request: InvocationCreateApiRequest,
    ) -> AppResult<InvocationCreateResponse> {
        self.send(self.http.post(self.url("/v1/invocations")).json(&request))
            .await
    }

    pub async fn invocation_status(
        &self,
        invocation_id: Uuid,
    ) -> AppResult<InvocationStatusResponse> {
        self.send(
            self.http
                .get(self.url(&format!("/v1/invocations/{invocation_id}"))),
        )
        .await
    }

    pub async fn invocation_list(
        &self,
        request: InvocationListApiRequest,
    ) -> AppResult<InvocationsResponse> {
        self.send(self.http.get(self.url("/v1/invocations")).query(&request))
            .await
    }

    pub async fn worker_list(&self) -> AppResult<WorkersResponse> {
        self.send(self.http.get(self.url("/v1/workers"))).await
    }

    pub async fn queue_list(&self) -> AppResult<QueuesResponse> {
        self.send(self.http.get(self.url("/v1/queues"))).await
    }

    pub async fn reconcile_tick(&self) -> AppResult<serde_json::Value> {
        self.send(self.http.post(self.url("/v1/reconcile/tick")))
            .await
    }

    pub async fn sweep_tick(&self) -> AppResult<serde_json::Value> {
        self.send(self.http.post(self.url("/v1/reconcile/sweep")))
            .await
    }

    pub async fn invocation_cleanup(
        &self,
        request: InvocationCleanupApiRequest,
    ) -> AppResult<InvocationCleanupResponse> {
        self.send(
            self.http
                .post(self.url("/v1/invocations/cleanup"))
                .json(&request),
        )
        .await
    }

    pub async fn invocation_claim_next(
        &self,
        request: InvocationClaimNextApiRequest,
    ) -> AppResult<Option<InvocationClaimResponse>> {
        self.send_optional(
            self.http
                .post(self.url("/v1/invocations/claim-next"))
                .json(&request),
        )
        .await
    }

    pub async fn invocation_append_events(
        &self,
        invocation_id: Uuid,
        request: InvocationEventBatchApiRequest,
    ) -> AppResult<()> {
        self.send_empty(
            self.http
                .post(self.url(&format!("/v1/invocations/{invocation_id}/events")))
                .json(&request),
        )
        .await
    }

    pub async fn invocation_complete(
        &self,
        invocation_id: Uuid,
        request: InvocationCompleteApiRequest,
    ) -> AppResult<()> {
        self.send_empty(
            self.http
                .post(self.url(&format!("/v1/invocations/{invocation_id}/complete")))
                .json(&request),
        )
        .await
    }

    pub async fn invocation_heartbeat(
        &self,
        invocation_id: Uuid,
        request: InvocationHeartbeatApiRequest,
    ) -> AppResult<InvocationHeartbeatResponse> {
        self.send(
            self.http
                .post(self.url(&format!("/v1/invocations/{invocation_id}/heartbeat")))
                .json(&request),
        )
        .await
    }

    pub async fn invocation_cancel(
        &self,
        invocation_id: Uuid,
        request: InvocationCancelApiRequest,
    ) -> AppResult<()> {
        self.send_empty(
            self.http
                .post(self.url(&format!("/v1/invocations/{invocation_id}/cancel")))
                .json(&request),
        )
        .await
    }

    pub async fn stream_invocation_events<F>(
        &self,
        invocation_id: Uuid,
        mut on_event: F,
    ) -> AppResult<()>
    where
        F: FnMut(InvocationEvent),
    {
        let response = self
            .http
            .get(self.url(&format!("/v1/invocations/{invocation_id}/events")))
            .send()
            .await
            .map_err(map_reqwest_error)?;
        let response = ensure_success(response).await?;
        let mut stream = response.bytes_stream();
        let mut buffer = String::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(map_reqwest_error)?;
            buffer.push_str(&String::from_utf8_lossy(&chunk));
            while let Some(idx) = buffer.find("\n\n") {
                let frame = buffer[..idx].to_string();
                buffer.drain(..idx + 2);
                if let Some(event) = parse_sse_frame(&frame)? {
                    let is_completed = event.event_type == "invocation.completed";
                    on_event(event);
                    if is_completed {
                        return Ok(());
                    }
                }
            }
        }
        Ok(())
    }

    async fn send<T: DeserializeOwned>(&self, request: reqwest::RequestBuilder) -> AppResult<T> {
        let response = request.send().await.map_err(map_reqwest_error)?;
        let response = ensure_success(response).await?;
        response.json().await.map_err(map_reqwest_error)
    }

    async fn send_optional<T: DeserializeOwned>(
        &self,
        request: reqwest::RequestBuilder,
    ) -> AppResult<Option<T>> {
        let response = request.send().await.map_err(map_reqwest_error)?;
        if response.status() == StatusCode::NO_CONTENT {
            return Ok(None);
        }
        let response = ensure_success(response).await?;
        response.json().await.map(Some).map_err(map_reqwest_error)
    }

    async fn send_empty(&self, request: reqwest::RequestBuilder) -> AppResult<()> {
        let response = request.send().await.map_err(map_reqwest_error)?;
        let _ = ensure_success(response).await?;
        Ok(())
    }

    fn url(&self, path: &str) -> String {
        format!("{}{}", self.base_url, path)
    }
}

fn parse_sse_frame(frame: &str) -> AppResult<Option<InvocationEvent>> {
    let mut data = None;
    for line in frame.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            data = Some(rest.trim_start().to_string());
        }
    }
    match data {
        Some(data) if !data.is_empty() => Ok(Some(serde_json::from_str(&data)?)),
        _ => Ok(None),
    }
}

async fn ensure_success(response: reqwest::Response) -> AppResult<reqwest::Response> {
    if response.status().is_success() {
        return Ok(response);
    }
    let status = response.status();
    let body = response.text().await.map_err(map_reqwest_error)?;
    Err(error_from_response_body(status, &body))
}

pub fn error_from_response_body(status: StatusCode, body: &str) -> AppError {
    let message = serde_json::from_str::<serde_json::Value>(body)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| body.to_string());
    match status {
        StatusCode::PRECONDITION_FAILED => AppError::SchemaOutOfDate,
        _ => AppError::Internal(message),
    }
}

fn map_reqwest_error(error: reqwest::Error) -> AppError {
    if error.is_timeout() {
        AppError::Internal(format!("request timed out: {error}"))
    } else {
        AppError::Internal(error.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::parse_sse_frame;

    #[test]
    fn parses_sse_json_frame() {
        let event = parse_sse_frame(
            "event: stdout.line\ndata: {\"event_type\":\"stdout.line\",\"timestamp\":\"2026-03-23T12:00:00Z\",\"text\":\"hello\",\"stream\":\"stdout\",\"dbt_event_name\":null,\"node_unique_id\":null,\"level\":null,\"exit_code\":null,\"error\":null}\n\n",
        )
        .expect("parse frame")
        .expect("event exists");
        assert_eq!(event.event_type, "stdout.line");
        assert_eq!(event.text.as_deref(), Some("hello"));
    }

    #[tokio::test]
    async fn client_returns_error_on_404() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(404)
                    .set_body_json(serde_json::json!({"error": "project not found"})),
            )
            .mount(&mock_server)
            .await;

        let client = super::DaemonClient::new(mock_server.uri());
        let err = client.project_list().await.expect_err("should fail");
        assert!(err.to_string().contains("project not found"));
    }

    #[tokio::test]
    async fn client_returns_error_on_500() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(500)
                    .set_body_json(serde_json::json!({"error": "internal server error"})),
            )
            .mount(&mock_server)
            .await;

        let client = super::DaemonClient::new(mock_server.uri());
        let err = client.project_list().await.expect_err("should fail");
        assert!(err.to_string().contains("internal server error"));
    }

    #[tokio::test]
    async fn client_returns_error_on_409_conflict() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/projects/prj_1/environments/prod/reconcile"))
            .respond_with(
                ResponseTemplate::new(409)
                    .set_body_json(serde_json::json!({"error": "environment is already reconciled to known desired state"})),
            )
            .mount(&mock_server)
            .await;

        let client = super::DaemonClient::new(mock_server.uri());
        let err = client
            .environment_reconcile(
                "prj_1",
                "prod",
                crate::api::EnvironmentReconcileApiRequest {},
            )
            .await
            .expect_err("should fail");
        assert!(err.to_string().contains("already reconciled"));
    }

    #[tokio::test]
    async fn client_handles_timeout() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_secs(60)))
            .mount(&mock_server)
            .await;

        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_millis(100))
            .build()
            .unwrap();
        let client = super::DaemonClient::with_http(mock_server.uri(), http);
        let err = client.project_list().await.expect_err("should time out");
        assert!(
            matches!(err, crate::error::AppError::Internal(ref message) if message.contains("timed out")),
            "expected timeout-style internal error, got: {err}"
        );
    }

    #[tokio::test]
    async fn client_returns_schema_out_of_date_on_412() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock_server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(412)
                    .set_body_json(serde_json::json!({"error": "schema out of date"})),
            )
            .mount(&mock_server)
            .await;

        let client = super::DaemonClient::new(mock_server.uri());
        let err = client.project_list().await.expect_err("should fail");
        assert!(matches!(err, crate::error::AppError::SchemaOutOfDate));
    }

    #[test]
    fn parse_sse_frame_returns_none_for_empty_data() {
        assert!(parse_sse_frame("event: ping\n\n").unwrap().is_none());
        assert!(parse_sse_frame("").unwrap().is_none());
    }

    #[test]
    fn parse_sse_frame_returns_none_for_data_only_whitespace() {
        assert!(parse_sse_frame("data: \n\n").unwrap().is_none());
    }
}
