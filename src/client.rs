use crate::api::{
    EnvironmentCreateApiRequest, EnvironmentReleaseApiRequest, EnvironmentResponse,
    EnvironmentRollbackApiRequest, EnvironmentUpdateApiRequest, EnvironmentVersionsResponse,
    EnvironmentsResponse, InvocationCancelApiRequest, InvocationClaimNextApiRequest,
    InvocationClaimResponse, InvocationCleanupApiRequest, InvocationCleanupResponse,
    InvocationCompleteApiRequest, InvocationCreateApiRequest, InvocationCreateResponse,
    InvocationEvent, InvocationEventBatchApiRequest, InvocationHeartbeatApiRequest,
    InvocationHeartbeatResponse, InvocationListApiRequest, InvocationStatusResponse,
    InvocationsResponse, MigrateResponse, ProjectInitApiRequest, ProjectResponse,
    ProjectShowApiRequest, ProjectUpdateApiRequest, ProjectsResponse, QueuesResponse,
    WorkersResponse,
};
use crate::error::{AppError, AppResult};
use futures_util::StreamExt;
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use std::path::Path;
use uuid::Uuid;

pub struct DaemonClient {
    base_url: String,
    http: reqwest::Client,
}

impl DaemonClient {
    pub fn new(base_url: String) -> Self {
        Self {
            base_url: base_url.trim_end_matches('/').to_string(),
            http: reqwest::Client::new(),
        }
    }

    pub async fn migrate(&self) -> AppResult<MigrateResponse> {
        self.send(self.http.post(self.url("/v1/state/migrate")))
            .await
    }

    pub async fn project_init(&self, request: ProjectInitApiRequest) -> AppResult<ProjectResponse> {
        self.send(self.http.post(self.url("/v1/projects:init")).json(&request))
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

    pub async fn project_show_with_context(
        &self,
        current_dir: &Path,
        project: Option<String>,
    ) -> AppResult<ProjectResponse> {
        self.send(
            self.http
                .post(self.url("/v1/projects/show"))
                .json(&ProjectShowApiRequest {
                    current_dir: current_dir.display().to_string(),
                    project,
                }),
        )
        .await
    }

    pub async fn environment_create(
        &self,
        request: EnvironmentCreateApiRequest,
    ) -> AppResult<EnvironmentResponse> {
        self.send(self.http.post(self.url("/v1/environments")).json(&request))
            .await
    }

    pub async fn environment_update(
        &self,
        project_id: &str,
        slug: &str,
        request: EnvironmentUpdateApiRequest,
    ) -> AppResult<EnvironmentResponse> {
        self.send(
            self.http
                .patch(self.url(&format!("/v1/projects/{project_id}/environments/{slug}")))
                .json(&request),
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
                .post(self.url(&format!("/v1/projects/{project_id}/environments/{slug}/release")))
                .json(&request),
        )
        .await
    }

    pub async fn environment_history(
        &self,
        project_id: &str,
        slug: &str,
    ) -> AppResult<EnvironmentVersionsResponse> {
        self.send(
            self.http
                .get(self.url(&format!("/v1/projects/{project_id}/environments/{slug}/history"))),
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
                .post(self.url(&format!("/v1/projects/{project_id}/environments/{slug}/rollback")))
                .json(&request),
        )
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
    let message = serde_json::from_str::<serde_json::Value>(&body)
        .ok()
        .and_then(|value| {
            value
                .get("error")
                .and_then(|value| value.as_str())
                .map(ToString::to_string)
        })
        .unwrap_or(body);
    Err(match status {
        StatusCode::PRECONDITION_FAILED => AppError::SchemaOutOfDate,
        _ => AppError::Io(std::io::Error::other(message)),
    })
}

fn map_reqwest_error(error: reqwest::Error) -> AppError {
    AppError::Io(std::io::Error::other(error.to_string()))
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
}
