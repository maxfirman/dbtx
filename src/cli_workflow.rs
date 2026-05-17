//! Shared CLI workflows for invocation follow/wait behavior.

use crate::api::{InvocationEvent, InvocationStatusResponse};
use crate::client::DaemonClient;
use crate::error::AppResult;
use uuid::Uuid;

pub(crate) async fn stream_and_wait_for_invocation<F>(
    client: &DaemonClient,
    invocation_id: Uuid,
    render_event: F,
) -> AppResult<InvocationStatusResponse>
where
    F: FnMut(InvocationEvent),
{
    client
        .stream_invocation_events(invocation_id, render_event)
        .await?;
    wait_for_invocation_completion(client, invocation_id).await
}

pub(crate) async fn wait_for_invocation_completion(
    client: &DaemonClient,
    invocation_id: Uuid,
) -> AppResult<InvocationStatusResponse> {
    loop {
        let status = client.invocation_status(invocation_id).await?;
        if !matches!(
            status.status,
            crate::api::InvocationLifecycleStatus::Running
        ) {
            return Ok(status);
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}
