//! Worker-side invocation control-plane session.

use crate::api::{
    InvocationClaimResponse, InvocationCompleteApiRequest, InvocationEventBatchApiRequest,
    InvocationHeartbeatApiRequest, InvocationHeartbeatResponse, InvocationLifecycleStatus,
};
use crate::client::DaemonClient;
use crate::error::AppResult;

pub(super) struct WorkerInvocationSession<'a> {
    client: &'a DaemonClient,
    claim: &'a InvocationClaimResponse,
}

impl<'a> WorkerInvocationSession<'a> {
    pub(super) fn new(client: &'a DaemonClient, claim: &'a InvocationClaimResponse) -> Self {
        Self { client, claim }
    }

    pub(super) async fn append_event(
        &self,
        event: crate::execution::ExecutionEvent,
    ) -> AppResult<()> {
        self.client
            .invocation_append_events(
                self.claim.invocation_id,
                InvocationEventBatchApiRequest {
                    worker_id: self.claim.worker_id.clone(),
                    lease_token: self.claim.lease_token,
                    events: vec![event],
                },
            )
            .await
    }

    pub(super) async fn heartbeat(&self) -> AppResult<InvocationHeartbeatResponse> {
        self.client
            .invocation_heartbeat(
                self.claim.invocation_id,
                InvocationHeartbeatApiRequest {
                    worker_id: self.claim.worker_id.clone(),
                    lease_token: self.claim.lease_token,
                },
            )
            .await
    }

    pub(super) async fn complete(
        &self,
        completion: crate::execution::ExecutionCompletion,
    ) -> AppResult<()> {
        self.client
            .invocation_complete(
                self.claim.invocation_id,
                InvocationCompleteApiRequest {
                    worker_id: self.claim.worker_id.clone(),
                    lease_token: self.claim.lease_token,
                    completion,
                },
            )
            .await
    }

    pub(super) async fn complete_failed(&self, error_message: &str) -> AppResult<()> {
        self.complete(crate::execution::ExecutionCompletion {
            status: InvocationLifecycleStatus::Failed,
            exit_code: 1,
            error: Some(error_message.to_string()),
            dbt_version: None,
            manifest: None,
            result: None,
        })
        .await
    }
}
