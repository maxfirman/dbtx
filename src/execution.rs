//! Execution mode definitions, timeouts, and completion types.
use crate::api::{InvocationExecutionModeApi, InvocationLifecycleStatus};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use utoipa::ToSchema;

pub const LOCAL_CLAIM_STARTUP_TIMEOUT: Duration = Duration::from_secs(10);
pub const SERVER_CLAIM_STARTUP_TIMEOUT: Duration = Duration::from_secs(60);
pub const LOCAL_HEARTBEAT_STALE_TIMEOUT: Duration = Duration::from_secs(15);
pub const SERVER_HEARTBEAT_STALE_TIMEOUT: Duration = Duration::from_secs(60);

pub fn claim_startup_timeout(mode: InvocationExecutionModeApi) -> Duration {
    match mode {
        InvocationExecutionModeApi::Local => LOCAL_CLAIM_STARTUP_TIMEOUT,
        InvocationExecutionModeApi::Server => SERVER_CLAIM_STARTUP_TIMEOUT,
    }
}

pub fn heartbeat_stale_timeout(mode: InvocationExecutionModeApi) -> Duration {
    match mode {
        InvocationExecutionModeApi::Local => LOCAL_HEARTBEAT_STALE_TIMEOUT,
        InvocationExecutionModeApi::Server => SERVER_HEARTBEAT_STALE_TIMEOUT,
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Server,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub enum ExecutionEventKind {
    StdoutLine,
    StderrLine,
    DbtLog,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExecutionEvent {
    pub kind: ExecutionEventKind,
    pub occurred_at: DateTime<Utc>,
    pub text: Option<String>,
    pub raw_line: Option<String>,
    pub dbt_event_name: Option<String>,
    pub node_unique_id: Option<String>,
    pub level: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, ToSchema)]
pub struct ExecutionCompletion {
    pub status: InvocationLifecycleStatus,
    pub exit_code: i32,
    pub error: Option<String>,
    pub dbt_version: Option<String>,
    pub manifest: Option<serde_json::Value>,
    pub result: Option<serde_json::Value>,
}
