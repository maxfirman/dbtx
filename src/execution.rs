use crate::api::InvocationLifecycleStatus;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    Server,
    Local,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExecutionEventKind {
    StdoutLine,
    StderrLine,
    DbtLog,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionEvent {
    pub kind: ExecutionEventKind,
    pub occurred_at: DateTime<Utc>,
    pub text: Option<String>,
    pub dbt_event_name: Option<String>,
    pub node_unique_id: Option<String>,
    pub level: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionCompletion {
    pub status: InvocationLifecycleStatus,
    pub exit_code: i32,
    pub error: Option<String>,
}
