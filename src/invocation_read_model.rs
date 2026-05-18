//! Invocation read-model concepts shared by operator views and persistence filters.

use crate::api::{InvocationCancelStateApi, InvocationLifecycleStatus, InvocationStatusResponse};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum InvocationDisplayStatus {
    Queued,
    Running,
    Cancelling,
    Succeeded,
    Failed,
    Canceled,
}

impl InvocationDisplayStatus {
    pub const ALL: [Self; 6] = [
        Self::Queued,
        Self::Running,
        Self::Cancelling,
        Self::Succeeded,
        Self::Failed,
        Self::Canceled,
    ];

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Cancelling => "cancelling",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }

    pub fn label(self) -> &'static str {
        match self {
            Self::Queued => "Queued",
            Self::Running => "Running",
            Self::Cancelling => "Cancelling",
            Self::Succeeded => "Succeeded",
            Self::Failed => "Failed",
            Self::Canceled => "Canceled",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "running" => Some(Self::Running),
            "cancelling" => Some(Self::Cancelling),
            "succeeded" => Some(Self::Succeeded),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            _ => None,
        }
    }

    pub fn from_invocation(invocation: &InvocationStatusResponse) -> Self {
        match invocation.status {
            InvocationLifecycleStatus::Running if invocation.claimed_by.is_none() => Self::Queued,
            InvocationLifecycleStatus::Running
                if !matches!(invocation.cancel_state, InvocationCancelStateApi::None) =>
            {
                Self::Cancelling
            }
            InvocationLifecycleStatus::Running => Self::Running,
            InvocationLifecycleStatus::Succeeded => Self::Succeeded,
            InvocationLifecycleStatus::Failed => Self::Failed,
            InvocationLifecycleStatus::Canceled => Self::Canceled,
        }
    }
}

impl std::fmt::Display for InvocationDisplayStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::{
        InvocationExecutionModeApi, InvocationLifecycleStatus, InvocationStatusResponse,
        InvocationWorkerHealthApi,
    };
    use chrono::Utc;
    use uuid::Uuid;

    fn base() -> InvocationStatusResponse {
        InvocationStatusResponse {
            invocation_id: Uuid::nil(),
            execution_mode: InvocationExecutionModeApi::Server,
            worker_queue: "default".to_string(),
            worker_health: InvocationWorkerHealthApi::Unclaimed,
            status: InvocationLifecycleStatus::Running,
            exit_code: None,
            error: None,
            started_at: Utc::now(),
            claimed_at: None,
            last_heartbeat_at: None,
            cancel_requested_at: None,
            completed_at: None,
            cancel_state: InvocationCancelStateApi::None,
            cancel_requested: false,
            claimed_by: None,
        }
    }

    #[test]
    fn parses_all_display_status_values() {
        for status in InvocationDisplayStatus::ALL {
            assert_eq!(
                InvocationDisplayStatus::parse(status.as_str()),
                Some(status)
            );
        }
        assert_eq!(InvocationDisplayStatus::parse("bogus"), None);
    }

    #[test]
    fn derives_display_status_from_invocation_state() {
        assert_eq!(
            InvocationDisplayStatus::from_invocation(&base()),
            InvocationDisplayStatus::Queued
        );

        let mut running = base();
        running.claimed_by = Some("worker-1".to_string());
        assert_eq!(
            InvocationDisplayStatus::from_invocation(&running),
            InvocationDisplayStatus::Running
        );

        let mut cancelling = running;
        cancelling.cancel_state = InvocationCancelStateApi::Requested;
        assert_eq!(
            InvocationDisplayStatus::from_invocation(&cancelling),
            InvocationDisplayStatus::Cancelling
        );
    }
}
