//! Worker dbt execution completion semantics.

use crate::api::InvocationLifecycleStatus;
use crate::dbt_runner::DbtChildResult;
use crate::error::{AppError, AppResult};
use serde_json::Value;

pub(super) struct WorkerDbtCompletion {
    pub(super) exit_code: i32,
    pub(super) completion: crate::execution::ExecutionCompletion,
}

impl WorkerDbtCompletion {
    pub(super) fn as_result(&self) -> AppResult<()> {
        if self.completion.status == InvocationLifecycleStatus::Canceled {
            Err(AppError::InvocationCanceled)
        } else if self.exit_code == 0 {
            Ok(())
        } else {
            Err(AppError::DbtFailed(self.exit_code))
        }
    }
}

pub(super) fn complete_worker_dbt_invocation(
    result: DbtChildResult,
    cancel_requested: bool,
    dbt_version: Option<String>,
    manifest: Option<Value>,
) -> WorkerDbtCompletion {
    let exit_code = if cancel_requested {
        130
    } else {
        result.exit_code
    };
    WorkerDbtCompletion {
        exit_code,
        completion: crate::execution::ExecutionCompletion {
            status: if cancel_requested {
                InvocationLifecycleStatus::Canceled
            } else if exit_code != 0 {
                InvocationLifecycleStatus::Failed
            } else {
                InvocationLifecycleStatus::Succeeded
            },
            exit_code,
            error: if cancel_requested {
                Some("invocation canceled".to_string())
            } else if exit_code == 0 {
                None
            } else {
                Some(format!("dbt invocation failed with exit code {exit_code}"))
            },
            dbt_version,
            manifest,
            result: None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn result(exit_code: i32) -> DbtChildResult {
        DbtChildResult {
            exit_code,
            stderr_lines: Vec::new(),
        }
    }

    #[test]
    fn successful_completion_maps_to_succeeded() {
        let completion = complete_worker_dbt_invocation(result(0), false, None, None);
        assert_eq!(completion.exit_code, 0);
        assert_eq!(
            completion.completion.status,
            InvocationLifecycleStatus::Succeeded
        );
        assert!(completion.completion.error.is_none());
        assert!(completion.as_result().is_ok());
    }

    #[test]
    fn failed_completion_maps_to_dbt_failed() {
        let completion = complete_worker_dbt_invocation(result(2), false, None, None);
        assert_eq!(
            completion.completion.status,
            InvocationLifecycleStatus::Failed
        );
        assert!(matches!(
            completion.as_result(),
            Err(AppError::DbtFailed(2))
        ));
    }

    #[test]
    fn canceled_completion_overrides_exit_code() {
        let completion = complete_worker_dbt_invocation(result(0), true, None, None);
        assert_eq!(completion.exit_code, 130);
        assert_eq!(
            completion.completion.status,
            InvocationLifecycleStatus::Canceled
        );
        assert!(matches!(
            completion.as_result(),
            Err(AppError::InvocationCanceled)
        ));
    }
}
