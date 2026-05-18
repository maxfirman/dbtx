//! Automatic reconciliation daemon: drift detection, blocked-plan sweep, and manifest preparation.
use crate::error::{AppError, AppResult};
use crate::process_state::ProcessState;
use crate::services::EnvironmentService;
use std::time::Duration;
use tracing::{error, info};

mod cycle;
pub use cycle::reconcile_environments_once;

/// Configuration for the reconciler daemon, resolved at startup.
#[derive(Debug, Clone)]
pub struct ReconcilerConfig {
    pub reconcile_interval: Duration,
    pub blocked_plan_sweep_interval: Duration,
}

impl ReconcilerConfig {
    /// Build config from environment variables, falling back to defaults.
    pub fn from_env() -> Self {
        Self {
            reconcile_interval: parse_interval_ms(
                std::env::var("DBTX_RECONCILE_INTERVAL_MS").ok().as_deref(),
                Duration::from_secs(5),
            ),
            blocked_plan_sweep_interval: parse_interval_ms(
                std::env::var("DBTX_BLOCKED_PLAN_SWEEP_INTERVAL_MS")
                    .ok()
                    .as_deref(),
                Duration::from_secs(2),
            ),
        }
    }
}

fn parse_interval_ms(value: Option<&str>, default: Duration) -> Duration {
    value
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|v| *v > 0)
        .map(Duration::from_millis)
        .unwrap_or(default)
}

pub async fn run(state: ProcessState, config: ReconcilerConfig) -> AppResult<()> {
    info!(
        reconcile_interval_ms = config.reconcile_interval.as_millis() as u64,
        blocked_plan_sweep_interval_ms = config.blocked_plan_sweep_interval.as_millis() as u64,
        "starting dbtx reconciler"
    );
    let mut reconcile_interval = tokio::time::interval(config.reconcile_interval);
    reconcile_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut blocked_interval = tokio::time::interval(config.blocked_plan_sweep_interval);
    blocked_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = reconcile_interval.tick() => {
                if let Err(err) = reconcile_environments_once(&state).await {
                    error!(error = %err, "environment reconcile sweep failed");
                }
            }
            _ = blocked_interval.tick() => {
                if let Err(err) = sweep_blocked_plans_once(&state).await {
                    error!(error = %err, "blocked plan sweep failed");
                }
            }
            _ = tokio::signal::ctrl_c() => {
                info!("received shutdown signal, stopping reconciler");
                return Ok(());
            }
        }
    }
}

pub async fn sweep_blocked_plans_once(state: &ProcessState) -> AppResult<usize> {
    let scopes = state.db().list_blocked_environment_scopes().await?;
    let mut admitted = 0usize;
    for (project_id, environment_id) in scopes {
        admitted +=
            auto_admit_blocked_plans_for_environment(state, project_id, environment_id).await?;
    }
    Ok(admitted)
}

pub async fn auto_admit_blocked_plans_for_environment(
    state: &ProcessState,
    project_id: i64,
    environment_id: i64,
) -> AppResult<usize> {
    let blocked_plan_ids = state
        .db()
        .list_blocked_environment_run_plan_ids(project_id, environment_id)
        .await?;
    let mut admitted = 0usize;

    for plan_id in blocked_plan_ids {
        let admission = EnvironmentService::new(state.db())
            .admit_and_start_plan(state, plan_id)
            .await?;
        let Some(invocation_id) = admission.invocation_id else {
            continue;
        };
        info!(
            plan_id = %plan_id,
            invocation_id = %invocation_id,
            project_id = project_id,
            environment_id = environment_id,
            "admitting blocked plan"
        );
        admitted += 1;
    }

    Ok(admitted)
}

fn should_ignore_reconcile_error(err: &AppError) -> bool {
    matches!(
        err,
        AppError::EnvironmentAlreadyReconciled
            | AppError::ReconciliationInProgress
            | AppError::ReconciliationEmptyPlan
            | AppError::PlanNotAdmissible(_, _)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::AppError;

    #[test]
    fn ignores_already_reconciled_error() {
        let err = AppError::EnvironmentAlreadyReconciled;
        assert!(should_ignore_reconcile_error(&err));
    }

    #[test]
    fn ignores_reconciliation_in_progress_error() {
        let err = AppError::ReconciliationInProgress;
        assert!(should_ignore_reconcile_error(&err));
    }

    #[test]
    fn ignores_empty_plan_error() {
        let err = AppError::ReconciliationEmptyPlan;
        assert!(should_ignore_reconcile_error(&err));
    }

    #[test]
    fn ignores_plan_not_admissible_error() {
        let err = AppError::PlanNotAdmissible(
            "550e8400-e29b-41d4-a716-446655440000".to_string(),
            "completed".to_string(),
        );
        assert!(should_ignore_reconcile_error(&err));
    }

    #[test]
    fn does_not_ignore_unrelated_io_error() {
        let err = AppError::Internal("connection refused".to_string());
        assert!(!should_ignore_reconcile_error(&err));
    }

    #[test]
    fn does_not_ignore_non_io_errors() {
        let err = AppError::SchemaOutOfDate;
        assert!(!should_ignore_reconcile_error(&err));
    }

    #[test]
    fn parse_interval_ms_defaults_when_none() {
        let default = Duration::from_secs(5);
        assert_eq!(parse_interval_ms(None, default), default);
    }

    #[test]
    fn parse_interval_ms_reads_value() {
        assert_eq!(
            parse_interval_ms(Some("2000"), Duration::from_secs(5)),
            Duration::from_millis(2000)
        );
    }

    #[test]
    fn parse_interval_ms_ignores_zero() {
        assert_eq!(
            parse_interval_ms(Some("0"), Duration::from_secs(5)),
            Duration::from_secs(5)
        );
    }

    #[test]
    fn parse_interval_ms_ignores_invalid() {
        assert_eq!(
            parse_interval_ms(Some("not_a_number"), Duration::from_secs(2)),
            Duration::from_secs(2)
        );
    }

    #[test]
    fn reconciler_config_has_expected_defaults() {
        // When env vars are not set, from_env() uses defaults
        let config = ReconcilerConfig {
            reconcile_interval: parse_interval_ms(None, Duration::from_secs(5)),
            blocked_plan_sweep_interval: parse_interval_ms(None, Duration::from_secs(2)),
        };
        assert_eq!(config.reconcile_interval, Duration::from_secs(5));
        assert_eq!(config.blocked_plan_sweep_interval, Duration::from_secs(2));
    }
}
