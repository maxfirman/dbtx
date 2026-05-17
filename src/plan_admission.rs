//! Plan admission startup workflow.

use crate::api::InvocationCommandApi;
use crate::error::AppResult;
use crate::invocation_bootstrap::start_prepared_invocation;
use crate::server::AppState;
use crate::services::EnvironmentService;
use uuid::Uuid;

#[derive(Debug, Clone)]
pub struct PlanAdmissionStart {
    pub invocation_id: Option<Uuid>,
}

pub async fn admit_and_start_plan(
    state: &AppState,
    plan_id: Uuid,
) -> AppResult<PlanAdmissionStart> {
    let invocation_id = Uuid::new_v4();
    let prepared = EnvironmentService::new(state.db())
        .admit_plan(invocation_id, plan_id)
        .await?;
    let Some(prepared_invocation) = prepared.prepared else {
        return Ok(PlanAdmissionStart {
            invocation_id: None,
        });
    };
    start_prepared_invocation(
        state,
        invocation_id,
        InvocationCommandApi::Build,
        Some(plan_id),
        prepared_invocation,
    )
    .await?;
    state
        .db()
        .mark_environment_run_plan_admitted(plan_id, invocation_id)
        .await?;
    Ok(PlanAdmissionStart {
        invocation_id: Some(invocation_id),
    })
}
