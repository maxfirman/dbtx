//! Worker Dbt process adapter: spawn, control-plane session, and execution loop.

use super::session::WorkerInvocationSession;
use crate::api::{InvocationClaimResponse, InvocationCommandApi, InvocationExecutionModeApi};
use crate::client::DaemonClient;
use crate::dbt_runner::{DbtExecutionConfig, DbtExecutionResult};
use crate::error::AppResult;
use std::ffi::OsString;
use std::path::Path;

pub(super) async fn run_worker_dbt_process(
    client: &DaemonClient,
    claim: &InvocationClaimResponse,
    command: &str,
    args: &[OsString],
    project_dir: &Path,
    parse_dbt_logs: bool,
) -> AppResult<DbtExecutionResult> {
    let dbt_path = crate::dbt_runner::dbt_path_from_env();
    let dbt_child = match crate::dbt_runner::DbtChild::spawn(&dbt_path, command, args, project_dir)
    {
        Ok(child) => child,
        Err(err) => {
            WorkerInvocationSession::new(client, claim)
                .complete_failed(&err.to_string())
                .await?;
            return Err(err);
        }
    };

    let session = WorkerInvocationSession::new(client, claim);
    let exec_config = DbtExecutionConfig {
        parse_dbt_logs,
        pretty_terminal_output: claim.execution_mode == InvocationExecutionModeApi::Local,
        invocation_id: claim.invocation_id,
        worker_id: claim.worker_id.clone(),
    };
    crate::dbt_runner::run_dbt_execution(dbt_child, &session, &exec_config).await
}

pub(super) fn command_persists_state(command: InvocationCommandApi) -> bool {
    command.persists_state()
}

#[cfg(test)]
mod tests {
    use super::command_persists_state;
    use crate::api::InvocationCommandApi;

    #[test]
    fn data_commands_persist_state() {
        assert!(command_persists_state(InvocationCommandApi::Build));
        assert!(command_persists_state(InvocationCommandApi::Run));
        assert!(command_persists_state(InvocationCommandApi::Test));
        assert!(command_persists_state(InvocationCommandApi::Seed));
        assert!(command_persists_state(
            InvocationCommandApi::ManifestPrepare
        ));
    }

    #[test]
    fn metadata_and_validation_commands_do_not_persist_state() {
        assert!(!command_persists_state(InvocationCommandApi::Ls));
        assert!(!command_persists_state(InvocationCommandApi::Release));
        assert!(!command_persists_state(
            InvocationCommandApi::ProjectValidate
        ));
        assert!(!command_persists_state(
            InvocationCommandApi::EnvironmentPrepare
        ));
        assert!(!command_persists_state(
            InvocationCommandApi::EnvironmentValidate
        ));
    }
}
