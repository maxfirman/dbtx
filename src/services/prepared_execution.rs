//! Prepared execution conversion for invocation startup.

use super::*;
use crate::api::{InvocationCommandApi, InvocationExecutionModeApi, InvocationExecutionSpecApi};
use crate::invocation_runtime::InvocationPersistence;

#[derive(Clone)]
pub(crate) struct PreparedInvocationStart {
    pub(crate) execution_spec: InvocationExecutionSpecApi,
    pub(crate) execution_mode: InvocationExecutionModeApi,
    pub(crate) persistence: Option<InvocationPersistence>,
}

impl LocalExecutionPrepared {
    pub(crate) fn into_invocation_start(
        self,
        command: InvocationCommandApi,
    ) -> PreparedInvocationStart {
        let execution_spec = match self.spec {
            PreparedExecutionSpec::Remote(spec) => InvocationExecutionSpecApi::Remote {
                command,
                args: os_args_to_strings(spec.args),
                repo_url: spec.repo_url,
                commit_sha: spec.commit_sha,
                project_root: spec.project_root,
                profiles_yml: spec.profiles_yml,
                state_manifest: spec.state_manifest,
            },
            PreparedExecutionSpec::ReleaseValidation(spec) => {
                InvocationExecutionSpecApi::ReleaseValidation {
                    repo_url: spec.repo_url,
                    git_ref: spec.git_ref,
                    git_commit_sha: spec.git_commit_sha,
                    git_branch: spec.git_branch,
                }
            }
            PreparedExecutionSpec::ProjectValidation(spec) => {
                InvocationExecutionSpecApi::ProjectValidation {
                    repo_url: spec.repo_url,
                    project_root: spec.project_root,
                }
            }
            PreparedExecutionSpec::EnvironmentPrepare(spec) => {
                InvocationExecutionSpecApi::EnvironmentPrepare {
                    repo_url: spec.repo_url,
                    selected_branch: spec.selected_branch,
                }
            }
            PreparedExecutionSpec::EnvironmentValidate(spec) => {
                InvocationExecutionSpecApi::EnvironmentValidate {
                    repo_url: spec.repo_url,
                    commit_sha: spec.commit_sha,
                    project_root: spec.project_root,
                    selected_branch: spec.selected_branch,
                    profiles_yml: spec.profiles_yml,
                }
            }
        };
        let execution_mode = InvocationExecutionModeApi::Server;
        let persistence = self.persistence.map(|p| InvocationPersistence {
            run_id: p.run_id,
            project_id: p.project_id,
            environment_id: p.environment_id,
            promote_base_manifest: p.promote_base_manifest,
            updates_actual_state: p.updates_actual_state,
        });
        PreparedInvocationStart {
            execution_spec,
            execution_mode,
            persistence,
        }
    }
}

fn os_args_to_strings(args: Vec<OsString>) -> Vec<String> {
    args.into_iter()
        .map(|value| value.to_string_lossy().into_owned())
        .collect()
}
