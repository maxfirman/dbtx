//! Reconcile input identity: typed reasons, preparation kinds, and fingerprints.

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReconcileReason {
    CodeChange,
    SourceStateChange,
}

impl ReconcileReason {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::CodeChange => "code_change",
            Self::SourceStateChange => "source_state_change",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "code_change" => Some(Self::CodeChange),
            "source_state_change" => Some(Self::SourceStateChange),
            _ => None,
        }
    }
}

impl std::fmt::Display for ReconcileReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreparationKind {
    TargetManifest,
}

impl PreparationKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::TargetManifest => "target_manifest",
        }
    }
}

impl std::fmt::Display for PreparationKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReconcileInputIdentity {
    pub reason: ReconcileReason,
    pub fingerprint: String,
}

impl ReconcileInputIdentity {
    pub fn code_change(desired_commit_sha: &str, baseline_run_id: Option<Uuid>) -> Self {
        Self {
            reason: ReconcileReason::CodeChange,
            fingerprint: code_change_input_fingerprint_for_baseline(
                desired_commit_sha,
                baseline_run_id,
            ),
        }
    }

    pub fn source_state_change(source_event_ids: &[i64]) -> Self {
        Self {
            reason: ReconcileReason::SourceStateChange,
            fingerprint: source_state_change_input_fingerprint(source_event_ids),
        }
    }

    pub fn matches_plan(&self, plan: &EnvironmentRunPlanRecord) -> bool {
        ReconcileReason::parse(&plan.reason) == Some(self.reason)
            && plan.input_fingerprint.as_deref() == Some(self.fingerprint.as_str())
    }

    pub fn target_manifest_preparation_fingerprint(&self) -> String {
        target_manifest_input_fingerprint(&self.fingerprint)
    }
}

pub fn target_manifest_input_fingerprint(reconcile_input_fingerprint: &str) -> String {
    format!("target_manifest:{reconcile_input_fingerprint}")
}

pub fn code_change_input_fingerprint_for_baseline(
    desired_commit_sha: &str,
    baseline_run_id: Option<Uuid>,
) -> String {
    match baseline_run_id {
        Some(baseline_run_id) => code_change_input_fingerprint(desired_commit_sha, baseline_run_id),
        None => format!("code_change:{desired_commit_sha}:initial"),
    }
}

pub fn code_change_input_fingerprint(desired_commit_sha: &str, baseline_run_id: Uuid) -> String {
    format!("code_change:{desired_commit_sha}:{baseline_run_id}")
}

pub fn source_state_change_input_fingerprint(source_event_ids: &[i64]) -> String {
    let mut event_ids = source_event_ids.to_vec();
    event_ids.sort_unstable();
    event_ids.dedup();
    let joined = event_ids
        .into_iter()
        .map(|id| id.to_string())
        .collect::<Vec<_>>()
        .join(",");
    format!("source_state_change:{joined}")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn plan(reason: &str, fingerprint: Option<String>) -> EnvironmentRunPlanRecord {
        EnvironmentRunPlanRecord {
            plan_id: Uuid::new_v4(),
            project_id: 1,
            environment_id: 1,
            status: PlanStatus::Planned,
            reason: reason.to_string(),
            input_fingerprint: fingerprint,
            target_git_branch: None,
            target_git_commit_sha: None,
            baseline_run_id: None,
            selection_spec: None,
            selected_resources: Vec::new(),
            resource_count: 0,
            source_event_id: None,
            admitted_invocation_id: None,
            superseded_by_plan_id: None,
            blocked_by_invocation_id: None,
            error: None,
            retry_count: 0,
            failure_count: 0,
            next_attempt_at: None,
            first_blocked_at: None,
            last_blocked_at: None,
            last_checked_at: None,
            admitted_at: None,
            completed_at: None,
            created_at: chrono::Utc::now(),
            updated_at: chrono::Utc::now(),
            metadata: Value::Null,
        }
    }

    #[test]
    fn source_state_fingerprint_is_order_insensitive() {
        assert_eq!(
            source_state_change_input_fingerprint(&[3, 1, 2, 1]),
            source_state_change_input_fingerprint(&[1, 2, 3])
        );
    }

    #[test]
    fn identity_matches_plan_reason_and_fingerprint() {
        let identity = ReconcileInputIdentity::source_state_change(&[9, 7]);
        let matching = plan(identity.reason.as_str(), Some(identity.fingerprint.clone()));
        assert!(identity.matches_plan(&matching));

        let wrong_reason = plan(
            ReconcileReason::CodeChange.as_str(),
            Some(identity.fingerprint.clone()),
        );
        assert!(!identity.matches_plan(&wrong_reason));
    }

    #[test]
    fn target_manifest_fingerprint_wraps_reconcile_input() {
        let identity = ReconcileInputIdentity::code_change("abc123", None);
        assert_eq!(
            identity.target_manifest_preparation_fingerprint(),
            "target_manifest:code_change:abc123:initial"
        );
    }
}
