//! Project creation, validation, and update operations.

use super::*;

pub struct ProjectService<'a> {
    db: &'a Db,
}

impl<'a> ProjectService<'a> {
    pub fn new(db: &'a Db) -> Self {
        Self { db }
    }

    pub async fn create_draft(
        &self,
        request: ProjectCreateRequest,
    ) -> AppResult<ProjectDraftRecord> {
        self.db
            .create_project_draft(CreateProjectDraftInput {
                git_repo_url: request.git_repo_url,
                project_root: request.project_root,
            })
            .await
    }

    pub async fn prepare_draft_validation(
        &self,
        draft_id: Uuid,
    ) -> AppResult<ProjectDraftValidationPrepared> {
        let invocation_id = Uuid::new_v4();
        let draft = self.db.mark_project_draft_validating(draft_id).await?;
        Ok(ProjectDraftValidationPrepared {
            spec: ProjectValidationSpec {
                repo_url: draft.git_repo_url.clone(),
                project_root: draft.project_root.clone(),
            },
            worker_queue: validation_worker_queue(),
            draft,
            invocation_id,
        })
    }

    pub async fn get_draft(&self, draft_id: Uuid) -> AppResult<ProjectDraftRecord> {
        self.db.get_project_draft(draft_id).await
    }

    pub async fn confirm_draft(&self, draft_id: Uuid) -> AppResult<ProjectRecord> {
        self.db.confirm_project_draft(draft_id).await
    }

    pub async fn update(&self, request: ProjectUpdateRequest) -> AppResult<ProjectRecord> {
        let existing = self.db.get_project_by_project_id(&request.project).await?;
        if existing.mode != "remote" {
            return Err(AppError::RemoteExecutionRequiresRemoteProject(
                existing.project_id.clone(),
                existing.mode,
            ));
        }
        let git_repo_url = request.git_repo_url.or(existing.git_repo_url.clone());
        let project_root = request.project_root.or(existing.project_root.clone());
        validate_remote_project_root(
            project_root
                .as_deref()
                .ok_or_else(|| AppError::InvalidRemoteProjectRoot(String::new()))?,
        )?;
        self.db
            .update_project(CreateProjectInput {
                project_id: existing.project_id.clone(),
                project_name: existing.project_name,
                mode: "remote".to_string(),
                git_repo_url,
                default_branch: existing.default_branch,
                project_root,
            })
            .await
    }

    pub async fn list(&self) -> AppResult<Vec<ProjectRecord>> {
        self.db.list_projects().await
    }

    pub async fn show(&self, project: String) -> AppResult<ProjectRecord> {
        self.db.get_project_by_project_id(&project).await
    }

    pub async fn delete(&self, project: String) -> AppResult<()> {
        self.db.delete_project(&project).await
    }
}
