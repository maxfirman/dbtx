//! Project and project draft CRUD operations.

use super::*;

impl Db {
    pub async fn create_project(&self, input: CreateProjectInput) -> AppResult<ProjectRecord> {
        validate_project_input(&input.mode, input.project_root.as_deref())?;
        let row = sqlx::query(
            r#"
            INSERT INTO projects (project_id, project_name, mode, git_repo_url, default_branch, project_root)
            VALUES ($1, $2, $3, $4, COALESCE($5, 'main'), $6)
            RETURNING id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata
            "#,
        )
        .bind(&input.project_id)
        .bind(&input.project_name)
        .bind(&input.mode)
        .bind(input.git_repo_url.as_deref())
        .bind(input.default_branch.as_deref())
        .bind(input.project_root.as_deref())
        .fetch_one(&self.pool)
        .await?;
        Ok(project_record_from_row(&row))
    }

    pub async fn update_project(&self, input: CreateProjectInput) -> AppResult<ProjectRecord> {
        validate_project_input(&input.mode, input.project_root.as_deref())?;
        let row = sqlx::query(
            r#"
            UPDATE projects
            SET project_name = $2,
                mode = $3,
                git_repo_url = $4,
                default_branch = COALESCE($5, 'main'),
                project_root = $6
            WHERE project_id = $1
            RETURNING id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata
            "#,
        )
        .bind(&input.project_id)
        .bind(&input.project_name)
        .bind(&input.mode)
        .bind(input.git_repo_url.as_deref())
        .bind(input.default_branch.as_deref())
        .bind(input.project_root.as_deref())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::ProjectIdNotFound(input.project_id.clone()))?;
        Ok(project_record_from_row(&row))
    }

    pub async fn upsert_project(&self, input: CreateProjectInput) -> AppResult<ProjectRecord> {
        validate_project_input(&input.mode, input.project_root.as_deref())?;
        let row = sqlx::query(
            r#"
            INSERT INTO projects (project_id, project_name, mode, git_repo_url, default_branch, project_root)
            VALUES ($1, $2, $3, $4, COALESCE($5, 'main'), $6)
            ON CONFLICT (project_id) DO UPDATE
            SET project_name = EXCLUDED.project_name,
                mode = EXCLUDED.mode,
                git_repo_url = EXCLUDED.git_repo_url,
                default_branch = EXCLUDED.default_branch,
                project_root = EXCLUDED.project_root
            RETURNING id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata
            "#,
        )
        .bind(&input.project_id)
        .bind(&input.project_name)
        .bind(&input.mode)
        .bind(input.git_repo_url.as_deref())
        .bind(input.default_branch.as_deref())
        .bind(input.project_root.as_deref())
        .fetch_one(&self.pool)
        .await?;
        Ok(project_record_from_row(&row))
    }

    pub async fn reinitialize_project_id(
        &self,
        existing_project_id: &str,
        input: CreateProjectInput,
    ) -> AppResult<ProjectRecord> {
        validate_project_input(&input.mode, input.project_root.as_deref())?;
        let mut tx = self.pool.begin().await?;
        let existing_row = sqlx::query("SELECT id FROM projects WHERE project_id = $1")
            .bind(existing_project_id)
            .fetch_optional(&mut *tx)
            .await?;

        let Some(existing_row) = existing_row else {
            tx.rollback().await?;
            return self.create_project(input).await;
        };

        let project_pk: i64 = existing_row.get("id");

        sqlx::query("DELETE FROM environments WHERE project_id = $1")
            .bind(project_pk)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM runs WHERE project_id = $1")
            .bind(project_pk)
            .execute(&mut *tx)
            .await?;

        let row = sqlx::query(
            r#"
            UPDATE projects
            SET project_id = $2,
                project_name = $3,
                mode = $4,
                git_repo_url = $5,
                default_branch = COALESCE($6, 'main'),
                project_root = $7
            WHERE id = $1
            RETURNING id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata
            "#,
        )
        .bind(project_pk)
        .bind(&input.project_id)
        .bind(&input.project_name)
        .bind(&input.mode)
        .bind(input.git_repo_url.as_deref())
        .bind(input.default_branch.as_deref())
        .bind(input.project_root.as_deref())
        .fetch_one(&mut *tx)
        .await?;

        tx.commit().await?;
        Ok(project_record_from_row(&row))
    }

    pub async fn list_projects(&self) -> AppResult<Vec<ProjectRecord>> {
        let rows = sqlx::query(
            "SELECT id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata FROM projects ORDER BY project_name",
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(rows.iter().map(project_record_from_row).collect())
    }

    pub async fn create_project_draft(
        &self,
        input: CreateProjectDraftInput,
    ) -> AppResult<ProjectDraftRecord> {
        validate_remote_project_root(&input.project_root)?;
        let row = sqlx::query(
            r#"
            INSERT INTO project_onboarding_drafts (
                id, git_repo_url, project_root, status
            )
            VALUES ($1, $2, $3, 'draft')
            RETURNING id, git_repo_url, project_root, status, validation_error, project_name,
                default_branch, validation_invocation_id, created_at, updated_at, validated_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(&input.git_repo_url)
        .bind(&input.project_root)
        .fetch_one(&self.pool)
        .await?;
        Ok(project_draft_record_from_row(&row))
    }

    pub async fn get_project_draft(&self, draft_id: Uuid) -> AppResult<ProjectDraftRecord> {
        let row = sqlx::query(
            r#"
            SELECT id, git_repo_url, project_root, status, validation_error, project_name,
                default_branch, validation_invocation_id, created_at, updated_at, validated_at
            FROM project_onboarding_drafts
            WHERE id = $1
            "#,
        )
        .bind(draft_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("project draft '{draft_id}' was not found"),
            ))
        })?;
        Ok(project_draft_record_from_row(&row))
    }

    pub async fn create_environment_draft(
        &self,
        input: CreateEnvironmentDraftInput,
    ) -> AppResult<EnvironmentDraftRecord> {
        let row = sqlx::query(
            r#"
            INSERT INTO environment_onboarding_drafts (
                id, project_id, git_branch, status
            )
            VALUES ($1, $2, $3, 'loading_git')
            RETURNING id, project_id, slug, git_branch, git_commit_sha, use_latest_commit,
                auto_deploy, immutable, adapter_type, schema_name, threads, profile_config,
                profile_secrets, branch_options, commit_options, status, validation_error,
                validation_invocation_id, created_at, updated_at, validated_at
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(input.project_id)
        .bind(input.default_branch.as_deref())
        .fetch_one(&self.pool)
        .await?;
        Ok(environment_draft_record_from_row(&row))
    }

    pub async fn get_environment_draft(&self, draft_id: Uuid) -> AppResult<EnvironmentDraftRecord> {
        let row = sqlx::query(
            r#"
            SELECT id, project_id, slug, git_branch, git_commit_sha, use_latest_commit,
                auto_deploy, immutable, adapter_type, schema_name, threads, profile_config,
                profile_secrets, branch_options, commit_options, status, validation_error,
                validation_invocation_id, created_at, updated_at, validated_at
            FROM environment_onboarding_drafts
            WHERE id = $1
            "#,
        )
        .bind(draft_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("environment draft '{draft_id}' was not found"),
            ))
        })?;
        Ok(environment_draft_record_from_row(&row))
    }

    pub async fn update_environment_draft(
        &self,
        draft_id: Uuid,
        input: UpdateEnvironmentDraftInput,
    ) -> AppResult<EnvironmentDraftRecord> {
        let encrypted_secrets = crate::profile::encrypt_json(&input.profile_secrets)?;
        let row = sqlx::query(
            r#"
            UPDATE environment_onboarding_drafts
            SET slug = $2,
                git_branch = $3,
                git_commit_sha = $4,
                use_latest_commit = $5,
                auto_deploy = $6,
                immutable = $7,
                adapter_type = $8,
                schema_name = $9,
                threads = $10,
                profile_config = $11,
                profile_secrets = $12,
                validation_error = NULL,
                updated_at = NOW()
            WHERE id = $1
            RETURNING id, project_id, slug, git_branch, git_commit_sha, use_latest_commit,
                auto_deploy, immutable, adapter_type, schema_name, threads, profile_config,
                profile_secrets, branch_options, commit_options, status, validation_error,
                validation_invocation_id, created_at, updated_at, validated_at
            "#,
        )
        .bind(draft_id)
        .bind(&input.slug)
        .bind(input.git_branch.as_deref())
        .bind(input.git_commit_sha.as_deref())
        .bind(input.use_latest_commit)
        .bind(input.auto_deploy)
        .bind(input.immutable)
        .bind(input.adapter_type.as_deref())
        .bind(input.schema_name.as_deref())
        .bind(input.threads)
        .bind(&input.profile_config)
        .bind(&encrypted_secrets)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("environment draft '{draft_id}' was not found"),
            ))
        })?;
        Ok(environment_draft_record_from_row(&row))
    }

    pub async fn mark_environment_draft_loading_git(
        &self,
        draft_id: Uuid,
    ) -> AppResult<EnvironmentDraftRecord> {
        let row = sqlx::query(
            r#"
            UPDATE environment_onboarding_drafts
            SET status = 'loading_git',
                validation_error = NULL,
                validation_invocation_id = NULL,
                validated_at = NULL,
                updated_at = NOW()
            WHERE id = $1
            RETURNING id, project_id, slug, git_branch, git_commit_sha, use_latest_commit,
                auto_deploy, immutable, adapter_type, schema_name, threads, profile_config,
                profile_secrets, branch_options, commit_options, status, validation_error,
                validation_invocation_id, created_at, updated_at, validated_at
            "#
        )
        .bind(draft_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, format!("environment draft '{draft_id}' was not found"))))?;
        Ok(environment_draft_record_from_row(&row))
    }

    pub async fn mark_environment_draft_validating(
        &self,
        draft_id: Uuid,
    ) -> AppResult<EnvironmentDraftRecord> {
        let row = sqlx::query(
            r#"
            UPDATE environment_onboarding_drafts
            SET status = 'validating',
                validation_error = NULL,
                validation_invocation_id = NULL,
                validated_at = NULL,
                updated_at = NOW()
            WHERE id = $1
            RETURNING id, project_id, slug, git_branch, git_commit_sha, use_latest_commit,
                auto_deploy, immutable, adapter_type, schema_name, threads, profile_config,
                profile_secrets, branch_options, commit_options, status, validation_error,
                validation_invocation_id, created_at, updated_at, validated_at
            "#
        )
        .bind(draft_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, format!("environment draft '{draft_id}' was not found"))))?;
        Ok(environment_draft_record_from_row(&row))
    }

    pub async fn attach_environment_draft_invocation(
        &self,
        draft_id: Uuid,
        invocation_id: Uuid,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            UPDATE environment_onboarding_drafts
            SET validation_invocation_id = $2,
                updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(draft_id)
        .bind(invocation_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn fail_environment_draft(
        &self,
        draft_id: Uuid,
        error: &str,
    ) -> AppResult<EnvironmentDraftRecord> {
        let row = sqlx::query(
            r#"
            UPDATE environment_onboarding_drafts
            SET status = 'failed',
                validation_error = $2,
                validated_at = NULL,
                updated_at = NOW()
            WHERE id = $1
            RETURNING id, project_id, slug, git_branch, git_commit_sha, use_latest_commit,
                auto_deploy, immutable, adapter_type, schema_name, threads, profile_config,
                profile_secrets, branch_options, commit_options, status, validation_error,
                validation_invocation_id, created_at, updated_at, validated_at
            "#
        )
        .bind(draft_id)
        .bind(error)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("environment draft '{draft_id}' was not found"),
            ))
        })?;
        Ok(environment_draft_record_from_row(&row))
    }

    pub async fn confirm_environment_draft(
        &self,
        draft_id: Uuid,
    ) -> AppResult<EnvironmentRecord> {
        let draft = self.get_environment_draft(draft_id).await?;
        if draft.status != DraftStatus::Validated {
            return Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "environment draft must be validated before confirmation",
            )));
        }
        let project = self.get_project_by_id(draft.project_id).await?;
        let project_ref = project.project_id.clone();
        let slug = draft.slug.clone();
        let profile_name = project.project_name.clone();
        self.create_environment(CreateEnvironmentInput {
            project: project_ref.clone(),
            slug: slug.clone(),
            profile_name,
            target_name: slug,
            baseline_slug: None,
            git_branch: draft.git_branch.clone(),
            git_commit_sha: draft.git_commit_sha.clone(),
            use_latest_commit: draft.use_latest_commit,
            auto_deploy: draft.auto_deploy,
            immutable: draft.immutable,
            pr_number: None,
            status: "active".to_string(),
            adapter_type: draft.adapter_type.clone().ok_or_else(|| AppError::InvalidProfileConfig("adapter type is required".to_string()))?,
            worker_queue: None,
            schema_name: draft.schema_name.clone(),
            threads: draft.threads,
            profile_config: draft.profile_config.clone(),
            profile_secrets: crate::profile::decrypt_json(&draft.profile_secrets)?,
        }).await
    }

    pub async fn mark_project_draft_validating(
        &self,
        draft_id: Uuid,
    ) -> AppResult<ProjectDraftRecord> {
        let row = sqlx::query(
            r#"
            UPDATE project_onboarding_drafts
            SET status = 'validating',
                validation_error = NULL,
                project_name = NULL,
                default_branch = NULL,
                validation_invocation_id = NULL,
                validated_at = NULL,
                updated_at = NOW()
            WHERE id = $1
            RETURNING id, git_repo_url, project_root, status, validation_error, project_name,
                default_branch, validation_invocation_id, created_at, updated_at, validated_at
            "#,
        )
        .bind(draft_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::Io(std::io::Error::new(
                std::io::ErrorKind::NotFound,
                format!("project draft '{draft_id}' was not found"),
            ))
        })?;
        Ok(project_draft_record_from_row(&row))
    }

    pub async fn attach_project_draft_invocation(
        &self,
        draft_id: Uuid,
        invocation_id: Uuid,
    ) -> AppResult<()> {
        sqlx::query(
            r#"
            UPDATE project_onboarding_drafts
            SET validation_invocation_id = $2,
                updated_at = NOW()
            WHERE id = $1
            "#,
        )
        .bind(draft_id)
        .bind(invocation_id)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn confirm_project_draft(&self, draft_id: Uuid) -> AppResult<ProjectRecord> {
        let draft = self.get_project_draft(draft_id).await?;
        if draft.status != DraftStatus::Validated {
            return Err(AppError::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "project draft must be validated before confirmation",
            )));
        }
        let project_name = draft.project_name.clone().ok_or_else(|| {
            AppError::Internal("validated project draft missing project_name".to_string())
        })?;
        let default_branch = draft.default_branch.clone().ok_or_else(|| {
            AppError::Internal("validated project draft missing default_branch".to_string())
        })?;
        let project_id = remote_project_id(&draft.git_repo_url, &draft.project_root, &project_name);
        self.upsert_project(CreateProjectInput {
            project_id,
            project_name,
            mode: "remote".to_string(),
            git_repo_url: Some(draft.git_repo_url),
            default_branch: Some(default_branch),
            project_root: Some(draft.project_root),
        })
        .await
    }

    pub async fn get_project_by_project_id(&self, project_id: &str) -> AppResult<ProjectRecord> {
        let row = sqlx::query(
            "SELECT id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata FROM projects WHERE project_id = $1",
        )
        .bind(project_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::ProjectIdNotFound(project_id.to_string()))?;
        Ok(project_record_from_row(&row))
    }

    pub async fn get_project_by_id(&self, id: i64) -> AppResult<ProjectRecord> {
        let row = sqlx::query(
            "SELECT id, project_id, project_name, mode, git_repo_url, default_branch, project_root, metadata FROM projects WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| AppError::ProjectIdNotFound(id.to_string()))?;
        Ok(project_record_from_row(&row))
    }

    pub async fn delete_project(&self, project_id: &str) -> AppResult<()> {
        let result = sqlx::query("DELETE FROM projects WHERE project_id = $1")
            .bind(project_id)
            .execute(&self.pool)
            .await;

        match result {
            Ok(done) => {
                if done.rows_affected() == 0 {
                    return Err(AppError::ProjectIdNotFound(project_id.to_string()));
                }
                Ok(())
            }
            Err(sqlx::Error::Database(db_err)) if db_err.code().as_deref() == Some("23503") => {
                Err(AppError::ProjectDeleteBlocked(project_id.to_string()))
            }
            Err(err) => Err(AppError::Sqlx(err)),
        }
    }

}
