//! Environment, environment draft, and environment version CRUD operations.

use super::*;

impl Db {
    pub async fn create_environment(
        &self,
        input: CreateEnvironmentInput,
    ) -> AppResult<EnvironmentRecord> {
        validate_environment_status(&input.status)?;
        let project = self.get_project_by_project_id(&input.project).await?;
        validate_environment_git_metadata(&project, &input.slug, input.git_commit_sha.as_deref())?;
        validate_environment_profile(
            &input.adapter_type,
            input.schema_name.as_deref().unwrap_or(""),
            input.threads,
            &input.profile_config,
            &input.profile_secrets,
            false,
        )?;
        let worker_queue = input
            .worker_queue
            .clone()
            .unwrap_or_else(|| "generic".to_string());
        let baseline = match input.baseline_slug.as_deref() {
            Some(baseline_slug) => Some(
                self.get_environment_by_project_id(project.id, &project.project_id, baseline_slug)
                    .await?,
            ),
            None => None,
        };
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query(
            r#"
            INSERT INTO environments (
                project_id, slug, profile_name, target_name, baseline_environment_id, git_branch, git_commit_sha,
                use_latest_commit, auto_deploy, immutable, pr_number, status, adapter_type,
                worker_queue, schema_name, threads, profile_config, profile_secrets
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)
            RETURNING id
            "#,
        )
        .bind(project.id)
        .bind(&input.slug)
        .bind(&input.profile_name)
        .bind(&input.target_name)
        .bind(baseline.as_ref().map(|env| env.id))
        .bind(input.git_branch.as_deref())
        .bind(input.git_commit_sha.as_deref())
        .bind(input.use_latest_commit)
        .bind(input.auto_deploy)
        .bind(input.immutable)
        .bind(input.pr_number)
        .bind(&input.status)
        .bind(&input.adapter_type)
        .bind(&worker_queue)
        .bind(input.schema_name.as_deref())
        .bind(input.threads)
        .bind(sqlx::types::Json(&input.profile_config))
        .bind(sqlx::types::Json(&input.profile_secrets))
        .fetch_one(&mut *tx)
        .await
        .map_err(|err| match &err {
            sqlx::Error::Database(db_err) if db_err.code().as_deref() == Some("23505") => {
                AppError::EnvironmentAlreadyExists(project.project_id.clone(), input.slug.clone())
            }
            _ => AppError::Sqlx(err),
        })?;
        let environment_id: i64 = row.get("id");
        let environment = self
            .get_environment_by_id_in_tx(&mut tx, environment_id)
            .await?;
        if let Some(source) = baseline.as_ref() {
            self.seed_environment_from_tx(&mut tx, &project, &environment, source, "clone")
                .await?;
        }
        tx.commit().await?;
        self.record_environment_version(&environment, "created")
            .await?;
        Ok(environment)
    }

    pub async fn update_environment(
        &self,
        input: UpdateEnvironmentInput,
    ) -> AppResult<EnvironmentRecord> {
        let project = self.get_project_by_project_id(&input.project).await?;
        let existing = self
            .get_environment_by_project_id(project.id, &project.project_id, &input.slug)
            .await?;

        let baseline_environment_id = match input.baseline_slug.as_deref() {
            Some(baseline_slug) => Some(
                self.get_environment_by_project_id(project.id, &project.project_id, baseline_slug)
                    .await?
                    .id,
            ),
            None => existing.baseline_environment_id,
        };
        let git_branch = input.git_branch.or(existing.git_branch.clone());
        let git_commit_sha = input.git_commit_sha.or(existing.git_commit_sha.clone());
        let use_latest_commit = input
            .use_latest_commit
            .unwrap_or(existing.use_latest_commit);
        let auto_deploy = input.auto_deploy.unwrap_or(existing.auto_deploy);
        let immutable = input.immutable.unwrap_or(existing.immutable);
        validate_environment_git_metadata(&project, &existing.slug, git_commit_sha.as_deref())?;
        let adapter_type = input
            .adapter_type
            .as_deref()
            .unwrap_or(&existing.adapter_type)
            .to_string();
        let worker_queue = input
            .worker_queue
            .as_deref()
            .unwrap_or(&existing.worker_queue)
            .to_string();
        let profile_name = input
            .profile_name
            .as_deref()
            .unwrap_or(&existing.profile_name)
            .to_string();
        let target_name = input
            .target_name
            .as_deref()
            .unwrap_or(&existing.target_name)
            .to_string();
        let schema_name = input
            .schema_name
            .as_deref()
            .unwrap_or(&existing.schema_name)
            .to_string();
        let threads = input.threads.or(existing.threads);
        validate_environment_profile(
            &adapter_type,
            &schema_name,
            threads,
            input
                .profile_config
                .as_ref()
                .unwrap_or(&existing.profile_config),
            input
                .profile_secrets
                .as_ref()
                .unwrap_or(&existing.profile_secrets),
            true,
        )?;
        let status = input.status.unwrap_or_else(|| existing.status.to_string());
        validate_environment_status(&status)?;

        sqlx::query(
            r#"
            UPDATE environments
            SET baseline_environment_id = $3,
                git_branch = $4,
                git_commit_sha = $5,
                use_latest_commit = $6,
                auto_deploy = $7,
                immutable = $8,
                pr_number = $9,
                status = $10,
                adapter_type = $11,
                worker_queue = $12,
                profile_name = $13,
                target_name = $14,
                schema_name = $15,
                threads = $16,
                profile_config = $17,
                profile_secrets = $18
            WHERE id = $1 AND project_id = $2
            "#,
        )
        .bind(existing.id)
        .bind(project.id)
        .bind(baseline_environment_id)
        .bind(git_branch.as_deref())
        .bind(git_commit_sha.as_deref())
        .bind(use_latest_commit)
        .bind(auto_deploy)
        .bind(immutable)
        .bind(input.pr_number.or(existing.pr_number))
        .bind(&status)
        .bind(&adapter_type)
        .bind(&worker_queue)
        .bind(&profile_name)
        .bind(&target_name)
        .bind(&schema_name)
        .bind(threads)
        .bind(sqlx::types::Json(
            input
                .profile_config
                .as_ref()
                .unwrap_or(&existing.profile_config),
        ))
        .bind(sqlx::types::Json(
            input
                .profile_secrets
                .as_ref()
                .unwrap_or(&existing.profile_secrets),
        ))
        .execute(&self.pool)
        .await?;

        let environment = self.get_environment_by_id(existing.id).await?;
        self.record_environment_version(&environment, "updated")
            .await?;
        Ok(environment)
    }

    pub async fn release_environment(
        &self,
        input: EnvironmentReleaseInput,
    ) -> AppResult<EnvironmentRecord> {
        let project = self.get_project_by_project_id(&input.project).await?;
        let existing = self
            .get_environment_by_project_id(project.id, &project.project_id, &input.slug)
            .await?;

        if existing.immutable {
            return Err(AppError::ImmutableEnvironment(existing.slug.clone()));
        }

        validate_environment_git_metadata(&project, &existing.slug, Some(&input.git_commit_sha))?;

        if existing.git_commit_sha.as_deref() == Some(input.git_commit_sha.as_str()) {
            return Ok(existing);
        }

        sqlx::query(
            r#"
            UPDATE environments
            SET git_branch = $3,
                git_commit_sha = $4
            WHERE id = $1 AND project_id = $2
            "#,
        )
        .bind(existing.id)
        .bind(project.id)
        .bind(input.git_branch.as_deref())
        .bind(&input.git_commit_sha)
        .execute(&self.pool)
        .await?;

        let environment = self.get_environment_by_id(existing.id).await?;
        self.record_environment_version(&environment, "released")
            .await?;
        Ok(environment)
    }

    pub async fn list_environments(&self, project: &str) -> AppResult<Vec<EnvironmentRecord>> {
        let project = self.get_project_by_project_id(project).await?;
        let query = environment_query("WHERE e.project_id = $1 ORDER BY e.slug");
        let rows = sqlx::query(&query)
            .bind(project.id)
            .fetch_all(&self.pool)
            .await?;
        rows.iter().map(environment_record_from_row).collect()
    }

    pub(crate) async fn list_auto_deploy_remote_environments(
        &self,
    ) -> AppResult<Vec<EnvironmentRecord>> {
        let query = environment_query(
            "WHERE p.mode = 'remote' AND e.auto_deploy = TRUE AND e.status = 'active' ORDER BY p.project_id ASC, e.slug ASC",
        );
        let rows = sqlx::query(&query).fetch_all(&self.pool).await?;
        rows.iter().map(environment_record_from_row).collect()
    }

    pub async fn list_environment_versions(
        &self,
        project: &str,
        slug: &str,
    ) -> AppResult<Vec<EnvironmentVersionRecord>> {
        let project = self.get_project_by_project_id(project).await?;
        let environment = self
            .get_environment_by_project_id(project.id, &project.project_id, slug)
            .await?;
        let rows = sqlx::query(
            r#"
            SELECT id, environment_id, project_id, recorded_at, reason, git_branch, git_commit_sha,
                   use_latest_commit, auto_deploy, immutable, baseline_environment_id, metadata
            FROM environment_versions
            WHERE environment_id = $1
            ORDER BY id DESC
            "#,
        )
        .bind(environment.id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows
            .iter()
            .map(environment_version_record_from_row)
            .collect())
    }

    pub async fn rollback_environment_to_version(
        &self,
        project: &str,
        slug: &str,
        version_id: i64,
    ) -> AppResult<EnvironmentRecord> {
        let project = self.get_project_by_project_id(project).await?;
        let existing = self
            .get_environment_by_project_id(project.id, &project.project_id, slug)
            .await?;
        let version = sqlx::query(
            r#"
            SELECT id, environment_id, project_id, recorded_at, reason, git_branch, git_commit_sha,
                   use_latest_commit, auto_deploy, immutable, baseline_environment_id, metadata
            FROM environment_versions
            WHERE id = $1 AND environment_id = $2
            "#,
        )
        .bind(version_id)
        .bind(existing.id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| {
            AppError::EnvironmentVersionNotFound(
                project.project_id.clone(),
                slug.to_string(),
                version_id,
            )
        })?;
        let version = environment_version_record_from_row(&version);
        validate_environment_git_metadata(
            &project,
            &existing.slug,
            version.git_commit_sha.as_deref(),
        )?;

        sqlx::query(
            r#"
            UPDATE environments
            SET baseline_environment_id = $3,
                git_branch = $4,
                git_commit_sha = $5
            WHERE id = $1 AND project_id = $2
            "#,
        )
        .bind(existing.id)
        .bind(project.id)
        .bind(version.baseline_environment_id)
        .bind(version.git_branch.as_deref())
        .bind(version.git_commit_sha.as_deref())
        .execute(&self.pool)
        .await?;

        let environment = self.get_environment_by_id(existing.id).await?;
        self.record_environment_version(&environment, "rolled_back")
            .await?;
        Ok(environment)
    }

    pub async fn get_environment(
        &self,
        project: &str,
        environment_slug: &str,
    ) -> AppResult<EnvironmentRecord> {
        let project = self.get_project_by_project_id(project).await?;
        self.get_environment_by_project_id(project.id, &project.project_id, environment_slug)
            .await
    }

    pub(crate) async fn list_active_environment_resources(
        &self,
        project: &str,
        environment_slug: &str,
        resource_type: Option<&str>,
    ) -> AppResult<Vec<EnvironmentActiveResourceRecord>> {
        let environment = self.get_environment(project, environment_slug).await?;
        let rows = sqlx::query(
            r#"
            SELECT
                invocation_id,
                run_id,
                unique_id,
                resource_type,
                selected_at,
                node_started_at,
                CASE
                    WHEN node_started_at IS NULL THEN 'selected'
                    ELSE 'running'
                END AS phase
            FROM invocation_selected_resources
            WHERE project_id = $1
              AND environment_id = $2
              AND finished_at IS NULL
              AND ($3::TEXT IS NULL OR resource_type = $3)
            ORDER BY COALESCE(node_started_at, selected_at) ASC, unique_id ASC
            "#,
        )
        .bind(environment.project_id)
        .bind(environment.id)
        .bind(resource_type)
        .fetch_all(&self.pool)
        .await?;

        Ok(rows
            .iter()
            .map(active_environment_resource_from_row)
            .collect())
    }
}
