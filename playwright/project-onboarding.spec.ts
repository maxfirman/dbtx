import { test, expect } from './fixtures';

async function createProject(page, repoUrl) {
  await page.getByRole('button', { name: 'Create Project' }).click();
  await expect(page.getByRole('heading', { name: 'Create Remote Project' })).toBeVisible();

  await page.getByLabel('Git URL').fill(repoUrl);
  await expect(page.getByLabel('Relative Project Path')).toHaveValue('.');
  const modalForm = page.locator('#modal-root form');
  const actionButton = modalForm.getByRole('button', { name: 'Create Project' });
  await actionButton.click();

  await expect(modalForm.getByRole('button', { name: 'Validating...' })).toBeVisible();
  await expect(page.getByText('Validation Steps')).toBeVisible();
  await expect(page.getByText('Validation Progress')).toBeVisible();
  await expect(page.getByText('Validating remote project…')).toBeVisible();
  await expect(page.getByText('Validation succeeded')).toBeVisible({ timeout: 30_000 });
  await expect(page.getByLabel('Git URL')).toBeDisabled();
  await expect(page.getByLabel('Relative Project Path')).toBeDisabled();
  await modalForm.getByRole('button', { name: 'Confirm Create' }).click();
}

async function createEnvironment(page, slug = 'dev') {
  const projectCard = page.locator('section').filter({ hasText: 'file://' });
  await projectCard.getByRole('button', { name: 'Create Environment' }).click();

  await expect(page.getByRole('heading', { name: 'Create Environment' })).toBeVisible();
  const modalForm = page.locator('#modal-root form');
  await expect(page.getByText('Loading branch and commit metadata…')).toBeVisible();
  await expect(page.getByText('Loading branch and commit metadata…')).toHaveCount(0, { timeout: 30_000 });

  await modalForm.locator('input[name="slug"]').fill(slug);
  await modalForm.locator('input[name="adapter_type"]').fill('duckdb');
  await modalForm.locator('input[name="schema_name"]').fill('main');
  await modalForm.locator('input[name="threads"]').fill('4');
  await modalForm.getByRole('button', { name: 'Add Field' }).click();
  await modalForm.locator('input[placeholder="database"]').fill('path');
  await modalForm.locator('input[placeholder="warehouse"]').fill('warehouse.duckdb');

  await modalForm.getByRole('button', { name: 'Create Environment' }).click();
  await expect(modalForm.getByRole('button', { name: 'Validating...' })).toBeVisible();
  await expect(page.getByText('Validation succeeded')).toBeVisible({ timeout: 30_000 });
  const confirmButton = page.locator('#modal-root form').getByRole('button', { name: 'Confirm Create' });
  await confirmButton.scrollIntoViewIfNeeded();
  await confirmButton.click();
  await expect(page).toHaveURL(new RegExp(`/ui/projects/.+/environments/${slug}$`));
}

test.describe('project onboarding', () => {
  test.beforeEach(async ({ page, app }) => {
    await page.goto(`${app.baseUrl}/ui/projects`);
  });

  test('shows validation progress and confirms a remote project', async ({ page, app }) => {
    await createProject(page, app.repoUrl);
    await expect(page).toHaveURL(new RegExp(`${app.baseUrl.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/ui/projects$`));
    await expect(page.locator('section').filter({ hasText: app.repoUrl })).toHaveCount(1);
  });

  test('shows validation failure for an invalid project path and lets the user retry', async ({ page, app }) => {
    await page.getByRole('button', { name: 'Create Project' }).click();
    await page.getByLabel('Git URL').fill(app.repoUrl);
    await page.getByLabel('Relative Project Path').fill('definitely/not/a/dbt/project');
    const modalForm = page.locator('#modal-root form');
    const createButton = modalForm.getByRole('button', { name: 'Create Project' });
    await createButton.click();

    await expect(page.getByText('Validation failed')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByText('Validation Steps')).toBeVisible();
    await expect(page.locator('#project-draft-panel').getByText('failed', { exact: true })).toHaveCount(1);
    await expect(page.getByText('Fix the repository or project path above, then click Create Project again to retry validation.')).toBeVisible();
    await expect(createButton).toBeEnabled();
    await expect(page.getByLabel('Git URL')).toBeEnabled();
    await expect(page.getByLabel('Relative Project Path')).toBeEnabled();

    await page.getByLabel('Relative Project Path').fill('.');
    await createButton.click();
    await expect(page.getByText('Validation succeeded')).toBeVisible({ timeout: 30_000 });
    await expect(modalForm.getByRole('button', { name: 'Confirm Create' })).toBeVisible();
    await modalForm.getByRole('button', { name: 'Confirm Create' }).click();
    await expect(page).toHaveURL(new RegExp(`${app.baseUrl.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/ui/projects$`));
    await expect(page.locator('section').filter({ hasText: app.repoUrl })).toHaveCount(1);
  });

  test('allows repeated retry attempts from the failed validation state', async ({ page, app }) => {
    await page.getByRole('button', { name: 'Create Project' }).click();
    await page.getByLabel('Git URL').fill(app.repoUrl);
    await page.getByLabel('Relative Project Path').fill('definitely/not/a/dbt/project');
    const modalForm = page.locator('#modal-root form');
    const createButton = modalForm.getByRole('button', { name: 'Create Project' });

    await createButton.click();
    await expect(page.getByText('Validation failed')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByText('Validation Steps')).toBeVisible();
    await expect(createButton).toBeEnabled();

    await createButton.click();
    await expect(page.getByText('Validation failed')).toBeVisible({ timeout: 30_000 });
    await expect(createButton).toBeEnabled();

    await page.getByLabel('Relative Project Path').fill('.');
    await createButton.click();
    await expect(page.getByText('Validation succeeded')).toBeVisible({ timeout: 30_000 });
    await modalForm.getByRole('button', { name: 'Confirm Create' }).click();
    await expect(page).toHaveURL(new RegExp(`${app.baseUrl.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/ui/projects$`));
  });

  test('deletes a project with typed confirmation', async ({ page, app }) => {
    await createProject(page, app.repoUrl);

    await page
      .locator('section')
      .filter({ hasText: app.repoUrl })
      .getByRole('button', { name: 'Delete Project' })
      .click();

    await expect(page.getByRole('heading', { name: 'Delete Project' })).toBeVisible();
    const deleteForm = page.locator('#modal-root form');
    const deleteButton = deleteForm.getByRole('button', { name: 'Delete Project' });
    const projectId = await deleteForm.getByRole('textbox').getAttribute('placeholder');
    await expect(deleteButton).toBeDisabled();

    await deleteForm.getByRole('textbox').fill(projectId ?? '');
    await expect(deleteButton).toBeEnabled();
    await deleteButton.click();

    await expect(page).toHaveURL(new RegExp(`${app.baseUrl.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/ui/projects$`));
    await expect(page.getByText(app.repoUrl)).toHaveCount(0);
  });

  test('creates a remote environment from the project card', async ({ page, app }) => {
    await createProject(page, app.repoUrl);
    await createEnvironment(page, 'dev-card');

    await expect(page).toHaveURL(
      new RegExp(`${app.baseUrl.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/ui/projects/.+/environments/dev-card$`),
    );
    await expect(page.getByRole('heading', { name: /jaffle_shop_project \/ dev-card/ })).toBeVisible();
    await expect(page.getByText('duckdb')).toBeVisible();
  });

  test('supports switching to manual commit selection when creating an environment', async ({ page, app }) => {
    await createProject(page, app.repoUrl);

    const projectCard = page.locator('section').filter({ hasText: app.repoUrl });
    await projectCard.getByRole('button', { name: 'Create Environment' }).click();

    await expect(page.getByRole('heading', { name: 'Create Environment' })).toBeVisible();
    const modalForm = page.locator('#modal-root form');
    await expect(page.getByText('Loading branch and commit metadata…')).toHaveCount(0, { timeout: 30_000 });

    const branchSelect = modalForm.locator('select[name="git_branch"]').last();
    await expect(branchSelect).toHaveValue('main');
    const refreshedForm = page.locator('#modal-root form');
    await expect(refreshedForm.locator('input[type="text"][disabled]').first()).toHaveValue(app.mainHeadSha);

    const useLatestToggle = refreshedForm.locator('input[name="use_latest_commit"]');
    await useLatestToggle.uncheck();

    const commitSelect = refreshedForm.locator('select[name="git_commit_sha"]');
    await expect(commitSelect).toBeEnabled();
    await expect(commitSelect).toHaveValue(app.mainHeadSha);

    await refreshedForm.locator('input[name="slug"]').fill('preview-manual');
    await refreshedForm.locator('input[name="adapter_type"]').fill('duckdb');
    await refreshedForm.locator('input[name="schema_name"]').fill('main');
    await refreshedForm.locator('input[name="threads"]').fill('4');
    await refreshedForm.getByRole('button', { name: 'Add Field' }).click();
    await refreshedForm.locator('input[placeholder="database"]').fill('path');
    await refreshedForm.locator('input[placeholder="warehouse"]').fill('warehouse.duckdb');

    const createButton = refreshedForm.getByRole('button', { name: 'Create Environment' });
    await createButton.click();
    await expect(page.getByText('Validation succeeded')).toBeVisible({ timeout: 30_000 });
    await expect(page.locator('#modal-root form').locator('input[name="slug"]')).toHaveCount(0);
    await expect(page.locator('#modal-root form').locator('select[name="git_branch"]')).toHaveCount(0);
    await expect(page.locator('#modal-root form').getByRole('button', { name: 'Confirm Create' })).toBeVisible();
    await page.locator('#modal-root form').getByRole('button', { name: 'Confirm Create' }).click();

    await expect(page).toHaveURL(
      new RegExp(`${app.baseUrl.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')}/ui/projects/.+/environments/preview-manual$`),
    );
    await expect(page.locator('input[name="git_branch"]').last()).toHaveValue('main');
    await expect(page.locator('input[name="git_commit_sha"]').last()).toHaveValue(app.mainHeadSha);
  });

  test('shows environment validation failure inline', async ({ page, app }) => {
    await createProject(page, app.repoUrl);

    const projectCard = page.locator('section').filter({ hasText: app.repoUrl });
    await projectCard.getByRole('button', { name: 'Create Environment' }).click();

    await expect(page.getByRole('heading', { name: 'Create Environment' })).toBeVisible();
    const modalForm = page.locator('#modal-root form');
    await expect(page.getByText('Loading branch and commit metadata…')).toHaveCount(0, { timeout: 30_000 });

    await modalForm.locator('input[name="slug"]').fill('invalid-env');
    await modalForm.locator('input[name="adapter_type"]').fill('not-a-real-adapter');
    await modalForm.locator('input[name="schema_name"]').fill('main');
    await modalForm.locator('input[name="threads"]').fill('4');
    await modalForm.getByRole('button', { name: 'Add Field' }).click();
    await modalForm.locator('input[placeholder="database"]').fill('path');
    await modalForm.locator('input[placeholder="warehouse"]').fill('warehouse.duckdb');

    const createButton = modalForm.getByRole('button', { name: 'Create Environment' });
    await createButton.click();

    await expect(page.getByText('Validation failed')).toBeVisible({ timeout: 30_000 });
    await expect(page.getByText('Update the environment settings above, then retry.')).toBeVisible();
    await expect(createButton).toBeEnabled();
    await expect(modalForm.locator('input[name="slug"]')).toBeEnabled();
    await expect(modalForm.locator('input[name="adapter_type"]')).toBeEnabled();
  });

  test('releases and rolls back an environment through the UI', async ({ page, app }) => {
    await createProject(page, app.repoUrl);
    await createEnvironment(page, 'release-ui');

    const branchInput = page.locator('input[name="git_branch"]').last();
    const commitInput = page.locator('input[name="git_commit_sha"]').last();
    await expect(branchInput).toHaveValue('main');
    await expect(commitInput).toHaveValue(app.mainHeadSha);

    await commitInput.fill(app.mainInitialSha);
    await page.getByRole('button', { name: 'Release Commit' }).click();
    await expect(page.locator('input[name="git_commit_sha"]').last()).toHaveValue(app.mainInitialSha);

    const releasedRow = page.locator('tbody tr').filter({ hasText: app.mainInitialSha }).first();
    await expect(releasedRow).toBeVisible();

    const rollbackButton = page
      .locator('tbody tr')
      .filter({ hasText: app.mainHeadSha })
      .first()
      .getByRole('button', { name: 'Rollback' });
    await rollbackButton.scrollIntoViewIfNeeded();
    await rollbackButton.click();

    await expect(page.locator('input[name="git_commit_sha"]').last()).toHaveValue(app.mainHeadSha);
    await expect(page.locator('tbody tr').filter({ hasText: app.mainHeadSha }).first()).toBeVisible();
  });
});
