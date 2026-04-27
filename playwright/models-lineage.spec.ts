import { test, expect } from './fixtures';

/** Helper: inject lineage data, load the script, and wait for mount. */
async function mountLineage(
  page: import('@playwright/test').Page,
  baseUrl: string,
  overrides?: { depth?: number; direction?: string; currentNodeId?: string },
) {
  await page.goto(`${baseUrl}/ui/catalog`);
  await expect(page.getByRole('heading', { name: 'Catalog' })).toBeVisible();

  const depth = overrides?.depth ?? 3;
  const direction = overrides?.direction ?? 'ancestors';
  const currentNodeId = overrides?.currentNodeId ?? 'model.pkg.b';

  await page.evaluate(
    (args) => {
      const main = document.querySelector('main');
      if (!main) return;
      main.innerHTML = '<div id="lineage-root" style="width:800px;height:500px;"></div>';
      (window as any).__LINEAGE_DATA__ = {
        nodes: [
          { id: 'model.pkg.a', data: { label: 'model_a', name: 'model_a', resource_type: 'model', status: 'success', materialized: 'table' } },
          { id: 'model.pkg.b', data: { label: 'model_b', name: 'model_b', resource_type: 'model', status: 'success', materialized: 'view' } },
          { id: 'model.pkg.c', data: { label: 'model_c', name: 'model_c', resource_type: 'model', status: null, materialized: 'table' } },
        ],
        edges: [
          { id: 'e1', source: 'model.pkg.a', target: 'model.pkg.b' },
          { id: 'e2', source: 'model.pkg.b', target: 'model.pkg.c' },
        ],
        currentNodeId: args.currentNodeId,
        baseUrl: '/ui/catalog/test/dev',
        depth: args.depth,
        direction: args.direction,
      };
      const s = document.createElement('script');
      s.src = `${args.baseUrl}/ui/assets/lineage.js?t=${Date.now()}`;
      document.body.appendChild(s);
    },
    { baseUrl, depth, direction, currentNodeId },
  );

  await page.waitForTimeout(2000);
  await page.evaluate(() => {
    const mountFn = (window as any).__dbtxMountLineage;
    if (mountFn) mountFn();
  });

  const lineageRoot = page.locator('#lineage-root');
  await expect(lineageRoot.locator('.svelte-flow')).toBeVisible({ timeout: 15_000 });
  await expect(lineageRoot.locator('.svelte-flow__node')).toHaveCount(3, { timeout: 10_000 });
  return lineageRoot;
}

test.describe('catalog UI', () => {
  test('catalog list page loads and filters render', async ({ page, app }) => {
    await page.goto(`${app.baseUrl}/ui/catalog`);
    await expect(page.getByRole('heading', { name: 'Catalog' })).toBeVisible();
    await expect(page.locator('select[name="project_id"]')).toBeVisible();
    await expect(page.locator('select[name="environment_slug"]')).toBeVisible();
    // Resource type multi-checkbox dropdown should be present
    await expect(page.locator('input[name="resource_type"]').first()).toBeAttached();
  });

  test('lineage JS asset is served and contains mount function', async ({ page, app }) => {
    const response = await page.request.get(`${app.baseUrl}/ui/assets/lineage.js`);
    expect(response.status()).toBe(200);
    expect(response.headers()['content-type']).toContain('javascript');
    const body = await response.text();
    expect(body.length).toBeGreaterThan(1000);
    expect(body).toContain('__dbtxMountLineage');
  });

  test('lineage CSS asset is served', async ({ page, app }) => {
    const response = await page.request.get(`${app.baseUrl}/ui/assets/lineage.css`);
    expect(response.status()).toBe(200);
    expect(response.headers()['content-type']).toContain('css');
    const body = await response.text();
    expect(body.length).toBeGreaterThan(100);
  });

  test('lineage component mounts and renders nodes', async ({ page, app }) => {
    const consoleErrors: string[] = [];
    page.on('console', (msg) => {
      if (msg.type() === 'error') consoleErrors.push(msg.text());
    });

    const lineageRoot = await mountLineage(page, app.baseUrl);

    // Should have edges rendered
    const edges = lineageRoot.locator('.svelte-flow__edge');
    await expect(edges).toHaveCount(2, { timeout: 10_000 });

    // All nodes should show their labels
    await expect(lineageRoot.locator('.svelte-flow__node').filter({ hasText: 'model_a' })).toBeVisible();
    await expect(lineageRoot.locator('.svelte-flow__node').filter({ hasText: 'model_b' })).toBeVisible();
    await expect(lineageRoot.locator('.svelte-flow__node').filter({ hasText: 'model_c' })).toBeVisible();

    // No critical JS errors
    const criticalErrors = consoleErrors.filter(
      (e) => !e.includes('favicon') && !e.includes('404') && !e.includes('ERR_CONNECTION') && !e.includes('net::'),
    );
    expect(criticalErrors).toEqual([]);
  });

  test('clicking a non-current node navigates with depth and direction', async ({ page, app }) => {
    await mountLineage(page, app.baseUrl, { depth: 4, direction: 'descendants' });

    // Intercept the navigation request triggered by window.location.href assignment
    let navigatedUrl: string | null = null;
    await page.route('**/ui/catalog/test/dev/**', (route) => {
      navigatedUrl = route.request().url();
      route.abort();
    });

    // Click model_a (non-current; current is model_b)
    const nodeA = page.locator('.svelte-flow__node').filter({ hasText: 'model_a' });
    await nodeA.click();

    // Wait briefly for the navigation request to fire
    await page.waitForTimeout(1000);

    expect(navigatedUrl).toBeTruthy();
    expect(navigatedUrl).toContain('/ui/catalog/test/dev/');
    expect(navigatedUrl).toContain('model.pkg.a');
    expect(navigatedUrl).toContain('tab=lineage');
    expect(navigatedUrl).toContain('depth=4');
    expect(navigatedUrl).toContain('direction=descendants');
  });

  test('clicking the current node does not navigate', async ({ page, app }) => {
    await mountLineage(page, app.baseUrl);

    let navigatedUrl: string | null = null;
    await page.route('**/ui/catalog/test/dev/**', (route) => {
      navigatedUrl = route.request().url();
      route.abort();
    });

    // Click model_b (the current node)
    const nodeB = page.locator('.svelte-flow__node').filter({ hasText: 'model_b' });
    await nodeB.click();

    await page.waitForTimeout(1000);
    expect(navigatedUrl).toBeNull();
  });
});
