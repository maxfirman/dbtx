import { test, expect } from './fixtures';

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

    // Navigate to the catalog list page (which loads base.html with all scripts)
    await page.goto(`${app.baseUrl}/ui/catalog`);
    await expect(page.getByRole('heading', { name: 'Catalog' })).toBeVisible();

    // Inject a lineage-root div and test data, then load the lineage script
    await page.evaluate((baseUrl) => {
      // Create the mount point
      const main = document.querySelector('main');
      if (!main) return;
      main.innerHTML = '<div id="lineage-root" style="width:800px;height:500px;"></div>';

      // Set the data
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
        currentNodeId: 'model.pkg.b',
        baseUrl: '/ui/catalog/test/dev',
      };

      // Load the lineage script
      const s = document.createElement('script');
      s.src = `${baseUrl}/ui/assets/lineage.js?t=${Date.now()}`;
      document.body.appendChild(s);
    }, app.baseUrl);

    // Wait for the svelte-flow container to appear
    const lineageRoot = page.locator('#lineage-root');

    // Wait for script to load and mount
    await page.waitForTimeout(2000);
    await page.evaluate(() => {
      const mountFn = (window as any).__dbtxMountLineage;
      if (mountFn) mountFn();
    });

    const svelteFlow = lineageRoot.locator('.svelte-flow');
    await expect(svelteFlow).toBeVisible({ timeout: 15_000 });

    // Should have 3 nodes rendered
    const nodes = lineageRoot.locator('.svelte-flow__node');
    await expect(nodes).toHaveCount(3, { timeout: 10_000 });

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
});
