import { mount, unmount } from 'svelte';
import App from './App.svelte';
import '@xyflow/svelte/dist/style.css';

let currentApp: Record<string, unknown> | null = null;

(window as any).__dbtxMountLineage = function () {
  const target = document.getElementById('lineage-root');
  if (!target || !(window as any).__LINEAGE_DATA__) return;

  if (currentApp) {
    try { unmount(currentApp); } catch (_) { /* ignore */ }
    currentApp = null;
  }

  target.innerHTML = '';
  currentApp = mount(App, { target });
};

// Auto-mount on initial script load
(window as any).__dbtxMountLineage();
