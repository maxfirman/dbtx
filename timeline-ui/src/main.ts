import { mount, unmount } from 'svelte';
import App from './App.svelte';

let currentApp: Record<string, unknown> | null = null;

(window as any).__dbtxMountTimeline = function () {
  const target = document.getElementById('timeline-root');
  if (!target || !(window as any).__TIMELINE_DATA__) return;

  if (currentApp) {
    try { unmount(currentApp); } catch (_) { /* ignore */ }
    currentApp = null;
  }

  target.innerHTML = '';
  currentApp = mount(App, { target });
};

(window as any).__dbtxMountTimeline();
