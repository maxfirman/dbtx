<script lang="ts">
  import { onMount } from 'svelte';
  import GanttChart from './GanttChart.svelte';

  interface Resource {
    unique_id: string;
    resource_type: string | null;
    status: string;
    started_at: string | null;
    finished_at: string | null;
  }

  interface TimelineData {
    resources: Resource[];
    invocation_started_at: string | null;
    is_terminal: boolean;
  }

  const FINISH_EVENTS = new Set([
    'NodeFinished', 'LogModelResult', 'LogSeedResult',
    'LogSnapshotResult', 'LogTestResult', 'LogFreshnessResult',
  ]);

  const config = (window as any).__TIMELINE_DATA__ as { sseUrl: string; apiUrl: string } | undefined;

  let resources = $state<Resource[]>([]);
  let invocationStartedAt = $state<string | null>(null);
  let isTerminal = $state(false);
  let loading = $state(true);
  let resourcesFetched = false;

  async function fetchTimeline() {
    if (!config?.apiUrl) return;
    try {
      const resp = await fetch(config.apiUrl);
      if (!resp.ok) return;
      const data: TimelineData = await resp.json();
      if (data.resources.length > 0) {
        resources = data.resources;
        resourcesFetched = true;
      }
      invocationStartedAt = data.invocation_started_at;
      isTerminal = data.is_terminal;
    } catch (_) { /* ignore */ }
    loading = false;
  }

  function updateResource(uid: string, patch: Partial<Resource>) {
    const idx = resources.findIndex(r => r.unique_id === uid);
    if (idx < 0) return;
    resources[idx] = { ...resources[idx], ...patch };
  }

  function handleSseEvent(event: MessageEvent) {
    let payload: any;
    try { payload = JSON.parse(event.data); } catch { return; }

    const uid = payload.node_unique_id;
    const evName = payload.dbt_event_name;
    if (!uid || !evName) return;

    // First NodeStart means selected_resources are in the DB — fetch them
    if (!resourcesFetched && evName === 'NodeStart') {
      fetchTimeline();
      return;
    }

    if (FINISH_EVENTS.has(evName)) {
      const failed = payload.level === 'error' ||
        (payload.text && /\b(error|fail)\b/i.test(payload.text));
      updateResource(uid, {
        status: failed ? 'error' : 'success',
        finished_at: payload.timestamp,
        started_at: resources.find(r => r.unique_id === uid)?.started_at || payload.timestamp,
      });
    } else if (uid) {
      updateResource(uid, {
        status: 'running',
        started_at: resources.find(r => r.unique_id === uid)?.started_at || payload.timestamp,
      });
    }
  }

  function handleCompleted(event: MessageEvent) {
    let payload: any;
    try { payload = JSON.parse(event.data); } catch { return; }
    isTerminal = true;
    const failed = payload.exit_code !== 0;
    for (let i = 0; i < resources.length; i++) {
      if (resources[i].status === 'running' && failed) {
        resources[i] = { ...resources[i], status: 'error', finished_at: payload.timestamp };
      }
    }
    if (resources.length === 0) fetchTimeline();
  }

  onMount(() => {
    fetchTimeline();

    if (!config?.sseUrl) return;
    const source = new EventSource(config.sseUrl);
    const eventTypes = ['invocation.started', 'stdout.line', 'stderr.line', 'dbt.log'];
    for (const t of eventTypes) source.addEventListener(t, handleSseEvent);
    source.addEventListener('invocation.completed', handleCompleted);
    source.onmessage = handleSseEvent;

    return () => source.close();
  });
</script>

{#if loading}
  <div class="px-5 py-6 text-sm text-slate-400">Loading timeline…</div>
{:else if resources.length === 0 && !isTerminal}
  <div class="px-5 py-6 text-sm text-slate-400">Waiting for resource selection…</div>
{:else if resources.length === 0}
  <div class="px-5 py-6 text-sm text-slate-400">No resources tracked for this invocation.</div>
{:else}
  <GanttChart {resources} {invocationStartedAt} {isTerminal} />
{/if}
