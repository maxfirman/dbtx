<script lang="ts">
  import { onMount } from 'svelte';

  interface Resource {
    unique_id: string;
    resource_type: string | null;
    status: string;
    started_at: string | null;
    finished_at: string | null;
  }

  let {
    resources = [],
    invocationStartedAt = null,
    isTerminal = false,
    modelBaseUrl = null,
  }: {
    resources: Resource[];
    invocationStartedAt: string | null;
    isTerminal: boolean;
    modelBaseUrl: string | null;
  } = $props();

  let now = $state(Date.now());
  let rafId: number | null = null;
  let lastFrame = 0;

  const startTime = $derived.by(() => {
    let min = Infinity;
    for (const r of resources) {
      if (r.started_at) min = Math.min(min, Date.parse(r.started_at));
    }
    return min < Infinity ? min : (invocationStartedAt ? Date.parse(invocationStartedAt) : now);
  });
  const elapsedMs = $derived(Math.max(now - startTime, 1));

  function terminalEndTime(): number {
    let max = 0;
    for (const r of resources) {
      if (r.finished_at) max = Math.max(max, Date.parse(r.finished_at));
    }
    return max || Date.now();
  }

  function tick(ts: number) {
    if (ts - lastFrame > 100) {
      lastFrame = ts;
      now = Date.now();
    }
    if (!isTerminal) {
      rafId = requestAnimationFrame(tick);
    }
  }

  onMount(() => {
    if (isTerminal) {
      now = terminalEndTime();
    } else {
      rafId = requestAnimationFrame(tick);
    }
    return () => { if (rafId != null) cancelAnimationFrame(rafId); };
  });

  $effect(() => {
    if (isTerminal && rafId != null) {
      cancelAnimationFrame(rafId);
      rafId = null;
      now = terminalEndTime();
    }
  });

  function shortName(uid: string): string {
    const parts = uid.split('.');
    return parts.length >= 3 ? parts.slice(2).join('.') : parts[parts.length - 1];
  }

  function typeBadge(rt: string | null): string {
    if (!rt) return '';
    const m: Record<string, string> = { model: 'M', seed: 'S', test: 'T', snapshot: 'N', source: 'R' };
    return m[rt] || rt.charAt(0).toUpperCase();
  }

  function barStyle(r: Resource): string {
    if (!r.started_at) return 'display:none';
    const s = Date.parse(r.started_at) - startTime;
    const e = r.finished_at ? Date.parse(r.finished_at) - startTime : now - startTime;
    const left = Math.max(0, (s / elapsedMs) * 100);
    const width = Math.max(0.5, ((e - s) / elapsedMs) * 100);
    return `left:${left}%;width:${width}%`;
  }

  function barColor(status: string): string {
    if (status === 'success') return 'bg-emerald-500';
    if (status === 'running') return 'bg-blue-500';
    if (status === 'error') return 'bg-red-500';
    return 'bg-slate-200';
  }

  function resourceUrl(r: Resource): string | null {
    if (!modelBaseUrl) return null;
    const rt = r.resource_type;
    if (rt === 'test') {
      // Link to parent model's Tests tab: find the model this test belongs to
      // Test unique_ids don't directly encode the parent, so link to the test's own detail
      const encoded = encodeURIComponent(r.unique_id);
      return `${modelBaseUrl}/${encoded}?tab=tests`;
    }
    if (rt === 'model' || rt === 'seed' || rt === 'snapshot' || rt === 'source') {
      const encoded = encodeURIComponent(r.unique_id);
      return `${modelBaseUrl}/${encoded}?tab=overview`;
    }
    return null;
  }

  function formatElapsed(ms: number): string {
    const s = Math.floor(ms / 1000);
    const m = Math.floor(s / 60);
    return m > 0 ? `${m}m ${s % 60}s` : `${s}s`;
  }
</script>

<div class="gantt">
  <div class="gantt-header">
    <div class="gantt-label-col text-xs font-medium text-slate-500 uppercase tracking-wide">Resource</div>
    <div class="gantt-bar-col text-xs text-slate-400 text-right">{formatElapsed(elapsedMs)}</div>
  </div>
  {#each resources as r (r.unique_id)}
    <div class="gantt-row">
      <div class="gantt-label-col" title={r.unique_id}>
        {#if r.resource_type}
          <span class="gantt-type-badge">{typeBadge(r.resource_type)}</span>
        {/if}
        {#if resourceUrl(r)}
          <a class="gantt-name gantt-link" href={resourceUrl(r)}>{shortName(r.unique_id)}</a>
        {:else}
          <span class="gantt-name">{shortName(r.unique_id)}</span>
        {/if}
      </div>
      <div class="gantt-bar-col">
        <div class="gantt-track">
          {#if r.started_at}
            <div class="gantt-bar {barColor(r.status)}" style={barStyle(r)}></div>
          {/if}
        </div>
      </div>
    </div>
  {/each}
</div>

<style>
  .gantt { padding: 0 1.25rem 1rem; }
  .gantt-header { display: flex; align-items: center; padding: 0.5rem 0; border-bottom: 1px solid #e2e8f0; }
  .gantt-row { display: flex; align-items: center; height: 2rem; }
  .gantt-row:not(:last-child) { border-bottom: 1px solid #f1f5f9; }
  .gantt-label-col { width: 200px; min-width: 200px; padding-right: 0.75rem; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; font-size: 0.8125rem; color: #334155; }
  .gantt-bar-col { flex: 1; min-width: 0; }
  .gantt-track { position: relative; height: 1.125rem; background: #f8fafc; border-radius: 0.25rem; overflow: hidden; }
  .gantt-bar { position: absolute; top: 0; height: 100%; border-radius: 0.25rem; min-width: 3px; transition: left 0.15s linear, width 0.15s linear; }
  .gantt-type-badge { display: inline-flex; align-items: center; justify-content: center; width: 1.125rem; height: 1.125rem; border-radius: 0.25rem; background: #e2e8f0; color: #64748b; font-size: 0.625rem; font-weight: 600; margin-right: 0.375rem; flex-shrink: 0; }
  .gantt-name { overflow: hidden; text-overflow: ellipsis; }
  .gantt-link { color: #334155; text-decoration: none; }
  .gantt-link:hover { color: #0f172a; text-decoration: underline; }
</style>
