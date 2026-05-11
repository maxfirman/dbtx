<script lang="ts">
  import { Handle, Position, type NodeProps } from '@xyflow/svelte';

  let { data }: NodeProps = $props();

  const statusColors: Record<string, string> = {
    success: '#22c55e',
    pass: '#22c55e',
    error: '#ef4444',
    fail: '#ef4444',
    skipped: '#94a3b8',
  };

  const reconcileColors: Record<string, string> = {
    reconciled: '#22c55e',
    stale: '#f59e0b',
    unknown: '#94a3b8',
    no_sources: '#e2e8f0',
  };

  let hovered = $state(false);
  let borderColor = $derived(data.isCurrent ? '#3b82f6' : hovered ? '#93c5fd' : '#e2e8f0');
  let borderWidth = $derived(data.isCurrent ? '2px' : '1px');
  let statusDot = $derived(statusColors[(data.status as string) || ''] || '#94a3b8');
  let testsPassing = $derived((data.testsPassing as number) || 0);
  let testsFailing = $derived((data.testsFailing as number) || 0);
  let hasTests = $derived(testsPassing > 0 || testsFailing > 0);

  let rs = $derived((data.reconcileState as any) || null);
  let codeColor = $derived(rs ? (reconcileColors[rs.code] || '#94a3b8') : '#e2e8f0');
  let sourceColor = $derived(rs ? (reconcileColors[rs.source] || '#e2e8f0') : '#e2e8f0');
  let hasReconcileState = $derived(rs && (rs.code !== 'unknown' || rs.source !== 'no_sources'));
</script>

<Handle type="target" position={Position.Left} />
<div
  role="button"
  tabindex="-1"
  onmouseenter={() => { if (!data.isCurrent) hovered = true; }}
  onmouseleave={() => { hovered = false; }}
  style="
    background: white;
    border: {borderWidth} solid {borderColor};
    border-radius: 8px;
    padding: 8px 12px;
    min-width: 180px;
    font-family: ui-sans-serif, system-ui, sans-serif;
    cursor: {data.isCurrent ? 'default' : 'pointer'};
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
  "
>
  <div style="display:flex;align-items:center;gap:6px;margin-bottom:2px;">
    <span style="width:8px;height:8px;border-radius:50%;background:{statusDot};flex-shrink:0;"></span>
    <span style="font-size:12px;font-weight:600;color:#0f172a;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;">
      {data.label || data.name || ''}
    </span>
  </div>
  <div style="font-size:10px;color:#64748b;display:flex;gap:8px;align-items:center;">
    <span>{data.resource_type || 'model'}</span>
    {#if data.materialized}
      <span>· {data.materialized}</span>
    {/if}
  </div>
  {#if hasReconcileState || hasTests}
    <div style="
      margin-top:6px;
      padding-top:5px;
      border-top:1px solid #f1f5f9;
      display:flex;
      align-items:center;
      gap:8px;
      font-size:10px;
      font-weight:500;
    ">
      {#if hasReconcileState}
        <span style="display:flex;align-items:center;gap:3px;" title="Code: {rs?.codeTooltip || ''}">
          <span style="width:7px;height:7px;border-radius:50%;background:{codeColor};"></span>
          <span style="color:#64748b;font-size:9px;">C</span>
        </span>
        <span style="display:flex;align-items:center;gap:3px;" title="Source: {rs?.sourceTooltip || ''}">
          <span style="width:7px;height:7px;border-radius:50%;background:{sourceColor};"></span>
          <span style="color:#64748b;font-size:9px;">S</span>
        </span>
      {/if}
      {#if testsPassing > 0}
        <span style="display:flex;align-items:center;gap:2px;color:#16a34a;">
          <svg width="10" height="10" viewBox="0 0 16 16" fill="none"><path d="M13.5 4.5L6.5 11.5L2.5 7.5" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>
          {testsPassing}
        </span>
      {/if}
      {#if testsFailing > 0}
        <span style="display:flex;align-items:center;gap:2px;color:#dc2626;">
          <svg width="10" height="10" viewBox="0 0 16 16" fill="none"><path d="M12 4L4 12M4 4l8 8" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg>
          {testsFailing}
        </span>
      {/if}
    </div>
  {/if}
</div>
<Handle type="source" position={Position.Right} />
