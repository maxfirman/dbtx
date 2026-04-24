<script lang="ts">
  import { SvelteFlow, Controls, MiniMap, Background, type Node, type Edge } from '@xyflow/svelte';
  import dagre from '@dagrejs/dagre';
  import LineageNode from './LineageNode.svelte';

  declare global {
    interface Window {
      __LINEAGE_DATA__?: {
        nodes: Array<{ id: string; data: Record<string, unknown> }>;
        edges: Array<{ id: string; source: string; target: string }>;
        currentNodeId: string;
        baseUrl: string;
      };
    }
  }

  const nodeTypes = { lineage: LineageNode };
  const raw = window.__LINEAGE_DATA__;

  function layout(
    rawNodes: Array<{ id: string; data: Record<string, unknown> }>,
    rawEdges: Array<{ id: string; source: string; target: string }>,
  ): { nodes: Node[]; edges: Edge[] } {
    const g = new dagre.graphlib.Graph();
    g.setDefaultEdgeLabel(() => ({}));
    g.setGraph({ rankdir: 'LR', nodesep: 40, ranksep: 120 });
    for (const n of rawNodes) {
      g.setNode(n.id, { width: 220, height: 70 });
    }
    for (const e of rawEdges) {
      g.setEdge(e.source, e.target);
    }
    dagre.layout(g);
    const nodes: Node[] = rawNodes.map((n) => {
      const pos = g.node(n.id);
      return {
        id: n.id,
        type: 'lineage',
        position: { x: pos.x - 110, y: pos.y - 35 },
        data: { ...n.data, isCurrent: n.id === raw?.currentNodeId },
      };
    });
    const edges: Edge[] = rawEdges.map((e) => ({
      ...e,
      type: 'smoothstep',
      animated: false,
    }));
    return { nodes, edges };
  }

  const initial = raw ? layout(raw.nodes, raw.edges) : { nodes: [], edges: [] };
  let nodes = $state.raw<Node[]>(initial.nodes);
  let edges = $state.raw<Edge[]>(initial.edges);

  function onNodeClick(_event: MouseEvent | TouchEvent, node: Node) {
    if (raw?.baseUrl && node.id !== raw.currentNodeId) {
      const uid = encodeURIComponent(node.id);
      window.location.href = `${raw.baseUrl}/${uid}?tab=lineage`;
    }
  }
</script>

{#if raw}
  <div style="width:100%;height:500px;">
    <SvelteFlow
      bind:nodes
      bind:edges
      {nodeTypes}
      fitView
      nodesDraggable={false}
      nodesConnectable={false}
      elementsSelectable={false}
      onnodeclick={(_event, node) => onNodeClick(_event, node)}
      proOptions={{ hideAttribution: true }}
    >
      <Controls />
      <MiniMap />
      <Background />
    </SvelteFlow>
  </div>
{:else}
  <div class="flex items-center justify-center h-64 text-slate-500">No lineage data available.</div>
{/if}
