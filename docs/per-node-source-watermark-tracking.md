# Per-Node Source Watermark Tracking

## Problem Statement

Today, source state satisfaction in dbtx is all-or-nothing per source key. When a source event triggers a reconciliation plan that selects N downstream models:

1. **Partial failure wastes work** — if 1 of 50 models fails, the source event remains unsatisfied. The next plan re-executes all 50 models, even though 49 already processed the data.
2. **Manual runs are invisible** — a manual `dbt build` that successfully processes downstream models doesn't advance source satisfaction. The reconciler will redundantly re-execute those models.
3. **No per-model freshness visibility** — operators cannot see which specific models are behind on which sources.

## Design

### Core Concept: Per-Node Source Event Watermark

Each node in the DAG stores a **watermark** per ancestor source: "as of my last successful execution, I have processed all source events up to event ID X."

The fundamental invariant:

> **A node's watermark for a given source can never exceed the minimum of its parents' watermarks for that source at the time the node's execution began.**

This guarantees a node only claims to have processed data that has actually flowed through every intermediate layer.

### Watermark Identity

Watermarks use the **source state event ID** (monotonic bigint from `source_state_events.id`). This avoids clock skew issues and provides strict ordering. The `observed_at` timestamp is stored alongside for display purposes only.

### Scope

Watermarks propagate through **all node types** in the manifest DAG (models, tests, seeds, snapshots, sources). If a node is downstream of a tracked source, it gets a watermark.

### Storage

#### Primary read model — current watermark state

```sql
CREATE TABLE node_source_watermarks (
    project_id            BIGINT NOT NULL,
    environment_id        BIGINT NOT NULL,
    unique_id             TEXT NOT NULL,
    source_key            TEXT NOT NULL,
    watermark_event_id    BIGINT NOT NULL,
    watermark_observed_at TIMESTAMPTZ,
    run_id                UUID NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (project_id, environment_id, unique_id, source_key)
);

CREATE INDEX idx_node_source_watermarks_source_key
    ON node_source_watermarks (project_id, environment_id, source_key);
```

One row per (node, source) pair per environment. Upserted on successful node execution. Optimized for fast reads by the reconciler and UI.

#### Candidate staging — pre-execution snapshots

```sql
CREATE TABLE node_source_watermark_candidates (
    run_id                UUID NOT NULL,
    unique_id             TEXT NOT NULL,
    source_key            TEXT NOT NULL,
    watermark_event_id    BIGINT NOT NULL,
    watermark_observed_at TIMESTAMPTZ,
    created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (run_id, unique_id, source_key)
);
```

Written on node start, read and deleted on node finish. Persisted in the database (not in-memory) to support multi-replica control plane deployments where the node start and node finish events may be processed by different server instances.

#### Audit log — propagation timing

```sql
CREATE TABLE node_source_watermark_log (
    id                  BIGSERIAL PRIMARY KEY,
    project_id          BIGINT NOT NULL,
    environment_id      BIGINT NOT NULL,
    unique_id           TEXT NOT NULL,
    source_key          TEXT NOT NULL,
    watermark_event_id  BIGINT NOT NULL,
    previous_event_id   BIGINT,
    run_id              UUID NOT NULL,
    invocation_id       UUID,
    recorded_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_watermark_log_source_event
    ON node_source_watermark_log (project_id, environment_id, source_key, watermark_event_id);
```

Append-only. Written alongside the watermark upsert, but only when the watermark actually advances. Enables:
- "When did source event X reach node Y?" — `MIN(recorded_at) WHERE watermark_event_id >= X AND unique_id = Y`
- "How long did event X take to fully propagate?" — `MAX(recorded_at)` across all downstream nodes
- Retention: prune after N days; only the latest entry per (node, source) is operationally needed.

#### Ancestor source mapping — precomputed per manifest

```sql
CREATE TABLE node_ancestor_sources (
    run_id      UUID NOT NULL,
    unique_id   TEXT NOT NULL,
    source_key  TEXT NOT NULL,
    PRIMARY KEY (run_id, unique_id, source_key)
);

CREATE INDEX idx_node_ancestor_sources_source
    ON node_ancestor_sources (run_id, source_key);
```

Populated when a manifest is persisted. Maps each node to all ancestor source keys it is downstream of. Computed via a recursive upward walk of `manifest_edges`. Only source nodes that have corresponding entries in `source_state_events` (i.e., tracked sources) are included.

This precomputation:
- Avoids expensive recursive DAG walks at node execution time
- Provides immediate knowledge of which nodes are affected by a source event
- Handles DAG changes cleanly: when a new manifest is persisted, the new ancestor mapping reflects the new graph structure; nodes that gained or lost source ancestors are immediately visible

### Watermark Computation

#### When: Pre-execution (snapshot) and Post-execution (commit)

Watermark computation happens in two phases per node execution:

**Phase 1 — Pre-execution snapshot (on `NodeStarted`):**

When a node begins execution, compute its *candidate watermark* for each ancestor source:

```
candidate_watermark(node, source) = MIN(
    watermark(parent, source) for each parent of node
    where parent has a watermark for source
)
```

For **source nodes themselves** (nodes whose `unique_id` matches a tracked `source_key`):

```
candidate_watermark(source_node, self) = MAX(source_state_events.id)
    WHERE source_key = source_node.unique_id
    AND project_id = ... AND environment_id = ...
```

The candidate watermark is computed *before* execution to ensure conservatism — we capture what was available at the time the node started, not what arrived during execution.

Candidates are persisted to `node_source_watermark_candidates` (not held in memory) to support multi-replica deployments.

**Phase 2 — Post-execution commit (on `NodeFinished` with success status):**

If the node succeeds:
1. Read candidates from `node_source_watermark_candidates` for `(run_id, unique_id)`
2. Batch upsert into `node_source_watermarks` with monotonic guard
3. Write advancement entries to `node_source_watermark_log` (only for watermarks that actually advanced)
4. Delete candidates from `node_source_watermark_candidates`

If the node fails:
1. Delete candidates from `node_source_watermark_candidates` (cleanup)
2. No watermark advancement

#### Monotonic guard

The upsert uses a guard to prevent watermarks from going backwards:

```sql
INSERT INTO node_source_watermarks (...)
VALUES (...)
ON CONFLICT (project_id, environment_id, unique_id, source_key) DO UPDATE SET
    watermark_event_id = EXCLUDED.watermark_event_id,
    watermark_observed_at = EXCLUDED.watermark_observed_at,
    run_id = EXCLUDED.run_id,
    updated_at = NOW()
WHERE node_source_watermarks.watermark_event_id < EXCLUDED.watermark_event_id
```

This ensures concurrent executions are safe — a slower invocation processing an older source event cannot regress a watermark already advanced by a faster invocation.

### Source Satisfaction (Revised)

With per-node watermarks, source event satisfaction becomes:

> A source event E is **satisfied** when all executable nodes downstream of E's source_key have `watermark_event_id >= E.id`.

This replaces the current single-flag `environment_source_state_status.latest_satisfied_event_id` model.

The reconciler query for finding stale nodes becomes:

```sql
SELECT nas.unique_id
FROM node_ancestor_sources nas
LEFT JOIN node_source_watermarks nsw
    ON nsw.project_id = $1
   AND nsw.environment_id = $2
   AND nsw.unique_id = nas.unique_id
   AND nsw.source_key = nas.source_key
WHERE nas.run_id = $3          -- current manifest run
  AND nas.source_key = $4      -- the source that changed
  AND (nsw.watermark_event_id IS NULL OR nsw.watermark_event_id < $5)
```

Using the precomputed `node_ancestor_sources` avoids a recursive CTE at query time.

### Reconciliation Planning Changes

The source-triggered reconciliation plan now selects only the **stale subset** of downstream nodes rather than all downstream nodes:

1. Find unsatisfied source events (unchanged)
2. **NEW**: Query `node_ancestor_sources` + `node_source_watermarks` to find nodes where watermark is behind the target event ID (or missing)
3. Create plan with the filtered `selected_resources`

This means partial failure naturally results in a smaller retry plan.

### Backward Compatibility

- `environment_source_state_status` is retained for the "is this source fully satisfied?" summary query (useful for UI and reconciler fast-path checks)
- It is now derived: a source is satisfied when all executable downstream nodes have watermarks >= the event. Successful plan completion does not mark source events satisfied directly.
- Existing environments with no watermark data: missing watermarks are treated as "never processed". This is conservative and may trigger one redundant execution per node on the first source-triggered plan after migration.

### Graph Changes

When the manifest changes (new commit deployed):
- **New edges** (model A now depends on source B): The new `node_ancestor_sources` entry shows A depends on B, but A has no watermark for B → treated as unsatisfied → included in next source-triggered plan for B.
- **Removed edges** (model A no longer depends on source B): The new `node_ancestor_sources` no longer lists B for A. The stale watermark row in `node_source_watermarks` is harmless (orphaned). A is no longer considered when checking satisfaction for B.
- **Removed nodes**: Stale watermark rows are harmless. Can be cleaned up lazily.
- **No invalidation of existing watermarks needed**: The precomputed ancestor mapping in the new manifest determines what matters; old watermark rows for removed relationships are simply ignored.

### Concurrency

Multiple invocations can run overlapping subsets of the graph simultaneously. The monotonic guard (`WHERE watermark_event_id < EXCLUDED.watermark_event_id`) ensures:
- Concurrent writes to the same node are safe (highest watermark wins)
- A slower invocation processing an older source event cannot regress a watermark

The pre-execution snapshot reads parent watermarks at a point in time. If a parent's watermark advances between the snapshot and the child's commit, the child gets a slightly conservative watermark — correct but not maximally fresh. It catches up on the next execution.

## Implementation Plan

### Phase 1: Schema, Ancestor Precomputation, and Storage Layer

**Migration:**
- Add `node_source_watermarks` table (primary read model)
- Add `node_source_watermark_candidates` table (transient staging)
- Add `node_source_watermark_log` table (audit trail)
- Add `node_ancestor_sources` table (precomputed per manifest)

**Ancestor precomputation (`db/runs.rs` — extend `persist_manifest_in_tx`):**

After persisting `manifest_nodes` and `manifest_edges`, populate `node_ancestor_sources`:

```sql
WITH RECURSIVE ancestors(unique_id, ancestor_id) AS (
    -- Base case: direct parents
    SELECT me.child_unique_id, me.parent_unique_id
    FROM manifest_edges me
    WHERE me.run_id = $1
    UNION
    -- Recursive: walk upward
    SELECT a.unique_id, me.parent_unique_id
    FROM ancestors a
    JOIN manifest_edges me ON me.child_unique_id = a.ancestor_id AND me.run_id = $1
)
INSERT INTO node_ancestor_sources (run_id, unique_id, source_key)
SELECT DISTINCT $1, a.unique_id, a.ancestor_id
FROM ancestors a
JOIN manifest_nodes mn ON mn.run_id = $1 AND mn.unique_id = a.ancestor_id
WHERE mn.resource_type = 'source'
  AND EXISTS (
      SELECT 1 FROM source_state_events sse
      WHERE sse.source_key = a.ancestor_id
        AND sse.project_id = $2
        AND sse.environment_id = $3
  )
```

Note: The `EXISTS` filter ensures only *tracked* sources (those with at least one source state event) are included. Untracked sources don't participate in watermark propagation.

**DB layer (new `db/watermarks.rs`):**
- `batch_insert_watermark_candidates(run_id, candidates: &[(unique_id, source_key, event_id, observed_at)])` — bulk insert on node start
- `load_watermark_candidates(run_id, unique_id)` — read candidates on node finish
- `delete_watermark_candidates(run_id, unique_id)` — cleanup after commit or failure
- `batch_upsert_node_source_watermarks(project_id, environment_id, entries: &[...])` — with monotonic guard, returns which entries actually advanced
- `batch_insert_watermark_log(entries: &[...])` — append to audit log for advanced entries
- `load_parent_watermarks(project_id, environment_id, parent_unique_ids: &[String])` — for computing candidate MIN
- `load_latest_source_event_id(project_id, environment_id, source_key)` — for source-node self-watermark
- `list_stale_downstream_nodes(project_id, environment_id, source_key, target_event_id, manifest_run_id)` — for reconciliation planning
- `load_node_ancestor_sources(run_id, unique_id)` — which sources a node depends on
- `are_all_downstream_nodes_satisfied(project_id, environment_id, source_key, target_event_id, manifest_run_id)` — for derived satisfaction check

### Phase 2: Watermark Computation in Event Processing

**Integration point: `persist_log_event` in `db/runs.rs`**

The existing `persist_log_event` handles `NormalizedNodeEvent` with `started_at` and `finished_at`. Extend:

1. **On node start** (`started_at` is Some, `finished_at` is None, and node has ancestor sources):
   - Load ancestor sources for this node from `node_ancestor_sources` for the current run
   - Load parent watermarks from `node_source_watermarks` for the node's direct parents
   - For source nodes: load `MAX(source_state_events.id)` for the matching source_key
   - Compute `MIN(parent watermarks)` per source
   - Batch insert candidates into `node_source_watermark_candidates`

2. **On node finish with promotable status** (`finished_at` is Some, status is success/pass):
   - Load candidates from `node_source_watermark_candidates`
   - Batch upsert into `node_source_watermarks` with monotonic guard
   - For entries that advanced: append to `node_source_watermark_log`
   - Delete candidates

3. **On node finish with failure/skip**:
   - Delete candidates from `node_source_watermark_candidates`

**Performance considerations:**
- Batch load all parent watermarks for a node in one query (nodes typically have 1-10 parents)
- The ancestor source lookup is a simple primary key read from `node_ancestor_sources`
- Candidate insert/read/delete are keyed by `(run_id, unique_id)` — fast primary key operations
- For nodes with no ancestor sources (not downstream of any tracked source), skip all watermark logic entirely

### Phase 3: Reconciliation Planning Integration

**Modify `EnvironmentService::reconcile()` for source_state_change path:**

Replace:
```rust
self.db.list_downstream_manifest_node_unique_ids(source_baseline_run_id, &source_keys).await?
```

With:
```rust
self.db.list_stale_downstream_nodes(
    environment.project_id,
    environment.id,
    &source_keys,
    &source_event_ids,
    source_baseline_run_id,
).await?
```

This returns only nodes where `watermark_event_id < target_event_id` (or no watermark exists).

**Modify `replan_pending_plan()` for source_state_change:**

Instead of checking `are_source_state_events_satisfied` (all-or-nothing), query for remaining stale nodes. If none are stale, complete the plan as noop.

**Feature flag:** Gate the new planning path behind an environment-level or global flag during rollout. Fall back to the existing all-downstream behavior when disabled.

### Phase 4: Source Satisfaction Derivation

**Update `environment_source_state_status` to be derived from per-node watermarks:**

During successful node watermark commits:

1. For each source key advanced by that node, check unsatisfied events with `are_all_downstream_nodes_satisfied()`
2. If all executable downstream nodes have watermarks >= the event ID, advance `environment_source_state_status.latest_satisfied_event_id`

This means manual runs that advance all executable downstream watermarks mark the source as satisfied immediately, without waiting for run completion or the next reconciler tick.

### Phase 5: UI and Observability

- Add watermark state to the environment detail view (per-source freshness summary)
- Add per-model watermark display to node state views
- Show "X of Y nodes up to date" for each source in the environment dashboard
- Add propagation timing view using the audit log: "source event X took Y seconds to reach all downstream nodes"

### Phase 6: Candidate Cleanup and Maintenance

- Periodic cleanup of orphaned `node_source_watermark_candidates` rows (from crashed runs that never completed)
- Periodic cleanup of `node_source_watermarks` rows for nodes no longer in the current manifest
- Retention policy for `node_source_watermark_log` (e.g., 30 days)
- Cleanup of `node_ancestor_sources` for old run_ids no longer referenced

## Migration Strategy

1. Deploy schema migration (new tables, no behavior change)
2. Deploy ancestor precomputation (starts populating `node_ancestor_sources` on new manifest persists)
3. Deploy watermark computation (starts populating watermarks on new node executions)
4. Existing environments start with empty watermarks → treated as "never processed" → first source-triggered plan after deployment executes all downstream nodes (same as today)
5. After one successful execution cycle, watermarks are populated and subsequent plans benefit from partial selection
6. Enable watermark-based planning (feature flag)
7. Once validated, deprecate the all-or-nothing satisfaction path

## Phased Delivery

| Phase | Scope | Risk | Dependencies |
|-------|-------|------|--------------|
| 1 | Schema + ancestor precomputation + storage layer | None (additive) | — |
| 2 | Watermark computation on node execution | Low (write-only, no behavior change) | Phase 1 |
| 3 | Reconciler uses watermarks for plan selection | Medium (changes what gets executed) | Phase 2 |
| 4 | Derived satisfaction from watermarks | Medium (changes when source is "done") | Phase 3 |
| 5 | UI and observability | None (read-only) | Phase 2 |
| 6 | Cleanup and maintenance | Low | Phase 2 |

Phases 1-2 can ship independently with no behavior change — they populate data silently. Phase 3 is the behavioral change that delivers the core value. Phase 4 closes the manual-run gap. Phases 5-6 are independent quality-of-life improvements.

## Open Questions

1. **Cleanup of orphaned watermark rows** — when nodes are removed from the manifest, their watermark rows become orphaned. Periodic cleanup job, or leave them? They're harmless but accumulate. Recommendation: periodic cleanup keyed to the current manifest's node set.

2. **Source nodes that aren't in source_state_events** — not all dbt sources will have corresponding source state events (only those with external ingestion tracking). Nodes downstream of untracked sources simply won't have watermarks for those sources. The `node_ancestor_sources` precomputation filters to tracked sources only.

3. **Interaction with code_change plans** — a code change plan rebuilds nodes because their checksums changed, not because of source freshness. Should a successful code_change execution also advance source watermarks? **Yes** — if the node ran successfully, it processed whatever data was available, so its watermark should advance to the current source state at execution time. This is handled naturally by the pre-execution snapshot reading the current source event state.

4. **Candidate table growth under high concurrency** — if many nodes start simultaneously, the candidates table sees a burst of inserts. These are short-lived (deleted on node finish) and keyed by `(run_id, unique_id)` so they don't accumulate. A periodic sweep of candidates older than N hours handles leaked rows from crashed runs.

5. **First-run bootstrapping** — on the very first execution of a node after this feature ships, it has no parent watermarks to inherit. The candidate computation yields no watermarks (MIN of empty set = nothing). After the source node itself runs and gets a self-watermark, subsequent runs of downstream nodes will start propagating. This means full propagation requires at least one execution of each layer after deployment — which happens naturally on the first source-triggered plan.
