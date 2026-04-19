# AGENTS.md

This file is for coding agents working in this repository.

## Project shape

`dbtx` has four binaries:

- `dbtx-server`
  - API server
  - HTML UI
  - invocation/event ingestion
  - immediate post-terminal unblock behavior
- `dbtx-worker`
  - execution plane
  - explicit queue consumers
- `dbtx-reconciler`
  - background reconcile loop
  - blocked-plan sweep
  - manifest-prepare coordination
- `dbtx`
  - operator CLI

Core modules:

- `src/db.rs`
  - schema-facing read/write model
  - large and central; change carefully
- `src/services.rs`
  - higher-level orchestration and planning logic
- `src/reconciler.rs`
  - automatic reconcile loop
- `src/worker.rs`
  - dbt execution runtime
- `src/ui.rs`
  - server-rendered HTML UI

## Working rules

### Migrations

- Always add schema changes in a new file under `migrations/`.
- Do not rewrite older migration files unless explicitly instructed.
- Keep migration names sequential and descriptive.

### Queues and workers

- Workers must specify one or more explicit `--queue` values.
- There is no implicit `any` queue.
- Invocation `execution_mode` and `worker_queue` are derived from the target environment, not from `POST /v1/invocations`.
- Validation/onboarding work uses the configurable validation queue.

### Reconciliation

- The reconciler is a separate daemon, not a server background task.
- Environment-scoped reconcile leases are the concurrency boundary for planning/admission logic.
- Automatic retry/backoff is keyed to persisted reconcile input identity.
- Newer desired commits or newer source events must be able to bypass stale cooldowns.
- Favor correctness over avoiding duplicate work:
  - unnecessary reruns are acceptable
  - missing required reruns is not

### Source state

- Source-driven reconcile should rely on explicit source event satisfaction, not `last_success_at` timestamp heuristics.
- If you touch source-state logic, keep the model version-aware and conservative.

### Selected resources

- Active-resource tracking is important scheduler state, not just UI decoration.
- Remote execution injects selected-resource logging into runtime worktrees.
- Avoid changes that weaken the guarantee that active resource overlap blocks admission.

### UI

- The operator UI is HTMX/Askama-based and intentionally server-rendered.
- Prefer incremental partial updates over client-heavy rewrites.
- Keep live status behavior consistent across dashboard, list, and detail views.

### Tests

Before finishing significant work, prefer:

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

When relevant:

- `cargo test --test projection -- --ignored`
- `npx playwright test`
- `cargo test --test real_dbt -- --ignored`

If you change reconciler, planning, admission, queues, or source-state behavior, add or update projection coverage.

## High-value invariants

- A worker process executes one invocation at a time.
- Automatic reconcile must not hot-loop failed work.
- Blocked plans should replan against the latest live state before admission.
- Automatic reconcile should not be environment-wide blocked by stale cooldowns when the input has materially changed.
- Local and remote execution should keep sharing the same core invocation/worker model.

## Editing guidance

- Prefer small, behaviorally coherent changes.
- Keep DB writes centralized when possible instead of duplicating state transitions in multiple layers.
- If you add a new scheduler/read-model concept, think about:
  - persistence
  - operator visibility
  - tests
  - retry/failure semantics

## Documentation

If you materially change architecture or operator behavior, update `README.md`.
