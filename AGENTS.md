# AGENTS.md

This file is for coding agents working in this repository.

## Project shape

`dbtx` has five binaries:

- `dbtx-server`
  - API server
  - HTML UI
  - invocation/event ingestion
  - request ID middleware (X-Request-Id)
  - immediate post-terminal unblock behavior
  - graceful shutdown on SIGINT
- `dbtx-worker`
  - execution plane
  - explicit queue consumers
  - graceful shutdown (finishes current invocation)
- `dbtx-reconciler`
  - background reconcile loop
  - blocked-plan sweep
  - manifest-prepare coordination
  - graceful shutdown on SIGINT
- `dbtx`
  - operator CLI
- `dbtx-migrate`
  - direct database migration runner
  - intended for init/bootstrap flows rather than normal operator use

## Module structure

### Database layer (`src/db/`)

Split into domain submodules:

- `db/mod.rs` — `Db` struct, connect, migrate, helper functions, constants
- `db/records.rs` — all record/input type definitions and status enums (`PlanStatus`, `EnvironmentStatus`, `DraftStatus`, `PreparationStatus`)
- `db/projects.rs` — project and draft CRUD
- `db/environments.rs` — environment, draft, and version CRUD
- `db/invocations.rs` — invocation lifecycle, workers, queues
- `db/reconciliation.rs` — plans, leases, source state, preparation
- `db/runs.rs` — run persistence, events, manifest, node state, finalization

Each submodule uses `use super::*;` and extends `impl Db`. Cross-module helper methods are `pub(super)`.

### Services layer (`src/services/`)

Split into domain submodules:

- `services/mod.rs` — shared types, enums, free helper functions, tests
- `services/projects.rs` — `ProjectService`
- `services/environments.rs` — `EnvironmentService` (reconciliation, planning, release)
- `services/invocations.rs` — `InvocationService`, fingerprint functions

### Other core modules

- `src/server.rs` — HTTP API routes, handlers, OpenAPI, error response mapping
- `src/ui/mod.rs` — server-rendered operator UI (HTMX/Askama)
- `src/worker.rs` — dbt execution runtime, git worktree management
- `src/reconciler.rs` — automatic reconcile daemon loop
- `src/dbt_runner.rs` — `DbtChild` struct for spawning dbt processes
- `src/dbt_utils.rs` — git state, profile generation, arg helpers
- `src/client.rs` — HTTP client for server API (with timeouts)
- `src/error.rs` — `AppError` enum with typed domain errors
- `src/event.rs` — dbt log event parsing and rendering
- `src/execution.rs` — execution mode, timeouts, completion types
- `src/invocation_bootstrap.rs` — invocation creation and startup
- `src/invocation_runtime.rs` — in-process event streaming
- `src/manifest.rs` — manifest parsing and reconstruction
- `src/profile.rs` — profile validation, encryption, generation
- `src/config.rs` — runtime configuration
- `src/api.rs` — API request/response types
- `src/cli.rs` — CLI argument definitions
- `src/cli_entry.rs` — CLI entry point handlers
- `src/cli_runtime.rs` — CLI subcommand handlers
- `src/cli_output.rs` — CLI output formatting

## Working rules

### Migrations

- Always add schema changes in a new file under `migrations/`.
- Do not rewrite older migration files unless explicitly instructed.
- Keep migration names sequential and descriptive.
- Local container bootstrap uses `dbtx-migrate`; keep it working as a direct DB path independent of the HTTP API.

### Error handling

- Use typed `AppError` variants for domain errors, not `io::Error::other()`.
- `AppError::Internal(String)` is the catch-all for unexpected errors.
- The server maps errors to HTTP status codes in `IntoResponse for ApiError`.
- Add new variants to the HTTP status mapping when adding new error types.

### Status fields

- Use typed enums (`PlanStatus`, `EnvironmentStatus`, `DraftStatus`, `PreparationStatus`, `InvocationLifecycleStatus`) instead of string comparisons.
- Enums live in `db/records.rs` and have `as_str()`, `parse()`, and `Display` impls.
- DB row mapping functions convert strings to enums via `::parse()`.

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

- `cargo test --test projection -- --ignored` (requires Docker)
- `npx playwright test`
- `cargo test --test real_dbt -- --ignored`

If you change reconciler, planning, admission, queues, or source-state behavior, add or update projection coverage.

Property-based tests (`proptest`) cover the planning algorithm invariants in `services/mod.rs`.

Wiremock-based tests cover client error handling in `client.rs`.

## High-value invariants

- A worker process executes one invocation at a time.
- Automatic reconcile must not hot-loop failed work.
- Blocked plans should replan against the latest live state before admission.
- Automatic reconcile should not be environment-wide blocked by stale cooldowns when the input has materially changed.
- Local and remote execution should keep sharing the same core invocation/worker model.
- All status fields use typed enums — never compare status strings directly.
- No `io::Error::other()` in production code — use `AppError` variants.
- No `unreachable!()` in production code — return errors instead.
- No `unsafe` in production code.

## Editing guidance

- Prefer small, behaviorally coherent changes.
- Keep DB writes centralized when possible instead of duplicating state transitions in multiple layers.
- Use `environment_query()` helper for environment SELECT queries to avoid column list drift.
- Use `DbtChild` from `dbt_runner.rs` for spawning dbt processes.
- If you add a new scheduler/read-model concept, think about:
  - persistence
  - operator visibility
  - tests
  - retry/failure semantics
- If you add a new `AppError` variant, add the HTTP status mapping in `server.rs`.

## Environment variables

Core:

- `DBTX_DATABASE_URL` — PostgreSQL connection string
- `DBTX_SERVICE_URL` — API server URL for CLI/worker
- `DBTX_SECRET_KEY` — encryption key for profile secrets
- `DBTX_DB_MAX_CONNECTIONS` — connection pool size (default: 20)

Worker / execution:

- `DBTX_DBT_PATH` — path to dbt executable
- `DBTX_GIT_CACHE_DIR` — git mirror cache directory
- `DBTX_WORKER_PATH` — worker binary path
- `DBTX_FUSION_DOWNLOAD_URL` — optional override for the Fusion-only worker Docker image build; defaults to the official dbt install script
- local compose mounts the host `~/.ssh` directory into the worker read-only for SSH git access

Reconciliation:

- `DBTX_VALIDATION_QUEUE` — queue for onboarding validation work
- `DBTX_RECONCILE_INTERVAL_MS` — reconcile loop interval (default: 5000)
- `DBTX_BLOCKED_PLAN_SWEEP_INTERVAL_MS` — blocked plan sweep interval (default: 2000)

## Documentation

If you materially change architecture or operator behavior, update `README.md`.
