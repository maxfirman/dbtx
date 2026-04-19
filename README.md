# dbtx

`dbtx` is a Rust control plane, execution plane, and operator UI for dbt execution, state capture, and reconciliation.

Today the system supports:

- local dbt execution with persisted state
- remote projects and remote environments
- browser-based project and environment onboarding
- worker-queue based remote execution
- live invocation, worker, and queue operator views
- selected-resource tracking for active dbt work
- reconciliation planning, blocked-plan re-admission, and an automatic reconciler loop

## Binaries

The runtime is split into four binaries:

- `dbtx-server`
  - API server
  - HTML UI
  - invocation/event ingestion
  - immediate post-terminal unblock handling
- `dbtx-worker`
  - executes claimed invocations
  - polls one or more explicit worker queues
- `dbtx-reconciler`
  - automatic reconcile daemon
  - drift detection
  - blocked-plan sweep
  - manifest-prepare coordination
- `dbtx`
  - operator CLI

## Architecture

### Control plane

`dbtx-server` persists:

- projects and environments
- invocations and runs
- manifests, manifest nodes, and manifest edges
- current node state
- active selected resources
- reconciliation plans
- reconcile preparation state
- source state events and satisfaction state
- worker and queue registry data

### Execution plane

`dbtx-worker` is queue-driven.

- workers must specify at least one `--queue`
- a single worker process executes one invocation at a time
- local and remote execution both use the same worker runtime
- remote execution runs server-side
- local opportunistic execution runs locally

### Reconciliation

`dbtx-reconciler` is the background control loop.

It currently:

- scans remote `auto_deploy = true` environments
- detects code drift and unsatisfied source state
- creates or reuses reconciliation plans
- starts `manifest_prepare` work for unseen desired commits
- periodically rechecks blocked plans
- replans blocked work against the latest live state before admission

The reconciler uses environment-scoped leases in the database. Automatic retry/backoff is keyed to a persisted reconcile input fingerprint, so newer desired commits or newer source events bypass stale cooldowns.

## Key concepts

### Projects and environments

- remote projects are created and validated through the UI or public API draft flow
- remote environments define:
  - branch / commit target
  - worker queue
  - deployment flags such as `auto_deploy` and `immutable`
- local environments are created opportunistically by the local CLI flow

### Invocations

Invocation execution mode and worker queue are derived from the target environment.

- remote environments always create server invocations
- local environments always create local invocations
- public `POST /v1/invocations` no longer accepts `execution_mode` or `worker_queue`

The UI derives additional display states:

- `queued`
- `running`
- `cancelling`
- `succeeded`
- `failed`
- `canceled`

### Selected resources

Remote dbt execution injects an `on-run-start` hook that logs `selected_resources`.

The control plane stores:

- which resources an invocation selected
- which resources have started
- which resources are still active

This data drives:

- active resource visibility
- blocked plan admission control
- future model-level concurrency control

### Reconciliation plans

The scheduler persists environment-scoped plans before an invocation exists.

Plan reasons currently include:

- `code_change`
- `source_state_change`
- `manual_retry`
- `manual_release`

Plans can move through states such as:

- `planned`
- `blocked`
- `admitted`
- `completed`
- `failed`
- `canceled`
- `superseded`

## Running locally

### 1. Start PostgreSQL

Use any PostgreSQL instance reachable by `DBTX_DATABASE_URL`.

Example:

```bash
export DBTX_DATABASE_URL=postgres://dbtx:dbtx@127.0.0.1:55432/dbtx
```

### 2. Start the API server

```bash
DBTX_DATABASE_URL=$DBTX_DATABASE_URL cargo run --bin dbtx-server -- --listen 127.0.0.1:8585
```

### 3. Apply migrations

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx -- state migrate
```

### 4. Start at least one worker

Remote/server worker example:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx-worker -- --execution-mode server --queue generic
```

Local worker example:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx-worker -- --execution-mode local --queue local-demo
```

Workers can consume multiple queues:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx-worker -- --execution-mode server --queue generic --queue analytics
```

### 5. Start the reconciler

```bash
DBTX_DATABASE_URL=$DBTX_DATABASE_URL cargo run --bin dbtx-reconciler
```

## UI

The main browser UI is served by `dbtx-server`.

Important views:

- `/`
  - dashboard
- `/ui/projects`
  - project list and remote onboarding
- `/ui/projects/{project_id}/environments/{slug}`
  - environment detail, reconcile state, plans, preparation state, active resources
- `/ui/invocations`
  - live invocation list with deep-linked filters and pagination
- `/ui/workers`
  - persistent worker registry view
- `/ui/queues`
  - persistent queue view

## Public API

The JSON API includes:

- project CRUD and draft onboarding
- environment CRUD and environment draft onboarding
- invocations
- active environment resources
- source state events
- reconciliation plans
- reconcile preparation state
- workers and queues

The OpenAPI document is generated from the server code.

## CLI examples

Run a dbt command against the current project:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx -- build --select orders+
```

List invocations:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx -- invocation list
```

Show an invocation:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx -- invocation show --invocation-id <uuid>
```

Cancel an invocation:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx -- invocation cancel --invocation-id <uuid>
```

Release an environment to a commit:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx -- environment release --project <project_id> --slug <environment_slug> --git-commit-sha <sha>
```

Rollback an environment to a version:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx -- environment rollback --project <project_id> --slug <environment_slug> --version-id <id>
```

## Important environment variables

Core:

- `DBTX_DATABASE_URL`
- `DBTX_SERVICE_URL`
- `DBTX_SECRET_KEY`
- `DBTX_DBT_PATH`
- `DBTX_WORKER_PATH`
- `DBTX_ENVIRONMENT_SLUG`

Worker / execution:

- `DBTX_GIT_CACHE_DIR`
- `DBTX_GIT_CACHE_TTL_HOURS`
- `DBTX_LOCAL_MACHINE_ID`
- `DBTX_ONE_SHOT_WORKER_LOG`

Reconciliation / scheduling:

- `DBTX_VALIDATION_QUEUE`
- `DBTX_RECONCILE_INTERVAL_MS`
- `DBTX_BLOCKED_PLAN_SWEEP_INTERVAL_MS`

Testing:

- `DBTX_TEST_DATABASE_URL`

## Testing

Fast validation:

```bash
cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

Browser tests:

```bash
npx playwright test
```

Docker-backed projection tests:

```bash
cargo test --test projection -- --ignored
```

Real dbt integration tests:

```bash
cargo test --test real_dbt -- --ignored
```

## Development notes

- add schema changes in new migration files; do not rewrite older migrations
- remote project/environment onboarding and validation are worker-backed
- the validation queue is configurable and separate from normal execution queues
- the reconciler is a separate daemon process, not part of the API server loop

## Licensing

`dbtx` is licensed under Apache License 2.0.

`dbtx` invokes a separately installed `dbt` / `dbt-fusion` executable. Users are responsible for complying with the license terms that apply to that executable.
