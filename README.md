# dbtx

`dbtx` is a Rust wrapper around `dbt-fusion` that persists run state to PostgreSQL.

The runtime is split into three binaries:

- `dbtx-server`: control plane and state persistence
- `dbtx-worker`: execution plane
- `dbtx`: client/operator CLI

Phase 1 supports:

- `dbtx state migrate`
- `dbtx build ...`
- `dbtx run ...`
- `dbtx ls ...`
- `dbtx test ...`
- `dbtx seed ...`
- `dbtx invocation list`
- `dbtx invocation show --invocation-id ...`
- `dbtx invocation cancel --invocation-id ...`
- `dbtx worker list`
- `dbtx queue list`

## Configuration

- `DBTX_DATABASE_URL`: PostgreSQL connection string for `dbtx-server`
- `DBTX_SERVICE_URL`: URL for the `dbtx-server` service
- `DBTX_WORKER_PATH`: optional path to the `dbtx-worker` executable used by `dbtx` for local execution
- `DBTX_DBT_PATH`: optional path to the `dbt` executable, defaults to `dbt`
- `DBTX_ENVIRONMENT_SLUG`: optional override for environment identity

## Execution Model

- `dbtx-server` owns the control plane:
  - projects, environments, runs, and reconstructed state
  - live invocation status and SSE log streaming
- `dbtx-worker` owns execution:
  - polls for claimable work in long-running mode
  - or executes one specific invocation in one-shot mode
- `dbtx` is a pure client:
  - talks to `dbtx-server`
  - for local execution, shells out to `dbtx-worker --execution-mode local --once --invocation-id ...`

This means both local execution and server-style execution use the same worker runtime.

## Examples

Start the control plane:

```bash
DBTX_DATABASE_URL=postgres://localhost/dbtx cargo run --bin dbtx-server -- --listen 127.0.0.1:8585
```

Start a long-running worker for server-mode execution:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx-worker -- --execution-mode server
```

Optionally start a long-running local-mode worker:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx-worker -- --execution-mode local
```

Apply schema migrations:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run -- state migrate
```

Run dbt with state capture:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run -- run --select orders+
```

Build dbt resources with state capture:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run -- build
```

List nodes using reconstructed state:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run -- ls --select orders+
```

Inspect active invocations:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run -- invocation list
```

Inspect active workers:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run -- worker list
```

Inspect queue backlog:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run -- queue list
```

Request cancellation for an invocation:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run -- invocation cancel --invocation-id <uuid>
```

Execute a single server-mode invocation with a one-shot worker:

```bash
DBTX_SERVICE_URL=http://127.0.0.1:8585 cargo run --bin dbtx-worker -- --execution-mode server --once
```

## Real Integration Tests

The repo includes ignored integration tests that run against:

- a real dbt Fusion CLI
- the vendored jaffle shop demo fixture under `tests/fixtures/jaffle_shop_project`
- DuckDB as the warehouse
- PostgreSQL for `dbtx` state, started automatically with `testcontainers` by default

Run them with:

```bash
cargo test --test real_dbt -- --ignored
```

Optional override:

- `DBTX_TEST_DATABASE_URL`: PostgreSQL URL for the integration test database; if unset, the tests start an ephemeral Postgres container automatically

## Licensing

`dbtx` is licensed under Apache License 2.0. The `dbtx` source code in this repository is independent of dbt Fusion and does not include dbt Fusion source code or binaries.

`dbtx` invokes a separately installed `dbt` / `dbt-fusion` executable. dbt Fusion is licensed separately by dbt Labs, and users of `dbtx` are responsible for obtaining and using dbt Fusion in compliance with the applicable dbt Labs license terms.
