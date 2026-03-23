# dbtx

`dbtx` is a Rust wrapper around `dbt-fusion` that persists run state to PostgreSQL.

Phase 1 supports:

- `dbtx state migrate`
- `dbtx build ...`
- `dbtx run ...`
- `dbtx ls ...`
- `dbtx test ...`
- `dbtx seed ...`

## Configuration

- `DBTX_DATABASE_URL`: PostgreSQL connection string
- `DBTX_DBT_PATH`: optional path to the `dbt` executable, defaults to `dbt`
- `DBTX_PROJECT_SLUG`: optional override for project identity
- `DBTX_ENVIRONMENT_SLUG`: optional override for environment identity

## Examples

Initialize the schema:

```bash
DBTX_DATABASE_URL=postgres://localhost/dbtx cargo run -- state migrate
```

Run dbt with state capture:

```bash
DBTX_DATABASE_URL=postgres://localhost/dbtx cargo run -- run --target dev --select orders+
```

Build dbt resources with state capture:

```bash
DBTX_DATABASE_URL=postgres://localhost/dbtx cargo run -- build --target dev
```

List nodes using reconstructed state:

```bash
DBTX_DATABASE_URL=postgres://localhost/dbtx cargo run -- ls --select orders+
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
