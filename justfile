set shell := ["zsh", "-lc"]

service_url := env_var_or_default("DBTX_SERVICE_URL", "http://127.0.0.1:8585")
database_url := env_var_or_default("DBTX_DATABASE_URL", "postgres://dbtx:dbtx@127.0.0.1:55432/dbtx")
listen := env_var_or_default("DBTX_LISTEN", "127.0.0.1:8585")

default:
    @just --list

build:
    cargo build

server:
    DBTX_DATABASE_URL={{database_url}} cargo run --bin dbtx-server -- --listen {{listen}}

worker-server:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx-worker -- --execution-mode server

worker-local:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx-worker -- --execution-mode local

migrate:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- state migrate

project-create *args:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- project create {{args}}

project-list:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- project list

environment-release *args:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- environment release {{args}}

environment-list project:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- environment list --project {{project}}

run *args:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- run {{args}}

build-dbt *args:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- build {{args}}

ls *args:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- ls {{args}}

test-dbt *args:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- test {{args}}

seed *args:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- seed {{args}}

invocations:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- invocation list

invocation-show invocation_id:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- invocation show --invocation-id {{invocation_id}}

invocation-cancel invocation_id:
    DBTX_SERVICE_URL={{service_url}} cargo run --bin dbtx -- invocation cancel --invocation-id {{invocation_id}}

real-tests:
    cargo test --test real_dbt -- --ignored

projection-tests:
    cargo test --test projection -- --ignored
