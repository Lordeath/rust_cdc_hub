# rust_cdc_hub

English | [简体中文](README.md)

`rust_cdc_hub` is a Rust-based CDC (Change Data Capture) synchronization tool. It reads MySQL binlog events and streams row changes to MySQL, StarRocks, MeiliSearch, or the console. It also includes checkpoint-based resume, automatic schema creation/migration, plugins, Prometheus metrics, and a lightweight built-in monitoring UI.

## Features

- **MySQL binlog source**: sync a single table, multiple tables, or all tables in a database.
- **Multiple sink targets**: MySQL, StarRocks, MeiliSearch, and Print are supported.
- **Checkpoint resume**: persist binlog positions to a checkpoint file and continue from the previous position after restart.
- **Automatic schema migration**: create databases, create tables, and add columns automatically where supported by the sink.
- **Plugin system**: built-in `ColumnIn` filtering plugin and `Plus` numeric-offset plugin.
- **Batching and retries**: configurable source/sink batch sizes and automatic retry on sink flush failures.
- **Built-in monitoring UI**: status page, health check, configuration summary, and Prometheus metrics.
- **Table selection**: supports `table_name: "*"`, comma-separated table names, and table-prefix exclusion.

## Architecture

```text
MySQL Binlog → MySQLSource → [Plugins] → Sink → MySQL / StarRocks / MeiliSearch / Print
                    ↓
             Checkpoint Manager
```

Workspace layout:

```text
rust_cdc_hub/
├── Cargo.toml                 # Workspace root
├── src/main.rs                # Entrypoint, UI, metrics, start/restart orchestration
├── common/                    # Core traits, data types, config, checkpoint, metrics
├── source/
│   └── source_mysql/          # MySQL binlog source
├── sink/
│   ├── sink_mysql/            # MySQL sink
│   ├── sink_starrocks/        # StarRocks sink
│   ├── sink_meilisearch/      # MeiliSearch sink
│   └── sink_print/            # Console sink
└── plugin/
    ├── plugin_column_in/      # Filter events by column values
    └── plugin_column_plus/    # Apply numeric offsets to selected columns
```

Core abstractions:

| Trait | Purpose |
| --- | --- |
| `Source` | Reads CDC events, loads plugins, and exposes table metadata. |
| `Sink` | Connects to the target, writes events, flushes buffers, and releases resources. |
| `Plugin` | Filters or transforms CDC events. |

## Quick Start

### 1. Prepare the environment

- Rust toolchain (stable is recommended).
- A MySQL source database with binlog enabled.
- A target system: MySQL, StarRocks, MeiliSearch, or Print for debugging.

Recommended MySQL source settings:

```ini
[mysqld]
server-id=1
log-bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
```

The CDC user usually needs table-read and binlog-replication permissions, for example:

```sql
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
```

> Adjust privileges according to your own least-privilege security policy.

### 2. Build

```sh
git clone https://github.com/Lordeath/rust_cdc_hub.git
cd rust_cdc_hub
cargo build -r
```

### 3. Create a configuration file

Example configurations are available in this repository:

- `config_examples/config_example_mysql.yaml`: MySQL → MySQL
- `config_examples/config_example_meili.yaml`: MySQL → MeiliSearch
- `config_examples/config_example_print.yaml`: MySQL → console output

You can also create your own `/path/to/config.yaml` using the examples below.

### 4. Run

Run from source:

```sh
export CONFIG_PATH=/path/to/config.yaml
cargo run -r
```

Run the compiled binary:

```sh
export CONFIG_PATH=/path/to/config.yaml
./target/release/rust_cdc_hub
```

## Configuration

The application loads a YAML or JSON configuration file from the `CONFIG_PATH` environment variable.

### Top-level options

| Field | Required | Description |
| --- | --- | --- |
| `source_type` | Yes | Source type. Currently supports `MySQL`. |
| `sink_type` | Yes | Sink type: `MySQL`, `Starrocks`, `MeiliSearch`, or `Print`. |
| `source_config` | Yes | Source connection and sync settings. |
| `sink_config` | Yes | Sink connection and write settings. |
| `auto_create_database` | No | Create target databases automatically. Defaults to `true`. |
| `auto_create_table` | No | Create target tables automatically. Defaults to `true`. |
| `auto_add_column` | No | Add missing target columns automatically. |
| `auto_modify_column` | No | Modify target columns automatically. |
| `plugins` | No | Plugin configuration list. |
| `source_batch_size` | No | Source batch size. |
| `sink_batch_size` | No | Sink batch size. |
| `checkpoint_file_path` | No | Checkpoint file path. |
| `log_level` | No | Log level, for example `debug` or `info`. |
| `enable_ui` | No | Enable the monitoring UI. Defaults to `true`. |
| `ui_bind` | No | UI bind address. |
| `ui_port` | No | UI port. |

### MySQL source options

| Field | Description |
| --- | --- |
| `host` / `port` | MySQL host and port. |
| `username` / `password` | MySQL credentials. |
| `database` | Source database name. |
| `table_name` | Table name; use a single table, comma-separated names, or `"*"` for all tables. |
| `except_table_name_prefix` | Exclude tables by prefix; use comma-separated prefixes. |
| `server_id` | Binlog replication server id. It must be unique in the MySQL topology. |
| `pk_column` | Primary-key column name. |

### MySQL → MySQL example

```yaml
source_type: MySQL
sink_type: MySQL
source_config:
  - host: 127.0.0.1
    port: 3306
    username: cdc_user
    password: cdc_password
    database: source_db
    table_name: "*"
    except_table_name_prefix: "tmp_,dws_"
    server_id: 10000
    pk_column: id

sink_config:
  - host: 127.0.0.1
    port: 3306
    username: sink_user
    password: sink_password
    database: target_db
    pk_column: id

auto_create_database: true
auto_create_table: true
auto_add_column: true
log_level: info
enable_ui: true
ui_port: 8080
```

### MySQL → MeiliSearch example

```yaml
source_type: MySQL
sink_type: MeiliSearch
source_config:
  - host: 127.0.0.1
    port: 3306
    username: cdc_user
    password: cdc_password
    database: source_db
    table_name: articles
    server_id: 10001
    pk_column: id

sink_config:
  - meili_url: http://127.0.0.1:7700
    meili_master_key: your_master_key
    table_name: articles
    meili_table_pk: id
```

### MySQL → Print example

```yaml
source_type: MySQL
sink_type: Print
source_config:
  - host: 127.0.0.1
    port: 3306
    username: root
    password: password
    database: test
    table_name: test_table
    server_id: 10002
    pk_column: id

sink_config:
  - {}
log_level: debug
```

## Plugins

### ColumnIn: filter by column values

An event is forwarded only when one of the configured columns contains a value listed in `values`; otherwise it is filtered out.

```yaml
plugins:
  - plugin_type: ColumnIn
    config:
      columns: is_deleted,is_delete
      values: 0,1
```

### Plus: numeric offset for selected columns

Adds a fixed integer to selected `table.column` fields. This can be useful for ID offset/migration scenarios.

```yaml
plugins:
  - plugin_type: Plus
    config:
      columns: orders.id,order_events.event_id
      plus: 10000000000
```

### DatabaseSplit: database split plugin (planned)

`DatabaseSplit` is planned for shard-split scenarios where part of the business data in one database shard needs to be synchronized to another database. It will not replace the CDC pipeline. Instead, it will add split-task management on top of the existing Source, Sink, filtering plugins, and Actix Web UI.

Planned goals:

- Explicitly enable database split mode through a plugin.
- Reuse filtering plugins such as `ColumnIn` for project, tenant, organization, or similar business-scope filters.
- Load split-task pages in the Actix Web UI for sync configuration, task status, checklists, and operation records.
- Keep data sync, cutover preparation, and cleanup SQL generation as separate steps.
- Do not require the source CDC account to have `DELETE` privileges by default; cleanup should first provide dry-run counts or generated SQL.

Example shape:

```yaml
plugins:
  - plugin_type: ColumnIn
    config:
      columns: project_id,projectId,tenant_id
      values: 10001,10002
  - plugin_type: DatabaseSplit
    config:
      task_name: split-project-10001-10002
      mode: shard_data_split
      cleanup_strategy: generate_sql
      cleanup_confirm_phrase: I_UNDERSTAND_THE_RISK
```

Planned UI capabilities:

- Split task overview: source database, target database, filters, checkpoints, and sync status.
- Sync checks: show initial-load and incremental catch-up status before manual cutover.
- Cleanup dry-run: count matching rows per table without deleting data or generating SQL files by default.
- Cleanup SQL generation: generate reviewable `DELETE` SQL after explicit confirmation.
- Operation records: store dry-run, SQL generation, confirmation phrase, operator notes, and timestamps.

## Monitoring UI and Metrics

When `enable_ui: true`, the application starts an Actix Web server. Use `ui_bind` and `ui_port` to configure the listening address and port. The port can also be overridden with the `UI_PORT` or `PORT` environment variable.

Common endpoints:

| Endpoint | Description |
| --- | --- |
| `/` | HTML status page. |
| `/health` | Health check. |
| `/status` | JSON status, configuration summary, flush time, restart count, and more. |
| `/metrics` | Prometheus metrics. |

## Docker

Build an image:

```sh
docker build --network host -t rust_cdc_hub:0.0.1 -f ./debian.dockerfile .
```

Run the image:

```sh
docker run --name rust_cdc_hub --rm -it \
  -e CONFIG_PATH=/config.yaml \
  -v /path/to/config.yaml:/config.yaml \
  rust_cdc_hub:0.0.1
```

You can also mount a locally built binary into a base image:

```sh
docker run --name rust_cdc_hub --rm -it \
  -e CONFIG_PATH=/config.yaml \
  -v /path/to/config.yaml:/config.yaml \
  -v "$(pwd)/target/release:/app" \
  debian:stable-20251117 \
  /app/rust_cdc_hub
```

## Development

Common commands:

```sh
# Debug build
cargo build --verbose

# Release build
cargo build -r

# Run all tests
cargo test --verbose

# Run one test
cargo test test_name -- --nocapture

# Run tests for a specific crate
cargo test -p common --verbose
```

To add a new Source or Sink:

1. Create a new crate under `source/` or `sink/`.
2. Implement the `Source` or `Sink` trait from `common`.
3. Add the new crate to the workspace `Cargo.toml`.
4. Register the new type in the factory in `source/src/lib.rs` or `sink/src/lib.rs`.

## Roadmap

- [x] MySQL Source
- [x] MySQL / StarRocks / MeiliSearch / Print Sink
- [x] checkpoint resume
- [x] automatic database/table/column creation
- [x] plugin system
- [x] built-in UI and Prometheus metrics
- [x] table/database include and exclude support
- [ ] Database split plugin: enable split mode through a plugin and load split-task management pages in Actix Web
- [ ] Split task status and operation records: record sync checks, dry-runs, SQL generation, confirmations, and operator notes
- [ ] Cleanup dry-run and delete SQL generation: count by default, generate reviewable SQL after confirmation, do not delete by default
- [ ] Generic command plugin for optional config cutover, service restart, notifications, and other external actions
- [ ] More complete DDL synchronization: MODIFY/DROP/RENAME and more
- [ ] Multi-target fan-out or table-based routing
- [ ] Failure bypass and replay: DLQ, error classification, exponential backoff
- [ ] Docker Compose E2E integration tests
- [ ] TLS, log redaction, rate limiting/backpressure, hot config reload

## Notes

- `server_id` must be unique; otherwise it may conflict with the MySQL cluster or other CDC jobs.
- Make sure the source database keeps binlogs long enough during initial snapshot and binlog consumption.
- Automatic schema creation/migration changes target schemas. Validate it in a test environment before enabling it in production.
- Configuration files usually contain secrets. Protect files referenced by `CONFIG_PATH` appropriately.

## License

Refer to the license file in this repository. If no license has been declared yet, add one before production usage or redistribution.
