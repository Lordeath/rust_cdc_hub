# rust_cdc_hub

English | [简体中文](README.md)

`rust_cdc_hub` is a Rust-based CDC (Change Data Capture) synchronization tool. It reads MySQL binlog events and streams row changes to MySQL, StarRocks, MeiliSearch, or the console. It also includes checkpoint-based resume, automatic schema creation/migration, plugins, Prometheus metrics, and a lightweight built-in monitoring UI.

## Features

- **MySQL binlog source**: sync a single table, multiple tables, or all tables in a database.
- **Multiple sink targets**: MySQL, StarRocks, MeiliSearch, Dameng, and Print are supported.
- **Checkpoint resume**: persist binlog positions to a checkpoint file and continue from the previous position after restart.
- **Automatic schema migration**: create databases, create tables, and add columns automatically where supported by the sink.
- **Plugin system**: built-in `ColumnIn` filtering plugin and `Plus` numeric-offset plugin.
- **Batching and retries**: configurable source/sink batch sizes and automatic retry on sink flush failures.
- **Built-in monitoring UI**: status page, health check, configuration summary, and Prometheus metrics.
- **Table selection**: supports `table_name: "*"`, comma-separated table names, and table-prefix exclusion.

## Architecture

```text
MySQL Binlog → MySQLSource → [Plugins] → Sink → MySQL / StarRocks / MeiliSearch / Dameng / Print
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
- `config_examples/config_example_dameng.yaml`: MySQL → Dameng
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
| `sink_type` | Yes | Sink type: `MySQL`, `Starrocks`, `MeiliSearch`, `Dameng`, or `Print`. |
| `source_config` | Yes | Source connection and sync settings. |
| `sink_config` | Yes | Sink connection and write settings. |
| `multi_mode` | No | Multi-database sync mode. Disabled by default; currently supports MySQL → MySQL/Dameng. |
| `auto_create_database` | No | Create target databases automatically. Defaults to `true`. |
| `auto_create_table` | No | Create target tables automatically. Defaults to `true`. |
| `auto_add_column` | No | Add missing target columns automatically. |
| `auto_modify_column` | No | Modify target columns automatically. |
| `sync_foreign_key_tables` | No | Include foreign-key-related tables during `table_name: "*"` discovery and add foreign-key constraints after MySQL/Dameng target initialization. Defaults to `true`. For MySQL targets, initial data writes temporarily disable foreign-key checks on the current session so existing target constraints do not block child rows loaded before parent rows. Set to `false` to keep the legacy behavior that excludes tables with foreign-key dependencies or references. |
| `sync_no_pk_table_schema` | No | Whether to sync table schemas that are not selected for CDC. Defaults to `true`; this includes no-primary-key tables, string primary keys, composite primary keys, and other unsupported primary-key shapes. These tables are schema-only, with primary keys and satisfiable foreign keys created when supported, but no initial data load or CDC writes. StarRocks skips them with a warning. |
| `sync_stored_procedure` | No | Sync source stored procedures and functions for MySQL → MySQL/Dameng. Defaults to `false`; `sync_stored_procedures` is also accepted. |
| `sync_stored_view` | No | Sync source views for MySQL → MySQL/Dameng. Defaults to `true`; `sync_stored_views` is also accepted. Existing target views with the same name are skipped. |
| `overwrite_stored_procedure` | No | When syncing stored procedures/functions, overwrite an existing target object with the same name. Defaults to `false`; `overwrite_stored_procedures` is also accepted. |
| `random_check_data_after_init` | No | Enable MySQL → Dameng fast sample verification mode. Defaults to `false`. When enabled, initialization only syncs a small sample for each CDC table instead of doing a full load. |
| `random_check_data_after_init_batch_size_min` | No | Number of sample rows to sync and verify per CDC table in fast sample verification mode. Defaults to `10`; recorded primary keys are queried from both MySQL and Dameng and compared column by column. |
| `plugins` | No | Plugin configuration list. |
| `source_batch_size` | No | Source batch size. |
| `sink_batch_size` | No | Sink batch size. |
| `checkpoint_file_path` | No | Checkpoint file path. |
| `log_level` | No | Log level, for example `debug` or `info`. |
| `enable_ui` | No | Enable the monitoring UI. Defaults to `true`. |
| `ui_bind` | No | UI bind address. |
| `ui_port` | No | UI port. Defaults to `18088`. |

### MySQL source options

| Field | Description |
| --- | --- |
| `host` / `port` | MySQL host and port. |
| `username` / `password` | MySQL credentials. |
| `database` | Source database name. |
| `table_name` | Table name; use a single table, comma-separated names, or `"*"` for automatic discovery. By default, `"*"` discovers all `BASE TABLE`s, including foreign-key-related tables. Tables with one integer primary key participate in data sync; other tables are schema-only. Set top-level `sync_no_pk_table_schema: false` to keep the legacy behavior of selecting only one-integer-primary-key tables. |
| `except_table_name_prefix` | Exclude tables by prefix; use comma-separated prefixes. |
| `server_id` | Binlog replication server id. It must be unique in the MySQL topology. |
| `statement_cache_capacity` | MySQL prepared statement cache capacity, passed through as SQLx `statement-cache-capacity`; set it to `"0"` to disable caching. If MySQL reports `Can't create more than max_prepared_stmt_count statements`, disconnect old clients or temporarily raise `max_prepared_stmt_count`, then restart the sync process. |

The primary-key column is detected from the MySQL table schema automatically. Tables that participate in data synchronization need exactly one integer primary key (`tinyint`, `smallint`, `mediumint`, `int`, `bigint`, and their unsigned variants). Tables that do not meet this requirement are schema-only when `sync_no_pk_table_schema: true`. The `pk_column` config key is deprecated; configuration loading fails if it appears. `meili_table_pk` is still required for the MeiliSearch sink because it defines the target index primary key.

Foreign-key tables are included by default. MySQL/Dameng targets create base tables first, load initial data, and then add foreign-key constraints. When only part of a schema is explicitly selected, constraints whose parent or child table is not selected are skipped with a warning; the table list is not expanded automatically. Dameng foreign-key creation is best-effort: if one constraint cannot be created because of index, data, or compatibility limitations, it is logged and sync continues.

### Multi-database sync mode (MySQL → MySQL/Dameng)

When `multi_mode.open: true`, `source_config[0].database` may contain comma-separated source databases and `sink_config[0].database` may contain a comma-separated target database allow-list. For Dameng, `sink_config[0].schema` may also contain the comma-separated target schema allow-list; when `schema` is configured, it is preferred as the Dameng target list. `database_route` must explicitly cover every source database.

```yaml
source_type: MySQL
sink_type: MySQL
source_config:
  - host: 127.0.0.1
    port: 3306
    username: cdc_user
    password: cdc_password
    database: newsee-soss,newsee-system
    table_name: "*"
    server_id: 10000

sink_config:
  - host: 127.0.0.1
    port: 3306
    username: sink_user
    password: sink_password
    database: newsee-system-soss,newsee-system-test

multi_mode:
  open: true
  init_parallelism: 4
  database_route:
    - source: newsee-soss
      sink: newsee-system-soss
    - source: newsee-system
      sink: newsee-system-test
```

Multi mode uses one binlog stream for the same source host and initializes source databases in parallel. `init_parallelism` defaults to `4`. Table filters (`table_name`, `include_table_regex`, `exclude_table_regex`, `except_table_name_prefix`) are shared by all source databases. Multiple source databases may route to the same target database/schema and table, but their primary key, columns, and column types must match.

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

sink_config:
  - host: 127.0.0.1
    port: 3306
    username: sink_user
    password: sink_password
    database: target_db

auto_create_database: true
auto_create_table: true
auto_add_column: true
sync_stored_procedure: false
sync_stored_view: true
overwrite_stored_procedure: false
log_level: info
enable_ui: true
ui_port: 8080
```

When `sync_stored_procedure` is enabled, the MySQL/Dameng sinks sync `PROCEDURE` and `FUNCTION` definitions during initialization according to the source-to-target database route. If a target object already exists and `overwrite_stored_procedure: false`, it is skipped; when set to `true`, the MySQL sink runs `DROP PROCEDURE/FUNCTION IF EXISTS` and recreates it, while the Dameng sink uses `CREATE OR REPLACE`. The sink removes `DEFINER=\`user\`@\`host\`` from `SHOW CREATE PROCEDURE/FUNCTION` before creating the target object, so the source database user is not copied to the target. MySQL → Dameng performs a basic DMSQL conversion; complex MySQL-specific routine syntax can fail initialization with the object name and error reason.

`sync_stored_view` is enabled by default. The MySQL/Dameng sinks read source `SHOW CREATE VIEW` output after target table schemas are ready, remove `DEFINER`, route the database/schema name, and create the target view. Existing target views with the same name are skipped. MySQL → Dameng performs a basic DMSQL conversion and retries dependent views before returning detailed failures with the view name, target schema, and SQL preview.

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

sink_config:
  - meili_url: http://127.0.0.1:7700
    meili_master_key: your_master_key
    table_name: articles
    meili_table_pk: id
```

### MySQL → Dameng example

```yaml
source_type: MySQL
sink_type: Dameng
source_config:
  - host: 127.0.0.1
    port: 3306
    username: cdc_user
    password: cdc_password
    database: source_db
    table_name: "*"
    server_id: 10003

sink_config:
  - host: 127.0.0.1
    port: 5236
    username: SYSDBA
    password: SYSDBA
    database: TARGET_SCHEMA
    init_insert_batch_rows: 16

auto_create_database: true  # Dameng: creates the target schema, not a physical database
auto_create_table: true
auto_add_column: true
sync_stored_view: true
random_check_data_after_init: false
random_check_data_after_init_batch_size_min: 10
```

Dameng uses a single-database, multi-schema model. `sink_config.database`/`sink_config.schema` is treated as the target schema; when `auto_create_database` is enabled, the sink runs `CREATE SCHEMA`, switches to that schema, and then creates tables/adds columns as needed. In multi mode, `database_route[].sink` is the target schema, and writes, table creation, column migration, and `IDENTITY_INSERT` state are isolated by target schema. `sink_config.init_insert_batch_rows` controls how many initialization rows are combined into each multi-row `INSERT`; it defaults to `16` and falls back to row-by-row writes if a batch insert fails. When `random_check_data_after_init` is enabled, every random-check startup first overwrites `/opt/fxm/datacheck-resule.log`; once checking starts, the file is overwritten again and appended with table-level results plus column mismatches.

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

When `enable_ui: true`, the application starts an Actix Web server. Use `ui_bind` and `ui_port` to configure the listening address and port. If no port is configured, it defaults to `18088`. The port can also be overridden with the `UI_PORT` or `PORT` environment variable.

When serving the UI behind an Nginx subpath such as `/rust_cdc_hub/`, the frontend uses relative URLs for `status`, `metrics`, `health`, and related endpoints, so a rewrite from the subpath to the application root is supported.

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
- [x] Database split plugin: enable split mode through a plugin and provide split-task management pages in Actix Web (V1)
- [x] Cleanup dry-run and reviewable SQL generation: count by default, generate reviewable SQL by download, do not delete by default
- [ ] Split task operation records: persist sync checks, dry-runs, SQL generation events, confirmations, and operator notes
- [ ] Generic command plugin for optional config cutover, service restart, notifications, and other external actions
- [ ] Dameng Sink support: prioritize writing MySQL CDC events into Dameng, including connectivity, type mapping, DDL compatibility, and sync validation
- [ ] Dameng Source support: support Dameng as a source database, covering change capture, schema introspection, type mapping, and checkpoint resume
- [ ] More complete DDL synchronization: MODIFY/DROP/RENAME and more
- [ ] Multi-target fan-out or table-based routing
- [ ] Failure bypass and replay: DLQ, error classification, exponential backoff
- [ ] Docker Compose E2E integration tests
- [ ] File-based logging: write runtime logs to files, with configurable log directory, rotation, and retention
- [ ] TLS, log redaction, rate limiting/backpressure, hot config reload

## Notes

- `server_id` must be unique; otherwise it may conflict with the MySQL cluster or other CDC jobs.
- Make sure the source database keeps binlogs long enough during initial snapshot and binlog consumption.
- Automatic schema creation/migration changes target schemas. Validate it in a test environment before enabling it in production.
- Configuration files usually contain secrets. Protect files referenced by `CONFIG_PATH` appropriately.

## License

This project is licensed under the Apache License 2.0. See [LICENSE](LICENSE).
