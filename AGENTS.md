# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

rust_cdc_hub is a Rust-based CDC (Change Data Capture) tool that reads MySQL binlog events and synchronizes data to various sink targets. It supports checkpoint/resume, automatic schema migration, and plugin extensibility.

## Build Commands

```sh
# Build release binary
cargo build -r

# Build with verbose output
cargo build --verbose

# Run the application (requires CONFIG_PATH env var)
export CONFIG_PATH=/path/to/config.yaml
cargo run -r

# Run all tests
cargo test --verbose

# Run a single test
cargo test test_name -- --nocapture

# Run tests in a specific crate
cargo test -p common --verbose
```

## Architecture

### Crate Structure (Workspace)

```
rust_cdc_hub/
├── Cargo.toml              # Workspace root
├── src/main.rs             # Entry point with UI server and Prometheus metrics
├── common/                 # Core traits, data types, config, checkpoint, metrics
├── source/
│   └── source_mysql/      # MySQL binlog source
├── sink/
│   ├── sink_mysql/        # MySQL sink
│   ├── sink_starrocks/    # StarRocks sink
│   ├── sink_meilisearch/  # MeiliSearch sink
│   └── sink_print/        # Console print sink (debug)
└── plugin/
    ├── plugin_column_in/  # Column filtering plugin
    └── plugin_column_plus/# Column addition plugin
```

### Core Traits (defined in `common/src/lib.rs`)

1. **`Source`** - Reads CDC events from a source database:
   - `start(sink)` - Start streaming events to sink
   - `add_plugins(plugins)` - Register transformation plugins
   - `get_table_info()` - Return schema metadata

2. **`Sink`** - Writes data to target systems:
   - `connect()` - Establish connection
   - `write_record()` - Write a single CDC event
   - `flush()` - Flush buffered data (called periodically by timer)

3. **`Plugin`** - Transforms CDC events:
   - `collect(data_buffer)` - Transform and return modified buffer

### Data Flow

```
MySQL Binlog → MySQLSource → [Plugins] → Sink → (MySQL/StarRocks/MeiliSearch/Print)
                    ↓
            Checkpoint Manager
            (file-based resume)
```

### Key Data Types

- **`DataBuffer`** - Represents a CDC event: table name, before/after images, operation type (CREATE/UPDATE/DELETE), binlog position
- **`Value`** - Enum covering all MySQL types with `resolve_string()` for conversion
- **`CdcConfig`** - Configuration struct parsed from YAML/JSON
- **`Operation`** - Enum: Insert, Update, Delete
- **`FlushByOperation`** - Trait for flushing strategies

## Configuration

Configuration is loaded from a YAML or JSON file specified via `CONFIG_PATH` environment variable.

### Example: MySQL to MySQL

```yaml
source_type: MySQL
sink_type: MySQL
source_config:
  - host: 192.168.1.52
    port: 3306
    username: root
    password: password
    database: source_db
    table_name: "*"           # or comma-separated: "table1,table2"
    except_table_name_prefix: "tmp_,dws_"  # exclude prefixes
    server_id: 10001
    pk_column: id             # primary key column

sink_config:
  - host: 192.168.1.53
    port: 3306
    username: root
    password: password
    database: target_db
    pk_column: id

# Optional settings
log_level: debug
enable_ui: true
ui_port: 8080
auto_create_database: true
auto_create_table: true
auto_add_column: true
```

### Example: MySQL to MeiliSearch

```yaml
source_type: MySQL
sink_type: MeiliSearch
source_config:
  - host: 192.168.1.103
    port: 3306
    username: root
    password: password
    database: source_db
    table_name: table_name
    server_id: 10000
    pk_column: id

sink_config:
  - meili_url: http://192.168.1.103:17700
    meili_master_key: your_key
    table_name: index_name
    meili_table_pk: id
```

### Key Config Options

- `source_type` / `sink_type` - Select implementations
- `source_config` / `sink_config` - Per-target connection parameters
- `table_name` - Table name, `*` for all tables, or comma-separated list
- `except_table_name_prefix` - Exclude tables with given prefixes
- `auto_create_database` / `auto_create_table` / `auto_add_column` - Schema migration flags
- `enable_ui` / `ui_port` - Built-in monitoring UI
- `server_id` - MySQL server ID for binlog replication (must be unique)

## Built-in UI & Monitoring

When `enable_ui: true` (default), the app starts an actix-web server:
- `/` - HTML status page
- `/health` - Health check
- `/status` - JSON status with config summary, last flush time, restart count
- `/metrics` - Prometheus metrics endpoint

Environment variables for UI:
- `UI_PORT` or `PORT` - Override UI port

## Implemented Features

- MySQL binlog as source
- Multiple sink targets: MySQL, StarRocks, MeiliSearch, Print
- Checkpoint/resume with file-based storage
- Automatic schema migration (create database/table, add columns)
- Plugin system for column filtering and addition
- Prometheus metrics (throughput, latency, failures, checkpoints)
- Built-in monitoring UI
- Table include/exclude patterns (prefix, blacklist)

## Adding New Sources or Sinks

1. Create new crate under `source/` or `sink/`
2. Implement the appropriate trait from `common`
3. Add to workspace `Cargo.toml` members list
4. Update factory in `source/src/lib.rs` or `sink/src/lib.rs` to handle new type

## Docker

```sh
# Build Docker image
docker build --network host -t my-cdc-hub:0.0.1 -f "./debian.dockerfile" .

# Run with Docker
docker run --name rust_cdc_hub --rm -it \
  -e CONFIG_PATH=/config.yaml \
  -v /path/to/config.yaml:/config.yaml \
  my-cdc-hub:0.0.1
```
