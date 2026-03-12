# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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

# Run tests
cargo test --verbose
```

## Architecture

### Crate Structure (Workspace)

- **`common`** - Core traits (`Source`, `Sink`, `Plugin`), data types (`DataBuffer`, `Value`, `Operation`), config parsing, checkpoint management, and metrics
- **`source`** - Source factory, currently only `source_mysql`
- **`sink`** - Sink factory with implementations: `sink_print`, `sink_mysql`, `sink_starrocks`, `sink_meilisearch`
- **`plugin`** - Plugin system with `plugin_column_in` (column filtering) and `plugin_column_plus` (column addition)
- **`src/main.rs`** - Entry point that wires Source → Plugin → Sink, includes embedded UI server (actix-web) and Prometheus metrics endpoint

### Core Traits (defined in `common/src/lib.rs`)

1. **`Source`** - Reads CDC events from a source database. Implementations must provide:
   - `start(sink)` - Start streaming events to sink
   - `add_plugins(plugins)` - Register transformation plugins
   - `get_table_info()` - Return schema metadata

2. **`Sink`** - Writes data to target systems. Key methods:
   - `connect()` - Establish connection
   - `write_record()` - Write a single CDC event
   - `flush()` - Flush buffered data (called periodically by timer)

3. **`Plugin`** - Transforms CDC events. Single method:
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

## Configuration

Configuration is loaded from a YAML or JSON file specified via `CONFIG_PATH` environment variable. See `config_example_mysql.yaml` and `config_example_starrocks.yaml` for examples.

Key config options:
- `source_type`, `sink_type` - Select implementations
- `source_config` / `sink_config` - Per-target connection parameters
- `auto_create_database`, `auto_create_table`, `auto_add_column` - Schema migration flags
- `enable_ui`, `ui_port` - Built-in monitoring UI

## Built-in UI & Monitoring

When `enable_ui: true` (default), the app starts an actix-web server with endpoints:
- `/` - HTML status page
- `/health` - Health check
- `/status` - JSON status
- `/metrics` - Prometheus metrics

## Adding New Sources or Sinks

1. Create new crate under `source/` or `sink/`
2. Implement the appropriate trait from `common`
3. Add to workspace `Cargo.toml`
4. Update factory in `source/src/lib.rs` or `sink/src/lib.rs` to handle new type
