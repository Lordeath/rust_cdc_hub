# rust_cdc_hub

[English](README.en.md) | 简体中文

`rust_cdc_hub` 是一个基于 Rust 的 CDC（Change Data Capture，变更数据捕获）同步工具。它读取 MySQL Binlog，将数据变更流式同步到 MySQL、StarRocks、MeiliSearch 或控制台打印等目标端，并内置断点续传、自动建库建表/加字段、插件处理、Prometheus 指标和轻量级监控 UI。

## 功能特性

- **MySQL Binlog 数据源**：支持单表、多表和全库表同步。
- **多种 Sink 目标端**：已支持 MySQL、StarRocks、MeiliSearch、Dameng、Print。
- **断点续传**：基于 checkpoint 文件记录位点，进程重启后从上次位点继续消费。
- **自动 Schema 迁移**：支持自动建库、自动建表、自动加字段等能力（按 Sink 能力生效）。
- **插件系统**：内置列值过滤插件 `ColumnIn` 和数值偏移插件 `Plus`。
- **批量与重试**：支持 Source/Sink batch 配置，Sink flush 失败时自动重试。
- **内置监控 UI**：提供状态页、健康检查、配置摘要和 Prometheus metrics。
- **表选择能力**：支持 `table_name: "*"`、逗号分隔表名和前缀排除。

## 架构概览

```text
MySQL Binlog → MySQLSource → [Plugins] → Sink → MySQL / StarRocks / MeiliSearch / Dameng / Print
                    ↓
             Checkpoint Manager
```

Workspace 结构：

```text
rust_cdc_hub/
├── Cargo.toml                 # Workspace 根配置
├── src/main.rs                # 程序入口、UI、metrics、启动/重启逻辑
├── common/                    # 核心 Trait、数据结构、配置、checkpoint、metrics
├── source/
│   └── source_mysql/          # MySQL Binlog Source
├── sink/
│   ├── sink_mysql/            # MySQL Sink
│   ├── sink_starrocks/        # StarRocks Sink
│   ├── sink_meilisearch/      # MeiliSearch Sink
│   └── sink_print/            # 控制台输出 Sink
└── plugin/
    ├── plugin_column_in/      # 按列值过滤事件
    └── plugin_column_plus/    # 对指定列做数值偏移
```

核心抽象：

| Trait | 作用 |
| --- | --- |
| `Source` | 读取 CDC 事件、加载插件、提供表结构信息。 |
| `Sink` | 连接目标端、写入事件、flush 缓冲区、释放资源。 |
| `Plugin` | 对 CDC 事件进行过滤或转换。 |

## 快速开始

### 1. 准备环境

- Rust toolchain（建议使用稳定版）。
- 可开启 Binlog 的 MySQL 源库。
- 目标端：MySQL、StarRocks、MeiliSearch 或仅使用 Print 调试。

MySQL 源库建议开启类似配置：

```ini
[mysqld]
server-id=1
log-bin=mysql-bin
binlog_format=ROW
binlog_row_image=FULL
```

同步账号通常需要读取表结构和复制 Binlog 的权限，例如：

```sql
GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%';
FLUSH PRIVILEGES;
```

> 实际授权请结合你的安全策略收敛到最小权限。

### 2. 编译

```sh
git clone https://github.com/Lordeath/rust_cdc_hub.git
cd rust_cdc_hub
cargo build -r
```

### 3. 创建配置文件

仓库提供了示例配置：

- `config_examples/config_example_mysql.yaml`：MySQL → MySQL
- `config_examples/config_example_meili.yaml`：MySQL → MeiliSearch
- `config_examples/config_example_dameng.yaml`：MySQL → 达梦
- `config_examples/config_example_print.yaml`：MySQL → 控制台打印

也可以参考下方配置章节自行创建，例如 `/path/to/config.yaml`。

### 4. 运行

源码运行：

```sh
export CONFIG_PATH=/path/to/config.yaml
cargo run -r
```

使用编译产物运行：

```sh
export CONFIG_PATH=/path/to/config.yaml
./target/release/rust_cdc_hub
```

## 配置说明

程序通过环境变量 `CONFIG_PATH` 指定 YAML/JSON 配置文件。

### 顶层配置

| 字段 | 必填 | 说明 |
| --- | --- | --- |
| `source_type` | 是 | Source 类型，目前支持 `MySQL`。 |
| `sink_type` | 是 | Sink 类型：`MySQL`、`Starrocks`、`MeiliSearch`、`Dameng`、`Print`。 |
| `source_config` | 是 | Source 连接与同步配置列表。 |
| `sink_config` | 是 | Sink 连接与写入配置列表。 |
| `multi_mode` | 否 | 多库同步模式配置，默认关闭；当前支持 MySQL → MySQL/Dameng。 |
| `auto_create_database` | 否 | 是否自动建库，默认 `true`。 |
| `auto_create_table` | 否 | 是否自动建表，默认 `true`。 |
| `auto_add_column` | 否 | 是否自动加字段。 |
| `auto_modify_column` | 否 | 是否自动修改字段。 |
| `sync_foreign_key_tables` | 否 | 是否在 `table_name: "*"` 自动发现时纳入外键相关表，并在 MySQL/Dameng 目标端初始化后补外键约束，默认 `true`。MySQL 目标端初始化写入期间会临时关闭当前 session 的外键检查，避免目标库已有外键时子表先写入失败。设为 `false` 时保留旧行为：排除有外键依赖或被外键引用的表。 |
| `sync_no_pk_table_schema` | 否 | 是否同步不参与 CDC 的表结构，默认 `true`；包括无主键、复合主键、超长字符串主键和其他不支持同步主键的表。这些表只建结构、主键和可满足依赖的外键，不做初始化数据和 CDC 同步；StarRocks 目标端会跳过并告警。 |
| `sync_stored_procedure` | 否 | MySQL → MySQL/Dameng 时是否同步源库存储过程和函数，默认 `false`；也兼容 `sync_stored_procedures`。 |
| `sync_stored_view` | 否 | MySQL → MySQL/Dameng 时是否同步源库视图，默认 `true`；也兼容 `sync_stored_views`。目标库已有同名视图时跳过。 |
| `overwrite_stored_procedure` | 否 | 同步存储过程/函数时，目标库已存在同名对象是否覆盖，默认 `false`；也兼容 `overwrite_stored_procedures`。 |
| `random_check_data_after_init` | 否 | MySQL → Dameng 快速样本验证模式，默认 `false`。开启后初始化阶段每张 CDC 表只同步少量样本数据，不做完整初始化。 |
| `random_check_data_after_init_batch_size_min` | 否 | 快速样本验证模式下每张 CDC 表同步并验证的样本行数，默认 `10`；程序会记录已同步主键，并按主键查询 MySQL 和达梦逐字段比较。 |
| `plugins` | 否 | 插件配置列表。 |
| `source_batch_size` | 否 | Source 批量读取/处理大小。 |
| `sink_batch_size` | 否 | Sink 批量写入大小。 |
| `checkpoint_file_path` | 否 | checkpoint 路径。未配置时使用 `/checkpoint/checkpoints.sqlite`；配置为目录时使用该目录下的 `checkpoints.sqlite`；配置为 `.db`/`.sqlite`/`.sqlite3` 文件时直接使用该文件。 |
| `checkpoint_flush_interval_secs` | 否 | checkpoint 后台任务刷盘间隔，默认 `30` 秒；配置为 `0` 或不配置时使用默认值。 |
| `log_level` | 否 | 日志级别，如 `debug`、`info`。 |
| `log_file` | 否 | 文件日志配置。默认不启用；启用后会继续打印 console，并按日期或大小滚动写入文件。 |
| `enable_ui` | 否 | 是否启用监控 UI，默认 `true`。 |
| `ui_bind` | 否 | UI 监听地址。 |
| `ui_port` | 否 | UI 监听端口，默认 `18088`。 |

### MySQL Source 配置

| 字段 | 说明 |
| --- | --- |
| `host` / `port` | MySQL 地址和端口。 |
| `username` / `password` | MySQL 账号密码。 |
| `database` | 源库名。 |
| `table_name` | 表名；可填单表、逗号分隔多表或 `"*"` 表示按规则自动发现表。`"*"` 默认发现所有 `BASE TABLE`，包括外键相关表；其中单一整数主键表或单一 `char`/`varchar` 主键表参与数据同步，其他表仅同步表结构。设置顶层 `sync_no_pk_table_schema: false` 时只纳入可数据同步的表。 |
| `except_table_name_prefix` | 排除指定前缀的表，多个前缀用逗号分隔。 |
| `server_id` | Binlog replication server id，必须与 MySQL 集群中其他 server id 不重复。 |
| `ssl_mode` | MySQL SSL模式，透传为SQLx连接参数 `ssl-mode`。可选：`disabled`、`preferred`、`required`、`verify_ca`、`verify_identity`；默认 `disabled`。如果源库/目标库必须使用SSL，请显式设置为 `required` 或证书校验模式。 |
| `statement_cache_capacity` | MySQL prepared statement 缓存容量，透传为 SQLx 连接参数 `statement-cache-capacity`；设为 `"0"` 可关闭缓存。MySQL 报 `Can't create more than max_prepared_stmt_count statements` 时，可先断开旧连接或临时调大 `max_prepared_stmt_count` 后重启同步进程。 |

主键列会从 MySQL 表结构自动识别；参与数据同步的表需要单一整数主键（`tinyint`、`smallint`、`mediumint`、`int`、`bigint` 及其 unsigned 变体），或单一 `char(n)` / `varchar(n)` 字符串主键（当前 `n <= 512`）。不满足该条件的表在 `sync_no_pk_table_schema: true` 时仅同步表结构。只要存在数据同步表，启动时会校验 MySQL `binlog_format=ROW` 且 `binlog_row_image=FULL`，否则直接失败，避免 UPDATE/DELETE 缺少旧行数据。配置项 `pk_column` 已弃用，配置文件中出现该字段会在启动解析时报错。MeiliSearch 目标端的 `meili_table_pk` 仍需单独配置为索引主键字段。

外键表默认参与同步。MySQL/Dameng 目标端会先创建基础表并完成初始化数据写入，再补外键约束；如果显式只同步部分表，缺少父表或子表的外键约束会跳过并记录告警，不会自动扩展同步表清单。达梦目标端会尽力补外键约束，单条约束因索引、数据或兼容性原因创建失败时会记录告警并继续同步。

### 多库同步模式（MySQL → MySQL/Dameng）

`multi_mode.open: true` 后，单个 `source_config[0].database` 可以用逗号配置多个源库，单个 `sink_config[0].database` 可以用逗号配置目标库清单；达梦目标端也可以用 `sink_config[0].schema` 配置目标 schema 清单，若配置了 `schema` 会优先作为达梦目标清单。`database_route` 必须显式覆盖每个源库。

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

多库模式下只创建一个 binlog stream 读取同一 host 的 binlog；初始化阶段按源库并行抽取，`init_parallelism` 默认 `4`。表过滤规则（`table_name`、`include_table_regex`、`exclude_table_regex`、`except_table_name_prefix`）对所有源库共用。允许多个源库路由到同一个目标库/schema 下的同名表，但源表主键、字段和字段类型必须一致。

### MySQL → MySQL 示例

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
log_file:
  enabled: false
  dir: /app/logs
  file_name: rust_cdc_hub.log
  max_size_mb: 100
  retention_days: 30
  max_backup_files: 300
  compress_gzip: true
enable_ui: true
ui_port: 8080
```

`log_file.enabled: true` 时，程序会写入 `/app/logs/rust_cdc_hub.log`，并按天或超过 `max_size_mb` 滚动；归档日志默认压缩为 `.gz`，同时受 `retention_days` 和 `max_backup_files` 限制。日志目录无法创建或写入时启动失败。

checkpoint 使用 SQLite + WAL 持久化。首次启动 SQLite checkpoint 时，会自动扫描同目录旧版 `checkpoint_*.json` 文件并导入，旧 JSON 文件会保留作为回滚备份。运行中只有 `CheckpointService` 后台任务持有 SQLite 连接并执行写入；source/sink 只把已确认安全的表级 checkpoint 和 binlog stream checkpoint 提交到内存队列，由后台任务按 `checkpoint_flush_interval_secs` 合并后批量刷盘。无变化时不会写盘；长时间没有目标表数据但 binlog 持续推进时，stream checkpoint 会记录最近安全消费位点，避免重启后从很旧的表级 checkpoint 重新扫描大量无效 binlog。SQLite WAL 模式会在 checkpoint 文件旁产生 `-wal` 和 `-shm` 文件。状态页和 `/status` 会展示 checkpoint 后台任务的存活、dirty 数量、最近成功/失败和最后持久化位点。

开启 `sync_stored_procedure` 后，MySQL/Dameng sink 初始化时会按源库到目标库的路由同步 `PROCEDURE` 和 `FUNCTION`。目标库已有同名对象且 `overwrite_stored_procedure: false` 时会跳过；设为 `true` 时，MySQL 目标端会先 `DROP PROCEDURE/FUNCTION IF EXISTS` 再重建，Dameng 目标端会使用 `CREATE OR REPLACE` 覆盖。同步前会从 `SHOW CREATE PROCEDURE/FUNCTION` 结果里去掉 `DEFINER=\`user\`@\`host\``，避免把源库用户带到目标库。MySQL → Dameng 会做基础 DMSQL 转换，复杂 MySQL 专有语法可能导致初始化失败并输出对象名和错误原因。

`sync_stored_view` 默认开启。MySQL/Dameng sink 会在目标表结构准备好后读取源库 `SHOW CREATE VIEW` 结果，去掉 `DEFINER`，按库/schema 路由创建目标视图；目标库已有同名视图时跳过。MySQL → Dameng 会做基础 DMSQL 转换，若视图依赖其他视图会自动重试，最终仍失败时会输出视图名、目标 schema 和 SQL 摘要。

### MySQL → MeiliSearch 示例

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

### MySQL → 达梦示例

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

auto_create_database: true  # 达梦：自动创建目标 schema，不是物理数据库
auto_create_table: true
auto_add_column: true
sync_stored_view: true
random_check_data_after_init: false
random_check_data_after_init_batch_size_min: 10
```

达梦是单库多模式模型，`sink_config.database`/`sink_config.schema` 会作为目标 schema 使用；开启 `auto_create_database` 时会执行 `CREATE SCHEMA`，然后切换到该 schema 再自动建表/补列。多库模式下，`database_route[].sink` 表示目标 schema，写入、建表、补列和 `IDENTITY_INSERT` 状态都会按目标 schema 隔离。`sink_config.init_insert_batch_rows` 控制初始化数据写入时每条多行 `INSERT` 合并的行数，默认 `16`；批量写失败会退回逐行写入。开启 `random_check_data_after_init` 时，每次启动随机验证模式都会先覆盖写入 `/opt/fxm/datacheck-resule.log`，校验开始后再次覆盖并追加表级结果和字段差异。

### MySQL → Print 示例

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

## 插件配置

### ColumnIn：按列值过滤

当指定列的值在 `values` 中时，事件继续向下游传递；否则事件被过滤。

```yaml
plugins:
  - plugin_type: ColumnIn
    config:
      columns: is_deleted,is_delete
      values: 0,1
```

### Plus：指定列数值偏移

对 `表名.列名` 指定的列加上固定整数，适合 ID 偏移等场景。

```yaml
plugins:
  - plugin_type: Plus
    config:
      columns: orders.id,order_events.event_id
      plus: 10000000000
```

### DatabaseSplit：数据库拆分插件（V1 可用）

`DatabaseSplit` 用于“分库场景下，将单个分库中的一部分业务数据拆分到另一个数据库”的迁移工作。它不是替代 CDC 同步链路，而是在现有 Source、Sink、过滤插件和 Actix Web UI 之上增加拆分任务管理能力。

设计目标：

- 通过插件显式进入数据库拆分模式。
- 复用 `ColumnIn` 等过滤插件完成项目、租户、组织等业务维度的数据筛选。
- 在 Actix Web 中加载拆分任务页面，展示同步配置、任务状态、清理检查项。
- 将“数据同步”“切换准备”“清理 SQL 生成”拆成独立步骤，避免一次性自动执行高风险操作。
- 默认不要求源库账号具备 `DELETE` 权限；清理阶段优先生成 SQL 或统计待清理数量。

V1 已实现项：

- 通过 `plugins` 加载 `DatabaseSplit` 即可进入拆分模式（控制型插件，不会进入事件处理链）。
- 拆分任务页面（`/split`）与状态接口（`/api/split/status`）已可用。
- `dry-run` 计数与 `DELETE` SQL 生成接口（仅生成 SQL，不执行）已可用。
- 生成的 SQL 会基于 sink 中已同步数据的主键打包导出。

建议用法示例：

```yaml
source_type: MySQL
sink_type: MySQL
source_config:
  - host: 127.0.0.1
    port: 3306
    username: cdc_user
    password: cdc_password
    database: source_shard_01
    table_name: "*"
    except_table_name_prefix: "tmp_,dws_"
    server_id: 10010

sink_config:
  - host: 127.0.0.1
    port: 3306
    username: sink_user
    password: sink_password
    database: target_shard_08

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

计划中的页面能力：

- 拆分任务概览：源库、目标库、过滤条件、checkpoint、同步状态。
- 同步检查：展示全量同步和增量追平情况，作为人工切换前参考（部分已在页面显示进度）。
- 清理 dry-run：只统计各表命中数量，不直接删除数据。
- 清理 SQL 生成：支持按批次生成可审查 `DELETE` SQL；执行仍需用户自助执行。
- 操作记录：记录 dry-run、SQL 生成、确认口令、执行人备注和时间（计划中，当前版本未持久化）。

清理阶段默认策略：

- 默认 `dry-run`，只做数量统计。
- 如需生成删除 SQL，需要二次确认，并输入配置中的确认口令。
- 工具不默认执行 `DELETE`，因为 CDC 账号通常只需要 `SELECT`、`REPLICATION SLAVE`、`REPLICATION CLIENT` 权限。
- 如果未来支持执行删除，也应作为显式可选能力，并要求单独配置具备删除权限的账号。

后续可扩展能力：

- `Command` 类插件：用于在页面中登记或触发外部命令，例如调用配置中心接口、执行脚本、发送通知等。
- 配置切换建议优先通过外部命令或人工流程完成，避免将特定 Spring、Docker 或部署平台逻辑固化到 CDC 主流程。

## 监控 UI 与 Metrics

当 `enable_ui: true` 时，程序会启动 Actix Web 服务。可通过 `ui_bind`、`ui_port` 配置监听地址和端口；未配置端口时默认监听 `18088`，也可用 `UI_PORT` 或 `PORT` 环境变量覆盖端口。

如果通过 Nginx 子路径转发，例如 `/rust_cdc_hub/`，前端页面会使用相对路径访问 `status`、`metrics`、`health` 等接口，可配合 `rewrite` 将子路径转发到程序根路径。

常用端点：

| 端点 | 说明 |
| --- | --- |
| `/` | HTML 状态页。 |
| `/health` | 健康检查。 |
| `/status` | JSON 状态、配置摘要、flush 时间、重启次数等。 |
| `/metrics` | Prometheus metrics。 |

## Docker

构建镜像：

```sh
docker build --network host -t rust_cdc_hub:0.0.1 -f ./debian.dockerfile .
```

运行镜像：

```sh
docker run --name rust_cdc_hub --rm -it \
  -e CONFIG_PATH=/config.yaml \
  -v /path/to/config.yaml:/config.yaml \
  -v /path/to/logs:/app/logs \
  rust_cdc_hub:0.0.1
```

如果未启用 `log_file`，日志仍只输出到 console，`/app/logs` 挂载不是必需的。

本地测试或远端部署需要连接信息时，可以在仓库根目录维护一个不提交 Git 的 `.env`。先让环境变量生效，再执行变量化命令；例如：

```sh
set -a
source .env
set +a

cd "$CDC_PROJECT_DIR" \
  && docker build --network host -t "$CDC_IMAGE" -f "$CDC_DOCKERFILE" . \
  && docker save -o "$CDC_ARCHIVE" "$CDC_IMAGE" \
  && scp "$CDC_ARCHIVE" "$CDC_REMOTE:$CDC_REMOTE_ARCHIVE" \
  && rm -f "$CDC_ARCHIVE" \
  && ssh "$CDC_REMOTE" docker load -i "$CDC_REMOTE_ARCHIVE" \
  && ssh "$CDC_REMOTE" docker rm -f "$CDC_CONTAINER" \
  && ssh "$CDC_REMOTE" docker run -d --name "$CDC_CONTAINER" --network host \
    -v "$CDC_REMOTE_ROOT:$CDC_REMOTE_ROOT" \
    -v "$CDC_CHECKPOINT_DIR:/checkpoint" \
    -e CONFIG_PATH="$CDC_CONFIG_PATH" \
    -e TZ="$CDC_TZ" \
    "$CDC_IMAGE" "$CDC_APP_ARGS" \
  && ssh "$CDC_REMOTE" docker logs -f --tail=2000 "$CDC_CONTAINER"
```

同一个 `.env` 也可以放置 `MONGODB_URI` 等测试连接串；文档和示例不要写入真实密码。

也可以直接挂载本地编译产物到基础镜像中运行：

```sh
docker run --name rust_cdc_hub --rm -it \
  -e CONFIG_PATH=/config.yaml \
  -v /path/to/config.yaml:/config.yaml \
  -v "$(pwd)/target/release:/app" \
  debian:stable-20251117 \
  /app/rust_cdc_hub
```

## 开发指南

常用命令：

```sh
# 调试构建
cargo build --verbose

# Release 构建
cargo build -r

# 运行全部测试
cargo test --verbose

# 运行单个测试
cargo test test_name -- --nocapture

# 运行指定 crate 测试
cargo test -p common --verbose
```

新增 Source 或 Sink 的步骤：

1. 在 `source/` 或 `sink/` 下创建新 crate。
2. 实现 `common` 中的 `Source` 或 `Sink` trait。
3. 将新 crate 加入 workspace `Cargo.toml`。
4. 在 `source/src/lib.rs` 或 `sink/src/lib.rs` 的 factory 中注册新类型。

## Roadmap

- [x] MySQL Source
- [x] MySQL / StarRocks / MeiliSearch / Print Sink
- [x] checkpoint 断点续传
- [x] 自动建库、建表、加字段
- [x] 插件系统
- [x] 内置 UI 与 Prometheus metrics
- [x] 表/库 include/exclude 能力
- [x] MySQL 字符串主键数据同步：支持单列 `char`/`varchar` 主键表参与初始化和 CDC
- [ ] MySQL 无主键表数据同步：评估基于 `binlog_row_image=FULL` 的初始化和增量应用策略
- [x] 数据库拆分插件：通过插件进入拆分模式，并在 Actix Web 中加载拆分任务管理页面（V1）
- [x] 清理 dry-run 与删除 SQL 生成：默认只统计数量，生成可审查 SQL，不默认执行删除
- [ ] 拆分任务状态与操作记录：记录同步检查、dry-run、SQL 生成、确认信息和人工备注（未持久化）
- [ ] 通用命令插件：登记或触发外部命令，用于配置切换、服务重启、通知等可选流程
- [ ] 达梦数据库 Sink 支持：优先实现 MySQL CDC 写入达梦，覆盖连接、类型映射、DDL 兼容和同步验证
- [ ] 达梦数据库 Source 支持：支持达梦作为数据源端，覆盖变更捕获、表结构读取、类型映射和断点续传
- [ ] MongoDB 支持：评估并实现 MongoDB 作为 CDC 源端或目标端，覆盖变更捕获、文档映射和断点续传
- [ ] 更完整的 DDL 同步（MODIFY/DROP/RENAME 等）
- [ ] 多目标 fan-out 或按表路由
- [ ] 失败旁路与重放（DLQ、错误分类、指数退避）
- [ ] Docker Compose E2E 集成测试
- [x] 日志文件化：支持将运行日志写入文件，并配置日志目录、滚动策略、压缩和保留周期
- [ ] TLS、日志脱敏、限流/背压、热重载配置

## 注意事项

- `server_id` 必须唯一，否则可能与 MySQL 集群或其他 CDC 任务冲突。
- 首次全量同步和 Binlog 消费期间，请确认源库 Binlog 保留时间足够长。
- 自动建库建表/字段变更功能会修改目标端 Schema，生产环境建议先在测试环境验证。
- `CONFIG_PATH` 指向的配置文件中包含密码等敏感信息，请妥善管理权限。

## License

本项目使用 Apache License 2.0，详见 [LICENSE](LICENSE)。
