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
| `sync_foreign_key_tables` | 否 | 是否在 `table_name: "*"` 自动发现时纳入外键相关表，并在 MySQL/Dameng 目标端初始化后补外键约束，默认 `true`。设为 `false` 时保留旧行为：排除有外键依赖或被外键引用的表。 |
| `sync_no_pk_table_schema` | 否 | 全表发现时是否额外同步无主键表结构，默认 `false`；这些表只建结构，不做初始化数据和 CDC 同步；StarRocks 目标端会跳过并告警。 |
| `sync_stored_procedure` | 否 | MySQL → MySQL 时是否同步源库存储过程，默认 `false`；也兼容 `sync_stored_procedures`。 |
| `overwrite_stored_procedure` | 否 | 同步存储过程时，目标库已存在同名过程是否先删除再重建，默认 `false`；也兼容 `overwrite_stored_procedures`。 |
| `plugins` | 否 | 插件配置列表。 |
| `source_batch_size` | 否 | Source 批量读取/处理大小。 |
| `sink_batch_size` | 否 | Sink 批量写入大小。 |
| `checkpoint_file_path` | 否 | checkpoint 文件路径。 |
| `log_level` | 否 | 日志级别，如 `debug`、`info`。 |
| `enable_ui` | 否 | 是否启用监控 UI，默认 `true`。 |
| `ui_bind` | 否 | UI 监听地址。 |
| `ui_port` | 否 | UI 监听端口，默认 `18088`。 |

### MySQL Source 配置

| 字段 | 说明 |
| --- | --- |
| `host` / `port` | MySQL 地址和端口。 |
| `username` / `password` | MySQL 账号密码。 |
| `database` | 源库名。 |
| `table_name` | 表名；可填单表、逗号分隔多表或 `"*"` 表示按规则自动发现表。`"*"` 默认筛选单一整数主键的 `BASE TABLE`，包括外键相关表；当顶层 `sync_no_pk_table_schema: true` 时，会额外纳入无主键 `BASE TABLE`，仅同步表结构。 |
| `except_table_name_prefix` | 排除指定前缀的表，多个前缀用逗号分隔。 |
| `server_id` | Binlog replication server id，必须与 MySQL 集群中其他 server id 不重复。 |
| `ssl_mode` | MySQL SSL模式，透传为SQLx连接参数 `ssl-mode`。可选：`disabled`、`preferred`、`required`、`verify_ca`、`verify_identity`；默认 `disabled`。如果源库/目标库必须使用SSL，请显式设置为 `required` 或证书校验模式。 |
| `statement_cache_capacity` | MySQL prepared statement 缓存容量，透传为 SQLx 连接参数 `statement-cache-capacity`；设为 `"0"` 可关闭缓存。MySQL 报 `Can't create more than max_prepared_stmt_count statements` 时，可先断开旧连接或临时调大 `max_prepared_stmt_count` 后重启同步进程。 |

主键列会从 MySQL 表结构自动识别；参与数据同步的表需要单一整数主键（`tinyint`、`smallint`、`mediumint`、`int`、`bigint` 及其 unsigned 变体）。配置项 `pk_column` 已弃用，配置文件中出现该字段会在启动解析时报错。MeiliSearch 目标端的 `meili_table_pk` 仍需单独配置为索引主键字段。

外键表默认参与同步。MySQL/Dameng 目标端会先创建基础表并完成初始化数据写入，再补外键约束；如果显式只同步部分表，缺少父表或子表的外键约束会跳过并记录告警，不会自动扩展同步表清单。

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
overwrite_stored_procedure: false
log_level: info
enable_ui: true
ui_port: 8080
```

开启 `sync_stored_procedure` 后，MySQL sink 初始化时会按源库到目标库的路由同步 `PROCEDURE`。目标库已有同名过程且 `overwrite_stored_procedure: false` 时会跳过；设为 `true` 时会执行 `DROP PROCEDURE IF EXISTS` 后重建。同步前会从 `SHOW CREATE PROCEDURE` 结果里去掉 `DEFINER=\`user\`@\`host\``，避免把源库用户带到目标库。

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

auto_create_database: true  # 达梦：自动创建目标 schema，不是物理数据库
auto_create_table: true
auto_add_column: true
```

达梦是单库多模式模型，`sink_config.database`/`sink_config.schema` 会作为目标 schema 使用；开启 `auto_create_database` 时会执行 `CREATE SCHEMA`，然后切换到该 schema 再自动建表/补列。多库模式下，`database_route[].sink` 表示目标 schema，写入、建表、补列和 `IDENTITY_INSERT` 状态都会按目标 schema 隔离。

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
  rust_cdc_hub:0.0.1
```

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
- [x] 数据库拆分插件：通过插件进入拆分模式，并在 Actix Web 中加载拆分任务管理页面（V1）
- [x] 清理 dry-run 与删除 SQL 生成：默认只统计数量，生成可审查 SQL，不默认执行删除
- [ ] 拆分任务状态与操作记录：记录同步检查、dry-run、SQL 生成、确认信息和人工备注（未持久化）
- [ ] 通用命令插件：登记或触发外部命令，用于配置切换、服务重启、通知等可选流程
- [ ] 达梦数据库 Sink 支持：优先实现 MySQL CDC 写入达梦，覆盖连接、类型映射、DDL 兼容和同步验证
- [ ] 更完整的 DDL 同步（MODIFY/DROP/RENAME 等）
- [ ] 多目标 fan-out 或按表路由
- [ ] 失败旁路与重放（DLQ、错误分类、指数退避）
- [ ] Docker Compose E2E 集成测试
- [ ] TLS、日志脱敏、限流/背压、热重载配置

## 注意事项

- `server_id` 必须唯一，否则可能与 MySQL 集群或其他 CDC 任务冲突。
- 首次全量同步和 Binlog 消费期间，请确认源库 Binlog 保留时间足够长。
- 自动建库建表/字段变更功能会修改目标端 Schema，生产环境建议先在测试环境验证。
- `CONFIG_PATH` 指向的配置文件中包含密码等敏感信息，请妥善管理权限。

## License

本项目使用 Apache License 2.0，详见 [LICENSE](LICENSE)。
