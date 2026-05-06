# rust_cdc_hub

[English](README.en.md) | 简体中文

`rust_cdc_hub` 是一个基于 Rust 的 CDC（Change Data Capture，变更数据捕获）同步工具。它读取 MySQL Binlog，将数据变更流式同步到 MySQL、StarRocks、MeiliSearch 或控制台打印等目标端，并内置断点续传、自动建库建表/加字段、插件处理、Prometheus 指标和轻量级监控 UI。

## 功能特性

- **MySQL Binlog 数据源**：支持单表、多表和全库表同步。
- **多种 Sink 目标端**：已支持 MySQL、StarRocks、MeiliSearch、Print。
- **断点续传**：基于 checkpoint 文件记录位点，进程重启后从上次位点继续消费。
- **自动 Schema 迁移**：支持自动建库、自动建表、自动加字段等能力（按 Sink 能力生效）。
- **插件系统**：内置列值过滤插件 `ColumnIn` 和数值偏移插件 `Plus`。
- **批量与重试**：支持 Source/Sink batch 配置，Sink flush 失败时自动重试。
- **内置监控 UI**：提供状态页、健康检查、配置摘要和 Prometheus metrics。
- **表选择能力**：支持 `table_name: "*"`、逗号分隔表名和前缀排除。

## 架构概览

```text
MySQL Binlog → MySQLSource → [Plugins] → Sink → MySQL / StarRocks / MeiliSearch / Print
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
| `sink_type` | 是 | Sink 类型：`MySQL`、`Starrocks`、`MeiliSearch`、`Print`。 |
| `source_config` | 是 | Source 连接与同步配置列表。 |
| `sink_config` | 是 | Sink 连接与写入配置列表。 |
| `auto_create_database` | 否 | 是否自动建库，默认 `true`。 |
| `auto_create_table` | 否 | 是否自动建表，默认 `true`。 |
| `auto_add_column` | 否 | 是否自动加字段。 |
| `auto_modify_column` | 否 | 是否自动修改字段。 |
| `plugins` | 否 | 插件配置列表。 |
| `source_batch_size` | 否 | Source 批量读取/处理大小。 |
| `sink_batch_size` | 否 | Sink 批量写入大小。 |
| `checkpoint_file_path` | 否 | checkpoint 文件路径。 |
| `log_level` | 否 | 日志级别，如 `debug`、`info`。 |
| `enable_ui` | 否 | 是否启用监控 UI，默认 `true`。 |
| `ui_bind` | 否 | UI 监听地址。 |
| `ui_port` | 否 | UI 监听端口。 |

### MySQL Source 配置

| 字段 | 说明 |
| --- | --- |
| `host` / `port` | MySQL 地址和端口。 |
| `username` / `password` | MySQL 账号密码。 |
| `database` | 源库名。 |
| `table_name` | 表名；可填单表、逗号分隔多表或 `"*"` 表示全部表。 |
| `except_table_name_prefix` | 排除指定前缀的表，多个前缀用逗号分隔。 |
| `server_id` | Binlog replication server id，必须与 MySQL 集群中其他 server id 不重复。 |
| `pk_column` | 主键列名。 |

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
    pk_column: id

sink_config:
  - meili_url: http://127.0.0.1:7700
    meili_master_key: your_master_key
    table_name: articles
    meili_table_pk: id
```

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
    pk_column: id

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

## 监控 UI 与 Metrics

当 `enable_ui: true` 时，程序会启动 Actix Web 服务。可通过 `ui_bind`、`ui_port` 配置监听地址和端口，也可用 `UI_PORT` 或 `PORT` 环境变量覆盖端口。

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

请以仓库中的许可证文件为准；如果仓库尚未声明许可证，请在生产使用或分发前补充许可证信息。
