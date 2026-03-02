# 如何编译
```sh
git clone https://github.com/Lordeath/rust_cdc_hub.git
cd rust_cdc_hub
cargo b -r
```

# 如何使用
## 源码运行
```sh
# 下载代码
git clone https://github.com/Lordeath/rust_cdc_hub.git
cd rust_cdc_hub
# 设置环境变量
export CONFIG_PATH=/mnt/d/project/meilisearch/config_example_meili.yaml
# 以release的方式运行
cargo run -r
```

## 直接运行
使用编译出来的程序运行
```sh
export CONFIG_PATH=/mnt/d/project/meilisearch/config_example_meili.yaml
chmod +x ./rust_cdc_hub
./rust_cdc_hub

```
## docker运行
```sh
# 用rust镜像
docker run --name rust_cdc_hub --rm -it -e CONFIG_PATH=/config_example_meili.yaml -v /mnt/d/project/meilisearch/config_example_meili.yaml:/config_example_meili.yaml -v /mnt/d/project/rust_cdc_hub/target/release:/app rust:latest /app/rust_cdc_hub
# 用debian镜像（更加轻便）
docker run --name rust_cdc_hub --rm -it -e CONFIG_PATH=/config_example_meili.yaml -v /mnt/d/project/meilisearch/config_example_meili.yaml:/config_example_meili.yaml -v /mnt/d/project/rust_cdc_hub/target/release:/app debian:stable-20251117 /app/rust_cdc_hub

```

## 自己打包成docker image

```sh
# 默认使用 debian:stable-20251117
# 这里把 XXXXXXXXXXXXXX 替换成你的镜像名称
docker build --network host -t XXXXXXXXXXXXXX:0.0.1 -f "./debian.dockerfile" .

```

## 使用docker镜像运行
```sh
# 使用镜像
docker run --name rust_cdc_hub --rm -it -e CONFIG_PATH=/config_example_meili.yaml -v /mnt/d/project/meilisearch/config_example_meili.yaml:/config_example_meili.yaml fangxiangmin/rust_cdc_hub:0.0.1

```



# 配置文件示例
## Mysql 到 Meilisearch
```yaml
source_type: MySQL
sink_type: MeiliSearch
source_config:
  - host: 192.168.1.103
    port: 3306
    username: root
    password: XXXXXXXXXXX
    database: XXX
    table_name: table_name_XXXXXXXXXX
    server_id: 10000
    pk_column: id

sink_config:
  - meili_url: http://192.168.1.103:17700
    meili_master_key: XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
    table_name: table_name_XXXXXXXXXX
    meili_table_pk: id

```
## Mysql 到 Mysql
```yaml
source_type: MySQL
sink_type: MySQL
source_config:
  - host: 192.168.1.103
    port: 3306
    username: root
    password: XXXXXXXXXXX
    database: XXX
    table_name: table_name_XXXXXXXXXX
    server_id: 10000
    pk_column: id

sink_config:
  - host: 192.168.1.104
    port: 3306
    username: root
    password: XXXXXXXXXXX
    database: test
    table_name: table_name_XXXXXXXXXX
    pk_column: id

```

## Mysql 到 控制台打印

# 想要实现的功能
- [x] mysql数据源
- [x] meilisearch 的 sink
- [x] mysql 的 sink
- [X] starrocks 的 sink
- [x] starrocks的sink时，自动建表，自动建库，自动加字段
- [x] 断点续传，挂掉之后自动重启，然后继续执行之前的作业
- [x] 重试的时候，不drop掉对象，防止重试的时候对象被drop。等到成功之后再drop对象
- [x] (mysql sink) auto create database
- [x] (mysql sink) auto create table
- [x] (mysql sink) auto add column
- [x] (mysql sink) all table sync
- [X] (mysql sink) 忽略这样的字段`fullPath` varchar(120) CHARACTER SET utf8mb3 COLLATE utf8mb3_general_ci GENERATED ALWAYS AS (concat(`path`,`HouseId`,_utf8mb3'/')) STORED,
- [x] (mysql source) 获取binlog的时候，指定获取的binlog位置，防止同步时间过长导致的数据丢失
- [x] (mysql sink) 解决pool超时的问题，自行进行连接池的管理，防止连接池超时



# 后续想要做的
- [x] 可视化界面
- [x] 指标监控：Prometheus metrics（吞吐、延迟、失败、重试、checkpoint位点）
- [x] 一致性快照：初始化全量与binlog消费对齐同一位点/GTID
- [ ] DDL 同步：捕获并处理常见表结构变更（ADD/MODIFY/DROP/RENAME）
- [x] 表/库选择增强：include/exclude（前缀、正则、黑白名单）
  - [ ] 动态加载配置
- [x] 多数据源完善：get_table_info 支持汇总多 source_config 的表信息
- [ ] 多目标写入：支持一个 source fan-out 到多个 sink 或按表路由
- [x] Checkpoint 存储可插拔：文件/SQLite/Redis（已实现 Manager 接口与文件存储）
- [ ] 失败旁路与重放：DLQ、按表隔离、指数退避与错误分类
- [ ] E2E 集成测试：docker compose + 自动验数（MySQL→MySQL/StarRocks/Meili）
- [ ] 安全与运维：TLS/证书、日志脱敏、限流/背压、热重载配置
