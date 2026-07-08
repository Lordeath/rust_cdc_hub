# MySQL 同步到达梦迁移注意事项

本文件记录本仓库在实现 MySQL source 到 Dameng sink 时需要长期遵守的迁移规则。通用数据库知识不要扩写到这里；只记录会影响本项目代码和验证的差异点。

参考资料：

- 达梦官方文档《从 MySQL 移植到 DM》：https://eco.dameng.com/document/dm/zh-cn/start/mysql_dm
- 达梦官方 FAQ《从 MySQL 迁移到 DM》：https://eco.dameng.com/document/dm/zh-cn/faq/faq-mysql-dm8-migrate.html

## 字符长度语义

- MySQL 5.0.3 以后，`CHAR(n)`、`VARCHAR(n)` 的 `n` 按字符数理解；达梦默认更容易按字节长度踩坑。
- 达梦 FAQ 提到：MySQL `varchar(1)` 可以存 1 个汉字；达梦按字节计时，GB18030 下一个汉字需要 2 字节，UTF-8 下一个汉字需要 3 字节。
- 本项目把 MySQL `char(n)`、`varchar(n)` 映射为达梦 `CHAR(n CHAR)`、`VARCHAR(n CHAR)`，不要退回字节语义。
- 不要默认把 `varchar(n)` 放大为 `varchar(n * factor)`：`VARCHAR(n CHAR)` 对齐的是 MySQL 的字符数语义，`ID_` 这类主键/索引列也不能为了绕过长度怀疑改成 `CLOB`。
- 已存在的达梦列也要检查：如果目标列是字节语义，或者目标字符长度小于源端字符长度，必须在 `auto_modify_column=true` 时修改。
- MySQL `text/json/enum/set` 等宽文本应映射为达梦 `CLOB`。如果目标端已有较窄的 `VARCHAR/CHAR`，也必须自动改为 `CLOB`，否则会出现 `-6169: 列[...]长度超出定义`。
- 不要为了绕过长度错误启用截断容错；同步工具必须保证不丢数据。

## 字符串函数长度语义

- MySQL `LENGTH(str)` 返回字节数；达梦 `LENGTH(str)` 返回字符数。迁移 routine/view 时，MySQL `LENGTH(...)` 必须转成达梦 `LENGTHB(...)`，避免中文等多字节字符场景结果变小。
- MySQL `CHAR_LENGTH(str)` 和达梦字符数语义一致，保留 `CHAR_LENGTH(...)`。

## 对象和 schema 映射

- MySQL 是单实例多库，达梦通常用 schema 对应 MySQL database；本项目通过 `multi_mode.database_route` 做 source database 到 target schema 映射。
- 达梦大小写、引号会影响对象定位。自动建 schema/table/column 时继续使用当前 quote 策略，避免大小写漂移。
- MySQL 允许某些对象名组合在实际库中共存；达梦对象命名空间更容易冲突。本项目遇到非 routine 对象占用同名 routine 时，使用稳定后缀创建，例如 `_procedure`、`_function`。
- MySQL `GENERATED ALWAYS` / `VIRTUAL GENERATED` / `STORED GENERATED` 列不在达梦侧自动建普通列；source 侧只用生成列位置做 binlog 列对齐，Dameng sink 的 DDL/DML/随机校验都要跳过这些列。

## 类型和默认值

- MySQL `AUTO_INCREMENT` 只能映射到达梦支持 identity 的数值类型；遇到显式写自增列时，需要使用 `SET IDENTITY_INSERT ... ON`。
- MySQL `timestamp/datetime` 的 `CURRENT_TIMESTAMP` 可以保留；`ON UPDATE CURRENT_TIMESTAMP` 不能照搬为列定义。
- MySQL 零日期、非法日期、宽松数值转换等行为与达梦不同，不要依赖达梦兼容参数吞错；同步前后应保留可诊断错误。
- MySQL `blob/binary` 映射为 `BLOB`，文本类大字段映射为 `CLOB`，运行时参数绑定也要区分文本和字节。
- 自动建表后的表注释同步为 `COMMENT ON TABLE`，列注释同步为 `COMMENT ON COLUMN`；不要把 MySQL 建表内 `COMMENT` 原样拼进达梦 `CREATE TABLE`。
- MySQL 普通/唯一 BTREE 二级索引可以同步为达梦 `CREATE [UNIQUE] INDEX`。前缀索引、表达式索引、全文/空间等非 BTREE 索引，以及映射到达梦 `CLOB/BLOB` 的列索引必须跳过并记录日志。

## 存储过程和函数

- 达梦官方 FAQ 说明 MySQL 到 DM 的语法兼容并不完整，表、视图、游标、系统函数、存储过程都可能需要改写。
- 本项目的 MySQL -> Dameng routine 转换集中在 `mysql_to_dameng` crate；新增语法兼容时必须补单测，并用真实远端同步日志验证。
- MySQL routine 中常见需要转换的语法包括：`DATE_FORMAT`、`DATE_ADD/SUB INTERVAL`、`IFNULL`、`LIMIT`、`UPDATE JOIN`、`REGEXP`、临时表/视图 DDL、`DECLARE HANDLER`、`LEAVE/LOOP`、`PREPARE/EXECUTE`、`SIGNAL SQLSTATE` 等。
- `CONVERT(expr, type)` 要转成 `CAST(expr AS type)`，`CAST(expr AS UNSIGNED)` 要转成达梦可识别的数值类型；`PERIOD_DIFF`、`MAKEDATE` 需要显式改写为达梦表达式。
- `FORCE/USE/IGNORE INDEX` 在 routine/view 转换中默认移除，不转达梦 hint，避免不同优化器 hint 语义造成误导。
- `ON DUPLICATE KEY UPDATE`、`REPLACE INTO`、`INSERT IGNORE`、`UPDATE ... ORDER BY ... LIMIT`、一次 `UPDATE JOIN` 同时更新多张表字段等非等价 SQL 不自动近似转换；必须返回带差异编号、对象名和 SQL 摘要的错误。
- 验证时不能只看进程启动成功；需要确认没有 `sync stored routines failed`、`create Dameng stored routine failed`、`语法分析出错`、`object name exists, skip stored routine` 等日志。

## 视图

- MySQL -> Dameng 独立视图转换也集中在 `mysql_to_dameng` crate。同步前从 `SHOW CREATE VIEW` 去掉 `DEFINER`，目标端已有同名视图时跳过；源库限定名必须按 `database_route` 改成目标 schema。
- 视图可能依赖其他视图或函数；同步失败时需要保留对象名、目标 schema 和 SQL 摘要，不能只吞错跳过。

## 验证要求

- 触碰 MySQL -> Dameng DDL、类型映射、routine 转换时，至少跑：
  - `cargo test -p mysql_to_dameng -- --nocapture`
  - `cargo test -p sink_dameng -- --nocapture`
  - `cargo check`
  - `docker build --network host -t fangxiangmin/rust_cdc_hub:0.0.3 -f "debian.dockerfile" .`
- 能访问现有远端环境时，还要按项目约定部署镜像并观察日志，直到进入 CDC 后没有新的达梦 DDL/DML 错误。
