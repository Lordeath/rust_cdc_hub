# AGENTS.md

本文件只记录这个仓库的长期偏好。通用 Rust、Cargo、Git 知识不要写进来。

## 必须优先遵守

- 回复和项目内说明优先使用中文。
- 不要提交临时 demo 配置、真实密码、连接信息；示例或临时配置放到 `/tmp`。
- 不要回滚用户已有改动；改动前后用 `git status` 确认范围。
- 修改代码前先看现有实现和测试，按当前风格小范围改。

## Git 操作

- 我完成任何改动后，包括文档改动，都要在验证通过后自动 commit 并 push，不要等用户再次要求。
- 提交前必须看 `git status`，只暂存本次任务相关文件；不要把用户其他改动混进 commit。
- 代码改动至少跑 `cargo check` 和相关 `cargo test`；触碰具体 crate 时优先跑对应 `cargo test -p <crate>`。
- 文档-only 变更至少检查 diff 和 `git status`；如果同一批里包含代码改动，仍然按代码改动验证。
- 检查失败、无法验证或工作区里有不确定的无关改动时，不要 commit/push；需要说明失败命令和原因。
- commit message 要简短，说明具体变更。

## 项目要点

- `rust_cdc_hub` 是 Rust CDC 同步工具，当前重点是 MySQL source 和各类 sink，特别是 Dameng sink。
- 运行配置由 `CONFIG_PATH` 指向 YAML/JSON。
- Dameng 里的 `auto_create_database` 按 schema 处理；建 schema/table/字段时注意达梦大小写、引号、字符长度语义。
- 表过滤配置在 `source_config`：`table_name: "*"` 表示全表，排除优先看 `exclude_table_regex`、`except_table_name_prefix`。
- 新增 source/sink 时按现有 workspace crate、factory 和 trait 模式接入。

## 验证偏好

- 小的 Rust 逻辑改动：跑 `cargo check` 和相关 crate 单测。
- 数据库同步或 DDL 行为：能本机复现时用现有 Docker 容器验证；不能复现要说明缺少的环境。
- 不为文档或注释变更强行跑完整测试。
