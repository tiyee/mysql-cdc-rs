# pikachu

[English](README.md) | 简体中文

基于 `mysql-canal` 的 MySQL CDC 命令行工具。`pikachu` 包提供 `pikachu-cdc` 可执行文件及默认 JSON Lines 事务处理器。

从 workspace 根目录构建和运行：

```sh
cargo build --release --locked -p pikachu
cp crates/pikachu/examples/config/multi-source.toml cdc.toml
export MYSQL_EAST_URL='mysql://cdc:password@127.0.0.1:3306'
export MYSQL_WEST_URL='mysql://cdc:password@127.0.0.1:3307'
./target/release/pikachu-cdc check cdc.toml
./target/release/pikachu-cdc run cdc.toml > changes.jsonl
```

运行前将 `[[sources.tables]]` 改为需要监控的精确库名、表名。单个来源也使用 `[[sources]]`；来源 ID 和 state 路径必须不同，state 的父目录须已存在。配置中的相对路径以 TOML 文件所在目录为基准。

| 命令 | 行为 |
| --- | --- |
| `pikachu-cdc check CONFIG.toml` | 校验配置、服务器能力及表 schema，不消费事件 |
| `pikachu-cdc init CONFIG.toml` | 保存初始 schema 和当前提交位点；已有 state 文件时报错 |
| `pikachu-cdc run CONFIG.toml` | 初始化缺失的 state，再从保存的位点持续消费 |
| `pikachu-cdc run CONFIG.toml --once` | 消费当前可用事件后退出 |
| `pikachu-cdc replay STATE.json BINLOG_FILE` | 回放一个本地 binlog，不修改 state |
| `pikachu-cdc --help` | 打印命令用法 |

首次 `init` 或 `run` 在短暂全局读锁内获取当前位点，不做全量数据快照；后续使用保存的历史 schema 与来源身份恢复。必须保留各来源检查点所需的 binlog。

[配置示例](examples/config/multi-source.toml) 使用具名来源，各自维护凭据、白名单和检查点。变更包含来源、库名、表名、操作、精确主键和变动字段名。仅在事务提交后输出，回滚丢弃缓冲。输出失败会停止消费；检查点持久化前发生崩溃可能导致重放。

使用检查点保存的历史 schema 回放本地 binlog：

```sh
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001
```

文件名须与 `state.file` 一致，文件须保留开头的 FORMAT_DESCRIPTION；从保存的事件边界开始回放，截断或未完成事务会报错。每次命令只读取一个来源的一个文件。

消费错误写入 stderr，并返回非零退出码；变更记录使用 stdout。默认处理器在保存检查点前 flush stdout，但这不代表下游持久化确认，消费方须容忍重放。任何非 BEGIN/COMMIT/ROLLBACK 的 QUERY 都以 `Error::Statement` 停止 CDC，DDL 不会自动刷新 schema 并继续。

1.0.0 在 crates.io 可用后，可使用 `cargo install pikachu --version 1.0.0 --locked` 安装，安装后的命令名为 `pikachu-cdc`。嵌入式 CDC 与原始复制可统一依赖 `mysql-cdc-rs`（`mysql_cdc_rs`）；`mysql-canal` 和 `mysql-replication` 仍可单独使用。配置步骤、输出契约、兼容矩阵和限制见[项目说明](https://github.com/tiyee/mysql-cdc-rs/blob/master/README.zh-CN.md)。

运行 CLI 回归测试：`cargo test --locked -p pikachu`。
