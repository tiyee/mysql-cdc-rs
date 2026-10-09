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

[配置示例](examples/config/multi-source.toml) 使用具名来源，各自维护凭据、白名单和检查点。变更包含来源、库名、表名、操作、精确主键和变动字段名。仅在事务提交后输出，回滚丢弃缓冲。输出失败会停止消费；检查点持久化前发生崩溃可能导致重放。

使用检查点保存的历史 schema 回放本地 binlog：

```sh
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001
```

发布后可使用 `cargo install pikachu` 安装。嵌入式 CDC 与原始复制可统一依赖 `mysql-cdc-rs`（`mysql_cdc_rs`）；`mysql-canal` 和 `mysql-replication` 仍可单独使用。配置步骤、输出契约、兼容矩阵和限制见[项目说明](https://github.com/tiyee/mysql-cdc-rs/blob/main/README.zh-CN.md)。

运行 CLI 回归测试：`cargo test --locked -p pikachu`。
