# mysql-canal

[English](README.md) | 简体中文

mysql-cdc 的中间层，基于 `mysql-replication` 将 binlog 转为通用 CDC 变更，管理事务、历史 schema 和检查点。上层 `pikachu` 提供 CLI；本库不注册默认输出处理器。

## 监听已提交事务

通过本地源码集成时，使用以下依赖，请将 `/path/to/mysql-cdc-rs` 替换为实际仓库路径。

```toml
[dependencies]
mysql-canal = { path = "/path/to/mysql-cdc-rs/crates/mysql-canal" }
```

```rust,no_run
use mysql_canal::{Canal, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let mut canal = Canal::new(AppConfig::read(Path::new("canal.toml"))?)?;
    canal.add_handle(|event: CommittedTransaction<'_>| {
        for change in event.changes {
            println!("{} {:?} {:?}", event.source_id, change.operation, change.pk);
        }
        Ok(())
    });
    canal.run()
}
```

`canal.toml` 使用[独立配置示例](examples/config/multi-source.toml)，从 `[[sources]]` 开始，每个来源必须包含 `[sources.mysql-replication]`。Canal 仅增加来源插件、表白名单、事务预算及检查点配置；连接和行镜像设置复用复制库类型。Pikachu 将相同配置嵌入 `[mysql-canal]`。

每个来源独立维护连接、schema 和检查点。初次从当前位点开始，不做全量快照；恢复按保存的历史 schema 解码。

## 事件与处理器

`CommittedTransaction` 包含 `source_id`、提交 `checkpoint` 和有序 `changes`。每条变更包含表、操作、精确主键、可选旧主键及字段索引。库只解码主键，其他字段值按长度跳过。

处理器按注册顺序同步执行；所有处理器成功后才保存检查点。COMMIT/XID 前不交付，回滚丢弃；完全过滤的提交仍交付空切片。失败可能导致重放，处理器须幂等。运行前必须注册至少一个处理器，慢处理器会施加背压。

`check()` 校验上游及表结构，`initialize()` 创建初始检查点，`run_once()` 消费当前可用事件。

## 来源模式与泛型解码

`Canal::new(AppConfig)` 统一消费所有配置来源。各 `[sources.mysql-replication]` 分别设置 `row_image = "full"` 或 `"minimal"`，同一消费实例可以包含两种模式。适配器按来源和 FDE 构造 `Decoder<Full>` 或 `Decoder<Minimal>`，行解码使用静态分派。

直接使用解码器时，显式指定泛型模式：

```rust
use mysql_canal::{Decoder, Full, Result, TableMeta};

fn full_decoder(tables: Vec<TableMeta>) -> Result<Decoder<Full>> {
    Decoder::<Full>::new(tables)
}
```

MINIMAL 使用 `Decoder::<Minimal>::new(tables)`。两种模式交付相同的 `Change` / `CommittedTransaction` 字段；FULL 的 `changed` 包含全部 after-image 列，也可能包含值未改变的列。见[监听示例](examples/listen.rs)。

## 离线回放

```sh
cargo run -p mysql-canal --example replay -- STATE.json /path/to/binlog.000001 full
```

`engine::Replay` 按历史检查点回放，显式接收 `TransactionSink`，不更新 state、不连接 MySQL。示例从 workspace 根目录运行，见[replay.rs](examples/replay.rs)。

## 参考

- [Rust 字段与接口参考](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/event-api.zh-CN.md)
- [配置、恢复与模式切换](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/configuration.zh-CN.md)
- [CDC 字段与输出契约](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/events.zh-CN.md)
- [事件流与分层职责](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.zh-CN.md)
- [来源和版本适配器开发](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/development.zh-CN.md)
- [合成与实库测试](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/testing.zh-CN.md)
