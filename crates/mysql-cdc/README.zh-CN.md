# mysql-cdc-rs

[English](README.md) | 简体中文

只需一个依赖即可使用 MySQL CDC 与复制功能。本聚合包重导出 `mysql-canal` 和 `mysql-replication`，保留原有类型、生命周期和行为。

处理分层为 `pikachu → mysql-canal → mysql-replication`，本聚合包提供两个库的入口，不增加处理阶段。通过 `replication` 订阅 binlog、解析协议事件，也可用 `events::RowsDecoder` 按需检查原始稀疏行镜像；通过 `Cdc` 使用白名单转换、已提交事务回调和检查点恢复。事件流与边界见[架构说明](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.zh-CN.md)。

项目尚未发布。以下依赖使用本地源码，请将 `/path/to/mysql-cdc-rs` 替换为实际仓库路径。

```toml
[dependencies]
mysql-cdc-rs = { path = "/path/to/mysql-cdc-rs/crates/mysql-cdc" }
```

Rust 导入路径为 `mysql_cdc_rs`。CDC 类型和模块可直接从包根路径导入，也可通过 `canal` 访问；原始 binlog 事件和复制传输通过 `replication` 访问。根路径的 `Error`、`Result` 属于 CDC 层；原始协议操作使用 `replication::Error`、`replication::Result`。

`canal.toml` 使用[独立 Canal 配置](https://github.com/tiyee/mysql-cdc-rs/blob/master/crates/mysql-canal/examples/config/multi-source.toml)，从 `[[sources]]` 开始，各来源必须包含 `[sources.mysql-replication]`。

```rust,no_run
use mysql_cdc_rs::{Cdc, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let mut cdc = Cdc::new(AppConfig::read(Path::new("canal.toml"))?)?;
    cdc.add_handle(|transaction: CommittedTransaction<'_>| {
        for change in transaction.changes {
            println!("{} {:?} {:?}", transaction.source_id, change.operation, change.pk);
        }
        Ok(())
    });
    cdc.run()
}
```

协议事件可直接传给 CDC 转换器，无需包装或复制：

```rust
use mysql_cdc_rs::{
    Change, Result,
    canal::engine::converter::EventConverter,
    replication::BinlogEvent,
};

fn convert(converter: &mut EventConverter, event: &BinlogEvent<'_>) -> Result<()> {
    converter.convert_binlog(event, &mut |change: Change<'_>| {
        println!("{:?} {:?}", change.operation, change.pk);
        Ok(())
    })?;
    Ok(())
}
```

`EventConverter` 在处理 ROWS 时即回调行变更，早于事务提交，并返回描述控制边界的 `EventAction`。上例仅演示转换；需要交付已提交事务时，应使用 `Cdc`，或将每个事件交给 `Processor`。直接转换回调不负责事务缓冲、丢弃回滚变更或检查点持久化。转换行事件之前须先处理该文件的 FDE。

离线原始事件入口为 `mysql_cdc_rs::replication::file::BinlogFile`，在线复制入口为 `mysql_cdc_rs::replication::stream::ReplicationStream`。完整示例见 [listen.rs](examples/listen.rs) 和 [dump_file.rs](examples/dump_file.rs)。从 workspace 根目录运行：

```sh
cargo run -p mysql-cdc-rs --example cdc_listen -- canal.toml
cargo run -p mysql-cdc-rs --example replication_dump_file -- /path/to/binlog.000001
```

CDC 使用具名来源，各自维护 schema、白名单和检查点。仅交付已提交变更，包含来源、库表、操作、精确主键和变动字段名。回滚丢弃缓冲；失败停止消费，检查点持久化前崩溃可能导致重放。库不注册默认输出处理器；`pikachu` 包提供 `pikachu-cdc` CLI。

底层两个库仍可单独依赖。配置、输出契约、版本支持和限制见[项目说明](https://github.com/tiyee/mysql-cdc-rs/blob/master/README.zh-CN.md)、[CDC 库](https://docs.rs/mysql-canal)和[复制库](https://docs.rs/mysql-replication)。

[事件参考](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/events.zh-CN.md) · [开发指南](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/development.zh-CN.md) · [测试指南](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/testing.zh-CN.md)
