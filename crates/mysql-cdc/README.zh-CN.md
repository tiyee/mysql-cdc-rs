# mysql-cdc-rs

[English](README.md) | 简体中文

只需一个依赖即可使用 MySQL CDC 与复制功能。本聚合包重导出 `mysql-canal` 和 `mysql-replication`，保留原有类型、生命周期和行为。

处理分层为 `pikachu → mysql-canal → mysql-replication`，本聚合包提供两个库的入口，不增加处理阶段。通过 `replication` 订阅 binlog、解析协议事件，也可用 `events::RowsDecoder<M>` 按需检查原始稀疏行镜像；通过 `canal::Canal` 使用白名单转换、已提交事务回调和检查点恢复。事件流与边界见[架构说明](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.zh-CN.md)。

通过本地源码集成时，使用以下依赖，请将 `/path/to/mysql-cdc-rs` 替换为实际仓库路径。

```toml
[dependencies]
mysql-cdc-rs = { path = "/path/to/mysql-cdc-rs/crates/mysql-cdc" }
```

Rust 导入路径为 `mysql_cdc_rs`，包根路径仅重导出 `canal` 和 `replication`。`canal::Canal` 管理消费生命周期，`canal::config::AppConfig` 定义配置，`canal::Error` / `canal::Result` 处理错误。原始读取器、复制流及其配置和错误归属 `replication`。聚合层没有独立的消费实例、配置或错误类型。

`canal.toml` 使用[独立 Canal 配置](https://github.com/tiyee/mysql-cdc-rs/blob/master/crates/mysql-canal/examples/config/multi-source.toml)，从 `[[sources]]` 开始，各来源必须包含 `[sources.mysql-replication]`。

```rust,no_run
use mysql_cdc_rs::canal::{Canal, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let mut canal = Canal::new(AppConfig::read(Path::new("canal.toml"))?)?;
    canal.add_handle(|transaction: CommittedTransaction<'_>| {
        for change in transaction.changes {
            println!(
                "{} {:?} {:?}",
                transaction.source_id, change.operation, change.pk
            );
        }
        Ok(())
    });
    canal.run()
}
```

协议事件可直接传给 CDC 转换器，无需包装或复制：

原始事件可用 `replication::BinlogEvent<Full>` / `BinlogEvent<Minimal>` 静态选择镜像模式，标记类型与 canal 共用。泛型 reader 自动关联 TABLE_MAP，行操作的 `fields()` 提供列定义与借用值；MINIMAL 的 `field()` 区分未记录与 SQL NULL，FULL 保证存在镜像包含全部列。缺失列名等可选元数据保持未知，Canal 变更通过 `fields` 提供日志 before/after 值及历史 SQL `ColumnMeta`。

```rust
use mysql_cdc_rs::{
    canal::{Change, Result, engine::converter::EventConverter},
    replication::{BinlogEvent, BinlogMode},
};

fn convert<M: BinlogMode>(
    converter: &mut EventConverter,
    event: &BinlogEvent<'_, M>,
) -> Result<()> {
    converter.convert_binlog(event, &mut |change: Change<'_>| {
        println!("{:?} {:?}", change.operation, change.pk);
        Ok(())
    })?;
    Ok(())
}
```

`EventConverter` 在处理 ROWS 时即回调行变更，早于事务提交，并返回描述控制边界的 `EventAction`。上例仅演示转换；需要交付已提交事务时，应使用 `Canal`，或将每个事件交给 `Processor`。直接转换回调不负责事务缓冲、丢弃回滚变更或检查点持久化。转换行事件之前须先处理该文件的 FDE。

离线原始事件入口为 `mysql_cdc_rs::replication::file::BinlogFile`，在线复制入口为 `mysql_cdc_rs::replication::stream::ReplicationStream`。完整示例见 [listen.rs](examples/listen.rs) 和 [dump_file.rs](examples/dump_file.rs)。从 workspace 根目录运行：

```sh
cargo run -p mysql-cdc-rs --example canal_listen -- canal.toml
cargo run -p mysql-cdc-rs --example replication_dump_file -- /path/to/binlog.000001
```

CDC 使用具名来源，各自维护 schema、白名单和检查点。仅交付已提交变更，包含来源、库表、操作、精确主键、变动字段名及携带元数据和值的 before/after 字段。回滚丢弃缓冲；失败停止消费，检查点持久化前崩溃可能导致重放。库不注册默认输出处理器；`pikachu` 包提供 `pikachu-cdc` CLI。

底层两个库仍可单独依赖。配置、输出契约、版本支持和限制见[项目说明](https://github.com/tiyee/mysql-cdc-rs/blob/master/README.zh-CN.md)、[CDC 库](https://docs.rs/mysql-canal)和[复制库](https://docs.rs/mysql-replication)。

[事件参考](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/events.zh-CN.md) · [开发指南](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/development.zh-CN.md) · [测试指南](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/testing.zh-CN.md)
