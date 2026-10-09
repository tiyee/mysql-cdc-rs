# mysql-cdc-rs

[English](README.md) | 简体中文

只需一个依赖即可使用 MySQL CDC 与复制功能。本聚合包重导出 `mysql-canal` 和 `mysql-replication`，保留原有类型、生命周期和行为。

```toml
[dependencies]
mysql-cdc-rs = "1.0.0"
```

Rust 导入路径为 `mysql_cdc_rs`。CDC 类型和模块可直接从包根路径导入，也可通过 `canal` 访问；原始 binlog 事件和复制传输通过 `replication` 访问。根路径的 `Error`、`Result` 属于 CDC 层；原始协议操作使用 `replication::Error`、`replication::Result`。

```rust,no_run
use mysql_cdc_rs::{Cdc, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let mut cdc = Cdc::new(AppConfig::read(Path::new("cdc.toml"))?)?;
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

离线原始事件入口为 `mysql_cdc_rs::replication::file::BinlogFile`，在线复制入口为 `mysql_cdc_rs::replication::stream::ReplicationStream`。完整示例见 [listen.rs](examples/listen.rs) 和 [dump_file.rs](examples/dump_file.rs)。从 workspace 根目录运行：

```sh
cargo run -p mysql-cdc-rs --example cdc_listen -- cdc.toml
cargo run -p mysql-cdc-rs --example replication_dump_file -- /path/to/binlog.000001
```

CDC 使用具名来源，各自维护 schema、白名单和检查点。仅交付已提交变更，包含来源、库表、操作、精确主键和变动字段名。回滚丢弃缓冲；失败停止消费，检查点持久化前崩溃可能导致重放。库不注册默认输出处理器；`pikachu` 包提供 `pikachu-cdc` CLI。

底层两个库仍可单独依赖。配置、输出契约、版本支持和限制见[项目说明](https://github.com/tiyee/mysql-cdc-rs/blob/main/README.zh-CN.md)、[CDC 库](https://docs.rs/mysql-canal)和[复制库](https://docs.rs/mysql-replication)。
