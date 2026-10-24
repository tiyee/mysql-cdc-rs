# mysql-replication

[English](README.md) | 简体中文

mysql-cdc 的底层 Rust 库，负责同步监听 MySQL binlog、解析协议并生成 `BinlogEvent`。可按需解码原始增删改行镜像；CDC 通用事件、事务交付和检查点由上层 `mysql-canal` 管理。

## 读取本地 binlog

通过本地源码集成时，使用以下依赖，请将 `/path/to/mysql-cdc-rs` 替换为实际仓库路径。

```toml
[dependencies]
mysql-replication = { path = "/path/to/mysql-cdc-rs/crates/mysql-replication" }
```

```rust,no_run
use mysql_replication::{Event, Minimal, Result, file::BinlogFile, parser::ParserConfig};
use std::{fs::File, io::BufReader};

fn main() -> Result<()> {
    let mut reader = BinlogFile::<_, Minimal>::new(
        BufReader::new(File::open("binlog.000001")?),
        ParserConfig::default(),
    )?;
    while let Some(event) = reader.next_event()? {
        println!("{} {}", event.header.server_id, event.header.log_pos);
        event.event.dump(&mut std::io::stderr())?;
        // Clone the complete raw buffer only when retaining this event.
        let owned = event.into_owned()?;
        let view = owned.view()?;
        assert_eq!(view.raw_data, owned.raw_data());
    }
    Ok(())
}
```

`next_event()` 返回完整原始字节、公共头及 `EventData` 事件体，借用有效期持续至下一次读取。只在需要跨读取持有时调用 `into_owned()`。`Event::dump` 输出逐字段诊断信息。文件须保留初始 FDE。

## 监听 MySQL

```rust,no_run
use mysql_replication::{
    Full, Result,
    config::{Config, RowImageMode},
    stream::ReplicationStream,
};

fn main() -> Result<()> {
    let mut settings = Config::new("MYSQL_URL", 123);
    settings.row_image = RowImageMode::Full;
    let mut stream = ReplicationStream::<Full>::connect(
        &settings.connection_config()?,
        settings.replication_config("binlog.000001", 4)?,
    )?;
    while let Some(event) = stream.next_event()? {
        println!("{}", event.header.log_pos);
    }
    Ok(())
}
```

凭据从 `MYSQL_URL` 环境变量读取。`row_image` 校验服务器设置，不修改 MySQL；文件与起点由调用者提供。`next_frame()` 返回已校验原始帧，在线 `next_view()` 直接借用驱动 body。

## 实例化时固定模式

```rust,no_run
use mysql_replication::{EventData, Full, Result, file::BinlogFile, parser::ParserConfig};
use std::{fs::File, io::BufReader};

fn main() -> Result<()> {
    let mut reader = BinlogFile::<_, Full>::new(
        BufReader::new(File::open("binlog.000001")?),
        ParserConfig::default(),
    )?;
    while let Some(event) = reader.next_event()? {
        if let EventData::Rows(rows) = &event.event {
            for row in rows.rows()? {
                let (before, after) = row?.operation.into_images();
                for image in before.into_iter().chain(after) {
                    for field in image.fields() {
                        let field = field?;
                        println!("{:?}: {:?}", field.column.name, field.value);
                    }
                }
            }
        }
        let owned = event.into_owned()?;
        let _view: mysql_replication::BinlogEvent<'_, Full> = owned.view()?;
    }
    Ok(())
}
```

`Minimal` / `Full` 贯穿 parser、reader、`BinlogEvent` 与 `RowsEvent`，所有类型化构造都显式选择模式。`ReplicationStream::<Full>::connect(connection, config)` 在连接前校验类型与配置一致。原始文件帧使用 `FrameReader`，原始网络帧/视图使用 `ReplicationConnection`，类型化 reader 只提供 `next_event()`。

## 解码行镜像

`BinlogFile<_, M>` / `ReplicationStream<M>` 在读取 TABLE_MAP 时自动绑定共享列定义，`RowsEvent<M>::rows()` 无需外部解码器。模式沿用同一组 `Full` / `Minimal` 标记并静态分派，行循环没有运行时模式判断。`Row.operation` 使用 `RowOperation::Insert { after }`、`Update { before, after }`、`Delete { before }`，必需镜像不会缺失。

`Minimal` 产生 `SparseImage`，`field(index)` 返回 `Result<Option<Field>>`，合法但未记录的列为 `None`；`Full` 产生 `CompleteImage`，返回 `Result<Field>` 并拒绝不完整镜像。两者越界都报错。`Field` 同时提供 `column: &ColumnInfo` 和借用的 `value: RowValue`，SQL NULL 表示为 `Field.value = RowValue::Null`。`fields()` 依次遍历实际存在的字段，`columns()` 提供全部表列定义。

列定义包含索引、可用的列名、wire 类型、可空性、符号、collation 和主键信息。列元数据与行镜像完整性独立：`binlog_row_image=FULL` 不保证列名，缺失可选属性为 `None`，不查询服务器当前 schema。`into_owned()` / `view()` 保留历史列计划和实际 checksum。parser 按来源隔离映射，在重映射时替换，在文件/语句边界清理；语句结束哨兵单独表示为 `EventData::RowsStatementEnd`。选择模式应在首次读取前完成。

自定义协议解析使用 `file::FrameReader` / `stream::ReplicationConnection`。显式 `RowsDecoder<M>` 与自动绑定复用同一组泛型事件和镜像，不提供另一套未分类行事件。原始解码不要求主键，也不缓冲事务；Canal 保留自身适配器和轻量变更输出。

```sh
cargo run -p mysql-replication --example decode_rows -- /path/to/binlog.000001 --row-image full
```

示例从 workspace 根目录运行。逐字段定义、NULL/缺列区别和模式访问接口见[事件参考](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/events.zh-CN.md)。

## 参考

- [Rust 字段与接口参考](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/event-api.zh-CN.md)
- [连接配置、解析选项与表预检](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/configuration.zh-CN.md)
- [分层职责与事件流](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.zh-CN.md)
- [开发与扩展指南](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/development.zh-CN.md)
- [协议与实库测试](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/testing.zh-CN.md)
