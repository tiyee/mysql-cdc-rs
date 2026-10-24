# mysql-replication

[English](README.md) | 简体中文

mysql-cdc 的底层 Rust 库，负责同步监听 MySQL binlog、解析协议并生成 `BinlogEvent`。可按需解码原始增删改行镜像；CDC 通用事件、事务交付和检查点由上层 `mysql-canal` 管理。

## 读取本地 binlog

```toml
[dependencies]
mysql-replication = "2.0.0"
```

```rust,no_run
use mysql_replication::{Event, Result, file::BinlogFile};
use std::{fs::File, io::BufReader};

fn main() -> Result<()> {
    let mut reader = BinlogFile::new(BufReader::new(File::open("binlog.000001")?))?;
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
use mysql_replication::{Result, config::{Config, RowImageMode}, stream::ReplicationStream};

fn main() -> Result<()> {
    let mut settings = Config::new("MYSQL_URL", 123);
    settings.row_image = RowImageMode::Full;
    let mut stream = ReplicationStream::connect(
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
use mysql_replication::{Full, Result, file::BinlogFile, parser::ParserConfig};
use std::{fs::File, io::BufReader};

fn main() -> Result<()> {
    let mut reader = BinlogFile::<_, Full>::for_mode(
        BufReader::new(File::open("binlog.000001")?),
        ParserConfig::default(),
    )?;
    while let Some(event) = reader.next_event()? {
        let owned = event.into_owned()?;
        let _view: mysql_replication::BinlogEvent<'_, Full> = owned.view()?;
    }
    Ok(())
}
```

`Minimal` / `Full` 贯穿 parser、reader、`BinlogEvent` 与 `RowsEvent`。`ReplicationStream::<Full>::for_mode(connection, config)` 在连接前校验类型与配置一致。`next_event()` 返回模式化事件；原始 `next_frame()` / `next_view()` 不携带模式校验。默认构造函数使用 `Unspecified`。

## 解码行镜像

用 TABLE_MAP 创建 `RowsDecoder`，通过 `RowsEvent::rows(&decoder)` 遍历 before/after。`Minimal` 使用 `SparseImage`，`Full` 使用 `CompleteImage` 并拒绝不完整镜像。原始解码不要求主键，也不缓冲事务。调用者负责维护、替换和清理 TABLE_MAP。

```sh
cargo run -p mysql-replication --example decode_rows -- /path/to/binlog.000001
cargo run -p mysql-replication --example decode_typed_rows -- /path/to/binlog.000001 --row-image full
```

示例从 workspace 根目录运行。逐字段定义、NULL/缺列区别和模式访问接口见[事件参考](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/events.zh-CN.md)。

## 参考

- [连接配置、解析选项与表预检](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/configuration.zh-CN.md)
- [分层职责与事件流](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.zh-CN.md)
- [开发与扩展指南](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/development.zh-CN.md)
- [协议与实库测试](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/testing.zh-CN.md)
