# mysql-replication

[English](README.md) | 简体中文

同步 MySQL 复制传输与借用 binlog 事件库。CDC 转换、白名单、历史 SQL schema、事务交付及持久化检查点属于上层 `mysql-canal` 公共库（导入路径 `mysql_canal`）。

需要通过一个依赖同时使用原始复制与 CDC 时，可依赖 `mysql-cdc-rs`，通过 `mysql_cdc_rs::replication` 导入本库 API。

```toml
[dependencies]
mysql-replication = "0.0.4"
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

`Event<'a>` 包含 `decode(&mut self, &'a [u8]) -> Result<()>` 与 `dump(&self, &mut dyn Write) -> Result<()>`。公共头 `EventHeader` 和具体事件体实现同一对象安全接口。头部解码接收恰好 19 字节，事件体解码不含公共头及 checksum。`dump` 用于可读诊断并传播写入错误，不承担二进制序列化或 CDC 输出。

`dump` 按字段逐行打印标签和值。例如 FDE 输出 `Version: 4`、`Server version: 8.4.6`、`Checksum algorithm: CRC32`，末尾为空行。checksum 算法来自已校验帧的尾部；仅解码事件体时保留调用方提供的值，未提供则为 `UNKNOWN`。`into_owned()` 与 `view()` 会保留该上下文。

`BinlogEvent` 关联 `raw_data`、完整公共头和具体 `EventData`。原始字节包含未修改的公共头、事件体、实际尾部及 FDE 算法字节，不包含文件 magic 或网络包封装。读取结果借用可复用缓冲区，到下一次读取前有效。`into_owned()` 持有一份原始缓冲区；`view()` 按需借用，不使用自引用或 unsafe。`EventData` 使用枚举，调用方通过匹配访问字段，无需下转型。

结构化解析覆盖 FDE、ROTATE、QUERY、TABLE_MAP、ROWS v1/v2、XID、普通/匿名/Tagged GTID、Previous GTIDs 与 v1 心跳。GTID 可选元数据用 `Option` 区分缺失与零，Tagged GTID serialization v2 使用独立解码器。其他载荷，包括 v2 心跳、XA、压缩事务与未知事件码，明确交付为 `EventData::Raw`，不宣称已解码支持。SQL 和库名字节保持借用，DDL 正常交付为 QUERY。

## 配置与按需解析

```rust,no_run
use mysql_replication::{Result, stream::{ConnectionConfig, ReplicationConfig, ReplicationStream, RowImage}, parser::ParserConfig};

fn main() -> Result<()> {
    let connection = ConnectionConfig::new("MYSQL_URL");
    let mut config = ReplicationConfig::new(123, "binlog.000001", 4);
    config.required_row_image = Some(RowImage::Minimal);
    config.parser = ParserConfig::minimal();
    let mut stream = ReplicationStream::connect(&connection, config)?;
    while let Some(view) = stream.next_view()? {
        // Borrowed body: no second raw-frame copy or row-value decoding.
        println!("{} {}", view.header.event_type, view.body.len());
    }
    Ok(())
}
```

连接配置支持凭据环境变量、TLS CA 和超时；复制配置支持 server ID、file/position、阻塞模式、上游行镜像要求及解析选项。行镜像要求只校验，不修改服务器全局设置。底层不查询业务表，也不要求主键。MySQL 发送已经存在的 binlog 字节，复制协议不支持按列裁剪网络传输。

- `next_event()` 返回具体协议事件和完整原始字节。
- `next_frame()` 校验长度/checksum，保留原始字节，事件体留给自定义适配器。
- 在线 `next_view()` 校验驱动事件并直接借用 body，适合不需要完整原始帧的消费者。
- `ParserConfig::minimal()` 跳过 GTID 元数据和 Previous GTIDs 解析，保留原事件码及明确的 raw 载荷。
- ROWS 默认延迟解析镜像，调用 `RowsEvent::images()` 时才读取 extra-data 和稀疏位图。`RowsDecode::ValidateImages` 可要求立即校验。所有模式均不构建完整行值。
- `TableMapEvent::columns()` 在表匹配后按需校验列结构。

结构化解析器根据每个文件的 FDE 选择声明的布局，支持已注册 MySQL 5.7、8.0–8.4、9.0、9.7 系列。未知系列或布局明确失败；`next_frame()` 可交给自定义事件体解析器。混用帧/视图与结构化读取时，需要将读取到的 FDE 帧交给自己的 `BinlogParser`；内置结构化解析器只观察 `next_event()` 调用。

离线文件必须保留开头 FDE，支持 CRC32/NONE，截断尾部和超过 64 MiB 的事件报错。在线传输复用 `mysql` 驱动；驱动会预解析 FDE/TABLE_MAP/ROTATE，并在本库校验事件大小之前分配内存，特殊 framing 可能需要其他传输实现。完整在线原始事件保留驱动读取的原头部字段、body、算法字节及存储的 checksum，不调用会重新计算 checksum 的序列化方法。`next_view()` 完全跳过这次原始帧组装。断线传播错误，不静默重连。

```sh
cargo test -p mysql-replication --locked
cargo test -p mysql-canal --locked
```

独立合成协议测试覆盖公共头、原始字节保真、CRC32/NONE、可选/Tagged GTID、延迟 ROWS、宽表稀疏位图和 owned 生命周期。实库 CDC 兼容测试与版本适配器位于 `mysql-canal`，见[项目说明](../../README.zh-CN.md)。
