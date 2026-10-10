# mysql-replication

[English](README.md) | 简体中文

底层库负责同步订阅 MySQL binlog、解析协议事件，并可按需解码借用的增、改、删行镜像。CDC 转换、白名单过滤、历史 SQL schema、事务交付及持久化检查点属于上层 `mysql-canal` 公共库（导入路径 `mysql_canal`）；`pikachu` CLI 再基于 canal 构建。

原始解析返回 `BinlogEvent` / `EventData`，行解码通过 TABLE_MAP 与 `RowsDecoder` 返回稀疏的 `Row { before, after }` 镜像，不生成 canal 的 `Change` 或 `CommittedTransaction`。普通 SELECT 不产生 CDC 变更，协议 QUERY 事件保存 SQL 字节，不是查询结果。两条解码路径见[架构说明](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.zh-CN.md)。

需要通过一个依赖同时使用原始复制与 CDC 时，可依赖 `mysql-cdc-rs`，通过 `mysql_cdc_rs::replication` 导入本库 API。

```toml
[dependencies]
mysql-replication = "1.0.0"
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
    let mut stream = ReplicationStream::connect_with_tables(
        &connection, config, [("app", "users"), ("app", "orders")],
    )?;
    while let Some(view) = stream.next_view()? {
        // Borrowed body: no second raw-frame copy or row-value decoding.
        println!("{} {}", view.header.event_type, view.body.len());
    }
    Ok(())
}
```

连接配置支持凭据环境变量、TLS CA 和超时；复制配置支持 server ID、file/position、阻塞模式、上游行镜像要求及解析选项。行镜像要求只校验，不修改服务器全局设置。MySQL 发送已经存在的 binlog 字节，复制协议不支持按列裁剪网络传输。

`ReplicationStream::connect_with_tables` 和 `from_connection_with_tables` 在发送 binlog 请求前，通过 INFORMATION_SCHEMA 检查调用方指定的表。每张表必须有单列 PRIMARY KEY，类型为有符号/无符号 TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT、精确 DECIMAL，或使用 utf8、utf8mb3、utf8mb4、ascii、latin1 的 CHAR/VARCHAR。表或主键缺失、联合主键、只有 UNIQUE 索引、FLOAT/DOUBLE、二进制/日期主键和不支持的字符集均报错，包含库表及可获取的列名；空表列表也报错。该列表仅控制检测，不过滤事件。原有 `connect` 和 `from_connection` 保留不做此检测的原始复制能力。可通过 `primary_key::validate_primary_keys(&mut conn, tables)` 单独检测而不启动复制。`mysql-canal` 的 MySQL 来源自动调用该检测，包括从检查点恢复时。

- `next_event()` 返回具体协议事件和完整原始字节。
- `next_frame()` 校验长度/checksum，保留原始字节，事件体留给自定义适配器。
- 在线 `next_view()` 校验驱动事件并直接借用 body，适合不需要完整原始帧的消费者。
- `ParserConfig::minimal()` 跳过 GTID 元数据和 Previous GTIDs 解析，保留原事件码及明确的 raw 载荷。
- ROWS 默认延迟解析镜像，调用 `RowsEvent::images()` 时才读取 extra-data 和稀疏位图。`RowsDecode::ValidateImages` 可要求立即校验。这些解析模式不解码字段值；下方独立的 `RowsDecoder` API 按需启用。
- `TableMapEvent::columns()` 在表匹配后按需校验列结构。

## 解码原始行镜像

事件框架位于 `events`：`BinlogEvent` 是通用容器，`EventHeader` 是公共头，`EventData` 是具体事件体枚举。容器所有权、控制事件、TABLE_MAP 和 ROWS 分别由独立模块实现；现有公共导入路径保持不变。`RowsEvent` 描述协议事件，`RowsDecoder` 独立于 CDC 策略编译列布局。

```rust,no_run
use mysql_replication::{Result, events::{RowValue, RowsDecoder, RowsEvent, TableMapEvent}};

fn inspect(table: &TableMapEvent<'_>, event: &RowsEvent<'_>) -> Result<()> {
    let decoder = RowsDecoder::new(table)?;
    for row in event.rows(&decoder)? {
        let row = row?;
        for (label, image) in [("before", row.before), ("after", row.after)] {
            if let Some(image) = image {
                for field in image.fields() {
                    let field = field?;
                    match field.value {
                        RowValue::Null => println!("{label}[{}] = NULL", field.column_index),
                        RowValue::Decimal(value) => println!("{label}[{}] = {value}", field.column_index),
                        value => println!("{label}[{}] = {value:?}", field.column_index),
                    }
                }
            }
        }
    }
    Ok(())
}
```

每个来源为当前有效的 TABLE_MAP 缓存独立解码器。table ID 重映射时替换解码器，文件或语句边界清理映射。解码器检查 table ID 和列数，但不能识别 ID、列数均相同的过期映射。解码器持有编译后的布局，可释放输入 TABLE_MAP。每个 ROWS 迭代器仅编译一次稀疏镜像计划，各行共享字段索引；行载荷保持借用，不分配完整行向量。每次交付行操作前校验其完整边界、字段长度、NULL 约束及压缩 DECIMAL 数字组。解码错误仅返回一次，随后结束该迭代器；此前的行可能已被调用方消费。该 API 不提供事务缓冲。

- WRITE 仅有 `after`，DELETE 仅有 `before`，UPDATE 有配对镜像。不存在的镜像为 `None`。镜像的 `fields()` 仅返回存在列，列索引为从零开始的表绝对序号。`RowValue::Null` 表示存在列的 SQL NULL；缺失列不填充、不推断。紧凑 NULL 位图支持稀疏镜像及超过 64 列的表。
- 整数通过 `IntegerValue` 保留实际编码宽度。根据已知 SQL 元数据调用 `signed()` 或 `unsigned()`；TABLE_MAP 必需元数据不包含符号信息。`DecimalValue` 保留 precision/scale，校验压缩数字组并精确格式化，包括 65 位数值。FLOAT/DOUBLE、BIT、ENUM 索引、SET 掩码和 YEAR 使用明确的值枚举。
- CHAR/VARCHAR 和 BLOB 返回借用的 `Bytes`，不含长度前缀，不猜测字符集。时间类型、JSON、geometry 和 vector 返回借用的 `Encoded` 字节；`decoder.columns()` 暴露其类型及编码元数据。该 API 不将它们转换成日期、JSON 对象或向量。旧 DECIMAL、typed array 和未知列类型明确报不支持。
- 非法长度、不完整 UPDATE 配对、非法元数据及 NOT NULL 列的 NULL 返回原始协议错误。原始行解码不要求主键；独立的指定表预检在调用方启用时应用主键限制。CDC 特有的解码错误属于 `mysql-canal`。CDC 路径继续跳读非主键值，不调用该解码器。

使用包含 FDE 和 TABLE_MAP 的 binlog 文件运行完整离线示例：

```sh
cargo run -p mysql-replication --example decode_rows -- /path/to/binlog.000001
```

结构化解析器根据每个文件的 FDE 选择声明的布局，支持已注册 MySQL 5.7、8.0–8.4、9.0、9.7 系列。生产者版本须包含完整数字 `major.minor.patch`，允许非空的 `-` 或 `+` 后缀（如 `5.7.44-log` 或 `8.0.45-0ubuntu0.24.04.1`）。畸形版本与 MariaDB 标识明确拒绝，数字版本解析规则与 CDC 共用。未知系列或布局明确失败；`next_frame()` 可交给自定义事件体解析器。混用帧/视图与结构化读取时，需要将读取到的 FDE 帧交给自己的 `BinlogParser`；内置结构化解析器只观察 `next_event()` 调用。

离线文件必须保留开头 FDE，支持 CRC32/NONE，截断尾部和超过 64 MiB 的事件报错。在线传输复用 `mysql` 驱动；驱动会预解析 FDE/TABLE_MAP/ROTATE，并在本库校验事件大小之前分配内存，特殊 framing 可能需要其他传输实现。完整在线原始事件保留驱动读取的原头部字段、body、算法字节及存储的 checksum，不调用会重新计算 checksum 的序列化方法。`next_view()` 完全跳过这次原始帧组装。断线传播错误，不静默重连。

```sh
cargo test -p mysql-replication --locked
cargo test -p mysql-canal --locked
```

独立合成协议测试覆盖公共头、原始字节保真、CRC32/NONE、可选/Tagged GTID、延迟 ROWS、解码后的 before/after 镜像、宽表稀疏/NULL 位图、整数符号边界、精确 DECIMAL、畸形字段和 owned 生命周期。两个默认忽略的 `mysql_live` 测试独立验证指定表主键预检和真实增改删稀疏镜像，包含精确值、NULL、字符串字节借用和非选中表。实库测试需要专用可销毁 MySQL 实例，串行执行，会创建/删除测试库；CDC 与 CLI 各自维护实库测试。共享矩阵与容器清理见[项目验证说明](https://github.com/tiyee/mysql-cdc-rs/blob/master/README.zh-CN.md#验证)。
