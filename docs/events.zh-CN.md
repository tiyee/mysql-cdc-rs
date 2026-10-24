# 事件参考

[English](events.md) | 简体中文

[文档目录](README.zh-CN.md)

本页集中定义原始协议事件、行镜像和 Canal 的通用 CDC 事件。配置与模式切换见[配置指南](configuration.zh-CN.md)，实现流程见[架构](architecture.zh-CN.md)。

## 事件类型与边界

| 类型 | 所属层 | 用途 |
| --- | --- | --- |
| `BinlogEvent<'a, M>` | replication | 原始头、字节和协议事件体 |
| `Event<'a>` / `EventData<'a, M>` | replication | 共用解码 trait / 实际事件体枚举 |
| `Change<'a>` / `OwnedChange` | canal | 通用行变更，包含精确主键及字段索引 |
| `CommittedTransaction<'a>` | canal | COMMIT 后交付的完整事务 |

`Event` 是 trait，没有公共字段。它与 Canal 的通用变更事件不是同一个类型。普通 SELECT 不产生 CDC 变更；协议 QUERY 保存 SQL 字节，也可表示 DDL。

`dump` 按字段逐行打印标签和值。例如 FDE 输出 `Version: 4`、`Server version: 8.4.6`、`Checksum algorithm: CRC32`，末尾为空行。checksum 算法来自已校验帧的尾部；仅解码事件体时保留调用方提供的值，未提供则为 `UNKNOWN`。`into_owned()` 与 `view()` 会保留该上下文。

结构化解析覆盖 FDE、ROTATE、QUERY、TABLE_MAP、ROWS v1/v2、XID、普通/匿名/Tagged GTID、Previous GTIDs 与 v1 心跳。GTID 可选元数据用 `Option` 区分缺失与零，Tagged GTID serialization v2 使用独立解码器。其他载荷，包括 v2 心跳、XA、压缩事务与未知事件码，明确交付为 `EventData::Raw`，不宣称已解码支持。SQL 和库名字节保持借用，DDL 正常交付为 QUERY。

## 事件公共字段与模式差异

Rust 类型名为 `BinlogEvent<'a, M>`。`Event<'a>` 是不含字段的共用、对象安全 **trait**；`EventData<'a, M>` 是保存在 `BinlogEvent.event` 中的枚举，通过匹配变体访问具体协议事件。`Minimal` 和 `Full` 区分 ROWS 策略及镜像类型，不增减包络字段或控制事件字段。

### 共用包络与公共头

以下是 `BinlogEvent<'a, M>` 的全部公共字段，适用于 `M = Unspecified`、`Minimal` 或 `Full`：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `raw_data` | `&'a [u8]` | 完整原始事件：公共头、body、实际 trailer/checksum，包含 FDE 算法字节；不包含文件 magic 和网络包头 |
| `header` | `EventHeader` | 下表所示的 binlog v4 公共头，长度为 19 字节 |
| `event` | `EventData<'a, M>` | 结构化协议 body，其中 ROWS 携带同一个 `M` |

内部 `body` 字段不公开，可通过 `event.frame().body` 访问排除公共头和 framing trailer 的借用 body。`OwnedBinlogEvent<M>` 没有公共字段，使用 `raw_data()` 和 `view()`；`into_owned()` / `view()` 保留 `M` 和原始事件字节，视图借用 owned 缓冲区。

| `EventHeader` 公共字段 | 类型 | 含义 |
| --- | --- | --- |
| `timestamp` | `u32` | 生产者记录的事件时间，单位为 Unix 秒 |
| `event_type` | `u8` | wire 事件码，使用 `event_type` 的具名常量 |
| `server_id` | `u32` | 生产者记录的 server ID |
| `event_size` | `u32` | 完整事件长度，包含公共头及存在的 trailer/checksum |
| `log_pos` | `u32` | 头中记录的位点，通常是下一条事件的位置；不是已确认的 CDC 检查点，某些控制事件可能为零 |
| `flags` | `u16` | 公共头标志，保留未知位 |

`Event<'a>` 仅提供 `decode(&mut self, data: &'a [u8]) -> Result<()>` 和 `dump(&self, writer: &mut dyn Write) -> Result<()>`，并要求实现 `Debug`。`EventHeader::decode` 接收精确的 19 字节，body 解码器只接收 body。trait 本身没有 `header`、`pk`、`before` 或 `after` 字段；`dump` 用于诊断输出。

### 两种模式共用的协议事件字段

`EventData` 标记为 non-exhaustive，外部匹配须保留兜底分支。各变体具体 body 的公共字段如下，仅 `Rows` 的载荷携带 `M`：

| 变体 / 载荷类型 | MINIMAL 与 FULL 共用的公共字段 |
| --- | --- |
| `FormatDescription` / `FormatDescriptionEvent<'a>` | `binlog_version: u16`、`server_version: &'a str`、`create_timestamp: u32`、`header_length: u8`、`post_header_lengths: &'a [u8]`、`checksum_algorithm: ChecksumAlgorithm` |
| `Rotate` / `RotateEvent<'a>` | `position: u64`、`next_file: &'a [u8]` |
| `Query` / `QueryEvent<'a>` | `thread_id: u32`、`execution_time: u32`、`error_code: u16`；`status_vars`、`schema`、`query` 均为 `&'a [u8]` |
| `TableMap` / `TableMapEvent<'a>` | `table_id: u64`、`flags: u16`；`schema`、`table`、`column_data` 均为 `&'a [u8]` |
| `Rows` / `RowsEvent<'a, M>` | `table_id: u64`、`flags: u16`、`kind: RowsKind`、`encoded_images: &'a [u8]` |
| `Xid` / `XidEvent` | `xid: u64` |
| `Gtid` 和 `AnonymousGtid` / `GtidEvent` | `flags: u8`、`sid: [u8; 16]`、`gno: i64`，以及下述可选元数据 |
| `TaggedGtid` / `TaggedGtidEvent<'a>` | `gtid: GtidEvent`、`tag: &'a str`、`unknown_fields: &'a [u8]` |
| `PreviousGtids` / `PreviousGtidsEvent<'a>` | `sid_count: u64`、`encoded_sids: &'a [u8]` |
| `Heartbeat` / `HeartbeatEvent<'a>` | `log_file: &'a [u8]` |
| `Raw` / `RawEvent<'a>` | `data: &'a [u8]`，保留未知或有意不解码的 body |

`GtidEvent` 的可选字段包括：`last_committed` / `sequence_number`（`Option<i64>`）；`immediate_commit_timestamp` / `original_commit_timestamp` / `transaction_length` / `commit_group_ticket`（`Option<u64>`）；`immediate_server_version` / `original_server_version`（`Option<u32>`）。它们是否存在取决于生产者元数据和解析选项，与行镜像模式无关；`ParserConfig` 禁用结构化 GTID 解码时，也可将其保留为 `Raw`。

`TableMapEvent::columns()` 返回 `TableColumns { count: usize, types, metadata, nullable, optional_metadata }`，后四项均为借用字节切片。`PreviousGtidsEvent::sids()` 返回 `SidIntervals { sid, intervals }`，两项也均为借用字节切片。这些访问接口在两种模式下相同。`RowsEvent.flags` 是 ROWS body 标志，与 `BinlogEvent.header.flags` 相互独立。

### 随模式变化的字段类型与访问接口

上述 `RowsEvent` 的四个公共字段保持相同名称和 wire 含义，差异体现在镜像和行解码结果中：

| API / 属性 | `Minimal` | `Full` |
| --- | --- | --- |
| 包络 / body 类型 | `BinlogEvent<'a, Minimal>` / `EventData<'a, Minimal>` | `BinlogEvent<'a, Full>` / `EventData<'a, Full>` |
| `RowsEvent<M>::rows(&RowsDecoder)` | `Result<TypedRows<'a, Minimal>>` | `Result<TypedRows<'a, Full>>` |
| 迭代器元素 | `Result<TypedRow<'a, Minimal>>` | `Result<TypedRow<'a, Full>>` |
| `TypedRow.before` 公共字段 | `Option<SparseImage<'a>>` | `Option<CompleteImage<'a>>` |
| `TypedRow.after` 公共字段 | `Option<SparseImage<'a>>` | `Option<CompleteImage<'a>>` |
| 镜像结构 | 只保留存在列；原始复制同样接受完整位图 | 每个存在的镜像必须包含全部列；拒绝稀疏输入，不补齐缺失值 |
| `image.field(index)` | `Result<Option<RowValue<'a>>>`；未记录或越界的列返回 `None` | `Result<RowValue<'a>>`；范围内列必定存在，越界访问报错 |
| 列索引 | `image.column_indices()` 返回实际存在列的从零开始的绝对索引 | 全部表列索引均存在，需要时使用 `image.as_ref().column_indices()` |

`TypedRowsEvent<M>` 没有公共字段，使用 `table_id()`、`flags()`、`kind()`、`column_count()` 和 `rows()` 访问。

两种镜像都提供 `column_count()`（表的总列数）和 `fields()`（实际存在字段的迭代器），内部缓冲区与执行计划不公开。两种模式的 `RowField` 公共字段都为 `column_index: u16` 和 `value: RowValue<'a>`；列名、SQL 符号和字符集解释不是某个模式额外提供的行字段。

`RowsEvent::images()` 返回共用的编码视图 `RowImages { extra_data: &'a [u8], column_count: usize, before_columns: &'a [u8], after_columns: &'a [u8], rows: &'a [u8] }`。WRITE 的 `before_columns` 指 **wire 中第一份位图**，实际描述 after 镜像；DELETE 的两个位图字段都引用 before 位图，UPDATE 才有独立的前后位图。这些字段名不会使 INSERT 多出 before 镜像。Full 解析在返回普通 ROWS 包络前检查完整位图；语句结束哨兵没有镜像。字段值在行迭代时校验。

默认的 `Unspecified` 保留未分类的原始接口：`RowsEvent::rows` 返回 `Rows<'a>`，包含 `Row { before: Option<RowImage<'a>>, after: Option<RowImage<'a>> }`，不据此确定生产者使用 MINIMAL 还是 FULL。`next_frame()` / `next_view()` 返回的原始视图同样不携带模式保证。

| 操作（`RowsKind`） | 两种模式的 `before` | 两种模式的 `after` |
| --- | --- | --- |
| INSERT（`Write`） | `None` | `Some(image)` |
| UPDATE（`Update`） | `Some(image)` | `Some(image)` |
| DELETE（`Delete`） | `Some(image)` | `None` |

须区分三种情况：`row.after` 为 `None` 表示该操作没有 after 镜像；`SparseImage::field(i)?` 返回 `None` 表示列未记录或越界；`Some(RowValue::Null)` 表示实际存在的列值为 SQL NULL。完整镜像中，实际存在的 SQL NULL 直接表示为 `RowValue::Null`。

```rust,no_run
use mysql_replication::{Full, Minimal, Result, events::{RowValue, TypedRow}};

fn inspect_minimal(row: TypedRow<'_, Minimal>) -> Result<()> {
    if let Some(after) = row.after {
        match after.field(0)? {
            None => println!("column 0 is absent"),
            Some(RowValue::Null) => println!("column 0 is SQL NULL"),
            Some(value) => println!("column 0: {value:?}"),
        }
    }
    Ok(())
}

fn inspect_full(row: TypedRow<'_, Full>) -> Result<()> {
    if let Some(after) = row.after {
        let value: RowValue<'_> = after.field(0)?;
        println!("column 0: {value:?}");
    }
    Ok(())
}
```

这些类型属于原始复制记录。Canal 的 `Change` / `OwnedChange` 则包含 `table`、`operation`、`pk`、`previous_pk`、`changed`，`CommittedTransaction` 包含 `source_id`、`checkpoint`、`changes`；这些公共字段在两种模式下保持一致。FULL 改变 after 镜像在 `changed` 中包含的列索引，不会使 CDC 输出增加完整行值。Canal 对选中单列主键表还要求 MINIMAL before 镜像仅包含主键，而原始 `Minimal` 无需主键，也接受稀疏或完整镜像。

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

## 通过泛型选择镜像类型

`RowsEvent` 保留通用的借用协议事件结构。`TypedRowsDecoder<M>` 将 TABLE_MAP 执行计划绑定到镜像模式，返回 `TypedRowsEvent<'a, M>` / `TypedRow<'a, M>`：

| 模式 | 镜像类型 | `field(index)` 返回值 |
| --- | --- | --- |
| `Minimal` | `SparseImage<'a>` | `Result<Option<RowValue<'a>>>`；缺失为 `None`，SQL NULL 为 `Some(RowValue::Null)` |
| `Full` | `CompleteImage<'a>` | `Result<RowValue<'a>>`；范围内的列全部存在，SQL NULL 为 `RowValue::Null` |

```rust,no_run
use mysql_replication::{Result, events::{
    Full, Minimal, RowValue, RowsEvent, TableMapEvent, TypedRowsDecoder,
}};

fn inspect(table: &TableMapEvent<'_>, event: &RowsEvent<'_>) -> Result<()> {
    let full = TypedRowsDecoder::<Full>::new(table)?;
    for row in full.rows(event)? {
        if let Some(after) = row?.after {
            let value: RowValue<'_> = after.field(0)?;
            println!("{value:?}");
        }
    }
    let minimal = TypedRowsDecoder::<Minimal>::new(table)?;
    for row in minimal.rows(event)? {
        if let Some(after) = row?.after {
            let value: Option<RowValue<'_>> = after.field(0)?;
            println!("{value:?}");
        }
    }
    Ok(())
}
```

`TypedRowsDecoder::<Full>::event` 在迭代前检查所有存在镜像的位图，不完整时返回 `Error::Unsupported`。字段值及 UPDATE 完整配对在交付每行前校验；畸形输入只返回一次错误，随后结束迭代。WRITE 只有 `after`，DELETE 只有 `before`，UPDATE 两者都有。泛型改变镜像类型，不改变操作本身是否存在前后镜像。`CompleteImage::try_from(raw_image)` 同样校验完整性。

`column_count()` 返回 TABLE_MAP 的总列数，包含缺失列。`fields()` 按绝对列序号流式遍历存在字段，顺序读取优先使用该接口。`field(index)` 无分配地扫描至目标列；越界时 FULL 返回 `Error::Invalid`，MINIMAL 返回 `None`。字节值保持借用，各行共享编译后的字段索引计划；类型化事件及行可以比解码器存活更久，但不能超过输入字节的生命周期。通过 `AsRef<RowImage>` 编写通用泛型逻辑，不分配完整行向量。

已有 `RowsDecoder` 可调用 `raw_event.with_mode::<Full>(&raw_decoder)`，或通过 `TypedRowsDecoder::<Full>::from(raw_decoder)` 复用执行计划。模式 trait 封闭，不能绕过完整性校验。MINIMAL 也接受完整位图：INSERT 或没有主键的表即使使用 MINIMAL 日志，也可能包含全部列。仅凭字节不能确定生产者设置；在线调用方另行通过 `required_row_image` 校验服务器设置。

运行时参数在入口通过 `match mode { ... run::<Minimal>(), ... run::<Full>() }` 选择泛型实现。以下示例默认 MINIMAL，也接受两种参数值：

```sh
cargo run -p mysql-replication --example decode_typed_rows -- /path/to/binlog.000001 --row-image full
cargo run -p mysql-replication --example decode_typed_rows -- /path/to/binlog.000001 --row-image minimal
```

## Canal 通用事件字段

`Change` 是转换回调中的借用视图；`into_owned()` 生成可跨事件持有的 `OwnedChange`。两者字段含义相同：

| 字段 | `Change<'a>` | `OwnedChange` | 含义 |
| --- | --- | --- | --- |
| `table` | `&'a Arc<TableMeta>` | `Arc<TableMeta>` | 历史库表及有序列元数据 |
| `operation` | `Operation` | `Operation` | `Insert`、`Update` 或 `Delete` |
| `pk` | `PrimaryKey<'a>` | `PrimaryKey<'static>` | 精确主键；主键变更时为新值 |
| `previous_pk` | `Option<PrimaryKey<'a>>` | `Option<PrimaryKey<'static>>` | 主键变更时的旧值 |
| `changed` | `&'a Arc<[u16]>` | `Arc<[u16]>` | 按表顺序排列、共享的绝对列索引 |

`PrimaryKey` 变体为 `Signed(i64)`、`Unsigned(u64)`、`Text(Cow<'a, str>)` 和 `Decimal(String)`。DECIMAL 不经过浮点转换。`OwnedChange::changed_names()` 通过 `table` 将索引映射为字段名。

| `CommittedTransaction<'a>` 字段 | 类型 | 含义 |
| --- | --- | --- |
| `source_id` | `&'a str` | 配置中的稳定来源身份 |
| `checkpoint` | `&'a State` | 所有处理器成功后才保存的提交边界 |
| `changes` | `&'a [OwnedChange]` | 按 binlog 顺序排列的变更；完全过滤的提交为空 |

`State` 携带历史 schema、文件/位点及来源 UUID、ID、插件。事务数据只在回调期间借用，需要跨回调持有时才克隆。`EventConverter` 的回调可能发生在 COMMIT 之前；提交交付应使用 `Cdc` 或 `Processor`。

MINIMAL 与 FULL 使用相同的通用类型和字段。区别在于下文所述的 `changed` 中 after-image 索引，两者都不增加完整行值。

## 输出语义

每行一条 JSON，同一来源按 binlog 顺序输出；不同来源按事务写入顺序合流，同一事务的行不会被其他来源插入。UPDATE 示例：

```json
{"source":"east","schema":"app","table":"users","operation":"update","pk":{"type":"unsigned","value":42},"changed":["name","age"]}
```

`source` 是配置中的稳定来源 ID，防止不同实例相同库表/主键混淆。`operation` 区分 insert/update/delete。主键变更时 `pk` 是新值，附带 `previous_pk`，消费者可以同时处理旧键与新键。INSERT 的 `changed` 是 after image 中存在的列；DELETE 是空数组；UPDATE 是 after image 位图中的列，包括写入 NULL 的字段和被修改的主键。

**FULL 模式下 INSERT/UPDATE 的 `changed` 包含 after image 的全部列，包括值未变化的字段和未变化的主键；不做新旧值比较，CDC 输出仍不包含完整行值。**

**这里的“变动字段”精确定义为 binlog after image 中记录的字段，不保证值确实不同。** MINIMAL 日志不包含非主键旧值，SQL 指定但值未改变的列也可能出现，无法仅凭日志逐值比较。依据：[MySQL binlog_row_image 文档](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html) 与 [MySQL 已确认的问题 #83418](https://bugs.mysql.com/bug.php?id=83418)。

主键支持：

- TINYINT / SMALLINT / MEDIUMINT / INT / BIGINT，含 UNSIGNED。按真实字节宽度解码，正确符号扩展，不经过浮点数。
- DECIMAL，保持 scale，输出 `{"type":"decimal","value":"-12345678.90"}`。
- CHAR / VARCHAR，支持 utf8、utf8mb3、utf8mb4、ascii、latin1。UTF-8 主键在底层 `Decoder` 回调中直接借用事件缓冲区；转为事务持有的 owned 值时才复制。CHAR 移除末尾空格填充。

拒绝联合主键、无主键、FLOAT/DOUBLE 主键、二进制主键、其他主键字符集。JSON 数字能表示 u64，但 JavaScript 消费方直接解析大整数会损失精度，应使用保留大整数的 JSON 解析器，或在自定义输出层转成字符串。

非主键字段支持跳过整数、FLOAT/DOUBLE、DECIMAL、BIT、日期时间及其小数秒、CHAR/VARCHAR/BINARY/VARBINARY、ENUM/SET、BLOB/TEXT、JSON、空间类型、VECTOR。非主键值不解码，因此不会为 JSON/字符串/BLOB 内容分配对象。NULL 位图仍按保存的 schema 校验：NOT NULL 列出现 NULL 时明确报错，不交付事务或持久化检查点。生成列、不可见列、旧 DECIMAL wire 类型和 typed array 明确报错，不推测布局。

## 事务与限制

只有 XID/COMMIT 后输出。ROLLBACK 丢弃缓冲。输出成功且 flush 后才原子替换 state，并同步文件和父目录；文件锁阻止同一 state 的并发消费者。输出或保存失败就停止。崩溃在输出与 state 保存之间会重复投递，消费者须幂等；stdout 的 flush 仅代表写入 OS/管道，**不代表下游持久化确认，不能承诺 exactly-once，也不能承诺下游故障时无丢失**。

遇到任何非 BEGIN/COMMIT/ROLLBACK 的 QUERY_EVENT 都停止，包含其他库的 DDL。这是保守策略，因为跨库 SQL、RENAME、TRUNCATE 无法按默认库安全过滤。DDL 不会自动刷新当前 schema 并继续读取历史流。线上迁移需要消费到 DDL 前，明确处理 schema 版本和 DDL 后位点；另建 state 从当前开始只适用于已确认旧积压处理完毕的切换，不应直接删除旧 state 跳过积压。

XA、部分 JSON 更新、压缩事务、MariaDB 扩展、非可忽略未知事件均拒绝。心跳、GTID、ROTATE、格式描述和普通可忽略事件被处理；恢复方式是 file/position，不是 GTID failover。断线返回错误，可由进程管理器重启后从 state 恢复；不在进程内静默重连。源码布局校验参考 [MySQL 8.4 格式描述源码](https://raw.githubusercontent.com/mysql/mysql-server/mysql-8.4.6/libs/mysql/binlog/event/control_events.cpp)。

## 错误边界

| Canal 错误 | 触发条件 |
| --- | --- |
| `Error::Schema` | 非主键 NOT NULL 列出现 NULL，或 schema 不匹配 |
| `Error::MissingPrimaryKey` | 主键缺失或为 NULL |
| `Error::Invalid` | 畸形生产者版本等非法输入 |
| `Error::Unsupported` | 未支持的厂商、FDE 布局或类型 |
| `Error::Statement` | 非 BEGIN/COMMIT/ROLLBACK 的 QUERY，包括 DDL |

错误停止消费，未确认事务不得推进磁盘检查点。复制库返回独立的 `mysql_replication::Error`；不要混用两层错误类型。验证这些边界的回归入口见[测试指南](testing.zh-CN.md)。
