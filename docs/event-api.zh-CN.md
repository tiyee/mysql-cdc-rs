# Rust 事件 API 参考

[English](event-api.md) | 简体中文

[文档目录](README.zh-CN.md)

本文供 Rust 接入和协议开发时查阅。事件含义、字段示例和模式对比先看[事件说明](events.zh-CN.md)；本页列出对应的类型、接口和生命周期。

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

以下是 `BinlogEvent<'a, M>` 的全部公共字段，适用于 `M = Minimal` 或 `Full`：

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
| `RowsStatementEnd` / `RowsStatementEndEvent<'a>` | `table_id()`、`flags()`、`kind()`；不包含列或行镜像 |
| `Xid` / `XidEvent` | `xid: u64` |
| `Gtid` 和 `AnonymousGtid` / `GtidEvent` | `flags: u8`、`sid: [u8; 16]`、`gno: i64`，以及下述可选元数据 |
| `TaggedGtid` / `TaggedGtidEvent<'a>` | `gtid: GtidEvent`、`tag: &'a str`、`unknown_fields: &'a [u8]` |
| `PreviousGtids` / `PreviousGtidsEvent<'a>` | `sid_count: u64`、`encoded_sids: &'a [u8]` |
| `Heartbeat` / `HeartbeatEvent<'a>` | `log_file: &'a [u8]` |
| `Raw` / `RawEvent<'a>` | `data: &'a [u8]`，保留未知或有意不解码的 body |

`GtidEvent` 的可选字段包括：`last_committed` / `sequence_number`（`Option<i64>`）；`immediate_commit_timestamp` / `original_commit_timestamp` / `transaction_length` / `commit_group_ticket`（`Option<u64>`）；`immediate_server_version` / `original_server_version`（`Option<u32>`）。它们是否存在取决于生产者元数据和解析选项，与行镜像模式无关；`ParserConfig` 禁用结构化 GTID 解码时，也可将其保留为 `Raw`。

`TableMapEvent::columns()` 返回 `TableColumns { count: usize, types, metadata, nullable, optional_metadata }`，后四项均为借用字节切片。`PreviousGtidsEvent::sids()` 返回 `SidIntervals { sid, intervals }`，两项也均为借用字节切片。这些访问接口在两种模式下相同。`RowsEvent.flags` 是 ROWS body 标志，与 `BinlogEvent.header.flags` 相互独立。

### 由泛型模式决定的字段类型与接口

读取前显式选择 `Full` 或 `Minimal`，canal 重导出同一组标记类型。parser、reader、事件包络、ROWS 载荷和镜像贯穿同一个 `M`，使用静态 trait 分派，不使用运行时模式枚举或逐行模式判断。

| API / 属性 | `Minimal` | `Full` |
| --- | --- | --- |
| 包络 / body 类型 | `BinlogEvent<'a, Minimal>` / `EventData<'a, Minimal>` | `BinlogEvent<'a, Full>` / `EventData<'a, Full>` |
| `RowsEvent<M>::rows()` | `Result<Rows<'a, Minimal>>` | `Result<Rows<'a, Full>>` |
| 迭代器元素 | `Result<Row<'a, Minimal>>` | `Result<Row<'a, Full>>` |
| `Row.operation` | `RowOperation<SparseImage<'a>>` | `RowOperation<CompleteImage<'a>>` |
| `image.field(index)` | `Result<Option<Field<'_, 'a>>>`；未记录为 `None` | `Result<Field<'_, 'a>>`；范围内列必定存在 |
| `image.fields()` | 按表列顺序遍历实际记录字段 | 按表列顺序遍历全部字段 |
| `image.columns()` | 全部表列定义，包括未记录的字段 | 全部表列定义 |
| 索引越界 | `Error::Invalid` | `Error::Invalid` |

`RowOperation<I>` 包含 `Insert { after: I }`、`Update { before: I, after: I }`、`Delete { before: I }`，类型上排除缺失必需镜像的状态。`before()` / `after()` 借用适用的镜像；`into_images()` 消费操作并返回对应的可选镜像对。

实际存在的 SQL NULL 表示为 `Field { value: RowValue::Null, .. }`。MINIMAL 的 `field(i)? == None` 只表示合法表列未记录，不会填充 NULL、默认值或服务器当前值。MINIMAL 同样接受完整位图；不能根据日志字节反推生产者配置。

`RowsEvent<M>::table()`、`columns()` 通过 `Result` 返回绑定的历史列定义。parser 按来源独立维护 TABLE_MAP，每次重映射都替换旧计划，即使 ID 和列数未变。FDE、ROTATE、QUERY、XID 和 ROWS 的语句结束标志会清理映射；最后一个普通行事件仍持有自己的计划。缺失 TABLE_MAP 或列数不匹配明确报错。构造时显式选择 `M`，reader 不提供运行中切换模式的接口。

`EventData::RowsStatementEnd` 单独表示特殊的语句结束哨兵，载荷提供 `table_id()`、`flags()`、`kind()`，没有列定义和行迭代器。它无需 TABLE_MAP 即可清理映射。两种模式都通过 `Error::Unsupported` 拒绝部分 JSON 行更新事件；原始帧和网络视图接口仍可读取其字节。

### 列元数据与借用字段值

`TableDefinition` 持有 `schema: Box<[u8]>`、`table: Box<[u8]>`、共享的 `columns: Arc<[ColumnInfo]>`，以及原始可选元数据 TLV `optional_metadata: Box<[u8]>`。每个 `ColumnInfo` 包含：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `index` | `u16` | 从零开始的表列绝对索引 |
| `name` | `Option<Box<str>>` | 日志记录的 UTF-8 列名 |
| `definition` | `ColumnDefinition` | 已校验的 wire 类型、实际类型、可空性和编码参数 |
| `unsigned` | `Option<bool>` | 日志记录的数字符号属性；`None` 表示未知 |
| `collation_id` | `Option<u32>` | 日志记录的字符排序规则 ID |
| `primary_key` | `Option<bool>` | 日志记录的主键成员关系；缺失主键元数据时保持未知 |
| `primary_key_prefix` | `Option<usize>` | 字符前缀长度；零表示使用整个主键列 |

`Field<'meta, 'event>` 包含 `column: &'meta ColumnInfo` 和 `value: RowValue<'event>`。事件和镜像通过 `Arc` 共享定义，行值借用事件字节。元数据借用不能超过镜像生命周期，字段值借用不能超过事件缓冲区生命周期。`into_owned()` 保存一份原始缓冲区及共享 TABLE_MAP 计划，`view()` 无需查询服务器或构造自引用即可重建借用字段，并保留实际 checksum。

列元数据与镜像完整性独立：`binlog_row_image=FULL` 不保证列名齐全，支持该设置的服务器通过 `binlog_row_metadata=FULL` 记录更多属性。缺失属性保持 `None`；畸形列名、符号、字符集或主键元数据明确报错。未知 TLV 保留在 `optional_metadata`，本 API 不展开 ENUM/SET 标签等其他属性。参见 [MySQL 元数据设置](https://dev.mysql.com/doc/refman/8.4/en/replication-options-binary-log.html#sysvar_binlog_row_metadata)。

整数保留实际 wire 宽度，根据已知元数据调用 `signed()` 或 `unsigned()`。DECIMAL 保留精确 precision 与 scale。文本/BLOB 保留借用字节；时间、JSON、空间和向量值保留编码字节，不隐式转换字符集或构造 JSON 对象。

## 读取携带字段的事件

```rust,no_run
use mysql_replication::{EventData, Full, Result, file::BinlogFile, parser::ParserConfig};
use std::{fs::File, io::BufReader};

fn inspect() -> Result<()> {
    let mut reader = BinlogFile::<_, Full>::new(
        BufReader::new(File::open("binlog.000001")?),
        ParserConfig::default(),
    )?;
    while let Some(event) = reader.next_event()? {
        if let EventData::Rows(rows) = event.event {
            for row in rows.rows()? {
                let (before, after) = row?.operation.into_images();
                for image in before.into_iter().chain(after) {
                    for field in image.fields() {
                        let field = field?;
                        println!(
                            "{} {:?}: {:?}",
                            field.column.index, field.column.name, field.value
                        );
                    }
                }
            }
        }
    }
    Ok(())
}
```

将同一 reader 的模式改为 `Minimal` 后产生 `SparseImage`。字段访问契约由编译器检查：

```rust,no_run
use mysql_replication::{
    Result,
    events::{CompleteImage, Field, SparseImage},
};

fn full(image: &CompleteImage<'_>) -> Result<()> {
    let field: Field<'_, '_> = image.field(0)?;
    println!("{:?}: {:?}", field.column.name, field.value);
    Ok(())
}

fn minimal(image: &SparseImage<'_>) -> Result<()> {
    let field: Option<Field<'_, '_>> = image.field(0)?;
    if let Some(field) = field {
        println!("{:?}: {:?}", field.column.name, field.value);
    }
    Ok(())
}
```

每个 ROWS 迭代器只编译一次稀疏镜像计划，各行共享字段索引，支持超过 64 列的表。`field(index)` 顺序扫描到指定列，连续访问优先使用 `fields()`。每个操作在交付前校验完整行边界、字段长度、NULL 约束及压缩 DECIMAL 数字组。后部错误只返回一次，随后结束迭代；此前的行可能已被消费。replication 不缓冲事务。

### 按模式读取具体行操作

以下代码读取[两种事件内容示例](events.zh-CN.md#minimal)中的四列表（`0:id`、`1:name`、`2:age`、`3:status`），显式接收不同的 `BinlogEvent` 类型。MINIMAL 的字段访问返回 `Option<Field>`，FULL 返回 `Field`；模式由函数参数确定，不在行循环中判断。

```rust,no_run
use mysql_replication::{
    BinlogEvent, EventData, Full, Minimal, Result,
    events::{Field, RowOperation, RowValue},
};

fn read_minimal(event: &BinlogEvent<'_, Minimal>) -> Result<()> {
    if let EventData::Rows(rows) = &event.event {
        for row in rows.rows()? {
            if let RowOperation::Update { before, after } = row?.operation {
                let name: Option<Field<'_, '_>> = after.field(1)?;
                println!("before id: {:?}, after name: {:?}", before.field(0)?, name);
                match after.field(2)? {
                    None => println!("age was not logged"),
                    Some(Field {
                        value: RowValue::Null,
                        ..
                    }) => println!("age is SQL NULL"),
                    Some(field) => println!("age: {:?}", field.value),
                }
                println!("after status: {:?}", after.field(3)?);
            }
        }
    }
    Ok(())
}

fn read_full(event: &BinlogEvent<'_, Full>) -> Result<()> {
    if let EventData::Rows(rows) = &event.event {
        for row in rows.rows()? {
            if let RowOperation::Update { before, after } = row?.operation {
                let name: Field<'_, '_> = after.field(1)?;
                println!(
                    "before name: {:?}, after name: {:?}",
                    before.field(1)?,
                    name
                );
                println!("after age: {:?}", after.field(2)?.value);
                println!("after status: {:?}", after.field(3)?.value);
            }
        }
    }
    Ok(())
}
```

上例 MINIMAL 的 `after name` 有值、`age is SQL NULL`、`after status` 为 `None`；FULL 的 after name、age 和 status 都是已记录字段，age 的值为 `RowValue::Null`。`before` / `after` 仅在 UPDATE 变体中成对出现，INSERT 和 DELETE 的字段形状见[行操作对照](events.zh-CN.md#三种行操作与两种镜像类型)。

## 原始传输与显式协议布局

`file::FrameReader<R>::new(reader)` 通过 `next_frame()` 返回已校验帧，不携带字段模式。`stream::ReplicationConnection` 提供原始网络 `next_frame()` / 借用的 `next_view()`；自定义适配器使用这些传输接口并自行解析结构。类型化 `BinlogFile<R, M>` 和 `ReplicationStream<M>` 只提供 `next_event()`，构造时必须显式指定 `M`。

对于调用方已经校验的独立布局，可使用 `RowsEvent::<M>::with_layout(kind, post_header, v2)` 加 `Event::decode(body)` 解析 ROWS 包络，再调用 `RowsDecoder::<M>::new(table_map)?.rows(&event)`，显式提供 TABLE_MAP。该路径与自动绑定使用同一组 `Row<M>`、`Rows<M>`、`Field` 和镜像类型，不提供另一套事件表示或跨模式转换。原始解码不要求主键；Canal 保留自身适配器和历史 SQL schema，并为统一镜像复用借用字段解码。

```sh
cargo run -p mysql-replication --example decode_rows -- /path/to/binlog.000001 --row-image full
cargo run -p mysql-replication --example decode_rows -- /path/to/binlog.000001 --row-image minimal
```

唯一的行解码示例只在命令行入口选择一次泛型实例，行解码使用静态分派。

## Canal 通用事件字段

`Change` 是转换回调中的借用视图；`into_owned()` 生成可跨事件持有的 `OwnedChange`。两者字段含义相同：

| 字段 | `Change<'a>` | `OwnedChange` | 含义 |
| --- | --- | --- | --- |
| `table` | `&'a Arc<TableMeta>` | `Arc<TableMeta>` | 历史库表及有序列元数据 |
| `operation` | `Operation` | `Operation` | `Insert`、`Update` 或 `Delete` |
| `pk` | `PrimaryKey<'a>` | `PrimaryKey<'static>` | 精确主键；主键变更时为新值 |
| `previous_pk` | `Option<PrimaryKey<'a>>` | `Option<PrimaryKey<'static>>` | 主键变更时的旧值 |
| `changed` | `&'a Arc<[u16]>` | `Arc<[u16]>` | 按表顺序排列、共享的绝对列索引 |
| `fields` | `RowOperation<FieldImage<'a>>` | `RowOperation<OwnedFieldImage>` | 日志 before/after 值及历史列定义 |

`PrimaryKey` 变体为 `Signed(i64)`、`Unsigned(u64)`、`Text(Cow<'a, str>)` 和 `Decimal(String)`。DECIMAL 不经过浮点转换。`OwnedChange::changed_names()` 通过 `table` 将索引映射为字段名。

| `CommittedTransaction<'a>` 字段 | 类型 | 含义 |
| --- | --- | --- |
| `source_id` | `&'a str` | 配置中的稳定来源身份 |
| `checkpoint` | `&'a State` | 所有处理器成功后才保存的提交边界 |
| `changes` | `&'a [OwnedChange]` | 按 binlog 顺序排列的变更；完全过滤的提交为空 |

`Canal::new(AppConfig)` 创建多来源消费实例，各来源独立配置复制行镜像模式；适配器在 FDE 边界构造 `Decoder<Full>` 或 `Decoder<Minimal>`，行解码使用静态分派。

`State` 携带历史 schema、文件/位点及来源 UUID、ID、插件。事务数据只在回调期间借用，需要跨回调持有时才克隆。`EventConverter` 的回调可能发生在 COMMIT 之前；提交交付应使用 `Canal` 或 `Processor`。

MINIMAL 与 FULL 使用相同的通用类型和字段。MINIMAL 仅提供已记录字段，FULL 的每份适用镜像提供全部列。泛型解码器在行回调前校验模式策略，多来源通用回调无需运行时模式判断。

JSON 输出、数据类型支持及提交和失败行为见[事件说明](events.zh-CN.md)。

### 读取 Canal 字段

`FieldImage` 和 `OwnedFieldImage` 提供 `table()`、`columns()`、`column_indices()`、`fields()` 和 `field(index)`。`Field` 包含 `index: u16`、`column: &ColumnMeta` 及 `value: RowValue`。索引为历史表的绝对列序号；`field(index)` 返回 `Result<Option<Field>>`，仅未记录列为 `None`，越界报错。SQL NULL 为 `RowValue::Null`。INSERT 只有 after，UPDATE 两者都有，DELETE 只有 before，由 `RowOperation` 变体表达。

转换 `Change` 借用事件值；`into_owned()` 仅复制每份镜像的紧凑 NULL 位图和编码值，共享不可变布局及历史列定义。提交回调接收 `OwnedChange`，读取值时借用该变更，需要跨回调保留时才克隆。输入缓冲区复用后镜像仍然有效。

原始 `RowsDecoder<M>::rows_from_images(kind, images)` 接收结构适配器已归一化的 `RowImages`，使用与 `rows(&event)` 相同的模式校验和操作类型。原始 `RowImage::into_owned()` 返回 `OwnedRowImage`，通过 `view()` 和 `fields()` 读取持有字节，无需重建完整事件缓冲区。`retained_bytes()` 估算镜像字节及共享布局成本，不含共享表定义。

序列化契约和两种模式见 [Canal / CLI JSON](events.zh-CN.md#canal--cli-json)。

```rust,no_run
use mysql_canal::{OwnedChange, Result};

fn inspect(change: &OwnedChange) -> Result<()> {
    for (side, image) in [
        ("before", change.fields.before()),
        ("after", change.fields.after()),
    ] {
        if let Some(image) = image {
            for field in image.fields() {
                let field = field?;
                println!(
                    "{} [{}] {} {}: {:?}",
                    side, field.index, field.column.name, field.column.column_type, field.value
                );
            }
        }
    }
    Ok(())
}
```

### 嵌入其他产品

产品直接包含 Canal 的 `AppConfig`，反序列化后调用 `app.resolve_paths(parent_toml_path)` 校验并解析相对路径。`AppConfig::read` 只读取从 `[[sources]]` 开始的独立 Canal 文件。

## Replication 连接与解析 API

Rust 配置中，`Config::replication` 使用复制库的 `mysql_replication::config::Config`，Canal 将其重导出为 `config::ReplicationConfig`；`config::RowImageMode` 同样重导出复制库类型。`SourceConfig` 仅包含 `plugin` 和 `adapter`。

`config::Config` 定义可复用的连接与订阅设置，支持 `config::RowImageMode::{Minimal, Full}`，默认 MINIMAL。该类型校验上游模式，不修改 MySQL，并构造底层连接/流配置。Canal 的必填 `[sources.mysql-replication]` 直接包含该类型。`connect_timeout_secs` 默认 10 秒，`read_timeout_secs` 默认 90 秒，均拒绝零值；`tls_ca` 可选，相对路径由嵌入应用解析。binlog 文件与位点由消费方在运行时提供。

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

需要显式选择解析方式或可选表预检时，仍可直接使用底层 API：

```rust,no_run
use mysql_replication::{
    Result,
    primary_key::validate_primary_keys,
    stream::{ConnectionConfig, ReplicationConfig, ReplicationConnection, connect},
};

fn main() -> Result<()> {
    let connection = ConnectionConfig::new("MYSQL_URL");
    let mut conn = connect(&connection)?;
    validate_primary_keys(&mut conn, [("app", "users"), ("app", "orders")])?;
    let config = ReplicationConfig::new(123, "binlog.000001", 4);
    let mut stream = ReplicationConnection::from_connection(conn, config)?;
    while let Some(view) = stream.next_view()? {
        // Borrowed body: no second raw-frame copy or row-value decoding.
        println!("{} {}", view.header.event_type, view.body.len());
    }
    Ok(())
}
```

连接配置支持凭据环境变量、TLS CA 和超时；复制配置支持 server ID、file/position、阻塞模式、上游行镜像要求及解析选项。行镜像要求只校验，不修改服务器全局设置。MySQL 发送已经存在的 binlog 字节，复制协议不支持按列裁剪网络传输。

`primary_key::validate_primary_keys(&mut conn, tables)` 在调用方构造传输前按需执行 INFORMATION_SCHEMA 预检。选中表需要支持的单列 PRIMARY KEY；缺失/联合主键及不支持的类型或字符集明确报错。表列表只控制预检，不过滤事件；原始传输和行解码本身不要求主键。Canal 自行负责其启动可用性检查。

- `next_event()` 返回具体协议事件和完整原始字节。
- `next_frame()` 校验长度/checksum，保留原始字节，事件体留给自定义适配器。
- 在线 `next_view()` 校验驱动事件并直接借用 body，适合不需要完整原始帧的消费者。
- `ParserConfig { decode_gtids: false, decode_previous_gtids: false }` 将相关事件体保留为原始字节。
- 类型化 parser 在事件阶段绑定 TABLE_MAP 并校验位图，字段值保持借用，在迭代时校验。
- `TableMapEvent::columns()` 提供 wire 元数据；类型化 parser 在返回 TABLE_MAP 前编译列定义。

## 协议与传输边界

结构化解析器根据每个文件的 FDE 选择声明的布局，支持已注册 MySQL 5.7、8.0–8.4、9.0、9.7 系列。生产者版本须包含完整数字 `major.minor.patch`，允许非空的 `-` 或 `+` 后缀（如 `5.7.44-log` 或 `8.0.45-0ubuntu0.24.04.1`）。畸形版本与 MariaDB 标识明确拒绝，数字版本解析规则与 CDC 共用。未知系列或布局明确失败；`next_frame()` 可交给自定义事件体解析器。混用帧/视图与结构化读取时，需要将读取到的 FDE 帧交给自己的 `BinlogParser`；内置结构化解析器只观察 `next_event()` 调用。

离线文件必须保留开头 FDE，支持 CRC32/NONE，截断尾部和超过 64 MiB 的事件报错。在线传输复用 `mysql` 驱动；驱动会预解析 FDE/TABLE_MAP/ROTATE，并在本库校验事件大小之前分配内存，特殊 framing 可能需要其他传输实现。完整在线原始事件保留驱动读取的原头部字段、body、算法字节及存储的 checksum，不调用会重新计算 checksum 的序列化方法。`next_view()` 完全跳过这次原始帧组装。断线传播错误，不静默重连。

## 错误边界

| Canal 错误 | 触发条件 |
| --- | --- |
| `Error::Schema` | 非主键 NOT NULL 列出现 NULL，或 schema 不匹配 |
| `Error::MissingPrimaryKey` | 主键缺失或为 NULL |
| `Error::Invalid` | 畸形生产者版本等非法输入 |
| `Error::Unsupported` | 未支持的厂商、FDE 布局或类型 |
| `Error::Statement` | 非 BEGIN/COMMIT/ROLLBACK 的 QUERY，包括 DDL |

错误停止消费，未确认事务不得推进磁盘检查点。复制库返回独立的 `mysql_replication::Error`；不要混用两层错误类型。验证这些边界的回归入口见[测试指南](testing.zh-CN.md)。

## 非主键字段支持

非主键字段镜像支持整数、FLOAT/DOUBLE、DECIMAL、BIT、日期时间及其小数秒、CHAR/VARCHAR/BINARY/VARBINARY、ENUM/SET、BLOB/TEXT、JSON、空间类型、VECTOR。非主键值以借用 `RowValue` 视图提供；二进制 JSON、时间、空间和向量内容保持编码字节，不构造应用对象。NULL 位图仍按保存的 schema 校验：NOT NULL 列出现 NULL 时明确报错，不交付事务或持久化检查点。生成列、不可见列、旧 DECIMAL wire 类型和 typed array 明确报错，不推测布局。
