# 开发指南

[English](development.md) | 简体中文

[文档目录](README.zh-CN.md)

从仓库根目录执行本页命令。需要 Rust 1.89 或更高版本；根目录是纯 Cargo workspace。使用方法见[系统 README](../README.zh-CN.md)，事件契约见[事件参考](events.zh-CN.md)，验证步骤见[测试指南](testing.zh-CN.md)。

## 目录结构

```text
crates/mysql-cdc/
├── src/lib.rs           # Unified CDC and replication exports
└── examples/            # Single-dependency listeners and raw-event readers
crates/pikachu/
├── src/bin/             # pikachu-cdc and its default JSON Lines handle
├── examples/config/     # CLI configuration
└── tests/               # CLI replay tests
crates/mysql-canal/
├── src/config/          # Named sources and CDC policy
├── src/model/           # Exact primary keys, changes and committed transaction views
├── src/protocol/mysql/  # CDC adapters and selective primary-key decoding
├── src/source/          # Source plugins, schema discovery and initial positions
├── src/engine/          # Canal lifecycle, event conversion, replay and checkpoints
├── src/sink/            # Transaction handlers, sinks and JSON Lines output
├── examples/            # CDC listeners, adapters and configuration
├── benches/             # Selective decoding benchmarks
└── tests/               # CDC, conversion and dedicated MySQL tests
crates/mysql-replication/
├── src/events/          # Typed binlog events and opt-in sparse row decoding
├── src/frame.rs         # Raw bytes, bounds and checksums
├── src/parser.rs        # Per-file layouts and configurable lazy parsing
├── src/version.rs       # Shared numeric producer-version validation
├── src/file.rs          # Offline event/frame reader
├── src/stream.rs        # Replication transport and borrowed body views
├── src/primary_key.rs   # Opt-in selected-table primary-key preflight
└── tests/events.rs      # Protocol regression tests
tests/infra/             # Dedicated MySQL test containers
scripts/release/         # Release tag validation and tests
docs/                   # Configuration, events, architecture, development, tests and releases
.github/workflows/       # CI, MySQL compatibility and workspace publishing
```

应用通过 `Canal` 传入 CDC 配置，通过 `TransactionHandler` 注册已提交事务监听。`mysql_cdc_rs::replication::BinlogEvent` 表示一条协议事件，`mysql_cdc_rs::canal::CommittedTransaction` 表示转换并提交后的变更集合。`engine::converter::EventConverter` 明确负责 binlog 视图到行变更及事务动作的转换；`Processor` 缓冲变更并在提交后交付。包含 DDL 的 QUERY 在复制层是有效事件，在 CDC 层触发 `Error::Statement`。

CDC 在线通道使用复制库的 `next_view()` 直接借用驱动 body，不额外组装完整原始缓冲区；离线回放使用 `next_frame()`。两者均在匹配表之后解析 ROWS 镜像，解码精确主键、校验日志字段值，并提供携带历史 SQL 元数据的 before/after 镜像。准确定位字段仍需表布局元数据。所选行镜像模式的检查针对上游配置，复制协议无法从已有 binlog 中按列请求数据。原始事件、解析配置和传输限制见[复制库说明](../crates/mysql-replication/README.zh-CN.md)。

原始行检查使用 `mysql_replication::events::RowsDecoder<M>`，返回携带借用镜像的泛型行操作，独立于 CDC 路径；见[行解码说明](event-api.zh-CN.md#读取携带字段的事件)。

## 性能设计

- TABLE_MAP 先做精确白名单查找。非白名单 ROWS 不解析位图和列值；同一 table_id 重映射会替换旧计划。
- 每张表预处理字段长度规则、主键索引和主键解码函数。整数解码用 const 泛型按宽度/符号实例化，运行时每个主键一次已选定的函数调用，无 `Any`、下转型或类型断言。
- 同一 schema 的 TABLE_MAP 内容相同时复用执行计划；每个 ROWS 事件仅生成一次稀疏镜像计划，NULL 位图按存在列的紧凑序号读取。
- 每行扫描实际存在列以校验主键、约束并解码借用字段；不构造逐行 `Vec<RowValue>`。时间、JSON、空间和向量值保留编码表示。
- 主键以外的值切片均借用原始缓冲区；`changed` 是一份事件内共享的 `Arc<[u16]>`，输出时映射字段名，不为每行复制字段名。
- table_id 用有序 Vec + 二分查找；位图逐字节取 `trailing_zeros` 并清除最低位，支持最多 4096 列。
- 核心代码禁止 `unsafe`；缺失主键、截断、元数据不一致和输出错误均返回 `Result`。

“零拷贝”限定在核心行解码：网络驱动仍需要接收/组装事件，持有跨事件的字符串主键也需要复制。数值主键行的借用回调无需逐行堆分配。命令行为了提交后可见会缓冲轻量 Change；`max_transaction_bytes` 限制其记账内存（含 Vec 预留空间、主键、持有字段字节及共享布局/索引的保守估算，在复制 owned 镜像前检查，不是进程 RSS 硬上限），超限直接停止且不推进位点。

```sh
cargo bench -p mysql-canal --bench minimal_rows
cargo bench -p mysql-canal --bench event_adapter
```

两个基准分别测量直接调用核心 Decoder，以及 EventParser + dyn RowDecoder 的完整适配分发路径；各自测量每事件 1 / 100 / 1000 行的 MINIMAL UPDATE 解码，不含网络、JSON、事务持有与磁盘同步，不可作为端到端吞吐承诺。命令行每个事务都会 flush 输出、写 state 并 fsync；小事务密集场景下，持久化通常比解码更贵。需要更高吞吐可实现 `TransactionSink` 接批量持久化 sink，并由 sink 确认后保存提交位点。多来源同步输出用一个共享锁，每事务只获取一次；慢输出会对各来源施加背压，没有无界消息队列。总事务预算上限是所有来源预算之和，另加驱动缓冲和元数据。源传输、版本适配与行解码器按连接/事件分发，sink 按事务分发；行回调仍保持借用类型。

库的 `Decoder::<M>::new(Vec<TableMeta>)` 接收按列顺序排列的白名单 schema，`M` 为 `Full` 或 `Minimal`。`table_map_event(TableMapEvent)` 预处理映射，`rows_event(RowsEvent, callback)` 交付借用的 `Change`；输入是由绑定 FDE 的 `EventParser` 产生的统一视图。跨事件保存时使用 `Change::into_owned()`。`Canal` 管理来源生命周期和事务处理器，`Processor` 管理提交及持久化，`Runtime` 调度多个来源。实库路径见 `crates/mysql-canal/tests/integration/mysql_live.rs`。

## 新版本与插件接口

插件采用编译期链接 + 运行时注册，全部是可用作 trait object 的 Rust 接口；运行时不加载 dylib。数值主键仍使用精确枚举，字符串借用事件切片，接口没有 `Any` 或 JSON 中间值。

| 接口 | 职责 |
| --- | --- |
| `SourcePlugin` / `SourceSession` / `EventStream` | 建立连接、读取 schema/起点、按借用切片交付事件；可替换驱动或接入特殊版本连接行为 |
| `VersionAdapter` | 匹配版本、校验能力和 FDE 布局、选择位点 SQL、创建事件解析器及行解码器 |
| `EventParser` | 将不同事件结构解析为 `ParsedEvent`：`TableMapEvent`、`RowsEvent` 或控制事件，所有载荷借用输入字节 |
| `RowDecoder` | 对统一视图进行 TABLE_MAP 预处理及 ROWS 解码，回调参数为借用的 `Change<'_>` |
| `TransactionSink` | 一次接收来源 ID、提交 file/position 和整批轻量 `OwnedChange`，成功返回后才保存 state |

`VersionRegistry` 内置 `mysql57`、`mysql80`、`mysql81`、`mysql82`、`mysql83`、`mysql84`、`mysql90`、`mysql97`。连接时按 `SELECT VERSION()` 选择适配器；每个日志文件按其 FDE 中的生产者版本重新选择，因此升级后仍能自动读取旧版日志。5.7/8.0–8.3 用 `SHOW MASTER STATUS`，8.4/9.0/9.7 用 `SHOW BINARY LOG STATUS`。除了版本号，还检查 ROW 与所选 MINIMAL/FULL、压缩/部分 JSON 开关、公共头和相关 post-header 长度，ROTATE 必须精确为 8 字节。原始事件解析与 CDC 共用完整数字 `major.minor.patch` 语法，允许非空的 `-` 或 `+` 后缀；内置解析器与适配器拒绝 MariaDB 生产者标识。自定义适配器可明确校验其他布局。9.7 继续复用严格校验的 binlog v4 布局，参考 [MySQL 9.7.2 格式描述源码](https://raw.githubusercontent.com/mysql/mysql-server/mysql-9.7.2/libs/mysql/binlog/event/control_events.cpp)；实库兼容仍以 CI 结果为准。

未注册的系列拒绝启动；不能仅因为 `major >= 5` 就推断兼容。MySQL 官方版本序列没有 5.8，5.7.8 则属于 5.7 系列，见 [MySQL 官方版本参考](https://downloads.mysql.com/docs/mysqld-version-reference-en.pdf)。如果“5.8”指厂商版本，需根据其真实版本和日志布局实现适配器。

新增版本时，实现 `VersionAdapter` 并调用 `runtime.versions.register(Arc::new(adapter))`。每次收到 FDE 后调用 `create_event_parser`，为这个文件建立独立解析器；`create_decoder` 负责列元数据和字段值编码的处理。完整可编译的接入模板在 `crates/mysql-canal/examples/custom_version.rs`；模板注册本身不代表已验证该新版本。

事件结构变化时覆盖 `create_event_parser`，返回自己的 `EventParser`。解析器直接从原始切片产生统一的 `TableMapEvent` / `RowsEvent`，可处理字段偏移、字段顺序、字节序以及新的事件码；无需重建标准布局的字节数组。控制事件由解析器直接返回 `ParsedEvent::Control`，转换器处理对应事务或日志动作。

默认 `MysqlEventParser` 按 FDE 预处理 TABLE_MAP 的 ID 宽度、ROWS v1/v2 头及 QUERY/ROTATE 的 post-header 长度，建立固定事件分发表。某 ROWS 类型在 FDE 中长度为 0，就拒绝该类型；不会因为记得一个事件码就假设它在该文件中可用。核心行循环只接收统一视图，不读取版本号或 wire event_type；ROW 镜像解析函数在 FDE/事件阶段选定，非白名单表仍在解析 extra-data、位图和值之前直接跳过。

新解析器使用 `RowsEvent::new(..., parse_images)` 提供镜像解析函数，返回借用的 `RowImages`。MySQL 类型编码相同时可继续复用默认 `Decoder`；如果列元数据或字段值编码也不同，覆盖 `create_decoder`，并实现 `RowDecoder::table_map_event` / `rows_event`。`RowDecoder` 必须实现 `table_map_event` 和 `rows_event`；所有 wire 解析均由绑定 FDE 的 `EventParser` 负责。`crates/mysql-canal/tests/integration/event_adapters.rs` 使用合成厂商格式验证 4 字节 ID、扩展控制头、不同字段顺序和字节序、新 ROWS 事件码、文件切换与 UTF-8 主键零拷贝；这些不是实库兼容 fixture。

共同的 binlog v4 帧（19 字节公共头、FDE 引导、CRC32/NONE）仍由传输层负责。当前 MySQL 驱动还会预解析 FDE/TABLE_MAP/ROTATE，因此改变这些结构的厂商版本可能在到达本层前就被驱动拒绝；这种情况需要同时实现 `SourcePlugin` 替换传输。仅注册 `VersionAdapter` 不保证能绕过驱动的布局限制。默认 Oracle MySQL 适配器仍严格校验已支持布局，不能把未知 FDE 的长度变化自动视为安全兼容。

```rust,ignore
let mut runtime = mysql_cdc_rs::canal::engine::Runtime::default();
runtime.versions.register(std::sync::Arc::new(MyVersionAdapter))?;
// runtime.sources.register(Arc::new(MySourcePlugin))?;
runtime.run(config, false, sink)?;
```

默认自动选择；需要覆盖内置同系列时，可在来源配置中设置 `adapter = "your-adapter"`。重叠匹配且没有指定 adapter 会报歧义错误，不依注册顺序悄悄覆盖。固定 adapter 也约束历史 FDE 的版本，跨系列升级恢复通常应保持自动选择并注册所需的各版本。

自定义版本适配器通过 `validate_server(variables, consumer_id, row_image)` 和 `create_decoder(tables, format, row_image)` 声明模式支持。共用 v4 默认实现支持两种模式，额外限制须由适配器显式校验。`RowDecoder` 仅接收统一视图，结构及控制事件分类由 `EventParser` 负责。

## 修改与验证

按所属层修改实现：复制协议放在 replication，转换、事务和恢复放在 canal，产品参数与默认处理器放在 Pikachu。新增集成测试须检查成员的 `autotests = false`，并显式注册测试目标；聚合包示例也须显式注册。首次发布前，公共接口按当前设计整理，同步维护示例和双语文档。四包保持同版，内部路径依赖固定为精确版本。

运行[测试指南](testing.zh-CN.md)中的本地检查。发布脚本、CI 和打包操作见[发布指南](releasing.md)。

## 实现数据流

### Replication：字节到协议事件及原始行

传输入口为 [`ReplicationStream`](../crates/mysql-replication/src/stream.rs)，离线文件入口为 [`BinlogFile`](../crates/mysql-replication/src/file.rs)。

| 读取 API | 返回值 | 解析边界 |
| --- | --- | --- |
| `next_event()` | `BinlogEvent<'_>` | 原始完整字节、公共头和结构化 `EventData`；默认延迟解析 ROWS 镜像 |
| `next_frame()` | `BinlogFrame<'_>` | 已校验的完整原始帧，body 由调用方解释 |
| 网络 `next_view()` | `mysql_replication::EventView<'_>` | 已校验的驱动头/body 视图，避免额外组装一份完整 raw frame |

[`BinlogParser`](../crates/mysql-replication/src/parser.rs) 根据各文件的 FORMAT_DESCRIPTION（FDE）选择布局。结构化事件包括 QUERY、TABLE_MAP、ROWS 及事务/控制事件。QUERY 是包含 SQL 字节的协议事件，其名称不代表 SELECT 结果或 CDC 查询操作。普通 SELECT 不会产生增、改、删 CDC 变更。协议解析可以返回包含 DDL 的 QUERY，CDC 是否在此停止由 canal 负责。

原始行解码由调用方按需启用：

```text
TABLE_MAP ──> RowsDecoder::new ──> compiled column layout
ROWS ───────> RowsEvent::rows ──> Row<M> { operation }
                                      └─> RowImage::fields ──> Field / RowValue
```

[`RowsKind`](../crates/mysql-replication/src/events/rows.rs) 区分 `Write`、`Update` 和 `Delete`。WRITE 只有 after 镜像，UPDATE 配对 before/after 镜像，DELETE 只有 before 镜像。[原始解码器](../crates/mysql-replication/src/events/rows/decoder.rs) 使用从零开始的绝对列序号返回实际存在的字段。缺失列保持缺失，实际存在的 SQL NULL 表示为 `RowValue::Null`；MINIMAL 镜像不能重建完整的新旧行。

TABLE_MAP 提供 wire 布局，原始解码器不猜测缺失的 SQL 符号、列名或字符集。调用方根据 SQL 元数据选择整数的有符号/无符号解释；字符串保留字节，时间、JSON、空间和向量值保留编码字节；精确 DECIMAL 保留 precision 与 scale。原始行解码不要求主键，也不管理事务、输出处理器或检查点。完整用法见[解码示例](../crates/mysql-replication/examples/decode_rows.rs)。

对于 `BinlogParser<Full>` / `BinlogParser<Minimal>`，静态模式策略按来源维护 TABLE_MAP 状态，为 ROWS 绑定历史计划，在重映射时替换并在文件/语句边界清理。`RowsEvent<M>::rows()` 返回 `Row { operation: RowOperation<M::Image> }`，`fields()` 提供列元数据和借用值，FULL/MINIMAL 使用静态分派。显式 `RowsDecoder<M>` 调用方自行维护映射。可选列名、符号、collation 和主键元数据在存在时解码，其他属性保留原始 TLV，缺失属性不会从当前 schema 猜测。原始行迭代可能先返回部分行，再遇到错误，不保证提交可见性。

独立的 `primary_key::validate_primary_keys` 在调用方打开原始传输前按需进行 INFORMATION_SCHEMA 预检，不过滤事件。Canal 在来源层独立预检，校验成功后打开 `ReplicationConnection`；原始解码仍不依赖主键策略。

### Canal：协议视图到已提交 CDC 变更

常规 CDC 路径不经过原始 `RowsDecoder`：

```text
Online:  ReplicationStream::next_view ──> canal EventView
Offline: replication BinlogFile::next_frame ──> canal EventView
                                                     │
                                                     v
                        EventConverter (per source)
                        ├─ FDE: select VersionAdapter, EventParser and RowDecoder
                        ├─ TABLE_MAP: match whitelist and compile table plan
                        ├─ ROWS: emit borrowed Change values
                        └─ control events: return EventAction
                                                     │
                                                     v
                        Processor + TransactionBuffer
                        ├─ retain OwnedChange until commit
                        ├─ rollback: discard buffered changes
                        └─ commit: TransactionSink
                                      └─> Canal handlers ──> save checkpoint
```

[`EventConverter`](../crates/mysql-canal/src/engine/converter.rs) 按各 FDE 的生产者版本选择适配器。[`EventParser`](../crates/mysql-canal/src/protocol/mysql/events/parser.rs) 归一化不同版本的事件布局。[`RowDecoder`](../crates/mysql-canal/src/protocol/mysql/version.rs) 接口及内置 [`Decoder`](../crates/mysql-canal/src/protocol/mysql/decoder.rs) 应用历史 SQL schema 与精确库表白名单。非选中表在行镜像解析前跳过；选中表校验精确主键、NULL 约束、字段编码及字节边界，再按相同的静态 `Decoder<M>` 策略附加借用字段镜像。

`Change<'_>` 包含表元数据、操作、精确主键、可选旧主键、共享变动列索引及 `fields: RowOperation<FieldImage>`。INSERT/UPDATE 索引遵循 after-image 位图，DELETE 的变动字段为空；after-image 中出现的字段不一定与旧值不同。来源身份由事务提供，JSON sink 将列索引映射为字段名。每个已记录字段提供绝对索引、历史 `ColumnMeta` 和 `RowValue`。`Change::into_owned()` 复制紧凑镜像字节并共享不可变布局；事务预算包含镜像字节及保守计入的共享布局，在复制前检查。

转换不等于已提交交付。`EventConverter::convert_binlog` 接收结构化 `BinlogEvent`，将其 frame 转成 CDC 视图后交给 canal 适配器，不消费预解码的原始 `RowValue` 对象。该转换回调可能早于 COMMIT，不缓冲事务，也不丢弃已回调的回滚行。需要事务交付时使用 [`Processor`](../crates/mysql-canal/src/engine/processor.rs) 或 [`Canal`](../crates/mysql-canal/src/engine/canal.rs)。

`Processor` 仅在跨事件保存变更时转成 owned 事务数据。COMMIT/XID 将完整事务交给 `TransactionSink`；`Canal` 的 sink 按注册顺序调用 `TransactionHandler`，传入借用事务数据的 `CommittedTransaction`，所有处理器成功后才保存检查点。完全过滤的提交仍交付空变更列表。回滚丢弃缓冲，可保存回滚位点。DDL 及其他不支持语句以 `Error::Statement` 停止 CDC；错误和未完成事务不会持久化未经确认的提交位点。输出成功后、检查点保存前崩溃可能重放，处理器须容忍重复。

### 启动、恢复与来源隔离

[`Runtime`](../crates/mysql-canal/src/engine/runtime.rs) 调度具名 `[[sources]]`。各来源独立持有连接/会话、历史 schema、TABLE_MAP 计划、事务预算、检查点及 state 文件锁。

1. 校验配置并连接所有来源。内置 [`MysqlSource`](../crates/mysql-canal/src/source/mysql/mod.rs) 检查服务器能力及选中表的当前主键，包括检查点恢复；这次预检发生在 runtime 初始化写入初始检查点之前，Canal 来源在请求 binlog 字节前再次检查指定主键。
2. 获取 state 文件锁。已有检查点须匹配来源 UUID、ID、插件及白名单；从检查点加载历史 schema，不用当前 INFORMATION_SCHEMA 覆盖它。
3. 缺少检查点时，短暂获取全局读锁，捕获选中表 schema 和当前提交位点，保存初始 state。不做全量数据快照，也不提供跨来源的一致初始化快照。
4. 每个来源启动独立 worker，从保存的位点消费，并在各文件 FDE 处重新选择生产者适配器。通过共享 sink 交付完整事务，不同来源的事务处理器不交错；多个实例之间没有全局事件顺序。
5. 任一 worker 失败后，请求其他 worker 在事件之间停止并传播失败。慢处理器施加背压，事务预算限制核算缓冲；不静默重连，不使用无界事件队列。

当前主键预检用于判断启动条件是否满足；历史字节仍按保存的历史 schema 解码，不会在 DDL 边界自动采用新结构。

### Pikachu 与聚合入口

[`pikachu-cdc`](../crates/pikachu/src/bin/pikachu-cdc.rs) 将 `check`、`init`、`run` 和 `run --once` 映射到 `Canal`，消费时注册 [`PrintHandle`](../crates/pikachu/src/bin/print_handle/mod.rs)。处理器委托 canal 的 `JsonLinesSink` 将变更写入 stdout，在持久化检查点前 flush；flush 不代表下游持久化确认。诊断写入 stderr。

`replay` 委托 Canal 的 [`engine::Replay`](../crates/mysql-canal/src/engine/replay.rs)。Replay 负责文件名与事件边界校验、FDE 引导、历史事件跳过及通过 `Processor` 交付事务。它使用传入检查点的历史 schema 和显式 `TransactionSink`，不更新 state 文件，也不查询当前服务器。每条事件（包括提交事件）均先校验位点再处理；后续错误之前可能已有成功交付的提交。CLI 不包含事件循环或协议分派。

协议检查使用 `mysql-replication`，嵌入式已提交 CDC 使用 `mysql-canal`，需要一个依赖覆盖二者时使用 [`mysql-cdc-rs`](../crates/mysql-cdc/src/lib.rs)。聚合库仅重导出 `canal` 和 `replication`，由各模块管理构造、配置和错误类型。消费入口为 `canal::Canal::new`，错误使用 `canal::Error` / `canal::Result`；原始读取器、复制流及其错误类型位于 `replication`。

泛型入口通过共用 `Minimal` / `Full` 类型在实例化时绑定模式。复制库将 `M` 贯穿解析器、读取器、`BinlogEvent`、`EventData`、`RowsEvent` 及 owned 视图；`Event` 保留为共用且对象安全的解码接口。Canal 通过 `Decoder<M>` 提供泛型行解码，`Canal::new(AppConfig)` 统一调度来源，各来源分别配置复制行镜像模式。内置适配器按来源和 FDE 装配 `Decoder<Full>` 或 `Decoder<Minimal>`，通用变更与事务交付继续共用类型。同一消费实例可以包含两种模式，动态适配器接口继续保持对象安全。

### 镜像策略

`mysql_replication::config::RowImageMode` 由 Canal 重导出，在 `[sources.mysql-replication]` 中按来源独立设置，默认 MINIMAL。Runtime 将模式传给在线服务器检查，以及 Processor → EventConverter → VersionAdapter → Decoder<M>；每次 FDE 重建解码器都保留该策略。离线回放显式传入模式。原始 `RowsEvent`、`RowsDecoder` 继续按位图工作，不依赖 CDC 策略。FULL 要求完整镜像并提供全部字段值，同时保留 UTF-8 主键借用和共享 after-image 字段索引。检查点保存历史 schema；模式属于运行参数，不能从当前服务器设置或 FDE 推断历史文件的模式。
