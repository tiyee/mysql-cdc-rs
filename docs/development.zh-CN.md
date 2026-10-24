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
├── src/cdc.rs           # Cdc and committed transaction handlers
├── src/config/          # Named sources and CDC policy
├── src/model/           # Exact primary keys and lightweight changes
├── src/protocol/mysql/  # CDC adapters and selective primary-key decoding
├── src/source/          # Source plugins, schema discovery and initial positions
├── src/engine/          # Event conversion, transactions, offline replay and checkpoints
├── src/sink/            # Transaction sinks and JSON Lines output
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

应用通过 `Cdc` 传入 CDC 配置，通过 `TransactionHandler` 注册已提交事务监听。`mysql_cdc_rs::replication::BinlogEvent` 表示一条协议事件，`mysql_cdc_rs::CommittedTransaction` 表示转换并提交后的变更集合。`engine::converter::EventConverter` 明确负责 binlog 视图到行变更及事务动作的转换；`Processor` 缓冲变更并在提交后交付。包含 DDL 的 QUERY 在复制层是有效事件，在 CDC 层触发 `Error::Statement`。

CDC 在线通道使用复制库的 `next_view()` 直接借用驱动 body，不额外组装完整原始缓冲区；离线回放使用 `next_frame()`。两者均在匹配表之后解析 ROWS 镜像，只解码主键，其余值按长度跳过。准确定位字段仍需表布局元数据。所选行镜像模式的检查针对上游配置，复制协议无法从已有 binlog 中按列请求数据。原始事件、解析配置和传输限制见[复制库说明](../crates/mysql-replication/README.zh-CN.md)。

原始行检查可按需使用 `mysql_replication::events::RowsDecoder`，返回借用的稀疏 before/after 镜像，独立于 CDC 路径；见[行解码说明](events.zh-CN.md#解码原始行镜像)。

## 性能设计

- TABLE_MAP 先做精确白名单查找。非白名单 ROWS 不解析位图和列值；同一 table_id 重映射会替换旧计划。
- 每张表预处理字段长度规则、主键索引和主键解码函数。整数解码用 const 泛型按宽度/符号实例化，运行时每个主键一次已选定的函数调用，无 `Any`、下转型或类型断言。
- 同一 schema 的 TABLE_MAP 内容相同时复用执行计划；每个 ROWS 事件仅生成一次稀疏镜像计划，NULL 位图按存在列的紧凑序号读取。
- 每行只扫描实际存在的列，用长度前缀跳过其他值。没有 `Vec<RowValue>`，没有整行反序列化。
- 主键以外的值切片均借用原始缓冲区；`changed` 是一份事件内共享的 `Arc<[u16]>`，输出时映射字段名，不为每行复制字段名。
- table_id 用有序 Vec + 二分查找；位图逐字节取 `trailing_zeros` 并清除最低位，支持最多 4096 列。
- 核心代码禁止 `unsafe`；缺失主键、截断、元数据不一致和输出错误均返回 `Result`。

“零拷贝”限定在核心行解码：网络驱动仍需要接收/组装事件，持有跨事件的字符串主键也需要复制。数值主键行的借用回调无需逐行堆分配。命令行为了提交后可见会缓冲轻量 Change；`max_transaction_bytes` 限制其记账内存（含 Vec 预留空间、主键与共享索引的保守估算，不是进程 RSS 硬上限），超限直接停止且不推进位点。

```sh
cargo bench -p mysql-canal --bench minimal_rows
cargo bench -p mysql-canal --bench event_adapter
```

两个基准分别测量直接调用核心 Decoder，以及 EventParser + dyn RowDecoder 的完整适配分发路径；各自测量每事件 1 / 100 / 1000 行的 MINIMAL UPDATE 解码，不含网络、JSON、事务持有与磁盘同步，不可作为端到端吞吐承诺。命令行每个事务都会 flush 输出、写 state 并 fsync；小事务密集场景下，持久化通常比解码更贵。需要更高吞吐可实现 `TransactionSink` 接批量持久化 sink，并由 sink 确认后保存提交位点。多来源同步输出用一个共享锁，每事务只获取一次；慢输出会对各来源施加背压，没有无界消息队列。总事务预算上限是所有来源预算之和，另加驱动缓冲和元数据。源传输、版本适配与行解码器按连接/事件分发，sink 按事务分发；行回调仍保持借用类型。

库接口 `Decoder::new(Vec<TableMeta>)` 接收按列序排列的白名单 schema；`table_map(&[u8])` 预处理表映射；`rows(event_type, &[u8], callback)` 将借用的 `Change` 传给回调。输入都是去掉 19 字节公共头和 checksum 后的事件 body，库不会自行假定完整行镜像。`Change::into_owned()` 用于需要跨缓冲区持有数据的 sink。包含提交管理的接入可以直接使用 `Processor`，真实网络接入见 `source::mysql::MysqlSource` 与 `crates/mysql-canal/tests/integration/mysql_live.rs`，多来源入口为 `Runtime`。

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

事件结构变化时覆盖 `create_event_parser`，返回自己的 `EventParser`。解析器直接从原始切片产生统一的 `TableMapEvent` / `RowsEvent`，可处理字段偏移、字段顺序、字节序以及新的事件码；无需重建标准布局的字节数组。控制事件可直接返回 `ParsedEvent::Control`；`classify` 负责 `Unclassified` 事件的分类，已被结构解析器识别的 QUERY/ROTATE/TABLE_MAP/ROWS 不再调用它。

默认 `MysqlEventParser` 按 FDE 预处理 TABLE_MAP 的 ID 宽度、ROWS v1/v2 头及 QUERY/ROTATE 的 post-header 长度，建立固定事件分发表。某 ROWS 类型在 FDE 中长度为 0，就拒绝该类型；不会因为记得一个事件码就假设它在该文件中可用。核心行循环只接收统一视图，不读取版本号或 wire event_type；ROW 镜像解析函数在 FDE/事件阶段选定，非白名单表仍在解析 extra-data、位图和值之前直接跳过。

新解析器使用 `RowsEvent::new(..., parse_images)` 提供镜像解析函数，返回借用的 `RowImages`。MySQL 类型编码相同时可继续复用默认 `Decoder`；如果列元数据或字段值编码也不同，覆盖 `create_decoder`，并实现 `RowDecoder::table_map_event` / `rows_event`。原始 body 方法处理标准布局；trait 的默认视图实现仅允许标准布局，遇到非标准布局会明确报错。`crates/mysql-canal/tests/integration/event_adapters.rs` 使用合成厂商格式验证 4 字节 ID、扩展控制头、不同字段顺序和字节序、新 ROWS 事件码、文件切换与 UTF-8 主键零拷贝；这些不是实库兼容 fixture。

共同的 binlog v4 帧（19 字节公共头、FDE 引导、CRC32/NONE）仍由传输层负责。当前 MySQL 驱动还会预解析 FDE/TABLE_MAP/ROTATE，因此改变这些结构的厂商版本可能在到达本层前就被驱动拒绝；这种情况需要同时实现 `SourcePlugin` 替换传输。仅注册 `VersionAdapter` 不保证能绕过驱动的布局限制。默认 Oracle MySQL 适配器仍严格校验已支持布局，不能把未知 FDE 的长度变化自动视为安全兼容。

```rust,ignore
let mut runtime = mysql_cdc_rs::engine::Runtime::default();
runtime.versions.register(std::sync::Arc::new(MyVersionAdapter))?;
// runtime.sources.register(Arc::new(MySourcePlugin))?;
runtime.run(config, false, sink)?;
```

默认自动选择；需要覆盖内置同系列时，可在来源配置中设置 `adapter = "your-adapter"`。重叠匹配且没有指定 adapter 会报歧义错误，不依注册顺序悄悄覆盖。固定 adapter 也约束历史 FDE 的版本，跨系列升级恢复通常应保持自动选择并注册所需的各版本。

## 修改与验证

按所属层修改实现：复制协议放在 replication，转换、事务和恢复放在 canal，产品参数与默认处理器放在 Pikachu。新增集成测试须检查成员的 `autotests = false`，并显式注册测试目标；聚合包示例也须显式注册。公共接口变动须评估语义化版本兼容性，并同步示例和双语文档。四包保持同版，内部路径依赖固定为精确版本。

运行[测试指南](testing.zh-CN.md)中的本地检查。发布脚本、CI 和打包操作见[发布指南](releasing.md)。
