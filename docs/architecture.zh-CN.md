# 架构与事件流

[English](architecture.md) | 简体中文

整套系统名称为 **mysql-cdc**；`mysql-cdc-rs` 是聚合包名称，不是第四个实现层。本文按当前代码梳理职责与数据流。配置、输出语义及兼容性见[项目说明](../README.zh-CN.md)，CI 与发布见[发布指南](releasing.md)。

## 包的分层

箭头表示 Cargo 依赖，从使用方指向提供方：

```text
pikachu (pikachu-cdc) ──> mysql-canal ──> mysql-replication
mysql-cdc-rs ──────────> mysql-canal
mysql-cdc-rs ─────────────────────────> mysql-replication
```

| 包 / Rust 导入路径 | 输入 | 职责与输出 |
| --- | --- | --- |
| `mysql-replication` / `mysql_replication` | MySQL 复制连接或本地 binlog 字节 | 校验帧、解析协议事件，可按需解码原始增、改、删的稀疏行镜像 |
| `mysql-canal` / `mysql_canal` | 借用事件视图、选中表的 schema 和来源配置 | 匹配表、提取精确主键及变动字段、缓冲事务、交付已提交变更并持久化检查点 |
| `pikachu` / 命令 `pikachu-cdc` | CLI 参数及 TOML、state、binlog 文件 | 调用 canal 命令接口，注册默认 JSON Lines 处理器 |
| `mysql-cdc-rs` / `mysql_cdc_rs` | 对两个库 API 的调用 | 根路径及 `canal` 重导出 canal，`replication` 重导出复制库，保留原类型和生命周期 |

`mysql-cdc-rs` 不增加处理阶段；`pikachu` 直接依赖 `mysql-canal`。仓库根目录是 Cargo workspace，不是另一个库包。

## 配置组合

复制库定义 `config::Config` 和 `RowImageMode`，包含连接/TLS 参数、复制 server ID、超时以及 MINIMAL/FULL 选择。Canal 的 `Config::replication` 必须包含该类型，并重导出为 `config::ReplicationConfig`；`SourceConfig` 不重复定义这些字段。来源插件/适配器选择、表白名单、事务预算和检查点路径由 Canal 配置负责。

Pikachu 配置直接包含必填的 `mysql-canal: AppConfig` 子配置；Canal 的每个来源再包含必填的 `mysql-replication` 子配置。Pikachu 根层只定义产品增量项，目前没有额外必填项；各层不重定义下层参数。产品配置示例：

```toml
[mysql-canal]

[[mysql-canal.sources]]
id = "east"
state_file = "east-state.json"

[mysql-canal.sources.source]
plugin = "mysql"

[mysql-canal.sources.mysql-replication]
url_env = "MYSQL_EAST_URL"
server_id = 901001
row_image = "full"
connect_timeout_secs = 10
read_timeout_secs = 90

[[mysql-canal.sources.tables]]
schema = "app"
table = "users"
```

直接使用 Canal 时，独立文件从 `[[sources]]` 开始，每个来源使用 `[sources.mysql-replication]`；Pikachu 文件则在外层嵌入 `mysql-canal`，如上例所示。产品和 Canal 的配置示例分别位于 `crates/pikachu/examples/config/` 与 `crates/mysql-canal/examples/config/`。缺失任何子配置、未知字段或将连接字段放在错误层级均报错。`AppConfig::resolve_paths` 支持嵌入配置，相对路径按最外层 TOML 文件解析；`AppConfig::read` 读取独立 Canal 文件。复制模式同时传入 Canal 的适配器与选择性解码器，使传输和 CDC 使用同一个设置。文件/位点从检查点提供。原始泛型 `Minimal`/`Full` 解码器继续独立校验稀疏/完整镜像，不引入 CDC 主键策略。

[事件字段参考](../crates/mysql-replication/README.zh-CN.md#事件公共字段与模式差异) 区分 `BinlogEvent` 包络、不含字段的 `Event` trait 和 `EventData` body，列出共用头/body 字段、MINIMAL/FULL 镜像类型及访问返回类型，并说明缺少镜像、列未记录和 SQL NULL 的区别。

## Replication：字节到协议事件及原始行

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
ROWS ───────> RowsEvent::rows ──> Row { before, after }
                                      └─> RowImage::fields ──> RowField / RowValue
```

[`RowsKind`](../crates/mysql-replication/src/events/rows.rs) 区分 `Write`、`Update` 和 `Delete`。WRITE 只有 after 镜像，UPDATE 配对 before/after 镜像，DELETE 只有 before 镜像。[原始解码器](../crates/mysql-replication/src/events/rows/decoder.rs) 使用从零开始的绝对列序号返回实际存在的字段。缺失列保持缺失，实际存在的 SQL NULL 表示为 `RowValue::Null`；MINIMAL 镜像不能重建完整的新旧行。

TABLE_MAP 提供 wire 布局，原始解码器不猜测 SQL 符号、列名或字符集。调用方根据 SQL 元数据选择整数的有符号/无符号解释；字符串保留字节，时间、JSON、空间和向量值保留编码字节；精确 DECIMAL 保留 precision 与 scale。原始行解码不要求主键，也不管理事务、输出处理器或检查点。完整用法见[解码示例](../crates/mysql-replication/examples/decode_rows.rs)。

调用方按来源独立维护 TABLE_MAP 计划，替换重映射的 ID，并在文件/语句边界清理计划。原始行迭代可能先返回部分行，再遇到错误，不保证提交可见性。

独立的 [`primary_key`](../crates/mysql-replication/src/primary_key.rs) 辅助模块通过 INFORMATION_SCHEMA 预检指定表。`connect_with_tables` / `from_connection_with_tables` 启用该检查，表列表不用于过滤复制流；普通 `connect` / `from_connection` 不做此预检。这些已有的可选公共 API 继续供原始复制调用方使用。Canal 在来源层独立预检，复用自身 CDC 键解码器的类型规则；校验后通过 `from_connection` 启动复制流。原始行解码器仍不依赖主键策略。

## Canal：协议视图到已提交 CDC 变更

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
                                      └─> Cdc handlers ──> save checkpoint
```

[`EventConverter`](../crates/mysql-canal/src/engine/converter.rs) 按各 FDE 的生产者版本选择适配器。[`EventParser`](../crates/mysql-canal/src/protocol/mysql/events/parser.rs) 归一化不同版本的事件布局。[`RowDecoder`](../crates/mysql-canal/src/protocol/mysql/version.rs) 接口及内置 [`Decoder`](../crates/mysql-canal/src/protocol/mysql/decoder.rs) 应用历史 SQL schema 与精确库表白名单。非选中表在行镜像解析前跳过；选中表仅解码主键，其余值按编码长度跳过，同时校验 NULL 约束与字节边界。

`Change<'_>` 包含表元数据、操作、精确主键、可选旧主键及共享的变动列索引。INSERT/UPDATE 索引遵循 after-image 位图，DELETE 的变动字段为空；after-image 中出现的字段不一定与旧值不同。来源身份由事务提供，JSON sink 将列索引映射为字段名。CDC 不输出完整行对象或非主键字段值。

转换不等于已提交交付。`EventConverter::convert_binlog` 接收结构化 `BinlogEvent`，将其 frame 转成 CDC 视图后交给 canal 适配器，不消费预解码的原始 `RowValue` 对象。该转换回调可能早于 COMMIT，不缓冲事务，也不丢弃已回调的回滚行。需要事务交付时使用 [`Processor`](../crates/mysql-canal/src/engine/processor.rs) 或 [`Cdc`](../crates/mysql-canal/src/cdc.rs)。

`Processor` 仅在跨事件保存变更时转成 owned 事务数据。COMMIT/XID 将完整事务交给 `TransactionSink`；`Cdc` 的 sink 按注册顺序调用 `TransactionHandler`，传入借用事务数据的 `CommittedTransaction`，所有处理器成功后才保存检查点。完全过滤的提交仍交付空变更列表。回滚丢弃缓冲，可保存回滚位点。DDL 及其他不支持语句以 `Error::Statement` 停止 CDC；错误和未完成事务不会持久化未经确认的提交位点。输出成功后、检查点保存前崩溃可能重放，处理器须容忍重复。

## 启动、恢复与来源隔离

[`Runtime`](../crates/mysql-canal/src/engine/runtime.rs) 调度具名 `[[sources]]`。各来源独立持有连接/会话、历史 schema、TABLE_MAP 计划、事务预算、检查点及 state 文件锁。

1. 校验配置并连接所有来源。内置 [`MysqlSource`](../crates/mysql-canal/src/source/mysql/mod.rs) 检查服务器能力及选中表的当前主键，包括检查点恢复；这次预检发生在 runtime 初始化写入初始检查点之前，Canal 来源在请求 binlog 字节前再次检查指定主键。
2. 获取 state 文件锁。已有检查点须匹配来源 UUID、ID、插件及白名单；从检查点加载历史 schema，不用当前 INFORMATION_SCHEMA 覆盖它。
3. 缺少检查点时，短暂获取全局读锁，捕获选中表 schema 和当前提交位点，保存初始 state。不做全量数据快照，也不提供跨来源的一致初始化快照。
4. 每个来源启动独立 worker，从保存的位点消费，并在各文件 FDE 处重新选择生产者适配器。通过共享 sink 交付完整事务，不同来源的事务处理器不交错；多个实例之间没有全局事件顺序。
5. 任一 worker 失败后，请求其他 worker 在事件之间停止并传播失败。慢处理器施加背压，事务预算限制核算缓冲；不静默重连，不使用无界事件队列。

当前主键预检用于判断启动条件是否满足；历史字节仍按保存的历史 schema 解码，不会在 DDL 边界自动采用新结构。

## Pikachu 与聚合入口

[`pikachu-cdc`](../crates/pikachu/src/bin/pikachu-cdc.rs) 将 `check`、`init`、`run` 和 `run --once` 映射到 `Cdc`，消费时注册 [`PrintHandle`](../crates/pikachu/src/bin/print_handle/mod.rs)。处理器委托 canal 的 `JsonLinesSink` 将变更写入 stdout，在持久化检查点前 flush；flush 不代表下游持久化确认。诊断写入 stderr。

`replay` 委托 Canal 的 [`engine::Replay`](../crates/mysql-canal/src/engine/replay.rs)。Replay 负责文件名与事件边界校验、FDE 引导、历史事件跳过及通过 `Processor` 交付事务。它使用传入检查点的历史 schema 和显式 `TransactionSink`，不更新 state 文件，也不查询当前服务器。每条事件（包括提交事件）均先校验位点再处理；后续错误之前可能已有成功交付的提交。CLI 不再包含事件循环或协议分派。

协议检查使用 `mysql-replication`，嵌入式已提交 CDC 使用 `mysql-canal`，需要一个依赖覆盖二者时使用 [`mysql-cdc-rs`](../crates/mysql-cdc/src/lib.rs)。聚合库根路径的 `Error`/`Result` 属于 canal，原始操作使用 `replication::Error`/`replication::Result`。

泛型入口通过共用 `Minimal` / `Full` 类型在实例化时绑定模式。复制库将 `M` 贯穿解析器、读取器、`BinlogEvent`、`EventData`、`RowsEvent` 及 owned 视图；`Event` 保留为共用且对象安全的解码接口。Canal 提供 `Cdc<M>` 和 `Decoder<M>`，固定模式实例在启动前拒绝冲突配置。内置适配器按各 FDE 装配泛型解码器，通用变更与事务交付继续共用类型。默认 `Configured` CDC 入口允许来源混用模式，动态适配器接口继续保持对象安全。

## 镜像策略

`mysql_replication::config::RowImageMode` 由 Canal 重导出，在 `[sources.mysql-replication]` 中按来源独立设置，默认 MINIMAL。Runtime 将模式传给在线服务器检查，以及 Processor → EventConverter → VersionAdapter → Decoder；每次 FDE 重建解码器都保留该策略。离线回放显式传入模式。原始 `RowsEvent`、`RowsDecoder` 继续按位图工作，不依赖 CDC 策略。FULL 要求完整镜像，仍仅解码主键，保留 UTF-8 主键借用和共享 after-image 字段索引。检查点保持 v1 格式与历史 schema；模式属于运行参数，不能从当前服务器设置或 FDE 推断历史文件的模式。
