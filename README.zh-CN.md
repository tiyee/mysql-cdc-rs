# mysql-cdc-rs

[English](README.md) | 简体中文

`mysql-cdc-rs`（导入路径 `mysql_cdc_rs`）作为统一库入口，重导出 `mysql-canal` 和 `mysql-replication`。`pikachu` 提供 CLI（命令名 `pikachu-cdc`），依赖独立的 CDC 公共库 `mysql-canal`（导入路径 `mysql_canal`）。底层包 `mysql-replication` 提供协议事件和复制传输，不依赖白名单、业务 schema、事务 sink 或检查点策略。CDC 只保留白名单表的**库名、表名、单列主键、变动字段名**，不构建整行值。内置 Oracle MySQL 5.7 / 8.0 / 8.1–8.4 / 9.0 / 9.7 的 `ROW + MINIMAL` 版本适配器，支持多个实例并行消费。实库兼容矩阵需单独运行。

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
├── src/source/          # Source plugins and historical schema discovery
├── src/engine/          # Event conversion, transactions and checkpoints
├── src/sink/            # Transaction sinks and JSON Lines output
├── examples/            # CDC listeners, adapters and configuration
├── benches/             # Selective decoding benchmarks
└── tests/               # CDC, conversion and dedicated MySQL tests
crates/mysql-replication/
├── src/events/          # Event trait, common header and concrete binlog events
├── src/frame.rs         # Raw bytes, bounds and checksums
├── src/parser.rs        # Per-file layouts and configurable lazy parsing
├── src/file.rs          # Offline event/frame reader
├── src/stream.rs        # Replication transport and borrowed body views
└── tests/events.rs      # Protocol regression tests
tests/infra/            # Dedicated MySQL test containers
scripts/release/         # Release tag validation and tests
docs/releasing.md        # CI and publishing guide (Chinese)
.github/workflows/       # CI, MySQL compatibility and workspace publishing
```

应用通过 `Cdc` 传入 CDC 配置，通过 `TransactionHandler` 注册已提交事务监听。`mysql_cdc_rs::replication::BinlogEvent` 表示一条协议事件，`mysql_cdc_rs::CommittedTransaction` 表示转换并提交后的变更集合。`engine::converter::EventConverter` 明确负责 binlog 视图到行变更及事务动作的转换；`Processor` 缓冲变更并在提交后交付。包含 DDL 的 QUERY 在复制层是有效事件，在 CDC 层触发 `Error::Statement`。

CDC 在线通道使用复制库的 `next_view()` 直接借用驱动 body，不额外组装完整原始缓冲区；离线回放使用 `next_frame()`。两者均在匹配表之后解析 ROWS 镜像，只解码主键，其余值按长度跳过。准确定位字段仍需表布局元数据。MINIMAL 检查针对上游配置，复制协议无法从已有 binlog 中按列请求数据。原始事件、解析配置和传输限制见[复制库说明](crates/mysql-replication/README.zh-CN.md)。

## 运行

从 workspace 构建 CLI：`cargo build --release --locked -p pikachu`；发布后可使用 `cargo install pikachu` 安装。库接入只需依赖 `mysql-cdc-rs`：

```toml
[dependencies]
mysql-cdc-rs = "0.0.4"
```

```rust,no_run
use mysql_cdc_rs::{CommittedTransaction, Cdc, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let config = AppConfig::read(Path::new("cdc.toml"))?;
    let mut replication = Cdc::new(config)?;
    replication.add_handle(|event: CommittedTransaction<'_>| {
        for change in event.changes {
            println!("{} {:?} {:?}", event.source_id, change.operation, change.pk);
        }
        Ok(())
    });
    replication.run()
}
```

CDC 类型和模块同时从根路径（如 `mysql_cdc_rs::Cdc`、`mysql_cdc_rs::config`）及 `mysql_cdc_rs::canal` 导出。原始事件和传输使用 `mysql_cdc_rs::replication`，如 `replication::BinlogEvent`、`replication::stream::ReplicationStream`。重导出保留原类型，协议事件可直接传给 CDC 转换器。根路径的 `Error`/`Result` 属于 CDC 层，原始复制使用 `replication::Error`/`replication::Result`。底层两个库仍可独立依赖，见[聚合包示例](crates/mysql-cdc/README.zh-CN.md)。

`Cdc::new` 也支持传入程序构造的 `AppConfig`。`add_handle` 接受闭包或 `TransactionHandler` 实现，多个 handle 按注册顺序执行。`CommittedTransaction` 包含来源 ID、事务提交位点及有序的 `OwnedChange` 变更列表；只在 COMMIT/XID 后回调，回滚与未完成事务不触发回调，完全被白名单过滤的提交收到空列表。全部 handle 成功后才保存检查点；失败停止消费，重启后可能重放先前 handle 已处理的事务，处理逻辑应容忍重放。回调借用事务数据，仅在需要跨回调持有时复制；不同来源的回调不交错，慢 handle 对消费施加背压。调用 `run()` 或 `run_once()` 前至少注册一个 handle，库没有默认输出；`run_once()` 消费当前可用事件后退出。完整示例见 [listen.rs](crates/mysql-canal/examples/listen.rs)。

CLI 注册默认 `PrintHandle`，将事件打印为 stdout JSON Lines；命令名继续使用 `pikachu-cdc`。使用 `crates/pikachu/examples/config/multi-source.toml`，每个 `[[sources]]` 都有独立 `id`、`state_file`、`source` 和 `tables`：

```sh
cargo build --release --locked
cp crates/pikachu/examples/config/multi-source.toml cdc.toml
# Set each source's [[sources.tables]] to exact database and table names, respecting server case rules.
export MYSQL_EAST_URL='mysql://cdc:password@east.example:3306'
export MYSQL_WEST_URL='mysql://cdc:password@west.example:3306'
./target/release/pikachu-cdc check cdc.toml
./target/release/pikachu-cdc run cdc.toml > changes.jsonl
```

配置统一使用 `[[sources]]` 列表；只连接一个实例时也使用一个具名条目。每个来源必须显式填写 `id` 和凭据环境变量 `sources.source.url_env`。来源的 ID 和 state 路径必须各不相同，state 的父目录须已存在；指向同一实例 UUID 的多个订阅还必须使用不同复制 `server_id`。不同实例可以复用复制 ID，但统一分配更便于运维。每个来源单独持有线程、连接、schema、table_id 映射、事务预算和提交位点。一个来源不能借用另一个来源的 TABLE_MAP，跨来源也没有全局事务或全局顺序。

任一 worker 失败，进程通知其他 worker 在事件之间停止并返回非零状态；当前阻塞的 MySQL 读取最长等待其 90 秒 read timeout。重启后每个来源从自己的已提交位点继续。各来源初始化逐个获取读锁，不提供跨实例一致性快照；若初始化中途失败，已有成功的 state 会保留，下一次使用 `run` 继续准备剩余来源。

需要 Rust 1.89+。`check` 检查服务器配置、表引擎及主键；字段的实际 wire 类型/长度在 TABLE_MAP 到达时进一步校验。配置相对路径以 TOML 所在目录为基准。凭据从环境变量读取，特殊字符应按 MySQL URL 规则编码；`source.tls_ca` 可配置 CA 证书，启用证书与域名验证。

初次 `run` 会短暂执行 `FLUSH TABLES WITH READ LOCK`，在锁内读取白名单 schema 和当前提交位点，解锁后开始消费。**它从当前位点开始，不做全量数据快照**。也可以先运行 `init cdc.toml` 生成起点，稍后 `run`；已经存在的 state 不会被 `init` 覆盖。`run cdc.toml --once` 消费当前可用事件后退出，适合补采与测试。

再次运行从 state 的提交位点恢复。state 必须包含非空的 `source_uuid`、`source_id` 和 `source_plugin`，同时保存该位点对应的列顺序、主键类型、字符集；恢复时使用保存的 schema，不能拿当前 INFORMATION_SCHEMA 去解释历史事件。服务器 UUID、来源 ID、来源插件和白名单必须与 state 一致。必须保留起点之后的 binlog。

服务器配置示例（5.7 没有后两个变量，不必配置）：

```ini
[mysqld]
server-id=1
log-bin=binlog
binlog-format=ROW
binlog-row-image=MINIMAL
binlog-transaction-compression=OFF
binlog-row-value-options=
```

写入连接也必须使用 `ROW + MINIMAL`，只改 GLOBAL 不会改变已有会话的 SESSION 值。程序会检查全局配置，并拒绝白名单表不符合 MINIMAL 的 before image。CDC 的 `server_id` 必须非零且与源和其他复制消费者不同。

权限示例，请由管理员将地址、密码和库替换为实际值：

```sql
CREATE USER 'cdc'@'127.0.0.1' IDENTIFIED BY 'replace-me';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'127.0.0.1';
GRANT SELECT ON app.* TO 'cdc'@'127.0.0.1';
-- Initialization uses a global read lock; RELOAD can be granted on MySQL 5.7/8.x.
GRANT RELOAD ON *.* TO 'cdc'@'127.0.0.1';
```

全局读锁会短暂阻塞写入，并可能等待正在执行的语句；应在合适的初始化窗口执行。之后的正常恢复不再获取全局读锁。白名单表必须是 InnoDB，避免把非事务表的变化误当作可回滚事务。

## 输出语义

每行一条 JSON，同一来源按 binlog 顺序输出；不同来源按事务写入顺序合流，同一事务的行不会被其他来源插入。UPDATE 示例：

```json
{"source":"east","schema":"app","table":"users","operation":"update","pk":{"type":"unsigned","value":42},"changed":["name","age"]}
```

`source` 是配置中的稳定来源 ID，防止不同实例相同库表/主键混淆。`operation` 区分 insert/update/delete。主键变更时 `pk` 是新值，附带 `previous_pk`，消费者可以同时处理旧键与新键。INSERT 的 `changed` 是 after image 中存在的列；DELETE 是空数组；UPDATE 是 after image 位图中的列，包括写入 NULL 的字段和被修改的主键。

**这里的“变动字段”精确定义为 binlog after image 中记录的字段，不保证值确实不同。** MINIMAL 日志不包含非主键旧值，SQL 指定但值未改变的列也可能出现，无法仅凭日志逐值比较。依据：[MySQL binlog_row_image 文档](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html) 与 [MySQL 已确认的问题 #83418](https://bugs.mysql.com/bug.php?id=83418)。

主键支持：

- TINYINT / SMALLINT / MEDIUMINT / INT / BIGINT，含 UNSIGNED。按真实字节宽度解码，正确符号扩展，不经过浮点数。
- DECIMAL，保持 scale，输出 `{"type":"decimal","value":"-12345678.90"}`。
- CHAR / VARCHAR，支持 utf8、utf8mb3、utf8mb4、ascii、latin1。UTF-8 主键在底层 `Decoder` 回调中直接借用事件缓冲区；转为事务持有的 owned 值时才复制。CHAR 移除末尾空格填充。

拒绝联合主键、无主键、FLOAT/DOUBLE 主键、二进制主键、其他主键字符集。JSON 数字能表示 u64，但 JavaScript 消费方直接解析大整数会损失精度，应使用保留大整数的 JSON 解析器，或在自定义输出层转成字符串。

非主键字段支持跳过整数、FLOAT/DOUBLE、DECIMAL、BIT、日期时间及其小数秒、CHAR/VARCHAR/BINARY/VARBINARY、ENUM/SET、BLOB/TEXT、JSON、空间类型、VECTOR。非主键值不解码，因此不会为 JSON/字符串/BLOB 内容分配对象。NULL 位图仍按保存的 schema 校验：NOT NULL 列出现 NULL 时明确报错，不交付事务或持久化检查点。生成列、不可见列、旧 DECIMAL wire 类型和 typed array 明确报错，不推测布局。

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

`VersionRegistry` 内置 `mysql57`、`mysql80`、`mysql81`、`mysql82`、`mysql83`、`mysql84`、`mysql90`、`mysql97`。连接时按 `SELECT VERSION()` 选择适配器；每个日志文件按其 FDE 中的生产者版本重新选择，因此升级后仍能自动读取旧版日志。5.7/8.0–8.3 用 `SHOW MASTER STATUS`，8.4/9.0/9.7 用 `SHOW BINARY LOG STATUS`。除了版本号，还检查 ROW/MINIMAL、压缩/部分 JSON 开关、公共头和相关 post-header 长度，ROTATE 必须精确为 8 字节。原始事件解析与 CDC 共用完整数字 `major.minor.patch` 语法，允许非空的 `-` 或 `+` 后缀；内置解析器与适配器拒绝 MariaDB 生产者标识。自定义适配器可明确校验其他布局。9.7 继续复用严格校验的 binlog v4 布局，参考 [MySQL 9.7.2 格式描述源码](https://raw.githubusercontent.com/mysql/mysql-server/mysql-9.7.2/libs/mysql/binlog/event/control_events.cpp)；实库兼容仍以 CI 结果为准。

未注册的系列拒绝启动；不能仅因为 `major >= 5` 就推断兼容。MySQL 官方版本序列没有 5.8，5.7.8 则属于 5.7 系列，见 [MySQL 官方版本参考](https://downloads.mysql.com/docs/mysqld-version-reference-en.pdf)。如果“5.8”指厂商版本，需根据其真实版本和日志布局实现适配器。

新增版本时，实现 `VersionAdapter` 并调用 `runtime.versions.register(Arc::new(adapter))`。每次收到 FDE 后调用 `create_event_parser`，为这个文件建立独立解析器；`create_decoder` 负责列元数据和字段值编码的处理。完整可编译的接入模板在 `crates/mysql-canal/examples/custom_version.rs`；模板注册本身不代表已验证该新版本。

事件结构变化时覆盖 `create_event_parser`，返回自己的 `EventParser`。解析器直接从原始切片产生统一的 `TableMapEvent` / `RowsEvent`，可处理字段偏移、字段顺序、字节序以及新的事件码；无需重建标准布局的字节数组。控制事件可直接返回 `ParsedEvent::Control`；`classify` 负责 `Unclassified` 事件的分类，已被结构解析器识别的 QUERY/ROTATE/TABLE_MAP/ROWS 不再调用它。

默认 `MysqlEventParser` 按 FDE 预处理 TABLE_MAP 的 ID 宽度、ROWS v1/v2 头及 QUERY/ROTATE 的 post-header 长度，建立固定事件分发表。某 ROWS 类型在 FDE 中长度为 0，就拒绝该类型；不会因为记得一个事件码就假设它在该文件中可用。核心行循环只接收统一视图，不读取版本号或 wire event_type；ROW 镜像解析函数在 FDE/事件阶段选定，非白名单表仍在解析 extra-data、位图和值之前直接跳过。

新解析器使用 `RowsEvent::new(..., parse_images)` 提供镜像解析函数，返回借用的 `RowImages`。MySQL 类型编码相同时可继续复用默认 `Decoder`；如果列元数据或字段值编码也不同，覆盖 `create_decoder`，并实现 `RowDecoder::table_map_event` / `rows_event`。原始 body 方法处理标准布局；trait 的默认视图实现仅允许标准布局，遇到非标准布局会明确报错。`crates/mysql-canal/tests/integration/event_adapters.rs` 使用合成厂商格式验证 4 字节 ID、扩展控制头、不同字段顺序和字节序、新 ROWS 事件码、文件切换与 UTF-8 主键零拷贝；这些不是实库兼容 fixture。

共同的 binlog v4 帧（19 字节公共头、FDE 引导、CRC32/NONE）仍由传输层负责。当前 MySQL 驱动还会预解析 FDE/TABLE_MAP/ROTATE，因此改变这些结构的厂商版本可能在到达本层前就被驱动拒绝；这种情况需要同时实现 `SourcePlugin` 替换传输。仅注册 `VersionAdapter` 不保证能绕过驱动的布局限制。默认 Oracle MySQL 适配器仍严格校验已支持布局，不能把未知 FDE 的长度变化自动视为安全兼容。

```rust
let mut runtime = mysql_cdc_rs::engine::Runtime::default();
runtime.versions.register(std::sync::Arc::new(MyVersionAdapter))?;
// runtime.sources.register(Arc::new(MySourcePlugin))?;
runtime.run(config, false, sink)?;
```

默认自动选择；需要覆盖内置同系列时，可在来源配置中设置 `adapter = "your-adapter"`。重叠匹配且没有指定 adapter 会报歧义错误，不依注册顺序悄悄覆盖。固定 adapter 也约束历史 FDE 的版本，跨系列升级恢复通常应保持自动选择并注册所需的各版本。

## 事务与限制

只有 XID/COMMIT 后输出。ROLLBACK 丢弃缓冲。输出成功且 flush 后才原子替换 state，并同步文件和父目录；文件锁阻止同一 state 的并发消费者。输出或保存失败就停止。崩溃在输出与 state 保存之间会重复投递，消费者须幂等；stdout 的 flush 仅代表写入 OS/管道，**不代表下游持久化确认，不能承诺 exactly-once，也不能承诺下游故障时无丢失**。

遇到任何非 BEGIN/COMMIT/ROLLBACK 的 QUERY_EVENT 都停止，包含其他库的 DDL。这是保守策略，因为跨库 SQL、RENAME、TRUNCATE 无法按默认库安全过滤。DDL 不会自动刷新当前 schema 并继续读取历史流。线上迁移需要消费到 DDL 前，明确处理 schema 版本和 DDL 后位点；另建 state 从当前开始只适用于已确认旧积压处理完毕的切换，不应直接删除旧 state 跳过积压。

XA、部分 JSON 更新、压缩事务、MariaDB 扩展、非可忽略未知事件均拒绝。心跳、GTID、ROTATE、格式描述和普通可忽略事件被处理；恢复方式是 file/position，不是 GTID failover。断线返回错误，可由进程管理器重启后从 state 恢复；不在进程内静默重连。源码布局校验参考 [MySQL 8.4 格式描述源码](https://raw.githubusercontent.com/mysql/mysql-server/mysql-8.4.6/libs/mysql/binlog/event/control_events.cpp)。

## 验证

```sh
cargo fmt --all -- --check
cargo test --workspace --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
```

默认测试包括事件结构适配与零拷贝、多来源线程隔离、相同 table_id 不同列序、来源身份绑定、独立位点、输出失败、版本系列选择、旧文件重选适配器、自定义事件/行解码器与注册冲突；并使用独立构造的协议字节覆盖符号扩展、稀疏 NULL、多行与主键变更、长 CHAR、多种跳读类型、超过 64 列、table_id 复用、截断、CRC32/NONE、事务可见性、位点保存顺序与文件锁。它们不是 mysqlbinlog 导出的实库 fixture。

GitHub Actions 在 PR / main、master 推送时运行 Ubuntu（Rust 1.89.0、stable）与 macOS（stable）测试、格式/Clippy/文档检查、下面的 MySQL 实库矩阵及 crates.io 打包验证。四个包保持相同版本，推送 `v<package.version>` tag 且全部 CI 通过后，`release.yml` 通过 GitHub OIDC Trusted Publishing 认证，按依赖顺序发布 `mysql-replication`、`mysql-canal`、`mysql-cdc-rs` 和 `pikachu`。四个包统一使用 `rust-lang/crates-io-auth-action@v1` 交换的短期 token，无需长期发布 secret；发布前须为每个 crate 分别配置可信发布者。CI 使用 `--dry-run` 验证 workspace 包，不上传任何包，也不需要发布凭据。完整步骤见 [CI 与发布说明](docs/releasing.md)。

真实 MySQL 集成测试（显式执行，在独立测试服务上创建并删除唯一命名的测试库）：

```sh
MYSQL_IMAGE=mysql:8.4 docker compose -f tests/infra/compose.yaml up -d --wait
export CDC_TEST_MYSQL_URL='mysql://root:cdc-test-only@127.0.0.1:13306'
cargo test -p mysql-canal --test mysql_live --locked -- --ignored --nocapture --test-threads=1
docker compose -f tests/infra/compose.yaml down -v
```

`mysql_live` 包含 11 个独立实库场景，均通过正式 `SourcePlugin` / `VersionAdapter` / `Processor` / sink 路径读取服务器实际生成的 binlog；不使用合成事件替代实库变更。输出比较包含 source、schema、table、operation、精确主键类型/值、previous_pk 与 changed，缺失、额外或重复的事件都会失败。

| 场景 | 校验内容 |
| --- | --- |
| 单行 INSERT / UPDATE / DELETE | 完整输出、只更新指定字段、写入 NULL、DELETE 的 changed 为空；同时跳过 JSON/时间/DECIMAL/BIT/ENUM/SET/BLOB 值 |
| 多行操作 | 批量新增、批量更新、批量删除逐行核对；零匹配 UPDATE/DELETE 不输出 |
| 整数主键 | TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT 的 signed/unsigned 边界及 NULL 跳读，包含 i64::MIN 与 u64::MAX |
| 主键变更 | UTF-8（含 emoji）、CHAR、ASCII、latin1、DECIMAL、无符号整数的新旧主键；DECIMAL 保留 scale |
| 宽表稀疏位图 | 80 列、主键在第 70 列、跨 64 位边界的 NULL 与非 NULL 更新 |
| 白名单 | 同库排除表、其他库同名表、混合事务过滤、纯过滤事务仍推进位点 |
| 实时事务消费 | 先启动阻塞 CDC，再执行混合行操作；处理 ROWS 后仍不输出、不保存位点，XID/COMMIT 后整批可见 |
| 回滚 | INSERT/UPDATE/DELETE 全部回滚后无输出，后续提交仍可正常监控 |
| 位点恢复 | 重新连接并读取磁盘 checkpoint，已提交事件不重发，新变更继续输出 |
| 输出失败 | sink 拒绝提交时不推进磁盘位点，从原位点重连能完整重放 |
| DDL 边界 | ALTER TABLE 明确失败停止，不使用旧 schema 继续输出，也不确认 DDL 后位点 |

这些场景必须串行执行：fixture 创建/删除数据库会产生 DDL，而 CDC 对任何来源的 DDL 都会停止。每个 fixture 有独立 schema、复制 ID 和临时 state，结束或断言失败时尝试清理数据库与状态目录。

替换 `MYSQL_IMAGE` 可分别测试 `mysql:5.7.44`、`mysql:8.0`、`mysql:8.4`、`mysql:9.7`；切换版本前须删除旧测试卷。`.github/workflows/mysql-compatibility.yml` 在每个版本上运行以上全部 11 个场景，覆盖 5.7、8.0 历史基线及目前全部 LTS 系列（8.4、9.7），系列标签跟踪补丁更新；不使用会跨系列移动的 `lts` 标签。CI 校验实际服务器系列，并记录具体补丁版本和所选适配器。版本范围依据 [MySQL 官方 LTS 说明](https://dev.mysql.com/doc/refman/9.7/en/mysql-releases.html)，新增 LTS 系列时须同时增加矩阵项与版本适配器。本地没有 Docker/MySQL 环境时无法执行实库测试。

离线回放使用**和起点相匹配**的 state/schema；binlog 必须保留开头的 FORMAT_DESCRIPTION：

```sh
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001
mysqlbinlog --hexdump --base64-output=DECODE-ROWS -vv /path/to/binlog.000001
```

离线回放使用 state 保存的明确来源 ID，不修改 state，跳过起点之前的历史事件，不能用错误年代的 schema。每次回放一个来源的一个文件，多个来源分别使用对应检查点回放。支持 CRC32 / NONE，事件大小上限 64 MiB，截断尾部返回错误。文件名需与 state 中的 file 一致；跨文件可在库层使用该来源的同一个 `Processor` 接续下一文件。
