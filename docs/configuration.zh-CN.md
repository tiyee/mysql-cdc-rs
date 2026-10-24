# 配置与运行

[English](configuration.md) | 简体中文

[文档目录](README.zh-CN.md)

本页是配置、服务器准备、恢复与回放的使用参考。快速体验见[系统 README](../README.zh-CN.md)，事件字段和输出语义见[事件参考](events.zh-CN.md)。

## 配置层级

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

直接集成 Canal 时，将 `crates/mysql-canal/examples/config/multi-source.toml` 复制为 `canal.toml`；该文件从 `[[sources]]` 开始，各来源必须包含 `[sources.mysql-replication]`。上方产品包装层仅供 Pikachu 使用。

CLI 注册默认 `PrintHandle`，将事件打印为 stdout JSON Lines；命令名继续使用 `pikachu-cdc`。产品配置必须包含 `mysql-canal`，Canal 的每个来源必须包含 `mysql-replication`；Pikachu 根层只放产品增量配置，目前没有额外必填项。使用 `crates/pikachu/examples/config/multi-source.toml`，每个 `[[mysql-canal.sources]]` 都有独立 `id`、`state_file`、`source` 和 `tables`：

```sh
cargo build --release --locked -p pikachu
cp crates/pikachu/examples/config/multi-source.toml cdc.toml
# Set each source's [[mysql-canal.sources.tables]] to exact database and table names, respecting server case rules.
export MYSQL_EAST_URL='mysql://cdc:password@east.example:3306'
export MYSQL_WEST_URL='mysql://cdc:password@west.example:3306'
./target/release/pikachu-cdc check cdc.toml
./target/release/pikachu-cdc run cdc.toml > changes.jsonl
```

| 命令 | 行为 |
| --- | --- |
| `pikachu-cdc check CONFIG.toml` | 校验来源、服务器能力及表 schema，不消费事件 |
| `pikachu-cdc init CONFIG.toml` | 保存初始 schema 和当前位点，不消费事件；已有 state 文件时报错 |
| `pikachu-cdc run CONFIG.toml` | 初始化缺失的 state，再从各自保存的位点持续消费 |
| `pikachu-cdc run CONFIG.toml --once` | 消费当前可用事件后退出 |
| `pikachu-cdc replay STATE.json BINLOG_FILE` | 按历史 schema 回放一个本地文件，不修改 state |

## 配置字段

字段路径相对每个 `sources` 条目；Pikachu 文件统一增加 `mysql-canal.` 前缀。各层拒绝未知字段和摊平字段。

| 字段 | 要求 / 默认值 | 所属层 |
| --- | --- | --- |
| `id` | 必填，非空且唯一 | Canal |
| `state_file` | 必填，路径唯一，父目录已存在 | Canal |
| `max_transaction_bytes` | 默认 67108864（64 MiB），必须非零 | Canal |
| `tables` | 非空精确库表列表，无重复 | Canal |
| `source.plugin` | `[source]` 必填；插件默认 `mysql` | Canal |
| `source.adapter` | 可选；默认按服务器与 FDE 自动选择 | Canal |
| `mysql-replication.url_env` | 必填，凭据环境变量名 | Replication |
| `mysql-replication.server_id` | 必填，非零复制 ID | Replication |
| `mysql-replication.tls_ca` | 可选，CA 证书路径 | Replication |
| `mysql-replication.row_image` | 默认 `minimal`，可选 `full` | Replication |
| `mysql-replication.connect_timeout_secs` | 默认 10，必须非零 | Replication |
| `mysql-replication.read_timeout_secs` | 默认 90，必须非零 | Replication |

嵌入其他产品时直接反序列化 Canal 的 `AppConfig`，再调用 `app.resolve_paths(parent_toml_path)` 校验并解析相对路径。`AppConfig::read` 只读取独立 Canal 格式。文件和位点由运行时检查点提供，不写入静态连接配置。

## 行镜像与模式切换

在每个 `[mysql-canal.sources.mysql-replication]` 中设置 `row_image = "minimal"` 或 `row_image = "full"`，省略时默认 `minimal`，各来源可以不同。在线命令接受可重复的 `--row-image SOURCE_ID=minimal|full` 覆盖参数，可与 `run --once` 合用；回放接受 `--row-image minimal|full`，默认 `minimal`：

```sh
./target/release/pikachu-cdc check cdc.toml --row-image east=full
./target/release/pikachu-cdc run cdc.toml --once --row-image east=full --row-image west=minimal
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001 --row-image full
```

这些选项校验输入，不修改 MySQL 配置，也不能改变已有 binlog 的内容。对支持的单列主键表，MINIMAL 要求 UPDATE/DELETE 的 before image 仅包含主键，FULL 要求每个存在的镜像包含全部列。CDC 不提供 NOBLOB 或自动检测模式。镜像位图不总能区分生产者配置，例如单列表的两种模式可能相同。

检查点继续使用 v1 格式，不保存行镜像模式；回放时须选择历史文件对应的模式。当前 GLOBAL 设置不能代表历史日志。切换模式时，暂停写入，在旧模式下消费到提交边界并停止消费者，再修改服务器和写入会话设置，最后以新模式启动。不要重新初始化检查点来跳过未消费数据。混合模式的历史日志可能校验失败。

## 来源、路径与恢复

Pikachu 配置统一使用 `[[mysql-canal.sources]]` 列表；只连接一个实例时也使用一个具名条目。每个来源必须显式填写 `id` 和凭据环境变量 `mysql-canal.sources.mysql-replication.url_env`。来源的 ID 和 state 路径必须各不相同，state 的父目录须已存在；指向同一实例 UUID 的多个订阅还必须使用不同复制 `server_id`。不同实例可以复用复制 ID，但统一分配更便于运维。每个来源单独持有线程、连接、schema、table_id 映射、事务预算和提交位点。一个来源不能借用另一个来源的 TABLE_MAP，跨来源也没有全局事务或全局顺序。

任一 worker 失败，进程通知其他 worker 在事件之间停止并返回非零状态；当前阻塞的 MySQL 读取最长等待其配置的 read timeout（默认 90 秒）。重启后每个来源从自己的已提交位点继续。各来源初始化逐个获取读锁，不提供跨实例一致性快照；若初始化中途失败，已有成功的 state 会保留，下一次使用 `run` 继续准备剩余来源。

需要 Rust 1.89+。`check` 检查服务器配置、表引擎及主键；字段的实际 wire 类型/长度在 TABLE_MAP 到达时进一步校验。配置相对路径以 TOML 所在目录为基准。凭据从环境变量读取，特殊字符应按 MySQL URL 规则编码；`mysql-canal.sources.mysql-replication.tls_ca` 可配置 CA 证书，启用证书与域名验证。

每次启动 MySQL 来源（包括从检查点恢复）都会在 `mysql-canal` 内检查白名单表当前的主键，与 CDC 转换复用同一套主键类型规则。主键缺失或不受支持时，在创建初始检查点和消费 binlog 前报错停止；历史事件仍按检查点中保存的 schema 解码。直接使用原始复制库时，可通过 `ReplicationStream::connect_with_tables` 或 `from_connection_with_tables` 传入指定表，见下文的 [Replication API](#replication-连接与解析-api)。

初次 `run` 会短暂执行 `FLUSH TABLES WITH READ LOCK`，在锁内读取白名单 schema 和当前提交位点，解锁后开始消费。**它从当前位点开始，不做全量数据快照**。也可以先运行 `init cdc.toml` 生成起点，稍后 `run`；已经存在的 state 不会被 `init` 覆盖。`run cdc.toml --once` 消费当前可用事件后退出，适合补采与测试。

再次运行从 state 的提交位点恢复。state 必须包含非空的 `source_uuid`、`source_id` 和 `source_plugin`，同时保存该位点对应的列顺序、主键类型、字符集；恢复时使用保存的 schema，不能拿当前 INFORMATION_SCHEMA 去解释历史事件。服务器 UUID、来源 ID、来源插件和白名单必须与 state 一致。必须保留起点之后的 binlog。

## MySQL 设置与权限

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

写入连接必须使用 ROW 和配置选择的 MINIMAL 或 FULL 行镜像模式；只改 GLOBAL 不会改变已有会话的 SESSION 值。程序检查全局配置，并按所选策略校验白名单表的行镜像。CDC 的 `server_id` 必须非零且与源和其他复制消费者不同。

权限示例，请由管理员将地址、密码和库替换为实际值：

```sql
CREATE USER 'cdc'@'127.0.0.1' IDENTIFIED BY 'replace-me';
GRANT REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc'@'127.0.0.1';
GRANT SELECT ON app.* TO 'cdc'@'127.0.0.1';
-- Initialization uses a global read lock; RELOAD can be granted on MySQL 5.7/8.x.
GRANT RELOAD ON *.* TO 'cdc'@'127.0.0.1';
```

全局读锁会短暂阻塞写入，并可能等待正在执行的语句；应在合适的初始化窗口执行。之后的正常恢复不再获取全局读锁。白名单表必须是 InnoDB，避免把非事务表的变化误当作可回滚事务。

## Replication 连接与解析 API

`config::Config` 定义可复用的连接与订阅设置，支持 `config::RowImageMode::{Minimal, Full}`，默认 MINIMAL。该类型校验上游模式，不修改 MySQL，并构造已有的底层连接/流配置。Canal 的必填 `[sources.mysql-replication]` 直接包含该类型。`connect_timeout_secs` 默认 10 秒，`read_timeout_secs` 默认 90 秒，均拒绝零值；`tls_ca` 可选，相对路径由嵌入应用解析。binlog 文件与位点由消费方在运行时提供。

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

需要显式选择解析方式或可选表预检时，仍可直接使用底层 API：

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

`ReplicationStream::connect_with_tables` 和 `from_connection_with_tables` 在发送 binlog 请求前，通过 INFORMATION_SCHEMA 检查调用方指定的表。每张表必须有单列 PRIMARY KEY，类型为有符号/无符号 TINYINT/SMALLINT/MEDIUMINT/INT/BIGINT、精确 DECIMAL，或使用 utf8、utf8mb3、utf8mb4、ascii、latin1 的 CHAR/VARCHAR。表或主键缺失、联合主键、只有 UNIQUE 索引、FLOAT/DOUBLE、二进制/日期主键和不支持的字符集均报错，包含库表及可获取的列名；空表列表也报错。该列表仅控制检测，不过滤事件。原有 `connect` 和 `from_connection` 保留不做此检测的原始复制能力。可通过 `primary_key::validate_primary_keys(&mut conn, tables)` 单独检测而不启动复制。这些可选公共辅助 API 继续保留；`mysql-canal` 自行负责 CDC 启动可用性检查，不再依赖它们。原始事件解析和行解码不要求主键。

- `next_event()` 返回具体协议事件和完整原始字节。
- `next_frame()` 校验长度/checksum，保留原始字节，事件体留给自定义适配器。
- 在线 `next_view()` 校验驱动事件并直接借用 body，适合不需要完整原始帧的消费者。
- `ParserConfig::minimal()` 跳过 GTID 元数据和 Previous GTIDs 解析，保留原事件码及明确的 raw 载荷。
- ROWS 默认延迟解析镜像，调用 `RowsEvent::images()` 时才读取 extra-data 和稀疏位图。`RowsDecode::ValidateImages` 可要求立即校验。这些解析模式不解码字段值；[事件参考](events.zh-CN.md)中的独立 `RowsDecoder` API 按需启用。
- `TableMapEvent::columns()` 在表匹配后按需校验列结构。

## 离线回放

离线回放使用**和起点相匹配**的 state/schema；binlog 必须保留开头的 FORMAT_DESCRIPTION：

```sh
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001
mysqlbinlog --hexdump --base64-output=DECODE-ROWS -vv /path/to/binlog.000001
```

离线回放使用 state 保存的明确来源 ID，不修改 state，跳过起点之前的历史事件，不能用错误年代的 schema。每次回放一个来源的一个文件，多个来源分别使用对应检查点回放。支持 CRC32 / NONE，事件大小上限 64 MiB，截断尾部返回错误。文件名需与 state 中的 file 一致；跨文件可在库层使用该来源的同一个 `Processor` 接续下一文件。

`ParserConfig::minimal()` 仅减少 GTID 等元数据解析，不代表要求 MINIMAL 行镜像。低层 `stream::ReplicationConfig.required_row_image = Some(RowImage::Full)` 要求上游 FULL；设为 `None` 跳过该设置检查，不改变已有字节或镜像类型。

## 协议与传输边界

结构化解析器根据每个文件的 FDE 选择声明的布局，支持已注册 MySQL 5.7、8.0–8.4、9.0、9.7 系列。生产者版本须包含完整数字 `major.minor.patch`，允许非空的 `-` 或 `+` 后缀（如 `5.7.44-log` 或 `8.0.45-0ubuntu0.24.04.1`）。畸形版本与 MariaDB 标识明确拒绝，数字版本解析规则与 CDC 共用。未知系列或布局明确失败；`next_frame()` 可交给自定义事件体解析器。混用帧/视图与结构化读取时，需要将读取到的 FDE 帧交给自己的 `BinlogParser`；内置结构化解析器只观察 `next_event()` 调用。

离线文件必须保留开头 FDE，支持 CRC32/NONE，截断尾部和超过 64 MiB 的事件报错。在线传输复用 `mysql` 驱动；驱动会预解析 FDE/TABLE_MAP/ROTATE，并在本库校验事件大小之前分配内存，特殊 framing 可能需要其他传输实现。完整在线原始事件保留驱动读取的原头部字段、body、算法字节及存储的 checksum，不调用会重新计算 checksum 的序列化方法。`next_view()` 完全跳过这次原始帧组装。断线传播错误，不静默重连。

## 从旧配置迁移到 2.0

2.0 要求每个来源必须包含 `[mysql-canal.sources.mysql-replication]`。将 `url_env`、`server_id`、`tls_ca` 和 `row_image` 从 `[mysql-canal.sources.source]` 移入该段；`[mysql-canal.sources.source]` 仅保留 `plugin`、`adapter`。Rust 的 `Config` 字面量须增加 `replication: config::ReplicationConfig::new(url_env, server_id)`，需要 FULL 时再设置其 `row_image`。`ReplicationConfig` 和 `RowImageMode` 直接重导出 `mysql_replication::config` 的类型。旧 TOML 层级会报错；`Decoder::new`、`EventConverter::new`、`Processor::new` / `with_registry` 继续默认 MINIMAL，显式模式入口使用 `with_row_image`。自定义版本适配器须通过 `validate_server_with_row_image` 和 `create_decoder_with_row_image` 显式支持 FULL；这两个方法的默认实现保留 MINIMAL，并拒绝 FULL。
