# mysql-canal

[English](README.md) | 简体中文

基于 `mysql-replication` 的 MySQL CDC 转换与已提交事务交付库。`pikachu` 包提供 `pikachu-cdc` CLI 及默认 JSON Lines 打印处理器；本库不默认输出。

本库位于 `pikachu → mysql-canal → mysql-replication` 的中间层。底层读取协议字节，也可解码原始行镜像；canal 使用历史 SQL schema 解释选中表的 ROWS 事件，提取精确主键和变动列索引。该路径使用自己的选择性 `RowDecoder`，不先通过复制库的 `RowsDecoder` 解出全部字段。普通 SELECT 查询不属于 CDC 变更模型。

需要通过一个依赖同时使用 CDC 与原始复制时，可依赖 `mysql-cdc-rs`（`mysql_cdc_rs`）；本库从该包根路径和 `canal` 导出。

```toml
[dependencies]
mysql-canal = "2.0.0"
```

```rust,no_run
use mysql_canal::{Cdc, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let mut canal = Cdc::new(AppConfig::read(Path::new("cdc.toml"))?)?;
    canal.add_handle(|event: CommittedTransaction<'_>| {
        for change in event.changes {
            println!("{} {:?} {:?}", event.source_id, change.operation, change.pk);
        }
        Ok(())
    });
    canal.run()
}
```

`Cdc::new` 接受从 TOML 读取或程序构造的 `AppConfig`。配置统一使用具名 `[[sources]]`，见[配置示例](examples/config/multi-source.toml)。每个来源显式配置 ID、插件、凭据环境变量、白名单及检查点路径；连接、历史 schema、TABLE_MAP、事务预算和检查点独立，不提供跨来源全局顺序。首次从当前提交位点消费，不做全量快照；后续使用检查点中的历史 schema 和来源身份恢复。

`add_handle` 接受 `TransactionHandler` 或闭包，收到的 `CommittedTransaction` 借用来源 ID、检查点及有序 `OwnedChange` 列表。COMMIT/XID 后交付，回滚丢弃，完全过滤的提交收到空列表。多个 handle 按注册顺序执行，不同来源的完整事务回调不交错；全部成功后才保存检查点。失败停止消费，输出成功到保存检查点之间的故障可能导致重放。仅在回调外持有时复制数据。调用 `run()` 或 `run_once()` 前至少注册一个 handle，慢处理器施加背压。`check()` 校验配置及服务器能力，`initialize()` 创建初始检查点而不消费。

原始 binlog 事件与 CDC 变更为独立类型。`engine::converter::EventConverter` 将借用协议视图（包括 `mysql_replication::BinlogEvent`）转换成行变更及事务动作。`engine::Processor` 缓冲到提交后交付，并持久化已确认检查点。包含 DDL 的 QUERY 在协议层有效，在 CDC 层以 `Error::Statement` 停止，不使用过时 schema 继续消费。

在线来源使用 `ReplicationStream::next_view()`，离线回放适配复制库的 `BinlogFile::next_frame()`，两者均交给 canal 的 `EventParser` 和选择性解码器。`EventConverter` 在提交前即可回调 `Change<'_>`，并返回 `EventAction`，自身不保证事务交付；`Processor` 将 `OwnedChange` 保留到 COMMIT/XID，`Cdc` 向处理器交付 `CommittedTransaction` 后再保存检查点。`convert_binlog` 通过适配结构化事件的 frame 接收输入，不消费预解码的原始行值。完整流程见[架构与事件流](https://github.com/tiyee/mysql-cdc-rs/blob/master/docs/architecture.zh-CN.md)。

MySQL 来源每次连接（包括从检查点恢复）都会把白名单交给 `mysql-replication` 做主键预检，在创建初始检查点或开始消费前拒绝无主键、只有 UNIQUE 索引、联合主键及不支持的主键。只检测指定表，恢复解码仍使用检查点的历史 schema。直接使用原始复制库时，通过 `ReplicationStream::connect_with_tables` 或 `from_connection_with_tables` 启用同一检测。

CDC 输出仅包含来源、库表、操作、精确主键、可选旧主键及变动字段名。支持单列有符号/无符号整数、精确 DECIMAL 和已支持字符集的 CHAR/VARCHAR 主键。INSERT/UPDATE 的变动字段按 after-image 位图，包括 NULL 与主键写入；DELETE 为空。不支持的主键、缺失主键、截断及元数据不匹配明确报错。稀疏镜像中的 NOT NULL 非主键列出现 NULL 同样报错，不交付事务或推进检查点。内置来源支持按来源选择 ROW + MINIMAL 或 FULL，默认 MINIMAL；表匹配后才解析镜像，只解码主键，其余字段按编码长度跳过。转换中的 UTF-8 主键和事件视图保持借用，字段索引共享 `Arc<[u16]>`。传输无法裁剪上游 binlog 已包含的列。

通过 `Cdc::with_runtime` 注册 `SourcePlugin` 或 `VersionAdapter`。`EventParser` 处理事件布局差异，`RowDecoder` 处理字段编码差异。每个 FDE 根据生产者重新选择适配器，支持升级前生成的历史文件。内置适配器覆盖已校验的 MySQL 5.7、8.0–8.4、9.0、9.7 系列；默认 ROTATE post-header 必须精确为 8 字节。生产者版本要求完整数字 `major.minor.patch`，允许非空的 `-` 或 `+` 后缀，内置适配器拒绝 MariaDB 标识；未知布局、压缩事务、XA、partial JSON 及未注册系列明确拒绝。实库验证与合成协议测试分开。参见 [custom_version.rs](examples/custom_version.rs)、[listen.rs](examples/listen.rs) 和[项目说明](https://github.com/tiyee/mysql-cdc-rs/blob/master/README.zh-CN.md)中的 CLI、输出契约及兼容矩阵。

校验失败保持提交边界：NOT NULL 非主键列出现 NULL 返回 `Error::Schema`，主键为 NULL 返回 `Error::MissingPrimaryKey`，畸形生产者版本返回 `Error::Invalid`，不支持的厂商或 FDE 布局返回 `Error::Unsupported`。协议回归单独核对错误类别和磁盘检查点不推进，与实库兼容测试区分。

```sh
cargo test -p mysql-canal --locked
cargo bench -p mysql-canal --bench minimal_rows
cargo bench -p mysql-canal --bench event_adapter
```

默认测试覆盖合成协议、转换、事务、检查点、多来源隔离及适配器。12 个默认忽略的 `mysql_live` 场景包含 check/init 和首次/恢复消费的主键拒绝。原始协议预检/行解码与在线 CLI 行为分别在 `mysql-replication` 和 `pikachu` 独立验证，共享 MySQL 矩阵串行执行三层测试。实库测试需要专用可销毁 MySQL 实例，会创建和删除测试数据库。使用仓库 `tests/infra/compose.yaml` 并按项目说明清理，不能连接生产实例。

## 行镜像选择与迁移

在 `[sources.source]` 下设置 `row_image = "full"` 消费 FULL，设置 `minimal` 或省略字段则使用 MINIMAL。2.0 中程序构造 `SourceConfig` 时须增加 `row_image: config::RowImageMode::Minimal` 或 `Full`。参数只校验上游设置，不修改服务器；CDC 不提供 NOBLOB 模式。

通过 `Decoder::with_row_image(tables, mode)`、`EventConverter::with_row_image(tables, registry, adapter, mode)` 显式选择策略。离线或自定义引擎使用 `Processor::with_row_image(state, state_path, budget, registry, adapter, mode)`；原构造函数保持 MINIMAL 默认。自定义适配器须实现 `validate_server_with_row_image` 与 `create_decoder_with_row_image` 才能支持 FULL，默认实现把 MINIMAL 委托给已有方法，并拒绝 FULL。

FULL 校验完整镜像，仍仅解码主键。INSERT/UPDATE 的 `changed` 按 after-image 位图输出，因此包括未改变的列；DELETE 仍为空。检查点保留 v1 历史 schema，不保存模式；回放须选择历史文件模式，切换生产者和消费者配置前须在提交边界消费完旧模式数据。
