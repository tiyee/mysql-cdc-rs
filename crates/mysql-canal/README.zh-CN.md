# mysql-canal

[English](README.md) | 简体中文

基于 `mysql-replication` 的 MySQL CDC 转换与已提交事务交付库。`pikachu-rs` 包提供 `pikachu-cdc` CLI 及默认 JSON Lines 打印处理器；本库不默认输出。

```toml
[dependencies]
mysql-canal = "0.0.2"
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

CDC 输出仅包含来源、库表、操作、精确主键、可选旧主键及变动字段名。支持单列有符号/无符号整数、精确 DECIMAL 和已支持字符集的 CHAR/VARCHAR 主键。INSERT/UPDATE 的变动字段按 after-image 位图，包括 NULL 与主键写入；DELETE 为空。不支持的主键、缺失主键、截断及元数据不匹配明确报错。内置来源要求 ROW + MINIMAL；表匹配后才解析镜像，只解码主键，其余字段按编码长度跳过。转换中的 UTF-8 主键和事件视图保持借用，字段索引共享 `Arc<[u16]>`。传输无法裁剪上游 binlog 已包含的列。

通过 `Cdc::with_runtime` 注册 `SourcePlugin` 或 `VersionAdapter`。`EventParser` 处理事件布局差异，`RowDecoder` 处理字段编码差异。每个 FDE 根据生产者重新选择适配器，支持升级前生成的历史文件。内置适配器覆盖已校验的 MySQL 5.7、8.0–8.4、9.0、9.7 系列；未知布局、压缩事务、XA、partial JSON 及未注册系列明确拒绝。实库验证与合成协议测试分开。参见 [custom_version.rs](examples/custom_version.rs)、[listen.rs](examples/listen.rs) 和[项目说明](https://github.com/tiyee/pikachu-rs/blob/main/README.zh-CN.md)中的 CLI、输出契约及兼容矩阵。

```sh
cargo test -p mysql-canal --locked
cargo bench -p mysql-canal --bench minimal_rows
cargo bench -p mysql-canal --bench event_adapter
```

默认测试覆盖合成协议、转换、事务、检查点、多来源隔离及适配器。11 个默认忽略的 `mysql_live` 场景需要专用可销毁 MySQL 实例，串行执行，会创建和删除测试数据库。使用仓库 `tests/infra/compose.yaml` 并按项目说明清理，不能连接生产实例。
