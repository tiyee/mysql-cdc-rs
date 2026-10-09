# mysql-replication

[English](README.md) | 简体中文

可复用的 Rust MySQL CDC 库，支持传入配置并注册已提交事件监听 handle。`pikachu-rs` 包负责 `pikachu-cdc` CLI，并自行注册默认打印 handle。

```toml
[dependencies]
mysql-replication = "0.0.1"
```

```rust,no_run
use mysql_replication::{Event, Replication, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let config = AppConfig::read(Path::new("cdc.toml"))?;
    let mut replication = Replication::new(config)?;
    replication.add_handle(|event: Event<'_>| {
        for change in event.changes {
            println!("{} {:?} {:?}", event.source_id, change.operation, change.pk);
        }
        Ok(())
    });
    replication.run()
}
```

配置可以在 Rust 中构造 `AppConfig`，也可以读取具名 `[[sources]]` TOML，见 `examples/config/multi-source.toml`。使用 `add_handle` 注册闭包或 `EventHandler` 实现；`run()` 持续消费，`run_once()` 消费当前可用事件后退出。运行前必须注册至少一个 handle，库没有默认输出。`check()` 验证来源配置，`initialize()` 生成初始检查点。

每个 `Event` 借用来源 ID、提交检查点和按 binlog 顺序排列的 `OwnedChange` 列表。变更包含表元数据、操作、精确主键、可选旧主键和变动字段索引/名称，不构建完整行值。handle 按注册顺序同步执行，不同来源的事务回调不交错；只在 COMMIT/XID 后调用，回滚与未完成事务不触发回调，完全被白名单过滤的提交收到空列表。全部 handle 成功后才保存检查点。失败停止消费，重启可能重放之前 handle 的副作用；慢 handle 直接施加背压。只在需要跨回调持有时复制变更。

每个来源独立维护连接、历史 schema、事务预算、映射及检查点。首次消费从当前位点开始，不提供数据快照；恢复使用检查点中的历史 schema。DDL 明确报错并停止。凭据从配置指定的环境变量读取。要求 Rust 1.89+、MySQL ROW + MINIMAL，以及支持的单列数字或字符串主键。

监听示例见 `examples/listen.rs`，自定义适配器与 `Replication::with_runtime` 示例见 `examples/custom_version.rs`。底层接口仍按 `config`、`engine`、`model`、`protocol::mysql`、`source`、`sink` 组织。CLI 配置、兼容性与输出契约见[仓库中文版 README](https://github.com/tiyee/pikachu-rs/blob/main/README.zh-CN.md)。
