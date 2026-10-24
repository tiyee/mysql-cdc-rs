# mysql-cdc

[English](README.md) | 简体中文

mysql-cdc 是 Rust MySQL CDC 系统，监听 binlog，按精确的库表白名单输出已提交事务中的行变更。支持多个独立来源、MINIMAL / FULL 行镜像及检查点恢复。

CDC 事件包含来源、库表、操作、精确主键和字段名；完整原始行镜像通过复制库按需读取。普通 SELECT 不产生 CDC 变更。

## 系统组成

```text
pikachu (pikachu-cdc) ──> mysql-canal ──> mysql-replication
mysql-cdc-rs ──────────> mysql-canal + mysql-replication
```

| 包 | 入口 | 职责 |
| --- | --- | --- |
| [mysql-replication](crates/mysql-replication/README.zh-CN.md) | `mysql_replication` | 监听、解析 binlog，生成原始事件 |
| [mysql-canal](crates/mysql-canal/README.zh-CN.md) | `mysql_canal` | 将 binlog 转为通用 CDC 事件，管理事务与检查点 |
| [pikachu](crates/pikachu/README.zh-CN.md) | `pikachu-cdc` | 基于 Canal 的 CLI 产品，默认输出 JSON Lines |
| [mysql-cdc-rs](crates/mysql-cdc/README.zh-CN.md) | `mysql_cdc_rs` | 统一库入口，重导出两个底层库 |

四个包当前版本均为 **2.0.0**，需要 Rust **1.89+**。内置适配器覆盖 Oracle MySQL 5.7、8.0–8.4、9.0 和 9.7；实库验证范围见[测试指南](docs/testing.zh-CN.md)。

## CLI 使用示例

先准备启用 binlog、使用 InnoDB 表及 `ROW + MINIMAL` 的 MySQL。账户权限与服务器配置见[配置指南](docs/configuration.zh-CN.md)。将以下内容保存为 `cdc.toml`，替换实际库表名；FULL 来源将 `row_image` 改为 `full`，并确保写入会话设置一致。

```toml
[mysql-canal]

[[mysql-canal.sources]]
id = "local"
state_file = "local-state.json"

[mysql-canal.sources.source]
plugin = "mysql"

[mysql-canal.sources.mysql-replication]
url_env = "MYSQL_URL"
server_id = 901001
row_image = "minimal"

[[mysql-canal.sources.tables]]
schema = "app"
table = "users"
```

从仓库根目录构建并运行：

```sh
cargo build --release --locked -p pikachu
export MYSQL_URL='mysql://cdc:password@127.0.0.1:3306'
./target/release/pikachu-cdc check cdc.toml
./target/release/pikachu-cdc run cdc.toml > changes.jsonl
```

Pikachu 必须包含 `mysql-canal`，每个 Canal 来源必须包含 `mysql-replication`，各层仅定义自身增量配置。来源 ID 和 state 路径必须各不相同；相对路径以配置文件为基准，state 父目录须已存在。多来源示例见[产品配置](crates/pikachu/examples/config/multi-source.toml)。

| 命令 | 用途 |
| --- | --- |
| `pikachu-cdc check cdc.toml` | 校验配置、上游能力及表结构 |
| `pikachu-cdc init cdc.toml` | 保存当前起点；拒绝覆盖已有 state |
| `pikachu-cdc run cdc.toml` | 从检查点持续消费；自动初始化缺失的 state |
| `pikachu-cdc run cdc.toml --once` | 消费当前可用事件后退出 |
| `pikachu-cdc replay local-state.json /path/to/binlog.000001` | 按历史 schema 回放单个文件，不修改 state |

输出示例：

```json
{"source":"local","schema":"app","table":"users","operation":"update","pk":{"type":"unsigned","value":42},"changed":["name","age"]}
```

`changed` 表示 after image 中记录的字段，FULL 下也包含值未改变的字段。字段、主键类型及 before/after 的完整说明见[事件参考](docs/events.zh-CN.md)。

## 库使用示例

```toml
[dependencies]
mysql-cdc-rs = "2.0.0"
```

```rust,no_run
use mysql_cdc_rs::{Cdc, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let config = AppConfig::read(Path::new("canal.toml"))?;
    let mut cdc = Cdc::new(config)?;
    cdc.add_handle(|transaction: CommittedTransaction<'_>| {
        for change in transaction.changes {
            println!("{} {:?} {:?}", transaction.source_id, change.operation, change.pk);
        }
        Ok(())
    });
    cdc.run()
}
```

`canal.toml` 使用[独立 Canal 配置](crates/mysql-canal/examples/config/multi-source.toml)，从 `[[sources]]` 开始；不要直接传入 Pikachu 的产品配置。根路径及 `canal` 导出 CDC API，`replication` 导出原始事件 API。库必须注册处理器，才能执行 `run` / `run_once`。

通过 `Cdc::<Minimal>::for_mode(config)` 或 `Cdc::<Full>::for_mode(config)` 可在实例化时固定模式；各来源配置须与类型一致。复制库也提供 `BinlogFile::<_, Full>::for_mode(...)`、`ReplicationStream::<Full>::for_mode(...)`，其 `BinlogEvent` 与行镜像携带相同模式。示例见[类型化监听](crates/mysql-canal/examples/listen_typed.rs)及[原始行解码](crates/mysql-replication/examples/decode_typed_rows.rs)。

## 使用边界

- 首次启动从当前提交位点开始，不做全量快照；后续使用检查点保存的历史 schema 恢复。
- COMMIT 后交付完整事务，回滚丢弃。所有处理器成功后才保存检查点，故障可能导致重放，消费方须幂等。
- 支持单列整数、精确 DECIMAL 及受支持字符集的 CHAR/VARCHAR 主键。DDL、未知布局和不支持的数据会报错停止。
- 每个来源独立维护连接和检查点，跨来源没有全局顺序。

## 文档

- [文档目录](docs/README.zh-CN.md)
- [配置、服务器准备与回放](docs/configuration.zh-CN.md)
- [事件字段、MINIMAL/FULL 差异与输出契约](docs/events.zh-CN.md)
- [架构与事件流](docs/architecture.zh-CN.md)
- [开发与扩展](docs/development.zh-CN.md)
- [测试与实库矩阵](docs/testing.zh-CN.md)
- [CI、打包与发布](docs/releasing.md)
