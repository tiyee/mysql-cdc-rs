# mysql-cdc

[English](README.md) | 简体中文

mysql-cdc 是 Rust MySQL CDC 系统，监听 binlog，按精确的库表白名单输出已提交事务中的行变更。支持多个独立来源、MINIMAL / FULL 行镜像及检查点恢复。

CDC 事件包含来源、库表、操作、精确主键和字段名；完整原始行镜像通过复制库按需读取。普通 SELECT 不产生 CDC 变更。

## 系统组成

```text
pikachu (pikachu-cdc) ──> mysql-canal ──> mysql-replication
mysql-cdc-rs ──────────> mysql-canal + mysql-replication
```

| 组件 | 使用方式 | 职责 |
| --- | --- | --- |
| [mysql-replication](crates/mysql-replication/README.zh-CN.md) | Rust 库 | 监听、解析 binlog，生成原始事件 |
| [mysql-canal](crates/mysql-canal/README.zh-CN.md) | Rust 库 | 将 binlog 转为通用 CDC 事件，管理事务与检查点 |
| [pikachu](crates/pikachu/README.zh-CN.md) | `pikachu-cdc` | 基于 Canal 的 CLI 产品，默认输出 JSON Lines |
| [mysql-cdc-rs](crates/mysql-cdc/README.zh-CN.md) | Rust 统一入口 | 统一库入口，重导出两个底层库 |

四个包当前清单版本均为 **2.0.1**，需要 Rust **1.89+**。内置适配器覆盖 Oracle MySQL 5.7、8.0–8.4、9.0 和 9.7；实库验证范围见[测试指南](docs/testing.zh-CN.md)。

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

## 一次更新会得到什么

例如来源 `local` 的 `app.users` 表使用无符号整数主键，应用修改主键 42 的记录并提交：

```sql
UPDATE app.users SET name = 'Bob', age = 21 WHERE id = 42;
```

MINIMAL 模式下，这条变更通知表示“哪条记录被修改、日志记录了哪些字段”：

```json
{
  "source": "local",
  "schema": "app",
  "table": "users",
  "operation": "update",
  "pk": {
    "type": "unsigned",
    "value": 42
  },
  "changed": [
    "name",
    "age"
  ]
}
```

通知中的 `pk` 是用来定位记录的主键，`changed` 是日志记录的字段名。它不会输出 Bob、21 等非主键值。FULL 下，`changed` 还会包含值未改变但被完整记录的字段。完整对比见[事件说明](docs/events.zh-CN.md)。

## 库使用示例

通过本地源码集成时，使用以下依赖，请将 `/path/to/mysql-cdc-rs` 替换为实际仓库路径。

```toml
[dependencies]
mysql-cdc-rs = { path = "/path/to/mysql-cdc-rs/crates/mysql-cdc" }
```

```rust,no_run
use mysql_cdc_rs::canal::{Canal, CommittedTransaction, Result, config::AppConfig};
use std::path::Path;

fn main() -> Result<()> {
    let config = AppConfig::read(Path::new("canal.toml"))?;
    let mut canal = Canal::new(config)?;
    canal.add_handle(|transaction: CommittedTransaction<'_>| {
        for change in transaction.changes {
            println!(
                "{} {:?} {:?}",
                transaction.source_id, change.operation, change.pk
            );
        }
        Ok(())
    });
    canal.run()
}
```

`canal.toml` 使用[独立 Canal 配置](crates/mysql-canal/examples/config/multi-source.toml)，从 `[[sources]]` 开始；不要直接传入 Pikachu 的产品配置。`mysql-cdc-rs` 仅重导出两个库模块：`canal::Canal` 管理转换、处理器和检查点，`replication` 管理原始读取器与复制流；各模块拥有自己的配置和错误类型。库必须注册处理器，才能执行 `run` / `run_once`。

按模式固定 Rust 类型及读取原始行值的示例见[Rust API 参考](docs/event-api.zh-CN.md)。

原始复制可用 `BinlogFile<_, Full>` / `BinlogFile<_, Minimal>` 或 `ReplicationStream<M>` 静态选择镜像类型。行事件自动关联 TABLE_MAP，`rows.rows()` 产生类型化操作，字段提供列定义与借用值；MINIMAL 区分未记录字段和 SQL NULL。列名等可选元数据缺失时保留未知，具体契约见 [Rust API 参考](docs/event-api.zh-CN.md)。

## MINIMAL / FULL 事件内容

| 模式 | 原始行事件类型 | 镜像内容 | 字段访问 |
| --- | --- | --- | --- |
| MINIMAL | `BinlogEvent<Minimal>` / `RowOperation<SparseImage>` | 实际记录的列及其定义和值 | `Result<Option<Field>>`，合法列未记录为 `None` |
| FULL | `BinlogEvent<Full>` / `RowOperation<CompleteImage>` | 每份存在的镜像包含全部列及其定义和值 | `Result<Field>`，拒绝不完整镜像 |

查看 [MINIMAL JSON 示例](docs/events.zh-CN.md#minimal-json)、[FULL JSON 示例](docs/events.zh-CN.md#full-json)，可对照同一条 UPDATE 的JSON 数据、事件结构、before/after 字段和实际访问结果；[Rust 示例](docs/event-api.zh-CN.md#按模式读取具体行操作)展示两种静态类型的读取方式。原始事件包含字段值；Canal 的提交变更仍只携带主键和变动字段索引。

## 使用边界

- 首次启动从当前提交位点开始，不做全量快照；后续使用检查点保存的历史 schema 恢复。
- COMMIT 后交付完整事务，回滚丢弃。所有处理器成功后才保存检查点，故障可能导致重放，消费方须幂等。
- 支持单列整数、精确 DECIMAL 及受支持字符集的 CHAR/VARCHAR 主键。DDL、未知布局和不支持的数据会报错停止。
- 每个来源独立维护连接和检查点，跨来源没有全局顺序。

## 文档

- [文档目录](docs/README.zh-CN.md)
- [配置、服务器准备与回放](docs/configuration.zh-CN.md)
- [事件说明与 MINIMAL/FULL 示例](docs/events.zh-CN.md)
- [系统如何工作](docs/architecture.zh-CN.md)
- [开发与扩展](docs/development.zh-CN.md)
- [测试与实库矩阵](docs/testing.zh-CN.md)
- [CI、打包与发布](docs/releasing.md)
