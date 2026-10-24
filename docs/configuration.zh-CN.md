# 配置与运行

[English](configuration.md) | 简体中文

[文档目录](README.zh-CN.md)

本页按“准备数据库 → 连接一个来源 → 检查并运行 → 恢复与回放”的顺序说明。本页命令均从源码构建后运行。

## 1. 准备 MySQL

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

## 2. 配置一个来源

将以下内容保存为 `cdc.toml`。它表示：连接环境变量 `MYSQL_EAST_URL` 指向的 MySQL，只读取 `app.users`，把消费进度保存到 `east-state.json`。请替换为实际库表名。

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
row_image = "minimal"

[[mysql-canal.sources.tables]]
schema = "app"
table = "users"
```

### 怎样读这个配置

| 配置部分 | 决定什么 |
| --- | --- |
| `mysql-canal` | Pikachu 使用的变更消费服务 |
| `mysql-canal.sources` | 一个具名来源；多个实例分别配置 |
| `sources.source` | 使用哪个来源插件，MySQL 填 `mysql` |
| `sources.mysql-replication` | 怎样连接数据库、使用哪个复制 ID 和行镜像模式 |
| `sources.tables` | 订阅哪些精确的库名和表名 |

Pikachu 包含 Canal，Canal 的每个来源再包含复制配置；每层只填写自己负责的设置。未知字段、缺少下层配置或放错层级都会报错。

### 常用设置

下表中的路径相对每个来源；在完整产品配置中，它们位于 `mysql-canal.sources` 下。

| 设置 | 怎样填写 | 默认值 / 限制 |
| --- | --- | --- |
| `id` | 给来源起一个稳定名称，例如 `east` | 必填且唯一；会出现在输出中 |
| `state_file` | 保存消费进度的文件，例如 `east-state.json` | 每个来源使用不同文件，父目录须已存在 |
| `tables` | 数据库与表名列表 | 必填、精确匹配、不重复 |
| `mysql-replication.url_env` | 保存 MySQL URL 的环境变量名 | 必填；密码不直接写入文件 |
| `mysql-replication.server_id` | 此消费者的复制连接编号，例如 `901001` | 必须非零，与上游及同实例其他消费者不同 |
| `mysql-replication.row_image` | `minimal` 或 `full` | 默认 `minimal`，须匹配实际日志 |
| `mysql-replication.tls_ca` | 用于验证服务器的 CA 证书路径 | 可选，启用证书与域名验证 |
| `mysql-replication.connect_timeout_secs` | 连接等待秒数 | 默认 10，不能为零 |
| `mysql-replication.read_timeout_secs` | 读取等待秒数 | 默认 90，不能为零 |
| `max_transaction_bytes` | 当前来源可缓冲的事务内存预算 | 默认 64 MiB，不能为零；不是进程总内存上限 |
| `source.plugin` | 来源类型 | 默认 `mysql`，仍需 `[source]` 段 |
| `source.adapter` | 指定版本适配器 | 通常不填，按服务器和日志版本自动选择 |

相对路径以配置文件所在目录为基准。连接 URL 中的特殊字符需要正确编码。配置不填写日志文件与起点，它们来自检查点。

## 3. 检查并开始消费

```sh
cargo build --release --locked -p pikachu
export MYSQL_EAST_URL='mysql://cdc:password@127.0.0.1:3306'
./target/release/pikachu-cdc check cdc.toml
./target/release/pikachu-cdc run cdc.toml > changes.jsonl
```

`check` 只校验连接、服务器设置和选中表的主键，不消费数据。`run` 在没有检查点时自动建立起点，之后持续消费；变更写入 stdout，诊断写入 stderr。

| 需要做什么 | 命令 |
| --- | --- |
| 只检查设置 | `pikachu-cdc check cdc.toml` |
| 先保存当前起点，稍后消费 | `pikachu-cdc init cdc.toml` |
| 持续消费 | `pikachu-cdc run cdc.toml` |
| 处理当前可用事件后退出 | `pikachu-cdc run cdc.toml --once` |

首次建立起点从**当前提交位置**开始，不补发已有数据，也不做全量快照。`init` 不覆盖已有检查点。

## 4. 恢复、回放与多个来源

停止后再次执行 `run`，会从各来源上次确认的检查点继续。不要手动改写进度文件，也不要删除它来跳过积压。恢复依赖该位置保存的历史表结构，上游必须保留所需日志。每次连接都会检查选中表的当前主键，但历史日志仍用检查点里的表结构解释。

本地回放不连接数据库、不修改进度，使用匹配的检查点和一个日志文件：

```sh
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001 --row-image minimal
```

文件名必须与检查点中的文件名一致，日志要保留开头的格式说明；截断或未完成事务会报错。每次回放一个来源的一个文件。

多个实例各增加一个 `[[mysql-canal.sources]]` 条目，分别设置名称、连接、订阅表和检查点，见[多来源示例](../crates/pikachu/examples/config/multi-source.toml)。各来源独立恢复，不提供跨实例全局顺序或一致初始化快照。一个来源失败会停止本次运行，阻塞读取可能等到超时才结束。

## 怎样选择和切换模式

模式含义先看[事件示例](events.zh-CN.md#minimal-和-full-记录了什么)。配置默认 MINIMAL；FULL 来源填写 `row_image = "full"`。模式只校验，不修改数据库设置，也不自动检测历史日志。

临时覆盖指定来源时，使用来源名称；回放只指定文件的模式：

```sh
./target/release/pikachu-cdc run cdc.toml --once --row-image east=full
./target/release/pikachu-cdc replay east-state.json /path/to/binlog.000001 --row-image full
```

切换前先暂停写入，以当前模式消费到提交边界并停止消费者，再修改服务器和写入连接的设置，最后以新模式恢复。检查点不保存模式，回放要自行选择历史文件对应的模式。CDC 不提供 NOBLOB 模式。

## 在应用中直接使用库

只使用 Canal 时，[独立配置](../crates/mysql-canal/examples/config/multi-source.toml)从 `[[sources]]` 开始，省去最外面的 `mysql-canal` 包装。原始连接与解析接口、在其他产品中嵌入配置的方式见[Rust API 参考](event-api.zh-CN.md#replication-连接与解析-api)。
