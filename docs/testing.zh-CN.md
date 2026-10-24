# 测试指南

[English](testing.md) | 简体中文

[文档目录](README.zh-CN.md)

从仓库根目录运行。普通测试使用合成协议数据，实库测试默认忽略；两者的通过状态应分别报告。

## 本地检查

```sh
cargo fmt --all -- --check
cargo test --workspace --locked
cargo check --workspace --all-targets --locked
cargo clippy --workspace --all-targets --locked -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --workspace --no-deps --locked
```

## 合成测试

默认测试包括事件结构适配与零拷贝、多来源线程隔离、相同 table_id 不同列序、来源身份绑定、独立位点、输出失败、版本系列选择、旧文件重选适配器、自定义事件/行解码器与注册冲突；并使用独立构造的协议字节覆盖符号扩展、稀疏 NULL、多行与主键变更、长 CHAR、多种跳读类型、超过 64 列、table_id 复用、截断、CRC32/NONE、事务可见性、位点保存顺序与文件锁。已注册的 `decode_rows` 示例测试随每个 Rust CI 目标执行，校验默认/显式模式参数、完整操作输出、拒绝稀疏输入、表映射重用与边界清理、截断 UPDATE 及输出失败。编译失败文档测试验证模式类型隔离与镜像借用生命周期。它们不是 mysqlbinlog 导出的实库 fixture。

携带字段的泛型事件回归独立构造 TABLE_MAP 可选 TLV 和 ROWS 字节，覆盖列名、MSB 优先符号位图、紧凑字符列 collation 索引、主键前缀、未知/畸形元数据、未记录/NULL 字段、owned 视图保留历史定义、重映射和来源隔离。原始实库行场景还会将泛型 `next_event()` 的自动绑定结果与现有手动解码器逐项比较。

Canal 字段回归比较完整借用/owned JSON、历史列元数据、稀疏缺列与 SQL NULL、无符号整数及精确 DECIMAL、文本/字节和编码值、输入缓冲区复用、混合来源模式、镜像内存预算，以及序列化失败不推进检查点。实库预期同时比较 before/after 字段、主键和变动字段名；二进制 JSON 仅在专门的语义断言中解码。

## 专用 MySQL 测试

真实 MySQL 集成测试（显式执行，在独立测试服务上创建并删除唯一命名的测试库）：

```sh
(
set -eu
cargo test -p mysql-replication -p mysql-canal -p pikachu --test mysql_live --locked --no-run
export CDC_TEST_ROW_IMAGE='minimal' # Use full to test FULL images.
cleanup_mysql_cdc_test() {
    docker compose -f tests/infra/compose.yaml down -v
}
trap cleanup_mysql_cdc_test EXIT
MYSQL_IMAGE=mysql:8.4 docker compose -f tests/infra/compose.yaml up -d --wait --wait-timeout 180
export CDC_TEST_MYSQL_URL='mysql://root:cdc-test-only@127.0.0.1:13306'
export CDC_TEST_MYSQL_SERIES='8.4'
cargo test -p mysql-replication --test mysql_live --locked -- --ignored --nocapture --test-threads=1
cargo test -p mysql-canal --test mysql_live --locked -- --ignored --nocapture --test-threads=1
cargo test -p pikachu --test mysql_live --locked -- --ignored --nocapture --test-threads=1
)
```

无论专用容器启动或测试成功与否，本次测试结束都要执行 `down -v`。切换镜像或 `CDC_TEST_ROW_IMAGE` 时同步修改对应系列/模式，并先删除旧测试卷；服务器与写入会话使用相同模式。

每个处理层都显式注册自己的 `mysql_live` 测试目标。MySQL 矩阵先编译三层测试，再启动专用容器并串行执行：

| 包 | 场景数 | 验证契约 |
| --- | --- | --- |
| `mysql-replication` | 4 | 主键预检、上游模式不匹配及跳过设置检查、原始与泛型稀疏/完整镜像、借用 `next_view()` 解码、宽表多行操作、精确值、NULL 及非选中表仍交付 |
| `mysql-canal` | 12 | CDC 转换、提交事务、来源启动校验与检查点恢复 |
| `pikachu` | 3 | 在线 `check`/`init`/`run --once`、完整 JSON Lines、过滤、回滚、恢复不重复、DDL 及预检失败位点 |

这 19 个场景消费服务器实际生成的 binlog 或查询真实元数据。默认 workspace 测试只编译它们，保持忽略；不使用合成事件替代实库变更。Canal 的 12 个场景通过正式 `SourcePlugin` / `VersionAdapter` / `Processor` / sink 路径运行。CDC 输出比较包含 source、schema、table、operation、精确主键类型/值、previous_pk、changed 与 fields，缺失、额外或重复的事件都会失败。

| 场景 | 校验内容 |
| --- | --- |
| 启动主键检测 | CDC check/init 及首次/恢复消费拒绝无主键、只有 UNIQUE 索引、联合主键及不支持的类型/字符集；检查点不变，非白名单表不影响校验 |
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

替换 `MYSQL_IMAGE` 可分别测试 `mysql:5.7.44`、`mysql:8.0`、`mysql:8.4`、`mysql:9.7`；切换版本前须删除旧测试卷。`.github/workflows/mysql.yml` 在每个版本上运行三包全部 19 个场景，分别使用 MINIMAL 和 FULL 模式，覆盖 5.7、8.0 历史基线及目前全部 LTS 系列（8.4、9.7），系列标签跟踪补丁更新；不使用会跨系列移动的 `lts` 标签。CI 校验实际服务器系列，并记录具体补丁版本和所选 CDC 适配器。版本范围依据 [MySQL 官方 LTS 说明](https://dev.mysql.com/doc/refman/9.7/en/mysql-releases.html)，新增 LTS 系列时须同时增加矩阵项与版本适配器。本地没有 Docker/MySQL 环境时无法执行实库测试。

## 按模块验证

| 修改领域 | 回归入口 |
| --- | --- |
| 原始协议、CRC、借用生命周期 | `crates/mysql-replication/tests/events.rs` |
| Canal 行解码与布局适配 | `crates/mysql-canal/tests/integration/decoder.rs`、`event_adapters.rs` |
| 转换、提交与检查点 | 同目录 `converter.rs`、`processor.rs` |
| 多来源与回放 | 同目录 `multi_source.rs`、`replay.rs` |
| CLI 参数与输出 | `crates/pikachu/tests/integration/cli.rs` |

```sh
cargo test -p mysql-replication --locked
cargo test -p mysql-canal --locked
cargo test -p pikachu --locked
python3 -m unittest discover -s scripts/release -p 'test_*.py'
```

性能测试及其测量边界见[开发指南](development.zh-CN.md#性能设计)，发布包验证见[发布指南](releasing.md)。
