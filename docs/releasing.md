# CI 与 crates.io 发布

仓库包含四个包：`mysql-replication`（导入路径 `mysql_replication`）负责复制传输与原始 binlog 事件；`mysql-canal`（导入路径 `mysql_canal`）提供 CDC 转换、事务和检查点公共库；`mysql-cdc-rs`（导入路径 `mysql_cdc_rs`，目录 `crates/mysql-cdc`）聚合两个库，根路径及 `canal` 导出 CDC API，`replication` 导出原始事件与传输；`pikachu` 仅提供 `pikachu-cdc` CLI，依赖 canal 并注册默认 JSON Lines 打印处理器。原始事件的 `dump` 按字段逐行打印，仅用于调试。`Cdc` 接收配置，应用通过 `add_handle` 注册 `TransactionHandler` 或闭包，接收 `CommittedTransaction`。binlog 先经 `EventConverter` 转为轻量行变更，再由事务引擎提交；两种事件类型独立。配置示例为 `crates/pikachu/examples/config/multi-source.toml`。四个包保持同版，依赖链为 CLI → canal → replication，聚合包同时依赖 canal 与 replication，精确依赖工作区版本。四个包统一通过 GitHub OIDC Trusted Publishing 认证，按依赖顺序上传复制库、canal 库、聚合包和 CLI，不读取长期 API token secret。

## CI 矩阵

`.github/workflows/ci.yml` 在 PR、main/master 推送和手动执行时运行，也供发布流程复用。

| 检查 | 范围 |
| --- | --- |
| 代码质量 | stable Rust：rustfmt、Clippy（warnings 拒绝）、rustdoc（warnings 拒绝） |
| Rust 测试 | Ubuntu / Rust 1.89.0、Ubuntu / stable、macOS / stable |
| 目标编译 | 同一 Rust 矩阵检查库、CLI、示例和 benchmark |
| 实库测试 | Ubuntu / MySQL 5.7、8.0、8.4 LTS、9.7 LTS，ROW + MINIMAL |
| 发布包 | `cargo publish --workspace --dry-run --locked --registry crates-io` |
| 发布保护 | Python 测试验证 tag 必须精确匹配 Cargo 版本，包括预发布版本 |

所有矩阵独立执行，单项失败不会取消其他项。`CI success` 汇总所有任务，可设为分支保护的必需检查。MySQL 矩阵由 `mysql-compatibility.yml` 统一维护，也可单独手动运行。当前运行时的位点持久化依赖 Unix 文件/目录同步语义，因此测试系统先覆盖 Linux 与 macOS。

每个 MySQL 版本都会串行执行 `mysql_live` 的全部 11 个实库场景，校验单行/多行 INSERT、UPDATE、DELETE、NULL 稀疏字段、整数边界、字符串/DECIMAL/数值主键变更、80 列位图、跨库表白名单、实时提交可见性、回滚、位点恢复、sink 失败重放，以及 DDL 停止边界。测试实际执行 SQL 并逐条比较 CDC 输出，不仅检查是否能连接服务器。普通协议回归另行覆盖 NOT NULL 列的非法 NULL、未知 ROTATE post-header 长度、畸形生产者版本及 MariaDB 标识拒绝，并检查错误类别与磁盘检查点；这些合成场景不计为实库验证。测试失败、少/多/重复输出或位点错误均阻止发布；完整覆盖表见 [README: Testing](../README.md#testing) 或[中文版“验证”](../README.zh-CN.md#验证)。

截至 2026-10，MySQL 矩阵保留 5.7 与 8.0 历史基线，并覆盖其后全部已发布 LTS 系列 8.4、9.7，依据 [MySQL 官方版本说明](https://dev.mysql.com/doc/refman/9.7/en/mysql-releases.html)。5.7 固定最终版 `mysql:5.7.44`，其余分别使用 `mysql:8.0`、`mysql:8.4`、`mysql:9.7` 系列标签跟踪可用补丁；不用 `latest` 或 `lts` 替代显式系列。实际补丁版本会写入测试日志，并用 `CDC_TEST_MYSQL_SERIES` 校验镜像确实属于对应系列。新 LTS 系列发布时，在矩阵和 `VersionRegistry` 中同时增加对应项。9.0 是 Innovation 系列，适配器保留，但不列入 LTS CI 矩阵。

打包检查会从生成的 `.crate` 解包并编译，检查包内是否缺文件，但不会上传；参考 [Cargo 官方发布说明](https://doc.rust-lang.org/cargo/reference/publishing.html)。复制库包包含其源码、原始事件协议测试、README、许可证与生成的 lockfile；canal 包包含 CDC 库、监听/适配器示例、CDC/转换/实库测试、benchmark、配置示例、README、许可证与生成的 lockfile；聚合包包含重导出入口、监听与原始事件示例、双语 README、许可证与生成的 lockfile；`crates/pikachu` 包包含 CLI、默认打印 handle、CLI 测试、配置示例、README、许可证与生成的 lockfile；根目录是纯 workspace，发布文档保留在根目录 `docs/` 下。包不包含工作流、AI 协作文件、本地 state 或凭据。打包使用 stable Cargo 的 workspace 发布能力，在临时本地 registry 中校验尚未上传的库依赖；保持 Rust 1.89 编译测试。自定义 registry 镜像替换可能影响 workspace 本地依赖校验，应在使用官方 crates.io 的独立 Cargo 配置中执行打包检查。

## 四个包的 OIDC Trusted Publishing 配置

`mysql-replication`、`mysql-canal`、`mysql-cdc-rs` 和 `pikachu` 均使用 GitHub Actions OIDC Trusted Publishing。发布前，以各 crate 所有者身份分别打开以下设置页，在 **Trusted Publishing** 中添加 GitHub 发布者。每个包都需要独立配置，不能只配置 CLI；依据 [crates.io 官方说明](https://crates.io/docs/trusted-publishing)。

| 包 | 设置页 |
| --- | --- |
| `mysql-replication` | [Trusted Publishing 设置](https://crates.io/crates/mysql-replication/settings) |
| `mysql-canal` | [Trusted Publishing 设置](https://crates.io/crates/mysql-canal/settings) |
| `mysql-cdc-rs` | [Trusted Publishing 设置](https://crates.io/crates/mysql-cdc-rs/settings) |
| `pikachu` | [Trusted Publishing 设置](https://crates.io/crates/pikachu/settings) |

四个包均填写相同的发布者信息：

| 字段 | 值 |
| --- | --- |
| Repository owner | `tiyee` |
| Repository name | `mysql-cdc-rs` |
| Workflow filename | `release.yml`（只填文件名） |
| Environment | 留空（当前发布 job 未指定 GitHub environment） |

CLI 包已从 `pikachu-rs` 改名为 `pikachu`，位于 `crates/pikachu`；旧包的可信发布者配置不会随本地改名迁移，必须为 `pikachu` 单独配置。

`release.yml` 只在 `publish` job 授予 `contents: read` 和 `id-token: write`，通过官方 [`rust-lang/crates-io-auth-action@v1`](https://github.com/rust-lang/crates-io-auth-action) 交换短期凭据。OIDC 认证在任何包上传之前执行，四个发布步骤各自通过 `CARGO_REGISTRY_TOKEN: ${{ steps.auth.outputs.token }}` 使用同一短期 token；任务结束后 action 自动撤销该凭据。发布仍按依赖顺序执行：

```sh
cargo publish --locked --registry crates-io --package mysql-replication
cargo publish --locked --registry crates-io --package mysql-canal
cargo publish --locked --registry crates-io --package mysql-cdc-rs
cargo publish --locked --registry crates-io --package pikachu
```

四个包的版本号必须一致。普通 CI 和版本检查没有 OIDC 写权限，也不需要发布凭据；CI 的 `cargo publish --workspace --dry-run` 只验证包内容，不上传任何包。

可信发布者的仓库与 workflow 文件名必须匹配实际执行的流程。若以后指定 GitHub environment，必须同步设置 job 的 `environment` 与 crates.io 的对应字段。工作流代码改为 OIDC 不会自动修改 crates.io 的可信发布者设置，须分别确认四个包的配置。

发布工作流不再读取 `secrets.CARGO_REGISTRY_TOKEN`。确认四个包的 OIDC 发布均成功后，可删除 GitHub 仓库中旧的 `CARGO_REGISTRY_TOKEN` secret，并在 crates.io 撤销已不再使用的长期 API token；不要仅删除 secret 而保留无用途的有效凭据。

## 发布版本

同步修改 `crates/pikachu/Cargo.toml`、`crates/mysql-canal/Cargo.toml`、`crates/mysql-replication/Cargo.toml` 与 `crates/mysql-cdc/Cargo.toml` 的 `package.version`，以及 CLI 的 `mysql-canal`、canal 的 `mysql-replication` 和聚合包的两个库精确依赖版本。执行 `cargo check --workspace` 更新 lockfile 并一同提交。版本保护脚本要求四个包同版、tag 精确匹配该版本，并验证库名称、依赖路径与版本；任一不匹配均拒绝发布。先让主分支 CI 通过，再推送与版本完全一致的新 tag。旧包 `pikachu-rs 0.0.1` 已完成首次发布；此记录不代表新包已发布。本次四个包统一为 `0.0.4`，对应标签为：

```sh
git tag v0.0.4
git push origin v0.0.4
```

`release.yml` 只响应 `v*.*.*` tag 推送，按顺序执行：校验 tag → 在该 tag 提交上复用完整 CI → OIDC 认证 → 使用短期 token 发布 `mysql-replication` → 发布 `mysql-canal` → 发布 `mysql-cdc-rs` → 发布 `pikachu`。可信发布者未配置或不匹配、OIDC 认证失败、版本不匹配、任一矩阵失败或打包失败都会阻止发布。`v0.0.4-rc.1` 等预发布 tag 也必须与 Cargo 版本精确一致。

发布成功后，使用方可添加：

```toml
[dependencies]
mysql-cdc-rs = "0.0.4"
```

统一库入口为 [mysql-cdc-rs](https://crates.io/crates/mysql-cdc-rs)，API 文档为 [docs.rs](https://docs.rs/mysql-cdc-rs)，直接导入 `mysql_cdc_rs`。CDC 类型可从根路径或 `canal` 导入，原始事件从 `replication` 导入；底层 [mysql-canal](https://crates.io/crates/mysql-canal) 与 [mysql-replication](https://crates.io/crates/mysql-replication) 仍可独立使用。CLI 使用 `cargo install pikachu` 安装。本次库与 CLI 均使用 `0.0.4`；不能覆盖旧包已发布的 `pikachu-rs 0.0.1`。工作流本身不会创建 GitHub Release 或推送 Git tag。

四个包发布不是原子操作。若部分包已上传而后续包失败，先确认各 crate 的实际状态，不要直接重跑整个发布 job 重复上传；可按依赖顺序恢复尚未发布的包。已成功上传的版本不能重复发布；修改内容需要同步增加四个包的版本号，并使用匹配的新 tag。
