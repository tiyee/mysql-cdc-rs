# CI 与 crates.io 发布

仓库是两个包组成的 Cargo workspace：`mysql-replication` 是可复用库（导入路径 `mysql_replication`），`pikachu-rs` 是依赖该库的 CLI 包（命令名 `pikachu-cdc`），CLI 注册默认打印 handle。库的配置驱动入口为 `Replication`，应用通过 `add_handle` 注册 `EventHandler` 或闭包。配置示例为 `examples/config/multi-source.toml`。两个包保持相同版本，CLI 精确依赖同版库。tag 自动发布先使用 API token 上传 `mysql-replication`，再使用 OIDC Trusted Publishing 上传 `pikachu-rs`。

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

每个 MySQL 版本都会串行执行 `mysql_live` 的全部 11 个实库场景，校验单行/多行 INSERT、UPDATE、DELETE、NULL 稀疏字段、整数边界、字符串/DECIMAL/数值主键变更、80 列位图、跨库表白名单、实时提交可见性、回滚、位点恢复、sink 失败重放，以及 DDL 停止边界。测试实际执行 SQL 并逐条比较 CDC 输出，不仅检查是否能连接服务器。测试失败、少/多/重复输出或位点错误均阻止发布；完整覆盖表见 [README: Testing](../README.md#testing) 或[中文版“验证”](../README.zh-CN.md#验证)。

截至 2026-10，MySQL 矩阵保留 5.7 与 8.0 历史基线，并覆盖其后全部已发布 LTS 系列 8.4、9.7，依据 [MySQL 官方版本说明](https://dev.mysql.com/doc/refman/9.7/en/mysql-releases.html)。5.7 固定最终版 `mysql:5.7.44`，其余分别使用 `mysql:8.0`、`mysql:8.4`、`mysql:9.7` 系列标签跟踪可用补丁；不用 `latest` 或 `lts` 替代显式系列。实际补丁版本会写入测试日志，并用 `CDC_TEST_MYSQL_SERIES` 校验镜像确实属于对应系列。新 LTS 系列发布时，在矩阵和 `VersionRegistry` 中同时增加对应项。9.0 是 Innovation 系列，适配器保留，但不列入 LTS CI 矩阵。

打包检查会从生成的 `.crate` 解包并编译，检查包内是否缺文件，但不会上传；参考 [Cargo 官方发布说明](https://doc.rust-lang.org/cargo/reference/publishing.html)。库包包含其源码、监听/适配器示例、协议与实库测试、benchmark、README、许可证与生成的 lockfile；CLI 包包含入口、默认打印 handle、CLI 测试、配置示例、文档、README、许可证与 lockfile。包不包含工作流、AI 协作文件、本地 state 或凭据。打包使用 stable Cargo 的 workspace 发布能力，在临时本地 registry 中校验尚未上传的库依赖；保持 Rust 1.89 编译测试。自定义 registry 镜像替换可能影响 workspace 本地依赖校验，应在使用官方 crates.io 的独立 Cargo 配置中执行打包检查。

## mysql-replication API token 配置

`mysql-replication` 首次发布使用 crates.io API token，由 `release.yml` 自动执行。维护者在 GitHub 仓库 **Settings → Secrets and variables → Actions** 中添加名为 `CARGO_REGISTRY_TOKEN` 的 repository secret，值为有权发布该 crate 的 API token。

工作流仅在库发布步骤中通过 `secrets.CARGO_REGISTRY_TOKEN` 注入环境变量，不在源码中填写真实 token，也不输出凭据。该步骤执行：

```sh
cargo publish --locked --registry crates-io --package mysql-replication
```

库上传成功后才进行 CLI 的 OIDC 认证和发布。两个包的版本号必须一致。CI 的 `cargo publish --workspace --dry-run` 只验证包内容，不上传任何包，也不需要发布 token。当前库使用 token 发布；若以后切换到 Trusted Publishing，应另行调整该步骤及 crates.io 配置。

## pikachu-rs Trusted Publishing 配置

`pikachu-rs` 已完成首次发布，CLI 的自动发布继续使用 GitHub Actions OIDC Trusted Publishing。CLI 发布步骤不使用库的 API token secret；依据 [crates.io 官方说明](https://crates.io/docs/trusted-publishing)。

以 CLI crate 所有者身份打开 [pikachu-rs 设置](https://crates.io/crates/pikachu-rs/settings)，在 **Trusted Publishing** 中添加 GitHub 发布者：

| 字段 | 值 |
| --- | --- |
| Repository owner | `tiyee` |
| Repository name | `pikachu-rs` |
| Workflow filename | `release.yml`（只填文件名） |
| Environment | 留空（当前发布 job 未指定 GitHub environment） |

`release.yml` 只在 `publish` job 授予 `contents: read` 和 `id-token: write`，通过官方 [`rust-lang/crates-io-auth-action@v1`](https://github.com/rust-lang/crates-io-auth-action) 交换短期凭据。`cargo publish` 的 `CARGO_REGISTRY_TOKEN` 环境变量来自 `steps.auth.outputs.token`，不是库发布使用的仓库 secret；任务结束后 action 自动撤销该短期凭据。普通 CI 和版本检查没有 OIDC 写权限，也不需要发布凭据。

可信发布者的仓库与 workflow 文件名必须匹配实际执行的流程。若以后指定 GitHub environment，必须同步设置 job 的 `environment` 与 crates.io 的对应字段。

`CARGO_REGISTRY_TOKEN` secret 当前用于 `mysql-replication`，不能在 CLI 的 Trusted Publishing 成功后删除。库 token 应按需轮换；只有库也切换到 Trusted Publishing 并验证成功后，才移除该 secret 并撤销对应 token。

## 发布版本

同步修改根 `Cargo.toml` 与 `crates/mysql-replication/Cargo.toml` 的 `package.version`，以及 CLI 的 `mysql-replication` 精确依赖版本。执行 `cargo check --workspace` 更新 lockfile 并一同提交。版本保护脚本要求两个包同版、tag 精确匹配该版本，并验证库名称、依赖路径与版本；任一不匹配均拒绝发布。先让主分支 CI 通过，再推送与版本完全一致的新 tag。`pikachu-rs 0.0.1` 已完成首次发布。本次两个包统一为 `0.0.2`，对应标签为：

```sh
git tag v0.0.2
git push origin v0.0.2
```

`release.yml` 只响应 `v*.*.*` tag 推送，按顺序执行：校验 tag → 在该 tag 提交上复用完整 CI → 使用仓库 secret token 发布 `mysql-replication` → OIDC 认证 → `cargo publish --locked --registry crates-io --package pikachu-rs`。可信发布者未配置或不匹配、OIDC 认证失败、版本不匹配、任一矩阵失败或打包失败都会阻止发布。`v0.0.2-rc.1` 等预发布 tag 也必须与 Cargo 版本精确一致。

发布成功后，使用方可添加：

```toml
[dependencies]
mysql-replication = "0.0.2"
```

库入口为 [mysql-replication](https://crates.io/crates/mysql-replication)，API 文档为 [docs.rs](https://docs.rs/mysql-replication)。CLI 使用 `cargo install pikachu-rs` 安装。本次库与 CLI 均使用 `0.0.2`；不能覆盖已发布的 `pikachu-rs 0.0.1`。工作流本身不会创建 GitHub Release 或推送 Git tag。

两个包发布不是原子操作。若库已上传但 CLI 失败，先确认各 crate 的实际状态，不要直接重跑整个发布 job 重复上传库；可单独恢复尚未发布的 CLI。已成功上传的版本不能重复发布；修改内容需要同步增加两个包的版本号，并使用匹配的新 tag。
