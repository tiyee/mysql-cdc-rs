# CI 与 crates.io 发布

公共 crate 名称为 `pikachu-rs`，Rust 导入路径为 `pikachu_rs`，仓库名为 `pikachu-rs`。首版 `0.0.1` 即支持多来源，同一个包包含库和 `pikachu-cdc` CLI。配置示例统一使用 `examples/config/multi-source.toml`。

## CI 矩阵

`.github/workflows/ci.yml` 在 PR、main/master 推送和手动执行时运行，也供发布流程复用。

| 检查 | 范围 |
| --- | --- |
| 代码质量 | stable Rust：rustfmt、Clippy（warnings 拒绝）、rustdoc（warnings 拒绝） |
| Rust 测试 | Ubuntu / Rust 1.89.0、Ubuntu / stable、macOS / stable |
| 目标编译 | 同一 Rust 矩阵检查库、CLI、示例和 benchmark |
| 实库测试 | Ubuntu / MySQL 5.7、8.0、8.4 LTS、9.7 LTS，ROW + MINIMAL |
| 发布包 | `cargo publish --dry-run --locked --registry crates-io --package pikachu-rs` |
| 发布保护 | Python 测试验证 tag 必须精确匹配 Cargo 版本，包括预发布版本 |

所有矩阵独立执行，单项失败不会取消其他项。`CI success` 汇总所有任务，可设为分支保护的必需检查。MySQL 矩阵由 `mysql-compatibility.yml` 统一维护，也可单独手动运行。当前运行时的位点持久化依赖 Unix 文件/目录同步语义，因此测试系统先覆盖 Linux 与 macOS。

每个 MySQL 版本都会串行执行 `mysql_live` 的全部 11 个实库场景，校验单行/多行 INSERT、UPDATE、DELETE、NULL 稀疏字段、整数边界、字符串/DECIMAL/数值主键变更、80 列位图、跨库表白名单、实时提交可见性、回滚、位点恢复、sink 失败重放，以及 DDL 停止边界。测试实际执行 SQL 并逐条比较 CDC 输出，不仅检查是否能连接服务器。测试失败、少/多/重复输出或位点错误均阻止发布；完整覆盖表见 [README: Testing](../README.md#testing) 或[中文版“验证”](../README.zh-CN.md#验证)。

截至 2026-10，MySQL 矩阵保留 5.7 与 8.0 历史基线，并覆盖其后全部已发布 LTS 系列 8.4、9.7，依据 [MySQL 官方版本说明](https://dev.mysql.com/doc/refman/9.7/en/mysql-releases.html)。5.7 固定最终版 `mysql:5.7.44`，其余分别使用 `mysql:8.0`、`mysql:8.4`、`mysql:9.7` 系列标签跟踪可用补丁；不用 `latest` 或 `lts` 替代显式系列。实际补丁版本会写入测试日志，并用 `CDC_TEST_MYSQL_SERIES` 校验镜像确实属于对应系列。新 LTS 系列发布时，在矩阵和 `VersionRegistry` 中同时增加对应项。9.0 是 Innovation 系列，适配器保留，但不列入 LTS CI 矩阵。

打包检查会从生成的 `.crate` 解包并编译，检查包内是否缺文件，但不会上传；参考 [Cargo 官方发布说明](https://doc.rust-lang.org/cargo/reference/publishing.html)。包包含源码、示例、测试、benchmark、文档、README、许可证与 lockfile；不包含工作流、本地 state 或凭据。

## Trusted Publishing 配置

首次发布完成后，后续版本使用 GitHub Actions OIDC Trusted Publishing，无需在 GitHub 保存长期 crates.io API token。首次创建 crate 仍需 API token；本流程用于已经发布的 `pikachu-rs`，依据 [crates.io 官方说明](https://crates.io/docs/trusted-publishing)。

以 crate 所有者身份打开 [pikachu-rs 设置](https://crates.io/crates/pikachu-rs/settings)，在 **Trusted Publishing** 中添加 GitHub 发布者：

| 字段 | 值 |
| --- | --- |
| Repository owner | `tiyee` |
| Repository name | `pikachu-rs` |
| Workflow filename | `release.yml`（只填文件名） |
| Environment | 留空（当前发布 job 未指定 GitHub environment） |

`release.yml` 只在 `publish` job 授予 `contents: read` 和 `id-token: write`，通过官方 [`rust-lang/crates-io-auth-action@v1`](https://github.com/rust-lang/crates-io-auth-action) 交换短期凭据。`cargo publish` 的 `CARGO_REGISTRY_TOKEN` 环境变量来自 `steps.auth.outputs.token`，不是仓库 secret；任务结束后 action 自动撤销该凭据。普通 CI 和版本检查没有 OIDC 写权限，也不需要发布凭据。

可信发布者的仓库与 workflow 文件名必须匹配实际执行的流程。若以后指定 GitHub environment，必须同步设置 job 的 `environment` 与 crates.io 的对应字段。

确认 Trusted Publishing 发布成功后，可以删除旧的 GitHub `CARGO_REGISTRY_TOKEN` secret，并撤销旧的专用 crates.io 发布 token。工作流已不读取该 secret，也不回退到长期 API token。

## 发布版本

修改 `Cargo.toml` 的 `package.version`，执行 `cargo check --locked`；如果因根包版本变化要求更新 lockfile，则运行 `cargo check` 并一同提交生成的 `Cargo.lock`。先让主分支 CI 通过，再推送与版本完全一致的新 tag。`0.0.1` 已完成首次发布，例如下一版修改为 `0.0.2` 后：

```sh
git tag v0.0.2
git push origin v0.0.2
```

`release.yml` 只响应 `v*.*.*` tag 推送，按顺序执行：校验 tag → 在该 tag 提交上复用完整 CI → OIDC 认证 → `cargo publish --locked --registry crates-io --package pikachu-rs`。可信发布者未配置或不匹配、OIDC 认证失败、版本不匹配、任一矩阵失败或打包失败都会阻止发布。`v0.0.2-rc.1` 等预发布 tag 也必须与 Cargo 版本精确一致。

发布成功后，使用方可添加：

```toml
[dependencies]
pikachu-rs = "0.0.1"
```

发布入口为 [crates.io](https://crates.io/crates/pikachu-rs)，API 文档入口为 [docs.rs](https://docs.rs/pikachu-rs)。工作流本身不会创建 GitHub Release 或推送 Git tag。

发布任务临时网络失败时，可在 GitHub Actions 重跑失败任务；若该版本已成功上传，则不能重复发布同一版本，应先确认 crates.io 状态。修改内容需要增加版本号并使用新 tag。
