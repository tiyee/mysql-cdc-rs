# CI 与 crates.io 发布

公共 crate 名称为 `pikachu-rs`，Rust 导入路径为 `pikachu_rs`，仓库名仍为 `pikachu-rs`。同一个包包含库和 `pikachu-cdc` CLI。

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

每个 MySQL 版本都会串行执行 `mysql_live` 的全部 11 个实库场景，校验单行/多行 INSERT、UPDATE、DELETE、NULL 稀疏字段、整数边界、字符串/DECIMAL/数值主键变更、80 列位图、跨库表白名单、实时提交可见性、回滚、位点恢复、sink 失败重放，以及 DDL 停止边界。测试实际执行 SQL 并逐条比较 CDC 输出，不仅检查是否能连接服务器。测试失败、少/多/重复输出或位点错误均阻止发布；完整覆盖表见 README 的“验证”部分。

截至 2026-10，MySQL 矩阵保留 5.7 与 8.0 历史基线，并覆盖其后全部已发布 LTS 系列 8.4、9.7，依据 [MySQL 官方版本说明](https://dev.mysql.com/doc/refman/9.7/en/mysql-releases.html)。5.7 固定最终版 `mysql:5.7.44`，其余分别使用 `mysql:8.0`、`mysql:8.4`、`mysql:9.7` 系列标签跟踪可用补丁；不用 `latest` 或 `lts` 替代显式系列。实际补丁版本会写入测试日志，并用 `CDC_TEST_MYSQL_SERIES` 校验镜像确实属于对应系列。新 LTS 系列发布时，在矩阵和 `VersionRegistry` 中同时增加对应项。9.0 是 Innovation 系列，适配器保留，但不列入 LTS CI 矩阵。

打包检查会从生成的 `.crate` 解包并编译，检查包内是否缺文件，但不会上传；参考 [Cargo 官方发布说明](https://doc.rust-lang.org/cargo/reference/publishing.html)。包包含源码、示例、测试、benchmark、文档、README、许可证与 lockfile；不包含工作流、本地 state 或凭据。

## 首次配置

1. 在 crates.io 创建账号，并生成有权发布 `pikachu-rs` 的 API token。首次发布需要创建新 crate 的权限；后续发布需要对应 crate 的所有者权限。crate 名称是否可用以 crates.io 实际结果为准。
2. 在 GitHub 仓库 **Settings → Secrets and variables → Actions → New repository secret** 添加 `CARGO_REGISTRY_TOKEN`，值为上面的 token，见 [GitHub secrets 文档](https://docs.github.com/en/actions/how-tos/write-workflows/choose-what-workflows-do/use-secrets)。此 token 只注入发布步骤，PR 与普通 CI 不需要它。
3. 提交所有源码、`Cargo.lock` 和 `.github/workflows/`，先让主分支 CI 通过。

## 发布版本

修改 `Cargo.toml` 的 `package.version`，执行 `cargo check --locked`；如果因根包版本变化要求更新 lockfile，则运行 `cargo check` 并一同提交生成的 `Cargo.lock`。提交该版本后推送与版本完全一致的 tag，例如当前 `0.0.1`：

```sh
git tag v0.0.1
git push origin v0.0.1
```

`release.yml` 只响应 `v*.*.*` tag 推送，按顺序执行：校验 tag → 在该 tag 提交上复用完整 CI → `cargo publish --locked --registry crates-io --package pikachu-rs`。缺少 token、版本不匹配、任一矩阵失败或打包失败都会阻止发布。`v0.0.2-rc.1` 等预发布 tag 也必须与 Cargo 版本精确一致。

发布成功后，使用方可添加：

```toml
[dependencies]
pikachu-rs = "0.0.1"
```

发布入口为 [crates.io](https://crates.io/crates/pikachu-rs)，API 文档入口为 [docs.rs](https://docs.rs/pikachu-rs)。这些链接在首次发布成功后才可用。工作流本身不会创建 GitHub Release 或推送 Git tag。

发布任务临时网络失败时，可在 GitHub Actions 重跑失败任务；若该版本已成功上传，则不能重复发布同一版本，应先确认 crates.io 状态。修改内容需要增加版本号并使用新 tag。
