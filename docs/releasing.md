# CI 与 crates.io 发布

[文档目录](README.zh-CN.md)

本页仅说明 CI、版本、打包和发布。系统使用见[README](../README.zh-CN.md)，本地与实库测试命令及覆盖范围见[测试指南](testing.zh-CN.md)，源码和扩展方式见[开发指南](development.zh-CN.md)。四个包保持同版，按 `mysql-replication` → `mysql-canal` → `mysql-cdc-rs` → `pikachu` 的依赖顺序发布。

## CI 矩阵

`.github/workflows/ci.yml` 在 PR、main/master 推送和手动执行时运行，也供发布流程复用。

| 检查 | 范围 |
| --- | --- |
| 代码质量 | stable Rust：rustfmt、Clippy（warnings 拒绝）、rustdoc（warnings 拒绝） |
| Rust 测试 | Ubuntu / Rust 1.89.0、Ubuntu / stable、macOS / stable |
| 目标编译 | 同一 Rust 矩阵检查库、CLI、示例和 benchmark |
| 实库测试 | Ubuntu / MySQL 5.7、8.0、8.4 LTS、9.7 LTS，分别运行 ROW + MINIMAL / FULL |
| 发布包 | `cargo publish --workspace --dry-run --locked --registry crates-io` |
| 发布保护 | Python 测试验证 tag 必须精确匹配 Cargo 版本，包括预发布版本 |

所有矩阵独立执行，单项失败不会取消其他项。`CI success` 汇总所有任务，可设为分支保护的必需检查。MySQL 矩阵由 `mysql.yml` 统一维护，也可单独手动运行。当前运行时的位点持久化依赖 Unix 文件/目录同步语义，因此测试系统先覆盖 Linux 与 macOS。

MySQL 矩阵按每个系列的 MINIMAL / FULL 两种模式启动专用容器，串行执行 replication、canal 和 CLI 的实库目标。完整场景与本地命令集中维护在[测试指南](testing.zh-CN.md)。合成协议回归和实库验证分别执行，不能互相替代。

截至 2026-10，MySQL 矩阵保留 5.7 与 8.0 历史基线，并覆盖其后全部已发布 LTS 系列 8.4、9.7，依据 [MySQL 官方版本说明](https://dev.mysql.com/doc/refman/9.7/en/mysql-releases.html)。5.7 固定最终版 `mysql:5.7.44`，其余分别使用 `mysql:8.0`、`mysql:8.4`、`mysql:9.7` 系列标签跟踪可用补丁；不用 `latest` 或 `lts` 替代显式系列。实际补丁版本会写入测试日志，并用 `CDC_TEST_MYSQL_SERIES` 校验镜像确实属于对应系列。新 LTS 系列发布时，在矩阵和 `VersionRegistry` 中同时增加对应项。9.0 是 Innovation 系列，适配器保留，但不列入 LTS CI 矩阵。

打包检查会从生成的 `.crate` 解包并编译，检查包内是否缺文件，但不会上传；参考 [Cargo 官方发布说明](https://doc.rust-lang.org/cargo/reference/publishing.html)。复制库包包含其源码、原始事件协议/实库测试、README、许可证与生成的 lockfile；canal 包包含 CDC 库、监听/适配器示例、CDC/转换/实库测试、benchmark、配置示例、README、许可证与生成的 lockfile；聚合包包含重导出入口、监听与原始事件示例、双语 README、许可证与生成的 lockfile；`crates/pikachu` 包包含 CLI、默认打印 handle、CLI 离线/实库测试、配置示例、README、许可证与生成的 lockfile；根目录是纯 workspace，发布文档保留在根目录 `docs/` 下。包不包含工作流、AI 协作文件、本地 state 或凭据。打包使用 stable Cargo 的 workspace 发布能力，在临时本地 registry 中校验尚未上传的库依赖；保持 Rust 1.89 编译测试。自定义 registry 镜像替换可能影响 workspace 本地依赖校验，应在使用官方 crates.io 的独立 Cargo 配置中执行打包检查。

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

`release.yml` 只在 `publish` job 授予 `contents: read` 和 `id-token: write`，通过官方 [`rust-lang/crates-io-auth-action@v1`](https://github.com/rust-lang/crates-io-auth-action) 交换短期凭据。OIDC 认证在任何包上传之前执行，四个发布步骤各自通过 `CARGO_REGISTRY_TOKEN: ${{ steps.auth.outputs.token }}` 使用同一短期 token；任务结束后 action 自动撤销该凭据。发布仍按依赖顺序执行：

```sh
cargo publish --locked --registry crates-io --package mysql-replication
cargo publish --locked --registry crates-io --package mysql-canal
cargo publish --locked --registry crates-io --package mysql-cdc-rs
cargo publish --locked --registry crates-io --package pikachu
```

四个包的版本号必须一致。普通 CI 和版本检查没有 OIDC 写权限，也不需要发布凭据；CI 的 `cargo publish --workspace --dry-run` 只验证包内容，不上传任何包。

可信发布者的仓库与 workflow 文件名必须匹配实际执行的流程。若以后指定 GitHub environment，必须同步设置 job 的 `environment` 与 crates.io 的对应字段。工作流不会自动修改 crates.io 的可信发布者设置，须分别确认四个包的配置。

## 发布版本

当前四个包的清单版本为 `2.0.1`，本页说明首次发布的准备和执行流程。配置以[当前配置指南](configuration.zh-CN.md)为准，事件类型和 MINIMAL/FULL 输出契约见[事件参考](events.zh-CN.md)。

首版事件 API 使用 `BinlogEvent<M>` 泛型贯穿模式；ROWS 自动关联历史列定义，行操作采用 `RowOperation`，FULL / MINIMAL 的 `field()` 分别返回 `Field` / `Option<Field>`，越界均报错。发布检查应包含双语事件文档、可编译示例、元数据/生命周期回归和实库自动绑定场景；不保留早期设计的旧签名或兼容别名。`canal::Canal::new(AppConfig)` 在 canal 模块内管理来源生命周期，聚合库仅重导出两个库模块，FULL / MINIMAL 泛型属于复制事件及 Canal 解码器；发布前校验混合来源的事务交付及各自检查点。版本保护以 workspace 根清单为入口，并核对根目录 `Cargo.lock` 中的四包版本。

发布按以下顺序准备：

1. 同步四个成员 `Cargo.toml` 的 `package.version`，以及 CLI 的 `mysql-canal`、canal 的 `mysql-replication`、聚合包两个库的精确依赖版本；同步根目录及各包双语 README 的版本示例。
2. 执行 `cargo check --workspace --all-targets` 更新根目录 `Cargo.lock`，确认只改变预期的工作区版本，不无关升级依赖。运行下面的本地检查。
3. 仅暂存本次项目文件，检查暂存内容后提交。保持工作区干净，再执行打包验证，以便包内 `.cargo_vcs_info.json` 对应实际发布提交。
4. 检查四个包的内容，将版本提交推送到主分支并等待完整 CI。确认每个 crate 的可信发布者配置，以及该版本尚未上传。
5. 检查远端不存在同名标签，按清单版本创建新的附注标签并推送；发布工作流会在标签提交上重新执行完整 CI。

本地检查按照[测试指南](testing.zh-CN.md#本地检查)执行；修改发布脚本时也运行该页的 Python 回归测试。

提交后验证四个发布包：

```sh
cargo publish --workspace --dry-run --locked --registry crates-io
```

该命令在临时本地 registry 中解析尚未上传的成员依赖，并逐包解包编译，不实际上传。不要通过删除 `--locked`、跳过验证或先上传库来绕过依赖解析失败。存在自定义 registry 镜像替换时，可在仓库外目录使用独立 `CARGO_HOME`，并通过 `--manifest-path` 指向 workspace；不要修改用户全局配置。

检查 `target/package` 下四个 `<crate>-<version>.crate`（workspace dry-run 可能将归档放在 `tmp-crate/` 子目录）中的文件列表、清单、精确依赖版本及 `.cargo_vcs_info.json`。确认包含预期源码、示例、测试、README、许可证及生成的 lockfile，不包含 AI 协作文件、本地 state 或凭据。需要分发本地归档时，可将验证后的四个包复制出来并生成 SHA256 校验文件；这些构建产物不加入源码提交。

以下是发布标签模板，先将 `v<version>` 替换为四个清单中完全一致的版本。版本保护脚本同时校验包名、内部依赖路径和精确版本。`git ls-remote` 若返回已有同名标签，应停止，不覆盖该标签：

```sh
set -eu
RELEASE_TAG='v<version>'
python3 scripts/release/check_version.py "$RELEASE_TAG"
EXISTING_RELEASE_TAG=$(git ls-remote origin "refs/tags/$RELEASE_TAG" "refs/tags/$RELEASE_TAG^{}")
if test -n "$EXISTING_RELEASE_TAG"; then
    printf 'Release tag already exists: %s\n' "$RELEASE_TAG" >&2
    exit 1
fi
git tag -a "$RELEASE_TAG" -m "Release $RELEASE_TAG"
git push origin "refs/tags/$RELEASE_TAG:refs/tags/$RELEASE_TAG"
git ls-remote origin "refs/tags/$RELEASE_TAG" "refs/tags/$RELEASE_TAG^{}"
```

`release.yml` 只响应 `v*.*.*` tag 推送，按顺序执行：校验 tag → 在该 tag 提交上复用完整 CI → OIDC 认证 → 使用短期 token 发布 `mysql-replication` → 发布 `mysql-canal` → 发布 `mysql-cdc-rs` → 发布 `pikachu`。可信发布者未配置或不匹配、OIDC 认证失败、版本不匹配、任一矩阵失败或打包失败都会阻止发布。`v1.0.0-rc.1` 等预发布 tag 也必须与 Cargo 版本精确一致。

发布成功后，使用方可添加：

```toml
[dependencies]
mysql-cdc-rs = "2.0.1"
```

统一库入口为 [mysql-cdc-rs](https://crates.io/crates/mysql-cdc-rs)，API 文档为 [docs.rs](https://docs.rs/mysql-cdc-rs)，直接导入 `mysql_cdc_rs`。聚合库根路径仅重导出 `canal` 与 `replication`：事务消费通过 `canal::Canal::new` 构造，配置和错误类型从对应模块导入；底层 [mysql-canal](https://crates.io/crates/mysql-canal) 与 [mysql-replication](https://crates.io/crates/mysql-replication) 仍可独立使用。CLI 使用 `cargo install pikachu --version 2.0.1 --locked` 安装，命令名为 `pikachu-cdc`。四个包清单版本均为 `2.0.1`。工作流本身不会创建 GitHub Release 或推送 Git tag。

四个包发布不是原子操作。若部分包已上传而后续包失败，先确认各 crate 的实际状态，不要直接重跑整个发布 job 重复上传；可按依赖顺序恢复尚未发布的包。已成功上传的版本不能重复发布；修改内容需要同步增加四个包的版本号，并使用匹配的新 tag。

## 发布结果核对

| 状态 | 核对依据 |
| --- | --- |
| 本地包验证通过 | 四个包的 dry-run 均成功，解包编译通过，内容及版本已检查 |
| 标签推送成功 | 远端 `refs/tags/<tag>^{}` 解析到预期发布提交；附注标签本身的对象 ID 与提交 ID 不同 |
| 完整 CI 通过 | 对应标签的版本校验、质量、Rust 矩阵、MySQL 矩阵、打包及汇总 job 均成功 |
| crates.io 发布成功 | publish job 的四次上传成功，并逐一确认每个 crate 的对应版本可用 |

不要将 dry-run 日志中的 `Uploading` 行当成上传成功：其后的 `aborting upload due to dry run` 表示未上传。标签推送成功也不能替代 CI 和四包发布结果确认。
