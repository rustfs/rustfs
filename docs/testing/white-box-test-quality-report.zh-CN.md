# RustFS 白盒测试质量检查

**Use this when:** you need the 2026-09-25 read-only audit of unit, integration, e2e, fuzz, fault, and release-validation coverage, including which gaps belong in which repository.
**Source of truth:** the tree at commit `edcc81a8fdac1901f8674050af9af2430d34cdce`, the workflows and scripts cited below, and the coverage artifact named in the coverage section. This file is a snapshot. Regenerate counts with the commands here; do not copy the tables into other documents.

本文只记录读到的代码和实际跑到的结果。本地没有编过测试，也没有在本机算出当前 HEAD 的行覆盖率。

## 1. 测量范围与限制

| 项 | 结果 |
|---|---|
| 主仓提交 | `edcc81a8fdac1901f8674050af9af2430d34cdce`（2026-09-25，`ci: replace ubuntu-latest runner with sm-standard-2 across workflows`） |
| 工作区成员 | `Cargo.toml` 的 `[workspace].members`：`rustfs` 加 `crates/` 下 50 个 crate，共 51 个。`fuzz/` 是独立 workspace，不在主 workspace 里 |
| 测试属性统计 | 对每个 crate 的 `src/**/*.rs` 与 `tests/**/*.rs` 做文本扫描：`#[test]`、`#[tokio::test]`、`proptest!`。这是属性出现次数，不是 nextest 展开后的用例数。`#[cfg(test)]` 里的属性也计入。未编译，因此 feature 门控下编不进去的测试仍被计入 |
| 覆盖率 | 未能在本机运行 `cargo llvm-cov` 或 tarpaulin。见第 4 节。使用的是 GitHub Actions run `32573798257`（2026-08-22，成功）的 `coverage-lcov-8` 产物。那次测量的树不是本次 HEAD |
| `rustfs/rustfs-release-validation` | 2026-09-25 对 `https://github.com/rustfs/rustfs-release-validation` 的访问返回 404。`gh repo view rustfs/rustfs-release-validation` 与 `gh repo list rustfs` 的公开仓库列表里都没有它。本仓检索不到该仓库名。文档里的 “release validation” 指的是 `scripts/run_ecstore_validation_suite.sh`，不是那个 GitHub 仓库 |
| `rustfs/auto-testing` | 同样 404，且 `.github/workflows/rustfs-functional-chain.yml` 写明它是私有仓库，`github.token` 读不到。脚本正文未读到。能确定的只有本仓 workflow 检出它之后调用的脚本名、触发方式和门禁关系 |
| `rustfs/s3chaos` | 已浅克隆，HEAD `e501cbfa09dca9d9f462a22f9bd4efcaa688938a`（2026-09-24，Merge pull request #101） |

本地工具链：`rustc 1.83.0`。`Cargo.toml` 的 `edition` 是 `2024`，`rust-version` 是 `1.98.1`，`rust-toolchain.toml` 的 channel 是 `stable`。`cargo llvm-cov` 不在 PATH 里。Edition 2024 从 1.85 起才可用，因此当前解释器无法编译这个 workspace，也就不能跑覆盖率或 nextest。CI 的覆盖率作业超时是 240 分钟（`.github/workflows/coverage.yml`），冷缓存要重编整个 workspace。本次没有启动那次编译。

## 2. 主仓测试分布

统计命令（在仓库根目录，对 `src` 与 `tests` 分别计数）：

```bash
# 概念上等价于本次使用的扫描：匹配 #[test]、#[tokio::test]、proptest!
# 排除 benches/ 与 examples/。数字是属性次数，不是 cargo nextest list 的用例数。
```

合计（51 个 workspace crate）：

| 指标 | 数量 |
|---|---|
| `src` 内 `#[test]` | 11026 |
| `src` 内 `#[tokio::test]` | 8285 |
| `src` 内二者之和 | 19311 |
| `src` 内 `proptest!` 块 | 18 |
| `src` 内 `#[ignore]` | 93 |
| 含 `tests/` 目录的 crate | 20（其中 `crates/madmin/tests` 只有 fixture JSON，没有 `.rs`） |
| `tests/**/*.rs` 里的 `#[test]` + `#[tokio::test]` | 773（`#[tokio::test]` 411，`#[test]` 362） |
| `tests/` 里的 `proptest!` 块 | 2 |
| 有 `benches/` 的 crate | 9，共 14 个 bench 源文件 |
| fuzz 二进制（`fuzz/Cargo.toml` 的 `[[bin]]`） | 5。不在主 workspace，PR 的 `cargo nextest` 不跑它们 |

`#[rstest]` 扫描结果为 0。属性测试是 `proptest!`，不是 quickcheck。

`crates/e2e_test` 的 846 个测试属性都在 `src/`（170 个 `.rs`），没有 `tests/` 目录。它被 `cargo nextest run --all --exclude e2e_test` 排除，只在 e2e profile 里跑。

### 2.1 按 crate

列：`src` 的 `#[test]` / `#[tokio::test]` / 二者之和 / `proptest!` 块；`tests/*.rs` 的测试属性之和；是否有 bench 源文件。

| Crate | src `#[test]` | src `#[tokio::test]` | src 合计 | `proptest!` | `tests/` 属性 | bench 文件 |
|---|---:|---:|---:|---:|---:|---:|
| `rustfs` | 3114 | 1891 | 5005 | 3 | 302 | 0 |
| `crates/ecstore` | 3024 | 2983 | 6007 | 2 | 35 | 6 |
| `crates/e2e_test` | 55 | 791 | 846 | 0 | 0 | 0 |
| `crates/scanner` | 534 | 368 | 902 | 0 | 21 | 0 |
| `crates/heal` | 201 | 500 | 701 | 0 | 74 | 0 |
| `crates/protocols` | 513 | 147 | 660 | 5 | 58 | 0 |
| `crates/kms` | 214 | 394 | 608 | 0 | 105 | 0 |
| `crates/targets` | 454 | 122 | 576 | 0 | 26 | 1 |
| `crates/obs` | 350 | 11 | 361 | 0 | 0 | 0 |
| `crates/iam` | 239 | 109 | 348 | 0 | 2 | 0 |
| `crates/filemeta` | 287 | 16 | 303 | 3 | 2 | 1 |
| `crates/madmin` | 191 | 18 | 209 | 0 | 0 | 0 |
| `crates/replication` | 198 | 0 | 198 | 0 | 2 | 0 |
| `crates/s3select-api` | 81 | 114 | 195 | 0 | 0 | 0 |
| `crates/rio` | 72 | 120 | 192 | 1 | 0 | 1 |
| `crates/s3select-query` | 89 | 98 | 187 | 0 | 0 | 0 |
| `crates/utils` | 193 | 22 | 215 | 1 | 0 | 1 |
| `crates/policy` | 119 | 48 | 167 | 0 | 20 | 0 |
| `crates/lifecycle` | 51 | 101 | 152 | 3 | 0 | 0 |
| `crates/object-data-cache` | 89 | 51 | 140 | 0 | 0 | 1 |
| `crates/lock` | 22 | 113 | 135 | 0 | 0 | 0 |
| `crates/notify` | 79 | 55 | 134 | 0 | 2 | 1 |
| `crates/io-metrics` | 112 | 9 | 121 | 0 | 1 | 1 |
| `crates/object-capacity` | 43 | 49 | 92 | 0 | 0 | 1 |
| `crates/log-analyzer` | 87 | 0 | 87 | 0 | 0 | 0 |
| `crates/scanner-metrics` | 26 | 49 | 75 | 0 | 0 | 0 |
| `crates/s3-client` | 47 | 18 | 65 | 0 | 0 | 0 |
| `crates/crypto` | 61 | 0 | 61 | 0 | 2 | 0 |
| `crates/data-usage` | 54 | 0 | 54 | 0 | 0 | 0 |
| `crates/protos` | 50 | 2 | 52 | 0 | 0 | 0 |
| `crates/io-core` | 37 | 12 | 49 | 0 | 0 | 0 |
| `crates/config` | 43 | 0 | 43 | 0 | 0 | 0 |
| `crates/audit` | 5 | 13 | 18 | 0 | 52 | 0 |
| `crates/trusted-proxies` | 22 | 0 | 22 | 0 | 46 | 0 |
| `crates/common` | 17 | 5 | 22 | 0 | 0 | 0 |
| `crates/checksums` | 26 | 0 | 26 | 0 | 0 | 0 |
| `crates/signer` | 30 | 0 | 30 | 0 | 0 | 0 |
| `crates/storage-api` | 29 | 2 | 31 | 0 | 0 | 0 |
| `crates/rio-v2` | 7 | 21 | 28 | 0 | 6 | 0 |
| `crates/concurrency` | 23 | 4 | 27 | 0 | 0 | 0 |
| `crates/credentials` | 24 | 0 | 24 | 0 | 0 | 0 |
| `crates/extension-schema` | 23 | 0 | 23 | 0 | 0 | 0 |
| `crates/zip` | 3 | 20 | 23 | 0 | 7 | 0 |
| `crates/keystone` | 15 | 4 | 19 | 0 | 10 | 0 |
| `crates/s3-types` | 21 | 0 | 21 | 0 | 0 | 0 |
| `crates/security-governance` | 20 | 0 | 20 | 0 | 0 | 0 |
| `crates/tls-runtime` | 17 | 3 | 20 | 0 | 0 | 0 |
| `crates/s3-ops` | 8 | 0 | 8 | 0 | 0 | 0 |
| `crates/heal-contracts` | 5 | 1 | 6 | 0 | 0 | 0 |
| `crates/license` | 2 | 0 | 2 | 0 | 0 | 0 |
| `crates/test-utils` | 0 | 1 | 1 | 0 | 0 | 0 |
| 合计 | 11026 | 8285 | 19311 | 18 | 773 | 14 |

`proptest!` 还出现在这些集成测试里：`crates/filemeta/tests/version_graph_roundtrip_proptest.rs`（1）、`crates/policy/tests/policy_eval_proptest.rs`（1）。加上 `src` 里的 18 块，共 20 个属性测试块。已读到的 `src` 位置包括 `crates/ecstore/src/erasure/coding/erasure.rs`、`crates/filemeta/src/filemeta.rs`、`crates/filemeta/src/fileinfo.rs`、`crates/lifecycle/src/core.rs`、`crates/utils/src/path.rs`、`crates/protocols/src/webdav/driver.rs`、`crates/protocols/src/ftps/driver.rs`、`crates/protocols/src/swift/object.rs`、`crates/rio/src/tee_reader.rs`、`crates/ecstore/src/bucket/utils.rs`、`rustfs/src/storage/options.rs`、`rustfs/src/on_demand_migration/list_through.rs`。块数不等于生成的 case 数；若干块把 `ProptestConfig` 的 cases 设成 24 到 256。

Bench 文件：

- `crates/ecstore/benches/`：`erasure_benchmark.rs`、`multipart_read_parts_benchmark.rs`、`single_block_non_inline_benchmark.rs`、`rename_data_meta_benchmark.rs`、`comparison_benchmark.rs`，以及 `storage_api/mod.rs`
- `crates/filemeta/benches/xl_meta_bench.rs`
- `crates/targets/benches/queue_store_benchmark.rs`
- `crates/io-metrics/benches/metrics_pipeline.rs`
- `crates/object-capacity/benches/capacity_scan.rs`
- `crates/object-data-cache/benches/cache_bench.rs`
- `crates/utils/benches/hash_hotpath_benchmark.rs`
- `crates/notify/benches/snapshot_mode_scan.rs`
- `crates/rio/benches/tee_reader.rs`

`docs/testing/README.md` 写明 benchmark 按需运行，从来不是门禁。`.github/workflows/performance-ab.yml` 是另一条线：nightly warp A/B，用预算判断回归，不是 Criterion。

### 2.2 Fuzz

`fuzz/README.md` 与 `fuzz/Cargo.toml` 注册 5 个 libFuzzer 目标：`bucket_validation`、`archive_extract`、`path_containment`、`local_metadata`、`policy_ingress`。`path_containment.rs` 与 `bucket_validation.rs` 各自 `mod` 一个 storage-api 辅助文件，它们不是独立目标。

没有 XML、HTTP 头、SigV4 或生命周期 XML 的 fuzz 目标。`local_metadata` 覆盖 `rustfs-filemeta` 解码和 `rustfs-utils` 块解压，这是已有的元数据 fuzz。

`.github/workflows/fuzz.yml`：相关路径的 PR 上做构建加每个目标 60 秒 smoke；每天 `17 2 * * *` 每个目标 300 秒。`docs/testing/ci-gates.md` 把它标成 report-only，不在 `scripts/ci_gate.py` 的 `CODE_JOBS` 里。崩溃产物会上传，但失败不挡住 PR 合并。

## 3. CI 实际跑什么

合并是否被挡住，以 `scripts/ci_gate.py` 的 `expected_results` 和 `docs/testing/ci-gates.md` 为准。`docs/testing/README.md` 与 `docs/testing/security-regressions.md` 仍把 PR 上的 `End-to-End Tests` 和 s3-tests 写成 report-only。这和当前门禁不一致：`e2e-tests` 与 `s3-implemented-tests` 在 `CODE_JOBS` 里，full 模式下必须 `success`，作业上禁止 `continue-on-error`。

### 3.1 每个非文档 PR 必须成功的代码作业

来自 `scripts/ci_gate.py` 的 `CODE_JOBS`，由 `.github/workflows/ci.yml` 的 `required-checks`（检查名 `Test and Lint`）汇总。文档-only PR 会把这些作业要求为 `skipped`。

| 作业 | 实际命令要点 |
|---|---|
| `test-and-lint` | `cargo clippy --all-targets -- -D warnings`（没有 `--all-features`）；`cargo nextest run --profile ci --all --exclude e2e_test`；`cargo test --all --doc`；之后 `scripts/check_test_wiring.py --check-core` 对照 `.config/ecstore-required-tests.json` |
| `offline-enrollment-e2e` | 离线注册边界（作业在 `ci.yml`） |
| `test-ilm-integration-serial` | `cargo nextest run -j1 --run-ignored ignored-only`，只跑被 `#[ignore]` 的 ILM 集成 |
| `test-and-lint-rio-v2` | `cargo clippy` / `cargo nextest` 带 `--features rio-v2`，包是 `rustfs` 与 `rustfs-ecstore` |
| `connect-short-credential-boundary` | `cargo test -p rustfs --test connect_registration --features connect-e2e-short-credentials`；release 构建带同一 feature 必须失败 |
| `test-and-lint-protocols` | 矩阵 `--features swift` 与 `--features sftp`，包是 `rustfs` 与 `rustfs-protocols` |
| `build-rustfs-debug-binary` | `python3 scripts/e2e_binary.py build --bins --features e2e-test-hooks` |
| `uring-integration` | `cargo test -p rustfs-ecstore --lib uring_ -- --test-threads=1` |
| `e2e-tests` | `e2e-smoke` profile，然后同一作业里无 `continue-on-error` 地跑 `scripts/e2e-run.sh`（s3s-e2e，钉在 s3s `62cb4a71dd759a6ec56b64c4c42fcc183a2c6a52`） |
| `s3-implemented-tests` | `scripts/s3-tests/run.sh`，默认 `TEST_SCOPE=implemented` |
| `s3-lifecycle-behavior-tests` | 同一 `run.sh`，生命周期行为名单 `scripts/s3-tests/lifecycle_behavior_tests.txt`（53 行） |

`e2e-full`、`build-rustfs-debug-binary-rio-v2`、`e2e-tests-rio-v2` 在 `OPTIONAL_JOBS`。`e2e-full` 只在 `merge_group`、`workflow_dispatch`，以及 push 到 `refs/heads/main` 时必须成功。rio-v2 的 debug 二进制和 e2e 只在 `schedule` 与 `workflow_dispatch` 必须成功。因此对象锁、multipart 鉴权、配额、校验和、加密等被 `e2e-full` 注释点名的重组，不会在每个 PR 上跑。

Clippy 与默认 nextest 都不带 `--all-features`。`rio-v2`、`swift`、`sftp` 有单独作业。`ftps` 与 `webdav` 出现在 nightly 的 `e2e-protocols`（`.github/workflows/e2e-replication-nightly.yml` 调用 `--features ftps,webdav,sftp`），不在每个 PR 的协议作业里。`pyroscope` 只出现在 `.github/workflows/build.yml` 的部分打包目标上，没有对应测试作业。

### 3.2 平台

PR 测试作业的 `runs-on` 是自托管 `sm-standard-*`（Linux）。`.github/workflows/build.yml` 的打包矩阵是六目标：`x86_64-unknown-linux-musl`、`aarch64-unknown-linux-musl`、`x86_64-unknown-linux-gnu`、`aarch64-unknown-linux-gnu`、`aarch64-apple-darwin`、`x86_64-pc-windows-msvc`。这是构建与打包，不是测试矩阵。main 上的 development push 会把矩阵收成 Linux。

`.github/workflows/windows-filesystem.yml` 在 Windows 上跑 `rustfs-ecstore` 的 rename 安全过滤，路径触发，`docs/testing/ci-gates.md` 标为 report-only。没有 macOS 测试作业。`nix.yml` 的 `nix flake check` 也是路径触发、report-only。

### 3.3 覆盖率、miri、sanitizer、审计

| 检查 | 现状 |
|---|---|
| `cargo llvm-cov` | `.github/workflows/coverage.yml`：周日 `43 7 * * 0`、手动，以及改到 `crates/iam`、`crates/kms`、`crates/policy`、`crates/crypto` 或基线文件的 PR。命令是 `cargo llvm-cov nextest --workspace --exclude e2e_test`，`NEXTEST_PROFILE=ci`。不测 doctest，不测 `e2e_test`。作业是 report-only |
| 安全 crate 棘轮 | `.config/coverage-baselines.toml` 的 `phase = "report-only"`，允许下降 1.0 个百分点。注释写明基线来自 run `29394996173`（2026-07-15）。`scripts/check_security_coverage.py` 把回退写进 summary，不使作业失败；证据缺失或损坏才失败关闭 |
| tarpaulin | 仓库 workflow 里没有 |
| miri | 全库检索 workflow / 脚本 / toml，没有 miri 作业 |
| ASan / TSan / MSan | 同样没有 |
| `cargo audit` | `.github/workflows/audit.yml` 写明不单开 `cargo-audit`，由 `cargo deny` 的 `advisories` 覆盖 RustSec |
| `cargo deny` | `deny.toml`：`advisories`、`sources`、`bans`、`licenses`，`all-features = true`。忽略 `RUSTSEC-2024-0436`（paste 无人维护）和 `RUSTSEC-2023-0071`（rsa Marvin 计时侧信道，注释写明尚无修复版本）。`bans.multiple-versions = "warn"`。`docs/testing/ci-gates.md` 把 `Cargo Deny` 标成路径触发的 report-only；每日 03:23 UTC 定时跑，失败不挡 PR |
| `unsafe` | `scripts/check_unsafe_code_allowances.sh` 要求 `unsafe_code` allow 附近有 SAFETY 注释。它在 quick checks 里，不证明每个 `unsafe` 块都有测试 |
| 依赖审查 | `audit.yml` 还有 workflow pin 报告和 dependency review，同样 report-only |

最近 30 次 `coverage.yml` 运行（2026-08-24 至 2026-09-02）全部是 `cancelled` 或 `failure`。最后一次成功是 2026-08-22 的 run `32573798257`（HEAD `09de44df0883`，作业名 `Workspace line coverage`，产物 `coverage-lcov-8`，未过期）。再往前的成功是 2026-08-16、2026-08-09、2026-07-26、2026-07-19、2026-07-15。因此“当前 main 的行覆盖率”没有成功测量。

那次成功导出（`cargo-llvm-cov` 0.8.7，llvm covexport 3.1.0）的全库行覆盖是 **489565 / 582333 = 84.07%**，函数 **50643 / 64468 = 78.56%**。branch 计数为 0（这次导出没有分支覆盖）。按路径把文件名含 `tests`、`_test.rs`、`_tests.rs` 或 `tests.rs` 的文件拆出去之后，其余文件是 **480542 / 570820 = 84.18%**。测试文件本身是 9023 / 11513 = 78.37%。这次拆分是对那份 JSON 的后处理，仍然是 2026-08-22 的树。

按 crate 的行覆盖（同一份 JSON，从低到高；只列出当时导出里出现的目录）：

| Crate | 行 | 行覆盖 | 函数覆盖 |
|---|---:|---:|---:|
| `crates/protos` | 2778/6721 | 41.33% | 35.47% |
| `crates/protocols` | 1365/3149 | 43.35% | 42.62% |
| `crates/trusted-proxies` | 1225/2316 | 52.89% | 59.02% |
| `crates/keystone` | 584/1071 | 54.53% | 41.46% |
| `crates/scanner` | 8065/11167 | 72.22% | 79.18% |
| `crates/tls-runtime` | 910/1251 | 72.74% | 65.82% |
| `crates/signer` | 1162/1546 | 75.16% | 73.87% |
| `crates/audit` | 1106/1444 | 76.59% | 71.99% |
| `crates/iam` | 9408/12032 | 78.19% | 72.69% |
| `crates/lock` | 4378/5532 | 79.14% | 69.27% |
| `crates/obs` | 13379/16782 | 79.72% | 81.52% |
| `rustfs` | 123007/153406 | 80.18% | 71.58% |
| `crates/targets` | 14961/18624 | 80.33% | 71.19% |
| `crates/heal` | 8548/10549 | 81.03% | 81.70% |
| `crates/ecstore` | 189736/216259 | 87.74% | 84.10% |
| `crates/filemeta` | 10679/12110 | 88.18% | 84.02% |
| `crates/kms` | 25410/28072 | 90.52% | 85.54% |
| `crates/policy` | 5717/6481 | 88.21% | 85.45% |
| `crates/crypto` | 464/489 | 94.89% | 80.85% |
| `crates/lifecycle` | 3890/3989 | 97.52% | 95.92% |
| `crates/config` | 685/694 | 98.70% | 100% |

导出里还有 `crates/replication` 89.58%、`crates/data-usage` 82.17%、`crates/rio` 83.42% 等，上表只保留和后面缺口相关的行。当时导出中没有这些当前 crate 目录：`crates/s3-client`、`crates/scanner-metrics`、`crates/license`、`crates/heal-contracts`。`e2e_test` 被命令排除。不能据此断言它们是 8 月 22 日之后才加入的。

`.config/coverage-baselines.toml` 的安全棘轮仍是 2026-07-15 的绝对行数（iam 5149/8131、kms 2950/4200、policy 4636/5464、crypto 469/494）。2026-08-22 的 iam 已是 9408/12032。棘轮比后来的成功测量更旧，而且只在改到那四个 crate 的 PR 上跑，并且不失败作业。

2026-08-22 导出里、当前树仍然存在、且可执行行不少于 300、行覆盖低于 50% 的非测试文件：

| 2026-08-22 行覆盖 | 文件 |
|---|---|
| 10.67%（431/4038） | `crates/protos/src/generated/proto_gen/node_service.rs`（生成代码） |
| 23.26%（80/344） | `crates/heal/src/heal/manager/auto_scan.rs` |
| 25.32%（80/316） | `crates/heal/src/heal/replacement_readiness.rs` |
| 32.04%（116/362） | `rustfs/src/storage/ecfs.rs` |
| 34.97%（114/326） | `crates/scanner/src/scanner_io/io_cache.rs` |
| 35.75%（507/1418） | `crates/protocols/src/webdav/driver.rs` |
| 39.17%（1184/3023） | `crates/ecstore/src/bucket/replication/replication_resyncer.rs` |
| 40.82%（220/539） | `rustfs/src/admin/handlers/policies.rs` |
| 43.07%（233/541） | `crates/filemeta/src/replication.rs` |
| 44.12%（210/476） | `crates/obs/src/telemetry/otel.rs` |
| 45.27%（431/952） | `crates/scanner/src/remote_scanner/stream.rs` |
| 46.49%（457/983） | `rustfs/src/storage/storage_api.rs` |

同一份导出里还有一组 `crates/ecstore` 的 client `api_*.rs` 低于 50%（含 multipart / streaming / remove / list）。那些路径在当前树里已经不存在，所以不能当成 HEAD 上的文件级缺口。它们只说明 8 月 22 日客户端写路径的覆盖偏低，之后发生过搬迁。

84% 不能解读成“高风险路径已经测完”。行覆盖把大量已执行的普通分支算进去，而下面第 5 节的缺口是行为与故障模型上的洞。

### 3.4 定时、兼容性与发布链

这些都不在 PR 的 `CODE_JOBS` 里。`docs/testing/ci-gates.md` 写明定时失败不会挡住 PR。

| Workflow | 触发 | 测什么 | 是否挡发布 |
|---|---|---|---|
| `.github/workflows/e2e-s3tests.yml` | 每周 | ceph/s3-tests 全量，单节点与 4 节点各四片。未实现特性的已分类失败只报告；回归、未分类、跑不完、基础设施错误使作业失败 | 不挡 tag 发布 |
| `.github/workflows/mint.yml` | 每周 + 手动 | MinIO mint，多 SDK。测试失败只进 summary；只有完全没有结果才失败 | 不挡。注释写明尚未收成基线门禁 |
| `.github/workflows/minio-interop.yml` | 每天 | 用 Docker 现场生成 MinIO 盘，跑 `rio-v2` 下被 `#[ignore]` 的 SSE 读回。KES/MinKMS 对象按设计不可读 | 不挡 PR |
| `.github/workflows/e2e-upgrade.yml` | 路径 PR、每周、以及数字 tag | `UPGRADE_SOURCE_VERSION` 钉在 `1.0.0-rc.5`。四格：直接升级、滚动升级、桶配置存活、回滚读当前桶元数据。PR 上 report-only | tag 上会跑，但是独立 workflow。`.github/workflows/build.yml` 不依赖它 |
| `.github/workflows/e2e-distributed.yml` | 存储敏感 PR + nightly | 4 节点 4 盘，含 chaos 与升级。新鲜度列表里有 | 不在 `ci_gate.py` 的 PR 必过集合里 |
| `.github/workflows/performance-ab.yml` | 每天 06:31 UTC | warp A/B 预算。手动可以 `--allow-regression` | 不挡 PR。合并后 24 小时内才发现回归 |
| `.github/workflows/nightly-gnu.yml` | 每天 | GNU 包、Vault、Vault HA failover | 给功能链提供包，不挡 GitHub Release |
| `.github/workflows/rustfs-functional-chain.yml` | `nightly-gnu` 的 schedule 完成，或手动 | 十二条打包套件，顺序固定 | 不挡 PR，也不被 `build.yml` 等待 |
| `.github/workflows/build.yml` → `docker.yml` / `package.yml` / `helm-package.yml` | tag / main | 构建、草稿 release、镜像漏洞扫描、DEB/RPM、Helm | 这是发布资产链本身。它不等待功能链、s3chaos 或 ecstore validation suite |

s3-tests 名单行数（`wc -l`）：`scripts/s3-tests/implemented_tests.txt` 556，`excluded_tests.txt` 317，`unimplemented_tests.txt` 37，`lifecycle_behavior_tests.txt` 53。PR 门禁跑 implemented 加生命周期行为名单，不跑 excluded。

`.config/ecstore-required-tests.json` 只固定一小集不变量：写仲裁、元数据回滚、过期写者丢锁、明文 Range、multipart 取消、未提交 LIST 版本隐藏、真实 MinIO xl.meta、损坏 part 数组、按需迁移三个源方言。`docs/testing/ci-gates.md` 写明这不是“所有存储不变量都已覆盖”，并且进程内重开测试不能证明掉电持久性。`crates/ecstore/tests/legacy_bitrot_read_test.rs` 在外部语料缺失时可以跳过，不能满足兼容性必过车道。

`scripts/run_ecstore_validation_suite.sh` 的 `quick` / `full` / `destructive` / `fuzz` 没有被任何 workflow 调用（`docs/testing/ecstore-validation-suite-design.md`）。`destructive` 才包含 `disk::local` 的 `crash_consistency`（掉电窗口里对象必须是旧版或新版，不能混杂）。

另有一组 `connect-*-acceptance.yml`（磁盘、锁、RPC、API、CPU/内存/线程画像）。抽查 `connect-top-disk-acceptance.yml` 只有 `workflow_dispatch`。它们不在 `scripts/ci_gate.py` 里。本次没有逐条读断言，不把它们算进存储正确性门禁。

### 3.5 `unwrap` / `expect` / `unsafe` 的静态扫描

对 `src/**/*.rs` 去掉文件名以 `_test.rs` 结尾的文件，并尝试跳过 `#[cfg(test)]` 块之后的文本计数。注释行被去掉。大文件里括号不平衡时，`cfg(test)` 剥离可能把测试代码留在计数里，或误删生产代码。因此下面是扫描估计，不是编译器诊断。

全库合计约：`.unwrap()` 1093，`.expect(` 6067，`panic!` 104，`unimplemented!` 1，`unreachable!` 35，`unsafe` 100。最高的是 `rustfs`（unwrap 729，expect 3177，unsafe 51）、`crates/scanner`（expect 1028）、`crates/heal`（unwrap 187，expect 872，panic 19）、`crates/ecstore`（expect 607，unsafe 31）。`crates/AGENTS.md` 要求库代码在测试之外不用 `unwrap` / `expect` / panic 控制流。`rustfs` 是二进制 crate，这条库规则不直接覆盖它，但 heal / scanner / ecstore 在 `crates/` 下。没有 miri 或 sanitizer 把这些点变成运行时证明。

## 4. 三个外部仓库

### 4.1 `rustfs-release-validation`

无法阅读。公开 URL 404，组织公开仓库列表中不存在，本仓无引用。不能描述它测什么。本仓里承担“发布前验证”这个名字的是：

- tag 驱动的 `build.yml` 及后续镜像/包/Helm
- 不进 CI 的 `scripts/run_ecstore_validation_suite.sh`
- tag 上会跑但不阻塞 `build.yml` 的 `e2e-upgrade.yml`
- 针对 nightly 包、不阻塞 tag 的功能链

### 4.2 `auto-testing`（私有，正文未读）

本仓把它钉在 `.config/functional-script-revision.txt` 的 `27e9584a2d776db9416d0edfd040d524d48b5d4d`。功能链在共享实验机上对一个 nightly `.deb` 顺序跑十二条。`docs/testing/functional-chain.md` 的顺序是：升级、S3、KMS、分层、存储、heal、池扩容、安全、复制、容错、table、性能。失败的套件不会取消后续套件；`complete-chain` 要求十二条和证据都成功。已知分歧在容错套件里可以记成 unsupported 而不失败（`.github/workflows/rustfs-fault-tolerance-test.yml` 的默认 `strict` 为 false）。

从 workflow 能确定的脚本入口（脚本内容在私有仓库里，本次未读）：

| 本仓 workflow | 调用的脚本 | 本仓注释里写明的范围 |
|---|---|---|
| `rustfs-upgrade-test.yml` | `rustfs-upgrade-test.sh` | 打包升级 |
| `rustfs-s3-compat-test.yml` | `rustfs-s3-compat-test.sh` | S3 兼容 |
| `rustfs-kms-test.yml` | `rustfs-kms-test.sh` | KMS |
| `rustfs-tier-test.yml` | `rustfs-tier-test.sh` | 分层 |
| `rustfs-storage-test.yml` | `rustfs-storage-test.sh` | 存储 |
| `rustfs-heal-test.yml` | `rustfs_heal_test.sh` | 节点离线后的 heal，用 warp 把存活节点写到给定 GiB |
| `rustfs-pool-expand-test.yml` | `rustfs_pool_expand.sh` | 池扩容，可经 nginx 阶段钩子 |
| `rustfs-security-test.yml` | `rustfs-security-test.sh` | 安全；非手动时打开 OIDC live |
| `rustfs-replication-test.yml` | `rustfs-replication-test.sh` | 复制 |
| `rustfs-fault-tolerance-test.yml` | `rustfs-fault-tolerance-test.sh` | 单节点 4 盘、4x1、4x4 EC4/EC8 的盘或节点丢失；读仲裁满足但仍 503 默认为已知分歧 |
| `rustfs-table-test.yml` | `rustfs-table-test.sh` | S3 Tables |
| `rustfs-performance-test.yml` | `rustfs_performance_test.sh` | 性能，作业上限 900 分钟 |
| `rustfs-fault-tolerance-matrix.yml` | 同一容错脚本 | 矩阵，手动 |

触发：功能链由 nightly GNU 的 schedule 完成来驱动；各套件也可 `workflow_dispatch` 或旧的 `repository_dispatch`。不在 PR 必过集合，也不被发布 workflow 依赖。`docs/testing/ci-gates.md` 写明：功能链的 workflow 状态不能代替 scanner/heal 证据登记表里的对象级 oracle，G01–G14 等发布要求仍是 `pending`。

### 4.3 `s3chaos`

公开仓库，针对 Kubernetes 上的 RustFS。两部分：

- `src/fault/`：Chaos Mesh 与 host device-mapper。README 列出 35 个可执行场景：I/O（`io-eio`、multipart 期间 EIO、读错、只读、延迟、`disk-full`、`dm-flakey*`、五类 `dm-drop-writes-after-ack-*`）、网络（单点分区、非对称分区、写仲裁丢失、延迟/丢包/抖动/损坏/重复）、Pod（kill、重启风暴、failure、带版本的热崩溃）、生命周期重启、CPU/内存压力、卷仲裁、`warp-under-chaos`。
- `src/protocol/`：IAM、STS、OIDC、桶策略、canned policy、授权，以及有界的兼容性补充（bucket、object、multipart、copy、versioning、listing）。Mint 由命令在独立集群上跑，仓库自己的 CI 不跑 Mint。

CI（`.github/workflows/ci.yml`）：push / PR 上 `cargo fmt`、clippy、`cargo test`，再静态校验 fault/protocol YAML。README 写明 fault 套件不会被 CI 执行。

`.github/workflows/protocol-live.yml`：只在 `workflow_dispatch` 和 `repository_dispatch` 类型 `rustfs-release-candidate` 时跑活体 smoke、回归、过期、OIDC。README 写“rustfs 仓库会对每个候选发送该事件”。在本次 rustfs 工作区（含 `.github/`）检索 `s3chaos` 与 `rustfs-release-candidate`，结果为空。因此当前公开的 rustfs 树没有这个发送方。不能排除仓库外的自动化。就本仓而言，这条活体门禁没有接上。

仍是 `Planned`、普通 `make fault-list` 会拒绝的资格场景（README）：`fresh-volume-replacement`、admin decommission、admin rebalance、`on-disk-bitrot`、`stale-disk-return-detect`。另有六项没有安全执行器：同 Pod 两卷 EIO、heal 期间网络分区、时钟偏移、负载中轮换凭证、裂脑、元数据分片损坏。`docs/DURABILITY_FAULT_TESTING_TODO.md` 把目标证明标成 PARTIAL：能解析 Pod/PV，还不能证明纠删集身份、数据/校验宽度或同集覆盖。掉电类 `dm-drop-writes-after-ack-*` 与 `warp-powerloss.yaml` 存在，但是人工集群资格，不是发布门禁。bitrot 检查器按 README 接受当前 XL2 1.3 / header 3 / meta 3、格式版本 1、纠删版本 3、SIPMOD+PARITY，遇到未知画像失败关闭；检查器不钉 RustFS 发行 tag。

## 5. 白盒缺口

每条都写已有覆盖和仍然缺的行为。优先级：P0 会在已确认写入之后丢数据、读回错误字节或把错误授权放出去，且现有门禁接不住；P1 是高风险但已有部分证明，或只在非阻塞车道；P2 是加深覆盖、平台或工程卫生。

| ID | 缺口 | 已有证据 | 仍缺什么 | 优先级 |
|---|---|---|---|---|
| G1 | 掉电与崩溃一致性不是发布门禁 | `crates/ecstore/src/disk/local.rs` 的 `crash_consistency` 模块；`docs/operations/durability-modes.md` 写明 `relaxed`（新桶默认）在掉电时可以丢掉已确认版本的 xl.meta；`docs/testing/ci-gates.md` 写明进程内重开不是掉电证明；destructive 套件不进 workflow | 真实掉电（杀进程不 flush、`dm-flakey` / drop-writes-after-ack）没有进 PR 或 tag 门禁。s3chaos 的对应场景要人工集群 | P0 |
| G2 | 父目录 fsync 在 `relaxed` 下关闭，缺少对应的持久性测试车道 | `docs/operations/durability-modes.md` 的写点表：`relaxed` 仍 fdatasync 分片和 multipart part，但不 fsync xl.meta、回滚备份和提交 rename 的父目录。`crates/ecstore/src/disk/local/commit.rs` 注释写明与非 inline 路径相同的掉电窗口 | 没有一条必过测试在 `RUSTFS_DURABILITY_MODE=relaxed` 或新桶默认下注入掉电并检查“旧或新、不能混合、不能复活已删除版本” | P0 |
| G3 | 盘上 bitrot 与陈旧盘返回仍是资格项 | 单元与 fixture：`crates/ecstore/src/erasure/coding/erasure.rs` 的 proptest，`crates/ecstore/tests/legacy_bitrot_read_test.rs`（外部语料可跳过），`crates/ecstore/src/set_disk/ops/heal/shard_integrity_rollout_tests.rs` 的 `bad-bitrot` 案例名。s3chaos README 把 `on-disk-bitrot` 与 `stale-disk-return` 标为 Planned | 没有定期、带发行身份的“改一个分片字节 → heal 重建 → 字节级比对”门禁。legacy bitrot 跳过不等于兼容车道通过 | P0 |
| G4 | Heal 的发布证据仍是 pending | `docs/testing/ci-gates.md`：scanner/heal 登记表里 G01–G14 与 R-E/R-D/R-L 保持 `pending`。已实现的是重启切片：`crates/e2e_test` 的 background-target-restart 与 ec84-target-drive-restart。`crates/heal/src/heal/manager/auto_scan.rs`、`replacement_readiness.rs` 在 2026-08-22 覆盖约 23% 与 25% | 掉电、全版本盘点、MRF 精确处置、混版本回滚、多池/多集、固定预算重启，都还没有可绑定的 oracle。功能链 heal 用 warp 写盘，workflow 成功不能代替这些 oracle | P0 |
| G5 | 复制重同步与元数据复制的历史覆盖低，且重同步故障模型不在 PR 必过集 | 2026-08-22：`crates/ecstore/src/bucket/replication/replication_resyncer.rs` 39.17%，`crates/filemeta/src/replication.rs` 43.07%。e2e 目标矩阵在 `e2e-repl-nightly`。功能链复制套件不挡 PR | 缺：复制进行中对端 5xx/慢/校验和不一致、删除标记与保留的部分失败、站点复制在网络分区后的收敛证明。这些要多集群，不应塞进 PR 单元测试 | P1 |
| G6 | 加密读回与 SSE-C 的默认构建分离 | `minio-interop.yml` 只在 nightly 证明 `rio-v2` 下 SSE-S3/SSE-KMS 的字节级读回；SSE-C 是检测。默认构建不含这条读路径。KES 信封按设计不可读。`crates/crypto` 历史行覆盖 94.89% 但函数覆盖 80.85%，且棘轮是 report-only | 缺：默认构建上 SSE-C 错误密钥/错误 MD5 必须失败且不泄露明文；SSE-KMS 密钥轮换（`docs/architecture/kms-bulk-rekey-contract.md` 所描述的批量重包）在对象仍可读的端到端证明进发布候选，而不是只在 nightly | P1 |
| G7 | 签名与 XML 入口没有 fuzz | `crates/signer/src/request_signature_v4.rs` 有单元测试；`fuzz/` 没有签名或 HTTP 头目标。XML 解析在 `rustfs/src/server/layer.rs`（`quick_xml`）和 admin/复制路径。GHSA 回归在 `docs/testing/security-regressions.md`，其中 `GHSA-m77q` 被测试钉成“仍然用根密钥签 STS”，是已知未修行为 | 缺：对 SigV4 预签名、chunked trailer、未签名 `x-amz-*` 的 fuzz；对 S3 XML（生命周期、策略、复制配置、错误体）的 fuzz。策略 JSON 已有 `policy_ingress` | P1 |
| G8 | `cargo deny` 不挡合并，且 rsa 侧信道被显式忽略 | `deny.toml` 忽略 `RUSTSEC-2023-0071`；`audit.yml` 每日跑但 `ci-gates.md` 标 report-only | 供应链门禁对 PR 是可见而非阻塞。rsa 忽略有注释和复审日期，但是认证相关依赖上的已知侧信道仍在图里 | P1 |
| G9 | 分布式锁与网络分区的证明在重车道 | `crates/lock/src/distributed_lock.rs` 有测试属性。`crates/e2e_test/src/fault_proxy.rs` 覆盖锁平面单向分区。`crates/e2e_test/src/distributed/chaos_test.rs` 有 blackhole。`crates/e2e_test/src/heal_erasure_disk_rebuild_test.rs` 有 iptables blackhole。容错功能链默认把“读仲裁够了仍然 503”记成已知分歧 | 每个 PR 不跑 `e2e-distributed`。s3chaos 的写仲裁分区与非对称分区不在其 CI。裂脑与 heal 期间分区仍是 Planned。锁丢失与写入提交的交错没有掉电级证明 | P1 |
| G10 | Multipart 边界有单元测试，客户端路径覆盖曾很低，大对象故障不在 PR | `.config/ecstore-required-tests.json` 含 multipart 取消。`crates/ecstore/src/set_disk/ops/multipart.rs` 有 `crash_consistency` 模块。s3-tests implemented 名单含一部分 multipart。2026-08-22 的 client multipart 源文件覆盖约 10%，该路径现已不在树中 | 缺一条当前树上、必过的证明：Complete 与 Abort 并发、part 号空洞、超过配额的 part、上传中磁盘 EIO、上传中节点被杀之后 ListParts/已完成对象的字节。s3chaos 有 `io-eio-during-multipart`，但不进 CI | P1 |
| G11 | 版本、删除标记、对象锁、保留的重组在 `e2e-full` | `e2e-full` 注释点名 object_lock。删除标记有迁移门禁子串（`docs/testing/README.md`）。s3chaos protocol 有 versioning 案例，活体不进 PR | 对象锁合规（WORM 在掉电、heal、复制之后仍拒绝删除）没有 P0 级发布证明。保留与 legal hold 的复制在 nightly 目标矩阵，不在每个 PR | P1 |
| G12 | ILM 与分层的集成被 `#[ignore]`，串行作业才跑 | `ci.yml` 的 `test-ilm-integration-serial` 用 `--run-ignored ignored-only`。`crates/lifecycle/src/core.rs` 有 proptest，2026-08-22 行覆盖 97.52%。分层功能链与 `rustfs-tier-test.yml` 不挡 PR | 评估器覆盖高，不代表扫描器真正删/转换对象的故障安全。缺：扫描中断后续跑、分层上传成功但本地元数据提交失败、远程版本 `None`/`""` 不带 versionId（仓库不变量）的回归如果只在 ignored 测试里，就要确认它确实被串行作业选中。本次没有跑 nextest，不能列出被 ignore 的具体函数名 | P1 |
| G13 | 滚动升级只钉一个旧版本，且不挡发布 | `e2e-upgrade.yml` 的 `UPGRADE_SOURCE_VERSION` 是 `1.0.0-rc.5`。功能链另有打包升级脚本，正文未读。xl.meta 当前版本在 `crates/filemeta/src/filemeta.rs` 的 `XL_META_VERSION`（值 3）。filemeta 有真实 MinIO fixture 与 version graph proptest | 缺：N-1 与 N-2 稳定版数据目录、混版本滚动（新写入者 + 旧读取者）、回滚之后旧二进制拒绝它读不懂的格式而不是读出混合字节。s3chaos bitrot 检查器也不钉发行 tag | P1 |
| G14 | S3 兼容有门禁，但 mint 与全量 s3-tests 不挡合并或发布 | PR 跑 556 行 implemented 名单，这是阻塞的。每周全量与 mint 不挡。`unimplemented_tests.txt` 37 行是已知未实现。mint 注释写明还没有 per-suite 基线 | 多 SDK 签名/编码差异只在每周 report-only 的 mint 里出现。发布 tag 不等待每周 s3-tests 或 mint | P1 |
| G15 | 性能回归是合并后才看 | Criterion bench 不是门禁。`performance-ab.yml` nightly warp，默认时长在 workflow 输入里是 12s 量级的热路径，不是容量曲线。功能链性能车道 900 分钟，不挡发布 | 缺：纠删码、xl.meta、multipart 读的 Criterion 结果没有基线比较；容量、延迟分位、heal 期间前台 IOPS 没有发布阻断预算 | P2 |
| G16 | 可观测性路径覆盖偏低，且没有告警回归门禁 | `crates/obs` 2026-08-22 行覆盖 79.72%。`crates/obs/src/telemetry/otel.rs` 44.12%。`rustfs/src/storage/ecfs.rs` 32.04% | 缺：指标名/标签在重构后不消失的契约测试进 PR；日志治理只在改 tracing 时由技能约束，没有测试证明密钥不会进 span | P2 |
| G17 | 磁盘满与 IO 错误注入是散点，不是模型 | `crates/heal/src/heal/mrf_queue.rs` 有需要外部满文件系统的 ENOSPC 探针（环境变量 `RUSTFS_SCANNER_HEAL_W13_ENOSPC_ROOT`）。`crates/ecstore/src/disk/os.rs` 有 Windows `ERROR_DISK_FULL` 映射测试。s3chaos 有 `disk-full` 与 `io-eio`，不进它的 CI | 主仓 PR 不注入 ENOSPC/EIO/只读。满盘探针依赖调用者提供小 tmpfs，CI 没有这条车道 | P1 |
| G18 | 内存与资源泄漏没有 sanitizer 或分配预算 | 没有 ASan/TSan/MSan/miri/dhat 作业。代码里有“不要泄漏”的断言（例如旧数据目录泄漏计数），那是逻辑断言，不是分配器证明。connect 内存画像 workflow 是手动 | 长时间 List/multipart/复制的 RSS 与 fd 上限没有门禁 | P2 |
| G19 | 配置解析有测试，故障组合没有 | `crates/config` 2026-08-22 行覆盖 98.70%（694 可执行行）。耐久性模式解析在 `crates/ecstore/src/disk/local.rs` | 缺的是非法组合与热重启：例如 `RUSTFS_DURABILITY_MODE` 非法值回退、新桶默认 `relaxed` 与进程默认 `strict` 同时存在时升级前后的模式。解析器本身不是空白 | P2 |
| G20 | 多平台行为几乎只在 Linux 上测 | Windows 只有路径触发的 rename 作业，report-only。macOS 只打包。io_uring 作业是 Linux 真实后端 | 原子 rename、保留删除、文件锁在 Windows/macOS 上的差异没有测试门禁 | P2 |
| G21 | 覆盖率信号已经断了 | 2026-08-24 之后没有成功的 coverage 运行；棘轮基线停在 2026-07-15；作业不失败 | 不能用覆盖率发现回退。生成 protobuf（`node_service.rs` 10.67%）会拉低 `crates/protos`，应在统计里分开，避免用它解释业务洞 | P1 |
| G22 | 测试文档与门禁合同不一致 | `docs/testing/README.md`、`docs/testing/security-regressions.md` 仍写 PR e2e / s3-tests report-only。`scripts/ci_gate.py` 要求它们 success | 读者会放过错过的阻塞车道。这是测试体系自身的缺陷 | P2 |

纠删码编解码本身不是空白：`crates/ecstore/src/erasure/coding/erasure.rs` 有 proptest，ecstore 有 6007 个 src 测试属性，2026-08-22 行覆盖 87.74%，并且 PR 必过集合包含写仲裁与损坏 part 数组。缺口在故障注入和发布证据，不在“没有编解码测试”。

IAM / 策略同样不是空白：`crates/policy/tests/policy_eval_proptest.rs`、`crates/iam` 348 个 src 属性加 GHSA 回归。2026-08-22 iam 行覆盖 78.19%、policy 88.21%。缺口是签名/XML fuzz、deny 不挡合并，以及 `GHSA-m77q` 这种被测试固定下来的未修行为。

## 6. 按目标的补测建议

### 6.1 稳定性（数据在故障后仍是旧或新，且可读）

| 优先级 | 测试 | 放在 |
|---|---|---|
| P0 | `relaxed` 与 `strict` 两档的掉电：杀进程且不 flush，对象必须全旧或全新；删除标记不能复活 | 主仓先做可重复的用户态故障（已有 `crash_consistency` 的进程内模型）；真实块设备掉电放 s3chaos，发布候选必须跑 |
| P0 | 单盘改一个数据分片、改 xl.meta 校验、陈旧盘重新接入：heal 后字节与版本集合与写入时一致 | s3chaos 把 Planned 的 `on-disk-bitrot` 与 `stale-disk-return` 收成可重复跑的套件；主仓保留算法级 proptest |
| P1 | 写仲裁边界上的非对称网络分区、heal 期间分区 | s3chaos（已有可执行的写仲裁分区；heal 期间分区仍 Planned） |
| P1 | 磁盘满与 EIO：PUT、CompleteMultipart、checkpoint 发布必须失败可见，不能留下半新半旧的可读对象 | 主仓用现有 ENOSPC 探针接进一条手动/定时作业；多盘 EIO 放 s3chaos |
| P2 | 长时间运行的 fd/RSS 上限 | 功能链或 s3chaos 的 warp 车道，设预算 |

### 6.2 性能

| 优先级 | 测试 | 放在 |
|---|---|---|
| P1 | 把现有 Criterion（纠删、xl.meta、multipart 读、hash）接到与上次发布二进制的 A/B 预算，而不是只在 nightly 热路径 warp | 主仓保存 bench 与预算脚本；比较结果放发布验证车道 |
| P2 | heal / 扫描期间前台 p99 与 IOPS | s3chaos 的 `warp-under-chaos` 或功能链性能套件。不进每个 PR |
| P2 | 覆盖率统计把生成的 protobuf 分开，避免 `crates/protos` 的 41% 掩盖业务 crate | 主仓 `scripts/coverage_per_crate.py` 的消费方式 |

### 6.3 升级

| 优先级 | 测试 | 放在 |
|---|---|---|
| P0 | 发布候选对上一个稳定版与上上个稳定版的数据目录：直接升级、滚动（新写旧读）、回滚。旧二进制遇到写不懂的 xl.meta 必须拒绝 | 主仓 `e2e-upgrade.yml` 扩大版本矩阵并让 tag 发布等待它；打包升级的长场景留在 auto-testing |
| P1 | `XL_META_VERSION` 与 MinIO fixture 的向后读：未知更高 meta 版本失败，而不是部分解码 | 主仓 `crates/filemeta`，随 PR |
| P1 | 新桶默认 `relaxed` 在升级前后保持，进程默认仍是 `strict` | 主仓配置/桶元数据测试，随 PR |

### 6.4 兼容性

| 优先级 | 测试 | 放在 |
|---|---|---|
| P1 | mint 按套件收成基线：已绿的套件回退即失败；未绿的保持清单 | 主仓 `.github/workflows/mint.yml`。它已经每周跑 |
| P1 | 每周 s3-tests 全量里新出现的失败必须分类进 implemented / excluded / unimplemented，不允许无分类 | 主仓已有每周作业，缺的是发布前看一眼的硬门禁 |
| P2 | 把 s3chaos protocol-live 真正接到候选镜像。本仓现在没有发送 `rustfs-release-candidate` 的 workflow | s3chaos 执行，主仓 `build.yml` 或发布技能负责 dispatch |
| P2 | `rio-v2` 的 MinIO SSE 读回在候选上跑，而不只是 nightly | 主仓 `minio-interop.yml` 增加 tag 触发 |

### 6.5 安全性

| 优先级 | 测试 | 放在 |
|---|---|---|
| P0 | `cargo deny` 的 advisories 对 PR 失败关闭。rsa 忽略要么有替换依赖，要么在发布说明里保持显式 | 主仓 `audit.yml` 进 `ci_gate.py`，或至少 advisories 子集进 quick checks |
| P1 | SigV4、预签名、chunked trailer、S3 XML 的 fuzz，种子来自 GHSA 回归里已有的请求 | 主仓 `fuzz/`，随 `fuzz.yml`。目标稳定后再考虑把 smoke 失败改成阻塞 |
| P1 | SSE-C 错误密钥不返回明文；批量 rekey 之后旧对象仍可解密 | 主仓 `crates/kms` 与 `crates/crypto` 的单元/集成；多节点放功能链 KMS 套件 |
| P2 | 策略评估继续用 proptest，补上条件键与 `ForAllValues` 的生成器，而不是只靠 GHSA 定点 | 主仓 `crates/policy` |
| P2 | 日志与 trace 的密钥扫描测试 | 主仓 `crates/obs` 或现有日志守卫脚本 |

### 6.6 可观测性

| 优先级 | 测试 | 放在 |
|---|---|---|
| P1 | 管理健康与 Prometheus 指标在盘丢失、锁仲裁丢失时的标签契约。容错套件已经会抓健康端点，但是默认不把“读仲裁足够却 503”当成失败 | auto-testing 的容错套件把契约写死；指标名契约的单元测试放主仓 `crates/obs` |
| P2 | `crates/obs/src/telemetry/otel.rs` 的导出失败必须可观测且不阻塞读写 | 主仓单元测试 |

### 6.7 数据持久性与正确性

这是稳定性的子集，单列是因为发布决策看的是字节而不是“服务还活着”。

| 优先级 | 测试 | 放在 |
|---|---|---|
| P0 | 把 scanner/heal 证据登记表里仍为 pending 的门禁接到真实 oracle，再允许发布声称 heal 完成 | 主仓 `scripts/check_test_wiring.py` 已有检查器；产生 oracle 的长跑放 e2e-distributed 或 s3chaos，不要用功能链的退出码代替 |
| P1 | 复制：提交的版本、删除标记、保留在对端失败后要么重试成功，要么源端状态可解释 | 主仓 nightly `e2e-repl-nightly` 升为发布候选必过；多站点放 auto-testing |
| P1 | 对象锁对象在 heal 与复制之后仍不能被普通 Delete 去掉 | 主仓 e2e，发布候选必过 |

### 6.8 运维与容灾

| 优先级 | 测试 | 放在 |
|---|---|---|
| P1 | 池扩容、decommission、rebalance 在有对象负载时的字节守恒 | 主仓已有迁移门禁子串（`data_movement`、`rebalance`、`decommission`）。破坏性的多节点放 auto-testing 的 pool expand，发布候选要看证据而不只看脚本退出码 |
| P1 | 备份恢复：停全集群、换盘、用 heal 重建，比对写入清单 | s3chaos 的 fresh-volume 资格，从 Planned 收成可调度套件 |
| P2 | 时钟偏移、负载中轮换凭证 | s3chaos，README 已列为没有执行器的 Planned。先补执行器，再谈门禁 |

### 6.9 多平台

| 优先级 | 测试 | 放在 |
|---|---|---|
| P2 | Windows rename 作业从 report-only 升为存储 PR 的必过，并加上掉电语义在 NTFS 上的差异说明 | 主仓 `windows-filesystem.yml` |
| P2 | macOS 至少跑 `rustfs-filemeta` 与 `disk::local` 的非 io_uring 子集 | 主仓新的定时作业，不进每个 Linux PR |

## 7. 建议落在哪个仓库

以现在的仓库边界为准：

- 主仓 `rustfs/rustfs`：随 PR 的逻辑、解析器、属性测试、fuzz smoke、单进程故障模型、s3-tests 白名单、升级 e2e、覆盖率与 `cargo deny`。判定标准是不需要多机、不需要跑几小时、失败应该挡住合并。
- `auto-testing`：已经在实验机上对 nightly `.deb` 做的长套件（升级、S3、KMS、分层、存储、heal、扩容、安全、复制、容错、table、性能）。继续放破坏性、多节点、会清盘的场景。它不该替代主仓的 oracle。
- `s3chaos`：Kubernetes 上的真实故障注入和协议活体。盘上 bitrot、掉电、网络分区、磁盘满属于这里。CI 应继续只做静态校验；活体必须由候选发布显式触发。
- `rustfs-release-validation`：今天不存在（公开 404）。不要往一个无法打开的仓库加测试。发布门禁应落在主仓已有的 tag workflow 上：让 `build.yml` 在发布资产之前等待一组指定的候选作业（升级矩阵、deny、必要的 s3-tests/mint 基线、s3chaos protocol-live 与一条 durability 套件）。若以后要把“候选编排”拆出主仓，再新建仓库，并让 `build.yml` 调它。在那个仓库出现并被本仓引用之前，编排逻辑放在主仓。

| 建议 | 仓库 | 理由 |
|---|---|---|
| 纠删/元数据/锁/策略的 proptest 与回归 | 主仓 | 贴近代码，PR 的 nextest 已经会跑 |
| SigV4 / XML / xl.meta fuzz | 主仓 `fuzz/` | 已有隔离 workspace 和 `fuzz.yml` |
| `cargo deny` advisories 改为 PR 失败 | 主仓 | 图在主仓，`deny.toml` 已在 |
| relaxed/strict 的进程内崩溃模型 | 主仓 | `crash_consistency` 已经在 `crates/ecstore/src/disk/local.rs` |
| 真实掉电、bitrot、陈旧盘、磁盘满、网络分区 | s3chaos | README 的执行模型就是专用集群；主仓 CI 跑不了 Chaos Mesh |
| 协议活体接到每个候选 | s3chaos 执行，主仓负责 dispatch | `protocol-live.yml` 已接受 `rustfs-release-candidate`，本仓尚未发送 |
| 多节点清盘的升级、扩容、复制、KMS、容错 | auto-testing | 功能链已经占着实验机和私有脚本；保持脚本与 `.config/functional-script-revision.txt` 的钉扎 |
| 候选是否允许打 tag | 主仓 `build.yml` 增加等待 | 发布资产今天只看构建链。外部仓库的绿不能自动挡住 tag，除非主仓去等它们 |
| 不存在的 release-validation 仓库 | 不使用 | 无法阅读，本仓也没有集成 |

## 8. 本次没有做的事

- 没有修改产品代码。
- 没有运行 `cargo test`、`cargo llvm-cov`、tarpaulin、fuzz 或任何 e2e。原因是本地 `rustc 1.83.0` 无法编译 edition 2024 / `rust-version` 1.98.1 的 workspace，且全库插桩测试在 CI 上的预算是 240 分钟。
- 没有把 84.07% 当作当前 HEAD 的覆盖率。那是 2026-08-22 `09de44df0883` 的测量。
- 没有阅读 `auto-testing` 的脚本正文，也没有找到 `rustfs-release-validation`。
- 测试属性次数不是 nextest 用例数。`#[ignore]`、feature 门控和宏展开都会让两边不一致。
