# Issue 797 implementation status on 2026-07-03

## 结论

Issue #797 的首轮工程实现已经覆盖 9 个子任务：HTTP data-plane 错误语义、remote lock RPC deadline、`ReadMultiple` decode hardening、HTTP client tuning profile、internode baseline metrics、shard locality observe/topology、shard locality opt-in scheduling、batch read-version RPC capability gate、batch processor adaptive observation。

当前分支的实现策略仍保持保守：

- correctness 修复已直接生效。
- 性能 tuning、调度和批量 RPC 默认关闭或保持 observe-only。
- 未新增平行 gRPC channel pool。
- 未将大流读写迁移到 gRPC stream。
- 未弱化 quorum、bitrot、versioning、delete marker、SSE、read repair 语义。

因此，本轮结果适合作为 PR 的第一阶段交付：先合入可靠性与可观测性基础，再通过灰度开关和多机多盘 A/B 数据决定是否启用性能路径。

## 已完成实现

| 任务 | 代码状态 | 行为默认值 | 关键提交 | 主要验证 |
| --- | --- | --- | --- | --- |
| Task 01 HTTP writer error semantics | `HttpWriter` shutdown 不再吞掉 background task `Ok(Err(_))` | correctness fix always on | `ac53eb4ad` | `cargo test -p rustfs-rio http_writer --lib` |
| Task 02 remote lock RPC deadline | remote lock release/refresh/status 等路径统一 deadline、eviction 和日志 | correctness fix always on | `3fb750f33` | `cargo test -p rustfs-lock remote_lock --lib`; focused ecstore lock RPC checks |
| Task 03 ReadMultiple decode hardening | corrupt payload 不再被静默丢弃，decode 错误可见 | correctness fix always on | `edd8c2c7c` | `cargo test -p rustfs read_multiple --lib`; `cargo test -p rustfs-ecstore read_multiple --lib` |
| Task 04 internode HTTP tuning profiles | data-plane HTTP profile 和 proxy gate 已接入 | `legacy` / proxy off | `ed956f77a` | `cargo test -p rustfs-rio http_runtime --lib`; focused profile parse tests |
| Task 05 baseline signals | internode HTTP/gRPC/GET stage baseline 指标已补齐 | observation only | `394572d7f` | `cargo test -p rustfs-io-metrics internode --lib`; focused metrics tests |
| Task 06 shard locality topology observe | `SameNode` 来源与 observe metrics 已接入 | observe-only / no scheduling change | `3373f9463` | `cargo test -p rustfs-ecstore shard_locality --lib` |
| Task 07 shard locality scheduling gate | locality scheduling 可 opt-in，保留 hedging 与 fallback | `off` | `ecfee401f` | `cargo test -p rustfs-ecstore shard_locality --lib` |
| Task 08 batch read-version RPC | 新增 `BatchReadVersion` RPC、payload decode 与 fallback；`auto` 才对旧节点 fallback，`on` 对 unsupported 显式失败 | `off` | `b48daeee5`, current follow-up | `cargo test -p rustfs-ecstore batch_metadata_rpc_mode --lib`; `cargo test -p rustfs-ecstore batch_read_version --lib`; `cargo test -p rustfs --lib batch_read_version` |
| Task 09 adaptive batch processor observe | processor latency/success/queue observation 与 suggestion 已接入 | `off` | `5ee4f3ebe`, `8c09e405f` | `cargo test -p rustfs-ecstore batch_processor --lib` |

## 当前灰度开关

```text
RUSTFS_INTERNODE_HTTP_TUNING_PROFILE=legacy|balanced|throughput
RUSTFS_INTERNODE_HTTP_PROXY=off|system
RUSTFS_SHARD_LOCALITY_SCHEDULING=off|observe|on
RUSTFS_METADATA_BATCH_READ=off|auto|on
RUSTFS_BATCH_PROCESSOR_ADAPTIVE=off|observe|on
```

推荐上线顺序：

1. 保持所有性能开关默认关闭，先验证 correctness 修复和 baseline metrics。
2. 在测试集群启用 `RUSTFS_INTERNODE_HTTP_TUNING_PROFILE=balanced`，只比较 HTTP data-plane。
3. 启用 `RUSTFS_SHARD_LOCALITY_SCHEDULING=observe`，确认 topology 标签稳定且不会改变 reader 选择。
4. 在混合版本测试集群启用 `RUSTFS_METADATA_BATCH_READ=auto`，验证 unsupported fallback。
5. 仅在 metrics 稳定后启用 `RUSTFS_SHARD_LOCALITY_SCHEDULING=on`。
6. `RUSTFS_BATCH_PROCESSOR_ADAPTIVE=observe` 先只收集建议，不作为自动扩缩容依据。

## 仍需完成的上线门禁

本轮 focused tests 已覆盖每个切片的关键行为，但在 PR-ready 前仍需补齐以下端到端证据：

1. 运行完整格式和本地门禁：

```bash
cargo fmt --all
cargo fmt --all --check
make pre-commit
```

2. PR-ready 前运行完整门禁：

```bash
make pre-pr
```

3. 多机多盘 A/B 基准：

```bash
scripts/run_object_batch_bench_enhanced.sh \
  --tool warp \
  --warp-mode mixed \
  --sizes 4KiB,64KiB,1MiB,10MiB,64MiB \
  --concurrencies 32,64,128 \
  --duration 60s \
  --rounds 10 \
  --out-dir target/bench/issue-797-final
```

4. 混合版本兼容验证：

- 新节点对旧节点：`RUSTFS_METADATA_BATCH_READ=auto` 必须 fallback 到 unary `read_version`。
- 新节点对新节点：batch path 必须返回与 unary path 一致的 `FileInfo`、version、delete marker 和错误语义。
- `RUSTFS_METADATA_BATCH_READ=on` 遇到 unsupported 节点时必须显式失败，不能静默降级。该语义已通过 mode-level regression test 固化，仍需集群级 mixed-version smoke 验证。

5. 指标核对：

- HTTP request duration、HTTP version、data-plane bytes、write shutdown errors。
- gRPC method duration、timeout、eviction。
- GET stage latency、reader setup、local/remote shard distribution。
- batch processor success/error/latency、queue saturation suggestion。

## 风险与回滚

必须保留以下回滚策略：

- HTTP tuning 异常：回滚到 `RUSTFS_INTERNODE_HTTP_TUNING_PROFILE=legacy`。
- shard locality 异常：回滚到 `RUSTFS_SHARD_LOCALITY_SCHEDULING=off`。
- batch metadata RPC 异常：回滚到 `RUSTFS_METADATA_BATCH_READ=off`。
- adaptive processor 指标异常：回滚到 `RUSTFS_BATCH_PROCESSOR_ADAPTIVE=off`。

触发回滚的信号：

- read quorum failure 或 object error rate 上升。
- GET/PUT p99 比基线恶化超过 10%。
- 503、timeout、connection reset 明显增加。
- mixed-version fallback 异常。
- internode bytes、CPU 或 RSS 异常放大。

## 下一步建议

下一步不建议继续扩大功能面，建议进入 PR 收口：

1. 执行 `make pre-commit`，修复本地门禁问题。
2. 执行混合版本 batch RPC 验证。
3. 执行 4 节点多盘 A/B benchmark。
4. 将 benchmark 输出、开关状态和 rollback plan 写入 PR 描述。
5. PR-ready 前执行 `make pre-pr`。
