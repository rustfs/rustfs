# Issue 797 multi-node internode performance execution plan

## 背景

GitHub backlog issue #797 提出的方向是优化多机多盘场景下的网络、gRPC、HTTP data-plane、分片调度、批处理与元数据路径。经过 5 个专家方向复核后，结论是：方向成立，但原始建议需要按当前 RustFS 架构重新拆分。

## 远程 main 复核记录

2026-07-02 已从 `origin/main` 切出新分支 `houseme/issue-797-internode-plan-refresh`，基于 `origin/main@70e1d79df` 重新核对关键路径。

复核结论：当前 9 个子任务的拆分、优先级和风险边界仍然成立，无需推翻或重排。新增记录见 `main-refresh-2026-07-02-zh.md`。

确认仍成立的关键事实：

- gRPC channel cache、keepalive、connect timeout、RPC timeout 已存在，不应新增平行 `GrpcChannelPool`。
- `ReadAt` 和 `WriteStream` 在 `NodeService` 中仍是 unimplemented，远端大流仍应优先优化 HTTP data-plane。
- `HttpWriter` shutdown 仍存在吞掉 background task `Ok(Err(_))` 的风险。
- remote lock 的 `release`、`release_locks_batch`、`refresh`、`force_release`、`check_status` 仍未统一走 `execute_rpc`。
- `ReadMultiple` 仍存在 JSON/msgpack 双转换和 decode 失败静默丢弃风险。
- distributed erasure 下 GET metadata cache 仍保守 bypass，这是正确边界。
- `SameNode` 仍缺生产级来源，当前 `shard_read_cost_for_disk` 只区分 `Local/Remote/Unknown`。

当前 RustFS 已经是：

- gRPC control-plane：metadata、lock、管理类 RPC。
- HTTP data-plane：远端磁盘大流读写和 walk-dir。
- EC GET pipeline：已有 `ShardReadCost`、hedging、stage metrics、metadata cache、read repair 等基础能力。

因此不建议直接实现新的 `GrpcChannelPool`，也不建议把 `ReadAt` stream 作为首要优化点。更高收益、更安全的路线是先修复 HTTP data-plane 与 lock/read-multiple 的可靠性问题，再做可观测与保守 tuning，最后按灰度开关推进 shard locality、batch metadata RPC 与 adaptive concurrency。

## 子任务顺序

1. `task-01-http-writer-error-semantics-zh.md`
   - 修复 HTTP writer shutdown/flush 错误传播。
2. `task-02-remote-lock-rpc-timeout-eviction-zh.md`
   - 统一 remote lock RPC timeout、日志与 connection eviction。
3. `task-03-read-multiple-serialization-decoding-zh.md`
   - 优化 `ReadMultiple` 序列化与 decode 错误处理。
4. `task-04-internode-http-client-tuning-zh.md`
   - 增加 HTTP data-plane client 的保守可配置 tuning。
5. `task-05-internode-observability-baseline-zh.md`
   - 建立 baseline、指标与压测证据闭环。
6. `task-06-shard-locality-observe-topology-zh.md`
   - 补齐 `SameNode` 拓扑来源和 observe-only metrics。
7. `task-07-shard-locality-opt-in-scheduling-zh.md`
   - 灰度启用 locality scheduling 与更安全的 hedging。
8. `task-08-batch-metadata-rpc-capability-gate-zh.md`
   - 在 capability gate 后新增 batch metadata/read-version RPC。
9. `task-09-adaptive-batch-processor-observe-only-zh.md`
   - 仅以 observe-only 方式设计 adaptive batch processor。

## 当前实现状态

截至 2026-07-03，9 个子任务均已按“先 correctness、再 observe、最后 opt-in”的顺序完成首轮实现，并保持高风险性能行为默认关闭。完整状态、验证证据和剩余上线门禁见 `implementation-status-2026-07-03-zh.md`。

| 子任务 | 状态 | 关键提交 |
| --- | --- | --- |
| Task 01 HTTP writer error semantics | 已实现 | `ac53eb4ad` |
| Task 02 remote lock RPC deadline | 已实现 | `3fb750f33` |
| Task 03 ReadMultiple decode hardening | 已实现 | `edd8c2c7c` |
| Task 04 internode HTTP tuning profiles | 已实现，默认 legacy | `ed956f77a` |
| Task 05 observability baseline signals | 已实现 | `394572d7f` |
| Task 06 shard locality topology observe | 已实现，observe-only | `3373f9463` |
| Task 07 shard locality scheduling gate | 已实现，默认 off | `ecfee401f` |
| Task 08 batch read-version RPC gate | 已实现，默认 off | `b48daeee5` |
| Task 09 batch processor adaptive observe | 已实现，默认 off | `5ee4f3ebe`, `8c09e405f` |

## 总体实施原则

- 所有行为变更默认关闭。
- correctness 修复优先于性能 tuning。
- 只复用现有架构，不新增平行配置体系。
- 不弱化 quorum、bitrot、versioning、delete marker、SSE、read repair 语义。
- 每个 PR 都必须有 focused tests；最终 PR-ready 才跑完整 `make pre-pr`。
- 性能结论必须覆盖 p50/p95/p99、error rate、吞吐、internode bytes、CPU/内存，不能只看 median。

## 建议灰度开关

```text
RUSTFS_INTERNODE_HTTP_TUNING_PROFILE=legacy|balanced|throughput
RUSTFS_INTERNODE_HTTP_PROXY=off|system
RUSTFS_SHARD_LOCALITY_SCHEDULING=off|observe|on
RUSTFS_METADATA_BATCH_READ=off|auto|on
RUSTFS_BATCH_PROCESSOR_ADAPTIVE=off|observe|on
```

## 总体验收门槛

- `cargo fmt --all --check`
- 相关 crate focused tests
- `make pre-commit`
- PR-ready 前执行 `make pre-pr`
- 多机多盘性能 A/B 至少 10 轮，固定数据集、固定并发、固定环境变量。

## 回滚标准

任一行为开关导致以下现象，应立即回滚到 `legacy` 或 `off`：

- read quorum failure 或 error rate 上升。
- GET/PUT p99 比基线恶化超过 10%。
- 503、timeout、connection reset 明显增加。
- mixed-version fallback 异常。
- internode bytes 或 CPU 使用异常放大。
