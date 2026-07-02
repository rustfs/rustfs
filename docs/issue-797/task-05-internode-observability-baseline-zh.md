# Task 05: internode observability and baseline evidence

## 目标

在继续调参或调度前，建立可复现的 baseline 与观测闭环，能区分瓶颈来自 gRPC control-plane、HTTP data-plane、remote lock、metadata fanout、reader setup、shard scheduling 还是本地磁盘/EC decode。

## 代码与脚本落点

- `crates/rio/src/http_runtime_sources.rs`
- `crates/rio/src/http_reader.rs`
- `crates/ecstore/src/cluster/rpc/runtime_sources.rs`
- `crates/ecstore/src/set_disk/read.rs`
- `crates/ecstore/src/erasure/coding/decode.rs`
- `scripts/run_object_batch_bench_enhanced.sh`
- `scripts/run_four_node_cluster_failover_bench.sh`
- `scripts/run_get_metrics_gate_smoke.sh`

## 需要补齐的指标

HTTP data-plane:

- client request duration。
- HTTP version。
- request count by operation。
- sent/received bytes。
- classified error。
- stall timeout count。
- write shutdown error count。
- retry effective/success。

gRPC control-plane:

- channel cache hit/miss。
- dial latency。
- dial failure。
- eviction count。
- timeout count。
- method-level request duration。

GET/EC:

- metadata fanout duration。
- quorum reached latency。
- reader setup scheduled/attempted/ready/deferred。
- shard read cost success distribution。
- remote avoided。
- fallback_to_remote。
- read repair count by reason。

## 实施步骤

1. 先盘点已有指标，避免重复新增。
2. 对缺口增加 metrics wrapper，优先复用 `rustfs_io_metrics` 已有风格。
3. 给 HTTP writer 错误、stall timeout、HTTP version 增加可查询标签。
4. 给 gRPC connection map 增加 cache hit/miss 与 eviction 指标。
5. 增加一份 baseline runbook：
   - 环境变量。
   - 集群拓扑。
   - warp 命令。
   - metrics capture。
   - 结果 CSV 位置。
6. 该任务不改变行为，只增加观测和文档。

## 验证命令

```bash
cargo test -p rustfs-io-metrics internode -- --nocapture
cargo test -p rustfs-rio metrics -- --nocapture
cargo test -p rustfs-ecstore get_object_stage_metrics -- --nocapture
```

脚本验证：

```bash
bash -n scripts/run_object_batch_bench_enhanced.sh
bash -n scripts/run_four_node_cluster_failover_bench.sh
bash -n scripts/run_get_metrics_gate_smoke.sh
```

## 基准矩阵

- 集群：4 节点、多盘、EC 4+2 和 8+4。
- workload：GET、PUT、mixed。
- object size：4KiB、64KiB、1MiB、10MiB、64MiB、1GiB。
- concurrency：32、64、128。
- rounds：至少 10 轮。

## 验收标准

- 每个后续 tuning PR 都能引用 baseline。
- 能从 metrics 判断瓶颈层级。
- p50/p95/p99、error rate、CPU、RSS、internode bytes 都有记录。
- 不把 readiness、warp host 配置、client 瓶颈误判为服务端优化收益。
