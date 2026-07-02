# Task 06: shard locality topology observe-only

## 目标

补齐 `ShardReadCost::SameNode` 的真实生产来源，并在 observe-only 模式下记录 locality 潜在收益，不改变实际读取调度。该任务为后续 opt-in shard scheduling 做准备。

## 代码落点

- `crates/ecstore/src/set_disk/shard_source.rs`
  - `ShardReadCost`
  - `StripeReadState`
- `crates/ecstore/src/set_disk/read.rs`
  - `shard_read_cost_for_disk`
  - reader setup fanout
  - get object pipeline metrics
- `crates/ecstore/src/erasure/coding/decode.rs`
  - `shard_read_launch_order`
  - `record_scheduled_read_cost`

## 当前问题

当前 enum 包含 `SameNode`，decode 排序也能处理，但生产路径基本只返回 `Local/Remote/Unknown`。因此“同节点优先”目前不是完整能力。

此外，reader setup 默认可能先对所有 shard 建 reader task。即使 decode 阶段 local-first，远端 reader/open 可能已经发起，削弱了 locality 优化收益。

## 实施步骤

1. 定义拓扑判断规则：
   - `Local`：本进程本地磁盘。
   - `SameNode`：同节点但非本地路径，或经过 endpoint/host_name 能证明在同一节点。
   - `Remote`：明确远端节点。
   - `Unknown`：无法判断或 disk 缺失。
2. 修改 `shard_read_cost_for_disk`，保守生成 `SameNode`。
3. observe-only 记录：
   - 每次 GET 的 cost 分布。
   - 当前实际 schedule 中 remote count。
   - 如果 local/same-node-first，理论可避免 remote 数。
   - quorum reached latency。
4. 不改变实际调度。
5. 增加 metrics label 前确认 cardinality，不把 object key、path 放进 label。

## 测试方案

单测覆盖：

- Local disk -> `Local`。
- 同节点 endpoint -> `SameNode`。
- 远端 endpoint -> `Remote`。
- None/missing -> `Unknown`。
- observe-only 不改变原始 launch order。

建议命令：

```bash
cargo test -p rustfs-ecstore shard_read_cost --lib
cargo test -p rustfs-ecstore shard_locality --lib
cargo test -p rustfs-io-metrics get_object_shard --lib
```

## 性能观测

无需启用调度，只观察：

- local/same-node 可满足 quorum 的比例。
- remote shard 实际成功数量。
- remote avoided potential。
- fallback 可能性。
- p95/p99 与 remote count 的相关性。

## 验收标准

- `SameNode` 有生产来源。
- observe-only 完全不改变 GET 结果和请求 fanout。
- metrics 能证明后续 locality scheduling 是否值得做。
- 不引入高 cardinality label。
