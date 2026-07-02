# Task 07: opt-in shard locality scheduling

## 目标

在 observe-only 证明收益后，通过灰度开关启用 shard locality scheduling，把 reader setup 和 decode launch order 统一改成拓扑感知，减少远端 shard fanout 和尾延迟，同时不弱化 quorum、bitrot、range、versioning、SSE、read repair 语义。

## 前置条件

必须完成：

- Task 05 observability baseline。
- Task 06 topology observe-only。
- 至少一组多机多盘数据证明 local/same-node quorum 有收益空间。

## 代码落点

- `crates/ecstore/src/set_disk/read.rs`
  - reader setup mode
  - `BitrotReaderSetupMode`
  - pending/drop/fallback 逻辑
- `crates/ecstore/src/erasure/coding/decode.rs`
  - `ReaderLaunchIter`
  - `shard_read_launch_order`
  - `shard_read_hedge_delay`
  - retire abandoned readers
- `crates/ecstore/src/set_disk/shard_source.rs`
  - cost propagation

## 实施步骤

1. 新增灰度开关：
   - `RUSTFS_SHARD_LOCALITY_SCHEDULING=off|observe|on`
   - 默认 `off`。
2. 调整排序：
   - 推荐顺序：`Local < SameNode < Remote < Unknown`。
   - `Unknown` 不应优先于明确可读的 remote。
3. reader setup 前移 locality：
   - 在 `on` 时不要默认 `AllShards`。
   - 先按 cost 建满足 read quorum 的最小集合。
   - 缺失、慢、corrupt 时再补 remote/parity。
4. hedging 调整：
   - 增加 `quorum+1` 或 `quorum+2` 上限。
   - 按 cost 分层 hedge。
   - 固定 100ms 可以保留为默认，但允许后续基于 metrics 调整。
5. read repair 语义保护：
   - 未尝试的 deferred shard 不得误判为 corrupt/missing。
   - 只有真实读失败或 decode recoverable error 才触发 repair。
6. 失败自动 fallback：
   - local/same-node 不足 quorum 时回到 legacy fanout。
   - metrics 标记 fallback reason。

## 测试方案

单测：

- local quorum 足够时不启动 remote。
- local 缺失时补 remote。
- corrupt local shard 触发 fallback 且结果正确。
- range GET 输出不变。
- missing deferred shard 不触发 read repair。

建议命令：

```bash
cargo test -p rustfs-ecstore shard_locality --lib
cargo test -p rustfs-ecstore codec_streaming --lib
cargo test -p rustfs-ecstore read_repair --lib
```

## 性能验证

矩阵：

- off vs observe vs on。
- EC 4+2、8+4。
- 4KiB、64KiB、1MiB、64MiB+。
- full GET、range GET。
- 正常、慢远端、远端节点重启、local shard missing。

指标：

- GET p50/p95/p99。
- remote shard read count。
- remote avoided。
- fallback_to_remote。
- read repair count。
- checksum/body hash 一致。

## 验收标准

- 默认关闭。
- opt-in 后 GET 正确性不变。
- error rate 不上升。
- 多机多盘场景 remote read count 明显下降。
- p95/p99 有稳定收益或无回退。
