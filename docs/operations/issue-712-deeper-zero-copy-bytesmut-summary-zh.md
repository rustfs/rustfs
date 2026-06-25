# Issue #712 deeper zero-copy `BytesMut` ingest 实验小结

## 1. 目的

本文记录 `houseme/issue-712-deeper-zero-copy` 分支上的第一版 deeper zero-copy 实验结果。

当前实验目标不是直接替换主线默认行为，而是验证：

1. `Erasure::encode` 的 block ingest 如果改成更 `BytesMut` / owned-buffer aware
2. 是否能让当前 `zero_copy_eager` ordinary PUT 路径的收益继续往下穿透

## 2. 当前实验实现

本轮实验改动只集中在两处：

1. `crates/ecstore/src/erasure_coding/erasure.rs`
2. `crates/ecstore/src/erasure_coding/encode.rs`

核心变化：

1. 增加 `encode_data_bytes_mut(...)`
2. 在 `Erasure::encode` 里增加一个 env-gated 的 `BytesMut` ingest 分支
3. 默认行为不变，只有显式设置：

```bash
RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST=true
```

时才会启用实验路径。

## 3. Focused 验证

本轮已完成并通过：

1. `cargo fmt --all --check`
2. `cargo test -p rustfs-ecstore encode_data_bytes_mut_matches_borrowed_path -- --nocapture`
3. `cargo test -p rustfs-ecstore encode_single_block_non_inline_payload_writes_all_shards -- --nocapture`
4. `cargo build -p rustfs --bins`

## 4. 小面 ordinary PUT 压测

验证面固定：

1. `16MiB / c16 / 60s`
2. `32MiB / c16 / 60s`

共跑 3 轮：

1. `issue712-deeper-zero-copy-bytesmut-v1`
2. `issue712-deeper-zero-copy-bytesmut-v2`
3. `issue712-deeper-zero-copy-bytesmut-v3`

### 4.1 `16MiB`

1. `v1`: `400.41 MiB/s`, `643.5ms`
2. `v2`: `281.44 MiB/s`, `971.4ms`
3. `v3`: `398.60 MiB/s`, `674.4ms`

汇总：

1. Avg throughput: `360.15 MiB/s`
2. Median throughput: `398.60 MiB/s`
3. Avg latency: `763.1ms`
4. Median latency: `674.4ms`

### 4.2 `32MiB`

1. `v1`: `370.94 MiB/s`, `1440.1ms`
2. `v2`: `265.00 MiB/s`, `2082.7ms`
3. `v3`: `272.01 MiB/s`, `1937.6ms`

汇总：

1. Avg throughput: `302.65 MiB/s`
2. Median throughput: `272.01 MiB/s`
3. Avg latency: `1820.1ms`
4. Median latency: `1937.6ms`

## 5. 与上一阶段 `zero_copy_eager` 对比

上一阶段主线分支的 ordinary PUT 小面结果为：

### `zero_copy_eager_v1`

1. `16MiB`: `318.77 MiB/s`, `851.3ms`
2. `32MiB`: `276.38 MiB/s`, `1846.5ms`

### `zero_copy_eager_v2`

1. `16MiB`: `242.44 MiB/s`, `1209.8ms`
2. `32MiB`: `339.04 MiB/s`, `1576.2ms`

## 6. 当前判断

到这一步，可以先收敛出下面几点：

1. `BytesMut ingest` 方向在 `16MiB` ordinary PUT 上有明显正收益信号
2. `32MiB` 的结果仍然不稳定
3. 这说明这条 deeper zero-copy 方向值得保留为实验线，但还不适合直接合回主线

换句话说：

1. 这不是失败实验
2. 但也不是已经具备稳定端到端收益的成熟优化

## 7. 当前建议

当前最稳妥的建议是：

1. 继续把 `RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST=true` 保持为 env-gated 实验能力
2. 不改默认行为
3. 如果继续推进，优先补：
   - 更多重复性普通 PUT 复测
   - 视情况加入 `64MiB` 面
4. 在证据收敛前，不把这条分支直接合回当前 `issue-712` 主线 PR

## 8. `32/64/128MiB` 聚焦矩阵

在继续推进时，又补了一组更聚焦的普通 PUT 矩阵：

1. `issue712-deeper-zero-copy-bytesmut-v6`
2. `issue712-deeper-zero-copy-bytesmut-v7`

固定条件：

1. `32MiB / 64MiB / 128MiB`
2. `c16`
3. `duration=60s`
4. `RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST=true`

### 8.1 `v6`

1. `32MiB`: `402.13 MiB/s`, `1285.7ms`
2. `64MiB`: `413.68 MiB/s`, `2583.3ms`
3. `128MiB`: `388.22 MiB/s`, `5496.0ms`

### 8.2 `v7`

1. `32MiB`: `327.00 MiB/s`, `1654.3ms`
2. `64MiB`: `334.44 MiB/s`, `3345.5ms`
3. `128MiB`: `381.49 MiB/s`, `5591.9ms`

## 9. `v6/v7` 两轮汇总

### `32MiB`

1. Avg throughput: `364.56 MiB/s`
2. Avg latency: `1470.0ms`

### `64MiB`

1. Avg throughput: `374.06 MiB/s`
2. Avg latency: `2964.4ms`

### `128MiB`

1. Avg throughput: `384.86 MiB/s`
2. Avg latency: `5543.9ms`

## 10. 阶段性判断更新

补完这组聚焦矩阵后，可以把结论进一步收窄为：

1. `BytesMut ingest` 在 `32/64/128MiB` 区间依然值得继续保留为实验方向
2. 但 `32MiB` 与 `64MiB` 的波动仍然明显，当前还不够稳定
3. `128MiB` 两轮结果更接近，稳定性相对更好

也就是说，这条 deeper-zero-copy 线当前更像是：

1. 对更大对象区间更有潜力
2. 但还没有足够证据支持直接收敛成默认行为

## 11. 当前最稳妥建议

到这一轮为止，建议更新为：

1. 继续把 `RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST=true` 作为 `env-only` 实验能力保留
2. 后续如果继续压测，优先关注 `64MiB` 与 `128MiB`
3. `32MiB` 可以保留，但不再作为唯一核心判断点
4. 当前不建议把这条线直接并回主线 PR
