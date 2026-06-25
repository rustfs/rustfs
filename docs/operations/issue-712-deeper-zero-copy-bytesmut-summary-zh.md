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

## 12. 更大对象区间补充矩阵

为了继续判断这条 deeper-zero-copy 线是否主要对更大对象区间成立，又补了一轮：

1. `issue712-deeper-zero-copy-bytesmut-v8`

固定条件：

1. `64MiB / 128MiB / 256MiB`
2. `c16`
3. `duration=60s`
4. `RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST=true`

结果：

1. `64MiB`: `431.39 MiB/s`, `2472.9ms`
2. `128MiB`: `437.21 MiB/s`, `4889.5ms`
3. `256MiB`: `435.43 MiB/s`, `9755.9ms`

## 13. 当前判断更新

补完 `64/128/256MiB` 这一轮之后，可以把结论进一步更新为：

1. 这条 `BytesMut ingest` deeper-zero-copy 线对更大对象区间的信号明显 stronger
2. `64MiB` 已经比前一轮 `v6/v7` 更强
3. `128MiB` 和 `256MiB` 两个点都保持了很高吞吐

这说明：

1. 这条实验线很可能不是“普适 plain PUT 优化”
2. 更像是对更大对象区间更有效

## 14. 当前最稳妥建议

到这一步，最稳妥的推进顺序可以更新为：

1. `RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST=true` 继续保留为 env-gated 实验能力
2. 后续验证重心优先放在：
   - `64MiB`
   - `128MiB`
   - `256MiB`
3. `16MiB` 和 `32MiB` 可以作为对照，但不再是核心判断区间
4. 在还没有更多重复性复测前，这条线仍然不直接并回主线 PR

## 15. `64/128/256MiB` 第二轮补充

为了继续验证更大对象区间，又补了一轮：

1. `issue712-deeper-zero-copy-bytesmut-v9`

结果：

1. `64MiB`: `269.37 MiB/s`, `3971.5ms`
2. `128MiB`: `260.68 MiB/s`, `8177.4ms`
3. `256MiB`: `259.07 MiB/s`, `14848.4ms`

## 16. 结果修正

把 `v8` 和 `v9` 合起来看：

### `64MiB`

1. `v8`: `431.39 MiB/s`, `2472.9ms`
2. `v9`: `269.37 MiB/s`, `3971.5ms`

### `128MiB`

1. `v8`: `437.21 MiB/s`, `4889.5ms`
2. `v9`: `260.68 MiB/s`, `8177.4ms`

### `256MiB`

1. `v8`: `435.43 MiB/s`, `9755.9ms`
2. `v9`: `259.07 MiB/s`, `14848.4ms`

这说明：

1. 更大对象区间虽然在 `v8` 中表现很好
2. 但 `v9` 再次出现了明显回落
3. 因此单靠 `v8/v9` 两轮结果，还不能把这条线判断成“已经稳定成立”的优化

## 17. `capture-backed v3` 确认性复测

为降低上一轮波动判断里的环境不确定性，又补了一轮带 supporting evidence 的确认性复测：

1. `issue712-deeper-zero-copy-capture-v3-20260625T080909Z`

固定条件：

1. `RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST=true`
2. `64MiB / 128MiB / 256MiB`
3. `c16`
4. `duration=60s`
5. `rounds=1`
6. `cooldown=20s`
7. 同步采集 `health/ready`、Prometheus text snapshot、host snapshot

结果：

1. `64MiB`: `379.49 MiB/s`, `2692.7ms`
2. `128MiB`: `364.71 MiB/s`, `5603.5ms`
3. `256MiB`: `358.57 MiB/s`, `11306.0ms`

`aggregate_median_summary.csv` 对应中位吞吐分别为：

1. `64MiB`: `397924106.24 B/s`
2. `128MiB`: `382426152.96 B/s`
3. `256MiB`: `375987896.32 B/s`

## 18. 这轮 capture 证据说明了什么

这轮目录：

1. `target/bench/issue712-deeper-zero-copy-capture-v3-20260625T080909Z`

supporting evidence 的关键点：

1. `25/25` 个 `health` 快照都正常
2. `25/25` 个 `ready` 快照都返回 `ready=true`，没有再出现上一轮那种持续 `503`
3. `25` 个 Prometheus text snapshot 都成功落盘
4. 末尾 snapshot 中 4 块盘的 `availability/io/timeout` error counter 都仍为 `0`
5. `internode dial/errors` counter 仍为 `0`
6. 末尾 snapshot 中 RustFS process resident memory 约为 `2.06 GiB`
7. `ps.*.txt` 能稳定看到 RustFS 进程，最后一个样本 RSS 约 `2013424 KiB`

也要保留一个约束：

1. 这轮 `host/iostat.txt` 在 macOS 上只记录到了 `iostat: illegal option -- x`
2. 所以这次 capture 对 CPU / health / ready / Prometheus 是可用的
3. 但对更细的磁盘侧 host telemetry 仍然不是完整证据链

## 19. 阶段性判断再收紧

补完这轮 `capture-backed v3` 后，可以把判断更新为：

1. 上一轮失败更像是运行环境/实例就绪问题导致的假阴性，不是这条实验分支必然失效
2. 在运行面稳定、带冷却时间、并补齐 supporting evidence 的前提下，这条线在 `64MiB+` 普通 PUT 上仍然能跑出一组干净结果
3. 这进一步说明 `Erasure::encode` ingest 的 deeper-zero-copy 方向值得继续保留为实验线
4. 但这轮仍然只有单次 `capture-backed` 确认，不足以证明它已经稳定优于主线
5. 当前仍应把这条能力保留为 `env-gated`
6. 这条线仍更适合作为实验 PR / Draft PR 持续收集证据
7. 后续如果再补验证，优先做同矩阵重复性复测，而不是直接改默认行为
8. 当前还不能把“对更大对象更有效”当成稳定结论

## 20. 当前最保守结论

到当前阶段，应把结论再次收紧为：

1. `BytesMut ingest` deeper-zero-copy 方向仍值得保留为实验线
2. 但目前无论 `16/32MiB` 还是 `64/128/256MiB`，都还没有形成足够稳定的端到端收益
3. 当前不建议继续机械追加更多相同矩阵轮次
4. 更合理的下一步应转向：
   - 先分析为什么 `v8` 和 `v9` 差异如此大
   - 再决定是否继续压测，或改为更细粒度观测
