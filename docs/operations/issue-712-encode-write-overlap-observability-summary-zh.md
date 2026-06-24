# Issue #712 encode / write overlap 观测小结

## 1. 目的

本文记录 `#712` 新一轮更细主线的第一步结果：

1. 不先冒进改 `encode.rs` 调度语义
2. 先把 `multipart_set_disk_encode` 内部拆成更细阶段
3. 用 focused benchmark 判断当前 overlap 更像卡在 encode 侧、write 侧，还是 batch barrier

## 2. 新增内部阶段

本轮在 `crates/ecstore/src/erasure_coding/encode.rs` 中补充了内部阶段观测，使用指标：

1. `rustfs_internal_stage_duration_ms`

新增阶段：

1. `erasure_encode_cpu`
2. `erasure_encode_send_wait`
3. `erasure_encode_recv_wait`
4. `erasure_encode_write`
5. `erasure_encode_shutdown`
6. `erasure_encode_batched_send_wait`
7. `erasure_encode_batched_recv_wait`
8. `erasure_encode_batched_write`
9. `erasure_encode_batched_shutdown`

## 3. focused baseline

profile：

1. `2g-128m-pc4`

默认 batched 配置（`RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS=4`）下，本轮 observed run 结果为：

1. Throughput: `351.96 MiB/s`
2. Avg latency: `5842.1ms`
3. path: `multipart_write_pipeline_batched_large`

从 raw sum / count 推出的内部均值近似为：

1. `erasure_encode_cpu`: `~2.95ms`
2. `erasure_encode_batched_send_wait`: `~0.01ms`
3. `erasure_encode_batched_recv_wait`: `~105.3ms`
4. `erasure_encode_batched_write`: `~75.4ms`
5. `erasure_encode_batched_shutdown`: `~0.90ms`

## 4. 第一轮判断

这组数据说明：

1. `send_wait` 几乎可以忽略，说明 encode producer 基本没有被 queue backpressure 卡住
2. `recv_wait` 明显高于 `write`，说明 consumer 侧更常见的是在等下一批 encode 结果，而不是 writer 太慢导致队列打满
3. `cpu` 本身并不大，真正放大的是 batched producer/consumer 之间的批次屏障

换句话说，这一轮更像是：

1. writer 在等 encoder / 等 batch 集齐
2. 而不是 encoder 在等 writer

## 5. 配置性验证

为了验证 batch barrier 假设，本轮只改一个变量：

1. `RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS=2`

同样只跑：

1. `2g-128m-pc4`

结果：

1. Throughput: `368.57 MiB/s`
2. Avg latency: `5537.3ms`
3. path: `multipart_write_pipeline_batched_large`

raw sum / count 推导的内部均值近似为：

1. `erasure_encode_cpu`: `~2.94ms`
2. `erasure_encode_batched_send_wait`: `~0.01ms`
3. `erasure_encode_batched_recv_wait`: `~48.9ms`
4. `erasure_encode_batched_write`: `~35.9ms`
5. `erasure_encode_batched_shutdown`: `~0.36ms`

## 6. 当前结论

这轮可以先收敛出一个相对明确的方向：

1. 当前 batched 路径的第一优先问题不像是 encode CPU 绝对太重
2. 更像是 batch size 偏大，导致 writer 侧等待下一批 encode 结果的时间被放大
3. 把 `RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS` 从 `4` 降到 `2` 后，结果明显好于默认值

## 7. 当前建议

下一步如果继续沿着 overlap 这条线推进，建议优先级如下：

1. 先把 `RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS=2` 作为 batched 路径的候选值继续复测
2. 再决定是否要把 multipart batched path 的 batch size 做成与 ordinary PUT 分离
3. 在没有更多证据前，不要继续尝试更激进的 batch 内单次 blocking encode 调度改法

## 8. 第二轮更稳妥复测

为了减少热态偏差，本轮又按 `4 -> 2 -> 4 -> 2` 做了四轮受控复测。

profile 固定：

1. `2g-128m-pc4`

结果：

1. `b4-r1`: `317.20 MiB/s`, `6515.2ms`
2. `b2-r1`: `300.38 MiB/s`, `6842.8ms`
3. `b4-r2`: `334.58 MiB/s`, `5966.5ms`
4. `b2-r2`: `358.77 MiB/s`, `5748.2ms`

从这组数据看：

1. `batch_blocks=2` 不是每一轮都赢
2. 但 `b2-r2` 明显优于同组前后的 `b4-r2`
3. 结果仍然存在不小波动，因此还不足以直接改代码默认值

## 9. 第二轮内部阶段对比

对 `b4-r2` 与 `b2-r2` 的 raw sum / count 做近似均值后，可以看到：

### `b4-r2`

1. `erasure_encode_batched_recv_wait`: `~107.4ms`
2. `erasure_encode_batched_write`: `~78.2ms`
3. `erasure_encode_cpu`: `~3.19ms`

### `b2-r2`

1. `erasure_encode_batched_recv_wait`: `~50.9ms`
2. `erasure_encode_batched_write`: `~37.7ms`
3. `erasure_encode_cpu`: `~3.06ms`

这说明：

1. `batch_blocks=2` 的主要收益方向仍然是降低 batched consumer 侧等待时间
2. `cpu` 本身没有发生决定性变化
3. 当前更像是在改善 batch barrier，而不是改变编码计算本体

## 10. 当前收敛结论

到这一轮为止，更稳妥的结论是：

1. `RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS=2` 仍然值得保留为候选配置
2. 它对 `erasure_encode_batched_recv_wait` 的改善方向是清晰的
3. 但吞吐/延迟收益还不够稳定，当前不建议直接改默认值
4. 下一步更适合继续以环境变量方式复测，而不是马上把默认值写死到代码里
