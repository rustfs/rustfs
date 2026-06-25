# Issue #712 multipart size-gated A/B 结果总结

## 1. 本轮目的

本轮不是继续扩面 benchmark，而是回答一个更窄的问题：

1. `set_disk.rs` 上的 multipart size-gated batching 是否真的在运行时生效
2. 如果生效，`2g-128m-pc4` 下是否优于纯 pipeline

## 2. 先修正的运行时问题

在继续验证中发现一个关键问题：

1. multipart 路径的 batching 分类使用了 `data.size()`
2. 但原逻辑是在 `data.stream` 被替换为空 reader 之后才读取这个 size
3. 这会导致 multipart size gate 在运行时无法按预期命中

本轮已在 multipart 路径上修正为：

1. 先捕获原始 `multipart_part_size`
2. 再执行 stream swap
3. 再基于原始 size 做 `classify_multipart_part_write_path(...)`

同时补充了 multipart path 计数标签：

1. `multipart_write_pipeline`
2. `multipart_write_pipeline_batched_large`
3. `multipart_write_single_block_non_inline`

## 3. focused 对照矩阵

本轮只保留受影响的 profile：

1. `2g-128m-pc4`

并做两组对照：

1. pipeline 基线：
   - `RUSTFS_MULTIPART_PUT_LARGE_BATCH_MIN_SIZE_BYTES=10737418240`
2. 默认门槛实验：
   - 使用默认 `128MiB` 门槛

附加做了一组强制 batched 校验：

1. `RUSTFS_MULTIPART_PUT_LARGE_BATCH_MIN_SIZE_BYTES=1`

## 4. 结果

### 4.1 修正后的 pipeline 基线

结果目录：

1. `target/bench/issue712-multipart-server-path-fixed-pipeline/summary.csv`

结果：

1. Throughput: `364.83 MiB/s`
2. Avg latency: `5604.5ms`
3. Path counter: 仅出现 `multipart_write_pipeline`
4. `multipart_set_disk_encode` P95: `7348.88ms`

### 4.2 修正后的默认门槛实验

结果目录：

1. `target/bench/issue712-multipart-server-path-fixed-batched/summary.csv`

结果：

1. Throughput: `359.00 MiB/s`
2. Avg latency: `5734.9ms`
3. Path counter: 已出现 `multipart_write_pipeline_batched_large`
4. `multipart_set_disk_encode` P95: `7375ms`

说明：

1. batched 路径在修正后已经真正开始命中
2. 但当前结果并没有优于纯 pipeline

### 4.3 强制 batched 校验

结果目录：

1. `target/bench/issue712-multipart-server-path-forced-batched/summary.csv`

结果：

1. Throughput: `362.00 MiB/s`
2. Avg latency: `5664.3ms`

这组结果同样没有优于修正后的 pipeline 基线。

## 5. 本轮结论

本轮可以明确收敛出三点：

1. multipart size-gated batching 的运行时命中问题已经被定位并修正
2. batched path 修正后确实可以被 Prometheus path counter 观察到
3. 在当前单机多盘 `2g-128m-pc4` focused 验证下，默认 `128MiB` 门槛没有带来正收益，反而略逊于纯 pipeline

## 6. 当前建议

在当前证据下，不建议把 multipart batched path 作为默认推荐优化结论直接推进。

更稳妥的后续方向是：

1. 先保留这条路径为可控实验能力
2. 继续围绕 `multipart_set_disk_encode` 本身做更细粒度优化
3. 如果还要继续试 batching，应先解释为什么 `128MiB` part 在当前实现下没有优于 pipeline，而不是直接继续扩大默认启用范围
