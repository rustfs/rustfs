# Issue #713 `>1GiB GET` 优化与验证手册

## 1. 目标

本文用于收口 `backlog#713` 当前这一轮 `>1GiB GET` 优化的实现点与验证方式。

当前这轮改动先解决三件事：

1. `sequential hint` 真正参与 GET I/O 策略计算
2. storage media buffer cap 不再被统一的 `1MiB` hard clamp 二次截断
3. `>1GiB`、非 range、允许 readahead 的 streaming GET，会走更大的 stream buffer 策略

## 2. 当前代码行为变化

### 2.1 多因素调度

涉及：

1. `rustfs/src/storage/concurrency/io_schedule.rs`

当前行为：

1. `IoSchedulingContext.is_sequential_hint=true` 时，会优先按 `Sequential` 模式参与 buffer / readahead 决策
2. storage profile 提供的 media cap 现在可以真实生效
3. 例如默认常量下：
   - `NVMe`: `2MiB`
   - `SSD`: `1MiB`
   - `HDD`: `512KiB`

### 2.2 GET body stream 策略

涉及：

1. `rustfs/src/app/object_usecase.rs`

当前行为：

1. `optimal_buffer_size` 不再被额外 `min(base_buffer_size)` 压回去
2. `>1GiB`、非 range、`enable_readahead=true` 的 streaming GET，会命中：
   - `large_sequential_readahead`
3. 该路径当前把 `ReaderStream` buffer 扩到：
   - `min(optimal_buffer_size * 2, 4MiB)`

### 2.3 GET observability

涉及：

1. `crates/io-metrics/src/lib.rs`

新增：

1. `rustfs_io_get_object_stream_strategy_total{strategy=...}`
2. `rustfs_io_get_object_stream_buffer_size_bytes{strategy=...}`
3. `rustfs_io_get_object_stream_response_size_bytes{strategy=...}`

当前策略标签：

1. `standard`
2. `large_sequential_readahead`

## 3. 推荐验证顺序

按下面顺序做最稳妥：

1. 先准备 plain `1GiB` / `2GiB` 对象
2. 先跑 `sequential` baseline
3. 再跑 `ranged_parallel`
4. 最后再决定是否继续补 encrypted / compressed

## 4. 准备测试对象

直接使用：

1. `scripts/prepare_gt1g_get_test_objects.sh`

示例：

```bash
bash scripts/prepare_gt1g_get_test_objects.sh \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket rustfs-bench \
  --prefix bench/issue713/plain
```

默认会准备：

1. `1GiB=plain-1g.bin`
2. `2GiB=plain-2g.bin`

默认最终对象路径：

1. `s3://rustfs-bench/bench/issue713/plain/plain-1g.bin`
2. `s3://rustfs-bench/bench/issue713/plain/plain-2g.bin`

## 5. 执行 GET 矩阵

直接使用：

1. `scripts/run_gt1g_get_http_matrix.sh`

这个脚本特点：

1. 不把响应体落到本地磁盘
2. 直接把 body 丢到 `/dev/null`
3. 用 `curl` 的 `size_download` 做字节校验和吞吐计算
4. 同时支持：
   - `sequential`
   - `ranged_parallel`

补充说明：

1. 如果本机没有 `mc`，脚本会自动 fallback 到仓库内置的 `rustfs/tests/gt1g_get_benchmark_tool.rs`
2. fallback 模式下，结果目录会落在：
   - `rustfs/target/bench/...`
3. 非 fallback 模式下，结果目录仍按脚本参数落在：
   - `target/bench/...`

### 5.1 baseline：sequential + ranged_parallel

```bash
bash scripts/run_gt1g_get_http_matrix.sh \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket rustfs-bench \
  --objects '1GiB=bench/issue713/plain/plain-1g.bin,2GiB=bench/issue713/plain/plain-2g.bin' \
  --modes sequential,ranged_parallel \
  --concurrencies 1,4,8 \
  --range-workers 4 \
  --rounds 3 \
  --cooldown-secs 20
```

### 5.2 只跑 sequential

```bash
bash scripts/run_gt1g_get_http_matrix.sh \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket rustfs-bench \
  --objects '1GiB=bench/issue713/plain/plain-1g.bin,2GiB=bench/issue713/plain/plain-2g.bin' \
  --modes sequential \
  --concurrencies 1,4,8 \
  --rounds 3 \
  --cooldown-secs 20
```

### 5.3 只跑 ranged_parallel

```bash
bash scripts/run_gt1g_get_http_matrix.sh \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket rustfs-bench \
  --objects '1GiB=bench/issue713/plain/plain-1g.bin,2GiB=bench/issue713/plain/plain-2g.bin' \
  --modes ranged_parallel \
  --concurrencies 1,4,8 \
  --range-workers 4,8 \
  --rounds 3 \
  --cooldown-secs 20
```

## 6. 输出目录

默认输出：

```text
target/bench/gt1g-get-http-<timestamp>/
```

重点看：

1. `round_results.csv`
2. `median_summary.csv`
3. `logs/`

`round_results.csv` 字段：

1. `object_label`
2. `object_key`
3. `mode`
4. `range_workers`
5. `concurrency`
6. `round`
7. `status`
8. `total_bytes`
9. `elapsed_ms`
10. `throughput_bps`
11. `avg_client_latency_ms`

## 7. 当前推荐观察点

### 7.1 sequential 是否命中大对象策略

查询：

```promql
sum by (strategy) (
  increase(rustfs_io_get_object_stream_strategy_total[5m])
)
```

预期：

1. `>1GiB` 顺序 GET 应出现 `large_sequential_readahead`

### 7.2 GET stream buffer 分布

```promql
histogram_quantile(
  0.95,
  sum by (strategy, le) (
    rate(rustfs_io_get_object_stream_buffer_size_bytes_bucket[5m])
  )
)
```

### 7.3 GET 端到端完成时长

```promql
histogram_quantile(
  0.95,
  sum by (le) (
    rate(rustfs_io_get_object_total_duration_seconds_bucket[5m])
  )
)
```

## 8. 当前判断建议

如果 sequential 在 `1GiB/2GiB`、`c1` 下仍然没有明显优于旧结果，优先检查：

1. `large_sequential_readahead` 是否真的命中
2. buffer size 是否仍停在 `1MiB`
3. 请求是否实际是 range / encrypted / compressed，从而没走当前优化路径

如果 ranged_parallel 明显优于 sequential，再继续补：

1. 客户端并行分片数建议
2. plain 与 encrypted/compressed 的拆分对比

## 9. 当前边界

这轮实现还没有直接覆盖：

1. encrypted `>1GiB GET` 的单独 read-ahead 策略
2. compressed `>1GiB GET` 的专门 index / range 成本优化
3. server-side 主动预取缓存

因此当前更适合先做：

1. plain sequential vs ranged_parallel 基线
2. 命中策略与吞吐结果收敛
3. 再决定下一刀是否继续进 encrypted / compressed
