# Issue #712 `RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS` 候选值低噪声复测矩阵

## 1. 目标

本文用于下一轮更小面、更低噪声的复测。

本轮只回答一个问题：

1. `RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS=2` 是否比 `4` 更稳地改善 `2g-128m-pc4` 的 multipart batched 路径

当前不回答：

1. 是否直接改代码默认值
2. 是否扩展到 ordinary PUT
3. 是否继续引入新的 encode 调度语义

## 2. 固定范围

只保留一个 profile：

1. `2g-128m-pc4`

只保留两个候选值：

1. `RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS=4`
2. `RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS=2`

不再混入：

1. `1g-64m-pc4`
2. `2g-256m-pc4`
3. 其他 batching / inflight 配置

## 3. 低噪声原则

本轮强制执行以下原则：

1. 每一轮都重启 RustFS 进程，避免 counters/sums 混轮
2. 每一轮之间固定冷却 `30s`
3. 每一轮都固定只跑 `2m`
4. 每一轮都只抓：
   - `summary.csv`
   - `path.json`
   - `internal_count.json`
   - `internal_sum.json`
5. 不看过程日志，不在中间临时扩项

## 4. 推荐矩阵

推荐使用交叉顺序，避免单边连续运行：

1. `b4-r1`
2. `b2-r1`
3. `b2-r2`
4. `b4-r2`
5. `b4-r3`
6. `b2-r3`

这是一个偏保守的 `ABBAAB` 顺序。

原因：

1. 不让 `2` 总是出现在后面
2. 不让 `4` 总是出现在后面
3. 能更快看出“只是后跑更快”还是“候选值真的更优”

## 5. 目录约定

建议统一使用：

```text
target/bench/issue712-batchblocks-candidate-runs/
  b4-r1/
  b2-r1/
  b2-r2/
  b4-r2/
  b4-r3/
  b2-r3/
```

每轮目录固定包含：

1. `summary.csv`
2. `path.json`
3. `internal_count.json`
4. `internal_sum.json`

## 6. 单轮执行模板

### 6.1 `batch_blocks=4`

启动：

```bash
env \
  RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true \
  RUSTFS_ADDRESS=127.0.0.1:9000 \
  RUSTFS_ACCESS_KEY=rustfsadmin \
  RUSTFS_SECRET_KEY=rustfsadmin \
  RUSTFS_RPC_SECRET=rustfs-rpc-secret \
  RUSTFS_REGION=us-east-1 \
  RUSTFS_CONSOLE_ENABLE=false \
  RUSTFS_OBS_ENDPOINT=http://127.0.0.1:4318 \
  RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS=4 \
  target/debug/rustfs server \
  /private/tmp/issue708-single-node-multidisk/d1 \
  /private/tmp/issue708-single-node-multidisk/d2 \
  /private/tmp/issue708-single-node-multidisk/d3 \
  /private/tmp/issue708-single-node-multidisk/d4
```

运行：

```bash
bash scripts/run_gt1g_multipart_put_server_path_focus.sh \
  --host 127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --profiles 2g-128m-pc4 \
  --duration 2m \
  --out-dir target/bench/issue712-batchblocks-candidate-runs/b4-r1
```

采集：

```bash
curl -fsS 'http://127.0.0.1:9090/api/v1/query?query=rustfs_s3_put_object_path_total{path=~"multipart_.*"}' \
  > target/bench/issue712-batchblocks-candidate-runs/b4-r1/path.json

curl -fsS 'http://127.0.0.1:9090/api/v1/query?query=rustfs_internal_stage_duration_ms_count{stage=~"erasure_encode.*"}' \
  > target/bench/issue712-batchblocks-candidate-runs/b4-r1/internal_count.json

curl -fsS 'http://127.0.0.1:9090/api/v1/query?query=rustfs_internal_stage_duration_ms_sum{stage=~"erasure_encode.*"}' \
  > target/bench/issue712-batchblocks-candidate-runs/b4-r1/internal_sum.json
```

### 6.2 `batch_blocks=2`

只改一个变量：

```bash
RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS=2
```

其余命令完全保持一致。

## 7. 每轮必须核对的点

### 7.1 path 命中

必须确认：

1. `multipart_write_pipeline_batched_large` 已命中

如果没有命中：

1. 该轮结果无效
2. 不要纳入比较

### 7.2 internal stages

至少比较以下 3 组：

1. `erasure_encode_batched_recv_wait`
2. `erasure_encode_batched_write`
3. `erasure_encode_cpu`

## 8. 推荐的人工比较方式

对每一轮，记录：

1. throughput
2. avg latency
3. `recv_wait_avg = internal_sum / internal_count`
4. `write_avg = internal_sum / internal_count`
5. `cpu_avg = internal_sum / internal_count`

重点不是单看 throughput，而是看：

1. `2` 是否更稳定地降低 `recv_wait_avg`
2. `2` 是否没有把 `write_avg` 或 `cpu_avg` 反向放大

## 9. 建议的判定门槛

只有同时满足下面两条，才建议继续往“默认值候选”方向推进：

1. 在至少 `3` 轮对照中，`batch_blocks=2` 的 throughput / latency 有 `>= 2` 轮优于 `4`
2. `recv_wait_avg` 的下降方向在大多数轮次都成立

如果只满足其中一条：

1. 保持它为候选 env 配置
2. 暂不改代码默认值

## 10. 当前建议

当前阶段最稳妥的动作顺序是：

1. 先按这份矩阵补足 `6` 轮
2. 再做一次汇总表
3. 最后才决定是否让 multipart batched path 默认使用 `2`
