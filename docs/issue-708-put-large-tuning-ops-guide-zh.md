# Issue #708 大对象 PUT 调参矩阵执行手册

## 1. 目的

本文是给压测和验证同学直接执行的 `#708` 操作手册，用于围绕大对象 PUT 做参数 A/B 收敛。

目标对象尺寸：

1. `16MiB`
2. `32MiB`

优先参数组：

1. `RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES`
2. `RUSTFS_OBJECT_IO_BUFFER_SIZE`
3. `RUSTFS_RUNTIME_WORKER_THREADS`
4. `RUSTFS_RUNTIME_MAX_BLOCKING_THREADS`
5. `RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE`

统一入口脚本：

- [scripts/run_put_large_tuning_matrix.sh](/Users/zhi/Documents/code/rust/rustfs/rustfs/scripts/run_put_large_tuning_matrix.sh)

## 2. 与 #706 的关系

`#708` 不是单独从零开始跑 benchmark，而是建立在 `#706` 的 baseline 基础上。

它复用的入口包括：

1. `scripts/run_put_large_stage_breakdown.sh`
2. `scripts/collect_put_large_stage_breakdown_artifacts.sh`
3. `scripts/run_put_large_stage_breakdown_with_capture.sh`

因此：

1. `#706` 负责 baseline 和 supporting evidence 基线
2. `#708` 负责在 baseline 之上做 profile 化调参矩阵

## 3. 执行前准备

### 3.1 压测 client 机器

先准备：

```bash
export RUSTFS_ACCESS_KEY="<your-access-key>"
export RUSTFS_SECRET_KEY="<your-secret-key>"
```

### 3.2 RustFS 节点 baseline 建议

如果你们要从当前推荐 baseline 起步，可在 RustFS 节点侧先设置：

```bash
export RUSTFS_ENABLE_PROFILING=true
export RUSTFS_PROF_CPU_MODE=continuous
export RUSTFS_PROF_CPU_FREQ=100
export RUSTFS_PROF_OUTPUT_DIR=target/profiles

export RUSTFS_OBJECT_IO_BUFFER_SIZE=262144
export RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=8388608
export RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES=25165824
export RUSTFS_RUNTIME_WORKER_THREADS=12
export RUSTFS_RUNTIME_MAX_BLOCKING_THREADS=512
```

如需短期增强诊断日志：

```bash
export RUSTFS_ISSUE3031_DIAG_ENABLE=true
```

## 4. 当前 profile 组

当前 tuning matrix runner 支持：

### 4.1 baseline

```text
baseline
```

### 4.2 inflight

```text
inflight-32m
inflight-48m
inflight-64m
```

对应：

```bash
RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES=33554432
RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES=50331648
RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES=67108864
```

### 4.3 io-buffer

```text
io-buffer-512k
io-buffer-1m
```

对应：

```bash
RUSTFS_OBJECT_IO_BUFFER_SIZE=524288
RUSTFS_OBJECT_IO_BUFFER_SIZE=1048576
```

### 4.4 runtime

```text
runtime-w16-b768
runtime-w20-b1024
```

对应：

```bash
RUSTFS_RUNTIME_WORKER_THREADS=16
RUSTFS_RUNTIME_MAX_BLOCKING_THREADS=768

RUSTFS_RUNTIME_WORKER_THREADS=20
RUSTFS_RUNTIME_MAX_BLOCKING_THREADS=1024
```

### 4.5 duplex

```text
duplex-16m
```

对应：

```bash
RUSTFS_OBJECT_DUPLEX_BUFFER_SIZE=16777216
```

## 5. 建议执行顺序

优先顺序建议保持与设计一致：

1. `inflight`
2. `io-buffer`
3. `runtime`
4. `duplex`

如果 baseline 还没有稳定，不要直接跑整套 `all`。

## 6. 先做 dry-run

先确认 profile 展开和输出目录没有问题：

### 6.1 只看 inflight

```bash
bash scripts/run_put_large_tuning_matrix.sh \
  --endpoint http://127.0.0.1:9000 \
  --access-key "$RUSTFS_ACCESS_KEY" \
  --secret-key "$RUSTFS_SECRET_KEY" \
  --group inflight \
  --concurrencies 16 \
  --rounds 1 \
  --duration 5s \
  --out-root target/bench/put-large-tuning-dryrun \
  --dry-run
```

### 6.2 看全部 profile

```bash
bash scripts/run_put_large_tuning_matrix.sh \
  --endpoint http://127.0.0.1:9000 \
  --access-key "$RUSTFS_ACCESS_KEY" \
  --secret-key "$RUSTFS_SECRET_KEY" \
  --group all \
  --concurrencies 16 \
  --rounds 1 \
  --duration 5s \
  --out-root target/bench/put-large-tuning-dryrun \
  --dry-run
```

dry-run 时重点确认：

1. baseline 会先跑
2. 非 baseline profile 会自动指向 `<out-root>/baseline`
3. 每个 profile 都会生成 `env_snapshot.env`
4. benchmark / capture 仍会复用 `#706` 的 one-shot 入口

## 7. 正式执行

### 7.1 推荐先跑 inflight 组

```bash
bash scripts/run_put_large_tuning_matrix.sh \
  --endpoint http://127.0.0.1:9000 \
  --access-key "$RUSTFS_ACCESS_KEY" \
  --secret-key "$RUSTFS_SECRET_KEY" \
  --group inflight \
  --nodes 4 \
  --disks-per-node 8 \
  --total-disks 32 \
  --cpu-per-node "16c" \
  --mem-per-node "32GiB" \
  --network "25Gbps" \
  --endpoint-mode direct \
  --erasure-set-drive-count 8 \
  --client-host "$(hostname)" \
  --concurrencies 16,32,64,96,128 \
  --sizes 16MiB,32MiB \
  --duration 120s \
  --rounds 3 \
  --capture-interval-secs 15 \
  --out-root target/bench/put-large-tuning-inflight
```

### 7.2 再跑 io-buffer 组

```bash
bash scripts/run_put_large_tuning_matrix.sh \
  --endpoint http://127.0.0.1:9000 \
  --access-key "$RUSTFS_ACCESS_KEY" \
  --secret-key "$RUSTFS_SECRET_KEY" \
  --group io-buffer \
  --nodes 4 \
  --disks-per-node 8 \
  --total-disks 32 \
  --cpu-per-node "16c" \
  --mem-per-node "32GiB" \
  --network "25Gbps" \
  --endpoint-mode direct \
  --erasure-set-drive-count 8 \
  --client-host "$(hostname)" \
  --concurrencies 16,32,64,96,128 \
  --sizes 16MiB,32MiB \
  --duration 120s \
  --rounds 3 \
  --capture-interval-secs 15 \
  --out-root target/bench/put-large-tuning-io-buffer
```

### 7.3 如果需要 profile 应用 / 重启 / reload

如果目标环境需要在每个 profile 切换后执行某个 apply/restart/reload 命令，可用：

```bash
bash scripts/run_put_large_tuning_matrix.sh \
  --endpoint http://127.0.0.1:9000 \
  --access-key "$RUSTFS_ACCESS_KEY" \
  --secret-key "$RUSTFS_SECRET_KEY" \
  --group runtime \
  --out-root target/bench/put-large-tuning-runtime \
  --apply-cmd "bash scripts/restart-rustfs.sh" \
  --apply-wait-secs 30
```

执行时脚本会给当前 profile 写出：

```text
<out-root>/<profile>/env_snapshot.env
```

并通过环境变量传给 apply 命令：

```text
RUSTFS_TUNING_ENV_FILE=<out-root>/<profile>/env_snapshot.env
```

因此 apply/restart 脚本可以直接读这份 env 文件决定如何应用参数。

## 8. 输出目录结构

每个 profile 都会单独落到：

```text
<out-root>/<profile>/
```

其中会包含：

1. `env_snapshot.env`
2. baseline 或 profile 对应的 benchmark + capture run-root

由于内部复用了 one-shot 入口，所以每个 profile 目录下仍会继续产生：

```text
run_manifest.txt
run_matrix.csv
aggregate_median_summary.csv
aggregate_baseline_compare.csv
runs/
captures/
```

## 9. 执行结束后请回传的结果

至少回传：

1. `baseline/aggregate_median_summary.csv`
2. 每个 profile 目录下的 `env_snapshot.env`
3. 每个 profile 目录下的 `aggregate_median_summary.csv`
4. 每个非 baseline profile 的 `aggregate_baseline_compare.csv`
5. 对应 `captures/*/capture-report.md`

如果可以，再补：

1. 各 profile 下 `runs/c128/median_summary.csv`
2. `pidstat.txt`
3. `iostat.txt`
4. 关键日志片段：
   - `put_object_store_inflight_slow`
   - `set_disk_put_object_stage_summary`
   - `SetDisk commit tail is slow`

## 10. 结果判读最小标准

每组 profile 至少需要回答：

1. throughput delta
2. PUT P95 delta
3. PUT P99 delta
4. `set_disk_encode` 是否下降
5. `set_disk_rename` 是否变化
6. RSS 是否仍可控

推荐门槛：

1. 吞吐提升 >= `8%`
2. P95 不劣化超过 `5%`
3. P99 不明显恶化
4. RSS 无失控上涨

## 11. 本轮先不要做的事

1. 不要一上来就跑 `all`，建议先跑 `inflight`
2. 不要手工同时改多组参数再压测
3. 不要在节点数、磁盘数、client host、endpoint mode 同时变化的情况下拿结果做横向对比

优先先把 `baseline -> inflight -> io-buffer` 这两三组结果跑稳，再决定是否继续推进 runtime / duplex，或者直接进入代码优化。
