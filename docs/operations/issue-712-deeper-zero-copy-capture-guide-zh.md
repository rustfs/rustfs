# Issue #712 deeper zero-copy 波动分析 Capture 手册

## 1. 目的

本文给 `houseme/issue-712-deeper-zero-copy` 分支补一套更适合解释波动的 `benchmark + capture` 入口。

适用场景：

1. `v8` / `v9` 这种差异明显但代码没有变化的情况
2. 需要同时采集 bench 结果、系统侧 telemetry、Prometheus text 指标

## 2. 使用前提

运行前请先确认本地 RustFS 已经使用下面的实验开关启动：

```bash
RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST=true
```

本文脚本不会负责启动 RustFS，只负责：

1. 跑普通 PUT 矩阵
2. 同步采集 supporting evidence

## 3. 专用脚本

直接使用：

1. `scripts/run_issue712_deeper_zero_copy_put_with_capture.sh`

默认固定：

1. `sizes=64MiB,128MiB,256MiB`
2. `concurrencies=16`
3. `duration=60s`
4. `rounds=1`
5. `capture-prom-metrics-urls=http://127.0.0.1:8889/metrics`

如果本机没有安装 `awscurl`：

1. 脚本会自动跳过 signed admin metrics 抓取
2. 仍然继续保留 `prom/` 与 `host/` 侧证据
3. 对应 `metrics/admin-metrics.*.ndjson` 中会写入说明文本

## 4. 推荐执行命令

```bash
bash scripts/run_issue712_deeper_zero_copy_put_with_capture.sh \
  --access-key rustfsadmin \
  --secret-key rustfsadmin
```

如果需要显式指定 endpoint：

```bash
bash scripts/run_issue712_deeper_zero_copy_put_with_capture.sh \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin
```

只有在你确认当前 pid 是最新 RustFS 进程时，才建议显式传：

```bash
--capture-rustfs-pid <pid>
```

默认更推荐让脚本自动解析当前 `rustfs server` 进程，避免把过期 pid 带进证据链。

## 5. 输出目录

默认输出目录：

```text
target/bench/issue712-deeper-zero-copy-capture-<timestamp>/
```

重点看：

1. `aggregate_median_summary.csv`
2. `captures/deeper-zero-copy-window/capture-report.md`
3. `captures/deeper-zero-copy-window/host/`
4. `captures/deeper-zero-copy-window/prom/`
5. `captures/deeper-zero-copy-window/metrics/`

## 6. 当前建议重点比对项

当你在分析 `v8` / `v9` 这类波动时，建议优先比对：

### bench 结果

1. throughput
2. avg latency

### host 侧

1. `iostat.txt`
2. `vm_stat`
3. `ps.*.txt`
4. `proc-io.*.txt`

### prom / metrics

1. `rustfs_s3_put_object_path_total`
2. `rustfs_internal_stage_duration_ms_{count,sum}{stage=~"erasure_encode.*"}`
3. `rustfs_s3_put_object_stage_duration_ms_*`

## 7. 当前定位思路

如果两轮代码没有变化，但结果差异很大，优先怀疑：

1. 系统缓存状态不同
2. 内存压力不同
3. I/O 调度或背景负载干扰
4. RustFS 进程的运行窗口内出现额外尖峰

这时候比起继续机械追加同样矩阵，更应该先把 supporting evidence 抓齐。
