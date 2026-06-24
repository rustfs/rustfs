# Issue #712 本地可查询 metrics backend 启动手册

## 1. 目的

本文用于打通 `#712` 第三批验证所需的本地可查询 metrics backend。

当前问题不是 multipart stage 指标没有打点，而是本地环境里没有可查询后端：

1. `rustfs/admin/v3/metrics` 可用，但返回的是管理侧 JSON 快照，不包含 `metrics` crate 的 histogram 指标
2. 本地默认没有 Prometheus / 可查询 metrics endpoint 在监听
3. 因此无法直接查询：
   - `multipart_ingress_prepare`
   - `multipart_set_disk_writer_setup`
   - `multipart_set_disk_encode`
   - `multipart_complete_tail`

本文的目标就是把这条链打通。

## 2. 推荐方案

推荐直接复用仓库内已有的 observability stack：

1. `.docker/observability/docker-compose.yml`

这套 stack 的优点：

1. 已经是仓库现成维护的方案
2. 包含 OTEL Collector、Prometheus、Grafana
3. 可以直接承接 RustFS 的 `RUSTFS_OBS_ENDPOINT`

## 3. 核心链路

启动后，链路应该是：

1. RustFS
   - `RUSTFS_OBS_ENDPOINT=http://host.docker.internal:4318`
2. OTEL Collector
   - 接收 OTLP/HTTP
3. Prometheus
   - 抓取 collector 暴露的 metrics
4. 查询
   - `http://127.0.0.1:9090`

## 4. 启动步骤

### 4.1 启动 observability stack

在仓库根目录执行：

```bash
cd .docker/observability
docker compose up -d
```

### 4.2 等待组件就绪

建议检查：

```bash
curl -fsS http://127.0.0.1:9090/-/ready
curl -fsS http://127.0.0.1:3000/api/health
```

若需要查看容器：

```bash
docker compose ps
```

### 4.3 启动 RustFS 时显式指向 OTEL Collector

本地单机多盘场景建议：

```bash
export RUSTFS_OBS_ENDPOINT=http://host.docker.internal:4318
```

如果使用我们现成的本地重启脚本：

```bash
bash scripts/restart_local_single_node_multidisk_rustfs.sh
```

请确保脚本最终生效的环境里包含：

```bash
RUSTFS_OBS_ENDPOINT=http://host.docker.internal:4318
```

## 5. 最小查询验证

### 5.1 确认 Prometheus 能查询到 RustFS 指标

```bash
curl -fsS 'http://127.0.0.1:9090/api/v1/query?query=rustfs_s3_put_object_total'
```

### 5.2 查询 stage 指标是否存在

```bash
curl -fsS 'http://127.0.0.1:9090/api/v1/label/stage/values'
```

如果链路打通，返回结果中应能看到：

1. `multipart_ingress_prepare`
2. `multipart_set_disk_writer_setup`
3. `multipart_set_disk_encode`
4. `multipart_complete_tail`

### 5.3 直接查询 P95

```bash
curl -fsS 'http://127.0.0.1:9090/api/v1/query?query=histogram_quantile(0.95,sum by(stage,le)(rate(rustfs_s3_put_object_stage_duration_ms_bucket{stage=~"multipart_.*"}[5m])))'
```

## 6. 建议的静默验证顺序

打通 backend 后，建议按如下顺序做第三批：

1. 启动 observability stack
2. 重启 RustFS 并确认 `RUSTFS_OBS_ENDPOINT` 生效
3. 先跑一轮 multipart baseline：
   - `1g-64m-pc4`
   - `2g-128m-pc4`
4. 不看过程日志，只保留：
   - `summary.csv`
   - Prometheus 查询结果
5. 最后整理：
   - throughput / reqps / avg latency
   - 4 个 multipart stage 的 P95 / P99

## 7. 推荐查询集合

### 7.1 multipart 阶段 P95

```promql
histogram_quantile(
  0.95,
  sum by (stage, le) (
    rate(rustfs_s3_put_object_stage_duration_ms_bucket{stage=~"multipart_.*"}[5m])
  )
)
```

### 7.2 multipart 阶段 P99

```promql
histogram_quantile(
  0.99,
  sum by (stage, le) (
    rate(rustfs_s3_put_object_stage_duration_ms_bucket{stage=~"multipart_.*"}[5m])
  )
)
```

### 7.3 complete tail 单独看

```promql
histogram_quantile(
  0.95,
  sum by (instance, le) (
    rate(rustfs_s3_put_object_stage_duration_ms_bucket{stage="multipart_complete_tail"}[5m])
  )
)
```

### 7.4 encode 单独看

```promql
histogram_quantile(
  0.95,
  sum by (instance, le) (
    rate(rustfs_s3_put_object_stage_duration_ms_bucket{stage="multipart_set_disk_encode"}[5m])
  )
)
```

## 8. 结果目录建议

推荐目录：

```text
target/bench/
  issue712-multipart-server-path-focus/
    summary.csv
    metrics-query.txt
    promql/
      multipart-stage-p95.txt
      multipart-stage-p99.txt
      multipart-complete-tail-p95.txt
      multipart-encode-p95.txt
```

## 9. 失败排查

### 9.1 Prometheus 起不来

检查：

```bash
cd .docker/observability
docker compose logs prometheus
```

### 9.2 RustFS 没有上报

检查：

1. `RUSTFS_OBS_ENDPOINT` 是否真的是 `http://host.docker.internal:4318`
2. OTEL collector 是否运行
3. RustFS 重启后是否带上了新环境变量

### 9.3 查不到 stage label

检查：

1. 是否真的跑过 multipart PUT 请求
2. `put_stage_metrics_enabled()` 是否在运行期被开启
3. 查询窗口是否太短

## 10. 建议

下次推进 `#712` 第三批时，不要先跑 benchmark，再临时找 metrics。

正确顺序应是：

1. 先按本文把可查询 backend 起好
2. 再跑 baseline
3. 最后只读：
   - `summary.csv`
   - Prometheus stage 查询结果
