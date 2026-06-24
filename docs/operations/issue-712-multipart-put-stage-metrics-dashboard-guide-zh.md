# Issue #712 multipart PUT 分阶段指标 Dashboard / PromQL 指南

## 1. 目的

本文给 `#712` 的第一批 server-path 观测增强配套一份可执行的 Dashboard / PromQL 指南。

本批次新增的 multipart 阶段指标依然复用现有指标名：

1. `rustfs_s3_put_object_stage_duration_ms`

但新增了四个 stage label：

1. `multipart_ingress_prepare`
2. `multipart_set_disk_writer_setup`
3. `multipart_set_disk_encode`
4. `multipart_complete_tail`

## 2. 使用前提

这些阶段指标严格受全局开关控制：

1. `rustfs_io_metrics::put_stage_metrics_enabled() == true`

如果该开关没有开启：

1. 不会上报这些阶段指标
2. 也不会额外做阶段计时

## 3. 推荐直接复用现有 Grafana Row

当前 Dashboard 中已经有：

1. `Large PUT Stage Breakdown`

这意味着：

1. 不需要重新设计一套全新 row
2. 只需要在现有 row / stage 变量里选新的 multipart stage label 即可

## 4. 推荐 PromQL

### 4.1 multipart 阶段 P95

```promql
histogram_quantile(
  0.95,
  sum by (stage, le) (
    rate(
      rustfs_s3_put_object_stage_duration_ms_bucket{
        job=~"$job",
        stage=~"multipart_.*"
      }[$__rate_interval]
    )
  )
)
```

### 4.2 multipart 阶段 P99

```promql
histogram_quantile(
  0.99,
  sum by (stage, le) (
    rate(
      rustfs_s3_put_object_stage_duration_ms_bucket{
        job=~"$job",
        stage=~"multipart_.*"
      }[$__rate_interval]
    )
  )
)
```

### 4.3 单实例 multipart 阶段 P95

```promql
histogram_quantile(
  0.95,
  sum by (instance, stage, le) (
    rate(
      rustfs_s3_put_object_stage_duration_ms_bucket{
        job=~"$job",
        instance=~"$instance",
        stage=~"multipart_.*"
      }[$__rate_interval]
    )
  )
)
```

### 4.4 multipart 与 ordinary PUT encode 对比

```promql
histogram_quantile(
  0.95,
  sum by (stage, le) (
    rate(
      rustfs_s3_put_object_stage_duration_ms_bucket{
        job=~"$job",
        stage=~"set_disk_encode|multipart_set_disk_encode"
      }[$__rate_interval]
    )
  )
)
```

### 4.5 multipart complete tail 重点盯盘

```promql
histogram_quantile(
  0.95,
  sum by (instance, le) (
    rate(
      rustfs_s3_put_object_stage_duration_ms_bucket{
        job=~"$job",
        stage="multipart_complete_tail",
        instance=~"$instance"
      }[$__rate_interval]
    )
  )
)
```

## 5. 推荐看板顺序

当你在看 `>1GiB multipart PUT` 时，建议按下面顺序看：

1. `multipart_ingress_prepare`
2. `multipart_set_disk_writer_setup`
3. `multipart_set_disk_encode`
4. `multipart_complete_tail`

解释顺序：

1. 如果 ingress 先高，先看 part ingress buffer / request-body handling
2. 如果 writer setup 高，先看 bitrot writer / disk availability / shard_file_size path
3. 如果 encode 高，先看 multipart 是否需要独立 encode strategy
4. 如果 complete tail 高，优先看 `complete_multipart_upload()` 的 metadata / checksum / rename tail

## 6. 推荐结合看的辅助指标

建议和上面四个阶段一起看：

1. `rustfs_io_put_object_concurrent_requests`
2. `rustfs_ec_encode_inflight_bytes_current`
3. host CPU
4. per-instance disk write throughput
5. readiness / write quorum 异常计数

## 7. 典型解释模板

### 7.1 ingress 高

可能原因：

1. part body stream buffering 不合适
2. `part.size` 与 ingress buffer 不匹配

### 7.2 writer setup 高

可能原因：

1. bitrot writer 构建成本偏高
2. online disk / writer init 慢
3. shard_file_size 相关路径有额外成本

### 7.3 encode 高

可能原因：

1. multipart part 仍然借用了 ordinary PUT encode 行为
2. `part.size` 太大，单 part encode CPU 时间过长
3. batching / inflight 参数不合适

### 7.4 complete tail 高

可能原因：

1. complete 阶段 part metadata 处理放大
2. checksum combine 成本高
3. rename / cleanup / commit tail 成本高

## 8. 建议的截图 / 归档内容

每次 `>1GiB multipart PUT` 复测，建议固定归档：

1. multipart stage P95 截图
2. multipart stage P99 截图
3. `multipart_complete_tail` 单实例截图
4. CPU / disk write 辅助图

## 9. 当前阶段建议

下一次进入 `#712` 继续推进时：

1. 先开 `put_stage_metrics_enabled`
2. 先跑推荐 baseline：
   - `1GiB -> 64MiB / pc4`
   - `2GiB -> 128MiB / pc4`
3. 先看 `multipart_complete_tail` 是否明显高于其他阶段
4. 再决定是先改 ingress / encode / writer setup / complete tail
