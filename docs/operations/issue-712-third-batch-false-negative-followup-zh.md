# Issue #712 第三批继续验证结果总结

## 1. 背景

`#712` 第三批的第一轮静默 baseline 已经确认：

1. `multipart_set_disk_encode` 是当前最主要热点
2. `multipart_complete_tail` 可见但不是第一瓶颈

在继续推进 multipart encode 优化线时，又出现了一次“指标空结果”的假阴性，需要单独记录，避免后续团队误判为打点无效。

## 2. 假阴性根因

现象：

1. multipart benchmark 成功完成
2. `summary.csv` 正常生成
3. Prometheus 中却只出现 ordinary PUT 的 stage：
   - `ingress_prepare`
   - `set_disk_writer_setup`
   - `set_disk_encode`
   - `set_disk_rename`
4. 没有任何 `multipart_*` stage

根因：

1. 本轮 benchmark 实际命中了 `127.0.0.1:9000` 上残留的旧 RustFS 进程
2. 该旧进程不是当前 worktree 的新二进制
3. 因此即使 benchmark 是 multipart PUT，也不会产出本批新增的 multipart stage 标签

这次问题的本质不是“新打点代码无效”，而是“验证目标进程不对”。

## 3. 如何识别这类假阴性

推荐直接查询：

```promql
topk(
  40,
  count by (__name__, stage) (
    {__name__=~"rustfs_s3_put_object_stage_duration_ms.*"}
  )
)
```

如果你跑的是 multipart PUT，但结果里只有 ordinary PUT stage，而没有：

1. `multipart_ingress_prepare`
2. `multipart_set_disk_writer_setup`
3. `multipart_set_disk_encode`
4. `multipart_complete_tail`

则优先判断为“验证目标进程可能不对”，而不要先判断为“metrics 打点无效”。

## 4. 前台纠偏复测

为了排除旧进程干扰，本轮直接使用当前 worktree 的 `target/debug/rustfs` 前台拉起服务，再做最小化 focused smoke。

复测 profile：

1. `2g-128m-pc4`

复测结果：

1. Throughput: `373.78 MiB/s`
2. Request rate: `2.92 obj/s`
3. Avg latency: `5471.4ms`

结果目录：

1. `target/bench/issue712-multipart-server-path-foreground-smoke/summary.csv`

## 5. 前台复测阶段指标

Prometheus 近窗口查询已经确认 multipart 四阶段都正常出现。

P95：

1. `multipart_ingress_prepare`: `4.75ms`
2. `multipart_set_disk_writer_setup`: `4.86ms`
3. `multipart_set_disk_encode`: `7375ms`
4. `multipart_complete_tail`: `13ms`

同时还能看到 ordinary PUT 的阶段指标，但这不影响判断；关键是：

1. multipart stage 已经实际落盘到 TSDB
2. encode 依旧是最显著热点

## 6. 当前直接结论

这轮继续验证后的结论没有变化，反而更稳：

1. `multipart_set_disk_encode` 仍然是 `>1GiB multipart PUT` 当前最主要优化目标
2. `multipart_complete_tail` 仍然是次级问题，不应抢在 encode 之前
3. `multipart_ingress_prepare` 和 `multipart_set_disk_writer_setup` 暂时不是第一优先级
4. 后续优化应继续围绕 multipart encode path，而不是先转去 complete tail / ingress

## 7. 对后续验证的要求

后续再做 multipart server-path 对照验证时，建议强制执行：

1. 先确认服务进程确实来自当前 worktree 二进制
2. 先确认 `multipart_*` stage 能被 Prometheus 查询到
3. 再读取 `summary.csv`
4. 最后再判断 encode 优化是否真的有效

否则很容易再次出现“summary 正常，但阶段指标是旧进程数据”的假阴性。
