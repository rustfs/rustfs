# Issue #712 multipart PUT server-path 静默验证 Runbook

## 1. 目的

本文给 `#712` 第二批工作提供一个只关注 multipart server path 的静默验证 runbook。

目标：

1. 不再扩散客户端参数矩阵
2. 固定当前推荐 baseline
3. 把注意力集中到 server-path 观测增强后的阶段结果

## 2. 固定 baseline

当前固定 baseline：

1. `1GiB -> 64MiB part / pc4`
2. `2GiB -> 128MiB part / pc4`

可选补充：

1. `2GiB -> 256MiB part / pc4`

但默认不作为首选 baseline。

## 3. 推荐脚本

直接使用：

1. `scripts/run_gt1g_multipart_put_server_path_focus.sh`

该脚本默认只跑：

1. `1g-64m-pc4`
2. `2g-128m-pc4`

可选补充：

1. `2g-256m-pc4`

## 4. 静默执行命令

### 4.1 默认两组

```bash
bash scripts/run_gt1g_multipart_put_server_path_focus.sh \
  --host 127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket-prefix issue712-multipart-focus \
  --duration 10m \
  --out-dir target/bench/issue712-multipart-server-path-focus
```

### 4.2 加上 `2g-256m-pc4`

```bash
bash scripts/run_gt1g_multipart_put_server_path_focus.sh \
  --host 127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --bucket-prefix issue712-multipart-focus \
  --duration 10m \
  --profiles 1g-64m-pc4,2g-128m-pc4,2g-256m-pc4 \
  --out-dir target/bench/issue712-multipart-server-path-focus-wide
```

## 5. 强制要求

这轮 runbook 的要求是：

1. 静默跑
2. 只读 `summary.csv`
3. 如需解释异常，再去看 dashboard / 日志

## 6. 结果目录

建议统一：

```text
target/bench/
  issue712-multipart-server-path-focus/
    run_manifest.txt
    commands.txt
    summary.csv
    logs/
    benchdata/
```

## 7. 需要记录的指标

最终结果表之外，强制记录以下阶段：

1. `multipart_ingress_prepare`
2. `multipart_set_disk_writer_setup`
3. `multipart_set_disk_encode`
4. `multipart_complete_tail`

同时建议固定记录 multipart path 计数：

1. `multipart_write_pipeline`
2. `multipart_write_pipeline_batched_large`
3. `multipart_write_single_block_non_inline`

## 8. 本轮的判断顺序

先看：

1. `summary.csv`

再看：

1. `multipart_complete_tail`
2. `multipart_set_disk_encode`
3. `multipart_set_disk_writer_setup`
4. `multipart_ingress_prepare`

## 9. 结果解释

### 9.1 `summary.csv` 先分出好坏组合

先回答：

1. `1GiB / 64MiB / pc4` 是否仍是最稳 baseline
2. `2GiB / 128MiB / pc4` 是否仍是最稳 baseline

### 9.2 再用 Dashboard 回答热点层

再回答：

1. `multipart_complete_tail` 是否最高
2. `multipart_set_disk_encode` 是否主导
3. `multipart_set_disk_writer_setup` 是否异常高
4. `multipart_ingress_prepare` 是否已经被 body buffering 放大

## 10. 下一步动作判定

### 如果 `multipart_set_disk_encode` 最高

下一步优先：

1. multipart part 专用 batching gate
2. multipart encode path 单独策略

### 如果 `multipart_complete_tail` 最高

下一步优先：

1. complete path metadata / checksum / rename tail 优化

### 如果 `multipart_set_disk_writer_setup` 最高

下一步优先：

1. writer init / bitrot writer path 优化

### 如果 `multipart_ingress_prepare` 最高

下一步优先：

1. part ingress buffer 分层
2. body read / HashReader 前的缓冲调整

## 11. `multipart_*` 指标为空时的排查顺序

如果本轮跑的是 multipart PUT，但 Prometheus 里查不到任何 `multipart_*` stage：

1. 先不要直接判定“新打点无效”
2. 先查当前 `rustfs_s3_put_object_stage_duration_ms` 里到底有哪些 stage
3. 如果只看到了 ordinary PUT 的 `ingress_prepare` / `set_disk_writer_setup` / `set_disk_encode` / `set_disk_rename`，要优先怀疑当前 `127.0.0.1:9000` 上跑的不是预期的新二进制

推荐先查：

```promql
topk(
  40,
  count by (__name__, stage) (
    {__name__=~"rustfs_s3_put_object_stage_duration_ms.*"}
  )
)
```

如果结果里只有 ordinary stage，而没有：

1. `multipart_ingress_prepare`
2. `multipart_set_disk_writer_setup`
3. `multipart_set_disk_encode`
4. `multipart_complete_tail`

则应优先检查：

1. 本轮 RustFS 进程是否确实来自当前 worktree 的 `target/debug/rustfs`
2. 重启脚本是否真的清掉了旧进程
3. `RUSTFS_OBS_ENDPOINT` 是否仍然指向当前可查询 backend

## 12. 已确认的假阴性根因样例

在 `2026-06-24` 的第三批继续验证中，出现过一次典型假阴性：

1. multipart benchmark 已经成功跑完
2. Prometheus 里却只有 ordinary PUT stage，没有任何 `multipart_*`
3. 根因并不是打点代码失效，而是 `127.0.0.1:9000` 上仍然挂着更早启动的旧 RustFS 进程

纠偏方式：

1. 用当前 worktree 的 `target/debug/rustfs` 前台直接拉起服务
2. 再跑最小化 focused smoke
3. 立刻查询 `multipart_*` stage

这次纠偏后的前台复测已经确认：

1. `multipart_*` 四个阶段可以正常上报
2. 当前热点仍然稳定落在 `multipart_set_disk_encode`
