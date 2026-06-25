# Issue #712 `zero_copy_eager` plain PUT 验证手册

## 1. 目的

本文用于记录当前 `zero_copy_eager` plain PUT 路径的实际使用方式、验证命令和当前边界。

这条路径当前的目标不是“端到端完全零拷贝”，而是先把 ordinary PUT 从“只有 zero-copy eligibility / metrics”推进到：

1. 真实存在的业务路径
2. 真实命中可观测的 `put_path`
3. 先减少请求体聚合阶段的额外复制

## 2. 当前启用条件

当前 `zero_copy_eager` 只在下面条件同时满足时才会命中：

1. 非加密
2. 非压缩
3. 非 extract 请求
4. 对象大小 `> 1MiB`
5. 对象大小 `<= 32MiB`
6. 请求长度已知
7. 不属于需要特殊处理的 aws-chunked 未知长度场景

## 3. 当前实现边界

这条路径已经是真实业务路径，但当前还不是端到端严格意义上的 zero-copy。

已经做到：

1. 请求体 chunk 以 `Bytes` 形式进入 `zero_copy_eager`
2. 不再先把整个对象拼成一个大 `Vec<u8>` 再进入后续写路径
3. 运行时会记录 `put_path=zero_copy_eager`
4. 会真实记录 `rustfs_zero_copy_write_total`

尚未做到：

1. `HashReader` 之后完全无复制
2. `Erasure::encode` 的 block ingest 完全无复制
3. shard write 全链路严格 zero-copy

因此当前最准确的描述是：

1. 这是 ordinary PUT 的真实 zero-copy eager ingress 路径
2. 不是完整的 end-to-end zero-copy write path

## 4. 推荐验证命令

### 4.1 启动本地单机多盘 RustFS

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
  target/debug/rustfs server \
  /private/tmp/issue708-single-node-multidisk/d1 \
  /private/tmp/issue708-single-node-multidisk/d2 \
  /private/tmp/issue708-single-node-multidisk/d3 \
  /private/tmp/issue708-single-node-multidisk/d4
```

### 4.2 小面 ordinary PUT 验证

```bash
bash scripts/run_put_large_stage_breakdown.sh \
  --endpoint http://127.0.0.1:9000 \
  --access-key rustfsadmin \
  --secret-key rustfsadmin \
  --sizes 16MiB,32MiB \
  --concurrencies 16 \
  --duration 60s \
  --rounds 1 \
  --retry-per-round 1 \
  --retry-sleep-secs 2 \
  --cooldown-secs 15 \
  --out-dir target/bench/issue712-zero-copy-eager-put-verify
```

## 5. 运行时必须核对的指标

### 5.1 PUT path 命中

必须确认：

```promql
rustfs_s3_put_object_path_total{
  path=~"zero_copy_eager|small_eager|streaming|stream_compressed"
}
```

如果看不到 `zero_copy_eager`：

1. 说明本轮没有真正命中这条路径
2. 不能据此评价它的收益

### 5.2 zero-copy write 指标

建议同时看：

```promql
rustfs_zero_copy_write_total
```

```promql
rustfs_zero_copy_write_size_bytes_sum
```

### 5.3 普通 PUT summary

最终固定读取：

1. `aggregate_median_summary.csv`

## 6. 当前已知结果

当前分支上已经验证过一次小面 ordinary PUT：

1. `16MiB, c16`: `318.77 MiB/s`, `851.3ms`
2. `32MiB, c16`: `276.38 MiB/s`, `1846.5ms`

并确认运行时命中了：

1. `put_path=zero_copy_eager`

这说明：

1. 这条路径已经不只是 eligibility / metrics
2. 它已经是真实参与 ordinary PUT 的运行时路径

## 7. 当前最稳妥的使用建议

当前阶段建议把这条路径当作：

1. 实验性 ordinary PUT 优化路径
2. 需要继续压测验证的真实实现

当前不建议把它描述成：

1. 已完成的端到端 zero-copy write path

## 8. 下一步方向

如果要继续把收益往下穿透，下一阶段优先顺序建议是：

1. 先继续验证 `zero_copy_eager` 的普通 PUT 命中率和端到端收益
2. 再评估是否要继续下钻 `Erasure::encode` 的 block ingest
3. `HashReader` 层暂时不是第一优先级
