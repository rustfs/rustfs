# Task 09: adaptive batch processor observe-only design

## 目标

为 `AsyncBatchProcessor` 的动态并发调节建立 observe-only 设计，先采集延迟、成功率、队列长度、timeout 与 downstream pressure，再决定是否进入 opt-in。该任务不应第一批落地为默认行为。

## 代码落点

- `crates/ecstore/src/services/batch_processor.rs`
  - `AsyncBatchProcessor`
  - `GlobalBatchProcessors`
- `crates/ecstore/src/set_disk/read.rs`
  - metadata/read fanout 调用点
- `crates/ecstore/src/cluster/rpc/remote_disk.rs`
  - remote disk RPC timeout 和错误分类

## 当前状态

`GlobalBatchProcessors` 使用固定并发：

- read: 16
- write: 8
- metadata: 12

固定并发简单稳定，但在多机多盘高并发下可能出现：

- 远端节点压力过高。
- metadata fanout 尾延迟扩大。
- 一刀切并发不适合不同集群规模。

直接动态调参风险也很大：

- 并发振荡。
- benchmark 不可复现。
- p50 提升但 p99 恶化。
- timeout 风暴中继续错误增大并发。

## observe-only 实施步骤

1. 增加 batch observation：
   - batch size。
   - configured concurrency。
   - queue wait。
   - execution latency。
   - success/error/timeout count。
   - downstream operation kind。
2. 不改变当前 semaphore 并发。
3. 计算建议值但只记录：
   - suggested_concurrency。
   - reason: improving/degrading/stable。
4. 设计 future opt-in 策略：
   - AIMD-like。
   - min/max clamp。
   - cool-down window。
   - per operation class 独立调节。
5. 明确禁止：
   - 在 read quorum error 上盲目增大并发。
   - 在远端节点异常时放大请求。
   - 默认启用 adaptive。

## 测试方案

单测：

- observe-only 不改变并发。
- success latency 下降时 suggested 增大。
- timeout/error 上升时 suggested 降低。
- min/max clamp 生效。
- cool-down 防止快速抖动。

建议命令：

```bash
cargo test -p rustfs-ecstore batch_processor -- --nocapture
cargo test -p rustfs-io-metrics batch_processor -- --nocapture
```

## 性能验证

只采集 observe-only：

- baseline fixed concurrency。
- observe-only suggested concurrency。
- workload: GET/PUT/mixed。
- concurrency: 32/64/128/256。
- fault injection: slow remote node, timeout, node restart。

关注：

- suggested 值是否稳定。
- p99 与 suggested 的关系。
- error rate 与 suggested 的关系。
- 是否出现振荡。

## 验收标准

- 第一阶段不改变行为。
- metrics 能解释为什么建议提高或降低并发。
- 没有高 cardinality label。
- 后续 opt-in 方案有明确 min/max/cooldown/error guard。
