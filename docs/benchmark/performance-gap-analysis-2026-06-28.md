# GET 小文件优化性能差距根因分析

## 1. 问题概述

### 1.1 预期 vs 实际

| 对象大小 | 预期提升 | 实际 vs main | 实际 vs MinIO |
|----------|----------|-------------|---------------|
| 1KiB | 3-14x | -12.5% (退步) | -88.1% |
| 4KiB | 3-10x | -10.4% (退步) | -80.2% |
| 10KiB | 3-10x | -11.3% (退步) | -87.4% |
| 100KiB | 3-8x | -12.1% (退步) | -78.7% |
| 1MiB | 2-5x | -8.8% (退步) | -71.5% |

**结论**：SF01-SF07+SF05 优化后，小文件性能不仅未超越 main，反而略有退步。

### 1.2 SF05 增量效果

| 对象大小 | SF01-07 | SF01-07+SF05 | 提升 |
|----------|--------:|-------------:|-----:|
| 1KiB | 2.43 | 2.52 | +3.7% |
| 4KiB | 9.73 | 10.12 | +4.0% |
| 10KiB | 19.41 | 25.33 | +30.5% |
| 100KiB | 212.04 | 243.95 | +15.1% |
| 1MiB | 1818.96 | 2070.06 | +13.8% |

SF05 有一定效果，但远未达到预期的 100-200us/请求的提升。

---

## 2. 根因分析

### 2.1 核心问题：Inline 快速路径未被触发

**预测假设**：小文件（1KiB-128KB）会被存储为 inline 数据，走快速路径跳过磁盘 I/O。

**实际情况**：warp 基准测试中，PUT 上传的对象**不一定是 inline 存储的**。

inline 数据的存储条件（在 `put_object` 时决定）：
- 对象大小 <= 8KB（默认 inline 阈值，非 128KB）
- 写入时由 ECStore 决定是否 inline

**关键发现**：
- 128KB 是**读取时** inline 快速路径的最大大小
- 但**写入时** inline 的阈值可能远小于 128KB
- 如果写入时未标记为 inline，读取时 `fi.inline_data()` 返回 false
- 所有 SF02/SF05 的快速路径都不会被触发

**验证方法**：检查 warp 上传的对象是否带有 `x-rustfs-inline-data` metadata key。

### 2.2 核心问题：预测基于热缓存，实际测试是冷读

**预测假设**：Metadata Cache TTL 增加到 2s 后，热点对象会频繁命中缓存。

**实际情况**：
- warp 每轮测试上传 8 个新对象
- 每轮测试持续 10s，冷却 10s
- 对于**新上传的对象**，metadata cache 第一次读取必然是 miss
- warp 的 GET 请求模式是：上传 → 立即读取 → 重复
- 在这种模式下，metadata cache 命中率极低

**结论**：SF03（Cache TTL 增加）在 warp 测试场景下几乎无效。

### 2.3 核心问题：优化的固定开销占比过小

**预测假设**：每次 GET 的固定开销约 723us（1KiB），优化后可降到 50-200us。

**实际情况**：实际固定开销可能远大于预测值，且优化只减少了其中一小部分。

开销分解（估算）：

| 开销项 | 预测值 | 实际值（估算） | SF优化是否触及 |
|--------|--------|---------------|---------------|
| Bucket 验证 | 500-2000us | ~100us (已有缓存) | SF01 ✓ |
| Metadata fanout | 500-2000us | ~200us | SF04 部分 |
| IO Planning (semaphore) | 100-200us | ~50us | SF05 ✓ |
| RwLock 获取 | ~10us | ~10us | 未优化 |
| EC decode setup | ~50us | ~50us | SF02 部分 |
| HTTP 响应构建 | ~100us | ~100us | 未优化 |
| Tokio runtime 调度 | ~50us | ~50us | 未优化 |
| **总计** | ~723us | **~560us** | 优化 ~150us |

**结论**：优化只减少了约 150us / 560us = **27%** 的固定开销。对于 1KiB 对象，总延迟从 ~720us 降到 ~570us，但 throughput 提升被其他因素抵消。

### 2.4 核心问题：SF04 移除 tokio::spawn 可能有负面效果

**预测假设**：`tokio::spawn` 增加 16-32us 开销，移除后可减少延迟。

**实际情况**：
- `tokio::spawn` 将每个磁盘读取作为独立任务调度
- 移除后，所有磁盘读取在同一个 task 中通过 `join_all` 并发执行
- 在高并发（32 并发）下，单个 task 内的 `join_all` 可能导致：
  - 所有磁盘读取共享同一个 task 的调度槽
  - 无法充分利用 tokio 的 work-stealing 调度
  - 在某些情况下反而增加延迟

**结论**：SF04 的优化方向可能有误，需要 benchmark 验证。

### 2.5 核心问题：SF06/SF07 的条件化检查可能引入新开销

**SF06**：`info.user_defined.contains_key("x-amz-expiration")` 检查
- HashMap 查找本身有开销
- 对于大多数对象（没有 expiration tag），这个检查是多余的
- 实际节省的 `resolve_put_object_expiration` 调用可能很快返回

**SF07**：`get_stage_metrics_enabled()` 检查
- 每次请求都要检查一个 `OnceLock<bool>`
- 虽然 `OnceLock::get()` 很快，但在 hot path 上的额外分支有开销
- 如果 metrics 默认禁用，这个检查永远返回 false，是纯开销

---

## 3. 代码质量问题（Review 发现）

### 3.1 CRITICAL: Inline 检测逻辑重复且不一致

**问题**：SF02 在 `set_disk/mod.rs` 检测 inline，SF05 在 `object_usecase.rs` 再次检测，条件不一致。

| 条件 | set_disk/mod.rs | object_usecase.rs |
|------|-----------------|-------------------|
| parts 检查 | `== 1` | `<= 1` |
| remote 检查 | `!is_remote()` | `tier.is_empty()` |
| data 存在 | `fi.data.is_some()` | 未检查 |
| 大小阈值 | `128 * 1024` | `128 * 1024` |

**风险**：两层判断不一致，可能导致：
- 外层跳过 semaphore，内层不走快速路径（无害但浪费逻辑）
- 外层认为是 inline，内层不认为（导致 semaphore 被错误跳过）

### 3.2 HIGH: make_bucket 未失效缓存

**问题**：`BUCKET_VALIDATED_CACHE` 在 `delete_bucket` 时失效，但 `make_bucket` 时不失效。

**风险**：如果 bucket 被删除后立即重建，缓存中仍有旧的验证结果。

### 3.3 MEDIUM: erasure decode 的 `written` 返回值未校验

**问题**：`erasure.decode()` 返回 `(written, err)`，但 `written` 未被检查。

**风险**：如果 decode 静默返回 0 bytes written 且无 error，调用方会得到空数据。

### 3.4 MEDIUM: SF04 移除 tokio::spawn 导致容错降级

**问题**：移除 `tokio::spawn` 后，单个磁盘读取的 panic 会导致整个 `read_version_meta_all` panic。

**风险**：原来 `JoinError` 被捕获并映射为 `DiskError::Unexpected`，现在会直接 unwind。

### 3.5 LOW: SF06 可能禁用了大部分对象的 lifecycle 检查

**问题**：`info.user_defined.contains_key("x-amz-expiration")` 假设没有这个 key 就不需要 lifecycle 检查。

**风险**：大多数对象没有预设的 expiration tag，但可能匹配 lifecycle 规则。

---

## 4. 为什么性能退步

### 4.1 优化未触及真正瓶颈

真正的瓶颈不在预测的 5 项开销中，而在：

1. **Tokio runtime 调度开销**：32 并发下的 task 切换和调度
2. **RwLock 争用**：metadata fanout 的 RwLock 在高并发下争用严重
3. **HTTP 连接管理**：warp 的连接池和 HTTP 解析开销
4. **系统调用开销**：即使 inline，仍需系统调用来发送 HTTP 响应

### 4.2 优化引入了新开销

1. **moka cache 查找**：每次 GET 都要查 moka cache（比 HashMap 慢）
2. **条件化检查**：SF06/SF07 的条件检查本身有开销
3. **inline 检测**：两层 inline 检测增加了分支和比较
4. **OnceLock 查找**：多次 `OnceLock::get()` 在 hot path 上

### 4.3 优化改变了执行路径的特性

1. **SF04 移除 tokio::spawn**：改变了并发调度特性
2. **SF05 重排执行顺序**：改变了锁获取顺序，可能影响调度
3. **SF03 Cache TTL 增加**：增加了缓存一致性风险

---

## 5. 结论与建议

### 5.1 优化策略需要调整

当前的优化策略是"逐个减少固定开销"，但：
- 每个优化只减少 10-50us
- 优化本身引入了 5-20us 的新开销
- 净效果很小甚至为负

**正确策略**：找到**真正的瓶颈**（profiling），然后针对性优化。

### 5.2 建议的下一步

1. **Profiling**：使用 `perf` 或 `flamegraph` 对 GET 路径进行 profiling
2. **对比 MinIO**：分析 MinIO 在小文件上的优势来源
3. **简化优化**：移除效果不明显的优化（SF06/SF07），保留有效的（SF02/SF05）
4. **统一 inline 检测**：消除重复逻辑，使用单一判断函数
5. **恢复容错**：恢复 `read.rs` 中的 `JoinError` 处理

### 5.3 保留的优化

| 优化项 | 保留? | 原因 |
|--------|-------|------|
| SF01: Bucket 缓存 | ✅ | 有效减少 stat_volume 调用 |
| SF02: Inline 快速路径 | ✅ | 对真正 inline 的对象有效 |
| SF03: Cache TTL | ⚠️ | 需要验证，可能需要降低 |
| SF04: 移除 spawn | ❌ | 可能有负面效果，需恢复容错 |
| SF05: 跳过 IO Planning | ✅ | 对 inline 对象有效 |
| SF06: 条件化 Lifecycle | ❌ | 效果不明显，可能有 bug |
| SF07: 条件化 Metrics | ❌ | 效果不明显，引入新开销 |
