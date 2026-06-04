# issue-658 DeleteObjects lock_batch 检测清单

本文用于定位以下现象的第一现场：

- `warp: <ERROR> Io error: Remote lock RPC timed out`
- `Lock acquisition timeout for resource ... after 5s`
- `remote lock RPC lock_batch ... (+997 more)`

目标是确认问题是否由 `DeleteObjects` 批量删除路径触发，以及具体是：

- 批次过大
- 对象过于集中
- 某些 locker 节点处理变慢
- 还是对象本身存在真实锁竞争

## 1. 开启诊断

在复现环境为 RustFS 进程增加：

```bash
RUSTFS_ISSUE3031_DIAG_ENABLE=true
```

该开关会打开本次补充的诊断日志，但不会改变实际删除行为。

## 2. 必抓日志关键字

复现时请重点保留以下日志：

```text
issue3031_delete_objects_lock_batch_context
issue3031_delete_objects_dist_batch_lock_summary
Remote lock RPC timed out
lock_batch
Lock acquisition timeout
Evicting cached remote lock connection after RPC failure
Evicted stale connection from cache
```

如果有后续级联，也请保留：

```text
list_path_raw: revjob err
Remote disk operation returned a network-like error
Detected missing shards during read, triggering background heal
Step 1: Checking object existence and metadata
```

## 3. 建议抓取时间窗口

以首次出现以下日志为中心：

```text
Remote lock RPC timed out
```

至少保留：

- 前 2 分钟
- 后 2 分钟

如果是高并发压测环境，建议保留前后 5 分钟。

## 4. 如何判读新增诊断日志

### 4.1 看 `issue3031_delete_objects_lock_batch_context`

重点字段：

- `bucket`
- `requested_object_count`
- `unique_lock_count`
- `locked_object_count`
- `failed_lock_count`
- `dist_erasure`
- `dist_lock_id_count`
- `failed_objects`

判读方式：

- 如果 `requested_object_count` 很大，接近 1000，说明这次就是典型的大批量 `DeleteObjects`
- 如果 `unique_lock_count` 接近 `requested_object_count`，说明对象去重后仍然是大批量锁
- 如果 `failed_lock_count > 0`，说明这批对象中已经有部分对象在分布式批量加锁阶段失败

### 4.2 看 `issue3031_delete_objects_dist_batch_lock_summary`

重点字段：

- `request_count`
- `locker_count`
- `write_quorum`
- `succeeded_count`
- `failed_count`
- `pending_count`
- `pending_clients`
- `errors_by_object`

判读方式：

- 如果 `request_count` 很大，同时 `failed_count > 0`，说明不是单对象问题，而是整批锁请求出现拥塞或竞争
- 如果 `pending_clients` 长时间不为 0，说明 locker client 返回很慢，批量锁 RPC 可能被卡住
- 如果 `errors_by_object` 里大量出现 `Remote lock RPC timed out` 或 client batch request failed，说明瓶颈更偏向 locker RPC 处理层

## 5. 与原始日志的对照关系

### 5.1 看到这类日志

```text
Remote lock RPC timed out ... op="lock_batch" ... (+997 more)
```

说明：

- 这不是普通单对象锁
- 是一批接近 1000 个对象的批量写锁请求

### 5.2 看到这类日志

```text
Lock acquisition timeout for resource ... after 5s
```

说明：

- 这是业务层总预算超时
- 内部往往已经经历过若干次 1s 级的 remote lock RPC 超时或重试

### 5.3 看到这些日志组合

```text
Remote lock RPC timed out
Evicting cached remote lock connection after RPC failure
Evicted stale connection from cache
```

说明：

- locker RPC 不只是慢
- 还触发了连接驱逐和重建
- 系统已经进入高压或失败恢复态

## 6. 一眼判定规则

如果同一个时间窗口里同时满足：

1. `issue3031_delete_objects_lock_batch_context`
   - `requested_object_count` 很大
   - `unique_lock_count` 很大
2. `issue3031_delete_objects_dist_batch_lock_summary`
   - `failed_count > 0`
   - `pending_clients` 明显存在
3. 原始日志出现
   - `Remote lock RPC timed out ... op="lock_batch"`
   - `Lock acquisition timeout ... after 5s`

那么优先判断为：

“大批量 DeleteObjects 在分布式批量加锁阶段触发了锁服务拥塞或热点竞争，而不是单纯网络抖动。”

## 7. 发给测试同学的话术模板

可直接转发：

```text
请在复现环境为 RustFS 增加环境变量：

RUSTFS_ISSUE3031_DIAG_ENABLE=true

然后按原来的压测/复现方式继续执行，并保留首次出现
"Remote lock RPC timed out"
前后至少 2 分钟完整日志。

请重点返回以下关键日志：

1. issue3031_delete_objects_lock_batch_context
2. issue3031_delete_objects_dist_batch_lock_summary
3. Remote lock RPC timed out
4. lock_batch
5. Lock acquisition timeout
6. Evicting cached remote lock connection after RPC failure
7. Evicted stale connection from cache

如果日志里同时能看到：
- requested_object_count 很大
- unique_lock_count 很大
- failed_count > 0
- pending_clients 存在
- 以及原始的 lock_batch timeout / 5s lock acquisition timeout

就优先说明问题发生在 DeleteObjects 的分布式批量加锁阶段，而不是普通单对象操作。
```

## 8. 备注

本清单的目标不是直接给出修复方案，而是先确认：

- 谁在发起 lock_batch
- 这批请求规模多大
- 是热点对象竞争还是 locker RPC 层拥塞
- 以及后续是否出现了连接驱逐与级联影响
