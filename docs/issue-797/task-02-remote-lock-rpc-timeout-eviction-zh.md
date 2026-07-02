# Task 02: unify remote lock RPC timeout and eviction

## 目标

统一 remote lock 所有 RPC 的 timeout、日志、connection eviction 行为，避免坏连接或远端节点异常时，只有 lock/lock_batch 能快速失败，而 release/refresh/force-release 等路径仍可能挂住或不驱逐 stale channel。

## 代码落点

- `crates/ecstore/src/cluster/rpc/remote_locker.rs`
  - `RemoteClient::execute_rpc`
  - `lock`
  - `lock_batch`
  - `release`
  - `release_locks_batch`
  - `refresh`
  - `force_release`
  - `check_status`
- `crates/config/src/constants/object.rs`
  - object lock RPC timeout 默认值
- `crates/protos/src/lib.rs`
  - `evict_failed_connection_with_log_level`

## 当前问题

remote lock 已有 `execute_rpc`，它能提供：

- `tokio::time::timeout`
- 统一日志
- scanner leader lock 降噪
- tonic error 后 evict cached channel
- timeout 后 evict cached channel

但部分 RPC 没有走该包装，导致不同 lock 操作在坏连接上的表现不一致。多机多盘高并发场景下，lock release/refresh 如果不能快速失败，会放大尾延迟和锁争用。

## 实施步骤

1. 梳理 `RemoteClient` 所有 RPC 方法。
2. 将以下方法统一改为 `execute_rpc`：
   - `release`
   - `release_locks_batch`
   - `refresh`
   - `force_release`
   - `check_status`
3. 保持 existing response shaping：
   - unlock 类失败仍要返回可解释的 `LockResponse`。
   - batch release 需要逐项保留失败结果。
4. 保留 scanner leader lock 降噪逻辑。
5. 对 timeout 文案、tonic code、resource summary 做统一结构化日志。
6. 确保 timeout 发生后调用 `evict_failed_connection_with_log_level`。

## 测试方案

新增或扩展 `remote_locker` 测试：

- lock timeout 会 evict。
- release timeout 会 evict。
- refresh tonic unavailable 会 evict。
- release_locks_batch 部分失败时结果长度与请求长度一致。
- scanner leader lock 使用 debug 级别，不产生高噪声 warn。

建议命令：

```bash
cargo test -p rustfs-ecstore remote_locker -- --nocapture
cargo test -p rustfs-lock remote_lock -- --nocapture
```

## 故障注入验证

多节点环境：

1. 启动 4 节点集群。
2. 运行高并发 PUT/DELETE/GET mixed workload。
3. kill 一个远端节点。
4. 观察 lock timeout、connection eviction、recovery 后 stale channel 是否刷新。

指标关注：

- remote lock RPC timeout count。
- connection eviction count。
- lock acquire latency p95/p99。
- stale channel retry success。

## 验收标准

- remote lock 所有 RPC 均有 deadline。
- timeout/tonic error 后 stale channel 被驱逐。
- release/refresh 不再在坏连接上长时间挂住。
- high contention 场景下 lock p99 不劣化。
- focused tests 通过。
