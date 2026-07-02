# Issue 797 origin/main refresh review

## 复核目标

按用户要求，从远程 `main` 分支切出全新分支后，重新核对 issue #797 相关关键路径，并判断是否需要更正或更新已拆分的任务文档。

## 分支与基线

- 原工作分支：`houseme/mimalloc-allocator-pr`
- 新分支：`houseme/issue-797-internode-plan-refresh`
- 新分支基线：`origin/main@70e1d79df`
- 文档目录：`docs/issue-797/`
- 文档 Git 状态：被 `.gitignore:51` 的 `docs/*` 忽略，属于本地文档交付。

## 复核结论

无需更正 9 个子任务的拆分、优先级和执行边界。需要补充的是：这些任务已经在最新 `origin/main` 上重新核对过，关键事实仍然成立。

## 关键路径复核结果

### 1. gRPC channel/cache/keepalive

结论：文档判断仍成立。

`create_new_channel` 已经使用 env-configurable timeout 和 keepalive，并在创建成功后缓存 channel：

- `crates/protos/src/lib.rs`
  - `internode_connect_timeout`
  - `internode_tcp_keepalive`
  - `internode_http2_keep_alive_interval`
  - `internode_http2_keep_alive_timeout`
  - `internode_rpc_timeout`
  - `create_new_channel`
  - `evict_failed_connection_with_log_level`
- `crates/ecstore/src/cluster/rpc/client.rs`
  - `node_service_time_out_client`

因此仍不建议新增一套平行 `GrpcChannelPool`。后续优化应集中在 client-side HTTP/2 window、metrics、eviction 覆盖面，而不是重造连接池。

### 2. HTTP data-plane writer

结论：Task 01 仍是最高优先级 correctness prerequisite。

`HttpWriter::new` 仍在后台 task 中执行 `request.send().await`，`poll_shutdown` 仍只区分 `Poll::Ready(Ok(_))` 和 `Poll::Ready(Err(_))`，没有区分 `Ok(Ok(()))` 与 `Ok(Err(err))`。

风险仍成立：

- 远端非 2xx 或连接错误可能在 shutdown 阶段被吞掉。
- `open_write` 的 retry/fault marking 语义不完整。
- 大流 PUT 压测中可能出现错误不可见。

### 3. remote lock RPC timeout/eviction

结论：Task 02 仍成立。

`execute_rpc` 已覆盖 `lock_batch`，但以下路径仍直接调用 tonic RPC，没有统一 timeout + eviction：

- `release`
- `release_locks_batch`
- `refresh`
- `force_release`
- `check_status`

这会导致坏连接或远端节点异常时，lock release/refresh/check_status 与 lock acquire 的失败语义不一致。

### 4. ReadMultiple serialization/decoding

结论：Task 03 仍成立。

`remote_disk.rs::read_multiple` 仍然：

- 同时发送 JSON 和 msgpack request。
- 对 `read_multiple_resps_bin` 使用 `filter_map(...ok())`。
- 对 JSON fallback 也使用 `filter_map(...ok())`。

`rustfs/src/storage/rpc/disk.rs::handle_read_multiple` 仍然：

- 先把 response item 转 JSON string。
- 再从 JSON string 反序列化。
- 再编码 msgpack。

因此性能和正确性风险仍在：不必要 CPU/alloc 与 decode 失败静默丢结果。

### 5. metadata cache and early stop

结论：Task 08 的保守边界仍成立。

`get_object_metadata_cache_bypass_reason` 仍在 distributed erasure 下返回 bypass reason。`should_allow_metadata_early_stop` 仍对 `read_data=true` 返回 false。

因此不应把 GET metadata cache 或 aggressive metadata early-stop 直接打开到 distributed erasure 场景。后续 batch metadata RPC 必须保留 quorum 在 `SetDisks` 层处理。

### 6. shard locality

结论：Task 06 和 Task 07 仍成立。

`ShardReadCost` 和 decode launch order 仍存在，但：

- `shard_read_cost_for_disk` 仍只返回 `Local`、`Remote`、`Unknown`。
- `SameNode` 仍没有生产级映射来源。
- locality preference 默认 false。
- hedging 仍是 `min(read_timeout, 100ms)`。
- 当前 rank 仍是 `Local|SameNode -> Unknown -> Remote`。

因此后续应先 observe-only 补拓扑与指标，再 opt-in 改调度。

## 对现有子任务文档的影响

无需修改每个子任务的主体方案。仅需要补充本文件和 README 中的复核说明。

保持原任务顺序：

1. Task 01: HTTP writer error semantics。
2. Task 02: remote lock RPC timeout/eviction。
3. Task 03: ReadMultiple serialization/decoding。
4. Task 04: internode HTTP client tuning。
5. Task 05: observability baseline。
6. Task 06: shard locality topology observe-only。
7. Task 07: shard locality opt-in scheduling。
8. Task 08: batch metadata RPC behind capability gate。
9. Task 09: adaptive batch processor observe-only。

## 后续建议

1. 如果开始编码，先从 Task 01、Task 02、Task 03 三个 correctness/perf prerequisite 入手。
2. 每个任务单独 PR，不要把 HTTP tuning、lock RPC、ReadMultiple、shard locality 合并到一个大 PR。
3. docs 目录当前被忽略；如需要纳入版本管理，需要显式调整 `.gitignore` 或使用 `git add -f`，否则保持本地方案文档即可。
