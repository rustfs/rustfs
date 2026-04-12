# RustFS Heal / Scanner 节点掉线治理实现总结与使用指南 2026-04-12

## 1. 文档目的

本文是当前仓库中 heal / scanner 节点掉线治理实现的落地总结和使用指南。

它面向两个场景：

1. 运维和发布人员快速理解当前版本已经具备什么能力。
2. 开发和排障人员在故障场景下快速定位应该看哪些开关、指标和 admin 输出。

本文不再重复总策略设计，只聚焦当前已经落地的实现、行为语义和使用方法。

---

## 2. 当前已落地能力总览

### 2.1 Patch 1：Correctness 与 Fail-Fast

当前已具备：

- 修复 `get_online_disks_with_healing_and_info()` 的 disk/info 索引错配。
- 为 `disk_info`、`read_metadata`、`list_dir`、`walk_dir` 补齐分级 timeout。
- 远端盘 timeout 后形成 `timeout -> faulty -> connection evict` 闭环。
- `HttpReader` 支持 stall timeout，避免后台 metadata 流无界卡死。

直接收益：

- scanner/heal 不再因为单个远端盘长时间挂起而被无界拖慢。
- timeout 行为可观测，且能触发后续状态机和恢复逻辑。

### 2.2 Patch 2：Scanner 防挂死与 set 级隔离

当前已具备：

- `list_path_raw()` 的 reader `peek()` 增加 timeout。
- 黑洞式 reader 会被排除出当前 merge，而不是无限阻塞聚合器。
- `scanner` 对单个 set 的失败做隔离，不再取消整轮扫描。

直接收益：

- 一个 set 挂死不会拖垮整个 scanner 周期。
- quorum 可在慢盘/坏盘场景下继续推进。

### 2.3 Patch 3：Scanner 与 Heal 解耦

当前已具备：

- scanner 不再 inline 执行 `heal_object().await`。
- scanner 改为发送 `HealChannelRequest` repair candidate。
- heal 提交链路具备 `accepted / merged / full / dropped` admission 语义。
- 低优先级对象在 queue 满时不会阻塞 scanner。
- 高优先级请求在 admission 失败时会升级成 scanner 局部失败并打专门指标。

直接收益：

- scanner 与 heal 的资源占用解耦。
- backlog 场景下 scanner 不会因为对象 heal 变慢而停住。

### 2.4 Patch 4：事件驱动调度、set bulkhead、页内并发

当前已具备：

- heal manager 在 `notify + interval` 混合模型下工作。
- 入队后可被 notify 快速唤醒调度器。
- 支持 per-set bulkhead，避免单个 set 吃满全部 heal 槽位。
- erasure set heal 支持页内对象并发。
- 已补齐 queue delay / task start / task running / scheduler skip / page concurrency 观测。
- 已补 rollback flags。

直接收益：

- repair candidate 从入队到执行更接近实时。
- backlog 下调度更公平。
- erasure set heal 吞吐提升，同时仍保留 checkpoint 路径。

### 2.5 Patch 5：Runtime Drive State 与 Membership Snapshot

当前已具备：

- runtime drive state machine：
  - `Online`
  - `Suspect`
  - `Offline`
  - `Returning`
- runtime state 变化指标和 admin 输出。
- membership snapshot 服务。
- scanner/heal 相关成员选择已改为优先基于 snapshot，而不是对所有磁盘做即时 full probe。
- admin/info 输出采用 hybrid 策略：
  - `Online/Returning`：继续即时 `disk_info()`
  - `Suspect/Offline`：直接使用 runtime snapshot fallback

直接收益：

- 成员选择语义开始从“即时抖动视图”转向“runtime 状态视图”。
- admin/info 在退化场景下不会再被坏盘 probe 拖慢。

---

## 3. 核心行为语义

### 3.1 成员选择语义

当前存在三类明确语义：

1. 严格 online 语义
   用于基础入口 `get_online_disks()` / `get_online_local_disks()`。
   只包含 runtime state 为 `Online` 的成员。

2. scanner / heal 候选语义
   用于 `get_online_disks_with_healing*()`。
   候选成员包含：
   - `Online`
   - `Suspect`
   - `Returning`
   不包含：
   - `Offline`

3. admin/info hybrid 语义
   `Online/Returning` 允许即时 probe。
   `Suspect/Offline` 不再做即时 probe，直接回退到 runtime snapshot 展示。

### 3.2 Scanner Heal Admission 语义

当前 heal candidate admission 结果固定为：

- `accepted`
- `merged`
- `full`
- `dropped`

低优先级对象请求：

- 优先 merge
- queue 满时允许 drop
- 不阻塞 scanner

高优先级请求：

- admission 失败会升级成 scanner 局部失败
- 依赖 set 级失败隔离和下一轮扫描继续恢复

### 3.3 Inline Heal 回滚开关语义

存在兼容开关：

- `RUSTFS_SCANNER_INLINE_HEAL_ENABLE`

注意：

- 当前实现不会真正恢复旧的 inline heal 执行路径。
- 该开关的作用是兼容灰度和排障窗口：
  - 若开启，会打一次明确 warning
  - scanner 仍继续使用 enqueue-based heal candidate 逻辑

对应指标：

- `rustfs_scanner_inline_heal_total`

该指标当前应保持为 `0`。

---

## 4. 关键配置项

### 4.1 Drive Timeout 与 Runtime State

- `RUSTFS_DRIVE_METADATA_TIMEOUT_SECS`
- `RUSTFS_DRIVE_DISK_INFO_TIMEOUT_SECS`
- `RUSTFS_DRIVE_LIST_DIR_TIMEOUT_SECS`
- `RUSTFS_DRIVE_WALKDIR_TIMEOUT_SECS`
- `RUSTFS_DRIVE_WALKDIR_STALL_TIMEOUT_SECS`
- `RUSTFS_DRIVE_SUSPECT_FAILURE_THRESHOLD`
- `RUSTFS_DRIVE_RETURNING_SUCCESS_THRESHOLD`
- `RUSTFS_DRIVE_RETURNING_PROBE_INTERVAL_SECS`
- `RUSTFS_DRIVE_OFFLINE_GRACE_PERIOD_SECS`
- `RUSTFS_DRIVE_LONG_OFFLINE_THRESHOLD_SECS`

### 4.2 Heal Admission 与 Queue 策略

- `RUSTFS_HEAL_QUEUE_SIZE`
- `RUSTFS_HEAL_LOW_PRIORITY_MERGE_ENABLE`
- `RUSTFS_HEAL_LOW_PRIORITY_DROP_WHEN_FULL`

### 4.3 Heal 调度与并发

- `RUSTFS_HEAL_MAX_CONCURRENT_HEALS`
- `RUSTFS_HEAL_MAX_CONCURRENT_PER_SET`
- `RUSTFS_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE`
- `RUSTFS_HEAL_SET_BULKHEAD_ENABLE`
- `RUSTFS_HEAL_PAGE_PARALLEL_ENABLE`
- `RUSTFS_HEAL_PAGE_OBJECT_CONCURRENCY`

### 4.4 Scanner

- `RUSTFS_SCANNER_ENABLED`
- `RUSTFS_HEAL_ENABLED`
- `RUSTFS_SCANNER_START_DELAY_SECS`
- `RUSTFS_SCANNER_SPEED`
- `RUSTFS_SCANNER_IDLE_MODE`
- `RUSTFS_SCANNER_INLINE_HEAL_ENABLE`

---

## 5. 关键指标说明

### 5.1 Drive 相关

- `rustfs_drive_op_timeout_total{endpoint,op}`
- `rustfs_drive_faulty_mark_total{endpoint,reason}`
- `rustfs_drive_connection_evict_total{endpoint,reason}`
- `rustfs_drive_runtime_state{pool,set,disk,state}`
- `rustfs_drive_state_transition_total{from,to,reason}`
- `rustfs_drive_recovery_class_total{class}`
- `rustfs_drive_offline_duration_seconds`

### 5.2 Scanner / Candidate Admission

- `rustfs_heal_candidate_enqueue_total{type,priority,result}`
- `rustfs_heal_candidate_merge_total{type}`
- `rustfs_heal_candidate_drop_total{type,reason}`
- `rustfs_heal_candidate_priority_reject_total{type,priority,result,reason}`
- `rustfs_scanner_set_failure_total{pool,set,stage}`
- `rustfs_list_path_raw_stall_total{drive}`
- `rustfs_scanner_inline_heal_total`

### 5.3 Heal Scheduler / Execution

- `rustfs_heal_queue_delay_seconds{type,set}`
- `rustfs_heal_task_start_total{type,set}`
- `rustfs_heal_task_running{type,set}`
- `rustfs_heal_scheduler_skip_total{reason,set}`
- `rustfs_heal_page_concurrency_current{set}`

---

## 6. 运维使用指南

### 6.1 目标：确认 scanner 是否被掉线节点拖住

优先看：

- `rustfs_drive_op_timeout_total`
- `rustfs_list_path_raw_stall_total`
- `rustfs_scanner_set_failure_total`

典型判断：

- 如果 `drive_op_timeout_total` 持续增长，同时 `faulty_mark_total` 和 `connection_evict_total` 同步增长，说明 fail-fast 闭环生效。
- 如果 `list_path_raw_stall_total` 增长但 scanner 周期仍推进，说明 Patch 2 生效。

### 6.2 目标：确认 scanner 与 heal 是否已经解耦

优先看：

- `rustfs_heal_candidate_enqueue_total`
- `rustfs_heal_candidate_merge_total`
- `rustfs_heal_candidate_drop_total`
- `rustfs_scanner_inline_heal_total`

典型判断：

- `rustfs_scanner_inline_heal_total == 0`
- 慢 heal 场景下，candidate admission 指标仍在增长，而 scanner 不被阻塞

### 6.3 目标：确认 heal 调度是否被单 set 吃满

优先看：

- `rustfs_heal_task_running`
- `rustfs_heal_scheduler_skip_total`
- `rustfs_heal_queue_delay_seconds`

典型判断：

- 如果某个 set 已达到 per-set 上限，应看到 `scheduler_skip_total{reason="set_limit"}` 增长。
- backlog 存在时，`queue_delay_seconds` 不应无限增长。

### 6.4 目标：确认 drive 是否已进入恢复稳定化流程

优先看：

- `rustfs_drive_runtime_state`
- `rustfs_drive_state_transition_total`
- `rustfs_drive_offline_duration_seconds`
- admin storage/info 输出里的：
  - `runtimeState`
  - `offlineDurationSeconds`

典型判断：

- 短时波动应表现为 `Online -> Suspect -> Online`
- 长离线恢复应表现为 `Offline -> Returning -> Online`

---

## 7. 推荐灰度与回滚顺序

### 7.1 Admission 与调度相关回滚

如果 heal backlog 行为异常，建议按如下顺序回退：

1. 先关闭 `RUSTFS_HEAL_PAGE_PARALLEL_ENABLE`
2. 再关闭 `RUSTFS_HEAL_SET_BULKHEAD_ENABLE`
3. 最后关闭 `RUSTFS_HEAL_EVENT_DRIVEN_SCHEDULER_ENABLE`

原因：

- 这样能先保留 scheduler 基础能力，只回退吞吐提升项。

### 7.2 Scanner 相关回滚

如果 scanner 场景下担心 heal admission 行为变化：

- 保留当前实现
- 不建议依赖 `RUSTFS_SCANNER_INLINE_HEAL_ENABLE` 恢复旧 inline heal 行为

原因：

- 该开关当前仅用于兼容告警，不会真正切回旧实现

### 7.3 Drive / Snapshot 相关回滚

若运行时状态机或 snapshot 选择引发异常：

- 先通过提高 timeout 和阈值减小 state 抖动
- 再排查 runtime state 和 admin/info 输出是否匹配
- 不建议直接撤销 fail-fast 闭环

---

## 8. 当前已知边界

当前版本已经把掉线治理的主链路补齐，但仍有几个明确边界：

1. `RUSTFS_SCANNER_INLINE_HEAL_ENABLE` 不是旧逻辑的真实回滚路径。
2. membership snapshot 当前主要覆盖 scanner/heal 和 admin/info 路径，未把所有潜在成员选择入口都改成统一 snapshot API。
3. recovery class 的长离线 repair 策略分层尚未完整扩展到更高层的自动 repair 编排。

---

## 9. 推荐验收命令

建议至少执行：

```bash
make pre-commit
```

若只做定向验证，建议优先：

```bash
cargo test -p rustfs-ecstore
cargo test -p rustfs-heal
cargo test -p rustfs-scanner
```

---

## 10. 一句话总结

当前版本已经把 heal / scanner 节点掉线治理从“被动超时 + 互相拖慢”推进到“可 fail-fast、可观测、可隔离、可调度、开始基于 runtime state 选择成员”的状态。
