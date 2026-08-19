# RustFS heal / scanner 全量功能分析与 MinIO 对标（v2）

> English version: [rustfs-heal-scanner-vs-minio-comprehensive-analysis-2026-08-16.md](rustfs-heal-scanner-vs-minio-comprehensive-analysis-2026-08-16.md)

- 日期：2026-08-16（基于 main 分支当日代码，审计时 HEAD ≈ `a118d7e4f`）
- 范围：`crates/heal`（src 19,560 行 + tests 2,274 行）、`crates/scanner`（src 约 26,000 行 + tests）、`crates/data-usage`、`crates/ecstore` 中 heal/heal_walk/bitrot_self_verify 与 config、`crates/common/src/heal_channel.rs`、`crates/madmin`（heal/scanner wire 类型）、`rustfs/src`（startup wiring、admin handlers、集群 RPC）
- 对标基线：minio/minio master（HEAD `7aac2a2c5b`，仓库已进入维护模式，master 冻结，即最终态）
- 方法：四路并行审计（heal crate / scanner crate / ecstore 集成层 / MinIO 源码研究），关键结论逐条人工抽验（文内标注"已亲验"处为一手验证）
- 本文档取代 `docs/rustfs-heal-scanner-vs-minio-parity-assessment.md`（2026-06-15 v1）。v1 之后 heal/scanner 相关提交超过 80 个（换盘自动修复全链路、resume 状态机、usage 收敛权威化、集群级 heal 协调、ILM restore 语义等），v1 的功能清单与差距判断已全面过时；v1 中"bloom filter 缺失"等结论经本次核实为**误判**（详见 §5.4）。

---

## 0. 结论摘要

1. **总体判断：heal 与 scanner 的核心功能链路已经完整**。对象级 heal（quorum 仲裁 + ETag 兜底 + bitrot Deep 校验 + dangling 处理）、erasure set 深扫（per-set disk-walk 并集枚举）、按版本断点续扫（schema 化持久层 + CAS 原子发布 + 崩溃窗口补齐）、换盘自动修复（readiness 校验 + 身份围栏 + durable intent + completion proof）、scanner 周期循环（leader lock + 持久化 leader-epoch 围栏）、data usage 统计（桶级/集群级、主+备+观测快照、epoch/cycle 防回退）、ILM 全动作（expiry/transition/noncurrent/free-version/delete-marker 清理）、admin Start/Query/Cancel 协议（clientToken 语义对齐 madmin）——以上均有实现且带回归测试。两个 crate 内**没有空实现/早退桩**，异常路径全部有日志 + 指标 + 错误语义。
2. **主要缺口集中在"入口与观测面"，而不是修复算法本身**：MRF/ECDecode/Metadata 三类任务执行体已实现但无生产触发入口（`HealEvent` 完全未接线）；`CheckAbandonedParts` 在 ecstore 三层全部 `NotImplemented`；heal/scanner trace 通道缺失；scanner 超限 S3 事件缺失；madmin 客户端方法缺失（只有 wire 类型）；heal 字节级进度/ETA 未实现。
3. **与 v1 认知的重要修正**：bloom filter 在 MinIO 当前 master **已删除**（`.bloomcycle.bin` 只存 cycle 计数），RustFS 现状与 MinIO 一致；MinIO scanner 同样是**集群级 leader 单例**，RustFS 的 leader.lock 模型与 MinIO 同型；RustFS 的 ETag 多数派兜底仲裁已实现（`crates/ecstore/src/set_disk/ops/heal.rs:525-567,679`，已亲验），v1 担心的仲裁缺口不存在。
4. **RustFS 在多处超出 MinIO**：remote_scanner RPC 协议（远端 peer 本地扫描而非 leader 跨网读远盘）、持久化 leader-epoch CAS 围栏、周期预算与 per-set/per-disk 并发闸、pending-heal 账本、durable replacement intent + completion proof 状态机、前台压力门控（mainline throttle）、集群 heal control coordinator + envelope 重放防护。
5. 差距分级统计：P1（行为/运维对齐缺口）8 项，P2（完善性）9 项，P3（清理/低风险）3 项，"按设计不追平"7 项。完整清单见 §6。

---

## 1. 架构总览

### 1.1 RustFS 三层架构

RustFS 把 MinIO 在 `cmd/` 内单体的 heal/scanner 拆成三层 + 两个独立 crate：

| 层 | 位置 | 职责 |
|---|---|---|
| 原语层 | `crates/ecstore/src/set_disk/ops/heal.rs`（~3,240 行）、`ops/heal_walk.rs`、`ops/bitrot_self_verify.rs`；上层封装 `store/heal.rs`、`store/heal_walk.rs`、`core/sets.rs` | 对象/桶/format/替换盘格式修复、disk-walk 并集枚举、写入路径 bitrot 自校验；由 `SetDisks`/`Sets`/`ECStore` 实现 `rustfs_storage_api::HealOperations` 契约（`crates/storage-api/src/object.rs:503-519`） |
| heal 运行时 | `crates/heal` | 进程级 HealManager（优先级队列/调度器/auto disk scanner/断点续传 resume）、HealChannelProcessor（消费全局 heal channel）、换盘替换恢复状态机 |
| scanner 运行时 | `crates/scanner` | 数据使用扫描、ILM 评估与入队、heal 候选生产、复制用量统计、remote scanner RPC |
| 共享协议 | `crates/common/src/heal_channel.rs`（~776 行） | Start/Query/Cancel 命令通道、`HealOpts`/`HealScanMode`/`HealRequestSource`/`HealAdmission*` 共享类型、`HealResultItem`（madmin） |
| 共享数据 | `crates/data-usage` | `DataUsageEntry/Info`、直方图、`hash_path`；scanner 产生、ecstore/admin 消费 |

启动链路（已亲验 wiring）：

1. `rustfs/src/startup_services.rs:93` → `init_background_service_runtime(store)`。
2. `rustfs/src/startup_background.rs:41-81`：创建全局 heal 服务取消令牌；读 `RUSTFS_SCANNER_ENABLED`（别名 `RUSTFS_ENABLE_SCANNER`，默认 true）与 `RUSTFS_HEAL_ENABLED`（别名 `RUSTFS_ENABLE_HEAL`，默认 true）；**只要 heal 或 scanner 任一开启就初始化 heal manager**（scanner 产生的 heal 候选需要消费端；两者都关时 heal channel 不初始化，`send_heal_request` 报 "Heal channel not initialized"）。
3. `crates/heal/src/lib.rs:142-216`：owned task 内原子初始化（caller 取消不会遗留半初始化 manager，`lib.rs:123-131`；`GLOBAL_HEAL_RUNTIME_INIT` 互斥单飞）→ `HealManager::start()` → `rustfs_common::heal_channel::init_heal_channels()` → spawn `HealChannelProcessor::start_with_receipts`。
4. `crates/heal/src/heal/manager.rs:1301-1356` `HealManager::start`：`start_scheduler()`（`manager.rs:2394-2461`，interval 默认 10s + `Notify` 事件驱动唤醒）→ `process_unclean_shutdown()`（`manager.rs:1362-1695`）→ `enable_auto_heal`（默认 true）时 `start_auto_disk_scanner()`（`manager.rs:2464-2999`）。
5. server ready 后 `rustfs/src/startup_lifecycle.rs:150-152`：`enable_scanner` 时 `init_data_scanner(token, store)`（`crates/scanner/src/scanner.rs:1293-1372`）。
6. 优雅停机：`rustfs/src/startup_shutdown.rs:308` `shutdown_ahm_services()`（取消令牌）；`:414` `clear_unclean_shutdown_markers()`。

### 1.2 MinIO 对应结构（master 最终态）

| MinIO 文件 | 职责 |
|---|---|
| `cmd/admin-heal-ops.go` | 手动 admin heal 序列（healSequence、clientToken/forceStart/forceStop） |
| `cmd/global-heal.go` | 常驻后台 heal 队列（newBgHealSequence，token 固定 `0000-…`，永不结束）+ `healErasureSet`（逐 set 全量对象 heal） |
| `cmd/background-heal-ops.go` | healRoutine worker 池（`_MINIO_HEAL_WORKERS`，默认 GOMAXPROCS/2）消费 healTask |
| `cmd/mrf.go` | MRF（Most Recent Fail）队列（容量 100,000），进程退出时持久化 `.minio.sys/buckets/.heal/mrf/list.bin` 并启动回放 |
| `cmd/background-newdisks-heal-ops.go` | 新盘/换盘自动 resync（monitorLocalDisksAndHeal 10s 轮询 + healFreshDisk + healingTracker） |
| `cmd/erasure-healing.go` / `erasure-healing-common.go` | 对象级 heal 核心（~800 行）、listAndHeal |
| `cmd/data-scanner.go` | scanner 循环（globalLeaderLock 集群单例）+ folderScanner + applyActions |
| `cmd/erasure.go`（nsScanner）/ `erasure-server-pool.go` | NSScanner 三层结构 |
| `cmd/bucket-lifecycle.go` | ILM 执行器（expiry/transition worker 池） |
| `cmd/xl-storage.go` | DiskInfo.Healing、CheckParts/VerifyFile、CleanAbandonedData、RenameData healing 分支 |
| `cmd/prepare-storage.go` | waitForFormatErasure 新盘启动握手 |

### 1.3 架构级差异（设计取舍，非缺陷）

1. **heal 队列模型**：MinIO 所有 heal（scanner 抽样/MRF/admin/新盘 resync）汇入单 channel + 固定 worker 池（新盘 resync 另有 per-drive worker 池）；RustFS 是优先级堆 + 去重合并 + 容量分级丢弃 + per-set bulkhead + 前台压力门控的多策略调度器（`manager.rs:3003-3420`）。RustFS 表达力更强，代价是"重复请求被合并"的可观测性问题（v1 已指出，现有 `HealAdmissionReceipt` canonical task_id + alias 机制回应了它，`manager.rs:1759-1846`）。
2. **scanner 远端盘访问**：MinIO leader 通过磁盘抽象层透明读写远端节点磁盘；RustFS leader 通过 remote_scanner RPC 把扫描执行下放到远端 peer 本地进行（`crates/scanner/src/remote_scanner.rs`），只回传结果与进度心跳。两者都是集群单 leader。RustFS 方案省 leader↔远端的元数据读放大，代价是需要维护独立 RPC 协议（HMAC 逐帧认证、会话重放缓存、fence 复验，`remote_scanner.rs:52-61,405-496,1024-1065`）。
3. **heal 状态持久化**：MinIO 用单文件 `.healing.bin`（msgp healingTracker，diskID 不匹配即重置）；RustFS 用 schema 化多文件（resume/checkpoint/intent/seal/proof 各自 CAS 发布，`resume.rs:38-61`），崩溃窗口显式补齐（`erasure_healer.rs:389-402`、`resume.rs:1027-1057`）。
4. **写路径自保护**：MinIO 写入后靠后台 heal 收敛；RustFS 在 PutObject/CompleteMultipartUpload 提交 rename 后主动检查 `convergence.needs_heal()` 并立即入队对象 heal（`set_disk/ops/object.rs:2291-2306`、`ops/multipart.rs:2574-2589`），另有读修复 read repair（`io_primitives.rs:1040-1160`）。

---

## 2. Heal 已实现功能全景

### 2.1 任务类型（`HealType`，`crates/heal/src/heal/task.rs:85-111`）

| 类型 | 语义 | 执行体 | 生产触发方 |
|---|---|---|---|
| `Cluster` | 所有 bucket 依次 heal（结构 + 可选递归对象），批内重试 ≤3 | `heal_cluster` task.rs:1420-1490 | channel：bucket 为空即 Cluster（channel.rs:576-577） |
| `Object{bucket,object,version_id}` | 单对象/版本；不存在时按 `recreate_missing` 重建或报错 | `heal_object` task.rs:855-1146 | admin、scanner、read-repair、写路径收敛、add_partial |
| `Bucket{bucket}` | 桶元数据/结构；`recursive` 再遍历全部对象版本 | `heal_bucket` task.rs:1284-1418 + `heal_bucket_objects` task.rs:1508-1698 | admin（POST /v3/heal/{bucket}）、scanner `build_bucket_heal_request` |
| `Prefix{bucket,prefix}` | 按前缀递归 | `heal_prefix` task.rs:1492-1506 | channel：`recursive && prefix` 非空（channel.rs:578-585） |
| `ErasureSet{buckets,set_disk_id}` | format 修复 + healing 标记 + 逐桶预处理 + 可恢复逐版本深扫 | `heal_erasure_set` task.rs:2158-2642 | admin（pool/set 参数）、auto disk scanner、unclean shutdown、renew_disk、durable replacement 恢复 |
| `Metadata{bucket,object}` | 仅元数据（Deep、不重建数据） | `heal_metadata` task.rs:1700-1859 | **无生产触发方**（§6 HS-01） |
| `MRF{meta_path}` | 失败路径驱动的 Deep 修复（recursive+update_parity） | `heal_mrf` task.rs:1861-1992 | **无生产触发方**（仅 `HealEvent` 可生成，未接线） |
| `ECDecode{bucket,object,version_id}` | EC 解码重建（Deep+recreate+update_parity），Urgent 优先级 | `heal_ec_decode` task.rs:1994-2156 | **无生产触发方**（仅 `HealEvent` 可生成，未接线） |

优先级 `Low/Normal/High/Urgent`（task.rs:168-179）；状态机 `Pending/Running/Retrying/Completed/Failed/Cancelled/Timeout`（task.rs:225-241）。

### 2.2 触发路径全景（admin 之外）

| 通道 | source | 优先级 | 证据 |
|---|---|---|---|
| Scanner 周期抽样（1/1024，`RUSTFS_HEAL_OBJECT_SELECT_PROB`） | Scanner | Low | `scanner_folder.rs:2117-2136`、`:1150`；`remove_corrupted=HEAL_DELETE_DANGLING(true)`、`recreate_missing=false`（`common/heal_channel.rs:24`、`scanner_folder.rs:510-511`） |
| Scanner 元数据损坏（get_size 失败分类 HealMetadata） | Scanner | High | `scanner_folder.rs:2147-2208`、`:1244-1260` |
| Scanner abandoned children（缓存有、盘上无，list_path_raw quorum 核查） | Scanner | High（桶级+对象级） | `scanner_folder.rs:2528-2792` |
| Scanner pending-heal 账本重试（heal 通道满被拒后持久化，每桶每轮 ≤128 条、上限 10k） | Scanner | 原优先级 | `scanner_folder.rs:1721-1763`、`:99-100` |
| auto disk scanner（unformatted 盘经 replacement_readiness 确认 / `runtime_state=="returning"` 盘 / durable intent 重入） | AutoHeal | Low | `manager.rs:2464-2999` |
| unclean shutdown 恢复（启动读 `unclean-shutdown` 标记 → 全部本地 set ErasureSet heal） | AutoHeal | Low | `manager.rs:1362-1695` |
| 写路径收敛（PutObject/CompleteMultipartUpload 后 `convergence.needs_heal()`） | Internal | Normal | `set_disk/ops/object.rs:2291-2306`、`ops/multipart.rs:2574-2589` |
| 部分对象 heal（add_partial） | Internal | Normal | `set_disk/ops/object.rs:5808-5825` |
| 旧数据目录清理残留 enqueue | Internal | Normal | `set_disk/core/io_primitives.rs:3880-3907` |
| 读修复（metadata_read_error / missing_shards / decode_error，TTL 去重缓存） | ReadRepair | Low | `set_disk/read.rs:407,995,1079` → `submit_read_repair_heal`（`io_primitives.rs:1105-1160`），`recreate_missing=true` |
| 盘重连遇 UnformattedDisk → send_heal_disk | AutoHeal | Normal | `set_disk/ops/locking.rs:339-347` |
| Admin API（含集群 coordinator 路由） | Admin | High | `rustfs/src/admin/handlers/heal.rs:174-212`、`:771-930` |
| 集群 RPC heal（peer 调用） | — | — | `rustfs/src/storage/rpc/node_service/heal.rs`、`ecstore/src/cluster/rpc/peer_s3_client.rs:296,1209` |

注意：MinIO 的 MRF 通道（读路径检出 part 缺失/损坏即时投递 + 队列持久化 + shutdown 回放，`cmd/mrf.go`、`erasure-object.go:395-410,800-812`）在 RustFS 由 read-repair + 写路径收敛**部分替代**；`HealType::MRF`/`ECDecode`/`Metadata` 三个执行体没有生产入口（详见 §6 HS-01）。

### 2.3 对象级 heal 语义（ecstore `set_disk/ops/heal.rs`）

流程（`heal_object_with_explicit_version_regen` :426 起）：

1. 取对象写锁（除非 `no_lock`）；`object` 以 `/` 结尾走对象目录 heal（`heal_object_dir_locked` :1587-1717：dangling 判定 + `remove` 删除 + 缺 volume 重建）。
2. `read_all_fileinfo` 全盘读 xl.meta，全部 not-found 视为已删除返回。
3. **quorum 仲裁 + ETag 兜底**（已亲验）：`list_online_disks` 以 mod-time quorum 为准；quorum 失效时回退 ETag 多数派仲裁（`:525-567` `filter_by_etag`/`quorum_etag`）；`pick_valid_fileinfo` 选 canonical 元数据；"meta 坏盘数 > parity" 的 cannotHeal 判定在 ETag 全盘一致时豁免（`:679`）。与 MinIO `filterDisksByETag` 双仲裁一致。
4. `disks_with_all_parts`（:562-572）按 `scan_mode` 校验 part：**Normal 仅 stat（CheckParts 语义），Deep 做全量 bitrot 校验（VerifyFile 语义）**；Normal 扫描检出 `FileCorrupt` 自动升级 Deep 重试一次（`:2022-2031`，与 MinIO erasure-healing.go:1101-1106 同型）；无 parity 对象（EC:0）bitrot 失败判不可恢复（`:700-726`）。
5. `should_heal_object_on_disk`（:606-650）逐盘分类 missing/corrupt/offline/outdated → 重建：per-part bitrot reader/writer（用 per-part checksum + 算法）、写临时卷后 rename 提交（`HEAL_RENAME_INCOMPLETE` 重试语义 :24）；dangling 删除安全检查 `dangling_delete_safety`（:1488）；**孤儿数据目录回收 `reclaim_orphan_data_dirs_best_effort`（:1428）**——这部分覆盖了 MinIO `CleanAbandonedData` 的主场景（但无独立 `CheckAbandonedParts` API，见 §6 HS-02）。
6. 版本化对象：枚举"每个版本"（`storage.rs:1494-1530`）；delete-marker 路径由 `latest_meta.deleted` 决定（`storage.rs:262-277` 注释）；回归测试 `tests/heal_b5_versioned_regression_test.rs:282,334`。
7. 显式版本重建 `try_regenerate_explicit_version_meta`（:1318）；transitioned 对象本地残留清理。
8. 写入路径另有 shard 级 bitrot 自校验 `verify_written_bitrot_shards`（`ops/bitrot_self_verify.rs:45-129`，HighwayHash256S，最终 rename 前校验刚写出的 shard，服务 EC:0 无 parity 场景）——**注意这不是后台 bitrot 巡检**；后台巡检由 scanner bitrot_cycle 驱动 Deep heal 承担。

heal crate 侧包装（`task.rs:855-1146`）：存在性检查（瞬时错误转 `TransientSkip` 不误判失败 :551-569）；scanner 合成目录规范化（:1148-1180）；`recreate_missing` 重建（:1183-1282）；data-usage-cache 对象锁超时豁免（:571-653）；not-found → treated_as_deleted 成功（:1012-1029）；结果 `HealResultItem` 保留至多 1024 条 + truncated 标志（:50,845-852）。

递归遍历（`heal_bucket_objects` task.rs:1508-1698）：分页枚举全部版本含 delete marker、瞬时错误指数退避重试 ≤3（2^n + 抖动 :620-627）、失败样本日志截断 ≤5 条、聚合 `BatchHealFailure`。

### 2.4 erasure set heal 与断点续扫

`heal_erasure_set`（task.rs:2158-2642）四阶段（4 步进度跟踪）：

1. **替换意图与恢复盘选择**（仅 AutoHeal + heal_endpoints 非空）：复用 durable intent 所在盘 / 排除目标端点选幸存盘；已完成代（CleanupPending）幂等收尾。
2. **格式修复**：`heal_replacement_format(dry_run, pool, set, targets)`（`storage.rs:1372-1384`，trait 默认实现 fail-closed）；逐目标盘结果必须全 ok（`erasure_healer.rs:97-102`）+ 身份围栏复核（task.rs:2410-2420）。
3. **healing 标记**：对目标盘写 owner CAS 标记 `{set_disk_id}:{task_id}`（`mod.rs:80-229`，CAS + 回滚 + 并发唯一 owner），使 `DiskInfo.healing` 为真（已亲验赋值链 `set_disk/mod.rs:4988`）。
4. **逐桶预处理 + 可恢复深扫**：`ErasureSetHealer::heal_erasure_set`（`erasure_healer.rs:242-278`）。

`ErasureSetHealer` 扫描细节（对标 MinIO `healErasureSet`，`heal_walk.rs:15-23` 模块注释明确引用 MinIO `global-heal.go` 的 listPathRaw + objQuorum=1 + mergeXLV2Versions）：

- **枚举器选择（backlog#920）**：Deep 或 AutoHeal → per-set **disk-walk 并集枚举** `list_versions_for_heal_page_disk_walk`（"任意盘上存在"即 sub-quorum 可重建；`storage.rs:1559-1644`，页界 1000 对象/10,000 版本，`dw1:` cursor）；普通请求走 read-quorum `list_object_versions`。
- **续扫游标**：权威 cursor 为 opaque continuation token（`v1:`=marker JSON、`dw1:`=disk-walk key，两命名空间互斥防误读，`storage.rs:81-260`）；每完成一页先持久化 cursor 再清 dedup 集合（`erasure_healer.rs:922-927`）。
- **页内并发**：FuturesUnordered + Semaphore，默认 `RUSTFS_HEAL_PAGE_OBJECT_CONCURRENCY=8`，Deep/AutoHeal 强制 1（`erasure_healer.rs:105-142`）。
- **per-version dedup**：`compose_key` 长度前缀注入编码（`resume.rs:281-288`）。
- **错误分类**：真缺席（FileNotFound 等）→ Absent（计成功）；基础设施瞬时（quorum/DiskNotFound/SlowDown 等）→ Transient（计 skipped）；其余 Failed（`erasure_healer.rs:148-182`，注释引 backlog#856/#799 B7：离线盘不得记 healed/absent）。
- **防死循环**：空页 truncated 或页尾版本身份不前进即中止（:933-949）。
- **完成判定**：failed/skipped/failed_buckets 任一 >0 不标记完成，`schedule_retry()` 复位 resume+checkpoint 两层（:561-626，backlog#855/B6/#1033：skip 轮不得标记完成）。
- **替换盘提交证据**：目标端点物理回读 `replacement_targets_have_version`（`ops/heal.rs:340-412`），未确认 → transient skip。

### 2.5 换盘自动修复（replacement recovery）

- **识别**（`replacement_readiness.rs:25-73`）：`replacement_mount_lease_root()` 存在、canonicalize 成功、是挂载点、物理设备 id 非空、与根设备不相交、不与兄弟盘共享物理设备（Linux 用 /proc/self/mountinfo mount-id+dev+ino）。非 root 挂载检查有回归测试（`manager.rs:3549`）。
- **状态机**（`resume.rs:63-73`）：`Intent → Rebuilding →（写 proof）Verified → CleanupPending → 清理`；`Abandoned` 终态；跨状态迁移先写持久层再变更（`save_state_strict`）。
- **持久化**（`resume.rs:38-61`，schema ResumeState=5/Checkpoint=5/proof=1）：`{task_id}_ahm_resume_state.json`、`_ahm_checkpoint.json`、`buckets/ahm-replacement/` 命名空间下 intent/seal/completion_proof；torn write + 无 seal 可识别并原子重建（:1316-1338）；CAS 发布、拒绝覆盖并发有效 proof（:1512-1585）。
- **恢复**：unclean shutdown 与周期扫描都从幸存盘恢复未完成/待清理替换代（`manager.rs:1435-1640,2663-2815`）；多代冲突/校验失败 → 冻结该 set（`replacement_recovery_blocked_sets`，`manager.rs:69-87,2782-2815`）。
- **对外快照**：`current_replacement_recovery_snapshot`（`lib.rs:262-333`）合并本地幸存盘记录，冲突 → Unknown/非 definitive；admin `GET /v4/heal/replacement-recovery`。

### 2.6 调度器（manager.rs）

- 优先级堆 + 同优先级 FIFO（:148-191,330-347）；dedup key 按类型（:469-506）；入队三态查重 active→queued→retrying（:1759-1785）；重复默认 Merged 并返回 canonical task_id（`HealAdmissionReceipt`，:1821-1846）+ client token alias（:1219-1246）。
- 容量：队列满时 best-effort 来源（Scanner/AutoHeal/ReadRepair）或低优先级被 Dropped(QueueFull)；Admin/Internal 可驱逐低优先级排队项（`push_displacing_lower_priority` :353-396）；80%/95% 压力分级（:885-909）。
- 并发：全局 `max_concurrent_heals`（默认 4）+ per-set bulkhead `max_concurrent_per_set`（默认 1）（:3040-3073,3434-3447）。
- 前台压力门控 mainline throttle：前台读/写 permit 利用率 ≥80% 时延迟 best-effort 任务（:919-1009,2999-3020）。
- 超时：任务级聚合超时（默认 300s），跨重试保留剩余预算（task.rs:444-451，PR #6101）。
- 可恢复重试：`is_recoverable_heal()`（error.rs:83-136）≤3 次、2^n 退避封顶 30s；retry 在独立 backoff task 中持有所有权（:3235-3382）。
- 完成态保留 10 分钟供查询（:42）。

### 2.7 Admin API 与集群协调

- 路由（`rustfs/src/admin/handlers/heal.rs:174-212`）：`POST /rustfs/admin/v3/heal/`、`/heal/{bucket}`、`/heal/{bucket}/{prefix}`（同一 POST 按 query `clientToken/forceStart/forceStop` 区分 start/query/cancel，与 mc admin heal 语义对齐）；`POST /v3/background-heal/status`；`GET /v4/heal/replacement-recovery`。权限 `HealAdminAction`（route_policy.rs:334-341）。
- 集群协调（heal.rs:771-930 + `node_service.rs:514-606`）：`heal_topology_fingerprint` + 按拓扑确定性选 coordinator 节点 + coordinator epoch；envelope 校验 + SHA256 digest 重放缓防重放；coordinator 非本机走 peer gRPC `heal_control`；`probe_heal_control` 能力探测（滚动升级场景）。
- 请求：body 为 `HealOpts`（`recursive/dryRun/remove/recreate/scanMode(0/1/2)/updateParity/nolock/pool/set`，serde camelCase，与 madmin.HealOpts 字段对齐）；根 heal start 需 `recursive=true` 或 `pool+set` 成对；body 上限 1MB。
- 响应：`HealStartSuccess{clientToken, clientAddress, startTime}`；`HealTaskStatus{summary, detail, startTime, settings, items, truncated, progress}`（summary ∈ running/finished/stopped/notFound）；`BackgroundHealStatus`（bitrot 起始时间/周期/当前模式 + `disabled/uninitialized/idle/active/degraded` 状态——peer 不可达显式 degraded 不冒充 idle，issue #5850 + `healOperations` 按优先级×来源矩阵 + 集群进度）。
- `HealResultItem`/`HealDriveInfo`/`HealItemType`/DriveState 枚举与 madmin JSON 兼容（`crates/madmin/src/heal_commands.rs:19-65`）。
- 状态 payload 超 8MiB 对折截断（channel.rs:37,73-104）；path-token 校验（错误 token 拒绝，空 path 仅匹配 Cluster）。

### 2.8 heal 指标与日志

指标：`rustfs_heal_admission_total{source,result,reason,context}`、`rustfs_heal_task_start_total`、`rustfs_heal_task_running{type,set}`、`rustfs_heal_queue_delay_seconds`、`rustfs_heal_scheduler_skip_total`、`rustfs_heal_mainline_throttle_total`、`rustfs_heal_page_concurrency_current{set}`、`rustfs_heal_candidate_enqueue/merge/drop/priority_reject_total`、`rustfs_heal_read_repair_dedup_total{reason}` 等。日志全部结构化 event style（PR #5720）；per-object 日志降级防风暴（`demote_to_debug_when!`，#5716/#5719/#5727）。

---

## 3. Scanner 已实现功能全景

### 3.1 循环、leader、立即触发

- **集群单 leader**：分布式 ns 写锁 `leader.lock`（`scanner.rs:3156-3207`，超时默认 5s）+ **持久化 leader-epoch CAS 围栏**：leader 用 ETag 前置条件向 `.bloomcycle.bin` 写 `RSCYC001` 编码的 (cycle, leader_epoch)（`scanner.rs:118,1850-1861,2177-2334`）；usage 快照再打 epoch fence（:2087-2153）。锁丢失 → 取消当前周期，30s 收敛（:108-111,2623-2642）。
- 抢锁后立即执行一轮；周期 = `RUSTFS_SCANNER_CYCLE` > config cycle > start_delay > 部署默认 > 速度档位（±10% 抖动、下限 1s）。
- **clean-idle 指数退避**：连续完整无脏周期间隔 ×2（封顶 24h；bitrot 周期压缩上限；桶有 lifecycle/replication 活动规则禁用，:383-456,1382-1512）。
- **superseded/deferred 退避**：5s 起指数退避封顶 30min（:105-106,3432-3438）；维护探测失败独立退避（:459-505）。
- **立即唤醒**：① dirty-usage 快路径——写路径 put/delete/multipart/bucket 操作调用 `record_dirty_usage_bucket`（`scanner_io.rs:222-235`；调用点 `rustfs/src/app/object_usecase.rs:6221` 等），自增 generation 并 Notify 唤醒 leader，脏桶优先排队（`scanner_io.rs:462-488`）；② 维护配置变更（lifecycle/replication 设置时 `record_scanner_maintenance_change`）；③ 运行时配置热更 generation+Notify；④ 集群活动快照变化。
- **集群协调**：`probe_scanner_activity` 汇集本机+peer 的 `ScannerNodeActivity`（instance_id/namespace_generation/maintenance_generation/protocol_version/topology_digest/data_movement_active/dirty usage），拓扑摘要覆盖 pools/sets/drives URL，协议版本不齐拒绝共享缓存锁（`scanner.rs:970-1068`）；**数据迁移（rebalance/decommission）期间推迟周期**（`scanner_io.rs:2226-2374`）；周期结束逐 peer RPC 确认 dirty-usage ack（`scanner.rs:2925-2952`）。

### 3.2 遍历模型

- 主遍历是**全量目录 walk**（tokio::fs::read_dir 递归，`scanner_folder.rs:1915-2234`），不走 metacache；metacache/`list_path_raw` 仅用于 abandoned children 跨盘核查（:2528-2792）。
- 三级并发：leader → per-set（信号量默认 4）→ per-disk 桶扫描（默认 4）→ 单盘递归；每桶每 set 缓存锁 `.scanner-cycle.lock.pool-N.set-M`（锁丢失取消该桶扫描，锁竞争重排队）；每盘单扫描准入（本地盘也走信号量，`scanner_io.rs:3246-3274`）。
- 桶顺序：shuffle 后按 dirty → 未缓存 → 已缓存重排（`scanner_io.rs:2947-2949,462-488`）；目录内按名字排序 + resume 提示旋转（`scanner_folder.rs:333-359`）。
- **断点续扫**：`DataUsageScanCheckpoint{version,resume_after,reason}` 持久于缓存 info（`data_usage_define.rs:68,293-307`）；预算耗尽/取消写入，恢复有 Used/Stale/NoHint 指标；续扫单位是目录（无跨周期对象级分页）。
- erasure 语义：发现 `xl.meta` 即对象边界不下钻；UUID data-dir 候选最多探测 64 entry；有数据无元数据 → 记 failed + 高优 heal；symlink 目录忽略/环跳过。
- 协作让出：每 N 对象（默认 128）`yield_now`。

### 3.3 大桶跳过策略（对标 MinIO compaction）

1. 缓存当前性复用：桶与扫描计划未变（name/source/snapshot_complete/plan digest/next_cycle/leader_epoch/cache_key_format 全匹配）整桶跳过（`scanner_io.rs:1062-1109`）。
2. compacted 目录 16 周期轮换窗口：`hash mod (next_cycle, 16)` 命中才重扫，否则从旧缓存拷贝（`scanner_folder.rs:74,2429-2442`）。
3. compaction 阈值：子项 <500 或纯对象叶子压缩为单 entry；子文件夹 ≥2500（根 10000）预压缩；children ≥10000 归约（:75-78,2314-2340,2846-2887）。
4. 失败对象 TTL 跳过：86400s/最多 10000 条（:88-91,1354-1381）。

与 MinIO master 对比：MinIO 的跳过策略同样是 hash-mod-16 周期 + compaction 阈值树（500/10000/2500），**bloom filter 已从 master 删除**。RustFS 的常量与结构与 MinIO 现状同源（MinIO 未采用跨盘 dirty-generation 优先，RustFS 额外多两层跳过——plan digest 与缓存当前性校验）。

### 3.4 data usage 统计

- 维度：每目录 entry（size/objects/versions/delete_markers/大小直方图/版本直方图/复制统计/failed_objects/per-tier stats/children/compacted，`data-usage/src/data_usage.rs:661-679`）；每对象 SizeSummary（含 per-ARN 复制目标统计、tier 统计，tier 分类：transitioned 完成记入其 tier 否则按 storage class，free version 不计）；桶级 `BucketUsageInfo`；集群级 `DataUsageInfo`（含 scanner_cycle/scanner_epoch 围栏 + usage_snapshot_complete）。
- 存储：每桶每 set `{bucket}/.usage-cache.bin`（主 + `.bkp` 备份 + CAS 重试）；权威集群快照 `buckets/data-usage/data-usage.json`（每 10 周期同步 `.bkp`，legacy 路径兼容）；陈旧快照拒绝写入（epoch/cycle/last_update 三重判定）；被竞争 superseded 的观测快照另存 `data-usage-observed.json`。
- 消费：`replace_bucket_usage_memory_from_info` 刷新桶用量内存 + 两层缓存失效（`scanner.rs:4142-4152`）→ bucket stats/quota/admin account_info/system；写路径内存实时叠加 overlay；启动读快照判断冷缓存跳过启动延迟。
- 未完成 multipart 不参与统计（与 MinIO 一致，MinIO 也不扫 multipart 桶）。

### 3.5 ILM 集成

- 每对象 `ScannerItem::apply_actions`（`scanner_folder.rs:747-1032`）：`Evaluator::new(lifecycle).with_lock_retention(...).with_replication_config(...).eval()` 批量评估。
- 已实现动作（IlmAction 全集，`common/src/metrics.rs:34-45`）：expiry 删除（Delete/DeleteRestored/DeleteRestoredVersion）、全版本删除（DeleteAllVersions/DelMarkerDeleteAllVersions，处理后停止后续版本）、transition（Transition/TransitionVersion，tier 列表运行时读取）、noncurrent 批量（DeleteVersionAction → `enqueue_by_newer_noncurrent`）、free-version 清理（`enqueue_free_version`）、object-lock retention 约束。**与 MinIO 的 9 个 ILM 动作一一对应**。
- 执行模型：scanner 是"发现与入队"角色（expiry 队列/transition 队列在 ecstore `bucket_lifecycle_ops.rs`），动作由 worker 池消费——与 MinIO globalExpiryState/globalTransitionState 同型。
- AbortIncompleteMultipartUpload 不在 scanner/ILM 内执行（MinIO 同样不在：`internal/bucket/lifecycle/rule.go` 有 FIXME，实际由 `erasureSets.cleanupStaleUploads` 全局例程承担）；RustFS 由 ecstore 独立后台任务 `init_background_stale_multipart_upload_cleanup`（`bucket_lifecycle_ops.rs:3289-3320`）+ 桶删除时 on-demand。
- 集成测试覆盖：transition+restore、free-version、noncurrent、delete-marker、0-day、后台扫描过期（`scanner/tests/lifecycle_integration_test.rs:1071-2095`）。

### 3.6 heal 候选生产（scanner 侧）

- 抽样：`hash mod_alt(next_cycle/prob_div, 1024/prob_div)`，进入 compacted 分支重扫时 prob_div=16 等效概率 ×16（与 MinIO 同款补偿，`scanner_folder.rs:125-127,2117-2122`）。
- deep/normal：周期级 `get_cycle_scan_mode`（bitrot_cycle 默认 30d，`scanner.rs:1626-1657`）→ 对象级带 `HealScanMode::Deep`；新鲜对象（60s 内修改）降级 Normal（:146-155）；状态持久 `.background-heal.json`（`BackgroundHealInfo{bitrot_start_time,bitrot_start_cycle,current_scan_mode}`，与 MinIO 同路径同结构）。
- scanner 只入队不内联执行（内联 heal 已移除，兼容旗标仅告警，`scanner_folder.rs:411-427`）；`HealScanMode::Deep` 只是标记，bitrot 校验读发生在 heal 消费端（ecstore Deep 路径）。
- 元数据损坏 → 高优 heal（`classify_get_size_failure` → HealMetadata）；abandoned children → list_path_raw quorum 核查 + 桶级/对象级高优 heal；healing 盘粘性跳过（`should_heal` :1628-1648）。
- pending-heal 账本：heal 通道满被拒持久化到缓存 info，下轮重试。
- 复制 heal：`queue_replication_heal` → replication 队列（走 replication 通道而非 heal channel）；per-ARN 复制用量统计。

### 3.7 remote_scanner RPC 协议（RustFS 特有）

请求 ≤16KB msgpack（version/request_id/server_epoch/session_id/session_sequence/bucket/next_cycle/leader_epoch/scan_plan_digest/skip_healing/scan_mode/budget）；帧 ≤2MB、HMAC-SHA256 逐帧认证（域 `rustfs-ns-scanner-frame-v3`）；进度心跳 1s（预算模式 250ms）；阶段播报 Scanning→Persisting；RPC 生命周期上限 24h、断连宽限 2min；防重放 session+sequence 缓存（容量 65536）；服务端校验 leader fence 与持久化 cycle 一致 + 每 5s fence 复验；结果 Complete/Partial/NamespaceNotFound/CycleAhead；不支持 v4 协议的远端盘回退 leader 本地扫描（`remote_scanner.rs` 全文件；`scanner_io.rs:2750-2812`）。

### 3.8 限速/预算/热更/观测

- DynamicSleeper 比例退避（速度档 fastest/fast/default/slow/slowest，同 MinIO 五档参数）；idle_mode 总闸；前台 S3 读流量每请求 10ms 封顶 250ms 额外退避。
- 周期预算 ScannerCycleBudget：max_duration/max_objects/max_directories（默认 0=不限），partial 周期仍推进 cycle 计数。
- runtime_config 三层来源（env > config > default）逐字段来源标记（Env/Config/ScannerCompatConfig/Default），admin `PUT /v3/config` 热更 → generation+Notify 即时生效；`GET /v3/scanner/status` 返回 enabled/freshness(fresh/stale/unknown)/metrics/cycle_schedule/runtime_config；`GET /v3/ilm/expiry/status` 返回 expiry 队列/worker/missed/blocked。
- 指标：leader lock、周期 complete/partial/deferred/superseded、versions scanned、per-source（Usage/Lifecycle/BucketReplication/SiteReplication/Heal/Bitrot/Alerts）checked/executed/queued/missed、checkpoint set/used/stale、当前路径（per-disk+bucket 实时）、缓存 save 系列、并发系列、告警（excess versions/version size/folders）。

---

## 4. 与 MinIO 逐项对标

### 4.1 heal 触发通道对照

| MinIO 通道 | RustFS 对应 | 状态 |
|---|---|---|
| A. 手动 admin heal（healSequence，clientToken/forceStart/forceStop） | heal channel Start/Query/Cancel + 集群 coordinator + envelope 重放防护 | ✅ 等价且增强（集群路由）；序列语义差异见 §6 HS-06 |
| B. 常驻后台 heal 队列（newBgHealSequence + healRoutine worker 池） | HealManager 常驻调度器 + 优先级队列 + bulkhead | ✅ 等价且增强 |
| C. 新盘/换盘自动 resync（monitorLocalDisksAndHeal 10s + healFreshDisk + healingTracker + waitForFormatErasure 握手） | auto disk scanner（10s）+ replacement_readiness + durable intent/proof 状态机 + heal_replacement_format | ✅ 等价且增强（identity fence + completion proof；MinIO 的 tracker 面向对外可见性更强，见 §6 HS-07） |
| D. MRF（队列 100k + 持久化 list.bin + shutdown 回放 + 读路径 corrupt 投递） | read-repair（Low+TTL 去重）+ 写路径 convergence heal 部分承担；`HealType::MRF` 执行体无生产入口 | ⚠️ 部分等价（§6 HS-01） |
| E. Scanner 抽样 heal（1/1024 + compacted ×16 补偿）+ abandoned children | 同款抽样 + ×16 补偿 + abandoned children + pending-heal 账本 | ✅ 等价且增强（账本） |
| F. 读路径内联触发 → MRF（GetObject part 缺失/损坏、元数据重建 missingBlocks>0） | read repair（missing_shards/decode_error/metadata_read_error 三入口） | ✅ 等价（入 heal 队列而非 MRF 队列） |

### 4.2 对象级 heal 语义对照

| 特性 | MinIO | RustFS | 状态 |
|---|---|---|---|
| mod-time quorum 仲裁 | listOnlineDisks | 同 | ✅ |
| ETag 多数派兜底（时钟漂移） | filterDisksByETag | `filter_by_etag`/`quorum_etag`（heal.rs:525-567） | ✅ 已亲验 |
| cannotHeal 的 ETag 豁免 | ETag 全一致豁免重试 | heal.rs:679 | ✅ |
| Normal=CheckParts（stat）/ Deep=VerifyFile（bitrot） | 是 | `disks_with_all_parts` 按 scan_mode（ops/heal.rs:562-572,978-1024） | ✅ |
| Normal 检出 corrupt 自动升 Deep 重试一次 | erasure-healing.go:1101-1106 | ops/heal.rs:2022-2031 | ✅ |
| dangling 判定（not-found > parity）+ 删除审计 | isObjectDangling/deleteIfDangling | `dangling_delete_safety`（:1488）+ scanner HEAL_DELETE_DANGLING | ✅（审计 tags 细节有差异） |
| 孤儿 data-dir/inline 清理（CleanAbandonedData） | CheckAbandonedParts（scanner 抽中 + admin Remove 时显式调用） | heal 路径内 `reclaim_orphan_data_dirs_best_effort`（:1428）；独立 API 三层 NotImplemented | ⚠️ 部分等价（§6 HS-02） |
| 版本化/delete-marker heal | HealObject versionID；nullVersionID 特判 | 逐版本枚举 + delete-marker latest heal（B5 回归） | ✅ |
| 对象级 healing 元数据标记（x-minio-healing，RenameData 跳过版本清理） | 有 | 无对象级标记；依赖盘级 healing.bin + NSLock + rename 语义 | ⚠️ 评估项（§6 HS-12） |
| Distribution/Index 一致性三处防线 | 有（manual modification 拒绝） | 目标盘格式结果全 ok 校验 + 身份围栏 | ✅（粒度不同） |
| 无 parity（EC:0）对象 | bitrot 不可恢复处理 | 判不可恢复（:700-726）+ 写入自校验 | ✅ 增强（写路径自校验） |
| 三层分布不一致拒绝 heal | 有 | heal_walk 归一化 + 页界防御 | ✅（实现方式不同） |
| multipart 孤儿对账 | CheckAbandonedParts 承担 | 显式 NotImplemented（由 lifecycle 清理承担） | ⚠️ §6 HS-02 |
| suspended/decommissioned pool 处理 | IsSuspended 跳过 | deferral 语义（store/heal.rs:192-207，PR #5876） | ✅ |
| heal 与并发删除互斥 | NSLock + healing 标记 | NSLock + 写锁 | ✅ |

### 4.3 新盘 resync 对照

| MinIO | RustFS | 状态 |
|---|---|---|
| waitForFormatErasure 四类可恢复错误无限等待握手 | startup 盘解析 + renew_disk 重连路径 | ✅（模型不同：RustFS 不在启动时阻塞等待 format） |
| HealFormat NSLock + errNoHealRequired + refFormat 不一致拒绝 | `heal_format`/`heal_replacement_format` fail-closed + 目标槽位限定（PR #1787 语义） | ✅ 增强 |
| per (pool,set) 分布式锁防并发 resync | set 级队列去重 + bulkhead（manager.rs:2854-2889） | ✅ |
| 全新集群检测（待 heal 盘数==总盘数不触发） | replacement_readiness（独立挂载点/物理设备校验，非 root） | ✅ 增强 |
| healingTracker（.healing.bin：Bytes/Items 计数、QueuedBuckets/HealedBuckets、Resume 快照、RetryAttempts ≤4、HealID 联动、diskID 变更重置） | resume/checkpoint schema 化持久层 + durable intent/proof（per-task 文件，CAS） | ✅ 等价且增强（崩溃窗口补齐）；但**对外快照可见性**弱于 MinIO（§6 HS-07） |
| 跳过 heal 开始后新写入版本（ModTime > Started） | 无同款过滤 | ⚠️ §6 HS-13 |
| 跳过 ILM 已过期版本（filterLifecycle） | 无同款过滤 | ⚠️ §6 HS-13 |
| worker 数 max(GOMAXPROCS,NR)/4 下限 4，heal:drive_workers 覆盖 | 页内并发 8（Deep/AutoHeal 强制 1）+ per-set bulkhead | ✅（参数模型不同） |
| 每 entry waitForLowHTTPReq 让路 | mainline throttle（前台利用率门控） | ✅ 增强 |
| heal 范围含 `.minio.sys/config`、`.minio.sys/buckets` 两个伪桶；最新桶优先 | ErasureSet 任务逐 bucket 预处理（含 meta bucket 语义由 heal_bucket 承担） | ✅（顺序无"最新优先"） |
| 失败整体重试 ≤4 次（resetHealing + errRetryHealing） | schedule_retry 复位双层 + 可恢复重试 ≤3 | ✅ |

### 4.4 scanner 对照

| MinIO | RustFS | 状态 |
|---|---|---|
| 集群单 leader（globalLeaderLock） | leader.lock + 持久化 leader-epoch CAS 围栏 | ✅ 增强（epoch 围栏防脑裂，MinIO 无持久化 epoch） |
| `.bloomcycle.bin` 只存 cycle（bloom 已删除） | 同路径存 cycle+leader_epoch（RSCYC001） | ✅ 对齐（v1 误判已修正） |
| folderScanner hash-mod-16 + compaction（500/10000/2500） | 同款常量 + plan digest + 缓存当前性校验 + dirty 优先 | ✅ 增强 |
| 每盘扫描并行 ≤GOMAXPROCS；healing 盘排除 | per-set/per-disk 信号量 + healing 盘粘性跳过 | ✅ |
| scannerSleeper（factor 2/max 1s，speed 档热更） | DynamicSleeper 同款 + idle_mode + 前台读退避 | ✅ 增强 |
| idle 语义：`scanner:idle_speed=on`（空闲时段才节流，忙时全速） | `RUSTFS_SCANNER_IDLE_MODE=true`（启用限速总闸） | ⚠️ 语义方向相反，§6 HS-14 |
| applyActions 顺序（heal→ILM→复制→告警） | apply_actions 同序（heal 候选→ILM→复制 heal→告警） | ✅ |
| ILM 9 动作 + 批量评估 + DeletePrefixObject 优化 | 同 9 动作 + 批量评估 + expiry 队列 | ✅（DeleteAllVersions 是否单调用优化未逐行核） |
| abandoned children（listPathRaw minDisks=N/2 发现漏写盘） | list_path_raw + quorum 核查 + 高优 heal | ✅ |
| incomplete multipart 独立例程（6h 间隔/24h 过期，rename 进 .trash） | ecstore 独立后台任务（可配间隔/过期） | ✅（trash 二段清理细节差异，§6 HS-18） |
| usage 维度（size/objects/versions/DM/直方图/复制/tier/bucket 级） | 全覆盖 + 集群快照三重防回退 | ✅ 增强 |
| prefix 级 usage（loadPrefixUsageFromBackend，console 消费） | 缓存内有目录树但仅 flatten 桶级 | ❌ §6 HS-08 |
| 超限事件 s3:ObjectManyVersions/LargeVersions/PrefixManyFolders + 审计 | 仅指标 alert_excess_*（默认 100/1TiB/65538 vs MinIO 100/1TB/50000） | ⚠️ §6 HS-04/HS-17 |
| scanner 指标 v3（bucket_scans/directories/objects/versions/last_activity） | rustfs_scanner_* 全套 + freshness | ✅（命名体系不同） |
| TraceScanner / realtime metrics（mc admin scanner status/trace） | 无 trace 通道；/v3/scanner/status 自有结构 | ⚠️ §6 HS-03 |

### 4.5 admin/CLI/API 面对照

| MinIO | RustFS | 状态 |
|---|---|---|
| `POST /minio/admin/v3/heal/...` start/status/cancel | `POST /rustfs/admin/v3/heal/...` 同三态 | ✅（路径前缀不同属预期） |
| `HealStartSuccess`/`HealTaskStatus`/`HealResultItem`/DriveState | 同名字段 JSON 兼容 | ✅ |
| `POST /v3/background-heal/status`（BgHealState 聚合） | 同路径 + degraded 语义 + operations 矩阵 | ✅ 增强（MRF per-endpoint 子状态无，因无 MRF） |
| `GET /v3/healthinfo` 每 drive `HealInfo *HealingDisk` | 无同款 healthinfo heal 字段（replacement-recovery v4 承担部分） | ⚠️ §6 HS-07 |
| madmin 客户端 HealStart/HealStatus/BackgroundHealStatus/ScannerStatus 方法 | 仅 wire 类型，无客户端方法 | ❌ §6 HS-05 |
| mc admin heal --pool/--set、--scan-mode、--force-start/stop | HealOpts 全字段支持（pool/set/scanMode/forceStart/forceStop） | ✅（服务端就绪；缺 mc 侧入口，HS-05） |
| ErrHealAlreadyRunning / ErrHealOverlappingPaths 类型化错误 | 去重合并 + 驱逐语义；无类型化重叠拒绝 | ⚠️ §6 HS-06 |
| 结果 backpressure（maxUnconsumedItems=1000、10s 保活流式、24h 未消费 abort） | 快照式查询（1024 条 + 8MiB 截断 + 10min 保留） | ⚠️ §6 HS-06 |
| `mc support inspect`/healing-bin 离线 dump | 无（inspect.rs 存在但 healing dump 未确认） | ⚠️ P3 |

### 4.6 观测面对照

| 维度 | MinIO | RustFS | 状态 |
|---|---|---|---|
| heal 指标 | minio_heal_objects_total/heal_total/errors_total/time_last_activity + v3 drive_health 2=healing | rustfs_heal_* 全套（admission/queue delay/running/throttle/page concurrency） | ✅（RustFS 缺 drive_health=healing 单一 gauge 等价物；DiskInfo.healing 已赋值） |
| scanner 指标 | v3 6 个 + realtime 18 项 | rustfs_scanner_* 全套 + per-source 维度 | ✅ |
| ILM 指标 | v3 5 个（expiry/transition pending/active/missed + versions_scanned） | ilm expiry status API + scanner per-source | ✅（指标与 API 形态不同） |
| trace | TraceHealing/TraceScanner 两通道 | 无 | ❌ §6 HS-03 |
| 审计 | HealObject 事件、dangling 删除审计、scanner:manyversions 等 | 结构化日志（event style）+ 指标；无 audit log 事件 | ⚠️ §6 HS-04 |
| 进度 | healingTracker Bytes/Items/QueuedBuckets/当前对象 + usage-cache 总量基线 | HealProgress{scanned/healed/failed/bytes/current_object/percentage}；bytes_processed 注释为 0、estimated_completion_time 恒 None | ⚠️ §6 HS-07 |

### 4.7 配置面对照（默认值）

| MinIO | RustFS | 备注 |
|---|---|---|
| `heal:bitrotscan`（默认 off；on=每轮；Nm=N×30×24h） | `heal.bitrot_cycle` / `RUSTFS_SCANNER_BITROT_CYCLE_SECS`（默认 30d=2592000s；0/on=每轮 Deep，off=禁用） | ✅ 同语义（RustFS 默认 30d，MinIO 默认 off——**默认值不同**，RustFS 更激进） |
| `heal:max_io=100`/`max_sleep=250ms`（waitForLowIO） | mainline throttle 阈值 80%/80%、max_sleep 250ms | ✅ 同型（阈值模型不同） |
| `heal:drive_workers`（默认 -1 自动） | 页内并发 8 + per-set 1 | ✅ 同型 |
| `_MINIO_HEAL_WORKERS`（GOMAXPROCS/2） | `RUSTFS_HEAL_MAX_CONCURRENT_HEALS=4` + `_MAX_CONCURRENT_PER_SET=1` | ✅ |
| `_MINIO_AUTO_DRIVE_HEALING`（on） | `RUSTFS_HEAL_AUTO_HEAL_ENABLE=true` | ✅ |
| `_MINIO_SCANNER`（on） | `RUSTFS_SCANNER_ENABLED=true` | ✅ |
| `scanner:speed` 五档（default=2x/1s/1m） | 同五档同名同参数 | ✅ |
| `scanner:idle_speed`（on） | `RUSTFS_SCANNER_IDLE_MODE`（true） | ⚠️ 语义方向（HS-14） |
| `scanner:alert_excess_versions=100` | 100 | ✅ |
| `scanner:alert_excess_folders=50000` | 65538（兼容 PBS 布局） | ⚠️ HS-17 |
| `ilm:expiration_workers=100`/`transition_workers=100` | ecstore expiry/transition worker 池（键见 ilm 子系统） | ✅（默认值未逐项核对） |
| `api:stale_upload_cleanup_interval=6h`/`expiry=24h` | ecstore 后台任务 env 可配 | ✅（默认值未逐项核对） |
| —（无） | `RUSTFS_HEAL_QUEUE_SIZE=10000`、`_TASK_TIMEOUT_SECS=300`、`_INTERVAL_SECS=10`、`_LOW_PRIORITY_MERGE/DROP`、`_PAGE_*`、`_SET_BULKHEAD`、`_MAINLINE_*`、`RUSTFS_SCANNER_CYCLE_MAX_*` 预算、`_MAX_CONCURRENT_SET/DISK_SCANS=4`、`_YIELD_EVERY_N_OBJECTS=128` 等 | RustFS 特有（更细粒度） |

### 4.8 RustFS 超出 MinIO 的部分

1. remote_scanner RPC（扫描执行下放远端 peer 本地，含 HMAC 认证/重放缓存/fence 复验/断连宽限）。
2. 持久化 leader-epoch CAS 围栏 + usage 快照 epoch/cycle 防回退（MinIO 仅锁，无持久 epoch）。
3. 周期预算（max_duration/objects/directories）+ partial 周期推进语义。
4. per-set/per-disk 扫描并发闸 + 每桶每 set 缓存锁。
5. pending-heal 账本（heal 通道满不丢候选）。
6. 换盘 durable intent + completion proof 状态机 + 身份围栏（MinIO healingTracker 无 proof）。
7. mainline throttle 前台压力门控（permit 利用率驱动）。
8. 集群 heal control coordinator + envelope 重放防护 + degraded 显式降级。
9. 写路径 shard bitrot 自校验（EC:0 场景）。
10. dirty-usage 快路径唤醒（写路径即时通知 + 脏桶优先）。
11. heal 运行时可观测矩阵（优先级×来源 operations snapshot）。
12. workload admission 联动（heal 调度器读前台压力快照）。

---

## 5. 差距与改进清单

分级定义：P1=行为/运维对齐缺口（影响生产运维或工具链兼容）；P2=完善性（功能在但缺一角）；P3=清理/低风险。每项含现状证据、MinIO 行为、影响、建议、验收方式。

### P1（8 项）

**HS-01 MRF/ECDecode/Metadata 三类 heal 任务无生产触发入口，HealEvent 未接线**
- 现状：`HealType::MRF/ECDecode/Metadata` 执行体完整（task.rs:1700-2156）但全仓库无生产触发方；`HealEvent`/`HealEventHandler`（event.rs:50-367）crate 外零引用（已亲验 grep）；channel 转换只产生 Cluster/Object/Bucket/Prefix/ErasureSet（channel.rs:566-601）。
- MinIO：mrf.go 独立 MRF 队列（容量 100k，满丢弃计数）、进程退出 msgp 持久化 `.heal/mrf/list.bin` + 启动回放、入队 <1s 延迟 1s（等网络恢复）、healSleeper 限速；读路径 GetObject part 缺失/损坏、元数据重建 missingBlocks>0、Put 部分成功、DeleteObject、multipart、peer client 共 7+ 投递点。
- 影响：RustFS 的 read-repair + 写路径收敛覆盖了主场景，但缺少：① 事件驱动的 Urgent ECDecode 重建入口（ecstore 解码失败时目前仅 Low read-repair）；② Metadata-only heal 入口（scanner HealMetadata 分类存在但走普通对象 heal）；③ MRF 队列持久化（重启丢未消费修复意图——scanner pending-heal 账本部分缓解）。
- 建议：三选一决策——(a) 接线 HealEvent（在 ecstore 解码失败/metadata 损坏点发事件）+ 实现持久化重试账本；(b) 删除 MRF/ECDecode/Metadata 死代码只保留文档说明；(c) 保留执行体、把 HealEvent 降级为内部 API。推荐 (a) 但需先量化 read-repair 是否已覆盖解码失败场景的响应时间要求。
- 验收：解码失败 → Urgent heal 请求链路 e2e；重启后 pending 修复意图回放；HealEvent 环形缓冲指标。

**HS-02 CheckAbandonedParts 三层 NotImplemented（abandoned data 独立对账入口缺失）**
- 现状：`set_disk/ops/heal.rs:2052-2056`、`core/sets.rs:1144-1148`、`store/heal.rs:258-266` 三层显式 `Err(NotImplemented)`（已亲验），注释"intentionally retained above the set layer until there is a concrete caller"。
- MinIO：`CheckAbandonedParts` → 每盘 `CleanAbandonedData`：读 xl.meta → 列 UUID data-dir + inline entries → 与 getDataDirs 差集 → 删多余 data-dir/inline 并重写 xl.meta；由 scanner 抽中 heal 与 admin heal Remove 时显式调用。
- 影响：RustFS heal 路径内 `reclaim_orphan_data_dirs_best_effort`（:1428）覆盖"heal 时回收孤儿目录"，但 ① 无独立触发点（MinIO 在对象未到 heal 阈值时也能清 abandoned data）；② inline data 孤儿条目清理未确认；③ multipart 孤儿对账明确不做（设计决定，由 lifecycle 承担）。
- 建议：评估把 `reclaim_orphan_data_dirs_best_effort` 提升为 heal_object 固定步骤（若尚非）+ 实现 HealOperations::check_abandoned_parts 真实现（调用同一回收逻辑），或明确文档化"由 lifecycle 承担"并关闭 API 面。
- 验收：构造 data-dir/inline 孤儿 → scanner 抽样/admin heal 后被清理；三层 API 返回成功或显式 NotSupported 文档化。

**HS-03 heal/scanner trace 通道缺失**
- 现状：TraceHealing/TraceScanner 零命中（已亲验 grep 全仓库）。
- MinIO：`madmin.TraceHealing`（mc admin trace --healing，FuncName=heal.Bucket/heal.Object/heal.CheckAbandonedParts，带 dry/remove/mode/version-id/disks/bytes）、`TraceScanner`（mc admin scanner trace，支持 --filter-size/--response-duration）。
- 影响：无法实时观测单个 heal/scanner 动作的耗时与参数；排障只能靠指标聚合与日志。
- 建议：在 heal channel 执行与 scanner folder/item 处理埋点，接入现有 admin trace 订阅面（若 rustfs 已有 trace 基建则复用，无则按 madmin TraceType 扩展）。
- 验收：mc 等价工具能订阅 heal/scanner trace 流。

**HS-04 scanner 超限 S3 事件与审计缺失**
- 现状：仅 `rustfs_scanner_excess_*_total` 指标（versions 100/version size 1TiB/folders 65538）。
- MinIO：发 `s3:ObjectManyVersions`（>100 版本）、`s3:ObjectLargeVersions`（累计 >1TB）、`s3:PrefixManyFolders`（>50000 子目录）事件（UserAgent: Scanner）+ scanner:manyversions/largeversions/manyprefixes 审计。
- 影响：依赖事件订阅做容量治理的用户（console/外部审计）收不到告警。
- 建议：scanner_folder 告警点接入 notify 事件发布（复用 lifecycle 事件通道语义）。
- 验收：配置桶通知后超限对象触发事件。

**HS-05 madmin 客户端方法缺失**
- 现状：`crates/madmin/src/heal_commands.rs` 只有 wire 类型（HealDriveInfo/Infos/HealResultItem）；无 HealStart/HealStatus/BackgroundHealStatus/ScannerStatus 客户端方法。
- MinIO：madmin-go 提供完整客户端；mc admin heal/scanner/status/trace 都建立在上面。
- 影响：mc 等管理工具无法直接对接 RustFS heal/scanner 管理面；自动化运维只能手写 HTTP。
- 建议：按 madmin-go 接口形状补客户端（服务端已就绪，纯客户端工作）。
- 验收：用 madmin 客户端完成 start→query→cancel 全流程。

**HS-06 admin heal 序列语义与 MinIO 差异**
- 现状：重复/重叠请求被去重合并（返回 canonical task_id）或驱逐；无 ErrHealAlreadyRunning/ErrHealOverlappingPaths 类型化错误（已亲验：manager.rs:1309 的 already_running 是幂等启动保护，非 admin 语义）；结果为快照式查询（1024 条/8MiB 截断/10min 保留），非 MinIO 的流式增量（clientToken 拉增量 + maxUnconsumedItems=1000 backpressure + 10s 保活 + 24h 未消费 abort）。
- 影响：mc admin heal 的交互模型（长连接拉增量）对 RustFS 表现为多次快照轮询；自动化脚本难以区分"已合并"与"新启动"。
- 建议：① 增量语义：channel query 支持自上次 clientToken 起的 items 增量（或 cursor）；② 重叠请求返回类型化错误码（或 receipt 中显式 merged_into 字段——现有 alias 机制已有基础）；③ forceStart 先停旧再启新语义核对。
- 验收：madmin 兼容客户端按 MinIO 模式轮询能取得全量 items。

**HS-07 healing 进度与盘级 healing 状态对外可见性不足**
- 现状：bytes 恢复进度 `progress.bytes_processed = 0 // set to 0 for now`（erasure_healer.rs:967）；`HealProgress::estimated_completion_time` 恒 None、`HealStatistics::add_healed_objects` 未写入（progress.rs:38,135-139 零调用）；healthinfo 无每盘 HealInfo 等价（MinIO HealingDisk：BytesDone/Failed/Skipped、ObjectsTotal 基线、QueuedBuckets/HealedBuckets、Resume 快照、当前 object）；v3 指标无 drive_health=2(healing) 单一 gauge 等价。
- 影响：换盘重建（可能数小时~天）期间运维无法回答"进行到哪/还剩多少/预计何时完成"。
- 建议：① erasure set heal 统计 bytes（heal_object 返回对象大小已可得）；② 从 usage-cache 读对象总量基线（MinIO 同款做法）；③ admin healthinfo/背景状态暴露每盘 healing 快照（DiskInfo.healing 已有，补聚合暴露）；④ ETA 由基线+速率推导。
- 验收：换盘重建中 admin 可见 bytes 进度与 ETA；mc info 等价输出 Healing 标志。

**HS-08 prefix 级 usage 未暴露**
- 现状：DataUsageCache 内目录树 entry 存在（hash_path 组织），但 `dui()` 只 flatten 到桶名（data_usage_define.rs:858-915）。
- MinIO：`loadPrefixUsageFromBackend`（30s cache）从每 set `.usage-cache.bin` 聚合 prefix usage，console 桶前缀统计消费。
- 影响：console/前端无法展示前缀级用量；大桶定位"哪个前缀占空间"无 API。
- 建议：实现 flatten 前缀查询 API（数据已在缓存内，纯聚合与暴露工作）。
- 验收：ListBuckets/PrefixUsage API 返回与前缀过滤匹配的统计。

### P2（9 项）

**HS-09 get_disk_status 恒返回 Ok（唯一 TODO）**：`crates/heal/src/heal/storage.rs:930-943`（已亲验）。当前无生产调用方（低风险）。建议：删除该方法或接 ecstore disk 状态真实现（DiskStatus 枚举已定义）。

**HS-10 HealStorageAPI 约 1/3 方法为死代码**：get_object_meta/get_object_data/put_object_data/delete_object/verify_object_integrity/ec_decode_rebuild/get_disk_status/format_disk/heal_bucket_metadata/get_object_size/get_object_checksum/list_objects_for_heal（非分页版，自带 memory_heavy 警告）均 0 调用方。建议：随 HS-01 决策一并清理或接线（死接口误导后续维护者以为存在调用路径）。

**HS-11 bitrot 自检缺失**：MinIO 启动时 bitrotSelfTest 对四算法已知向量自检失败即 Fatal（防静默数据损坏）。RustFS 无等价（已亲验 grep）。建议：启动时对 HighwayHash256S 等在用算法做已知向量自检（低成本高价值）。

**HS-12 对象级 healing 元数据标记评估**：MinIO heal 期间对象打 `x-minio-healing:true`，RenameData 据此跳过版本清理/legacy purge（漏掉会导致 heal 与并发删除互毁）。RustFS 无对象级标记（已亲验 grep object.rs 无 healing 分支），依赖 NSLock + rename 语义。建议：审计 RustFS rename 提交路径是否存在"heal 提交与并发 delete/version 清理竞争"窗口；若无则文档化差异，若有则补标记等价机制。

**HS-13 erasure set heal 无"跳过新写入/ILM 已过期版本"过滤**：MinIO resync 跳过 ModTime>tracker.Started 的版本（避免 heal 追新写入尾巴）与 ILM 已过期版本（避免白做）。RustFS erasure_healer 未实现同款过滤（按版本 dedup 有，时间/ILM 过滤无）。影响：重建尾部长尾（持续写入的桶 heal 完成判定被新版本推迟）与无效 heal 工作量。建议：disk-walk 枚举处加 started_at 时间过滤 + evaluator 预检。

**HS-14 scanner idle 语义方向与 MinIO 相反**：MinIO `scanner:idle_speed=on`（默认）= 集群空闲时才节流、忙时全速；RustFS `RUSTFS_SCANNER_IDLE_MODE=true`（默认）= 限速总闸（false=完全不休眠）。两者默认行为可能相近（都限速）但参数语义不可互换，迁移文档需显式说明；若追求 mc config 兼容需重命名/重语义。建议：先文档化差异，评估是否对齐语义。

**HS-15 alert_excess_folders 默认值差异**：RustFS 65538（兼容 PBS/Proxmox 布局，scanner_folder.rs:79）vs MinIO 50000。行为差异默认即触发阈值不同。建议：文档化（保留 65538 有本地理由）。

**HS-16 单机默认周期钩子未启用**：`single_disk_default_cycle_secs(_features) -> None` 恒空（scanner.rs:1428-1430），单机部署无专属默认周期覆盖。建议：决定单机默认周期策略后启用或删除钩子。

**HS-17 DeleteAllVersions 批量优化核对**：MinIO 用 DeletePrefix+DeletePrefixObject 单调用代替逐版本 fan-out。RustFS expiry 队列路径是否同款优化未逐行核实（集成测试覆盖行为正确性）。建议：核对 `apply_expiry_rule` 全版本删除路径，若无前缀单调用优化则评估补齐。

### P3（3 项）

**HS-18 trash/临时目录二段清理细节核对**：MinIO `.minio.sys/tmp/.trash` 清理（delete_cleanup_interval 默认 5m + deleteCleanupSleeper）与 stale uploads rename-into-trash 二段式。RustFS 有 delete_tail_activity.rs 与 stale multipart 任务，二段语义是否完整对齐未逐行核实。建议：对照补齐或文档化。

**HS-19 root heal 直连死路径清理**：`should_handle_root_heal_directly` 恒 false（admin/handlers/heal.rs:1200-1202，测试锁定），store.heal_format 直连分支不可达。建议：删除死分支或恢复直连路径作为集群协调失败的降级。

**HS-20 兼容旗标与死指标清理**：`RUSTFS_SCANNER_INLINE_HEAL_ENABLE`（开启仅告警）+ `rustfs_scanner_inline_heal_total` 死指标 + `rustfs_common::metrics` 中 scanner 域代码分层迁移（backlog #1843 已登记）。建议：随分层迁移一并清理。

### 按设计不追平（7 项，记录以防后续误判为缺口）

1. **bloom filter**：MinIO master 已删除；RustFS `.bloomcycle.bin` 复用为 cycle/epoch 围栏与 MinIO 现状一致。
2. **scanner 集群单 leader**：双方一致；RustFS 额外有 epoch 围栏。
3. **heal 不发 S3 bucket notification**：双方一致（heal 结果走 admin status）。
4. **incomplete multipart 不在 scanner/ILM 内执行**：双方一致（独立后台例程）。
5. **内联 heal 移除**：RustFS 有意为之（scanner 只入队），MinIO 的 applyHealing 内联路径不做对标。
6. **heal 序列常驻保活（10s 空白回写）**：RustFS 快照式查询模型不同，按 HS-06 处理增量语义即可，不复制流式保活。
7. **`.trash`/`tmp-old` 路径名兼容**：RustFS 布局常量独立，不逐字对齐 MinIO 路径。

---

## 6. 配置默认值总表（RustFS）

heal（env 前缀 `RUSTFS_HEAL_`，`crates/config/src/constants/heal.rs`，消费于 `manager.rs:724-800`）：

| 配置 | 默认 | 热更新 |
|---|---|---|
| AUTO_HEAL_ENABLE | true | 否 |
| QUEUE_SIZE | 10000 | 否 |
| INTERVAL_SECS | 10 | 否（启动时固定） |
| TASK_TIMEOUT_SECS | 300 | 否 |
| MAX_CONCURRENT_HEALS | 4 | 否 |
| MAX_CONCURRENT_PER_SET | 1（≤min(全局,值)） | 否 |
| LOW_PRIORITY_MERGE_ENABLE | true | 否 |
| LOW_PRIORITY_DROP_WHEN_FULL | true | 否 |
| PAGE_OBJECT_CONCURRENCY | 8（Deep/AutoHeal 强制 1） | 否 |
| EVENT_DRIVEN_SCHEDULER_ENABLE | true | 否 |
| SET_BULKHEAD_ENABLE | true | 否 |
| PAGE_PARALLEL_ENABLE | true | 否 |
| MAINLINE_THROTTLE_ENABLE | true | 否 |
| MAINLINE_READ/WRITE_UTILIZATION_HIGH_PERCENT | 80/80 | 否 |
| MAINLINE_MAX_SLEEP_MS | 250 | 否 |
| （总开关）RUSTFS_HEAL_ENABLED | true | 否 |
| admin 子系统 heal.bitrot_cycle | 30d | 是（经 scanner runtime config） |

scanner（admin 子系统 `scanner`，`crates/config/src/constants/scanner.rs` + `ecstore/src/config/scanner.rs` + `runtime_config.rs:527-673`）：

| 键 | env | 默认 |
|---|---|---|
| speed | RUSTFS_SCANNER_SPEED | default（2x/1s/60s） |
| delay / max_wait / cycle / start_delay | RUSTFS_SCANNER_* | 派生/空 |
| cycle_max_duration/objects/directories | …_MAX_* | 0（不限） |
| bitrot_cycle | …_BITROT_CYCLE_SECS | 2592000（30d；0/on=每轮，off=禁用） |
| idle_mode | …_IDLE_MODE | true |
| cache_save_timeout | …_CACHE_SAVE_TIMEOUT_SECS | 30s |
| max_concurrent_set_scans / disk_scans | …_MAX_CONCURRENT_* | 4/4 |
| yield_every_n_objects | …_YIELD_EVERY_N_OBJECTS | 128 |
| alert_excess_versions / version_size / folders | …_ALERT_* | 100 / 1TiB / 65538 |

scanner 内部 env：`RUSTFS_DATA_USAGE_UPDATE_DIR_CYCLES=16`、`RUSTFS_HEAL_OBJECT_SELECT_PROB=1024`、`RUSTFS_SCANNER_DEEP_VERIFY_COOLDOWN_SECS=60`、`RUSTFS_DATA_USAGE_FAILED_OBJECT_TTL_SECS=86400`/`_MAX=10000`、`RUSTFS_LOCK_ACQUIRE_TIMEOUT=5s`、`RUSTFS_SCANNER_ENABLED=true`、`RUSTFS_SCANNER_INLINE_HEAL_ENABLE=false`（兼容告警）。

全部 17 个 scanner 键支持 env > config 双通道 + admin PUT 热更（generation+Notify 即时生效）；heal 运行时参数目前仅 env（无 admin 热更入口，`Arc<RwLock<HealConfig>>` 结构已预留）。

---

## 7. 相关 backlog / 历史索引

- 换盘自动修复系列（已闭环）：backlog #1786（冗余假绿算法）、#1787（目标槽位限定）、#1789（resume 与 healing marker 绑定 replacement 实例）、#1791（黑白盒验收矩阵）。
- #801 DiskInfo.healing 从未赋值（已修复闭环，现 `set_disk/mod.rs:4988` 有赋值链）。
- #1651 Scanner 指标节点/source/bucket-drive 维度（OPEN，本分析 §3.8/§4.6 相关）。
- #1843 crates/common 83% scanner/heal 域代码分层迁移（OPEN，含 HS-20）。
- 代码注释引用的历史缺陷（现已有防护与回归测试）：#856/#799 B7（离线盘误记 healed）、#855/B6/#1033（skip 不得标记完成）、#920（sub-quorum 并集枚举）、#856 B5（按版本续扫）、#5173（bitrot trailing bytes）、#5029（回归节点 stale 版本合并）。
- v1 对标文档：`docs/rustfs-heal-scanner-vs-minio-parity-assessment.md`（本文取代）、落地手册 `docs/rustfs-heal-scanner-vs-minio-improvement-playbook.md`（部分条目已被后续实现超越）。
- 换盘深度分析：`docs/new-disk-replacement-and-healing-deep-analysis-zh.md`、`docs/node-disk-identity-and-healing-analysis-zh.md`。

## 8. 审计方法与局限

- 四路并行审计（heal crate 逐文件、scanner crate 逐文件、ecstore 集成层 wiring、MinIO master 源码研究）+ 主会话对关键"缺失"结论逐条亲验（get_disk_status TODO、HealEvent 零外部引用、.bloomcycle.bin 无 bloom 实现、check_abandoned_parts 三层 NotImplemented、ETag 兜底已实现、trace 通道零命中、already_running 语义）。
- 未逐行核实的点（已在文中标注"未确认/未逐行核"）：DeleteAllVersions 前缀单调用优化（HS-17）、trash 二段清理细节（HS-18）、ilm worker 默认值对照、stale multipart 默认值对照、mc CLI flag 逐字拼写（MinIO 侧）。其中 HS-17 与 HS-18 已于 2026-08-19 完成逐行核实，结论见 §9.2/§9.3。
- MinIO 侧引用以其 master `7aac2a2c5b` 为准；RustFS 侧行号以 2026-08-16 工作区为准，后续演进请以符号名检索为准。

## 9. 落地结果（2026-08-19 更新）

本审计衍生的 14 个子 issue（backlog #1865~#1878）已全部闭环。本节为差距清单 HS-01~HS-20 的最终处置记录，也是下一轮对标重审的增量基线。

### 9.1 已落地（PR 均已合并 main）

- HS-01 MRF 接线 + 持久化修复账本（#1865，PR #6189）：决策选 (a)。common MRF channel（bounded 8192、try_send 永不阻塞）+ heal mrf_queue（100k 条 / 8MiB 双限环形）+ `buckets/.heal/mrf/journal.bin` CRC 持久化回放（torn tail 截断、回放后删除）+ 三投递点（read decode_error→Urgent ECDecode、scanner 元数据损坏→High Metadata、add_partial→Normal）+ `RUSTFS_HEAL_MRF_ENABLE` 一键回退。
- HS-02 abandoned parts/data-dir 对账（#1866，PR #6179）：接通 abandoned 检查入口，保留 dry-run / reclaim 计数。
- HS-03 heal/scanner trace 通道（#1867，PR #6179）：进程内 trace bus + `/v3/trace` admin 流式订阅 + heal task / abandoned-parts / scanner folder / ILM / heal-candidate trace producer。
- HS-04 scanner 超限 S3 事件（#1868，PR #6176）：`s3:Scanner:ManyVersions/LargeVersions/BigPrefix` 三事件 + 24h 边沿冷却；HS-15 阈值差异文档化（`docs/operations/scanner-excess-alerts.md`）。
- HS-05 madmin 客户端一期（#1869，PR #6166）：SigV4 admin 客户端 heal/scanner 方法；增量消费方法待 follow-up（协议已由 HS-06 并入）。
- HS-06 admin heal 增量语义与类型化重叠（#1870，PR #6206）：`sinceSeq/nextSeq/minSeq` 增量游标（wire additive、缺省=全量快照）+ `RUSTFS_HEAL_OVERLAP_POLICY`（默认 merge 不变；minio_error 下 AlreadyRunning/OverlappingPaths 类型化拒绝）+ forceStart 先停旧再启新。
- HS-07 healing 进度可见性（#1871，PR #6179）：data-usage 总量基线 + baseline/current/healed 计数。
- HS-08 prefix usage（#1872，PR #6171）：`GET /v3/usage/{bucket}`。
- HS-11 bitrot 启动自检（#1873，PR #6165）。
- HS-13 heal 跳过过滤（#1875，PR #6179）：过滤命中版本不再计为失败。
- HS-16 单机周期钩子（#1878，PR #6250）：删恒 None 钩子，决策记录见 `docs/operations/heal-scanner-parity-notes-zh.md`。
- HS-09/10/19/20 死代码清理批（#1877，PR #6256）：净 −911 行零行为变更；`get_disk_status` TODO（全仓库唯一产品 TODO）清零；HS-01 联动的 `ec_decode_rebuild`/`get_object_meta` 保留并加 Reserved 注释（MRF 当前经 `heal_object` 执行）。

### 9.2 核对后确认"已实现 / 非缺口"（审计期误判修正，累计四例）

- bloom filter（§0 已修正）：MinIO master 已删除，双方现状一致。
- ETag 兜底仲裁（§0 已修正）：RustFS 已有实现（`set_disk/ops/heal.rs`）。
- HS-17（#1876，2026-08-19 逐行核实后关闭）：DeleteAllVersions 前缀单调用优化 RustFS 已完整实现——`apply_expiry_on_non_transitioned_objects` 对 `delete_all()` 两 action 设 `delete_prefix + delete_prefix_object` 后单次 `delete_object`（`bucket_lifecycle_ops.rs:5047-5056`），SetDisks 分支一次写锁 + 一次全版本 quorum 读 + 内联逐版本 object-lock 检查（`set_disk/ops/object.rs:5566-5612`），与 MinIO `expire.go` 的 `applyExpiryOnNonTransitionedObjects` 逐行对齐。§8 原列"未逐行核实"的本项已有结论：现状即优化路径，无需实现。
- HS-14（#1878，PR #6250 附带核对）：MinIO"idle=空闲才节流"是 2024-01 minio/minio#18734 之前的行为（`scannerIdleMode` 现为静态配置，`idle_speed=on` 默认即始终按速度档节流，"idle"命名是历史残留）；RustFS `RUSTFS_SCANNER_IDLE_MODE` 与 MinIO 当前语义方向一致，且另有 MinIO 没有的前台读退避下限。真实迁移陷阱（变量须 `RUSTFS_` 前缀、`on/off` vs `true/false` 词表、`false` 连前台保护一起关）已文档化于 `docs/operations/heal-scanner-parity-notes-zh.md`。

### 9.3 审计型结论（无需改代码）

- HS-12（#1874，PR #6183）：不存在 MinIO 用 `x-minio-healing` 防御的那类竞争——所有同 (bucket, object) 提交面在同一把对象级 ns 写锁互斥，heal 锁 guard 覆盖 rename 提交全程；交付 2 个并发不变量回归测试 + `docs/operations/heal-concurrency-safety-notes-zh.md` 交点矩阵。
- HS-18（#1878，2026-08-19 逐行核实）：trash/tmp 三段清理全对齐——stale multipart 隔离-清理等价且更安全（`delete_all_with_quorum` 逐盘递归删即 `move_to_trash` rename 进 `.rustfs.sys/tmp/.trash`，另有锁 + fence）、trash 排空基本等价（无逐条 sleeper 节流，5m 周期天然限频）、tmp 非 trash 24h 回收等价（RustFS 5m 比 MinIO 6h 更及时）；周期默认 24h/6h/5m 三项全对齐。§8 原列"未逐行核实"的本项已有结论。

### 9.4 移交 follow-up（汇总于 backlog#1862 评论区）

HS-01 bitrot GET→MRF 全链路 e2e、kill -9 journal 回放 e2e、队列满压测 RSS（≤ 预算+10%）；HS-05/06 madmin 增量消费方法 + wire 单一来源化 + embedded e2e + 多轮轮询 soak；HS-08 多盘 scanner 周期 e2e；HS-04 超限审计条目；HS-18 低于 quorum 的 stale-multipart 崩溃残留窗口（扇出中途崩溃且已清盘数 > parity 时 FileNotFound 不在忽略集导致不自然收敛，修复需专用 quorum 变体）。

下一轮重审建议：跟随 heal/scanner 下一个大特性落地后触发，以本节为增量基线。
