# Heal/Scanner 配置与语义对照（MinIO parity 决策记录）

对应 backlog rustfs/backlog#1878（父 #1862，批 HS-14/HS-16/HS-18）。本页沉淀三项"决策 + 文档化"结论：scanner idle 节流语义对照与迁移警告（HS-14）、单机默认扫描周期决策（HS-16）、stale multipart 与 tmp/.trash 清理三段核对（HS-18），并顺带收录 bitrot_cycle 与 alert_excess_folders 两项已确认的默认值差异。所有 MinIO 侧结论均于 2026-08 按 minio/minio master 逐源码核对（引用文件为上游路径），不转述二手资料。

运行时旋钮的完整清单、状态端点与调参流程见 [Scanner Runtime Controls](scanner-runtime-controls.md)；excess 告警阈值差异见 [Scanner Excess Alerts](scanner-excess-alerts_zh.md)；heal 并发模型对照见 [Heal 并发安全说明](heal-concurrency-safety-notes-zh.md)。

## 1. HS-14：scanner idle 节流语义对照

### RustFS 当前语义（三因子）

RustFS 的 scanner 步进节流由三个因子共同决定（crates/scanner/src/sleeper.rs）：

1. **总闸 `scanner.idle_mode` / `RUSTFS_SCANNER_IDLE_MODE`（默认 `true`）**：`false` 时所有节流 sleep 全部跳过，scanner 全速推进；`true` 时按下面两因子计算 sleep。
2. **速度档**（`scanner.speed` / `RUSTFS_SCANNER_SPEED`，默认 `default`）：档位表与 MinIO 完全一致（见下表）。目录级 sleep = `1ms × factor`（上限 `max_wait`）；对象级 sleep = `本对象处理耗时 × factor`，下限 1ms、上限 `max_wait`。
3. **前台读退避下限**：`current_foreground_read_activity()` 取并发 GetObject 请求数（rustfs/src/storage/concurrency/request_guard.rs 的 `GetObjectGuard`）与流式读计数（`ForegroundReadGuard`）的较大值，换算为 `10ms × 活跃读数`、封顶 250ms 的下限；该下限对目录级与对象级 sleep 都生效（`.max(foreground_sleep)`），且**可以超过速度档的 `max_wait`**（自身封顶 250ms）。速度档为 `fastest`（factor=0）时预设 sleep 为 0，但只要 `idle_mode=true`，前台读下限仍然生效。

| 速度档 | sleep factor | 单次 sleep 上限 | 周期间隔 |
|---|---:|---:|---:|
| `fastest` | 0 | 0 | 1s |
| `fast` | 1× | 100ms | 1m |
| `default` | 2× | 1s | 1m |
| `slow` | 10× | 15s | 1m |
| `slowest` | 100× | 15s | 30m |

实际行为矩阵（RustFS）：

| `idle_mode` | 速度档 | 前台并发读 = 0 | 前台并发读 > 0 |
|---|---|---|---|
| `false` | 任意 | 完全不休眠，全速 | 完全不休眠，全速（前台退避也被总闸关闭） |
| `true` | `fastest` | 预设 sleep = 0，等效全速 | 每步 sleep = 前台读下限（10ms×读数，封顶 250ms） |
| `true` | 其余档 | 每步 sleep = 预设值（1ms~15s 封顶） | 每步 sleep = max(预设值, 前台读下限) |

周期间隔的解析优先级为 env `RUSTFS_SCANNER_CYCLE` > 持久化 `scanner.cycle` > `scanner.start_delay` > 启动期默认覆盖（当前恒无）> 速度档派生（crates/scanner/src/runtime_config.rs）。另有 `scanner.yield_every_n_objects`（默认 128）的协作式让出，与节流 sleep 相互独立。

### MinIO 当前语义（master 逐源码核对）

MinIO 的对应开关是 `scanner:idle_speed` / `MINIO_SCANNER_IDLE_SPEED`（internal/config/scanner/scanner.go）：取值为空串或 `on`（默认）时 `IdleMode=0`，取值 `off` 时 `IdleMode=1`。启动/配置加载时一次性写入 `scannerIdleMode`（cmd/config-current.go），扫描侧闭包 `weSleep = scannerIdleMode.Load() == 0`（cmd/xl-storage-disk-id-check.go）：**`on`（默认）= 目录级与对象级节流 sleep 始终插入（按速度档 factor，minSleep 100µs）；`off` = 两条节流路径完全不 sleep，全速扫描**。当前上游没有任何按 S3 请求/磁盘活动动态调整节流的逻辑——这是静态开关。

命名具有误导性，是历史残留：2024-01 之前 `weSleep` 由磁盘活动驱动（"Entire queue is full, so we sleep"，即有并发 S3/heal 活动才 sleep），minio/minio#18734（commit 7705605b）把该活动门替换为上述静态配置（初版取值 `throttled`/`full`，后改为 `on`/`off`），上游残留注释 "default is throttled when idle"、"Sleep always or based on incoming S3 requests" 均是替换前的语义描述，与现行代码不符。

### 对照与迁移警告

| 维度 | RustFS | MinIO（master） |
|---|---|---|
| 开关名 | `scanner.idle_mode` / `RUSTFS_SCANNER_IDLE_MODE` | `scanner:idle_speed` / `MINIO_SCANNER_IDLE_SPEED` |
| 取值 | 布尔 `true`/`false` | `on`/`off` |
| 默认 | `true`（节流开启） | `on`（节流开启） |
| 开 = | 节流总闸开：速度档 sleep + 前台读下限 | 节流总闸开：速度档 sleep |
| 关 = | 完全不休眠（含前台读下限一并失效） | 完全不休眠 |
| 活动耦合 | 有：前台并发读抬高 sleep 下限（10ms×读数，封顶 250ms） | 无（2024-01 起为静态开关） |
| 速度档表 | 两边完全一致（上表） | 同左 |

迁移警告：

- **环境变量名不可照搬**：RustFS 只读取 `RUSTFS_*` 前缀，不解析 `MINIO_SCANNER_*` 任何别名（crates/scanner、crates/utils 的 env 读取无别名链，测试还专门断言 `MINIO_SCANNER_SPEED`/`MINIO_SCANNER_CYCLE` 不泄漏生效）。照搬 `MINIO_SCANNER_IDLE_SPEED=off` 到 RustFS 会静默无效，必须改写成 `RUSTFS_SCANNER_IDLE_MODE=false`。
- **取值词表不同**：`on/off` vs `true/false`，不能原样复制。
- **方向澄清（修正父 issue 的预设）**：按当前上游源码，MinIO `idle_speed` 与 RustFS `idle_mode` 在"开=节流、关=全速"方向上是一致的，并非反向；父 issue 中"MinIO on=集群空闲才节流、off=始终按 delay 节流"的矩阵描述的是 2024-01 之前的活动耦合行为与反向解读，与 master 不符。真正需要写进迁移手册的差异是：MinIO 的 `idle_speed` 名称暗示"空闲时才慢"但实际是静态总闸；RustFS 的 `idle_mode=true` 在总闸之上还叠加了 MinIO 没有的前台读保护下限。
- **`false` 是大锤**：RustFS `idle_mode=false` 会连前台读退避一起关闭，scanner 与前台读完全抢盘；仅在 benchmark 或可独占 IO 的窗口使用。

**决策（HS-14）：保持现状。** RustFS 语义更直观（`idle_mode` = 节流总闸，`true` 即自适应限速），且比 MinIO 多一层前台读保护；不新增 `RUSTFS_SCANNER_IDLE_SPEED` 兼容别名（无社区强诉求不做，避免双入口漂移）。本节即对照表与迁移警告的正式落点。

## 2. bitrot_cycle 默认差异

| 项 | RustFS | MinIO |
|---|---|---|
| 键 | `heal.bitrot_cycle` / `RUSTFS_SCANNER_BITROT_CYCLE_SECS`（scanner.bitrot_cycle 为兼容旧键） | `heal:bitrotscan` / `MINIO_HEAL_BITROTSCAN` |
| 默认 | 30 天（crates/config/src/constants/heal.rs 的 `DEFAULT_HEAL_BITROT_CYCLE_SECS`）：按墙钟周期把扫描切深扫（deep bitrot） | `off`（internal/config/heal/heal.go 默认 `EnableOff`）：不做周期性深扫，仅普通扫描 + 管理端手动深扫 |
| 对齐方式 | 迁移 MinIO 行为：`heal.bitrot_cycle=off` 或 `RUSTFS_SCANNER_BITROT_CYCLE_SECS=disabled` | 反向：`heal:bitrotscan=<秒>` |

RustFS 的 30 天默认是刻意的耐用性默认（周期性全量 bitrot 校验），代价是每 30 天一轮深扫 IO；单机场景另有清洁空闲退避封顶约 42 分钟的墙钟保护（见 scanner-runtime-controls.md）。这是行为差异而非缺陷，文档化即可。

## 3. alert_excess_folders 默认差异

RustFS 默认 65538（容纳 Proxmox Backup Server 每目录 65536 chunk 的布局），MinIO 默认 50000。差异原因、另两个 excess 阈值（versions=100 相同、version_size TiB vs TB）、事件名映射与冷却语义已完整记录在 [Scanner Excess Alerts](scanner-excess-alerts_zh.md)，此处不重复。

## 4. HS-18：stale multipart 与 tmp/.trash 清理三段核对

MinIO 把"清理已删除数据"拆成三段：stale upload 先 rename 进 `.minio.sys/tmp/.trash/<uuid>` 隔离（rename 快、原子）；trash 由独立例程排空；tmp 下非 trash 的旧目录单独回收。逐段核对 RustFS：

| 段 | MinIO | RustFS | 判定 |
|---|---|---|---|
| stale multipart → 隔离 | `cleanupStaleUploadsOnDisk`（cmd/erasure-multipart.go）逐盘列出 multipart 目录，按 uploadID 目录名里的 UnixNano 判龄，超过 `stale_uploads_expiry`（默认 24h）即 `renameAll` 进 `.minio.sys/tmp/.trash/<uuid>`，空 sha 目录、tmp 旧目录同法 | `cleanup_stale_multipart_uploads_in_set`（crates/ecstore/src/bucket/lifecycle/bucket_lifecycle_ops.rs）发现候选后取 ns 写锁 + 重查（`lock_stale_multipart_cleanup`），`delete_all_with_quorum` 扇出逐盘递归删除，而 LocalDisk 的递归删除内部就是 `move_to_trash`（crates/ecstore/src/disk/local.rs）把目录 rename 进 `.rustfs.sys/tmp/.trash/<uuid>` | 行为等价（都是先隔离后清理）；RustFS 额外有写锁 + quorum 重查 + 锁丢失 fence（crates/ecstore/src/set_disk/ops/multipart.rs 的 `StaleMultipartCleanupGuard`），防并发 CompleteMultipartUpload 竞争，安全性强于 MinIO 的无锁 rename |
| trash 排空 | 每 `delete_cleanup_interval`（默认 5m，internal/config/api/api.go）逐盘删 `.trash` 内条目，逐条以 `deleteCleanupSleeper`（factor 5 / 25ms，cmd/globals.go）节流 | 每盘独立 `cleanup_deleted_objects_loop`，`DELETED_OBJECTS_CLEANUP_INTERVAL` = 5m（crates/ecstore/src/disk/local.rs），先排空 `.trash` 再回收 tmp 旧目录；排空为顺序 `remove_dir_all`/`remove_file`，**无逐条 sleep 节流** | 基本等价；唯一差异是 RustFS 排空不节流，trash 积压大时单轮 IO 更突发（5m 周期天然限频），文档化，如实测出现清理风暴再补节流 |
| tmp 非 trash 旧目录 | 并在 `cleanupStaleUploadsOnDisk` 内：非 `.trash` 的 tmp 目录超过 `stale_uploads_expiry`（24h）rename 进 trash（随 6h 任务） | `cleanup_stale_tmp_objects`（crates/ecstore/src/disk/local.rs）随 5m 循环执行：非 `.trash` 目录超过 `STALE_TMP_OBJECT_EXPIRY` = 24h 即 rename 进 trash；另有启动时 tmp → tmp-old 整体换名 + 后台删除的崩溃安全路径 | 行为等价（阈值同为 24h）；RustFS 检查频率 5m vs MinIO 6h，回收更及时 |

周期与环境变量默认值对照（两边一致）：

| 项 | RustFS | MinIO |
|---|---|---|
| stale upload 过期阈值 | `RUSTFS_API_STALE_UPLOADS_EXPIRY`，默认 24h | `MINIO_API_STALE_UPLOADS_EXPIRY`，默认 24h |
| stale multipart 清理周期 | `RUSTFS_API_STALE_UPLOADS_CLEANUP_INTERVAL`，默认 6h | `MINIO_API_STALE_UPLOADS_CLEANUP_INTERVAL`，默认 6h |
| trash 排空周期 | 5m（常量，暂无开关） | `MINIO_API_DELETE_CLEANUP_INTERVAL`，默认 5m |

关于 rustfs/src/delete_tail_activity.rs：它**不覆盖三段中的任何一段**。该模块是 delete 尾部活动的进程内指标计数（inflight gauge + 耗时 histogram），供 allocator 回收压力判断（rustfs/src/allocator_reclaim.rs）使用；生产代码目前只在对象复用路径使用 `Replication`/`Notify` 两个 stage 计数，`Tail`/`Cleanup` 枚举值暂无调用点。

崩溃残留窗口结论：

- trash 内部残留（排空中途崩溃）：`.trash/<uuid>` 是自包含目录，下一轮 5m tick 重扫 `.trash` 自然收敛，与 MinIO 相同。
- 跨盘扇出中途崩溃（部分盘已 rename 进 trash、其余未动）：若剩余盘数仍满足写 quorum，下一轮 6h 任务重新发现候选并重删，自然收敛；若已清理盘数超过 parity（剩余低于写 quorum），`check_multipart_upload_path_exists` 因 `FileNotFound` 不在 `OBJECT_OP_IGNORED_ERRS`（crates/ecstore/src/disk/error_reduce.rs）而判 quorum 失败，候选被跳过，残留 uploadID 目录不会被该任务收敛（不可见于 S3 API，仅占盘空间）。该窗口极窄（逐盘 rename 为毫秒级，需恰在扇出中途且已过 parity 盘时进程死亡）。MinIO 同场景会收敛（逐盘独立处理、无 quorum 闸门）。**分级：有崩溃残留窗口（极窄）→ 登记后续修复**；修复需为清理守卫提供把"已不存在"计为达成终态的专用 quorum 变体（不能改共享的 `check_multipart_upload_path_exists` 语义，它同时服务 CompleteMultipartUpload），超出本批"几行小修"边界，不在本 PR 扩 scope。

## 5. HS-16：单机（ErasureSD）默认扫描周期决策

启动期曾有预留钩子 `single_disk_default_cycle_secs`，可按维护特征（lifecycle/replication/巡检失败）为单机覆盖默认周期，但从未接线、恒返回 `None`，已删除（本批 PR）。决策：**单机默认周期保持速度档派生（`default` 档 = 60s），不做特殊覆盖**。理由：其一，无任何实测依据表明单机冷启动 ILM 延迟需要更短周期，凭空缩短只会放大空闲扫描频次；其二，单机已有清洁空闲退避（连续干净周期间隔翻倍，默认 bitrot 窗口下封顶约 42 分钟，见 scanner-runtime-controls.md），空闲时的周期压力已被消化；其三，若确有诉求，用户可用 `RUSTFS_SCANNER_CYCLE` / `scanner.cycle` 显式配置，无需内置特殊路径。需要更激进短周期的场景应先拿实测数据再议。

## 6. 决策摘要

- HS-14：保持 `RUSTFS_SCANNER_IDLE_MODE` 现语义（true=节流总闸+前台读下限，false=全速），文档化对照表与迁移警告，不做兼容别名。
- HS-16：删除恒 `None` 的单机默认周期钩子，单机周期保持速度档派生 + 清洁空闲退避。
- HS-18：三段清理行为等价（trash 排空无逐条节流、tmp 回收频率 5m vs 6h 两处小差异文档化）；跨盘扇出的极窄崩溃残留窗口登记后续；周期默认值 24h/6h/5m 与 MinIO 对齐。
