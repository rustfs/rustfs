# P1 逐条复审订正与方案计划

> 复审基线:main @ `77f2b948c`(7 个 P0 修复 #5748~#5754 已全部合入)
> 复审方式:5 组对抗性复审 agent 并行,先怀疑后确认;以 RustFS 自身功能契约为正确性标准,不以"未对齐 MinIO"为根因;RustFS 更优/独特设计标注"保持不变"
> 参照:MinIO 源码、mc@cf909e1063a9、madmin-go v3.0.109、minio-go v7.0.91
> 日期:2026-08-06

---

## 〇、复审总裁定表

| 项 | 主题 | 复审结论 | 关键订正 | 工作量 |
|---|---|---|---|---|
| P1-1 | ILM expiry 复制语义 | CONFIRMED(范围扩大) | 发送点共 4 处非 1 处;接收端无门禁;修复重心移到接收端 merge | M |
| P1-3 | 自动跨站元数据 heal | CONFIRMED(范围收窄) | 真实缺口="retry queue 有账本无消费者";不移植 MinIO 全量 heal | M |
| P1-5 | GET/HEAD 远端 proxy | CONFIRMED | 同步复制模式是已实现的部分缓解(保持不变);proxy 指标语义被出站 HEAD 污染 | L(P0 段 M) |
| P1-6 | 三类时间戳头收发 | CONFIRMED(缺口扩大) | 实为三段缺失:tagging 无本地写入方 + 不发头 + 接收端无 LWW 合并点 | M |
| P1-7 | ARN 前缀不互认 | CONFIRMED+(加重) | 新发现 FromStr id/region 互换 bug;madmin ParseARN 硬校验实锤 → 生成侧必须改 | M |
| P1-8 | 配置校验缺口 + StorageClass | 部分 CONFIRMED | 2MB 子项 REFUTED(MinIO 亦无);StorageClass 属刻意设计成立(MinIO 也不消费 rule 级,target 级 RustFS 已生效)| S |
| P1-11 | replication-metrics snake_case | CONFIRMED | BucketStats 复用内部 RPC 线格式实锤 → 必须独立响应 DTO | M |
| P1-12 | replication-reset 响应壳 | CONFIRMED(面缩小) | 致命键仅 5 个(壳 `Targets`≠`target` + 4 个字段名);其余靠 Go 大小写不敏感能对上 | S |
| P1-13 | mrf/diff 聚合响应 | CONFIRMED(症状加重) | 实际输出**伪数据行**而非空;diff/mrf 数据源均可支撑逐条流 | diff S / mrf M |
| P1-14 | set-remote-target 请求体 | 原缺口已缓解;**新 CONFIRMED 阻断** | #5754 后 26 字段已全覆盖;但**零值 `expiration` 恒被拒 → mc replicate add 仍 100% 失败**;latency 单位 round-trip 污染 | S(**建议立即修**) |
| P1-15 | site state RMW 竞争 | CONFIRMED(加重) | hook 路径 enqueue/dequeue 同进程内绕过既有 Mutex → 单节点即可触发 | M-L |
| P1-16 | 状态机类型双份定义 | CONFIRMED(加重+收窄) | drift 已发生(MrfOpKind 两侧不一致);但 filemeta 侧 worker DTO 是死代码,活跃双份仅 3 个 wire 类型;"抽公共 crate"否决 | S+M |
| P1-17 | 桶复制逻辑分裂 | CONFIRMED;微文件合并子项 REFUTED | boundary 微文件是棘轮机制的机械接缝(守护脚本按文件名锚定),合并负收益;缺的是完成判据 | M0=S,整体 L |
| P1-18 | 超长函数 | 行数 CONFIRMED;apply_iam_item 降级 | apply_iam_item 长而不复杂(6 臂 dispatch),不拆降 P2;其余 4 个给纯移动拆分草案 | M |
| P1-19 | 源→目标版本身份策略 | CONFIRMED(范围收窄) | delete-marker 的"捕获+持久化映射"模式已落地(保持不变);推荐能力探测+显式拒绝而非全量映射 | M |
| P1-20 | scanner 补偿边界 e2e | CONFIRMED(缺口收窄) | 决策函数单测与 Failed-heal e2e 已存在;缺 existing-object 矩阵与 Replica 防环 e2e;附完整入队真值表 | M |
| P1-21 | delayed purge 静默丢弃 | CONFIRMED | 映射损坏防护已加固(保持不变);`let _ =` 与无 MRF 通道仍在;附带发现 MRF outcome 恒 false 滞留问题 | M |
| P1-22 | 桶复制 SSE 能力 | CONFIRMED(前提订正) | SSE-S3 自 #5633 已 fail closed,被 ignore 的 e2e 理由过期(先摘 ignore);SSE-C 缺的是目标侧头摄取 | L(4 阶段) |

**"保持不变"清单(复审确认的 RustFS 更优/刻意设计,不纳入修复)**:per-PUT 即时元数据传播 hook(优于 MinIO 纯周期 heal)、单向推送+stale 守卫收敛模型、delete 走 merge-with-empty(优于 MinIO 整删)、delete-marker 版本映射持久化+损坏拒猜、同步复制模式(partition_by_sync)、能力契约式显式拒绝+`deny_unknown_fields`(字段清单已与 madmin v3.0.109 同步)、StorageClass 显式拒绝非 STANDARD(target 级已真正生效)、replication-check 真实探针写删、响应中的 RustFS 增强字段(ResetBeforeDate/Error/可观测性键,Go 忽略未知键可共存)。

---

## 一、紧急项(建议立即处理)

### ⚡ P1-14 新阻断:零值 `expiration` 拒绝 → mc replicate add 仍 100% 失败

- **证据**:Go `omitempty` 不省略零值 `time.Time`(已用 Go 程序按 madmin 逐字 tag 实测),mc/madmin marshal 恒输出 `"credentials":{"expiration":"0001-01-01T00:00:00Z"}` 与 `"resetBeforeDate":"0001-01-01T00:00:00Z"`;RustFS `handlers/replication.rs:286-291` 对 `expiration.is_some()` 一律 400。#5754 的测试全部用手写 payload(`expiration: None`),未被现网形状打中。
- **修复(S)**:①`expiration` 改"非 Go 零值时间才拒"(与 `sessionToken` trim-empty 判断对称);②`latency` 请求字段直接忽略(消除 #5754 后纳秒响应 ↔ 毫秒请求的 round-trip 1e6 倍污染);③把"Go 真实 marshal 形状 payload"固化为测试夹具惯例。
- **红灯测试**:用实测 Go marshal 全形状 body(含零值 expiration/resetBeforeDate/latency{0,0,0}/edge:false/healthCheckDuration:60000000000)打 set-remote-target,期望 200;非零 expiration 仍 400(能力契约保持)。

### ⚡ P1-7 附带 bug:ARN FromStr 字段互换

`arn.rs` Display 输出 `{type}:{region}:{id}:{bucket}`,FromStr 却读 `id=parts[3], region=parts[4]`——id 与 region 互换。当前仅因消费方只用 arn_type 而潜伏。随 P1-7 一并修。

---

## 二、逐项方案计划

### P1-1 ILM expiry 复制语义(M)

**订正后事实**:发送完整 lifecycle XML 的路径 4 处——PUT hook(`bucket_usecase.rs:2177-2180`)、DELETE hook(`:1512-1514`,触发接收端**整删**)、import(`bucket_meta.rs:948-951`)、build_sr_info/bootstrap(`site_replication.rs:4190,2241-2249`);接收端 `apply_bucket_meta_item`(`:7669-7683`)整体覆盖/删除,且**无 `replicate_ilm_expiry` 门禁**。P0 后已有缓解(发送开关、bootstrap 跳过、stale 判定)只解决"发不发/新旧",不解决"发什么/怎么合"。

**方案**:接收端 merge 为主(信任边界),发送端 expiry-only 提取为辅:
1. 新增纯函数 `extract_expiry_only(cfg)` 与 `merge_expiry_rules(local, incoming)`——语义对齐 MinIO `mergeWithCurrentLCConfig`,两处 RustFS 改进:incoming 一律先剥 transition(防旧端);`None` 走 merge-with-empty 而非整删(**MinIO 整删连本地 transition 一起删是缺陷,不照抄**);
2. 接收端 lc-config 分支改 读→merge→条件写/删,保留 stale 判定与 incarnation 守卫;补 `replicate_ilm_expiry` 门禁;
3. 4 个发送点接 `extract_expiry_only`;expiry 判定用 RustFS 口径(含 `del_marker_expiration`)。

**红灯测试**:L1 单测 5 例(提取剥离/合并保留 T/防御剥离/merge-with-empty/import 无 transition);L3 e2e——B 配本地 transition,A PUT expiry → B 两者共存;A DELETE lifecycle → B transition 仍在。
**兼容**:旧端发完整 XML → 新接收端剥后 merge 正确;新端 expiry-only → 旧接收端仍整覆盖(不劣于现状)。规则按 ID 对齐,`rule-{idx}` 撞名同 MinIO 语义,文档注明。

### P1-3 自动跨站 heal → 改为"retry queue 自动 drain"(M)

**订正后事实**:retry queue 是现成增量账本(失败即入队 `:3243-3262`,持久化于 state,`retry_count` 字段存在)但**全库无消费者**;手动 repair 是本地快照单向推送,收敛方向依赖运维判断。即时 hook + 显式 repair 模型保持不变。

**方案**:
- 阶段 1(核心):周期任务挂进现有 reconcile ticker,per-event 重发(body 从本地当前元数据重建,复用 `SiteReplicationRepairTask::send`,天然发"当前值"+对端 stale 守卫幂等);指数退避(`retry_count`+上限转 failed);drain 全程包分布式锁去抖(先用 `with_config_object_write_lock` 专用对象,P1-15 落地后并入统一 state store);结构化 tracing 汇总一条。
- 阶段 2(可选,默认关闭):每 N tick 比对 repair plan token,不同才自动 dry-run→execute。**不移植** MinIO 跨站取最新 pull 语义(各站各自 drain 即双向收敛)。

**红灯测试**:L2——state 带 retry event,调 `drain_site_replication_retry_queue()`(现不存在),fake peer 成功后断言队列清空;退避断言。L3——停 B→A PUT policy 失败入队→起 B→drain 后 B 收到且 SRRetryStats 归零。

### P1-5 GET/HEAD 远端 proxy(L;P0 段 M)

**订正后事实**:`SUFFIX_SOURCE_PROXY_REQUEST` 零消费者;`ProxyMetric` 字段与 admin 汇总通路已就位,但 resyncer 把**出站** HEAD 计入 `head_total` 污染语义;`disable_proxy` 管道存在无人消费;同步复制模式(`partition_by_sync`,`replication_pool.rs:2667-2689`)是部分缓解但不等价(手动 per-target、失败仍 404、不覆盖兜底窗口)。防环头当前仅潜在问题,但 proxy 实现与防环识别**必须同 PR**(否则 RustFS↔RustFS 成环)。

**方案**(P0 段):新增 `replication_proxy_boundary.rs`——`proxy_targets`(version_suspended/入站 proxy 头/disable_proxy 三重 gate)+ `proxy_get/head_to_replication_target`(走现有 TargetClient,range/条件头透传);触发点在 usecase 层 NotFound/VersionNotFound 分支;接收侧 options.rs 解析防环头,出站双前缀发送;`tokio::timeout`(~3s env 可调)、仅 2xx 采纳其余回落本地 404、复用离线标记短路;指标接 `record_replication_proxy` 并纠正 resyncer 计数语义。P1 段:tagging 三操作 proxy(依赖 P1-6)。
**红灯测试**:e2e 双站断复制链路后从对端 GET/HEAD 应 200(现 404);防环负例(带头请求不转发、计数不增);降级负例(target 全离线时限时 404);disable_proxy 负例。

### P1-6 时间戳头收发(M;三段修复)

**订正后事实**:①`SUFFIX_TAGGING_TIMESTAMP` 全仓无写入方(retention/legalhold 已有双前缀写入);②`PutObjectOptions::header()` 只序列化 4 个内部头,三类时间戳被丢弃,multipart 同;③接收端不解析,且 replica PUT 是 verbatim 覆盖——解析后必须在写盘前与本地版本做 per-类别 LWW 合并才有效;④`AdvancedPutOptions` 默认 `now_utc()` 无法当"未设置"哨兵,需 Option 化。

**方案**:阶段 0——`put/delete_object_tagging` 落 `SUFFIX_TAGGING_TIMESTAMP`(双前缀);阶段 1——新增三个 suffix 常量(对齐 MinIO headers.go:239-243),三字段 Option 化,`header()` 与 multipart 条件序列化;阶段 2——接收端解析(仅授权复制请求)+ PUT 路径 LWW 合并并持久化赢家时间戳(合并仅限三类元数据,不触碰数据与其余元数据,与 verbatim-replica 不变式共存)。
**红灯测试**:单测 header 双前缀序列化断言/未设置缺席断言;接收端解析单测;e2e active-active tagging 并发收敛(晚者胜,现 main 旧值覆盖新值为红)。

### P1-7 ARN 前缀(M)

**订正后事实**:madmin `ParseARN` 硬校验 `arn:minio:` 前缀 + ID/bucket 非空(v3.0.109 remote-target-commands.go:50-63);mc 爆炸点仅 `replicate update`(fatalIf)与 `replicate ls`(软降级);`replicate add` 把 ARN 当不透明串不受影响——解释了"add 通 update 挂"。RustFS ARN 结构(`type::id:bucket`)与 madmin 兼容,仅 vendor token 障碍;另有 FromStr id/region 互换 bug(见紧急项)。

**方案(推荐路线 A)**:生成侧默认改 `arn:minio:`(留常量可品牌化);解析侧接受双前缀(存量 `arn:rustfs:` 靠双前缀解析 + 现有字符串等值匹配继续工作);修字段序;改 `generate_arn`、`site_replication.rs:6329` 与相关测试断言。混合版本集群前缀不一致靠双前缀解析吸收;不做存量数据前缀归一化改写。
**红灯测试**:单测 `from_str("arn:minio:replication:us-east-1:depl:bucket")` 成功且 id/region 正确(现双重红灯);round-trip 属性测试;e2e set-remote-target 返回 ARN 可被 madmin 语义解析、预置 `arn:minio:` 目标可 remove。

### P1-8 配置校验(S)

**订正后事实**:2MB 上限 REFUTED(MinIO 亦无显式检查,剔除);StorageClass 已缓解且刻意设计成立——MinIO 自己也不消费 rule 级 `Destination.StorageClass`(复制 PUT 用 target 级 `tgt.StorageClass`),RustFS target 级 storage_class 已真正生效(`bucket_target_sys.rs:1633-1634`),容忍显式 STANDARD 已实现。仍缺:规则数≤1000、≥1 条、Priority 唯一非负、ID≤255、Filter 互斥、Tag×DeleteMarkerReplication 互斥、sameTarget 拒绝。

**方案**:`config.rs` 新增 `validate_replication_config_structure` 纯函数,`bucket_usecase.rs:2418` 接入;StorageClass 保持现状+契约文档化("rule 级请改用 remote target 的 storageclass 字段")。
**红灯测试**:单测逐格(1001 规则/重复 Priority/256 字符 ID/Filter 并存/Tag+DMR)期望特定错误;e2e aws-sdk 形状 XML 断言 InvalidRequest。

### P1-11 replication-metrics DTO(M)

**订正后事实**:`BucketStats` 走内部 peer RPC 线格式(`rmp_serde::to_vec_named` 字段名入线,node_service.rs:1401 / peer_rest_client.rs:88-104)——**改原结构 serde 名会破坏混合版本集群 RPC,禁止**;必须走 #5754 的响应 DTO 模式(同文件先例 `remote_target_admin_json`)。

**方案**:新增仅 Serialize 的 `MetricsV2Dto{uptime,currStats,queueStats,downtimeInfo}`/`MetricsDto`/`TargetMetricsDto`,显式映射(`q_stat`→`queued`、`bandwidth_limit_bytes_per_sec`→`limitInBits`、failed→TimedErrStats total-only);`queueStats.nodes` 先填本机一条;RustFS 可观测性扩展键保留(Go 忽略未知键,双栖零成本)。
**红灯测试**:e2e 用镜像 minio-go MetricsV2 tag 的结构反序列化断言 `currStats.completedReplicationSize > 0`(现全零);DTO 键名 snapshot 单测。

### P1-12 replication-reset 响应壳(S)

**订正后事实**:致命键仅 5 个——壳 `Targets`≠`target`、`Status`≠`resyncStatus`、`ReplicatedSize`≠`completedReplicationSize`、`ReplicatedCount`≠`replicationCount`、`FailedSize/FailedCount`≠`failedReplicationSize/failedReplicationCount`;其余(Arn/ResetID/StartTime/...)靠 Go 大小写不敏感能对上;`ResetBeforeDate`/`Error` 是增强字段可保留。响应结构是 router.rs 独立 DTO 无内部复用,改名零风险。

**方案**:纯 serde rename(建议全字段精确对齐 madmin 小写形态),保留增强键+文档标注。
**红灯测试**:e2e 断言响应含 `target` 数组且 `target[0].resetid` 非空、status 侧 `resyncStatus`/`completedReplicationSize` 键存在。

### P1-13 mrf/diff 流式响应(diff S / mrf M)

**订正后事实**:症状比"输出空"更糟——聚合对象会被 madmin `json.Decoder` 成功解码一次,`mc replicate backlog` 输出一条 object 为空的**伪行**(静默伪数据);路线 A(保持聚合+文档化)无法消除伪行且与 madmin 同 path 无内容协商,**不可行**。数据源评估:diff 已逐条扫描只需去壳;mrf 的 durable backlog(`MrfReplicateEntry` 字段恰好覆盖 `ReplicationMRF` 所需)已可枚举。

**方案(路线 B)**:diff 去壳输出 NDJSON `DiffInfo` 形状(仅 `IsDeleteMarker`/`ReplicationStatus` 需 rename;truncation 信息入日志不入流);mrf 遍历 durable entries 逐条输出 `ReplicationMRF` 形状(nodeName 填本机);聚合响应保留在 `?aggregate=true`(RustFS 扩展,deliberate 注释随迁)。条目量有 `REPLICATION_DIFF_MAX_SCAN` 封顶,内存拼 NDJSON 即可不必真流式。
**红灯测试**:e2e 制造失败复制后逐行反序列化断言至少一条 `object` 非空(现为伪空行);diff 断言无 `Entries` 壳。

### P1-14 set-remote-target(S,含紧急项)

见"一、紧急项"。另:`deny_unknown_fields` **保留**(推荐)——字段清单已与 madmin v3.0.109 全同步,严格模式+显式清单兼得契约哲学与防静默;代价写进维护清单:"madmin 版本升级时同步字段清单"(加对照 madmin tag 列表的常量测试防漂移)。

### P1-15 site state 统一 store(M-L,两 PR)

**订正后事实**:主 state 有进程内 Mutex(`:347`)但两处不完备——①无分布式锁(多节点 RMW 丢更新);②**retry event enqueue/dequeue 不持锁**(挂在所有 hook 广播路径上,同进程即可丢更新);reload 路径完全无锁(稳态不写盘收窄窗口,迁移期可覆盖并发写)。repair state 的 `with_config_object_write_lock` + no-lock IO 是正确样板(`:1097-1114`);两套归一化的语义差异(JSON-level 容忍畸形 peer)是**有意的**,统一时必须保留。锁序注释 `:346` 可挂靠。

**方案**:PR1——新建 `admin/site_replication_state.rs`:两阶段归一化合一(JSON 宽容清洗→类型化)、`read_state()/update_state(F)`(分布式锁包完整 RMW,锁内禁网络调用与嵌套配置锁)、常量收敛;service reload 接入;迁移 service 侧 5 个归一化测试保语义。PR2——迁移全部 ~30 个 RMW 调用点(含 enqueue/dequeue),**移除**进程内 Mutex(避免双锁新顺序约束);dequeue 热路径保留"先无锁读、命中才进 update_state"两段式;更新锁序注释。每个调用点做重入审查(现有 drop-reacquire 模式保持)。
**红灯测试**:L2 单进程并发——持锁 RMW(mark_pending_rotation_peer_acked)×绕锁写者(enqueue_retry_event)注入交错,断言最终 state 两者共存(现必丢其一,确定性红灯);L1 归一化等价性测试迁移;L3 双节点并发(nice-to-have)。
**风险**:盘上格式不变;锁超时从"静默丢更新"变"显式报错",hook 路径保持 warn 不阻断 S3 主路径。

### P1-16 类型对账护栏(S)+ 死代码清理(M)

**订正后事实**:drift 已发生(filemeta 侧 `MrfOpKind` 缺 Metadata/Heal/ExistingObject 三 variant、`MrfReplicateEntry` 缺 force_delete/target_arns)——但 filemeta 侧 8 个 worker DTO 全是**死代码**(零消费者);活跃双份仅 `ReplicationStatusType/VersionPurgeStatusType/ReplicationState` 三个 wire 类型(filemeta 绑 xl.meta 磁盘格式,replication 绑 MRF/resync 持久化格式);boundary 枚举转换 `as_str()` 兜底 `_ => Empty` 会静默降级。"抽公共 leaf crate"否决(两 wire 格式演进节奏不同,迁移规则 #12 本意是所有权独立)。

**方案**:Step 1(S,即刻)——boundary 加对账测试:两侧枚举穷尽 match(新增 variant 即编译失败)+ as_str 双向 round-trip + ReplicationState 全字段往返;Step 2(M)——清理 filemeta 侧 ~600 行死代码 DTO,注意 crates.io semver(先 `#[deprecated]` 一版再删);Step 3(S)——replication 侧注释指向对账测试。

### P1-17 迁移完成判据(M0=S;整体 L)

**订正后事实**:"合并 boundary 微文件"REFUTED——守护脚本按具体文件名锚定每个 boundary,合并要同步改脚本+mod+导入点而功能收益为零;微文件是棘轮机制的机械接缝。唯一可退役:`datatypes.rs`(消费者迁完即删)。README 建议的第一步(event sink/runtime boundary)实际已部分落地,文档滞后。

**方案**:M0(S)文档 PR——完成判据 = Required Contracts 表 "Current dependency to remove" 列清空;终态 = pool/resyncer/state 移入 crates/replication,boundary 随 crate 移动自然消解;更新 split-plan "Proposal only" 状态。M2(M)resyncer 纯决策逻辑下沉;M3(L)trait 稳定后移 worker 运行时(全计划唯一高危段,最后做);M4(S)统一退役 boundary 与守护条目。**不做**批量合并微文件。

### P1-18 超长函数拆分(M;4 个 PR)

**订正后事实**:行数确认(resync_bucket 537 / start_mrf_processor 306 / replicate_all 409 / delete 路径 replicate_object 299 / apply_iam_item 255);`apply_iam_item` **降级 P2 不拆**(6 臂 dispatch,每臂线性短小,拆分违反 "Prefer direct, local code");`replicate_object` 有两个同名体,原清单指 delete 路径 trait impl。

**方案**(每函数独立 PR,纯移动,`git diff --color-moved=dimmed-zebra` 验证):
1. `resync_bucket`(最优先,三处历史并发 bug 注释所在):acquire_resync_leadership / load_resync_replication_config / spawn workers+collector 三段抽出,并发 bug 注释随代码移动,每个 return 前的 mark_status 逐一保持;
2. `start_mrf_processor`:抽 `reconstruct_mrf_delete/object` 纯函数(主循环 -150 行,重建逻辑可单测);
3. `replicate_all` + delete 路径 `replicate_object`:各拆 3-4 个阶段 helper;**明确不合并两函数**(delete-marker 404/405 校验语义是刻意差异)。
**排序依赖**:先 P1-18 拆分、后 P1-17 M2/M3 迁移(小函数降低搬运风险)。

### P1-19 版本身份策略(M,推荐方案 B)

**订正后事实**:#5752 已合入(PUT/multipart initiate 带 query,RustFS 目标侧也支持);PUT 响应 `x-amz-version-id` 仍被丢弃(`:1891 Ok(_)`);**delete-marker 子案已系统性缓解**——`remove_object` 捕获目标版本号→`target_delete_marker_version_ids` 持久化进 xl.meta(含上限与损坏标记)→延迟 purge 优先用映射、损坏拒猜(**保持不变**);RustFS 无"仅支持 MinIO 目标"契约声明;replication-check 探针已捕获响应版本号但不比对。MinIO 同样丢弃响应版本号(平价),RustFS 已有两点增强。

**方案对比**:A 全量映射持久化(完整但 xl.meta 膨胀、全链路改造,L);**B(推荐)**:契约=仅支持"沿用源版本 ID"的目标,在 replication-check 增加 VersionFidelity phase(探针 PUT 带 versionId query,比对响应版本号)+ `validate_target` 复用同一探测,不镜像则新错误 `BucketRemoteTargetVersionMismatch` 显式拒绝/告警(M);C 混合(无需求支撑)。探针是主动写,进 validate_target 会扩 set-target 副作用面——可先只做 check phase + 运行期首次 PUT 抽查告警。
**红灯测试**:FakeS3Target 加 `assign_own_version_ids` 开关模拟原生 S3,断言版本删除复制落空(现红)与探测后显式拒绝(修后绿)。

### P1-20 scanner 补偿边界 e2e(M,纯测试)

**订正后事实**:决策函数单测(queue.rs 7 例等)与 scanner 驱动的 Failed-heal e2e(target 断电恢复/源重启重放,FAST_SCANNER_ENV)已存在;真实缺口=无任何"先写对象→后配复制"的 existing-object 用例。完整入队真值表已梳理(见复审记录):Enabled×Empty 补齐、Pending/Failed 恒补(不受 existing 开关影响)、Disabled×Empty 永不补、Replica 恒不补(防环)、null-version 永不入队、reset_id 重置补齐。

**方案**:e2e 矩阵 1-2 个用例(先 PUT 四种来源对象含 Copy/Snowball 产物→后配 Enabled/Disabled 规则→正例 wait_for_replicated_object / 负例 assert_failed_replication_stays_absent_for ≥3 周期,**"永不补齐"是契约必须显式断言**)+ Replica 防环变体 + queue.rs 补 2 格单测;null-version 跳过行为先写"记录现状"断言并注明出处。不改产品代码。

### P1-21 delayed purge 失败处理(M)

**订正后事实**:静默点两处——target client 缺失 `continue` 无日志(`:1673-1675`)、`let _ = remove_object`(`:1693-1700`);5 次循环是等源 marker 消失非重试;purge 调用后无条件 break;MRF 入队接口(`queue_replica_delete_task`,队满自动落盘)同 crate 可用无分层障碍;映射优先/损坏拒猜是已加固项保持不变。**附带发现**(建议单独跟进):`requires_delayed_purge` 恒真使 delete-marker 类 MRF 条目 outcome 恒 false → 重放永远 Missed 保留,可能永久滞留。

**方案**:①purge 函数返回 per-target 成败,失败 warn(带 event 常量)+ metrics,client 缺失同样 warn(S);②循环内失败重试、轮次耗尽入 MRF、入队失败 warn+metric 兜底(S/M);③两层失败注入测试(mock 503 断言重试/状态/MRF;FakeS3Target inject 断言故障清除后最终收敛)(M)。风险:MRF 重放重发 DELETE marker 创建——mtime 幂等,风险低。

### P1-22 SSE 能力(L,4 阶段)

**订正后事实**:fail-closed 由 #5633 引入(`replication_target_boundary.rs:101-174`),普通/Heal/Resync/Multipart 全走同一函数;SSE-C 发送半边已建(内部头→`X-Rustfs-Replication-*` 映射+CRC),**目标侧摄取代码完全缺失**(链路必断,e2e 已钉 FAILED);SSE-S3 契约 e2e 的 `#[ignore]` 理由(backlog#1291 silently drops)已被 #5633 过期;直传托管 SSE 不可行(封存密钥绑本站 KMS),MinIO 是源解密+目标重加密;ecstore 已有 `ObjectEncryptionResolver` trait seam,解密不破分层。

**方案**:阶段 0(S)摘 ignore + 补 encrypted resync/heal e2e 钉全矩阵 fail-closed 现状;阶段 1(M)SSE-C 目标侧头摄取+加密尺寸/CRC(MinIO :1670-1740 参照);阶段 2(M/L)SSE-S3 经 resolver 解密+目标 AES256 重加密(resolver 未注册必须继续 fail closed;multipart 按明文尺寸分片);阶段 3(L)SSE-KMS + key id 随行开关(目标站无同名 key 显式失败,禁止回退 SSE-S3)。过渡期全矩阵维持 fail closed,禁止明文降级。

---

## 三、执行批次建议

| 批次 | 内容 | 性质 |
|---|---|---|
| **B0 立即** | P1-14 零值 expiration + latency 忽略(S);P1-7 FromStr 字段互换(并入 P1-7 或先行) | mc 阻断修复 |
| **B1 小改动高收益** | P1-12 响应壳 rename(S)、P1-13 diff 去壳(S)、P1-8 结构校验(S)、P1-16 Step1 对账测试(S)、P1-17 M0 文档判据(S)、P1-22 阶段 0 摘 ignore(S) | serde/校验/测试护栏 |
| **B2 数据一致性** | P1-21 purge 失败处理(M)→ P1-20 scanner 矩阵 e2e(M,纯测试)→ P1-19 方案 B 能力探测(M)→ P1-15 state store PR1+PR2(M-L) | 一致性核心 |
| **B3 互操作补齐** | P1-7 ARN 路线 A(M)、P1-11 MetricsV2 DTO(M)、P1-13 mrf 流(M)、P1-6 时间戳三段(M)、P1-1 ILM merge(M)、P1-3 retry drain(M) | mc/跨站语义 |
| **B4 大功能与架构** | P1-5 proxy P0 段(M→L)、P1-22 阶段 1-3(L)、P1-18 四函数拆分(M)→ P1-17 M2-M4(L)、P1-16 Step2 死代码(M) | 长期 |

**批内依赖**:P1-6 先于 P1-5 的 tagging proxy;P1-18 先于 P1-17 M2/M3;P1-15 PR1 的锁对象可先供 P1-3 drain 使用。
