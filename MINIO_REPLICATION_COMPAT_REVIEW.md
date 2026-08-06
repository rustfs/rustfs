# RustFS 站点复制 / 桶复制 — MinIO 兼容性审查报告

> 审查日期:2026-08-05
> 审查对象:RustFS(worktree `reatang/minio-compatibility-review-03a7fb`)vs MinIO(`/Users/tang/Documents/GitHub/minio`)
> 审查方式:白盒代码对比(5 个维度并行审查)+ P0 问题对抗性复核
> 审查维度:站点复制白盒对比、桶复制白盒对比、mc 工具兼容性、S3 标准协议兼容性、代码结构与分层

---

## 一、总体结论

| 领域 | 兼容性评价 |
|---|---|
| **站点复制(RustFS↔RustFS + mc 管理)** | 良好。admin 端点全覆盖、JSON 结构对齐 madmin-go、请求体 DARE 加密兼容,mc admin replicate 全家桶基本可用 |
| **站点复制(RustFS↔MinIO 混合组网)** | **断裂**。4 个 P0:出站 join 路径 404、metainfo 大小写解析失败、STS item 类型名不一致、policy-mapping userType 数值错位 |
| **桶复制(控制面,S3 标准 API)** | 良好。Put/Get/DeleteBucketReplication、错误码、状态机字符串、xl.meta 内部键均对齐 |
| **桶复制(数据面,RustFS→MinIO)** | **断裂**。复制 PUT 缺 `?versionId=` 导致目标端版本漂移(P0);CopyObject 完全不复制(P0) |
| **mc 桶复制命令** | **部分断裂**。`mc replicate add` 默认参数即失败(P0);status/resync/backlog 响应结构不匹配导致静默空输出(P1) |
| **代码结构** | 桶复制侧迁移架构有纪律但成本高;**站点复制侧无领域层,约 9500 行业务逻辑堆在 admin handler,且存在 3 处反向依赖违反项目分层不变量(P0)** |

**做得好的地方**(已确认兼容,无需整改):复制状态机字符串(PENDING/COMPLETED/FAILED/REPLICA 含 legacy COMPLETE)、xl.meta 内部键双前缀(x-rustfs-internal- + x-minio-internal-)读写、ReplicateDecision 内部状态串格式、复制内部头主链路双前缀、Delete/VersionPurge 语义、Resync reset-id 判定、admin 路由 `/minio/admin/v3` 前缀别名、madmin DARE 加密流解密、站点复制 gob netperf 编码、`site-repl-<deploymentID>` 规则模板。

---

## 二、P0 问题清单(8 项)

| # | 问题 | 来源维度 | 断裂方向 |
|---|---|---|---|
| P0-1 | 出站 peer join 使用 MinIO 已移除的遗留路径 `/site-replication/join` → 404 | 站点复制 | RustFS→MinIO |
| P0-2 | 解析 MinIO metainfo(SRInfo)字段大小写不匹配 → add preflight 失败 | 站点复制 | RustFS→MinIO |
| P0-3 | STS 凭证复制 item 类型名 `sts-credential` vs `sts-account` | 站点复制 | 双向 |
| P0-4 | policy-mapping `userType` 数值语义错位(RustFS: None=0/Svc=1/Sts=2/Reg=3;MinIO: reg=0/sts=1/svc=2)→ 权限静默漂移 | 站点复制 | 双向 |
| P0-5 | 复制 PUT/CompleteMultipart 不携带 `?versionId=` query → MinIO 端版本号漂移、版本删除永久 no-op、双端静默发散(**功能视角复核:定级调整为 P1**,问题重述为"普通复制对象缺少可靠的源→目标版本身份策略";versionId query 是可行修复之一而非唯一正确方案) | 桶复制 | RustFS→MinIO |
| P0-6 | CopyObject(含 metadata-replace 自拷贝)完全不触发复制调度,对象静默不复制(**功能视角复核:定级调整为 P1**;scanner 在 ExistingObjectReplication 启用+状态为空时可最终补齐,但同步复制语义失效,且继承 stale COMPLETED / 显式 Disabled 场景长期漏复制) | 桶复制 + S3 协议 | 所有方向 |
| P0-7 | `mc replicate add` 默认参数(healthcheck-seconds=60)被硬拒 400;且字段单位按秒解析而 wire 为纳秒 | mc 兼容 | mc→RustFS |
| P0-8 | 架构:站点复制约 9500 行业务逻辑堆在 admin handler 单文件;app/storage 层 3 处反向导入 admin 层,违反 ARCHITECTURE.md 分层不变量 #1(**对抗复核后降级为 P1**:反向边已被 arch 守卫棘轮基线锁死,属受控技术债) | 代码结构 | — |

每项 P0 的对抗性复核结论、验证方案与解决方案见 **第五节**。

**修复状态(2026-08-05)**:7 项确认 P0 已全部修复并创建 PR(红灯→绿灯 TDD):P0-1 [#5748](https://github.com/rustfs/rustfs/pull/5748)、P0-2 [#5749](https://github.com/rustfs/rustfs/pull/5749)、P0-3 [#5750](https://github.com/rustfs/rustfs/pull/5750)、P0-4 [#5751](https://github.com/rustfs/rustfs/pull/5751)、P0-5 [#5752](https://github.com/rustfs/rustfs/pull/5752)、P0-6+P1-10 [#5753](https://github.com/rustfs/rustfs/pull/5753)、P0-7 [#5754](https://github.com/rustfs/rustfs/pull/5754)。合并顺序:#5748+#5749 同批;#5752 先于 #5753。

---

## 三、P1 问题清单

### 站点复制

| # | 问题 | 证据 | 影响 |
|---|---|---|---|
| P1-1 | ILM(lc-config)复制语义:对外开关限定 `replicateILMExpiry`,但发送端把**完整** lifecycle.xml 放入 `expiry_lc_config`,接收端整体覆盖/删除本地配置(功能视角复核:**确认,维持 P1**;更新时间检查只能拒旧,不能修复整体覆盖语义) | RustFS `bucket_meta.rs:948-951`、`site_replication.rs:7590-7683` vs MinIO `site-replication.go:1784-1810,6138` | lifecycle 同时含 expiry 与本地 transition 时,非 expiry 规则被错误传播或本地 transition 被覆盖。**缺"同步 expiry 后保留本地 transition"测试** |
| ~~P1-2~~→**P2-25** | `SRInfo.ilmExpiryRules` 从不填充,ILM 一致性状态恒为空(功能视角复核:**降级 P2**——仅影响管理面可观测性,不改变对象数据) | `site_replication.rs:4152-4266,4855-4868` | `mc admin replicate status --ilm-expiry-rules` 恒空,ILM 漂移不可见 |
| P1-3 | 无自动跨站元数据 heal(MinIO 有周期 heal 协程) | RustFS 仅 600s 本地 wiring 修复(`site_replication_reconcile.rs:34,59-81`)+ 手动 repair 端点 vs MinIO `site-replication.go:4257-4288` | 错过的 IAM/bucket 元数据更新持续漂移,须手工 repair |
| P1-4(拆分) | ①`sync` 同步复制指控:功能视角复核**不成立/证据不足**——RustFS 自身契约明确将 `sync_state` 定义为站点可达性/配置完整性健康状态且有测试,不能以他家同名字段判其错误(属"RustFS 独特设计保持不变"项,撤销);②`defaultbandwidth`:**确认,降级 P2**——公共 API 接受并持久化,但建 site replication bucket target 时不应用,reconcile 只保留既有 `bandwidth_limit`,配置成功但不生效 | `site_replication.rs:6303-6357,5004-5027` | ②为用户可见的"配置成功但无效"能力缺口 |

### 桶复制 / S3 协议

| # | 问题 | 证据 | 影响 |
|---|---|---|---|
| P1-5 | 未复制完成对象的 GET/HEAD 远端 proxy 未实现;也不识别 MinIO 的 `X-Minio-Source-Proxy-Request` 防环头 | 仅指标占位(`storage_api.rs:799-804`);`SUFFIX_SOURCE_PROXY_REQUEST` 定义后无人使用 vs MinIO `bucket-replication.go:2334,2409,2534` | active-active 复制滞后窗口内 RustFS 端 404 |
| P1-6 | `X-Minio-Source-Replication-{Tagging,Retention,LegalHold}-Timestamp` 三个时间戳头收发均缺失 | `replication_target_boundary.rs:251-297` 填了 options 但 `PutObjectOptions::header()` 不序列化;接收端不解析 vs MinIO `object-api-options.go:377-399` | active-active 下标签/retention/legal-hold 并发修改的 LWW 冲突解析退化,可能元数据回滚 |
| P1-7 | ARN 前缀 `arn:rustfs:` 与 `arn:minio:` 不互认(解析侧强制 `arn:rustfs:`) | `crates/ecstore/src/bucket/target/arn.rs:43,51` vs MinIO `bucket-targets.go:709` | 存量 MinIO 复制配置迁移被 StaleTarget 拒;原生 madmin SDK 解析 RustFS ARN 失败 |
| P1-8 | PutBucketReplication 校验缺口(规则数/Priority 唯一/ID 长度/Filter 互斥/2MB 上限全缺)+ 主动拒绝 `Destination.StorageClass` 等 MinIO/AWS 合法字段 | `bucket_usecase.rs:582-616`、`config.rs:143-232` vs MinIO `internal/bucket/replication/replication.go:29-90` | 非法配置被接受、优先级冲突行为不可预测;存量 AWS/Terraform 配置(含 StorageClass)直接 400 |
| ~~P1-9~~→**P2-26** | GetObject 响应缺 `x-amz-replication-status` 头(HEAD 有 GET 无),且 GET 专门把它从 metadata 过滤掉(功能视角复核:**降级 P2**;GET/HEAD 不一致确认,缺 GET replication-status 回归测试) | `object_usecase.rs:5696-5735`、`options.rs:702` vs MinIO `api-headers.go:236-238` | 依赖 GET 判断复制状态的客户端/监控失效;修复约一行 |
| P1-10 | Snowball auto-extract 解包对象不触发复制(功能视角复核:**确认,维持 P1**,但"全部永不复制"不准确——scanner 在状态空+ExistingObjectReplication 启用时可补齐;显式 Disabled 等场景长期遗漏,即时复制始终失效。带 REPLICA 状态的入站成员须继续避免回环)。**已随 [#5753](https://github.com/rustfs/rustfs/pull/5753) 修复**(含入站复制 PUT 不再被误派发 extract 的次生缺陷) | `object_usecase.rs:8201` vs MinIO `object-handlers.go:2452,2510-2511` | 批量导入对象不即时复制;缺普通解包成员复制结果的测试(已在 #5753 补充 e2e) |

### mc 响应结构(静默空输出类)

| # | 问题 | 证据 | 影响 |
|---|---|---|---|
| P1-11 | `?replication-metrics[=2]` 响应为 Rust snake_case,minio-go MetricsV2 期望 camelCase(`currStats`/`queueStats`/…) | `stats.rs:617-770`、`admin/router.rs:1583-1592` vs MinIO `bucket-stats.go:154-188` | `mc replicate status` 不报错但全零(静默错误) |
| P1-12 | replication-reset(resync)响应壳不匹配:`{"Targets":[{"Arn","ResetID",...}]}` vs `{"target":[{"arn","resetid","resyncStatus",...}]}` | `router.rs:126-198,1735-1803` vs MinIO `bucket-replication-utils.go:613-636` | `mc replicate resync start/status` 输出空;仅响应壳问题,修复成本低 |
| P1-13 | `/v3/replication/mrf` 与 `/v3/replication/diff` 返回单个聚合对象而非条目流(代码自述 deliberate) | `replication.rs:695-725,879-911,998-1047` vs madmin-go `replication-api.go:104-176` | `mc replicate backlog` 输出空;`node`/`arn`/`verbose` 参数被忽略 |
| P1-14 | set-remote-target 请求体 `deny_unknown_fields` + 字段名偏差(期望 `bandwidth_limit`,madmin 发 `bandwidthlimit`;`session_token` vs `sessionToken` 等) | `handlers/replication.rs:88-95,108-163` vs madmin-go `bucket-targets.go:76` | `mc replicate add/update --bandwidth` 整请求失败;凡 omitempty 字段一旦出现即 400 |

### 代码结构

| # | 问题 | 证据 | 影响 |
|---|---|---|---|
| P1-15 | 站点复制状态两套归一化实现(handler 类型化 vs service 无类型 JSON),且 reload 的 read→normalize→save 全程无共同分布式对象锁,存在 lost-update 竞争;repair state 已用 `with_config_object_write_lock` 包住完整 RMW,主 state 未采用同等保护(功能视角复核:**确认,维持 P1**;进程内 `SITE_REPLICATION_STATE_LOCK` 与单次 read/save 各自的对象锁均不能保护跨调用 RMW:A 读旧→B 另节点写入→A 用旧快照覆盖,B 丢失) | `handlers/site_replication.rs:114,347,1039-1130` vs `service/site_replication.rs:26-135` | 归一化语义可 drift;多节点/RPC 并发写状态互相覆盖。**缺多节点/双写者 lost-update 回归测试** |
| P1-16 | 复制状态机类型双份定义:`rustfs-filemeta` 与 `rustfs-replication` 各持一份(ReplicationStatusType/VersionPurgeStatusType/ReplicationState/MrfReplicateEntry/ReplicateObjectInfo),靠 boundary 双向转换 | `crates/filemeta/src/replication.rs` vs `crates/replication/src/filemeta.rs` | 状态机语义修改须同步两处+转换层,漏一处即静默数据语义错误;建议加 enum 对账测试 |
| P1-17 | 桶复制逻辑分裂:`crates/replication` 仅契约,执行引擎(pool 5947 行、resyncer 4090 行)仍在 ecstore,中间 20+ 个 boundary/bridge 微文件;迁移无完成判据,脚手架有固化风险 | `crates/ecstore/src/bucket/replication/README.md`、`mod.rs:15-45` | 可读性/可维护性成本;需设定迁移里程碑 |
| P1-18 | 超长函数集中在复制热路径:`resync_bucket` 536 行、`replicate_all` 403 行、`start_mrf_processor` 305 行、`apply_iam_item` 248 行 | `replication_resyncer.rs:546`、`replication_pool.rs`、`site_replication.rs:7806` | 正确性审查与修改风险高 |

### 第三方复审新增与调整项(功能视角二次复核后)

| # | 问题 | 来源 | 影响 |
|---|---|---|---|
| P1-19 | 普通复制对象缺少可靠的源→目标版本身份策略:PUT 响应的目标版本 ID 未捕获/持久化,对不支持 versionId query 的目标(原生 AWS S3 等),后续版本删除复制落空;MRF 只会重试同一个错误身份,HEAD ETag fallback 不能修复删除 | P0-5 复审 | 非 MinIO 系目标的版本化复制双端发散。缺"目标自行分配版本 ID"场景测试 |
| P1-20 | 缺少 scanner 补偿边界的 e2e:ExistingObjectReplication Enabled/Disabled × 空状态/继承状态 组合下的补齐与不补齐行为无回归覆盖(Copy 与 Snowball 两路径) | P0-6/P1-10 复审 | scanner 兜底语义变化不可见 |
| P1-21 | delete-marker 延迟 purge 失败静默丢弃(由 P2-20① 升级):目标删除失败无日志/状态/MRF,目标端 marker/版本可能永久残留 | P2-20 复核升级 | 数据一致性;缺失败注入测试 |
| P1-22 | 桶复制整体 SSE 支持能力缺口(替代原 P2-23):SSE-S3/SSE-KMS 所有复制模式统一 fail closed,SSE-C 失败被 e2e 钉为当前行为,无 encrypted-object resync e2e | P2-23 复核改写 | 加密对象跨站不复制;需覆盖普通复制/Heal/Resync/Multipart 四模式 |

### 功能视角二次复核采纳记录(backlog#1675,基于 main f0c4fbd28)

复核共 10 项,判定依据为 RustFS 自身功能契约与实际调用链,不以对齐 MinIO 为正确性标准。采纳结果:

| 原编号 | 复核结论 | 采纳动作 |
|---|---|---|
| P0-5 | 确认,P0→P1,问题重述为"源→目标版本身份策略缺失" | 定级调整;修复已合 [#5752](https://github.com/rustfs/rustfs/pull/5752);残留缺口 P1-19 |
| P0-6 | 确认,P0→P1,scanner 描述纠正 | 定级调整;修复已合 [#5753](https://github.com/rustfs/rustfs/pull/5753);测试缺口 P1-20 |
| P1-1 | 确认,维持 P1 | 补记"expiry 同步后保留本地 transition"测试缺口 |
| P1-2 | 确认,P1→P2(仅管理面可观测性) | 改编号 P2-25 |
| P1-4 | 拆分:`sync` 指控不成立(RustFS 自身契约定义为健康状态,有测试);`defaultbandwidth` 确认为 P2 能力缺口 | `sync` 撤销并归入"独特设计保持不变";`defaultbandwidth` 降 P2 |
| P1-9 | 确认,P1→P2 | 改编号 P2-26;补记缺 GET 回归测试 |
| P1-10 | 确认,维持 P1,"全部永不复制"改为"即时复制失效+部分场景长期遗漏" | 已随 [#5753](https://github.com/rustfs/rustfs/pull/5753) 修复(含回环防护) |
| P1-15 | 确认,维持 P1(竞争机理精确化:跨调用 RMW 无共同分布式锁) | 补记缺双写者 lost-update 测试 |
| P2-20① | 确认,P2→P1(延迟 purge 失败静默丢弃部分) | 升级为 P1-21;②③维持 P2 |
| P2-23 | resync 专属指控不成立;暴露桶复制整体 SSE 能力缺口 | 撤销原表述,改立 P1-22 |

**复核指出的测试补齐清单**(均未运行跨实例集成验证,需落地):目标自行分配版本 ID、Copy/Snowball scanner 补偿边界、lifecycle expiry/transition 保留、site state 双写竞争、delayed purge 失败注入、encrypted-object resync。

---

## 四、P2 问题清单

### 站点复制
- **P2-1** `showDeleted` 选项与 `bucketDeletedTimestamp` 未实现(`site_replication.rs:1364-1381`)
- **P2-2** 错误码泛化:统一 `InvalidRequest`/`InternalError`,无 MinIO 的 9 个 `XMinioSiteReplication*` 专用码(400/503 语义丢失)
- **P2-3** `make-with-versioning` 忽略 `versioningEnabled`/`forceCreate` 参数,恒 true(`site_replication.rs:8597-8627`)
- **P2-4** netperf 返回"不支持"占位(gob 格式兼容不会崩);devnull 有请求体大小上限(MinIO 无限 discard)
- **P2-5** Metrics 摘要仅含本站,无 per-peer 链路统计(downtime/latency/失败窗口)
- **P2-6** `external-user`/`credential` IAM item 未实现——与本仓 MinIO 版本等价缺失,结构已预留;对接新版 MinIO 时会成缺口
- **P2-7** 本地 deploymentID 缺失时回退 endpoint 哈希(16 位 hex,非 UUID 形态)

### 桶复制 / S3 协议
- **P2-8** 遗留内部 client 头名错误:`X-Source-DeleteMarker`/`X-Check-Replication-Ready` 缺 `X-Minio-` 前缀(`client/api_stat.rs:191-231`,当前路径未激活,潜伏缺陷)
- **P2-9** Remote target admin 错误码扁平化(MinIO 有 404/503 专用码,RustFS 统一 400/500)
- **P2-10** Remote target 拒绝 `disableProxy`/`edge`/`edgeSyncBeforeExpiry` 等 madmin 字段(非默认参数,影响小)
- **P2-11** `list-remote-targets` 序列化偏差:`bandwidth_limit`/`storage_class`/`deployment_id`/`reset_id`/`session_token` vs madmin 的 `bandwidthlimit`/`storageclass`/`deploymentID`/`resetID`/`sessionToken`;`healthCheckDuration`/`totalDowntime` 按秒序列化而 Go 按纳秒解;`type` 过滤参数被忽略
- **P2-12** set-remote-target?update=true 忽略 madmin 的 op 标志(creds/sync/proxy/…),固定整体覆盖
- **P2-13** XML 反序列化:Rule 内未知元素严格报 MalformedXML(顶层却跳过,行为不一致);缺 `<Role>` 报 MalformedXML(Go 容忍)——向前兼容性差,当前主流客户端不受影响
- **P2-14** `ReplicaModifications` 默认 Disabled(与 AWS 一致、与 MinIO 的注入 Enabled 分歧);PUT 时不像 MinIO 那样注入默认元素回写
- **P2-15** PutBucketReplication 要求预先注册 remote target(与 MinIO 同构、与纯 AWS 流程分歧),报错未指引先建 target
- **P2-16** GetBucketReplication 响应无 xmlns(与 MinIO 一致,极少数严格 SDK 可能拒收)
- **P2-17** 站点复制启用时不阻止普通用户直接改桶复制配置(MinIO 非 root 报 `ErrReplicationDenyEditError`)
- **P2-18** Prometheus 指标名对齐 metrics-v3 但注册前缀为 rustfs 体系;versioning 错误文案与 MinIO 不同(code 一致)

### 代码结构
- **P2-19** `apply_iam_item` / bucket-ops 用裸字符串 match 分发,无法穷尽检查;建议改 `#[serde(tag)]` 枚举
- **P2-20(拆分)** 静默吞错:①`replication_resyncer.rs:1693` delete-marker 延迟 purge 失败被 `let _ =` 丢弃,target client 缺失时直接跳过——**功能视角复核:升级为 P1-21**(失败后无日志、无状态更新、不入 MRF,目标 delete marker/版本可能永久残留;启动前的 5 次循环只是等源 marker 消失,不是对目标删除失败的重试。缺注入目标删除失败并验证重试/状态/MRF 的测试);②`site_replication.rs:8661` purge-deleted-bucket 吞掉非 NotFound 错误、`:9227` cancel resync 失败无痕迹——维持 P2
- **P2-21** `MrfV2` 全套机制(Error/Capabilities/Readiness/Reader/Envelope)未接线,生产只用 v1,属投机代码
- **P2-22** `persist_site_replication_state` 双重 clone + 双重 normalize(`site_replication.rs:1143-1152` → `:1116-1122`)
- **P2-23(撤销并改写)** 原"resync 不处理 SSE"指控不成立——`ReplicationType::Resync` 与普通复制/Heal 最终走同一 `replication_put_object_options`,`// TODO: SSE` 不构成 resync 独立行为差异。真实状态:SSE-S3/SSE-KMS 在**所有复制模式**下统一 fail closed,SSE-C 普通桶复制失败已被现有 e2e 钉为当前行为,且无 encrypted-object resync e2e → 改立能力项 **P1-22"桶复制整体 SSE 支持"**(需分别覆盖普通复制、Heal、手动 Resync、Multipart)
- **P2-24** `crates/replication` 命名误导(名为复制引擎实为契约库),建议 lib.rs 顶部文档说明
- 正面确认:生产代码 unwrap/expect 纪律良好(几乎全在测试模块);MinIO 概念映射(ReplicationPool/Resyncer/MRF/TargetClient)桶复制侧清晰,站点复制侧缺 `SiteReplicationSys` 聚合体

---

## 五、P0 问题对抗性分析(复核结论 + 验证方案 + 解决方案)

### P0-1 出站 peer join 路径 — **CONFIRMED(比原指控更严重)**

**复核结论**:指控全部成立,且加重三点:
1. `/minio/admin/v3/site-replication/join` 在 MinIO 历史上**从未存在过**(`git log -S` 追到功能诞生的 2021 年首个提交,注册的就是 `peer/join`)。RustFS 实现者疑似被 MinIO `admin-handlers-site-replication.go:76` 一条过时的文档注释误导。
2. 无任何 404 回退、版本探测或 feature flag;唯一的重试逻辑只针对 secret 不匹配(`site_replication.rs:3036-3082`),404 直接失败。
3. 现有单测 `:13683-13696` 正在**固化错误行为**(测试名声称匹配 MinIO 路由,断言的却是不存在的路由)。RustFS↔RustFS 之所以不暴雷,是因为 RustFS 入站自己注册了该错误路径的兼容别名,掩盖了 bug。

**影响面**:RustFS 发起的 add(含 MinIO 站点)、服务账号轮换通知 MinIO peer 均断;MinIO→RustFS 与 RustFS↔RustFS 不受影响;其余 peer/* 端点走通用前缀改写,路径正确。

**修路径还不够,还有三处 join 协议分歧须同批修**:①加密判定 `site_replication_peer_payload_encrypted`(:2899-2901)只对旧路径加密,MinIO `SRPeerJoin` 强制解密,须跟随路径改;②MinIO join 成功返回**空 body**,RustFS `:8163` 强制解析 `SRPeerJoinResponse` 会失败,须容忍空 body(peer 身份回退用 preflight 已取得的数据合成);③`deferSyncStateEnable`/`bootstrapToken` 对 MinIO 无效但不阻断(行为差异,建议日志标注)。

**验证方案**:
- 单测:翻转 `:13683`/`:13699` 两个测试断言为 `peer/join`(把固化 bug 的测试变成回归防护)。
- 集成测:测试内起 axum stub 精确复刻 `admin-router.go` 路由(仅注册 `PUT .../peer/join`,其余 404),handler 内用 `decrypt_stream_io` 验证 body 是 madmin 兼容密文,返回 200 空 body;断言修复前 404、修复后全链路成功。
- e2e:docker compose(rustfs+minio),RustFS 侧 `mc admin replicate add`,MinIO 侧 `mc admin trace -a` 断言 `PUT .../peer/join` 200。注意:**e2e 会先被 P0-2 的 preflight 挡住,两问题必须同批修复才能全链路验证**。

**解决方案**(均在 `handlers/site_replication.rs`):删除 :2885-2886 的 join 特判使其落入通用前缀改写;:2899-2901 加密判定改为对 `peer/join` 返回 true;:8163 响应解析容忍空 body;更新两个单测。
**滚动升级风险**:必须保留入站的 `/v3/site-replication/join` 旧路径路由(旧版 RustFS 出站仍发它);发版前对最近 release tag 复核旧版入站已注册 `peer/join`。

### P0-2 SRInfo 大小写不匹配 — **CONFIRMED(范围精确化)**

**复核结论**:成立。madmin-go v3.0.109(minio go.mod 锁定版)`SRInfo` 除 `APIVersion` 外 12 个顶层字段**全部无 json tag**,Go 按 PascalCase 序列化;RustFS `SRInfo` serde 大小写敏感、全字段 `#[serde(default)]` → 解析 MinIO 输出**不报错而是静默全空**。精确化:**不兼容仅限 SRInfo 顶层 12 个字段**,嵌套结构(SRBucketInfo/SRStateInfo/SRIAMPolicy 等)madmin 本就带小写 tag,不受影响。`:5581` 的 `"buckets"|"Buckets"` 手写双读证明作者已知 MinIO 输出 PascalCase,只是未系统化修复。

**影响面**:RustFS 发起 add 时 preflight 硬失败("site did not report deploymentID")——**触发顺序先于 P0-1 的 join**;`mc admin replicate status` 对 MinIO peer 静默显示全空/全 mismatch(HTTP 200,无报错)。MinIO 读 RustFS 方向因 Go unmarshal 大小写不敏感而无恙。

**验证方案**:
- 单测(crates/madmin):用 Go `json.Marshal(madmin.SRInfo{...})` 真实生成的 PascalCase JSON 作 fixture,断言反序列化后字段非空;再加序列化回归断言输出仍为 camelCase(保证 RustFS↔RustFS 不回归)。
- 集成测:stub 在 metainfo 端点返回 PascalCase body,走 `remote_add_preflight_info`,断言不再报错。
- e2e:与 P0-1 同批,`mc admin replicate status --json` 断言 MinIO 站点条目完整。

**解决方案**:`crates/madmin/src/site_replication.rs:642-670` 为 12 个顶层字段逐一加 `#[serde(alias = "...")]`(精确取 Go 字段名,注意是 `ILMExpiryRules` 不是 `IlmExpiryRules`)。alias 只影响反序列化,出站格式零变化,风险几乎为零。**只加顶层、不扩散到嵌套结构**,并留注释说明原因。回归防护关键是把 Go 真实输出固化为测试 fixture。

### P0-7 `mc replicate add` 默认参数被拒 + 单位错误 — **CONFIRMED**

**复核结论**:全部反驳方向反向坐实(本地有 mc 源码,非推断):
- mc `replicate-add.go:93-95` 默认 `healthcheck-seconds=60`,`:301-303` 无条件调用 `SetRemoteTarget`,失败即终止,无跳过路径;
- madmin `bucket-targets.go:79` `HealthCheckDuration time.Duration` 无自定义 Marshal → wire 上是纳秒整数 `60000000000`;
- RustFS `handlers/replication.rs:213-225` 对非零值必拒 400;`mc replicate update` 同样失败;无老端点绕过。
- **单位错误独立成立且双向**:请求侧按 `Duration::from_secs` 解析(60e9 ns 会被当 60e9 秒 ≈ 1900 年);响应/持久化侧 `bucket_target.rs:195-197` 按秒序列化,mc 按纳秒解(60s 显示为 60ns),同时构成与 MinIO `bucket-targets.json` 的持久化格式偏差。
- **为何没被发现**:这是刻意的"能力契约式拒绝"策略,且有单测 `replication.rs:1353-1379` 固化拒绝行为;e2e 全部自行构造 JSON、不含该字段,测的是"RustFS 自己的请求形态"而非"mc 默认请求形态"。缓解:`--healthcheck-seconds 0` 时字段 omitempty 被省略可通过,但默认路径必失败,P0 成立。

**验证方案**:复现——`mc replicate add rustfs/src --remote-bucket http://ak:sk@target/dst` 预期 400;修复后——madmin 形态 payload(60e9 ns)单测断言内部 Duration==60s;set→list 往返断言响应为纳秒;e2e 增加"mc 默认 payload"用例;持久化防御性读回归(旧秒格式升级后读取不变)。

**解决方案(分阶段)**:
1. **解阻塞**:从不支持清单移除 `healthCheckDuration`(能力契约版本号递增);请求按 `Duration::from_nanos` 解析(`total_downtime` 同步核查);调度上显式忽略并在契约/文档标注"接受但暂不生效";响应侧新增 DTO 按纳秒序列化(**勿直接改 `bucket_target.rs` 的 `duration_seconds`,它同时是持久化格式**);持久化读取加防御(≥10^7 视为纳秒),写入统一新格式。
2. **落地语义**:`bucket_target_sys.rs:332-441` heartbeat 循环改为按 target 取值,对齐 MinIO(默认 5s、有下限)。
3. **防复发**:建立容器内跑真 mc 命令的兼容 e2e 通道,覆盖 `replicate add/update/status`。

### P0-8 站点复制架构 — **事实 CONFIRMED,定性部分 REFUTED,降级为 P1**

**复核结论**:巨型文件(14614 行,非测试约 9533 行,24 个 handler)与三处反向导入全部属实;但"失察"定性被推翻:
- `scripts/check_layer_dependencies.sh` **已建模并拦截**这些边,`layer-dependency-baseline.txt` 棘轮基线逐条列出全部 46 条存量反向边,**新增反向边 CI 必炸**;
- `ecfs.rs` 被脚本刻意归类为 interface 层(有意的建模决策);
- ARCHITECTURE.md 自己声明部分不变量 "currently violated... documenting them makes violations explicit and trackable";git 历史显示这是已知、受控、正在偿还的过渡态。
- **结论:不构成正确性风险,从 P0 降为 P1(可维护性债务)**。真实成本:9.5k 行单文件的评审/合并冲突/增量编译负担,hook 直连使 app/storage 单测无法脱离 admin 层。

**验证方案**:每阶段跑 `make pre-pr`;每消除一条反向边即**删除基线对应行**(而非重生成),使回归必炸;行为回归靠 site replication e2e + 路由快照测试 + `git diff --color-moved` 评审纯移动。

**解决方案(分阶段)**:
1. **解反向依赖(低风险,先做)**:复用 `site_replication_reconcile.rs` 已验证的 OnceLock 注册模式——bucket 三个 hook 在 app 层定义 fn-pointer 契约、admin 构建路由时注册;`node_service.rs` 的 reload 走 infra 层"运行时重载注册表"。注册缺失时显式降级(warn + no-op)。
2. **文件拆分(纯移动)**:`site_replication.rs` → 模块目录:`transport`(peer client/DNS/TLS)、`gob`、`state`(注意 config key 路径不可变)、`iam_sync`、`heal`、`handlers`(24 个薄 handler)。
3. **领域下沉(风险最高,最后做)**:hook 解耦后把 gob/transport/状态机移入独立 crate,注意全局状态清单(`docs/architecture/global-state-inventory.md:114`)。

### P0-3 STS item 类型名不一致 — **CONFIRMED(双向硬断)**

**复核结论**:成立,且两端都是**报错而非静默忽略**:MinIO 收到 `"sts-credential"` 走 default 分支返回 400 `errSRInvalidRequest`;RustFS 收到 `"sts-account"` 返回 NotImplemented。两端 heal/重试机制都会永久重试失败(MinIO 日志持续 "Unable to heal temporary credentials")。MinIO 当前版本 STS 复制发送面很广(AssumeRole/WebIdentity/ClientGrants/LDAPIdentity/Certificate 全系 + sftp/ftp + heal 路径)。除类型串外 `SRSTSCredential` 字段双方完全对齐——**只差这一个字符串**(推测 RustFS 实现时把 madmin 的 JSON 字段名 `stsCredential` 误当成了类型常量)。

**影响面**:跨厂商 STS 临时凭证双向不复制(客户端在对端站点 `InvalidAccessKeyId`),纯可用性问题,无权限漂移;RustFS↔RustFS 自洽。

**验证方案**:单测——出站产物断言 `type == "sts-account"`(改 `federated_identity.rs:497` 现有快照测试);入站构造 `"sts-account"` item 断言不落 NotImplemented。e2e——compose(RustFS+MinIO,root 凭证必须一致,否则 token 验签失败会误判修复无效):对 MinIO assume-role 拿临时凭证访问 RustFS,修复前 InvalidAccessKeyId、修复后成功;反向同测。

**解决方案**:出站(`sts.rs:248`、`federated_identity.rs:241`)改发 `"sts-account"`(提常量集中定义);入站(`site_replication.rs:7857`)match 臂改 `"sts-account" | "sts-credential"`(**永久保留旧别名**兼容旧 RustFS peer)。滚动升级窗口内新→旧 RustFS 会降级(warn+重试,peer 升级后收敛);STS 凭证短生命周期,不建议为此拆两阶段发布。

### P0-4 policy-mapping userType 数值错位 — **CONFIRMED(比指控更严重)**

**复核结论**:数值表属实(RustFS: None=0/Svc=1/Sts=2/Reg=3;MinIO: unknown=-1/reg=0/sts=1/svc=2),wire 上确为数值、无翻译层。对抗复核修正与加重:
- **RustFS→MinIO 方向今天"侥幸能用"**:RustFS 当前只出站 Reg=3 与组的 0,MinIO 对超范围值静默落 default 分支,恰好落对位置;
- **MinIO→RustFS 方向三类断裂**:①**组映射硬失败(新发现)**——MinIO 组映射发 `UserType: -1`,RustFS `user_type: u64` 反序列化直接报错,整个 item 被拒,组→策略映射完全无法同步;②STS 用户映射(MinIO 发 1)被 RustFS 解释为 Svc,落错前缀/缓存,联邦用户在 RustFS 站点**静默丢权限**;③svc=2 被解释为 Sts,同类错位;
- **低概率提权路径**:LDAP DN/OIDC 主体的映射被误存入常规用户缓存后,若本地恰有同名静态用户则继承本不属于它的策略——名字碰撞概率低但非零,这是保 P0 的理由。

**验证方案**:单测——wire 编解码全矩阵(-1/0/1/2/3/非法值);e2e——MinIO 侧 `mc admin policy attach --group` 修复前 RustFS 查不到组实体、修复后可见;`mc idp ldap policy attach` 修复前落 `policydb/service-accounts/` 且访问被拒、修复后落 `sts-users/` 且放行;反向回归守住"侥幸兼容";混版本(旧+新 RustFS)双向 attach 互通。

**解决方案(核心原则:不改 `UserType::to_u64/from_u64`)**——该编码被集群内部节点 RPC 使用(`node_service.rs:1513`),改动会破坏同集群滚动重启。只在站点复制 wire 边界加 MinIO 语义编解码:
1. `SRPolicyMapping.user_type` 由 `u64` 改 `i64`(必须,才能收下 -1);
2. 出站 `sr_wire_user_type`:Reg→0/Sts→1/Svc→2,组一律发 0(对 MinIO 与旧 RustFS 同时兼容);入站 `user_type_from_sr_wire`:-1→None/0→Reg/1→Sts/2→Svc/**3→Reg(旧 RustFS 别名,永久保留)**;
3. 兼容矩阵已逐格验证:新↔旧 RustFS、MinIO↔新 RustFS 全通;唯一残余窗口(未来出站 Sts/Svc 映射对旧 RustFS 错读)当前不可达,在 doc comment 写明约束;
4. 回归防护:编解码矩阵单测 + "wire 常量契约"字面值断言测试(防止将来被"顺手统一"回内部编码)+ e2e 进 P0 套件;顺带把 `SRCredInfo.iam_user_type` 一并改 `i64` 复用同一编解码,消除同族隐患。

### P0-5 复制 PUT 缺 `?versionId=` query — **CONFIRMED**

**复核结论**:所有反驳方向均失败,指控成立:
- minio-go 官方复制端(v7.0.91)`api-put-object-streaming.go:767-776` 等三处全部是 `urlValues.Set("versionId", ...)`——**query,不是 header**;`x-minio-source-version-id` 这个 header 在 MinIO 全仓不存在,被静默忽略;
- multipart 的版本在 **initiate 时**决定(`erasure-multipart.go:458-460`,为空即生成新 UUID),complete 不读 versionId;
- aws-sdk-s3 `PutObjectInput` 无 versionId 成员属实,但 DELETE 路径已用 `.set_version_id()` 正确落 query,证明是遗漏而非不可行;
- RustFS↔RustFS 不受影响的原因:RustFS 接收端有私有 header fallback(`options.rs:296-301`),恰好掩盖了 bug。

**影响加重**:除版本漂移与按版本删除永久 no-op 外,目标校验/heal 用源 versionId `head_object` 永远 miss → **反复重传,目标端版本无限膨胀**。另有边缘缺陷:RustFS 内部 null 版本是 nil-UUID,直接发 query 会被 MinIO 当真实版本;minio-go 约定发字面 `"null"`。

**验证方案**:L1 e2e(本仓可落地,红→绿)——复用 `crates/e2e_test/src/fake_s3_target/`(已解析 versionId query 并写 journal),断言 PutObject/CreateMultipartUpload 请求的 query == 源版本;L2 互操作(docker + 真 MinIO)`mc ls --versions` 断言目标 versionId == 源、删源版本目标同步消失;L3 单测 nil-UUID→`"null"` 映射。

**解决方案**(`bucket_target_sys.rs`):`put_object`/`create_multipart_upload` 在 `map_request` 闭包内改写 URI 追加 `versionId` query(nil-UUID 映射 `"null"`);保留双 header 兼容旧版 RustFS 接收端;顺带核对 delete 路径的 nil-UUID 映射。**签名安全性已验证**:`map_request` 挂在 `modify_before_signing`,query 会进 canonical request,不会 SignatureDoesNotMatch。非版本化目标桶沿用"空则不发",`"null"` 值 MinIO 免检。

### P0-6 CopyObject 不触发复制 — **CONFIRMED(附带加重发现)**

**复核结论**:三个反驳方向全部不成立:
- copy 直接调 `store.copy_object`,不经 put 路径;ecstore 层 copy 实现无任何调度;
- **scanner 兜底不存在(关键)**:heal 入队条件是状态为 Pending/Failed 或手动 resync;而 copy 路径不 stamp PENDING(对照 put 路径 `object_usecase.rs:5255-5266`),状态为空 → heal 判定 Skip。
- **加重发现**:copy 路径没有 MinIO `filterReplicationStatusMetadata` 的等价清理——COPY 指令下源对象的旧复制状态可能原样带到目的对象,**伪造 COMPLETED 假状态**。
- 附带 P1(snowball `execute_put_object_extract`)同样确认:无 stamp 无 schedule。

**影响面**:配复制规则的桶上,CopyObject 写入的对象(跨桶复制、rename 工作流、REPLACE 元数据更新)永不复制、scanner 不捞、仅手动 resync 可补;还可能带 stale 假状态。

**验证方案**:e2e(参照 `replication_extension_test.rs` 双实例)——copy 后断言目的对象在目标桶超时内出现、源 COMPLETED、目标 REPLICA、无 stale 状态;snowball 参照 `snowball_auto_extract_test.rs` 加成员对象复制断言;usecase 单测用 `storage_api.rs:641` 现有 test-only 调用计数断言 copy/extract 触发决策与调度。

**解决方案**(`object_usecase.rs`):
1. `execute_copy_object` 在 `store.copy_object` 之前算一次 `dsc = must_replicate_object(...)`,`replicate_any` 时向 `dst_opts.user_defined` stamp pending + timestamp(严格镜像 put 路径,单一 dsc 决策贯穿两阶段);
2. 同处清理源带来的复制状态 reserved 元数据;
3. copy 成功、锁释放后 `schedule_object_replication`;
4. `execute_put_object_extract` 对每个解出对象同样处理。
风险已排除:replica 判定内置于 `must_replicate_object` 不会回环;self-copy 调度与 MinIO 一致。
**落地顺序约束:先修 P0-5 再修 P0-6**——否则 copy 的失败重试经 heal 兜底后,只会在 MinIO 端制造更多漂移版本。

### 第三方复审修正(2026-08-05,修复分支均已完成 review)

**P0-5 修正**:问题的准确表述应为"**普通复制对象缺少可靠的源→目标版本身份策略**"——复制 PUT 只返回成功/失败,未捕获目标实际分配的版本 ID(已核实 `bucket_target_sys.rs` put 路径无 `res.version_id()` 捕获,delete 路径 :2030 有);multipart 只保留 upload ID。`fix/p0-5` 的 versionId query 方案对 MinIO/RustFS 目标成立(目标端沿用源版本 ID,身份问题消解),但对**忽略该私有 query 的目标(如原生 AWS S3)**身份问题仍在:目标自行生成版本 ID → 后续按源版本 ID 的删除复制落空。第三方建议定级 P1(修复已完成,残留缺口另行跟进):可选方案包括捕获 PUT 响应的 `x-amz-version-id` 并持久化源→目标映射。→ 记为 **P1-19(新增)**。

**P0-6 修正**:scanner"兜底不存在"的表述过度。已核实 `crates/replication/src/operation.rs` `resync_target_for_object`:无 reset 记录且复制状态为 Empty 时返回 `replicate=true`,即 ExistingObjectReplication 启用时 scanner **可能最终补齐**空状态对象,无需手动 resync。准确结论:即时/同步复制语义失效(P0 定级依据),且以下场景**长期**漏复制——①源对象 COMPLETED 等复制元数据被 Copy 继承致误判(`fix/p0-6` 已修,清理先于决策);②显式 ExistingObjectReplication=Disabled;③其他无法进入 existing-object 补偿的场景。`fix/p0-6` 分支已含 copy 调度 e2e 与 stale 元数据白盒断言;**scanner 补偿边界的 e2e 仍缺** → 记为 **P1-20(新增)**。

### 对抗性复核总览

| 问题 | 复核结论 | 关键修正/加重 |
|---|---|---|
| P0-1 join 路径 | CONFIRMED,加重 | 路径在 MinIO 从未存在;现有单测固化错误;修复需同批改加密判定与空响应容忍 |
| P0-2 SRInfo 大小写 | CONFIRMED,精确化 | 仅顶层 12 个无 tag 字段;preflight 失败先于 P0-1 触发 |
| P0-3 STS 类型名 | CONFIRMED | 双向硬断、两端 heal 永久重试;只差一个字符串 |
| P0-4 userType 错位 | CONFIRMED,加重 | MinIO 组映射发 -1 → RustFS u64 解析硬失败;存在低概率名字碰撞提权路径;修复不得触碰内部 RPC 编码 |
| P0-5 versionId query | CONFIRMED,加重 | heal 反复重传致目标版本膨胀;nil-UUID 需映射 "null" |
| P0-6 CopyObject | CONFIRMED,加重 | scanner 兜底不存在;stale COMPLETED 假状态;须在 P0-5 之后落地 |
| P0-7 healthCheckDuration | CONFIRMED | 单位错误双向独立成立;有单测固化拒绝行为 |
| P0-8 架构 | 事实 CONFIRMED,定性 REFUTED | 反向边被棘轮基线锁死,降级 P1(受控技术债) |

---

## 六、修复路线图(2026-08-05 更新)

**✅ 第一批已完成**:全部 7 项 P0 已修复并创建 PR(见第二节修复状态;P1-10 snowball 随 #5753 一并修复)。待合并,注意顺序约束:#5748+#5749 同批、#5752 先于 #5753。

**第二批(数据一致性优先,采纳功能视角复核定级)**
1. **P1-21** delete-marker 延迟 purge 失败静默丢弃(复核升级,数据一致性,建议单独小 PR + 失败注入测试)
2. **P1-19** 源→目标版本身份策略(捕获 PUT 响应 `x-amz-version-id` / 持久化映射,覆盖非 MinIO 系目标)
3. **P1-1** ILM expiry 同步语义(只传播 expiry、保留接收端本地 transition + 对应测试)
4. **P1-15** site state RMW 分布式锁统一(对齐 repair state 的 `with_config_object_write_lock` 模式)+ 双写者回归测试
5. **P1-22** 桶复制 SSE 能力(普通复制/Heal/Resync/Multipart 四模式,先补 encrypted-object e2e 钉现状)

**第三批(mc 可观测性与互操作补齐)**
6. P1-11/12/14 mc 响应结构 serde rename(改动小、消除静默空输出)
7. P1-7 ARN 解析侧兼容 `arn:minio:` 前缀
8. P1-5 GET/HEAD proxy、P1-6 时间戳头、P1-3 自动跨站 heal
9. P1-20 scanner 补偿边界 e2e;P0-7 阶段 2(per-target 心跳 + healthcheck update op)
10. P2-26 GET 补 `x-amz-replication-status`(约一行)+ 回归测试;P2 清单其余项

**第四批(架构与长期)**
11. P0-8(降级 P1)架构:先解 3 处反向依赖(复用 reconcile 注册模式),再拆分/下沉站点复制领域模块
12. P1-16 类型对账测试、P1-17 迁移完成判据、P1-8 配置校验补齐
