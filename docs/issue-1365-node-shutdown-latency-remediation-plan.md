# Issue #1365 节点下线后约 30 秒访问抖动的可执行实战方案

- Issue: https://github.com/rustfs/rustfs/issues/1365
- 关联分析: `docs/issue-1365-node-shutdown-latency-analysis.md`
- 关联任务拆分: `docs/issue-1365-node-shutdown-latency-task-breakdown.md`
- 文档类型: 根因落地方案 + 实施顺序 + 验证矩阵
- 生成时间: 2026-04-27

## 1. 结论先行

Issue #1365 不是单一根因，而是两个时间窗叠加后的结果:

1. **入口层时间窗**: Nginx 没有对故障 RustFS 节点做健康摘除，也没有启用有效的 `proxy_next_upstream`，导致请求可能直接落到已宕机节点。
2. **集群内部时间窗**: RustFS 当前主线虽然已经具备远端盘运行态、主动健康探测、连接驱逐与恢复探测，但首次碰撞故障节点时，仍存在走到 `30s` 级超时上限的慢路径。

因此，用户看到的“节点关机后 30 秒左右访问中断或高延迟”并不等于“每一层都固定 30 秒”，而是:

- 外部请求可能先被负载均衡打到坏节点
- 内部 fanout 又可能等待最慢远端 future
- 某些远端盘操作默认还允许跑到 `RUSTFS_DRIVE_MAX_TIMEOUT_DURATION=30s`

本方案的目标不是只解释问题，而是把它拆成可按批次提交、可灰度上线、可回滚的执行路径。

## 2. 当前代码现状

基于当前 `main` 分支代码，已经确认以下事实。

### 2.1 已经存在的正向能力

1. `crates/ecstore/src/rpc/remote_disk.rs`
   - 远端盘已有 `Online / Suspect / Offline / Returning` 运行态。
   - 已有主动健康探测与恢复探测。
   - 发生 timeout 时会 `mark_faulty_and_evict()`。
2. `crates/ecstore/src/set_disk/lock.rs`
   - 正常在线盘筛选已经走 `strict_online_candidates()`。
   - 一旦盘被标记为 `Offline`，后续很多正常 I/O 路径会避开该盘。
3. `crates/protos/src/lib.rs`
   - internode gRPC 连接建立超时已是 `3s`。
   - 连接保活和 HTTP/2 keepalive 已开启。

### 2.2 仍然保留的问题窗口

1. `crates/protos/src/lib.rs`
   - `RPC_TIMEOUT_SECS` 仍是硬编码 `30s`。
2. `crates/ecstore/src/rpc/remote_disk.rs`
   - 只有真正 `timeout()` 或“timeout-like error”才会立刻把远端盘标坏。
   - 一般网络错误，例如 `connection refused`、`transport closed`、某些 gRPC `Unavailable`，并不会统一走“立即摘除”。
3. `crates/ecstore/src/disk/disk_store.rs`
   - 主动健康检查周期 `CHECK_EVERY=15s`。
   - 探测超时 `CHECK_TIMEOUT_DURATION=5s`。
4. `crates/ecstore/src/set_disk/read.rs`、`crates/ecstore/src/set_disk/write.rs` 等
   - 仍有大量 `join_all` 等待所有 future 完成后再推进仲裁。
   - 在盘尚未被摘除的窗口内，慢节点会继续拖慢整批请求。
5. `crates/ecstore/src/rpc/peer_rest_client.rs`
   - 控制面 peer client 出错后会驱逐连接，但没有和数据面完全一致的 offline fast-fail 状态机。

## 3. 目标与非目标

## 3.1 目标

1. 节点掉线后的**首次故障感知时间**从“最坏 30 秒级”降到“秒级”。
2. 正常读写路径中，满足 quorum 后尽快返回，不再等待所有慢节点。
3. 控制面、管理面、数据面在“节点临时不可达”时表现一致，优先 fast-fail。
4. 所有改动都不削弱 EC quorum、bitrot、metadata 一致性和修复语义。

## 3.2 非目标

1. 不做“简单粗暴地把所有内部超时都改成 5 秒”。
   - 大对象流式读写和部分重命名/清理路径仍需要更长超时或分级超时。
2. 不做“全库一键把 `join_all` 改成 quorum-first`”。
   - 某些 fanout 是语义上需要等待全部结果或需要回滚信息的，不能机械替换。
3. 不把这次任务扩大成完整负载均衡产品化。
   - 入口层先给出运维止血方案，代码层聚焦 RustFS 自身。

## 4. MinIO 当前做法对照

对照 MinIO 当前主线代码，可归纳出四个关键模式。

### 4.1 网络错误立即离线

MinIO 的 `internal/rest/client.go` 在 REST 请求遇到网络错误时，会立即 `MarkOffline()`，随后:

1. 当前请求直接失败。
2. 后续请求直接从 offline 状态 fast-fail。
3. 后台单独跑健康检查做恢复。

这意味着 MinIO 不把“继续在业务请求路径上重试坏节点”当作默认行为。

### 4.2 独立健康检查通道

MinIO 的 `cmd/storage-rest-client.go` 会给 storage REST client 绑定独立 health check 回调，健康探测与正常业务调用解耦。

### 4.3 更短的控制面超时

MinIO 当前实现里:

1. REST 默认超时是 `10s`。
2. 健康检查 timeout 是 `1s`。
3. grid 连接默认拨号超时 `2s`。
4. 连接层会按 ping/pong 失联及时断链重连。

### 4.4 业务层不等待“明知已坏”的节点

MinIO 的 offline 标记一旦生效，后续大量调用会先基于 client state fast-fail，而不是继续把请求压到网络层等待完整超时。

## 5. 总体实施路线

推荐按四个阶段推进，而不是一次大改:

1. **Phase 0: 运维止血**
   - 先降低入口层把请求送到坏节点的概率。
2. **Phase 1: 第 3 步落地**
   - 将“网络错误立即标坏”贯通到 RustFS 数据面。
3. **Phase 2: 第 4 步落地**
   - 细化并配置化 internode/control-plane/metadata timeout，消除不必要的 30 秒兜底。
4. **Phase 3: 第 6 步落地**
   - 将适合 quorum-first 的 fanout 路径改成“够用即返回”，不再等待尾部慢节点。
5. **Phase 4: 管理面收口**
   - 将 peer/admin 的节点离线行为与数据面统一。

## 6. Phase 0: 入口层止血方案

这部分不改 RustFS 代码，但建议优先执行，否则用户仍可能把“入口层卡顿”误判为“RustFS 没修好”。

### 6.1 Nginx 建议配置

至少启用:

```nginx
proxy_next_upstream error timeout http_502 http_503 http_504;
proxy_next_upstream_tries 2;
proxy_next_upstream_timeout 3s;
```

若环境允许，建议补充:

1. upstream 被动失败摘除。
2. 更成熟的四层或七层健康检查负载均衡。
3. 对 console 和 S3 API 分开设置 upstream 策略。

### 6.2 验收标准

1. 单节点关机后，Nginx 入口不再持续把请求稳定打到已宕机节点。
2. 请求失败应收敛到秒级，而不是固定长时间挂起。

## 7. Phase 1: 第 3 步落地

## 7.1 目标

将 RustFS 现有的 `DiskHealthTracker` 从“timeout 才摘盘”升级为“明确网络不可达就摘盘”。

### 7.2 设计原则

1. **只对明确的网络/传输层失败立即摘盘**。
2. **不要把业务语义错误误判为离线**。
3. **第一次失败就应尽快触发后续 fast-fail**，而不是继续在同一路径上等待完整超时。

### 7.3 具体改造项

#### A. 在 `crates/ecstore/src/rpc/remote_disk.rs` 增加统一错误分类器

新增建议:

1. `fn is_network_like_error(err: &Error) -> bool`
2. 识别范围:
   - `std::io::ErrorKind::ConnectionRefused`
   - `ConnectionReset`
   - `BrokenPipe`
   - `UnexpectedEof`
   - `TimedOut`
   - tonic/transport 类 `unavailable`、`transport error`
   - 已关闭连接、底层通道不可用

实现要求:

1. 不能只靠字符串 contains 做全部判断，但允许对少量 tonic/transport 文本做兜底。
2. timeout-like 逻辑保留，但升级为 network-like 的子集。

#### B. 将 `execute_with_timeout_for_op()` 的故障摘除触发条件扩大

当前仅在:

1. `time::timeout()` 超时
2. `timeout-like error`

时调用 `mark_faulty_and_evict()`。

改造后应变成:

1. `timeout` -> 立即 `mark_faulty_and_evict()`
2. `network-like error` -> 立即 `mark_faulty_and_evict()`
3. 纯业务错误，例如 `FileNotFound`、`VolumeNotFound`、`Corrupt`，不触发摘盘

#### C. `get_client()` 失败时也要纳入状态机

现在 `get_client()` 失败只是返回 `Error::other(...)`。

改造建议:

1. 若 `node_service_time_out_client()` 失败原因属于网络类，直接触发当前远端盘 `mark_faulty_and_evict("connect_error")`。
2. 避免“连接建不起来，但运行态还是 online”的不一致。

#### D. 同步 `peer_s3_client.rs`

`crates/ecstore/src/rpc/peer_s3_client.rs` 与 `remote_disk.rs` 的健康模型应保持一致，否则会出现一类 internode 路径先收敛、另一类路径仍拖慢的行为差异。

#### E. 补测试

至少新增以下测试:

1. 连接拒绝时立即标坏。
2. transport/gRPC unavailable 时立即标坏。
3. `FileNotFound` 不标坏。
4. 标坏后后续请求 fast-fail，不再重复等待长超时。

### 7.4 Phase 1 验收标准

1. 第一次网络错误出现后，盘运行态在秒级内转为 `Suspect/Offline`。
2. 同一坏盘上的第二批请求应明显快于第一批请求。
3. `strict_online_candidates()` 能在后续请求中真正把坏盘排除。

## 8. Phase 2: 第 4 步落地

## 8.1 目标

拆掉“所有 internode 问题最终都走到 30 秒”的粗粒度超时模型。

### 8.2 问题拆分

当前 30 秒窗口主要来自两条链:

1. `crates/protos/src/lib.rs`
   - `RPC_TIMEOUT_SECS=30`
2. `crates/ecstore/src/disk/disk_store.rs`
   - 大量远端盘操作默认走 `get_max_timeout_duration()`
   - `DEFAULT_DRIVE_MAX_TIMEOUT_DURATION_SECS=30`

因此，Phase 2 不只是“把一个常量改小”，而是要把超时按操作类别分层。

### 8.3 推荐超时分层

#### A. internode transport 层

适用文件:

1. `crates/protos/src/lib.rs`
2. `crates/ecstore/src/rpc/client.rs`

建议新增配置项，优先放入 `crates/config/src/constants/` 新模块，例如 `internode.rs`:

1. `RUSTFS_INTERNODE_CONNECT_TIMEOUT_SECS`
2. `RUSTFS_INTERNODE_RPC_TIMEOUT_SECS`
3. `RUSTFS_INTERNODE_TCP_KEEPALIVE_SECS`
4. `RUSTFS_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS`
5. `RUSTFS_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS`

推荐默认值:

1. connect timeout: `2-3s`
2. rpc timeout: `5-10s`
3. keepalive interval: `3-5s`
4. keepalive timeout: `2-3s`

#### B. drive metadata/control-plane 层

适用文件:

1. `crates/ecstore/src/disk/disk_store.rs`
2. `crates/config/src/constants/drive.rs`
3. `crates/ecstore/src/rpc/remote_disk.rs`

现有 dedicated timeout:

1. `RUSTFS_DRIVE_METADATA_TIMEOUT_SECS`
2. `RUSTFS_DRIVE_DISK_INFO_TIMEOUT_SECS`
3. `RUSTFS_DRIVE_LIST_DIR_TIMEOUT_SECS`
4. `RUSTFS_DRIVE_WALKDIR_TIMEOUT_SECS`
5. `RUSTFS_DRIVE_WALKDIR_STALL_TIMEOUT_SECS`

建议改造:

1. 保持 dedicated timeout 为主。
2. **不要再让 metadata 类 timeout 依赖 `ENV_DRIVE_MAX_TIMEOUT_DURATION` 别名兜底**。
3. metadata/control-plane 默认维持在 `3-5s`。

原因:

如果继续允许 metadata timeout 回退到全局 `30s`，那只要运维设置了一个大一点的 legacy 值，问题就会再次放大。

#### C. 大对象/长耗时路径

这里不要一刀切。

建议:

1. `read_file_stream`、`read_all`、`rename_data`、部分清理/修复路径，可以保留更高上限。
2. 但应尽量变成独立 dedicated timeout，而不是继续统挂 `get_max_timeout_duration()`。

### 8.4 主动健康检查参数配置化

当前硬编码位置:

1. `CHECK_EVERY=15s`
2. `CHECK_TIMEOUT_DURATION=5s`

建议配置项:

1. `RUSTFS_DRIVE_ACTIVE_CHECK_INTERVAL_SECS`
2. `RUSTFS_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS`
3. 保留现有 `RUSTFS_DRIVE_SUSPECT_FAILURE_THRESHOLD`
4. 保留现有 `RUSTFS_DRIVE_RETURNING_PROBE_INTERVAL_SECS`

推荐默认值:

1. active check interval: `3-5s`
2. active check timeout: `1-2s`

### 8.5 Phase 2 验收标准

1. internode 控制面调用不再默认卡到 30 秒。
2. metadata 类远端盘操作在节点掉线时收敛到 3-5 秒级。
3. 大对象流路径不因过度缩短 timeout 出现大面积误杀。

## 9. Phase 3: 第 6 步落地

## 9.1 目标

把“适合 quorum-first 的 fanout 路径”改成:

1. 达到 quorum 即返回
2. 尾部慢节点不再阻塞主响应
3. 必要时取消剩余 future 或将其转为后台清理

### 9.2 核心原则

不是所有 `join_all` 都应该改。

必须先区分三类路径:

1. **quorum-capable**
   - 达到读/写法定票数即可继续
2. **all-results-required**
   - 需要完整回滚信息，必须等全量结果
3. **best-effort fanout**
   - 通知类、广播类，失败不该阻塞主路径，但也不一定需要提前返回

### 9.3 第一批建议改造路径

优先处理“对用户延迟最敏感、又最适合 quorum-first”的路径。

#### A. `crates/ecstore/src/set_disk/read.rs`

优先目标:

1. `read_version()` 一类元数据读取
2. 先收集足够票数再推进，而不是 `join_all`

建议实现:

1. `FuturesUnordered`
2. 边收集边统计:
   - 成功数
   - 可忽略错误数
   - 已经不可能达到 quorum 时提前失败

#### B. `crates/ecstore/src/set_disk/list.rs`

目录/列表类路径通常比大对象数据流更适合 quorum-first。

目标:

1. 元数据够用即返回
2. 坏盘尾部结果不影响主响应

#### C. `crates/ecstore/src/set_disk/multipart.rs`

优先处理只需要读取足够状态即可决策的部分，避免在列 multipart 时等待全部远端。

### 9.4 第二批谨慎改造路径

以下路径不要第一刀就改。

#### A. `crates/ecstore/src/set_disk/write.rs`

原因:

1. 写路径常常不仅需要 quorum 结果，还要知道哪些盘成功、哪些盘失败，以便 cleanup/undo。
2. 过早返回会影响错误归约和回滚语义。

建议:

1. 先保留全量结果收集语义。
2. 如果后续要优化，必须把“主响应返回”和“尾部 cleanup”拆成两个阶段。

#### B. `crates/ecstore/src/notification_sys.rs`

通知系统很多是 best-effort 广播，不适合简单套用“读写 quorum”语义。

建议:

1. 不阻塞主路径
2. 统计失败
3. 必要时后台补偿

### 9.5 推荐实现框架

优先统一一个小型 helper，而不是每处手写一遍。

建议位置:

1. `crates/ecstore/src/batch_processor.rs`
2. 或新增 `crates/ecstore/src/quorum_fanout.rs`

helper 能力建议:

1. 并发提交 futures
2. 按 quorum 收集
3. 支持“成功足够即提前返回”
4. 支持“必败即提前失败”
5. 支持保留部分失败信息用于 debug

### 9.6 Phase 3 验收标准

1. 单节点掉线时，读元数据/列目录等用户敏感操作不再稳定卡到慢节点尾部。
2. 读写 quorum 语义与现有数据耐久性约束保持一致。
3. cleanup/undo 路径没有因为提前返回而丢失必要信息。

## 10. Phase 4: 管理面与控制面统一

## 10.1 目标

将 `PeerRestClient` 的行为与 `RemoteDisk` 更一致，避免:

1. 数据面已经 fast-fail
2. admin/console 仍在另一条路径上长时间等待

### 10.2 建议改造

1. `crates/ecstore/src/rpc/peer_rest_client.rs`
   - 增加 peer offline 状态缓存或统一健康状态引用。
2. `local_storage_info()`、`server_info()` 等 peer 调用
   - 对网络类错误快速返回
   - 进入短期 offline 状态
   - 后台再做恢复探测

### 10.3 验收标准

1. 节点掉线后，console/admin 面板刷新不再单独卡 30 秒。
2. `/health/ready`、cluster info、console 视图对离线节点的反应更一致。

## 11. 文件级任务清单

建议按下面的顺序实施。

### Task 1

目标: 立即标坏网络错误

涉及文件:

1. `crates/ecstore/src/rpc/remote_disk.rs`
2. `crates/ecstore/src/rpc/peer_s3_client.rs`
3. `crates/ecstore/src/rpc/client.rs`

产出:

1. 统一网络错误分类
2. 网络错误立即 `mark_faulty_and_evict()`
3. 回归测试

### Task 2

目标: internode timeout 配置化

涉及文件:

1. `crates/protos/src/lib.rs`
2. `crates/config/src/constants/mod.rs`
3. `crates/config/src/constants/internode.rs`（新增）

产出:

1. internode 各类 timeout 的 ENV 常量
2. 默认值收敛
3. 文档化

### Task 3

目标: drive active health check 配置化

涉及文件:

1. `crates/ecstore/src/disk/disk_store.rs`
2. `crates/config/src/constants/drive.rs`

产出:

1. active check interval/timeout 环境变量
2. 默认值调整
3. 相关测试

### Task 4

目标: 元数据 timeout 去掉 30 秒 legacy 兜底

涉及文件:

1. `crates/ecstore/src/disk/disk_store.rs`
2. `crates/config/src/constants/drive.rs`

产出:

1. metadata/list/disk_info/walkdir 使用 dedicated timeout
2. 不再被 `RUSTFS_DRIVE_MAX_TIMEOUT_DURATION` 间接放大

### Task 5

目标: 第一批 quorum-first fanout

涉及文件:

1. `crates/ecstore/src/set_disk/read.rs`
2. `crates/ecstore/src/set_disk/list.rs`
3. `crates/ecstore/src/set_disk/multipart.rs`
4. `crates/ecstore/src/batch_processor.rs` 或新增 helper

产出:

1. quorum-first helper
2. 第一批接入路径
3. targeted tests

### Task 6

目标: 控制面 peer fast-fail

涉及文件:

1. `crates/ecstore/src/rpc/peer_rest_client.rs`
2. 相关 admin/console 聚合路径

产出:

1. peer offline 快速失败
2. 控制面恢复探测

## 12. 验证矩阵

必须做四类故障注入，不要只测 `kill -9`。

### 12.1 直连存活节点

目的:

1. 排除 nginx 入口影响
2. 纯测 RustFS 集群内部收敛

### 12.2 经过 nginx 入口

目的:

1. 验证入口层与内部层组合后的真实用户体验

### 12.3 故障类型

1. `kill -9`
   - 模拟进程异常退出
2. 物理/虚拟机关机
   - 模拟断电
3. `iptables DROP`
   - 模拟黑洞超时
4. `systemctl stop`
   - 模拟优雅停机

### 12.4 观测指标

至少记录:

1. 首次失败请求耗时
2. 第二批请求耗时
3. `rustfs_drive_runtime_state`
4. `rustfs_drive_op_timeout_total`
5. internode dial errors
6. admin/console 刷新耗时
7. Nginx upstream 错误数

### 12.5 通过标准

建议门槛:

1. 首次碰撞坏节点:
   - 目标收敛到 `<= 5-10s`
2. 第二批请求:
   - 应明显低于首次请求，理想为 `<= 1-3s`
3. 节点恢复重新纳入:
   - 不出现长时间脑裂式 oscillation

### 12.6 本地 Docker 验证路径

当所有子任务代码落地完成后，允许并建议使用仓库现有 Docker 模式进行本地最终验证。

优先使用两条路径:

1. 单节点基础可用性与接口回归
   - `docker-compose-simple.yml`
   - 用于验证服务启动、`/health`、`/health/ready`、基本 S3/console 连通性
2. S3 兼容与端到端动作回归
   - `DEPLOY_MODE=docker ./scripts/s3-tests/run.sh`
   - 使用仓库现有 Docker 模式启动容器并执行兼容测试

建议顺序:

1. 先确认 Docker 启动后的健康状态:
   - `curl http://127.0.0.1:9000/health`
   - `curl http://127.0.0.1:9000/health/ready`
2. 再执行 Docker 模式下的兼容测试:
   - `DEPLOY_MODE=docker ./scripts/s3-tests/run.sh`
3. 若后续补齐多节点 Docker/Compose 验证编排，再追加节点下线注入验证:
   - 停单容器
   - 观察首次请求、第二批请求和控制面恢复行为

说明:

1. 当前仓库已有成熟的 Docker 模式 S3 测试脚本，适合作为“所有子任务完成后的本地最终验证入口”。
2. 若要严格复现 Issue #1365 的“节点下线”场景，最终仍建议补一套多节点 Docker Compose 验证编排。

## 13. 推荐提交拆分

为了保持可审阅粒度，建议拆成 4 个 commit:

1. `feat(ecstore): mark remote disks faulty on network errors`
2. `feat(config): make internode and active check timeouts configurable`
3. `perf(ecstore): use quorum-first fanout for metadata reads`
4. `feat(admin): fast-fail offline peers in control-plane paths`

## 14. 灰度与回滚

## 14.1 灰度顺序

1. 先上线入口层止血配置。
2. 再上线 Phase 1 和 Phase 2。
3. 确认没有误判 healthy 节点后，再上线 Phase 3 quorum-first。
4. 最后收控制面。

## 14.2 回滚策略

1. timeout 配置化改动可通过 ENV 回退到旧值。
2. quorum-first helper 初期可保留 feature flag 或临时环境开关。
3. 如果出现误判离线，应优先回滚“网络错误分类规则”，不要回滚整个 drive health state。

## 15. 最终建议

对 Issue #1365，最值得优先做的不是“大面积重构”，而是下面三件事:

1. **先把网络错误立即摘盘落地**
   - 这是收益最大、改动最聚焦的一步。
2. **把 internode 和 metadata timeout 从 30 秒模型里拆出来**
   - 这是把“最坏 30 秒”变成“秒级收敛”的关键。
3. **只在适合的路径做 quorum-first**
   - 先做读元数据、列目录，不要第一刀去碰复杂写回滚路径。

如果按这条路线推进，RustFS 可以在不削弱数据正确性的前提下，把“节点刚掉线时的可见抖动”从当前的最坏 30 秒级，收敛到更接近 MinIO 当前行为的秒级窗口。
