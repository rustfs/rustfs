# Issue #4003：ListObjectsV2 大规模列表性能实战优化方案

日期：2026-06-29

关联：
- rustfs 问题： https://github.com/rustfs/rustfs/issues/4003
- Backlog 主 Issue： https://github.com/rustfs/backlog/issues/788
- 子任务：#785 #786 #787

## 1) 问题定义与现象
在超大桶（约 74.5 万对象）上，`ListObjectsV2` 在递归列表场景（`rclone size`、`rclone lsf --recursive`）表现非常慢。

当前路径的核心问题：
- 列表场景默认仍偏向保守的一致性配置；
- 每页列表可能触发重复扫描与归并放大，导致分页成本远超期望；
- 续传标记偏 marker 化，没形成可优化的“低开销续跑”语义。

## 2) 五位专家视角下的根因拆解
### 2.1 存储引擎视角
- 列表时 ask-disks quorum 决策成本高，且“严格模式”对大目录带来更高开销。
- merge/walk 逻辑正确但未对分页放大做特别优化。

### 2.2 API 兼容性视角
- List API 的行为正确性（无重复、无漏）。
- 后续 cursor 化不能破坏旧 token 兼容。

### 2.3 性能视角
- 页面级放大是主要瓶颈，尤其在多磁盘元数据合并阶段。
- 优化点应先聚焦“减少每页扫描成本”和“低风险可回退配置”。

### 2.4 运营风险视角
- 优先以可运行时切换的参数落地，而非一次性重构。
- 回退路径必须清晰且成本低。

### 2.5 SRE 观测视角
- 需要把每页扫描量、归并次数、token 回退频率纳入指标，否则优化效果难量化。

## 3) 本次已完成变更（Phase-1 第一刀）
### 3.1 引入列表专用 quorum 配置
文件：`crates/ecstore/src/store/list_objects.rs`
- 新增环境变量：`RUSTFS_LIST_OBJECTS_QUORUM`
- 默认值：`optimal`
- 新增方法：`list_objects_quorum_from_env()`
- 将列表相关 6 个入口改为该参数：
  - `ECStore::list_objects`
  - `ECStore::list_object_versions`
  - `Sets::list_objects`
  - `Sets::list_object_versions`
  - `SetDisks::list_objects`
  - `SetDisks::list_object_versions`

### 3.2 单测补齐
`crates/ecstore/src/store/list_objects.rs` 测试模块新增：
- `list_objects_quorum_from_env_defaults_to_optimal`
- `list_objects_quorum_from_env_honors_supported_value`

## 4) 变更分阶段执行计划
### 阶段 1（当前）：列表性能基线优化，不改协议
目标：只调优查询侧行为，保持外部语义不变。
- 列表专用 quorum 与默认值上线；
- 增强列表指标与日志（扫描/归并/提前停止）；
- 建立对比基线，确认 `rclone` 场景改善。

验收：
- 响应正确性不变；
- 大列表场景延时和 CPU 明显下降；
- `RUSTFS_LIST_OBJECTS_QUORUM=strict` 可秒级回退。

### 阶段 2：分页续传语义工程
目标：实现可版本化 cursor。
- 引入版本化 continuation token；
- 保持 legacy token 向后兼容；
- 增补重复/跳页防护测试。

验收：
- 同输入同 token 重放结果稳定；
- 不丢/不重；
- 兼容旧客户端。

### 阶段 3：指数级场景结构优化
目标：引入索引辅助列表主路径。
- 评估并实现索引路径；
- walker 保持 fallback；
- 指定回放/重建策略。

验收：
- 超大桶 list 延时和放大系数持续下降；
- 一致性问题有明确应急与恢复动作。

## 5) 验证清单
- `cargo test -p rustfs-ecstore list_objects -- --nocapture`
- `cargo test -p rustfs-ecstore list_objects_quorum_from_env`
- `scripts/validate_issue_787_list_quorum.sh`（离线）以及 `scripts/validate_issue_787_list_quorum.sh --full`（如环境可连则做 live）
- 压测对比：
  - `rclone lsf --recursive`
  - `rclone size`
- 对照指标：
  - p99 延时
  - 每页扫描对象数/归并时间
  - 是否出现重复、跳过对象
  - 资源使用（CPU、磁盘读）

## 6) #787 验收与上线方案（进行中）

### 6.1 live benchmark 验证方案

- 压测基线：
  - 目标桶：预先准备的生产同规模桶（建议 ≥ 70万 对象，含深层前缀）
  - 工具：`rclone lsf --recursive` + `rclone size`
  - 端点：本地可用的 RustFS 节点，配置 `RUSTFS_ACCESS_KEY` / `RUSTFS_SECRET_KEY`
  - 分页：`--max-keys` 取 1000 / 5000 两档，比较单页 p95/p99

- 对比配置（至少两组）：
  - 基线：`RUSTFS_LIST_OBJECTS_QUORUM=strict`
  - 候选：`RUSTFS_LIST_OBJECTS_QUORUM=optimal`（或 `reduced`，如 strict 对某些环境不可接受）

- 每次压测必交付：
  - p95/p99 分页延时
  - 全量递归列表完成耗时
  - 跨页重放校验下的重复/漏对象数
  - 每页 key 数及 continuation token 递进情况

- `scripts/validate_issue_787_list_quorum.sh --full`
  - 用 `MODE_LIST` 表示要对比的模式，例如：
    - `MODE_LIST=strict,optimal,reduced`
  - 建议设置 `SERVER_RESTART_CMD` 与 `SERVER_WAIT_SECONDS=20`，在同一日志目录下产出生效模式分片。

- 建议验收阈值（草案）：
  - 候选方案 p99 分页延时较 baseline 下降 ≥ 10%，回退不超过 +10%
  - 连续分页零重复/零漏
  - API 列表语义不变

### 6.2 已完成的本地 benchmark 证据包（2026-06-30）

本次用于补齐证据缺口的本地 benchmark 环境：
- 单节点本地 RustFS，同一数据目录在三模式间复用
- 桶：`issue787-bench`
- 对象布局：6,000 个零字节对象，混合递归前缀：
  - `alpha/obj-*`
  - `beta/deep/obj-*`
  - `gamma/nested/path/obj-*`
- 请求形态：整桶递归 `ListObjectsV2`
- 分页：`max-keys=1000`
- 样本规模：每模式 15 次 full pass、每次 6 页、每模式共 90 个分页样本

结果汇总：

| 模式 | Page p95 (ms) | Page p99 (ms) | Wall avg (ms) | 相对 strict 的 p99 变化 | 相对 strict 的 wall avg 变化 | 连续性 |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| `strict` | 255.413 | 275.438 | 1469.938 | baseline | baseline | pass |
| `optimal` | 259.146 | 267.342 | 1485.912 | -2.939% | +1.087% | pass |
| `reduced` | 255.937 | 272.557 | 1477.955 | -1.046% | +0.545% | pass |

基于 benchmark 的结论：
- 两个候选模式在本地样本中都没有突破 strict 基线的 p99 安全护栏；
- 但两者都没有达到 `>= 10%` 的分页延时改善或 wall-clock 改善要求；
- 因此，当前证据**不足以支持**把 quorum-only 调优作为 `#4003` 的生产级独立缓解方案；
- 建议：把 quorum 调优保留为可运营控制的实验/诊断开关，把真正的 Phase-3 缓解重心转到 index-backed listing。

### 6.3 现场执行记录（本次）

- 当前在本地执行记录：
  - 直接执行 `scripts/restart_local_single_node_multidisk_rustfs.sh` 与 `target/debug/rustfs server` 启动链路
  - 9000 与 19000 端口均出现启动失败：`Operation not permitted (os error 1)`（HTTP bind 失败）
  - 归档日志目录：
    - `target/issue-787-live-benchmark-20260630-031521/`

- 当前状态：
  - #787 live benchmark 当前 **受执行环境限制阻塞**，未能形成完整线上对比结果
  - #785 的离线验收已闭环，可作为 #787 后续评估前置条件

### 6.4 上线回滚文档要求

- 回滚参数：
  - 当前默认值：`RUSTFS_LIST_OBJECTS_QUORUM=optimal`
  - 安全回退：`RUSTFS_LIST_OBJECTS_QUORUM=strict`
- 上线阈值：
  - p99 延时失效阈值 +20%
  - 错误率失效阈值 +10%
  - 连续性测试重复/漏数异常立即回滚
- 上线顺序：
  - 第 1 阶段：影子流量验证（与现网同构 prefix）
  - 第 2 阶段：小范围桶/前缀灰度
  - 第 3 阶段：连续两次验收窗口均通过后再扩大范围

### 6.5 index-backed 工程化路线（下一阶段）

- 目标：引入可选 index 辅助列表路径，并保留 walk 回退。
- 最小状态机（初版）：
  1. 读取 marker/continuation 的上下文后进行策略选择：`walk` 或 `index`
  2. 走 index 时先做版本与布局校验，不通过则立即回退到 walk 并记录 `marker_fallback`
  3. 当 fallback 触发时记录 metric 和日志，避免“悄悄切换”
  4. 仅 index 成功路径返回的 token 中携带 index 源标记，避免兼容歧义
- 下一里程碑验收指标：
  - 跨页 smoke 保持零重复/零漏
  - index 回退率在稳定阶段可控（目标 < 1%）
  - 回退开关与 `RUSTFS_LIST_OBJECTS_QUORUM` 的现有保护链路保持独立互不影响

### 6.6 显式 index-backed 设计产物

Continuation token 兼容矩阵：

| Token 家族 | 当前/规划的产生方 | 解析动作 | 首选读路径 | 回退行为 |
| --- | --- | --- | --- | --- |
| legacy marker-only | 旧客户端 / pre-v2 服务端 | 按 legacy continuation 解析 | walker | 仅在 XML / query 自身损坏时失败 |
| v2 cache-tag token | 当前 Phase-2 实现 | 解析 marker + cache tag，保持 replay 语义 | walker + metacache resume | cache tag 损坏或 replay 不一致时强制 refresh |
| 规划中的 v3 index-cursor token | 未来 index-assisted planner | 解析 marker + index cursor 元数据 + manifest epoch | index | token 解析 / epoch / checksum 失败时立即降级 walker |

规划中的索引 schema（仅设计，不代表当前 PR 已实现）：
- `bucket`
- `prefix_scope`
- `object_key`
- `version_id`（版本桶可选）
- `delete_marker`
- `etag`
- `size`
- `last_modified`
- `cursor_seq`
- `manifest_epoch`
- `record_checksum`

规划中的写路径 hook：
- `PutObject` / `CopyObject` / `CompleteMultipartUpload`：写入 upsert 记录
- `DeleteObject` / `DeleteObjectVersion` / 生命周期过期：写入 tombstone
- replication / heal / rebuild：向同一 mutation stream 发送可重放 repair event

一致性、回填、重建方案：
- 初始构建触发：bucket/range 被纳入 index-assisted listing
- 回填模型：按 prefix 顺序 walker snapshot，并为每个 bucket/range 持久化 watermark
- 漂移检测：周期性对同一 continuation 边界下的 index page 与 walker page 做抽样比对
- 重建触发：
  - manifest epoch 不一致
  - record checksum 不一致
  - fallback rate 超阈值
  - operator 在 repair/heal 之后手动触发
- 规划中的运维工具接口（设计名）：
  - `list-index build`
  - `list-index verify`
  - `list-index rebuild`
  - `list-index repair`

Fallback 状态机要求：
- token 解析失败 -> walker
- manifest epoch 不一致 -> walker + `marker_fallback` metric
- 抽样漂移不一致 -> walker + alert
- fallback/error budget burn 过快 -> 对受影响 bucket/range 关闭 index path

### 6.7 rollout / rollback 闭环产物

上线阶段矩阵：

| 阶段 | 范围 | 成功条件 | 中止条件 |
| --- | --- | --- | --- |
| 0 | 本地/离线验证 | continuity pass，unit/static pass | 本地 smoke 出现重复/漏数 |
| A | 影子 benchmark（生产形态桶） | 连续两个窗口 continuity pass 且延时稳定 | 任意 continuity failure |
| B | canary bucket / prefix range | error budget 在阈值内，fallback rate 受控 | p99 +20%、error +10%、fallback 激增 |
| C | 扩大到更多 prefix family | 连续两个窗口保持 canary 水平 | 同类回归重复出现 |
| D | 大范围启用 | 观测稳定 | operator 触发回滚 |

显式回滚条件：
- 跨页重复对象
- truncation / continuation 切换后漏对象
- p99 延时回归 `> +20%`
- 错误率回归 `> +10%`
- fallback rate 高于计划阈值
- index 抽样漂移不一致

回滚动作：
- quorum-only 实验：设置 `RUSTFS_LIST_OBJECTS_QUORUM=strict`
- 未来 index-assisted planner：关闭 index path，强制走 walker
- token 家族回归：停止发放规划中的 v3 index token，仅保留 legacy / v2 解析兼容

对 `#787` 的闭环解释：
- benchmark 证据现在已补齐；
- 结论是 quorum-only rollout 证据不足；
- design / rollout 产物已经就位，后续真实缓解方向应转向 index-backed listing。

## 7) 风险与回滚
- 风险：默认 `optimal` 在某些奇异部署下可能导致一致性感知差异。
- 低风险处置：设置 `RUSTFS_LIST_OBJECTS_QUORUM=strict` 回退。
- 任何问题：回退本次提交即可恢复原行为。
