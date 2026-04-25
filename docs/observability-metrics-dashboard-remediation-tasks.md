# RustFS Observability Metrics and Dashboard Remediation Tasks

## 任务拆分原则

- 每个任务都应能单独实现、单独验证、单独 review
- 优先先做“契约统一”和“命名统一”，再做“补点”和“面板”
- 每个任务都要明确：
  - 目标
  - 主要文件
  - 关键输出
  - 验收标准
  - 依赖关系

## 执行顺序总览

1. [x] 建立指标 inventory 与 canonical naming contract
2. [x] 扫描并迁移 dot notation 指标到 underscore notation
3. [x] 为 `crates/obs` 补齐 runtime scheduler 接线骨架
4. [x] 统一 HTTP / request 指标语义
5. [x] 统一 scanner / background job 指标语义
6. [ ] 补齐 internode / system network 指标
7. [ ] 补齐 cluster usage / erasure set / config 指标
8. [ ] 补齐 IAM / audit / notification 指标治理
9. [ ] 补齐 replication 尤其 tagging 相关真实埋点
10. [ ] 重构 `rustfs.json` 主仪表盘信息架构
11. [ ] 对高成本查询增加 recording rules / 查询治理
12. [ ] 完成端到端验证与文档收尾

## Task 1: 建立指标 inventory 与 canonical naming contract

### 目标

- 全量盘点 repo 内 metrics 源头
- 建立“RustFS canonical metrics 命名规范”
- 明确每个指标族的 owner 与所属 subsystem

### 主要文件

- `crates/obs/src/metrics/schema/entry/metric_name.rs`
- `crates/obs/src/metrics/schema/*`
- `crates/io-metrics/src/*`
- `crates/common/src/*`
- `rustfs/src/server/http.rs`
- `rustfs/src/storage/*`

### 关键输出

- 指标 inventory 表
- canonical naming 约定
- dot / underscore 清单
- 主题归属表

已落地文档：

- `docs/observability-metrics-inventory.md`
- `docs/observability-metrics-naming-contract.md`

### 验收标准

- 任一主要主题指标都可追溯到唯一 canonical family
- 新增文档中明确列出禁止继续新增 dot notation

### 依赖关系

- 无

## Task 2: 扫描并迁移 dot notation 指标到 underscore notation

### 目标

- 将当前 repo 内 dot notation 指标全部迁移到 underscore canonical naming
- 清理 dashboard 和 README 中的 dot 风格引用

### 主要文件

- `crates/io-metrics/src/lib.rs`
- `crates/io-metrics/src/*.rs`
- `crates/common/src/internode_metrics.rs`
- `rustfs/src/storage/*.rs`
- `crates/obs/README.md`
- `.docker/observability/grafana/dashboards/rustfs.json`

### 关键输出

- `rustfs.s3.get_object.total` -> `rustfs_s3_get_object_total`
- `rustfs.zero_copy.memory.saved.bytes` -> `rustfs_zero_copy_memory_saved_bytes_total` 或等价 canonical family
- `rustfs.io.buffer.size.bytes` -> `rustfs_io_buffer_size_bytes`
- `rustfs.internode.*` -> `rustfs_system_network_internode_*`

### 验收标准

- 代码中不再保留主路径 dot notation 指标
- dashboard 查询不再依赖 dot 名称
- 若有兼容窗口，明确标注过渡规则与下线时间

本轮完成范围：

- 代码侧 `counter!` / `gauge!` / `histogram!` / `describe_*` 中的 dot notation 指标已迁移为 underscore notation
- `crates/obs` 相关 README 中的旧点号指标示例已同步改为下划线 canonical names
- 保留未改项仅限非指标字符串，例如日志文件名 `rustfs.log`、URL、文档路径

### 依赖关系

- 依赖 Task 1

## Task 3: 为 `crates/obs` 补齐 runtime scheduler 接线骨架

### 目标

- 把已有但未接入 runtime 的 collector 逐步接到 `scheduler.rs`
- 先完成调度骨架，再逐个接 stats source

### 主要文件

- `crates/obs/src/metrics/scheduler.rs`
- `crates/obs/src/metrics/stats_collector.rs`
- `crates/obs/src/metrics/collectors/mod.rs`

### 关键输出

- `cluster_config`
- `cluster_erasure_set`
- `cluster_iam`
- `cluster_usage`
- `ilm`
- `system_network`
- `request`
- `scanner`

对应的调度入口与周期配置

### 验收标准

- `scheduler.rs` 中可以明确看到上述 collector 的 runtime 接线
- 对于暂未接到真实 source 的 collector，要么返回显式空集并标记 TODO，要么在本任务中直接补真实 source

本轮完成范围：

- `scheduler.rs` 已补齐 `cluster_config`、`cluster_erasure_set`、`cluster_iam`、`cluster_usage`、`ilm`、`system_network`、`request`、`scanner` 的 runtime 调度入口
- `stats_collector.rs` 已新增对应 no-op / placeholder source，后续主题任务可以在不改调度骨架的情况下逐项填入真实数据源
- 已通过 `cargo check -p rustfs-obs`

### 依赖关系

- 依赖 Task 1

## Task 4: 统一 HTTP / request 指标语义

### 目标

- 统一 `rustfs/src/server/http.rs` 与 `crates/obs` request collector 的语义
- 定义唯一的 HTTP server 核心指标族

### 主要文件

- `rustfs/src/server/http.rs`
- `crates/obs/src/metrics/collectors/request.rs`
- `crates/obs/src/metrics/schema/request.rs`
- `crates/obs/src/metrics/stats_collector.rs`

### 关键输出

- 请求总量、失败总量、活动请求、请求时延、请求体大小、响应体大小统一
- 标签统一为低基数：
  - `method`
  - `status_class`
  - `api_type`
  - `operation`

### 验收标准

- dashboard 中 request 面板不再同时依赖两套不一致语义
- `Request Rate by Method`、延迟、body size 面板都能直接消费 canonical family

本轮完成范围：

- `rustfs/src/server/http.rs` 已统一到 `rustfs_http_server_*` 核心指标族
- 已补齐：
  - `requests_total`
  - `failures_total`
  - `active_requests`
  - `request_duration_seconds`
  - `request_body_bytes_total`
  - `request_body_size_bytes`
  - `response_body_bytes_total`
  - `response_body_size_bytes`
- 标签已统一为低基数入口维度：
  - `method`
  - `status_class`
- 已通过 `cargo check -p rustfs --lib`

### 依赖关系

- 依赖 Task 1
- 建议在 Task 2 之后执行

## Task 5: 统一 scanner / background job 指标语义

### 目标

- 对齐 `crates/common/src/metrics.rs` 与 `crates/obs` scanner collector
- 补齐 scanner / ILM / background jobs 的完整观测面

### 主要文件

- `crates/common/src/metrics.rs`
- `crates/scanner/src/*`
- `crates/obs/src/metrics/collectors/scanner.rs`
- `crates/obs/src/metrics/collectors/ilm.rs`
- `crates/obs/src/metrics/stats_collector.rs`

### 关键输出

- scanner：
  - cycles
  - cycle duration
  - bucket scans
  - directories scanned
  - objects scanned
  - versions scanned
  - last activity
- ILM：
  - pending
  - active
  - missed
  - scanned
  - result breakdown

### 验收标准

- dashboard 中 scanner 行既能看吞吐，也能看周期健康
- `ILM / Scanner` 不再是混合半空面板

本轮完成范围：

- `stats_collector.rs` 已将 scanner source 接到 `global_metrics().report()`
- `stats_collector.rs` 已将 ILM source 接到：
  - `GLOBAL_ExpiryState`
  - `GLOBAL_TransitionState`
  - `global_metrics().report()` 的 ILM 聚合计数
- `scheduler.rs` 中的 background workflow 调度现已产出真实 scanner / ILM 指标，而不再是 placeholder
- 已通过 `cargo check -p rustfs-obs`

### 依赖关系

- 依赖 Task 3

## Task 6: 补齐 internode / system network 指标

### 目标

- 将现有 internode 指标并入 `rustfs_system_network_internode_*`
- 打通 `system_network` collector 的真实 source

### 主要文件

- `crates/common/src/internode_metrics.rs`
- `crates/obs/src/metrics/schema/system_network.rs`
- `crates/obs/src/metrics/collectors/system_network.rs`
- `crates/obs/src/metrics/stats_collector.rs`

### 关键输出

- sent / recv bytes
- dial errors
- request errors
- dial avg time

### 验收标准

- `System Network Internode (All)` 面板有真实数据
- 名称和标签全部变为 underscore canonical family

### 依赖关系

- 依赖 Task 2
- 依赖 Task 3

## Task 7: 补齐 cluster usage / erasure set / config 指标

### 目标

- 为集群运营态与存储健康态提供真实 producer

### 主要文件

- `crates/obs/src/metrics/collectors/cluster_usage.rs`
- `crates/obs/src/metrics/collectors/cluster_erasure_set.rs`
- `crates/obs/src/metrics/collectors/cluster_config.rs`
- `crates/obs/src/metrics/stats_collector.rs`
- `rustfs_ecstore` / `storage_info()` / data usage 相关逻辑

### 关键输出

- `cluster_usage_objects`
- `cluster_usage_buckets`
- `cluster_erasure_set`
- `cluster_config`

### 验收标准

- 这些 prefix explorer 不再天然空
- capacity / erasure set / bucket usage 主面板均可直接消费

### 依赖关系

- 依赖 Task 3

## Task 8: 补齐 IAM / audit / notification 指标治理

### 目标

- 统一 IAM / audit / notification 的面板消费语义
- 明确哪些指标是 queue、哪些是 throughput、哪些是 failures

### 主要文件

- `crates/obs/src/metrics/collectors/cluster_iam.rs`
- `crates/obs/src/metrics/collectors/audit.rs`
- `crates/obs/src/metrics/collectors/notification.rs`
- `crates/obs/src/metrics/collectors/notification_target.rs`
- 对应 source module

### 关键输出

- IAM sync / plugin authn
- audit queue / failures / totals
- notification send progress / target queue / failures

### 验收标准

- 安全与投递主题有独立 row
- query 和 panel 名称语义一致

### 依赖关系

- 依赖 Task 3

## Task 9: 补齐 replication 尤其 tagging 相关真实埋点

### 目标

- 找到 replication tagging 操作的真实逻辑点
- 去掉当前 stats collector 中的零值占位实现

### 主要文件

- `crates/obs/src/metrics/stats_collector.rs`
- `crates/obs/src/metrics/collectors/bucket_replication.rs`
- `crates/obs/src/metrics/schema/bucket_replication.rs`
- `rustfs_ecstore::bucket::replication::*`
- `rustfs/src/storage/*` replication request 相关流程

### 关键输出

- `proxied_put_tagging_requests_total`
- `proxied_get_tagging_requests_total`
- `proxied_delete_tagging_requests_total`
- 对应 failures

### 验收标准

- `Bucket Replication Proxy Tagging Requests` 面板不再长期为 0
- `Bucket Replication Target Latency`、失败数、失败字节、吞吐在 bucket/target 维度语义清晰

### 依赖关系

- 依赖 Task 3
- 建议在 Task 2 后执行

## Task 10: 重构 `rustfs.json` 主仪表盘信息架构

### 目标

- 将当前混合仪表盘重构为高标准主视图
- 只保留高频、高价值、低歧义面板在默认展开区

### 主要文件

- `.docker/observability/grafana/dashboards/rustfs.json`

### 关键输出

- `Overview`
- `API RED`
- `Storage and Capacity`
- `Replication`
- `Background Services`
- `Host and Process USE`
- `Security and Delivery`
- `Debug / Raw Explorer`

### 验收标准

- 主视图不再出现天然空 row
- query 成本下降
- panel story 更完整
- 所有关键 panel 有 description / unit / threshold

### 依赖关系

- 依赖 Task 4 到 Task 9 的至少一部分完成

## Task 11: 对高成本查询增加 recording rules / 查询治理

### 目标

- 将高成本 quantile、ratio、topN、wide regex 查询从主 dashboard 中下沉或预计算

### 主要文件

- `.docker/observability/prometheus.yml`
- 如需要新增：
  - `.docker/observability/prometheus-rules/*.yml`
- `.docker/observability/grafana/dashboards/rustfs.json`

### 关键输出

- request / replication / log cleaner / scanner 的 recording rules
- dashboard 改用 recording rules 或更轻量的 query

### 验收标准

- 主 dashboard 默认加载更快
- Prometheus 查询负载下降
- quantile 面板仍保留足够精度与可读性

### 依赖关系

- 依赖 Task 10

## Task 12: 端到端验证与文档收尾

### 目标

- 对所有改动做最终验证
- 确保文档、dashboard、代码、查询保持一致

### 主要文件

- `docs/observability-metrics-dashboard-remediation-plan.md`
- `docs/observability-metrics-dashboard-remediation-tasks.md`
- 相关代码与 dashboard 文件

### 建议验证命令

```bash
cargo fmt --all
cargo fmt --all --check
make pre-commit
```

必要时补充：

- 启动本地 observability stack
- 触发关键 API / scanner / replication / audit / notification 路径
- 在 Prometheus 中确认 metrics 存在
- 在 Grafana 中确认 panel 正常显示

### 验收标准

- 命名规范一致
- dashboard 可导入、可显示、可下钻
- 关键主题面板均有数据
- 文档与实际实现一致

### 依赖关系

- 依赖全部前置任务

## 可并行建议

可并行的任务组合：

- Task 2 与 Task 3 可并行
- Task 4 / Task 5 / Task 6 / Task 7 / Task 8 / Task 9 可按主题并行
- Task 10 可在 Task 4-9 中后期开始，但最终收尾应等待指标族稳定

## 建议的提交粒度

建议按以下粒度提交，便于审阅：

1. inventory + naming contract
2. dot -> underscore migration
3. scheduler wiring
4. request metrics unification
5. scanner / ilm metrics
6. internode metrics
7. cluster usage / erasure set / config
8. IAM / audit / notification
9. replication tagging metrics
10. dashboard restructure
11. recording rules and query optimization
12. final docs and verification
