已基于当前 RustFS 代码对该 issue 做了 5 个方向的专家复核，结论如下：

1. 方向成立，但不能直接照原方案“一口气实现”。当前 RustFS 已经是 gRPC control-plane + HTTP data-plane；远端大数据流主要走 `/rustfs/rpc/read_file_stream`、`put_file_stream`、`walk_dir`，不是 proto 里的 `ReadAt stream`。
2. gRPC channel cache、keepalive、connect timeout、RPC timeout 已经存在，因此不建议新增一套 `GrpcChannelPool`。更值得做的是补齐 client-side HTTP/2/window/pool tuning、connection eviction 覆盖面和观测指标。
3. 当前最高优先级不是调参，而是先修可靠性坑：
   - `HttpWriter` shutdown/flush 不能吞掉后台写流错误。
   - remote lock 的 release/refresh/force-release 等 RPC 应统一纳入 timeout + eviction。
   - `ReadMultiple` 应避免 JSON/msgpack 双转换，并且 decode 失败不能 `filter_map(...ok())` 静默丢结果。
4. metadata cache/prefetch 不能直接打开到 distributed erasure。当前分布式场景 bypass cache 是合理的，因为需要保护 quorum、versioning、delete marker、data movement、no_lock 等语义。
5. shard locality 已有基础能力，但仍缺生产级 `SameNode` 来源、reader setup 前移和更安全的 hedging。建议先 observe-only，再 opt-in。

建议拆分为以下可执行子任务：

- Task 01: 修复 internode HTTP writer 错误传播。
- Task 02: 统一 remote lock RPC timeout/eviction。
- Task 03: 优化 `ReadMultiple` 序列化和 decode 错误处理。
- Task 04: 增加保守可回滚的 internode HTTP client tuning。
- Task 05: 补齐 internode observability 和 baseline。
- Task 06: 增加 shard locality topology observe-only。
- Task 07: 灰度启用 opt-in shard locality scheduling。
- Task 08: 在 capability gate 后新增 batch metadata RPC。
- Task 09: adaptive batch processor 先做 observe-only 设计。

回滚原则：所有行为变更默认关闭，通过 env profile/feature gate 启用；若 p99 比基线恶化超过 10%、read quorum/error rate 上升、503/timeout 增加或 mixed-version fallback 异常，应立即回退到 legacy/off。

本地已把每个子任务拆成单独、可执行、包含实施步骤/风险/测试/验收标准的文档，保存在 `docs/issue-797/` 下，后续可以按这些文档逐个 PR 推进。
