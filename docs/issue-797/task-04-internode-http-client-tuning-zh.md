# Task 04: conservative internode HTTP client tuning

## 目标

为 HTTP data-plane client 增加保守、可配置、可回滚的连接池与 HTTP/2 tuning，不改变默认行为，避免直接强制 HTTP/2 prior knowledge 或扩大内存窗口造成兼容性和内存风险。

## 代码落点

- `crates/rio/src/http_reader.rs`
  - `build_http_client`
  - `get_http_client`
  - proxy decision
  - TLS/mTLS identity handling
- `crates/config/src/constants/internode.rs`
  - 新增或复用 internode HTTP tuning 常量
- `rustfs/src/server/http.rs`
  - server-side HTTP/2 参数，用于 client/server 对齐分析

## 当前能力

client 已有：

- connect timeout。
- TCP keepalive。
- HTTP/2 keepalive interval。
- HTTP/2 keepalive timeout。
- keepalive while idle。
- TLS generation cache。
- local loopback no-proxy。

server 侧已经配置 HTTP/2 window/adaptive window/max streams，但 client 侧尚未对齐。

## 主要风险

- 强制 `http2_prior_knowledge()` 可能破坏 HTTP/1.1、proxy、明文 h2c、TLS/mTLS 兼容性。
- 盲目调大 stream/connection window 会增加每连接内存占用。
- 只对 loopback 禁 proxy，非 loopback 集群内网可能走 system proxy。
- mTLS identity 解析失败当前偏 fail-open，需要明确部署预期。

## 实施步骤

1. 新增 profile 型配置：
   - `legacy`：保持现状。
   - `balanced`：增加 pool idle、轻量 window、no_proxy 默认。
   - `throughput`：更大 idle pool/window，仅用于压测或明确启用。
2. 配置项建议：
   - `RUSTFS_INTERNODE_HTTP_TUNING_PROFILE`
   - `RUSTFS_INTERNODE_HTTP_POOL_MAX_IDLE_PER_HOST`
   - `RUSTFS_INTERNODE_HTTP_POOL_IDLE_TIMEOUT_SECS`
   - `RUSTFS_INTERNODE_HTTP2_INITIAL_STREAM_WINDOW_SIZE`
   - `RUSTFS_INTERNODE_HTTP2_INITIAL_CONNECTION_WINDOW_SIZE`
   - `RUSTFS_INTERNODE_HTTP2_ADAPTIVE_WINDOW`
   - `RUSTFS_INTERNODE_HTTP_PROXY`
3. 默认值保持 legacy。
4. proxy 策略：
   - internode RPC 默认 `no_proxy()`。
   - 如需 system proxy，显式 `RUSTFS_INTERNODE_HTTP_PROXY=system`。
5. TLS/mTLS 策略：
   - 保留当前兼容行为。
   - 增加指标和日志区分 mTLS identity missing/invalid。
   - 后续单独任务评估 fail-closed 模式。
6. 不在第一 PR 中强制 h2c prior knowledge。

## 测试方案

单测覆盖：

- legacy profile 生成现有行为。
- balanced/throughput profile 参数解析和 clamp。
- 非法 env 值 fallback。
- loopback/no_proxy 保持。
- internode proxy off/system 决策。

建议命令：

```bash
cargo test -p rustfs-rio http_client -- --nocapture
cargo test -p rustfs-rio proxy -- --nocapture
cargo test -p rustfs-config internode -- --nocapture
```

## 性能验证

A/B 组合：

- legacy vs balanced vs throughput。
- HTTP/1.1 实际路径 vs TLS HTTP/2 ALPN。
- 10MiB、64MiB、1GiB stream。
- concurrency 32、64、128。

关注：

- internode bytes。
- connection reuse。
- read/write p95/p99。
- CPU。
- RSS。
- error rate。

## 验收标准

- 默认行为不变。
- tuning profile 可观测、可回滚。
- 不强制破坏 HTTP/1.1/proxy/TLS。
- balanced profile 在多机多盘大流下有稳定收益或至少无回退。
