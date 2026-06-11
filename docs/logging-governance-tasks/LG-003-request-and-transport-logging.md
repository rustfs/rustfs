# LG-003 request / transport 日志收敛

## 目标

把 request 主链路日志从“多条步骤日志”收敛成“单条主 completion 事件 +
必要 debug 细节”，降低默认 transport 噪音，同时保留排障能力。

## 范围

- `rustfs/src/server/http.rs`
- `rustfs/src/server/layer.rs`
- `rustfs/src/storage/request_context.rs`

## 非目标

- 不移除 request span
- 不修改 trace context 传播
- 不修改 readiness gate / metrics 主逻辑

## 详细步骤

1. 识别 started / response / chunk / eos / failure 的当前输出点
2. 定义默认 request 主日志字段集合
3. 保留 request span，减少默认逐步 debug
4. 统一 TLS handshake、connection reset、parse failure 的字段和级别
5. 只在 debug 或 feature gate 下保留高频 transport 明细

## 验证方法

- focused `rustfs` server tests
- 手工跑一次正常请求、错误请求、TLS 握手失败样本
- 确认 request_id / trace_id / status_code / duration_ms 可稳定检索

## 风险与验收标准

### 风险

- 误删排障信息
- 与现有 `TraceLayer` / span 逻辑发生冲突

### 验收

- 成功请求默认只有一条主 request 日志
- transport 错误仍具备可诊断字段
- request 链路关联字段完整
