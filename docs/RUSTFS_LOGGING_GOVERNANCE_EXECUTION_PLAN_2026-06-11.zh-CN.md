# RustFS 日志治理执行总览 2026-06-11

本文件将日志治理方案从 `docs/architecture/` 迁移到 `docs/` 根目录，
并作为后续执行入口。

配套文档：

- 规范文档：
  [`RUSTFS_LOGGING_STANDARD_AND_GUARDRAILS_2026-06-11.zh-CN.md`](./RUSTFS_LOGGING_STANDARD_AND_GUARDRAILS_2026-06-11.zh-CN.md)
- 任务卡目录：
  [`logging-governance-tasks/README.md`](./logging-governance-tasks/README.md)

## 背景

当前 RustFS 的日志底座已经具备：

- `tracing`
- 本地 stdout / rolling file / OTLP
- `request_id` / `trace_id` / `span_id`
- 基础降噪过滤

真正的问题不在“有没有日志”，而在：

1. 输出契约未完全统一
2. 调用点长期并存两套写法
3. 高频路径存在重复噪音
4. 敏感字段脱敏不一致
5. 级别语义不稳定

## 执行边界

本轮治理优先关注：

- `crates/obs`
- `rustfs/src/server`
- `rustfs/src/main.rs`
- `rustfs/src/startup_iam.rs`
- `rustfs/src/auth.rs`
- `rustfs/src/protocols/client.rs`
- `crates/audit/src`
- `crates/notify/src`
- `crates/ecstore/src`

本轮不做：

- 替换日志框架
- 一次性全仓重写日志
- 修改存储正确性语义
- 修改 readiness / audit / notify 运行时语义

## 批次拆分

| 编号 | 任务 | 优先级 | 文档 |
|---|---|---|---|
| LG-001 | 日志契约、脱敏规则、最小 guard rails | P0 | [logging-governance-tasks/LG-001-logging-contract-and-guard-rails.md](./logging-governance-tasks/LG-001-logging-contract-and-guard-rails.md) |
| LG-002 | 观测底座输出契约统一 | P1 | [logging-governance-tasks/LG-002-observability-substrate-unification.md](./logging-governance-tasks/LG-002-observability-substrate-unification.md) |
| LG-003 | request / transport 日志收敛 | P1 | [logging-governance-tasks/LG-003-request-and-transport-logging.md](./logging-governance-tasks/LG-003-request-and-transport-logging.md) |
| LG-004 | startup / auth / 脱敏治理 | P1 | [logging-governance-tasks/LG-004-startup-auth-and-redaction.md](./logging-governance-tasks/LG-004-startup-auth-and-redaction.md) |
| LG-005 | audit / notify 运行时日志治理 | P1 | [logging-governance-tasks/LG-005-audit-and-notify-runtime-cleanup.md](./logging-governance-tasks/LG-005-audit-and-notify-runtime-cleanup.md) |
| LG-006 | ECStore 热路径与后台噪音治理 | P2 | [logging-governance-tasks/LG-006-ecstore-hotpath-and-background-noise-reduction.md](./logging-governance-tasks/LG-006-ecstore-hotpath-and-background-noise-reduction.md) |
| LG-007 | 反回退验证与收口 | P2 | [logging-governance-tasks/LG-007-anti-regression-verification.md](./logging-governance-tasks/LG-007-anti-regression-verification.md) |

## 推荐顺序

1. LG-001
2. LG-002
3. LG-003
4. LG-004
5. LG-005
6. LG-006
7. LG-007

说明：

- `LG-001` 先落规则，否则后续批次只能做风格性改写。
- `LG-002` 先统一底座，避免同一事件在 stdout / file / OTLP 下出现不同契约。
- `LG-006` 风险最高，必须最后进入，并按 ECStore 子域拆小 PR。

## 统一验收口径

所有实现批次默认要回答这几件事：

1. 是否提升了查询性，而不是只改文案
2. 是否降低了默认 `info` / `warn` 噪音
3. 是否避免记录敏感字段
4. 是否保留了 request / target / background task 的可关联性
5. 是否没有改变业务语义与关键运行时边界

## 最小实施建议

最安全的第一步不是大面积改调用点，而是：

1. 先落规范文档
2. 再落 redaction helper / format helper
3. 再补最小 guard rails
4. 最后做大面积调用点收敛

这样能避免后续批次做成纯风格 churn。
