# RustFS 日志治理任务总览

本目录将
[`../RUSTFS_LOGGING_GOVERNANCE_EXECUTION_PLAN_2026-06-11.zh-CN.md`](../RUSTFS_LOGGING_GOVERNANCE_EXECUTION_PLAN_2026-06-11.zh-CN.md)
拆成可独立推进的任务卡。

配套规范：

- [`../RUSTFS_LOGGING_STANDARD_AND_GUARDRAILS_2026-06-11.zh-CN.md`](../RUSTFS_LOGGING_STANDARD_AND_GUARDRAILS_2026-06-11.zh-CN.md)

## 任务列表

| 编号 | 任务 | 优先级 | 文档 |
|---|---|---|---|
| LG-001 | 日志契约、脱敏规则、最小 guard rails | P0 | [LG-001-logging-contract-and-guard-rails.md](./LG-001-logging-contract-and-guard-rails.md) |
| LG-002 | 观测底座输出契约统一 | P1 | [LG-002-observability-substrate-unification.md](./LG-002-observability-substrate-unification.md) |
| LG-003 | request / transport 日志收敛 | P1 | [LG-003-request-and-transport-logging.md](./LG-003-request-and-transport-logging.md) |
| LG-004 | startup / auth / 脱敏治理 | P1 | [LG-004-startup-auth-and-redaction.md](./LG-004-startup-auth-and-redaction.md) |
| LG-005 | audit / notify 运行时日志治理 | P1 | [LG-005-audit-and-notify-runtime-cleanup.md](./LG-005-audit-and-notify-runtime-cleanup.md) |
| LG-006 | ECStore 热路径与后台噪音治理 | P2 | [LG-006-ecstore-hotpath-and-background-noise-reduction.md](./LG-006-ecstore-hotpath-and-background-noise-reduction.md) |
| LG-007 | 反回退验证与收口 | P2 | [LG-007-anti-regression-verification.md](./LG-007-anti-regression-verification.md) |

## 推荐顺序

1. LG-001
2. LG-002
3. LG-003
4. LG-004
5. LG-005
6. LG-006
7. LG-007

## 并行关系

### 可并行

- LG-002 和 LG-001 的文档/guard helper 准备工作
- LG-003 和 LG-004 的方案细化
- LG-005 的运行时梳理与 LG-007 的校验方案设计

### 不建议并行

- LG-002 和 LG-003
- LG-004 和 LG-005
- LG-006 与其他高改动批次

原因：

- `LG-002` 会决定输出契约，`LG-003` 依赖它
- `LG-004` 与 `LG-005` 都可能改 shared runtime / redaction helper 的使用方式
- `LG-006` 风险最高，应该单独控范围

## 每张任务卡统一要求

1. 目标
2. 范围
3. 非目标
4. 详细步骤
5. 验证方法
6. 风险与验收标准
