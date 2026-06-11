# LG-002 观测底座输出契约统一

## 目标

统一 `crates/obs` 层面的本地 stdout、rolling file、OTLP 旁路日志契约，
确保同一事件不会因为 sink 不同而变成不同形态。

## 范围

- `crates/obs/src/telemetry/local.rs`
- `crates/obs/src/telemetry/otel.rs`
- `crates/obs/src/telemetry/filter.rs`
- 如有必要，`crates/obs/src/config.rs`

## 非目标

- 不修改业务调用点含义
- 不改变 OTLP enablement 语义
- 不更改现有 metrics 命名

## 详细步骤

1. 梳理 stdout-only、file logging、OTLP + stdout mirror 的当前差异
2. 统一输出格式策略，优先保证 machine-readable
3. 统一 span event 默认策略，避免默认输出过多生命周期事件
4. 收敛 observability 自身初始化日志，改为摘要式
5. 保持 `RUST_LOG` 优先级与现有过滤语义兼容

## 验证方法

- `cargo test -p rustfs-obs`
- 比较 stdout/file/OTLP fallback 场景下的事件形态是否一致
- 手工检查初始化日志是否减少重复或语义冲突

## 风险与验收标准

### 风险

- 输出格式变化可能影响已有运维消费方式
- 过滤调整可能引入日志过多或过少

### 验收

- 本地 stdout 与 file 主要事件契约一致
- observability 初始化日志更简洁
- `RUST_LOG` 覆盖逻辑不退化
