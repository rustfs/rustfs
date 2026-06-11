# LG-005 audit / notify 运行时日志治理

## 目标

统一 audit / notify 运行时生命周期、dispatch、replay、idle 状态的日志表达，
减少“未配置 / 已启动 / 跳过 / 重试”类重复噪音。

## 范围

- `crates/audit/src`
- `crates/notify/src`
- 按需触达 `crates/targets/src`

## 非目标

- 不改变 audit dispatch 语义
- 不改变 notify target runtime 语义
- 不改变 replay worker 行为

## 详细步骤

1. 统一 target lifecycle 事件名与字段
2. 将 sentence-first 写法改成事件 + 字段形式
3. 清理预期内 idle / disabled / no-config 的 `warn` 噪音
4. 保留 target_id、reason、error、retry_count 等关键诊断字段
5. 对 replay / dispatch 失败做更聚合的表达

## 验证方法

- `cargo test -p rustfs-audit`
- `cargo test -p rustfs-notify`
- shared helper 变动时补充 `rustfs-targets` focused tests

## 风险与验收标准

### 风险

- 运行时日志治理容易误碰 shared runtime helper
- 如果级别改错，会影响运维对 target 健康的判断

### 验收

- audit / notify 生命周期更清晰
- 预期内空闲状态不再刷屏
- 失败日志仍可按 target 检索
