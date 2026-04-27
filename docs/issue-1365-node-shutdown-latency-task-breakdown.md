# Issue #1365 子任务拆分与质量红线

- Issue: https://github.com/rustfs/rustfs/issues/1365
- 关联方案: `docs/issue-1365-node-shutdown-latency-remediation-plan.md`
- 关联分析: `docs/issue-1365-node-shutdown-latency-analysis.md`
- 文档类型: 子任务拆分 + 执行顺序 + 质量闸门
- 生成时间: 2026-04-27

## 1. 总体原则

本任务必须以“**降低节点掉线后的请求抖动**”为目标，但**不能通过牺牲正确性换取表面延迟下降**。

以下红线在所有子任务中都必须成立:

1. 不削弱 EC quorum、bitrot、metadata 一致性、cleanup/undo 语义。
2. 不把业务语义错误误判为节点离线。
3. 不做与本问题无关的大范围重构。
4. 每个子任务都必须携带对应的测试或验证补强。
5. 任一子任务如未通过验收和验证，不允许进入下一个子任务。

## 2. 质量红线

“禁止引入新的 bug 及错误”在本次拆分中落实为以下硬性门槛:

1. **行为红线**
   - 不允许出现新的 panic、unwrap/expect 风险路径。
   - 不允许让 healthy 节点被大面积误判为 offline。
   - 不允许让写路径在缺少必要 cleanup 信息时提前返回。
2. **测试红线**
   - 每个子任务至少要新增或修正一组与本次行为相关的回归测试。
   - 每个子任务完成后必须先跑针对性验证，再进入下一步。
3. **合入红线**
   - 子任务级验收未通过，不得提交。
   - 最终未通过总闸门，不得作为完成态。

## 3. 子任务执行顺序

1. [Subtask 01 - 网络错误立即摘盘](tasks/issue-1365-subtask-01-network-error-fast-fail.md)
2. [Subtask 02 - Internode 超时配置化](tasks/issue-1365-subtask-02-internode-timeouts-config.md)
3. [Subtask 03 - Drive 健康探测与元数据超时收敛](tasks/issue-1365-subtask-03-drive-health-and-metadata-timeouts.md)
4. [Subtask 04 - Quorum-first 读路径改造](tasks/issue-1365-subtask-04-quorum-first-read-paths.md)
5. [Subtask 05 - Peer 控制面 fast-fail 收口](tasks/issue-1365-subtask-05-peer-control-plane-fast-fail.md)
6. [Subtask 06 - 最终验证与回归闸门](tasks/issue-1365-subtask-06-verification-and-regression-gate.md)

## 4. 执行策略

### 4.1 为什么按这个顺序

1. 先做 Subtask 01
   - 这是收益最大、改动最聚焦的止血点。
   - 能先把“首次碰撞坏节点”的感知时间压缩到秒级。
2. 再做 Subtask 02 和 03
   - 这两步负责拆掉 30 秒级粗粒度 timeout 模型。
   - 也为后续 quorum-first 提供更稳定的失败收敛基础。
3. 再做 Subtask 04
   - 只改读元数据、列目录等适合 quorum-first 的路径。
   - 故意不先动复杂写回滚路径。
4. 最后做 Subtask 05
   - 把控制面与数据面行为统一，避免“数据面恢复了，console 还在卡”。

### 4.2 禁止事项

1. 不得在 Subtask 04 中顺手重写写路径。
2. 不得在 Subtask 01 中混入 timeout 配置化。
3. 不得在 Subtask 05 中顺手调整 `/health/ready` 语义，除非有专门回归测试覆盖。

## 5. 阶段性交付要求

每个子任务完成后，交付物至少包括:

1. 代码改动
2. 相关测试
3. 子任务级验证记录
4. 是否满足该子任务“禁止引入新 bug”的证据说明

## 6. 完成定义

只有满足以下条件，Issue #1365 这一轮改造才算完成:

1. 6 个子任务全部完成。
2. 每个子任务的验收标准全部满足。
3. 最终验证闸门通过。
4. 不存在未解释的行为回归、panic、新的错误路径或明显误判离线现象。
