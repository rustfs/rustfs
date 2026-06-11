# LG-006 ECStore 热路径与后台噪音治理

## 目标

在不改变存储语义的前提下，优先治理 ECStore 高体量、高频、运维价值低的日志点。

## 范围

优先从这些子域切入，而不是一次性扫整个 `crates/ecstore`：

- replication pool / resync
- lifecycle background work
- tier config / migration
- bucket metadata parse warnings
- disk startup / cleanup

## 非目标

- 不改变 quorum / bitrot / metadata correctness
- 不做 crate-wide 风格重写
- 不重写热路径控制流

## 详细步骤

1. 为每个子域先列出高频噪音点
2. 识别哪些 `info!/warn!` 实际上是 expected fallback / loop detail
3. 把重复句子改成稳定字段或阶段性摘要
4. 优先改最具运维价值的日志，不做纯文本润色
5. 每个子域单独提交，避免大 diff

## 验证方法

- focused `cargo test -p rustfs-ecstore`
- 对相关背景任务做手工日志样本比对
- 重点检查热路径没有额外分配、没有额外阻塞

## 风险与验收标准

### 风险

- 本批风险最高，容易误碰正确性敏感路径
- 大面积统一风格会制造高 churn

### 验收

- 默认 `info` / `warn` 量显著下降
- 日志更便于按 bucket / target / path / phase 检索
- 存储语义和关键行为不漂移
