# LG-004 startup / auth / 脱敏治理

## 目标

统一 startup / auth 相关日志的级别与输出方式，并优先清掉敏感字段直出风险。

## 范围

- `rustfs/src/main.rs`
- `rustfs/src/startup_iam.rs`
- `rustfs/src/license.rs`
- `rustfs/src/auth.rs`
- `rustfs/src/protocols/client.rs`

## 非目标

- 不改启动顺序
- 不改 IAM / license / auth 业务语义
- 不改变协议请求构造逻辑

## 详细步骤

1. 以 `startup_iam` 为参考统一 retry / degraded / recovered 语义
2. 收敛 startup logo、TLS、compat、license 的默认 `info` 输出
3. 清理 `auth.rs` 中 access key 直接打印
4. 清理 `protocols/client.rs` 中 access key 直接打印
5. 引入统一掩码 helper 或最小展示策略

## 验证方法

- focused `rustfs` tests
- 手工检查启动日志样本
- 检查 access key、token、secret 等字段不再明文出现

## 风险与验收标准

### 风险

- startup 路径对运维很敏感，过度精简会损失上下文
- auth 路径如果脱敏策略不统一，会出现新旧混杂

### 验收

- startup 日志更摘要化
- 重试日志级别稳定
- 不再出现敏感字段裸露
