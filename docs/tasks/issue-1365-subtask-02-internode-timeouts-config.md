# Subtask 02 - Internode 超时配置化

## 目标

- 将 internode transport 的关键 timeout 从硬编码改为可配置。
- 拆掉“默认总是走 30 秒 RPC 超时”的粗粒度模型。

## 实施范围

- `crates/protos/src/lib.rs`
- `crates/config/src/constants/mod.rs`
- `crates/config/src/constants/internode.rs`（新增）
- 相关测试或默认值验证

## 具体执行项

1. 提取以下配置项:
   - `RUSTFS_INTERNODE_CONNECT_TIMEOUT_SECS`
   - `RUSTFS_INTERNODE_RPC_TIMEOUT_SECS`
   - `RUSTFS_INTERNODE_TCP_KEEPALIVE_SECS`
   - `RUSTFS_INTERNODE_HTTP2_KEEPALIVE_INTERVAL_SECS`
   - `RUSTFS_INTERNODE_HTTP2_KEEPALIVE_TIMEOUT_SECS`
2. 将 `crates/protos/src/lib.rs` 改为从配置常量读取默认值。
3. 为默认值与配置读取路径补单元测试或等价验证。

## 风险边界

1. 不得在本子任务中修改 drive metadata timeout。
2. 不得把 internode timeout 调得过低导致 healthy 节点大量误杀。
3. 不得改变 TLS、认证、连接缓存等与 timeout 无关的语义。

## 验收标准

1. internode 关键 timeout 均不再硬编码。
2. 默认值收敛到比当前 30 秒更合理的秒级范围。
3. 相关 crate 可成功编译并通过新增测试。

## 必做验证

1. 配置读取或默认值测试。
2. 运行目标验证:
   - `cargo test -p rustfs-config -- --nocapture`
   - `cargo check -p rustfs-protos -p rustfs-ecstore -p rustfs`

## 提交要求

- 提交信息建议: `feat(config): make internode timeouts configurable`
- 仅提交 timeout 配置化相关代码与测试。
