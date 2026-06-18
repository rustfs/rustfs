# Issue #3557 执行方案

## 概览

- issue: `#3557`
- 目标：为管理面配置的外连 URL 增加共享 egress guard
- 当前执行分支：
  - `houseme/issue-3557-egress-guard`

本轮优先做最小闭环，不一次性覆盖所有外连点。

## 实现边界

- 第一批接入点：
  - `rustfs/src/admin/handlers/oidc.rs`
  - `rustfs/src/admin/router.rs` 中 object lambda webhook client
  - `crates/targets/src/target/webhook.rs`
- 第一批暂不扩展：
  - keystone
  - warm tier / replication target
  - 其他非 HTTP 出口

## 关键实现点

1. 新增共享 egress guard helper
2. 最小策略：
   - 只允许绝对 URL
   - 阻止 loopback
   - 阻止 RFC1918 私网
   - 阻止 link-local
   - 阻止 metadata endpoint `169.254.169.254`
3. 第一版优先用 host 级静态校验和 IP 字面量校验，不先引入复杂 DNS rebinding 处理
4. 每个接入点保留现有 TLS 逻辑，仅在构造 client 或校验配置前增加 egress guard

## 测试矩阵

- 单元测试：
  - `127.0.0.1`
  - `localhost`
  - `169.254.169.254`
  - RFC1918 地址
  - 合法公网 `https://example.com`
- 集成验证：
  - OIDC 配置校验拒绝不安全地址
  - webhook 配置拒绝不安全地址
  - object lambda webhook client 构造前拒绝不安全地址

## 默认约束

- 第一版不新增用户配置项
- 第一版不改协议行为和返回格式，只增加拒绝路径
- 第一版先覆盖最清晰的 HTTP 外连点，再决定是否向 keystone / tier 扩展
