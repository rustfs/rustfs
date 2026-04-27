# Subtask 05 - Peer 控制面 fast-fail 收口

## 目标

- 将控制面 peer 调用在节点离线时的行为与数据面统一。
- 避免 console/admin 仍单独卡在长时间 peer 请求上。

## 实施范围

- `crates/ecstore/src/rpc/peer_rest_client.rs`
- 相关 admin/console 聚合路径
- 相关回归测试

## 具体执行项

1. 为 peer client 增加离线 fast-fail 能力，或复用现有健康状态模型。
2. 对 `local_storage_info()`、`server_info()` 等 peer 请求补网络类快速失败策略。
3. 为控制面恢复探测补最小必要机制。
4. 为控制面聚合场景补回归测试。

## 风险边界

1. 不得误改 `/health/ready` 的判定语义。
2. 不得让控制面直接屏蔽本应暴露的真实错误信息。
3. 不得在本子任务中顺带改 unrelated admin route。

## 验收标准

1. 节点掉线后，console/admin 聚合请求不会再单独卡 30 秒。
2. 控制面错误反馈与数据面离线状态更一致。
3. 不引入 readiness 或 route registration 回归。

## 必做验证

1. 新增/修正 peer/admin 相关回归测试。
2. 运行目标验证:
   - `cargo test -p rustfs-ecstore peer_rest_client -- --nocapture`
   - `cargo test -p rustfs admin_usecase -- --nocapture`
   - `cargo test -p rustfs route_registration_test -- --nocapture`

## 提交要求

- 提交信息建议: `feat(admin): fast-fail offline peers in control-plane paths`
- 仅提交 peer 控制面相关改动与测试。
