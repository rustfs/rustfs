# Subtask 01 - 网络错误立即摘盘

## 目标

- 将远端盘健康状态机从“只有 timeout 才摘盘”升级为“明确网络不可达就摘盘”。
- 将首次碰撞坏节点后的后续请求收敛到 fast-fail，而不是重复等待长超时。

## 实施范围

- `crates/ecstore/src/rpc/remote_disk.rs`
- `crates/ecstore/src/rpc/peer_s3_client.rs`
- `crates/ecstore/src/rpc/client.rs`
- 相关回归测试

## 具体执行项

1. 为 `remote_disk.rs` 增加统一网络错误分类逻辑。
2. 将 `execute_with_timeout_for_op()` 的摘盘条件扩展到明确网络错误。
3. 对 `get_client()` 失败补上网络类错误摘盘。
4. 同步 `peer_s3_client.rs` 的健康判定行为。
5. 为“网络错误立即摘盘”和“业务错误不摘盘”分别补测试。

## 风险边界

1. 只允许对网络/传输层失败做立即摘盘。
2. 不得把 `FileNotFound`、`VolumeNotFound`、`Corrupt` 之类业务错误混入 offline 判定。
3. 不得在本子任务中顺手修改 timeout 默认值。

## 验收标准

1. 连接拒绝、连接重置、transport unavailable 等网络错误可触发 `mark_faulty_and_evict()`。
2. 纯业务错误不会触发摘盘。
3. 同一坏盘上的第二批请求明显快于第一批请求。
4. 不新增 panic、unwrap、expect 风险路径。

## 必做验证

1. 新增/修正 `remote_disk` 相关回归测试。
2. 新增/修正 `peer_s3_client` 相关回归测试。
3. 运行目标验证:
   - `cargo test -p rustfs-ecstore remote_disk -- --nocapture`
   - `cargo test -p rustfs-ecstore peer_s3_client -- --nocapture`

## 提交要求

- 提交信息建议: `feat(ecstore): fast-fail remote disks on network errors`
- 仅提交本子任务相关代码与测试，不混入 docs 以外的无关改动。
