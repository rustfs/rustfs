# Subtask 03 - Drive 健康探测与元数据超时收敛

## 目标

- 将主动健康检查的周期与超时配置化。
- 清理 metadata/control-plane 路径对 `RUSTFS_DRIVE_MAX_TIMEOUT_DURATION=30s` 的 legacy 放大依赖。

## 实施范围

- `crates/ecstore/src/disk/disk_store.rs`
- `crates/config/src/constants/drive.rs`
- `crates/ecstore/src/rpc/remote_disk.rs`
- 相关回归测试

## 具体执行项

1. 为 active health check 增加配置项:
   - `RUSTFS_DRIVE_ACTIVE_CHECK_INTERVAL_SECS`
   - `RUSTFS_DRIVE_ACTIVE_CHECK_TIMEOUT_SECS`
2. 将 `CHECK_EVERY`、`CHECK_TIMEOUT_DURATION` 改为从配置读取。
3. 让 metadata/list/disk_info/walkdir 等路径优先使用 dedicated timeout。
4. 去掉 metadata 路径对 legacy `ENV_DRIVE_MAX_TIMEOUT_DURATION` 的间接兜底依赖。
5. 为默认值、配置读取和行为收敛补测试。

## 风险边界

1. 不得一刀切缩短所有大对象流式操作的 timeout。
2. 不得让 walkdir、read_all、rename_data 等长耗时路径在 healthy 环境下大量误超时。
3. 不得弱化已有的 failure threshold 和 returning probe 语义。

## 验收标准

1. active health check 周期与 timeout 已配置化。
2. metadata/control-plane 路径不再被 legacy 30 秒值轻易放大。
3. healthy 环境下没有因为 timeout 收缩导致新的明显误报。

## 必做验证

1. 新增/修正 `drive.rs` 和 `disk_store.rs` 相关测试。
2. 运行目标验证:
   - `cargo test -p rustfs-ecstore disk_store -- --nocapture`
   - `cargo check -p rustfs-ecstore -p rustfs`

## 提交要求

- 提交信息建议: `feat(ecstore): tighten drive health and metadata timeout defaults`
- 只提交 active check 与 metadata timeout 收敛相关改动。
