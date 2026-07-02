补充一次基于远程 `main` 的复核：

已从 `origin/main` 切出新分支 `houseme/issue-797-internode-plan-refresh`，基线为 `origin/main@70e1d79df`，并重新核对了关键路径。

复核结论：前面拆出的 9 个子任务仍然成立，无需推翻或重排；仅补充了基于最新 `origin/main` 的复核记录。

确认仍成立的关键点：

- gRPC channel cache、keepalive、connect timeout、RPC timeout 已存在，不应新增平行 `GrpcChannelPool`。
- `ReadAt`/`WriteStream` 在 `NodeService` 中仍是 unimplemented，远端大流仍应优先优化 HTTP data-plane。
- `HttpWriter` shutdown 仍存在吞掉后台写流 `Ok(Err(_))` 的风险，Task 01 仍应优先。
- remote lock 的 `release`、`release_locks_batch`、`refresh`、`force_release`、`check_status` 仍未统一走 `execute_rpc`，Task 02 仍成立。
- `ReadMultiple` 仍存在 JSON/msgpack 双转换和 decode 失败静默丢弃风险，Task 03 仍成立。
- distributed erasure 下 metadata cache 仍保守 bypass，这是正确边界，不建议直接打开 cache/prefetch。
- `SameNode` 仍缺生产级来源，shard locality 仍应先 observe-only，再 opt-in。

本地已更新 `docs/issue-797/README-zh.md`，并新增 `docs/issue-797/main-refresh-2026-07-02-zh.md` 记录本次复核细节。
