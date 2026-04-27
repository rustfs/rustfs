# Subtask 04 - Quorum-first 读路径改造

## 目标

- 将适合 quorum-first 的读元数据/列表类 fanout 路径改成“达到 quorum 即返回”。
- 避免尾部坏节点或慢节点继续阻塞主响应。

## 实施范围

- `crates/ecstore/src/set_disk/read.rs`
- `crates/ecstore/src/set_disk/list.rs`
- `crates/ecstore/src/set_disk/multipart.rs`
- `crates/ecstore/src/batch_processor.rs` 或新增 fanout helper
- 相关回归测试

## 具体执行项

1. 抽取可复用的 quorum-first fanout helper。
2. 第一批接入读元数据路径。
3. 第二批接入目录/列表路径。
4. 如适合，再接入 multipart 状态读取路径。
5. 为“满足 quorum 提前返回”和“无法达到 quorum 提前失败”补测试。

## 风险边界

1. 只改 **quorum-capable** 路径。
2. 不得在本子任务中顺手改写 `set_disk/write.rs` 的回滚语义。
3. 不得以减少等待为名丢失错误归约所需的关键信息。

## 验收标准

1. 单节点掉线时，读元数据/列目录等用户敏感操作不再稳定等待全部远端结果。
2. 达到 quorum 时可提前返回。
3. 无法达到 quorum 时可以明确提前失败。
4. 不引入读一致性回归。

## 必做验证

1. 新增/修正 `set_disk` 读路径相关回归测试。
2. 至少覆盖:
   - 一个远端离线但仍满足 quorum
   - 远端失败过多导致 quorum 不足
3. 运行目标验证:
   - `cargo test -p rustfs-ecstore set_disk -- --nocapture`

## 提交要求

- 提交信息建议: `perf(ecstore): use quorum-first fanout for read metadata paths`
- 严禁在本提交中混入复杂写路径语义调整。
