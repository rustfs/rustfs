# ADR 0001 Gap Audit (2026-04-01)

关联文档：
- [0001-zero-copy-data-plane.md](./0001-zero-copy-data-plane.md)

状态标记：
- `[x]` 已实现
- `[~]` 部分实现 / 需要继续补齐
- `[ ]` 尚未实现

审计范围：
- 基于当前工作树代码进行核对。
- 包含当前已存在的 staged 变更：`crates/ecstore/src/set_disk/read.rs`、`crates/ecstore/benches/direct_chunk_benchmark.rs`、`rustfs/src/app/object_usecase/zero_copy_tests.rs`。

本轮已补齐：
- [x] 新增 `RUSTFS_OBJECT_ZERO_COPY_MODE` / `RUSTFS_OBJECT_ZERO_COPY_MMAP_WINDOW_BYTES` / `RUSTFS_OBJECT_ZERO_COPY_MAX_ACTIVE_MMAP_BYTES` 配置常量。
- [x] LocalDisk chunk fast path 支持多窗口 mmap、active mmap bytes 限流和 `mmap_disabled` fallback。
- [x] `PooledChunk` 改为基于 `Bytes::from_owner(...)` 的共享视图，不再在 `as_bytes()` / `slice()` 时强制复制。
- [x] LocalDisk chunk fast path 改为 lazy mmap window stream，并新增 active mmap bytes gauge。
- [x] plain PUT reduced-copy 不再回退到 `WarpReader<AsyncRead>`，而是直接走 `Reader + BlockReadable` hash path。
- [x] bitrot verifier 改为 source-stream -> verified-chunk 流，坏帧错误延迟到真实消费时暴露。
- [x] transformed PUT 新增 size-preserving SSE fast path：加密链路现在可复用 reduced-copy ingress，并通过 block-native `EncryptReader` 接进 encode 热路径。

## 总结

当前实现与 ADR 的对齐情况：

| Phase | 结论 | 说明 |
|------|------|------|
| Phase 1 | `Mostly Done` | 复制模式词汇表和主指标已经存在，但旧 `zero_copy_*` 语义还没有完全退场。 |
| Phase 2 | `Partially Done` | chunk 核心抽象、兼容层和磁盘接口已落地；lazy mmap window stream 和 active mmap gauge 已补上，但 pool producer / compat 收口仍未完成。 |
| Phase 3 | `Mostly Done` | local/plain GET fast path、chunk -> HTTP bridge 已接通；仍有部分 guard/fallback 语义需要继续收口。 |
| Phase 4 | `Mostly Done` | direct shard path 与 bitrot verifier 已经流式化；远端盘透传与 reconstructed decoder 抽象仍未收口。 |
| Phase 5 | `Partially Done` | cache/writeback 所有权收口做了一部分，但离 ADR 要求的统一 freeze/shared body 仍有距离。 |
| Phase 6 | `Partially Done` | plain PUT 已经进入 chunk-native hash/block 边界，但 block assembler / encoder 抽象和主链路重组还没完成。 |
| Phase 7 | `Partially Done` | SSE transformed PUT 已有一条 size-preserving fast path；压缩和独立 fallback taxonomy 仍未完成。 |

## Phase 1: 语义校正

- [x] `CopyMode` 与 `FallbackReason` 已集中定义在 `crates/io-metrics/src/lib.rs`。
- [x] GET chunk bridge 已映射到 `CopyMode::{TrueZeroCopy, SharedBytes, SingleCopy, Reconstructed}`，见 `crates/object-io/src/get.rs`。
- [x] PUT 已区分 attempted fast path 和 effective copy mode，见 `rustfs/src/app/object_usecase/put_object_flow.rs`。
- [~] 旧 `record_zero_copy_read` / `record_zero_copy_fallback` / `record_zero_copy_write` 仍然存在并继续被调用，见 `crates/io-metrics/src/lib.rs`、`crates/ecstore/src/disk/local.rs`、`crates/ecstore/src/bitrot.rs`。
  需要继续补齐：
  - 把旧 `zero_copy_*` 调用点彻底迁移到 `record_io_path_selected` / `record_io_copy_mode` / `record_io_fallback`。
  - 明确旧指标的 deprecation 退出计划，避免后续继续被误用。

## Phase 2: 基础设施落地

- [x] `IoChunk` / `MappedChunk` / `PooledChunk` / `BoxChunkStream` / `ChunkSource` 已存在，见 `crates/io-core/src/chunk.rs`。
- [x] `ChunkStreamReader` 已存在，见 `crates/io-core/src/adapter.rs`。
- [x] `DiskAPI::read_file_chunks(...)` 已扩展到 Local/Remote disk，见 `crates/ecstore/src/disk/mod.rs`、`crates/ecstore/src/disk/local.rs`、`crates/ecstore/src/rpc/remote_disk.rs`。
- [x] LocalDisk 现在支持 zero-copy mode、mmap window、active mmap bytes 配置，见 `crates/config/src/constants/zero_copy.rs`、`crates/ecstore/src/disk/local.rs`。
- [x] `PooledChunk` 现在具备共享 owner 语义，见 `crates/io-core/src/chunk.rs`。
- [x] LocalDisk 的多窗口 mmap 已改成 lazy chunk stream，不再在请求开始时一次性构造 `Vec<IoChunk>`。
- [~] `read_file_zero_copy()` 仍然是独立实现，没有反向复用 chunk 版接口。
- [~] pool-backed chunk producer 仍然没有真正接进主读路径；当前大多数 fallback 仍直接返回 `Shared(Bytes)`，并没有生成 `PooledChunk`。
- [~] mmap active-bytes / pool usage 的独立指标只完成了一半：active mmap bytes gauge 已落地，pooled chunk usage 还没有。

优先补齐：
- [x] 把 LocalDisk 的多窗口 mmap 从 eager `Vec<IoChunk>` 变成 lazy chunk stream。
- [~] 增加 active mmap bytes / pooled chunk 使用情况的可观测指标。
- [ ] 为 buffered fallback 真正产出 `PooledChunk`，而不是继续直接 `Shared(Bytes)`。

## Phase 3: 读 fast path v1

- [x] `build_chunk_blob(...)` 已把 `BoxChunkStream` 直接桥接到 `StreamingBlob`，见 `crates/object-io/src/get.rs`。
- [x] GET fast path 选择逻辑已存在，app 层可走 chunk body，见 `rustfs/src/app/object_usecase/get_object_zero_copy.rs` 与 `crates/object-io/src/get.rs`。
- [x] LocalDisk chunk fast path 现在支持非零 offset 和多窗口返回，见 `crates/ecstore/src/disk/local.rs`。
- [x] 本轮新增测试覆盖 large-read multi-window 场景，见 `crates/ecstore/src/disk/local.rs`。
- [~] `ChunkHttpBody` 没有以 ADR 命名存在，当前是等价实现 `build_chunk_blob(...)`。
- [~] fallback reason 词汇表仍偏薄，Local GET 仍未覆盖所有 ADR 中提到的细粒度原因。
- [~] `read_file_zero_copy()` 仍会独立 mmap，不是 “chunk API -> 单个 `Bytes`” 的渐进兼容层。

优先补齐：
- [ ] 收敛 range guard / fallback reason，避免不同读路径各自定义兼容行为。
- [ ] 把 compat `read_file_zero_copy()` 重新绑定到 chunk 层，以免两套 mmap 行为继续分叉。

## Phase 4: 读 fast path v2

- [x] `create_bitrot_chunk_stream(...)` 已存在，bitrot decode 已能返回 chunk 结果，见 `crates/ecstore/src/bitrot.rs`。
- [x] `SetDisks::get_object_chunks(...)` 已存在 reconstructed path，见 `crates/ecstore/src/set_disk/read.rs`。
- [x] staged 变更已经继续扩展 `send_direct_data_shard_chunks(...)` 的 block 边界处理和 whole-multipart reconstructed 覆盖。
- [x] `create_bitrot_chunk_stream(...)` 现在按 source stream 增量校验和产出 verified chunk，不再先 drain 到 `Vec<IoChunk>`。
- [x] bitrot verifier 现在是 source-stream 驱动；后续坏帧错误会在真实消费时暴露，而不是在构造阶段一次性聚合。
- [~] 远端盘 `read_file_chunks()` 仍是 `read_file_zero_copy() -> Shared(Bytes)` 一次性包装，见 `crates/ecstore/src/rpc/remote_disk.rs`。
- [~] reconstructed path 仍依赖 `erasure.decode(&mut writer, ...)` 写入通道 writer，没有独立的 `ErasureChunkDecoder` / `PooledChunk` reconstruction 抽象。
- [~] `BitrotChunkVerifier` 语义已经体现在流式状态机里，但 `BitrotChunkSource` / `ErasureChunkDecoder` 这些 ADR 中点名的专用类型还没有显式成型。

优先补齐：
- [x] 把 bitrot verifier 改成真正的 source-stream -> verified-chunk 流，不再先聚合 `Vec<IoChunk>`。
- [ ] 为 remote disk 增加真正的 chunk/bytes 流式透传，而不是一把 `read_file_zero_copy()`。
- [ ] 把 reconstructed path 从 writer-style decode 收敛为显式 chunk decoder，便于 copy-mode 和 pool usage 精确打点。

## Phase 5: 缓存与小对象收口

- [~] `crates/object-io/src/get.rs` 已经存在 `GetObjectCacheWriteback` 等结构，说明 cache/writeback 收口工作已经开始。
- [~] 状态文档显示 cache-hit borrowed view 与 writeback metadata 已做过一轮 ownership cleanup，但本轮代码核对没有看到“单一 frozen body 在 cache-hit / seek-support / writeback 之间统一复用”的闭环证据。
- [~] cache 与 disk fast path 的指标边界仍未完全从代码层面收敛验证。
- [ ] `rustfs/src/storage/concurrency/object_cache.rs` 还没有体现 ADR 所要求的“统一 freeze 后共享体”作为唯一数据表示。

优先补齐：
- [ ] 明确 cache-hit body、seek buffering、cache writeback 是否共用同一份 `Bytes`。
- [ ] 给 cache served / cache writeback / disk fast path 增加互斥且稳定的指标约束。

## Phase 6: 写 reduced-copy v1

- [x] PUT ingress 规划类型已经存在：`PutObjectIngressKind`, `PutObjectIngressPlan`, `PutObjectBodyPlan`，见 `crates/object-io/src/put.rs`。
- [x] “reduced-copy candidate” 已经能从 HTTP body 进入 `PutObjectReducedCopyIngress`，并有针对 `build_put_object_reduced_copy_reader(...)` 的测试，见 `crates/object-io/src/put.rs`。
- [x] reduced-copy candidate 在 `build_put_object_plain_hash_stage(...)` 里已经直接落到 `Reader + BlockReadable`，不再先回退到 `WarpReader<AsyncRead>`。
- [x] app 层 plain PUT 已经把这条 reduced-copy 路径记成 fast path，而不再只是“阶段边界占位”。
- [ ] `BoxIngressStream` 没有落地。
- [ ] 独立的 `HashStage` 没有落地。
- [ ] `BlockAssembler` 没有落地。
- [ ] `ErasureChunkEncoder` 没有落地。
- [ ] store put 主链路仍是 `PutObjReader` / `HashReader` / 现有 encode 流程，没有真正 chunk-native PUT 数据面。

优先补齐：
- [x] 先做 plain PUT 的 chunk-native hash stage，去掉 `ReducedCopyCandidate -> Reader -> HashReader` 这层回退。
- [ ] 在 `ecstore` 侧补 block assembler + shard encode 的最小闭环，哪怕先只覆盖 plain/non-SSE/non-compress PUT。

## Phase 7: 写变换适配

- [~] transformed 路径的 copy mode 已有语义标记：`resolve_put_effective_copy_mode(...)` 会返回 `Transformed`。
- [~] transformed 路径里压缩仍是旧 reader 变换链；但 size-preserving 的 SSE 路径现在已经能复用 reduced-copy ingress 和 block-native `EncryptReader`。
- [ ] 压缩 chunk-native 写路径未实现。
- [x] SSE/KMS/SSE-C 的 size-preserving transformed PUT 已有一条 chunk-native fast path 原型。
- [ ] transformed fallback reason 还没有形成与 plain PUT 完全隔离的独立体系。

优先补齐：
- [ ] 明确 transformed PUT 的 fallback taxonomy。
- [x] 先选一条最简单的变换链路做 chunk-native 原型，再决定是否继续扩展。

## 推荐下一批实现顺序

1. [ ] `Phase 6 / plain PUT reduced-copy v1`
   原因：这是 ADR 当前实现最大的真实缺口，现有代码还停留在 compat reader 边界。
2. [ ] `Phase 4 / bitrot verifier stream 化`
   原因：当前 bitrot/EC 仍有明显 eager aggregation，直接限制 true zero-copy 读路径可信度。
3. [ ] `Phase 2 / lazy mmap window stream + active-bytes metrics`
   原因：本轮已补配置和窗口化，但还不是 ADR 目标的完整窗口管理模型。
4. [ ] `Phase 5 / cache body ownership 收口`
   原因：这块关系到 small-object/cache-hit 是否会反复复制，也是读路径收口的最后一段。
