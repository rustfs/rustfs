# Task 08: batch metadata RPC behind capability gate

## 目标

在现有 `ReadMultiple` 优化完成后，再新增窄边界的 batch metadata/read-version RPC。该能力必须在 capability gate 后启用，支持 mixed-version fallback，不在 RPC 层跨 disk 做 quorum。

## 为什么不能先做

当前已经有：

- `ReadMetadata`
- `ReadXL`
- `ReadVersion`
- `ReadMultiple`
- GET object metadata cache
- metadata fanout quorum reducer

直接新增泛化 `BatchReadMetadata` 会增加 proto 复杂度和 rolling upgrade 风险。如果没有真实热点证据，收益不确定。

## 代码落点

- `crates/protos/src/node.proto`
- `crates/protos/src/generated/`
- `crates/ecstore/src/cluster/rpc/remote_disk.rs`
- `rustfs/src/storage/rpc/disk.rs`
- `crates/ecstore/src/set_disk/read.rs`

## 协议边界

推荐新增能力：

- 单 remote disk。
- 多 path。
- 可选 version id。
- 不跨 disk 聚合。
- 不计算 quorum。
- partial failure 每项显式返回。

不推荐：

- 在 RPC 层跨 disk 做 quorum。
- 在 remote server 内部替 client 做 metadata selection。
- 让 batch RPC 绕过 existing object quorum logic。

## 实施步骤

1. 增加 capability probe：
   - 节点支持 batch metadata 时才启用。
   - 不支持时 fallback 到现有 unary/read_multiple 路径。
2. proto 增加 request/response：
   - `BatchReadMetadataRequest`
   - `BatchReadMetadataResponse`
   - `BatchMetadataItemResult`
3. response 每项包含：
   - success。
   - data/msgpack payload。
   - optional error。
   - index/path。
4. remote client：
   - 优先 batch。
   - fallback unary。
   - decode error 明确返回。
5. set-level fanout：
   - 仍由 `SetDisks` 做 quorum。
   - batch 只减少同一 remote disk 的多次 RPC。
6. 增加大小限制：
   - max batch items。
   - max payload bytes。
   - timeout 单独配置。

## 测试方案

单测：

- server 支持 batch 成功。
- server 不支持 batch fallback。
- partial failure 不影响其他 item。
- item decode failure 返回错误。
- max batch limit 生效。

mixed-version 测试：

- 新 client + 老 server。
- 老 client + 新 server。
- 部分节点支持 batch、部分不支持。

建议命令：

```bash
cargo test -p rustfs-ecstore batch_metadata -- --nocapture
cargo test -p rustfs --lib batch_metadata -- --nocapture
cargo test -p rustfs-ecstore read_multiple -- --nocapture
```

## 性能验证

目标 workload：

- 小对象 GET/HEAD。
- list/metadata-heavy 场景。
- 多节点 EC。
- 高并发 64/128。

关注：

- gRPC request count。
- metadata fanout duration。
- quorum reached latency。
- CPU allocation。
- p95/p99。

## 验收标准

- 默认 `off` 或 `auto`。
- mixed-version fallback 可靠。
- 不绕过 quorum。
- partial failure 可解释。
- 小对象 metadata-heavy workload 有可测收益。
