# Task 03: optimize ReadMultiple serialization and decoding

## 目标

修复 `ReadMultiple` 当前性能与正确性问题：服务端不再做 JSON -> object -> msgpack 的不必要往返，客户端不再用 `filter_map(...ok())` 静默丢弃 decode 失败项。该任务优先级高于新增 `BatchReadMetadata`，因为它沿用现有协议和调用路径，收益更确定。

## 代码落点

- `crates/ecstore/src/cluster/rpc/remote_disk.rs`
  - `RemoteDisk::read_multiple`
- `rustfs/src/storage/rpc/disk.rs`
  - `handle_read_multiple`
- `crates/ecstore/src/set_disk/read.rs`
  - `read_multiple_files`
  - `collect_read_multiple_results`
- `crates/protos/src/node.proto`
  - `ReadMultipleRequest`
  - `ReadMultipleResponse`

## 当前问题

当前服务端在处理 `ReadMultipleResp` 时，可能先序列化 JSON，再从 JSON 反序列化成对象，再编码 msgpack。这对小对象高并发 GET/HEAD 路径会增加 CPU 和 allocation。

客户端解析 `read_multiple_resps_bin` 时使用 `filter_map(|buf| decode_msgpack_or_json(...).ok())`，如果某个响应 decode 失败，会被静默丢弃，最终可能表现为结果不足或错误归因模糊。

## 实施步骤

1. 服务端直接从 `Vec<ReadMultipleResp>` 编码 msgpack：
   - 不经 JSON roundtrip。
   - 保留 JSON 字段作为兼容 fallback。
2. 客户端 decode 时改为显式错误处理：
   - 任一 item decode 失败，返回带上下文的 `DiskError`。
   - 错误包含 item index、payload kind、remote endpoint。
3. 保持 mixed-version 兼容：
   - 优先读 `read_multiple_resps_bin`。
   - 为空时 fallback 到 JSON string list。
4. 增加响应数量校验：
   - decoded item count 与服务端返回 count 不一致时记录 warn 或 error。
   - 不静默成功。
5. 为 set-level reducer 保持原语义：
   - quorum 仍由 `SetDisks` 控制。
   - 单 disk read_multiple 的 partial failure 不跨 disk 做 quorum。

## 测试方案

单测覆盖：

- msgpack 优先路径成功。
- JSON fallback 成功。
- 单个 msgpack item decode 失败返回错误。
- decode 失败不能被静默丢弃。
- `collect_read_multiple_results` 在 quorum impossible 时快速失败。
- batch response 长度与请求文件数量一致。

建议命令：

```bash
cargo test -p rustfs-ecstore read_multiple -- --nocapture
cargo test -p rustfs-ecstore collect_read_multiple_results -- --nocapture
cargo test -p rustfs --lib handle_read_multiple -- --nocapture
```

## 性能验证

目标 workload：

- 小对象 GET：4KiB、64KiB、1MiB。
- HEAD/object metadata heavy workload。
- 多节点 EC。

指标：

- metadata fanout latency。
- gRPC request count。
- CPU usage。
- allocation pressure。
- GET p95/p99。
- decode error count。

## 验收标准

- 不再有 silent decode drop。
- 服务端消除不必要 JSON roundtrip。
- JSON fallback 仍兼容老路径。
- 小对象高并发 GET p95/p99 不回退，并 ideally 有可测 CPU/latency 收益。
