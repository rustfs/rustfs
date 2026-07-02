# Task 01: fix internode HTTP writer error semantics

## 目标

修复 HTTP data-plane 写流错误可能在 `shutdown()` 阶段被吞掉的问题，确保远端 PUT/append 流的 HTTP status、连接错误、background task error 能准确返回给上层 `RemoteDisk`，并被 retry、fault marking、metrics 正确感知。

## 代码落点

- `crates/rio/src/http_reader.rs`
  - `HttpWriter::new`
  - `AsyncWrite for HttpWriter`
  - `poll_flush`
  - `poll_shutdown`
  - `err_rx`
  - `handle: JoinHandle<std::io::Result<()>>`
- `crates/ecstore/src/cluster/rpc/internode_data_transport.rs`
  - `TcpHttpInternodeDataTransport::open_write`
- `crates/ecstore/src/cluster/rpc/remote_disk.rs`
  - remote write retry/fault marking 相关调用路径

## 当前问题

`HttpWriter` 后台 task 返回类型是 `JoinHandle<std::io::Result<()>>`。当前 shutdown 逻辑如果拿到 `Poll::Ready(Ok(_))` 就视为成功，存在把 `Ok(Err(e))` 当作成功的风险。

风险结果：

- 远端节点返回非 2xx，调用方可能在 shutdown 后仍认为成功。
- connection reset、connection refused、body stream aborted 可能不能稳定传回上层。
- `RemoteDisk` 对失败的 retry/fault marking 被削弱。
- 压测时会出现吞吐看似正常但对象写入错误被延迟或隐藏。

## 实施步骤

1. 在 `poll_shutdown` 中精确区分：
   - `Poll::Ready(Ok(Ok(())))`：成功。
   - `Poll::Ready(Ok(Err(err)))`：返回该 `io::Error`。
   - `Poll::Ready(Err(join_err))`：转换为 `io::ErrorKind::Other`。
2. 在 `poll_flush` 和 `poll_shutdown` 开头优先检查 `err_rx`：
   - 如果后台 task 已经发送错误，立即返回该错误。
   - 避免调用方继续写入已失败的 stream。
3. 明确 `HttpWriter::new()` 的连接语义：
   - 短期：保留异步后台发送，但文档和测试覆盖“错误可能在首次 write/flush/shutdown 返回”。
   - 中期：可评估增加 ready handshake，使 `open_write` 能覆盖更多连接建立失败。
4. 增加 classified error 记录：
   - write shutdown error。
   - background task returned error。
   - body stream aborted。
5. 保持 backpressure：
   - 不扩大 `HTTP_WRITER_CHANNEL_CAPACITY`。
   - 不盲目调大 `HTTP_WRITER_BUFFER_SIZE`。

## 测试方案

新增或扩展 `rustfs-rio` 单测：

- 服务端返回 `500`，`poll_shutdown` 必须返回错误。
- 服务端提前关闭连接，`poll_flush` 或 `poll_shutdown` 必须返回错误。
- 后台 task 返回 `Ok(Err(e))` 时不得吞错。
- 成功 PUT 的 happy path 不受影响。

建议命令：

```bash
cargo test -p rustfs-rio http_writer -- --nocapture
cargo test -p rustfs-rio internode_status_error -- --nocapture
```

## 性能验证

该任务主要是 correctness prerequisite，但仍需验证不会引入明显写入性能回退：

```bash
scripts/run_object_batch_bench_enhanced.sh \
  --tool warp \
  --warp-mode put \
  --sizes 10MiB,64MiB \
  --concurrencies 32,128 \
  --duration 30s \
  --out-dir target/bench/issue-797-task01-http-writer
```

## 验收标准

- 写流错误不再被 shutdown 吞掉。
- 远端非 2xx 能稳定返回到调用方。
- retry/fault marking 路径能看到网络类错误。
- focused tests 通过。
- PUT 基准 p95/p99 不比基线恶化超过 5%。
