# Issue 662 实施方案：Erasure Write 诊断增强与安全局部重试

本文档给出 RustFS backlog issue `#662` 的可执行实施版方案，聚焦以下两类问题：

- erasure write quorum 失败时，日志与外部错误信息不够直接，不利于快速排障。
- internode `put_file_stream` 写路径对瞬时失败缺少安全边界内的局部自恢复能力。

本文档不是泛泛建议，而是面向直接实施的工程方案，覆盖：

- 按文件的逐项改造清单
- 伪代码接口设计
- 测试用例列表
- 分阶段落地顺序
- 风险边界与验收标准

## 1. 背景与问题定义

Issue 中的实际现象是：

- 某次 `PutObject` 期间，8 个 shard 中有 3 个 shard 写失败。
- 写 quorum 门槛为 `6`，成功 shard 为 `5`，因此 `5 < 6` 导致失败。
- HAProxy 将请求转发到其他节点后，重试成功。

从当前源码看，问题分成两层：

1. 当前 quorum 失败日志没有直接回答：
   - 需要多少副本成功
   - 实际成功多少
   - 失败多少
   - 失败主要属于哪一类
2. 当前 internode HTTP 写失败在传递过程中被压平为字符串，导致无法稳定区分：
   - connect timeout
   - connection refused
   - DNS 解析失败
   - connection reset
   - remote 5xx
   - body 中途异常中断

第三个容易被误解的问题是“为什么不直接给所有 shard 写失败补一次 retry”。原因是当前大对象写路径是流式写入，服务端协议没有 offset/resume 语义，盲目重试并不安全。

## 2. 现状源码结论

### 2.1 quorum 失败摘要不足

当前关键位置：

- `crates/ecstore/src/disk/error_reduce.rs`
- `crates/ecstore/src/erasure_coding/encode.rs`

`MultiWriter::write()` 和 `MultiWriter::shutdown()` 在失败时只输出：

- `offline-disks={}/{}` 
- 原始 `errs={:?}`

缺失：

- `required`
- `achieved`
- `failed`
- `retryable_failures`
- `dominant_error`

### 2.2 internode HTTP 写错误被字符串化

当前关键位置：

- `crates/rio/src/http_reader.rs`

`HttpWriter` 把 `reqwest::Error` 包成：

```text
HTTP request failed: {e}
```

这会丢掉后续做结构化归类所需的错误语义。

### 2.3 当前没有真正的局部 shard 重试

当前关键位置：

- `crates/ecstore/src/erasure_coding/encode.rs`
- `crates/ecstore/src/rpc/remote_disk.rs`
- `crates/ecstore/src/rpc/internode_data_transport.rs`

现状是：

- shard 写入阶段每轮只写一次
- 建流失败直接报错
- 大对象流式写中途失败后，没有 resume 语义

### 2.4 当前协议不支持安全的 mid-stream retry

当前服务端入口：

- `rustfs/src/storage/rpc/http_service.rs`

`put_file_stream` 只是：

- 按顺序接收 body
- 直接写入目标 writer
- 最终 flush

服务端没有返回“当前已经成功提交的 offset”，因此客户端无法安全判断是否可以从某个偏移继续写。

## 3. 设计目标

本次实施目标分为三层。

### 3.1 可观测性目标

一次失败日志要能直接回答：

- quorum 要求多少
- 当前成功多少
- 当前失败多少
- offline disk 有多少
- 失败主要属于哪种类型
- 是否存在可重试的瞬时失败

### 3.2 正确性目标

- 不改变现有 write quorum 语义
- 不引入不安全的重复写
- 不引入半提交状态下的错误重放
- 不把服务端内部路径和 URL 过度暴露给 S3 客户端

### 3.3 性能目标

- 成功热路径不引入明显额外开销
- 失败统计只在失败路径构造
- 不为大对象写路径引入全量 replay buffer
- retry 限制在安全边界内，次数极小

## 4. 非目标

本次方案明确不做以下事情：

- 不在当前通用大对象流式写路径上直接补“中途失败后继续写”的重试
- 不修改 write quorum 判定算法
- 不把所有 internode 失败都自动转为 retry
- 不在同一个 PR 中引入新的复杂写会话协议

## 5. 分阶段落地策略

建议按三个阶段拆分。

### 阶段 A：诊断增强

目标：

- 补齐 quorum 失败摘要
- 外部错误体更明确
- 不改写入语义

交付物：

- 失败摘要 helper
- 结构化日志字段
- 更清晰的 S3 `InternalError` message

### 阶段 B：internode 错误结构化分类

目标：

- 保留 `reqwest` 失败的语义信息
- 让日志和 metrics 能区分网络失败类型

交付物：

- `InternodeHttpError` 内部错误分类
- 错误源链保留
- 统一错误分类 helper

### 阶段 C：安全边界内的局部重试

目标：

- 只在安全可重放的路径上增加一次有限 retry

交付物：

- 建流阶段 retry
- `encode_inline_small()` 路径 retry
- retry metrics

### 阶段 C 补充评估：当前不建议给 `encode_inline_small()` 增加 shard 级 retry

结论：

- 当前代码形态下，不建议直接给 `encode_inline_small()` 增加 shard 级 retry。

原因：

1. `encode_inline_small()` 仅在 `use_fast_path = is_inline_buffer && data.size() <= fi.erasure.block_size` 时使用。
2. `is_inline_buffer = true` 时，`create_bitrot_writer()` 返回的是 `CustomWriter::InlineBuffer`，不是远端 internode writer。
3. 因此这条 fast path 当前吸收的是“内存内联对象的小对象编码写入”，不是 issue 662 关心的 remote shard HTTP 写失败。

源码依据：

- `crates/ecstore/src/set_disk.rs`
- `crates/ecstore/src/bitrot.rs`
- `crates/ecstore/src/erasure_coding/bitrot.rs`
- `crates/ecstore/src/erasure_coding/encode.rs`

这意味着：

- 当前给 `encode_inline_small()` 做 shard 级 retry，几乎没有生产收益。
- 反而会制造一种误导，好像 small fast path 已经具备 remote shard 安全重试语义。

### 如果未来要做 `encode_inline_small()` 的 shard 级 retry，必须满足的边界

只有同时满足以下边界，才适合单独设计：

1. 每个 shard 在该路径内是“单次整 shard 写入”，失败后只能整 shard 全量重放。
2. shard 原始数据仍完整保留在内存中。
3. 只能重试失败 shard，不能重试已成功 shard。
4. writer 重建必须具备“fresh target”语义。
5. 只能对 retryable internode transport error 做一次 retry。
6. `shutdown()` 失败不应在该路径做 writer rebuild retry。

### fresh target 语义要求

如果未来 small fast path 被扩展到 non-inline remote writer，writer 重建至少要满足以下二选一：

1. 先显式删除旧 tmp shard，再重建同路径 writer。
2. 或者直接创建新的带 attempt 后缀的 tmp shard 路径。

否则 reopen 同一路径是否等价于 truncate + rewrite 并不自证安全。

### 当前最合理的策略

当前推荐策略是：

1. 不实现 `encode_inline_small()` shard 级 retry。
2. 将这一结论保留在设计文档中，避免后续误判。
3. 将后续评估重点转向 “small non-inline path 是否存在可独立切分的安全优化点”。

### 评估结论：`small non-inline path` 值得拆出独立性能 fast path

结论：

- `small non-inline path` 不适合做 shard 级 retry。
- 但它值得拆出一个独立的“single-block non-inline fast path”。

原因：

1. 当前 `put_object` 只有在 `is_inline_buffer && data.size() <= fi.erasure.block_size` 时才走 `encode_inline_small()`。
2. 当对象很小但 `is_inline_buffer = false` 时，当前代码仍然走通用 `encode()` 流水线。
3. `put_object_part` 当前也没有 small fast path，小 part 即使只有一个 block，也会走通用 `encode()`。
4. 通用 `encode()` 会引入：
   - `tokio::spawn`
   - `mpsc channel`
   - inflight queue / memory accounting
   - producer / consumer task 协调
5. 对于“单块小对象但非 inline”场景，上述机制不是必须成本。

源码依据：

- `crates/ecstore/src/set_disk.rs`
- `crates/ecstore/src/erasure_coding/encode.rs`
- `crates/ecstore/src/config/storageclass.rs`

### 设计目标：single-block non-inline fast path

该 fast path 是纯性能优化，不承担 retry 目标。

目标：

- 跳过通用 `encode()` 的异步流水线调度成本
- 保持现有 writer、quorum、shutdown、rename、metadata 语义不变
- 对 `put_object` 和 `put_object_part` 都可复用

非目标：

- 不增加 shard retry
- 不重建 writer
- 不改变 `put_file_stream` 协议
- 不改变 tmp object / multipart rename 语义

### 触发条件建议

建议抽出统一判定 helper，例如：

```rust
fn should_use_single_block_non_inline_fast_path(
    is_inline_buffer: bool,
    object_size: i64,
    block_size: usize,
) -> bool
```

建议条件：

- `!is_inline_buffer`
- `object_size > 0`
- `object_size <= block_size as i64`

说明：

- `object_size <= block_size` 表示编码只会生成一个 block。
- 单 block 场景不需要通用 `encode()` 的 producer/consumer 管线。

### 实施落点建议

#### 1. `crates/ecstore/src/erasure_coding/encode.rs`

新增一个与 `encode_inline_small()` 并列的 helper，例如：

```rust
pub async fn encode_single_block_non_inline<R>(
    self: Arc<Self>,
    reader: R,
    writers: &mut [Option<BitrotWriterWrapper>],
    quorum: usize,
) -> std::io::Result<(R, usize)>
where
    R: AsyncRead + Send + Sync + Unpin
```

建议实现：

1. `read_to_end` 读入内存
2. `total == 0` 直接返回
3. `debug_assert!(total <= self.block_size)`
4. `encode_data(&buf)`
5. `MultiWriter::write(shards).await`
6. `MultiWriter::shutdown().await`
7. 返回 `(reader, total)`

该 helper 和 `encode_inline_small()` 的核心区别：

- 目标不是 inline data，而是 non-inline writer
- 不负责 writer rebuild retry
- 只承担“单 block 省掉异步流水线”的性能收益

#### 2. `crates/ecstore/src/set_disk.rs` 的 `put_object`

当前逻辑：

- inline small -> `encode_inline_small()`
- 其他 -> `encode()`

建议改成三路：

1. `inline small` -> `encode_inline_small()`
2. `single-block non-inline small` -> `encode_single_block_non_inline()`
3. 其他 -> `encode()`

建议 helper 变量：

```rust
let use_inline_fast_path = is_inline_buffer && data.size() <= fi.erasure.block_size as i64;
let use_single_block_non_inline_fast_path =
    !is_inline_buffer && data.size() > 0 && data.size() <= fi.erasure.block_size as i64;
```

#### 3. `crates/ecstore/src/set_disk.rs` 的 `put_object_part`

当前永远走：

```rust
Arc::new(erasure).encode(stream, &mut writers, write_quorum).await?
```

建议为 multipart part 复用同一个条件：

- `data.size() > 0`
- `data.size() <= fi.erasure.block_size as i64`

满足时走：

```rust
encode_single_block_non_inline(...)
```

否则保持原路径。

### 为什么这个方案是安全的

该方案不改变以下关键语义：

- writer 仍由现有 `create_bitrot_writer()` 创建
- 每个 shard 仍只写一次
- 仍使用 `MultiWriter::write()` 做 quorum 判定
- 仍使用 `MultiWriter::shutdown()` 关流
- `put_object` 后续 metadata / rename / lock 流程不变
- `put_object_part` 后续 `rename_part()` 流程不变

因此它是“跳过异步流水线调度”的性能优化，不是语义优化。

### 预期收益

预期收益集中在小对象 remote/tmp writer 场景：

- 少一次 `tokio::spawn`
- 少一个 `mpsc` channel
- 少 producer / consumer 线程协作
- 少 inflight block bookkeeping

对超小对象和小 multipart part，理论上会更划算。

### 风险边界

风险主要在两个点：

1. 不要误把它和 retry 绑定。
2. 不要放宽触发条件到多 block 对象。

因此必须坚持：

- 只用于 `object_size <= block_size`
- 不做 writer rebuild
- 不做 retry

### 测试建议

新增测试至少包括：

1. `put_object` 在 `!is_inline_buffer && size <= block_size` 时走 single-block fast path。
2. `put_object_part` 在 `size <= block_size` 时走 single-block fast path。
3. `size > block_size` 时仍走通用 `encode()`。
4. fast path 下 writer 仍会 `shutdown()`。
5. fast path 不改变产出的 shard 数量与写入后 metadata 基本语义。

### 性能验证建议

因为这是性能优化，建议在默认启用前补一个最小基准或对比测试。

建议优先验证：

1. `put_object` 小对象、non-inline、single-block
2. `put_object_part` 小 part、single-block
3. 与当前 `encode()` 路径相比的 wall time / allocation / task overhead

如需 bench，可考虑：

- 在 `crates/ecstore/benches/` 下补一个聚焦 small single-block encode path 的 benchmark
- 或先补一个更轻量的 test-only timing scaffold 做定性验证

## 6. 按文件的逐项改造清单

## 6.1 `crates/ecstore/src/disk/error_reduce.rs`

### 目标

新增 quorum 失败摘要构造能力，但不修改现有 `reduce_write_quorum_errs()` 返回语义。

### 改造项

1. 新增轻量摘要结构：

```rust
pub struct WriteQuorumFailureSummary {
    pub required: usize,
    pub achieved: usize,
    pub failed: usize,
    pub total: usize,
    pub offline_disks: usize,
    pub ignored_failures: usize,
    pub retryable_failures: usize,
    pub dominant_error: Option<Error>,
}
```

2. 新增 helper：

```rust
pub fn build_write_quorum_failure_summary(
    errors: &[Option<Error>],
    ignored_errs: &[Error],
    quorum: usize,
) -> WriteQuorumFailureSummary
```

3. 新增 dominant error 选择逻辑：

- 复用 `reduce_errs()` 的已有归约思路
- 但摘要 helper 不改变当前 `reduce_write_quorum_errs()` 的兼容行为

4. 新增 retryable classification 计数 helper：

```rust
pub fn count_retryable_failures(errors: &[Option<Error>]) -> usize
```

### 实施要求

- 不要把新逻辑嵌进 `reduce_write_quorum_errs()` 本体导致行为漂移
- 新 helper 可以只在失败路径调用

## 6.2 `crates/ecstore/src/disk/error.rs`

### 目标

增强 `DiskError::Io` 承载的上下文表达能力，但不破坏现有枚举结构。

### 改造项

1. 保持 `DiskError::Io(io::Error)` 不变。
2. 为后续 `InternodeHttpError` 的 source chain 保留通路。
3. 新增帮助方法：

```rust
impl DiskError {
    pub fn is_retryable_internode_write_failure(&self) -> bool
}
```

### 实施要求

- 不新增过于宽泛的“所有 Io 都可 retry”判断
- retryable 只针对 internode 写路径确定的瞬时失败分类

## 6.3 `crates/rio/src/http_reader.rs`

### 目标

让 `HttpWriter` 能返回结构化的 internode HTTP 错误，而不是纯字符串。

### 改造项

1. 新增内部错误分类：

```rust
#[derive(Debug, thiserror::Error)]
pub enum InternodeHttpError {
    #[error("internode connect timeout")]
    ConnectTimeout,
    #[error("internode connection refused")]
    ConnectionRefused,
    #[error("internode dns resolution failed")]
    DnsResolutionFailed,
    #[error("internode connection reset")]
    ConnectionReset,
    #[error("internode body stream aborted")]
    BodyStreamAborted,
    #[error("internode http status {0}")]
    HttpStatus(reqwest::StatusCode),
    #[error("internode request failed: {message}")]
    Unknown { message: String },
}
```

2. 新增错误分类 helper：

```rust
fn classify_reqwest_error(err: &reqwest::Error) -> InternodeHttpError
```

3. 新增状态码分类 helper：

```rust
fn classify_http_status(status: reqwest::StatusCode) -> InternodeHttpError
```

4. `HttpWriter::new()` 内部发送请求失败时，不再直接：

```rust
Error::other(format!("HTTP request failed: {e}"))
```

改为：

```rust
let classified = classify_reqwest_error(&e);
let io_err = io::Error::new(io::ErrorKind::Other, classified);
```

5. `non-200 status` 失败也统一分类。

6. 在 `record_internode_error(...)` 之外，增加更细粒度日志字段：

- `operation`
- `classification`
- `url_path`
- `method`

### 实施要求

- 只记录 path，不在默认日志里散落完整敏感 URL query
- 如果确实需要完整 URL，仅放 debug 级别或受控日志中

## 6.4 `crates/ecstore/src/rpc/remote_disk.rs`

### 目标

在建流阶段引入最小重试能力。

### 改造项

1. 抽出统一建流 helper：

```rust
async fn open_write_with_retry(
    &self,
    request: WriteStreamRequest,
) -> Result<FileWriter>
```

2. `append_file()` 和 `create_file()` 都改为走该 helper。

3. helper 行为：

- 第一次直接尝试
- 如果失败且属于 retryable internode write failure，则等待固定退避后重试一次
- 第二次失败直接返回

4. 增加轻量 retry 常量：

```rust
const INTERNODE_WRITE_OPEN_RETRY_MAX_ATTEMPTS: usize = 2;
const INTERNODE_WRITE_OPEN_RETRY_BACKOFF: Duration = Duration::from_millis(20);
```

### 实施要求

- 只重试建流阶段
- 不在这里尝试重放已经开始的 body 数据

## 6.5 `crates/ecstore/src/erasure_coding/encode.rs`

### 目标

统一输出失败摘要，并只在安全路径引入局部 retry。

### 改造项

1. 新增失败摘要格式化 helper：

```rust
fn format_write_quorum_failure(summary: &WriteQuorumFailureSummary) -> String
```

2. `MultiWriter::write()` 失败时：

- 先构造 `WriteQuorumFailureSummary`
- 再记录结构化日志
- 再向上返回带摘要的 `io::Error`

3. `MultiWriter::shutdown()` 失败时同样处理。

4. 对 `encode_inline_small()` 增加安全 retry：

- shard 数据还在内存中
- 某个 writer 本轮写失败且被归类为 retryable 时
- 允许重建该 writer 并对该 shard 整 shard 重放一次

5. 不对通用 `encode()` 流式路径增加 mid-stream retry。

### 实施要求

- `encode()` 热路径不应为 retry 保存额外大块历史数据
- retry 逻辑应只放在 `encode_inline_small()` 或显式受控的 helper 中

## 6.6 `crates/ecstore/src/bitrot.rs`

### 目标

为 small inline retry 提供可重建 writer 的必要边界。

### 改造项

1. 视实现需要，给 `create_bitrot_writer()` 的调用方保留足够的重建信息：

- disk 引用
- volume
- path
- length
- shard_size
- checksum_algo

2. 如果现有 `BitrotWriterWrapper` 无法直接替换失败 writer，则在 `encode_inline_small()` 侧保留 writer rebuild 所需输入。

### 实施要求

- 不引入抽象过度
- 优先在调用点局部存储重建所需参数

## 6.7 `rustfs/src/storage/rpc/http_service.rs`

### 目标

暂不改协议，只补诊断字段与服务端错误日志清晰度。

### 改造项

1. `handle_put_file()` 失败时的日志增加：

- `disk`
- `volume`
- `path`
- `append`
- `size`
- `stage=create|append|write_body|flush`

2. 保持现有 HTTP 接口语义不变。

3. 不在本阶段加入 offset/resume 协议。

### 实施要求

- 服务端日志可详细
- HTTP response body 仍保持简洁，避免暴露过多内部结构

## 6.8 `crates/io-metrics/src/internode_metrics.rs`

### 目标

补充低基数 retry/分类指标。

### 改造项

新增或扩展以下指标记录接口：

```rust
record_write_retry_attempt(classification, backend)
record_write_retry_success(classification, backend)
record_write_failure_classified(classification, backend)
record_erasure_write_quorum_failure(dominant_error)
```

### 实施要求

- label 必须低基数
- 不能把 endpoint、volume、path 作为 metrics label

## 7. 伪代码接口设计

## 7.1 quorum 失败摘要

```rust
pub struct WriteQuorumFailureSummary {
    pub required: usize,
    pub achieved: usize,
    pub failed: usize,
    pub total: usize,
    pub offline_disks: usize,
    pub ignored_failures: usize,
    pub retryable_failures: usize,
    pub dominant_error: Option<Error>,
}

pub fn build_write_quorum_failure_summary(
    errors: &[Option<Error>],
    ignored_errs: &[Error],
    quorum: usize,
) -> WriteQuorumFailureSummary {
    let total = errors.len();
    let achieved = errors.iter().filter(|err| err.is_none()).count();
    let failed = total.saturating_sub(achieved);
    let offline_disks = count_errs(errors, &Error::DiskNotFound);
    let ignored_failures = errors
        .iter()
        .filter_map(|err| err.as_ref())
        .filter(|err| is_ignored_err(ignored_errs, err))
        .count();
    let retryable_failures = count_retryable_failures(errors);
    let (_, dominant_error) = reduce_errs(errors, ignored_errs);

    WriteQuorumFailureSummary {
        required: quorum,
        achieved,
        failed,
        total,
        offline_disks,
        ignored_failures,
        retryable_failures,
        dominant_error,
    }
}
```

## 7.2 reqwest 错误分类

```rust
fn classify_reqwest_error(err: &reqwest::Error) -> InternodeHttpError {
    if err.is_timeout() {
        return InternodeHttpError::ConnectTimeout;
    }

    if err.is_connect() {
        let msg = err.to_string().to_ascii_lowercase();
        if msg.contains("dns") || msg.contains("name or service not known") {
            return InternodeHttpError::DnsResolutionFailed;
        }
        if msg.contains("refused") {
            return InternodeHttpError::ConnectionRefused;
        }
        return InternodeHttpError::Unknown { message: err.to_string() };
    }

    if let Some(status) = err.status() {
        return InternodeHttpError::HttpStatus(status);
    }

    let msg = err.to_string().to_ascii_lowercase();
    if msg.contains("connection reset") || msg.contains("broken pipe") {
        return InternodeHttpError::ConnectionReset;
    }
    if msg.contains("body") || msg.contains("stream") {
        return InternodeHttpError::BodyStreamAborted;
    }

    InternodeHttpError::Unknown {
        message: err.to_string(),
    }
}
```

## 7.3 建流阶段 retry

```rust
async fn open_write_with_retry(
    &self,
    request: WriteStreamRequest,
) -> Result<FileWriter> {
    let mut attempt = 1;

    loop {
        let result = self.data_transport.open_write(request.clone()).await;
        match result {
            Ok(writer) => {
                if attempt > 1 {
                    record_write_retry_success(...);
                }
                return Ok(writer);
            }
            Err(err) => {
                let retryable = err.is_retryable_internode_write_failure();
                if !retryable || attempt >= INTERNODE_WRITE_OPEN_RETRY_MAX_ATTEMPTS {
                    return Err(err);
                }

                record_write_retry_attempt(...);
                tokio::time::sleep(INTERNODE_WRITE_OPEN_RETRY_BACKOFF).await;
                attempt += 1;
            }
        }
    }
}
```

## 7.4 `encode_inline_small()` 的安全 retry

```rust
async fn write_small_shards_with_retry(
    writers: &mut [Option<BitrotWriterWrapper>],
    shards: Vec<Bytes>,
    rebuild_inputs: &[ShardWriterRebuildInput],
    quorum: usize,
) -> io::Result<()> {
    let mut first_pass_errs = vec![None; writers.len()];

    for i in 0..writers.len() {
        if let Some(writer) = writers[i].as_mut() {
            match writer.write(&shards[i]).await {
                Ok(n) if n == shards[i].len() => {}
                Ok(_) => first_pass_errs[i] = Some(Error::ShortWrite),
                Err(e) => first_pass_errs[i] = Some(Error::from(e)),
            }
        } else {
            first_pass_errs[i] = Some(Error::DiskNotFound);
        }
    }

    let success = first_pass_errs.iter().filter(|err| err.is_none()).count();
    if success >= quorum {
        return Ok(());
    }

    for i in 0..writers.len() {
        let Some(err) = first_pass_errs[i].as_ref() else {
            continue;
        };
        if !err.is_retryable_internode_write_failure() {
            continue;
        }

        let rebuilt = rebuild_inputs[i].rebuild().await?;
        writers[i] = Some(rebuilt);
        writers[i]
            .as_mut()
            .expect("rebuilt writer must exist")
            .write(&shards[i])
            .await?;
        first_pass_errs[i] = None;
    }

    let success = first_pass_errs.iter().filter(|err| err.is_none()).count();
    if success >= quorum {
        return Ok(());
    }

    let summary = build_write_quorum_failure_summary(&first_pass_errs, OBJECT_OP_IGNORED_ERRS, quorum);
    Err(io::Error::other(format_write_quorum_failure(&summary)))
}
```

## 8. 测试用例列表

## 8.1 `crates/ecstore/src/disk/error_reduce.rs`

### 单测 1：quorum 摘要基础统计

输入：

- total = 8
- quorum = 6
- errors 中 5 个 `None`
- 3 个 `Some(Io(...))`

断言：

- `required == 6`
- `achieved == 5`
- `failed == 3`
- `total == 8`

### 单测 2：offline disk 统计正确

输入：

- 两个 `DiskNotFound`
- 一个普通 `Io`

断言：

- `offline_disks == 2`

### 单测 3：dominant error 选择正确

输入：

- 两个 `ConnectionReset`
- 一个 `Timeout`

断言：

- `dominant_error == ConnectionReset`

### 单测 4：retryable failure 统计正确

输入：

- `ConnectTimeout`
- `ConnectionReset`
- `DiskNotFound`

断言：

- `retryable_failures == 2`

## 8.2 `crates/rio/src/http_reader.rs`

### 单测 5：503 分类正确

通过本地测试 server 返回 `503`。

断言：

- error classification 为 `HttpStatus(503)`

### 单测 6：连接失败分类正确

连接不存在的本地端口。

断言：

- classification 为 `ConnectionRefused` 或 `ConnectTimeout`

### 单测 7：body 中途异常分类正确

测试 server 在响应或接收阶段中途断开。

断言：

- classification 为 `BodyStreamAborted` 或 `ConnectionReset`

## 8.3 `crates/ecstore/src/rpc/remote_disk.rs`

### 单测 8：建流阶段 retry 一次后成功

模拟：

- 第一次 `open_write` 失败，且为 retryable
- 第二次成功

断言：

- 总尝试次数为 `2`
- 返回成功

### 单测 9：非 retryable 错误不重试

模拟：

- 第一次失败是非 retryable

断言：

- 总尝试次数为 `1`

## 8.4 `crates/ecstore/src/erasure_coding/encode.rs`

### 单测 10：`encode_inline_small()` 单 shard retry 后满足 quorum

模拟：

- 8 个 shard
- quorum = 6
- 初次 3 个失败，其中 1 个 retry 后恢复

断言：

- 最终成功

### 单测 11：`encode_inline_small()` retry 后仍不足 quorum

模拟：

- 8 个 shard
- quorum = 6
- 初次 4 个失败
- retry 仅恢复 1 个，最终成功数仍小于 6

断言：

- 返回包含 `required/achieved/failed` 的失败信息

### 单测 12：通用 `encode()` 路径不做 mid-stream retry

模拟：

- 流式路径中途失败

断言：

- 不触发重建 writer 的 retry 分支

## 8.5 服务端与端到端验证

### 集成验证 1：失败日志内容

手工或自动注入 3 个 shard internode 失败。

断言日志中出现：

- `required=6`
- `achieved=5`
- `failed=3`
- `dominant_error=...`

### 集成验证 2：S3 客户端错误体简洁

断言外部错误体包含：

- `erasure write quorum`
- `required`
- `achieved`

不应包含：

- 完整 internode URL query
- 过多内部路径细节

### 集成验证 3：small inline 一次瞬时失败自恢复

断言：

- 客户端无需重试
- 服务端内部一次 retry 后成功

## 9. 日志与指标规范

## 9.1 建议日志字段

写失败日志字段建议：

- `required`
- `achieved`
- `failed`
- `total`
- `offline_disks`
- `retryable_failures`
- `dominant_error`
- `operation=put_file_stream`

## 9.2 建议指标

- `rustfs_internode_write_failures_total{classification,backend}`
- `rustfs_internode_write_retries_total{classification,backend}`
- `rustfs_internode_write_retry_success_total{classification,backend}`
- `rustfs_erasure_write_quorum_failures_total{dominant_error}`

## 9.3 指标约束

- label 必须低基数
- 不要把 `endpoint`、`path`、`disk uuid` 作为 label

## 10. 风险与控制

## 10.1 最大风险

最大的风险不是“少一次 retry”，而是“不安全地重试了已经部分写入的流”。

因此必须坚持：

- 大对象通用流式写路径本次不做 mid-stream retry
- 只有在数据可安全重放时才允许 retry

## 10.2 兼容性风险

- `DiskError::Io` 仍保留，兼容大部分现有错误处理
- 失败 message 更清晰，但不改变核心错误类型语义

## 10.3 性能风险

- 失败摘要如果在成功路径构造，会给热路径加压
- 必须确保 summary 只在失败时生成

## 11. PR 切分建议

建议至少拆为两个 PR。

### PR1：诊断增强与错误分类

范围：

- `error_reduce.rs`
- `encode.rs`
- `http_reader.rs`
- 少量 metrics/logging 补充

目标：

- 快速交付排障价值
- 风险最低

### PR2：安全边界内 retry

范围：

- `remote_disk.rs`
- `encode_inline_small()` 相关逻辑
- retry metrics

目标：

- 只吸收明确可恢复的瞬时失败

### 后续 RFC/PR：真正的 resumable internode write

范围：

- `WriteStreamRequest`
- `put_file_stream` 协议
- offset / session / resume

目标：

- 支持真正的 mid-stream shard 局部重试

## 12. 验证命令

代码改造完成后，至少执行：

```bash
cargo test -p rustfs-ecstore
cargo test -p rustfs-rio
cargo fmt --all
cargo fmt --all --check
make pre-commit
```

文档-only 变更阶段可以不执行上述代码命令，但真正开始改代码后必须补齐。

## 13. 验收标准

以下条件全部满足，视为 issue 第一阶段完成：

- 写 quorum 失败日志能直接表达 `required/achieved/failed`
- internode HTTP 写失败具备结构化错误分类
- small inline 路径可安全吸收一次瞬时 retryable failure
- 通用大对象流式写路径没有引入不安全重试
- 相关单测、集成测试和质量门禁通过

## 14. 最终建议

推荐执行顺序：

1. 先合入 PR1，快速提升日志与错误诊断质量。
2. 再合入 PR2，只在安全边界内补一次有限 retry。
3. 若后续业务确实需要“大对象 mid-stream 局部重试”，单独走协议升级设计，不在现有流式写路径上硬补。

这是当前 RustFS 代码结构下，最稳、最高性价比、也最符合 correctness 优先原则的落地方案。
