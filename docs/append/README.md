# RustFS Append Upload Guide

RustFS 支持通过追加写（append）方式向同一个对象持续写入新数据。该能力面向日志收集、顺序写入大型文件或需要断点续写的批处理任务，可以避免频繁地拆分对象或管理临时文件。

本指南介绍 append 的语义、HTTP 接口约定、命令行与 SDK 示例，以及排障要点。

## Feature Overview

| 项目 | 说明 |
| --- | --- |
| 启用方式 | 普通 `PUT Object` 请求新增 `x-amz-write-offset-bytes` 头 |
| 偏移（offset） | 表示即将写入的数据在对象中的起始字节位置，必须是非负整数 |
| 初次写入 | 未存在对象时，可使用 `offset=0` 或直接省略该头 |
| 追加写入 | 偏移量必须等于当前对象长度（可通过 `HEAD Object` 获取） |
| 原子性 | RustFS 会校验偏移长度并拒绝不匹配的请求，防止出现空洞或覆盖 |
| 缓存一致性 | 成功写入后会自动触发缓存失效，确保后续 `GET` 能看到最新内容 |

## Request Flow

1. **初始化对象**  
   通过常规 `PUT` 创建对象，或携带 `x-amz-write-offset-bytes: 0`（仅限对象尚不存在时）。

2. **计算下一次 offset**  
   使用 `HEAD Object` 查询 `Content-Length`，该值即下一块数据的偏移。  
   ```bash
   aws s3api head-object \
     --bucket my-bucket \
     --key logs/app.log \
     --endpoint-url http://127.0.0.1:9000 \
     --query 'ContentLength'
   ```

3. **发送 append 请求**  
   将 `x-amz-write-offset-bytes` 设为上一步得到的偏移，`Content-Length` 填写本次追加块的大小，主体携带新增内容。

4. **校验结果**  
   服务器返回 `200 OK` 表示写入成功，可再次 `HEAD` 或 `GET` 验证总长度。

## Command-Line Example

使用支持 SigV4 的 `curl`（7.75+）即可直接演示：

```bash
# 1. 初次写入
curl --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
  --data-binary @chunk-0000.bin \
  http://127.0.0.1:9000/test-bucket/append-demo

# 2. 追加 1 KiB（offset = 1024）
curl --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
  -H "x-amz-write-offset-bytes: 1024" \
  --data-binary @chunk-0001.bin \
  http://127.0.0.1:9000/test-bucket/append-demo
```

若传入 offset 与现有长度不符，RustFS 会返回 `4xx` 错误并保留原内容，可由客户端重新读取长度后重试。

## Rust SDK Example

```rust
use aws_sdk_s3::{Client, types::ByteStream};

async fn append_part(client: &Client, bucket: &str, key: &str, offset: i64, data: Vec<u8>) -> anyhow::Result<()> {
    let mut op = client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(data))
        .customize()
        .await?;

    op.config_mut()
        .insert_header("x-amz-write-offset-bytes", offset.to_string());

    op.send().await?;
    Ok(())
}
```

搭配一次 `head_object()` 获取 `content_length()` 即可完成「计算 offset → 调用 `append_part`」的完整流程。

## Validation & Troubleshooting

- **Wrong offset**：如果 offset 小于 0 或与现有大小不一致，会收到 `InvalidArgument` 错误。重新 `HEAD` 并更新 offset 后再试。
- **对象不存在**：为不存在的对象指定非 0 偏移会被拒绝；首次写入请省略该头或设置成 0。
- **并发竞争**：多个客户端并发写同一个对象时，后到的请求若检测到偏移不一致会失败，从而提醒上层重新读取长度。
- **状态检查**：`GET`/`HEAD` 会立即看到追加后的大小；同时后台已刷新复制/一致性元数据，无需手动清 cache。

## Known Limitations

| 限制 | 说明 |
| --- | --- |
| SSE-S3 / SSE-C 对象 | 当前追加写不支持 `x-amz-server-side-encryption: AES256` 或客户自带密钥，针对这类对象会返回 4xx。 |
| 压缩对象 | 当对象带有 RustFS 内部的压缩元数据（如 `x-rustfs-meta-x-rustfs-internal-compression`）时禁止追加，需改用完整重写。 |
| Header 编码 | `x-amz-write-offset-bytes` 必须是合法 UTF-8 且能解析为 64 位非负整数。 |
| 大块写入 | 单次追加仍需遵循 `PUT Object` 的大小限制（最大 5 GiB）；更大的流量建议通过常规多段上传完成后再 append。 |

## Regression Tests

端到端用例位于 `crates/e2e_test/src/reliant/append.rs`，涵盖基本追加、错误偏移、连续多次追加、压缩/加密限制验证等场景。开发过程中可运行：

```bash
cargo test --package e2e_test --test reliant -- append
```

以快速验证 append 功能与未来修改的兼容性。
