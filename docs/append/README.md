# RustFS Append Upload Guide

RustFS supports appending new data to the same object via append writes. This is designed for log ingestion, sequentially writing large files, or batch jobs that need resumable uploads, avoiding frequent object splits or temporary files.

This guide covers append semantics, HTTP contracts, CLI and SDK examples, and troubleshooting tips.

## Feature Overview

| Item | Description |
| --- | --- |
| Enablement | Add the `x-amz-write-offset-bytes` header to a normal `PUT Object` request |
| Offset | The starting byte position of the incoming data in the object; must be a non-negative integer |
| First write | When the object does not exist, use `offset=0` or omit the header |
| Append write | Offset must equal the current object length (obtainable via `HEAD Object`) |
| Atomicity | RustFS validates the offset and rejects mismatches to prevent holes or overwrites |
| Cache consistency | A successful write triggers cache invalidation so subsequent `GET` sees the latest content |

## Request Flow

1. **Initialize the object**  
   Create it with a regular `PUT`, or include `x-amz-write-offset-bytes: 0` (only if the object does not exist).

2. **Compute the next offset**  
   Use `HEAD Object` to read `Content-Length`, which is the next chunk’s offset.  
   ```bash
   aws s3api head-object \
     --bucket my-bucket \
     --key logs/app.log \
     --endpoint-url http://127.0.0.1:9000 \
     --query 'ContentLength'
   ```

3. **Send the append request**  
   Set `x-amz-write-offset-bytes` to the offset from step 2, set `Content-Length` to the size of this append chunk, and send the new content in the body.

4. **Verify the result**  
   A `200 OK` means success; you can `HEAD` or `GET` again to confirm the total length.

## Command-Line Example

You can demo the flow directly with SigV4-capable `curl` (7.75+):

```bash
# 1. First write
curl --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
  --data-binary @chunk-0000.bin \
  http://127.0.0.1:9000/test-bucket/append-demo

# 2. Append 1 KiB (offset = 1024)
curl --aws-sigv4 "aws:amz:us-east-1:s3" \
  --user "$AWS_ACCESS_KEY_ID:$AWS_SECRET_ACCESS_KEY" \
  -H "x-amz-write-offset-bytes: 1024" \
  --data-binary @chunk-0001.bin \
  http://127.0.0.1:9000/test-bucket/append-demo
```

If the provided offset differs from the existing length, RustFS returns a `4xx` error and preserves the original content; the client can re-read the length and retry.

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

Pair it with a `head_object()` call to get `content_length()` for the full “compute offset → call `append_part`” loop.

## Validation & Troubleshooting

- **Wrong offset**: If the offset is negative or differs from the current size, you will get an `InvalidArgument` error. Re-run `HEAD` to update the offset and retry.
- **Object missing**: A non-zero offset for a non-existent object is rejected; for the first write, omit the header or set it to 0.
- **Concurrent writers**: When multiple clients append to the same object, later requests with mismatched offsets fail, signaling the caller to re-fetch the length.
- **State checks**: `GET`/`HEAD` immediately reflect the appended size; replication/consistency metadata is already refreshed, so manual cache clearing is unnecessary.

## Known Limitations

| Limitation | Description |
| --- | --- |
| SSE-S3 / SSE-C objects | Append is not supported with `x-amz-server-side-encryption: AES256` or customer-managed keys; such requests return 4xx. |
| Compressed objects | Objects carrying RustFS internal compression metadata (for example `x-rustfs-meta-x-rustfs-internal-compression`) cannot be appended; rewrite the object instead. |
| Header encoding | `x-amz-write-offset-bytes` must be valid UTF-8 and parseable as a 64-bit non-negative integer. |
| Large chunks | A single append still follows `PUT Object` limits (max 5 GiB); use multipart upload first for larger payloads, then append. |

## Regression Tests

End-to-end cases live in `crates/e2e_test/src/reliant/append.rs`, covering basic append, wrong offset, repeated appends, and compression/encryption constraints. During development you can run:

```bash
cargo test --package e2e_test --test reliant -- append
```

to quickly validate append behavior and guard against future regressions.
