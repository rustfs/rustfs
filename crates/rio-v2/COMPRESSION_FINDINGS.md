# rio-v2 S2 Compression Findings

Date: 2026-05-20

## Summary

rio-v2 的压缩读写链路在接入 RustFS 服务后出现过 `S2 CRC mismatch`。这次排查确认了一个重要结论：

`minlz 0.1.3` 的读端和 CRC 工具目前可用，但写端不能直接承担全部 S2/Snappy block 生成工作。

当前 rio-v2 的妥协方案是：

- S2 frame/header/index 由 rio-v2 自己维护。
- chunk checksum 使用 `minlz::crc::crc`。
- chunk decode 使用 `minlz::decode`，以兼容 MinIO/klauspost S2 payload。
- chunk encode 使用 `snap::raw::Encoder` 生成 Snappy-compatible raw block。
- 对外 metadata 仍保持 `klauspost/compress/s2`，落盘格式仍是 S2 framed stream。

这个方案不是最理想的一库到底，但它清楚地隔离了风险：读端兼容 MinIO，写端避免 `minlz` encoder 当前发现的问题。

## Observed Failure

服务路径上传对象后，GET 返回 body stream error，日志中出现：

```text
S2 CRC mismatch: expected=09fb768c actual=e400f1b2 compressed=true payload_len=622797 decompressed_len=1048576
```

重要现象：

- 失败发生在第一个 1MiB S2 chunk。
- chunk header 中保存的 checksum 与原始 1MiB plaintext 对得上。
- 但 compressed payload 解出来的 plaintext 不等于原始数据。
- 使用 Go `github.com/klauspost/compress/s2` 解同一段 raw stream 也失败。
- 忽略 CRC 强行解压后，输出从很早的位置开始与原文件不同。

这说明问题不是 rio-v2 读端 CRC 计算错误，也不是 ECStore 读路径重建错误，而是写入时生成了不正确的 compressed payload。

## Reproduction Scope

为了缩小范围，临时加入了 `tmp/simple_io`：

- 只启动 ECStore。
- 使用 4 个本地目录模拟 erasure set。
- 执行 put/get/range get。
- 支持导出 raw compressed stream。
- 支持 multipart 和 stream source。

同时加入了 `tmp/klauspost_s2_probe`：

- 使用 Go klauspost/s2 验证 RustFS 写出的 raw S2 stream 是否可被参考实现解码。

这些工具确认：

- rio-v2 读端可以读取 Go klauspost 生成的 S2 stream。
- RustFS 服务路径曾写出 Go klauspost 也无法解码的 S2 stream。
- 使用 `snap::raw::Encoder` 后，Go klauspost 可以解码 raw stream，sha256 与原文件一致。

## Why Not Use Only minlz

`minlz` 看起来提供了完整能力：

- `minlz::crc::crc`
- `minlz::decode`
- `minlz::encode`
- `minlz::encode_snappy`

但本次实测发现两个写端风险。

### `minlz::encode`

`minlz::encode` 生成的是 S2 extension block。服务路径中使用它时，曾产生这样的坏块：

- checksum 是根据原始 plaintext 计算的。
- payload decode 后得到的 plaintext 却不等于原始 plaintext。
- Go klauspost/s2 解同一 payload 也报 CRC mismatch。

所以它不是当前 rio-v2 写路径可以信任的 encoder。

### `minlz::encode_snappy`

`minlz::encode_snappy` 文档上更接近我们想要的 Snappy-compatible raw block。它在小样本测试中可以被 `minlz::decode` 解回原文。

但把 rio-v2 写端切换到 `minlz::encode_snappy` 后，`cargo test -p rustfs-rio-v2` 在现有测试中发生 stack overflow 并 abort：

```text
thread 'compress_reader::tests::s2_compress_reader_skips_index_for_small_streams' has overflowed its stack
fatal runtime error: stack overflow, aborting
```

因此它目前也不能直接用于 rio-v2 的生产写路径。

## Current Compromise

当前实现使用 `snap::raw::Encoder` 只生成 S2 compressed data chunk 的 raw Snappy payload：

```rust
fn encode_block(uncompressed: &[u8]) -> io::Result<Vec<u8>> {
    SnappyEncoder::new()
        .compress_vec(uncompressed)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))
}
```

rio-v2 仍然负责 S2 frame：

- stream identifier: `S2sTwO`
- chunk type: compressed or uncompressed
- 24-bit chunk length
- masked CRC32C checksum
- seekable index metadata

这样可以保持落盘格式兼容，同时避开当前不可靠的 `minlz` 写端 encoder。

## Validation

使用同一个测试对象：

```text
tmp/mc.darwin-arm64.RELEASE.2025-08-13T08-35-41Z
actual_size = 29692306
sha256      = a877fd0c183409da9f20f9d6e1811987298bbbca1aa03428eebdffba79fb9445
```

验证结果：

```text
compression-size = 15319337
actual-size      = 29692306
full read match  = true
range read match = true
```

Go klauspost/s2 验证：

```text
decoded sha256 = a877fd0c183409da9f20f9d6e1811987298bbbca1aa03428eebdffba79fb9445
```

相关命令：

```bash
cargo test -p rustfs-rio-v2
cargo check -p rustfs --features rio-v2
cargo fmt --all --check
```

都已通过。

## Compatibility Notes

这次改动只在 `crates/rio-v2` 内处理，不修改 `crates/rio`。

原因：

- `crates/rio` 是现在线上使用的实现。
- rio-v2 是实验性迁移路径。
- 主线合并时必须保证老用户已有文件格式仍可正常读取。

读路径仍需要兼容：

- RustFS legacy metadata。
- MinIO/klauspost S2 framed streams。
- rio-v2 新写出的 S2 framed streams。

## Future Work

后续更理想的方向有两个：

1. 修复或替换 `minlz` 写端。

   需要定位：

   - `minlz::encode` 为什么会生成 payload 与 checksum 对不上的 block。
   - `minlz::encode_snappy` 为什么会在 rio-v2 测试中 stack overflow。

2. 把 S2 block encode/decode/crc 收敛到同一个可靠实现。

   收敛前必须保留这些验证：

   - rio-v2 单测 roundtrip。
   - multipart stream-source put/get。
   - range read，尤其跨 part/range 边界。
   - Go klauspost/s2 解码 raw stream。
   - MinIO fixture decode。

在这些验证通过之前，不建议把写路径重新切回 `minlz::encode` 或 `minlz::encode_snappy`。
