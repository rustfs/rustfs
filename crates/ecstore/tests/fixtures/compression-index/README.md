# Compression-index compatibility fixtures

These are synthetic public fixtures, generated with RustFS source and the locked dependencies at `017b85a5bfc3c2c779af942b68e3a7c42b3a9d81`. They contain no captured user object or credentials.

`legacy-lz4-stream.bin` was emitted by `rustfs_rio::CompressReader` using LZ4 and its default 1 MiB block size. The compressor and utility compression source files are byte-identical between this commit and `17aa75a1759e856e93b8c1df2fd447b5ce92e30d`.

`pre-zigzag-index.bin` was emitted by compiling the complete, unmodified `crates/rio/src/compress_index.rs` module from `17aa75a1759e856e93b8c1df2fd447b5ce92e30d`, before the signed-varint fix. The compressor's serializable Index state was deserialized into that historical Index type and passed to its writer. Its historical reader verified complete consumption, identical state and two seek results before the bytes were retained. This pins a historical writer module; it is not a full historical release or a captured stored object.

`zigzag-index.bin` was emitted from the same Index state using the current writer, and serves as an indexed-read control. The old and new index files intentionally differ.

## Reproducing the input

The plaintext length is `3 * 1024 * 1024 + 123`. For each byte offset, let `chunk = offset / 1024` and `lane = offset % 1024`. The first eight bytes of each 1024-byte chunk are `u64::to_le_bytes(chunk)`; the remainder is `b"abcdefghijklmnopqrstuvwxyz"[(offset + chunk) % 26]`.

Drain the legacy LZ4 CompressReader into a byte vector, obtain its Index through TryGetIndex, then serialize that Index state through serde_json. Compile the pinned historical module, deserialize the state into its Index, and call into_vec. Emit the current writer's into_vec result separately. Do not recreate the old index through the reader being tested.

The recorded state has `est_block_uncomp = 0`, total compressed offset `52137`, total uncompressed offset `3145728`, and these `(compressed, uncompressed)` entries:

- `(17085, 1048576)`
- `(34663, 2097152)`
- `(52137, 3145728)`

The final partial block is part of the stream; the recorded totals are the Index state exposed by this historical compressor, not a replacement for the object's actual length.

| Bytes | Length | SHA-256 |
| --- | ---: | --- |
| Plaintext | 3145851 | `68f9edf6f9600395d6d2b5517d97570fecd2a0c6dbe6130617ef415a3b48d89c` |
| Legacy LZ4 stream | 52212 | `cbb8ead867a159a6a089a3a09fd424986cc8995bcb384434d0b95d0d9bdfef7e` |
| Historical index | 47 | `ff4fb74d14a8ddc0d3c31d565c30b62e142df02ac0d19fa270c666b98a8e7171` |
| Current index | 51 | `8a819221f9bdaa2bd05e14eead2833746ea01cb1c68086bffa25a731c252174e` |

Tests exercise the production read planner and GetObjectReader rather than decoding the expected bytes with a replacement implementation. Historical-body and fallback cases apply to both legacy and rio-v2 builds; the exact legacy indexed-offset control is feature-gated because each backend has its own index representation. No throughput or application-level release qualification is inferred from these fixtures.
