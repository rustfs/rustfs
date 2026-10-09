# Dynamic block size HTTP regression

**Use this when:** validating S3 block hints, multipart layout persistence, or
reads after disabling the bounded-frame write rollout.

`scripts/dynamic_block_size_e2e.py` checks S3 PUT and multipart initiation hints
against a disposable RustFS endpoint. Install `boto3` in a test virtualenv and
export `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` for that endpoint. The SDK
performs SigV4 signing; the helper bypasses proxies and disables retries.

Start a freshly built RustFS binary with four isolated local volumes, compression
disabled, and these process environment variables:

```bash
RUSTFS_EC_BLOCK_SIZE_HINT_ENABLE=true
RUSTFS_EC_BLOCK_SIZE_HINT_FLEET_CONFIRMED=true
RUSTFS_EC_READ_QUANTUM_ENABLE=true
RUSTFS_EC_READ_QUANTUM_FLEET_CONFIRMED=true
```

For four test directories on the same physical disk, the existing local-test
configuration `RUSTFS_UNSAFE_BYPASS_DISK_CHECK=true` is also necessary. Use it
only for this disposable functional test; it does not validate disk independence.

Four volumes use two data shards and two parity shards. This geometry supports
the bounded bitrot read quantum; a geometry with six or twelve data shards does
not currently select a smaller quantum for the allowed block sizes.

```bash
python scripts/dynamic_block_size_e2e.py --endpoint http://127.0.0.1:19180 \
  --bucket dynamic-block-restart-test --keep
```

The helper writes non-inline deterministic payloads using 64 KiB, 256 KiB, 1 MiB,
and 4 MiB hints, plus default and unsupported-size probes. It checks actual B
and hint status/reason in write responses, then validates HEAD, full GET, and
Ranges spanning EC block and multipart boundaries. A malformed enabled hint
must return `InvalidArgument`. JSON output contains no credentials. Use a fresh
bucket for write runs; existing probe objects are never overwritten.

Stop only that test server, restart the same binary with the same volumes and
all four gates false, then verify persisted layouts without rewriting objects:

```bash
python scripts/dynamic_block_size_e2e.py --endpoint http://127.0.0.1:19180 \
  --bucket dynamic-block-restart-test --verify-only
```

Separately run against a server with block hint enable false using
`--write-policy disabled`, or enable true and fleet confirmation false using
`--write-policy fleet-unconfirmed`. Both must report ignored hints and actual
B of 1 MiB. Without `--keep`, ordinary write runs remove only the objects they
created and their disposable bucket. Verify-only runs never delete data.

B is exposed by `x-rustfs-effective-ec-block-size`. The internal q marker is
deliberately filtered from public metadata. For isolated local volumes, decode
an object's `xl.meta` using the existing filemeta example:

```bash
cargo run -p rustfs-filemeta --example dump_fileinfo -- /path/to/object/xl.meta
```

Confirm both `x-rustfs-internal-ec-read-quantum` and
`x-minio-internal-ec-read-quantum` agree. With two data shards, non-inline 4 MiB
objects use q=1048576; 64 KiB and 256 KiB use q=16384 and q=65536 respectively.
The 1 MiB layout keeps the historical full shard frame and has no q marker.

For a cross-repository probe, write one task-prefixed object with BrewFS's
`object-put-bench`, then check its actual layout and Range bytes:

```bash
python scripts/dynamic_block_size_e2e.py --endpoint http://127.0.0.1:19180 \
  --bucket brewfs-task-bucket --external-prefix dynamic-block-task/ \
  --external-block-size 4194304 --external-size 6291713 \
  --external-sha256 "$EXPECTED_SHA256"
```

External verification reads only the selected prefix and never deletes it. The
optional size and SHA-256 checks validate the intended payload before comparing
Range bytes to full GET bytes. SHA-256 must be exactly 64 hexadecimal characters.
