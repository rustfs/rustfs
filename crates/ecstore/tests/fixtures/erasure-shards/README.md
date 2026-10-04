# Pinned external erasure shards

These are backend object files captured from two checksum-verified released
processes, then copied after each process stopped:

| Corpus | Producer | Source commit | Read layout |
| --- | --- | --- | --- |
| `minio/` | MinIO `RELEASE.2025-07-23T15-54-02Z` | `7ced9663e6a791fef9dc6be798ff24cda9c730ac` | GF8 Vandermonde, MinIO HighwayHash key |
| `legacy/` | RustFS `1.0.0-alpha.37` | `371119f7336b74fdbcee37a8fcccb9d91c276f47` | Historical GF16, even-padded shards, `[3,4,2,1]` HighwayHash key |

Each manifest pins the official release URL, archive and executable SHA256,
producer version output, capture geometry, and every `xl.meta` and external
`part.1` file. The MinIO executable was downloaded directly from its official
GitHub release. The historical RustFS executable came from its official release
archive. Both hashes were checked before execution. No current RustFS encoder
generated these shards.

The legacy sample proves reading a historical released RustFS format. It is not
evidence from an independently implemented GF16 codec. The MinIO sample is from
an external producer. Neither sample proves distributed failure tolerance,
power-loss durability, transitioned objects, or reverse migration into a live
MinIO drive set.

## Payload and capture

Both processes used twelve disposable local directories with `EC:6`, producing
six data and six parity shards. The object is the smallest odd size above six
128-KiB inline thresholds: 786433 bytes. Its actual external shard payload is
131073 bytes for MinIO and 131074 bytes for the historical even-padded layout.
All twelve shard file hashes are distinct in each corpus.

`plaintext.bin` is the complete independent oracle. It is a nonperiodic SHA256
counter stream, not an encoder roundtrip:

```python
import hashlib
size = 6 * 128 * 1024 + 1
seed = b"RustFS backlog #2248 E10 pinned fixture v1"
payload = b"".join(
    hashlib.sha256(seed + counter.to_bytes(8, "little")).digest()
    for counter in range((size + 31) // 32)
)[:size]
```

The pinned plaintext SHA256 is
`b5a83332327961a31b65a8731c64559fd04ff8ac9f6124022201dbbdba5ffa01`.
Each producer's S3 GET was compared with the complete oracle before stopping
the process and copying its object tree. Credentials were temporary environment
values and are absent from this corpus.

The capture commands and nonsecret settings are in the manifests. Both servers
bound to a disposable loopback port. MinIO used `MINIO_CI_CD=1` to allow multiple
directories on the host's root filesystem; this is a local format capture, not
a production drive deployment. RustFS used `RUSTFS_CONSOLE_ENABLE=false` and a
temporary log directory.

MinIO exited on SIGTERM. The historical RustFS process did not exit within the
20-second shutdown budget, so it was killed and reaped before copying. That
shutdown is recorded in its manifest; it is not a crash-durability assertion.

To reproduce the upload after starting the pinned producer, use the existing
`crates/rio-v2/tests/minio_fixture_lab/lab.py` S3 client with the temporary
endpoint and credentials: create `erasure-fixtures`, PUT the oracle as
`non-inline.bin`, then GET it and compare every byte. Stop the producer before
copying `backend/disk1` through `backend/disk12`'s object directories. Preserve
their data-directory UUIDs and metadata without re-encoding. A fresh capture
has different UUIDs and timestamps, so replacing this corpus also requires
reviewing and updating all per-file hashes.

## Required test

```bash
cargo nextest run --locked -p rustfs-ecstore \
  --test pinned_erasure_fixtures_test --profile ci --retries 0
```

CI must select and pass all four tests without skips or retries.

The positive tests import the captured files into isolated twelve-drive stores
and use the production `get_object_reader` metadata and codec dispatch. A data
slot's actual part file is removed before the first GET in each recovery case;
no current encoder writes the imported payload. Each schedule has a 30-second
budget. Low-level decode is used only for the opposite-codec and insufficient
quorum negative checks.

The test validates the manifest and every file before decoding. Missing or
modified files fail; no fixture-dependent successful return or ignore flag is
provided. It checks exact full bodies, odd Range boundaries and tails, recovery
after removing a real data shard, bitrot rejection before exposing bytes,
insufficient read quorum, and rejection of an opposite-codec reconstruction.
