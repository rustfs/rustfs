**Use this when:** verifying the provenance or complete-payload behavior of the pinned MinIO server-config fixture.

# Pinned MinIO server configuration

`minio-2025-09-07.json` is the complete, byte-for-byte payload of the
`.minio.sys/config/config.json` object written by a released MinIO process.
It was captured on 2026-10-22, after changing all four heal settings reported
in RustFS issue #8766. No RustFS encoder, hand-written fixture builder, JSON
pretty-printer, field removal, or secret substitution produced these bytes.

| Producer | Pin |
| --- | --- |
| MinIO release | `RELEASE.2025-09-07T16-13-09Z` |
| Reported source commit | `07c3a429bfed433e49018cb0f78a52145d4bedeb` |
| Runtime | `go1.24.6 linux/arm64` |
| Official binary | [minio.linux-arm64.RELEASE.2025-09-07T16-13-09Z](https://github.com/minio/minio/releases/download/RELEASE.2025-09-07T16-13-09Z/minio.linux-arm64.RELEASE.2025-09-07T16-13-09Z) |
| Binary SHA256 | `5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d` |
| Client release | `RELEASE.2025-08-13T08-35-41Z` |
| Official client | [mc.linux-arm64.RELEASE.2025-08-13T08-35-41Z](https://github.com/minio/mc/releases/download/RELEASE.2025-08-13T08-35-41Z/mc.linux-arm64.RELEASE.2025-08-13T08-35-41Z) |
| Client SHA256 | `14c8c9616cfce4636add161304353244e8de383b2e2752c0e9dad01d4c27c12c` |
| Captured object size | 9648 bytes |
| Captured object ETag / MD5 | `ce1d24421fd9c1b4a16bd44064af9404` |
| Fixture SHA256 | `1e201d2eb5373c28aaae3e637e6b81fa67d6e5e984e6ba7de9be29e04d264473` |

The executable hashes matched the official release asset digests before
execution. The process ran in a disposable local Linux Docker container using
four directories in a newly created local Docker volume, with two data and
two parity shards. `MINIO_CI_CD=1` allowed the directories to share the volume's
filesystem. The server listened on a temporary loopback-only host port. This
is a configuration-format capture, not a durability or production-topology
test. Single-drive MinIO does not register the heal subsystem in this release.

The only non-default persisted configuration change was:

```console
mc admin config set fixture heal bitrotscan=2m drive_workers=3 max_io=17 max_sleep=750ms
```

`mc admin config get fixture heal` returned:

```text
heal bitrotscan=2m max_sleep=750ms max_io=17 drive_workers=3
```

The root credentials were disposable environment values, are not in this
corpus, and were checked to be absent from the captured bytes. All persisted
password, secret, token, and API-key fields are empty. No credentials were
redacted from the fixture because none were present.

## Capture and format

The server exited on SIGTERM before copying its four config `xl.meta` files.
The object was inlined: each file contained a 32-byte HighwayHash prefix
followed by its 4824-byte shard. The metadata reported distribution
`[4,1,2,3]`, so disk 2 held systematic data shard 1 and disk 3 held data shard 2.
Concatenating these two payloads, after removing their bitrot prefixes,
recovered the complete object. The recovered payload's MD5 matched the ETag
reported by all four metadata files, and its size matched their object size.

This release wrote the object as **plaintext**. It is a subsystem map with
`_` default-instance KVS arrays, and it retains actual MinIO null defaults:
`cache._`, `crawler._`, and `credentials._`. Those nulls are significant:
deserializing the whole document into RustFS's native `Config` can fail even
though the surrounding structure resembles that format. Tests must retain
the complete fixture and exercise the external decoder fallback; extracting
only the heal section would miss this boundary.

The fixture proves acceptance of this released MinIO producer's persisted
shape and preservation of its four heal settings. It does not prove legacy
encrypted-config interoperability, all MinIO releases, or the application of
MinIO heal settings by RustFS. In particular, `bitrotscan=2m` is a MinIO
two-month setting and is not a seconds-valued RustFS `bitrot_cycle` alias.
