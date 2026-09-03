# Snowball Auto-Extract Limits

RustFS accepts MinIO-compatible Snowball auto-extract uploads. Archive
members are streamed into objects while RustFS enforces entry-count, path,
PAX metadata, per-object, cumulative unpacked-size, and decoded-stream
limits.

## Size limits

The defaults remain compatible with the existing safety policy:

| Environment variable | Default | Hard maximum | Meaning |
| --- | ---: | ---: | --- |
| `RUSTFS_SNOWBALL_MAX_ENTRY_BYTES` | 1 GiB | 1 TiB | Maximum unpacked size of one archive member |
| `RUSTFS_SNOWBALL_MAX_UNPACKED_BYTES` | 10 GiB | 10 TiB | Maximum cumulative unpacked object bytes in one request |

Invalid values use the default. Zero is treated as one byte, values above the
hard maximum are clamped, and the per-entry limit is never allowed to exceed
the cumulative request limit. RustFS derives a separate decoded-stream limit
with bounded room for tar headers and PAX metadata; it cannot be disabled.

Increasing either limit raises the maximum work performed by one admitted
request. Snowball archive decoder admission remains globally bounded, so a
larger archive cannot create an unbounded number of concurrent decoders.
Restart RustFS after changing these environment variables.

## Small-member concurrency

For requests that set Snowball ignore-errors and do not use bucket quota
accounting, RustFS stages members up to 128 KiB and commits at most 16 at a
time. Requests that must stop on the first write error and quota-enabled
requests remain serial so their observable error and accounting behavior does
not change.

Set `RUSTFS_SNOWBALL_EXTRACT_MAX_INFLIGHT=1` to restore fully serial member
commits. Values are clamped to the range 1 through 16.
