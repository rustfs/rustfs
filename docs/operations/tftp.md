# TFTP (Trivial File Transfer Protocol)

RustFS exposes an optional UDP TFTP sidecar for PXE boot file serving and
similar read-mostly workflows. The implementation uses the [`async-tftp`](https://crates.io/crates/async-tftp)
crate and maps transfers to the S3 storage API.

For RRQ/WRQ sequence diagrams, env-knob meaning, and `async-tftp` dependency
options, see [TFTP transfer model](tftp-transfer-model.md).

## Enable

Build with the `tftp` feature and set environment variables at runtime:

```bash
cargo build --release --bin rustfs --features tftp

export RUSTFS_TFTP_ENABLE=true
export RUSTFS_TFTP_ADDRESS=0.0.0.0:6969
export RUSTFS_TFTP_ACCESS_KEY=<service-account-access-key>
export RUSTFS_TFTP_SECRET_KEY=<service-account-secret-key>
export RUSTFS_TFTP_DEFAULT_BUCKET=pxe-boot
export RUSTFS_TFTP_ACCESS_MODE=ro   # ro | wo | rw
```

## Path mapping

| `RUSTFS_TFTP_DEFAULT_BUCKET` | Client RRQ/WRQ filename | Resolved object |
| --- | --- | --- |
| `pxe-boot` | `pxelinux.0` | bucket `pxe-boot`, key `pxelinux.0` |
| unset | `/mybucket/boot/vmlinuz` | bucket `mybucket`, key `boot/vmlinuz` |

Both modes apply the same key validation (control characters and internal
`__XLDIR__` markers are rejected).

## Security

TFTP has **no wire authentication**. Restrict access at the network layer
(PXE VLAN only) and use a dedicated IAM identity with least privilege.
Temporary STS credentials are rejected at startup.

## Transfer semantics

- **RRQ**: streamed ranged `GetObject` reads with bounded memory.
- **WRQ**: streamed multipart upload; **commit happens only on successful
  `close()`** after the full transfer completes. Failed or timed-out WRQs
  abort in-progress multipart uploads and never commit partial objects.
- Concurrent transfers are capped by `RUSTFS_TFTP_MAX_CONCURRENT_TRANSFERS`.
- Per-transfer size is capped by `RUSTFS_TFTP_MAX_TRANSFER_BYTES` and the
  client-advertised `tsize` option when present.
- RRQ/WRQ block size and window size are negotiated with the client; RustFS
  caps them via `RUSTFS_TFTP_MAX_BLOCK_SIZE` and `RUSTFS_TFTP_MAX_WINDOW_SIZE`.

## Environment reference

| Variable | Default | Description |
| --- | --- | --- |
| `RUSTFS_TFTP_ENABLE` | `false` | Master switch |
| `RUSTFS_TFTP_ADDRESS` | `0.0.0.0:6969` | UDP bind address |
| `RUSTFS_TFTP_ACCESS_KEY` | (required) | IAM access key for authorization |
| `RUSTFS_TFTP_SECRET_KEY` | (required) | Secret for the access key |
| `RUSTFS_TFTP_DEFAULT_BUCKET` | unset | Lock all requests to one bucket |
| `RUSTFS_TFTP_ACCESS_MODE` | `ro` | `ro`, `wo`, or `rw` |
| `RUSTFS_TFTP_MAX_BLOCK_SIZE` | `65464` | RFC 2348 block size ceiling |
| `RUSTFS_TFTP_MAX_WINDOW_SIZE` | `65535` | RFC 7440 window size ceiling |
| `RUSTFS_TFTP_MAX_CONCURRENT_TRANSFERS` | `64` | Concurrent RRQ/WRQ limit |
| `RUSTFS_TFTP_MAX_TRANSFER_BYTES` | `268435456` | Per-transfer byte ceiling (256 MiB) |
| `RUSTFS_TFTP_BACKEND_OP_TIMEOUT_SECS` | `60` | S3 call timeout |
| `RUSTFS_TFTP_READ_FETCH_BYTES` | `4194304` | RRQ read-ahead window (4 MiB) |

Port **69** requires `CAP_NET_BIND_SERVICE` or root on Linux; the default
uses **6969** so the sidecar can start without extra capabilities.

## Known limitations (stock `async-tftp` 0.4.2)

These are wire-protocol behaviors in `async-tftp`, not RustFS S3 mapping bugs.
Until upstream fixes are released or the crate is vendored with patches, use:

| Limitation | Practical workaround |
| --- | --- |
| `windowsize > 1` can fail with client `got wrong block` after lossy/late ACK | Use `--option "windowsize 1"`; raise `blksize` (up to 65464) for speed |
| Client TFTP ERROR after OACK may leave the server waiting for ACK#0 | Do not reject OACK; keep `blksize` in `8..=65464` and aligned with `RUSTFS_TFTP_MAX_BLOCK_SIZE` |
| Out-of-range `blksize` may be silently ignored at parse time | Send only RFC-valid block sizes |

Details, local fix branches, and merge-vs-vendor-vs-document options:
[tftp-transfer-model.md](tftp-transfer-model.md#known-async-tftp-issues-and-local-branches).
