# TFTP Server for RustFS

Trivial File Transfer Protocol (TFTP) server implementation for RustFS, as defined in [RFC 1350](https://tools.ietf.org/html/rfc1350).

Each TFTP RRQ (read request) is translated into an S3 `GetObject` call, and each WRQ (write request) is translated into an S3 `PutObject` call against the configured `StorageBackend`.

> **Note:** TFTP has no built-in authentication. Operators must secure the TFTP port at the network layer.

## Architecture

```
┌──────────┐   RRQ/WRQ    ┌──────────────┐   GetObject/PutObject   ┌───────────────┐
│  Client  │ ──────────── │  TftpServer  │ ─────────────────────── │ StorageBackend │
│  (UDP)   │   DATA/ACK   │  (async-tftp)│                          │     (S3)       │
└──────────┘              └──────────────┘                          └───────────────┘
```

- **RRQ (read):** Downloads the entire object from S3 into an in-memory buffer, then streams it to the TFTP client.
- **WRQ (write):** Accumulates incoming bytes into an in-memory buffer; on transfer completion (drop of the writer), uploads the buffer to S3 in a single `PutObject` call.

## Feature Gate

The TFTP module is feature-gated behind the `tftp` feature:

```toml
[dependencies]
rustfs-protocols = { features = ["tftp"] }
```

## Configuration

The server is configured via environment variables:

| Variable | Required | Default | Description |
|---|---|---|---|
| `RUSTFS_TFTP_ENABLE` | Yes | false | Set to true value to enable the TFTP server |
| `RUSTFS_TFTP_ADDRESS` | No | `0.0.0.0:6969` | UDP bind address |
| `RUSTFS_TFTP_DEFAULT_BUCKET` | No | (none) | Default S3 bucket; when set, all paths use this bucket and the path becomes the object key. When not set, paths are resolved as `/<bucket>/<key>` |
| `RUSTFS_TFTP_ACCESS_MODE` | No | `rw` | Access mode: `ro`/`read-only` (read only), `wo`/`write-only` (write only), `rw`/`readwrite` (read-write) |
| `RUSTFS_TFTP_ACCESS_KEY` | No | (admin user) | Access key for S3 authentication. When set, all requests are authorized under that user's IAM policy |

## Path Resolution

### With `RUSTFS_TFTP_DEFAULT_BUCKET` (default bucket mode)

All path components are treated as the S3 object key within the configured bucket:

```
PUT /config/router.cfg     →  s3://<default_bucket>/config/router.cfg
GET /firmware/v2.3.bin     →  s3://<default_bucket>/firmware/v2.3.bin
```

### Without `RUSTFS_TFTP_DEFAULT_BUCKET` (bucket-from-path mode)

The first path component is extracted as the bucket name, and the remainder is the object key:

```
GET /images/logo.png       →  bucket = "images", key = "logo.png"
PUT /backups/db/dump.sql   →  bucket = "backups", key = "db/dump.sql"
```

## Error Codes

All errors are returned as [TFTP error packets (RFC 1350 §5)](https://tools.ietf.org/html/rfc1350#section-5) with code and message.

### `FileNotFound` (code 1)

| Trigger | Description |
|---|---|
| S3 `GetObject` failure | Object does not exist, bucket not found, or storage backend error during RRQ |

### `PermissionDenied` (code 2)

| Trigger | Description |
|---|---|
| Access key rejected | IAM `check_key` returned `is_valid = false` — the configured access key is invalid or revoked |
| Identity missing | IAM returned `is_valid = true` but the user identity is absent (nil identity) |
| Policy denies operation | The IAM policy attached to the authenticated user does not allow `GetObject` (RRQ) or `PutObject` (WRQ) on the target bucket/key |

### `Msg(...)` (code 0 — "Not defined, see error message")

| Trigger | Example message |
|---|---|
| Write-only server, RRQ received | `"TFTP server is write-only"` |
| Read-only server, WRQ received | `"TFTP server is read-only"` |
| Empty path with default bucket | `"path '/' is a empty path;"` |
| Control characters in path | `"Invalid path: control characters are not allowed in TFTP paths"` |
| Internal directory marker in path | `"Invalid path: internal directory marker is not allowed in TFTP paths"` |
| Bucket-only path without default bucket | `"path '/mybucket' has no key after bucket prefix; use /<bucket>/<key> or set RUSTFS_TFTP_BUCKET"` |
| IAM service unavailable | `"Internal authentication service unavailable"` (IAM system not reachable or `check_key` call failed) |
| Object body read failure | `"Failed to read object body"` (S3 stream chunk error during RRQ data transfer) |

> **Note:** WRQ upload failures (e.g., S3 `PutObject` returning an error, or `PutObjectInput::build` failure) are logged server-side but do **not** result in a TFTP error packet, because the client has already disconnected by the time the upload is attempted in `VecWriter::drop`.

## Usage Example

```rust
use rustfs_protocols::tftp::{TftpConfig, TftpServer, TftpAccessMode};
use std::sync::Arc;

let config = TftpConfig {
    bind_addr: "0.0.0.0:6969".parse().unwrap(),
    default_bucket: Some("tftp-bucket".to_string()),
    access_key: "my-access-key".to_string(),
    mode: TftpAccessMode::ReadWrite,
};

let server = TftpServer::new(config, Arc::new(my_storage_backend));
server.start(shutdown_rx).await?;
```

## Security Considerations

TFTP provides **no encryption** and **no authentication** at the protocol level. RustFS compensates with three configurable controls:

- **`RUSTFS_TFTP_ACCESS_KEY`** — binds all TFTP operations to a specific IAM user; the user's policy determines which buckets and actions are allowed.
- **`RUSTFS_TFTP_DEFAULT_BUCKET`** — when set, locks all access to a single bucket, preventing clients from reaching other buckets in the cluster.
- **`RUSTFS_TFTP_ACCESS_MODE`** — restricts the transfer direction to read-only (`ro`), write-only (`wo`), or both (`rw`).

These compose: the most restrictive combination of all three takes effect. Additionally, restrict the TFTP UDP port at the network layer (firewall, VLAN) to trusted clients only.
