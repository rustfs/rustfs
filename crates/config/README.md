[![RustFS](https://rustfs.com/images/rustfs-github.png)](https://rustfs.com)

# RustFS Config - Configuration Management

<p align="center">
  <strong>Configuration management and validation module for RustFS distributed object storage</strong>
</p>

<p align="center">
  <a href="https://github.com/rustfs/rustfs/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/rustfs/rustfs/actions/workflows/ci.yml/badge.svg" /></a>
  <a href="https://docs.rustfs.com/">📖 Documentation</a>
  · <a href="https://github.com/rustfs/rustfs/issues">🐛 Bug Reports</a>
  · <a href="https://github.com/rustfs/rustfs/discussions">💬 Discussions</a>
</p>

---

## 📖 Overview

**RustFS Config** provides configuration management and validation capabilities for the [RustFS](https://rustfs.com) distributed object storage system. For the complete RustFS experience, please visit the [main RustFS repository](https://github.com/rustfs/rustfs).

## ✨ Features

- Multi-format configuration support (TOML, YAML, JSON, ENV)
- Environment variable integration and override
- Configuration validation and type safety
- Hot-reload capabilities for dynamic updates
- Default value management and fallbacks
- Secure credential handling and encryption

## 📚 Documentation

For comprehensive documentation, examples, and usage guides, please visit the main [RustFS repository](https://github.com/rustfs/rustfs).

## Environment Variable Naming Conventions

RustFS uses a flat naming style for top-level configuration: environment variables are `RUSTFS_*` without nested module segments.

Examples:
- `RUSTFS_REGION`
- `RUSTFS_ADDRESS`
- `RUSTFS_VOLUMES`
- `RUSTFS_LICENSE`
- `RUSTFS_LICENSE_PUBLIC_KEY`

Current guidance:
- Prefer module-specific names only when they are not top-level product configuration.
- Renamed variables must keep backward-compatible aliases until before beta.
- Alias usage must emit deprecation warnings and be treated as transitional only.
- Deprecated example:
  - `RUSTFS_ENABLE_SCANNER` -> `RUSTFS_SCANNER_ENABLED`
  - `RUSTFS_ENABLE_HEAL` -> `RUSTFS_HEAL_ENABLED`
  - `RUSTFS_DATA_SCANNER_START_DELAY_SECS` -> `RUSTFS_SCANNER_START_DELAY_SECS`

## License environment variables

- `RUSTFS_LICENSE` contains the signed license token.
- `RUSTFS_LICENSE_PUBLIC_KEY` contains the RSA public key used to verify signed license tokens.

## CORS environment variables

- `RUSTFS_CORS_ALLOWED_ORIGINS` defaults to empty, so the S3 endpoint emits no generic CORS headers unless configured. Set `*` for wildcard origins without credentials, or a comma-separated allow-list for credentialed explicit origins.
- `RUSTFS_CONSOLE_CORS_ALLOWED_ORIGINS` defaults to `*` for the console service.

## Browser redirect environment variables

- `RUSTFS_BROWSER_REDIRECT_URL` sets the externally reachable browser origin used for OIDC callback, console success redirect, and logout fallback URLs. Configure it to the public scheme and authority without a path, for example `https://console.example.com`. In load-balancer deployments, keep OIDC authorize and callback requests on the same backend node because the in-flight OIDC `state` is local to the RustFS node.

## Distributed endpoint locality

- `RUSTFS_LOCAL_ENDPOINT_HOST` identifies this server's host in a distributed `RUSTFS_VOLUMES` topology without resolving every peer during startup. Set it to exactly one host, without a scheme, port, or path. It is accepted only for orchestrated URL topologies and must match at least one endpoint on the RustFS server port; invalid or unmatched values fail startup. Leave it unset to retain DNS-based locality discovery.

## Scanner environment aliases

- `RUSTFS_SCANNER_SPEED` (canonical, also accepts `MINIO_SCANNER_SPEED`)
- `RUSTFS_SCANNER_DELAY` (canonical)
- `RUSTFS_SCANNER_MAX_WAIT_SECS` (canonical)
- `RUSTFS_SCANNER_CYCLE` (canonical, also accepts `MINIO_SCANNER_CYCLE`)
- `RUSTFS_SCANNER_START_DELAY_SECS` (canonical)
- `RUSTFS_DATA_SCANNER_START_DELAY_SECS` (deprecated alias for compatibility)
- `RUSTFS_SCANNER_IDLE_MODE` (canonical)
- `RUSTFS_SCANNER_CACHE_SAVE_TIMEOUT_SECS` (canonical)
- `RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS` (canonical)
- `RUSTFS_SCANNER_CYCLE_MAX_OBJECTS` (canonical)
- `RUSTFS_SCANNER_CYCLE_MAX_DIRECTORIES` (canonical)

Scanner cycle budget controls:

- When `RUSTFS_SCANNER_CYCLE_MAX_DURATION_SECS` is unset, the finite default is 1800 seconds (30 minutes), matching the scanner benchmark guidance.
- An explicit `0` preserves the compatibility behavior of an unbounded runtime budget. Object and directory budgets likewise remain unbounded when explicitly set to `0`.
- A timed-out cycle cancels cooperative scanner work, then fences its leader epoch before releasing the lease. An uncooperative I/O operation is dropped after the bounded shutdown window; its cursor is not claimed to be durable and the scanner reports `recovery-required` when the worker cannot stop cooperatively, the cycle state was not confirmed durable, or epoch fencing cannot be persisted.

## Mmap read environment aliases

- `RUSTFS_OBJECT_MMAP_READ_ENABLE` (canonical)
- `RUSTFS_OBJECT_ZERO_COPY_ENABLE` (deprecated alias for compatibility)

## Health compatibility switches

- `RUSTFS_HEALTH_ENDPOINT_ENABLE`
  - controls canonical `/health`, `/health/live`, and `/health/ready` endpoint exposure.
- `RUSTFS_HEALTH_MINIMAL_RESPONSE_ENABLE`
  - enables minimal payload mode for GET health responses (`status`, `ready` only).
- `RUSTFS_HEALTH_READINESS_CACHE_TTL_MS`
  - TTL for readiness cache evaluation.
- `RUSTFS_HEALTH_OBJECT_PROGRESS_ENABLE`
  - withdraws readiness when bounded object read/write stages stop completing while requests remain active.
  - default is `true`.
- `RUSTFS_HEALTH_OBJECT_PROGRESS_TIMEOUT_MS`
  - maximum time without completion in a bounded object stage before readiness is withdrawn.
  - default is `30000`; `0` uses the default.
  - the effective value is at least 5 seconds longer than `RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT`.
  - this readiness SLO is independent of disk read/write failure deadlines and may withdraw traffic before those deadlines expire.
- `RUSTFS_HEALTH_COMPAT_BUSY_CHECK_ENABLE`
  - enables busy protection behavior for health probes.
  - default is `false`.
- `RUSTFS_HEALTH_COMPAT_BUSY_MAX_ACTIVE_REQUESTS`
  - max active HTTP requests; health probes return `429` when active requests reach or exceed this value.
  - `0` disables thresholding even if busy protection is enabled.
- `RUSTFS_HEALTH_COMPAT_KMS_READY_CHECK_ENABLE`
  - enables KMS readiness enforcement for `/health/ready`.
  - default is `false`.

## Object lock admission environment variables

- `RUSTFS_PUT_COMMIT_NAMESPACE_LOCK_ACQUIRE_TIMEOUT_MS`
  - experimental same-object PUT commit namespace-lock admission budget.
  - default is `0`, which disables this override and keeps `RUSTFS_OBJECT_LOCK_ACQUIRE_TIMEOUT` behavior.
  - when set, only `put_object_commit` write-lock acquisition is bounded by this millisecond budget; other namespace lock users keep the global object-lock timeout.
  - timeout returns S3 `SlowDown`, so clients should use normal SDK retry handling.
  - this is not a fdatasync or group-commit switch. Track fdatasync batching separately with `rustfs_s3_put_object_rename_fdatasync_batch_files`.

## Drive timeout environment variables

- `RUSTFS_DRIVE_METADATA_TIMEOUT_SECS`
- `RUSTFS_DRIVE_DISK_INFO_TIMEOUT_SECS`
- `RUSTFS_DRIVE_LIST_DIR_TIMEOUT_SECS`
- `RUSTFS_DRIVE_WALKDIR_TIMEOUT_SECS`
- `RUSTFS_DRIVE_WALKDIR_STALL_TIMEOUT_SECS`

Legacy compatibility fallback:
- `RUSTFS_DRIVE_MAX_TIMEOUT_DURATION`
  This legacy variable is treated as a deprecated fallback for the operation-specific drive timeout variables above when a canonical variable is unset.

Drive timeout health-action policy:
- `RUSTFS_DRIVE_TIMEOUT_HEALTH_ACTION`
  - `mark_failure` (default): timeout marks failure and may transition drive runtime state.
  - `ignore_scanner`: timeout does not mark failure for scanner-sensitive operations (`walk_dir`, `read_metadata`, `list_dir`, `disk_info`).

Drive timeout profile preset:
- `RUSTFS_DRIVE_TIMEOUT_PROFILE`
  - `default` (default): keep current timeout defaults.
  - `high_latency`: use 60s default timeout for scanner-sensitive operations when no per-operation timeout override is set (`read_metadata`, `disk_info`, `list_dir`, `walk_dir`, `walk_dir_stall`).
- Precedence:
  - Explicit per-operation timeout env (`RUSTFS_DRIVE_*_TIMEOUT_SECS`) takes highest precedence.
  - Then `RUSTFS_DRIVE_MAX_TIMEOUT_DURATION` legacy fallback.
  - Then the profile-derived default (`default` or `high_latency`).

## Startup filesystem boundary policy

- `RUSTFS_UNSUPPORTED_FS_POLICY` controls startup behavior when RustFS detects local endpoint filesystems that are outside the supported production boundary.
  - `warn` (default): log warning and continue startup.
  - `fail`: abort startup with an error.

RustFS production guidance remains direct-attached local POSIX filesystems. Network-mounted filesystems (for example `nfs`, `cifs`, `smb2`, and `fuse.*`) are treated as unsupported by this startup guard.

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](../../LICENSE) file for details.
