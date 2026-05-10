# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Fixed
- **Helm Ingress**: `customAnnotations` are now merged with class-specific annotations (nginx/traefik) instead of being ignored when `ingress.className` is set.

### Added
- **OpenStack Keystone Authentication Integration**: Full support for OpenStack Keystone authentication via X-Auth-Token headers
  - Tower-based middleware (`KeystoneAuthLayer`) self-contained within `rustfs-keystone` crate
  - Task-local storage for async-safe credential passing between middleware and auth handlers
  - Automatic detection of Keystone credentials (access keys prefixed with `keystone:`)
  - Role-based permission mapping (admin/reseller_admin roles grant owner permissions)
  - Token caching for high-performance validation with configurable cache size and TTL
  - Dual authentication support: Keystone and standard AWS Signature v4 work simultaneously
  - Immediate 401 response for invalid tokens (no fallback to local auth)
  - XML-formatted error responses compatible with S3 API
  - Comprehensive integration documentation with manual testing guide
  - **32 unit and integration tests** covering middleware, auth handlers, task-local storage, and role detection
- **SFTPv3 Protocol Support**: SSH-hosted SFTPv3 subsystem that translates each file operation into S3 calls against the local object store. Authentication uses IAM credentials (SSH username = access key, SSH password = secret key).
  - Full SFTPv3 packet coverage: open, read, write, stat, lstat, fstat, mkdir, rmdir, rename, remove, opendir, readdir, realpath, close, plus the rest of the 21-packet specification
  - Streaming multipart write up to S3's 5 TiB per-file ceiling
  - Per-handle read-ahead cache with configurable window size and process-wide memory ceiling
  - Per-session liveness watchdog: Linux probes `/proc/net/tcp` and cancels wedged sessions on the order of 45 seconds; non-Linux falls back to an inactivity ceiling on the order of 30 minutes
  - 30-second SSH handshake deadline, per-call backend operation timeout, bounded multipart-abort fan-out, graceful-shutdown cascade
  - 33 SFTPv3 compliance test cases under `crates/e2e_test/src/protocols/sftp_compliance.rs` spread across three entry points: `test_sftp_compliance_suite` (shared session), `test_sftp_compliance_readonly` (read-only mode), and `test_sftp_compliance_standalone` (one rustfs spawn per case)
  - Four-layer regression-prevention tests guard against silent feature deletion: compile-time module assertion, module-presence unit test, cross-module `Protocol` enum assertion, end-to-end SSH banner test against the running binary

### Changed
- **HTTP Server Stack**: Integrated `KeystoneAuthLayer` middleware from `rustfs-keystone` crate into service stack (positioned after ReadinessGateLayer)
- **IAMAuth**: Enhanced `get_secret_key()` to return empty secret for Keystone credentials (bypasses signature validation)
- **Auth Module**: Modified `check_key_valid()` to retrieve Keystone credentials from task-local storage and determine admin status
- **`StorageBackend` trait**: extended with multipart upload methods (`create_multipart_upload`, `upload_part`, `complete_multipart_upload`, `abort_multipart_upload`) plus `upload_part_copy`. Streaming-upload code path is now available to FTPS, WebDAV, and Swift drivers as well.
- **`Protocol` enum**: new `Protocol::Sftp` variant with corresponding `S3Action` mappings. Every match arm on `Protocol` updated to handle the new variant exhaustively.

### Technical Details
- Middleware is self-contained in `rustfs-keystone` crate following the trusted-proxies pattern for integration-specific middleware
- Uses `BoxBody` pattern for Hyper 1.x compatibility
- Task-local storage provides request-scoped credential passing without modifying HTTP request/response types
- Integration preserves existing S3 authentication flow while adding Keystone support
- Zero breaking changes to existing functionality
- No new top-level directories in main binary crate (middleware lives in integration crate)
- SSH/SFTP wire handling via the `russh` and `russh-sftp` crates. SFTPv3 framing is implemented by `russh-sftp`; the rustfs-side `SftpDriver` implements `russh_sftp::server::Handler` and dispatches to the storage backend
- Drop-time abort for in-flight multipart uploads honours IAM Deny on `AbortMultipartUpload`. `start_multipart_upload` caches the authorisation decision so the synchronous `Drop` path can honour Allow / Deny policies without re-querying IAM
- Per-handle read cache uses an `Arc<AtomicU64>` shared across every `SftpDriver` instance to enforce a process-wide memory ceiling. On ceiling breach the populate is skipped and the read serves correctly via a single-call backend fetch
- Per-session liveness watchdog runs as a tokio task per accepted connection. Reads `/proc/net/tcp` and `/proc/net/tcp6` to look up the (local, peer) tuple's TCP state and cancels via `tokio_util::sync::CancellationToken` when wedge conditions are confirmed across two consecutive ticks
- Path canonicalisation rejects paths containing `\0`, `\r`, or `\n` and resolves traversal via `path::clean()` before any backend dispatch
- Cipher / KEX / MAC / host-key algorithm allowlists are hardcoded with no environment override. Strict-KEX (CVE-2023-48795 / Terrapin) marker presence asserted by unit test
- Per-session handle cap (default 64, configurable 8 to 1024) with UUID-generated handle ids
- Crate-level `#![deny(unsafe_code)]` is in force across `crates/protocols`. Socket fd duplication for the watchdog uses the safe `AsFd::try_clone_to_owned` path (Linux/Unix); non-Unix falls back to the inactivity ceiling
- `cfg(unix)` gating around platform-specific imports (`std::os::fd::AsFd`, `std::os::unix::fs::PermissionsExt`); non-Unix targets fail SFTP at config-load with `SftpInitError::UnsupportedPlatform`

### Documentation
- Updated `crates/keystone/README.md` with complete integration architecture and workflow
- Added detailed manual testing guide with 10 test scenarios
- Updated main `README.md` to list Keystone authentication as available feature
- Added troubleshooting section for common integration issues
- Module-level rustdoc on `crates/protocols/src/sftp/mod.rs` describing the public API surface, configuration contract, and the architecture of the read cache and the wedge watchdog

### Configuration
New environment variables:
- `RUSTFS_KEYSTONE_ENABLE` - Enable/disable Keystone authentication (default: false)
- `RUSTFS_KEYSTONE_AUTH_URL` - Keystone API endpoint URL
- `RUSTFS_KEYSTONE_VERSION` - Keystone API version (v3)
- `RUSTFS_KEYSTONE_ADMIN_USER` - Admin username for privileged operations
- `RUSTFS_KEYSTONE_ADMIN_PASSWORD` - Admin password
- `RUSTFS_KEYSTONE_ADMIN_PROJECT` - Admin project name
- `RUSTFS_KEYSTONE_ADMIN_DOMAIN` - Admin domain name (default: Default)
- `RUSTFS_KEYSTONE_CACHE_SIZE` - Token cache size (default: 10000)
- `RUSTFS_KEYSTONE_CACHE_TTL` - Token cache TTL in seconds (default: 300)
- `RUSTFS_KEYSTONE_VERIFY_SSL` - Verify SSL certificates (default: true)
- `RUSTFS_SFTP_ENABLE` - Enable/disable SFTP (default: false)
- `RUSTFS_SFTP_ADDRESS` - Listen address (default: 0.0.0.0:2222)
- `RUSTFS_SFTP_HOST_KEY_DIR` - Directory containing host key files (must exist; each file must be 0o600 or 0o400)
- `RUSTFS_SFTP_IDLE_TIMEOUT` - Session idle timeout in seconds (default: 600)
- `RUSTFS_SFTP_PART_SIZE` - Multipart part size in bytes (default: 16 MiB)
- `RUSTFS_SFTP_READ_ONLY` - Reject write packets at the protocol layer (default: false)
- `RUSTFS_SFTP_BANNER` - Optional SSH banner text
- `RUSTFS_SFTP_HANDLES_PER_SESSION` - Per-session open-handle cap, 8 to 1024 (default: 64)
- `RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS` - Per-call backend deadline in seconds, 5 to 600 (default: 60)
- `RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES` - Per-handle read-cache window in bytes, 256 KiB to 64 MiB or 0 to disable (default: 4 MiB)
- `RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES` - Process-wide read-cache memory ceiling in bytes, 16 MiB minimum (default: 256 MiB)

### Files Added
- `crates/protocols/src/sftp/mod.rs` - SFTP module entry point, public API surface, crate-level rustdoc, regression-prevention test
- `crates/protocols/src/sftp/config.rs` - `SftpConfig` and `SftpInitError` types, env-var resolvers, host-key directory loader with permission enforcement
- `crates/protocols/src/sftp/constants.rs` - Named constants grouped by purpose: S3 error codes, HTTP error codes, POSIX mode bits, protocol identifiers, operational limits
- `crates/protocols/src/sftp/server.rs` - `SftpServer` SSH server, russh handler, password authentication against IAM, accept loop, per-session task spawn
- `crates/protocols/src/sftp/driver.rs` - `SftpDriver` per-session SFTPv3 handler dispatching each operation onto the `StorageBackend`
- `crates/protocols/src/sftp/state.rs` - `HandleState` variants for read, write-buffering, write-streaming, write-failed handles
- `crates/protocols/src/sftp/lifecycle.rs` - Per-session activity stamp, weak-ref registry, `/proc/net/tcp` probe for the wedge watchdog
- `crates/protocols/src/sftp/wedge_watchdog.rs` - Per-session liveness watchdog cancelling sessions silent at the SFTP layer while the kernel reports CLOSE_WAIT
- `crates/protocols/src/sftp/read_cache.rs` - Per-handle in-memory read-ahead cache with shared atomic accumulator for the process-wide memory ceiling
- `crates/protocols/src/sftp/attrs.rs` - SFTPv3 `FileAttributes` mapping for objects and directories, longname formatting, mtime clamping
- `crates/protocols/src/sftp/dir.rs` - OPENDIR / READDIR pagination, root-bucket listing, sub-directory listing under a prefix
- `crates/protocols/src/sftp/errors.rs` - `SftpError` thiserror enum and S3-error classification into SFTPv3 status codes
- `crates/protocols/src/sftp/paths.rs` - Path canonicalisation, traversal rejection, `\0` / `\r` / `\n` rejection, bucket+key decomposition
- `crates/protocols/src/sftp/read.rs` - READ packet handler, EOF semantics, `MAX_READ_LEN` bound, integration with the read cache
- `crates/protocols/src/sftp/write.rs` - WRITE packet handler, in-memory buffering up to part size, transition to streaming multipart, CLOSE finalisation
- `crates/protocols/src/sftp/test_support.rs` - Test fixtures and helper builders for SFTP unit tests
- `crates/protocols/src/common/dummy_storage.rs` - In-memory `StorageBackend` test backend covering every method, used by SFTP unit tests and the FTPS / Swift / WebDAV test suites
- `crates/e2e_test/src/protocols/sftp_core.rs` - End-to-end regressions for the handshake deadline, idle-timeout disconnect, and the wedge watchdog
- `crates/e2e_test/src/protocols/sftp_compliance.rs` - SFTPv3 compliance suite entry points (`test_sftp_compliance_suite`, `test_sftp_compliance_readonly`, `test_sftp_compliance_standalone`)
- `crates/e2e_test/src/protocols/sftp_compliance_tests.rs` - Per-case test bodies (CMPTST-01..33), shared fixture helpers, lifecycle counters
- `crates/e2e_test/src/protocols/sftp_helpers.rs` - SFTP-specific test helpers and fixture seeders

### Files Modified
- `crates/keystone/src/middleware.rs` - Created Keystone authentication middleware (self-contained in keystone crate)
- `crates/keystone/src/lib.rs` - Exported middleware module and KEYSTONE_CREDENTIALS
- `crates/keystone/Cargo.toml` - Added Tower/HTTP dependencies for middleware functionality
- `rustfs/src/server/http.rs` - Integrated KeystoneAuthLayer from rustfs-keystone crate
- `rustfs/src/auth.rs` - Enhanced IAMAuth and check_key_valid for Keystone support, imported KEYSTONE_CREDENTIALS from rustfs-keystone
- `crates/keystone/README.md` - Comprehensive integration documentation
- `README.md` - Added Keystone as available feature
- `Cargo.toml` - Added the `sftp` feature alongside the existing protocol features
- `Cargo.lock` - Updated to include the new `russh`, `russh-sftp`, `socket2`, `tokio-util`, `subtle`, `uuid` dependencies and their transitive crates
- `crates/protocols/Cargo.toml` - Declared `russh`, `russh-sftp`, `socket2`, `tokio-util`, `subtle`, `uuid` under the `sftp` feature flag
- `crates/protocols/src/lib.rs` - Added `pub mod sftp` behind `#[cfg(feature = "sftp")]` plus the crate-level `#![deny(unsafe_code)]` lint
- `crates/protocols/src/common/client/s3.rs` - Extended the `StorageBackend` trait with `create_multipart_upload`, `upload_part`, `complete_multipart_upload`, `abort_multipart_upload`, and `upload_part_copy`
- `crates/protocols/src/common/session.rs` - Added the `Protocol::Sftp` variant and its `S3Action` mappings
- `crates/protocols/src/common/gateway.rs` - Handles the new `Protocol::Sftp` variant exhaustively
- `crates/protocols/src/common/mod.rs` - Exposed the new `dummy_storage` module
- `crates/protocols/src/constants.rs` - Added shared POSIX mode-bit constants used by SFTP and other protocols
- `crates/config/src/constants/protocols.rs` - `RUSTFS_SFTP_*` environment variable names and defaults
- `crates/utils/src/retry.rs` - Added the generic exponential-backoff retry helper used by the SFTP write path
- `crates/e2e_test/Cargo.toml` - Added the e2e test dependencies for SFTP (paramiko fixture, SSH keypair generation)
- `crates/e2e_test/src/protocols/mod.rs` - Registered the new `sftp_core`, `sftp_compliance`, `sftp_compliance_tests`, and `sftp_helpers` modules
- `crates/e2e_test/src/protocols/README.md` - Documented the SFTP test entry points and case index
- `crates/e2e_test/src/protocols/test_env.rs` - Added SFTP host-key directory provisioning to the shared protocol test environment
- `crates/e2e_test/src/protocols/test_runner.rs` - Wired the SFTP entry points into the runner
- `rustfs/Cargo.toml` - Added the `sftp` feature flag
- `rustfs/src/lib.rs` - One-line addition exporting the SFTP wiring
- `rustfs/src/init.rs` - Build and start the `SftpServer` when `RUSTFS_SFTP_ENABLE` is true
- `rustfs/src/main.rs` - Routed shutdown signals to the SFTP server alongside the other protocols
- `rustfs/src/protocols/client.rs` - Client-builder support for the new `Protocol::Sftp` variant

### Testing
- 16 unit tests in rustfs-keystone crate (config, auth, middleware, identity)
- 10 integration tests in rustfs-keystone crate (task-local storage, middleware layer, scope isolation)
- 6 auth unit tests in rustfs crate (role detection, task-local storage, Keystone credential handling)
- **Total: 32 tests** passing with zero compilation errors
- Manual testing guide provided for end-to-end validation
- All Keystone tests passing with `cargo test --all --exclude e2e_test`
- 33 SFTPv3 compliance test cases (CMPTST-01..33) split across three entry points: `test_sftp_compliance_suite` (shared session, cases 01-14), `test_sftp_compliance_readonly` (read-only mode, cases 15-23), `test_sftp_compliance_standalone` (one rustfs spawn per case, cases 24-33)
- Regression-prevention tests at four layers: compile-time module assertion in `crates/protocols/src/lib.rs`, module-presence unit test in `crates/protocols/src/sftp/mod.rs`, cross-module `Protocol` enum assertion, and end-to-end SSH banner test against the running binary
- Standalone end-to-end regressions for the SSH handshake deadline, the idle-timeout disconnect path, and the wedge watchdog (Linux fast-kill and the cross-platform fallback path)
- Inline unit tests in every SFTP source file covering pure helpers (path canonicalisation, attribute mapping, S3-error classification, env-var bound resolvers)
- Strict-KEX (CVE-2023-48795) marker presence assertion as a unit test in `crates/protocols/src/sftp/server.rs`
- All tests passing with `cargo test --all --features sftp` against a 64-bit Linux target

---

## Previous Releases

See [GitHub Releases](https://github.com/rustfs/rustfs/releases) for previous version history.
