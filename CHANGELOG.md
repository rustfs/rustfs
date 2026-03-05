# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

### Changed
- **HTTP Server Stack**: Integrated `KeystoneAuthLayer` middleware from `rustfs-keystone` crate into service stack (positioned after ReadinessGateLayer)
- **IAMAuth**: Enhanced `get_secret_key()` to return empty secret for Keystone credentials (bypasses signature validation)
- **Auth Module**: Modified `check_key_valid()` to retrieve Keystone credentials from task-local storage and determine admin status

### Technical Details
- Middleware is self-contained in `rustfs-keystone` crate following the trusted-proxies pattern for integration-specific middleware
- Uses `BoxBody` pattern for Hyper 1.x compatibility
- Task-local storage provides request-scoped credential passing without modifying HTTP request/response types
- Integration preserves existing S3 authentication flow while adding Keystone support
- Zero breaking changes to existing functionality
- No new top-level directories in main binary crate (middleware lives in integration crate)

### Documentation
- Updated `crates/keystone/README.md` with complete integration architecture and workflow
- Added detailed manual testing guide with 10 test scenarios
- Updated main `README.md` to list Keystone authentication as available feature
- Added troubleshooting section for common integration issues

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

### Files Modified
- `crates/keystone/src/middleware.rs` - Created Keystone authentication middleware (self-contained in keystone crate)
- `crates/keystone/src/lib.rs` - Exported middleware module and KEYSTONE_CREDENTIALS
- `crates/keystone/Cargo.toml` - Added Tower/HTTP dependencies for middleware functionality
- `rustfs/src/server/http.rs` - Integrated KeystoneAuthLayer from rustfs-keystone crate
- `rustfs/src/auth.rs` - Enhanced IAMAuth and check_key_valid for Keystone support, imported KEYSTONE_CREDENTIALS from rustfs-keystone
- `crates/keystone/README.md` - Comprehensive integration documentation
- `README.md` - Added Keystone as available feature

### Testing
- 16 unit tests in rustfs-keystone crate (config, auth, middleware, identity)
- 10 integration tests in rustfs-keystone crate (task-local storage, middleware layer, scope isolation)
- 6 auth unit tests in rustfs crate (role detection, task-local storage, Keystone credential handling)
- **Total: 32 tests** passing with zero compilation errors
- Manual testing guide provided for end-to-end validation
- All tests passing with `cargo test --all --exclude e2e_test`

---

## Previous Releases

See [GitHub Releases](https://github.com/rustfs/rustfs/releases) for previous version history.
