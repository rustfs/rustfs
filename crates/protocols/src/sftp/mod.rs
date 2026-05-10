// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! SFTP protocol support for RustFS.
//!
//! Provides an SSH server with the SFTP file transfer subsystem enabled.
//! Each SFTP operation is translated into one or more S3 API calls against
//! the local RustFS object store via the StorageBackend trait.
//!
//! The module is feature-gated behind the sftp feature and is composed of
//! seven user-facing submodules:
//!
//! - config: configuration loading from environment variables, plus host
//!   key discovery and validation.
//! - constants: protocol limits, timeouts, and other named numeric values
//!   used by the server and driver.
//! - server: russh handler implementation, password authentication against
//!   IAM, and subsystem dispatch onto the SFTP driver.
//! - driver: SFTP operation handlers that translate each request into one
//!   or more S3 calls on the supplied storage backend.
//! - lifecycle: per-session activity record, the registry the accept loop
//!   walks, and the kernel TCP-state probe used by the watchdog.
//! - wedge_watchdog: per-session liveness watchdog that observes both the
//!   SFTP-handler activity stamp and the TCP socket state.
//! - read_cache: per-handle in-memory read-ahead cache with a process-wide
//!   memory ceiling.
//!
//! Configuration contract. Eleven RUSTFS_SFTP_* environment variables drive
//! the server: RUSTFS_SFTP_ENABLE, RUSTFS_SFTP_ADDRESS, RUSTFS_SFTP_HOST_KEY_DIR,
//! RUSTFS_SFTP_IDLE_TIMEOUT, RUSTFS_SFTP_PART_SIZE, RUSTFS_SFTP_READ_ONLY,
//! RUSTFS_SFTP_BANNER, RUSTFS_SFTP_HANDLES_PER_SESSION,
//! RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS, RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES,
//! RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES. Defaults and validation bounds
//! live on the constants in the limits module.
//!
//! Architecture. Two cross-cutting subsystems backstop session reliability
//! and read throughput:
//!
//! - Session-liveness watchdog. Every accepted connection runs under a
//!   per-session watchdog that observes the SFTP-handler activity stamp
//!   and the kernel TCP state for the connection. Sessions that fall
//!   silent at the SFTP layer while the kernel reports CLOSE_WAIT are
//!   canceled on a bounded schedule. The watchdog backstops resource
//!   accumulation regardless of which layer stalled. On Linux the
//!   detection latency is on the order of 45 seconds; on non-Linux
//!   targets the watchdog falls back to an inactivity ceiling on the
//!   order of 30 minutes.
//!
//! - Per-handle read cache. Each open File handle holds an in-memory
//!   buffer. On a cache miss the driver fetches a configurable byte
//!   window from the backend, returns the requested portion, and stores
//!   the rest. Subsequent reads inside that window are served from
//!   memory. Total cache memory across every live handle is bounded by
//!   a shared atomic accumulator enforced against the process-wide
//!   ceiling. On ceiling breach the populated is skipped and the read
//!   serves correctly via a single backend call without storing the
//!   bytes for re-use.
//!
//! Authentication mirrors the S3 baseline: identities are looked up through
//! rustfs_iam and the supplied secret is compared in constant time against
//! the stored secret. Failures are logged via tracing warn and return an SSH
//! authentication rejection.
//!
//! Public types: SftpServer is the entry point an embedder constructs and
//! drives. SftpConfig and SftpInitError are the configuration and error
//! types returned by configuration loading. SftpDriver is the per-session
//! handler dispatch type. SftpError is the error type returned by SFTP
//! operations.
//!
//! Platform support. Host-key permission enforcement uses Unix mode bits.
//! On non-Unix targets SftpConfig::load_host_keys returns
//! SftpInitError::UnsupportedPlatform and the SFTP listener does not start.
//!
//! Peer-initiated signal requests on an open SFTP channel are intercepted
//! by the russh::server::Handler::signal override on SshSessionHandler in
//! server.rs, which logs the probe and rejects without acting.

pub mod config;
pub(crate) mod constants;
pub mod server;

mod attrs;
mod dir;
mod driver;
mod errors;
mod lifecycle;
mod paths;
mod read;
mod read_cache;
mod state;
mod wedge_watchdog;
mod write;

#[cfg(test)]
mod test_support;

pub use config::{SftpConfig, SftpInitError};
pub use driver::SftpDriver;
pub use errors::SftpError;
pub use server::SftpServer;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::session::Protocol;

    // Compile-time check that Protocol::Sftp, SftpConfig, and SftpInitError
    // remain exported. Renaming or removing any of these breaks the test.
    #[test]
    fn sftp_module_and_variant_exist() {
        let _variant = Protocol::Sftp;
        let _config_type_name = std::any::type_name::<SftpConfig>();
        let _error_type_name = std::any::type_name::<SftpInitError>();
    }
}
