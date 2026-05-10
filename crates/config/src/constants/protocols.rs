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

//! Protocol server configuration constants

/// Default FTP server bind address (non-encrypted)
pub const DEFAULT_FTP_ADDRESS: &str = "0.0.0.0:8021";

/// Default FTPS server bind address (FTP over TLS)
pub const DEFAULT_FTPS_ADDRESS: &str = "0.0.0.0:8022";

/// Default FTP passive ports range (optional)
pub const DEFAULT_FTP_PASSIVE_PORTS: Option<&str> = None;

/// Default FTPS passive ports range (optional)
pub const DEFAULT_FTPS_PASSIVE_PORTS: Option<&str> = None;

/// Default FTP external IP (auto-detected)
pub const DEFAULT_FTP_EXTERNAL_IP: Option<&str> = None;

/// Default FTPS external IP (auto-detected)
pub const DEFAULT_FTPS_EXTERNAL_IP: Option<&str> = None;

/// Environment variable names
pub const ENV_FTP_ENABLE: &str = "RUSTFS_FTP_ENABLE";
pub const ENV_FTP_ADDRESS: &str = "RUSTFS_FTP_ADDRESS";
pub const ENV_FTP_PASSIVE_PORTS: &str = "RUSTFS_FTP_PASSIVE_PORTS";
pub const ENV_FTP_EXTERNAL_IP: &str = "RUSTFS_FTP_EXTERNAL_IP";

pub const ENV_FTPS_ENABLE: &str = "RUSTFS_FTPS_ENABLE";
pub const ENV_FTPS_ADDRESS: &str = "RUSTFS_FTPS_ADDRESS";
pub const ENV_FTPS_TLS_ENABLED: &str = "RUSTFS_FTPS_TLS_ENABLED";
pub const ENV_FTPS_CERTS_DIR: &str = "RUSTFS_FTPS_CERTS_DIR";
pub const ENV_FTPS_CA_FILE: &str = "RUSTFS_FTPS_CA_FILE";
pub const ENV_FTPS_PASSIVE_PORTS: &str = "RUSTFS_FTPS_PASSIVE_PORTS";
pub const ENV_FTPS_EXTERNAL_IP: &str = "RUSTFS_FTPS_EXTERNAL_IP";

/// Default WebDAV server bind address
pub const DEFAULT_WEBDAV_ADDRESS: &str = "0.0.0.0:8080";

/// WebDAV environment variable names
pub const ENV_WEBDAV_ENABLE: &str = "RUSTFS_WEBDAV_ENABLE";
pub const ENV_WEBDAV_ADDRESS: &str = "RUSTFS_WEBDAV_ADDRESS";
pub const ENV_WEBDAV_TLS_ENABLED: &str = "RUSTFS_WEBDAV_TLS_ENABLED";
pub const ENV_WEBDAV_CERTS_DIR: &str = "RUSTFS_WEBDAV_CERTS_DIR";
pub const ENV_WEBDAV_CA_FILE: &str = "RUSTFS_WEBDAV_CA_FILE";
pub const ENV_WEBDAV_MAX_BODY_SIZE: &str = "RUSTFS_WEBDAV_MAX_BODY_SIZE";
pub const ENV_WEBDAV_REQUEST_TIMEOUT: &str = "RUSTFS_WEBDAV_REQUEST_TIMEOUT";

/// Default SFTP server bind address.
pub const DEFAULT_SFTP_ADDRESS: &str = "0.0.0.0:2222";

/// Default for SFTP host-key directory. None means no default. Operators
/// must set RUSTFS_SFTP_HOST_KEY_DIR explicitly when SFTP is enabled.
pub const DEFAULT_SFTP_HOST_KEY_DIR: Option<&str> = None;

/// SFTP environment variable names.
pub const ENV_SFTP_ENABLE: &str = "RUSTFS_SFTP_ENABLE";
pub const ENV_SFTP_ADDRESS: &str = "RUSTFS_SFTP_ADDRESS";
pub const ENV_SFTP_HOST_KEY_DIR: &str = "RUSTFS_SFTP_HOST_KEY_DIR";
pub const ENV_SFTP_IDLE_TIMEOUT: &str = "RUSTFS_SFTP_IDLE_TIMEOUT";
/// S3 multipart part size in bytes. Default DEFAULT_SFTP_PART_SIZE (16 MiB).
/// Valid range 5 MiB to 5 GiB (S3 protocol bounds), enforced at startup.
///
/// The per-upload size ceiling is part_size * 10_000 (the S3 parts cap),
/// so the default caps single uploads at 160 GiB. Deployments expecting
/// larger single files must raise this: 64 MiB -> 640 GiB, 128 MiB ->
/// 1.25 TiB, 512 MiB -> 5 TiB (S3 object max). Rename is not affected;
/// multipart_copy scales the per-part size dynamically and handles up
/// to the 5 TiB S3 object limit regardless of this setting.
pub const ENV_SFTP_PART_SIZE: &str = "RUSTFS_SFTP_PART_SIZE";
pub const ENV_SFTP_READ_ONLY: &str = "RUSTFS_SFTP_READ_ONLY";
pub const ENV_SFTP_BANNER: &str = "RUSTFS_SFTP_BANNER";
/// Optional environment variable. If RUSTFS_SFTP_HANDLES_PER_SESSION
/// is not set in the process environment, the server uses the default
/// of 64 handles per session and emits no warning. If set, the value
/// must be in the inclusive range 8 to 1024. Out-of-range values fall
/// back to the default of 64 with a warn-level log naming the
/// requested value and the bounds.
///
/// Caps the maximum number of simultaneously-open SFTP handles per
/// session. A handle is the server-side identifier returned by
/// SSH_FXP_OPEN and SSH_FXP_OPENDIR. One client typically uses one
/// handle per file in flight plus one per directory listing.
/// Operators running clients with deep pipelining may raise this.
pub const ENV_SFTP_HANDLES_PER_SESSION: &str = "RUSTFS_SFTP_HANDLES_PER_SESSION";

/// Optional environment variable. If RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS
/// is not set in the process environment, the server uses the default
/// of 60 seconds and emits no warning. If set, the value must be in
/// the inclusive range 5 to 600 seconds. Out-of-range values fall
/// back to the default with a warn-level log naming the requested
/// value and the bounds.
///
/// Bounds every storage backend call issued by the SFTP driver. A
/// backend that does not respond within this many seconds returns
/// Failure to the client and emits a warn log naming the backend
/// method. This catches a backend that accepted the request and never
/// returned a body, which the SSH keepalive cannot detect because the
/// transport itself remains live.
pub const ENV_SFTP_BACKEND_OP_TIMEOUT_SECS: &str = "RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS";

/// Optional environment variable. If RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES
/// is not set in the process environment, the server uses a 4 MiB
/// default and emits no warning. If set, the value must be in the
/// inclusive range MAX_READ_LEN (256 KiB) to 64 MiB. Out-of-range
/// values fall back to the default with a warn-level log naming the
/// requested value and the bounds.
///
/// Per-handle byte window the SFTP read path fetches in one backend
/// call on a cache miss. Subsequent FXP_READs within that window are
/// served from the buffer without a backend round trip. For
/// sequential downloads the backend round-trip count drops by
/// window_bytes / MAX_READ_LEN. Random-access workloads should set
/// the window equal to MAX_READ_LEN to opt out of read-ahead.
pub const ENV_SFTP_READ_CACHE_WINDOW_BYTES: &str = "RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES";

/// Optional environment variable. If
/// RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES is not set in the process
/// environment, the server uses a 256 MiB default and emits no
/// warning. If set, the value must be at least 16 MiB. Below-min
/// values fall back to the default with a warn-level log naming the
/// requested value and the bound.
///
/// Process-wide ceiling on cumulative read cache memory across every
/// live SFTP handle. Once the accumulator plus a new window would
/// exceed this value, the populate call on the per-handle cache is
/// skipped. The read still completes from the freshly-fetched bytes
/// without storing them in the cache, at the cost of one backend
/// call per FXP_READ. High-concurrency deployments expecting many
/// parallel downloads should raise this in step with the per-session
/// handle cap.
pub const ENV_SFTP_READ_CACHE_TOTAL_MEM_BYTES: &str = "RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES";

/// Default idle session timeout in seconds.
pub const DEFAULT_SFTP_IDLE_TIMEOUT: u64 = 600;

/// Default S3 multipart upload part size in bytes (16 MiB).
///
/// The per-upload size ceiling is part_size * 10_000 (the S3 parts cap),
/// so the default gives a 160 GiB single-upload limit. Deployments that
/// expect single files larger than this must raise part_size:
/// 64 MiB -> 640 GiB, 128 MiB -> 1.25 TiB, 512 MiB -> 5 TiB (S3 max).
/// The minimum is 5 MiB and the maximum is 5 GiB (S3 protocol bounds).
pub const DEFAULT_SFTP_PART_SIZE: u64 = 16_777_216;

/// Default read-only mode (disabled).
pub const DEFAULT_SFTP_READ_ONLY: bool = false;

/// Default SSH identification string (no version disclosure).
pub const DEFAULT_SFTP_BANNER: &str = "SSH-2.0-RustFS";
