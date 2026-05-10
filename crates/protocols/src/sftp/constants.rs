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

//! Named constants for the SFTP protocol implementation, grouped by purpose.
//!
//! s3_error_codes: AWS S3 error-code substrings the driver matches when
//! classifying backend errors into SFTP status codes.
//!
//! http_error_codes: HTTP status-code substrings the driver matches when
//! a backend reports an HTTP error by number rather than by S3 code.
//!
//! posix: POSIX mode bits (S_IFDIR, S_IFREG, permission triples) returned
//! in SFTP FileAttributes for S3 resources.
//!
//! protocol: SFTP protocol version supported by the driver and the SSH
//! subsystem name clients request.
//!
//! limits: caps, defaults, and AWS-imposed constants used across the SFTP
//! driver and server.

/// S3 error-code substrings matched by the driver when classifying backend
/// errors into SFTP status codes. The constants below are fragments of the
/// public AWS S3 error-code vocabulary, which backends include in their
/// error messages.
pub mod s3_error_codes {
    /// AWS S3 error code returned by HeadObject / GetObject when the
    /// key does not exist.
    pub const NO_SUCH_KEY: &str = "NoSuchKey";
    /// AWS S3 error code returned by HeadBucket when the bucket does
    /// not exist.
    pub const NO_SUCH_BUCKET: &str = "NoSuchBucket";
    /// Generic "not found" string emitted by S3-compatible backends
    /// (MinIO, Wasabi, ecstore) that do not always use the AWS
    /// NoSuchKey / NoSuchBucket vocabulary on every miss.
    pub const NOT_FOUND: &str = "NotFound";
    /// AWS error code returned when an IAM policy denies the requested
    /// action on the resource.
    pub const ACCESS_DENIED: &str = "AccessDenied";
    /// Generic forbidden string emitted by S3-compatible backends that
    /// do not always use the AWS AccessDenied vocabulary.
    pub const FORBIDDEN: &str = "Forbidden";
    /// Returned by AbortMultipartUpload when the upload_id is no
    /// longer live (already completed, already aborted, or reclaimed
    /// by the bucket lifecycle rule). Drop's retry loop downgrades
    /// this to a debug log to avoid noise when the tombstone-retry
    /// path races a successful inline completion.
    pub const NO_SUCH_UPLOAD: &str = "NoSuchUpload";
}

/// HTTP status-code substrings matched by the driver when a backend
/// reports an HTTP error by number rather than by S3 error code. These
/// are a different vocabulary from s3_error_codes (HTTP wire statuses
/// rather than S3 API error codes) and kept in a separate module.
pub mod http_error_codes {
    pub const NOT_FOUND: &str = "404";
    pub const FORBIDDEN: &str = "403";
}

/// POSIX mode bits returned in SFTP FileAttributes for S3 resources.
/// SFTPv3 draft section 5 defines the permissions field as a u32
/// carrying POSIX stat.h mode bits. S3 has no POSIX mode metadata, so
/// the server returns a fixed type bit (S_IFDIR for buckets and
/// prefixes, S_IFREG for objects) combined with a conventional
/// permission triple. Clients that inspect the type bit to distinguish
/// files from directories would otherwise treat every entry as a
/// regular file.
pub mod posix {
    use crate::constants::paths::{DIR_MODE, DIR_PERMISSIONS, FILE_MODE, FILE_PERMISSIONS};

    /// Directory mode returned for bucket and prefix entries.
    /// S_IFDIR | 0o755 = 0o040755.
    pub const POSIX_DIR_MODE: u32 = DIR_MODE | DIR_PERMISSIONS;

    /// Regular-file mode returned for object entries.
    /// S_IFREG | 0o644 = 0o100644.
    pub const POSIX_FILE_MODE: u32 = FILE_MODE | FILE_PERMISSIONS;

    /// POSIX file-type mask (S_IFMT). Isolates the four high bits of a
    /// mode value so the file-type field can be compared against
    /// S_IFDIR, S_IFREG, S_IFLNK, and the other POSIX type constants.
    /// Compiled in test builds only; the runtime path reads the full
    /// mode from POSIX_DIR_MODE / POSIX_FILE_MODE.
    #[cfg(test)]
    pub const POSIX_TYPE_MASK: u32 = 0o170000;
}

/// SFTP protocol identifiers and version numbers.
pub mod protocol {
    /// SFTP protocol version supported by this server. The wire format and
    /// packet semantics are defined by the SFTP Internet Draft
    /// draft-ietf-secsh-filexfer-02. Later drafts (versions 4 to 6) change
    /// the attribute and timestamp encodings. Supporting them would require
    /// a separate driver type, not a parameter on the version-3 driver.
    pub const SFTP_VERSION: u32 = 3;

    /// SSH subsystem name that clients request to start SFTP.
    pub const SFTP_SUBSYSTEM_NAME: &str = "sftp";
}

/// Limits, defaults, and AWS-defined constants used across the SFTP
/// driver and server. Three roles share this module.
///
/// AWS-imposed limits. S3_COPY_OBJECT_MAX_SIZE, S3_MIN_PART_SIZE,
/// S3_MAX_PART_SIZE, and S3_MAX_MULTIPART_PARTS reflect the S3 API
/// contract and do not change per deployment.
///
/// Operational bounds. DEFAULT_HANDLES_PER_SESSION, the
/// BACKEND_OP_TIMEOUT trio (DEFAULT, MIN, MAX), the READ_CACHE_*
/// values, and SHUTDOWN_DRAIN_TIMEOUT_SECS govern per-session and
/// process-wide resource use. Each has a paired RUSTFS_SFTP_* env var
/// for operator override.
///
/// SSH transport overrides. SSH_MAXIMUM_PACKET_SIZE,
/// SSH_CHANNEL_BUFFER_SIZE, and SSH_EVENT_BUFFER_SIZE override russh
/// defaults so the inbound mpsc absorbs client pipelining during
/// multi-MB transfers.
pub mod limits {
    /// Maximum payload size accepted from a single READ request, in bytes.
    /// Matches OpenSSH's default chunk size and bounds per-request memory.
    pub const MAX_READ_LEN: u32 = 256 * 1024;

    /// Default number of simultaneously-open SFTP handles per session.
    /// Used when RUSTFS_SFTP_HANDLES_PER_SESSION is unset or out of
    /// range. 64 covers the typical OpenSSH / rsync / WinSCP
    /// pipelining ceiling.
    pub const DEFAULT_HANDLES_PER_SESSION: usize = 64;

    /// Lower validation bound on RUSTFS_SFTP_HANDLES_PER_SESSION.
    /// Below this a single client opening one file plus a directory
    /// listing already runs out of handles.
    pub const HANDLES_PER_SESSION_MIN: usize = 8;

    /// Upper validation bound on RUSTFS_SFTP_HANDLES_PER_SESSION.
    /// Each handle can hold a part_size-sized buffer (write path), so
    /// at default part_size = 16 MiB the worst-case session memory
    /// is 16 GiB at this cap.
    pub const HANDLES_PER_SESSION_MAX: usize = 1024;

    /// Seconds between SSH keepalive probes. Passed into
    /// russh::server::Config at server-build time. russh sends an
    /// SSH-level keepalive request after this many seconds of silence.
    /// If the client does not respond after KEEPALIVE_MAX consecutive
    /// probes the connection is closed.
    ///
    /// This detects dead TCP connections where the client disappeared
    /// without sending FIN (network failure, killed process, etc).
    /// Active but slow connections are unaffected because they still
    /// respond to the small SSH keepalive packets even during large
    /// transfers. OpenSSH's ServerAliveInterval defaults to 15 seconds
    /// on the client side. 15 seconds on the server side is consistent
    /// with that.
    pub const KEEPALIVE_INTERVAL_SECS: u64 = 15;

    /// Number of consecutive missed keepalive responses before russh
    /// closes the connection. Passed into russh::server::Config at
    /// server-build time. With KEEPALIVE_INTERVAL_SECS = 15, a truly
    /// dead connection is closed within ~45 seconds.
    pub const KEEPALIVE_MAX: usize = 3;

    /// Wallclock deadline applied to russh::server::run_stream while
    /// the SSH KEX and password auth handshake completes. A peer that
    /// completes TCP and stalls before KEXINIT (or that drives KEX or
    /// auth so slowly that no SSH-layer timer fires) is dropped after
    /// this many seconds, freeing the spawn-task slot. Inactivity and
    /// keepalive timers do not cover this window because they run
    /// inside the post-handshake session loop.
    pub const HANDSHAKE_DEADLINE_SECS: u64 = 30;

    /// Tick interval for the per-session wedge watchdog. Worst-case
    /// detection latency is WEDGE_FAST_KILL_SILENCE_SECS + one tick.
    pub const WEDGE_WATCHDOG_TICK_SECS: u64 = 15;

    /// Silence threshold at which a session whose underlying TCP socket
    /// is in CLOSE_WAIT is force-cancelled by the watchdog.
    ///
    /// A healthy session is never simultaneously silent at the SFTP
    /// handler AND in CLOSE_WAIT: peer FIN normally surfaces as Ok(0)
    /// on the SSH library read poll within milliseconds. 30 s leaves
    /// room for two keepalive intervals (15 s each) before the
    /// watchdog overrides, so a transient scheduler stall does not
    /// trip it.
    pub const WEDGE_FAST_KILL_SILENCE_SECS: u64 = 30;

    /// Fallback silence threshold. The only kill path on non-Linux
    /// targets, where /proc/net/tcp is unavailable and the watchdog's
    /// CLOSE_WAIT probe always returns None. On Linux it is the
    /// backstop for cases where /proc/net/tcp is unreadable for some
    /// other reason (filesystem permissions, namespace tricks) or
    /// where the wedge surfaces in a state other than CLOSE_WAIT.
    /// 1800 s sits above russh's default inactivity_timeout (600 s)
    /// so russh's own inactivity close fires first on a healthy idle session.
    pub const WEDGE_FALLBACK_KILL_SILENCE_SECS: u64 = 1800;

    // The three constants below override russh defaults for the SSH
    // transport the SFTP subsystem runs on. russh defaults
    // (channel_buffer_size 100, event_buffer_size 10) are tight enough
    // that the inbound mpsc fills under client pipelining, the
    // session-loop reading arm blocks on chan.send(...).await, and
    // inbound CHANNEL_WINDOW_ADJUST stops being drained. PuTTY-derived
    // stacks (FileZilla, Cyberduck) reach the limit during multi-MB
    // downloads.

    /// Maximum SSH packet size advertised by the server, in bytes.
    /// Matches russh's default. Set explicitly so behaviour does not
    /// depend on russh's chosen default.
    pub const SSH_MAXIMUM_PACKET_SIZE: u32 = 32 * 1024;

    /// Capacity of the bounded mpsc that russh's session loop uses
    /// for inbound CHANNEL_DATA. russh default is 100. Raised to
    /// defer fill past typical client pipelining depths.
    pub const SSH_CHANNEL_BUFFER_SIZE: usize = 1024;

    /// Capacity of the bounded mpsc that russh's session loop uses
    /// for channel-level events. russh default is 10. Raised to
    /// defer fill past typical client pipelining depths.
    pub const SSH_EVENT_BUFFER_SIZE: usize = 1024;

    // The four constants below are S3 protocol limits defined by the AWS
    // S3 API. They are not SFTP operational policy and do not change per
    // deployment. The ecstore client crate defines the same four values
    // under different names (ABS_MIN_PART_SIZE, MAX_PART_SIZE,
    // MAX_PARTS_COUNT, MAX_SINGLE_PUT_OBJECT_SIZE). They live here as
    // SFTP-scoped copies because the protocols crate must not depend on
    // ecstore internals: the StorageBackend trait abstraction would leak.

    /// S3 CopyObject single-shot size limit (5 GiB). Source objects
    /// larger than this require UploadPartCopy. Mirrors the
    /// MAX_SINGLE_PUT_OBJECT_SIZE constant in ecstore but cannot be
    /// imported from there.
    pub const S3_COPY_OBJECT_MAX_SIZE: u64 = 5 * 1024 * 1024 * 1024;

    /// S3 minimum part size in bytes (5 MiB). Every part of a multipart
    /// upload except the last must be at least this size, or
    /// CompleteMultipartUpload returns EntityTooSmall. Mirrors ecstore's
    /// ABS_MIN_PART_SIZE but cannot be imported from there.
    pub const S3_MIN_PART_SIZE: u64 = 5 * 1024 * 1024;

    /// S3 maximum part size in bytes (5 GiB). Any single UploadPart call
    /// carrying a body larger than this is rejected with EntityTooLarge.
    /// Mirrors the MAX_PART_SIZE constant in ecstore but cannot be
    /// imported from there. AWS sets S3_COPY_OBJECT_MAX_SIZE and
    /// S3_MAX_PART_SIZE independently to 5 GiB; the values are not
    /// coupled. Future S3 versions could move them apart, so they
    /// remain separate constants.
    pub const S3_MAX_PART_SIZE: u64 = 5 * 1024 * 1024 * 1024;

    /// Maximum number of parts in a single multipart upload (S3 limit).
    /// Exceeding this causes UploadPart to fail. Mirrors ecstore's
    /// MAX_PARTS_COUNT but cannot be imported from there.
    pub const S3_MAX_MULTIPART_PARTS: i32 = 10_000;

    /// Maximum seconds the SFTP server waits for session tasks to
    /// finish after a shutdown signal before the runtime cancels them.
    /// This is the cleanup-grace window for the Drop impl on each
    /// SftpDriver (which issues AbortMultipartUpload for live
    /// upload_ids), not a transfer-completion window. In-flight
    /// transfers do not need to finish inside this timer. Cancellation
    /// past this timeout leaves any remaining upload_ids to the bucket
    /// AbortIncompleteMultipartUpload lifecycle rule.
    pub const SHUTDOWN_DRAIN_TIMEOUT_SECS: u64 = 30;

    /// Maximum number of buckets returned by the root READDIR. S3
    /// ListBuckets is not paginated so the backend can hand back an
    /// arbitrarily long response. Truncating here bounds the Vec
    /// allocation and keeps the SSH channel window usage low for a
    /// principal with many visible buckets. Overflow is logged as a
    /// warn so operators know truncation happened.
    pub const ROOT_LISTING_MAX_ENTRIES: usize = 10_000;

    /// Maximum entries requested per ListObjectsV2 page for READDIR.
    /// The S3 default is 1000. Asking for a specific value keeps the
    /// per-page allocation and SSH channel window usage under operator
    /// control. Each entry's longname is bounded by a filename plus a
    /// fixed-width header, so 1000 entries stays under the 2 MiB
    /// channel window.
    pub const READDIR_PAGE_MAX_KEYS: i32 = 1_000;

    /// Default per-call deadline applied to every StorageBackend
    /// invocation issued by the SFTP driver. A backend that does not
    /// respond within this many seconds returns Failure to the client
    /// and emits a warn log naming the backend method. Used when
    /// RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS is unset or out of range.
    /// The keepalive timer (KEEPALIVE_INTERVAL_SECS times KEEPALIVE_MAX,
    /// approximately 45 s) closes a stuck SSH transport but cannot detect
    /// a backend that accepted the request and never returned a body.
    /// This deadline closes that gap.
    pub const DEFAULT_BACKEND_OP_TIMEOUT_SECS: u64 = 60;

    /// Lower validation bound on RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS.
    /// Below 5 s a healthy backend under load (cold-cache HEAD on a
    /// large bucket, multipart Complete on hundreds of parts) can
    /// time out under normal operating conditions.
    pub const BACKEND_OP_TIMEOUT_MIN_SECS: u64 = 5;

    /// Upper validation bound on RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS.
    /// 600 s is the longest single backend call expected in normal
    /// use. Above that the SSH keepalive (about 45 s) takes over the
    /// liveness role.
    pub const BACKEND_OP_TIMEOUT_MAX_SECS: u64 = 600;

    /// Maximum number of retries the small-file PutObject path in
    /// commit_write attempts after a transient backend error
    /// (SlowDown, RequestTimeout, Throttling, InternalError, etc).
    /// Three retries covers the typical S3 retry-after window without
    /// holding the SFTP CLOSE response open beyond the keepalive
    /// timer. Total elapsed before giving up is the sum of
    /// COMMIT_WRITE_BACKOFF_MS plus the cumulative call time.
    pub const COMMIT_WRITE_MAX_RETRIES: usize = 3;

    /// Backoff schedule between commit_write PutObject retries, in
    /// milliseconds. Index zero is the wait between attempt 0 and
    /// attempt 1, and so on. The exponential 250 / 500 / 1000 cadence
    /// matches typical S3 SDK defaults and stays inside the worst-case
    /// 2 s combined wait that a CLOSE response can absorb without the
    /// client surfacing a hang.
    pub const COMMIT_WRITE_BACKOFF_MS: [u64; COMMIT_WRITE_MAX_RETRIES] = [250, 500, 1000];

    /// Per-handle read cache window size in bytes. On a cache miss
    /// the driver fetches at most this many bytes from the backend,
    /// then returns the requested portion to the client and stores
    /// the rest in the per-handle buffer. With the 4 MiB default and
    /// the 256 KiB MAX_READ_LEN, sixteen FXP_READs are returned from
    /// one backend call. Overridable per installation via
    /// RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES.
    pub const READ_CACHE_WINDOW_DEFAULT: u64 = 4 * 1024 * 1024;

    /// Lower validation bound on RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES
    /// for non-zero values. The cache-window floor reflects MAX_READ_LEN.
    /// Below it a single MAX_READ_LEN FXP_READ cannot be satisfied from
    /// one cached chunk, so the per-handle allocation costs memory with
    /// no benefit. To turn the cache off entirely, use the
    /// READ_CACHE_DISABLED sentinel.
    pub const READ_CACHE_WINDOW_MIN: u64 = MAX_READ_LEN as u64;

    /// Sentinel value for RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES that
    /// disables the per-handle read cache. The populate path is
    /// short-circuited, no buffer is retained between FXP_READs, and
    /// the process-wide accumulator is not touched. Each FXP_READ
    /// takes one backend call.
    pub const READ_CACHE_DISABLED: u64 = 0;

    /// Upper validation bound on RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES.
    /// Bounds single-handle memory at a value that fits inside
    /// READ_CACHE_TOTAL_MEM_DEFAULT even with four concurrent
    /// handles open.
    pub const READ_CACHE_WINDOW_MAX: u64 = 64 * 1024 * 1024;

    /// Process-wide ceiling on cumulative read cache memory across
    /// every live SFTP handle. When the accumulator plus a new
    /// window would exceed this value, the populate call is skipped.
    /// The read still completes from the freshly-fetched bytes
    /// without storing them in the cache. The next FXP_READ on the
    /// same handle issues a fresh backend call instead of being
    /// returned from the buffer. Overridable per installation via
    /// RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES.
    pub const READ_CACHE_TOTAL_MEM_DEFAULT: u64 = 256 * 1024 * 1024;

    /// Lower validation bound on
    /// RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES. Below this value, even
    /// a single window at the default window size cannot be stored
    /// without breaching the cap, leaving every read on the no-cache
    /// path.
    pub const READ_CACHE_TOTAL_MEM_MIN: u64 = 16 * 1024 * 1024;
}
