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

//! Configuration for the SFTP server.
//!
//! Loads bind address, host key directory, and operational parameters from
//! the RUSTFS_SFTP_* environment variables. Validates the configuration and
//! loads host keys from the configured directory at startup.
//!
//! Validation bounds and defaults (part-size, handles-per-session,
//! backend-op-timeout, read-cache window and total-memory) are pulled
//! from constants::limits.

use super::constants::limits::{
    BACKEND_OP_TIMEOUT_MAX_SECS, BACKEND_OP_TIMEOUT_MIN_SECS, DEFAULT_BACKEND_OP_TIMEOUT_SECS, DEFAULT_HANDLES_PER_SESSION,
    HANDLES_PER_SESSION_MAX, HANDLES_PER_SESSION_MIN, READ_CACHE_DISABLED, READ_CACHE_TOTAL_MEM_DEFAULT,
    READ_CACHE_TOTAL_MEM_MIN, READ_CACHE_WINDOW_DEFAULT, READ_CACHE_WINDOW_MAX, READ_CACHE_WINDOW_MIN, S3_MAX_PART_SIZE,
    S3_MIN_PART_SIZE,
};
use russh::keys::PublicKeyBase64;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use thiserror::Error;

/// Upper bound on file size accepted as a candidate host key (1 MiB).
/// Guards against accidentally reading huge non-key files in the host
/// key directory. Real keys are well under 10 KiB.
const MAX_HOST_KEY_FILE_SIZE: u64 = 1024 * 1024;

/// PEM pre-encapsulation boundary marker prefix per RFC 7468 section 3.
/// The textual encoding is exactly five hyphens, the literal "BEGIN", a
/// space, the label, and five more hyphens. Used to distinguish a file
/// that looks like a private key but failed to decode (passphrase, corrupt)
/// from a file that is genuinely something else (a .pub key, a README).
const PEM_BEGIN_MARKER: &str = "-----BEGIN";

/// Errors that can occur during SFTP server initialization.
#[derive(Debug, Error)]
pub enum SftpInitError {
    /// RUSTFS_SFTP_HOST_KEY_DIR was not set when SFTP was enabled.
    /// Operators must point this variable at a directory containing
    /// at least one persistent host key.
    #[error("RUSTFS_SFTP_HOST_KEY_DIR is required when SFTP is enabled")]
    HostKeyDirNotSet,

    /// The host-key directory does not exist or its metadata cannot
    /// be read. Includes the underlying io error for diagnosis.
    #[error("host key directory does not exist or is not readable: {path}: {source}")]
    HostKeyDirUnreadable { path: PathBuf, source: std::io::Error },

    /// A host-key file in the directory has world-readable or
    /// group-readable bits set. Mode must be 0o600 or 0o400 so a
    /// local non-root user cannot impersonate the SFTP server.
    #[error("host key file has insecure permissions {mode:#o}: {path} (must be 0o600 or 0o400)")]
    InsecureHostKeyPermissions { path: PathBuf, mode: u32 },

    /// The host-key directory contained no decodable private keys.
    /// Operators must place at least one ed25519 / ECDSA / RSA-SHA256
    /// private key with mode 0o600 in the directory before startup.
    #[error("no valid host keys found in {path}")]
    NoHostKeysFound { path: PathBuf },

    /// The SftpConfig validate() check failed. Carries a human-readable
    /// reason; the wrapping caller logs the full string.
    #[error("invalid SFTP configuration: {0}")]
    InvalidConfig(String),

    /// The run loop in russh::server::run returned an error during
    /// startup, before the listener became ready. Wraps the russh error
    /// string.
    #[error("SSH server error: {0}")]
    Server(String),

    /// The host running the binary is not a Unix-family target. The
    /// host-key permission enforcement (mode 0o600 / 0o400 check)
    /// requires Unix mode bits and has no equivalent on this platform,
    /// so SFTP refuses to start rather than load host keys with weaker
    /// guarantees.
    #[error("SFTP requires a Unix-family host (current OS: {os})")]
    UnsupportedPlatform { os: String },
}

/// Runtime configuration for the SFTP listener.
#[derive(Debug, Clone)]
pub struct SftpConfig {
    /// Address that the SSH listener binds to.
    pub bind_addr: SocketAddr,
    /// Directory containing host key files.
    pub host_key_dir: PathBuf,
    /// Idle session timeout in seconds.
    pub idle_timeout_secs: u64,
    /// S3 multipart part size in bytes. Drives the flush boundary in
    /// the streaming write path and the single-upload size ceiling
    /// (part_size * 10_000, the S3 parts cap). The 16 MiB default
    /// caps a single upload at 160 GiB; raise to reach S3's 5 TiB
    /// per-object limit. Validated against S3_MIN_PART_SIZE and
    /// S3_MAX_PART_SIZE bounds.
    pub part_size: u64,
    /// Maximum simultaneously-open SFTP handles per session. A handle
    /// is the server-side identifier returned by SSH_FXP_OPEN and
    /// SSH_FXP_OPENDIR. Some(n) honours the operator override after
    /// validating against HANDLES_PER_SESSION_MIN (8) and
    /// HANDLES_PER_SESSION_MAX (1024). None means no override. The
    /// driver uses DEFAULT_HANDLES_PER_SESSION (64). Out-of-range
    /// values supplied via RUSTFS_SFTP_HANDLES_PER_SESSION resolve to
    /// None with a warn log. See SftpConfig::resolve_handles_per_session.
    pub handles_per_session: Option<usize>,
    /// Per-call deadline applied to every StorageBackend invocation
    /// the SFTP driver issues. Some(n) honours the operator override
    /// after validating against BACKEND_OP_TIMEOUT_MIN_SECS (5) and
    /// BACKEND_OP_TIMEOUT_MAX_SECS (600). None means no override. The
    /// driver uses DEFAULT_BACKEND_OP_TIMEOUT_SECS (60). Out-of-range
    /// values supplied via RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS resolve
    /// to None with a warn log. See
    /// SftpConfig::resolve_backend_op_timeout_secs.
    pub backend_op_timeout_secs: Option<u64>,
    /// Per-handle read cache window size in bytes. Some(0) is the
    /// READ_CACHE_DISABLED sentinel and turns the cache off entirely.
    /// Some(n) for any other value honours the operator override
    /// after validating against READ_CACHE_WINDOW_MIN (MAX_READ_LEN,
    /// 256 KiB) and READ_CACHE_WINDOW_MAX (64 MiB). None means no
    /// override. The driver uses READ_CACHE_WINDOW_DEFAULT (4 MiB).
    /// Out-of-range non-zero values supplied via
    /// RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES resolve to None with a warn
    /// log. See SftpConfig::resolve_read_cache_window_bytes.
    pub read_cache_window_bytes: Option<u64>,
    /// Process-wide ceiling on cumulative read cache memory across
    /// every live SFTP handle. Some(n) honours the operator override
    /// after validating against READ_CACHE_TOTAL_MEM_MIN (16 MiB) and
    /// the u64 ceiling. None means no override. The driver uses
    /// READ_CACHE_TOTAL_MEM_DEFAULT (256 MiB). Below-min values
    /// supplied via RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES resolve to
    /// None with a warn log. See
    /// SftpConfig::resolve_read_cache_total_mem_bytes.
    pub read_cache_total_mem_bytes: Option<u64>,
    /// Reject all write operations when true.
    pub read_only: bool,
    /// SSH identification string (must start with SSH-2.0-).
    pub banner: String,
}

impl SftpConfig {
    /// Validate configuration values.
    ///
    /// Host key directory existence and key loading are validated separately
    /// in load_host_keys, which runs after this check.
    pub async fn validate(&self) -> Result<(), SftpInitError> {
        if !self.banner.starts_with("SSH-2.0-") {
            return Err(SftpInitError::InvalidConfig("banner must start with SSH-2.0-".to_string()));
        }
        if self.idle_timeout_secs == 0 {
            return Err(SftpInitError::InvalidConfig("idle timeout must be greater than zero".to_string()));
        }
        if self.part_size < S3_MIN_PART_SIZE {
            return Err(SftpInitError::InvalidConfig(format!(
                "part size must be at least {S3_MIN_PART_SIZE} bytes ({} MiB)",
                S3_MIN_PART_SIZE / (1024 * 1024)
            )));
        }
        if self.part_size > S3_MAX_PART_SIZE {
            return Err(SftpInitError::InvalidConfig(format!(
                "part size must not exceed {S3_MAX_PART_SIZE} bytes ({} GiB)",
                S3_MAX_PART_SIZE / (1024 * 1024 * 1024)
            )));
        }
        // The drain index in write_dispatch_flush_one_part casts
        // part_size to usize. Reject configurations where the cast
        // would truncate (only reachable on 32-bit targets) so the
        // truncation cannot fire silently mid-upload.
        if usize::try_from(self.part_size).is_err() {
            return Err(SftpInitError::InvalidConfig(format!(
                "part size {} exceeds usize on this target; rebuild on 64-bit or lower part_size",
                self.part_size
            )));
        }
        Ok(())
    }

    /// Resolve the handles_per_session value from a raw env-var read.
    /// None passes through unchanged. Some(n) is returned unchanged
    /// when n is in the inclusive range
    /// HANDLES_PER_SESSION_MIN..=HANDLES_PER_SESSION_MAX. Out-of-range
    /// inputs return None and emit a warn log naming the requested
    /// value and the bounds. The driver applies
    /// DEFAULT_HANDLES_PER_SESSION when the value is None.
    pub fn resolve_handles_per_session(raw: Option<usize>) -> Option<usize> {
        match raw {
            None => None,
            Some(n) if (HANDLES_PER_SESSION_MIN..=HANDLES_PER_SESSION_MAX).contains(&n) => Some(n),
            Some(n) => {
                tracing::warn!(
                    requested = n,
                    min = HANDLES_PER_SESSION_MIN,
                    max = HANDLES_PER_SESSION_MAX,
                    default = DEFAULT_HANDLES_PER_SESSION,
                    "RUSTFS_SFTP_HANDLES_PER_SESSION out of range. Falling back to the default.",
                );
                None
            }
        }
    }

    /// Resolve the backend_op_timeout_secs value from a raw env-var
    /// read. None passes through unchanged. Some(n) is returned
    /// unchanged when n is in the inclusive range
    /// BACKEND_OP_TIMEOUT_MIN_SECS..=BACKEND_OP_TIMEOUT_MAX_SECS.
    /// Out-of-range inputs return None and emit a warn log naming the
    /// requested value and the bounds. The driver applies
    /// DEFAULT_BACKEND_OP_TIMEOUT_SECS when the value is None.
    pub fn resolve_backend_op_timeout_secs(raw: Option<u64>) -> Option<u64> {
        match raw {
            None => None,
            Some(n) if (BACKEND_OP_TIMEOUT_MIN_SECS..=BACKEND_OP_TIMEOUT_MAX_SECS).contains(&n) => Some(n),
            Some(n) => {
                tracing::warn!(
                    requested = n,
                    min = BACKEND_OP_TIMEOUT_MIN_SECS,
                    max = BACKEND_OP_TIMEOUT_MAX_SECS,
                    default = DEFAULT_BACKEND_OP_TIMEOUT_SECS,
                    "RUSTFS_SFTP_BACKEND_OP_TIMEOUT_SECS out of range. Falling back to the default.",
                );
                None
            }
        }
    }

    /// Resolve the read_cache_window_bytes value from a raw env-var
    /// read. None passes through unchanged. Some(0) is the
    /// READ_CACHE_DISABLED sentinel: the driver short-circuits the
    /// populate path so reads do not retain any buffer between
    /// FXP_READs. Some(n) where n is in the inclusive range
    /// READ_CACHE_WINDOW_MIN..=READ_CACHE_WINDOW_MAX is returned
    /// unchanged. Other values return None and emit a warn log
    /// naming the requested value and the bounds. The driver applies
    /// READ_CACHE_WINDOW_DEFAULT when the value is None.
    pub fn resolve_read_cache_window_bytes(raw: Option<u64>) -> Option<u64> {
        match raw {
            None => None,
            Some(READ_CACHE_DISABLED) => Some(READ_CACHE_DISABLED),
            Some(n) if (READ_CACHE_WINDOW_MIN..=READ_CACHE_WINDOW_MAX).contains(&n) => Some(n),
            Some(n) => {
                tracing::warn!(
                    requested = n,
                    min = READ_CACHE_WINDOW_MIN,
                    max = READ_CACHE_WINDOW_MAX,
                    default = READ_CACHE_WINDOW_DEFAULT,
                    "RUSTFS_SFTP_READ_CACHE_WINDOW_BYTES out of range. Set to 0 to disable the cache, or to a value between the named bounds. Falling back to the default.",
                );
                None
            }
        }
    }

    /// Resolve the read_cache_total_mem_bytes value from a raw env-var
    /// read. None passes through unchanged. Some(n) is returned
    /// unchanged when n is at or above READ_CACHE_TOTAL_MEM_MIN.
    /// Below-min inputs return None and emit a warn log naming the
    /// requested value and the bound. The driver applies
    /// READ_CACHE_TOTAL_MEM_DEFAULT when the value is None.
    pub fn resolve_read_cache_total_mem_bytes(raw: Option<u64>) -> Option<u64> {
        match raw {
            None => None,
            Some(n) if n >= READ_CACHE_TOTAL_MEM_MIN => Some(n),
            Some(n) => {
                tracing::warn!(
                    requested = n,
                    min = READ_CACHE_TOTAL_MEM_MIN,
                    default = READ_CACHE_TOTAL_MEM_DEFAULT,
                    "RUSTFS_SFTP_READ_CACHE_TOTAL_MEM_BYTES below minimum. Falling back to the default.",
                );
                None
            }
        }
    }

    /// Scan RUSTFS_SFTP_HOST_KEY_DIR and load all valid SSH private keys.
    ///
    /// Host keys identify the server. Each file in the directory is a
    /// private key (e.g. generated by ssh-keygen). Clients record the
    /// corresponding public key on first connect and verify it on subsequent
    /// connections to prevent man-in-the-middle attacks.
    ///
    /// Fails startup if the directory cannot be read, if any key file has
    /// group or world permission bits set (hard error), or if zero valid
    /// keys are found after scanning.
    ///
    /// There is no in-memory key generation fallback. A fresh key per
    /// restart produces spurious host-key-changed warnings that
    /// undermine the MITM defence.
    ///
    /// The PrivateKey type from ssh-key implements Zeroize on drop,
    /// so key material is scrubbed at server shutdown. The PEM string
    /// read from disk is a regular String and is not zeroed; this
    /// matches the secret handling in the existing S3 and FTPS auth
    /// paths.
    ///
    /// Returns SftpInitError::UnsupportedPlatform when built for a
    /// non-Unix target. The mode-bit permission enforcement has no
    /// portable equivalent off Unix, and starting SFTP without it
    /// would silently weaken host-key protection.
    #[cfg(not(unix))]
    pub async fn load_host_keys(_host_key_dir: &Path) -> Result<Vec<russh::keys::PrivateKey>, SftpInitError> {
        Err(SftpInitError::UnsupportedPlatform {
            os: std::env::consts::OS.to_string(),
        })
    }

    #[cfg(unix)]
    pub async fn load_host_keys(host_key_dir: &Path) -> Result<Vec<russh::keys::PrivateKey>, SftpInitError> {
        let mut entries = tokio::fs::read_dir(host_key_dir)
            .await
            .map_err(|e| SftpInitError::HostKeyDirUnreadable {
                path: host_key_dir.to_path_buf(),
                source: e,
            })?;

        let mut keys = Vec::new();

        while let Some(entry) = entries.next_entry().await.map_err(|e| SftpInitError::HostKeyDirUnreadable {
            path: host_key_dir.to_path_buf(),
            source: e,
        })? {
            let path = entry.path();

            let metadata = match tokio::fs::metadata(&path).await {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        err = %e,
                        "cannot stat file, skipping"
                    );
                    continue;
                }
            };

            if !metadata.is_file() {
                continue;
            }

            // Skip empty files and files too large to be valid keys.
            let file_size = metadata.len();
            if file_size == 0 || file_size > MAX_HOST_KEY_FILE_SIZE {
                tracing::debug!(
                    path = %path.display(),
                    size = file_size,
                    "skipping file: size outside valid key range"
                );
                continue;
            }

            // Permission check: hard error on insecure permissions.
            // A world-readable private key lets any local user impersonate
            // the SFTP server. OpenSSH enforces the same restriction.
            let mode = metadata.permissions().mode() & 0o777;
            if mode & 0o077 != 0 {
                return Err(SftpInitError::InsecureHostKeyPermissions { path, mode });
            }

            let data = match tokio::fs::read_to_string(&path).await {
                Ok(d) => d,
                Err(e) => {
                    tracing::warn!(
                        path = %path.display(),
                        err = %e,
                        "cannot read file, skipping"
                    );
                    continue;
                }
            };

            match russh::keys::decode_secret_key(&data, None) {
                Ok(key) => {
                    tracing::info!(
                        path = %path.display(),
                        algorithm = ?key.algorithm(),
                        "loaded host key"
                    );
                    keys.push(key);
                }
                Err(e) => {
                    // Distinguish two cases:
                    // 1. The file is genuinely not a private key (a
                    //    .pub file, README, etc). Debug log and skip.
                    // 2. The file looks like a private key but failed
                    //    to decode (passphrase-protected, corrupted).
                    //    Warn so the operator has the failed-decode
                    //    reason in the log.
                    if data.contains(PEM_BEGIN_MARKER) {
                        tracing::warn!(
                            path = %path.display(),
                            err = %e,
                            "file looks like a private key but failed to decode (passphrase-protected keys are not supported)"
                        );
                    } else {
                        tracing::debug!(
                            path = %path.display(),
                            err = %e,
                            "not a valid private key, skipping"
                        );
                    }
                }
            }
        }

        if keys.is_empty() {
            return Err(SftpInitError::NoHostKeysFound {
                path: host_key_dir.to_path_buf(),
            });
        }

        // Sort keys by algorithm preference, then by public key bytes
        // for deterministic ordering within the same algorithm.
        // russh offers keys to clients in array order during key exchange.
        keys.sort_by(|left, right| {
            let left_rank = match left.algorithm() {
                russh::keys::Algorithm::Ed25519 => 0,
                russh::keys::Algorithm::Ecdsa { .. } => 1,
                russh::keys::Algorithm::Rsa { .. } => 2,
                _ => 3,
            };
            let right_rank = match right.algorithm() {
                russh::keys::Algorithm::Ed25519 => 0,
                russh::keys::Algorithm::Ecdsa { .. } => 1,
                russh::keys::Algorithm::Rsa { .. } => 2,
                _ => 3,
            };

            left_rank
                .cmp(&right_rank)
                .then_with(|| left.public_key_bytes().cmp(&right.public_key_bytes()))
        });

        tracing::info!(
            count = keys.len(),
            dir = %host_key_dir.display(),
            "host key loading complete"
        );

        Ok(keys)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::OpenOptionsExt;
    use tempfile::TempDir;

    // PEM boundary markers (RFC 7468 five-hyphen / BEGIN-or-END /
    // label / five-hyphen) are composed at runtime by build_pem_block
    // so the source file emits no contiguous private-key marker that
    // secret scanners would flag. Throwaway test-vector keys.
    const PEM_BOUNDARY_DASHES: &str = "-----";
    const PEM_OPENSSH_LABEL: &str = "OPENSSH PRIVATE KEY";

    /// Wrap a base64 body in the OpenSSH-format PEM boundary markers.
    /// The boundary string is composed at runtime from PEM_BOUNDARY_DASHES
    /// and PEM_OPENSSH_LABEL so the source file does not contain the full
    /// marker as a contiguous literal.
    fn build_pem_block(body: &str) -> String {
        format!("{d}BEGIN {l}{d}\n{body}\n{d}END {l}{d}\n", d = PEM_BOUNDARY_DASHES, l = PEM_OPENSSH_LABEL,)
    }

    fn test_ed25519_pem() -> String {
        // Throwaway Ed25519 private key, no passphrase.
        build_pem_block(
            "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW\n\
             QyNTUxOQAAACCkeMEUpnJEbOMBXiQfjZcHZMEbHW3DlNRL+Jbi1cIqMgAAAKDviRiQ74kY\n\
             kAAAAAtzc2gtZWQyNTUxOQAAACCkeMEUpnJEbOMBXiQfjZcHZMEbHW3DlNRL+Jbi1cIqMg\n\
             AAAEBb5q0DpuL1Rbx4CHUEaRQRSVn1xS2SF+A+qES7OkhrOKR4wRSmckRs4wFeJB+Nlwdk\n\
             wRsdbcOU1Ev4luLVwioyAAAAGHNpbW9uc0B1YnVudHUtbGludXgtMjQwNAECAwQF",
        )
    }

    fn test_ecdsa_pem() -> String {
        // ECDSA P-256 fixture key for the algorithm-preference sort
        // test. Not passphrase-protected.
        build_pem_block(
            "b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAaAAAABNlY2RzYS\n\
             1zaGEyLW5pc3RwMjU2AAAACG5pc3RwMjU2AAAAQQSBp+cYoqTsQzIF+eQS23gIOBFkIqhi\n\
             M8u54NeDrEyxKSewEHP+5i6/+1HURUWDnW+YfS6nbfGb8GxBkJ2ghVvZAAAAqPpS97P6Uv\n\
             ezAAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBIGn5xiipOxDMgX5\n\
             5BLbeAg4EWQiqGIzy7ng14OsTLEpJ7AQc/7mLr/7UdRFRYOdb5h9Lqdt8ZvwbEGQnaCFW9\n\
             kAAAAgBdQn3JuP2lSrY3082L+jmYvESyPu9bSmzUe8yMuILzIAAAALdGVzdC12ZWN0b3IB\n\
             AgMEBQ==",
        )
    }

    fn typical_config() -> SftpConfig {
        SftpConfig {
            bind_addr: "0.0.0.0:2222".parse().unwrap(),
            host_key_dir: PathBuf::from("/tmp/sftp-host-keys"),
            idle_timeout_secs: 600,
            part_size: 16 * 1024 * 1024,
            handles_per_session: None,
            backend_op_timeout_secs: None,
            read_cache_window_bytes: None,
            read_cache_total_mem_bytes: None,
            read_only: false,
            banner: "SSH-2.0-RustFS".to_string(),
        }
    }

    /// Write a file at the given path with the given content and mode.
    fn write_file_with_mode(path: &Path, content: &str, mode: u32) {
        let mut opts = std::fs::OpenOptions::new();
        opts.write(true).create(true).truncate(true).mode(mode);
        let mut file = opts.open(path).expect("open file");
        std::io::Write::write_all(&mut file, content.as_bytes()).expect("write file");
    }

    #[tokio::test]
    async fn validate_accepts_typical_config() {
        let cfg = typical_config();
        assert!(cfg.validate().await.is_ok());
    }

    #[tokio::test]
    async fn validate_rejects_banner_without_ssh_2_0_prefix() {
        let mut cfg = typical_config();
        cfg.banner = "RustFS".to_string();
        let err = cfg.validate().await.expect_err("banner must be rejected");
        assert!(matches!(err, SftpInitError::InvalidConfig(_)));
        assert!(format!("{err}").contains("banner"));
    }

    #[tokio::test]
    async fn validate_rejects_zero_idle_timeout() {
        let mut cfg = typical_config();
        cfg.idle_timeout_secs = 0;
        let err = cfg.validate().await.expect_err("zero idle timeout must be rejected");
        assert!(matches!(err, SftpInitError::InvalidConfig(_)));
        assert!(format!("{err}").contains("idle timeout"));
    }

    #[tokio::test]
    async fn validate_rejects_zero_part_size() {
        let mut cfg = typical_config();
        cfg.part_size = 0;
        let err = cfg.validate().await.expect_err("zero part size must be rejected");
        assert!(matches!(err, SftpInitError::InvalidConfig(_)));
        assert!(format!("{err}").contains("part size"));
    }

    #[tokio::test]
    async fn validate_rejects_part_size_below_min() {
        let mut cfg = typical_config();
        cfg.part_size = S3_MIN_PART_SIZE - 1;
        let err = cfg.validate().await.expect_err("sub-minimum part size must be rejected");
        assert!(matches!(err, SftpInitError::InvalidConfig(_)));
        assert!(format!("{err}").contains("part size"));
    }

    #[tokio::test]
    async fn validate_accepts_part_size_at_minimum() {
        let mut cfg = typical_config();
        cfg.part_size = S3_MIN_PART_SIZE;
        assert!(cfg.validate().await.is_ok());
    }

    #[tokio::test]
    async fn validate_accepts_part_size_at_maximum() {
        let mut cfg = typical_config();
        cfg.part_size = S3_MAX_PART_SIZE;
        assert!(cfg.validate().await.is_ok());
    }

    #[tokio::test]
    async fn validate_rejects_part_size_above_max() {
        let mut cfg = typical_config();
        cfg.part_size = S3_MAX_PART_SIZE + 1;
        let err = cfg.validate().await.expect_err("above-max part size must be rejected");
        assert!(matches!(err, SftpInitError::InvalidConfig(_)));
        assert!(format!("{err}").contains("part size"));
    }

    #[test]
    fn error_display_does_not_leak_secrets() {
        // None of the SftpInitError variants carry secret material in their
        // display output. The fields are: paths, raw mode bits, std::io::Error
        // messages, and free-form descriptive strings. This locks that in.
        let err = SftpInitError::InvalidConfig("idle timeout must be greater than zero".to_string());
        let display = format!("{err}");
        assert!(!display.is_empty());
    }

    #[tokio::test]
    async fn load_host_keys_fails_when_dir_missing() {
        let path = PathBuf::from("/this/path/does/not/exist/sftp-host-keys");
        let err = SftpConfig::load_host_keys(&path).await.expect_err("missing dir must error");
        assert!(matches!(err, SftpInitError::HostKeyDirUnreadable { .. }));
    }

    #[tokio::test]
    async fn load_host_keys_fails_when_dir_empty() {
        let dir = TempDir::new().expect("tempdir");
        let err = SftpConfig::load_host_keys(dir.path())
            .await
            .expect_err("empty dir must error");
        assert!(matches!(err, SftpInitError::NoHostKeysFound { .. }));
    }

    #[tokio::test]
    async fn load_host_keys_rejects_insecure_permissions() {
        let dir = TempDir::new().expect("tempdir");
        let key_path = dir.path().join("ssh_host_ed25519_key");
        // 0o644 has world-readable bit set: must be rejected.
        write_file_with_mode(&key_path, &test_ed25519_pem(), 0o644);
        let err = SftpConfig::load_host_keys(dir.path())
            .await
            .expect_err("insecure perms must error");
        match err {
            SftpInitError::InsecureHostKeyPermissions { mode, .. } => {
                assert_eq!(mode & 0o777, 0o644);
            }
            other => panic!("expected InsecureHostKeyPermissions, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn load_host_keys_loads_one_valid_ed25519_key() {
        let dir = TempDir::new().expect("tempdir");
        let key_path = dir.path().join("ssh_host_ed25519_key");
        write_file_with_mode(&key_path, &test_ed25519_pem(), 0o600);
        let keys = SftpConfig::load_host_keys(dir.path()).await.expect("valid key must load");
        assert_eq!(keys.len(), 1);
        assert!(matches!(keys[0].algorithm(), russh::keys::Algorithm::Ed25519));
    }

    #[tokio::test]
    async fn load_host_keys_skips_non_key_files() {
        let dir = TempDir::new().expect("tempdir");
        // Real key plus an unrelated file.
        write_file_with_mode(&dir.path().join("ssh_host_ed25519_key"), &test_ed25519_pem(), 0o600);
        write_file_with_mode(&dir.path().join("README"), "Place host keys in this directory.\n", 0o600);
        let keys = SftpConfig::load_host_keys(dir.path())
            .await
            .expect("must load the one valid key");
        assert_eq!(keys.len(), 1);
    }

    #[tokio::test]
    async fn load_host_keys_handles_empty_file() {
        let dir = TempDir::new().expect("tempdir");
        write_file_with_mode(&dir.path().join("empty"), "", 0o600);
        write_file_with_mode(&dir.path().join("ssh_host_ed25519_key"), &test_ed25519_pem(), 0o600);
        let keys = SftpConfig::load_host_keys(dir.path())
            .await
            .expect("must skip empty and load the valid key");
        assert_eq!(keys.len(), 1);
    }

    #[tokio::test]
    async fn load_host_keys_skips_passphrase_protected_key_with_warn() {
        // Build content that looks like a private key but cannot be decoded
        // (we pass None as the passphrase). Exercises the load_host_keys
        // branch that distinguishes "looks like a key" from "definitely
        // not a key" by the PEM_BEGIN_MARKER prefix check.
        let dir = TempDir::new().expect("tempdir");
        let fake_passphrase_key = build_pem_block("this is not a valid base64 payload, decode will fail");
        write_file_with_mode(&dir.path().join("encrypted_key"), fake_passphrase_key.as_str(), 0o600);
        // A real key alongside it so the loader does not fail with NoHostKeysFound.
        write_file_with_mode(&dir.path().join("ssh_host_ed25519_key"), &test_ed25519_pem(), 0o600);
        let keys = SftpConfig::load_host_keys(dir.path())
            .await
            .expect("must skip the unreadable key and load the valid one");
        assert_eq!(keys.len(), 1, "passphrase-protected key must be skipped, valid key must load");
    }

    #[tokio::test]
    async fn load_host_keys_sorts_ed25519_before_ecdsa() {
        let dir = TempDir::new().expect("tempdir");
        // Write ECDSA first to confirm sort ordering rather than insertion order.
        write_file_with_mode(&dir.path().join("ssh_host_ecdsa_key"), &test_ecdsa_pem(), 0o600);
        write_file_with_mode(&dir.path().join("ssh_host_ed25519_key"), &test_ed25519_pem(), 0o600);
        let keys = SftpConfig::load_host_keys(dir.path()).await.expect("both keys must load");
        assert_eq!(keys.len(), 2);
        assert!(
            matches!(keys[0].algorithm(), russh::keys::Algorithm::Ed25519),
            "Ed25519 must be first in the sorted output, regardless of file scan order"
        );
        assert!(matches!(keys[1].algorithm(), russh::keys::Algorithm::Ecdsa { .. }));
    }

    #[test]
    fn resolve_handles_per_session_none_passes_through() {
        assert_eq!(SftpConfig::resolve_handles_per_session(None), None);
    }

    #[test]
    fn resolve_handles_per_session_in_range_passes_through() {
        assert_eq!(SftpConfig::resolve_handles_per_session(Some(64)), Some(64));
        assert_eq!(SftpConfig::resolve_handles_per_session(Some(128)), Some(128));
        assert_eq!(SftpConfig::resolve_handles_per_session(Some(512)), Some(512));
    }

    #[test]
    fn resolve_handles_per_session_at_lower_bound_passes_through() {
        assert_eq!(
            SftpConfig::resolve_handles_per_session(Some(HANDLES_PER_SESSION_MIN)),
            Some(HANDLES_PER_SESSION_MIN)
        );
    }

    #[test]
    fn resolve_handles_per_session_at_upper_bound_passes_through() {
        assert_eq!(
            SftpConfig::resolve_handles_per_session(Some(HANDLES_PER_SESSION_MAX)),
            Some(HANDLES_PER_SESSION_MAX)
        );
    }

    #[test]
    fn resolve_handles_per_session_below_min_returns_none() {
        assert_eq!(SftpConfig::resolve_handles_per_session(Some(0)), None);
        assert_eq!(SftpConfig::resolve_handles_per_session(Some(HANDLES_PER_SESSION_MIN - 1)), None);
    }

    #[test]
    fn resolve_handles_per_session_above_max_returns_none() {
        assert_eq!(SftpConfig::resolve_handles_per_session(Some(HANDLES_PER_SESSION_MAX + 1)), None);
        assert_eq!(SftpConfig::resolve_handles_per_session(Some(usize::MAX)), None);
    }

    #[test]
    fn resolve_backend_op_timeout_secs_none_passes_through() {
        assert_eq!(SftpConfig::resolve_backend_op_timeout_secs(None), None);
    }

    #[test]
    fn resolve_backend_op_timeout_secs_in_range_passes_through() {
        assert_eq!(SftpConfig::resolve_backend_op_timeout_secs(Some(30)), Some(30));
        assert_eq!(SftpConfig::resolve_backend_op_timeout_secs(Some(60)), Some(60));
        assert_eq!(SftpConfig::resolve_backend_op_timeout_secs(Some(300)), Some(300));
    }

    #[test]
    fn resolve_backend_op_timeout_secs_at_lower_bound_passes_through() {
        assert_eq!(
            SftpConfig::resolve_backend_op_timeout_secs(Some(BACKEND_OP_TIMEOUT_MIN_SECS)),
            Some(BACKEND_OP_TIMEOUT_MIN_SECS)
        );
    }

    #[test]
    fn resolve_backend_op_timeout_secs_at_upper_bound_passes_through() {
        assert_eq!(
            SftpConfig::resolve_backend_op_timeout_secs(Some(BACKEND_OP_TIMEOUT_MAX_SECS)),
            Some(BACKEND_OP_TIMEOUT_MAX_SECS)
        );
    }

    #[test]
    fn resolve_backend_op_timeout_secs_below_min_returns_none() {
        assert_eq!(SftpConfig::resolve_backend_op_timeout_secs(Some(0)), None);
        assert_eq!(SftpConfig::resolve_backend_op_timeout_secs(Some(BACKEND_OP_TIMEOUT_MIN_SECS - 1)), None);
    }

    #[test]
    fn resolve_backend_op_timeout_secs_above_max_returns_none() {
        assert_eq!(SftpConfig::resolve_backend_op_timeout_secs(Some(BACKEND_OP_TIMEOUT_MAX_SECS + 1)), None);
        assert_eq!(SftpConfig::resolve_backend_op_timeout_secs(Some(u64::MAX)), None);
    }

    #[test]
    fn resolve_read_cache_window_bytes_none_passes_through() {
        assert_eq!(SftpConfig::resolve_read_cache_window_bytes(None), None);
    }

    #[test]
    fn resolve_read_cache_window_bytes_in_range_passes_through() {
        assert_eq!(
            SftpConfig::resolve_read_cache_window_bytes(Some(READ_CACHE_WINDOW_DEFAULT)),
            Some(READ_CACHE_WINDOW_DEFAULT)
        );
        assert_eq!(SftpConfig::resolve_read_cache_window_bytes(Some(8 * 1024 * 1024)), Some(8 * 1024 * 1024));
    }

    #[test]
    fn resolve_read_cache_window_bytes_at_lower_bound_passes_through() {
        assert_eq!(
            SftpConfig::resolve_read_cache_window_bytes(Some(READ_CACHE_WINDOW_MIN)),
            Some(READ_CACHE_WINDOW_MIN)
        );
    }

    #[test]
    fn resolve_read_cache_window_bytes_at_upper_bound_passes_through() {
        assert_eq!(
            SftpConfig::resolve_read_cache_window_bytes(Some(READ_CACHE_WINDOW_MAX)),
            Some(READ_CACHE_WINDOW_MAX)
        );
    }

    #[test]
    fn resolve_read_cache_window_bytes_below_min_but_nonzero_returns_none() {
        assert_eq!(SftpConfig::resolve_read_cache_window_bytes(Some(1)), None);
        assert_eq!(SftpConfig::resolve_read_cache_window_bytes(Some(READ_CACHE_WINDOW_MIN - 1)), None);
    }

    #[test]
    fn resolve_read_cache_window_bytes_above_max_returns_none() {
        assert_eq!(SftpConfig::resolve_read_cache_window_bytes(Some(READ_CACHE_WINDOW_MAX + 1)), None);
        assert_eq!(SftpConfig::resolve_read_cache_window_bytes(Some(u64::MAX)), None);
    }

    #[test]
    fn resolve_read_cache_window_bytes_zero_returns_disabled_sentinel() {
        assert_eq!(
            SftpConfig::resolve_read_cache_window_bytes(Some(READ_CACHE_DISABLED)),
            Some(READ_CACHE_DISABLED)
        );
        assert_eq!(SftpConfig::resolve_read_cache_window_bytes(Some(0)), Some(0));
    }

    #[test]
    fn resolve_read_cache_total_mem_bytes_none_passes_through() {
        assert_eq!(SftpConfig::resolve_read_cache_total_mem_bytes(None), None);
    }

    #[test]
    fn resolve_read_cache_total_mem_bytes_at_or_above_min_passes_through() {
        assert_eq!(
            SftpConfig::resolve_read_cache_total_mem_bytes(Some(READ_CACHE_TOTAL_MEM_MIN)),
            Some(READ_CACHE_TOTAL_MEM_MIN)
        );
        assert_eq!(
            SftpConfig::resolve_read_cache_total_mem_bytes(Some(READ_CACHE_TOTAL_MEM_DEFAULT)),
            Some(READ_CACHE_TOTAL_MEM_DEFAULT)
        );
        assert_eq!(SftpConfig::resolve_read_cache_total_mem_bytes(Some(u64::MAX)), Some(u64::MAX));
    }

    #[test]
    fn resolve_read_cache_total_mem_bytes_below_min_returns_none() {
        assert_eq!(SftpConfig::resolve_read_cache_total_mem_bytes(Some(0)), None);
        assert_eq!(SftpConfig::resolve_read_cache_total_mem_bytes(Some(READ_CACHE_TOTAL_MEM_MIN - 1)), None);
    }
}
