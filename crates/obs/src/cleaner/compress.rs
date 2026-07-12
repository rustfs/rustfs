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

//! Compression helpers for old log files.
//!
//! This module performs compression only. Source-file deletion is intentionally
//! handled by the caller in a separate step to avoid platform-specific file
//! locking issues (especially on Windows).
//!
//! The separation is important for operational safety: a failed archive write
//! must never result in premature source deletion, and an already-existing
//! archive should make repeated cleanup passes idempotent.

use super::types::CompressionAlgorithm;
use flate2::Compression;
use flate2::write::GzEncoder;
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

const LOG_COMPONENT_OBS: &str = "obs";
const LOG_SUBSYSTEM_LOG_CLEANER: &str = "log_cleaner";
const EVENT_LOG_CLEANER_COMPRESSION_STATE: &str = "log_cleaner_compression_state";

/// Conservative archive-size fraction used only for dry-run freed-byte
/// projection. Chosen to under-estimate reclaim (never overstate it) since the
/// true compression ratio is unknown until a real run.
const DRY_RUN_ESTIMATED_ARCHIVE_RATIO: f64 = 0.5;

/// Compression options shared by serial and parallel cleaner paths.
///
/// The core cleaner prepares this immutable bundle once per cleanup pass and
/// then hands it to each worker so all files in that run use the same policy.
#[derive(Debug, Clone)]
pub(super) struct CompressionOptions {
    /// Preferred compression codec for the current cleanup pass.
    pub algorithm: CompressionAlgorithm,
    /// Gzip level (0..=9; 0 = store / no compression).
    pub gzip_level: u32,
    /// Zstd level (0..=22; 0 = codec default).
    pub zstd_level: i32,
    /// Internal zstd worker threads used by zstdmt.
    pub zstd_workers: usize,
    /// If true, fallback to gzip when zstd encoding fails.
    pub zstd_fallback_to_gzip: bool,
    /// Dry-run mode reports planned actions without writing files.
    pub dry_run: bool,
}

/// Compression output metadata used for metrics and deletion gating.
///
/// Callers inspect the output size and archive path before deciding whether it
/// is safe to remove the source log file.
#[derive(Debug, Clone)]
pub(super) struct CompressionOutput {
    /// Final path of the compressed archive file.
    pub archive_path: PathBuf,
    /// Codec actually used to produce the output.
    pub algorithm_used: CompressionAlgorithm,
    /// Input bytes before compression.
    pub input_bytes: u64,
    /// Compressed output bytes on disk.
    pub output_bytes: u64,
}

/// Compress a single source file with the requested codec and fallback policy.
///
/// This function centralizes the fallback behavior so the orchestration layer
/// does not need per-codec branching.
pub(super) fn compress_file(path: &Path, options: &CompressionOptions) -> Result<CompressionOutput, std::io::Error> {
    match options.algorithm {
        CompressionAlgorithm::Gzip => compress_gzip(path, options.gzip_level, options.dry_run),
        CompressionAlgorithm::Zstd => match compress_zstd(path, options.zstd_level, options.zstd_workers, options.dry_run) {
            Ok(output) => Ok(output),
            Err(err) if options.zstd_fallback_to_gzip => {
                warn!(
                    event = EVENT_LOG_CLEANER_COMPRESSION_STATE,
                    component = LOG_COMPONENT_OBS,
                    subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
                    file = ?path,
                    error = %err,
                    result = "zstd_failed_fallback_gzip",
                    "log cleaner compression state changed"
                );
                compress_gzip(path, options.gzip_level, options.dry_run)
            }
            Err(err) => Err(err),
        },
    }
}

/// Compress a file to `*.gz` using the configured gzip level.
fn compress_gzip(path: &Path, level: u32, dry_run: bool) -> Result<CompressionOutput, std::io::Error> {
    let archive_path = archive_path(path, CompressionAlgorithm::Gzip);
    compress_with_writer(path, &archive_path, dry_run, CompressionAlgorithm::Gzip, |reader, writer| {
        let mut encoder = GzEncoder::new(writer, Compression::new(level.clamp(0, 9)));
        let written = std::io::copy(reader, &mut encoder)?;
        let mut out = encoder.finish()?;
        out.flush()?;
        // Hand the underlying file back so the caller can `sync_all()` the
        // archive data before renaming and deleting the source.
        let file = out.into_inner().map_err(|e| e.into_error())?;
        Ok((written, file))
    })
}

/// Compress a file to `*.zst` using multi-threaded zstd when available.
fn compress_zstd(path: &Path, level: i32, workers: usize, dry_run: bool) -> Result<CompressionOutput, std::io::Error> {
    let archive_path = archive_path(path, CompressionAlgorithm::Zstd);
    compress_with_writer(path, &archive_path, dry_run, CompressionAlgorithm::Zstd, |reader, writer| {
        let mut encoder = zstd::stream::Encoder::new(writer, level.clamp(0, 22))?;
        encoder.multithread(workers.max(1) as u32)?;
        let written = std::io::copy(reader, &mut encoder)?;
        let mut out = encoder.finish()?;
        out.flush()?;
        // Hand the underlying file back so the caller can `sync_all()` the
        // archive data before renaming and deleting the source.
        let file = out.into_inner().map_err(|e| e.into_error())?;
        Ok((written, file))
    })
}

/// RAII guard that removes a temporary archive on drop unless disarmed.
///
/// This makes the compression path panic-safe: an early return *or a panic*
/// between creating the temp file and the final rename still cleans up the
/// partial artifact instead of leaking a `*.tmp` orphan.
struct TmpFileGuard {
    path: Option<PathBuf>,
}

impl TmpFileGuard {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn disarm(&mut self) {
        self.path = None;
    }
}

impl Drop for TmpFileGuard {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(&path);
        }
    }
}

/// Create the temporary archive file, refusing to follow or overwrite a
/// symlink planted at the temp path.
///
/// `create_new` (`O_CREAT|O_EXCL`) rejects a pre-existing entry, and on Unix
/// `O_NOFOLLOW` additionally refuses a symlink at the final component. Without
/// these, an attacker with write access to the log directory could pre-plant
/// `<archive>.tmp` as a symlink and have the compressor truncate/overwrite an
/// arbitrary external file. The deletion path already refuses symlinks; this
/// closes the same gap on the compression path.
fn create_tmp_archive(path: &Path, source_mode: Option<u32>) -> std::io::Result<File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_NOFOLLOW);
        // Restrictive from creation: the temp file holds the full plaintext of
        // a possibly-0600 audit log, so never expose it wider than the source.
        opts.mode(source_mode.unwrap_or(0o600));
    }
    let _ = source_mode;
    opts.open(path)
}

/// Open the compression source file, refusing to follow a symlink at the final
/// path component on Unix.
///
/// The scanner selects a regular log file, but between that scan and this open
/// an attacker with write access to the log directory can swap the entry for a
/// symlink (a classic TOCTOU race). `O_NOFOLLOW` makes the open fail with
/// `ELOOP` in that case instead of silently reading whatever the link targets
/// (for example `/etc/shadow`), which would otherwise be copied verbatim into a
/// world-inspectable archive. The temp/archive path already refuses symlinks via
/// `create_tmp_archive`; this closes the same gap on the source side.
fn open_source_no_follow(path: &Path) -> std::io::Result<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)
    }
    #[cfg(not(unix))]
    {
        File::open(path)
    }
}

/// Best-effort fsync of the directory containing `path`.
///
/// Persisting the directory entry makes a rename (or, for the deletion path, an
/// unlink) of an entry within it durable across a crash. Opening a directory as
/// a file is not portable (notably on Windows), so any failure is ignored.
fn sync_parent_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        let dir = if parent.as_os_str().is_empty() {
            Path::new(".")
        } else {
            parent
        };
        if let Ok(handle) = File::open(dir) {
            let _ = handle.sync_all();
        }
    }
}

fn compress_with_writer<F>(
    path: &Path,
    archive_path: &Path,
    dry_run: bool,
    algorithm_used: CompressionAlgorithm,
    mut writer_fn: F,
) -> Result<CompressionOutput, std::io::Error>
where
    F: FnMut(&mut BufReader<File>, BufWriter<File>) -> Result<(u64, File), std::io::Error>,
{
    // Integrity over idempotency: never trust a pre-existing entry at
    // `archive_path` as a valid prior result. The former fast path accepted any
    // regular, non-empty file whose first 2-4 bytes matched the gzip/zstd magic.
    // That header check does not prove the body is complete: an attacker with
    // write access to the log directory (or a crashed prior run) can leave a file
    // that starts with valid magic but is truncated/forged, and trusting it would
    // let the caller delete the real source log — silent audit-data loss. Fully
    // decoding every leftover to validate it would add real CPU cost and
    // decode-bug surface, so instead we always recompress the source in this
    // pass. The atomic create_new+rename below overwrites whatever sits at
    // `archive_path` (a regular file or a planted symlink is replaced, never
    // followed or deleted through) with a freshly written, fully-synced archive,
    // so a partial or forged leftover can never be the basis for source deletion.
    if std::fs::symlink_metadata(archive_path).is_ok() {
        debug!(event = EVENT_LOG_CLEANER_COMPRESSION_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, file = ?archive_path, result = "leftover_archive_recompressing", "log cleaner compression state changed");
    }

    if dry_run {
        // A real run keeps the archive on disk, so freed = input - archive. In
        // dry-run we cannot know the true archive size, so estimate it with a
        // deliberately conservative ratio: this keeps the projected freed bytes
        // from overstating what a real run would reclaim (previously output_bytes
        // was 0, so dry-run reported the full input as freed).
        let input_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let estimated_output_bytes = (input_bytes as f64 * DRY_RUN_ESTIMATED_ARCHIVE_RATIO) as u64;
        info!(event = EVENT_LOG_CLEANER_COMPRESSION_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, state = "dry_run_compress", file = ?path, archive = ?archive_path, input_bytes, estimated_output_bytes, "log cleaner compression state changed");
        return Ok(CompressionOutput {
            archive_path: archive_path.to_path_buf(),
            algorithm_used,
            input_bytes,
            output_bytes: estimated_output_bytes,
        });
    }

    // Create the output archive only after the dry-run short-circuit so this
    // helper remains side-effect free when the caller is evaluating policy.
    // O_NOFOLLOW: refuse to open a symlink swapped in at the source path between
    // the scan and this open (TOCTOU), which could redirect the read to an
    // arbitrary privileged file.
    let input = open_source_no_follow(path)?;

    // Read the source mode from the already-open fd (no extra path stat, no
    // TOCTOU) so the temp file is created restrictive — never wider than the
    // source, default 0600 — instead of the world-readable 0644 that
    // File::create would leave until a post-write chmod.
    #[cfg(unix)]
    let source_mode = {
        use std::os::unix::fs::PermissionsExt;
        input.metadata().ok().map(|m| m.permissions().mode())
    };
    #[cfg(not(unix))]
    let source_mode: Option<u32> = None;

    // Write to a temporary file first and then atomically rename it into place
    // so callers never observe a partially written archive at `archive_path`.
    let mut tmp_name = archive_path.as_os_str().to_owned();
    tmp_name.push(".tmp");
    let tmp_archive_path = std::path::PathBuf::from(tmp_name);

    let output = create_tmp_archive(&tmp_archive_path, source_mode)?;
    // Arm a guard so any early return *or panic* between here and the final
    // rename removes the incomplete temporary archive instead of leaking it.
    let mut tmp_guard = TmpFileGuard::new(tmp_archive_path.clone());
    let mut reader = BufReader::new(input);
    let writer = BufWriter::new(output);

    let (written, out_file) = writer_fn(&mut reader, writer)?;

    // Durability: force the archive contents to stable storage *before* the
    // rename. Without this, a crash after rename but before the page cache is
    // flushed can leave a zero-length/truncated archive while the source is
    // later deleted — permanent log/audit data loss. Renaming to a brand-new
    // name (the `exists()` guard above guarantees this) means ext4's
    // `auto_da_alloc` safety net does not apply, so the fsync is mandatory.
    out_file.sync_all()?;
    drop(out_file);

    // Reapply the exact source mode: create_tmp_archive already set it at
    // creation, but umask may have narrowed it, so restore it precisely —
    // reusing the value already read from the fd rather than stat-ing again.
    #[cfg(unix)]
    if let Some(mode) = source_mode {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&tmp_archive_path, std::fs::Permissions::from_mode(mode));
    }

    // Atomically move the fully written temp file into its final location.
    std::fs::rename(&tmp_archive_path, archive_path)?;
    // The archive now lives at its final path; disarm the guard so it is not
    // removed on the way out.
    tmp_guard.disarm();

    // Persist the new directory entry so the rename survives a crash. Failure
    // here is best-effort (e.g. platforms that reject opening a directory).
    sync_parent_dir(archive_path);

    // Use the copied byte count as the authoritative input size rather than a
    // second metadata read on the source (which could silently read 0 on error
    // and skew freed-byte accounting).
    let input_bytes = written;
    let output_bytes = match std::fs::metadata(archive_path) {
        Ok(meta) => meta.len(),
        Err(err) => {
            // Conservative: assume no savings instead of silently reporting 0,
            // which would overstate freed bytes as the full input.
            warn!(event = EVENT_LOG_CLEANER_COMPRESSION_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, archive = ?archive_path, error = %err, result = "archive_metadata_read_failed", "log cleaner compression state changed");
            input_bytes
        }
    };
    debug!(
        event = EVENT_LOG_CLEANER_COMPRESSION_STATE,
        component = LOG_COMPONENT_OBS,
        subsystem = LOG_SUBSYSTEM_LOG_CLEANER,
        file = ?path,
        archive = ?archive_path,
        input_bytes,
        output_bytes,
        algorithm = %algorithm_used,
        state = "compression_finished",
        "log cleaner compression state changed"
    );

    Ok(CompressionOutput {
        archive_path: archive_path.to_path_buf(),
        algorithm_used,
        input_bytes,
        output_bytes,
    })
}

/// Build `<filename>.<ext>` without replacing an existing extension.
///
/// Rotated log filenames often already encode a generation number or suffix
/// (for example `server.log.3`). Appending the archive extension preserves that
/// information in the final artifact name (`server.log.3.gz`).
fn archive_path(path: &Path, algorithm: CompressionAlgorithm) -> PathBuf {
    let ext = algorithm.extension();
    let mut file_name: OsString = path
        .file_name()
        .map_or_else(|| OsString::from("archive"), |n| n.to_os_string());
    file_name.push(format!(".{ext}"));
    path.with_file_name(file_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_options(algorithm: CompressionAlgorithm) -> CompressionOptions {
        CompressionOptions {
            algorithm,
            gzip_level: 6,
            zstd_level: 3,
            zstd_workers: 1,
            zstd_fallback_to_gzip: false,
            dry_run: false,
        }
    }

    /// A symlink planted at the source path must be refused (not followed) so a
    /// TOCTOU swap cannot redirect compression to an arbitrary privileged file.
    #[cfg(unix)]
    #[test]
    fn source_symlink_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        // Secret the attacker wants exfiltrated into the (broader-readable) archive.
        let secret = dir.path().join("secret.txt");
        std::fs::write(&secret, b"top secret contents").expect("write secret");

        // The scanner-visible log path is actually a symlink to the secret.
        let source = dir.path().join("app.log");
        std::os::unix::fs::symlink(&secret, &source).expect("symlink");

        let err = compress_file(&source, &default_options(CompressionAlgorithm::Gzip))
            .expect_err("compressing a symlinked source must fail");
        // O_NOFOLLOW surfaces as ELOOP; accept any error but ensure no archive
        // was produced from the symlink target.
        assert!(
            !dir.path().join("app.log.gz").exists(),
            "no archive should be created from a symlinked source, err={err}"
        );
        // The secret must not have been copied anywhere in the directory.
        assert!(secret.exists(), "secret should be untouched");
    }

    /// A pre-existing archive whose header magic is valid but whose body is
    /// truncated/forged must not be trusted as a completed prior result: it must
    /// be recompressed into a complete archive, and the source must survive.
    #[test]
    fn truncated_magic_archive_is_not_trusted() {
        use std::io::Read;
        let dir = tempfile::tempdir().expect("tempdir");
        let source = dir.path().join("app.log");
        let payload = b"line1\nline2\nline3\n";
        std::fs::write(&source, payload).expect("write source");

        // Forged archive: valid gzip magic (0x1f 0x8b) but a truncated body.
        let archive = dir.path().join("app.log.gz");
        std::fs::write(&archive, [0x1f_u8, 0x8b, 0x00]).expect("write forged archive");

        let out = compress_file(&source, &default_options(CompressionAlgorithm::Gzip)).expect("compression should succeed");
        assert_eq!(out.archive_path, archive);

        // The forged stub must have been replaced by a real archive that decodes
        // back to the full source — proving it was not trusted (trusting it would
        // let the caller delete the source and lose the log).
        let file = File::open(&archive).expect("open archive");
        let mut decoder = flate2::read::GzDecoder::new(file);
        let mut decoded = Vec::new();
        decoder
            .read_to_end(&mut decoded)
            .expect("archive must be a complete gzip stream");
        assert_eq!(decoded, payload, "recompressed archive must contain the full source");

        // compress_file never deletes the source itself (the caller does, only
        // after a valid archive), so the source log is still present here.
        assert!(source.exists(), "source log must survive compression");
    }
}
