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

/// Compression options shared by serial and parallel cleaner paths.
///
/// The core cleaner prepares this immutable bundle once per cleanup pass and
/// then hands it to each worker so all files in that run use the same policy.
#[derive(Debug, Clone)]
pub(super) struct CompressionOptions {
    /// Preferred compression codec for the current cleanup pass.
    pub algorithm: CompressionAlgorithm,
    /// Gzip level (1..=9).
    pub gzip_level: u32,
    /// Zstd level (1..=21).
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
        let mut encoder = GzEncoder::new(writer, Compression::new(level.clamp(1, 9)));
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
        let mut encoder = zstd::stream::Encoder::new(writer, level.clamp(1, 21))?;
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
fn create_tmp_archive(path: &Path) -> std::io::Result<File> {
    let mut opts = std::fs::OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        opts.custom_flags(libc::O_NOFOLLOW);
    }
    opts.open(path)
}

/// Best-effort fsync of the directory containing `path`.
///
/// Persisting the directory entry makes a rename (or, for the deletion path, an
/// unlink) of an entry within it durable across a crash. Opening a directory as
/// a file is not portable (notably on Windows), so any failure is ignored.
fn sync_parent_dir(path: &Path) {
    if let Some(parent) = path.parent() {
        let dir = if parent.as_os_str().is_empty() { Path::new(".") } else { parent };
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
    // Keep idempotent behavior: existing archive means this file has already
    // been handled in a previous cleanup pass.
    if archive_path.exists() {
        debug!(event = EVENT_LOG_CLEANER_COMPRESSION_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, file = ?archive_path, state = "archive_exists", "log cleaner compression state changed");
        let input_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
        let output_bytes = std::fs::metadata(archive_path).map(|m| m.len()).unwrap_or(0);
        return Ok(CompressionOutput {
            archive_path: archive_path.to_path_buf(),
            algorithm_used,
            input_bytes,
            output_bytes,
        });
    }

    let input_bytes = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    if dry_run {
        info!(event = EVENT_LOG_CLEANER_COMPRESSION_STATE, component = LOG_COMPONENT_OBS, subsystem = LOG_SUBSYSTEM_LOG_CLEANER, state = "dry_run_compress", file = ?path, archive = ?archive_path, input_bytes, "log cleaner compression state changed");
        return Ok(CompressionOutput {
            archive_path: archive_path.to_path_buf(),
            algorithm_used,
            input_bytes,
            output_bytes: 0,
        });
    }

    // Create the output archive only after the dry-run short-circuit so this
    // helper remains side-effect free when the caller is evaluating policy.
    let input = File::open(path)?;

    // Write to a temporary file first and then atomically rename it into place
    // so callers never observe a partially written archive at `archive_path`.
    let mut tmp_name = archive_path.as_os_str().to_owned();
    tmp_name.push(".tmp");
    let tmp_archive_path = std::path::PathBuf::from(tmp_name);

    let output = create_tmp_archive(&tmp_archive_path)?;
    // Arm a guard so any early return *or panic* between here and the final
    // rename removes the incomplete temporary archive instead of leaking it.
    let mut tmp_guard = TmpFileGuard::new(tmp_archive_path.clone());
    let mut reader = BufReader::new(input);
    let writer = BufWriter::new(output);

    let (_written, out_file) = writer_fn(&mut reader, writer)?;

    // Durability: force the archive contents to stable storage *before* the
    // rename. Without this, a crash after rename but before the page cache is
    // flushed can leave a zero-length/truncated archive while the source is
    // later deleted — permanent log/audit data loss. Renaming to a brand-new
    // name (the `exists()` guard above guarantees this) means ext4's
    // `auto_da_alloc` safety net does not apply, so the fsync is mandatory.
    out_file.sync_all()?;
    drop(out_file);

    // Preserve Unix mode bits so rotated archives keep the same access policy.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if let Ok(src_meta) = std::fs::metadata(path) {
            let mode = src_meta.permissions().mode();
            let _ = std::fs::set_permissions(&tmp_archive_path, std::fs::Permissions::from_mode(mode));
        }
    }

    // Atomically move the fully written temp file into its final location.
    std::fs::rename(&tmp_archive_path, archive_path)?;
    // The archive now lives at its final path; disarm the guard so it is not
    // removed on the way out.
    tmp_guard.disarm();

    // Persist the new directory entry so the rename survives a crash. Failure
    // here is best-effort (e.g. platforms that reject opening a directory).
    sync_parent_dir(archive_path);

    let output_bytes = std::fs::metadata(archive_path).map(|m| m.len()).unwrap_or(0);
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
