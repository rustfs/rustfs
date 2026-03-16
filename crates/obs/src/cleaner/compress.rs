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

use super::types::CompressionAlgorithm;
use flate2::Compression;
use flate2::write::GzEncoder;
use std::ffi::OsString;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use tracing::{debug, info, warn};

/// Compression options shared by serial/parallel cleaner paths.
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
pub(super) fn compress_file(path: &Path, options: &CompressionOptions) -> Result<CompressionOutput, std::io::Error> {
    match options.algorithm {
        CompressionAlgorithm::Gzip => compress_gzip(path, options.gzip_level, options.dry_run),
        CompressionAlgorithm::Zstd => match compress_zstd(path, options.zstd_level, options.zstd_workers, options.dry_run) {
            Ok(output) => Ok(output),
            Err(err) if options.zstd_fallback_to_gzip => {
                warn!(
                    file = ?path,
                    error = %err,
                    "zstd compression failed, fallback to gzip"
                );
                compress_gzip(path, options.gzip_level, options.dry_run)
            }
            Err(err) => Err(err),
        },
    }
}

fn compress_gzip(path: &Path, level: u32, dry_run: bool) -> Result<CompressionOutput, std::io::Error> {
    let archive_path = archive_path(path, CompressionAlgorithm::Gzip);
    compress_with_writer(path, &archive_path, dry_run, CompressionAlgorithm::Gzip, |reader, writer| {
        let mut encoder = GzEncoder::new(writer, Compression::new(level.clamp(1, 9)));
        let written = std::io::copy(reader, &mut encoder)?;
        let mut out = encoder.finish()?;
        out.flush()?;
        Ok(written)
    })
}

fn compress_zstd(path: &Path, level: i32, workers: usize, dry_run: bool) -> Result<CompressionOutput, std::io::Error> {
    let archive_path = archive_path(path, CompressionAlgorithm::Zstd);
    compress_with_writer(path, &archive_path, dry_run, CompressionAlgorithm::Zstd, |reader, writer| {
        let mut encoder = zstd::stream::Encoder::new(writer, level.clamp(1, 21))?;
        encoder.multithread(workers.max(1) as u32)?;
        let written = std::io::copy(reader, &mut encoder)?;
        let mut out = encoder.finish()?;
        out.flush()?;
        Ok(written)
    })
}

fn compress_with_writer<F>(
    path: &Path,
    archive_path: &Path,
    dry_run: bool,
    algorithm_used: CompressionAlgorithm,
    mut writer_fn: F,
) -> Result<CompressionOutput, std::io::Error>
where
    F: FnMut(&mut BufReader<File>, BufWriter<File>) -> Result<u64, std::io::Error>,
{
    // Keep idempotent behavior: existing archive means this file has already
    // been handled in a previous cleanup pass.
    if archive_path.exists() {
        debug!(file = ?archive_path, "compressed archive already exists, skipping");
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
        info!("[DRY RUN] Would compress file: {:?} -> {:?}", path, archive_path);
        return Ok(CompressionOutput {
            archive_path: archive_path.to_path_buf(),
            algorithm_used,
            input_bytes,
            output_bytes: 0,
        });
    }

    let input = File::open(path)?;
    let output = File::create(archive_path)?;
    let mut reader = BufReader::new(input);
    let writer = BufWriter::new(output);
    let _ = writer_fn(&mut reader, writer)?;

    // Preserve Unix mode bits so rotated archives keep the same access policy.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;

        if let Ok(src_meta) = std::fs::metadata(path) {
            let mode = src_meta.permissions().mode();
            let _ = std::fs::set_permissions(archive_path, std::fs::Permissions::from_mode(mode));
        }
    }

    let output_bytes = std::fs::metadata(archive_path).map(|m| m.len()).unwrap_or(0);
    debug!(
        file = ?path,
        archive = ?archive_path,
        input_bytes,
        output_bytes,
        algorithm = %algorithm_used,
        "compression finished"
    );

    Ok(CompressionOutput {
        archive_path: archive_path.to_path_buf(),
        algorithm_used,
        input_bytes,
        output_bytes,
    })
}

/// Build `<filename>.<ext>` path without replacing existing extension.
fn archive_path(path: &Path, algorithm: CompressionAlgorithm) -> PathBuf {
    let ext = algorithm.extension();
    let mut file_name: OsString = path
        .file_name()
        .map_or_else(|| OsString::from("archive"), |n| n.to_os_string());
    file_name.push(format!(".{ext}"));
    path.with_file_name(file_name)
}
