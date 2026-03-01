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

//! Gzip compression helper for old log files.
//!
//! Files are compressed in place: `<name>` → `<name>.gz`.  The original file
//! is **not** deleted here — deletion is handled by the caller after
//! compression succeeds.

use flate2::Compression;
use flate2::write::GzEncoder;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;
use tracing::{debug, info};

/// Compress `path` to `<path>.gz` using gzip.
///
/// If a `.gz` file for the given path already exists the function returns
/// `Ok(())` immediately without overwriting the existing archive.
///
/// # Arguments
/// * `path`    - Path to the uncompressed log file.
/// * `level`   - Gzip compression level (`1`–`9`); clamped automatically.
/// * `dry_run` - When `true`, log what would be done without writing anything.
///
/// # Errors
/// Propagates any I/O error encountered while opening, reading, writing, or
/// flushing files.
pub(super) fn compress_file(path: &Path, level: u32, dry_run: bool) -> Result<(), std::io::Error> {
    let gz_path = path.with_extension("gz");

    if gz_path.exists() {
        debug!("Compressed file already exists, skipping: {:?}", gz_path);
        return Ok(());
    }

    if dry_run {
        info!("[DRY RUN] Would compress file: {:?} -> {:?}", path, gz_path);
        return Ok(());
    }

    let input = File::open(path)?;
    let output = File::create(&gz_path)?;

    let mut reader = BufReader::new(input);
    let mut writer = BufWriter::new(output);

    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level.clamp(1, 9)));
    std::io::copy(&mut reader, &mut encoder)?;
    let compressed = encoder.finish()?;

    writer.write_all(&compressed)?;
    writer.flush()?;

    debug!(
        "Compressed {:?} -> {:?} ({} bytes -> {} bytes)",
        path,
        gz_path,
        std::fs::metadata(path).map(|m| m.len()).unwrap_or(0),
        compressed.len()
    );

    Ok(())
}
