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

//! Log file cleanup module
//!
//! Supports:
//! - walkdir directory traversal
//! - Dual strategy cleanup by file count limit + total size limit
//! - Automatic gzip compression of old files (optional)
//! - Batch deletion using fs operations

use flate2::Compression;
use flate2::write::GzEncoder;
use std::fs::File;
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;
use tracing::{debug, error, info, warn};
use walkdir::WalkDir;

/// File information structure for sorting and cleanup decisions
#[derive(Debug, Clone)]
struct FileInfo {
    path: PathBuf,
    size: u64,
    modified: SystemTime,
}

/// Log file cleanup manager
pub struct LogCleaner {
    log_dir: PathBuf,
    file_prefix: String,
    keep_count: usize,
    max_total_size_bytes: u64,
    compress_old_files: bool,
    gzip_compression_level: u32,
}

impl LogCleaner {
    /// Create a new log cleaner
    ///
    /// # Arguments
    /// * `log_dir` - Log directory path
    /// * `file_prefix` - Log file prefix to match
    /// * `keep_count` - Minimum number of files to keep
    /// * `max_total_size_bytes` - Maximum total size in bytes (0 = unlimited)
    /// * `compress_old_files` - Whether to compress old files before deletion
    /// * `gzip_compression_level` - Gzip compression level (1-9)
    pub fn new(
        log_dir: PathBuf,
        file_prefix: String,
        keep_count: usize,
        max_total_size_bytes: u64,
        compress_old_files: bool,
        gzip_compression_level: u32,
    ) -> Self {
        Self {
            log_dir,
            file_prefix,
            keep_count,
            max_total_size_bytes,
            compress_old_files,
            gzip_compression_level: gzip_compression_level.clamp(1, 9),
        }
    }

    /// Perform cleanup operation
    ///
    /// Returns the number of files deleted and total bytes freed
    pub fn cleanup(&self) -> Result<(usize, u64), std::io::Error> {
        if !self.log_dir.exists() {
            debug!("Log directory does not exist: {:?}", self.log_dir);
            return Ok((0, 0));
        }

        // Collect all matching log files
        let mut files = self.collect_log_files()?;

        if files.is_empty() {
            debug!("No log files found in directory: {:?}", self.log_dir);
            return Ok((0, 0));
        }

        // Sort by modification time (oldest first)
        files.sort_by_key(|f| f.modified);

        let total_files = files.len();
        let total_size: u64 = files.iter().map(|f| f.size).sum();

        info!(
            "Found {} log files, total size: {} bytes ({:.2} MB)",
            total_files,
            total_size,
            total_size as f64 / 1024.0 / 1024.0
        );

        // Determine files to delete
        let files_to_delete = self.select_files_to_delete(&files, total_size);

        if files_to_delete.is_empty() {
            debug!("No files need to be deleted");
            return Ok((0, 0));
        }

        // Optionally compress files before deletion
        if self.compress_old_files {
            for file_info in &files_to_delete {
                if let Err(e) = self.compress_file(&file_info.path) {
                    warn!("Failed to compress file {:?}: {}", file_info.path, e);
                }
            }
        }

        // Delete files
        let (deleted_count, freed_bytes) = self.delete_files(&files_to_delete)?;

        info!(
            "Cleanup completed: deleted {} files, freed {} bytes ({:.2} MB)",
            deleted_count,
            freed_bytes,
            freed_bytes as f64 / 1024.0 / 1024.0
        );

        Ok((deleted_count, freed_bytes))
    }

    /// Collect all log files matching the prefix
    fn collect_log_files(&self) -> Result<Vec<FileInfo>, std::io::Error> {
        let mut files = Vec::new();

        for entry in WalkDir::new(&self.log_dir)
            .max_depth(1)
            .follow_links(false)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();

            // Skip directories
            if !path.is_file() {
                continue;
            }

            // Check if filename matches prefix
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if !filename.starts_with(&self.file_prefix) {
                    continue;
                }

                // Skip already compressed files
                if filename.ends_with(".gz") {
                    continue;
                }

                // Get file metadata
                if let Ok(metadata) = entry.metadata()
                    && let Ok(modified) = metadata.modified()
                {
                    files.push(FileInfo {
                        path: path.to_path_buf(),
                        size: metadata.len(),
                        modified,
                    });
                }
            }
        }

        Ok(files)
    }

    /// Select files to delete based on count and size limits
    fn select_files_to_delete(&self, files: &[FileInfo], total_size: u64) -> Vec<FileInfo> {
        let mut to_delete = Vec::new();

        // Always keep at least keep_count files
        if files.len() <= self.keep_count {
            return to_delete;
        }

        let mut current_size = total_size;
        let can_delete_count = files.len() - self.keep_count;

        // Delete oldest files first
        for (idx, file_info) in files.iter().enumerate() {
            // Don't delete if we're at the minimum count
            if idx >= can_delete_count {
                break;
            }

            // Check if we need to delete based on size limit
            let should_delete = if self.max_total_size_bytes > 0 {
                current_size > self.max_total_size_bytes
            } else {
                false
            };

            if should_delete {
                to_delete.push(file_info.clone());
                current_size = current_size.saturating_sub(file_info.size);
            } else {
                // If size limit is met, stop deleting
                break;
            }
        }

        to_delete
    }

    /// Compress a file using gzip
    fn compress_file(&self, path: &Path) -> Result<(), std::io::Error> {
        let compressed_path = path.with_extension("gz");

        // Skip if compressed file already exists
        if compressed_path.exists() {
            return Ok(());
        }

        let input_file = File::open(path)?;
        let output_file = File::create(&compressed_path)?;

        let mut reader = BufReader::new(input_file);
        let mut writer = BufWriter::new(output_file);

        let compression_level = Compression::new(self.gzip_compression_level);
        let mut encoder = GzEncoder::new(Vec::new(), compression_level);

        std::io::copy(&mut reader, &mut encoder)?;
        let compressed_data = encoder.finish()?;

        writer.write_all(&compressed_data)?;
        writer.flush()?;

        debug!(
            "Compressed {:?} -> {:?} ({} bytes -> {} bytes)",
            path,
            compressed_path,
            std::fs::metadata(path)?.len(),
            compressed_data.len()
        );

        Ok(())
    }

    /// Delete specified files
    fn delete_files(&self, files: &[FileInfo]) -> Result<(usize, u64), std::io::Error> {
        let mut deleted_count = 0;
        let mut freed_bytes = 0;

        for file_info in files {
            match std::fs::remove_file(&file_info.path) {
                Ok(()) => {
                    deleted_count += 1;
                    freed_bytes += file_info.size;
                    debug!("Deleted file: {:?}", file_info.path);
                }
                Err(e) => {
                    error!("Failed to delete file {:?}: {}", file_info.path, e);
                }
            }
        }

        Ok((deleted_count, freed_bytes))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::TempDir;

    fn create_test_log_file(dir: &Path, name: &str, size: usize) -> Result<PathBuf, std::io::Error> {
        let path = dir.join(name);
        let mut file = File::create(&path)?;
        let data = vec![b'X'; size];
        file.write_all(&data)?;
        file.flush()?;
        Ok(path)
    }

    #[test]
    fn test_log_cleaner_basic() -> Result<(), std::io::Error> {
        let temp_dir = TempDir::new()?;
        let log_dir = temp_dir.path().to_path_buf();

        // Create test files
        create_test_log_file(&log_dir, "app.log.2024-01-01", 1024)?;
        create_test_log_file(&log_dir, "app.log.2024-01-02", 1024)?;
        create_test_log_file(&log_dir, "app.log.2024-01-03", 1024)?;
        create_test_log_file(&log_dir, "other.log", 1024)?; // Should be ignored

        let cleaner = LogCleaner::new(log_dir.clone(), "app.log.".to_string(), 2, 2048, false, 6);

        let (deleted_count, freed_bytes) = cleaner.cleanup()?;

        assert_eq!(deleted_count, 1);
        assert_eq!(freed_bytes, 1024);

        Ok(())
    }

    #[test]
    fn test_log_cleaner_keep_count() -> Result<(), std::io::Error> {
        let temp_dir = TempDir::new()?;
        let log_dir = temp_dir.path().to_path_buf();

        // Create 5 test files
        for i in 1..=5 {
            create_test_log_file(&log_dir, &format!("app.log.2024-01-0{}", i), 1024)?;
        }

        let cleaner = LogCleaner::new(log_dir.clone(), "app.log.".to_string(), 3, 0, false, 6);

        let (deleted_count, _) = cleaner.cleanup()?;

        // Should keep at least 3 files even with size limit 0
        assert_eq!(deleted_count, 0);

        Ok(())
    }

    #[test]
    fn test_collect_log_files() -> Result<(), std::io::Error> {
        let temp_dir = TempDir::new()?;
        let log_dir = temp_dir.path().to_path_buf();

        create_test_log_file(&log_dir, "app.log.2024-01-01", 1024)?;
        create_test_log_file(&log_dir, "app.log.2024-01-02", 2048)?;
        create_test_log_file(&log_dir, "other.log", 512)?;

        let cleaner = LogCleaner::new(log_dir.clone(), "app.log.".to_string(), 1, 0, false, 6);
        let files = cleaner.collect_log_files()?;

        assert_eq!(files.len(), 2);

        Ok(())
    }
}
