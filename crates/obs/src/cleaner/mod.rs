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

//! Log-file cleanup subsystem.
//!
//! This module provides [`LogCleaner`], a configurable manager that
//! periodically removes, compresses, or archives old rolling log files.
//!
//! ## Sub-modules
//!
//! | Module      | Responsibility                                           |
//! |-------------|----------------------------------------------------------|
//! | `types`     | Shared data types (`FileInfo`)                           |
//! | `scanner`   | Filesystem traversal — discovers eligible files          |
//! | `compress`  | Gzip compression helper                                  |
//! | `core`      | Core orchestration — selection, compression, deletion    |
//!
//! ## Usage
//!
//! ```no_run
//! use std::path::PathBuf;
//! use rustfs_obs::LogCleaner;
//! use rustfs_obs::types::FileMatchMode;
//!
//! let cleaner = LogCleaner::new(
//!     PathBuf::from("/var/log/rustfs"),
//!     "rustfs.log.".to_string(),
//!     FileMatchMode::Prefix,
//!     10,              // keep_count
//!     2 * 1024 * 1024 * 1024, // max_total_size_bytes (2 GiB)
//!     0,               // max_single_file_size_bytes (unlimited)
//!     true,            // compress_old_files
//!     6,               // gzip_compression_level
//!     30,              // compressed_file_retention_days
//!     vec![],          // exclude_patterns
//!     true,            // delete_empty_files
//!     3600,            // min_file_age_seconds (1 hour)
//!     false,           // dry_run
//! );
//!
//! let (deleted, freed_bytes) = cleaner.cleanup().expect("cleanup failed");
//! println!("Deleted {deleted} files, freed {freed_bytes} bytes");
//! ```

mod compress;
mod core;
mod scanner;
pub mod types;

pub use core::LogCleaner;

#[cfg(test)]
mod tests {
    use super::core::LogCleaner;
    use super::scanner;
    use super::types::FileMatchMode;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use tempfile::TempDir;

    fn create_log_file(dir: &Path, name: &str, size: usize) -> std::io::Result<()> {
        let path = dir.join(name);
        let mut f = File::create(path)?;
        f.write_all(&vec![b'X'; size])?;
        f.flush()
    }

    /// Build a cleaner with sensible test defaults (no compression, no age gate).
    fn make_cleaner(dir: std::path::PathBuf, keep: usize, max_bytes: u64) -> LogCleaner {
        LogCleaner::new(
            dir,
            "app.log.".to_string(),
            FileMatchMode::Prefix,
            keep,
            max_bytes,
            0,          // max_single_file_size_bytes
            false,      // compress_old_files
            6,          // gzip_compression_level
            30,         // compressed_file_retention_days
            Vec::new(), // exclude_patterns
            true,       // delete_empty_files
            0,          // min_file_age_seconds (0 = no age gate in tests)
            false,      // dry_run
        )
    }

    #[test]
    fn test_cleanup_removes_oldest_when_over_size() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();

        create_log_file(&dir, "app.log.2024-01-01", 1024)?;
        create_log_file(&dir, "app.log.2024-01-02", 1024)?;
        create_log_file(&dir, "app.log.2024-01-03", 1024)?;
        create_log_file(&dir, "other.log", 1024)?; // not managed

        // Total managed = 3 072 bytes; limit = 2 048; keep_count = 2 → must delete 1.
        let cleaner = make_cleaner(dir.clone(), 2, 2048);
        let (deleted, freed) = cleaner.cleanup()?;

        assert_eq!(deleted, 1, "should delete exactly one file");
        assert_eq!(freed, 1024);
        Ok(())
    }

    #[test]
    fn test_cleanup_respects_keep_count() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();

        for i in 1..=5 {
            create_log_file(&dir, &format!("app.log.2024-01-0{i}"), 1024)?;
        }

        // No size limit, keep_count = 3 → nothing to delete (5 > 3 but size == 0 limit).
        let cleaner = make_cleaner(dir.clone(), 3, 0);
        let (deleted, _) = cleaner.cleanup()?;
        assert_eq!(deleted, 0, "keep_count prevents deletion when no size limit");
        Ok(())
    }

    #[test]
    fn test_cleanup_ignores_unrelated_files() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();

        create_log_file(&dir, "app.log.2024-01-01", 1024)?;
        create_log_file(&dir, "app.log.2024-01-02", 1024)?;
        create_log_file(&dir, "other.log", 512)?; // different prefix

        let cleaner = make_cleaner(dir.clone(), 1, 512);
        let (deleted, _) = cleaner.cleanup()?;

        // "other.log" must not be counted or deleted.
        assert_eq!(deleted, 1, "only managed files should be deleted");
        Ok(())
    }

    #[test]
    fn test_collect_log_files_counts_correctly() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();

        create_log_file(&dir, "app.log.2024-01-01", 1024)?;
        create_log_file(&dir, "app.log.2024-01-02", 2048)?;
        create_log_file(&dir, "other.log", 512)?;

        let files = scanner::collect_log_files(&dir, "app.log.", FileMatchMode::Prefix, &[], 0, true, false)?;
        assert_eq!(files.len(), 2, "scanner should find exactly 2 managed files");
        Ok(())
    }

    #[test]
    fn test_dry_run_does_not_delete() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();

        create_log_file(&dir, "app.log.2024-01-01", 1024)?;
        create_log_file(&dir, "app.log.2024-01-02", 1024)?;
        create_log_file(&dir, "app.log.2024-01-03", 1024)?;

        let cleaner = LogCleaner::new(
            dir.clone(),
            "app.log.".to_string(),
            FileMatchMode::Prefix,
            1,
            1024,
            0,
            false,
            6,
            30,
            vec![],
            true,
            0,
            true,
        );
        let (deleted, _freed) = cleaner.cleanup()?;

        // dry_run=true reports deletions but doesn't actually remove files.
        assert!(deleted > 0, "dry_run should report files as deleted");
        assert_eq!(std::fs::read_dir(&dir)?.count(), 3, "no files should actually be removed");
        Ok(())
    }

    #[test]
    fn test_cleanup_suffix_matching() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();

        create_log_file(&dir, "2026-03-01-06-21.rustfs.log", 1024)?;
        create_log_file(&dir, "2026-03-01-06-22.rustfs.log", 1024)?;
        create_log_file(&dir, "other.log", 1024)?; // not managed

        let cleaner = LogCleaner::new(
            dir.clone(),
            "rustfs.log".to_string(),
            FileMatchMode::Suffix,
            1,
            1024,
            0,
            false,
            6,
            30,
            vec![],
            true,
            0,
            false,
        );
        let (deleted, freed) = cleaner.cleanup()?;

        assert_eq!(deleted, 1, "should delete exactly one file");
        assert_eq!(freed, 1024);
        Ok(())
    }
}
