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
//! let cleaner = LogCleaner::builder(
//!     PathBuf::from("/var/log/rustfs"),
//!     "rustfs.log.".to_string(),
//!     "rustfs.log".to_string(),
//! )
//! .match_mode(FileMatchMode::Prefix)
//! .keep_files(10)
//! .max_total_size_bytes(2 * 1024 * 1024 * 1024) // 2 GiB
//! .max_single_file_size_bytes(0) // unlimited
//! .compress_old_files(true)
//! .gzip_compression_level(6)
//! .compressed_file_retention_days(30)
//! .exclude_patterns(vec![])
//! .delete_empty_files(true)
//! .min_file_age_seconds(3600) // 1 hour
//! .dry_run(false)
//! .build();
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
        LogCleaner::builder(dir, "app.log.".to_string(), "app.log".to_string())
            .match_mode(FileMatchMode::Prefix)
            .keep_files(keep)
            .max_total_size_bytes(max_bytes)
            .min_file_age_seconds(0) // 0 = no age gate in tests
            .delete_empty_files(true)
            .build()
    }

    #[test]
    fn test_cleanup_removes_oldest_when_over_size() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();

        create_log_file(&dir, "app.log.2024-01-01", 1024)?;
        create_log_file(&dir, "app.log.2024-01-02", 1024)?;
        create_log_file(&dir, "app.log.2024-01-03", 1024)?;
        create_log_file(&dir, "other.log", 1024)?; // not managed

        // Total managed = 3 072 bytes; limit = 2 048; keep_files = 2 → must delete 1.
        let cleaner = make_cleaner(dir.clone(), 2, 2048);
        let (deleted, freed) = cleaner.cleanup()?;

        assert_eq!(deleted, 1, "should delete exactly one file");
        assert_eq!(freed, 1024);
        Ok(())
    }

    #[test]
    fn test_cleanup_respects_keep_files() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();

        for i in 1..=5 {
            create_log_file(&dir, &format!("app.log.2024-01-0{i}"), 1024)?;
        }

        let cleaner = make_cleaner(dir.clone(), 3, 0);
        let (deleted, _) = cleaner.cleanup()?;

        // Updated expectation: keep_files acts as a limit (ceiling), so excess files are deleted.
        assert_eq!(deleted, 2, "keep_files should enforce a maximum file count");
        Ok(())
    }

    #[test]
    fn test_cleanup_ignores_unrelated_files() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();

        create_log_file(&dir, "app.log.2024-01-01", 1024)?;
        create_log_file(&dir, "app.log.2024-01-02", 1024)?;
        create_log_file(&dir, "other.log", 512)?; // different prefix

        // keep_files=1 with no size limit: only the oldest managed file is deleted.
        // max_bytes=1500 means no total-size limit so only the keep_files constraint fires.
        let cleaner = make_cleaner(dir.clone(), 1, 1500);
        let (deleted, _) = cleaner.cleanup()?;

        // "other.log" must not be counted or deleted; only 1 managed file removed.
        assert_eq!(deleted, 1, "only managed files should be deleted");
        assert!(dir.join("other.log").exists(), "unrelated file must not be deleted");
        Ok(())
    }

    #[test]
    fn test_collect_log_files_counts_correctly() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();

        create_log_file(&dir, "app.log.2024-01-01", 1024)?;
        create_log_file(&dir, "app.log.2024-01-02", 2048)?;
        create_log_file(&dir, "other.log", 512)?;

        let result = scanner::scan_log_directory(&dir, "app.log.", Some("app.log"), FileMatchMode::Prefix, &[], 0, true, false)?;
        assert_eq!(result.logs.len(), 2, "scanner should find exactly 2 managed files");
        Ok(())
    }

    #[test]
    fn test_dry_run_does_not_delete() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();

        create_log_file(&dir, "app.log.2024-01-01", 1024)?;
        create_log_file(&dir, "app.log.2024-01-02", 1024)?;
        create_log_file(&dir, "app.log.2024-01-03", 1024)?;

        let cleaner = LogCleaner::builder(dir.clone(), "app.log.".to_string(), "app.log".to_string())
            .match_mode(FileMatchMode::Prefix)
            .keep_files(1)
            .max_total_size_bytes(1024)
            .dry_run(true)
            .build();
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

        let cleaner = LogCleaner::builder(dir.clone(), ".rustfs.log".to_string(), "current.log".to_string())
            .match_mode(FileMatchMode::Suffix)
            .keep_files(1)
            .max_total_size_bytes(1024)
            .build();
        let (deleted, freed) = cleaner.cleanup()?;

        assert_eq!(deleted, 1, "should delete exactly one file");
        assert_eq!(freed, 1024);
        Ok(())
    }
}
