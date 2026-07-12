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
//! This module exposes the high-level [`LogCleaner`] API used by the
//! observability layer to manage rotated log files after they are no longer
//! active. The implementation is intentionally split into small focused
//! sub-modules so each concern can evolve independently without turning the
//! cleaner into a monolithic state machine.
//!
//! At a high level, one cleanup pass follows this lifecycle:
//!
//! 1. scan the target directory and classify matching entries;
//! 2. decide which regular log files exceed retention constraints;
//! 3. optionally compress those files using the configured codec;
//! 4. delete only files that are safe to remove;
//! 5. separately expire already-compressed archives by age.
//!
//! The design favors operational safety over aggressive reclamation:
//! compression and deletion are decoupled, symlinks are rejected on removal,
//! and dry-run mode reports intent without mutating the filesystem.
//!
//! ## Sub-modules
//!
//! | Module      | Responsibility                                           |
//! |-------------|----------------------------------------------------------|
//! | `types`     | Shared enums and metadata (`FileInfo`, match/compression choices) |
//! | `scanner`   | Filesystem traversal, pattern matching, empty-file handling       |
//! | `compress`  | Archive creation helpers for gzip and zstd                       |
//! | `core`      | Selection, parallel/serial processing, secure deletion           |
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

/// Primary entry point for the cleaner subsystem.
///
/// Re-exported from [`core`] so callers can construct a cleaner without
/// knowing the internal module layout.
pub use core::LogCleaner;

#[cfg(test)]
mod tests {
    use super::core::LogCleaner;
    use super::scanner;
    use super::types::{CompressionAlgorithm, FileMatchMode};
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use std::sync::mpsc;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Create a test log file with deterministic contents and size.
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

    fn make_parallel_compress_cleaner(dir: std::path::PathBuf, workers: usize) -> LogCleaner {
        LogCleaner::builder(dir, "app.log.".to_string(), "app.log".to_string())
            .match_mode(FileMatchMode::Prefix)
            .keep_files(0)
            .compress_old_files(true)
            .parallel_compress(true)
            .parallel_workers(workers)
            .min_file_age_seconds(0)
            .delete_empty_files(true)
            .build()
    }

    fn assert_parallel_cleanup_completes(file_count: usize, workers: usize) -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        for i in 0..file_count {
            create_log_file(&dir, &format!("app.log.2024-01-{i:03}"), 256)?;
        }

        let cleaner = make_parallel_compress_cleaner(dir.clone(), workers);
        let (tx, rx) = mpsc::sync_channel(1);
        std::thread::spawn(move || {
            let _ = tx.send(cleaner.cleanup());
        });

        let result = rx
            .recv_timeout(Duration::from_secs(5))
            .expect("parallel cleanup should finish without blocking on the bounded results channel")?;

        let compressed_suffixes = CompressionAlgorithm::compressed_suffixes();
        let archive_count = std::fs::read_dir(&dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                compressed_suffixes.iter().any(|suffix| name.ends_with(suffix))
            })
            .count();
        let original_count = std::fs::read_dir(&dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                name.starts_with("app.log.") && !compressed_suffixes.iter().any(|suffix| name.ends_with(suffix))
            })
            .count();
        let archive_bytes: u64 = std::fs::read_dir(&dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                compressed_suffixes.iter().any(|suffix| name.ends_with(suffix))
            })
            .map(|entry| entry.metadata().map(|metadata| metadata.len()).unwrap_or(0))
            .sum();

        assert_eq!(result.0, file_count, "all rotated logs should be deleted after compression");
        assert_eq!(
            result.1,
            (file_count * 256) as u64 - archive_bytes,
            "freed bytes should exclude archive bytes that still remain on disk"
        );
        assert_eq!(archive_count, file_count, "each rotated log should leave behind one archive");
        assert_eq!(original_count, 0, "compressed source logs should be removed");
        Ok(())
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
        let cleaner = make_cleaner(dir, 2, 2048);
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

        let cleaner = make_cleaner(dir, 3, 0);
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

        // keep_files=1 and max_bytes=1500: deleting one managed file (1024 bytes) leaves
        // a single managed file of 1024 bytes, which satisfies both the file-count and
        // size limits.  "other.log" (different prefix) must never be touched.
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

        let cleaner = LogCleaner::builder(dir, ".rustfs.log".to_string(), "current.log".to_string())
            .match_mode(FileMatchMode::Suffix)
            .keep_files(1)
            .max_total_size_bytes(1024)
            .build();
        let (deleted, freed) = cleaner.cleanup()?;

        assert_eq!(deleted, 1, "should delete exactly one file");
        assert_eq!(freed, 1024);
        Ok(())
    }

    #[test]
    fn test_parallel_cleanup_handles_more_results_than_channel_capacity() -> std::io::Result<()> {
        assert_parallel_cleanup_completes(12, 4)
    }

    #[test]
    fn test_parallel_cleanup_handles_results_within_channel_capacity() -> std::io::Result<()> {
        assert_parallel_cleanup_completes(6, 4)
    }

    // ---- OLC-14: safety/correctness regression coverage ----

    use std::time::SystemTime;

    fn set_mtime_ago(path: &Path, ago: Duration) {
        let when = SystemTime::now() - ago;
        File::options()
            .write(true)
            .open(path)
            .expect("open for mtime")
            .set_modified(when)
            .expect("set mtime");
    }

    #[cfg(unix)]
    #[test]
    fn symlink_matching_pattern_is_never_deleted() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        let outside = TempDir::new()?;
        let target = outside.path().join("precious.txt");
        std::fs::write(&target, b"precious")?;
        // A symlink named to match the managed pattern.
        std::os::unix::fs::symlink(&target, dir.join("app.log.2024-01-01"))?;
        create_log_file(&dir, "app.log.2024-01-02", 1024)?;

        // keep_files=0 would delete every managed *regular* file.
        let cleaner = make_cleaner(dir.clone(), 0, 0);
        let _ = cleaner.cleanup()?;

        assert!(target.exists(), "external symlink target must never be deleted");
        assert!(
            dir.join("app.log.2024-01-01").exists(),
            "symlink entry must survive (skipped via symlink_metadata, not followed)"
        );
        Ok(())
    }

    #[test]
    fn expired_archives_deleted_fresh_kept() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        create_log_file(&dir, "app.log.2024-01-01.gz", 100)?;
        create_log_file(&dir, "app.log.2024-01-02.gz", 100)?;
        set_mtime_ago(&dir.join("app.log.2024-01-01.gz"), Duration::from_secs(2 * 24 * 3600));

        let cleaner = LogCleaner::builder(dir.clone(), "app.log.".to_string(), "app.log".to_string())
            .match_mode(FileMatchMode::Prefix)
            .keep_files(100)
            .compressed_file_retention_days(1)
            .min_file_age_seconds(0)
            .build();
        let (deleted, _) = cleaner.cleanup()?;

        assert_eq!(deleted, 1, "only the aged archive should expire");
        assert!(!dir.join("app.log.2024-01-01.gz").exists());
        assert!(dir.join("app.log.2024-01-02.gz").exists());
        Ok(())
    }

    #[test]
    fn archives_bounded_by_byte_cap_when_retention_disabled() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        create_log_file(&dir, "app.log.2024-01-01.gz", 100)?;
        create_log_file(&dir, "app.log.2024-01-02.gz", 100)?;
        // Age the first so the oldest-first byte-cap trim evicts it.
        set_mtime_ago(&dir.join("app.log.2024-01-01.gz"), Duration::from_secs(100));

        let cleaner = LogCleaner::builder(dir.clone(), "app.log.".to_string(), "app.log".to_string())
            .match_mode(FileMatchMode::Prefix)
            .keep_files(100)
            .compressed_file_retention_days(0) // retention disabled
            .max_total_size_bytes(150) // 200 total > 150 -> trim the oldest
            .min_file_age_seconds(0)
            .build();
        let (deleted, _) = cleaner.cleanup()?;

        assert_eq!(deleted, 1, "byte cap must bound archives even with retention off");
        assert!(!dir.join("app.log.2024-01-01.gz").exists(), "oldest archive trimmed");
        assert!(dir.join("app.log.2024-01-02.gz").exists());
        Ok(())
    }

    #[test]
    fn gz_and_zst_files_classified_as_archives() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        create_log_file(&dir, "app.log.2024-01-01.gz", 100)?;
        create_log_file(&dir, "app.log.2024-01-02.zst", 100)?;
        create_log_file(&dir, "app.log.2024-01-03", 100)?; // plain log

        let result = scanner::scan_log_directory(&dir, "app.log.", Some("app.log"), FileMatchMode::Prefix, &[], 0, false, false)?;
        assert_eq!(result.compressed_archives.len(), 2, "gz and zst are archives");
        assert_eq!(result.logs.len(), 1, "only the plain file is a regular log");
        Ok(())
    }

    #[test]
    fn max_single_file_size_deletes_only_oversized() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        create_log_file(&dir, "app.log.2024-01-01", 100)?;
        create_log_file(&dir, "app.log.2024-01-02", 100)?;
        create_log_file(&dir, "app.log.2024-01-03", 5000)?;

        let cleaner = LogCleaner::builder(dir.clone(), "app.log.".to_string(), "app.log".to_string())
            .match_mode(FileMatchMode::Prefix)
            .keep_files(3)
            .max_total_size_bytes(0)
            .max_single_file_size_bytes(1000)
            .min_file_age_seconds(0)
            .build();
        let (deleted, _) = cleaner.cleanup()?;

        assert_eq!(deleted, 1);
        assert!(!dir.join("app.log.2024-01-03").exists(), "oversized file removed");
        assert!(dir.join("app.log.2024-01-01").exists());
        assert!(dir.join("app.log.2024-01-02").exists());
        Ok(())
    }

    #[test]
    fn min_age_protects_fresh_nonempty_log() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        create_log_file(&dir, "app.log.2024-01-01", 1024)?;

        let cleaner = LogCleaner::builder(dir.clone(), "app.log.".to_string(), "app.log".to_string())
            .match_mode(FileMatchMode::Prefix)
            .keep_files(0)
            .min_file_age_seconds(3600)
            .build();
        let (deleted, _) = cleaner.cleanup()?;

        assert_eq!(deleted, 0, "fresh non-empty log younger than min age must be untouched");
        assert!(dir.join("app.log.2024-01-01").exists());
        Ok(())
    }

    #[test]
    fn active_file_matching_pattern_is_protected() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        // The active file both equals active_filename AND matches the suffix pattern.
        create_log_file(&dir, "current.rustfs.log", 1024)?;
        create_log_file(&dir, "old.rustfs.log", 1024)?;

        let cleaner = LogCleaner::builder(dir.clone(), ".rustfs.log".to_string(), "current.rustfs.log".to_string())
            .match_mode(FileMatchMode::Suffix)
            .keep_files(0)
            .min_file_age_seconds(0)
            .build();
        let _ = cleaner.cleanup()?;

        assert!(dir.join("current.rustfs.log").exists(), "active file must never be deleted");
        assert!(!dir.join("old.rustfs.log").exists(), "non-active rotated log removed");
        Ok(())
    }

    #[test]
    fn invalid_exclude_glob_does_not_abort_build() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        create_log_file(&dir, "app.log.keep", 100)?;
        create_log_file(&dir, "app.log.drop", 100)?;

        // "[invalid" is dropped with a warning; "*keep*" is a valid exclude.
        let cleaner = LogCleaner::builder(dir.clone(), "app.log.".to_string(), "app.log".to_string())
            .match_mode(FileMatchMode::Prefix)
            .keep_files(0)
            .exclude_patterns(vec!["[invalid".to_string(), "*keep*".to_string()])
            .min_file_age_seconds(0)
            .build();
        let _ = cleaner.cleanup()?;

        assert!(dir.join("app.log.keep").exists(), "valid exclude must still protect its file");
        assert!(!dir.join("app.log.drop").exists(), "non-excluded rotated log removed");
        Ok(())
    }

    #[test]
    fn dry_run_with_compression_creates_no_archive() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        create_log_file(&dir, "app.log.2024-01-01", 1024)?;

        let cleaner = LogCleaner::builder(dir.clone(), "app.log.".to_string(), "app.log".to_string())
            .match_mode(FileMatchMode::Prefix)
            .keep_files(0)
            .compress_old_files(true)
            .compression_algorithm(CompressionAlgorithm::Gzip)
            .parallel_compress(false)
            .min_file_age_seconds(0)
            .dry_run(true)
            .build();
        let (deleted, freed) = cleaner.cleanup()?;

        assert!(deleted > 0, "dry-run reports intended deletions");
        assert!(freed > 0, "dry-run reports projected freed bytes");
        assert!(dir.join("app.log.2024-01-01").exists(), "dry-run must not delete");
        assert!(!dir.join("app.log.2024-01-01.gz").exists(), "dry-run must not create an archive");
        Ok(())
    }

    #[test]
    fn gzip_archive_round_trips_and_source_removed() -> std::io::Result<()> {
        use std::io::Read;
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        let content = vec![b'A'; 4096];
        {
            let mut f = File::create(dir.join("app.log.2024-01-01"))?;
            f.write_all(&content)?;
            f.flush()?;
        }

        let cleaner = LogCleaner::builder(dir.clone(), "app.log.".to_string(), "app.log".to_string())
            .match_mode(FileMatchMode::Prefix)
            .keep_files(0)
            .compress_old_files(true)
            .compression_algorithm(CompressionAlgorithm::Gzip)
            .parallel_compress(false)
            .min_file_age_seconds(0)
            .build();
        let _ = cleaner.cleanup()?;

        assert!(!dir.join("app.log.2024-01-01").exists(), "source removed after compression");
        let gz = dir.join("app.log.2024-01-01.gz");
        assert!(gz.exists(), "archive created");
        let mut decoded = Vec::new();
        flate2::read::GzDecoder::new(File::open(&gz)?).read_to_end(&mut decoded)?;
        assert_eq!(decoded, content, "archive must decompress to the original bytes");
        Ok(())
    }

    #[test]
    fn idempotent_archive_branch_trusts_valid_archive() -> std::io::Result<()> {
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        let content = vec![b'B'; 2048];
        let write_source = || -> std::io::Result<()> {
            let mut f = File::create(dir.join("app.log.2024-01-01"))?;
            f.write_all(&content)?;
            f.flush()
        };
        write_source()?;

        let build = || {
            LogCleaner::builder(dir.clone(), "app.log.".to_string(), "app.log".to_string())
                .match_mode(FileMatchMode::Prefix)
                .keep_files(0)
                .compress_old_files(true)
                .compression_algorithm(CompressionAlgorithm::Gzip)
                .parallel_compress(false)
                .min_file_age_seconds(0)
                .build()
        };

        // Pass 1: create the archive and delete the source.
        let _ = build().cleanup()?;
        let gz = dir.join("app.log.2024-01-01.gz");
        assert!(gz.exists());
        let archive_len = std::fs::metadata(&gz)?.len();

        // Recreate the source; pass 2 must trust the existing valid archive and
        // delete the source again without rewriting the archive.
        write_source()?;
        let (deleted, _) = build().cleanup()?;
        assert_eq!(deleted, 1);
        assert!(!dir.join("app.log.2024-01-01").exists(), "source deleted via idempotent branch");
        assert_eq!(std::fs::metadata(&gz)?.len(), archive_len, "existing valid archive unchanged");
        Ok(())
    }
}
