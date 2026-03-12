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

//! A custom rolling file appender that supports both time-based and size-based rotation.
//!
//! This is a lightweight replacement for `tracing_appender::rolling::RollingFileAppender`
//! which only supports time-based rotation. This implementation ensures that active
//! log files do not grow indefinitely by rotating them when they exceed a configured size.

use crate::cleaner::types::FileMatchMode;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Copy)]
pub enum Rotation {
    Minutely,
    Hourly,
    Daily,
    #[allow(dead_code)]
    Never,
}

impl Rotation {
    fn check_should_roll(&self, last: u64, now: u64) -> bool {
        match self {
            Rotation::Minutely => now / 60 != last / 60,
            Rotation::Hourly => now / 3600 != last / 3600,
            Rotation::Daily => now / 86400 != last / 86400,
            Rotation::Never => false,
        }
    }
}

pub struct RollingAppender {
    dir: PathBuf,
    filename: String,
    rotation: Rotation,
    max_size_bytes: u64,
    match_mode: FileMatchMode,

    file: Option<File>,
    size: u64,
    last_roll_ts: u64,
}

impl RollingAppender {
    /// Create and immediately validate a new `RollingAppender`.
    ///
    /// The log directory is created if it does not already exist, and the
    /// active log file is opened (or created) eagerly so that configuration
    /// errors — e.g. an invalid filename — surface at initialisation time
    /// rather than on the first write.
    ///
    /// # Errors
    /// Returns an [`io::Error`] if the directory cannot be created, the file
    /// cannot be opened/created, or the path contains invalid characters.
    pub fn new(
        dir: impl AsRef<Path>,
        filename: String,
        rotation: Rotation,
        max_size_bytes: u64,
        match_mode: FileMatchMode,
    ) -> io::Result<Self> {
        let mut appender = Self {
            dir: dir.as_ref().to_path_buf(),
            filename,
            rotation,
            max_size_bytes,
            match_mode,
            file: None,
            size: 0,
            last_roll_ts: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
        };
        // Eagerly open the file to validate the path and capture accurate
        // initial size / last-roll timestamp.
        appender.open_file()?;
        Ok(appender)
    }

    fn active_file_path(&self) -> PathBuf {
        self.dir.join(&self.filename)
    }

    fn open_file(&mut self) -> io::Result<()> {
        if self.file.is_some() {
            return Ok(());
        }

        let path = self.active_file_path();
        // Ensure directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        // Open in append mode
        let file = fs::OpenOptions::new().create(true).append(true).open(&path)?;

        let meta = file.metadata()?;
        self.size = meta.len();

        // Seed `last_roll_ts` from the file's modification time so that a
        // process restart correctly triggers time-based rotation if the active
        // file belongs to a previous period (e.g. yesterday's file opened
        // after midnight).  If `mtime` is unavailable, fall back to the
        // current time (already set in `new`).
        if let Ok(modified) = meta.modified()
            && let Ok(dur) = modified.duration_since(UNIX_EPOCH) {
                self.last_roll_ts = dur.as_secs();
            }

        self.file = Some(file);
        Ok(())
    }

    fn should_roll(&self, write_len: u64) -> bool {
        // 1. Size-based check (Cheap, check first)
        // If max_size is set (non-zero) and writing would exceed it, roll immediately.
        if self.max_size_bytes > 0 && (self.size + write_len) > self.max_size_bytes {
            return true;
        }

        // 2. Time-based check (Requires syscall for time)
        // We check this after size check to avoid unnecessary time calls if size forces a roll.
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

        self.rotation.check_should_roll(self.last_roll_ts, now)
    }

    fn roll(&mut self) -> io::Result<()> {
        // 1. Close current file first to ensure all buffers are flushed to OS (if any)
        // and handle released.
        self.file = None;

        let active_path = self.active_file_path();
        if !active_path.exists() {
            return Ok(());
        }

        // 2. Generate archive name.
        // Use nanosecond resolution to reduce same-second collision risk
        // (e.g. rapid size-based rotations during log bursts).
        let now_duration = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        let now_ts = now_duration.as_secs();
        let unique_suffix = now_duration.as_nanos();

        // Match naming strategy with LogCleaner expectations.
        let base_archive_name = match self.match_mode {
            FileMatchMode::Suffix => {
                // Suffix mode: timestamp BEFORE filename.
                // e.g. rustfs.log -> 1678886400123456789.rustfs.log
                format!("{}.{}", unique_suffix, self.filename)
            }
            FileMatchMode::Prefix => {
                // Prefix mode: timestamp AFTER filename.
                // e.g. rustfs -> rustfs.1678886400123456789
                format!("{}.{}", self.filename, unique_suffix)
            }
        };

        // 3. Rename with collision fallback.
        // On POSIX, `fs::rename` is atomic but silently overwrites the
        // destination, so we pre-check for existence instead of relying on
        // `ErrorKind::AlreadyExists` (which is only reliable on Windows).
        let mut attempt: u32 = 0;
        loop {
            let archive_name = if attempt == 0 {
                base_archive_name.clone()
            } else {
                format!("{}.{}", base_archive_name, attempt)
            };
            let archive_path = self.dir.join(&archive_name);

            if !archive_path.exists() {
                match fs::rename(&active_path, &archive_path) {
                    Ok(_) => break,
                    Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                        // Race on Windows; bump counter and retry.
                        attempt = attempt.saturating_add(1);
                    }
                    Err(e) => {
                        eprintln!("RollingAppender: Failed to rename log file during rotation: {}", e);
                        return Err(e);
                    }
                }
            } else {
                attempt = attempt.saturating_add(1);
            }

            if attempt > 100 {
                // Exhausted retries; log a warning and give up for this cycle.
                eprintln!("RollingAppender: Could not find a unique archive name after 100 attempts; skipping rotation.");
                return Ok(());
            }
        }

        // 4. Reset state
        self.size = 0;
        self.last_roll_ts = now_ts;

        // 5. Re-open (creates new active file)
        self.open_file()?;

        Ok(())
    }
}

impl Write for RollingAppender {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Ensure file is open
        if self.file.is_none() {
            self.open_file()?;
        }

        // Check rotation
        if self.should_roll(buf.len() as u64)
            && let Err(e) = self.roll()
        {
            // If rotation fails, we log to stderr and try to continue writing to the active file
            // to avoid losing logs if possible.
            eprintln!("RollingAppender: failed to rotate log file: {}", e);
        }

        // Ensure file is open (in case roll closed it and failed to open new one, or open_file failed above)
        if self.file.is_none() {
            self.open_file()?;
        }

        if let Some(file) = &mut self.file {
            let n = file.write(buf)?;
            self.size += n as u64;
            Ok(n)
        } else {
            Err(io::Error::other("Failed to open log file"))
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        if let Some(file) = &mut self.file {
            file.flush()
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    fn count_files(dir: &Path) -> usize {
        fs::read_dir(dir)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().path().is_file())
            .count()
    }

    // ── Construction ──────────────────────────────────────────────────────────

    #[test]
    fn test_new_creates_file_eagerly() {
        let tmp = TempDir::new().unwrap();
        let _appender = RollingAppender::new(tmp.path(), "test.log".to_string(), Rotation::Daily, 0, FileMatchMode::Suffix)
            .expect("should create appender without error");
        assert!(tmp.path().join("test.log").exists(), "active log file should be created on new()");
    }

    #[test]
    fn test_new_invalid_filename_returns_error() {
        let tmp = TempDir::new().unwrap();
        // Null byte is invalid on both Unix and Windows.
        let result = RollingAppender::new(tmp.path(), "invalid\0name.log".to_string(), Rotation::Daily, 0, FileMatchMode::Suffix);
        assert!(result.is_err(), "null byte in filename must produce an error");
    }

    // ── Basic writes ──────────────────────────────────────────────────────────

    #[test]
    fn test_write_stores_content() {
        let tmp = TempDir::new().unwrap();
        let mut appender =
            RollingAppender::new(tmp.path(), "test.log".to_string(), Rotation::Daily, 0, FileMatchMode::Suffix).unwrap();

        appender.write_all(b"hello world\n").expect("write should succeed");
        appender.flush().expect("flush should succeed");

        let content = fs::read_to_string(tmp.path().join("test.log")).unwrap();
        assert_eq!(content, "hello world\n");
    }

    // ── Size-based rotation ────────────────────────────────────────────────────

    #[test]
    fn test_size_rotation_creates_archive() {
        let tmp = TempDir::new().unwrap();
        // Allow only 5 bytes before rotating.
        let mut appender =
            RollingAppender::new(tmp.path(), "app.log".to_string(), Rotation::Never, 5, FileMatchMode::Suffix).unwrap();

        // First write: 5 bytes exactly — no rotation yet.
        appender.write_all(b"12345").expect("write should succeed");
        // Second write: would push past the limit — rotation should occur first.
        appender.write_all(b"abcde").expect("write after rotation should succeed");
        appender.flush().unwrap();

        // There should now be 2 files: the active log + 1 archive.
        assert_eq!(count_files(tmp.path()), 2, "one rotation should have produced one archive");

        // The active file should only contain the second write.
        let content = fs::read_to_string(tmp.path().join("app.log")).unwrap();
        assert_eq!(content, "abcde");
    }

    #[test]
    fn test_multiple_size_rotations_produce_unique_archives() {
        let tmp = TempDir::new().unwrap();
        // Force a rotation on every write of 4+ bytes.
        let mut appender =
            RollingAppender::new(tmp.path(), "app.log".to_string(), Rotation::Never, 3, FileMatchMode::Suffix).unwrap();

        for _ in 0..5 {
            appender.write_all(b"abcd").expect("write should succeed");
        }
        appender.flush().unwrap();

        let file_count = count_files(tmp.path());
        // At least 5 archives (one per rotation) plus the active file.
        assert!(
            file_count >= 5,
            "each burst write should produce a distinct archive; got {file_count} files"
        );
    }

    // ── Archive filename format ────────────────────────────────────────────────

    #[test]
    fn test_suffix_mode_archive_name() {
        let tmp = TempDir::new().unwrap();
        let mut appender =
            RollingAppender::new(tmp.path(), "app.log".to_string(), Rotation::Never, 3, FileMatchMode::Suffix).unwrap();

        appender.write_all(b"1234").expect("write should succeed");
        appender.flush().unwrap();

        let archives: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n != "app.log")
            .collect();

        assert_eq!(archives.len(), 1);
        // Suffix mode: "<nanos>.app.log"
        assert!(
            archives[0].ends_with(".app.log"),
            "archive should end with '.app.log' in Suffix mode; got '{}'",
            archives[0]
        );
    }

    #[test]
    fn test_prefix_mode_archive_name() {
        let tmp = TempDir::new().unwrap();
        let mut appender =
            RollingAppender::new(tmp.path(), "app".to_string(), Rotation::Never, 3, FileMatchMode::Prefix).unwrap();

        appender.write_all(b"1234").expect("write should succeed");
        appender.flush().unwrap();

        let archives: Vec<_> = fs::read_dir(tmp.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .filter(|n| n != "app")
            .collect();

        assert_eq!(archives.len(), 1);
        // Prefix mode: "app.<nanos>"
        assert!(
            archives[0].starts_with("app."),
            "archive should start with 'app.' in Prefix mode; got '{}'",
            archives[0]
        );
    }

    // ── Restart with existing file ─────────────────────────────────────────────

    #[test]
    fn test_restart_with_existing_file_reads_size() {
        let tmp = TempDir::new().unwrap();
        let log_path = tmp.path().join("app.log");
        fs::write(&log_path, b"existing content").unwrap();

        let appender =
            RollingAppender::new(tmp.path(), "app.log".to_string(), Rotation::Daily, 0, FileMatchMode::Suffix).unwrap();

        assert_eq!(
            appender.size,
            b"existing content".len() as u64,
            "size should reflect existing file content"
        );
    }
}
