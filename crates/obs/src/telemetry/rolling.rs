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
use jiff::Zoned;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub enum Rotation {
    Minutely,
    Hourly,
    Daily,
    #[allow(dead_code)]
    Never,
}

/// Global per-process counter used to disambiguate archive filenames that
/// may otherwise collide when multiple rotations occur within the same
/// timestamp tick.
static ROLL_UNIQUIFIER: AtomicU64 = AtomicU64::new(0);

impl Rotation {
    fn check_should_roll(&self, last: i64, now: i64) -> bool {
        match self {
            Rotation::Minutely => now / 60 != last / 60,
            Rotation::Hourly => now / 3600 != last / 3600,
            Rotation::Daily => {
                // Align daily rotation with the local day boundary rather than UTC midnight.
                // We shift both timestamps by the current local offset before bucketing into days.
                let offset_secs = Zoned::now().offset().seconds() as i64;
                (now + offset_secs) / 86400 != (last + offset_secs) / 86400
            }
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
    // Store as seconds since Unix epoch
    last_roll_ts: i64,
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
    /// Returns an [`io::Error`] if:
    /// - `filename` is not a plain file name (absolute path, path separators,
    ///   or `..` components are rejected to prevent path traversal).
    /// - The directory cannot be created.
    /// - The active log file cannot be opened/created.
    pub fn new(
        dir: impl AsRef<Path>,
        filename: String,
        rotation: Rotation,
        max_size_bytes: u64,
        match_mode: FileMatchMode,
    ) -> io::Result<Self> {
        // Validate that `filename` is a plain file name: not absolute and no
        // directory components (separators or `..`).  If `file_name()` equals
        // the entire path, there can be no parent-directory traversal.
        {
            let p = Path::new(&filename);
            let is_plain_name = !p.is_absolute() && p.file_name().map(|n| n == p.as_os_str()).unwrap_or(false);
            if !is_plain_name {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    format!("log filename must be a plain file name with no path components, got: {filename:?}"),
                ));
            }
        }

        let mut appender = Self {
            dir: dir.as_ref().to_path_buf(),
            filename,
            rotation,
            max_size_bytes,
            match_mode,
            file: None,
            size: 0,
            last_roll_ts: Zoned::now().timestamp().as_second(),
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

        // Add retry logic to handle transient FS issues (e.g. anti-virus, indexing, or quick rename race)
        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;

        for i in 0..MAX_RETRIES {
            // Open in append mode
            match fs::OpenOptions::new().create(true).append(true).open(&path) {
                Ok(file) => {
                    let meta = file.metadata()?;
                    self.size = meta.len();

                    // Seed `last_roll_ts` from the file's modification time so that a
                    // process restart correctly triggers time-based rotation if the active
                    // file belongs to a previous period.
                    if let Ok(modified) = meta.modified() {
                        // Convert SystemTime to jiff::Timestamp
                        if let Ok(ts) = jiff::Timestamp::try_from(modified) {
                            self.last_roll_ts = ts.as_second();
                        }
                    }

                    self.file = Some(file);
                    return Ok(());
                }
                Err(e) => {
                    last_error = Some(e);
                    // Exponential backoff: 10ms, 20ms, 40ms
                    thread::sleep(Duration::from_millis(10 * (1 << i)));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| io::Error::other("Failed to open log file after retries")))
    }

    fn should_roll(&self, write_len: u64) -> bool {
        // 1. Size-based check (Cheap, check first)
        // If max_size is set (non-zero) and writing would exceed it, roll immediately.
        if self.max_size_bytes > 0 && (self.size + write_len) > self.max_size_bytes {
            return true;
        }

        // 2. Time-based check
        // We check this after size check to avoid unnecessary time calls if size forces a roll.
        let now = Zoned::now().timestamp().as_second();

        self.rotation.check_should_roll(self.last_roll_ts, now)
    }

    fn roll(&mut self) -> io::Result<()> {
        // 1. Close current file first to ensure all buffers are flushed to OS (if any)
        // and handle released.
        if let Some(mut file) = self.file.take()
            && let Err(e) = file.flush() {
                eprintln!("Failed to flush log file before rotation: {}", e);
                return Err(e);
            }

        let active_path = self.active_file_path();
        if !active_path.exists() {
            return Ok(());
        }

        // 2. Generate archive name.
        // Format: YYYYMMDDHHMMSS.uuuuuu (Microsecond/Nanosecond precision)
        // We use jiff's strftime. "%Y%m%d%H%M%S%.6f" gives microsecond precision.
        let now = Zoned::now();
        let timestamp_str = now.strftime("%Y%m%d%H%M%S%.6f").to_string();

        // Add a unique counter to prevent collisions in high-concurrency/fast-rotation scenarios.
        let counter = ROLL_UNIQUIFIER.fetch_add(1, Ordering::Relaxed);

        // Final suffix/prefix part: timestamp + counter
        // Example: 20231027103001.123456-0
        let unique_part = format!("{}-{}", timestamp_str, counter);

        // Match naming strategy with LogCleaner expectations.
        let archive_name = match self.match_mode {
            FileMatchMode::Suffix => {
                // Suffix mode: timestamp BEFORE filename.
                // e.g. rustfs.log -> 20231027103001.123456-0.rustfs.log
                format!("{}.{}", unique_part, self.filename)
            }
            FileMatchMode::Prefix => {
                // Prefix mode: timestamp AFTER filename.
                // e.g. rustfs -> rustfs.20231027103001.123456-0
                format!("{}.{}", self.filename, unique_part)
            }
        };

        // 3. Rename the active file to the archive path.
        let archive_path = self.dir.join(&archive_name);

        // Robust Rename Strategy:
        // On Windows, file locking (e.g. by AV software or indexers) can cause `rename` to fail
        // spuriously with PermissionDenied. We implement a short retry loop with backoff.
        const MAX_RETRIES: u32 = 3;
        let mut last_error = None;

        for i in 0..MAX_RETRIES {
            match fs::rename(&active_path, &archive_path) {
                Ok(_) => {
                    // Success!
                    // 4. Reset state
                    self.size = 0;

                    // 5. Re-open (creates new active file)
                    self.open_file()?;

                    // Explicitly update last_roll_ts to the rotation time.
                    // This overrides whatever open_file() derived from mtime, ensuring
                    // we stick to the logical rotation time.
                    self.last_roll_ts = now.timestamp().as_second();
                    return Ok(());
                }
                Err(e) => {
                    // Decide if we should retry based on error kind
                    let should_retry = match e.kind() {
                        // Windows often returns PermissionDenied for locked files
                        io::ErrorKind::PermissionDenied => true,
                        io::ErrorKind::Interrupted => true,
                        _ => false,
                    };

                    last_error = Some(e);

                    if !should_retry {
                        break;
                    }

                    // Exponential backoff: 10ms, 20ms, 40ms...
                    thread::sleep(Duration::from_millis(10 * (1 << i)));
                }
            }
        }

        // 6. Recovery Failure
        // If we exhausted retries, we MUST NOT lose log data.
        // We re-open the ACTIVE file (which is still there because rename failed).
        // The file will grow beyond max_size, but availability > strict sizing.
        eprintln!(
            "RollingAppender: Failed to rotate log file after {} retries. Error: {:?}",
            MAX_RETRIES, last_error
        );

        // Attempt to re-open existing active file to allow continued writing
        self.open_file()?;

        // Return the error so it can be logged/handled, even though we recovered the handle.
        Err(last_error.unwrap_or_else(|| io::Error::other("Unknown rename error")))
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

    #[test]
    fn test_new_rejects_path_with_separators() {
        let tmp = TempDir::new().unwrap();
        // A filename containing path separators could escape the log directory.
        let result = RollingAppender::new(tmp.path(), "subdir/app.log".to_string(), Rotation::Daily, 0, FileMatchMode::Suffix);
        assert!(result.is_err(), "filename with path separator must be rejected");
    }

    #[test]
    fn test_new_rejects_parent_directory_traversal() {
        let tmp = TempDir::new().unwrap();
        // "../secret.log" would write outside the log directory.
        let result = RollingAppender::new(tmp.path(), "../secret.log".to_string(), Rotation::Daily, 0, FileMatchMode::Suffix);
        assert!(result.is_err(), "parent-directory traversal in filename must be rejected");
    }

    #[test]
    fn test_new_rejects_absolute_path_as_filename() {
        let tmp = TempDir::new().unwrap();
        let result = RollingAppender::new(tmp.path(), "/etc/app.log".to_string(), Rotation::Daily, 0, FileMatchMode::Suffix);
        assert!(result.is_err(), "absolute path as filename must be rejected");
    }

    /// On Windows, backslash is a path separator and must be rejected.
    #[cfg(windows)]
    #[test]
    fn test_new_rejects_backslash_path_separator_on_windows() {
        let tmp = TempDir::new().unwrap();
        let result = RollingAppender::new(tmp.path(), "subdir\\app.log".to_string(), Rotation::Daily, 0, FileMatchMode::Suffix);
        assert!(result.is_err(), "backslash path separator in filename must be rejected on Windows");
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
        // Suffix mode: "<timestamp>-<counter>.app.log"
        // Since timestamp contains digits and we use high precision, checking strictly is hard,
        // but it should definitely NOT be the old unix timestamp format (just digits).
        // It should contain "-" before "app.log" due to our new format.
        assert!(
            archives[0].ends_with(".app.log"),
            "archive should end with '.app.log' in Suffix mode; got '{}'",
            archives[0]
        );
        // Check for new format chars (YMD)
        // 20xx...
        assert!(
            archives[0].starts_with("20"),
            "archive should start with year (20xx); got '{}'",
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
        // Prefix mode: "app.<timestamp>-<counter>"
        assert!(
            archives[0].starts_with("app.20"),
            "archive should start with 'app.20' in Prefix mode; got '{}'",
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
