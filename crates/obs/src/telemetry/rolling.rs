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
use crate::global::{
    METRIC_LOG_CLEANER_ACTIVE_FILE_SIZE_BYTES, METRIC_LOG_CLEANER_ROTATION_DROPPED_BYTES_TOTAL,
    METRIC_LOG_CLEANER_ROTATION_DURATION_SECONDS, METRIC_LOG_CLEANER_ROTATION_FAILURES_TOTAL, METRIC_LOG_CLEANER_ROTATION_TOTAL,
};
use jiff::{Timestamp, ZonedDateTime, tz::TimeZone};
use metrics::{counter, gauge, histogram};
use rustfs_config::observability::DEFAULT_OBS_LOG_ACTIVE_FILE_ROLL_SIZE_BYTES;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy)]
pub enum Rotation {
    Minutely,
    Hourly,
    Daily,
    #[allow(
        dead_code,
        reason = "constructed only by this file's rolling-appender tests; the lib target cannot see them (backlog#1823)"
    )]
    Never,
}

/// Global per-process counter used to disambiguate archive filenames that
/// may otherwise collide when multiple rotations occur within the same
/// timestamp tick.
static ROLL_UNIQUIFIER: AtomicU64 = AtomicU64::new(0);
const ROTATION_RETRY_COOLDOWN: Duration = Duration::from_secs(5);
const ROTATION_RETRY_MAX_COOLDOWN: Duration = Duration::from_secs(60);
/// Consecutive rotation failures after which the appender tightens its active
/// file hard cap instead of allowing a grace overrun.
const ROTATION_FAILURE_HARD_CAP_STREAK: u32 = 3;
/// Emergency grace active-file byte cap applied immediately on rotation
/// failure when size rolling was explicitly disabled (`max_size_bytes == 0`),
/// tightened to `DEFAULT_OBS_LOG_ACTIVE_FILE_ROLL_SIZE_BYTES` once failures
/// reach `ROTATION_FAILURE_HARD_CAP_STREAK`.
const ROTATION_FAILURE_UNBOUNDED_FALLBACK_CAP_BYTES: u64 = DEFAULT_OBS_LOG_ACTIVE_FILE_ROLL_SIZE_BYTES.saturating_mul(2);

impl Rotation {
    fn check_should_roll(&self, last: i64, now: i64) -> bool {
        match self {
            Rotation::Minutely => now / 60 != last / 60,
            Rotation::Hourly => now / 3600 != last / 3600,
            Rotation::Daily => {
                let last_day = Timestamp::new(last, 0)
                    .ok()
                    .map(|ts| ts.to_zoned_datetime(TimeZone::system()).date());
                let now_day = Timestamp::new(now, 0)
                    .ok()
                    .map(|ts| ts.to_zoned_datetime(TimeZone::system()).date());

                match (last_day, now_day) {
                    (Some(last_day), Some(now_day)) => last_day != now_day,
                    _ => now / 86400 != last / 86400,
                }
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
    rotation_retry_cooldown_until: Option<Instant>,
    rotation_retry_base_cooldown: Duration,
    consecutive_rotation_failures: u32,
    dropped_bytes_due_to_rotation_failure: u64,
    shedding_warning_emitted_for_streak: bool,
    /// Woken after every successful roll so the cleaner can enforce the
    /// directory budget on the file that was just rolled.
    roll_signal: Option<Arc<tokio::sync::Notify>>,
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
            last_roll_ts: ZonedDateTime::now().timestamp().as_second(),
            rotation_retry_cooldown_until: None,
            rotation_retry_base_cooldown: ROTATION_RETRY_COOLDOWN,
            consecutive_rotation_failures: 0,
            dropped_bytes_due_to_rotation_failure: 0,
            shedding_warning_emitted_for_streak: false,
            roll_signal: None,
        };
        // Eagerly open the file to validate the path and capture accurate
        // initial size / last-roll timestamp.
        appender.open_file(true)?;
        Ok(appender)
    }

    /// Notify `signal` after every successful roll.
    pub(crate) fn with_roll_signal(mut self, signal: Arc<tokio::sync::Notify>) -> Self {
        self.roll_signal = Some(signal);
        self
    }

    pub(crate) fn active_file_path(&self) -> PathBuf {
        self.dir.join(&self.filename)
    }

    #[cfg(unix)]
    pub(crate) fn active_file_identity(&self) -> io::Result<(u64, u64)> {
        use std::os::unix::fs::MetadataExt;

        let metadata = self
            .file
            .as_ref()
            .ok_or_else(|| io::Error::other("rolling log file is not open"))?
            .metadata()?;
        Ok((metadata.dev(), metadata.ino()))
    }

    fn open_file(&mut self, seed_last_roll_ts: bool) -> io::Result<()> {
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

                    // Seed `last_roll_ts` from the file's modification time only on initial
                    // startup so a process restart triggers time-based rotation when the
                    // active file belongs to a previous period. Reopening after a failed
                    // rotation must preserve `self.last_roll_ts` so subsequent time-based
                    // rotation retries continue after backoff instead of stalling for the
                    // rest of the period.
                    if seed_last_roll_ts
                        && let Ok(modified) = meta.modified()
                        && let Ok(ts) = jiff::Timestamp::try_from(modified)
                    {
                        self.last_roll_ts = ts.as_second();
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
        if self
            .rotation_retry_cooldown_until
            .is_some_and(|deadline| deadline > Instant::now())
        {
            return false;
        }

        // 1. Size-based check (Cheap, check first)
        // If max_size is set (non-zero) and writing would exceed it, roll immediately.
        if self.max_size_bytes > 0 && self.size.saturating_add(write_len) > self.max_size_bytes {
            return true;
        }

        // 2. Time-based check
        // We check this after size check to avoid unnecessary time calls if size forces a roll.
        let now = ZonedDateTime::now().timestamp().as_second();

        self.rotation.check_should_roll(self.last_roll_ts, now)
    }

    /// Hard ceiling enforced on the active log file while rotation is failing.
    ///
    /// A transient rename failure keeps a bounded grace ceiling (`2 * max_size_bytes`,
    /// or `ROTATION_FAILURE_UNBOUNDED_FALLBACK_CAP_BYTES` when size rolling is off)
    /// from the first failure so brief filesystem locks do not immediately lose logs
    /// while still preventing an uncapped window during backoff. Sustained failures
    /// (`>= ROTATION_FAILURE_HARD_CAP_STREAK`) tighten the cap to `max_size_bytes`
    /// (or `DEFAULT_OBS_LOG_ACTIVE_FILE_ROLL_SIZE_BYTES` when size rolling is off)
    /// so an unrotatable file can never exhaust disk space.
    fn rotation_failure_hard_cap(&self) -> Option<u64> {
        if self.consecutive_rotation_failures == 0 {
            return None;
        }
        if self.max_size_bytes > 0 {
            if self.consecutive_rotation_failures >= ROTATION_FAILURE_HARD_CAP_STREAK {
                Some(self.max_size_bytes)
            } else {
                Some(self.max_size_bytes.saturating_mul(2))
            }
        } else if self.consecutive_rotation_failures >= ROTATION_FAILURE_HARD_CAP_STREAK {
            Some(DEFAULT_OBS_LOG_ACTIVE_FILE_ROLL_SIZE_BYTES)
        } else {
            Some(ROTATION_FAILURE_UNBOUNDED_FALLBACK_CAP_BYTES)
        }
    }

    fn record_rotation_failure(&mut self, rotate_started: Instant) {
        self.consecutive_rotation_failures = self.consecutive_rotation_failures.saturating_add(1);
        self.shedding_warning_emitted_for_streak = false;
        let shift = self.consecutive_rotation_failures.saturating_sub(1).min(4);
        let backoff = self
            .rotation_retry_base_cooldown
            .saturating_mul(1u32 << shift)
            .min(ROTATION_RETRY_MAX_COOLDOWN);
        self.rotation_retry_cooldown_until = Some(Instant::now() + backoff);
        counter!(METRIC_LOG_CLEANER_ROTATION_FAILURES_TOTAL).increment(1);
        histogram!(METRIC_LOG_CLEANER_ROTATION_DURATION_SECONDS).record(rotate_started.elapsed().as_secs_f64());
    }

    fn roll(&mut self) -> io::Result<()> {
        let rotate_started = std::time::Instant::now();
        // 1. Close current file first to ensure all buffers are flushed to OS (if any)
        // and handle released.
        if let Some(mut file) = self.file.take()
            && let Err(e) = file.flush()
        {
            eprintln!("Failed to flush log file before rotation: {}", e);
            self.record_rotation_failure(rotate_started);
            return Err(e);
        }

        let active_path = self.active_file_path();
        if !active_path.exists() {
            return Ok(());
        }

        // 2. Generate archive name.
        // Format: YYYYMMDDHHMMSS.uuuuuu (Microsecond/Nanosecond precision)
        // We use jiff's strftime. "%Y%m%d%H%M%S%.6f" gives microsecond precision.
        let now = ZonedDateTime::now();
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
                    self.open_file(false)?;

                    // Explicitly update last_roll_ts to the rotation time.
                    // This overrides whatever open_file() derived from mtime, ensuring
                    // we stick to the logical rotation time.
                    self.last_roll_ts = now.timestamp().as_second();
                    self.rotation_retry_cooldown_until = None;
                    self.consecutive_rotation_failures = 0;
                    self.shedding_warning_emitted_for_streak = false;
                    counter!(METRIC_LOG_CLEANER_ROTATION_TOTAL).increment(1);
                    histogram!(METRIC_LOG_CLEANER_ROTATION_DURATION_SECONDS).record(rotate_started.elapsed().as_secs_f64());
                    gauge!(METRIC_LOG_CLEANER_ACTIVE_FILE_SIZE_BYTES).set(self.size as f64);
                    if let Some(signal) = &self.roll_signal {
                        signal.notify_one();
                    }
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
        // Re-open the active file so transient rename errors keep logging up to
        // `rotation_failure_hard_cap()`, while sustained failures shed excess
        // writes instead of growing the unrotatable file without bound.
        eprintln!(
            "RollingAppender: Failed to rotate log file after {} retries. Error: {:?}",
            MAX_RETRIES, last_error
        );
        self.record_rotation_failure(rotate_started);

        // Attempt to re-open existing active file to allow continued writing
        // without clobbering `last_roll_ts`.
        self.open_file(false)?;

        // Return the error so it can be logged/handled, even though we recovered the handle.
        Err(last_error.unwrap_or_else(|| io::Error::other("Unknown rename error")))
    }
}

impl Write for RollingAppender {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        // Ensure file is open
        if self.file.is_none() {
            self.open_file(false)?;
        }

        // Check rotation
        if self.should_roll(buf.len() as u64)
            && let Err(e) = self.roll()
        {
            // If rotation fails, we log to stderr and enforce the rotation-failure
            // hard ceiling below instead of allowing unbounded disk growth.
            eprintln!("RollingAppender: failed to rotate log file: {}", e);
        }

        // Shed writes above the active-file hard cap while rotation is broken.
        if let Some(hard_cap) = self.rotation_failure_hard_cap()
            && self.size.saturating_add(buf.len() as u64) > hard_cap
        {
            let dropped = buf.len() as u64;
            self.dropped_bytes_due_to_rotation_failure = self.dropped_bytes_due_to_rotation_failure.saturating_add(dropped);
            counter!(METRIC_LOG_CLEANER_ROTATION_DROPPED_BYTES_TOTAL).increment(dropped);
            if !self.shedding_warning_emitted_for_streak {
                self.shedding_warning_emitted_for_streak = true;
                eprintln!(
                    "RollingAppender: rotation failed {} consecutive times; shedding writes above {} bytes (active size {} bytes, total shed {} bytes) until rotation recovers",
                    self.consecutive_rotation_failures, hard_cap, self.size, self.dropped_bytes_due_to_rotation_failure
                );
            }
            return Ok(buf.len());
        }

        // Ensure file is open (in case roll closed it and failed to open new one, or open_file failed above)
        if self.file.is_none() {
            self.open_file(false)?;
        }

        if let Some(file) = &mut self.file {
            let n = file.write(buf)?;
            self.size += n as u64;
            gauge!(METRIC_LOG_CLEANER_ACTIVE_FILE_SIZE_BYTES).set(self.size as f64);
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

    /// A storm with the shipped roll behavior: the appender rolls by size and
    /// wakes the cleaner, and the directory stays within the byte budget plus
    /// the active file although every rolled file is younger than the age gate.
    #[test]
    fn log_directory_stays_bounded_under_a_storm() -> io::Result<()> {
        use crate::cleaner::LogCleaner;

        const ROLL_SIZE: u64 = 4 * 1024;
        const BUDGET: u64 = 16 * 1024;
        let tmp = TempDir::new()?;
        let dir = tmp.path().to_path_buf();
        let signal = Arc::new(tokio::sync::Notify::new());
        let mut appender =
            RollingAppender::new(&dir, "rustfs.log".to_string(), Rotation::Never, ROLL_SIZE, FileMatchMode::Suffix)?
                .with_roll_signal(signal.clone());
        let cleaner = LogCleaner::builder(dir.clone(), "rustfs.log".to_string(), "rustfs.log".to_string())
            .match_mode(FileMatchMode::Suffix)
            .keep_files(30)
            .max_total_size_bytes(BUDGET)
            .compress_old_files(false)
            .min_file_age_seconds(3600)
            .build();
        let directory_bytes = |dir: &Path| -> io::Result<u64> {
            let mut total = 0;
            for entry in fs::read_dir(dir)? {
                total += entry?.metadata()?.len();
            }
            Ok(total)
        };
        let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build()?;

        let line = [b'x'; 512];
        let mut passes = 0;
        let mut peak = 0;
        // 512 KiB of log lines, 32 times the budget.
        for _ in 0..1024 {
            appender.write_all(&line)?;
            // The cleanup loop runs one pass per reported roll.
            let rolled = runtime.block_on(async { tokio::time::timeout(Duration::ZERO, signal.notified()).await.is_ok() });
            if rolled {
                cleaner.cleanup()?;
                passes += 1;
            }
            peak = peak.max(directory_bytes(&dir)?);
        }
        appender.flush()?;

        assert!(passes > 100, "size rolls woke the cleaner {passes} times");
        // One freshly rolled file may sit above the budget until its pass runs.
        assert!(
            peak <= BUDGET + 2 * ROLL_SIZE,
            "peak directory size {peak} exceeded the budget {BUDGET} plus the active and one rolled file"
        );
        assert!(directory_bytes(&dir)? <= BUDGET + ROLL_SIZE);
        Ok(())
    }

    #[test]
    fn every_successful_roll_wakes_the_roll_signal() {
        let tmp = TempDir::new().unwrap();
        let signal = Arc::new(tokio::sync::Notify::new());
        let mut appender = RollingAppender::new(tmp.path(), "app.log".to_string(), Rotation::Never, 5, FileMatchMode::Suffix)
            .unwrap()
            .with_roll_signal(signal.clone());
        let runtime = tokio::runtime::Builder::new_current_thread().enable_time().build().unwrap();
        let woken = || runtime.block_on(async { tokio::time::timeout(Duration::ZERO, signal.notified()).await.is_ok() });

        appender.write_all(b"12345").expect("write should succeed");
        assert!(!woken(), "no roll yet");
        appender.write_all(b"abcde").expect("write after rotation should succeed");
        assert!(woken(), "the size roll is reported");
        assert!(!woken(), "one roll is one wake-up");
    }

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

    /// Unix timestamp of a wall-clock time in the process-local time zone,
    /// which is the zone `Rotation::Daily` uses to decide the calendar day.
    ///
    /// The fixtures below use mid-January dates, which sit on no DST or
    /// offset transition in any zone, so every wall-clock time exists exactly once.
    fn local_ts(year: i32, month: u32, day: u32, hour: u32, min: u32, sec: u32) -> i64 {
        jiff::civil::DateTime::new(year as i16, month as i8, day as i8, hour as i8, min as i8, sec as i8, 0)
            .expect("fixture local time must be valid")
            .to_zoned_datetime(TimeZone::system())
            .expect("fixture local time must exist exactly once")
            .timestamp()
            .as_second()
    }

    #[test]
    fn test_daily_rotation_uses_calendar_day_boundaries() {
        // Fixed timestamps so the result never depends on when the test runs
        // (a `now()`-based version failed in the first minute after midnight).
        let noon = local_ts(2026, 1, 15, 12, 0, 0);

        // Same calendar day: no rotation.
        assert!(!Rotation::Daily.check_should_roll(noon - 60, noon), "60s earlier on the same day");
        assert!(
            !Rotation::Daily.check_should_roll(local_ts(2026, 1, 15, 0, 0, 0), local_ts(2026, 1, 15, 23, 59, 59)),
            "first and last second of the same day"
        );

        // Previous calendar day: rotate.
        assert!(
            Rotation::Daily.check_should_roll(local_ts(2026, 1, 14, 12, 0, 0), noon),
            "same time of day, one day earlier"
        );

        // Crossing midnight by a single second: rotate.
        let before_midnight = local_ts(2026, 1, 14, 23, 59, 59);
        let midnight = local_ts(2026, 1, 15, 0, 0, 0);
        assert_eq!(midnight - before_midnight, 1, "fixture must be exactly one second apart");
        assert!(
            Rotation::Daily.check_should_roll(before_midnight, midnight),
            "23:59:59 -> 00:00:00 crosses the day boundary"
        );

        // One second on either side of the boundary stays within a day.
        assert!(!Rotation::Daily.check_should_roll(midnight, midnight + 1), "00:00:00 -> 00:00:01");
        assert!(
            !Rotation::Daily.check_should_roll(before_midnight - 1, before_midnight),
            "23:59:58 -> 23:59:59"
        );
    }

    #[test]
    fn test_rotation_cooldown_skips_immediate_retry() {
        let tmp = TempDir::new().unwrap();
        let mut appender =
            RollingAppender::new(tmp.path(), "app.log".to_string(), Rotation::Never, 3, FileMatchMode::Suffix).unwrap();
        appender.size = 4;
        appender.rotation_retry_cooldown_until = Some(Instant::now() + Duration::from_secs(1));

        assert!(!appender.should_roll(1));
    }

    #[test]
    fn continuous_rotation_failure_caps_active_file_growth_and_recovers() {
        let tmp = TempDir::new().expect("temp dir");
        let max_size = 64u64;
        let mut appender =
            RollingAppender::new(tmp.path(), "app.log".to_string(), Rotation::Never, max_size, FileMatchMode::Suffix)
                .expect("create appender");
        appender.rotation_retry_base_cooldown = Duration::from_millis(1);

        // Fill active file to max_size.
        appender.write_all(&[b'a'; 64]).expect("initial write");
        assert_eq!(appender.size, 64);

        // Simulate a transient first rotation failure: grace cap is 2 * max_size (128 bytes).
        appender.record_rotation_failure(Instant::now());
        appender.rotation_retry_cooldown_until = Some(Instant::now() + Duration::from_secs(60));
        assert_eq!(appender.rotation_failure_hard_cap(), Some(128));

        // 32 bytes fit within the 2x grace window.
        appender.write_all(&[b'b'; 32]).expect("grace write");
        assert_eq!(appender.size, 96);
        assert_eq!(appender.dropped_bytes_due_to_rotation_failure, 0);

        // Additional writes that would exceed 2 * max_size must be shed instead of growing disk.
        for _ in 0..100 {
            appender.write_all(&[b'c'; 64]).expect("shedding returns Ok");
        }
        appender.flush().expect("flush");
        assert!(
            fs::metadata(tmp.path().join("app.log")).expect("active meta").len() <= 2 * max_size,
            "active file must never exceed 2x max_size while rotation is failing"
        );
        assert_eq!(appender.dropped_bytes_due_to_rotation_failure, 100 * 64);

        // Sustained streak (>= 3) tightens cap down to max_size.
        appender.record_rotation_failure(Instant::now());
        appender.record_rotation_failure(Instant::now());
        assert_eq!(appender.rotation_failure_hard_cap(), Some(max_size));

        // Once cooldown expires and filesystem allows rename again, rotation recovers cleanly.
        appender.rotation_retry_cooldown_until = None;
        appender.write_all(b"recovered\n").expect("recovered write");
        appender.flush().expect("flush after recovery");
        assert_eq!(appender.consecutive_rotation_failures, 0);
        assert_eq!(appender.rotation_failure_hard_cap(), None);
        assert_eq!(fs::read_to_string(tmp.path().join("app.log")).expect("read active"), "recovered\n");
        assert_eq!(count_files(tmp.path()), 2, "recovered rotation must archive previous active file");
    }

    #[cfg(unix)]
    #[test]
    fn readonly_directory_rotation_failure_stops_unbounded_file_growth() -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TempDir::new()?;
        let max_size = 64u64;
        let mut appender =
            RollingAppender::new(tmp.path(), "app.log".to_string(), Rotation::Never, max_size, FileMatchMode::Suffix)?;
        appender.rotation_retry_base_cooldown = Duration::from_secs(60);
        appender.write_all(&[b'x'; 64])?;
        appender.flush()?;

        // Make the directory non-writable (0o555) while keeping app.log writable (0o644):
        // rename() fails with PermissionDenied, while reopening app.log in append mode succeeds.
        fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o555))?;

        // Privileged users (e.g. root in containers) bypass POSIX directory write bits.
        if File::create(tmp.path().join(".write-probe")).is_ok() {
            fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o755))?;
            return Ok(());
        }

        let write_res = (|| -> io::Result<()> {
            for _ in 0..200 {
                appender.write_all(&[b'y'; 64])?;
            }
            appender.flush()
        })();

        // Always restore directory permissions before asserting / dropping TempDir.
        fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o755))?;
        write_res?;

        let final_len = fs::metadata(tmp.path().join("app.log"))?.len();
        assert!(
            final_len <= 2 * max_size,
            "active file grew to {final_len} bytes under continuous rotation failure (cap {})",
            2 * max_size
        );
        assert!(appender.consecutive_rotation_failures >= 1, "rotation failure must be recorded");
        assert!(
            appender.dropped_bytes_due_to_rotation_failure > 0,
            "excess writes must be shed while directory prevents rotation"
        );

        // Directory is writable again; expire cooldown and verify clean recovery.
        appender.rotation_retry_cooldown_until = None;
        appender.write_all(b"after-recovery\n")?;
        appender.flush()?;
        assert_eq!(appender.consecutive_rotation_failures, 0);
        assert_eq!(fs::read_to_string(tmp.path().join("app.log"))?, "after-recovery\n");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn time_based_rotation_failure_preserves_last_roll_ts_and_caps_when_max_size_zero() -> io::Result<()> {
        use std::os::unix::fs::PermissionsExt;

        struct RestoreDirPerms<'a>(&'a Path);
        impl Drop for RestoreDirPerms<'_> {
            fn drop(&mut self) {
                let _ = fs::set_permissions(self.0, fs::Permissions::from_mode(0o755));
            }
        }

        let tmp = TempDir::new()?;
        let mut appender = RollingAppender::new(tmp.path(), "app.log".to_string(), Rotation::Hourly, 0, FileMatchMode::Suffix)?;
        appender.rotation_retry_base_cooldown = Duration::from_secs(60);
        appender.write_all(b"seed\n")?;
        appender.flush()?;

        // Wind `last_roll_ts` back two hours so time-based rotation is due.
        let original_last_roll_ts = ZonedDateTime::now().timestamp().as_second() - 7200;
        appender.last_roll_ts = original_last_roll_ts;

        let _restore = RestoreDirPerms(tmp.path());
        fs::set_permissions(tmp.path(), fs::Permissions::from_mode(0o555))?;
        if File::create(tmp.path().join(".write-probe")).is_ok() {
            return Ok(());
        }

        // Failure #1: `open_file(false)` must NOT clobber `last_roll_ts`, and a
        // bounded emergency ceiling must apply immediately from failure #1.
        appender.write_all(b"after-failure-1\n")?;
        appender.flush()?;
        assert_eq!(appender.consecutive_rotation_failures, 1);
        assert_eq!(
            appender.last_roll_ts, original_last_roll_ts,
            "failed rotation reopen must preserve last_roll_ts so time-based retries keep firing"
        );
        assert_eq!(
            appender.rotation_failure_hard_cap(),
            Some(ROTATION_FAILURE_UNBOUNDED_FALLBACK_CAP_BYTES),
            "max_size_bytes == 0 must enforce a bounded grace cap starting on failure #1"
        );

        // Expire cooldown and retry twice more; `should_roll` must still fire
        // even though the write above updated app.log's mtime to the current hour.
        for expected_failures in [2u32, 3u32] {
            appender.rotation_retry_cooldown_until = None;
            assert!(
                appender.should_roll(1),
                "time-based rotation must remain due after failure #{}",
                expected_failures - 1
            );
            appender.write_all(b"retry-write\n")?;
            appender.flush()?;
            assert_eq!(appender.consecutive_rotation_failures, expected_failures);
            assert_eq!(appender.last_roll_ts, original_last_roll_ts);
        }

        // Sustained failure (>= 3) tightens the fallback cap and sheds excess writes.
        assert_eq!(appender.rotation_failure_hard_cap(), Some(DEFAULT_OBS_LOG_ACTIVE_FILE_ROLL_SIZE_BYTES));
        let pre_shed_len = fs::metadata(tmp.path().join("app.log"))?.len();
        appender.size = DEFAULT_OBS_LOG_ACTIVE_FILE_ROLL_SIZE_BYTES;
        appender.write_all(&[b'z'; 64])?;
        appender.flush()?;
        assert_eq!(
            fs::metadata(tmp.path().join("app.log"))?.len(),
            pre_shed_len,
            "writes exceeding tightened fallback cap must be shed without disk growth"
        );
        assert_eq!(appender.dropped_bytes_due_to_rotation_failure, 64);

        // Restore writability, expire cooldown, and verify clean time-based recovery.
        drop(_restore);
        appender.size = pre_shed_len;
        appender.rotation_retry_cooldown_until = None;
        appender.write_all(b"recovered-hourly\n")?;
        appender.flush()?;
        assert_eq!(appender.consecutive_rotation_failures, 0);
        assert_eq!(appender.rotation_failure_hard_cap(), None);
        assert!(
            appender.last_roll_ts > original_last_roll_ts,
            "successful recovery rotation must advance last_roll_ts"
        );
        assert_eq!(fs::read_to_string(tmp.path().join("app.log"))?, "recovered-hourly\n");
        assert_eq!(count_files(tmp.path()), 2);
        Ok(())
    }
}
