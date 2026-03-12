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
    pub fn new(
        dir: impl AsRef<Path>,
        filename: String,
        rotation: Rotation,
        max_size_bytes: u64,
        match_mode: FileMatchMode,
    ) -> Self {
        Self {
            dir: dir.as_ref().to_path_buf(),
            filename,
            rotation,
            max_size_bytes,
            match_mode,
            file: None,
            size: 0,
            last_roll_ts: SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs(),
        }
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

        self.size = file.metadata()?.len();
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

        // 2. Generate archive name
        // Use unix timestamp prefix for easy sorting and uniqueness
        let now_ts = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs();

        // Match naming strategy with LogCleaner expectations
        let archive_name = match self.match_mode {
            FileMatchMode::Suffix => {
                // Suffix mode: timestamp BEFORE filename.
                // e.g. rustfs.log -> 1678886400.rustfs.log
                format!("{}.{}", now_ts, self.filename)
            }
            FileMatchMode::Prefix => {
                // Prefix mode: timestamp AFTER filename.
                // e.g. rustfs -> rustfs.1678886400
                format!("{}.{}", self.filename, now_ts)
            }
        };

        let archive_path = self.dir.join(archive_name);

        // 3. Rename
        // Note: Rename is atomic on POSIX.
        if let Err(e) = fs::rename(&active_path, &archive_path) {
            eprintln!("RollingAppender: Failed to rename log file during rotation: {}", e);
            return Err(e);
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
