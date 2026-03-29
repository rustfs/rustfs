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

//! Aligned pread-based file reader.
//!
//! This module provides an aligned, position-based file reader that uses
//! `pread`/`FileExt::read_at` for I/O operations. It performs reads at
//! 512-byte-aligned offsets and sizes, making it suitable as a foundation
//! for workloads where alignment matters.
//!
//! Note: This reader does **not** set the `O_DIRECT` flag and therefore does
//! not bypass the OS page cache. It is an aligned `pread`-based reader, not
//! true Direct I/O. To implement true O_DIRECT on Linux, the file must be
//! opened with `O_DIRECT` via `libc::open`.
//!
//! # Platform Support
//!
//! The `read_at` implementation is only available on Unix-like platforms.
//! On other platforms, this reader will return an error.

use std::io::{self};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/// Errors that can occur during aligned pread operations.
#[derive(Debug, Clone)]
pub enum DirectIoError {
    /// Platform doesn't support `read_at`-based I/O
    UnsupportedPlatform,
    /// File descriptor doesn't support this reader
    UnsupportedFile,
    /// I/O error occurred
    Io(String),
    /// Invalid alignment (reads require 512-byte-aligned offset and size)
    AlignmentError { offset: u64, size: usize },
}

impl std::fmt::Display for DirectIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedPlatform => write!(f, "Aligned pread not supported on this platform"),
            Self::UnsupportedFile => write!(f, "File doesn't support this reader"),
            Self::Io(msg) => write!(f, "I/O error: {}", msg),
            Self::AlignmentError { offset, size } => {
                write!(f, "Alignment error: offset={}, size={}", offset, size)
            }
        }
    }
}

impl std::error::Error for DirectIoError {}

impl From<io::Error> for DirectIoError {
    fn from(err: io::Error) -> Self {
        Self::Io(err.to_string())
    }
}

/// Aligned pread-based file reader for Unix platforms.
///
/// This reader performs I/O using `pread`/`FileExt::read_at` at
/// 512-byte-aligned offsets and sizes, without modifying the file's
/// current position.
///
/// **Note:** This reader does **not** set the `O_DIRECT` flag and therefore
/// does **not** bypass the OS page cache. It is an aligned `pread`-based
/// reader. To implement true O_DIRECT, the file must be opened with
/// `O_DIRECT` via `libc::open`.
///
/// # Platform Support
///
/// Only available on Linux (uses `FileExt::read_at`). On other platforms,
/// use `ZeroCopyObjectReader` with memory mapping instead.
///
/// # Alignment Requirements
///
/// Reads have strict alignment requirements:
/// - File offset must be aligned to 512 bytes
/// - Buffer size must be a multiple of 512 bytes
/// - Buffer address must be aligned (handled internally)
///
/// # Example
///
/// ```ignore
/// use rustfs_io_core::DirectIoReader;
///
/// // Linux only
/// #[cfg(target_os = "linux")]
/// let reader = DirectIoReader::new(file, offset, size)?;
/// ```
#[cfg(target_os = "linux")]
pub struct DirectIoReader {
    /// Underlying file handle used for aligned pread I/O
    file: std::fs::File,
    /// Current read position
    pos: u64,
    /// Remaining bytes to read
    remaining: usize,
    /// Buffer for aligned reads
    buffer: Vec<u8>,
    /// Current position in the buffer
    buffer_pos: usize,
    /// Amount of data in the buffer
    buffer_len: usize,
}

#[cfg(target_os = "linux")]
impl DirectIoReader {
    /// Alignment requirement for reads (512 bytes for most systems)
    pub const ALIGNMENT: usize = 512;

    /// Create a new aligned pread-based reader.
    ///
    /// # Arguments
    ///
    /// * `file` - File to read from
    /// * `offset` - Starting offset in the file (must be 512-byte aligned)
    /// * `size` - Number of bytes to read (must be 512-byte aligned)
    ///
    /// # Returns
    ///
    /// A `DirectIoReader` that reads the file at the given offset.
    ///
    /// # Errors
    ///
    /// Returns an error if offset or size are not 512-byte aligned.
    pub fn new(file: std::fs::File, offset: u64, size: usize) -> Result<Self, DirectIoError> {
        // Check alignment
        if !offset.is_multiple_of(Self::ALIGNMENT as u64) {
            return Err(DirectIoError::AlignmentError { offset, size });
        }
        if !size.is_multiple_of(Self::ALIGNMENT) {
            return Err(DirectIoError::AlignmentError { offset, size });
        }

        Ok(Self {
            file,
            pos: offset,
            remaining: size,
            buffer: Vec::new(),
            buffer_pos: 0,
            buffer_len: 0,
        })
    }

    /// Read a chunk of data using Direct I/O.
    ///
    /// This method performs aligned reads and handles the buffering
    /// required for Direct I/O operations.
    fn read_chunk(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        // If buffer is exhausted, read more data
        if self.buffer_pos >= self.buffer_len {
            if self.remaining == 0 {
                return Ok(0);
            }

            // Allocate aligned buffer
            let chunk_size = (self.remaining).min(64 * 1024); // 64KB chunks
            let aligned_size = chunk_size.div_ceil(Self::ALIGNMENT) * Self::ALIGNMENT;

            self.buffer = vec![0u8; aligned_size];

            // Use pread for atomic read at position (no file offset modification)
            use std::os::unix::fs::FileExt;
            let n = self.file.read_at(&mut self.buffer, self.pos)?;

            self.buffer_pos = 0;
            self.buffer_len = n;
            self.pos += n as u64;
            self.remaining -= n;

            if n == 0 {
                return Ok(0);
            }
        }

        // Copy from buffer to user buffer
        let available = self.buffer_len - self.buffer_pos;
        let to_copy = buf.len().min(available);
        buf[..to_copy].copy_from_slice(&self.buffer[self.buffer_pos..self.buffer_pos + to_copy]);
        self.buffer_pos += to_copy;

        Ok(to_copy)
    }
}

#[cfg(target_os = "linux")]
impl AsyncRead for DirectIoReader {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let filled = buf.filled().len();
        let mut remaining = buf.initialize_unfilled();

        while !remaining.is_empty() {
            match self.read_chunk(remaining) {
                Ok(0) => break,
                Ok(n) => {
                    remaining = &mut remaining[n..];
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }

        let _n_read = buf.filled().len() - filled;
        Poll::Ready(Ok(()))
    }
}

/// Aligned pread reader stub for non-Linux platforms.
///
/// On non-Linux platforms, `read_at`-based I/O is not available through this
/// type. This stub exists to provide a consistent API across platforms.
#[cfg(not(target_os = "linux"))]
pub struct DirectIoReader {
    _priv: (),
}

#[cfg(not(target_os = "linux"))]
impl DirectIoReader {
    /// Create a new aligned pread reader (not supported on this platform).
    ///
    /// Always returns an error on non-Linux platforms.
    pub fn new(_file: std::fs::File, _offset: u64, _size: usize) -> Result<Self, DirectIoError> {
        Err(DirectIoError::UnsupportedPlatform)
    }
}

#[cfg(not(target_os = "linux"))]
impl AsyncRead for DirectIoReader {
    fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "Aligned pread-based I/O not supported on this platform",
        )))
    }
}

impl std::fmt::Debug for DirectIoReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        #[cfg(target_os = "linux")]
        {
            f.debug_struct("DirectIoReader")
                .field("pos", &self.pos)
                .field("remaining", &self.remaining)
                .field("buffer_len", &self.buffer_len)
                .finish()
        }
        #[cfg(not(target_os = "linux"))]
        {
            f.debug_struct("DirectIoReader").field("platform", &"unsupported").finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_alignment_check() {
        #[cfg(target_os = "linux")]
        {
            // Valid alignment
            let file = std::fs::File::open("/dev/zero").unwrap();
            assert!(DirectIoReader::new(file, 0, 512).is_ok(), "Should succeed with aligned offset and size");

            // Invalid offset
            let file = std::fs::File::open("/dev/zero").unwrap();
            assert!(DirectIoReader::new(file, 1, 512).is_err(), "Should fail with unaligned offset");

            // Invalid size
            let file = std::fs::File::open("/dev/zero").unwrap();
            assert!(DirectIoReader::new(file, 0, 511).is_err(), "Should fail with unaligned size");
        }

        #[cfg(not(target_os = "linux"))]
        {
            // Non-Linux should return UnsupportedPlatform
            let file = std::fs::File::open("/dev/null").unwrap();
            assert!(matches!(DirectIoReader::new(file, 0, 512), Err(DirectIoError::UnsupportedPlatform)));
        }
    }
}
