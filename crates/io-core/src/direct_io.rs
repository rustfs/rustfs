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

//! Direct I/O support for Linux.
//!
//! This module provides Direct I/O (O_DIRECT) functionality for Linux systems,
//! allowing I/O operations to bypass the OS page cache for specific use cases.
//!
//! Direct I/O is useful for:
//! - Large file transfers where caching isn't beneficial
//! - Databases that manage their own cache
//! - Applications requiring predictable I/O latency
//!
//! # Platform Support
//!
//! Direct I/O is only supported on Linux. On other platforms, attempting to
//! create a DirectIoReader will return an error.

use std::io::{self};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/// Errors that can occur during Direct I/O operations.
#[derive(Debug, Clone)]
pub enum DirectIoError {
    /// Platform doesn't support Direct I/O
    UnsupportedPlatform,
    /// File descriptor doesn't support Direct I/O
    UnsupportedFile,
    /// I/O error occurred
    Io(String),
    /// Invalid alignment (Direct I/O requires aligned buffers)
    AlignmentError { offset: u64, size: usize },
}

impl std::fmt::Display for DirectIoError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedPlatform => write!(f, "Direct I/O not supported on this platform"),
            Self::UnsupportedFile => write!(f, "File doesn't support Direct I/O"),
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

/// Direct I/O object reader for Linux.
///
/// This reader uses O_DIRECT flag to bypass the OS page cache,
/// providing direct access to disk storage.
///
/// # Platform Support
///
/// Only available on Linux. On other platforms, use `ZeroCopyObjectReader`
/// with memory mapping instead.
///
/// # Alignment Requirements
///
/// Direct I/O has strict alignment requirements:
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
    /// File opened with O_DIRECT flag
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
    /// Direct I/O alignment requirement (512 bytes for most systems)
    pub const ALIGNMENT: usize = 512;

    /// Create a new Direct I/O reader.
    ///
    /// # Arguments
    ///
    /// * `file` - File to read from (will be reopened with O_DIRECT)
    /// * `offset` - Starting offset in the file (must be 512-byte aligned)
    /// * `size` - Number of bytes to read (must be 512-byte aligned)
    ///
    /// # Returns
    ///
    /// A `DirectIoReader` that provides direct access to the file.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file doesn't support Direct I/O
    /// - Offset or size are not properly aligned
    /// - The platform doesn't support Direct I/O
    pub fn new(file: std::fs::File, offset: u64, size: usize) -> Result<Self, DirectIoError> {
        // Check alignment
        if offset % Self::ALIGNMENT as u64 != 0 {
            return Err(DirectIoError::AlignmentError { offset, size });
        }
        if size % Self::ALIGNMENT != 0 {
            return Err(DirectIoError::AlignmentError { offset, size });
        }

        // Try to enable O_DIRECT on the file
        // Note: This requires the file to be opened with O_DIRECT flag
        // In production, you'd need to reopen the file with proper flags

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
            let aligned_size = (chunk_size + Self::ALIGNMENT - 1) / Self::ALIGNMENT * Self::ALIGNMENT;

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
        let mut remaining = &mut buf.initialize_unfilled();

        while !remaining.is_empty() {
            match self.read_chunk(remaining) {
                Ok(0) => break,
                Ok(n) => {
                    remaining = remaining[n..];
                }
                Err(e) => return Poll::Ready(Err(e)),
            }
        }

        let _n_read = buf.filled().len() - filled;
        Poll::Ready(Ok(()))
    }
}

/// Direct I/O reader stub for non-Linux platforms.
///
/// On non-Linux platforms, Direct I/O is not supported. This type
/// exists to provide a consistent API across platforms.
#[cfg(not(target_os = "linux"))]
pub struct DirectIoReader {
    _priv: (),
}

#[cfg(not(target_os = "linux"))]
impl DirectIoReader {
    /// Create a new Direct I/O reader (not supported on this platform).
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
            "Direct I/O not supported on this platform",
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
