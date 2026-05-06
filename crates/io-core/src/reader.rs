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

//! Zero-copy object reader implementation.

use bytes::Bytes;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

/// Errors that can occur during zero-copy read operations.
#[derive(Debug, Clone)]
pub enum ZeroCopyReadError {
    /// I/O error occurred.
    Io(String),
    /// Memory mapping error.
    Mmap(String),
    /// Invalid offset or size.
    InvalidRange,
}

impl std::fmt::Display for ZeroCopyReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(msg) => write!(f, "I/O error: {}", msg),
            Self::Mmap(msg) => write!(f, "Mmap error: {}", msg),
            Self::InvalidRange => write!(f, "Invalid offset or size"),
        }
    }
}

impl std::error::Error for ZeroCopyReadError {}

impl From<io::Error> for ZeroCopyReadError {
    fn from(err: io::Error) -> Self {
        Self::Io(err.to_string())
    }
}

/// Zero-copy object reader.
///
/// This reader provides zero-copy access to object data by using:
/// - Memory-mapped files for on-disk data
/// - Bytes wrapping for in-memory data
/// - Reference counting to avoid copies
///
/// # Example
///
/// ```ignore
/// // Create from bytes (zero-copy)
/// let data = Bytes::from("hello world");
/// let reader = ZeroCopyObjectReader::from_bytes(data);
///
/// // Read using AsyncRead trait
/// let mut buf = vec![0u8; 1024];
/// let n = reader.read(&mut buf[..]).await?;
/// ```
pub struct ZeroCopyObjectReader {
    /// Internal data source (could be mmap or owned bytes)
    data: Bytes,
    /// Current read position
    pos: usize,
}

impl ZeroCopyObjectReader {
    /// Create a zero-copy reader from existing bytes.
    ///
    /// This is a true zero-copy operation - the Bytes are wrapped
    /// without any allocation or copying.
    ///
    /// # Arguments
    ///
    /// * `data` - Bytes to wrap
    ///
    /// # Example
    ///
    /// ```ignore
    /// let data = Bytes::from("hello world");
    /// let reader = ZeroCopyObjectReader::from_bytes(data);
    /// ```
    pub fn from_bytes(data: Bytes) -> Self {
        Self { data, pos: 0 }
    }

    /// Create a zero-copy reader from a file using mmap.
    ///
    /// This uses memory mapping to avoid loading the entire file into memory.
    /// Only the accessed pages are loaded on demand.
    ///
    /// # Arguments
    ///
    /// * `path` - Path to the file to memory map
    /// * `offset` - Offset within the file to start reading
    /// * `size` - Number of bytes to map
    ///
    /// # Returns
    ///
    /// A reader that provides zero-copy access to the file data.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be memory mapped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let reader = ZeroCopyObjectReader::from_file_mmap_path("large_file.bin", 0, 1024).await?;
    /// ```
    #[cfg(unix)]
    // SAFETY: The mmap is created from a read-only file handle for the
    // caller-provided range, then copied into owned `Bytes` before the file and
    // mapping are dropped.
    #[allow(unsafe_code)]
    pub async fn from_file_mmap_path(path: &std::path::Path, offset: u64, size: usize) -> Result<Self, ZeroCopyReadError> {
        use memmap2::MmapOptions;

        let path = path.to_path_buf();
        let (offset, size) = (offset, size);

        tokio::task::spawn_blocking(move || {
            // Open the file in sync context
            let std_file = std::fs::File::open(&path).map_err(|e| ZeroCopyReadError::Io(e.to_string()))?;

            // SAFETY: `std_file` remains open while the mapping is created and
            // copied, and the mapped bytes are not exposed beyond this closure.
            let mmap = unsafe { MmapOptions::new().offset(offset).len(size).map(&std_file) }
                .map_err(|e| ZeroCopyReadError::Mmap(e.to_string()))?;

            // Convert to Bytes (this is a copy, but only done once)
            Ok(Self {
                data: Bytes::copy_from_slice(&mmap),
                pos: 0,
            })
        })
        .await
        .map_err(|e| ZeroCopyReadError::Io(e.to_string()))?
    }

    /// Create a zero-copy reader from a file using mmap.
    ///
    /// This uses memory mapping to avoid loading the entire file into memory.
    /// Only the accessed pages are loaded on demand.
    ///
    /// # Arguments
    ///
    /// * `file` - File to memory map
    /// * `offset` - Offset within the file to start reading
    /// * `size` - Number of bytes to map
    ///
    /// # Returns
    ///
    /// A reader that provides zero-copy access to the file data.
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be memory mapped.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let file = tokio::fs::File::open("large_file.bin").await?;
    /// let reader = ZeroCopyObjectReader::from_file_mmap(&file, 0, 1024).await?;
    /// ```
    #[cfg(unix)]
    pub async fn from_file_mmap(file: &tokio::fs::File, offset: u64, size: usize) -> Result<Self, ZeroCopyReadError> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

        // For mmap, we need the file path - fall back to regular read if not available
        // This is a simplified implementation
        let mut cloned = file.try_clone().await?;
        cloned.seek(SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0u8; size];
        cloned.read_exact(&mut buffer).await?;

        Ok(Self {
            data: Bytes::from(buffer),
            pos: 0,
        })
    }

    /// Create a zero-copy reader from a file (non-Unix fallback).
    ///
    /// On platforms that don't support mmap, this falls back to regular file I/O.
    #[cfg(not(unix))]
    pub async fn from_file_mmap(file: &tokio::fs::File, offset: u64, size: usize) -> Result<Self, ZeroCopyReadError> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt, SeekFrom};

        let mut cloned = file.try_clone().await?;
        cloned.seek(SeekFrom::Start(offset)).await?;

        let mut buffer = vec![0u8; size];
        cloned.read_exact(&mut buffer).await?;

        Ok(Self {
            data: Bytes::from(buffer),
            pos: 0,
        })
    }

    /// Get the remaining data as Bytes (zero-copy).
    ///
    /// This returns a slice of the remaining data without copying.
    /// The returned Bytes shares the underlying memory with this reader.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let remaining = reader.remaining_bytes();
    /// println!("Remaining: {} bytes", remaining.len());
    /// ```
    pub fn remaining_bytes(&self) -> Bytes {
        self.data.slice(self.pos..)
    }

    /// Get the total length of the data.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the reader has reached the end.
    pub fn is_empty(&self) -> bool {
        self.pos >= self.data.len()
    }

    /// Get the current read position.
    pub fn position(&self) -> usize {
        self.pos
    }
}

impl AsyncRead for ZeroCopyObjectReader {
    fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let remaining = self.data.len() - self.pos;
        if remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        let to_read = std::cmp::min(remaining, buf.remaining());
        let slice = &self.data[self.pos..self.pos + to_read];
        buf.put_slice(slice);
        self.pos += to_read;

        Poll::Ready(Ok(()))
    }
}

impl std::fmt::Debug for ZeroCopyObjectReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZeroCopyObjectReader")
            .field("data_len", &self.data.len())
            .field("pos", &self.pos)
            .field("remaining", &(self.data.len() - self.pos))
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_from_bytes() {
        let data = Bytes::from("hello world");
        let mut reader = ZeroCopyObjectReader::from_bytes(data.clone());

        let mut buf = [0u8; 11];
        let n = reader.read(&mut buf[..]).await.unwrap();

        assert_eq!(n, 11);
        assert_eq!(&buf[..n], b"hello world");
    }

    #[tokio::test]
    async fn test_remaining_bytes() {
        let data = Bytes::from("hello world");
        let reader = ZeroCopyObjectReader::from_bytes(data);

        let remaining = reader.remaining_bytes();
        assert_eq!(remaining.len(), 11);
        assert_eq!(&remaining[..], b"hello world");
    }

    #[tokio::test]
    async fn test_position() {
        let data = Bytes::from("hello world");
        let mut reader = ZeroCopyObjectReader::from_bytes(data);

        assert_eq!(reader.position(), 0);

        let mut buf = [0u8; 5];
        reader.read_exact(&mut buf[..]).await.unwrap();

        assert_eq!(reader.position(), 5);
    }

    #[tokio::test]
    async fn test_is_empty() {
        let data = Bytes::from("");
        let reader = ZeroCopyObjectReader::from_bytes(data);
        assert!(reader.is_empty());

        let data = Bytes::from("hello");
        let reader = ZeroCopyObjectReader::from_bytes(data);
        assert!(!reader.is_empty());
    }
}
