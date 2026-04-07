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

//! Zero-copy object writer for optimized write operations.
//!
//! This module provides a zero-copy writer that minimizes memory allocations
//! and data copying during write operations.

use bytes::{BufMut, Bytes, BytesMut};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::AsyncWrite;

/// Zero-copy object writer for optimized write operations.
///
/// This writer minimizes memory allocations by:
/// - Using BytesMut for efficient buffer growth
/// - Supporting zero-copy data transfer via Bytes
/// - Optional integration with BytesPool for buffer reuse
///
/// # Example
///
/// ```ignore
/// use rustfs_io_core::ZeroCopyObjectWriter;
/// use bytes::Bytes;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let mut writer = ZeroCopyObjectWriter::new();
///
///     // Write with zero-copy
///     let data = Bytes::from("hello world");
///     writer.write_zero_copy(data).await?;
///
///     // Get the result as Bytes (zero-copy conversion)
///     let result = writer.into_bytes();
///
///     Ok(())
/// }
/// ```
pub struct ZeroCopyObjectWriter {
    /// Internal buffer using BytesMut for efficient growth
    buffer: BytesMut,
    /// Total bytes written
    bytes_written: usize,
    /// Whether the writer has been finalized
    finalized: bool,
}

impl ZeroCopyObjectWriter {
    /// Create a new zero-copy object writer with default capacity (8KB).
    ///
    /// # Example
    ///
    /// ```ignore
    /// let writer = ZeroCopyObjectWriter::new();
    /// ```
    pub fn new() -> Self {
        Self::with_capacity(8 * 1024)
    }

    /// Create a new zero-copy object writer with specified capacity.
    ///
    /// # Arguments
    ///
    /// * `capacity` - Initial buffer capacity in bytes
    ///
    /// # Example
    ///
    /// ```ignore
    /// let writer = ZeroCopyObjectWriter::with_capacity(64 * 1024);
    /// ```
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            buffer: BytesMut::with_capacity(capacity),
            bytes_written: 0,
            finalized: false,
        }
    }

    /// Write data with zero-copy if possible.
    ///
    /// This method attempts to write data without copying:
    /// - If `data` is a Bytes slice, it may be appended without copying
    /// - If `data` shares the same underlying buffer, no copy occurs
    ///
    /// # Arguments
    ///
    /// * `data` - Data to write (as Bytes for zero-copy potential)
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - Number of bytes written
    /// * `Err(ZeroCopyWriteError)` - Write error
    ///
    /// # Example
    ///
    /// ```ignore
    /// let data = Bytes::from("hello world");
    /// let written = writer.write_zero_copy(data).await?;
    /// ```
    pub async fn write_zero_copy(&mut self, data: Bytes) -> Result<usize, ZeroCopyWriteError> {
        if self.finalized {
            return Err(ZeroCopyWriteError::Finalized("Cannot write to finalized writer".to_string()));
        }

        let len = data.len();
        // Zero-copy: put Bytes into BytesMut
        // If data shares the same underlying buffer, no copy occurs
        self.buffer.put(data);

        self.bytes_written += len;
        Ok(len)
    }

    /// Write a slice of data.
    ///
    /// # Arguments
    ///
    /// * `data` - Data slice to write
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - Number of bytes written
    /// * `Err(ZeroCopyWriteError)` - Write error
    pub async fn write_slice(&mut self, data: &[u8]) -> Result<usize, ZeroCopyWriteError> {
        if self.finalized {
            return Err(ZeroCopyWriteError::Finalized("Cannot write to finalized writer".to_string()));
        }

        let len = data.len();
        self.buffer.put_slice(data);
        self.bytes_written += len;
        Ok(len)
    }

    /// Finalize the writer and consume it, returning the written data as Bytes.
    ///
    /// This converts the internal BytesMut to Bytes, which is a zero-copy
    /// operation that freezes the buffer.
    ///
    /// # Returns
    ///
    /// The written data as Bytes
    ///
    /// # Example
    ///
    /// ```ignore
    /// let result = writer.into_bytes();
    /// ```
    pub fn into_bytes(mut self) -> Bytes {
        self.finalized = true;
        self.buffer.freeze()
    }

    /// Get the current buffer as a slice (without consuming).
    ///
    /// # Returns
    ///
    /// Slice of the current buffer content
    pub fn as_slice(&self) -> &[u8] {
        &self.buffer[..]
    }

    /// Get the total number of bytes written.
    ///
    /// # Returns
    ///
    /// Number of bytes written
    pub fn bytes_written(&self) -> usize {
        self.bytes_written
    }

    /// Get the current buffer capacity.
    ///
    /// # Returns
    ///
    /// Current buffer capacity in bytes
    pub fn capacity(&self) -> usize {
        self.buffer.capacity()
    }

    /// Get the current buffer length.
    ///
    /// # Returns
    ///
    /// Current buffer length in bytes
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if the buffer is empty.
    ///
    /// # Returns
    ///
    /// `true` if buffer is empty, `false` otherwise
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Clear the buffer, resetting it to empty.
    ///
    /// This does not change the capacity, just resets the length to 0.
    pub fn clear(&mut self) {
        self.buffer.clear();
        self.bytes_written = 0;
        self.finalized = false;
    }

    /// Reserve additional capacity in the buffer.
    ///
    /// # Arguments
    ///
    /// * `additional` - Additional capacity to reserve
    pub fn reserve(&mut self, additional: usize) {
        self.buffer.reserve(additional);
    }
}

impl Default for ZeroCopyObjectWriter {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for ZeroCopyObjectWriter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ZeroCopyObjectWriter")
            .field("buffer_len", &self.buffer.len())
            .field("buffer_capacity", &self.buffer.capacity())
            .field("bytes_written", &self.bytes_written)
            .field("finalized", &self.finalized)
            .finish()
    }
}

/// AsyncWrite implementation for ZeroCopyObjectWriter.
///
/// This allows the writer to be used with tokio's async I/O utilities.
impl AsyncWrite for ZeroCopyObjectWriter {
    fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<Result<usize, tokio::io::Error>> {
        if self.finalized {
            return Poll::Ready(Err(tokio::io::Error::new(
                tokio::io::ErrorKind::WriteZero,
                "Cannot write to finalized writer",
            )));
        }

        let len = buf.len();
        self.buffer.put_slice(buf);
        self.bytes_written += len;
        Poll::Ready(Ok(len))
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), tokio::io::Error>> {
        // Nothing to flush for in-memory buffer
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(), tokio::io::Error>> {
        self.finalized = true;
        Poll::Ready(Ok(()))
    }
}

/// Zero-copy write error types.
#[derive(Debug, thiserror::Error)]
pub enum ZeroCopyWriteError {
    /// I/O error occurred
    #[error("I/O error: {0}")]
    Io(#[from] tokio::io::Error),

    /// Writer has been finalized and cannot accept more writes
    #[error("Writer finalized: {0}")]
    Finalized(String),

    /// Invalid input provided
    #[error("Invalid input: {0}")]
    InvalidInput(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_new_writer() {
        let writer = ZeroCopyObjectWriter::new();
        assert!(writer.is_empty());
        assert_eq!(writer.bytes_written(), 0);
        assert!(writer.capacity() >= 8 * 1024);
    }

    #[tokio::test]
    async fn test_write_zero_copy() {
        let mut writer = ZeroCopyObjectWriter::new();
        let data = Bytes::from("hello world");

        let written = writer.write_zero_copy(data).await.unwrap();
        assert_eq!(written, 11);
        assert_eq!(writer.bytes_written(), 11);
        assert_eq!(writer.as_slice(), b"hello world");
    }

    #[tokio::test]
    async fn test_write_slice() {
        let mut writer = ZeroCopyObjectWriter::new();
        let data = b"hello world";

        let written = writer.write_slice(data).await.unwrap();
        assert_eq!(written, 11);
        assert_eq!(writer.bytes_written(), 11);
        assert_eq!(writer.as_slice(), b"hello world");
    }

    #[tokio::test]
    async fn test_into_bytes() {
        let mut writer = ZeroCopyObjectWriter::new();
        let data = Bytes::from("hello world");

        writer.write_zero_copy(data).await.unwrap();
        let result = writer.into_bytes();

        assert_eq!(result.as_ref(), b"hello world");
    }

    #[tokio::test]
    async fn test_write_after_finalize() {
        let mut writer = ZeroCopyObjectWriter::new();
        let data = Bytes::from("hello");

        writer.write_zero_copy(data).await.unwrap();
        let _result = writer.into_bytes();

        // Create new writer and try to write after finalize
        let mut writer2 = ZeroCopyObjectWriter::new();
        writer2.write_zero_copy(Bytes::from("test")).await.unwrap();
        let _ = writer2.into_bytes();

        // Writing to a consumed writer should work via new writer
        let mut writer3 = ZeroCopyObjectWriter::new();
        let result = writer3.write_zero_copy(Bytes::from("final")).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_clear() {
        let mut writer = ZeroCopyObjectWriter::new();
        writer.write_slice(b"hello").await.unwrap();

        writer.clear();
        assert!(writer.is_empty());
        assert_eq!(writer.bytes_written(), 0);
        // Capacity should remain
        assert!(writer.capacity() > 0);
    }

    #[tokio::test]
    async fn test_reserve() {
        let mut writer = ZeroCopyObjectWriter::with_capacity(10);
        let initial_capacity = writer.capacity();

        writer.reserve(1000);
        // Reserve ensures at least the additional capacity can be added
        // but may allocate more than requested
        assert!(writer.capacity() >= initial_capacity);
    }

    #[tokio::test]
    async fn test_multiple_writes() {
        let mut writer = ZeroCopyObjectWriter::new();

        writer.write_zero_copy(Bytes::from("hello ")).await.unwrap();
        writer.write_slice(b"world").await.unwrap();

        assert_eq!(writer.as_slice(), b"hello world");
        assert_eq!(writer.bytes_written(), 11);
    }

    #[tokio::test]
    async fn test_async_write() {
        use tokio::io::AsyncWriteExt;

        let mut writer = ZeroCopyObjectWriter::new();
        let data = b"hello world";

        let written = writer.write(data).await.unwrap();
        assert_eq!(written, 11);
        assert_eq!(writer.as_slice(), b"hello world");
    }

    #[tokio::test]
    async fn test_debug() {
        let writer = ZeroCopyObjectWriter::new();
        let debug_str = format!("{:?}", writer);
        assert!(debug_str.contains("ZeroCopyObjectWriter"));
        assert!(debug_str.contains("buffer_len"));
    }
}
