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

//! Core chunk ownership abstractions for the zero-copy data plane.

use crate::pool::PooledBuffer;
use bytes::Bytes;
use futures_core::Stream;
use std::io;
use std::pin::Pin;

/// Boxed asynchronous stream of I/O chunks.
pub type BoxChunkStream = Pin<Box<dyn Stream<Item = io::Result<IoChunk>> + Send + Sync + 'static>>;

/// Source of chunked data.
pub trait ChunkSource {
    fn into_chunk_stream(self) -> BoxChunkStream
    where
        Self: Sized;
}

/// Owned chunk variants used by the zero-copy data plane.
#[derive(Debug)]
pub enum IoChunk {
    Shared(Bytes),
    Mapped(MappedChunk),
    Pooled(PooledChunk),
}

impl IoChunk {
    /// Returns the visible length of this chunk.
    #[must_use]
    pub fn len(&self) -> usize {
        match self {
            Self::Shared(bytes) => bytes.len(),
            Self::Mapped(chunk) => chunk.len(),
            Self::Pooled(chunk) => chunk.len(),
        }
    }

    /// Returns true when the chunk has no visible data.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a shared read-only view of the visible bytes.
    #[must_use]
    pub fn as_bytes(&self) -> Bytes {
        match self {
            Self::Shared(bytes) => bytes.clone(),
            Self::Mapped(chunk) => chunk.as_bytes(),
            Self::Pooled(chunk) => chunk.as_bytes(),
        }
    }

    /// Returns a sliced view relative to the currently visible bytes.
    pub fn slice(&self, offset: usize, len: usize) -> io::Result<Self> {
        match self {
            Self::Shared(bytes) => {
                validate_slice_bounds(bytes.len(), offset, len)?;
                Ok(Self::Shared(bytes.slice(offset..offset + len)))
            }
            Self::Mapped(chunk) => chunk.slice(offset, len).map(Self::Mapped),
            Self::Pooled(chunk) => chunk.slice(offset, len).map(Self::Pooled),
        }
    }
}

/// Logical view into mapped file bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MappedChunk {
    bytes: Bytes,
    logical_offset: usize,
    logical_len: usize,
}

impl MappedChunk {
    pub fn new(bytes: Bytes, logical_offset: usize, logical_len: usize) -> io::Result<Self> {
        validate_slice_bounds(bytes.len(), logical_offset, logical_len)?;
        Ok(Self {
            bytes,
            logical_offset,
            logical_len,
        })
    }

    /// Returns the visible length of this mapped chunk.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.logical_len
    }

    /// Returns true when the chunk has no visible data.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.logical_len == 0
    }

    /// Returns the visible bytes for this logical view.
    #[must_use]
    pub fn as_bytes(&self) -> Bytes {
        self.bytes
            .slice(self.logical_offset..self.logical_offset.saturating_add(self.logical_len))
    }

    /// Returns a sliced logical view relative to the current logical view.
    pub fn slice(&self, offset: usize, len: usize) -> io::Result<Self> {
        validate_slice_bounds(self.logical_len, offset, len)?;
        Self::new(self.bytes.clone(), self.logical_offset + offset, len)
    }
}

/// Placeholder pooled chunk variant for commit 4.
///
/// This is backed by a `PooledBuffer` and exposes a visible read-only window.
#[derive(Debug)]
pub struct PooledChunk {
    buffer: PooledBuffer,
    len: usize,
}

impl PooledChunk {
    pub fn new(buffer: PooledBuffer, len: usize) -> io::Result<Self> {
        validate_slice_bounds(buffer.len(), 0, len)?;
        Ok(Self { buffer, len })
    }

    /// Convenience constructor for detached test and compatibility values.
    pub fn from_bytes(bytes: Bytes) -> io::Result<Self> {
        let len = bytes.len();
        Self::new(PooledBuffer::from_bytes(bytes), len)
    }

    /// Returns the visible length of this pooled chunk.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.len
    }

    /// Returns true when the chunk has no visible data.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Returns the visible bytes for this pooled chunk.
    #[must_use]
    pub fn as_bytes(&self) -> Bytes {
        Bytes::copy_from_slice(&self.buffer[..self.len])
    }

    /// Returns a sliced pooled chunk relative to the current visible view.
    pub fn slice(&self, offset: usize, len: usize) -> io::Result<Self> {
        validate_slice_bounds(self.len, offset, len)?;
        Self::from_bytes(Bytes::copy_from_slice(&self.buffer[offset..offset + len]))
    }
}

fn validate_slice_bounds(visible_len: usize, offset: usize, len: usize) -> io::Result<()> {
    let end = offset
        .checked_add(len)
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "chunk slice overflows"))?;
    if end > visible_len {
        return Err(io::Error::new(io::ErrorKind::InvalidInput, "chunk slice exceeds visible length"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shared_chunk_len_and_slice() {
        let chunk = IoChunk::Shared(Bytes::from_static(b"abcdef"));
        assert_eq!(chunk.len(), 6);
        assert!(!chunk.is_empty());
        assert_eq!(chunk.as_bytes(), Bytes::from_static(b"abcdef"));
        assert_eq!(chunk.slice(1, 3).unwrap().as_bytes(), Bytes::from_static(b"bcd"));
    }

    #[test]
    fn test_mapped_chunk_len_and_slice() {
        let chunk = MappedChunk::new(Bytes::from_static(b"abcdefgh"), 2, 4).unwrap();
        assert_eq!(chunk.len(), 4);
        assert_eq!(chunk.as_bytes(), Bytes::from_static(b"cdef"));
        assert_eq!(chunk.slice(1, 2).unwrap().as_bytes(), Bytes::from_static(b"de"));
    }

    #[test]
    fn test_pooled_chunk_len_and_as_bytes() {
        let chunk = PooledChunk::from_bytes(Bytes::from_static(b"hello")).unwrap();
        assert_eq!(chunk.len(), 5);
        assert_eq!(chunk.as_bytes(), Bytes::from_static(b"hello"));
        assert_eq!(chunk.slice(1, 3).unwrap().as_bytes(), Bytes::from_static(b"ell"));
    }

    #[test]
    fn test_io_chunk_as_bytes_for_all_variants() {
        let shared = IoChunk::Shared(Bytes::from_static(b"s"));
        let mapped = IoChunk::Mapped(MappedChunk::new(Bytes::from_static(b"mapped"), 0, 6).unwrap());
        let pooled = IoChunk::Pooled(PooledChunk::from_bytes(Bytes::from_static(b"p")).unwrap());

        assert_eq!(shared.as_bytes(), Bytes::from_static(b"s"));
        assert_eq!(mapped.as_bytes(), Bytes::from_static(b"mapped"));
        assert_eq!(pooled.as_bytes(), Bytes::from_static(b"p"));
    }
}
