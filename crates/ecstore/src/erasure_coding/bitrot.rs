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

use bytes::Bytes;
use pin_project_lite::pin_project;
use rustfs_utils::HashAlgorithm;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tracing::error;
use uuid::Uuid;

pin_project! {
    /// BitrotReader reads (hash+data) blocks from an async reader and verifies hash integrity.
    pub struct BitrotReader<R> {
        #[pin]
        inner: R,
        hash_algo: HashAlgorithm,
        shard_size: usize,
        buf: Vec<u8>,
        hash_buf: Vec<u8>,
        // hash_read: usize,
        // data_buf: Vec<u8>,
        // data_read: usize,
        // hash_checked: bool,
        id: Uuid,
    }
}

impl<R> BitrotReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    /// Create a new BitrotReader.
    pub fn new(inner: R, shard_size: usize, algo: HashAlgorithm) -> Self {
        let hash_size = algo.size();
        Self {
            inner,
            hash_algo: algo,
            shard_size,
            buf: Vec::new(),
            hash_buf: vec![0u8; hash_size],
            // hash_read: 0,
            // data_buf: Vec::new(),
            // data_read: 0,
            // hash_checked: false,
            id: Uuid::new_v4(),
        }
    }

    /// Read a single (hash+data) block, verify hash, and return the number of bytes read into `out`.
    /// Returns an error if hash verification fails or data exceeds shard_size.
    pub async fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        if out.len() > self.shard_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("data size {} exceeds shard size {}", out.len(), self.shard_size),
            ));
        }

        let hash_size = self.hash_algo.size();
        // Read hash

        if hash_size > 0 {
            self.inner.read_exact(&mut self.hash_buf).await.map_err(|e| {
                error!("bitrot reader read hash error: {}", e);
                e
            })?;
        }

        // Read data
        let mut data_len = 0;
        while data_len < out.len() {
            let n = self.inner.read(&mut out[data_len..]).await.map_err(|e| {
                error!("bitrot reader read data error: {}", e);
                e
            })?;
            if n == 0 {
                break;
            }
            data_len += n;
        }

        if hash_size > 0 {
            let actual_hash = self.hash_algo.hash_encode(&out[..data_len]);
            if actual_hash.as_ref() != self.hash_buf.as_slice() {
                error!("bitrot reader hash mismatch, id={} data_len={}, out_len={}", self.id, data_len, out.len());
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bitrot hash mismatch"));
            }
        }
        Ok(data_len)
    }
}

pin_project! {
    /// BitrotWriter writes (hash+data) blocks to an async writer.
    pub struct BitrotWriter<W> {
        #[pin]
        inner: W,
        hash_algo: HashAlgorithm,
        shard_size: usize,
        finished: bool,
    }
}

impl<W> BitrotWriter<W>
where
    W: AsyncWrite + Unpin + Send + Sync,
{
    /// Create a new BitrotWriter.
    pub fn new(inner: W, shard_size: usize, algo: HashAlgorithm) -> Self {
        let hash_algo = algo;
        Self {
            inner,
            hash_algo,
            shard_size,
            finished: false,
        }
    }

    pub fn into_inner(self) -> W {
        self.inner
    }

    /// Write a (hash+data) block. Returns the number of data bytes written.
    /// Returns an error if called after a short write or if data exceeds shard_size.
    pub async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.is_empty() {
            return Ok(0);
        }

        if self.finished {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "bitrot writer already finished"));
        }

        if buf.len() > self.shard_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!("data size {} exceeds shard size {}", buf.len(), self.shard_size),
            ));
        }

        if buf.len() < self.shard_size {
            self.finished = true;
        }

        let hash_algo = &self.hash_algo;

        if hash_algo.size() > 0 {
            let hash = hash_algo.hash_encode(buf);
            if hash.as_ref().is_empty() {
                error!("bitrot writer write hash error: hash is empty");
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, "hash is empty"));
            }
            self.inner.write_all(hash.as_ref()).await?;
        }

        self.inner.write_all(buf).await?;

        self.inner.flush().await?;

        let n = buf.len();

        Ok(n)
    }

    pub async fn shutdown(&mut self) -> std::io::Result<()> {
        self.inner.shutdown().await
    }
}

pub fn bitrot_shard_file_size(size: usize, shard_size: usize, algo: HashAlgorithm) -> usize {
    if algo != HashAlgorithm::HighwayHash256S {
        return size;
    }
    size.div_ceil(shard_size) * algo.size() + size
}

pub async fn bitrot_verify<R: AsyncRead + Unpin + Send>(
    mut r: R,
    want_size: usize,
    part_size: usize,
    algo: HashAlgorithm,
    _want: Bytes, // FIXME: useless parameter?
    mut shard_size: usize,
) -> std::io::Result<()> {
    let mut hash_buf = vec![0; algo.size()];
    let mut left = want_size;

    if left != bitrot_shard_file_size(part_size, shard_size, algo.clone()) {
        return Err(std::io::Error::other("bitrot shard file size mismatch"));
    }

    while left > 0 {
        let n = r.read_exact(&mut hash_buf).await?;
        left -= n;

        if left < shard_size {
            shard_size = left;
        }

        let mut buf = vec![0; shard_size];
        let read = r.read_exact(&mut buf).await?;

        let actual_hash = algo.hash_encode(&buf);
        if actual_hash.as_ref() != &hash_buf[0..n] {
            return Err(std::io::Error::other("bitrot hash mismatch"));
        }

        left -= read;
    }

    Ok(())
}

/// Custom writer enum that supports inline buffer storage
pub enum CustomWriter {
    /// Inline buffer writer - stores data in memory
    InlineBuffer(Vec<u8>),
    /// Disk-based writer using tokio file
    Other(Box<dyn AsyncWrite + Unpin + Send + Sync>),
}

impl CustomWriter {
    /// Create a new inline buffer writer
    pub fn new_inline_buffer() -> Self {
        Self::InlineBuffer(Vec::new())
    }

    /// Create a new disk writer from any AsyncWrite implementation
    pub fn new_tokio_writer<W>(writer: W) -> Self
    where
        W: AsyncWrite + Unpin + Send + Sync + 'static,
    {
        Self::Other(Box::new(writer))
    }

    /// Get the inline buffer data if this is an inline buffer writer
    pub fn get_inline_data(&self) -> Option<&[u8]> {
        match self {
            Self::InlineBuffer(data) => Some(data),
            Self::Other(_) => None,
        }
    }

    /// Extract the inline buffer data, consuming the writer
    pub fn into_inline_data(self) -> Option<Vec<u8>> {
        match self {
            Self::InlineBuffer(data) => Some(data),
            Self::Other(_) => None,
        }
    }
}

impl AsyncWrite for CustomWriter {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::InlineBuffer(data) => {
                data.extend_from_slice(buf);
                std::task::Poll::Ready(Ok(buf.len()))
            }
            Self::Other(writer) => {
                let pinned_writer = std::pin::Pin::new(writer.as_mut());
                pinned_writer.poll_write(cx, buf)
            }
        }
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::InlineBuffer(_) => std::task::Poll::Ready(Ok(())),
            Self::Other(writer) => {
                let pinned_writer = std::pin::Pin::new(writer.as_mut());
                pinned_writer.poll_flush(cx)
            }
        }
    }

    fn poll_shutdown(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::InlineBuffer(_) => std::task::Poll::Ready(Ok(())),
            Self::Other(writer) => {
                let pinned_writer = std::pin::Pin::new(writer.as_mut());
                pinned_writer.poll_shutdown(cx)
            }
        }
    }
}

/// Wrapper around BitrotWriter that uses our custom writer
pub struct BitrotWriterWrapper {
    bitrot_writer: BitrotWriter<CustomWriter>,
    writer_type: WriterType,
}

/// Enum to track the type of writer we're using
enum WriterType {
    InlineBuffer,
    Other,
}

impl std::fmt::Debug for BitrotWriterWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BitrotWriterWrapper")
            .field(
                "writer_type",
                &match self.writer_type {
                    WriterType::InlineBuffer => "InlineBuffer",
                    WriterType::Other => "Other",
                },
            )
            .finish()
    }
}

impl BitrotWriterWrapper {
    /// Create a new BitrotWriterWrapper with custom writer
    pub fn new(writer: CustomWriter, shard_size: usize, checksum_algo: HashAlgorithm) -> Self {
        let writer_type = match &writer {
            CustomWriter::InlineBuffer(_) => WriterType::InlineBuffer,
            CustomWriter::Other(_) => WriterType::Other,
        };

        Self {
            bitrot_writer: BitrotWriter::new(writer, shard_size, checksum_algo),
            writer_type,
        }
    }

    /// Write data to the bitrot writer
    pub async fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.bitrot_writer.write(buf).await
    }

    pub async fn shutdown(&mut self) -> std::io::Result<()> {
        self.bitrot_writer.shutdown().await
    }

    /// Extract the inline buffer data, consuming the wrapper
    pub fn into_inline_data(self) -> Option<Vec<u8>> {
        match self.writer_type {
            WriterType::InlineBuffer => {
                let writer = self.bitrot_writer.into_inner();
                writer.into_inline_data()
            }
            WriterType::Other => None,
        }
    }
}

#[cfg(test)]
mod tests {

    use super::BitrotReader;
    use super::BitrotWriter;
    use rustfs_utils::HashAlgorithm;
    use std::io::Cursor;

    #[tokio::test]
    async fn test_bitrot_read_write_ok() {
        let data = b"hello world! this is a test shard.";
        let data_size = data.len();
        let shard_size = 8;

        let buf: Vec<u8> = Vec::new();
        let writer = Cursor::new(buf);
        let mut bitrot_writer = BitrotWriter::new(writer, shard_size, HashAlgorithm::HighwayHash256);

        let mut n = 0;
        for chunk in data.chunks(shard_size) {
            n += bitrot_writer.write(chunk).await.unwrap();
        }
        assert_eq!(n, data.len());

        // Read
        let reader = bitrot_writer.into_inner();
        let reader = Cursor::new(reader.into_inner());
        let mut bitrot_reader = BitrotReader::new(reader, shard_size, HashAlgorithm::HighwayHash256);
        let mut out = Vec::new();
        let mut n = 0;
        while n < data_size {
            let mut buf = vec![0u8; shard_size];
            let m = bitrot_reader.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..m], &data[n..n + m]);

            out.extend_from_slice(&buf[..m]);
            n += m;
        }

        assert_eq!(n, data_size);
        assert_eq!(data, &out[..]);
    }

    #[tokio::test]
    async fn test_bitrot_read_hash_mismatch() {
        let data = b"test data for bitrot";
        let data_size = data.len();
        let shard_size = 8;
        let buf: Vec<u8> = Vec::new();
        let writer = Cursor::new(buf);
        let mut bitrot_writer = BitrotWriter::new(writer, shard_size, HashAlgorithm::HighwayHash256);
        for chunk in data.chunks(shard_size) {
            let _ = bitrot_writer.write(chunk).await.unwrap();
        }
        let mut written = bitrot_writer.into_inner().into_inner();
        // change the last byte to make hash mismatch
        let pos = written.len() - 1;
        written[pos] ^= 0xFF;
        let reader = Cursor::new(written);
        let mut bitrot_reader = BitrotReader::new(reader, shard_size, HashAlgorithm::HighwayHash256);

        let count = data_size.div_ceil(shard_size);

        let mut idx = 0;
        let mut n = 0;
        while n < data_size {
            let mut buf = vec![0u8; shard_size];
            let res = bitrot_reader.read(&mut buf).await;

            if idx == count - 1 {
                // The last chunk should trigger an error
                assert!(res.is_err());
                assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
                break;
            }

            let m = res.unwrap();

            assert_eq!(&buf[..m], &data[n..n + m]);

            n += m;
            idx += 1;
        }
    }

    #[tokio::test]
    async fn test_bitrot_read_write_none_hash() {
        let data = b"bitrot none hash test data!";
        let data_size = data.len();
        let shard_size = 8;

        let buf: Vec<u8> = Vec::new();
        let writer = Cursor::new(buf);
        let mut bitrot_writer = BitrotWriter::new(writer, shard_size, HashAlgorithm::None);

        let mut n = 0;
        for chunk in data.chunks(shard_size) {
            n += bitrot_writer.write(chunk).await.unwrap();
        }
        assert_eq!(n, data.len());

        let reader = bitrot_writer.into_inner();
        let reader = Cursor::new(reader.into_inner());
        let mut bitrot_reader = BitrotReader::new(reader, shard_size, HashAlgorithm::None);
        let mut out = Vec::new();
        let mut n = 0;
        while n < data_size {
            let mut buf = vec![0u8; shard_size];
            let m = bitrot_reader.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..m], &data[n..n + m]);
            out.extend_from_slice(&buf[..m]);
            n += m;
        }
        assert_eq!(n, data_size);
        assert_eq!(data, &out[..]);
    }
}
