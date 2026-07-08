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

use pin_project_lite::pin_project;
use rustfs_utils::HashAlgorithm;
use std::io::IoSlice;
use std::time::Duration;
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
        skip_verify: bool,
        last_verify_duration: Duration,
        id: Uuid,
    }
}

impl<R> BitrotReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    /// Create a new BitrotReader.
    pub fn new(inner: R, shard_size: usize, algo: HashAlgorithm, skip_verify: bool) -> Self {
        let hash_size = algo.size();
        Self {
            inner,
            hash_algo: algo,
            shard_size,
            buf: Vec::new(),
            hash_buf: vec![0u8; hash_size],
            skip_verify,
            last_verify_duration: Duration::ZERO,
            id: Uuid::new_v4(),
        }
    }

    pub(crate) fn last_verify_duration(&self) -> Duration {
        self.last_verify_duration
    }

    /// Read a single (hash+data) block, verify hash, and return the number of bytes read into `out`.
    /// Returns an error if hash verification fails or data exceeds shard_size.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        self.last_verify_duration = Duration::ZERO;
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

        // A short read (EOF before the caller-sized buffer is filled) means a
        // truncated/incomplete shard. Return an error so the caller drops this
        // reader from the stripe and reconstruction from parity engages, instead
        // of silently returning fewer bytes and shifting every downstream byte
        // (backlog#799 B2). This is deliberately BEFORE and independent of the
        // bitrot hash check so it also fires under skip_verify /
        // HashAlgorithm::None / parity=0, where there is no hash to catch it. The
        // caller sizes `out` to exactly the expected shard length for the current
        // stripe, so "buffer full" == "shard complete".
        if data_len < out.len() {
            error!("bitrot reader short shard read: id={} got {} of {} bytes", self.id, data_len, out.len());
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                format!("short shard read: got {data_len} of {} bytes", out.len()),
            ));
        }

        if hash_size > 0 && !self.skip_verify {
            let verify_start = std::time::Instant::now();
            let actual_hash = self.hash_algo.hash_encode(&out[..data_len]);
            self.last_verify_duration = verify_start.elapsed();
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
    #[cfg_attr(feature = "hotpath", hotpath::measure(label = "BitrotWriter::write"))]
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
            write_all_vectored(&mut self.inner, hash.as_ref(), buf).await?;
        } else {
            self.inner.write_all(buf).await?;
        }

        let n = buf.len();

        Ok(n)
    }

    pub async fn shutdown(&mut self) -> std::io::Result<()> {
        self.inner.flush().await?;
        self.inner.shutdown().await
    }
}

async fn write_all_vectored<W>(writer: &mut W, hash: &[u8], data: &[u8]) -> std::io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut hash_offset = 0;
    let mut data_offset = 0;

    while hash_offset < hash.len() || data_offset < data.len() {
        let slices = [IoSlice::new(&hash[hash_offset..]), IoSlice::new(&data[data_offset..])];
        let written = writer.write_vectored(&slices).await?;
        if written == 0 {
            return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "failed to write hash and data"));
        }

        let hash_remaining = hash.len() - hash_offset;
        if written < hash_remaining {
            hash_offset += written;
            continue;
        }

        hash_offset = hash.len();
        data_offset += written - hash_remaining;
    }

    Ok(())
}

pub fn bitrot_shard_file_size(size: usize, shard_size: usize, algo: HashAlgorithm) -> usize {
    if algo != HashAlgorithm::HighwayHash256S && algo != HashAlgorithm::HighwayHash256SLegacy {
        return size;
    }
    size.div_ceil(shard_size) * algo.size() + size
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub async fn bitrot_verify<R: AsyncRead + Unpin + Send>(
    mut r: R,
    want_size: usize,
    part_size: usize,
    algo: HashAlgorithm,
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

    fn poll_write_vectored(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        bufs: &[IoSlice<'_>],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Self::InlineBuffer(data) => {
                let total = bufs.iter().map(|buf| buf.len()).sum::<usize>();
                for buf in bufs {
                    data.extend_from_slice(buf);
                }
                std::task::Poll::Ready(Ok(total))
            }
            Self::Other(writer) => {
                let pinned_writer = std::pin::Pin::new(writer.as_mut());
                pinned_writer.poll_write_vectored(cx, bufs)
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        match self {
            Self::InlineBuffer(_) => true,
            Self::Other(writer) => writer.is_write_vectored(),
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
    use std::io::{Cursor, IoSlice};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::{Context, Poll};
    use tokio::io::AsyncWrite;

    #[derive(Default)]
    struct VectoredCountingWriter {
        vectored_writes: Arc<AtomicUsize>,
        writes: Vec<u8>,
    }

    impl AsyncWrite for VectoredCountingWriter {
        fn poll_write(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Err(std::io::Error::other("poll_write should not be used")))
        }

        fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_write_vectored(
            mut self: std::pin::Pin<&mut Self>,
            _cx: &mut Context<'_>,
            bufs: &[IoSlice<'_>],
        ) -> Poll<std::io::Result<usize>> {
            self.vectored_writes.fetch_add(1, Ordering::SeqCst);
            let total = bufs.iter().map(|buf| buf.len()).sum::<usize>();
            for buf in bufs {
                self.writes.extend_from_slice(buf);
            }
            Poll::Ready(Ok(total))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }
    }

    #[derive(Default)]
    struct CountingWriter {
        flushes: Arc<AtomicUsize>,
        shutdowns: Arc<AtomicUsize>,
        writes: Vec<u8>,
    }

    impl AsyncWrite for CountingWriter {
        fn poll_write(mut self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            self.writes.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.flushes.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: std::pin::Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            self.shutdowns.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(Ok(()))
        }
    }

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
        let mut bitrot_reader = BitrotReader::new(reader, shard_size, HashAlgorithm::HighwayHash256, false);
        let mut out = Vec::new();
        let mut n = 0;
        while n < data_size {
            // Size the buffer to the expected shard length for this stripe (the
            // last stripe is legitimately shorter); BitrotReader now requires the
            // buffer to be filled exactly, matching how the decode/heal paths size
            // per-stripe shard buffers (backlog#799 B2).
            let this_size = shard_size.min(data_size - n);
            let mut buf = vec![0u8; this_size];
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
        let mut bitrot_reader = BitrotReader::new(reader, shard_size, HashAlgorithm::HighwayHash256, false);

        let count = data_size.div_ceil(shard_size);

        let mut idx = 0;
        let mut n = 0;
        while n < data_size {
            let this_size = shard_size.min(data_size - n);
            let mut buf = vec![0u8; this_size];
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
        let mut bitrot_reader = BitrotReader::new(reader, shard_size, HashAlgorithm::None, false);
        let mut out = Vec::new();
        let mut n = 0;
        while n < data_size {
            // Size the buffer to the expected shard length for this stripe (the
            // last stripe is legitimately shorter); BitrotReader now requires the
            // buffer to be filled exactly, matching how the decode/heal paths size
            // per-stripe shard buffers (backlog#799 B2).
            let this_size = shard_size.min(data_size - n);
            let mut buf = vec![0u8; this_size];
            let m = bitrot_reader.read(&mut buf).await.unwrap();
            assert_eq!(&buf[..m], &data[n..n + m]);
            out.extend_from_slice(&buf[..m]);
            n += m;
        }
        assert_eq!(n, data_size);
        assert_eq!(data, &out[..]);
    }

    #[tokio::test]
    async fn bitrot_read_short_shard_errors_even_when_skip_verify() {
        // A truncated shard (fewer bytes than the caller's expected-size buffer)
        // must be an error, not a silent Ok(short) — including on the skip-verify
        // / no-hash paths where there is no bitrot hash to catch it. This is the
        // core B2 fix (backlog#799): a short read is a shard error so the decoder
        // drops it and reconstructs from parity instead of shifting downstream
        // bytes.
        let shard_size = 16usize;
        for (algo, skip_verify) in [
            (HashAlgorithm::None, true),
            (HashAlgorithm::None, false),
            (HashAlgorithm::HighwayHash256, true),
        ] {
            let label = format!("{algo:?}");
            let writer = Cursor::new(Vec::<u8>::new());
            let mut w = BitrotWriter::new(writer, shard_size, algo.clone());
            w.write(&[7u8; 16]).await.unwrap();
            let written = w.into_inner().into_inner();
            // Drop the last 4 data bytes so the shard is truncated.
            let truncated = written[..written.len() - 4].to_vec();

            let mut r = BitrotReader::new(Cursor::new(truncated), shard_size, algo, skip_verify);
            let mut out = vec![0u8; shard_size];
            let res = r.read(&mut out).await;
            assert!(res.is_err(), "short shard must error (algo={label}, skip_verify={skip_verify})");
            assert_eq!(
                res.unwrap_err().kind(),
                std::io::ErrorKind::UnexpectedEof,
                "short shard must be UnexpectedEof (algo={label}, skip_verify={skip_verify})"
            );
        }
    }

    #[tokio::test]
    async fn test_bitrot_writer_flushes_once_on_shutdown() {
        let flushes = Arc::new(AtomicUsize::new(0));
        let shutdowns = Arc::new(AtomicUsize::new(0));
        let writer = CountingWriter {
            flushes: flushes.clone(),
            shutdowns: shutdowns.clone(),
            writes: Vec::new(),
        };
        let mut bitrot_writer = BitrotWriter::new(writer, 8, HashAlgorithm::None);

        bitrot_writer.write(b"12345678").await.unwrap();
        bitrot_writer.write(b"abc").await.unwrap();

        assert_eq!(flushes.load(Ordering::SeqCst), 0);
        assert_eq!(shutdowns.load(Ordering::SeqCst), 0);

        bitrot_writer.shutdown().await.unwrap();

        assert_eq!(flushes.load(Ordering::SeqCst), 1);
        assert_eq!(shutdowns.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_bitrot_writer_uses_vectored_write_for_hash_and_data() {
        let vectored_writes = Arc::new(AtomicUsize::new(0));
        let writer = VectoredCountingWriter {
            vectored_writes: vectored_writes.clone(),
            writes: Vec::new(),
        };
        let mut bitrot_writer = BitrotWriter::new(writer, 8, HashAlgorithm::HighwayHash256);

        bitrot_writer.write(b"payload").await.unwrap();

        assert!(vectored_writes.load(Ordering::SeqCst) > 0);
    }
}
