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

use crate::compress_index::{Index, TryGetIndex};
use pin_project_lite::pin_project;
use rustfs_utils::compress::{CompressionAlgorithm, compress_block, decompress_block};
use rustfs_utils::{put_uvarint, uvarint};
use std::cmp::min;
use std::io::{self};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
// use tracing::error;

const COMPRESS_TYPE_COMPRESSED: u8 = 0x00;
const COMPRESS_TYPE_UNCOMPRESSED: u8 = 0x01;
const COMPRESS_TYPE_END: u8 = 0xFF;

const DEFAULT_BLOCK_SIZE: usize = 1 << 20; // 1MB
const HEADER_LEN: usize = 8;

pin_project! {
    #[derive(Debug)]
    /// A reader wrapper that compresses data on the fly using DEFLATE algorithm.
    pub struct CompressReader<R> {
        #[pin]
        pub inner: R,
        buffer: Vec<u8>,
        pos: usize,
        done: bool,
        block_size: usize,
        compression_algorithm: CompressionAlgorithm,
        index: Index,
        written: usize,
        uncomp_written: usize,
        temp_buffer: Vec<u8>,
        read_buffer: Vec<u8>,
    }
}

impl<R> CompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub fn new(inner: R, compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
            pos: 0,
            done: false,
            compression_algorithm,
            block_size: DEFAULT_BLOCK_SIZE,
            index: Index::new(),
            written: 0,
            uncomp_written: 0,
            temp_buffer: Vec::with_capacity(DEFAULT_BLOCK_SIZE),
            read_buffer: vec![0u8; DEFAULT_BLOCK_SIZE],
        }
    }

    /// Optional: allow users to customize block_size
    pub fn with_block_size(inner: R, block_size: usize, compression_algorithm: CompressionAlgorithm) -> Self {
        debug_assert!(block_size > 0, "CompressReader block_size must be non-zero");
        Self {
            inner,
            buffer: Vec::new(),
            pos: 0,
            done: false,
            compression_algorithm,
            block_size,
            index: Index::new(),
            written: 0,
            uncomp_written: 0,
            temp_buffer: Vec::with_capacity(block_size),
            read_buffer: vec![0u8; block_size],
        }
    }
}

impl<R> TryGetIndex for CompressReader<R> {
    fn try_get_index(&self) -> Option<&Index> {
        Some(&self.index)
    }
}

impl<R> AsyncRead for CompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();
        // Copy from buffer first if available
        if *this.pos < this.buffer.len() {
            let to_copy = min(buf.remaining(), this.buffer.len() - *this.pos);
            buf.put_slice(&this.buffer[*this.pos..*this.pos + to_copy]);
            *this.pos += to_copy;
            if *this.pos == this.buffer.len() {
                this.buffer.clear();
                *this.pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        if *this.done {
            return Poll::Ready(Ok(()));
        }
        // Fill temporary buffer
        while this.temp_buffer.len() < *this.block_size {
            let remaining = *this.block_size - this.temp_buffer.len();
            let mut temp_buf = ReadBuf::new(&mut this.read_buffer[..remaining]);
            match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                Poll::Pending => {
                    if this.temp_buffer.is_empty() {
                        return Poll::Pending;
                    }
                    break;
                }
                Poll::Ready(Ok(())) => {
                    let n = temp_buf.filled().len();
                    if n == 0 {
                        if this.temp_buffer.is_empty() {
                            *this.done = true;
                            return Poll::Ready(Ok(()));
                        }
                        break;
                    }
                    this.temp_buffer.extend_from_slice(&temp_buf.filled()[..n]);
                }
                Poll::Ready(Err(e)) => {
                    // error!("CompressReader poll_read: read inner error: {e}");
                    return Poll::Ready(Err(e));
                }
            }
        }
        // Process accumulated data
        if !this.temp_buffer.is_empty() {
            let uncompressed_data = &this.temp_buffer;
            let out = build_compressed_block(uncompressed_data, *this.compression_algorithm);
            *this.written += out.len();
            *this.uncomp_written += uncompressed_data.len();
            if let Err(e) = this.index.add(*this.written as i64, *this.uncomp_written as i64) {
                // error!("CompressReader index add error: {e}");
                return Poll::Ready(Err(e));
            }
            *this.buffer = out;
            *this.pos = 0;
            this.temp_buffer.clear(); // More efficient way to clear
            let to_copy = min(buf.remaining(), this.buffer.len());
            buf.put_slice(&this.buffer[..to_copy]);
            *this.pos += to_copy;
            if *this.pos == this.buffer.len() {
                this.buffer.clear();
                *this.pos = 0;
            }
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }
}

delegate_reader_capabilities_generic_no_index!(CompressReader<R>, inner);

pin_project! {
    /// A reader wrapper that decompresses data on the fly using DEFLATE algorithm.
    /// Header format:
    /// - First byte: compression type (00 = compressed, 01 = uncompressed, FF = end)
    /// - Bytes 1-3: length of compressed data (little-endian)
    /// - Bytes 4-7: CRC32 checksum of uncompressed data (little-endian)
    #[derive(Debug)]
    pub struct DecompressReader<R> {
        #[pin]
        pub inner: R,
        buffer: Vec<u8>,
        buffer_pos: usize,
        finished: bool,
        // A previously surfaced stream error is sticky: without this, a caller
        // that polls again after an error would restart at the header phase and
        // read a truncated tail as a clean EOF, converting the error into a
        // silently short body.
        poisoned: bool,
        // Fields for saving header read progress across polls
        header_buf: [u8; 8],
        header_read: usize,
        // Fields for saving compressed block read progress across polls.
        // `compressed_len > 0` means a block payload is in flight: the header has
        // been fully parsed and `compressed_read` bytes of the payload are already
        // consumed from the inner stream. The header phase must not run again (and
        // must not reset `compressed_read`) until this block completes, or a
        // `Poll::Pending` in the middle of a payload would silently drop the bytes
        // read so far and desynchronize the block framing.
        compressed_buf: Vec<u8>,
        compressed_read: usize,
        compressed_len: usize,
        compression_algorithm: CompressionAlgorithm,
    }
}

impl<R> DecompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub fn new(inner: R, compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
            buffer_pos: 0,
            finished: false,
            poisoned: false,
            header_buf: [0u8; 8],
            header_read: 0,
            compressed_buf: Vec::new(),
            compressed_read: 0,
            compressed_len: 0,
            compression_algorithm,
        }
    }
}

impl<R> AsyncRead for DecompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();
        // Copy from buffer first if available
        if *this.buffer_pos < this.buffer.len() {
            let to_copy = min(buf.remaining(), this.buffer.len() - *this.buffer_pos);
            buf.put_slice(&this.buffer[*this.buffer_pos..*this.buffer_pos + to_copy]);
            *this.buffer_pos += to_copy;
            if *this.buffer_pos == this.buffer.len() {
                this.buffer.clear();
                *this.buffer_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
        if *this.finished {
            return Poll::Ready(Ok(()));
        }
        if *this.poisoned {
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "decompress reader previously failed")));
        }

        if *this.compressed_len == 0 {
            // Read the 8-byte block header, resuming across polls via `header_read`.
            while *this.header_read < HEADER_LEN {
                let mut temp = [0u8; HEADER_LEN];
                let mut temp_buf = ReadBuf::new(&mut temp[0..HEADER_LEN - *this.header_read]);
                match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        let n = temp_buf.filled().len();
                        if n == 0 {
                            if *this.header_read == 0 {
                                // Clean EOF on a block boundary.
                                *this.finished = true;
                                return Poll::Ready(Ok(()));
                            }
                            *this.poisoned = true;
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "unexpected EOF while reading compressed block header",
                            )));
                        }
                        this.header_buf[*this.header_read..*this.header_read + n].copy_from_slice(&temp_buf.filled()[..n]);
                        *this.header_read += n;
                    }
                    Poll::Ready(Err(e)) => {
                        // error!("DecompressReader poll_read: read header error: {e}");
                        *this.poisoned = true;
                        return Poll::Ready(Err(e));
                    }
                }
            }

            let typ = this.header_buf[0];
            let len =
                (this.header_buf[1] as usize) | ((this.header_buf[2] as usize) << 8) | ((this.header_buf[3] as usize) << 16);
            *this.header_read = 0;

            // `CompressReader` never emits an end block — a stream terminates on
            // inner EOF, which is what lets concatenated per-part streams decode as
            // one. This branch is kept for streams that do carry the marker.
            if typ == COMPRESS_TYPE_END {
                *this.compressed_read = 0;
                *this.compressed_len = 0;
                *this.finished = true;
                return Poll::Ready(Ok(()));
            }
            if typ != COMPRESS_TYPE_COMPRESSED && typ != COMPRESS_TYPE_UNCOMPRESSED {
                // error!("DecompressReader unknown compression type: {typ}");
                *this.poisoned = true;
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown compression type")));
            }
            if len == 0 {
                *this.poisoned = true;
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid compressed block length")));
            }

            if this.compressed_buf.len() < len {
                this.compressed_buf.resize(len, 0);
            }
            *this.compressed_len = len;
            *this.compressed_read = 0;
        }

        // Fill the in-flight block payload, resuming across polls via `compressed_read`.
        while *this.compressed_read < *this.compressed_len {
            let mut temp_buf = ReadBuf::new(&mut this.compressed_buf[*this.compressed_read..*this.compressed_len]);
            match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {
                    let n = temp_buf.filled().len();
                    if n == 0 {
                        *this.compressed_read = 0;
                        *this.compressed_len = 0;
                        *this.poisoned = true;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "unexpected EOF while reading compressed block payload",
                        )));
                    }
                    *this.compressed_read += n;
                }
                Poll::Ready(Err(e)) => {
                    // error!("DecompressReader poll_read: read compressed block error: {e}");
                    *this.compressed_read = 0;
                    *this.compressed_len = 0;
                    *this.poisoned = true;
                    return Poll::Ready(Err(e));
                }
            }
        }

        let typ = this.header_buf[0];
        let crc = (this.header_buf[4] as u32)
            | ((this.header_buf[5] as u32) << 8)
            | ((this.header_buf[6] as u32) << 16)
            | ((this.header_buf[7] as u32) << 24);
        let compressed_buf = &this.compressed_buf[..*this.compressed_len];
        // `compressed_buf`'s length comes from the untrusted 24-bit header length field, so it
        // can be shorter than 16 bytes. `uvarint` is safe on any slice length (reads at most 10
        // bytes and stops at the terminator), so pass the whole slice instead of a fixed
        // `[0..16]` index that panics on corrupted/truncated blocks shorter than 16 bytes.
        let (uncompress_len, uvarint) = uvarint(compressed_buf);
        // Reject a length prefix that could not be decoded: `uvarint <= 0` means the varint was
        // empty/unterminated (0) or overflowed (negative — as usize it would index far past the
        // buffer and panic the slice below). The `> len` bound is belt-and-suspenders (uvarint's
        // positive return is always <= buf.len()) but keeps the slice panic-free regardless.
        if uvarint <= 0 || uvarint as usize > compressed_buf.len() {
            *this.compressed_read = 0;
            *this.compressed_len = 0;
            *this.poisoned = true;
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "Invalid compressed block length prefix")));
        }
        let compressed_data = &compressed_buf[uvarint as usize..];
        let decompressed = if typ == COMPRESS_TYPE_COMPRESSED {
            match decompress_block(compressed_data, *this.compression_algorithm) {
                Ok(out) => out,
                Err(e) => {
                    // error!("DecompressReader decompress_block error: {e}");
                    *this.compressed_read = 0;
                    *this.compressed_len = 0;
                    *this.poisoned = true;
                    return Poll::Ready(Err(e));
                }
            }
        } else {
            // The header phase already rejected every type other than
            // COMPRESS_TYPE_COMPRESSED / COMPRESS_TYPE_UNCOMPRESSED.
            compressed_data.to_vec()
        };
        if decompressed.is_empty() {
            // The writer never emits zero-length plaintext blocks; an empty
            // decode surfacing as Ready(Ok) with no bytes would read as EOF and
            // silently truncate the stream.
            *this.poisoned = true;
            *this.compressed_read = 0;
            *this.compressed_len = 0;
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "Empty compressed block")));
        }
        if decompressed.len() != uncompress_len as usize {
            // error!("DecompressReader decompressed length mismatch: {} != {}", decompressed.len(), uncompress_len);
            *this.compressed_read = 0;
            *this.compressed_len = 0;
            *this.poisoned = true;
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "Decompressed length mismatch")));
        }
        let actual_crc = {
            let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
            hasher.update(&decompressed);
            hasher.finalize() as u32
        };
        if actual_crc != crc {
            // error!("DecompressReader CRC32 mismatch: actual {actual_crc} != expected {crc}");
            *this.compressed_read = 0;
            *this.compressed_len = 0;
            *this.poisoned = true;
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "CRC32 mismatch")));
        }
        *this.buffer = decompressed;
        *this.buffer_pos = 0;
        *this.compressed_read = 0;
        *this.compressed_len = 0;
        let to_copy = min(buf.remaining(), this.buffer.len());
        buf.put_slice(&this.buffer[..to_copy]);
        *this.buffer_pos += to_copy;
        if *this.buffer_pos == this.buffer.len() {
            this.buffer.clear();
            *this.buffer_pos = 0;
        }
        Poll::Ready(Ok(()))
    }
}

delegate_reader_capabilities_generic_no_index!(DecompressReader<R>, inner);

/// Build compressed block with header + uvarint + compressed data
fn build_compressed_block(uncompressed_data: &[u8], compression_algorithm: CompressionAlgorithm) -> Vec<u8> {
    let crc = {
        let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
        hasher.update(uncompressed_data);
        hasher.finalize() as u32
    };
    let compressed_data = compress_block(uncompressed_data, compression_algorithm);
    let uncompressed_len = uncompressed_data.len();
    let mut uncompressed_len_buf = [0u8; 10];
    let int_len = put_uvarint(&mut uncompressed_len_buf[..], uncompressed_len as u64);
    let len = compressed_data.len() + int_len;
    let mut header = [0u8; HEADER_LEN];
    header[0] = COMPRESS_TYPE_COMPRESSED;
    header[1] = (len & 0xFF) as u8;
    header[2] = ((len >> 8) & 0xFF) as u8;
    header[3] = ((len >> 16) & 0xFF) as u8;
    header[4] = (crc & 0xFF) as u8;
    header[5] = ((crc >> 8) & 0xFF) as u8;
    header[6] = ((crc >> 16) & 0xFF) as u8;
    header[7] = ((crc >> 24) & 0xFF) as u8;
    let mut out = Vec::with_capacity(len + HEADER_LEN);
    out.extend_from_slice(&header);
    out.extend_from_slice(&uncompressed_len_buf[..int_len]);
    out.extend_from_slice(&compressed_data);
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngExt;
    use std::io::Cursor;
    use tokio::io::{AsyncReadExt, BufReader};

    #[tokio::test]
    async fn test_compress_reader_basic() {
        let data = b"hello world, hello world, hello world!";
        let reader = Cursor::new(&data[..]);
        let mut compress_reader = CompressReader::new(reader, CompressionAlgorithm::Gzip);

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        // DecompressReader unpacking
        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::Gzip);
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, data);
    }

    #[tokio::test]
    async fn test_compress_reader_basic_deflate() {
        let data = b"hello world, hello world, hello world!";
        let reader = BufReader::new(&data[..]);
        let mut compress_reader = CompressReader::new(reader, CompressionAlgorithm::Deflate);

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        // DecompressReader unpacking
        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::Deflate);
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, data);
    }

    #[tokio::test]
    async fn test_compress_reader_empty() {
        let data = b"";
        let reader = BufReader::new(&data[..]);
        let mut compress_reader = CompressReader::new(reader, CompressionAlgorithm::Gzip);

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::Gzip);
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, data);
    }

    #[tokio::test]
    async fn test_compress_reader_large() {
        // Generate 1MB of random bytes
        let mut data = vec![0u8; 1024 * 1024 * 32];
        rand::rng().fill(&mut data[..]);
        let reader = Cursor::new(data.clone());
        let mut compress_reader = CompressReader::new(reader, CompressionAlgorithm::Gzip);

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::Gzip);
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, &data);
    }

    #[tokio::test]
    async fn test_compress_reader_large_deflate() {
        // Generate 1MB of random bytes
        let mut data = vec![0u8; 1024 * 1024 * 3 + 512];
        rand::rng().fill(&mut data[..]);
        let reader = Cursor::new(data.clone());
        let mut compress_reader = CompressReader::new(reader, CompressionAlgorithm::default());

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::default());
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, &data);
    }

    /// Wraps a reader so every other poll returns `Poll::Pending` and every
    /// `Ready` poll serves at most `chunk` bytes. This is the shape a duplex
    /// pipe produces when the erasure writer is slower than the decoder, which
    /// is exactly what desynchronized the block framing before the resumable
    /// payload state was added (rustfs/rustfs#5957 multipart GET truncation).
    struct PendingChunkReader<R> {
        inner: R,
        chunk: usize,
        pending_next: bool,
    }

    impl<R> PendingChunkReader<R> {
        fn new(inner: R, chunk: usize) -> Self {
            Self {
                inner,
                chunk,
                pending_next: true,
            }
        }
    }

    impl<R: AsyncRead + Unpin> AsyncRead for PendingChunkReader<R> {
        fn poll_read(
            mut self: std::pin::Pin<&mut Self>,
            cx: &mut std::task::Context<'_>,
            buf: &mut tokio::io::ReadBuf<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            if self.pending_next {
                self.pending_next = false;
                cx.waker().wake_by_ref();
                return std::task::Poll::Pending;
            }
            self.pending_next = true;
            let cap = self.chunk.min(buf.remaining());
            let mut scratch = vec![0u8; cap];
            let mut inner_buf = tokio::io::ReadBuf::new(&mut scratch);
            match std::pin::Pin::new(&mut self.inner).poll_read(cx, &mut inner_buf) {
                std::task::Poll::Ready(Ok(())) => {
                    buf.put_slice(inner_buf.filled());
                    std::task::Poll::Ready(Ok(()))
                }
                other => other,
            }
        }
    }

    fn patterned_payload(size: usize, seed: u8) -> Vec<u8> {
        (0..size)
            .map(|i| ((i as u64).wrapping_mul(2_654_435_761).wrapping_add(seed as u64) >> 3) as u8)
            .collect()
    }

    /// Root-cause regression for the multipart compressed GET truncation: a
    /// `Poll::Pending` in the middle of a block payload must not drop the bytes
    /// already consumed. Before the resumable payload state, the decoder reset
    /// `compressed_read` on every re-poll and surfaced
    /// `LZ4 error: ERROR_frameType_unknown` mid-stream.
    #[tokio::test]
    async fn test_decompress_reader_survives_pending_mid_payload() {
        let data = patterned_payload(100 * 1024, 7);
        let mut compress_reader =
            CompressReader::with_block_size(Cursor::new(data.clone()), 8192, CompressionAlgorithm::default());
        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        for chunk in [1usize, 3, 7, 8, 17, 1000, 8192] {
            let inner = PendingChunkReader::new(Cursor::new(compressed.clone()), chunk);
            let mut decompress_reader = DecompressReader::new(inner, CompressionAlgorithm::default());
            let mut decompressed = Vec::new();
            decompress_reader.read_to_end(&mut decompressed).await.unwrap();
            assert_eq!(decompressed, data, "pending-chunked decode must be byte-exact for chunk={chunk}");
        }
    }

    /// Two independently compressed streams concatenated back to back — the
    /// on-disk shape of a compressed multipart object — must decode across the
    /// stream boundary even when every poll can suspend mid-block.
    #[tokio::test]
    async fn test_decompress_reader_survives_pending_across_concatenated_streams() {
        let part1 = patterned_payload(64 * 1024, 7);
        let part2 = patterned_payload(24 * 1024, 61);

        let mut stored = Vec::new();
        for part in [&part1, &part2] {
            let mut compress_reader =
                CompressReader::with_block_size(Cursor::new(part.clone()), 8192, CompressionAlgorithm::default());
            let mut compressed = Vec::new();
            compress_reader.read_to_end(&mut compressed).await.unwrap();
            stored.extend_from_slice(&compressed);
        }

        let mut expected = part1;
        expected.extend_from_slice(&part2);

        for chunk in [1usize, 5, 8, 13, 4096] {
            let inner = PendingChunkReader::new(Cursor::new(stored.clone()), chunk);
            let mut decompress_reader = DecompressReader::new(inner, CompressionAlgorithm::default());
            let mut decompressed = Vec::new();
            decompress_reader.read_to_end(&mut decompressed).await.unwrap();
            assert_eq!(
                decompressed, expected,
                "concatenated part streams must decode byte-exact for chunk={chunk}"
            );
        }
    }

    /// After the first stream error, every further poll must keep failing.
    /// Without the sticky poison a retrying caller would restart at the header
    /// phase and read the truncated tail as a clean EOF — converting a hard
    /// error into a silently short body.
    #[tokio::test]
    async fn test_decompress_reader_error_is_sticky() {
        let data = patterned_payload(32 * 1024, 7);
        let mut compress_reader = CompressReader::with_block_size(Cursor::new(data), 8192, CompressionAlgorithm::default());
        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();
        compressed.truncate(compressed.len() - 3);

        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut out = Vec::new();
        let first = decompress_reader
            .read_to_end(&mut out)
            .await
            .expect_err("truncated payload must error");
        assert_eq!(first.kind(), std::io::ErrorKind::UnexpectedEof);

        let mut retry = Vec::new();
        let second = decompress_reader
            .read_to_end(&mut retry)
            .await
            .expect_err("a poll after the first error must not turn into a clean EOF");
        assert_eq!(second.kind(), std::io::ErrorKind::InvalidData);
        assert!(retry.is_empty(), "no bytes may be produced after the stream failed");
    }

    /// A stream cut off in the middle of a block payload must fail with a clean
    /// UnexpectedEof instead of decoding a short buffer.
    #[tokio::test]
    async fn test_decompress_reader_truncated_payload_is_unexpected_eof() {
        let data = patterned_payload(32 * 1024, 7);
        let mut compress_reader = CompressReader::with_block_size(Cursor::new(data), 8192, CompressionAlgorithm::default());
        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        compressed.truncate(compressed.len() - 3);
        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut out = Vec::new();
        let err = decompress_reader
            .read_to_end(&mut out)
            .await
            .expect_err("truncated payload must error");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    /// A stream cut off in the middle of a block header must fail with a clean
    /// UnexpectedEof instead of parsing a garbage header.
    #[tokio::test]
    async fn test_decompress_reader_truncated_header_is_unexpected_eof() {
        let data = patterned_payload(12 * 1024, 7);
        let mut compress_reader = CompressReader::with_block_size(Cursor::new(data), 8192, CompressionAlgorithm::default());
        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        // Keep the first full block plus 3 bytes of the next header.
        let ln = (compressed[1] as usize) | ((compressed[2] as usize) << 8) | ((compressed[3] as usize) << 16);
        let first_block_end = 8 + ln;
        assert!(compressed.len() > first_block_end, "fixture must contain more than one block");
        compressed.truncate(first_block_end + 3);

        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut out = Vec::new();
        let err = decompress_reader
            .read_to_end(&mut out)
            .await
            .expect_err("truncated header must error");
        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    // Regression: a corrupted block whose 24-bit length field is < 16 must not panic.
    // Header layout (HEADER_LEN = 8): [type, len_lo, len_mid, len_hi, crc0..crc3], then `len`
    // bytes of block body. Pre-fix, poll_read sliced `compressed_buf[0..16]` unconditionally,
    // panicking with "range end index 16 out of range for slice of length N" when N < 16.
    #[tokio::test]
    async fn test_decompress_reader_short_block_no_panic() {
        let len: usize = 3;
        let mut input = vec![
            COMPRESS_TYPE_COMPRESSED,
            (len & 0xFF) as u8,
            ((len >> 8) & 0xFF) as u8,
            ((len >> 16) & 0xFF) as u8,
        ];
        input.extend_from_slice(&[0u8; 4]); // bogus CRC
        // Body: a uvarint claiming uncompressed length = 127, followed by 2 bytes that are not
        // a valid compressed stream — post-fix this must surface as a clean InvalidData error.
        input.extend_from_slice(&[0x7f, 0xAB, 0xCD]);

        let mut decompress_reader = DecompressReader::new(Cursor::new(input), CompressionAlgorithm::default());
        let mut out = Vec::new();
        let res = decompress_reader.read_to_end(&mut out).await;
        assert!(res.is_err(), "corrupted short block must return an error, not panic or succeed");
        assert_eq!(res.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
    }

    // Header-level fail-closed matrix, built by hand so the decoder is exercised against bytes no
    // encoder in this crate can produce. Header layout (HEADER_LEN = 8):
    // [type, len_lo, len_mid, len_hi, crc0..crc3], then `len` body bytes = uvarint(plain_len) + data.
    #[tokio::test]
    async fn test_decompress_reader_header_validation_matrix() {
        // Build a block whose body is `uvarint(plain.len()) + plain` (i.e. the
        // COMPRESS_TYPE_UNCOMPRESSED shape), with the header CRC taken over the plaintext exactly
        // like the production writer does.
        fn build_raw_block(typ: u8, plain: &[u8], len_override: Option<usize>) -> Vec<u8> {
            let crc = {
                let mut hasher = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32IsoHdlc);
                hasher.update(plain);
                hasher.finalize() as u32
            };
            let mut uvarint_buf = [0u8; 10];
            let int_len = put_uvarint(&mut uvarint_buf[..], plain.len() as u64);
            let body_len = int_len + plain.len();
            let len = len_override.unwrap_or(body_len);

            let mut out = Vec::with_capacity(HEADER_LEN + body_len);
            out.push(typ);
            out.push((len & 0xFF) as u8);
            out.push(((len >> 8) & 0xFF) as u8);
            out.push(((len >> 16) & 0xFF) as u8);
            out.extend_from_slice(&crc.to_le_bytes());
            out.extend_from_slice(&uvarint_buf[..int_len]);
            out.extend_from_slice(plain);
            out
        }

        let plain = b"uncompressed passthrough payload";

        // (a) A well-formed uncompressed block decodes to the plaintext verbatim.
        let mut out = Vec::new();
        DecompressReader::new(
            Cursor::new(build_raw_block(COMPRESS_TYPE_UNCOMPRESSED, plain, None)),
            CompressionAlgorithm::default(),
        )
        .read_to_end(&mut out)
        .await
        .expect("a well-formed uncompressed block must decode");
        assert_eq!(out.as_slice(), plain.as_slice());

        // (b) An unknown block type must be rejected instead of being treated as passthrough.
        let mut out = Vec::new();
        let err = DecompressReader::new(Cursor::new(build_raw_block(0x7E, plain, None)), CompressionAlgorithm::default())
            .read_to_end(&mut out)
            .await
            .expect_err("unknown compression type must error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("Unknown compression type"), "got: {err}");

        // (c) A zero-length block would stall the decoder, so it must be rejected up front.
        let mut out = Vec::new();
        let err = DecompressReader::new(
            Cursor::new(build_raw_block(COMPRESS_TYPE_UNCOMPRESSED, plain, Some(0))),
            CompressionAlgorithm::default(),
        )
        .read_to_end(&mut out)
        .await
        .expect_err("zero-length block must error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("Invalid compressed block length"), "got: {err}");

        // (d) A block that decodes to zero plaintext bytes must be rejected: the
        // writer never emits empty blocks, and an empty decode surfacing as
        // Ready(Ok) with no bytes would read as EOF and silently truncate.
        let mut out = Vec::new();
        let err = DecompressReader::new(
            Cursor::new(build_raw_block(COMPRESS_TYPE_UNCOMPRESSED, b"", None)),
            CompressionAlgorithm::default(),
        )
        .read_to_end(&mut out)
        .await
        .expect_err("empty block must error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("Empty compressed block"), "got: {err}");
    }

    // Directly exercises the length-prefix guard: an unterminated varint (all continuation bytes)
    // makes `uvarint` return 0, which must be rejected as an invalid length prefix.
    #[tokio::test]
    async fn test_decompress_reader_unterminated_length_prefix_is_rejected() {
        let len: usize = 3;
        let mut input = vec![
            COMPRESS_TYPE_COMPRESSED,
            (len & 0xFF) as u8,
            ((len >> 8) & 0xFF) as u8,
            ((len >> 16) & 0xFF) as u8,
        ];
        input.extend_from_slice(&[0u8; 4]); // bogus CRC
        input.extend_from_slice(&[0x80, 0x80, 0x80]); // 3 continuation bytes, no terminator

        let mut decompress_reader = DecompressReader::new(Cursor::new(input), CompressionAlgorithm::default());
        let mut out = Vec::new();
        let err = decompress_reader
            .read_to_end(&mut out)
            .await
            .expect_err("unterminated length prefix must error");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidData);
        assert!(err.to_string().contains("length prefix"), "got: {err}");
    }
}
