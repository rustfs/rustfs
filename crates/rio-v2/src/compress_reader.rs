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

use minlz::{crc::crc, decode, encode};
use pin_project_lite::pin_project;
use rand::RngExt;
use rustfs_rio::{EtagResolvable, HashReaderDetector, HashReaderMut, Index, TryGetIndex};
use rustfs_utils::CompressionAlgorithm;
use std::cmp::min;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

const MAGIC_CHUNK: &[u8] = b"\xff\x06\x00\x00S2sTwO";
const MAGIC_CHUNK_SNAPPY: &[u8] = b"\xff\x06\x00\x00sNaPpY";
const CHUNK_TYPE_COMPRESSED_DATA: u8 = 0x00;
const CHUNK_TYPE_UNCOMPRESSED_DATA: u8 = 0x01;
const CHUNK_TYPE_INDEX: u8 = 0x99;
const CHUNK_TYPE_PADDING: u8 = 0xfe;
const CHUNK_TYPE_STREAM_IDENTIFIER: u8 = 0xff;
const DEFAULT_BLOCK_SIZE: usize = 1 << 20;
const MAX_CHUNK_SIZE: usize = (1 << 24) - 1;
const CHECKSUM_SIZE: usize = 4;
const CHUNK_HEADER_LEN: usize = 4;
const ENCRYPTED_PADDING_MULTIPLE: usize = 256;
const MIN_INDEX_SIZE: usize = 8 << 20;

pin_project! {
    #[derive(Debug)]
    pub struct CompressReader<R> {
        #[pin]
        inner: R,
        buffer: Vec<u8>,
        pos: usize,
        done: bool,
        block_size: usize,
        index: Index,
        written: usize,
        uncompressed_written: usize,
        temp_buffer: Vec<u8>,
        read_buffer: Vec<u8>,
        wrote_stream_header: bool,
        padding_multiple: Option<usize>,
    }
}

impl<R> CompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub fn new(inner: R, _compression_algorithm: CompressionAlgorithm) -> Self {
        Self::with_block_size(inner, DEFAULT_BLOCK_SIZE, CompressionAlgorithm::default())
    }

    pub fn with_block_size(inner: R, block_size: usize, _compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
            pos: 0,
            done: false,
            block_size,
            index: Index::new(),
            written: 0,
            uncompressed_written: 0,
            temp_buffer: Vec::with_capacity(block_size),
            read_buffer: vec![0u8; block_size],
            wrote_stream_header: false,
            padding_multiple: None,
        }
    }

    pub fn with_encrypted_padding(inner: R, _compression_algorithm: CompressionAlgorithm) -> Self {
        let mut reader = Self::new(inner, CompressionAlgorithm::default());
        reader.padding_multiple = Some(ENCRYPTED_PADDING_MULTIPLE);
        reader
    }
}

impl<R> TryGetIndex for CompressReader<R> {
    fn try_get_index(&self) -> Option<&Index> {
        (self.uncompressed_written > MIN_INDEX_SIZE).then_some(&self.index)
    }
}

impl<R> AsyncRead for CompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();

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

        while this.temp_buffer.len() < *this.block_size {
            let remaining = *this.block_size - this.temp_buffer.len();
            let mut read_buf = ReadBuf::new(&mut this.read_buffer[..remaining]);
            match this.inner.as_mut().poll_read(cx, &mut read_buf) {
                Poll::Pending => {
                    if this.temp_buffer.is_empty() {
                        return Poll::Pending;
                    }
                    break;
                }
                Poll::Ready(Ok(())) => {
                    let n = read_buf.filled().len();
                    if n == 0 {
                        break;
                    }
                    this.temp_buffer.extend_from_slice(read_buf.filled());
                }
                Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
            }
        }

        if this.temp_buffer.is_empty() {
            if let Some(padding_multiple) = *this.padding_multiple {
                if let Some(padding_chunk) = build_padding_chunk(*this.written, padding_multiple)? {
                    *this.written += padding_chunk.len();
                    this.index.total_compressed = *this.written as i64;
                    *this.buffer = padding_chunk;
                    *this.pos = 0;
                    *this.done = true;

                    let to_copy = min(buf.remaining(), this.buffer.len());
                    buf.put_slice(&this.buffer[..to_copy]);
                    *this.pos += to_copy;
                    if *this.pos == this.buffer.len() {
                        this.buffer.clear();
                        *this.pos = 0;
                    }
                    return Poll::Ready(Ok(()));
                }
            }

            *this.done = true;
            return Poll::Ready(Ok(()));
        }

        let mut out = Vec::new();
        if !*this.wrote_stream_header {
            out.extend_from_slice(MAGIC_CHUNK);
            *this.written += MAGIC_CHUNK.len();
            *this.wrote_stream_header = true;
        }

        if let Err(err) = this.index.add(*this.written as i64, *this.uncompressed_written as i64) {
            return Poll::Ready(Err(err));
        }

        let block = build_s2_chunk(this.temp_buffer.as_slice())?;
        *this.uncompressed_written += this.temp_buffer.len();
        *this.written += block.len();
        this.index.total_uncompressed = *this.uncompressed_written as i64;
        this.index.total_compressed = *this.written as i64;

        out.extend_from_slice(&block);
        this.temp_buffer.clear();
        *this.buffer = out;
        *this.pos = 0;

        let to_copy = min(buf.remaining(), this.buffer.len());
        buf.put_slice(&this.buffer[..to_copy]);
        *this.pos += to_copy;
        if *this.pos == this.buffer.len() {
            this.buffer.clear();
            *this.pos = 0;
        }

        Poll::Ready(Ok(()))
    }
}

impl<R> EtagResolvable for CompressReader<R>
where
    R: EtagResolvable,
{
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.inner.try_resolve_etag()
    }
}

impl<R> HashReaderDetector for CompressReader<R>
where
    R: HashReaderDetector,
{
    fn is_hash_reader(&self) -> bool {
        self.inner.is_hash_reader()
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        self.inner.as_hash_reader_mut()
    }
}

pin_project! {
    #[derive(Debug)]
    pub struct DecompressReader<R> {
        #[pin]
        inner: R,
        buffer: Vec<u8>,
        buffer_pos: usize,
        finished: bool,
        header_buf: [u8; CHUNK_HEADER_LEN],
        header_read: usize,
        chunk_type: u8,
        chunk_buf: Vec<u8>,
        chunk_len: usize,
        chunk_read: usize,
        stream_initialized: bool,
    }
}

impl<R> DecompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    pub fn new(inner: R, _compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
            buffer_pos: 0,
            finished: false,
            header_buf: [0u8; CHUNK_HEADER_LEN],
            header_read: 0,
            chunk_type: 0,
            chunk_buf: Vec::new(),
            chunk_len: 0,
            chunk_read: 0,
            stream_initialized: false,
        }
    }
}

impl<R> AsyncRead for DecompressReader<R>
where
    R: AsyncRead + Unpin + Send + Sync,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut this = self.project();

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

        loop {
            if *this.finished {
                return Poll::Ready(Ok(()));
            }

            while *this.header_read < CHUNK_HEADER_LEN {
                let mut read_buf = ReadBuf::new(&mut this.header_buf[*this.header_read..]);
                match this.inner.as_mut().poll_read(cx, &mut read_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        let n = read_buf.filled().len();
                        if n == 0 {
                            if *this.header_read == 0 {
                                *this.finished = true;
                                return Poll::Ready(Ok(()));
                            }
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "unexpected EOF while reading S2 chunk header",
                            )));
                        }
                        *this.header_read += n;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                }
            }

            *this.chunk_type = this.header_buf[0];
            *this.chunk_len =
                (this.header_buf[1] as usize) | ((this.header_buf[2] as usize) << 8) | ((this.header_buf[3] as usize) << 16);
            *this.header_read = 0;

            if this.chunk_buf.len() < *this.chunk_len {
                this.chunk_buf.resize(*this.chunk_len, 0);
            }
            *this.chunk_read = 0;

            while *this.chunk_read < *this.chunk_len {
                let mut read_buf = ReadBuf::new(&mut this.chunk_buf[*this.chunk_read..*this.chunk_len]);
                match this.inner.as_mut().poll_read(cx, &mut read_buf) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(())) => {
                        let n = read_buf.filled().len();
                        if n == 0 {
                            return Poll::Ready(Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "unexpected EOF while reading S2 chunk body",
                            )));
                        }
                        *this.chunk_read += n;
                    }
                    Poll::Ready(Err(err)) => return Poll::Ready(Err(err)),
                }
            }

            let chunk = &this.chunk_buf[..*this.chunk_len];
            match *this.chunk_type {
                CHUNK_TYPE_STREAM_IDENTIFIER => {
                    if chunk != &MAGIC_CHUNK[CHUNK_HEADER_LEN..] && chunk != &MAGIC_CHUNK_SNAPPY[CHUNK_HEADER_LEN..] {
                        return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "invalid S2 stream identifier")));
                    }
                    *this.stream_initialized = true;
                    continue;
                }
                CHUNK_TYPE_COMPRESSED_DATA => {
                    *this.stream_initialized = true;
                    let decompressed = decode_chunk(chunk, true)?;
                    *this.buffer = decompressed;
                }
                CHUNK_TYPE_UNCOMPRESSED_DATA => {
                    *this.stream_initialized = true;
                    let decompressed = decode_chunk(chunk, false)?;
                    *this.buffer = decompressed;
                }
                CHUNK_TYPE_INDEX | CHUNK_TYPE_PADDING | 0x80..=0xfd => {
                    *this.stream_initialized = true;
                    continue;
                }
                _ => {
                    if !*this.stream_initialized && *this.chunk_type != CHUNK_TYPE_COMPRESSED_DATA {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("unknown S2 chunk type: 0x{:02x}", *this.chunk_type),
                        )));
                    }
                    return Poll::Ready(Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown S2 chunk type: 0x{:02x}", *this.chunk_type),
                    )));
                }
            }

            *this.buffer_pos = 0;
            let to_copy = min(buf.remaining(), this.buffer.len());
            buf.put_slice(&this.buffer[..to_copy]);
            *this.buffer_pos += to_copy;
            if *this.buffer_pos == this.buffer.len() {
                this.buffer.clear();
                *this.buffer_pos = 0;
            }
            return Poll::Ready(Ok(()));
        }
    }
}

impl<R> EtagResolvable for DecompressReader<R>
where
    R: EtagResolvable,
{
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.inner.try_resolve_etag()
    }
}

impl<R> HashReaderDetector for DecompressReader<R>
where
    R: HashReaderDetector,
{
    fn is_hash_reader(&self) -> bool {
        self.inner.is_hash_reader()
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        self.inner.as_hash_reader_mut()
    }
}

fn build_s2_chunk(uncompressed: &[u8]) -> io::Result<Vec<u8>> {
    let compressed = encode(uncompressed);
    let checksum = crc(uncompressed);
    let dst_limit = uncompressed.len().saturating_sub(uncompressed.len() / 32).saturating_sub(5);
    let (chunk_type, payload) = if compressed.len() <= dst_limit {
        (CHUNK_TYPE_COMPRESSED_DATA, compressed)
    } else {
        (CHUNK_TYPE_UNCOMPRESSED_DATA, uncompressed.to_vec())
    };

    let chunk_len = payload.len() + CHECKSUM_SIZE;
    if chunk_len > MAX_CHUNK_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "S2 chunk exceeds 24-bit framing limit"));
    }

    let mut out = Vec::with_capacity(CHUNK_HEADER_LEN + chunk_len);
    out.push(chunk_type);
    out.push((chunk_len & 0xff) as u8);
    out.push(((chunk_len >> 8) & 0xff) as u8);
    out.push(((chunk_len >> 16) & 0xff) as u8);
    out.extend_from_slice(&checksum.to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn build_padding_chunk(current_size: usize, padding_multiple: usize) -> io::Result<Option<Vec<u8>>> {
    if padding_multiple == 0 || current_size % padding_multiple == 0 {
        return Ok(None);
    }

    let padding_len = (padding_multiple - ((current_size + CHUNK_HEADER_LEN) % padding_multiple)) % padding_multiple;
    if padding_len > MAX_CHUNK_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "S2 padding exceeds 24-bit framing limit"));
    }

    let mut out = Vec::with_capacity(CHUNK_HEADER_LEN + padding_len);
    out.push(CHUNK_TYPE_PADDING);
    out.push((padding_len & 0xff) as u8);
    out.push(((padding_len >> 8) & 0xff) as u8);
    out.push(((padding_len >> 16) & 0xff) as u8);

    if padding_len > 0 {
        let mut padding = vec![0u8; padding_len];
        rand::rng().fill(padding.as_mut_slice());
        out.extend_from_slice(&padding);
    }

    Ok(Some(out))
}

fn decode_chunk(chunk: &[u8], compressed: bool) -> io::Result<Vec<u8>> {
    if chunk.len() < CHECKSUM_SIZE {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "S2 chunk smaller than checksum header"));
    }

    let expected_crc = u32::from_le_bytes(chunk[..CHECKSUM_SIZE].try_into().expect("checksum header"));
    let payload = &chunk[CHECKSUM_SIZE..];
    let decompressed = if compressed {
        decode(payload).map_err(|err| io::Error::new(io::ErrorKind::InvalidData, format!("S2 decode error: {err}")))?
    } else {
        payload.to_vec()
    };

    let actual_crc = crc(&decompressed);
    if actual_crc != expected_crc {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "S2 CRC mismatch"));
    }

    Ok(decompressed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    fn s2_chunk_types(stream: &[u8]) -> Vec<u8> {
        let mut chunk_types = Vec::new();
        let mut offset = 0usize;
        while offset + CHUNK_HEADER_LEN <= stream.len() {
            let chunk_type = stream[offset];
            let chunk_len =
                (stream[offset + 1] as usize) | ((stream[offset + 2] as usize) << 8) | ((stream[offset + 3] as usize) << 16);
            chunk_types.push(chunk_type);
            offset += CHUNK_HEADER_LEN + chunk_len;
        }
        chunk_types
    }

    #[tokio::test]
    async fn s2_compress_reader_roundtrip() {
        let plaintext = b"hello-rio-v2-s2-".repeat(32_768);
        let mut reader = CompressReader::new(Cursor::new(plaintext.clone()), CompressionAlgorithm::default());
        let mut compressed = Vec::new();
        reader.read_to_end(&mut compressed).await.expect("read compressed data");

        assert!(compressed.starts_with(MAGIC_CHUNK));

        let mut decompressor = DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut actual = Vec::new();
        decompressor.read_to_end(&mut actual).await.expect("read decompressed data");

        assert_eq!(actual, plaintext);
    }

    #[tokio::test]
    async fn s2_decompress_reader_returns_bytes_on_first_read() {
        let plaintext = b"abcdefghijklmnopqrstuvwxyz".to_vec();
        let mut compressed = Vec::new();
        CompressReader::new(Cursor::new(plaintext.clone()), CompressionAlgorithm::default())
            .read_to_end(&mut compressed)
            .await
            .expect("compress plaintext");

        let mut decompressor = DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut buf = [0u8; 64];
        let n = decompressor.read(&mut buf).await.expect("read first decompressed chunk");

        assert!(n > 0);
        assert_eq!(&buf[..n], plaintext.as_slice());
    }

    #[tokio::test]
    async fn s2_compress_reader_with_encrypted_padding_emits_padding_frame() {
        let plaintext = b"encrypted-padding-check-".repeat(8192);
        let mut reader = CompressReader::with_encrypted_padding(Cursor::new(plaintext.clone()), CompressionAlgorithm::default());
        let mut compressed = Vec::new();
        reader.read_to_end(&mut compressed).await.expect("read compressed data");

        assert_eq!(compressed.len() % ENCRYPTED_PADDING_MULTIPLE, 0);
        assert!(s2_chunk_types(&compressed).contains(&CHUNK_TYPE_PADDING));

        let mut decompressor = DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut actual = Vec::new();
        decompressor.read_to_end(&mut actual).await.expect("read decompressed data");

        assert_eq!(actual, plaintext);
    }

    #[tokio::test]
    async fn s2_compress_reader_skips_index_for_small_streams() {
        let plaintext = b"index-threshold-check-".repeat(16_384);
        let mut reader = CompressReader::new(Cursor::new(plaintext.clone()), CompressionAlgorithm::default());
        let mut compressed = Vec::new();
        reader.read_to_end(&mut compressed).await.expect("read compressed data");

        assert!(reader.try_get_index().is_none());

        let mut decompressor = DecompressReader::new(Cursor::new(compressed), CompressionAlgorithm::default());
        let mut actual = Vec::new();
        decompressor.read_to_end(&mut actual).await.expect("read decompressed data");

        assert_eq!(actual, plaintext);
    }
}
