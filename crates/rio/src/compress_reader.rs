use crate::compress_index::{Index, TryGetIndex};
use crate::{EtagResolvable, HashReaderDetector};
use crate::{HashReaderMut, Reader};
use pin_project_lite::pin_project;
use rustfs_utils::compress::{CompressionAlgorithm, compress_block, decompress_block};
use rustfs_utils::{put_uvarint, put_uvarint_len, uvarint};
use std::io::{self};
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

const COMPRESS_TYPE_COMPRESSED: u8 = 0x00;
const COMPRESS_TYPE_UNCOMPRESSED: u8 = 0x01;
const COMPRESS_TYPE_END: u8 = 0xFF;

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
        temp_pos: usize,
    }
}

impl<R> CompressReader<R>
where
    R: Reader,
{
    pub fn new(inner: R, compression_algorithm: CompressionAlgorithm) -> Self {
        Self {
            inner,
            buffer: Vec::new(),
            pos: 0,
            done: false,
            compression_algorithm,
            block_size: 1 << 20, // Default 1MB
            index: Index::new(),
            written: 0,
            uncomp_written: 0,
            temp_buffer: Vec::with_capacity(1 << 20), // 预分配1MB容量
            temp_pos: 0,
        }
    }

    /// Optional: allow users to customize block_size
    pub fn with_block_size(inner: R, block_size: usize, compression_algorithm: CompressionAlgorithm) -> Self {
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
            temp_pos: 0,
        }
    }
}

impl<R> TryGetIndex for CompressReader<R>
where
    R: Reader,
{
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
        // If buffer has data, serve from buffer first
        if *this.pos < this.buffer.len() {
            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len() - *this.pos);
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

        // 如果临时缓冲区未满，继续读取数据
        while this.temp_buffer.len() < *this.block_size {
            let remaining = *this.block_size - this.temp_buffer.len();
            let mut temp = vec![0u8; remaining];
            let mut temp_buf = ReadBuf::new(&mut temp);

            match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                Poll::Pending => {
                    // 如果临时缓冲区为空，返回 Pending
                    if this.temp_buffer.is_empty() {
                        return Poll::Pending;
                    }
                    // 否则继续处理已读取的数据
                    break;
                }
                Poll::Ready(Ok(())) => {
                    let n = temp_buf.filled().len();
                    if n == 0 {
                        // EOF
                        if this.temp_buffer.is_empty() {
                            // // 如果没有累积的数据，写入结束标记
                            // let mut header = [0u8; 8];
                            // header[0] = 0xFF;
                            // *this.buffer = header.to_vec();
                            // *this.pos = 0;
                            // *this.done = true;
                            // let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
                            // buf.put_slice(&this.buffer[..to_copy]);
                            // *this.pos += to_copy;
                            return Poll::Ready(Ok(()));
                        }
                        // 有累积的数据，处理它
                        break;
                    }
                    this.temp_buffer.extend_from_slice(&temp[..n]);
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
            }
        }

        // 处理累积的数据
        if !this.temp_buffer.is_empty() {
            let uncompressed_data = &this.temp_buffer;
            let crc = crc32fast::hash(uncompressed_data);
            let compressed_data = compress_block(uncompressed_data, *this.compression_algorithm);

            let uncompressed_len = uncompressed_data.len();
            let compressed_len = compressed_data.len();
            let int_len = put_uvarint_len(uncompressed_len as u64);

            let len = compressed_len + int_len;
            let header_len = 8;

            let mut header = [0u8; 8];
            header[0] = COMPRESS_TYPE_COMPRESSED;
            header[1] = (len & 0xFF) as u8;
            header[2] = ((len >> 8) & 0xFF) as u8;
            header[3] = ((len >> 16) & 0xFF) as u8;
            header[4] = (crc & 0xFF) as u8;
            header[5] = ((crc >> 8) & 0xFF) as u8;
            header[6] = ((crc >> 16) & 0xFF) as u8;
            header[7] = ((crc >> 24) & 0xFF) as u8;

            let mut out = Vec::with_capacity(len + header_len);
            out.extend_from_slice(&header);

            let mut uncompressed_len_buf = vec![0u8; int_len];
            put_uvarint(&mut uncompressed_len_buf, uncompressed_len as u64);
            out.extend_from_slice(&uncompressed_len_buf);

            out.extend_from_slice(&compressed_data);

            *this.written += out.len();
            *this.uncomp_written += uncompressed_len;

            this.index.add(*this.written as i64, *this.uncomp_written as i64)?;

            *this.buffer = out;
            *this.pos = 0;
            this.temp_buffer.clear();

            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
            buf.put_slice(&this.buffer[..to_copy]);
            *this.pos += to_copy;
            if *this.pos == this.buffer.len() {
                this.buffer.clear();
                *this.pos = 0;
            }

            // println!("write block, to_copy: {}, pos: {}, buffer_len: {}", to_copy, this.pos, this.buffer.len());
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
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
    /// A reader wrapper that decompresses data on the fly using DEFLATE algorithm.
    // 1~3 bytes store the length of the compressed data
    // The first byte stores the type of the compressed data: 00 = compressed, 01 = uncompressed
    // The first 4 bytes store the CRC32 checksum of the compressed data
    #[derive(Debug)]
    pub struct DecompressReader<R> {
        #[pin]
        pub inner: R,
        buffer: Vec<u8>,
        buffer_pos: usize,
        finished: bool,
        // New fields for saving header read progress across polls
        header_buf: [u8; 8],
        header_read: usize,
        header_done: bool,
        // New fields for saving compressed block read progress across polls
        compressed_buf: Option<Vec<u8>>,
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
            header_buf: [0u8; 8],
            header_read: 0,
            header_done: false,
            compressed_buf: None,
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

        // Serve from buffer if any
        if *this.buffer_pos < this.buffer.len() {
            let to_copy = std::cmp::min(buf.remaining(), this.buffer.len() - *this.buffer_pos);
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

        // Read header, support saving progress across polls
        while !*this.header_done && *this.header_read < 8 {
            let mut temp = [0u8; 8];
            let mut temp_buf = ReadBuf::new(&mut temp[0..8 - *this.header_read]);
            match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {
                    let n = temp_buf.filled().len();
                    if n == 0 {
                        break;
                    }
                    this.header_buf[*this.header_read..*this.header_read + n].copy_from_slice(&temp_buf.filled()[..n]);
                    *this.header_read += n;
                }
                Poll::Ready(Err(e)) => {
                    return Poll::Ready(Err(e));
                }
            }
            if *this.header_read < 8 {
                // Header not fully read, return Pending or Ok, wait for next poll
                return Poll::Pending;
            }
        }

        if !*this.header_done && *this.header_read == 0 {
            return Poll::Ready(Ok(()));
        }

        let typ = this.header_buf[0];
        let len = (this.header_buf[1] as usize) | ((this.header_buf[2] as usize) << 8) | ((this.header_buf[3] as usize) << 16);
        let crc = (this.header_buf[4] as u32)
            | ((this.header_buf[5] as u32) << 8)
            | ((this.header_buf[6] as u32) << 16)
            | ((this.header_buf[7] as u32) << 24);

        // Header is used up, reset header_read
        *this.header_read = 0;
        *this.header_done = true;

        // Save compressed block read progress across polls
        if this.compressed_buf.is_none() {
            *this.compressed_len = len;
            *this.compressed_buf = Some(vec![0u8; *this.compressed_len]);
            *this.compressed_read = 0;
        }
        let compressed_buf = this.compressed_buf.as_mut().unwrap();
        while *this.compressed_read < *this.compressed_len {
            let mut temp_buf = ReadBuf::new(&mut compressed_buf[*this.compressed_read..]);
            match this.inner.as_mut().poll_read(cx, &mut temp_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Ok(())) => {
                    let n = temp_buf.filled().len();
                    if n == 0 {
                        break;
                    }
                    *this.compressed_read += n;
                }
                Poll::Ready(Err(e)) => {
                    this.compressed_buf.take();
                    *this.compressed_read = 0;
                    *this.compressed_len = 0;
                    return Poll::Ready(Err(e));
                }
            }
        }

        // After reading all, unpack
        let (uncompress_len, uvarint) = uvarint(&compressed_buf[0..16]);
        let compressed_data = &compressed_buf[uvarint as usize..];
        let decompressed = if typ == COMPRESS_TYPE_COMPRESSED {
            match decompress_block(compressed_data, *this.compression_algorithm) {
                Ok(out) => out,
                Err(e) => {
                    this.compressed_buf.take();
                    *this.compressed_read = 0;
                    *this.compressed_len = 0;
                    return Poll::Ready(Err(e));
                }
            }
        } else if typ == COMPRESS_TYPE_UNCOMPRESSED {
            compressed_data.to_vec()
        } else if typ == COMPRESS_TYPE_END {
            // Handle end marker
            this.compressed_buf.take();
            *this.compressed_read = 0;
            *this.compressed_len = 0;
            *this.finished = true;
            return Poll::Ready(Ok(()));
        } else {
            this.compressed_buf.take();
            *this.compressed_read = 0;
            *this.compressed_len = 0;
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "Unknown compression type")));
        };
        if decompressed.len() != uncompress_len as usize {
            this.compressed_buf.take();
            *this.compressed_read = 0;
            *this.compressed_len = 0;
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "Decompressed length mismatch")));
        }

        let actual_crc = crc32fast::hash(&decompressed);
        if actual_crc != crc {
            this.compressed_buf.take();
            *this.compressed_read = 0;
            *this.compressed_len = 0;
            return Poll::Ready(Err(io::Error::new(io::ErrorKind::InvalidData, "CRC32 mismatch")));
        }
        *this.buffer = decompressed;
        *this.buffer_pos = 0;
        // Clear compressed block state for next block
        this.compressed_buf.take();
        *this.compressed_read = 0;
        *this.compressed_len = 0;
        *this.header_done = false;
        let to_copy = std::cmp::min(buf.remaining(), this.buffer.len());
        buf.put_slice(&this.buffer[..to_copy]);
        *this.buffer_pos += to_copy;

        if *this.buffer_pos == this.buffer.len() {
            this.buffer.clear();
            *this.buffer_pos = 0;
        }

        Poll::Ready(Ok(()))
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

#[cfg(test)]
mod tests {
    use crate::WarpReader;

    use super::*;
    use std::io::Cursor;
    use tokio::io::{AsyncReadExt, BufReader};

    #[tokio::test]
    async fn test_compress_reader_basic() {
        let data = b"hello world, hello world, hello world!";
        let reader = Cursor::new(&data[..]);
        let mut compress_reader = CompressReader::new(WarpReader::new(reader), CompressionAlgorithm::Gzip);

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        // DecompressReader解包
        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::Gzip);
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, data);
    }

    #[tokio::test]
    async fn test_compress_reader_basic_deflate() {
        let data = b"hello world, hello world, hello world!";
        let reader = BufReader::new(&data[..]);
        let mut compress_reader = CompressReader::new(WarpReader::new(reader), CompressionAlgorithm::Deflate);

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        // DecompressReader解包
        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::Deflate);
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, data);
    }

    #[tokio::test]
    async fn test_compress_reader_empty() {
        let data = b"";
        let reader = BufReader::new(&data[..]);
        let mut compress_reader = CompressReader::new(WarpReader::new(reader), CompressionAlgorithm::Gzip);

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::Gzip);
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, data);
    }

    #[tokio::test]
    async fn test_compress_reader_large() {
        use rand::Rng;
        // Generate 1MB of random bytes
        let mut data = vec![0u8; 1024 * 1024];
        rand::rng().fill(&mut data[..]);
        let reader = Cursor::new(data.clone());
        let mut compress_reader = CompressReader::new(WarpReader::new(reader), CompressionAlgorithm::Gzip);

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::Gzip);
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, &data);
    }

    #[tokio::test]
    async fn test_compress_reader_large_deflate() {
        use rand::Rng;
        // Generate 1MB of random bytes
        let mut data = vec![0u8; 1024 * 1024 * 3 + 512];
        rand::rng().fill(&mut data[..]);
        let reader = Cursor::new(data.clone());
        let mut compress_reader = CompressReader::new(WarpReader::new(reader), CompressionAlgorithm::default());

        let mut compressed = Vec::new();
        compress_reader.read_to_end(&mut compressed).await.unwrap();

        let mut decompress_reader = DecompressReader::new(Cursor::new(compressed.clone()), CompressionAlgorithm::default());
        let mut decompressed = Vec::new();
        decompress_reader.read_to_end(&mut decompressed).await.unwrap();

        assert_eq!(&decompressed, &data);
    }
}
