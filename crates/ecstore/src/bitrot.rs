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

use crate::disk::{self, DiskAPI as _, DiskStore, error::DiskError};
use crate::erasure_coding::{BitrotReader, BitrotWriterWrapper, CustomWriter};
use crate::store_api::{GetObjectChunkCopyMode, GetObjectChunkPath, GetObjectChunkResult};
use bytes::{Bytes, BytesMut};
use futures_util::{StreamExt, stream};
use rustfs_io_core::IoChunk;
use rustfs_utils::HashAlgorithm;
use std::io::Cursor;
use std::time::Instant;
use tokio::io::AsyncRead;
use tracing::debug;

fn classify_chunk_copy_mode(source_direct: bool, copied: bool) -> GetObjectChunkCopyMode {
    if copied {
        GetObjectChunkCopyMode::SingleCopy
    } else if source_direct {
        GetObjectChunkCopyMode::TrueZeroCopy
    } else {
        GetObjectChunkCopyMode::SharedBytes
    }
}

struct ChunkSpan {
    bytes: Bytes,
    chunk: IoChunk,
    copied: bool,
}

struct ChunkCursor<'a> {
    chunks: &'a [IoChunk],
    chunk_index: usize,
    chunk_offset: usize,
    consumed: usize,
    total_len: usize,
}

impl<'a> ChunkCursor<'a> {
    fn new(chunks: &'a [IoChunk]) -> Self {
        Self {
            chunks,
            chunk_index: 0,
            chunk_offset: 0,
            consumed: 0,
            total_len: chunks.iter().map(IoChunk::len).sum(),
        }
    }

    fn remaining(&self) -> usize {
        self.total_len.saturating_sub(self.consumed)
    }

    fn skip_empty_chunks(&mut self) {
        while let Some(chunk) = self.chunks.get(self.chunk_index) {
            if self.chunk_offset < chunk.len() {
                break;
            }
            self.chunk_index += 1;
            self.chunk_offset = 0;
        }
    }

    fn advance(&mut self, len: usize) {
        self.consumed += len;
        self.chunk_offset += len;
        self.skip_empty_chunks();
    }

    fn take_span(&mut self, len: usize) -> std::io::Result<ChunkSpan> {
        self.skip_empty_chunks();
        if self.remaining() < len {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "truncated bitrot chunk source"));
        }

        let Some(chunk) = self.chunks.get(self.chunk_index) else {
            return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "missing bitrot chunk source"));
        };
        let available = chunk.len().saturating_sub(self.chunk_offset);

        if len <= available {
            let span = match chunk {
                IoChunk::Shared(bytes) => {
                    let bytes = bytes.slice(self.chunk_offset..self.chunk_offset + len);
                    ChunkSpan {
                        bytes: bytes.clone(),
                        chunk: IoChunk::Shared(bytes),
                        copied: false,
                    }
                }
                IoChunk::Mapped(mapped) => {
                    let chunk = IoChunk::Mapped(mapped.slice(self.chunk_offset, len)?);
                    let bytes = chunk.as_bytes();
                    ChunkSpan {
                        bytes,
                        chunk,
                        copied: false,
                    }
                }
                IoChunk::Pooled(pooled) => {
                    let bytes = pooled.as_bytes().slice(self.chunk_offset..self.chunk_offset + len);
                    ChunkSpan {
                        bytes: bytes.clone(),
                        chunk: IoChunk::Shared(bytes),
                        copied: true,
                    }
                }
            };
            self.advance(len);
            return Ok(span);
        }

        let mut aggregate = BytesMut::with_capacity(len);
        let mut remaining = len;
        while remaining > 0 {
            self.skip_empty_chunks();
            let Some(chunk) = self.chunks.get(self.chunk_index) else {
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "truncated bitrot chunk source"));
            };
            let available = chunk.len().saturating_sub(self.chunk_offset);
            let take = available.min(remaining);
            aggregate.extend_from_slice(&chunk.as_bytes()[self.chunk_offset..self.chunk_offset + take]);
            self.advance(take);
            remaining -= take;
        }

        let bytes = aggregate.freeze();
        Ok(ChunkSpan {
            bytes: bytes.clone(),
            chunk: IoChunk::Shared(bytes),
            copied: true,
        })
    }
}

/// Create a BitrotReader from either inline data or disk file stream
///
/// # Parameters
/// * `inline_data` - Optional inline data, if present, will use Cursor to read from memory
/// * `disk` - Optional disk reference for file stream reading
/// * `bucket` - Bucket name for file path
/// * `path` - File path within the bucket
/// * `offset` - Starting offset for reading
/// * `length` - Length to read
/// * `shard_size` - Shard size for erasure coding
/// * `checksum_algo` - Hash algorithm for bitrot verification
/// * `skip_verify` - If true, skip checksum verification
/// * `use_zero_copy` - If true, use zero-copy read (mmap on Unix)
#[allow(clippy::too_many_arguments)]
pub async fn create_bitrot_reader(
    inline_data: Option<&[u8]>,
    disk: Option<&DiskStore>,
    bucket: &str,
    path: &str,
    offset: usize,
    length: usize,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify: bool,
    use_zero_copy: bool,
) -> disk::error::Result<Option<BitrotReader<Box<dyn AsyncRead + Send + Sync + Unpin>>>> {
    // Calculate the total length to read, including the checksum overhead
    let length = length.div_ceil(shard_size) * checksum_algo.size() + length;
    let offset = offset.div_ceil(shard_size) * checksum_algo.size() + offset;
    if let Some(data) = inline_data {
        // Use inline data
        let mut rd = Cursor::new(Bytes::copy_from_slice(data));
        // Apply the computed offset so inline data matches disk read behavior
        rd.set_position(offset as u64);
        let reader = BitrotReader::new(
            Box::new(rd) as Box<dyn AsyncRead + Send + Sync + Unpin>,
            shard_size,
            checksum_algo,
            skip_verify,
        );
        Ok(Some(reader))
    } else if let Some(disk) = disk {
        // Read from disk
        if use_zero_copy {
            // Try zero-copy read first (uses mmap on Unix)
            let start = Instant::now();
            match disk.read_file_zero_copy(bucket, path, offset, length).await {
                Ok(bytes) => {
                    let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

                    // Record zero-copy metrics
                    rustfs_io_metrics::record_zero_copy_read(bytes.len(), duration_ms);

                    // Log successful zero-copy read
                    debug!(
                        size = bytes.len(),
                        path = %path,
                        "zero_copy_read_success"
                    );

                    // Wrap Bytes in Cursor for AsyncRead
                    // The Bytes is reference-counted, so this is zero-copy
                    let rd = Cursor::new(bytes);
                    let reader = BitrotReader::new(
                        Box::new(rd) as Box<dyn AsyncRead + Send + Sync + Unpin>,
                        shard_size,
                        checksum_algo,
                        skip_verify,
                    );
                    Ok(Some(reader))
                }
                Err(e) => {
                    // Record zero-copy fallback
                    rustfs_io_metrics::record_zero_copy_fallback(&format!("{:?}", e));

                    // Log zero-copy fallback
                    debug!(
                        reason = %format!("{:?}", e),
                        path = %path,
                        "zero_copy_fallback"
                    );

                    // Fall back to regular stream read on error
                    match disk.read_file_stream(bucket, path, offset, length).await {
                        Ok(rd) => {
                            let reader = BitrotReader::new(rd, shard_size, checksum_algo, skip_verify);
                            Ok(Some(reader))
                        }
                        Err(_e2) => {
                            // Return the original error from zero-copy attempt
                            Err(e)
                        }
                    }
                }
            }
        } else {
            // Use regular stream read
            match disk.read_file_stream(bucket, path, offset, length).await {
                Ok(rd) => {
                    let reader = BitrotReader::new(rd, shard_size, checksum_algo, skip_verify);
                    Ok(Some(reader))
                }
                Err(e) => Err(e),
            }
        }
    } else {
        // Neither inline data nor disk available
        Ok(None)
    }
}

/// Create a chunk stream from bitrot-encoded data, preserving source chunk provenance when possible.
#[allow(clippy::too_many_arguments)]
pub async fn create_bitrot_chunk_stream(
    inline_data: Option<&[u8]>,
    disk: Option<&DiskStore>,
    bucket: &str,
    path: &str,
    offset: usize,
    length: usize,
    total_data_size: usize,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify: bool,
    use_zero_copy: bool,
) -> disk::error::Result<Option<GetObjectChunkResult>> {
    let fetch_start = (offset / shard_size) * shard_size;
    let fetch_end = (offset + length).div_ceil(shard_size) * shard_size;
    let fetch_end = fetch_end.min(total_data_size);
    let fetch_length = fetch_end.saturating_sub(fetch_start);
    let trim_prefix = offset.saturating_sub(fetch_start);
    let hash_size = checksum_algo.size();
    let encoded_length = fetch_length.div_ceil(shard_size) * hash_size + fetch_length;
    let encoded_offset = fetch_start.div_ceil(shard_size) * hash_size + fetch_start;

    let (source_chunks, source_direct) = if let Some(data) = inline_data {
        (
            vec![IoChunk::Shared(
                Bytes::copy_from_slice(data).slice(encoded_offset..encoded_offset + encoded_length),
            )],
            false,
        )
    } else if let Some(disk) = disk {
        if use_zero_copy {
            let mut stream = disk.read_file_chunks(bucket, path, encoded_offset, encoded_length).await?;
            let mut chunks = Vec::new();
            let mut direct = true;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                direct &= matches!(chunk, IoChunk::Mapped(_));
                chunks.push(chunk);
            }
            (chunks, direct)
        } else {
            let bytes = disk.read_file_zero_copy(bucket, path, encoded_offset, encoded_length).await?;
            (vec![IoChunk::Shared(bytes)], false)
        }
    } else {
        return Ok(None);
    };

    let (data_chunks, copied) =
        decode_bitrot_chunk_source(&source_chunks, shard_size, checksum_algo, skip_verify).map_err(DiskError::other)?;
    let data_chunks = trim_chunk_vec(data_chunks, trim_prefix, length).map_err(DiskError::other)?;
    let stream = stream::iter(data_chunks.into_iter().map(Ok::<IoChunk, std::io::Error>));
    Ok(Some(GetObjectChunkResult {
        stream: Box::pin(stream),
        path: GetObjectChunkPath::Direct,
        copy_mode: classify_chunk_copy_mode(source_direct, copied),
    }))
}

fn trim_chunk_vec(chunks: Vec<IoChunk>, offset: usize, length: usize) -> std::io::Result<Vec<IoChunk>> {
    let mut skip = offset;
    let mut remaining = length;
    let mut result = Vec::new();

    for chunk in chunks {
        if remaining == 0 {
            break;
        }

        let chunk_len = chunk.len();
        if skip >= chunk_len {
            skip -= chunk_len;
            continue;
        }

        let start = skip;
        let take = (chunk_len - start).min(remaining);
        result.push(chunk.slice(start, take)?);
        remaining -= take;
        skip = 0;
    }

    Ok(result)
}

fn decode_bitrot_chunk_source(
    source_chunks: &[IoChunk],
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify: bool,
) -> std::io::Result<(Vec<IoChunk>, bool)> {
    let hash_size = checksum_algo.size();
    let mut cursor = ChunkCursor::new(source_chunks);
    let mut result = Vec::new();
    let mut copied = false;

    while cursor.remaining() > 0 {
        let expected_hash = if hash_size > 0 {
            Some(cursor.take_span(hash_size)?)
        } else {
            None
        };

        let data_len = shard_size.min(cursor.remaining());
        if data_len == 0 {
            break;
        }

        let data_span = cursor.take_span(data_len)?;
        copied |= data_span.copied;
        if let Some(expected_hash) = expected_hash {
            copied |= expected_hash.copied;
            if !skip_verify && checksum_algo.hash_encode(data_span.bytes.as_ref()).as_ref() != expected_hash.bytes.as_ref() {
                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bitrot hash mismatch"));
            }
        }

        result.push(data_span.chunk);
    }

    Ok((result, copied))
}

#[doc(hidden)]
pub fn decode_bitrot_chunk_source_for_bench(
    source_chunks: &[IoChunk],
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify: bool,
) -> std::io::Result<(Vec<IoChunk>, bool)> {
    decode_bitrot_chunk_source(source_chunks, shard_size, checksum_algo, skip_verify)
}

/// Create a new BitrotWriterWrapper based on the provided parameters
///
/// # Parameters
/// - `is_inline_buffer`: If true, creates an in-memory buffer writer; if false, uses disk storage
/// - `disk`: Optional disk instance for file creation (used when is_inline_buffer is false)
/// - `shard_size`: Size of each shard for bitrot calculation
/// - `checksum_algo`: Hash algorithm to use for bitrot verification
/// - `volume`: Volume/bucket name for disk storage
/// - `path`: File path for disk storage
/// - `length`: Expected file length for disk storage
///
/// # Returns
/// A Result containing the BitrotWriterWrapper or an error
pub async fn create_bitrot_writer(
    is_inline_buffer: bool,
    disk: Option<&DiskStore>,
    volume: &str,
    path: &str,
    length: i64,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
) -> disk::error::Result<BitrotWriterWrapper> {
    let writer = if is_inline_buffer {
        CustomWriter::new_inline_buffer()
    } else if let Some(disk) = disk {
        let length = if length > 0 {
            let length = length as usize;
            (length.div_ceil(shard_size) * checksum_algo.size() + length) as i64
        } else {
            0
        };

        let file = disk.create_file("", volume, path, length).await?;
        CustomWriter::new_tokio_writer(file)
    } else {
        return Err(DiskError::DiskNotFound);
    };

    Ok(BitrotWriterWrapper::new(writer, shard_size, checksum_algo))
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::StreamExt;

    #[tokio::test]
    async fn test_create_bitrot_reader_with_inline_data() {
        let test_data = b"hello world test data";
        let shard_size = 16;
        let checksum_algo = HashAlgorithm::HighwayHash256S;

        let result = create_bitrot_reader(
            Some(test_data),
            None,
            "test-bucket",
            "test-path",
            0,
            0,
            shard_size,
            checksum_algo,
            false,
            false,
        )
        .await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_create_bitrot_reader_with_zero_copy_enabled() {
        let test_data = b"hello world test data";
        let shard_size = 16;
        let checksum_algo = HashAlgorithm::HighwayHash256S;

        // Test with zero-copy enabled (should work the same for inline data)
        let result = create_bitrot_reader(
            Some(test_data),
            None,
            "test-bucket",
            "test-path",
            0,
            0,
            shard_size,
            checksum_algo,
            false,
            true,
        )
        .await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_some());
    }

    #[tokio::test]
    async fn test_create_bitrot_chunk_stream_with_inline_data() {
        let shard_size = 4;
        let checksum_algo = HashAlgorithm::HighwayHash256S;
        let shard1 = b"abcd";
        let shard2 = b"ef";

        let mut encoded = Vec::new();
        encoded.extend_from_slice(checksum_algo.hash_encode(shard1).as_ref());
        encoded.extend_from_slice(shard1);
        encoded.extend_from_slice(checksum_algo.hash_encode(shard2).as_ref());
        encoded.extend_from_slice(shard2);

        let mut stream = create_bitrot_chunk_stream(
            Some(&encoded),
            None,
            "test-bucket",
            "test-path",
            0,
            shard1.len() + shard2.len(),
            shard1.len() + shard2.len(),
            shard_size,
            checksum_algo,
            false,
            false,
        )
        .await
        .unwrap()
        .unwrap()
        .stream;

        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await {
            collected.extend_from_slice(&chunk.unwrap().as_bytes());
        }

        assert_eq!(collected, b"abcdef");
    }

    #[tokio::test]
    async fn test_create_bitrot_chunk_stream_detects_hash_mismatch() {
        let shard_size = 4;
        let checksum_algo = HashAlgorithm::HighwayHash256S;
        let shard = b"abcd";

        let mut encoded = Vec::new();
        let mut bad_hash = checksum_algo.hash_encode(shard).as_ref().to_vec();
        bad_hash[0] ^= 0xFF;
        encoded.extend_from_slice(&bad_hash);
        encoded.extend_from_slice(shard);

        let result = create_bitrot_chunk_stream(
            Some(&encoded),
            None,
            "test-bucket",
            "test-path",
            0,
            shard.len(),
            shard.len(),
            shard_size,
            checksum_algo,
            false,
            false,
        )
        .await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_create_bitrot_chunk_stream_trims_range_after_decode() {
        let shard_size = 4;
        let checksum_algo = HashAlgorithm::HighwayHash256S;
        let shard1 = b"abcd";
        let shard2 = b"efgh";

        let mut encoded = Vec::new();
        encoded.extend_from_slice(checksum_algo.hash_encode(shard1).as_ref());
        encoded.extend_from_slice(shard1);
        encoded.extend_from_slice(checksum_algo.hash_encode(shard2).as_ref());
        encoded.extend_from_slice(shard2);

        let mut stream = create_bitrot_chunk_stream(
            Some(&encoded),
            None,
            "test-bucket",
            "test-path",
            1,
            5,
            shard1.len() + shard2.len(),
            shard_size,
            checksum_algo,
            false,
            false,
        )
        .await
        .unwrap()
        .unwrap()
        .stream;

        let mut collected = Vec::new();
        while let Some(chunk) = stream.next().await {
            collected.extend_from_slice(&chunk.unwrap().as_bytes());
        }

        assert_eq!(collected, b"bcdef");
    }

    #[test]
    fn test_decode_bitrot_chunk_source_preserves_aligned_multi_chunk_slices() {
        let shard_size = 4;
        let checksum_algo = HashAlgorithm::Md5;
        let shard1 = b"abcd";
        let shard2 = b"efgh";

        let mut encoded_chunk_one = Vec::new();
        encoded_chunk_one.extend_from_slice(checksum_algo.hash_encode(shard1).as_ref());
        encoded_chunk_one.extend_from_slice(shard1);

        let mut encoded_chunk_two = Vec::new();
        encoded_chunk_two.extend_from_slice(checksum_algo.hash_encode(shard2).as_ref());
        encoded_chunk_two.extend_from_slice(shard2);

        let source_chunks = vec![
            IoChunk::Shared(Bytes::from(encoded_chunk_one)),
            IoChunk::Shared(Bytes::from(encoded_chunk_two)),
        ];
        let (decoded, copied) = decode_bitrot_chunk_source(&source_chunks, shard_size, checksum_algo, false).unwrap();

        assert!(!copied, "frame-aligned multi-chunk source should not require aggregate copies");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].as_bytes(), Bytes::from_static(b"abcd"));
        assert_eq!(decoded[1].as_bytes(), Bytes::from_static(b"efgh"));
    }

    #[test]
    fn test_decode_bitrot_chunk_source_marks_cross_chunk_frame_as_copied() {
        let shard_size = 4;
        let checksum_algo = HashAlgorithm::Md5;
        let shard1 = b"abcd";
        let shard2 = b"efgh";

        let hash1 = checksum_algo.hash_encode(shard1).as_ref().to_vec();
        let hash2 = checksum_algo.hash_encode(shard2).as_ref().to_vec();
        let mut encoded = Vec::new();
        encoded.extend_from_slice(&hash1);
        encoded.extend_from_slice(shard1);
        encoded.extend_from_slice(&hash2);
        encoded.extend_from_slice(shard2);

        let split = hash1.len() + 2;
        let source_chunks = vec![
            IoChunk::Shared(Bytes::copy_from_slice(&encoded[..split])),
            IoChunk::Shared(Bytes::copy_from_slice(&encoded[split..])),
        ];
        let (decoded, copied) = decode_bitrot_chunk_source(&source_chunks, shard_size, checksum_algo, false).unwrap();

        assert!(copied, "cross-chunk frame should be classified as requiring a copy");
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].as_bytes(), Bytes::from_static(b"abcd"));
        assert_eq!(decoded[1].as_bytes(), Bytes::from_static(b"efgh"));
    }

    #[tokio::test]
    async fn test_create_bitrot_reader_with_inline_offset_starts_at_requested_shard() {
        let shard_size = 4;
        let checksum_algo = HashAlgorithm::HighwayHash256S;
        let payload = b"abcdefghijkl";

        let mut writer = create_bitrot_writer(
            true,
            None,
            "test-volume",
            "test-path",
            payload.len() as i64,
            shard_size,
            checksum_algo.clone(),
        )
        .await
        .expect("inline bitrot writer");

        for chunk in payload.chunks(shard_size) {
            writer.write(chunk).await.expect("write chunk");
        }

        let inline_data = writer.into_inline_data().expect("inline buffer");
        let mut reader = create_bitrot_reader(
            Some(&inline_data),
            None,
            "test-bucket",
            "test-path",
            shard_size,
            shard_size,
            shard_size,
            checksum_algo,
            false,
            false,
        )
        .await
        .expect("create reader")
        .expect("reader");

        let mut out = [0u8; 4];
        let n = reader.read(&mut out).await.expect("read second shard");

        assert_eq!(n, shard_size);
        assert_eq!(&out[..n], b"efgh");
    }

    #[tokio::test]
    async fn test_create_bitrot_reader_without_data_or_disk() {
        let shard_size = 16;
        let checksum_algo = HashAlgorithm::HighwayHash256S;

        let result =
            create_bitrot_reader(None, None, "test-bucket", "test-path", 0, 1024, shard_size, checksum_algo, false, false).await;

        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_create_bitrot_writer_inline() {
        use rustfs_utils::HashAlgorithm;

        let wrapper = create_bitrot_writer(
            true, // is_inline_buffer
            None, // disk not needed for inline buffer
            "test-volume",
            "test-path",
            1024, // length
            1024, // shard_size
            HashAlgorithm::HighwayHash256S,
        )
        .await;

        assert!(wrapper.is_ok());
        let mut wrapper = wrapper.unwrap();

        // Test writing some data
        let test_data = b"hello world";
        let result = wrapper.write(test_data).await;
        assert!(result.is_ok());

        // Test getting inline data
        let inline_data = wrapper.into_inline_data();
        assert!(inline_data.is_some());
        // The inline data should contain both hash and data
        let data = inline_data.unwrap();
        assert!(!data.is_empty());
    }

    #[tokio::test]
    async fn test_create_bitrot_writer_disk_without_disk() {
        use rustfs_utils::HashAlgorithm;

        // Test error case: trying to create disk writer without providing disk instance
        let wrapper = create_bitrot_writer(
            false, // is_inline_buffer = false, so needs disk
            None,  // disk = None, should cause error
            "test-volume",
            "test-path",
            1024, // length
            1024, // shard_size
            HashAlgorithm::HighwayHash256S,
        )
        .await;

        assert!(wrapper.is_err());
        let error = wrapper.unwrap_err();
        println!("error: {error:?}");
        assert_eq!(error, DiskError::DiskNotFound);
    }
}
