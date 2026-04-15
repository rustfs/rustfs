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
use bytes::Bytes;
use rustfs_utils::HashAlgorithm;
use std::io::Cursor;
use std::time::Instant;
use tokio::io::AsyncRead;
use tracing::debug;

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
        if use_zero_copy && disk.is_local() {
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
