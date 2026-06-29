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

use crate::disk::{self, DiskAPI as _, DiskStore, FileReader, error::DiskError};
use crate::erasure::coding::{BitrotReader, BitrotWriterWrapper, CustomWriter};
use bytes::Bytes;
use rustfs_config::{DEFAULT_OBJECT_MMAP_READ_ENABLE, ENV_OBJECT_MMAP_READ_ENABLE, ENV_OBJECT_ZERO_COPY_ENABLE};
use rustfs_utils::HashAlgorithm;
use std::future::Future;
use std::io::{self, Cursor};
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::io::{AsyncRead, ReadBuf};
use tracing::debug;

type BoxedObjectReader = Box<dyn AsyncRead + Send + Sync + Unpin>;
type OpenObjectReaderFuture = Pin<Box<dyn Future<Output = disk::error::Result<Option<BoxedObjectReader>>> + Send>>;

pub(crate) fn object_mmap_read_enabled() -> bool {
    rustfs_utils::get_env_bool_with_aliases(
        ENV_OBJECT_MMAP_READ_ENABLE,
        &[ENV_OBJECT_ZERO_COPY_ENABLE],
        DEFAULT_OBJECT_MMAP_READ_ENABLE,
    )
}

#[derive(Clone)]
struct BitrotReaderSource {
    inline_data: Option<Bytes>,
    disk: Option<DiskStore>,
    bucket: String,
    path: String,
    offset: usize,
    length: usize,
    use_mmap_read: bool,
}

impl BitrotReaderSource {
    async fn open(self) -> disk::error::Result<Option<BoxedObjectReader>> {
        if let Some(data) = self.inline_data {
            let mut rd = Cursor::new(data);
            let offset = u64::try_from(self.offset).map_err(|_| DiskError::FileCorrupt)?;
            rd.set_position(offset);
            Ok(Some(Box::new(rd)))
        } else if let Some(disk) = self.disk {
            open_disk_reader(&disk, &self.bucket, &self.path, self.offset, self.length, self.use_mmap_read)
                .await
                .map(Some)
        } else {
            Ok(None)
        }
    }
}

struct DeferredObjectReader {
    state: Mutex<DeferredObjectReaderState>,
}

enum DeferredObjectReaderState {
    Pending(Option<BitrotReaderSource>),
    Opening(OpenObjectReaderFuture),
    Ready(BoxedObjectReader),
    Failed,
}

impl DeferredObjectReader {
    fn new(source: BitrotReaderSource) -> Self {
        Self {
            state: Mutex::new(DeferredObjectReaderState::Pending(Some(source))),
        }
    }
}

impl AsyncRead for DeferredObjectReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        loop {
            let mut state = match self.state.lock() {
                Ok(state) => state,
                Err(_) => return Poll::Ready(Err(io::Error::other("deferred bitrot reader state poisoned"))),
            };

            match &mut *state {
                DeferredObjectReaderState::Pending(source) => {
                    let Some(source) = source.take() else {
                        *state = DeferredObjectReaderState::Failed;
                        return Poll::Ready(Err(io::Error::other("deferred bitrot reader source missing")));
                    };
                    *state = DeferredObjectReaderState::Opening(Box::pin(source.open()));
                }
                DeferredObjectReaderState::Opening(open) => match open.as_mut().poll(cx) {
                    Poll::Pending => return Poll::Pending,
                    Poll::Ready(Ok(Some(reader))) => {
                        *state = DeferredObjectReaderState::Ready(reader);
                    }
                    Poll::Ready(Ok(None)) => {
                        *state = DeferredObjectReaderState::Failed;
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            "deferred bitrot reader source missing",
                        )));
                    }
                    Poll::Ready(Err(err)) => {
                        *state = DeferredObjectReaderState::Failed;
                        return Poll::Ready(Err(disk_error_to_io_error(err)));
                    }
                },
                DeferredObjectReaderState::Ready(reader) => return Pin::new(reader).poll_read(cx, buf),
                DeferredObjectReaderState::Failed => {
                    return Poll::Ready(Err(io::Error::other("deferred bitrot reader already failed")));
                }
            }
        }
    }
}

fn disk_error_to_io_error(err: DiskError) -> io::Error {
    let kind = match err {
        DiskError::Timeout | DiskError::SourceStalled => io::ErrorKind::TimedOut,
        DiskError::DiskNotFound | DiskError::FileNotFound | DiskError::FileVersionNotFound | DiskError::PathNotFound => {
            io::ErrorKind::NotFound
        }
        DiskError::FileCorrupt | DiskError::PartMissingOrCorrupt | DiskError::BitrotHashAlgoInvalid => io::ErrorKind::InvalidData,
        DiskError::Io(io_err) => return io_err,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, err.to_string())
}

async fn open_disk_reader(
    disk: &DiskStore,
    bucket: &str,
    path: &str,
    offset: usize,
    length: usize,
    use_mmap_read: bool,
) -> disk::error::Result<FileReader> {
    if use_mmap_read && disk.is_local() {
        let start = Instant::now();
        match disk.read_file_zero_copy(bucket, path, offset, length).await {
            Ok(bytes) => {
                let duration_ms = start.elapsed().as_secs_f64() * 1000.0;

                rustfs_io_metrics::record_zero_copy_read(bytes.len(), duration_ms);
                debug!(
                    size = bytes.len(),
                    path = %path,
                    "zero_copy_read_success"
                );

                return Ok(Box::new(Cursor::new(bytes)));
            }
            Err(err) => {
                let reason = format!("{err:?}");
                rustfs_io_metrics::record_zero_copy_fallback(&reason);
                debug!(
                    reason = %reason,
                    path = %path,
                    "zero_copy_fallback"
                );

                return match disk.read_file_stream(bucket, path, offset, length).await {
                    Ok(reader) => Ok(reader),
                    Err(_) => Err(err),
                };
            }
        }
    }

    disk.read_file_stream(bucket, path, offset, length).await
}

fn bitrot_encoded_range(offset: usize, length: usize, shard_size: usize, checksum_algo: HashAlgorithm) -> (usize, usize) {
    (
        offset.div_ceil(shard_size) * checksum_algo.size() + offset,
        length.div_ceil(shard_size) * checksum_algo.size() + length,
    )
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
/// * `use_mmap_read` - If true, use zero-copy read (mmap on Unix)
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
    use_mmap_read: bool,
) -> disk::error::Result<Option<BitrotReader<Box<dyn AsyncRead + Send + Sync + Unpin>>>> {
    let (offset, length) = bitrot_encoded_range(offset, length, shard_size, checksum_algo.clone());
    let source = BitrotReaderSource {
        inline_data: inline_data.map(Bytes::copy_from_slice),
        disk: disk.cloned(),
        bucket: bucket.to_string(),
        path: path.to_string(),
        offset,
        length,
        use_mmap_read,
    };

    source
        .open()
        .await
        .map(|reader| reader.map(|reader| BitrotReader::new(reader, shard_size, checksum_algo, skip_verify)))
}

#[allow(clippy::too_many_arguments)]
pub fn create_deferred_bitrot_reader(
    inline_data: Option<Bytes>,
    disk: Option<DiskStore>,
    bucket: &str,
    path: &str,
    offset: usize,
    length: usize,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify: bool,
    use_mmap_read: bool,
) -> BitrotReader<Box<dyn AsyncRead + Send + Sync + Unpin>> {
    let (offset, length) = bitrot_encoded_range(offset, length, shard_size, checksum_algo.clone());
    let source = BitrotReaderSource {
        inline_data,
        disk,
        bucket: bucket.to_string(),
        path: path.to_string(),
        offset,
        length,
        use_mmap_read,
    };

    BitrotReader::new(Box::new(DeferredObjectReader::new(source)), shard_size, checksum_algo, skip_verify)
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

    #[test]
    fn object_mmap_read_enabled_accepts_legacy_zero_copy_alias() {
        temp_env::with_vars(
            [
                (ENV_OBJECT_MMAP_READ_ENABLE, None::<&str>),
                (ENV_OBJECT_ZERO_COPY_ENABLE, Some("false")),
            ],
            || {
                assert!(!object_mmap_read_enabled());
            },
        );
    }

    #[test]
    fn object_mmap_read_enabled_prefers_canonical_env() {
        temp_env::with_vars(
            [
                (ENV_OBJECT_MMAP_READ_ENABLE, Some("true")),
                (ENV_OBJECT_ZERO_COPY_ENABLE, Some("false")),
            ],
            || {
                assert!(object_mmap_read_enabled());
            },
        );
    }

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
    async fn test_deferred_bitrot_reader_opens_inline_source_on_read() {
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
        let mut reader = create_deferred_bitrot_reader(
            Some(inline_data.into()),
            None,
            "test-bucket",
            "test-path",
            shard_size,
            shard_size,
            shard_size,
            checksum_algo,
            false,
            false,
        );

        let mut out = [0u8; 4];
        let n = reader.read(&mut out).await.expect("read deferred second shard");

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
