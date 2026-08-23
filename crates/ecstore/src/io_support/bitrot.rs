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

use crate::diagnostics::get::{
    GET_STAGE_READER_MMAP_ACCESS_CHECK, GET_STAGE_READER_MMAP_BLOCKING_TASK, GET_STAGE_READER_MMAP_BLOCKING_WAIT,
    GET_STAGE_READER_MMAP_COPY_BUFFER, GET_STAGE_READER_MMAP_DIRECT_READ_COPY, GET_STAGE_READER_MMAP_FILE_OPEN,
    GET_STAGE_READER_MMAP_MAP, GET_STAGE_READER_MMAP_METADATA_LOOKUP, GET_STAGE_READER_MMAP_METADATA_VALIDATE,
    GET_STAGE_READER_MMAP_PATH_RESOLVE, GET_STAGE_READER_OPEN_MMAP_COPY_FALLBACK, GET_STAGE_READER_OPEN_MMAP_COPY_SUCCESS,
    GET_STAGE_READER_OPEN_STREAM, GET_STAGE_READER_STREAM_FIRST_READ, record_get_stage_duration_if_enabled,
};
#[cfg(feature = "hotpath")]
use crate::disk::FileWriter;
use crate::disk::{self, DiskAPI as _, DiskStore, FileReader, MmapCopyStageMetrics, error::DiskError};
use crate::erasure::coding::{BitrotReader, BitrotWriterWrapper, CustomWriter, ShardChunkRead};
use bytes::Bytes;
use rustfs_config::{
    DEFAULT_OBJECT_MMAP_READ_ENABLE, DEFAULT_OBJECT_MMAP_READ_MAX_LENGTH, ENV_OBJECT_MMAP_READ_ENABLE,
    ENV_OBJECT_MMAP_READ_MAX_LENGTH, ENV_OBJECT_ZERO_COPY_ENABLE,
};
use rustfs_rio::ChunkReaderBox;
use rustfs_utils::HashAlgorithm;
use std::future::Future;
use std::io::{self, Cursor};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Instant;
use tokio::io::{AsyncRead, ReadBuf};
use tracing::debug;

#[cfg(all(test, feature = "hotpath"))]
tokio::task_local! {
    static FORCE_MMAP_COPY_FAILURE_FOR_TEST: ();
}

/// A shard source for the bitrot reader.
///
/// `InMemory` keeps the `Bytes` concrete instead of erasing it behind
/// `dyn AsyncRead`, so `BitrotReader` can slice the `[hash][data]` block straight
/// out of the page-cache copy rather than copying it into a scratch buffer first
/// (rustfs/backlog#1159). Everything else is a stream and keeps the old path.
pub enum ShardReader {
    InMemory(Cursor<Bytes>),
    Chunked(ChunkReaderBox),
    Stream(Box<dyn AsyncRead + Send + Sync + Unpin>),
}

#[cfg(test)]
impl ShardReader {
    pub(crate) fn inline_bytes(&self) -> Option<&Bytes> {
        match self {
            Self::InMemory(cursor) => Some(cursor.get_ref()),
            Self::Chunked(_) | Self::Stream(_) => None,
        }
    }
}

impl AsyncRead for ShardReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut tokio::io::ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Self::InMemory(cursor) => Pin::new(cursor).poll_read(cx, buf),
            Self::Chunked(reader) => Pin::new(&mut **reader).poll_read(cx, buf),
            Self::Stream(reader) => Pin::new(reader).poll_read(cx, buf),
        }
    }
}

impl crate::erasure::coding::ShardSource for ShardReader {
    fn try_take_block(&mut self, n: usize) -> Option<Bytes> {
        match self {
            Self::InMemory(cursor) => cursor.try_take_block(n),
            Self::Chunked(_) | Self::Stream(_) => None,
        }
    }

    fn poll_read_chunk(self: Pin<&mut Self>, cx: &mut Context<'_>, max: usize) -> Poll<io::Result<ShardChunkRead>> {
        let Self::Chunked(reader) = self.get_mut() else {
            return Poll::Ready(Ok(ShardChunkRead::Unsupported));
        };
        match Pin::new(&mut **reader).poll_read_chunk(cx, max) {
            Poll::Ready(Ok(Some(chunk))) => Poll::Ready(Ok(ShardChunkRead::Chunk(chunk))),
            Poll::Ready(Ok(None)) => Poll::Ready(Ok(ShardChunkRead::Eof)),
            Poll::Ready(Err(err)) => Poll::Ready(Err(err)),
            Poll::Pending => Poll::Pending,
        }
    }
}

type BoxedObjectReader = ShardReader;
type OpenObjectReaderFuture = Pin<Box<dyn Future<Output = disk::error::Result<Option<BoxedObjectReader>>> + Send>>;

#[derive(Clone, Copy)]
pub(crate) struct BitrotReaderStageMetrics {
    pub(crate) path: &'static str,
    pub(crate) reader_construction_stage: &'static str,
    pub(crate) file_open_stage: &'static str,
    pub(crate) bitrot_reader_init_stage: &'static str,
}

pub(crate) fn object_mmap_read_enabled() -> bool {
    rustfs_utils::get_env_bool_with_aliases(
        ENV_OBJECT_MMAP_READ_ENABLE,
        &[ENV_OBJECT_ZERO_COPY_ENABLE],
        DEFAULT_OBJECT_MMAP_READ_ENABLE,
    )
}

/// Mmap-copy read length cap. Cached: this is consulted on every shard open
/// and `std::env::var` takes a process-global lock. In test builds the env
/// var is read directly so `temp_env` overrides take effect.
pub(crate) fn object_mmap_read_max_length() -> usize {
    #[cfg(test)]
    {
        rustfs_utils::get_env_usize(ENV_OBJECT_MMAP_READ_MAX_LENGTH, DEFAULT_OBJECT_MMAP_READ_MAX_LENGTH)
    }
    #[cfg(not(test))]
    {
        static CACHED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
        *CACHED.get_or_init(|| rustfs_utils::get_env_usize(ENV_OBJECT_MMAP_READ_MAX_LENGTH, DEFAULT_OBJECT_MMAP_READ_MAX_LENGTH))
    }
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
    stage_metrics: Option<BitrotReaderStageMetrics>,
}

impl BitrotReaderSource {
    async fn open(self) -> disk::error::Result<Option<BoxedObjectReader>> {
        open_reader_source(
            self.inline_data,
            self.disk.as_ref(),
            &self.bucket,
            &self.path,
            self.offset,
            self.length,
            self.use_mmap_read,
            self.stage_metrics.map(|metrics| metrics.path),
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn open_reader_source(
    inline_data: Option<Bytes>,
    disk: Option<&DiskStore>,
    bucket: &str,
    path: &str,
    offset: usize,
    length: usize,
    use_mmap_read: bool,
    metrics_path: Option<&'static str>,
) -> disk::error::Result<Option<BoxedObjectReader>> {
    if let Some(data) = inline_data {
        let mut reader = Cursor::new(data);
        reader.set_position(u64::try_from(offset).map_err(|_| DiskError::FileCorrupt)?);
        Ok(Some(ShardReader::InMemory(reader)))
    } else if let Some(disk) = disk {
        open_disk_reader(disk, bucket, path, offset, length, use_mmap_read, metrics_path)
            .await
            .map(Some)
    } else {
        Ok(None)
    }
}

struct FirstReadMetricsReader {
    inner: FileReader,
    metrics_path: &'static str,
    stage: &'static str,
    started_at: Option<Instant>,
    recorded: bool,
}

impl FirstReadMetricsReader {
    fn new(inner: FileReader, metrics_path: &'static str, stage: &'static str) -> Self {
        Self {
            inner,
            metrics_path,
            stage,
            started_at: None,
            recorded: false,
        }
    }
}

impl AsyncRead for FirstReadMetricsReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if self.recorded {
            return Pin::new(&mut self.inner).poll_read(cx, buf);
        }

        let filled_before = buf.filled().len();
        if self.started_at.is_none() {
            self.started_at = Some(Instant::now());
        }

        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                if buf.filled().len() > filled_before {
                    self.recorded = true;
                    record_get_stage_duration_if_enabled(self.metrics_path, self.stage, self.started_at.take());
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

struct DeferredObjectReader {
    state: Arc<Mutex<DeferredObjectReaderState>>,
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
            state: Arc::new(Mutex::new(DeferredObjectReaderState::Pending(Some(source)))),
        }
    }

    fn stripe_handle(&self, stripe_stride: usize) -> DeferredReaderStripeHandle {
        DeferredReaderStripeHandle {
            state: Arc::clone(&self.state),
            stripe_stride,
        }
    }
}

/// Handle to a still-unopened [`DeferredObjectReader`] that can advance the
/// pending open offset by whole bitrot blocks (stripes) before the first read.
///
/// The GET lockstep decode reads only the data shards while the object is
/// healthy and keeps every parity slot as an unopened deferred reader. When a
/// data shard dies at stripe `k`, the decoder uses this handle to shift the
/// parity reader's pending open offset by `k` encoded blocks — the same
/// `bitrot_encoded_range` geometry used when the reader was created — so its
/// first read returns stripe `k` and the lockstep alignment invariant holds
/// (backlog#923; alignment rule from backlog#832).
///
/// `stripe_stride` is `shard_size + checksum_algo.size()` per full stripe.
/// For `hash_size == 0` (e.g. `HashAlgorithm::None`) this degrades to the
/// identity mapping `k * shard_size`, matching `bitrot_encoded_range`.
#[derive(Clone)]
pub(crate) struct DeferredReaderStripeHandle {
    state: Arc<Mutex<DeferredObjectReaderState>>,
    stripe_stride: usize,
}

impl DeferredReaderStripeHandle {
    /// Advance the pending source by `stripes` full stripes.
    ///
    /// Returns `false` when the reader has already been opened (or failed):
    /// its stream position is then unknown to the caller and it must not be
    /// engaged mid-object.
    pub(crate) fn advance_stripes(&self, stripes: usize) -> bool {
        if stripes == 0 {
            return true;
        }
        let Ok(mut state) = self.state.lock() else {
            return false;
        };
        match &mut *state {
            DeferredObjectReaderState::Pending(Some(source)) => {
                let Some(delta) = self.stripe_stride.checked_mul(stripes) else {
                    return false;
                };
                let Some(offset) = source.offset.checked_add(delta) else {
                    return false;
                };
                source.offset = offset;
                source.length = source.length.saturating_sub(delta);
                true
            }
            _ => false,
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
    metrics_path: Option<&'static str>,
) -> disk::error::Result<ShardReader> {
    let metrics_path = metrics_path.filter(|_| rustfs_io_metrics::get_stage_metrics_enabled());
    let stage_metrics_enabled = metrics_path.is_some();

    // Preserve HTTP body ownership only on healthy remote reads. Instrumented
    // and local paths retain their existing AsyncRead wrappers.
    if use_mmap_read
        && !disk.is_local()
        && !stage_metrics_enabled
        && !cfg!(feature = "hotpath")
        && let Some(reader) = disk.read_file_stream_chunks(bucket, path, offset, length).await?
    {
        return Ok(ShardReader::Chunked(reader));
    }

    // Mmap-copy materializes the whole `offset..offset+length` range as one
    // owned allocation before any byte is served, and GET/heal shard reads
    // request the entire part span in one call. Over-cap reads (e.g. a huge
    // single-part object, rustfs#5123) take the bounded streaming path below.
    let use_mmap_copy = if use_mmap_read && disk.is_local() {
        let mmap_read_cap = object_mmap_read_max_length();
        let within_cap = length <= mmap_read_cap;
        if !within_cap {
            rustfs_io_metrics::record_zero_copy_fallback("read_length_exceeds_cap");
            debug!(
                length,
                mmap_read_cap,
                path = %path,
                "zero_copy_read_skipped_over_cap"
            );
        }
        within_cap
    } else {
        false
    };

    if use_mmap_copy {
        let start = stage_metrics_enabled.then(Instant::now);
        let zero_copy_start = Instant::now();
        let mmap_metrics = metrics_path.map(|metrics_path| MmapCopyStageMetrics {
            path: metrics_path,
            access_check_stage: GET_STAGE_READER_MMAP_ACCESS_CHECK,
            path_resolve_stage: GET_STAGE_READER_MMAP_PATH_RESOLVE,
            metadata_lookup_stage: GET_STAGE_READER_MMAP_METADATA_LOOKUP,
            metadata_validate_stage: GET_STAGE_READER_MMAP_METADATA_VALIDATE,
            blocking_wait_stage: GET_STAGE_READER_MMAP_BLOCKING_WAIT,
            blocking_task_stage: GET_STAGE_READER_MMAP_BLOCKING_TASK,
            file_open_stage: GET_STAGE_READER_MMAP_FILE_OPEN,
            mmap_map_stage: GET_STAGE_READER_MMAP_MAP,
            mmap_copy_stage: GET_STAGE_READER_MMAP_COPY_BUFFER,
            direct_read_copy_stage: GET_STAGE_READER_MMAP_DIRECT_READ_COPY,
        });
        let mmap_result = {
            #[cfg(all(test, feature = "hotpath"))]
            if FORCE_MMAP_COPY_FAILURE_FOR_TEST.try_with(|_| ()).is_ok() {
                Err(DiskError::other("forced mmap-copy failure for test"))
            } else {
                disk.read_file_mmap_copy_with_metrics(bucket, path, offset, length, mmap_metrics)
                    .await
            }
            #[cfg(not(all(test, feature = "hotpath")))]
            {
                disk.read_file_mmap_copy_with_metrics(bucket, path, offset, length, mmap_metrics)
                    .await
            }
        };
        match mmap_result {
            Ok(bytes) => {
                let duration_ms = zero_copy_start.elapsed().as_secs_f64() * 1000.0;

                rustfs_io_metrics::record_zero_copy_read(bytes.len(), duration_ms);
                if let Some(metrics_path) = metrics_path {
                    record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_READER_OPEN_MMAP_COPY_SUCCESS, start);
                }
                debug!(
                    size = bytes.len(),
                    path = %path,
                    "zero_copy_read_success"
                );

                return Ok(ShardReader::InMemory(Cursor::new(bytes)));
            }
            Err(err) => {
                if let Some(metrics_path) = metrics_path {
                    record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_READER_OPEN_MMAP_COPY_FALLBACK, start);
                }
                let reason = format!("{err:?}");
                rustfs_io_metrics::record_zero_copy_fallback(&reason);
                debug!(
                    reason = %reason,
                    path = %path,
                    "zero_copy_fallback"
                );

                let stream_start = stage_metrics_enabled.then(Instant::now);
                let stream_result = disk.read_file_stream(bucket, path, offset, length).await;
                if let Some(metrics_path) = metrics_path {
                    record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_READER_OPEN_STREAM, stream_start);
                }

                return match stream_result {
                    Ok(reader) => {
                        #[cfg(feature = "hotpath")]
                        let reader = instrument_raw_shard_reader(reader, disk.is_local());
                        Ok(wrap_first_read_metrics(reader, metrics_path))
                    }
                    Err(_) => Err(err),
                };
            }
        }
    }

    let stream_start = stage_metrics_enabled.then(Instant::now);
    let reader = disk.read_file_stream(bucket, path, offset, length).await?;
    #[cfg(feature = "hotpath")]
    let reader = instrument_raw_shard_reader(reader, disk.is_local());
    if let Some(metrics_path) = metrics_path {
        record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_READER_OPEN_STREAM, stream_start);
    }
    Ok(wrap_first_read_metrics(reader, metrics_path))
}

fn wrap_first_read_metrics(reader: FileReader, metrics_path: Option<&'static str>) -> ShardReader {
    if let Some(metrics_path) = metrics_path
        && rustfs_io_metrics::get_stage_metrics_enabled()
    {
        return ShardReader::Stream(Box::new(FirstReadMetricsReader::new(
            reader,
            metrics_path,
            GET_STAGE_READER_STREAM_FIRST_READ,
        )));
    }

    ShardReader::Stream(reader)
}

// The labels are deliberately fixed: object keys, disk paths, and remote hosts
// are all high-cardinality or sensitive and belong nowhere in a profiling report.
#[cfg(feature = "hotpath")]
const RAW_SHARD_READ_LOCAL_LABEL: &str = "EC raw shard read local";
#[cfg(feature = "hotpath")]
const RAW_SHARD_READ_REMOTE_LABEL: &str = "EC raw shard read remote";
#[cfg(feature = "hotpath")]
const RAW_SHARD_WRITE_LOCAL_LABEL: &str = "EC raw shard write local";
#[cfg(feature = "hotpath")]
const RAW_SHARD_WRITE_REMOTE_LABEL: &str = "EC raw shard write remote";

#[cfg(feature = "hotpath")]
fn instrument_raw_shard_reader(reader: FileReader, is_local: bool) -> FileReader {
    // `io!` aggregates by call site, so the local and remote branches must remain distinct.
    if is_local {
        Box::new(hotpath::io!(reader, label = RAW_SHARD_READ_LOCAL_LABEL))
    } else {
        Box::new(hotpath::io!(reader, label = RAW_SHARD_READ_REMOTE_LABEL))
    }
}

#[cfg(feature = "hotpath")]
fn instrument_raw_shard_writer(writer: FileWriter, is_local: bool) -> FileWriter {
    // `io!` aggregates by call site, so the local and remote branches must remain distinct.
    if is_local {
        Box::new(hotpath::io!(writer, label = RAW_SHARD_WRITE_LOCAL_LABEL))
    } else {
        Box::new(hotpath::io!(writer, label = RAW_SHARD_WRITE_REMOTE_LABEL))
    }
}

fn bitrot_encoded_range(offset: usize, length: usize, shard_size: usize, checksum_algo: HashAlgorithm) -> (usize, usize) {
    adjust_shard_read_params(offset, length, shard_size, &checksum_algo)
}

/// Adjusts a raw (offset, length) pair to account for per-shard checksum overhead.
/// Returns (adjusted_offset, adjusted_length).
pub(crate) fn adjust_shard_read_params(
    offset: usize,
    length: usize,
    shard_size: usize,
    checksum_algo: &HashAlgorithm,
) -> (usize, usize) {
    let adj_len = length.div_ceil(shard_size) * checksum_algo.size() + length;
    let adj_off = offset.div_ceil(shard_size) * checksum_algo.size() + offset;
    (adj_off, adj_len)
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
/// * `use_mmap_read` - If true, use mmap-copy read (mmap on Unix)
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
) -> disk::error::Result<Option<BitrotReader<ShardReader>>> {
    create_bitrot_reader_with_stage_metrics(
        inline_data,
        disk,
        bucket,
        path,
        offset,
        length,
        shard_size,
        checksum_algo,
        skip_verify,
        use_mmap_read,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_bitrot_reader_with_stage_metrics(
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
    stage_metrics: Option<BitrotReaderStageMetrics>,
) -> disk::error::Result<Option<BitrotReader<ShardReader>>> {
    create_bitrot_reader_from_bytes_with_stage_metrics(
        inline_data.map(Bytes::copy_from_slice),
        disk,
        bucket,
        path,
        offset,
        length,
        shard_size,
        checksum_algo,
        skip_verify,
        use_mmap_read,
        stage_metrics,
    )
    .await
}

/// Create a BitrotReader from owned inline Bytes or a disk file stream.
///
/// Passing `Bytes` preserves the shared inline data buffer and avoids copying
/// shard payloads that are already owned by metadata.
#[allow(clippy::too_many_arguments)]
pub async fn create_bitrot_reader_from_bytes(
    inline_data: Option<Bytes>,
    disk: Option<&DiskStore>,
    bucket: &str,
    path: &str,
    offset: usize,
    length: usize,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify: bool,
    use_mmap_read: bool,
) -> disk::error::Result<Option<BitrotReader<ShardReader>>> {
    create_bitrot_reader_from_bytes_with_stage_metrics(
        inline_data,
        disk,
        bucket,
        path,
        offset,
        length,
        shard_size,
        checksum_algo,
        skip_verify,
        use_mmap_read,
        None,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn create_bitrot_reader_from_bytes_with_stage_metrics(
    inline_data: Option<Bytes>,
    disk: Option<&DiskStore>,
    bucket: &str,
    path: &str,
    offset: usize,
    length: usize,
    shard_size: usize,
    checksum_algo: HashAlgorithm,
    skip_verify: bool,
    use_mmap_read: bool,
    stage_metrics: Option<BitrotReaderStageMetrics>,
) -> disk::error::Result<Option<BitrotReader<ShardReader>>> {
    let stage_metrics = stage_metrics.filter(|_| rustfs_io_metrics::get_stage_metrics_enabled());
    let stage_metrics_enabled = stage_metrics.is_some();

    let reader_construction_start = stage_metrics_enabled.then(Instant::now);
    let (offset, length) = bitrot_encoded_range(offset, length, shard_size, checksum_algo.clone());
    if let Some(metrics) = stage_metrics {
        record_get_stage_duration_if_enabled(metrics.path, metrics.reader_construction_stage, reader_construction_start);
    }

    let file_open_start = stage_metrics_enabled.then(Instant::now);
    let reader = open_reader_source(
        inline_data,
        disk,
        bucket,
        path,
        offset,
        length,
        use_mmap_read,
        stage_metrics.map(|metrics| metrics.path),
    )
    .await?;
    if let Some(metrics) = stage_metrics {
        record_get_stage_duration_if_enabled(metrics.path, metrics.file_open_stage, file_open_start);
    }

    let bitrot_reader_init_start = stage_metrics_enabled.then(Instant::now);
    let reader = reader.map(|reader| BitrotReader::new(reader, shard_size, checksum_algo, skip_verify));
    if let Some(metrics) = stage_metrics {
        record_get_stage_duration_if_enabled(metrics.path, metrics.bitrot_reader_init_stage, bitrot_reader_init_start);
    }

    Ok(reader)
}

#[allow(clippy::too_many_arguments)]
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
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
) -> BitrotReader<ShardReader> {
    create_deferred_bitrot_reader_with_stripe_handle(
        inline_data,
        disk,
        bucket,
        path,
        offset,
        length,
        shard_size,
        checksum_algo,
        skip_verify,
        use_mmap_read,
    )
    .0
}

/// Like [`create_deferred_bitrot_reader`], but also returns a
/// [`DeferredReaderStripeHandle`] that can realign the still-unopened reader
/// to a later stripe before its first read (backlog#923).
#[allow(clippy::too_many_arguments)]
pub(crate) fn create_deferred_bitrot_reader_with_stripe_handle(
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
) -> (BitrotReader<ShardReader>, DeferredReaderStripeHandle) {
    let stripe_stride = shard_size + checksum_algo.size();
    let (offset, length) = bitrot_encoded_range(offset, length, shard_size, checksum_algo.clone());
    let inline_source = inline_data.is_some();
    let source = BitrotReaderSource {
        inline_data,
        disk,
        bucket: if inline_source { String::new() } else { bucket.to_string() },
        path: if inline_source { String::new() } else { path.to_string() },
        offset,
        length,
        use_mmap_read,
        stage_metrics: None,
    };

    let deferred = DeferredObjectReader::new(source);
    let handle = deferred.stripe_handle(stripe_stride);
    // The deferred parity reader opens its source lazily, so it cannot hand out an
    // in-memory block up front; it stays on the streaming path. Parity shards are
    // only read when a data shard fails, so the fast path is not needed here.
    let reader = BitrotReader::new(ShardReader::Stream(Box::new(deferred)), shard_size, checksum_algo, skip_verify);
    (reader, handle)
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
/// Size hint handed to `DiskAPI::create_file` for a bitrot-wrapped shard.
///
/// A known length is grown by one checksum per shard so the on-disk file size
/// matches what the bitrot writer emits. A negative length is the
/// unknown-size sentinel (`HashReader::SIZE_PRESERVE_LAYER`, used by SSE and
/// compression) and must be preserved: `RemoteDisk::create_file` forwards it
/// in the `put_file_stream` query, and the receiver only treats `size > 0` as
/// a fixed body length when locating the authenticated trailer. Clamping it
/// to `0` would claim an empty body and misframe the stream. `0` stays `0`
/// because a genuinely empty object still means an empty body.
fn bitrot_create_file_size(length: i64, shard_size: usize, checksum_algo: &HashAlgorithm) -> i64 {
    if length <= 0 {
        return length;
    }
    let length = length as usize;
    (length.div_ceil(shard_size) * checksum_algo.size() + length) as i64
}

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
        let length = bitrot_create_file_size(length, shard_size, &checksum_algo);

        let file = disk.create_file("", volume, path, length).await?;
        #[cfg(feature = "hotpath")]
        let file = instrument_raw_shard_writer(file, disk.is_local());
        CustomWriter::new_tokio_writer(file)
    } else {
        return Err(DiskError::DiskNotFound);
    };

    Ok(BitrotWriterWrapper::new(writer, shard_size, checksum_algo))
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_rio::ChunkReader;
    use std::collections::VecDeque;

    #[test]
    fn bitrot_create_file_size_grows_known_length_by_checksums() {
        // 10 bytes over 4-byte shards = 3 shards, each followed by a 32-byte hash.
        assert_eq!(bitrot_create_file_size(10, 4, &HashAlgorithm::HighwayHash256), 10 + 3 * 32);
        assert_eq!(bitrot_create_file_size(10, 4, &HashAlgorithm::None), 10);
    }

    #[test]
    fn bitrot_create_file_size_keeps_empty_and_unknown_distinct() {
        assert_eq!(bitrot_create_file_size(0, 4, &HashAlgorithm::HighwayHash256), 0);
        // SSE/compression streams advertise SIZE_PRESERVE_LAYER (-1); the remote
        // put_file_stream receiver relies on a non-positive size to parse the auth
        // trailer from the stream tail, so the sentinel must survive untouched.
        assert_eq!(
            bitrot_create_file_size(rustfs_rio::HashReader::SIZE_PRESERVE_LAYER, 4, &HashAlgorithm::HighwayHash256),
            rustfs_rio::HashReader::SIZE_PRESERVE_LAYER
        );
    }

    struct TestChunkReader {
        chunks: VecDeque<Bytes>,
    }

    impl TestChunkReader {
        fn new(bytes: Bytes, fragment_sizes: &[usize]) -> Self {
            let mut chunks = VecDeque::new();
            let mut offset = 0;
            for &size in fragment_sizes {
                let end = (offset + size).min(bytes.len());
                if offset < end {
                    chunks.push_back(bytes.slice(offset..end));
                }
                offset = end;
            }
            if offset < bytes.len() {
                chunks.push_back(bytes.slice(offset..));
            }
            Self { chunks }
        }
    }

    impl AsyncRead for TestChunkReader {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Err(io::Error::other("test chunk reader must use chunk handoff")))
        }
    }

    impl ChunkReader for TestChunkReader {
        fn poll_read_chunk(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<Bytes>>> {
            let Some(mut chunk) = self.chunks.pop_front() else {
                return Poll::Ready(Ok(None));
            };
            let take = chunk.len().min(max);
            if take < chunk.len() {
                self.chunks.push_front(chunk.split_off(take));
            }
            chunk.truncate(take);
            Poll::Ready(Ok(Some(chunk)))
        }
    }

    #[cfg(feature = "hotpath")]
    use crate::cluster::rpc::RemoteDisk;
    #[cfg(feature = "hotpath")]
    use crate::cluster::rpc::internode_data_transport::{
        InternodeDataTransport, InternodeDataTransportCapabilities, ReadStreamRequest, WalkDirStreamRequest, WriteStreamRequest,
    };
    #[cfg(feature = "hotpath")]
    use crate::disk::{Disk, DiskOption, error::Result};

    #[cfg(feature = "hotpath")]
    #[derive(Debug, Clone, Default)]
    struct TestRemoteDataTransport {
        bytes: Arc<Mutex<Vec<u8>>>,
        write_error: Option<io::ErrorKind>,
    }

    #[cfg(feature = "hotpath")]
    impl TestRemoteDataTransport {
        fn with_write_error(kind: io::ErrorKind) -> Self {
            Self {
                bytes: Arc::default(),
                write_error: Some(kind),
            }
        }

        fn bytes(&self) -> Vec<u8> {
            self.bytes
                .lock()
                .expect("test remote transport bytes lock should not be poisoned")
                .clone()
        }
    }

    #[cfg(feature = "hotpath")]
    #[derive(Debug)]
    struct TestRemoteWriter {
        bytes: Arc<Mutex<Vec<u8>>>,
        write_error: Option<io::ErrorKind>,
    }

    #[cfg(feature = "hotpath")]
    impl tokio::io::AsyncWrite for TestRemoteWriter {
        fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            if let Some(kind) = self.write_error {
                return Poll::Ready(Err(io::Error::from(kind)));
            }
            self.bytes
                .lock()
                .expect("test remote transport bytes lock should not be poisoned")
                .extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[cfg(feature = "hotpath")]
    #[async_trait::async_trait]
    impl InternodeDataTransport for TestRemoteDataTransport {
        async fn open_read(&self, _request: ReadStreamRequest) -> Result<FileReader> {
            Ok(Box::new(Cursor::new(self.bytes())))
        }

        async fn open_write(&self, _request: WriteStreamRequest) -> Result<FileWriter> {
            Ok(Box::new(TestRemoteWriter {
                bytes: Arc::clone(&self.bytes),
                write_error: self.write_error,
            }))
        }

        async fn open_walk_dir(&self, _request: WalkDirStreamRequest) -> Result<FileReader> {
            panic!("open_walk_dir must not be used by the raw shard I/O test")
        }

        fn name(&self) -> &'static str {
            "bitrot-test-remote"
        }

        fn capabilities(&self) -> InternodeDataTransportCapabilities {
            InternodeDataTransportCapabilities::tcp_http()
        }
    }

    async fn local_test_disk() -> (DiskStore, tempfile::TempDir) {
        use crate::disk::endpoint::Endpoint;
        use crate::disk::{DiskOption, new_disk};

        let dir = tempfile::tempdir().expect("tempdir should be created");
        let mut endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        endpoint.set_pool_index(0);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(0);
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("local disk should be created");

        (disk, dir)
    }

    #[cfg(feature = "hotpath")]
    async fn remote_test_disk(transport: TestRemoteDataTransport) -> (DiskStore, TestRemoteDataTransport) {
        use crate::disk::endpoint::Endpoint;

        let endpoint = Endpoint {
            url: url::Url::parse("http://remote-node:9000/data/rustfs0").expect("test remote endpoint should parse"),
            is_local: false,
            pool_idx: 0,
            set_idx: 0,
            disk_idx: 0,
        };
        let remote = RemoteDisk::new(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
            Arc::new(transport.clone()),
        )
        .await
        .expect("test remote disk should be created");

        (Arc::new(Disk::Remote(Box::new(remote))), transport)
    }

    async fn round_trip_disk_bitrot(disk: &DiskStore, bucket: &str, path: &str, payload: &[u8], shard_size: usize) -> Vec<u8> {
        disk.make_volume(bucket).await.expect("volume should be created");
        let mut writer = create_bitrot_writer(
            false,
            Some(disk),
            bucket,
            path,
            i64::try_from(payload.len()).expect("test payload length should fit i64"),
            shard_size,
            HashAlgorithm::None,
        )
        .await
        .expect("disk bitrot writer should open the raw shard file");
        for chunk in payload.chunks(shard_size) {
            writer
                .write(chunk)
                .await
                .expect("disk bitrot writer should preserve each shard block");
        }
        writer.shutdown().await.expect("disk bitrot writer should close cleanly");

        let mut reader = create_bitrot_reader(
            None,
            Some(disk),
            bucket,
            path,
            0,
            payload.len(),
            shard_size,
            HashAlgorithm::None,
            false,
            false,
        )
        .await
        .expect("disk bitrot reader should open the raw shard file")
        .expect("disk bitrot reader should exist");
        let mut actual = Vec::with_capacity(payload.len());
        while actual.len() < payload.len() {
            let remaining = payload.len() - actual.len();
            let mut chunk = vec![0; remaining.min(shard_size)];
            let read = reader
                .read(&mut chunk)
                .await
                .expect("disk bitrot reader should return the complete shard body");
            assert!(read > 0, "disk bitrot reader must not end before the expected shard body is complete");
            actual.extend_from_slice(&chunk[..read]);
        }

        actual
    }

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

    #[cfg(feature = "hotpath")]
    #[test]
    fn raw_shard_io_wrappers_report_fixed_labels_and_preserve_bytes() {
        const CHILD_ENV: &str = "RUSTFS_HOTPATH_RAW_SHARD_IO_TEST_CHILD";
        if std::env::var_os(CHILD_ENV).is_none() {
            let status = std::process::Command::new(std::env::current_exe().expect("test executable path should be available"))
                .arg("--exact")
                .arg("io_support::bitrot::tests::raw_shard_io_wrappers_report_fixed_labels_and_preserve_bytes")
                .arg("--nocapture")
                .env(CHILD_ENV, "1")
                .status()
                .expect("isolated HotPath I/O test process should start");
            assert!(status.success(), "isolated HotPath I/O test process should pass");
            return;
        }

        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime should be created")
            .block_on(raw_shard_io_wrappers_report_fixed_labels_and_preserve_bytes_in_isolated_process());
    }

    #[cfg(feature = "hotpath")]
    async fn raw_shard_io_wrappers_report_fixed_labels_and_preserve_bytes_in_isolated_process() {
        use hotpath::{Format, HotpathGuardBuilder, Section};
        use tokio::io::AsyncReadExt;

        let report_dir = tempfile::tempdir().expect("report tempdir should be created");
        let report_path = report_dir.path().join("hotpath-io.json");
        let guard = HotpathGuardBuilder::new("raw_shard_io_test")
            .format(Format::Json)
            .output_path(&report_path)
            .sections(vec![Section::Io])
            .build();

        let (disk, _dir) = local_test_disk().await;
        let bucket = "test-bucket";
        let path = "obj/hotpath-part.1";
        let payload = b"local shard bytes";
        let shard_size = 4;
        let local_read = round_trip_disk_bitrot(&disk, bucket, path, payload, shard_size).await;
        assert_eq!(local_read, payload, "local raw shard I/O instrumentation must not alter stored bytes");

        let fallback_path = "obj/hotpath-mmap-fallback-part.1";
        let fallback_payload = b"mmap fallback shard bytes";
        disk.write_all("test-bucket", fallback_path, Bytes::from_static(fallback_payload))
            .await
            .expect("fallback shard file should be written");
        let mut fallback_reader = FORCE_MMAP_COPY_FAILURE_FOR_TEST
            .scope((), open_disk_reader(&disk, bucket, fallback_path, 0, fallback_payload.len(), true, None))
            .await
            .expect("mmap-copy failure should fall back to a raw shard stream");
        assert!(
            matches!(fallback_reader, ShardReader::Stream(_)),
            "forced mmap-copy failure must use the streaming fallback"
        );
        let mut fallback_read = Vec::new();
        fallback_reader
            .read_to_end(&mut fallback_read)
            .await
            .expect("mmap-copy fallback stream should preserve bytes");
        assert_eq!(fallback_read, fallback_payload);

        let (remote_disk, remote_transport) = remote_test_disk(TestRemoteDataTransport::default()).await;
        let remote_payload = b"remote shard bytes";
        let mut remote_writer = create_bitrot_writer(
            false,
            Some(&remote_disk),
            bucket,
            "obj/hotpath-remote-part.1",
            i64::try_from(remote_payload.len()).expect("remote payload length should fit i64"),
            shard_size,
            HashAlgorithm::None,
        )
        .await
        .expect("remote bitrot writer should use the production raw writer path");
        for chunk in remote_payload.chunks(shard_size) {
            remote_writer
                .write(chunk)
                .await
                .expect("remote bitrot writer should preserve bytes");
        }
        remote_writer
            .shutdown()
            .await
            .expect("remote bitrot writer should close cleanly");
        assert_eq!(remote_transport.bytes(), remote_payload);

        let mut reader = create_bitrot_reader(
            None,
            Some(&remote_disk),
            bucket,
            "obj/hotpath-remote-part.1",
            0,
            remote_payload.len(),
            shard_size,
            HashAlgorithm::None,
            false,
            true,
        )
        .await
        .expect("remote bitrot reader should use the production raw reader path")
        .expect("remote bitrot reader should exist");
        let mut remote_read = Vec::with_capacity(remote_payload.len());
        while remote_read.len() < remote_payload.len() {
            let remaining = remote_payload.len() - remote_read.len();
            let mut chunk = vec![0; remaining.min(shard_size)];
            let read = reader
                .read(&mut chunk)
                .await
                .expect("remote bitrot reader should preserve bytes");
            assert!(read > 0, "remote bitrot reader must not end before the expected shard body is complete");
            remote_read.extend_from_slice(&chunk[..read]);
        }
        assert_eq!(remote_read, remote_payload);

        let (failing_remote_disk, _) = remote_test_disk(TestRemoteDataTransport::with_write_error(io::ErrorKind::Other)).await;
        let mut failing_writer = create_bitrot_writer(
            false,
            Some(&failing_remote_disk),
            bucket,
            "obj/hotpath-remote-write-error-part.1",
            4,
            shard_size,
            HashAlgorithm::None,
        )
        .await
        .expect("failing remote bitrot writer should open before its first write");
        let write_error = failing_writer
            .write(b"fail")
            .await
            .expect_err("raw shard writer failures must remain visible through the bitrot writer");
        assert_eq!(write_error.kind(), io::ErrorKind::Other);

        let (would_block_remote_disk, _) =
            remote_test_disk(TestRemoteDataTransport::with_write_error(io::ErrorKind::WouldBlock)).await;
        let mut would_block_writer = create_bitrot_writer(
            false,
            Some(&would_block_remote_disk),
            bucket,
            "obj/hotpath-remote-write-would-block-part.1",
            4,
            shard_size,
            HashAlgorithm::None,
        )
        .await
        .expect("would-block remote bitrot writer should open before its first write");
        let would_block = would_block_writer
            .write(b"wait")
            .await
            .expect_err("would-block must remain visible to the caller");
        assert_eq!(would_block.kind(), io::ErrorKind::WouldBlock);

        drop(guard);
        let report = std::fs::read_to_string(&report_path).expect("HotPath I/O report should be written");
        let report: serde_json::Value = serde_json::from_str(&report).expect("HotPath I/O report should be valid JSON");
        let entries = report["io"]["data"]
            .as_array()
            .expect("HotPath I/O report should include data rows");
        let io_bytes = |label: &str, direction: &str| {
            let byte_count: u64 = entries
                .iter()
                .filter(|entry| entry["label"].as_str().is_some_and(|entry_label| entry_label == label))
                .filter_map(|entry| entry[direction]["bytes"].as_u64())
                .sum();
            assert!(byte_count > 0, "report must include fixed label {label}");
            byte_count
        };
        let io_errors = |label: &str, direction: &str| {
            entries
                .iter()
                .filter(|entry| entry["label"].as_str().is_some_and(|entry_label| entry_label == label))
                .filter_map(|entry| entry[direction]["errors"].as_u64())
                .sum::<u64>()
        };
        let payload_len = u64::try_from(payload.len()).expect("test payload length should fit u64");
        assert_eq!(
            io_bytes(RAW_SHARD_READ_LOCAL_LABEL, "read"),
            payload_len + u64::try_from(fallback_payload.len()).expect("fallback payload length should fit u64")
        );
        assert_eq!(io_bytes(RAW_SHARD_WRITE_LOCAL_LABEL, "write"), payload_len);
        assert_eq!(
            io_bytes(RAW_SHARD_READ_REMOTE_LABEL, "read"),
            u64::try_from(remote_payload.len()).expect("remote payload length should fit u64")
        );
        assert_eq!(
            io_bytes(RAW_SHARD_WRITE_REMOTE_LABEL, "write"),
            u64::try_from(remote_payload.len()).expect("remote payload length should fit u64")
        );
        assert_eq!(
            io_errors(RAW_SHARD_WRITE_REMOTE_LABEL, "write"),
            1,
            "the wrapper must record the real remote writer failure without changing it, while excluding WouldBlock"
        );
        for label in [
            RAW_SHARD_READ_LOCAL_LABEL,
            RAW_SHARD_READ_REMOTE_LABEL,
            RAW_SHARD_WRITE_LOCAL_LABEL,
            RAW_SHARD_WRITE_REMOTE_LABEL,
        ] {
            assert!(
                !label.contains(['/', ':', '?', '@']),
                "raw shard I/O labels must not carry a path, host, query, or credential delimiter: {label}"
            );
        }
    }

    #[test]
    fn object_mmap_read_max_length_defaults_and_env_override() {
        temp_env::with_var(ENV_OBJECT_MMAP_READ_MAX_LENGTH, None::<&str>, || {
            assert_eq!(object_mmap_read_max_length(), DEFAULT_OBJECT_MMAP_READ_MAX_LENGTH);
        });
        temp_env::with_var(ENV_OBJECT_MMAP_READ_MAX_LENGTH, Some("1024"), || {
            assert_eq!(object_mmap_read_max_length(), 1024);
        });
        temp_env::with_var(ENV_OBJECT_MMAP_READ_MAX_LENGTH, Some("0"), || {
            assert_eq!(object_mmap_read_max_length(), 0);
        });
    }

    // rustfs#5123: whole-part shard reads of large single-part objects must not
    // be materialized in memory by the mmap-copy path; over-cap reads stream.
    #[tokio::test]
    async fn open_disk_reader_streams_when_length_exceeds_mmap_cap() {
        use tokio::io::AsyncReadExt;

        let (disk, _dir) = local_test_disk().await;

        let payload = vec![7u8; 4096];
        disk.make_volume("test-bucket").await.expect("volume should be created");
        disk.write_all("test-bucket", "obj/part.1", Bytes::from(payload.clone()))
            .await
            .expect("shard file should be written");

        temp_env::async_with_vars([(ENV_OBJECT_MMAP_READ_MAX_LENGTH, Some("1024"))], async {
            let over_cap = open_disk_reader(&disk, "test-bucket", "obj/part.1", 0, payload.len(), true, None)
                .await
                .expect("over-cap read should open");
            assert!(
                matches!(over_cap, ShardReader::Stream(_)),
                "read longer than the mmap cap must take the streaming path"
            );
            let mut over_cap = over_cap;
            let mut streamed = Vec::new();
            over_cap
                .read_to_end(&mut streamed)
                .await
                .expect("streaming fallback should read the range");
            assert_eq!(streamed, payload, "streaming fallback must return the same bytes");

            let under_cap = open_disk_reader(&disk, "test-bucket", "obj/part.1", 0, 512, true, None)
                .await
                .expect("under-cap read should open");
            assert!(
                matches!(under_cap, ShardReader::InMemory(_)),
                "read within the mmap cap keeps the mmap-copy fast path"
            );

            let at_cap = open_disk_reader(&disk, "test-bucket", "obj/part.1", 0, 1024, true, None)
                .await
                .expect("at-cap read should open");
            assert!(
                matches!(at_cap, ShardReader::InMemory(_)),
                "read of exactly the mmap cap keeps the mmap-copy fast path"
            );
        })
        .await;

        // Cap of 0 disables mmap-copy for every non-empty read.
        temp_env::async_with_vars([(ENV_OBJECT_MMAP_READ_MAX_LENGTH, Some("0"))], async {
            let disabled = open_disk_reader(&disk, "test-bucket", "obj/part.1", 0, 512, true, None)
                .await
                .expect("read with cap 0 should open");
            assert!(
                matches!(disabled, ShardReader::Stream(_)),
                "cap 0 must route every non-empty read to the streaming path"
            );
        })
        .await;
    }

    #[tokio::test]
    async fn disk_bitrot_reader_and_writer_preserve_full_shard_body() {
        let (disk, _dir) = local_test_disk().await;
        let bucket = "test-bucket";
        let path = "obj/wrapped-part.1";
        let payload = b"wrapped shard body";
        let actual = round_trip_disk_bitrot(&disk, bucket, path, payload, 4).await;

        assert_eq!(actual, payload, "raw shard I/O instrumentation must not alter stored bytes");
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

        // Test with mmap-copy enabled (should work the same for inline data)
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
            i64::try_from(payload.len()).expect("test payload length should fit i64"),
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
    async fn test_create_bitrot_reader_from_bytes_preserves_inline_body() {
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

        let inline_data = Bytes::from(writer.into_inline_data().expect("inline buffer"));
        let mut reader = create_bitrot_reader_from_bytes(
            Some(inline_data),
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
        .expect("create reader from bytes")
        .expect("reader");

        let mut out = [0u8; 4];
        let n = reader.read(&mut out).await.expect("read second shard from bytes");

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

    /// Merge gate for backlog#923: engaging a parity shard at stripe `k`
    /// converts `k` stripes into an encoded byte offset via the
    /// `bitrot_encoded_range` geometry. Cover both on-disk formats: the
    /// streaming checksum layout (32-byte hash per block) and the
    /// no-per-block-hash layout (`hash_size == 0`), where the mapping must
    /// degrade to the identity `k * shard_size`.
    #[tokio::test]
    async fn test_deferred_stripe_handle_aligns_reader_to_requested_stripe() {
        let shard_size = 4;
        let payload = b"aaaabbbbccccdddd"; // 4 full bitrot blocks

        for algo in [HashAlgorithm::HighwayHash256S, HashAlgorithm::None] {
            let mut writer =
                create_bitrot_writer(true, None, "test-volume", "test-path", payload.len() as i64, shard_size, algo.clone())
                    .await
                    .expect("inline bitrot writer");
            for chunk in payload.chunks(shard_size) {
                writer.write(chunk).await.expect("write chunk");
            }
            let inline_data = writer.into_inline_data().expect("inline buffer");

            for stripe in 0..payload.len() / shard_size {
                let (mut reader, handle) = create_deferred_bitrot_reader_with_stripe_handle(
                    Some(inline_data.clone().into()),
                    None,
                    "test-bucket",
                    "test-path",
                    0,
                    payload.len(),
                    shard_size,
                    algo.clone(),
                    false,
                    false,
                );

                assert!(
                    handle.advance_stripes(stripe),
                    "pending deferred reader must accept a stripe advance (algo={algo:?}, stripe={stripe})"
                );

                let mut out = [0u8; 4];
                let n = reader.read(&mut out).await.expect("read shard block after stripe advance");
                assert_eq!(n, shard_size);
                assert_eq!(
                    &out[..n],
                    &payload[stripe * shard_size..(stripe + 1) * shard_size],
                    "advanced reader must return exactly stripe {stripe} (algo={algo:?})"
                );
            }
        }
    }

    /// Bitrot verification must stay active for a parity shard engaged
    /// mid-object: after `advance_stripes(k)` the reader verifies stripe `k`'s
    /// block against stripe `k`'s stored hash.
    #[tokio::test]
    async fn test_deferred_stripe_handle_preserves_bitrot_verification_after_advance() {
        let shard_size = 4;
        let algo = HashAlgorithm::HighwayHash256S;
        let payload = b"aaaabbbbccccdddd";

        let mut writer =
            create_bitrot_writer(true, None, "test-volume", "test-path", payload.len() as i64, shard_size, algo.clone())
                .await
                .expect("inline bitrot writer");
        for chunk in payload.chunks(shard_size) {
            writer.write(chunk).await.expect("write chunk");
        }
        let mut inline_data = writer.into_inline_data().expect("inline buffer");

        // Corrupt one payload byte of block 2 (blocks are hash + data).
        let block_len = algo.size() + shard_size;
        inline_data[2 * block_len + algo.size()] ^= 0x01;

        let (mut reader, handle) = create_deferred_bitrot_reader_with_stripe_handle(
            Some(inline_data.into()),
            None,
            "test-bucket",
            "test-path",
            0,
            payload.len(),
            shard_size,
            algo,
            false,
            false,
        );
        assert!(handle.advance_stripes(2));

        let mut out = [0u8; 4];
        let err = reader
            .read(&mut out)
            .await
            .expect_err("bitrot mismatch at the advanced stripe must fail the read");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// A reader that has already been opened has an unknown stream position
    /// from the handle's point of view; the advance must be refused so the
    /// caller retires the reader instead of engaging it out of alignment.
    #[tokio::test]
    async fn test_deferred_stripe_handle_rejects_advance_after_open() {
        let shard_size = 4;
        let algo = HashAlgorithm::HighwayHash256S;
        let payload = b"aaaabbbb";

        let mut writer =
            create_bitrot_writer(true, None, "test-volume", "test-path", payload.len() as i64, shard_size, algo.clone())
                .await
                .expect("inline bitrot writer");
        for chunk in payload.chunks(shard_size) {
            writer.write(chunk).await.expect("write chunk");
        }
        let inline_data = writer.into_inline_data().expect("inline buffer");

        let (mut reader, handle) = create_deferred_bitrot_reader_with_stripe_handle(
            Some(inline_data.into()),
            None,
            "test-bucket",
            "test-path",
            0,
            payload.len(),
            shard_size,
            algo,
            false,
            false,
        );

        let mut out = [0u8; 4];
        reader.read(&mut out).await.expect("first read opens the deferred source");

        assert!(!handle.advance_stripes(1), "an opened deferred reader must reject stripe advances");
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

    #[tokio::test]
    async fn shard_reader_chunked_path_verifies_fragmented_remote_block() {
        const SHARD_SIZE: usize = 1024;
        let algo = HashAlgorithm::HighwayHash256S;
        let data = vec![42u8; SHARD_SIZE];
        let mut encoded = Vec::new();
        crate::erasure::coding::BitrotWriter::new(&mut encoded, SHARD_SIZE, algo.clone())
            .write(&data)
            .await
            .expect("test shard should encode");

        let source = TestChunkReader::new(Bytes::from(encoded), &[3, 7, 17, 31]);
        let mut reader = BitrotReader::new(ShardReader::Chunked(Box::new(source)), SHARD_SIZE, algo, false);
        let mut output = Vec::with_capacity(SHARD_SIZE);
        reader
            .read_appending(&mut output, SHARD_SIZE)
            .await
            .expect("fragmented remote shard should verify");

        assert_eq!(output, data);
    }

    #[tokio::test]
    async fn shard_reader_chunked_path_handles_more_than_one_poll_budget() {
        const SHARD_SIZE: usize = 1024;
        let algo = HashAlgorithm::HighwayHash256S;
        let data = vec![42u8; SHARD_SIZE];
        let mut encoded = Vec::new();
        crate::erasure::coding::BitrotWriter::new(&mut encoded, SHARD_SIZE, algo.clone())
            .write(&data)
            .await
            .expect("test shard should encode");

        let fragment_sizes = vec![1; encoded.len()];
        let source = TestChunkReader::new(Bytes::from(encoded), &fragment_sizes);
        let mut reader = BitrotReader::new(ShardReader::Chunked(Box::new(source)), SHARD_SIZE, algo, false);
        let mut output = Vec::with_capacity(SHARD_SIZE);
        reader
            .read_appending(&mut output, SHARD_SIZE)
            .await
            .expect("fragmented remote shard should verify after multiple polls");

        assert_eq!(output, data);
    }
}
