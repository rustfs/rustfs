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

use crate::disk::error::Error;
use crate::disk::error_reduce::{
    OBJECT_OP_IGNORED_ERRS, WriteQuorumFailureSummary, build_write_quorum_failure_summary, reduce_write_quorum_errs,
};
use crate::erasure::coding::BitrotWriterWrapper;
use crate::erasure::coding::Erasure;
use crate::runtime::sources as runtime_sources;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::sync::Arc;
use std::time::Instant;
use std::vec;
use tokio::io::AsyncRead;
use tokio::runtime::RuntimeFlavor;
use tokio::sync::mpsc;
use tracing::error;

const ENV_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES: &str = "RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES";
const ENV_RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS: &str = "RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS";
const ENV_RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST: &str = "RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST";
const DEFAULT_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BLOCKS: usize = 32;
const DEFAULT_RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS: usize = 4;
const DEFAULT_RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST: bool = false;

/// Cached value of `RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES` env var.
/// Read once at first use via `OnceLock` to avoid per-encode syscall.
static CACHED_MAX_INFLIGHT_BYTES: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
static CACHED_BATCH_BLOCKS: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
static CACHED_BYTESMUT_INGEST: std::sync::OnceLock<bool> = std::sync::OnceLock::new();

#[inline(always)]
fn stage_timer_if_enabled() -> Option<Instant> {
    rustfs_io_metrics::put_stage_metrics_enabled().then(Instant::now)
}

#[inline(always)]
fn record_internal_stage_if_enabled(stage: &'static str, started_at: Option<Instant>) {
    if let Some(started_at) = started_at {
        rustfs_io_metrics::record_stage_duration(stage, started_at.elapsed().as_secs_f64() * 1000.0);
    }
}

fn encode_channel_capacity(expanded_block_bytes: usize, max_inflight_bytes: usize) -> usize {
    if expanded_block_bytes == 0 {
        return 1;
    }

    max_inflight_bytes
        .saturating_div(expanded_block_bytes)
        .clamp(1, DEFAULT_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BLOCKS)
}

fn encode_batch_block_count() -> usize {
    *CACHED_BATCH_BLOCKS.get_or_init(|| {
        rustfs_utils::get_env_usize(ENV_RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS, DEFAULT_RUSTFS_ERASURE_ENCODE_BATCH_BLOCKS)
            .clamp(1, DEFAULT_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BLOCKS)
    })
}

fn erasure_encode_max_inflight_bytes() -> usize {
    *CACHED_MAX_INFLIGHT_BYTES.get_or_init(|| {
        rustfs_utils::get_env_usize(
            ENV_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES,
            DEFAULT_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES,
        )
    })
}

fn use_bytesmut_ingest() -> bool {
    *CACHED_BYTESMUT_INGEST.get_or_init(|| {
        rustfs_utils::get_env_bool(ENV_RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST, DEFAULT_RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST)
    })
}
/// Read up to `limit` bytes into `buf`'s uninitialized spare capacity, appending after its
/// current length, and distinguish a clean EOF from a short read.
///
/// Mirrors `rustfs_utils::read_full_or_eof` semantics: returns `Ok(None)` when EOF is reached
/// before any byte is read, `Ok(Some(n))` once at least one byte is read, preserves
/// `InvalidData` errors (e.g. checksum mismatches) raised after a partial fill, and wraps other
/// partial-fill errors as `UnexpectedEof`. Unlike `resize` followed by a slice read, the spare
/// capacity is never zero-filled first, so full-block ingest skips a per-block memset.
async fn read_full_buf_or_eof<R>(reader: &mut R, buf: &mut BytesMut, limit: usize) -> std::io::Result<Option<usize>>
where
    R: AsyncRead + Unpin,
{
    use bytes::BufMut as _;
    use tokio::io::AsyncReadExt as _;

    debug_assert!(buf.is_empty(), "block ingest buffer must start empty");
    debug_assert!(limit > 0, "limit must be non-zero (block_size is validated upstream)");
    let mut total = 0;
    while total < limit {
        let mut limited = (&mut *buf).limit(limit - total);
        let n = match reader.read_buf(&mut limited).await {
            Ok(n) => n,
            Err(e) => {
                if total == 0 {
                    return Err(e);
                }
                // Preserve InvalidData (e.g. checksum mismatch) instead of wrapping it as
                // UnexpectedEof, so proper error handling can occur upstream.
                if e.kind() == std::io::ErrorKind::InvalidData {
                    return Err(e);
                }
                return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, e));
            }
        };
        if n == 0 {
            break;
        }
        total += n;
    }
    if total == 0 { Ok(None) } else { Ok(Some(total)) }
}

fn queued_block_bytes(block: &[Bytes]) -> usize {
    block.iter().map(Bytes::len).sum()
}

async fn drain_queued_inflight_bytes(rx: &mut mpsc::Receiver<Vec<Bytes>>) {
    while let Some(block) = rx.recv().await {
        rustfs_io_metrics::remove_ec_encode_inflight_bytes(queued_block_bytes(&block));
    }
}

fn queued_batch_bytes(batch: &[Vec<Bytes>]) -> usize {
    batch.iter().map(|block| queued_block_bytes(block)).sum()
}

async fn drain_queued_batched_inflight_bytes(rx: &mut mpsc::Receiver<Vec<Vec<Bytes>>>) {
    while let Some(batch) = rx.recv().await {
        rustfs_io_metrics::remove_ec_encode_inflight_bytes(queued_batch_bytes(&batch));
    }
}

fn dominant_error_summary_label(summary: &WriteQuorumFailureSummary) -> &'static str {
    summary.dominant_error_label
}

fn format_write_quorum_failure(summary: &WriteQuorumFailureSummary) -> String {
    format!(
        "erasure write quorum (required={}, achieved={}, failed={}, total={}, offline-disks={}/{}, retryable-failures={}, dominant-error={})",
        summary.required,
        summary.achieved,
        summary.failed,
        summary.total,
        summary.offline_disks,
        summary.total,
        summary.retryable_failures,
        dominant_error_summary_label(summary)
    )
}

fn quorum_dominant_error_metric_label(summary: &WriteQuorumFailureSummary) -> &'static str {
    dominant_error_summary_label(summary)
}

/// Progress deadlines that bound how long shard writers may stall.
///
/// A single shard write (or writer shutdown) is a unit of forward progress: for
/// a remote shard the underlying `HttpWriter` buffers the bytes and hands them
/// to a background HTTP task, so a peer that accepts the connection but never
/// drains the body eventually wedges the write once the bounded buffers fill.
/// Without a deadline that stalled writer is awaited forever and pins an
/// otherwise-healthy write quorum (rustfs/backlog#1319).
///
/// * `stall_timeout` is re-armed on every shard write, so it bounds a stall, not
///   the total transfer time of a large object — a slow-but-honest writer that
///   keeps completing shards is never killed.
/// * `absolute_cap` is an optional administrator backstop: the shard writers for
///   one object are engaged for at most this long in aggregate. It defends
///   against a "slow-drip" peer that produces just enough progress to reset the
///   per-shard timeout on every block while never converging. Disabled by
///   default so a legitimate large upload over a slow link is not killed on
///   total time alone.
#[derive(Debug, Clone, Copy)]
pub(crate) struct WriteProgressPolicy {
    stall_timeout: Option<std::time::Duration>,
    absolute_cap: Option<std::time::Duration>,
}

impl WriteProgressPolicy {
    /// Build a policy, mapping a zero duration to "disabled" for both knobs.
    pub(crate) fn new(stall_timeout: std::time::Duration, absolute_cap: std::time::Duration) -> Self {
        Self {
            stall_timeout: (!stall_timeout.is_zero()).then_some(stall_timeout),
            absolute_cap: (!absolute_cap.is_zero()).then_some(absolute_cap),
        }
    }
}

impl Default for WriteProgressPolicy {
    fn default() -> Self {
        Self::new(
            crate::disk::disk_store::get_object_disk_write_stall_timeout(),
            crate::disk::disk_store::get_object_disk_write_absolute_cap(),
        )
    }
}

pub(crate) struct MultiWriter<'a> {
    writers: &'a mut [Option<BitrotWriterWrapper>],
    write_quorum: usize,
    errs: Vec<Option<Error>>,
    policy: WriteProgressPolicy,
    /// Absolute deadline for the whole object, armed lazily on the first
    /// progress-bounded operation when `policy.absolute_cap` is set.
    absolute_deadline: Option<tokio::time::Instant>,
}

impl<'a> MultiWriter<'a> {
    pub fn new(writers: &'a mut [Option<BitrotWriterWrapper>], write_quorum: usize) -> Self {
        Self::with_policy(writers, write_quorum, WriteProgressPolicy::default())
    }

    pub fn with_policy(writers: &'a mut [Option<BitrotWriterWrapper>], write_quorum: usize, policy: WriteProgressPolicy) -> Self {
        let length = writers.len();
        MultiWriter {
            writers,
            write_quorum,
            errs: vec![None; length],
            policy,
            absolute_deadline: None,
        }
    }

    /// Effective budget for one shard operation: the smaller of the per-shard
    /// stall timeout and the time remaining until the object's absolute cap.
    /// Returns `None` when neither deadline is configured (wait indefinitely).
    fn next_progress_budget(&mut self) -> Option<std::time::Duration> {
        if let Some(cap) = self.policy.absolute_cap {
            let deadline = *self
                .absolute_deadline
                .get_or_insert_with(|| tokio::time::Instant::now() + cap);
            let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
            match self.policy.stall_timeout {
                Some(stall) => Some(stall.min(remaining)),
                None => Some(remaining),
            }
        } else {
            self.policy.stall_timeout
        }
    }

    async fn write_shard(writer_opt: &mut Option<BitrotWriterWrapper>, err: &mut Option<Error>, shard: &Bytes) {
        match writer_opt {
            Some(writer) => {
                match writer.write(shard).await {
                    Ok(n) => {
                        if n < shard.len() {
                            *err = Some(Error::ShortWrite);
                            *writer_opt = None; // Mark as failed
                        } else {
                            *err = None;
                        }
                    }
                    Err(e) => {
                        *err = Some(Error::from(e));
                        *writer_opt = None; // Mark as failed so the caller drops this disk before commit
                    }
                }
            }
            None => {
                *err = Some(Error::DiskNotFound);
            }
        }
    }

    pub async fn write(&mut self, data: Vec<Bytes>) -> std::io::Result<()> {
        assert_eq!(data.len(), self.writers.len());

        let budget = self.next_progress_budget();
        {
            let mut futures = FuturesUnordered::new();
            for ((writer_opt, err), shard) in self.writers.iter_mut().zip(self.errs.iter_mut()).zip(data.iter()) {
                if err.is_some() {
                    continue; // Skip if we already have an error for this writer
                }
                // A shard write that makes no forward progress within `budget` is
                // failed and its disk dropped, so a stalled peer cannot pin an
                // otherwise-healthy write quorum (rustfs/backlog#1319). `budget`
                // is recomputed per block, so it bounds a stall — not the total
                // transfer time — and a slow-but-honest writer is never killed.
                futures.push(async move {
                    match budget {
                        Some(budget) => match tokio::time::timeout(budget, Self::write_shard(writer_opt, err, shard)).await {
                            Ok(()) => {}
                            Err(_elapsed) => {
                                *err = Some(Error::Timeout);
                                *writer_opt = None;
                            }
                        },
                        None => Self::write_shard(writer_opt, err, shard).await,
                    }
                });
            }
            while let Some(()) = futures.next().await {}
        }

        let nil_count = self.errs.iter().filter(|&e| e.is_none()).count();
        if nil_count >= self.write_quorum {
            return Ok(());
        }

        let write_err =
            reduce_write_quorum_errs(&self.errs, OBJECT_OP_IGNORED_ERRS, self.write_quorum).unwrap_or(Error::ErasureWriteQuorum);
        let summary = build_write_quorum_failure_summary(&self.errs, OBJECT_OP_IGNORED_ERRS, self.write_quorum);
        let summary_text = format_write_quorum_failure(&summary);
        runtime_sources::record_erasure_write_quorum_failure("write", quorum_dominant_error_metric_label(&summary));
        error!(
            required = summary.required,
            achieved = summary.achieved,
            failed = summary.failed,
            total = summary.total,
            offline_disks = summary.offline_disks,
            retryable_failures = summary.retryable_failures,
            dominant_error = summary.dominant_error_label,
            returned_error = %write_err,
            errs = ?self.errs,
            "Erasure encode write quorum unavailable: {summary_text}"
        );
        Err(std::io::Error::other(format!("Failed to write data: {summary_text}")))
    }

    async fn shutdown_writer(writer_opt: &mut Option<BitrotWriterWrapper>, err: &mut Option<Error>) {
        match writer_opt {
            Some(writer) => match writer.shutdown().await {
                Ok(()) => {
                    *err = None;
                }
                Err(e) => {
                    *err = Some(Error::from(e));
                    *writer_opt = None;
                }
            },
            None => {
                *err = Some(Error::DiskNotFound);
            }
        }
    }

    pub async fn shutdown(&mut self) -> std::io::Result<()> {
        crate::hp_guard!("MultiWriter::shutdown");
        let budget = self.next_progress_budget();
        {
            let mut futures = FuturesUnordered::new();
            for (writer_opt, err) in self.writers.iter_mut().zip(self.errs.iter_mut()) {
                if err.is_some() {
                    continue;
                }
                // Shutdown flushes the tail and waits for the remote to finish the
                // HTTP request; a black-hole peer that never responds would hang
                // here forever for a small object whose bytes were fully buffered
                // (so `write` never blocked). Bound it with the same progress
                // budget and drop the stalled writer before the quorum check.
                futures.push(async move {
                    match budget {
                        Some(budget) => match tokio::time::timeout(budget, Self::shutdown_writer(writer_opt, err)).await {
                            Ok(()) => {}
                            Err(_elapsed) => {
                                *err = Some(Error::Timeout);
                                *writer_opt = None;
                            }
                        },
                        None => Self::shutdown_writer(writer_opt, err).await,
                    }
                });
            }
            while let Some(()) = futures.next().await {}
        }

        let nil_count = self.errs.iter().filter(|&e| e.is_none()).count();
        if nil_count >= self.write_quorum {
            return Ok(());
        }

        let write_err =
            reduce_write_quorum_errs(&self.errs, OBJECT_OP_IGNORED_ERRS, self.write_quorum).unwrap_or(Error::ErasureWriteQuorum);
        let summary = build_write_quorum_failure_summary(&self.errs, OBJECT_OP_IGNORED_ERRS, self.write_quorum);
        let summary_text = format_write_quorum_failure(&summary);
        runtime_sources::record_erasure_write_quorum_failure("shutdown", quorum_dominant_error_metric_label(&summary));
        error!(
            required = summary.required,
            achieved = summary.achieved,
            failed = summary.failed,
            total = summary.total,
            offline_disks = summary.offline_disks,
            retryable_failures = summary.retryable_failures,
            dominant_error = summary.dominant_error_label,
            returned_error = %write_err,
            errs = ?self.errs,
            "Erasure encode shutdown quorum unavailable: {summary_text}"
        );
        Err(std::io::Error::other(format!("Failed to shutdown writers: {summary_text}")))
    }
}

impl Erasure {
    async fn encode_block(self: Arc<Self>, encode_buf: Vec<u8>, len: usize) -> std::io::Result<(Vec<Bytes>, Vec<u8>)> {
        let encode_stage_start = stage_timer_if_enabled();
        let encode_once = move || {
            let res = self.encode_data(&encode_buf[..len]);
            (res, encode_buf)
        };

        let (res, returned_buf) = match tokio::runtime::Handle::current().runtime_flavor() {
            // EC encode is a short CPU burst (~110µs per 1MiB block, p99 ~542µs).
            // On the multi-threaded runtime block_in_place parked the worker and
            // churned the scheduler for a cost comparable to the encode itself
            // (rustfs/backlog#932); at this duration a direct inline call is
            // cheaper and equally correct. CurrentThread (and any other flavor)
            // keep spawn_blocking so the sole executor thread is never blocked
            // and block_in_place's multi-thread-only requirement is respected.
            RuntimeFlavor::MultiThread => encode_once(),
            RuntimeFlavor::CurrentThread => tokio::task::spawn_blocking(encode_once)
                .await
                .map_err(|err| std::io::Error::other(format!("EC encode task failed: {err}")))?,
            _ => tokio::task::spawn_blocking(encode_once)
                .await
                .map_err(|err| std::io::Error::other(format!("EC encode task failed: {err}")))?,
        };

        record_internal_stage_if_enabled("erasure_encode_cpu", encode_stage_start);
        Ok((res?, returned_buf))
    }

    async fn encode_block_bytes_mut(self: Arc<Self>, encode_buf: BytesMut, len: usize) -> std::io::Result<Vec<Bytes>> {
        let encode_stage_start = stage_timer_if_enabled();
        let encode_once = move || self.encode_data_bytes_mut(encode_buf, len);

        let res = match tokio::runtime::Handle::current().runtime_flavor() {
            // Same rationale as encode_block: inline the short EC burst on the
            // multi-threaded runtime instead of parking a worker via
            // block_in_place; keep spawn_blocking on single-threaded flavors.
            RuntimeFlavor::MultiThread => encode_once(),
            RuntimeFlavor::CurrentThread => tokio::task::spawn_blocking(encode_once)
                .await
                .map_err(|err| std::io::Error::other(format!("EC encode task failed: {err}")))?,
            _ => tokio::task::spawn_blocking(encode_once)
                .await
                .map_err(|err| std::io::Error::other(format!("EC encode task failed: {err}")))?,
        };

        record_internal_stage_if_enabled("erasure_encode_cpu", encode_stage_start);
        res
    }

    async fn encode_small_direct<R>(
        self: Arc<Self>,
        mut reader: R,
        writers: &mut [Option<BitrotWriterWrapper>],
        quorum: usize,
        require_single_block: bool,
    ) -> std::io::Result<(R, usize)>
    where
        R: AsyncRead + Send + Sync + Unpin,
    {
        use tokio::io::AsyncReadExt;

        let mut buf = Vec::with_capacity(self.block_size);
        let total = if require_single_block {
            let read_limit = self
                .block_size
                .checked_add(1)
                .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "erasure block_size is too large"))?;
            let read_limit = u64::try_from(read_limit)
                .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidInput, "erasure block_size exceeds u64"))?;
            (&mut reader).take(read_limit).read_to_end(&mut buf).await?
        } else {
            reader.read_to_end(&mut buf).await?
        };

        if total == 0 {
            return Ok((reader, 0));
        }

        if require_single_block && total > self.block_size {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "single-block non-inline fast path expects total <= block_size",
            ));
        }

        let shards = self.encode_data_owned(buf)?;
        let mut mw = MultiWriter::new(writers, quorum);
        mw.write(shards).await?;
        mw.shutdown().await?;
        Ok((reader, total))
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn encode<R>(
        self: Arc<Self>,
        reader: R,
        writers: &mut [Option<BitrotWriterWrapper>],
        quorum: usize,
    ) -> std::io::Result<(R, usize)>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        let use_bytesmut_ingest = use_bytesmut_ingest();
        self.encode_with_ingest_mode(reader, writers, quorum, use_bytesmut_ingest)
            .await
    }

    /// Streaming encode with an explicit ingest-buffer strategy. `encode` resolves the
    /// strategy from `RUSTFS_ERASURE_ENCODE_BYTESMUT_INGEST` once per process via
    /// `OnceLock`, so the cached value latches on first use; tests must call this
    /// directly with an explicit flag (not `encode` + env vars) to exercise both paths.
    async fn encode_with_ingest_mode<R>(
        self: Arc<Self>,
        mut reader: R,
        writers: &mut [Option<BitrotWriterWrapper>],
        quorum: usize,
        use_bytesmut_ingest: bool,
    ) -> std::io::Result<(R, usize)>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        if self.block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "erasure block_size must be non-zero",
            ));
        }

        // Bound queued encoded blocks by memory budget to avoid per-request spikes.
        let expanded_block_bytes = self.shard_size().saturating_mul(self.total_shard_count());
        let max_inflight_bytes = erasure_encode_max_inflight_bytes();
        let inflight_blocks = encode_channel_capacity(expanded_block_bytes, max_inflight_bytes);
        let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(inflight_blocks);

        let task = tokio::spawn(async move {
            let block_size = self.block_size;
            let mut total = 0;
            if use_bytesmut_ingest {
                // HP-10 (rustfs/backlog#931): reserve the EC-expanded block size up front.
                // Both shard-size formulas are monotone in data_len, so the resize to
                // `need_total_size` inside `encode_data_bytes_mut` always stays within this
                // capacity and never reallocates. Reading into uninitialized spare capacity
                // (instead of resize + slice read) also skips zero-filling each fresh buffer.
                let ingest_capacity = expanded_block_bytes.max(block_size);
                let mut buf = BytesMut::with_capacity(ingest_capacity);
                loop {
                    match read_full_buf_or_eof(&mut reader, &mut buf, block_size).await {
                        Ok(Some(n)) => {
                            debug_assert!(n > 0, "non-zero block_size prevents zero-length reads");
                            debug_assert_eq!(buf.len(), n, "ingest buffer length must equal bytes read");
                            total += n;
                            let encode_buf = buf;
                            let res = self.clone().encode_block_bytes_mut(encode_buf, n).await?;
                            buf = BytesMut::with_capacity(ingest_capacity);
                            let queued_bytes = queued_block_bytes(&res);
                            rustfs_io_metrics::add_ec_encode_inflight_bytes(queued_bytes);
                            let send_wait_stage_start = stage_timer_if_enabled();
                            if let Err(err) = tx.send(res).await {
                                rustfs_io_metrics::remove_ec_encode_inflight_bytes(queued_bytes);
                                return Err(std::io::Error::other(format!("Failed to send encoded data : {err}")));
                            }
                            record_internal_stage_if_enabled("erasure_encode_send_wait", send_wait_stage_start);
                        }
                        Ok(None) => break,
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                            if let Some(inner) = e.get_ref()
                                && rustfs_rio::is_checksum_mismatch(inner)
                            {
                                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                            }
                            return Err(e);
                        }
                        Err(e) => return Err(e),
                    }
                }
            } else {
                let mut buf = vec![0u8; block_size];
                loop {
                    match rustfs_utils::read_full_or_eof(&mut reader, &mut buf).await {
                        Ok(Some(n)) => {
                            debug_assert!(n > 0, "non-zero block_size prevents zero-length reads");
                            total += n;
                            let encode_buf = std::mem::take(&mut buf);
                            let (res, returned_buf) = self.clone().encode_block(encode_buf, n).await?;
                            buf = returned_buf;
                            let queued_bytes = queued_block_bytes(&res);
                            rustfs_io_metrics::add_ec_encode_inflight_bytes(queued_bytes);
                            let send_wait_stage_start = stage_timer_if_enabled();
                            if let Err(err) = tx.send(res).await {
                                rustfs_io_metrics::remove_ec_encode_inflight_bytes(queued_bytes);
                                return Err(std::io::Error::other(format!("Failed to send encoded data : {err}")));
                            }
                            record_internal_stage_if_enabled("erasure_encode_send_wait", send_wait_stage_start);
                        }
                        Ok(None) => {
                            break;
                        }
                        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                            // Check if the inner error is a checksum mismatch - if so, propagate it
                            if let Some(inner) = e.get_ref()
                                && rustfs_rio::is_checksum_mismatch(inner)
                            {
                                return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                            }
                            return Err(e);
                        }
                        Err(e) => {
                            return Err(e);
                        }
                    }
                }
            }

            Ok((reader, total))
        });

        let mut writers = MultiWriter::new(writers, quorum);

        let mut write_err = None;

        loop {
            let recv_wait_stage_start = stage_timer_if_enabled();
            let Some(block) = rx.recv().await else {
                break;
            };
            record_internal_stage_if_enabled("erasure_encode_recv_wait", recv_wait_stage_start);
            if block.is_empty() {
                break;
            }
            let queued_bytes = queued_block_bytes(&block);
            rustfs_io_metrics::remove_ec_encode_inflight_bytes(queued_bytes);
            let write_stage_start = stage_timer_if_enabled();
            if let Err(err) = writers.write(block).await {
                write_err = Some(err);
                break;
            }
            record_internal_stage_if_enabled("erasure_encode_write", write_stage_start);
        }

        if let Some(err) = write_err {
            task.abort();
            let _ = task.await;
            drain_queued_inflight_bytes(&mut rx).await;
            let shutdown_stage_start = stage_timer_if_enabled();
            if let Err(shutdown_err) = writers.shutdown().await {
                error!("failed to shutdown erasure writers after write error: {:?}", shutdown_err);
            }
            record_internal_stage_if_enabled("erasure_encode_shutdown", shutdown_stage_start);
            return Err(err);
        }

        let (reader, total) = task.await??;
        let shutdown_stage_start = stage_timer_if_enabled();
        writers.shutdown().await?;
        record_internal_stage_if_enabled("erasure_encode_shutdown", shutdown_stage_start);
        Ok((reader, total))
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn encode_batched<R>(
        self: Arc<Self>,
        mut reader: R,
        writers: &mut [Option<BitrotWriterWrapper>],
        quorum: usize,
    ) -> std::io::Result<(R, usize)>
    where
        R: AsyncRead + Send + Sync + Unpin + 'static,
    {
        if self.block_size == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "erasure block_size must be non-zero",
            ));
        }

        let expanded_block_bytes = self.shard_size().saturating_mul(self.total_shard_count());
        let max_inflight_bytes = erasure_encode_max_inflight_bytes();
        let inflight_blocks = encode_channel_capacity(expanded_block_bytes, max_inflight_bytes);
        let batch_blocks = encode_batch_block_count().min(inflight_blocks);
        let channel_capacity = inflight_blocks.div_ceil(batch_blocks).max(1);
        let (tx, mut rx) = mpsc::channel::<Vec<Vec<Bytes>>>(channel_capacity);

        let task = tokio::spawn(async move {
            let block_size = self.block_size;
            let mut total = 0;
            let mut buf = vec![0u8; block_size];
            let mut pending_batch = Vec::with_capacity(batch_blocks);
            let mut pending_batch_bytes = 0usize;
            loop {
                match rustfs_utils::read_full_or_eof(&mut reader, &mut buf).await {
                    Ok(Some(n)) => {
                        debug_assert!(n > 0, "non-zero block_size prevents zero-length reads");
                        total += n;
                        let encode_buf = std::mem::take(&mut buf);
                        let (res, returned_buf) = self.clone().encode_block(encode_buf, n).await?;
                        buf = returned_buf;
                        let queued_bytes = queued_block_bytes(&res);
                        pending_batch_bytes = pending_batch_bytes.saturating_add(queued_bytes);
                        pending_batch.push(res);

                        if pending_batch.len() >= batch_blocks {
                            rustfs_io_metrics::add_ec_encode_inflight_bytes(pending_batch_bytes);
                            let send_wait_stage_start = stage_timer_if_enabled();
                            if let Err(err) = tx.send(pending_batch).await {
                                rustfs_io_metrics::remove_ec_encode_inflight_bytes(pending_batch_bytes);
                                return Err(std::io::Error::other(format!("Failed to send encoded data : {err}")));
                            }
                            record_internal_stage_if_enabled("erasure_encode_batched_send_wait", send_wait_stage_start);
                            pending_batch = Vec::with_capacity(batch_blocks);
                            pending_batch_bytes = 0;
                        }
                    }
                    Ok(None) => {
                        break;
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        if let Some(inner) = e.get_ref()
                            && rustfs_rio::is_checksum_mismatch(inner)
                        {
                            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()));
                        }
                        return Err(e);
                    }
                    Err(e) => {
                        return Err(e);
                    }
                }
            }

            if !pending_batch.is_empty() {
                rustfs_io_metrics::add_ec_encode_inflight_bytes(pending_batch_bytes);
                let send_wait_stage_start = stage_timer_if_enabled();
                if let Err(err) = tx.send(pending_batch).await {
                    rustfs_io_metrics::remove_ec_encode_inflight_bytes(pending_batch_bytes);
                    return Err(std::io::Error::other(format!("Failed to send encoded data : {err}")));
                }
                record_internal_stage_if_enabled("erasure_encode_batched_send_wait", send_wait_stage_start);
            }

            Ok((reader, total))
        });

        let mut writers = MultiWriter::new(writers, quorum);
        let mut write_err = None;

        loop {
            let recv_wait_stage_start = stage_timer_if_enabled();
            let Some(batch) = rx.recv().await else {
                break;
            };
            record_internal_stage_if_enabled("erasure_encode_batched_recv_wait", recv_wait_stage_start);
            rustfs_io_metrics::remove_ec_encode_inflight_bytes(queued_batch_bytes(&batch));
            let write_stage_start = stage_timer_if_enabled();
            for block in batch {
                if let Err(err) = writers.write(block).await {
                    write_err = Some(err);
                    break;
                }
            }
            record_internal_stage_if_enabled("erasure_encode_batched_write", write_stage_start);
            if write_err.is_some() {
                break;
            }
        }

        if let Some(err) = write_err {
            task.abort();
            let _ = task.await;
            drain_queued_batched_inflight_bytes(&mut rx).await;
            let shutdown_stage_start = stage_timer_if_enabled();
            if let Err(shutdown_err) = writers.shutdown().await {
                error!("failed to shutdown erasure writers after write error: {:?}", shutdown_err);
            }
            record_internal_stage_if_enabled("erasure_encode_batched_shutdown", shutdown_stage_start);
            return Err(err);
        }

        let (reader, total) = task.await??;
        let shutdown_stage_start = stage_timer_if_enabled();
        writers.shutdown().await?;
        record_internal_stage_if_enabled("erasure_encode_batched_shutdown", shutdown_stage_start);
        Ok((reader, total))
    }

    /// Fast path for small inline objects: skip tokio::spawn + mpsc channel.
    /// Reads all data, encodes directly, writes shards sequentially.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn encode_inline_small<R>(
        self: Arc<Self>,
        reader: R,
        writers: &mut [Option<BitrotWriterWrapper>],
        quorum: usize,
    ) -> std::io::Result<(R, usize)>
    where
        R: AsyncRead + Send + Sync + Unpin,
    {
        self.encode_small_direct(reader, writers, quorum, false).await
    }

    /// Fast path for single-block non-inline objects: avoids the producer/consumer
    /// pipeline in `encode()` while keeping the same writer/quorum/shutdown semantics.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub async fn encode_single_block_non_inline<R>(
        self: Arc<Self>,
        reader: R,
        writers: &mut [Option<BitrotWriterWrapper>],
        quorum: usize,
    ) -> std::io::Result<(R, usize)>
    where
        R: AsyncRead + Send + Sync + Unpin,
    {
        self.encode_small_direct(reader, writers, quorum, true).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::erasure::coding::{BitrotWriterWrapper, CustomWriter};
    use rustfs_rio::HardLimitReader;
    use rustfs_utils::HashAlgorithm;
    use std::future::Future;
    use std::io::Cursor;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::{AsyncWrite, AsyncWriteExt};

    fn erasure_with_zero_block_size() -> Erasure {
        let mut erasure = Erasure::default();
        erasure.data_shards = 1;
        erasure
    }

    #[derive(Clone, Default)]
    struct DeferredCommitWriter {
        buffered: Vec<u8>,
        committed: Arc<Mutex<Vec<u8>>>,
    }

    impl DeferredCommitWriter {
        fn new(committed: Arc<Mutex<Vec<u8>>>) -> Self {
            Self {
                buffered: Vec::new(),
                committed,
            }
        }
    }

    impl AsyncWrite for DeferredCommitWriter {
        fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            self.buffered.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            let buffered = std::mem::take(&mut self.buffered);
            let mut committed = self.committed.lock().unwrap();
            committed.extend_from_slice(&buffered);
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Clone, Default)]
    struct FailingWriteWriter;

    impl AsyncWrite for FailingWriteWriter {
        fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8]) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Err(std::io::Error::other("injected write failure")))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Clone, Default)]
    struct ShortWriteWriter;

    impl AsyncWrite for ShortWriteWriter {
        fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            Poll::Ready(Ok(buf.len().saturating_sub(1)))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Clone, Default)]
    struct ShutdownFailWriter {
        buffered: Vec<u8>,
    }

    impl AsyncWrite for ShutdownFailWriter {
        fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            self.buffered.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Err(std::io::Error::other("injected shutdown failure")))
        }
    }

    /// A writer that models a "black-hole" peer: it accepts up to `stall_after`
    /// bytes and then parks every subsequent `poll_write` forever (never
    /// registering a waker), the way a remote `HttpWriter` wedges once its
    /// bounded buffers fill and the peer never drains the body. With
    /// `stall_after = 0` it stalls on the very first byte. `poll_shutdown`
    /// completes only when `stall_shutdown` is false.
    #[derive(Clone)]
    struct StallingWriter {
        accepted: Arc<std::sync::atomic::AtomicUsize>,
        stall_after: usize,
        stall_shutdown: bool,
    }

    impl StallingWriter {
        fn stalls_on_write(stall_after: usize) -> Self {
            Self {
                accepted: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                stall_after,
                stall_shutdown: false,
            }
        }

        fn stalls_on_shutdown() -> Self {
            Self {
                accepted: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
                stall_after: usize::MAX,
                stall_shutdown: true,
            }
        }
    }

    impl AsyncWrite for StallingWriter {
        fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            use std::sync::atomic::Ordering;
            let already = self.accepted.load(Ordering::SeqCst);
            if already >= self.stall_after {
                // Black hole: no forward progress, no waker registered.
                return Poll::Pending;
            }
            let take = buf.len().min(self.stall_after - already);
            self.accepted.fetch_add(take, Ordering::SeqCst);
            Poll::Ready(Ok(take))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            if self.stall_shutdown {
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
    }

    /// A writer that always makes forward progress but each `poll_write` first
    /// waits `delay` (on the runtime timer). Under a paused virtual clock this
    /// deterministically models a slow-but-honest peer: it must never be killed
    /// by the per-shard stall deadline as long as `delay < stall_timeout`, yet an
    /// absolute cap can still bound its aggregate engagement.
    struct SlowWriter {
        delay: std::time::Duration,
        timer: Option<Pin<Box<tokio::time::Sleep>>>,
    }

    impl SlowWriter {
        fn new(delay: std::time::Duration) -> Self {
            Self { delay, timer: None }
        }
    }

    impl AsyncWrite for SlowWriter {
        fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
            let delay = self.delay;
            let timer = self.timer.get_or_insert_with(|| Box::pin(tokio::time::sleep(delay)));
            match timer.as_mut().poll(cx) {
                Poll::Ready(()) => {
                    self.timer = None;
                    Poll::Ready(Ok(buf.len()))
                }
                Poll::Pending => Poll::Pending,
            }
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    fn bitrot_writer<W>(writer: W, shard_size: usize) -> BitrotWriterWrapper
    where
        W: AsyncWrite + Send + Sync + Unpin + 'static,
    {
        BitrotWriterWrapper::new(CustomWriter::new_tokio_writer(writer), shard_size, HashAlgorithm::HighwayHash256S)
    }

    /// Like `bitrot_writer` but with no interleaved hash, so one shard write
    /// issues a single `poll_write` to the underlying fake. Used by the paused
    /// virtual-clock stall tests so each block maps to exactly one modeled delay.
    fn bitrot_writer_plain<W>(writer: W, shard_size: usize) -> BitrotWriterWrapper
    where
        W: AsyncWrite + Send + Sync + Unpin + 'static,
    {
        BitrotWriterWrapper::new(CustomWriter::new_tokio_writer(writer), shard_size, HashAlgorithm::None)
    }

    #[tokio::test]
    async fn helper_writers_cover_flush_and_shutdown_paths() {
        let mut failing_write = FailingWriteWriter;
        failing_write.flush().await.expect("failing-write flush should succeed");
        failing_write.shutdown().await.expect("failing-write shutdown should succeed");

        let mut short_write = ShortWriteWriter;
        let written = short_write
            .write(b"short")
            .await
            .expect("short-write helper should report a partial write");
        assert_eq!(written, 4);
        short_write.flush().await.expect("short-write flush should succeed");
        short_write.shutdown().await.expect("short-write shutdown should succeed");

        let mut shutdown_fail = ShutdownFailWriter::default();
        shutdown_fail.flush().await.expect("shutdown-fail flush should succeed");
        let err = shutdown_fail
            .shutdown()
            .await
            .expect_err("shutdown-fail writer should reject shutdown");
        assert_eq!(err.to_string(), "injected shutdown failure");
    }

    #[tokio::test]
    async fn multi_writer_short_write_fails_before_shutdown() {
        let mut writers = vec![Some(bitrot_writer(ShortWriteWriter, 16))];
        let err = {
            let mut writer = MultiWriter::new(&mut writers, 1);
            writer
                .write(vec![Bytes::from_static(b"short-write payload")])
                .await
                .expect_err("short writes must fail the shard writer")
        };

        assert!(err.to_string().contains("Failed to write data"));
        assert!(writers[0].is_none(), "short-write shard must be removed before commit");
    }

    #[tokio::test]
    async fn multi_writer_reports_fallback_summary_when_only_offline_writers_remain() {
        let mut writers = vec![None, None];
        let err = {
            let mut writer = MultiWriter::new(&mut writers, 1);
            writer
                .write(vec![Bytes::from_static(b"offline-a"), Bytes::from_static(b"offline-b")])
                .await
                .expect_err("offline writers cannot satisfy write quorum")
        };

        let err = err.to_string();
        assert!(err.contains("Failed to write data"));
        assert!(err.contains("offline-disks=2/2"));
        assert!(err.contains("required=1"));

        let shutdown_err = {
            let mut writer = MultiWriter::new(&mut writers, 1);
            writer
                .shutdown()
                .await
                .expect_err("offline writers cannot satisfy shutdown quorum")
        };

        let shutdown_err = shutdown_err.to_string();
        assert!(shutdown_err.contains("Failed to shutdown writers"));
        assert!(shutdown_err.contains("offline-disks=2/2"));
        assert!(shutdown_err.contains("required=1"));
    }

    #[tokio::test]
    async fn multi_writer_reports_quorum_failure_when_quorum_exceeds_writer_count() {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let mut writers = vec![Some(bitrot_writer(DeferredCommitWriter::new(committed), 16))];
        let mut writer = MultiWriter::new(&mut writers, 2);

        let err = writer
            .write(vec![Bytes::from_static(b"quorum impossible")])
            .await
            .expect_err("write quorum above writer count must fail");
        let err = err.to_string();
        assert!(err.contains("Failed to write data"));
        assert!(err.contains("required=2"));
        assert!(err.contains("erasure write quorum"));

        let shutdown_err = writer
            .shutdown()
            .await
            .expect_err("shutdown quorum above writer count must fail");
        let shutdown_err = shutdown_err.to_string();
        assert!(shutdown_err.contains("Failed to shutdown writers"));
        assert!(shutdown_err.contains("required=2"));
        assert!(shutdown_err.contains("erasure write quorum"));
    }

    // The production wiring (`MultiWriter::new`) must arm a real deadline by
    // default, and honor `0` as "disabled". This guards against the fix silently
    // regressing to wait-forever behavior on the default PUT path.
    #[test]
    fn default_write_progress_policy_is_armed_and_configurable() {
        temp_env::with_var_unset(rustfs_config::ENV_OBJECT_DISK_WRITE_STALL_TIMEOUT, || {
            temp_env::with_var_unset(rustfs_config::ENV_OBJECT_DISK_WRITE_ABSOLUTE_CAP, || {
                let policy = WriteProgressPolicy::default();
                assert_eq!(
                    policy.stall_timeout,
                    Some(Duration::from_secs(rustfs_config::DEFAULT_OBJECT_DISK_WRITE_STALL_TIMEOUT)),
                    "the default PUT path must be protected by a stall deadline"
                );
                assert_eq!(policy.absolute_cap, None, "the absolute cap is disabled by default");
            });
        });

        temp_env::with_var(rustfs_config::ENV_OBJECT_DISK_WRITE_STALL_TIMEOUT, Some("0"), || {
            let policy = WriteProgressPolicy::default();
            assert_eq!(policy.stall_timeout, None, "0 disables the stall deadline (wait indefinitely)");
        });
    }

    fn four_shards() -> Vec<Bytes> {
        vec![
            Bytes::from_static(b"shard-0"),
            Bytes::from_static(b"shard-1"),
            Bytes::from_static(b"shard-2"),
            Bytes::from_static(b"shard-3"),
        ]
    }

    /// Four full-`shard_size` shards. A shard shorter than `shard_size` marks the
    /// bitrot writer `finished`, so multi-block tests must send full shards to
    /// keep writing across iterations.
    fn four_full_shards(shard_size: usize) -> Vec<Bytes> {
        (0..4).map(|i| Bytes::from(vec![0x40 + i as u8; shard_size])).collect()
    }

    // rustfs/backlog#1319: a single black-hole writer that never accepts a byte
    // must be failed within the stall budget so the remaining 3/4 writers still
    // meet a write quorum of 3 — the write must not wait forever.
    #[tokio::test(start_paused = true)]
    async fn multi_writer_write_stall_fails_black_hole_but_meets_quorum() {
        let committed: Vec<Arc<Mutex<Vec<u8>>>> = (0..3).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let mut writers = vec![
            Some(bitrot_writer_plain(StallingWriter::stalls_on_write(0), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[0].clone()), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[1].clone()), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[2].clone()), 64)),
        ];

        {
            let policy = WriteProgressPolicy::new(Duration::from_secs(5), Duration::ZERO);
            let mut mw = MultiWriter::with_policy(&mut writers, 3, policy);
            mw.write(four_shards())
                .await
                .expect("one stalled writer must still satisfy a 3/4 write quorum without hanging");
        }

        assert!(writers[0].is_none(), "the black-hole writer must be failed and dropped before commit");
        assert!(writers[1].is_some() && writers[2].is_some() && writers[3].is_some());
    }

    // Two black-hole writers drop the healthy count to 2/4, below the quorum of
    // 3, so the write must fail cleanly (not hang).
    #[tokio::test(start_paused = true)]
    async fn multi_writer_write_stall_two_black_holes_fail_quorum() {
        let committed: Vec<Arc<Mutex<Vec<u8>>>> = (0..2).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let mut writers = vec![
            Some(bitrot_writer_plain(StallingWriter::stalls_on_write(0), 64)),
            Some(bitrot_writer_plain(StallingWriter::stalls_on_write(0), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[0].clone()), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[1].clone()), 64)),
        ];

        let policy = WriteProgressPolicy::new(Duration::from_secs(5), Duration::ZERO);
        let mut mw = MultiWriter::with_policy(&mut writers, 3, policy);
        let err = mw
            .write(four_shards())
            .await
            .expect_err("two stalled writers must fail the write quorum instead of hanging");
        assert!(err.to_string().contains("Failed to write data"));
    }

    // A small object whose bytes were fully buffered leaves `write` succeeding
    // for a black-hole peer; the stall must then be caught during shutdown so it
    // cannot pin the quorum there either.
    #[tokio::test(start_paused = true)]
    async fn multi_writer_shutdown_stall_fails_black_hole_but_meets_quorum() {
        let committed: Vec<Arc<Mutex<Vec<u8>>>> = (0..3).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let mut writers = vec![
            Some(bitrot_writer_plain(StallingWriter::stalls_on_shutdown(), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[0].clone()), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[1].clone()), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[2].clone()), 64)),
        ];

        {
            let policy = WriteProgressPolicy::new(Duration::from_secs(5), Duration::ZERO);
            let mut mw = MultiWriter::with_policy(&mut writers, 3, policy);
            mw.write(four_shards()).await.expect("all writers accept the buffered shard");
            mw.shutdown()
                .await
                .expect("a single shutdown stall must still satisfy a 3/4 quorum without hanging");
        }

        assert!(writers[0].is_none(), "the writer that stalled shutdown must be failed and dropped");
    }

    #[tokio::test(start_paused = true)]
    async fn multi_writer_shutdown_stall_two_black_holes_fail_quorum() {
        let committed: Vec<Arc<Mutex<Vec<u8>>>> = (0..2).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let mut writers = vec![
            Some(bitrot_writer_plain(StallingWriter::stalls_on_shutdown(), 64)),
            Some(bitrot_writer_plain(StallingWriter::stalls_on_shutdown(), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[0].clone()), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[1].clone()), 64)),
        ];

        let policy = WriteProgressPolicy::new(Duration::from_secs(5), Duration::ZERO);
        let mut mw = MultiWriter::with_policy(&mut writers, 3, policy);
        mw.write(four_shards()).await.expect("all writers accept the buffered shard");
        let err = mw
            .shutdown()
            .await
            .expect_err("two shutdown stalls must fail the shutdown quorum instead of hanging");
        assert!(err.to_string().contains("Failed to shutdown writers"));
    }

    // A slow-but-honest writer that keeps completing shards (delay < stall
    // budget) must never be killed, even across many blocks, when no absolute cap
    // is configured.
    #[tokio::test(start_paused = true)]
    async fn multi_writer_slow_but_progressing_writer_is_not_killed() {
        let mut writers = vec![
            Some(bitrot_writer_plain(SlowWriter::new(Duration::from_secs(1)), 64)),
            Some(bitrot_writer_plain(SlowWriter::new(Duration::from_secs(1)), 64)),
            Some(bitrot_writer_plain(SlowWriter::new(Duration::from_secs(1)), 64)),
            Some(bitrot_writer_plain(SlowWriter::new(Duration::from_secs(1)), 64)),
        ];

        {
            let policy = WriteProgressPolicy::new(Duration::from_secs(5), Duration::ZERO);
            let mut mw = MultiWriter::with_policy(&mut writers, 3, policy);
            for _ in 0..8 {
                mw.write(four_full_shards(64))
                    .await
                    .expect("a writer that keeps making progress within the stall budget must not be failed");
            }
        }

        assert!(writers.iter().all(Option::is_some), "no slow-but-honest writer should be dropped");
    }

    // Slow-drip defense: the per-shard stall timer resets on every block, so a
    // peer that dribbles just enough progress each block is not caught by the
    // stall timeout alone. The optional absolute cap bounds its aggregate
    // engagement and fails it within a finite budget, while the healthy writers
    // still meet quorum.
    #[tokio::test(start_paused = true)]
    async fn multi_writer_absolute_cap_bounds_slow_drip_writer() {
        let committed: Vec<Arc<Mutex<Vec<u8>>>> = (0..3).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let mut writers = vec![
            Some(bitrot_writer_plain(SlowWriter::new(Duration::from_secs(8)), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[0].clone()), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[1].clone()), 64)),
            Some(bitrot_writer_plain(DeferredCommitWriter::new(committed[2].clone()), 64)),
        ];

        {
            // stall (10s) never fires for an 8s-per-block writer, but the 15s
            // absolute cap does once its aggregate engagement crosses the cap.
            let policy = WriteProgressPolicy::new(Duration::from_secs(10), Duration::from_secs(15));
            let mut mw = MultiWriter::with_policy(&mut writers, 3, policy);
            for _ in 0..4 {
                mw.write(four_full_shards(64))
                    .await
                    .expect("healthy writers keep the 3/4 quorum while the drip writer is bounded");
            }
        }

        assert!(
            writers[0].is_none(),
            "the slow-drip writer must be failed within the absolute cap, not held forever"
        );
    }

    #[tokio::test]
    async fn drain_queued_inflight_bytes_consumes_pending_blocks() {
        let (tx, mut rx) = mpsc::channel(2);
        tx.send(vec![Bytes::from_static(b"queued")]).await.unwrap();
        drop(tx);

        drain_queued_inflight_bytes(&mut rx).await;

        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn drain_queued_batched_inflight_bytes_consumes_pending_batches() {
        let (tx, mut rx) = mpsc::channel(2);
        tx.send(vec![vec![Bytes::from_static(b"queued")]]).await.unwrap();
        drop(tx);

        drain_queued_batched_inflight_bytes(&mut rx).await;

        assert!(rx.recv().await.is_none());
    }

    #[tokio::test]
    async fn encode_shutdowns_writers_after_small_shards() {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let writer = DeferredCommitWriter::new(committed.clone());
        let mut writers = vec![Some(bitrot_writer(writer, 16))];

        let erasure = Arc::new(Erasure::new(1, 0, 16));
        let reader = tokio::io::BufReader::new(Cursor::new(b"small payload".to_vec()));
        let (_reader, written) = erasure.encode(reader, &mut writers, 1).await.unwrap();

        assert_eq!(written, b"small payload".len());
        assert!(!committed.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn encode_bytesmut_ingest_streaming_path_writes_and_shutdowns_writers() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const TOTAL_SHARDS: usize = DATA_SHARDS + PARITY_SHARDS;
        const BLOCK_SIZE: usize = 32;

        let committed: Vec<Arc<Mutex<Vec<u8>>>> = (0..TOTAL_SHARDS).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let mut writers: Vec<Option<BitrotWriterWrapper>> = committed
            .iter()
            .map(|c| Some(bitrot_writer(DeferredCommitWriter::new(c.clone()), BLOCK_SIZE / DATA_SHARDS)))
            .collect();

        let payload = vec![0x5a; BLOCK_SIZE * 2 + 7];
        let erasure = Arc::new(Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE));
        let reader = tokio::io::BufReader::new(Cursor::new(payload.clone()));
        let (_reader, written) = erasure
            .encode_with_ingest_mode(reader, &mut writers, DATA_SHARDS, true)
            .await
            .expect("BytesMut ingest path should encode the streaming payload");

        assert_eq!(written, payload.len());
        for (index, committed) in committed.iter().enumerate() {
            assert!(
                !committed.lock().expect("committed buffer should be lockable").is_empty(),
                "shard {index} should receive bytesmut-ingest data"
            );
        }
    }

    #[tokio::test]
    async fn encode_streaming_write_quorum_failure_aborts_and_reports_error() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;

        let committed = Arc::new(Mutex::new(Vec::new()));
        let mut writers = vec![
            Some(bitrot_writer(DeferredCommitWriter::new(committed.clone()), BLOCK_SIZE / DATA_SHARDS)),
            Some(bitrot_writer(FailingWriteWriter, BLOCK_SIZE / DATA_SHARDS)),
            None,
            None,
        ];

        let payload = vec![3u8; BLOCK_SIZE * 8];
        let erasure = Arc::new(Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE));
        let reader = tokio::io::BufReader::new(Cursor::new(payload));
        let err = erasure
            .encode(reader, &mut writers, DATA_SHARDS)
            .await
            .expect_err("streaming encode must fail when write quorum is unavailable");

        assert!(err.to_string().contains("Failed to write data"));
    }

    #[tokio::test]
    async fn encode_inline_small_write_quorum_failure_does_not_commit_partial_data() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;

        let committed = Arc::new(Mutex::new(Vec::new()));
        let mut writers = vec![
            Some(bitrot_writer(DeferredCommitWriter::new(committed.clone()), BLOCK_SIZE / DATA_SHARDS)),
            Some(bitrot_writer(FailingWriteWriter, BLOCK_SIZE / DATA_SHARDS)),
            None,
            None,
        ];

        let erasure = Arc::new(Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE));
        let reader = tokio::io::BufReader::new(Cursor::new(b"write quorum failure payload".to_vec()));
        let err = erasure
            .encode_inline_small(reader, &mut writers, DATA_SHARDS)
            .await
            .expect_err("write quorum failure must fail the inline encode");

        assert!(err.to_string().contains("Failed to write data"));
        assert!(
            committed.lock().expect("committed buffer should be lockable").is_empty(),
            "successful writer must not be committed when write quorum fails before shutdown"
        );
    }

    #[tokio::test]
    async fn encode_inline_small_shutdown_quorum_failure_is_reported() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;

        let committed = Arc::new(Mutex::new(Vec::new()));
        let mut writers = vec![
            Some(bitrot_writer(DeferredCommitWriter::new(committed.clone()), BLOCK_SIZE / DATA_SHARDS)),
            Some(bitrot_writer(ShutdownFailWriter::default(), BLOCK_SIZE / DATA_SHARDS)),
            Some(bitrot_writer(ShutdownFailWriter::default(), BLOCK_SIZE / DATA_SHARDS)),
            Some(bitrot_writer(ShutdownFailWriter::default(), BLOCK_SIZE / DATA_SHARDS)),
        ];

        let erasure = Arc::new(Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE));
        let reader = tokio::io::BufReader::new(Cursor::new(b"shutdown quorum failure payload".to_vec()));
        let err = erasure
            .encode_inline_small(reader, &mut writers, DATA_SHARDS)
            .await
            .expect_err("shutdown quorum failure must fail the inline encode");

        assert!(err.to_string().contains("Failed to shutdown writers"));
        assert!(
            !committed.lock().expect("committed buffer should be lockable").is_empty(),
            "the successful writer should have committed before shutdown quorum failure was reported"
        );
    }

    #[tokio::test]
    async fn encode_returns_unexpected_eof_for_truncated_limited_reader() {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let writer = DeferredCommitWriter::new(committed);
        let mut writers = vec![Some(BitrotWriterWrapper::new(
            CustomWriter::new_tokio_writer(writer),
            16,
            HashAlgorithm::HighwayHash256S,
        ))];

        let erasure = Arc::new(Erasure::new(1, 0, 16));
        let truncated = HardLimitReader::new(Cursor::new(b"short".to_vec()), 10);

        let err = match erasure.encode(truncated, &mut writers, 1).await {
            Ok(_) => panic!("truncated input must fail"),
            Err(err) => err,
        };

        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[tokio::test]
    async fn encode_rejects_zero_block_size() {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let writer = DeferredCommitWriter::new(committed);
        let mut writers = vec![Some(BitrotWriterWrapper::new(
            CustomWriter::new_tokio_writer(writer),
            16,
            HashAlgorithm::HighwayHash256S,
        ))];

        let erasure = Arc::new(erasure_with_zero_block_size());
        let reader = tokio::io::BufReader::new(Cursor::new(b"payload".to_vec()));
        let err = erasure
            .encode(reader, &mut writers, 1)
            .await
            .expect_err("zero block size must be rejected");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("block_size"));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn encode_works_on_current_thread_runtime() {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let writer = DeferredCommitWriter::new(committed);
        let mut writers = vec![Some(bitrot_writer(writer, 16))];

        let erasure = Arc::new(Erasure::new(1, 0, 16));
        let reader = tokio::io::BufReader::new(Cursor::new(b"current-thread payload".to_vec()));
        let (_reader, written) = erasure.encode(reader, &mut writers, 1).await.unwrap();

        assert_eq!(written, b"current-thread payload".len());
    }

    // Covers the RuntimeFlavor::MultiThread arm of encode_block /
    // encode_block_bytes_mut, which runs the EC compute inline instead of via
    // block_in_place (rustfs/backlog#932). The other tests default to the
    // current-thread runtime and only exercise the spawn_blocking arm.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn encode_works_on_multi_thread_runtime() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const TOTAL_SHARDS: usize = DATA_SHARDS + PARITY_SHARDS;
        const BLOCK_SIZE: usize = 32;

        let payload = vec![9u8; BLOCK_SIZE * 3 + 7];

        // Streaming encode() drives encode_block (Vec ingest).
        let streamed: Vec<Arc<Mutex<Vec<u8>>>> = (0..TOTAL_SHARDS).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let mut streamed_writers: Vec<Option<BitrotWriterWrapper>> = streamed
            .iter()
            .map(|c| Some(bitrot_writer(DeferredCommitWriter::new(c.clone()), BLOCK_SIZE / DATA_SHARDS)))
            .collect();
        let erasure = Arc::new(Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE));
        let (_r, streamed_total) = erasure
            .clone()
            .encode(
                tokio::io::BufReader::new(Cursor::new(payload.clone())),
                &mut streamed_writers,
                DATA_SHARDS,
            )
            .await
            .expect("streaming encode should succeed on the multi-threaded runtime");
        assert_eq!(streamed_total, payload.len());

        // Batched encode() drives encode_block_bytes_mut (BytesMut ingest).
        let batched: Vec<Arc<Mutex<Vec<u8>>>> = (0..TOTAL_SHARDS).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let mut batched_writers: Vec<Option<BitrotWriterWrapper>> = batched
            .iter()
            .map(|c| Some(bitrot_writer(DeferredCommitWriter::new(c.clone()), BLOCK_SIZE / DATA_SHARDS)))
            .collect();
        let (_r2, batched_total) = erasure
            .encode_batched(tokio::io::BufReader::new(Cursor::new(payload.clone())), &mut batched_writers, DATA_SHARDS)
            .await
            .expect("batched encode should succeed on the multi-threaded runtime");
        assert_eq!(batched_total, payload.len());

        // Both ingest paths must produce identical shard bytes regardless of the
        // runtime flavor / dispatch strategy.
        for (index, (a, b)) in streamed.iter().zip(batched.iter()).enumerate() {
            let a = a.lock().expect("streamed shard lockable").clone();
            let b = b.lock().expect("batched shard lockable").clone();
            assert!(!a.is_empty(), "shard {index} should receive committed data");
            assert_eq!(a, b, "shard {index} must match between streaming and batched encode");
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn encode_block_bytes_mut_works_on_current_thread_runtime() {
        let erasure = Arc::new(Erasure::new(2, 2, 64));
        let payload = b"bytesmut current-thread payload";
        let shards = erasure
            .clone()
            .encode_block_bytes_mut(bytes::BytesMut::from(&payload[..]), payload.len())
            .await
            .expect("bytesmut encode should succeed on current-thread runtime");

        let expected_shard_size = payload.len().div_ceil(erasure.data_shards);
        assert_eq!(shards.len(), erasure.total_shard_count());
        assert!(shards.iter().all(|shard| shard.len() == expected_shard_size));

        let mut restored = Vec::new();
        for shard in shards.iter().take(erasure.data_shards) {
            restored.extend_from_slice(shard);
        }
        restored.truncate(payload.len());
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    async fn encode_batched_writes_full_and_tail_batches() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const TOTAL_SHARDS: usize = DATA_SHARDS + PARITY_SHARDS;
        const BLOCK_SIZE: usize = 32;

        let committed: Vec<Arc<Mutex<Vec<u8>>>> = (0..TOTAL_SHARDS).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let mut writers: Vec<Option<BitrotWriterWrapper>> = committed
            .iter()
            .map(|c| Some(bitrot_writer(DeferredCommitWriter::new(c.clone()), BLOCK_SIZE / DATA_SHARDS)))
            .collect();

        let payload = vec![7u8; BLOCK_SIZE * 5 + 3];
        let erasure = Arc::new(Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE));
        let reader = tokio::io::BufReader::new(Cursor::new(payload.clone()));
        let (_reader, total) = erasure
            .encode_batched(reader, &mut writers, DATA_SHARDS)
            .await
            .expect("batched encode should write full and tail batches");

        assert_eq!(total, payload.len());
        for (index, committed) in committed.iter().enumerate() {
            assert!(
                !committed.lock().expect("committed buffer should be lockable").is_empty(),
                "shard {index} should receive committed batched data"
            );
        }
    }

    #[tokio::test]
    async fn encode_batched_write_quorum_failure_aborts_and_reports_error() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 32;

        let committed = Arc::new(Mutex::new(Vec::new()));
        let mut writers = vec![
            Some(bitrot_writer(DeferredCommitWriter::new(committed.clone()), BLOCK_SIZE / DATA_SHARDS)),
            Some(bitrot_writer(FailingWriteWriter, BLOCK_SIZE / DATA_SHARDS)),
            None,
            None,
        ];

        let payload = vec![9u8; BLOCK_SIZE * 8];
        let erasure = Arc::new(Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE));
        let reader = tokio::io::BufReader::new(Cursor::new(payload));
        let err = erasure
            .encode_batched(reader, &mut writers, DATA_SHARDS)
            .await
            .expect_err("batched encode must fail when write quorum is unavailable");

        assert!(err.to_string().contains("Failed to write data"));
    }

    #[tokio::test]
    async fn encode_batched_rejects_zero_block_size() {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let writer = DeferredCommitWriter::new(committed);
        let mut writers = vec![Some(bitrot_writer(writer, 16))];

        let erasure = Arc::new(erasure_with_zero_block_size());
        let reader = tokio::io::BufReader::new(Cursor::new(b"payload".to_vec()));
        let err = erasure
            .encode_batched(reader, &mut writers, 1)
            .await
            .expect_err("zero block size must be rejected");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("block_size"));
    }

    /// encode_inline_small: empty reader returns (reader, 0) without writing to any shard.
    #[tokio::test]
    async fn encode_inline_small_empty_stream_returns_zero() {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let writer = DeferredCommitWriter::new(committed.clone());
        // 1 data shard, 0 parity shards, block_size = 16
        let mut writers = vec![Some(BitrotWriterWrapper::new(
            CustomWriter::new_tokio_writer(writer),
            16,
            HashAlgorithm::HighwayHash256S,
        ))];

        let erasure = Arc::new(Erasure::new(1, 0, 16));
        let reader = tokio::io::BufReader::new(Cursor::new(Vec::<u8>::new()));
        let (_reader, total) = erasure.encode_inline_small(reader, &mut writers, 1).await.unwrap();

        assert_eq!(total, 0);
        // No shutdown was called, so nothing should be committed
        assert!(committed.lock().unwrap().is_empty());
    }

    /// encode_inline_small: small payload is encoded into the correct number of shards
    /// and each writer receives data after shutdown.
    #[tokio::test]
    async fn encode_inline_small_payload_writes_all_shards() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const TOTAL_SHARDS: usize = DATA_SHARDS + PARITY_SHARDS;
        const BLOCK_SIZE: usize = 64;

        let committed: Vec<Arc<Mutex<Vec<u8>>>> = (0..TOTAL_SHARDS).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();

        let mut writers: Vec<Option<BitrotWriterWrapper>> = committed
            .iter()
            .map(|c| {
                Some(BitrotWriterWrapper::new(
                    CustomWriter::new_tokio_writer(DeferredCommitWriter::new(c.clone())),
                    BLOCK_SIZE / DATA_SHARDS,
                    HashAlgorithm::HighwayHash256S,
                ))
            })
            .collect();

        let payload = b"hello inline small";
        let erasure = Arc::new(Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE));
        let reader = tokio::io::BufReader::new(Cursor::new(payload.to_vec()));
        let (_reader, total) = erasure.encode_inline_small(reader, &mut writers, DATA_SHARDS).await.unwrap();

        assert_eq!(total, payload.len());
        // All shards must have received data (shutdown flushed the bitrot header + shard bytes)
        for (i, c) in committed.iter().enumerate() {
            assert!(!c.lock().unwrap().is_empty(), "shard {i} should have received data");
        }
    }

    #[tokio::test]
    async fn encode_single_block_non_inline_payload_writes_all_shards() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const TOTAL_SHARDS: usize = DATA_SHARDS + PARITY_SHARDS;
        const BLOCK_SIZE: usize = 64;

        let committed: Vec<Arc<Mutex<Vec<u8>>>> = (0..TOTAL_SHARDS).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();

        let mut writers: Vec<Option<BitrotWriterWrapper>> = committed
            .iter()
            .map(|c| {
                Some(BitrotWriterWrapper::new(
                    CustomWriter::new_tokio_writer(DeferredCommitWriter::new(c.clone())),
                    BLOCK_SIZE / DATA_SHARDS,
                    HashAlgorithm::HighwayHash256S,
                ))
            })
            .collect();

        let payload = b"hello single block";
        let erasure = Arc::new(Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE));
        let reader = tokio::io::BufReader::new(Cursor::new(payload.to_vec()));
        let (_reader, total) = erasure
            .encode_single_block_non_inline(reader, &mut writers, DATA_SHARDS)
            .await
            .unwrap();

        assert_eq!(total, payload.len());
        for (i, c) in committed.iter().enumerate() {
            assert!(!c.lock().unwrap().is_empty(), "shard {i} should have received data");
        }
    }

    #[tokio::test]
    async fn encode_single_block_non_inline_rejects_multi_block_payload() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const TOTAL_SHARDS: usize = DATA_SHARDS + PARITY_SHARDS;
        const BLOCK_SIZE: usize = 64;

        let committed: Vec<Arc<Mutex<Vec<u8>>>> = (0..TOTAL_SHARDS).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();

        let mut writers: Vec<Option<BitrotWriterWrapper>> = committed
            .iter()
            .map(|c| {
                Some(BitrotWriterWrapper::new(
                    CustomWriter::new_tokio_writer(DeferredCommitWriter::new(c.clone())),
                    BLOCK_SIZE / DATA_SHARDS,
                    HashAlgorithm::HighwayHash256S,
                ))
            })
            .collect();

        let payload = vec![1u8; BLOCK_SIZE + 1];
        let erasure = Arc::new(Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE));
        let reader = tokio::io::BufReader::new(Cursor::new(payload));
        let err = erasure
            .encode_single_block_non_inline(reader, &mut writers, DATA_SHARDS)
            .await
            .expect_err("single-block fast path must reject oversized readers");

        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
        assert!(err.to_string().contains("single-block non-inline fast path"));
        for c in committed {
            assert!(c.lock().unwrap().is_empty());
        }
    }

    #[tokio::test]
    async fn read_full_buf_or_eof_returns_none_on_empty_reader() {
        let mut reader = Cursor::new(Vec::<u8>::new());
        let mut buf = BytesMut::with_capacity(16);

        let res = read_full_buf_or_eof(&mut reader, &mut buf, 8).await.unwrap();

        assert_eq!(res, None);
        assert!(buf.is_empty());
    }

    #[tokio::test]
    async fn read_full_buf_or_eof_reads_partial_tail() {
        let data = b"tail".to_vec();
        let mut reader = Cursor::new(data.clone());
        let mut buf = BytesMut::with_capacity(64);

        let res = read_full_buf_or_eof(&mut reader, &mut buf, 16).await.unwrap();

        assert_eq!(res, Some(data.len()));
        assert_eq!(&buf[..], &data[..]);
        assert!(buf.capacity() >= 64, "pre-reserved spare capacity must be kept");
    }

    #[tokio::test]
    async fn read_full_buf_or_eof_stops_at_limit() {
        let data = vec![7u8; 32];
        let mut reader = Cursor::new(data.clone());
        let mut buf = BytesMut::with_capacity(64);

        let res = read_full_buf_or_eof(&mut reader, &mut buf, 16).await.unwrap();

        assert_eq!(res, Some(16));
        assert_eq!(&buf[..], &data[..16]);

        // The remaining bytes are still readable as the next block.
        let mut next = BytesMut::with_capacity(64);
        let res = read_full_buf_or_eof(&mut reader, &mut next, 16).await.unwrap();
        assert_eq!(res, Some(16));
        assert_eq!(&next[..], &data[16..]);
    }

    async fn committed_shards_for_ingest_mode(use_bytesmut_ingest: bool, uses_legacy: bool, payload: &[u8]) -> Vec<Vec<u8>> {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const TOTAL_SHARDS: usize = DATA_SHARDS + PARITY_SHARDS;
        const BLOCK_SIZE: usize = 64;

        let committed: Vec<Arc<Mutex<Vec<u8>>>> = (0..TOTAL_SHARDS).map(|_| Arc::new(Mutex::new(Vec::new()))).collect();
        let mut writers: Vec<Option<BitrotWriterWrapper>> = committed
            .iter()
            .map(|c| Some(bitrot_writer(DeferredCommitWriter::new(c.clone()), BLOCK_SIZE / DATA_SHARDS)))
            .collect();

        let erasure = Arc::new(Erasure::new_with_options(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE, uses_legacy));
        let reader = tokio::io::BufReader::new(Cursor::new(payload.to_vec()));
        let (_reader, total) = erasure
            .encode_with_ingest_mode(reader, &mut writers, DATA_SHARDS, use_bytesmut_ingest)
            .await
            .expect("encode should succeed");
        assert_eq!(total, payload.len());

        committed
            .iter()
            .map(|c| c.lock().expect("committed buffer should be lockable").clone())
            .collect()
    }

    /// HP-10 (rustfs/backlog#931) merge gate: the BytesMut ingest path must produce
    /// byte-for-byte identical shard streams to the default Vec ingest path, for both
    /// legacy-aware shard-size formulas, across empty, sub-block, exactly-full-block,
    /// and multi-block-with-partial-tail payloads.
    #[tokio::test]
    async fn bytesmut_ingest_matches_vec_ingest_byte_for_byte() {
        const BLOCK_SIZE: usize = 64;
        let payloads: Vec<Vec<u8>> = vec![
            Vec::new(),
            b"tiny".to_vec(),
            (0..BLOCK_SIZE as u32).map(|i| i as u8).collect(), // exactly one full block
            vec![3u8; BLOCK_SIZE * 4],                         // whole number of blocks
            (0..(BLOCK_SIZE * 3 + 7) as u32).map(|i| (i % 251) as u8).collect(), // partial tail
        ];

        for uses_legacy in [false, true] {
            for payload in &payloads {
                let vec_path = committed_shards_for_ingest_mode(false, uses_legacy, payload).await;
                let bytesmut_path = committed_shards_for_ingest_mode(true, uses_legacy, payload).await;
                assert_eq!(
                    vec_path,
                    bytesmut_path,
                    "ingest paths must be byte-identical (legacy={uses_legacy}, payload_len={})",
                    payload.len()
                );
            }
        }
    }

    #[test]
    fn encode_channel_capacity_never_returns_zero() {
        assert_eq!(encode_channel_capacity(0, 1024), 1);
        assert_eq!(encode_channel_capacity(4096, 0), 1);
        assert_eq!(encode_channel_capacity(4096, 1024), 1);
    }

    #[test]
    fn write_quorum_failure_summary_uses_stable_dominant_error_label() {
        let err = Error::from(rustfs_rio::new_test_internode_http_io_error(
            rustfs_rio::InternodeHttpErrorKind::ConnectionReset,
        ));
        let summary = WriteQuorumFailureSummary {
            required: 2,
            achieved: 0,
            failed: 2,
            total: 2,
            offline_disks: 0,
            ignored_failures: 0,
            retryable_failures: 2,
            dominant_error: Some(err),
            dominant_error_label: "connection_reset",
        };
        let text = format_write_quorum_failure(&summary);

        assert!(text.contains("dominant-error=connection_reset"));
        assert!(!text.contains("/rustfs/rpc/put_file_stream"));
        assert!(!text.contains("PUT "));
    }

    #[test]
    fn encode_channel_capacity_respects_budget_and_hard_cap() {
        assert_eq!(encode_channel_capacity(4 * 1024 * 1024, 32 * 1024 * 1024), 8);
        assert_eq!(encode_channel_capacity(1536 * 1024, 32 * 1024 * 1024), 21);
        assert_eq!(encode_channel_capacity(16 * 1024 * 1024, 32 * 1024 * 1024), 2);
        assert_eq!(encode_channel_capacity(1, usize::MAX), DEFAULT_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BLOCKS);
    }
}
