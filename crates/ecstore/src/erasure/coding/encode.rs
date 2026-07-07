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

pub(crate) struct MultiWriter<'a> {
    writers: &'a mut [Option<BitrotWriterWrapper>],
    write_quorum: usize,
    errs: Vec<Option<Error>>,
}

impl<'a> MultiWriter<'a> {
    pub fn new(writers: &'a mut [Option<BitrotWriterWrapper>], write_quorum: usize) -> Self {
        let length = writers.len();
        MultiWriter {
            writers,
            write_quorum,
            errs: vec![None; length],
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

        {
            let mut futures = FuturesUnordered::new();
            for ((writer_opt, err), shard) in self.writers.iter_mut().zip(self.errs.iter_mut()).zip(data.iter()) {
                if err.is_some() {
                    continue; // Skip if we already have an error for this writer
                }
                futures.push(Self::write_shard(writer_opt, err, shard));
            }
            while let Some(()) = futures.next().await {}
        }

        let nil_count = self.errs.iter().filter(|&e| e.is_none()).count();
        if nil_count >= self.write_quorum {
            return Ok(());
        }

        if let Some(write_err) = reduce_write_quorum_errs(&self.errs, OBJECT_OP_IGNORED_ERRS, self.write_quorum) {
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
            return Err(std::io::Error::other(format!("Failed to write data: {summary_text}")));
        }

        let summary = build_write_quorum_failure_summary(&self.errs, OBJECT_OP_IGNORED_ERRS, self.write_quorum);
        Err(std::io::Error::other(format!(
            "Failed to write data: {}: {}",
            format_write_quorum_failure(&summary),
            self.errs
                .iter()
                .map(|e| e.as_ref().map_or_else(|| "<nil>".to_string(), |e| e.to_string()))
                .collect::<Vec<_>>()
                .join(", ")
        )))
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
        {
            let mut futures = FuturesUnordered::new();
            for (writer_opt, err) in self.writers.iter_mut().zip(self.errs.iter_mut()) {
                if err.is_some() {
                    continue;
                }
                futures.push(Self::shutdown_writer(writer_opt, err));
            }
            while let Some(()) = futures.next().await {}
        }

        let nil_count = self.errs.iter().filter(|&e| e.is_none()).count();
        if nil_count >= self.write_quorum {
            return Ok(());
        }

        if let Some(write_err) = reduce_write_quorum_errs(&self.errs, OBJECT_OP_IGNORED_ERRS, self.write_quorum) {
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
            return Err(std::io::Error::other(format!("Failed to shutdown writers: {summary_text}")));
        }

        let summary = build_write_quorum_failure_summary(&self.errs, OBJECT_OP_IGNORED_ERRS, self.write_quorum);
        Err(std::io::Error::other(format!(
            "Failed to shutdown writers: {}: {}",
            format_write_quorum_failure(&summary),
            self.errs
                .iter()
                .map(|e| e.as_ref().map_or_else(|| "<nil>".to_string(), |e| e.to_string()))
                .collect::<Vec<_>>()
                .join(", ")
        )))
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
            RuntimeFlavor::MultiThread => tokio::task::block_in_place(encode_once),
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
            RuntimeFlavor::MultiThread => tokio::task::block_in_place(encode_once),
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

    pub async fn encode<R>(
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

        // Bound queued encoded blocks by memory budget to avoid per-request spikes.
        let expanded_block_bytes = self.shard_size().saturating_mul(self.total_shard_count());
        let max_inflight_bytes = erasure_encode_max_inflight_bytes();
        let inflight_blocks = encode_channel_capacity(expanded_block_bytes, max_inflight_bytes);
        let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(inflight_blocks);

        let task = tokio::spawn(async move {
            let block_size = self.block_size;
            let use_bytesmut_ingest = use_bytesmut_ingest();
            let mut total = 0;
            if use_bytesmut_ingest {
                let mut buf = BytesMut::with_capacity(block_size);
                buf.resize(block_size, 0);
                loop {
                    match rustfs_utils::read_full_or_eof(&mut reader, &mut buf[..]).await {
                        Ok(Some(n)) => {
                            debug_assert!(n > 0, "non-zero block_size prevents zero-length reads");
                            total += n;
                            let encode_buf = buf;
                            let res = self.clone().encode_block_bytes_mut(encode_buf, n).await?;
                            buf = BytesMut::with_capacity(block_size);
                            buf.resize(block_size, 0);
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
    use std::io::Cursor;
    use std::pin::Pin;
    use std::sync::{Arc, Mutex};
    use std::task::{Context, Poll};
    use tokio::io::AsyncWrite;

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

    fn bitrot_writer<W>(writer: W, shard_size: usize) -> BitrotWriterWrapper
    where
        W: AsyncWrite + Send + Sync + Unpin + 'static,
    {
        BitrotWriterWrapper::new(CustomWriter::new_tokio_writer(writer), shard_size, HashAlgorithm::HighwayHash256S)
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

        let erasure = Arc::new(Erasure::new(1, 0, 0));
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

        let erasure = Arc::new(Erasure::new(1, 0, 0));
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
