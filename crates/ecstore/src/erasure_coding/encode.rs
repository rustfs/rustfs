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
use crate::erasure_coding::BitrotWriterWrapper;
use crate::erasure_coding::Erasure;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::sync::Arc;
use std::vec;
use tokio::io::AsyncRead;
use tokio::sync::mpsc;
use tracing::error;

const ENV_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES: &str = "RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES";
const DEFAULT_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES: usize = 32 * 1024 * 1024;
const DEFAULT_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BLOCKS: usize = 32;

fn encode_channel_capacity(expanded_block_bytes: usize, max_inflight_bytes: usize) -> usize {
    if expanded_block_bytes == 0 {
        return 1;
    }

    max_inflight_bytes
        .saturating_div(expanded_block_bytes)
        .clamp(1, DEFAULT_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BLOCKS)
}

fn queued_block_bytes(block: &[Bytes]) -> usize {
    block.iter().map(Bytes::len).sum()
}

async fn drain_queued_inflight_bytes(rx: &mut mpsc::Receiver<Vec<Bytes>>) {
    while let Some(block) = rx.recv().await {
        rustfs_io_metrics::remove_ec_encode_inflight_bytes(queued_block_bytes(&block));
    }
}

fn format_write_quorum_failure(summary: &WriteQuorumFailureSummary) -> String {
    let dominant_error = summary
        .dominant_error
        .as_ref()
        .map(ToString::to_string)
        .unwrap_or_else(|| "successful writes dominated before quorum failed".to_string());
    format!(
        "erasure write quorum (required={}, achieved={}, failed={}, total={}, offline-disks={}/{}, retryable-failures={}, dominant-error={})",
        summary.required,
        summary.achieved,
        summary.failed,
        summary.total,
        summary.offline_disks,
        summary.total,
        summary.retryable_failures,
        dominant_error
    )
}

fn quorum_dominant_error_metric_label(summary: &WriteQuorumFailureSummary) -> &'static str {
    match summary.dominant_error.as_ref() {
        Some(Error::DiskNotFound) => "disk_not_found",
        Some(Error::ShortWrite) => "short_write",
        Some(err) => match err.internode_http_error_kind() {
            Some(kind) => kind.metric_label(),
            None => "other_error",
        },
        None => "nil_dominated",
    }
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
            rustfs_io_metrics::internode_metrics::global_internode_metrics()
                .record_erasure_write_quorum_failure("write", quorum_dominant_error_metric_label(&summary));
            error!("reduce_write_quorum_errs: {:?}, {}, errs={:?}", write_err, summary_text, self.errs);
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
            rustfs_io_metrics::internode_metrics::global_internode_metrics()
                .record_erasure_write_quorum_failure("shutdown", quorum_dominant_error_metric_label(&summary));
            error!(
                "reduce_write_quorum_errs during shutdown: {:?}, {}, errs={:?}",
                write_err, summary_text, self.errs
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
        let total = reader.read_to_end(&mut buf).await?;

        if total == 0 {
            return Ok((reader, 0));
        }

        if require_single_block {
            debug_assert!(total <= self.block_size, "single-block non-inline fast path expects total <= block_size");
        }

        let shards = self.encode_data(&buf)?;
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
        let max_inflight_bytes = rustfs_utils::get_env_usize(
            ENV_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES,
            DEFAULT_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BYTES,
        );
        let inflight_blocks = encode_channel_capacity(expanded_block_bytes, max_inflight_bytes);
        let (tx, mut rx) = mpsc::channel::<Vec<Bytes>>(inflight_blocks);

        let task = tokio::spawn(async move {
            let block_size = self.block_size;
            let mut total = 0;
            let mut buf = vec![0u8; block_size];
            loop {
                match rustfs_utils::read_full_or_eof(&mut reader, &mut buf).await {
                    Ok(Some(n)) => {
                        debug_assert!(n > 0, "non-zero block_size prevents zero-length reads");
                        total += n;
                        let erasure = self.clone();
                        let encode_buf = std::mem::take(&mut buf);
                        let (res, returned_buf) = tokio::task::spawn_blocking(move || {
                            let res = erasure.encode_data(&encode_buf[..n]);
                            (res, encode_buf)
                        })
                        .await
                        .map_err(|err| std::io::Error::other(format!("EC encode task failed: {err}")))?;
                        buf = returned_buf;
                        let res = res?;
                        let queued_bytes = queued_block_bytes(&res);
                        rustfs_io_metrics::add_ec_encode_inflight_bytes(queued_bytes);
                        if let Err(err) = tx.send(res).await {
                            rustfs_io_metrics::remove_ec_encode_inflight_bytes(queued_bytes);
                            return Err(std::io::Error::other(format!("Failed to send encoded data : {err}")));
                        }
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

            Ok((reader, total))
        });

        let mut writers = MultiWriter::new(writers, quorum);

        let mut write_err = None;

        while let Some(block) = rx.recv().await {
            if block.is_empty() {
                break;
            }
            let queued_bytes = queued_block_bytes(&block);
            rustfs_io_metrics::remove_ec_encode_inflight_bytes(queued_bytes);
            if let Err(err) = writers.write(block).await {
                write_err = Some(err);
                break;
            }
        }

        if let Some(err) = write_err {
            task.abort();
            let _ = task.await;
            drain_queued_inflight_bytes(&mut rx).await;
            if let Err(shutdown_err) = writers.shutdown().await {
                error!("failed to shutdown erasure writers after write error: {:?}", shutdown_err);
            }
            return Err(err);
        }

        let (reader, total) = task.await??;
        writers.shutdown().await?;
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
    use crate::erasure_coding::{BitrotWriterWrapper, CustomWriter};
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

    #[tokio::test]
    async fn encode_shutdowns_writers_after_small_shards() {
        let committed = Arc::new(Mutex::new(Vec::new()));
        let writer = DeferredCommitWriter::new(committed.clone());
        let mut writers = vec![Some(BitrotWriterWrapper::new(
            CustomWriter::new_tokio_writer(writer),
            16,
            HashAlgorithm::HighwayHash256S,
        ))];

        let erasure = Arc::new(Erasure::new(1, 0, 16));
        let reader = tokio::io::BufReader::new(std::io::Cursor::new(b"small payload".to_vec()));
        let (_reader, written) = erasure.encode(reader, &mut writers, 1).await.unwrap();

        assert_eq!(written, b"small payload".len());
        assert!(!committed.lock().unwrap().is_empty());
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
        let reader = tokio::io::BufReader::new(std::io::Cursor::new(b"payload".to_vec()));
        let err = erasure
            .encode(reader, &mut writers, 1)
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
        let reader = tokio::io::BufReader::new(std::io::Cursor::new(Vec::<u8>::new()));
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
        let reader = tokio::io::BufReader::new(std::io::Cursor::new(payload.to_vec()));
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
        let reader = tokio::io::BufReader::new(std::io::Cursor::new(payload.to_vec()));
        let (_reader, total) = erasure
            .encode_single_block_non_inline(reader, &mut writers, DATA_SHARDS)
            .await
            .unwrap();

        assert_eq!(total, payload.len());
        for (i, c) in committed.iter().enumerate() {
            assert!(!c.lock().unwrap().is_empty(), "shard {i} should have received data");
        }
    }

    #[test]
    fn encode_channel_capacity_never_returns_zero() {
        assert_eq!(encode_channel_capacity(0, 1024), 1);
        assert_eq!(encode_channel_capacity(4096, 0), 1);
        assert_eq!(encode_channel_capacity(4096, 1024), 1);
    }

    #[test]
    fn encode_channel_capacity_respects_budget_and_hard_cap() {
        assert_eq!(encode_channel_capacity(4 * 1024 * 1024, 32 * 1024 * 1024), 8);
        assert_eq!(encode_channel_capacity(1536 * 1024, 32 * 1024 * 1024), 21);
        assert_eq!(encode_channel_capacity(16 * 1024 * 1024, 32 * 1024 * 1024), 2);
        assert_eq!(encode_channel_capacity(1, usize::MAX), DEFAULT_RUSTFS_ERASURE_ENCODE_MAX_INFLIGHT_BLOCKS);
    }
}
