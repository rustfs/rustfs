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

use crate::disk::error::Error as DiskError;
use crate::erasure_codec::bridge::ErasureDecodeEngine;
use crate::get_diagnostics::{
    GET_OBJECT_PATH_CODEC_STREAMING, GET_READER_BUFFER_OUTPUT, GET_READER_BUFFER_PREFETCH, GET_READER_POLL_PENDING,
    GET_READER_POLL_READY_DATA, GET_READER_POLL_READY_EMPTY, GET_READER_POLL_READY_ERROR, GET_READER_PREFETCH_DIRECT,
    GET_READER_PREFETCH_EOF, GET_READER_PREFETCH_ERROR_DEFERRED, GET_READER_PREFETCH_ERROR_IMMEDIATE, GET_READER_PREFETCH_STORED,
    GET_STAGE_DECODE, GET_STAGE_EMIT, GET_STAGE_FILL, GET_STAGE_OUTPUT_LOCK_WAIT, GET_STAGE_OUTPUT_POLL, GET_STAGE_RECONSTRUCT,
    GET_STAGE_STRIPE_READ,
};
use crate::set_disk::shard_source::{ShardStripeSource, StripeReadState};
use std::io;
use std::io::ErrorKind;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll, ready};
use std::time::Instant;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::task::JoinHandle;

type FillTask<S, W> = JoinHandle<FillResult<S, W>>;

struct FillResult<S, W> {
    source: S,
    workspace: W,
    result: io::Result<Option<Vec<u8>>>,
}

pub(crate) struct ErasureDecodeReader<S, E>
where
    E: ErasureDecodeEngine,
{
    source: Option<S>,
    engine: E,
    workspace: Option<E::Workspace>,
    output_buf: Vec<u8>,
    output_pos: usize,
    prefetched_buf: Option<Vec<u8>>,
    prefetch_error: Option<io::Error>,
    prefetch_wait_started_at: Option<Instant>,
    remaining: usize,
    // Bounded lookahead: at most one background stripe read/decode is in flight.
    fill: Option<FillTask<S, E::Workspace>>,
}

impl<S, E> ErasureDecodeReader<S, E>
where
    S: ShardStripeSource + Send + 'static,
    E: ErasureDecodeEngine + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(source: S, engine: E, total_length: usize) -> io::Result<Self> {
        if engine.data_shards() == 0 {
            return Err(io::Error::new(ErrorKind::InvalidInput, "erasure reader requires data shards"));
        }
        if engine.block_size() == 0 {
            return Err(io::Error::new(ErrorKind::InvalidInput, "erasure reader requires non-zero block size"));
        }

        let shard_len = engine.block_size().div_ceil(engine.data_shards());
        let workspace = engine.prepare_workspace(shard_len)?;

        Ok(Self {
            source: Some(source),
            engine,
            workspace: Some(workspace),
            output_buf: Vec::new(),
            output_pos: 0,
            prefetched_buf: None,
            prefetch_error: None,
            prefetch_wait_started_at: None,
            remaining: total_length,
            fill: None,
        })
    }

    fn poll_fill_result(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<Option<Vec<u8>>>> {
        if self.fill.is_none() {
            let Some(mut source) = self.source.take() else {
                return Poll::Ready(Err(io::Error::new(ErrorKind::BrokenPipe, "erasure reader source missing")));
            };
            let Some(mut workspace) = self.workspace.take() else {
                self.source = Some(source);
                return Poll::Ready(Err(io::Error::new(ErrorKind::BrokenPipe, "erasure reader workspace missing")));
            };

            let engine = self.engine.clone();
            let remaining = self.remaining;
            self.fill = Some(tokio::spawn(async move {
                let fill_stage_start = Instant::now();
                let stripe_read_stage_start = Instant::now();
                let state = source.read_next_stripe().await;
                rustfs_io_metrics::record_get_object_stage_duration(
                    GET_OBJECT_PATH_CODEC_STREAMING,
                    GET_STAGE_STRIPE_READ,
                    stripe_read_stage_start.elapsed().as_secs_f64(),
                );
                let decode_stage_start = Instant::now();
                let result = decode_stripe(&engine, &mut workspace, state, remaining);
                rustfs_io_metrics::record_get_object_stage_duration(
                    GET_OBJECT_PATH_CODEC_STREAMING,
                    GET_STAGE_DECODE,
                    decode_stage_start.elapsed().as_secs_f64(),
                );
                rustfs_io_metrics::record_get_object_stage_duration(
                    GET_OBJECT_PATH_CODEC_STREAMING,
                    GET_STAGE_FILL,
                    fill_stage_start.elapsed().as_secs_f64(),
                );
                FillResult {
                    source,
                    workspace,
                    result,
                }
            }));
        }

        let fill = self
            .fill
            .as_mut()
            .ok_or_else(|| io::Error::new(ErrorKind::BrokenPipe, "erasure reader fill future missing"))?;
        let fill_result = ready!(Pin::new(fill).poll(cx));
        let FillResult {
            source,
            workspace,
            result,
        } = match fill_result {
            Ok(result) => result,
            Err(err) => {
                self.fill = None;
                return Poll::Ready(Err(io::Error::other(format!("erasure reader fill task failed: {err}"))));
            }
        };

        self.source = Some(source);
        self.workspace = Some(workspace);
        self.fill = None;

        match result {
            Ok(Some(buf)) => {
                if buf.is_empty() && self.remaining > 0 {
                    return Poll::Ready(Err(DiskError::LessData.into()));
                }
                rustfs_io_metrics::record_get_object_reader_stripe(GET_OBJECT_PATH_CODEC_STREAMING);
                rustfs_io_metrics::record_get_object_reader_bytes(GET_OBJECT_PATH_CODEC_STREAMING, buf.len());
                self.remaining -= buf.len();
                Poll::Ready(Ok(Some(buf)))
            }
            Ok(None) => {
                if self.remaining == 0 {
                    Poll::Ready(Ok(None))
                } else {
                    Poll::Ready(Err(DiskError::LessData.into()))
                }
            }
            Err(err) => Poll::Ready(Err(err)),
        }
    }

    fn poll_prefetch(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.prefetched_buf.is_some() || self.prefetch_error.is_some() || self.remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        if self.prefetch_wait_started_at.is_none() {
            self.prefetch_wait_started_at = Some(Instant::now());
        }

        let fill = match self.poll_fill_result(cx) {
            Poll::Ready(result) => {
                if let Some(started_at) = self.prefetch_wait_started_at.take() {
                    rustfs_io_metrics::record_get_object_reader_prefetch_wait(
                        GET_OBJECT_PATH_CODEC_STREAMING,
                        started_at.elapsed().as_secs_f64(),
                    );
                }
                result
            }
            Poll::Pending => return Poll::Pending,
        };

        match fill {
            Ok(Some(buf)) => {
                if self.output_pos < self.output_buf.len() {
                    rustfs_io_metrics::record_get_object_reader_prefetch(
                        GET_OBJECT_PATH_CODEC_STREAMING,
                        GET_READER_PREFETCH_STORED,
                    );
                    rustfs_io_metrics::record_get_object_reader_buffer(
                        GET_OBJECT_PATH_CODEC_STREAMING,
                        GET_READER_BUFFER_PREFETCH,
                        buf.len(),
                    );
                    self.prefetched_buf = Some(buf);
                } else {
                    rustfs_io_metrics::record_get_object_reader_prefetch(
                        GET_OBJECT_PATH_CODEC_STREAMING,
                        GET_READER_PREFETCH_DIRECT,
                    );
                    rustfs_io_metrics::record_get_object_reader_buffer(
                        GET_OBJECT_PATH_CODEC_STREAMING,
                        GET_READER_BUFFER_OUTPUT,
                        buf.len(),
                    );
                    self.output_buf = buf;
                    self.output_pos = 0;
                }
                Poll::Ready(Ok(()))
            }
            Ok(None) => {
                rustfs_io_metrics::record_get_object_reader_prefetch(GET_OBJECT_PATH_CODEC_STREAMING, GET_READER_PREFETCH_EOF);
                Poll::Ready(Ok(()))
            }
            Err(err) => {
                if self.output_pos < self.output_buf.len() {
                    rustfs_io_metrics::record_get_object_reader_prefetch(
                        GET_OBJECT_PATH_CODEC_STREAMING,
                        GET_READER_PREFETCH_ERROR_DEFERRED,
                    );
                    self.prefetch_error = Some(err);
                    Poll::Ready(Ok(()))
                } else {
                    rustfs_io_metrics::record_get_object_reader_prefetch(
                        GET_OBJECT_PATH_CODEC_STREAMING,
                        GET_READER_PREFETCH_ERROR_IMMEDIATE,
                    );
                    Poll::Ready(Err(err))
                }
            }
        }
    }
}

impl<S, E> Drop for ErasureDecodeReader<S, E>
where
    E: ErasureDecodeEngine,
{
    fn drop(&mut self) {
        if let Some(fill) = self.fill.take() {
            fill.abort();
        }
    }
}

impl<S, E> Unpin for ErasureDecodeReader<S, E> where E: ErasureDecodeEngine {}

impl<S, E> AsyncRead for ErasureDecodeReader<S, E>
where
    S: ShardStripeSource + Send + 'static,
    E: ErasureDecodeEngine + Clone + Send + Sync + 'static,
{
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        loop {
            if self.output_pos < self.output_buf.len() {
                if self.prefetched_buf.is_none()
                    && self.prefetch_error.is_none()
                    && self.remaining > 0
                    && let Poll::Ready(result) = self.poll_prefetch(cx)
                {
                    result?;
                }

                let available = &self.output_buf[self.output_pos..];
                let read_buf_remaining_before = buf.remaining();
                let output_remaining_before = available.len();
                let copy_len = available.len().min(buf.remaining());
                let copy_start = Instant::now();
                buf.put_slice(&available[..copy_len]);
                self.output_pos += copy_len;
                if copy_len > 0 {
                    rustfs_io_metrics::record_get_object_reader_copy(
                        GET_OBJECT_PATH_CODEC_STREAMING,
                        copy_len,
                        read_buf_remaining_before,
                        output_remaining_before,
                        copy_start.elapsed().as_secs_f64(),
                    );
                }
                return Poll::Ready(Ok(()));
            }

            if let Some(next_buf) = self.prefetched_buf.take() {
                rustfs_io_metrics::record_get_object_reader_buffer(
                    GET_OBJECT_PATH_CODEC_STREAMING,
                    GET_READER_BUFFER_OUTPUT,
                    next_buf.len(),
                );
                self.output_buf = next_buf;
                self.output_pos = 0;
                continue;
            }

            if let Some(err) = self.prefetch_error.take() {
                return Poll::Ready(Err(err));
            }

            if self.remaining == 0 {
                return Poll::Ready(Ok(()));
            }

            ready!(self.poll_prefetch(cx))?;
        }
    }
}

pub(crate) struct SyncErasureDecodeReader<R> {
    inner: Mutex<R>,
}

impl<R> SyncErasureDecodeReader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self {
            inner: Mutex::new(inner),
        }
    }
}

impl<R> AsyncRead for SyncErasureDecodeReader<R>
where
    R: AsyncRead + Unpin + Send,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let lock_wait_start = Instant::now();
        let mut inner = match self.inner.lock() {
            Ok(inner) => {
                rustfs_io_metrics::record_get_object_stage_duration(
                    GET_OBJECT_PATH_CODEC_STREAMING,
                    GET_STAGE_OUTPUT_LOCK_WAIT,
                    lock_wait_start.elapsed().as_secs_f64(),
                );
                inner
            }
            Err(_) => {
                rustfs_io_metrics::record_get_object_stage_duration(
                    GET_OBJECT_PATH_CODEC_STREAMING,
                    GET_STAGE_OUTPUT_LOCK_WAIT,
                    lock_wait_start.elapsed().as_secs_f64(),
                );
                return Poll::Ready(Err(io::Error::other("erasure decode reader lock poisoned")));
            }
        };
        let read_buf_remaining_before = buf.remaining();
        let filled_before = buf.filled().len();
        let poll_start = Instant::now();
        let result = Pin::new(&mut *inner).poll_read(cx, buf);
        let poll_duration = poll_start.elapsed().as_secs_f64();
        let filled_bytes = buf.filled().len().saturating_sub(filled_before);
        let poll_outcome = match &result {
            Poll::Ready(Ok(())) if filled_bytes > 0 => GET_READER_POLL_READY_DATA,
            Poll::Ready(Ok(())) => GET_READER_POLL_READY_EMPTY,
            Poll::Ready(Err(_)) => GET_READER_POLL_READY_ERROR,
            Poll::Pending => GET_READER_POLL_PENDING,
        };
        rustfs_io_metrics::record_get_object_stage_duration(
            GET_OBJECT_PATH_CODEC_STREAMING,
            GET_STAGE_OUTPUT_POLL,
            poll_duration,
        );
        rustfs_io_metrics::record_get_object_reader_poll(
            GET_OBJECT_PATH_CODEC_STREAMING,
            poll_outcome,
            read_buf_remaining_before,
            filled_bytes,
            poll_duration,
        );
        result
    }
}

fn decode_stripe<E>(
    engine: &E,
    workspace: &mut E::Workspace,
    state: StripeReadState,
    remaining: usize,
) -> io::Result<Option<Vec<u8>>>
where
    E: ErasureDecodeEngine,
{
    if state.slots().is_empty() {
        return Ok(None);
    }
    if !state.can_decode() {
        return Err(DiskError::ErasureReadQuorum.into());
    }

    let reconstruct_stage_start = Instant::now();
    if state.data_shards_complete(engine.data_shards()) {
        rustfs_io_metrics::record_get_object_stage_duration(
            GET_OBJECT_PATH_CODEC_STREAMING,
            GET_STAGE_RECONSTRUCT,
            reconstruct_stage_start.elapsed().as_secs_f64(),
        );
        let emit_stage_start = Instant::now();
        let output = emit_data_shards(&state, engine.data_shards(), engine.block_size(), remaining)?;
        rustfs_io_metrics::record_get_object_stage_duration(
            GET_OBJECT_PATH_CODEC_STREAMING,
            GET_STAGE_EMIT,
            emit_stage_start.elapsed().as_secs_f64(),
        );
        return Ok(Some(output));
    }

    let (mut shards, _errs) = state.into_parts();
    if let Err(err) = engine.reconstruct_into(&mut shards, workspace) {
        rustfs_io_metrics::record_get_object_stage_duration(
            GET_OBJECT_PATH_CODEC_STREAMING,
            GET_STAGE_RECONSTRUCT,
            reconstruct_stage_start.elapsed().as_secs_f64(),
        );
        return Err(err);
    }
    rustfs_io_metrics::record_get_object_stage_duration(
        GET_OBJECT_PATH_CODEC_STREAMING,
        GET_STAGE_RECONSTRUCT,
        reconstruct_stage_start.elapsed().as_secs_f64(),
    );

    if shards.len() < engine.data_shards() {
        return Err(io::Error::new(
            ErrorKind::UnexpectedEof,
            "decoded stripe has fewer shards than data shard count",
        ));
    }

    let emit_stage_start = Instant::now();
    let mut output = Vec::with_capacity(engine.block_size().min(remaining));
    for shard in shards.iter().take(engine.data_shards()) {
        if output.len() >= remaining {
            break;
        }
        let Some(shard) = shard else {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "decoded stripe is missing a data shard"));
        };
        let copy_len = shard.len().min(remaining - output.len());
        output.extend_from_slice(&shard[..copy_len]);
    }
    rustfs_io_metrics::record_get_object_stage_duration(
        GET_OBJECT_PATH_CODEC_STREAMING,
        GET_STAGE_EMIT,
        emit_stage_start.elapsed().as_secs_f64(),
    );

    Ok(Some(output))
}

fn emit_data_shards(state: &StripeReadState, data_shards: usize, block_size: usize, remaining: usize) -> io::Result<Vec<u8>> {
    let mut output = Vec::with_capacity(block_size.min(remaining));
    for index in 0..data_shards {
        if output.len() >= remaining {
            break;
        }
        let Some(slot) = state.slot_by_index(index) else {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "decoded stripe is missing a data shard"));
        };
        let Some(shard) = slot.data_bytes() else {
            return Err(io::Error::new(ErrorKind::UnexpectedEof, "decoded stripe is missing a data shard"));
        };
        let copy_len = shard.len().min(remaining - output.len());
        output.extend_from_slice(&shard[..copy_len]);
    }
    Ok(output)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::erasure_codec::bridge::LegacyEcDecodeEngine;
    use crate::erasure_coding::Erasure;
    use crate::set_disk::shard_source::{ShardSlot, StripeReadState};
    use std::collections::VecDeque;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::io::AsyncReadExt;
    use tokio::task::yield_now;
    use tokio::time::{Duration, timeout};

    struct VecStripeSource {
        stripes: VecDeque<StripeReadState>,
        read_quorum: usize,
        read_count: Option<Arc<AtomicUsize>>,
    }

    #[async_trait::async_trait]
    impl ShardStripeSource for VecStripeSource {
        async fn read_next_stripe(&mut self) -> StripeReadState {
            if let Some(read_count) = &self.read_count {
                read_count.fetch_add(1, Ordering::SeqCst);
            }
            self.stripes
                .pop_front()
                .unwrap_or_else(|| StripeReadState::new(Vec::new(), self.read_quorum))
        }
    }

    fn source_from_data(erasure: &Erasure, data: &[u8], missing_indexes: &[usize]) -> VecStripeSource {
        let read_quorum = erasure.data_shards;
        let stripes = data
            .chunks(erasure.block_size)
            .map(|chunk| {
                let shards = erasure
                    .encode_data(chunk)
                    .expect("test stripe should encode")
                    .into_iter()
                    .enumerate()
                    .map(|(index, shard)| {
                        if missing_indexes.contains(&index) {
                            None
                        } else {
                            Some(shard.to_vec())
                        }
                    })
                    .collect();
                StripeReadState::from_parts(shards, Vec::new(), read_quorum)
            })
            .collect();

        VecStripeSource {
            stripes,
            read_quorum,
            read_count: None,
        }
    }

    async fn decode_all(erasure: Erasure, data: &[u8], missing_indexes: &[usize]) -> io::Result<Vec<u8>> {
        let source = source_from_data(&erasure, data, missing_indexes);
        let engine = LegacyEcDecodeEngine::new(erasure);
        let mut reader = ErasureDecodeReader::new(source, engine, data.len())?;
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).await?;
        Ok(decoded)
    }

    #[tokio::test]
    async fn erasure_decode_reader_reads_single_stripe() {
        let erasure = Erasure::new(4, 2, 64);
        let data = b"single stripe decode reader output";

        let decoded = decode_all(erasure, data, &[])
            .await
            .expect("single stripe reader should decode");

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn erasure_decode_reader_reads_multiple_stripes() {
        let erasure = Erasure::new(4, 2, 32);
        let data = (0..150u16).map(|value| value.to_le_bytes()[0]).collect::<Vec<_>>();

        let decoded = decode_all(erasure, &data, &[])
            .await
            .expect("multi stripe reader should decode");

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn erasure_decode_reader_stops_at_eof_for_empty_object() {
        let erasure = Erasure::new(4, 2, 32);
        let source = source_from_data(&erasure, &[], &[]);
        let engine = LegacyEcDecodeEngine::new(erasure);
        let mut reader = ErasureDecodeReader::new(source, engine, 0).expect("empty reader should be constructed");
        let mut decoded = Vec::new();

        let read = reader
            .read_to_end(&mut decoded)
            .await
            .expect("empty reader should finish without reading stripes");

        assert_eq!(read, 0);
        assert!(decoded.is_empty());
    }

    #[tokio::test]
    async fn erasure_decode_reader_reconstructs_missing_data_shard() {
        let erasure = Erasure::new(4, 2, 32);
        let data = (0..120u16)
            .map(|value| value.wrapping_mul(17).to_le_bytes()[0])
            .collect::<Vec<_>>();

        let decoded = decode_all(erasure, &data, &[1])
            .await
            .expect("reader should reconstruct one missing data shard");

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn erasure_decode_reader_reads_when_only_parity_shards_are_missing() {
        let erasure = Erasure::new(4, 2, 32);
        let data = (0..120u16)
            .map(|value| value.wrapping_mul(11).to_le_bytes()[0])
            .collect::<Vec<_>>();

        let decoded = decode_all(erasure, &data, &[4, 5])
            .await
            .expect("reader should emit complete data shards without parity reconstruction");

        assert_eq!(decoded, data);
    }

    #[test]
    fn emit_data_shards_preserves_output_order_for_out_of_order_slots() {
        let state = StripeReadState::new(
            vec![
                ShardSlot::data(1, b"cd".to_vec()),
                ShardSlot::data(0, b"ab".to_vec()),
                ShardSlot::data(2, b"ef".to_vec()),
            ],
            2,
        );

        let output = emit_data_shards(&state, 3, 6, 5).expect("out-of-order data slots should emit by shard index");

        assert_eq!(output, b"abcde");
    }

    #[tokio::test]
    async fn erasure_decode_reader_reports_short_source() {
        let erasure = Erasure::new(4, 2, 32);
        let source = VecStripeSource {
            stripes: VecDeque::new(),
            read_quorum: erasure.data_shards,
            read_count: None,
        };
        let engine = LegacyEcDecodeEngine::new(erasure);
        let mut reader = ErasureDecodeReader::new(source, engine, 1).expect("reader should be constructed");
        let mut decoded = Vec::new();

        let err = reader
            .read_to_end(&mut decoded)
            .await
            .expect_err("reader should reject EOF before requested length");

        assert_eq!(err.kind(), ErrorKind::Other);
        assert!(decoded.is_empty());
    }

    #[tokio::test]
    async fn erasure_decode_reader_prefetches_next_stripe_while_output_remains() {
        let erasure = Erasure::new(4, 2, 32);
        let data = (0..96u16)
            .map(|value| value.wrapping_mul(3).to_le_bytes()[0])
            .collect::<Vec<_>>();
        let read_count = Arc::new(AtomicUsize::new(0));
        let mut source = source_from_data(&erasure, &data, &[]);
        source.read_count = Some(Arc::clone(&read_count));
        let engine = LegacyEcDecodeEngine::new(erasure);
        let mut reader = ErasureDecodeReader::new(source, engine, data.len()).expect("reader should be constructed");
        let mut first_read = [0u8; 1];

        let read = reader.read(&mut first_read).await.expect("first read should succeed");

        assert_eq!(read, first_read.len());
        assert_eq!(first_read[0], data[0]);
        timeout(Duration::from_secs(1), async {
            while read_count.load(Ordering::SeqCst) < 2 {
                yield_now().await;
            }
        })
        .await
        .expect("reader should start reading the next stripe before the current output buffer is fully consumed");
    }
}
