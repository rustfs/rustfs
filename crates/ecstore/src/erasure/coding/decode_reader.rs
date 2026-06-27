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
    GET_OBJECT_PATH_CODEC_STREAMING, GET_READER_BUFFER_OUTPUT, GET_READER_BUFFER_PREFETCH, GET_READER_POLL_PENDING,
    GET_READER_POLL_READY_DATA, GET_READER_POLL_READY_EMPTY, GET_READER_POLL_READY_ERROR, GET_READER_PREFETCH_DIRECT,
    GET_READER_PREFETCH_EOF, GET_READER_PREFETCH_ERROR_DEFERRED, GET_READER_PREFETCH_ERROR_IMMEDIATE, GET_READER_PREFETCH_STORED,
    GET_STAGE_DECODE, GET_STAGE_EMIT, GET_STAGE_FILL, GET_STAGE_OUTPUT_LOCK_WAIT, GET_STAGE_OUTPUT_POLL, GET_STAGE_RECONSTRUCT,
    GET_STAGE_STRIPE_READ, get_stage_timer_if_enabled, record_get_stage_duration_if_enabled,
};
use crate::disk::error::Error as DiskError;
use crate::erasure::codec::bridge::ErasureDecodeEngine;
use crate::set_disk::shard_source::{ShardStripeSource, StripeReadState};
use std::collections::VecDeque;
use std::io;
use std::io::ErrorKind;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::{Context, Poll, ready};
use std::time::Instant;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::task::JoinHandle;

const ENV_RUSTFS_GET_CODEC_STREAMING_MAX_INFLIGHT: &str = "RUSTFS_GET_CODEC_STREAMING_MAX_INFLIGHT";
const DEFAULT_RUSTFS_GET_CODEC_STREAMING_MAX_INFLIGHT: usize = 2;
const FILL_POLICY_SINGLE_INFLIGHT: &str = "single_inflight";
const FILL_POLICY_DUAL_INFLIGHT: &str = "dual_inflight";

type FillTask<S, W> = JoinHandle<FillResult<S, W>>;

struct FillResult<S, W> {
    source: S,
    workspace: W,
    result: io::Result<Option<Vec<u8>>>,
    queued_buffers: VecDeque<Vec<u8>>,
    deferred_error: Option<io::Error>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FillPolicy {
    SingleInFlight,
    DualInFlight,
}

impl FillPolicy {
    fn from_env() -> Self {
        match rustfs_utils::get_env_usize(
            ENV_RUSTFS_GET_CODEC_STREAMING_MAX_INFLIGHT,
            DEFAULT_RUSTFS_GET_CODEC_STREAMING_MAX_INFLIGHT,
        ) {
            2 => Self::DualInFlight,
            _ => Self::SingleInFlight,
        }
    }

    const fn max_inflight(self) -> usize {
        match self {
            Self::SingleInFlight => 1,
            Self::DualInFlight => 2,
        }
    }

    const fn additional_queued_buffers(self) -> usize {
        self.max_inflight().saturating_sub(1)
    }

    const fn as_str(self) -> &'static str {
        match self {
            Self::SingleInFlight => FILL_POLICY_SINGLE_INFLIGHT,
            Self::DualInFlight => FILL_POLICY_DUAL_INFLIGHT,
        }
    }
}

pub(crate) struct ErasureDecodeReader<S, E>
where
    E: ErasureDecodeEngine,
{
    metrics_path: &'static str,
    stage_metrics_enabled: bool,
    fill_policy: FillPolicy,
    source: Option<S>,
    engine: E,
    workspace: Option<E::Workspace>,
    output_buf: Vec<u8>,
    output_pos: usize,
    prefetched_bufs: VecDeque<Vec<u8>>,
    prefetch_error: Option<io::Error>,
    prefetch_wait_started_at: Option<Instant>,
    output_wait_started_at: Option<Instant>,
    remaining: usize,
    // Bounded lookahead controlled by `FillPolicy`.
    fill: Option<FillTask<S, E::Workspace>>,
}

impl<S, E> ErasureDecodeReader<S, E>
where
    S: ShardStripeSource + Send + 'static,
    E: ErasureDecodeEngine + Clone + Send + Sync + 'static,
{
    pub(crate) fn new(source: S, engine: E, total_length: usize) -> io::Result<Self> {
        Self::new_with_metrics_path(source, engine, total_length, GET_OBJECT_PATH_CODEC_STREAMING)
    }

    pub(crate) fn new_with_metrics_path(
        source: S,
        engine: E,
        total_length: usize,
        metrics_path: &'static str,
    ) -> io::Result<Self> {
        Self::new_with_fill_policy_inner(source, engine, total_length, metrics_path, FillPolicy::from_env())
    }

    fn new_with_fill_policy_inner(
        source: S,
        engine: E,
        total_length: usize,
        metrics_path: &'static str,
        fill_policy: FillPolicy,
    ) -> io::Result<Self> {
        if engine.data_shards() == 0 {
            return Err(io::Error::new(ErrorKind::InvalidInput, "erasure reader requires data shards"));
        }
        if engine.block_size() == 0 {
            return Err(io::Error::new(ErrorKind::InvalidInput, "erasure reader requires non-zero block size"));
        }

        let shard_len = engine.block_size().div_ceil(engine.data_shards());
        let workspace = engine.prepare_workspace(shard_len)?;

        Ok(Self {
            metrics_path,
            stage_metrics_enabled: rustfs_io_metrics::get_stage_metrics_enabled(),
            fill_policy,
            source: Some(source),
            engine,
            workspace: Some(workspace),
            output_buf: Vec::new(),
            output_pos: 0,
            prefetched_bufs: VecDeque::new(),
            prefetch_error: None,
            prefetch_wait_started_at: None,
            output_wait_started_at: None,
            remaining: total_length,
            fill: None,
        })
    }

    #[cfg(test)]
    fn new_with_fill_policy(
        source: S,
        engine: E,
        total_length: usize,
        metrics_path: &'static str,
        fill_policy: FillPolicy,
    ) -> io::Result<Self> {
        Self::new_with_fill_policy_inner(source, engine, total_length, metrics_path, fill_policy)
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
            let metrics_path = self.metrics_path;
            let stage_metrics_enabled = self.stage_metrics_enabled;
            let fill_policy = self.fill_policy;
            let remaining = self.remaining;
            self.fill = Some(tokio::spawn(async move {
                let mut queued_buffers = VecDeque::new();
                let mut deferred_error = None;
                let fill_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
                let stripe_read_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
                let state = source.read_next_stripe().await;
                record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_STRIPE_READ, stripe_read_stage_start);
                let decode_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
                let result = decode_stripe(metrics_path, stage_metrics_enabled, &engine, &mut workspace, state, remaining);
                record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_DECODE, decode_stage_start);
                if let Ok(Some(first_buf)) = result.as_ref() {
                    let mut remaining_after_first = remaining.saturating_sub(first_buf.len());
                    for _ in 0..fill_policy.additional_queued_buffers() {
                        if remaining_after_first == 0 {
                            break;
                        }
                        let stripe_read_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
                        let state = source.read_next_stripe().await;
                        record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_STRIPE_READ, stripe_read_stage_start);
                        let decode_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
                        let queued_result = decode_stripe(
                            metrics_path,
                            stage_metrics_enabled,
                            &engine,
                            &mut workspace,
                            state,
                            remaining_after_first,
                        );
                        record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_DECODE, decode_stage_start);
                        match queued_result {
                            Ok(Some(buf)) => {
                                remaining_after_first = remaining_after_first.saturating_sub(buf.len());
                                queued_buffers.push_back(buf);
                            }
                            Ok(None) => {
                                if remaining_after_first > 0 {
                                    deferred_error = Some(DiskError::LessData.into());
                                }
                                break;
                            }
                            Err(err) => {
                                deferred_error = Some(err);
                                break;
                            }
                        }
                    }
                }
                record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_FILL, fill_stage_start);
                FillResult {
                    source,
                    workspace,
                    result,
                    queued_buffers,
                    deferred_error,
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
            queued_buffers,
            deferred_error,
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
        if let Some(deferred_error) = deferred_error {
            self.prefetch_error = Some(deferred_error);
        }
        self.prefetched_bufs.extend(queued_buffers);

        match result {
            Ok(Some(buf)) => {
                if buf.is_empty() && self.remaining > 0 {
                    return Poll::Ready(Err(DiskError::LessData.into()));
                }
                rustfs_io_metrics::record_get_object_reader_stripe(self.metrics_path);
                rustfs_io_metrics::record_get_object_reader_bytes(self.metrics_path, buf.len());
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
        if self.prefetched_bufs.len() >= self.fill_policy.max_inflight() || self.prefetch_error.is_some() || self.remaining == 0 {
            return Poll::Ready(Ok(()));
        }

        if self.stage_metrics_enabled && self.prefetch_wait_started_at.is_none() {
            self.prefetch_wait_started_at = Some(Instant::now());
        }

        let fill = match self.poll_fill_result(cx) {
            Poll::Ready(result) => {
                if self.stage_metrics_enabled
                    && let Some(started_at) = self.prefetch_wait_started_at.take()
                {
                    rustfs_io_metrics::record_get_object_reader_prefetch_wait(
                        self.metrics_path,
                        started_at.elapsed().as_secs_f64(),
                    );
                }
                result
            }
            Poll::Pending => return Poll::Pending,
        };

        match fill {
            Ok(Some(buf)) => {
                let output_has_remaining = self.output_pos < self.output_buf.len();
                let mut queued_count = 0usize;
                let mut queued_bytes = 0usize;

                if output_has_remaining {
                    rustfs_io_metrics::record_get_object_fill_completed_before_output_drained(
                        self.metrics_path,
                        self.fill_policy.as_str(),
                    );
                    queued_bytes += buf.len();
                    queued_count += 1;
                    self.prefetched_bufs.push_back(buf);
                } else {
                    rustfs_io_metrics::record_get_object_reader_prefetch(self.metrics_path, GET_READER_PREFETCH_DIRECT);
                    rustfs_io_metrics::record_get_object_reader_buffer(self.metrics_path, GET_READER_BUFFER_OUTPUT, buf.len());
                    self.output_buf = buf;
                    self.output_pos = 0;
                }

                if !self.prefetched_bufs.is_empty() {
                    let total_prefetched_bytes = self.prefetched_bufs.iter().map(Vec::len).sum::<usize>();
                    if total_prefetched_bytes > queued_bytes {
                        queued_bytes = total_prefetched_bytes;
                    }
                    if self.prefetched_bufs.len() > queued_count {
                        queued_count = self.prefetched_bufs.len();
                    }
                }
                if queued_count > 0 {
                    rustfs_io_metrics::record_get_object_fill_queued(self.metrics_path, self.fill_policy.as_str(), queued_count);
                    rustfs_io_metrics::record_get_object_reader_prefetch_bytes(
                        self.metrics_path,
                        self.fill_policy.as_str(),
                        queued_bytes,
                    );
                    rustfs_io_metrics::record_get_object_reader_prefetch(self.metrics_path, GET_READER_PREFETCH_STORED);
                    rustfs_io_metrics::record_get_object_reader_buffer(
                        self.metrics_path,
                        GET_READER_BUFFER_PREFETCH,
                        queued_bytes,
                    );
                }
                Poll::Ready(Ok(()))
            }
            Ok(None) => {
                rustfs_io_metrics::record_get_object_reader_prefetch(self.metrics_path, GET_READER_PREFETCH_EOF);
                Poll::Ready(Ok(()))
            }
            Err(err) => {
                if self.output_pos < self.output_buf.len() {
                    rustfs_io_metrics::record_get_object_reader_prefetch(self.metrics_path, GET_READER_PREFETCH_ERROR_DEFERRED);
                    self.prefetch_error = Some(err);
                    Poll::Ready(Ok(()))
                } else {
                    rustfs_io_metrics::record_get_object_reader_prefetch(self.metrics_path, GET_READER_PREFETCH_ERROR_IMMEDIATE);
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
            rustfs_io_metrics::record_get_object_fill_cancelled_on_drop(self.metrics_path, self.fill_policy.as_str());
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
        let filled_before_poll = buf.filled().len();

        loop {
            if self.output_pos < self.output_buf.len() {
                if self.prefetched_bufs.len() < self.fill_policy.max_inflight()
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
                let copy_start = get_stage_timer_if_enabled(self.stage_metrics_enabled);
                buf.put_slice(&available[..copy_len]);
                self.output_pos += copy_len;
                if copy_len > 0
                    && let Some(copy_start) = copy_start
                {
                    rustfs_io_metrics::record_get_object_reader_copy(
                        self.metrics_path,
                        copy_len,
                        read_buf_remaining_before,
                        output_remaining_before,
                        copy_start.elapsed().as_secs_f64(),
                    );
                }
                if buf.remaining() == 0 {
                    return Poll::Ready(Ok(()));
                }
                continue;
            }

            if let Some(next_buf) = self.prefetched_bufs.pop_front() {
                rustfs_io_metrics::record_get_object_reader_buffer(self.metrics_path, GET_READER_BUFFER_OUTPUT, next_buf.len());
                self.output_buf = next_buf;
                self.output_pos = 0;
                continue;
            }

            if self.prefetch_error.is_some() && buf.filled().len() > filled_before_poll {
                return Poll::Ready(Ok(()));
            }
            if let Some(err) = self.prefetch_error.take() {
                return Poll::Ready(Err(err));
            }

            if self.remaining == 0 {
                return Poll::Ready(Ok(()));
            }

            if self.output_wait_started_at.is_none() {
                self.output_wait_started_at = get_stage_timer_if_enabled(self.stage_metrics_enabled);
            }
            let prefetch = match self.poll_prefetch(cx) {
                Poll::Ready(result) => result,
                Poll::Pending if buf.filled().len() > filled_before_poll => return Poll::Ready(Ok(())),
                Poll::Pending => return Poll::Pending,
            };
            if self.stage_metrics_enabled
                && let Some(started_at) = self.output_wait_started_at.take()
            {
                rustfs_io_metrics::record_get_object_fill_waited_by_output(
                    self.metrics_path,
                    self.fill_policy.as_str(),
                    started_at.elapsed().as_secs_f64(),
                );
            }
            prefetch?;
        }
    }
}

pub(crate) struct SyncErasureDecodeReader<R> {
    inner: Mutex<R>,
    metrics_path: &'static str,
    stage_metrics_enabled: bool,
}

impl<R> SyncErasureDecodeReader<R> {
    pub(crate) fn new(inner: R) -> Self {
        Self::new_with_metrics_path(inner, GET_OBJECT_PATH_CODEC_STREAMING)
    }

    pub(crate) fn new_with_metrics_path(inner: R, metrics_path: &'static str) -> Self {
        Self {
            inner: Mutex::new(inner),
            metrics_path,
            stage_metrics_enabled: rustfs_io_metrics::get_stage_metrics_enabled(),
        }
    }
}

impl<R> AsyncRead for SyncErasureDecodeReader<R>
where
    R: AsyncRead + Unpin + Send,
{
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let stage_metrics_enabled = self.stage_metrics_enabled;
        let lock_wait_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let mut inner = match self.inner.lock() {
            Ok(inner) => {
                record_get_stage_duration_if_enabled(self.metrics_path, GET_STAGE_OUTPUT_LOCK_WAIT, lock_wait_start);
                inner
            }
            Err(_) => {
                record_get_stage_duration_if_enabled(self.metrics_path, GET_STAGE_OUTPUT_LOCK_WAIT, lock_wait_start);
                return Poll::Ready(Err(io::Error::other("erasure decode reader lock poisoned")));
            }
        };
        let read_buf_remaining_before = stage_metrics_enabled.then(|| buf.remaining());
        let filled_before = stage_metrics_enabled.then(|| buf.filled().len());
        let poll_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let result = Pin::new(&mut *inner).poll_read(cx, buf);
        if let (Some(read_buf_remaining_before), Some(filled_before), Some(poll_start)) =
            (read_buf_remaining_before, filled_before, poll_start)
        {
            let poll_duration = poll_start.elapsed().as_secs_f64();
            let filled_bytes = buf.filled().len().saturating_sub(filled_before);
            let poll_outcome = match &result {
                Poll::Ready(Ok(())) if filled_bytes > 0 => GET_READER_POLL_READY_DATA,
                Poll::Ready(Ok(())) => GET_READER_POLL_READY_EMPTY,
                Poll::Ready(Err(_)) => GET_READER_POLL_READY_ERROR,
                Poll::Pending => GET_READER_POLL_PENDING,
            };
            rustfs_io_metrics::record_get_object_stage_duration(self.metrics_path, GET_STAGE_OUTPUT_POLL, poll_duration);
            rustfs_io_metrics::record_get_object_reader_poll(
                self.metrics_path,
                poll_outcome,
                read_buf_remaining_before,
                filled_bytes,
                poll_duration,
            );
        }
        result
    }
}

fn decode_stripe<E>(
    metrics_path: &'static str,
    stage_metrics_enabled: bool,
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

    let reconstruct_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
    if state.data_shards_complete(engine.data_shards()) {
        record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_RECONSTRUCT, reconstruct_stage_start);
        let emit_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let output = emit_data_shards(&state, engine.data_shards(), engine.block_size(), remaining)?;
        record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_EMIT, emit_stage_start);
        return Ok(Some(output));
    }

    let (mut shards, _errs) = state.into_parts();
    if let Err(err) = engine.reconstruct_into(&mut shards, workspace) {
        record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_RECONSTRUCT, reconstruct_stage_start);
        return Err(err);
    }
    record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_RECONSTRUCT, reconstruct_stage_start);

    if shards.len() < engine.data_shards() {
        return Err(io::Error::new(
            ErrorKind::UnexpectedEof,
            "decoded stripe has fewer shards than data shard count",
        ));
    }

    let emit_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
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
    record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_EMIT, emit_stage_start);

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
    use crate::erasure::codec::bridge::{
        CodecStreamingDecodeEngine, ErasureDecodeEngine, LegacyEcDecodeEngine, RustfsCodecDecodeEngine,
    };
    use crate::erasure::coding::decode::ParallelReader;
    use crate::erasure::coding::{BitrotReader, Erasure};
    use crate::set_disk::shard_source::{ShardSlot, StripeReadState};
    use rustfs_utils::HashAlgorithm;
    use std::collections::VecDeque;
    use std::future::pending;
    use std::io::Cursor;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use temp_env::with_var;
    use tokio::io::AsyncReadExt;
    use tokio::sync::Notify;
    use tokio::task::yield_now;
    use tokio::time::{Duration, timeout};

    struct VecStripeSource {
        stripes: VecDeque<StripeReadState>,
        read_quorum: usize,
        read_count: Option<Arc<AtomicUsize>>,
    }

    struct BlockingSource {
        started: Arc<Notify>,
        dropped: Arc<AtomicUsize>,
        read_quorum: usize,
    }

    struct BlockingSourceDropGuard {
        dropped: Arc<AtomicUsize>,
    }

    impl Drop for BlockingSourceDropGuard {
        fn drop(&mut self) {
            self.dropped.fetch_add(1, Ordering::SeqCst);
        }
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

    #[async_trait::async_trait]
    impl ShardStripeSource for BlockingSource {
        async fn read_next_stripe(&mut self) -> StripeReadState {
            let _guard = BlockingSourceDropGuard {
                dropped: Arc::clone(&self.dropped),
            };
            self.started.notify_one();
            pending::<()>().await;
            StripeReadState::new(Vec::new(), self.read_quorum)
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

    async fn decode_all_with_engine<E>(
        erasure: &Erasure,
        engine: E,
        data: &[u8],
        missing_indexes: &[usize],
    ) -> io::Result<Vec<u8>>
    where
        E: ErasureDecodeEngine + Clone + Send + Sync + 'static,
    {
        let source = source_from_data(erasure, data, missing_indexes);
        let mut reader = ErasureDecodeReader::new_with_fill_policy(
            source,
            engine,
            data.len(),
            GET_OBJECT_PATH_CODEC_STREAMING,
            FillPolicy::SingleInFlight,
        )?;
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).await?;
        Ok(decoded)
    }

    async fn decode_all(erasure: Erasure, data: &[u8], missing_indexes: &[usize]) -> io::Result<Vec<u8>> {
        let engine = LegacyEcDecodeEngine::new(erasure.clone());
        decode_all_with_engine(&erasure, engine, data, missing_indexes).await
    }

    #[test]
    fn fill_policy_defaults_to_dual_inflight() {
        with_var(ENV_RUSTFS_GET_CODEC_STREAMING_MAX_INFLIGHT, None::<&str>, || {
            assert_eq!(FillPolicy::from_env(), FillPolicy::DualInFlight);
        });

        with_var(ENV_RUSTFS_GET_CODEC_STREAMING_MAX_INFLIGHT, Some("2"), || {
            assert_eq!(FillPolicy::from_env(), FillPolicy::DualInFlight);
        });

        with_var(ENV_RUSTFS_GET_CODEC_STREAMING_MAX_INFLIGHT, Some("99"), || {
            assert_eq!(FillPolicy::from_env(), FillPolicy::SingleInFlight);
        });
    }

    #[test]
    #[serial_test::serial]
    fn erasure_decode_reader_caches_stage_metrics_enabled_at_construction() {
        let erasure = Erasure::new(4, 2, 32);
        let data = b"metrics switch cache";

        rustfs_io_metrics::set_get_stage_metrics_enabled(false);
        let source = source_from_data(&erasure, data, &[]);
        let engine = LegacyEcDecodeEngine::new(erasure.clone());
        let reader = ErasureDecodeReader::new_with_fill_policy(
            source,
            engine,
            data.len(),
            GET_OBJECT_PATH_CODEC_STREAMING,
            FillPolicy::SingleInFlight,
        )
        .expect("reader should be constructed");
        assert!(!reader.stage_metrics_enabled);

        rustfs_io_metrics::set_get_stage_metrics_enabled(true);
        let source = source_from_data(&erasure, data, &[]);
        let engine = LegacyEcDecodeEngine::new(erasure);
        let reader = ErasureDecodeReader::new_with_fill_policy(
            source,
            engine,
            data.len(),
            GET_OBJECT_PATH_CODEC_STREAMING,
            FillPolicy::SingleInFlight,
        )
        .expect("reader should be constructed");
        assert!(reader.stage_metrics_enabled);

        rustfs_io_metrics::set_get_stage_metrics_enabled(false);
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
        let mut reader = ErasureDecodeReader::new_with_fill_policy(
            source,
            engine,
            0,
            GET_OBJECT_PATH_CODEC_STREAMING,
            FillPolicy::SingleInFlight,
        )
        .expect("empty reader should be constructed");
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
    async fn erasure_decode_reader_dual_inflight_prefetches_an_extra_stripe() {
        let erasure = Erasure::new(4, 2, 16);
        let data = (0..48u8).collect::<Vec<_>>();
        let read_count = Arc::new(AtomicUsize::new(0));
        let mut source = source_from_data(&erasure, &data, &[]);
        source.read_count = Some(Arc::clone(&read_count));
        let engine = LegacyEcDecodeEngine::new(erasure);
        let mut reader = ErasureDecodeReader::new_with_fill_policy(
            source,
            engine,
            data.len(),
            GET_OBJECT_PATH_CODEC_STREAMING,
            FillPolicy::DualInFlight,
        )
        .expect("reader should be constructed");
        let mut first_read = [0u8; 1];

        let read = reader.read(&mut first_read).await.expect("first read should succeed");

        assert_eq!(read, 1);
        timeout(Duration::from_secs(1), async {
            while read_count.load(Ordering::SeqCst) < 3 {
                yield_now().await;
            }
        })
        .await
        .expect("dual inflight policy should prefetch two future stripes before current output drains");
    }

    #[tokio::test]
    async fn erasure_decode_reader_defers_short_read_error_until_buffer_drains() {
        let erasure = Erasure::new(4, 2, 32);
        let first_stripe = (0..32u8).collect::<Vec<_>>();
        let source = VecStripeSource {
            stripes: VecDeque::from([
                source_from_data(&erasure, &first_stripe, &[])
                    .stripes
                    .pop_front()
                    .expect("first stripe should exist"),
                StripeReadState::new(Vec::new(), erasure.data_shards),
            ]),
            read_quorum: erasure.data_shards,
            read_count: None,
        };
        let engine = LegacyEcDecodeEngine::new(erasure);
        let mut reader = ErasureDecodeReader::new_with_fill_policy(
            source,
            engine,
            first_stripe.len() + 1,
            GET_OBJECT_PATH_CODEC_STREAMING,
            FillPolicy::SingleInFlight,
        )
        .expect("reader should be constructed");
        let mut decoded = Vec::new();

        let err = reader
            .read_to_end(&mut decoded)
            .await
            .expect_err("short-read error should surface after buffered output drains");

        assert_eq!(err.kind(), ErrorKind::Other);
        assert_eq!(decoded, first_stripe);
    }

    #[tokio::test]
    async fn erasure_decode_reader_drop_aborts_inflight_fill_task() {
        let started = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicUsize::new(0));
        let source = BlockingSource {
            started: Arc::clone(&started),
            dropped: Arc::clone(&dropped),
            read_quorum: 1,
        };
        let engine = LegacyEcDecodeEngine::new(Erasure::new(1, 0, 32));
        let task = tokio::spawn(async move {
            let mut reader = ErasureDecodeReader::new_with_fill_policy(
                source,
                engine,
                1,
                GET_OBJECT_PATH_CODEC_STREAMING,
                FillPolicy::SingleInFlight,
            )
            .expect("reader should be constructed");
            let mut first_read = [0u8; 1];
            let _ = reader.read(&mut first_read).await;
        });

        started.notified().await;
        task.abort();
        let _ = task.await;

        timeout(Duration::from_secs(1), async {
            while dropped.load(Ordering::SeqCst) == 0 {
                yield_now().await;
            }
        })
        .await
        .expect("reader drop should abort the in-flight fill task");
    }

    #[tokio::test]
    async fn erasure_decode_reader_rejects_inconsistent_reconstruction_sources() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;

        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let data = (0u8..64u8).collect::<Vec<_>>();
        let encoded = erasure.encode_data(&data).expect("test stripe should encode");
        let mut corrupt_parity = encoded[DATA_SHARDS].to_vec();
        corrupt_parity[0] ^= 0x80;

        let source = VecStripeSource {
            stripes: VecDeque::from([StripeReadState::from_parts(
                vec![
                    None,
                    Some(encoded[1].to_vec()),
                    Some(corrupt_parity),
                    Some(encoded[DATA_SHARDS + 1].to_vec()),
                ],
                Vec::new(),
                DATA_SHARDS,
            )]),
            read_quorum: DATA_SHARDS,
            read_count: None,
        };
        let engine = LegacyEcDecodeEngine::new(erasure);
        let mut reader = ErasureDecodeReader::new_with_fill_policy(
            source,
            engine,
            data.len(),
            GET_OBJECT_PATH_CODEC_STREAMING,
            FillPolicy::SingleInFlight,
        )
        .expect("reader should be constructed");
        let mut decoded = Vec::new();

        let err = reader
            .read_to_end(&mut decoded)
            .await
            .expect_err("streaming reader must reject inconsistent reconstruction sources");

        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("inconsistent read source shards"));
        assert!(decoded.is_empty());
    }

    #[tokio::test]
    async fn erasure_decode_reader_rustfs_engine_matches_legacy_with_missing_data() {
        let erasure = Erasure::new(4, 2, 32);
        let data = b"rustfs codec reader output must match legacy reader output exactly";
        let legacy = LegacyEcDecodeEngine::new(erasure.clone());
        let rustfs = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");

        let legacy_decoded = decode_all_with_engine(&erasure, legacy, data, &[1])
            .await
            .expect("legacy reader should decode");
        let rustfs_decoded = decode_all_with_engine(&erasure, rustfs, data, &[1])
            .await
            .expect("rustfs codec reader should decode");

        assert_eq!(rustfs_decoded, legacy_decoded);
        assert_eq!(rustfs_decoded, data);
    }

    #[tokio::test]
    async fn erasure_decode_reader_rustfs_engine_handles_empty_object() {
        let erasure = Erasure::new(4, 2, 32);
        let engine = RustfsCodecDecodeEngine::new(&erasure).expect("engine should be created");

        let decoded = decode_all_with_engine(&erasure, engine, b"", &[])
            .await
            .expect("empty object should decode");

        assert!(decoded.is_empty());
    }

    #[tokio::test]
    async fn erasure_decode_reader_verifying_parallel_source_rejects_inconsistent_reconstruction_sources() {
        const DATA_SHARDS: usize = 2;
        const PARITY_SHARDS: usize = 2;
        const BLOCK_SIZE: usize = 64;

        let erasure = Erasure::new(DATA_SHARDS, PARITY_SHARDS, BLOCK_SIZE);
        let data = (0u8..64u8).collect::<Vec<_>>();
        let shard_size = erasure.shard_size();
        let encoded = erasure.encode_data(&data).expect("test stripe should encode");
        let mut corrupt_parity = encoded[DATA_SHARDS].to_vec();
        corrupt_parity[0] ^= 0x80;
        let readers = vec![
            None,
            Some(BitrotReader::new(
                Cursor::new(encoded[1].to_vec()),
                shard_size,
                HashAlgorithm::None,
                false,
            )),
            Some(BitrotReader::new(Cursor::new(corrupt_parity), shard_size, HashAlgorithm::None, false)),
            Some(BitrotReader::new(
                Cursor::new(encoded[DATA_SHARDS + 1].to_vec()),
                shard_size,
                HashAlgorithm::None,
                false,
            )),
        ];
        let source = ParallelReader::new_with_metrics_path_and_reconstruction_verification(
            readers,
            erasure.clone(),
            0,
            data.len(),
            Some(GET_OBJECT_PATH_CODEC_STREAMING),
        );
        let engine = LegacyEcDecodeEngine::new(erasure);
        let mut reader = ErasureDecodeReader::new_with_fill_policy(
            source,
            engine,
            data.len(),
            GET_OBJECT_PATH_CODEC_STREAMING,
            FillPolicy::SingleInFlight,
        )
        .expect("reader should be constructed");
        let mut decoded = Vec::new();

        let err = reader
            .read_to_end(&mut decoded)
            .await
            .expect_err("streaming reader must reject inconsistent reconstruction sources");

        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("inconsistent read source shards"));
        assert!(decoded.is_empty());
    }

    #[tokio::test]
    async fn erasure_decode_reader_codec_streaming_engine_enum_matches_legacy() {
        let erasure = Erasure::new(4, 2, 32);
        let data = b"selected codec streaming engine preserves reader output";
        let legacy = CodecStreamingDecodeEngine::legacy(erasure.clone());
        let rustfs = CodecStreamingDecodeEngine::rustfs(&erasure).expect("engine should be created");

        let legacy_decoded = decode_all_with_engine(&erasure, legacy, data, &[2])
            .await
            .expect("legacy enum reader should decode");
        let rustfs_decoded = decode_all_with_engine(&erasure, rustfs, data, &[2])
            .await
            .expect("rustfs enum reader should decode");

        assert_eq!(rustfs_decoded, legacy_decoded);
        assert_eq!(rustfs_decoded, data);
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
        let mut reader = ErasureDecodeReader::new_with_fill_policy(
            source,
            engine,
            1,
            GET_OBJECT_PATH_CODEC_STREAMING,
            FillPolicy::SingleInFlight,
        )
        .expect("reader should be constructed");
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
        let mut reader = ErasureDecodeReader::new_with_fill_policy(
            source,
            engine,
            data.len(),
            GET_OBJECT_PATH_CODEC_STREAMING,
            FillPolicy::SingleInFlight,
        )
        .expect("reader should be constructed");
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
