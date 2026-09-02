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

//! Bounded tee for async readers: one source read, two ordered consumers.
//!
//! [`tee_reader`] splits a single [`AsyncRead`] source into a [`TeePrimary`]
//! that drives the source and a [`TeeSecondary`] that observes an identical
//! copy of every byte through a byte-bounded queue. Chunks are shared as
//! [`Bytes`] between the two sides, so the only extra work per chunk is one
//! `memcpy` into the queue.
//!
//! **Intended for small objects only.** The queue holds at most
//! `buffer_bytes` plus one chunk; when it is full the primary returns
//! `Pending` until the secondary catches up, so both sides advance at the
//! pace of the slowest consumer. Do not put this in front of a large body
//! whose secondary consumer may stall (for example a slow disk write behind
//! a fast client): the primary would stall with it.
//!
//! Termination semantics:
//!
//! - Source EOF: the secondary sees EOF after draining the queue.
//! - Source error: the primary gets the original error, the secondary gets an
//!   `io::Error` with the same [`io::ErrorKind`] so a partial stream is never
//!   mistaken for a complete object.
//! - Secondary dropped: the primary keeps serving its own caller; the tee
//!   becomes a pass-through.
//! - Primary dropped before EOF: by default the secondary gets
//!   [`io::ErrorKind::BrokenPipe`]. With
//!   [`TeeOptions::drain_on_primary_drop`] the remaining source bytes are
//!   moved to a background task that keeps feeding the secondary until EOF
//!   or [`TeeOptions::max_drain_bytes`], at which point the secondary gets a
//!   [`TeeDrainLimitExceeded`] error rather than a silent EOF.

use crate::compress_index::TryGetIndex;
use crate::{EtagResolvable, HashReaderDetector};
use bytes::{Buf, Bytes};
use futures::Stream;
use std::collections::VecDeque;
use std::fmt;
use std::future::poll_fn;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::task::{Context, Poll, Waker};
use thiserror::Error;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};

/// Default cap on bytes a background drain task may pull from the source
/// after the primary is dropped.
pub const DEFAULT_TEE_MAX_DRAIN_BYTES: usize = 64 * 1024 * 1024;

/// Chunk size used by the background drain task.
const DRAIN_CHUNK_BYTES: usize = 256 * 1024;

/// Behaviour knobs for [`tee_reader_with_options`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TeeOptions {
    /// When the primary is dropped before the source reached EOF, keep
    /// reading the source in a background task and feed the secondary
    /// instead of failing it with `BrokenPipe`.
    pub drain_on_primary_drop: bool,
    /// Upper bound on bytes the drain task may read after the primary drop.
    /// Exceeding it fails the secondary with [`TeeDrainLimitExceeded`].
    pub max_drain_bytes: usize,
}

impl Default for TeeOptions {
    fn default() -> Self {
        Self {
            drain_on_primary_drop: false,
            max_drain_bytes: DEFAULT_TEE_MAX_DRAIN_BYTES,
        }
    }
}

/// The background drain read more than `max_drain_bytes` after the primary
/// was dropped; the secondary stream is incomplete.
#[derive(Error, Debug, Clone, PartialEq, Eq)]
#[error("tee drain exceeded max_drain_bytes ({max_drain_bytes})")]
pub struct TeeDrainLimitExceeded {
    pub max_drain_bytes: usize,
}

type BoxedSource = Box<dyn AsyncRead + Unpin + Send>;

/// Why no more chunks will be queued.
#[derive(Debug)]
enum Terminal {
    Eof,
    SourceError { kind: io::ErrorKind, message: String },
    PrimaryDropped,
    DrainLimitExceeded { max_drain_bytes: usize },
}

impl Terminal {
    fn secondary_error(&self) -> Option<io::Error> {
        match self {
            Terminal::Eof => None,
            Terminal::SourceError { kind, message } => Some(io::Error::new(*kind, message.clone())),
            Terminal::PrimaryDropped => Some(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "tee primary dropped before the source reached EOF",
            )),
            Terminal::DrainLimitExceeded { max_drain_bytes } => Some(io::Error::other(TeeDrainLimitExceeded {
                max_drain_bytes: *max_drain_bytes,
            })),
        }
    }
}

#[derive(Debug)]
struct State {
    queue: VecDeque<Bytes>,
    queued_bytes: usize,
    terminal: Option<Terminal>,
    secondary_alive: bool,
    /// Waker of whoever pushes chunks: the primary, or the drain task.
    producer_waker: Option<Waker>,
    secondary_waker: Option<Waker>,
}

#[derive(Debug)]
struct Shared {
    capacity: usize,
    state: Mutex<State>,
    drain_tasks: AtomicUsize,
}

enum Capacity {
    Available,
    SecondaryGone,
}

impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        // The critical sections only touch plain data, so a poisoned lock
        // cannot leave the queue in a state that is unsafe to keep using.
        self.state.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Wait until the queue is below capacity, or the secondary is gone.
    fn poll_capacity(&self, cx: &mut Context<'_>) -> Poll<Capacity> {
        let mut state = self.lock();
        if !state.secondary_alive {
            return Poll::Ready(Capacity::SecondaryGone);
        }
        if state.queued_bytes >= self.capacity {
            state.producer_waker = Some(cx.waker().clone());
            return Poll::Pending;
        }
        Poll::Ready(Capacity::Available)
    }

    fn push(&self, chunk: Bytes) {
        let mut state = self.lock();
        if !state.secondary_alive || state.terminal.is_some() {
            return;
        }
        state.queued_bytes += chunk.len();
        state.queue.push_back(chunk);
        let waker = state.secondary_waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    fn finish(&self, terminal: Terminal) {
        let mut state = self.lock();
        if state.terminal.is_none() {
            state.terminal = Some(terminal);
        }
        let waker = state.secondary_waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Abort the secondary: discard queued chunks and fail it on next poll.
    fn abort_secondary(&self) {
        let mut state = self.lock();
        if state.terminal.is_none() {
            state.queue.clear();
            state.queued_bytes = 0;
            state.terminal = Some(Terminal::PrimaryDropped);
        }
        let waker = state.secondary_waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }

    /// Copy queued bytes into `buf`, or report the terminal outcome once the
    /// queue is empty. Both checks run under one lock so a chunk pushed
    /// between them cannot leave the secondary parked without a wake-up.
    fn poll_read_into(&self, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let mut state = self.lock();
        if let Some(front) = state.queue.front_mut() {
            let n = front.len().min(buf.remaining());
            buf.put_slice(&front[..n]);
            if n == front.len() {
                state.queue.pop_front();
            } else {
                front.advance(n);
            }
            state.queued_bytes -= n;
            let waker = state.producer_waker.take();
            drop(state);
            if let Some(waker) = waker {
                waker.wake();
            }
            return Poll::Ready(Ok(()));
        }
        Self::poll_terminal_locked(&mut state, cx)
    }

    /// Pop a whole chunk, or report the terminal outcome once the queue is
    /// empty (same single-lock rule as [`Shared::poll_read_into`]).
    fn poll_next_chunk(&self, cx: &mut Context<'_>) -> Poll<io::Result<Option<Bytes>>> {
        let mut state = self.lock();
        if let Some(chunk) = state.queue.pop_front() {
            state.queued_bytes -= chunk.len();
            let waker = state.producer_waker.take();
            drop(state);
            if let Some(waker) = waker {
                waker.wake();
            }
            return Poll::Ready(Ok(Some(chunk)));
        }
        Self::poll_terminal_locked(&mut state, cx).map_ok(|()| None)
    }

    fn poll_terminal_locked(state: &mut State, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &state.terminal {
            None => {
                state.secondary_waker = Some(cx.waker().clone());
                Poll::Pending
            }
            Some(terminal) => Poll::Ready(terminal.secondary_error().map_or(Ok(()), Err)),
        }
    }

    fn secondary_dropped(&self) {
        let mut state = self.lock();
        state.secondary_alive = false;
        state.queue.clear();
        state.queued_bytes = 0;
        let waker = state.producer_waker.take();
        drop(state);
        if let Some(waker) = waker {
            waker.wake();
        }
    }
}

/// Split `source` into a primary and a secondary reader that both observe the
/// full byte stream.
///
/// Intended for small objects only: `buffer_bytes` bounds the bytes queued
/// for the secondary (a value of `0` is treated as `1`), the primary may
/// overshoot by at most one chunk, and once the queue is full the primary
/// returns `Pending` until the secondary consumes, so both sides advance at
/// the pace of the slowest consumer. Termination and drop semantics are
/// described on [`TeePrimary`], [`TeeSecondary`] and [`TeeOptions`].
pub fn tee_reader<R>(source: R, buffer_bytes: usize) -> (TeePrimary, TeeSecondary)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    tee_reader_with_options(source, buffer_bytes, TeeOptions::default())
}

/// [`tee_reader`] with explicit [`TeeOptions`].
pub fn tee_reader_with_options<R>(source: R, buffer_bytes: usize, options: TeeOptions) -> (TeePrimary, TeeSecondary)
where
    R: AsyncRead + Unpin + Send + 'static,
{
    let shared = Arc::new(Shared {
        capacity: buffer_bytes.max(1),
        state: Mutex::new(State {
            queue: VecDeque::new(),
            queued_bytes: 0,
            terminal: None,
            secondary_alive: true,
            producer_waker: None,
            secondary_waker: None,
        }),
        drain_tasks: AtomicUsize::new(0),
    });
    let primary = TeePrimary {
        source: Some(Box::new(source)),
        shared: Arc::clone(&shared),
        options,
    };
    let secondary = TeeSecondary { shared };
    (primary, secondary)
}

/// The side of the tee that drives the source.
///
/// Every chunk read from the source is queued for the [`TeeSecondary`] before
/// being returned to the caller; when the queue is full `poll_read` returns
/// `Pending` without touching the source, which keeps cancellation of the
/// caller's read future safe (a chunk is either fully delivered to both sides
/// in one poll or not read at all).
pub struct TeePrimary {
    /// `None` only after `Drop` moved the source into the drain task.
    source: Option<BoxedSource>,
    shared: Arc<Shared>,
    options: TeeOptions,
}

impl fmt::Debug for TeePrimary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TeePrimary")
            .field("options", &self.options)
            .field("capacity", &self.shared.capacity)
            .finish_non_exhaustive()
    }
}

impl AsyncRead for TeePrimary {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        let this = &mut *self;
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        let Some(source) = this.source.as_mut() else {
            return Poll::Ready(Ok(()));
        };

        let tee_active = {
            let mut state = this.shared.lock();
            if !state.secondary_alive || state.terminal.is_some() {
                false
            } else if state.queued_bytes >= this.shared.capacity {
                state.producer_waker = Some(cx.waker().clone());
                return Poll::Pending;
            } else {
                true
            }
        };

        let before = buf.filled().len();
        match Pin::new(source).poll_read(cx, buf) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(err)) => {
                if tee_active {
                    this.shared.finish(Terminal::SourceError {
                        kind: err.kind(),
                        message: err.to_string(),
                    });
                }
                Poll::Ready(Err(err))
            }
            Poll::Ready(Ok(())) => {
                if tee_active {
                    let filled = &buf.filled()[before..];
                    if filled.is_empty() {
                        this.shared.finish(Terminal::Eof);
                    } else {
                        this.shared.push(Bytes::copy_from_slice(filled));
                    }
                }
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl Drop for TeePrimary {
    fn drop(&mut self) {
        let Some(source) = self.source.take() else {
            return;
        };
        {
            let state = self.shared.lock();
            if state.terminal.is_some() || !state.secondary_alive {
                return;
            }
        }
        if self.options.drain_on_primary_drop
            && let Ok(handle) = tokio::runtime::Handle::try_current()
        {
            let guard = DrainGuard::new(Arc::clone(&self.shared));
            handle.spawn(drain_source(source, guard, self.options.max_drain_bytes));
            return;
        }
        self.shared.abort_secondary();
    }
}

impl EtagResolvable for TeePrimary {}
impl HashReaderDetector for TeePrimary {}
impl TryGetIndex for TeePrimary {}

/// Tracks one live drain task. Dropping it (normal exit, cancellation, or a
/// runtime that never ran the task) guarantees the secondary is unblocked.
struct DrainGuard {
    shared: Arc<Shared>,
}

impl DrainGuard {
    fn new(shared: Arc<Shared>) -> Self {
        shared.drain_tasks.fetch_add(1, Ordering::AcqRel);
        Self { shared }
    }
}

impl Drop for DrainGuard {
    fn drop(&mut self) {
        self.shared.abort_secondary();
        self.shared.drain_tasks.fetch_sub(1, Ordering::AcqRel);
    }
}

async fn drain_source(mut source: BoxedSource, guard: DrainGuard, max_drain_bytes: usize) {
    let shared = Arc::clone(&guard.shared);
    let mut scratch = vec![0u8; DRAIN_CHUNK_BYTES.min(max_drain_bytes.max(1))];
    let mut drained = 0usize;
    loop {
        if let Capacity::SecondaryGone = poll_fn(|cx| shared.poll_capacity(cx)).await {
            return;
        }
        // Once the budget is spent, a one-byte probe distinguishes a clean
        // EOF from a source that still has data.
        let want = if drained >= max_drain_bytes {
            1
        } else {
            (max_drain_bytes - drained).min(scratch.len())
        };
        match source.read(&mut scratch[..want]).await {
            Err(err) => {
                shared.finish(Terminal::SourceError {
                    kind: err.kind(),
                    message: err.to_string(),
                });
                return;
            }
            Ok(0) => {
                shared.finish(Terminal::Eof);
                return;
            }
            Ok(n) => {
                if drained >= max_drain_bytes {
                    shared.finish(Terminal::DrainLimitExceeded { max_drain_bytes });
                    return;
                }
                drained += n;
                shared.push(Bytes::copy_from_slice(&scratch[..n]));
            }
        }
    }
}

/// The side of the tee that observes a copy of the primary's stream.
///
/// After the source reaches EOF the secondary drains the queue and reports
/// EOF. A source error surfaces here as an `io::Error` of the same kind. If
/// the primary is dropped early the secondary fails with
/// [`io::ErrorKind::BrokenPipe`], unless the tee was created with
/// [`TeeOptions::drain_on_primary_drop`]. Dropping the secondary turns the
/// primary into a plain pass-through.
#[derive(Debug)]
pub struct TeeSecondary {
    shared: Arc<Shared>,
}

impl TeeSecondary {
    /// Consume the secondary as a stream of whole chunks (no extra copy).
    pub fn into_stream(self) -> TeeStream {
        TeeStream {
            secondary: self,
            done: false,
        }
    }

    /// Number of background drain tasks still alive for this tee (0 or 1).
    /// Diagnostics only.
    pub fn active_drain_tasks(&self) -> usize {
        self.shared.drain_tasks.load(Ordering::Acquire)
    }
}

impl AsyncRead for TeeSecondary {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        self.shared.poll_read_into(cx, buf)
    }
}

impl Drop for TeeSecondary {
    fn drop(&mut self) {
        self.shared.secondary_dropped();
    }
}

impl EtagResolvable for TeeSecondary {}
impl HashReaderDetector for TeeSecondary {}
impl TryGetIndex for TeeSecondary {}

/// Chunk stream over a [`TeeSecondary`], see [`TeeSecondary::into_stream`].
///
/// Yields each queued [`Bytes`] chunk as-is. After an error or EOF the
/// stream is fused and keeps returning `None`.
#[derive(Debug)]
pub struct TeeStream {
    secondary: TeeSecondary,
    done: bool,
}

impl Stream for TeeStream {
    type Item = io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        match self.secondary.shared.poll_next_chunk(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Ok(Some(chunk))) => Poll::Ready(Some(Ok(chunk))),
            Poll::Ready(Ok(None)) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Ready(Err(err)) => {
                self.done = true;
                Poll::Ready(Some(Err(err)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use proptest::prelude::*;
    use tokio::io::AsyncReadExt;
    use tokio::task::yield_now;

    /// In-memory source that hands out data in a caller-defined sequence of
    /// chunk sizes, optionally returning `Pending` before every chunk and
    /// failing at a given offset.
    struct ChunkedSource {
        data: Bytes,
        pos: usize,
        chunk_sizes: Vec<usize>,
        chunk_idx: usize,
        yield_before_chunk: bool,
        yield_pending: bool,
        fail_at: Option<(usize, io::ErrorKind)>,
    }

    impl ChunkedSource {
        fn new(data: impl Into<Bytes>, chunk_sizes: Vec<usize>) -> Self {
            assert!(!chunk_sizes.is_empty());
            Self {
                data: data.into(),
                pos: 0,
                chunk_sizes,
                chunk_idx: 0,
                yield_before_chunk: false,
                yield_pending: false,
                fail_at: None,
            }
        }

        fn yielding(mut self) -> Self {
            self.yield_before_chunk = true;
            self.yield_pending = true;
            self
        }

        fn failing_at(mut self, offset: usize, kind: io::ErrorKind) -> Self {
            self.fail_at = Some((offset, kind));
            self
        }
    }

    impl AsyncRead for ChunkedSource {
        fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            if self.yield_before_chunk && self.yield_pending {
                self.yield_pending = false;
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            self.yield_pending = true;
            if let Some((offset, kind)) = self.fail_at
                && self.pos >= offset
            {
                return Poll::Ready(Err(io::Error::new(kind, "injected source failure")));
            }
            let size = self.chunk_sizes[self.chunk_idx % self.chunk_sizes.len()];
            self.chunk_idx += 1;
            let remaining = self.data.len() - self.pos;
            let n = size.min(remaining).min(buf.remaining());
            let n = self.fail_at.map_or(n, |(offset, _)| n.min(offset - self.pos));
            buf.put_slice(&self.data[self.pos..self.pos + n]);
            self.pos += n;
            Poll::Ready(Ok(()))
        }
    }

    fn pattern_bytes(len: usize, seed: u64) -> Vec<u8> {
        let mut state = seed | 1;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 24) as u8
            })
            .collect()
    }

    async fn read_all<R: AsyncRead + Unpin>(reader: &mut R, read_size: usize) -> io::Result<Vec<u8>> {
        let mut out = Vec::new();
        let mut buf = vec![0u8; read_size];
        loop {
            let n = reader.read(&mut buf).await?;
            if n == 0 {
                return Ok(out);
            }
            out.extend_from_slice(&buf[..n]);
        }
    }

    /// Upper bound on scheduler turns a drain task may take to exit; the
    /// clock is paused in these tests, so a busy yield loop must be bounded
    /// by iterations rather than wall time.
    const MAX_DRAIN_EXIT_YIELDS: usize = 10_000;

    async fn wait_for_drain_tasks(drain_tasks: &AtomicUsize) {
        for _ in 0..MAX_DRAIN_EXIT_YIELDS {
            if drain_tasks.load(Ordering::Acquire) == 0 {
                return;
            }
            yield_now().await;
        }
        panic!("drain task did not exit within {MAX_DRAIN_EXIT_YIELDS} scheduler turns");
    }

    async fn wait_for_drain_exit(secondary: &TeeSecondary) {
        wait_for_drain_tasks(&secondary.shared.drain_tasks).await;
    }

    fn drain_error(err: &io::Error) -> Option<&TeeDrainLimitExceeded> {
        err.get_ref().and_then(|inner| inner.downcast_ref::<TeeDrainLimitExceeded>())
    }

    proptest! {
        #![proptest_config(ProptestConfig { cases: 24, ..ProptestConfig::default() })]
        #[test]
        fn tee_reader_property_both_sides_match_source(
            len in prop_oneof![3 => 0usize..=64 * 1024, 1 => 0usize..=8 * 1024 * 1024],
            chunk_sizes in prop::collection::vec(1usize..=256 * 1024, 1..8),
            buffer_bytes in prop_oneof![1 => 1usize..=64, 1 => 1usize..=1024 * 1024],
            primary_read in 1usize..=192 * 1024,
            secondary_read in 1usize..=192 * 1024,
            seed in any::<u64>(),
        ) {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            let data = pattern_bytes(len, seed);
            let (primary_out, secondary_out) = runtime.block_on(async {
                let (mut primary, mut secondary) =
                    tee_reader(ChunkedSource::new(data.clone(), chunk_sizes).yielding(), buffer_bytes);
                let primary_task = tokio::spawn(async move { read_all(&mut primary, primary_read).await });
                let secondary_out = read_all(&mut secondary, secondary_read).await;
                (primary_task.await.expect("primary task"), secondary_out)
            });
            prop_assert_eq!(&primary_out.expect("primary read"), &data);
            prop_assert_eq!(&secondary_out.expect("secondary read"), &data);
        }
    }

    #[tokio::test]
    async fn tee_reader_backpressure_primary_pending_until_secondary_consumes() {
        let data = pattern_bytes(16 * 1024, 7);
        let (mut primary, mut secondary) = tee_reader(ChunkedSource::new(data.clone(), vec![512]), 1024);

        let mut buf = vec![0u8; 4096];
        let mut advanced = 0usize;
        loop {
            let mut read = tokio_test::task::spawn(primary.read(&mut buf));
            match read.poll() {
                Poll::Ready(Ok(n)) => {
                    assert!(n > 0, "source must not hit EOF during the backpressure phase");
                    advanced += n;
                }
                Poll::Ready(Err(err)) => panic!("unexpected error: {err}"),
                Poll::Pending => break,
            }
        }
        assert!(advanced <= 1024 + 512, "primary advanced {advanced} bytes past buffer + one chunk");
        assert!(advanced >= 1024, "primary must fill the buffer before blocking, got {advanced}");

        // A blocked primary stays blocked until the secondary consumes.
        let mut blocked = tokio_test::task::spawn(primary.read(&mut buf));
        assert!(blocked.poll().is_pending());
        drop(blocked);

        let mut secondary_buf = vec![0u8; 256];
        let n = secondary.read(&mut secondary_buf).await.expect("secondary read");
        assert_eq!(n, 256);
        assert_eq!(&secondary_buf[..n], &data[..256]);

        let mut resumed = tokio_test::task::spawn(primary.read(&mut buf));
        match resumed.poll() {
            Poll::Ready(Ok(n)) => assert!(n > 0),
            other => panic!("primary must resume after secondary consumed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn tee_reader_source_error_propagates_kind_to_both() {
        let data = pattern_bytes(8 * 1024, 11);
        let source = ChunkedSource::new(data.clone(), vec![1024]).failing_at(3072, io::ErrorKind::ConnectionReset);
        let (mut primary, mut secondary) = tee_reader(source, 64 * 1024);

        let mut primary_out = Vec::new();
        let primary_err = primary
            .read_to_end(&mut primary_out)
            .await
            .expect_err("primary must observe the source error");
        assert_eq!(primary_err.kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(primary_out, &data[..3072]);

        let mut secondary_out = Vec::new();
        let secondary_err = secondary
            .read_to_end(&mut secondary_out)
            .await
            .expect_err("secondary must not see EOF after a source error");
        assert_eq!(secondary_err.kind(), io::ErrorKind::ConnectionReset);
        assert_eq!(secondary_out, &data[..3072]);
    }

    #[tokio::test]
    async fn tee_reader_source_error_reaches_stream() {
        let data = pattern_bytes(2048, 5);
        let source = ChunkedSource::new(data.clone(), vec![1024]).failing_at(1024, io::ErrorKind::TimedOut);
        let (mut primary, secondary) = tee_reader(source, 64 * 1024);
        let mut stream = secondary.into_stream();

        let mut sink = Vec::new();
        let _ = primary.read_to_end(&mut sink).await.expect_err("source fails");

        let mut out = Vec::new();
        let err = loop {
            match stream.next().await.expect("stream ends with an error item, not None") {
                Ok(chunk) => out.extend_from_slice(&chunk),
                Err(err) => break err,
            }
        };
        assert_eq!(out, &data[..1024]);
        assert_eq!(err.kind(), io::ErrorKind::TimedOut);
        assert!(stream.next().await.is_none(), "stream is fused after an error");
    }

    #[tokio::test]
    async fn tee_reader_secondary_drop_primary_reads_full() {
        let data = pattern_bytes(64 * 1024, 3);
        // Buffer far smaller than the data: without the drop the primary would block.
        let (mut primary, secondary) = tee_reader(ChunkedSource::new(data.clone(), vec![4096]), 1024);

        let mut first = vec![0u8; 1024];
        let n = primary.read(&mut first).await.expect("first read");
        assert_eq!(n, 1024);
        drop(secondary);

        let mut rest = Vec::new();
        primary
            .read_to_end(&mut rest)
            .await
            .expect("primary continues after secondary drop");
        let mut all = first;
        all.extend_from_slice(&rest);
        assert_eq!(all, data);
    }

    #[tokio::test]
    async fn tee_reader_primary_drop_default_secondary_broken_pipe() {
        let data = pattern_bytes(16 * 1024, 9);
        let (mut primary, mut secondary) = tee_reader(ChunkedSource::new(data.clone(), vec![1024]), 64 * 1024);

        let mut head = vec![0u8; 2048];
        primary.read_exact(&mut head).await.expect("read head");
        drop(primary);

        let mut buf = vec![0u8; 4096];
        let err = secondary.read(&mut buf).await.expect_err("secondary must fail immediately");
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
        let again = secondary.read(&mut buf).await.expect_err("error is sticky");
        assert_eq!(again.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn tee_reader_primary_drop_after_eof_keeps_secondary_complete() {
        let data = pattern_bytes(8 * 1024, 13);
        let (mut primary, mut secondary) = tee_reader(ChunkedSource::new(data.clone(), vec![1000]), 64 * 1024);

        let mut sink = Vec::new();
        primary.read_to_end(&mut sink).await.expect("primary to EOF");
        drop(primary);

        let out = read_all(&mut secondary, 777).await.expect("secondary reads everything");
        assert_eq!(out, data);
    }

    #[tokio::test(start_paused = true)]
    async fn tee_reader_primary_drop_drain_secondary_reads_full_and_task_exits() {
        let data = pattern_bytes(256 * 1024, 17);
        let options = TeeOptions {
            drain_on_primary_drop: true,
            max_drain_bytes: DEFAULT_TEE_MAX_DRAIN_BYTES,
        };
        // Small buffer so the drain task has to wait for the secondary repeatedly.
        let (mut primary, mut secondary) =
            tee_reader_with_options(ChunkedSource::new(data.clone(), vec![3000]).yielding(), 4096, options);

        let mut head = vec![0u8; 5000];
        primary.read_exact(&mut head).await.expect("read head");
        assert_eq!(secondary.active_drain_tasks(), 0);
        drop(primary);
        assert_eq!(secondary.active_drain_tasks(), 1);

        let out = read_all(&mut secondary, 1500)
            .await
            .expect("secondary reads everything via drain");
        assert_eq!(out, data);
        wait_for_drain_exit(&secondary).await;
        assert_eq!(secondary.active_drain_tasks(), 0);

        let mut buf = [0u8; 8];
        assert_eq!(secondary.read(&mut buf).await.expect("EOF stays EOF"), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn tee_reader_drain_limit_exceeded_secondary_error() {
        let data = pattern_bytes(64 * 1024, 19);
        let options = TeeOptions {
            drain_on_primary_drop: true,
            max_drain_bytes: 10 * 1024,
        };
        let (mut primary, mut secondary) =
            tee_reader_with_options(ChunkedSource::new(data.clone(), vec![1024]), 64 * 1024, options);

        let mut head = vec![0u8; 4096];
        primary.read_exact(&mut head).await.expect("read head");
        drop(primary);

        let mut out = Vec::new();
        let err = secondary
            .read_to_end(&mut out)
            .await
            .expect_err("hitting max_drain_bytes must not look like EOF");
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert_eq!(
            drain_error(&err),
            Some(&TeeDrainLimitExceeded {
                max_drain_bytes: 10 * 1024
            })
        );
        assert_eq!(out, &data[..4096 + 10 * 1024], "bytes within the budget are still delivered");
        wait_for_drain_exit(&secondary).await;
    }

    #[tokio::test(start_paused = true)]
    async fn tee_reader_drain_limit_exact_eof_is_clean() {
        let data = pattern_bytes(12 * 1024, 23);
        let options = TeeOptions {
            drain_on_primary_drop: true,
            max_drain_bytes: 8 * 1024,
        };
        let (mut primary, mut secondary) =
            tee_reader_with_options(ChunkedSource::new(data.clone(), vec![1024]), 64 * 1024, options);

        let mut head = vec![0u8; 4096];
        primary.read_exact(&mut head).await.expect("read head");
        drop(primary);

        let out = read_all(&mut secondary, 4096)
            .await
            .expect("source ends exactly at the budget");
        assert_eq!(out, data);
        wait_for_drain_exit(&secondary).await;
    }

    #[tokio::test(start_paused = true)]
    async fn tee_reader_drain_stops_when_secondary_dropped() {
        let data = pattern_bytes(1024 * 1024, 29);
        let options = TeeOptions {
            drain_on_primary_drop: true,
            max_drain_bytes: DEFAULT_TEE_MAX_DRAIN_BYTES,
        };
        let (mut primary, mut secondary) =
            tee_reader_with_options(ChunkedSource::new(data, vec![4096]).yielding(), 1024, options);

        let mut head = vec![0u8; 1024];
        primary.read_exact(&mut head).await.expect("read head");
        drop(primary);
        assert_eq!(secondary.active_drain_tasks(), 1);

        let mut buf = vec![0u8; 512];
        secondary.read_exact(&mut buf).await.expect("one read while draining");
        let shared = Arc::clone(&secondary.shared);
        drop(secondary);

        wait_for_drain_tasks(&shared.drain_tasks).await;
        assert!(shared.lock().queue.is_empty());
    }

    #[test]
    fn tee_reader_drain_without_runtime_falls_back_to_broken_pipe() {
        let data = pattern_bytes(4096, 31);
        let options = TeeOptions {
            drain_on_primary_drop: true,
            max_drain_bytes: DEFAULT_TEE_MAX_DRAIN_BYTES,
        };
        let (mut primary, mut secondary) = tee_reader_with_options(ChunkedSource::new(data, vec![1024]), 64 * 1024, options);

        let mut head = vec![0u8; 1024];
        tokio_test::block_on(primary.read_exact(&mut head)).expect("read head");
        drop(primary);
        assert_eq!(secondary.active_drain_tasks(), 0);

        let mut buf = vec![0u8; 64];
        let err = tokio_test::block_on(secondary.read(&mut buf)).expect_err("no runtime to drain on");
        assert_eq!(err.kind(), io::ErrorKind::BrokenPipe);
    }

    #[tokio::test]
    async fn tee_reader_cancel_safety_no_lost_or_duplicate_chunks() {
        let data = pattern_bytes(96 * 1024, 37);
        let source = ChunkedSource::new(data.clone(), vec![700, 1300, 1, 4096, 333]).yielding();
        let (mut primary, mut secondary) = tee_reader(source, 2048);

        let mut primary_out = Vec::new();
        let mut secondary_out = Vec::new();
        let mut primary_done = false;
        let mut secondary_done = false;
        let mut primary_buf = vec![0u8; 2500];
        let mut secondary_buf = vec![0u8; 900];
        let mut cancelled = 0usize;

        while !(primary_done && secondary_done) {
            if !primary_done {
                let mut read = tokio_test::task::spawn(primary.read(&mut primary_buf));
                match read.poll() {
                    Poll::Ready(Ok(0)) => primary_done = true,
                    Poll::Ready(Ok(n)) => {
                        drop(read);
                        primary_out.extend_from_slice(&primary_buf[..n]);
                    }
                    Poll::Ready(Err(err)) => panic!("primary error: {err}"),
                    // Cancel the read future at its Pending point.
                    Poll::Pending => cancelled += 1,
                }
            }
            if !secondary_done {
                let mut read = tokio_test::task::spawn(secondary.read(&mut secondary_buf));
                match read.poll() {
                    Poll::Ready(Ok(0)) => secondary_done = true,
                    Poll::Ready(Ok(n)) => {
                        drop(read);
                        secondary_out.extend_from_slice(&secondary_buf[..n]);
                    }
                    Poll::Ready(Err(err)) => panic!("secondary error: {err}"),
                    Poll::Pending => cancelled += 1,
                }
            }
        }

        assert!(cancelled > 0, "the test must actually exercise cancellation");
        assert_eq!(primary_out, data);
        assert_eq!(secondary_out, data);
    }

    #[tokio::test]
    async fn tee_reader_into_stream_yields_whole_chunks() {
        let data = pattern_bytes(10 * 1024, 41);
        let (mut primary, secondary) = tee_reader(ChunkedSource::new(data.clone(), vec![1024, 2048]), 64 * 1024);
        let mut stream = secondary.into_stream();

        let mut sink = Vec::new();
        primary.read_to_end(&mut sink).await.expect("primary reads all");
        assert_eq!(sink, data);

        let mut chunks = Vec::new();
        let mut out = Vec::new();
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.expect("chunk ok");
            chunks.push(chunk.len());
            out.extend_from_slice(&chunk);
        }
        assert_eq!(out, data);
        assert!(chunks.len() >= 2, "chunks should map to source reads, got {chunks:?}");
        assert!(stream.next().await.is_none(), "stream is fused after EOF");
    }

    #[tokio::test]
    async fn tee_reader_empty_source_gives_both_sides_eof() {
        let (mut primary, mut secondary) = tee_reader(ChunkedSource::new(Vec::new(), vec![1024]), 16);
        let mut buf = [0u8; 16];
        assert_eq!(primary.read(&mut buf).await.expect("primary"), 0);
        assert_eq!(secondary.read(&mut buf).await.expect("secondary"), 0);
    }

    #[tokio::test]
    async fn tee_reader_zero_capacity_read_does_not_touch_source() {
        let data = pattern_bytes(2048, 43);
        // Buffer larger than the data: the sequential primary-then-secondary reads below must not block.
        let (mut primary, mut secondary) = tee_reader(ChunkedSource::new(data.clone(), vec![1024]), 64 * 1024);
        let mut empty = [];
        assert_eq!(primary.read(&mut empty).await.expect("empty primary read"), 0);
        assert_eq!(secondary.read(&mut empty).await.expect("empty secondary read"), 0);

        let mut sink = Vec::new();
        primary.read_to_end(&mut sink).await.expect("primary");
        assert_eq!(sink, data);
        assert_eq!(read_all(&mut secondary, 100).await.expect("secondary"), data);
    }

    #[tokio::test]
    async fn tee_reader_secondary_feeds_hash_reader() {
        let data = pattern_bytes(3 * 1024, 47);
        let (mut primary, secondary) = tee_reader(ChunkedSource::new(data.clone(), vec![512]), 64 * 1024);
        let size = data.len() as i64;
        let mut hash_reader = crate::HashReader::from_stream(secondary, size, size, None, None, false).expect("hash reader");

        let mut sink = Vec::new();
        primary.read_to_end(&mut sink).await.expect("primary");
        let mut out = Vec::new();
        hash_reader
            .read_to_end(&mut out)
            .await
            .expect("hash reader consumes the secondary");
        assert_eq!(out, data);
    }
}
