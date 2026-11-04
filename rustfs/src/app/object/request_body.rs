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

//! Client inactivity is observed before decoding, but charged only while the
//! final, transformed reader is waiting. Compression can return buffered output
//! after its input returned Pending, so the transport cannot infer read demand.

use super::{LOG_COMPONENT_APP, LOG_SUBSYSTEM_OBJECT};
use crate::app::storage_api::request_body::{ByteStream, DynByteStream, RemainingLength, StdError, StreamingBlob};
use crate::error::ClientBodyReadTimeout;
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http_body::{Body, Frame, SizeHint};
use parking_lot::Mutex;
use rustfs_rio::{DynReader, EtagResolvable, HashReaderDetector, HashReaderMut, Index, TryGetIndex};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::time::{Instant, Sleep};

const EVENT_UPLOAD_BODY_PROGRESS_STALLED: &str = "upload_body_progress_stalled";
const EVENT_UPLOAD_PART_BODY_READ_STALLED: &str = "upload_part_body_read_stalled";

pub(crate) const UPLOAD_PROGRESS_WINDOW: Duration = Duration::from_secs(60);
pub(crate) const UPLOAD_PROGRESS_BYTES: u64 = 64 * 1024;

struct ReadPolicy {
    timeout: Duration,
    bucket: String,
    key: String,
    request_id: String,
    expected_decoded_bytes: u64,
}

#[derive(Default)]
struct ReadBudget {
    policy: Option<ReadPolicy>,
    demand: bool,
    waiting_since: Option<Instant>,
    waited: Duration,
    raw_bytes_received: u64,
    progress_floor: bool,
    progress_bytes: u64,
    progress_waited: Duration,
    finished: bool,
}

impl ReadBudget {
    fn pause(&mut self) {
        if let Some(start) = self.waiting_since.take() {
            let elapsed = start.elapsed();
            self.waited = self.waited.saturating_add(elapsed);
            self.progress_waited = self.progress_waited.saturating_add(elapsed);
        }
        self.demand = false;
    }
}

/// Server-owned extension shared by the raw Body and the storage-facing reader.
#[derive(Clone, Default)]
pub(crate) struct BodyReadControl(Arc<SharedBudget>);

#[derive(Default)]
struct SharedBudget {
    active: AtomicBool,
    budget: Mutex<ReadBudget>,
}

impl BodyReadControl {
    #[cfg(test)]
    pub(crate) fn activate(
        &self,
        timeout: Duration,
        bucket: &str,
        key: &str,
        request_id: &str,
        expected_decoded_bytes: u64,
    ) -> bool {
        if timeout.is_zero() {
            return false;
        }
        let mut state = self.0.budget.lock();
        if state.finished {
            return false;
        }
        state.policy = Some(ReadPolicy {
            timeout,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            request_id: request_id.to_owned(),
            expected_decoded_bytes,
        });
        self.0.active.store(true, Ordering::Release);
        true
    }

    pub(crate) fn activate_upload(&self, timeout: Duration, bucket: &str, key: &str, request_id: &str, expected: u64) {
        let mut state = self.0.budget.lock();
        if state.finished {
            return;
        }
        state.policy = Some(ReadPolicy {
            timeout,
            bucket: bucket.to_owned(),
            key: key.to_owned(),
            request_id: request_id.to_owned(),
            expected_decoded_bytes: expected,
        });
        state.progress_floor = true;
        self.0.active.store(true, Ordering::Release);
    }

    fn begin_read(&self) {
        let mut state = self.0.budget.lock();
        if !state.finished {
            state.demand = true;
        }
    }

    fn pause_read(&self) {
        self.0.budget.lock().pause();
    }

    fn progress(&self, bytes: usize) {
        // Other HTTP operations never activate this upload policy.
        if !self.0.active.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.0.budget.lock();
        state.raw_bytes_received = state
            .raw_bytes_received
            .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
        // Charge the pending interval before resetting inactivity. Small
        // frames do not reset the minimum-progress window or bank credit.
        if let Some(start) = state.waiting_since.take() {
            state.progress_waited = state.progress_waited.saturating_add(start.elapsed());
        }
        state.progress_bytes = state.progress_bytes.saturating_add(bytes as u64);
        if state.progress_bytes >= UPLOAD_PROGRESS_BYTES {
            state.progress_bytes = 0;
            state.progress_waited = Duration::ZERO;
        }
        state.waited = Duration::ZERO;
    }

    fn finish(&self) {
        let mut state = self.0.budget.lock();
        self.0.active.store(false, Ordering::Release);
        state.finished = true;
        state.policy = None;
        state.waiting_since = None;
        state.demand = false;
    }

    fn waiting_deadline(&self) -> Option<Instant> {
        if !self.0.active.load(Ordering::Acquire) {
            return None;
        }
        let mut state = self.0.budget.lock();
        let timeout = state.policy.as_ref()?.timeout;
        if !state.demand || state.finished {
            return None;
        }
        let inactivity_remaining = if timeout.is_zero() {
            Duration::MAX
        } else {
            timeout.saturating_sub(state.waited)
        };
        let remaining = if state.progress_floor {
            inactivity_remaining.min(UPLOAD_PROGRESS_WINDOW.saturating_sub(state.progress_waited))
        } else {
            inactivity_remaining
        };
        let start = *state.waiting_since.get_or_insert_with(Instant::now);
        // A timeout beyond the clock's representable range cannot elapse.
        start.checked_add(remaining)
    }

    fn expire(&self) -> Option<ClientBodyReadTimeout> {
        let (policy, raw_bytes_received, progress_floor) = {
            let mut state = self.0.budget.lock();
            let timeout = state.policy.as_ref()?.timeout;
            let elapsed = state.waiting_since?.elapsed();
            let inactivity_expired = !timeout.is_zero() && state.waited.saturating_add(elapsed) >= timeout;
            let progress_expired =
                state.progress_floor && state.progress_waited.saturating_add(elapsed) >= UPLOAD_PROGRESS_WINDOW;
            if !state.demand || (!inactivity_expired && !progress_expired) {
                return None;
            }
            if progress_expired {
                state.policy.as_mut()?.timeout = UPLOAD_PROGRESS_WINDOW;
            }
            state.finished = true;
            self.0.active.store(false, Ordering::Release);
            state.demand = false;
            state.waiting_since = None;
            (state.policy.take()?, state.raw_bytes_received, state.progress_floor)
        };
        tracing::error!(
            event = if progress_floor { EVENT_UPLOAD_BODY_PROGRESS_STALLED } else { EVENT_UPLOAD_PART_BODY_READ_STALLED },
            component = LOG_COMPONENT_APP,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            state = "stall_timeout",
            operation = if progress_floor { "Upload" } else { "UploadPart" },
            request_id = %policy.request_id,
            bucket = %policy.bucket,
            key = %policy.key,
            raw_bytes_received,
            expected_decoded_bytes = policy.expected_decoded_bytes,
            timeout_secs = policy.timeout.as_secs(),
            "Upload request body read budget exhausted"
        );
        Some(ClientBodyReadTimeout {
            timeout: policy.timeout,
            raw_bytes_received,
        })
    }
}

#[derive(Debug)]
pub(crate) enum ObservedBodyError<E> {
    Transport(E),
    Inactivity(ClientBodyReadTimeout),
}

impl<E: fmt::Display> fmt::Display for ObservedBodyError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => error.fmt(f),
            Self::Inactivity(error) => error.fmt(f),
        }
    }
}

impl<E: Error + 'static> Error for ObservedBodyError<E> {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        Some(match self {
            Self::Transport(error) => error,
            Self::Inactivity(error) => error,
        })
    }
}

/// Wraps the retained raw HTTP body, so a synthesized error never marks the
/// underlying transport complete or prevents HTTP/1 early-response draining.
pub(crate) struct ObservedBody<B> {
    inner: B,
    control: BodyReadControl,
    timer: Option<Pin<Box<Sleep>>>,
    ended: bool,
}

impl<B> ObservedBody<B> {
    pub(crate) fn new(inner: B, control: BodyReadControl) -> Self {
        Self {
            inner,
            control,
            timer: None,
            ended: false,
        }
    }

    fn poll_timeout(&mut self, cx: &mut Context<'_>) -> Option<ClientBodyReadTimeout> {
        let deadline = self.control.waiting_deadline()?;
        let timer = self.timer.get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
        if timer.deadline() != deadline {
            timer.as_mut().reset(deadline);
        }
        if timer.as_mut().poll(cx).is_ready() {
            return self.control.expire();
        }
        None
    }
}

impl<B: Body<Data = Bytes> + Unpin> Body for ObservedBody<B> {
    type Data = Bytes;
    type Error = ObservedBodyError<B::Error>;

    fn poll_frame(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, Self::Error>>> {
        if self.ended {
            return Poll::Ready(None);
        }
        let already_waiting = self.control.0.budget.lock().waiting_since.is_some();
        if already_waiting && let Some(error) = self.poll_timeout(cx) {
            self.ended = true;
            return Poll::Ready(Some(Err(ObservedBodyError::Inactivity(error))));
        }
        // Ignore empty data without resetting the budget. Bound work per poll
        // even if a body repeatedly returns immediately-ready empty frames.
        for _ in 0..32 {
            match Pin::new(&mut self.inner).poll_frame(cx) {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Some(data) = frame.data_ref() {
                        if data.is_empty() {
                            if let Some(error) = self.poll_timeout(cx) {
                                self.ended = true;
                                return Poll::Ready(Some(Err(ObservedBodyError::Inactivity(error))));
                            }
                            continue;
                        }
                        self.control.progress(data.len());
                    }
                    return Poll::Ready(Some(Ok(frame)));
                }
                Poll::Ready(Some(Err(error))) => {
                    self.ended = true;
                    self.control.finish();
                    return Poll::Ready(Some(Err(ObservedBodyError::Transport(error))));
                }
                Poll::Ready(None) => {
                    self.ended = true;
                    self.control.finish();
                    return Poll::Ready(None);
                }
                Poll::Pending => {
                    if let Some(error) = self.poll_timeout(cx) {
                        self.ended = true;
                        return Poll::Ready(Some(Err(ObservedBodyError::Inactivity(error))));
                    }
                    return Poll::Pending;
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn is_end_stream(&self) -> bool {
        self.ended || self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        if self.ended {
            SizeHint::with_exact(0)
        } else {
            self.inner.size_hint()
        }
    }
}

impl<B> Drop for ObservedBody<B> {
    fn drop(&mut self) {
        self.control.finish();
    }
}

/// Must wrap the final HashReader's inner reader after all write transforms.
/// Current erasure readers are owned by their read future/producer: canceling
/// that owner drops this reader. A future retained-reader cancellation path
/// must explicitly pause its read demand before retaining the reader.
pub(crate) struct DemandReader<R = DynReader> {
    inner: R,
    control: BodyReadControl,
}

impl<R> DemandReader<R> {
    pub(crate) fn new(inner: R, control: BodyReadControl) -> Self {
        Self { inner, control }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for DemandReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        self.control.begin_read();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if result.is_ready() {
            self.control.pause_read();
        }
        result
    }
}

impl<R> Drop for DemandReader<R> {
    fn drop(&mut self) {
        self.control.finish();
    }
}

impl<R: EtagResolvable> EtagResolvable for DemandReader<R> {
    fn is_etag_reader(&self) -> bool {
        self.inner.is_etag_reader()
    }
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.inner.try_resolve_etag()
    }
}

impl<R: HashReaderDetector> HashReaderDetector for DemandReader<R> {
    fn is_hash_reader(&self) -> bool {
        self.inner.is_hash_reader()
    }
    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        self.inner.as_hash_reader_mut()
    }
}

impl<R: TryGetIndex> TryGetIndex for DemandReader<R> {
    fn try_get_index(&self) -> Option<&Index> {
        self.inner.try_get_index()
    }
}

/// Establish demand for eager reads and archive sniffing before the final
/// transformed reader exists. Pending demand is subsequently paused by the
/// outer DemandReader when a transform returns buffered output.
pub(crate) fn demand_upload_body(body: StreamingBlob, control: BodyReadControl) -> StreamingBlob {
    StreamingBlob::new(DemandStream {
        inner: body.into(),
        control,
    })
}
struct DemandStream {
    inner: DynByteStream,
    control: BodyReadControl,
}
impl Stream for DemandStream {
    type Item = Result<Bytes, StdError>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.control.begin_read();
        let result = self.inner.poll_next_unpin(cx);
        if result.is_ready() {
            self.control.pause_read();
        }
        result
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
impl ByteStream for DemandStream {
    fn remaining_length(&self) -> RemainingLength {
        self.inner.remaining_length()
    }
}

/// Fallback for direct protocol callers without the raw HTTP observation.
/// HTTP uses ObservedBody because a signed chunk may not yield decoded output
/// until its full payload has arrived and its signature has been verified.
/// The final DemandReader pauses pending reads when transforms return output.
pub(crate) fn guard_upload_progress(
    body: StreamingBlob,
    timeout: Duration,
    bucket: &str,
    key: &str,
    request_id: &str,
    expected: u64,
) -> (StreamingBlob, BodyReadControl) {
    let control = BodyReadControl::default();
    control.activate_upload(timeout, bucket, key, request_id, expected);
    let stream = UploadProgressStream {
        inner: body.into(),
        control: control.clone(),
        timer: None,
        ended: false,
    };
    (StreamingBlob::new(stream), control)
}

struct UploadProgressStream {
    inner: DynByteStream,
    control: BodyReadControl,
    timer: Option<Pin<Box<Sleep>>>,
    ended: bool,
}

impl UploadProgressStream {
    fn poll_timeout(&mut self, cx: &mut Context<'_>) -> Option<ClientBodyReadTimeout> {
        let deadline = self.control.waiting_deadline()?;
        let timer = self.timer.get_or_insert_with(|| Box::pin(tokio::time::sleep_until(deadline)));
        if timer.deadline() != deadline {
            timer.as_mut().reset(deadline);
        }
        timer.as_mut().poll(cx).is_ready().then(|| self.control.expire()).flatten()
    }
}

impl Stream for UploadProgressStream {
    type Item = Result<Bytes, StdError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        self.control.begin_read();
        // An overdue read cannot be revived by a late frame (including EOF).
        // Do not start a wait until the source actually stalls.
        let already_waiting = self.control.0.budget.lock().waiting_since.is_some();
        if already_waiting && let Some(error) = self.poll_timeout(cx) {
            self.ended = true;
            return Poll::Ready(Some(Err(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, error)))));
        }
        let mut empty_frames_only = true;
        for _ in 0..32 {
            match self.inner.poll_next_unpin(cx) {
                Poll::Ready(Some(Ok(bytes))) if !bytes.is_empty() => {
                    self.control.progress(bytes.len());
                    self.control.pause_read();
                    return Poll::Ready(Some(Ok(bytes)));
                }
                Poll::Ready(Some(Ok(_))) => {}
                Poll::Ready(other) => {
                    self.ended = true;
                    self.control.finish();
                    return Poll::Ready(other);
                }
                Poll::Pending => {
                    empty_frames_only = false;
                    break;
                }
            }
        }
        if let Some(error) = self.poll_timeout(cx) {
            self.ended = true;
            return Poll::Ready(Some(Err(Box::new(std::io::Error::new(std::io::ErrorKind::TimedOut, error)))));
        }
        // Also bound an always-ready source of empty frames.
        if empty_frames_only {
            cx.waker().wake_by_ref();
        }
        Poll::Pending
    }
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}
impl ByteStream for UploadProgressStream {
    fn remaining_length(&self) -> RemainingLength {
        self.inner.remaining_length()
    }
}
impl Drop for UploadProgressStream {
    fn drop(&mut self) {
        self.control.finish();
    }
}

#[cfg(test)]
mod tests;
