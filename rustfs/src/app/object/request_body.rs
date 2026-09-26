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
use crate::error::ClientBodyReadTimeout;
use bytes::Bytes;
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

const EVENT_UPLOAD_PART_BODY_READ_STALLED: &str = "upload_part_body_read_stalled";

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
    finished: bool,
}

impl ReadBudget {
    fn pause(&mut self) {
        if let Some(start) = self.waiting_since.take() {
            self.waited = self.waited.saturating_add(start.elapsed());
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
        // Other HTTP operations never activate this UploadPart policy.
        if !self.0.active.load(Ordering::Acquire) {
            return;
        }
        let mut state = self.0.budget.lock();
        state.raw_bytes_received = state
            .raw_bytes_received
            .saturating_add(u64::try_from(bytes).unwrap_or(u64::MAX));
        state.waited = Duration::ZERO;
        state.waiting_since = None;
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
        let remaining = timeout.saturating_sub(state.waited);
        let start = *state.waiting_since.get_or_insert_with(Instant::now);
        // A timeout beyond the clock's representable range cannot elapse.
        start.checked_add(remaining)
    }

    fn expire(&self) -> Option<ClientBodyReadTimeout> {
        let (policy, raw_bytes_received) = {
            let mut state = self.0.budget.lock();
            let timeout = state.policy.as_ref()?.timeout;
            if !state.demand || state.waited.saturating_add(state.waiting_since?.elapsed()) < timeout {
                return None;
            }
            state.finished = true;
            self.0.active.store(false, Ordering::Release);
            state.demand = false;
            state.waiting_since = None;
            (state.policy.take()?, state.raw_bytes_received)
        };
        tracing::error!(
            event = EVENT_UPLOAD_PART_BODY_READ_STALLED,
            component = LOG_COMPONENT_APP,
            subsystem = LOG_SUBSYSTEM_OBJECT,
            state = "stall_timeout",
            operation = "UploadPart",
            request_id = %policy.request_id,
            bucket = %policy.bucket,
            key = %policy.key,
            raw_bytes_received,
            expected_decoded_bytes = policy.expected_decoded_bytes,
            timeout_secs = policy.timeout.as_secs(),
            "UploadPart request body read stalled"
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
pub(crate) struct DemandReader {
    inner: DynReader,
    control: BodyReadControl,
}

impl DemandReader {
    pub(crate) fn new(inner: DynReader, control: BodyReadControl) -> Self {
        Self { inner, control }
    }
}

impl AsyncRead for DemandReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        self.control.begin_read();
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        if result.is_ready() {
            self.control.pause_read();
        }
        result
    }
}

impl Drop for DemandReader {
    fn drop(&mut self) {
        self.control.finish();
    }
}

impl EtagResolvable for DemandReader {
    fn is_etag_reader(&self) -> bool {
        self.inner.is_etag_reader()
    }
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.inner.try_resolve_etag()
    }
}

impl HashReaderDetector for DemandReader {
    fn is_hash_reader(&self) -> bool {
        self.inner.is_hash_reader()
    }
    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        self.inner.as_hash_reader_mut()
    }
}

impl TryGetIndex for DemandReader {
    fn try_get_index(&self) -> Option<&Index> {
        self.inner.try_get_index()
    }
}

#[cfg(test)]
mod tests;
