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

//! Upload frame/chunk boundary. Protocol edges supply a native body; storage consumes chunks.
//! This adapter preserves trailers and source errors without applying another length check.

use crate::app::storage_api::request_body::{ByteStream, StdError, StreamingBlob};
use bytes::Bytes;
use futures::{Stream, StreamExt};
use http::HeaderMap;
use http_body::Body as _;
use parking_lot::Mutex;
use rustfs_gateway_stream::{
    Body, Payload, PayloadCaps, PayloadRead, PayloadStream, StreamError, StreamErrorKind, TrailingHeaders,
};
use rustfs_rio::{SharedTrailerSource, TrailerSource, TrailerValue};
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

/// The legacy edge preserves the producer's length hint without enforcing it a second time.
pub(crate) fn from_legacy(body: StreamingBlob) -> Body {
    let stream = LegacyPayload { body, ended: false };
    Body::from_payload(Payload::Stream(Box::pin(stream)))
}

#[derive(Debug)]
struct ProducerSource(StdError);

impl std::fmt::Display for ProducerSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}
impl std::error::Error for ProducerSource {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.0.as_ref())
    }
}

struct LegacyPayload {
    body: StreamingBlob,
    ended: bool,
}

impl PayloadStream for LegacyPayload {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<PayloadRead, StreamError>> {
        if self.ended {
            return Poll::Ready(Err(StreamError::polled_after_eof()));
        }
        // Empty transport frames are not progress. Bound each poll without reading ahead.
        for _ in 0..32 {
            match self.body.poll_next_unpin(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(bytes))) if bytes.is_empty() => continue,
                Poll::Ready(Some(Ok(bytes))) => return Poll::Ready(Ok(PayloadRead::Chunk(bytes))),
                Poll::Ready(Some(Err(error))) => {
                    self.ended = true;
                    return Poll::Ready(Err(StreamError::upstream(Box::new(ProducerSource(error)))));
                }
                Poll::Ready(None) => {
                    self.ended = true;
                    return Poll::Ready(Ok(PayloadRead::Eof {
                        trailers: TrailingHeaders::empty(),
                    }));
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }

    fn caps(&self) -> PayloadCaps {
        let mut caps = PayloadCaps::PUSH;
        caps.set(PayloadCaps::KNOWN_LENGTH, self.len_hint().is_some());
        caps
    }

    fn len_hint(&self) -> Option<u64> {
        self.body.remaining_length().exact().map(|length| length as u64)
    }
}

#[derive(Default)]
pub(crate) struct FrameTrailers(Mutex<Option<HeaderMap>>);

impl TrailerSource for FrameTrailers {
    fn lookup(&self, name: &str) -> TrailerValue {
        match self.0.lock().as_ref() {
            None => TrailerValue::Pending,
            Some(headers) => headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map_or(TrailerValue::Missing, |value| TrailerValue::Present(value.to_owned())),
        }
    }
}

/// Sync is needed only by RIO's reader trait; polling owns this mutex exclusively.
pub(crate) struct BodyChunks {
    body: Mutex<Body>,
    trailers: Arc<FrameTrailers>,
    ended: bool,
}

impl BodyChunks {
    #[cfg(test)]
    pub(super) fn new(body: Body) -> (Self, SharedTrailerSource) {
        Self::with_trailers(body, None)
    }

    pub(crate) fn with_trailers(body: Body, trailers: Option<Arc<FrameTrailers>>) -> (Self, SharedTrailerSource) {
        let trailers = trailers.unwrap_or_default();
        (
            Self {
                body: Mutex::new(body),
                trailers: Arc::clone(&trailers),
                ended: false,
            },
            trailers,
        )
    }
}

impl Stream for BodyChunks {
    type Item = Result<Bytes, StdError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.ended {
            return Poll::Ready(None);
        }
        match Pin::new(self.body.get_mut()).poll_frame(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Err(error))) => {
                self.ended = true;
                // Preserve the original producer's concrete error and its classification.
                let error: StdError = match error.kind() {
                    StreamErrorKind::Upstream(source) if source.is::<ProducerSource>() => match error.into_kind() {
                        StreamErrorKind::Upstream(source) => match source.downcast::<ProducerSource>() {
                            Ok(source) => source.0,
                            Err(source) => source,
                        },
                        _ => unreachable!("the kind was checked before consuming the error"),
                    },
                    _ => Box::new(error),
                };
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(frame))) => match frame.into_data() {
                Ok(bytes) => Poll::Ready(Some(Ok(bytes))),
                Err(frame) => {
                    *self.trailers.0.lock() = Some(frame.into_trailers().unwrap_or_default());
                    self.ended = true;
                    Poll::Ready(None)
                }
            },
            Poll::Ready(None) => {
                *self.trailers.0.lock() = Some(HeaderMap::new());
                self.ended = true;
                Poll::Ready(None)
            }
        }
    }
}

pub(super) fn from_io_stream<S>(stream: S) -> Body
where
    S: Stream<Item = std::io::Result<Bytes>> + Send + 'static,
{
    Body::from_payload(Payload::Stream(Box::pin(IoPayload {
        stream: Box::pin(stream),
        ended: false,
    })))
}

struct IoPayload<S> {
    stream: Pin<Box<S>>,
    ended: bool,
}

impl<S: Stream<Item = std::io::Result<Bytes>>> PayloadStream for IoPayload<S> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<PayloadRead, StreamError>> {
        if self.ended {
            return Poll::Ready(Err(StreamError::polled_after_eof()));
        }
        for _ in 0..32 {
            match self.stream.as_mut().poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Ok(bytes))) if bytes.is_empty() => continue,
                Poll::Ready(Some(Ok(bytes))) => return Poll::Ready(Ok(PayloadRead::Chunk(bytes))),
                Poll::Ready(Some(Err(error))) => {
                    self.ended = true;
                    return Poll::Ready(Err(StreamError::upstream(Box::new(ProducerSource(Box::new(error))))));
                }
                Poll::Ready(None) => {
                    self.ended = true;
                    return Poll::Ready(Ok(PayloadRead::Eof {
                        trailers: TrailingHeaders::empty(),
                    }));
                }
            }
        }
        cx.waker().wake_by_ref();
        Poll::Pending
    }
    fn caps(&self) -> PayloadCaps {
        PayloadCaps::PUSH
    }
    fn len_hint(&self) -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::storage_api::request_body::RemainingLength;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct LegacyProbe {
        frames: VecDeque<Option<Result<Bytes, StdError>>>,
        length: Option<usize>,
        polls: Arc<AtomicUsize>,
    }
    impl Stream for LegacyProbe {
        type Item = Result<Bytes, StdError>;
        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            match self.frames.pop_front() {
                Some(None) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Some(Some(frame)) => Poll::Ready(Some(frame)),
                None => Poll::Ready(None),
            }
        }
    }
    impl ByteStream for LegacyProbe {
        fn remaining_length(&self) -> RemainingLength {
            self.length.map_or_else(RemainingLength::unknown, RemainingLength::new_exact)
        }
    }

    #[tokio::test]
    async fn legacy_body_preserves_demand_pending_and_length_validation_location() {
        for (length, bytes) in [(Some(0), b"".as_slice()), (Some(4), b"a"), (Some(1), b"abcd"), (None, b"abc")] {
            let polls = Arc::new(AtomicUsize::new(0));
            let body = from_legacy(StreamingBlob::new(LegacyProbe {
                frames: VecDeque::from([None, Some(Ok(Bytes::copy_from_slice(bytes)))]),
                length,
                polls: Arc::clone(&polls),
            }));
            assert_eq!(body.len_hint(), length.map(|value| value as u64));
            let (chunks, trailers) = BodyChunks::new(body);
            let (chunks, control) = super::super::request_body::guard_native_upload_progress(
                Box::new(chunks),
                std::time::Duration::from_secs(30),
                "bucket",
                "key",
                "request",
                length.unwrap_or(0) as u64,
            );
            let mut chunks = super::super::request_body::demand_native_upload(chunks, control);
            assert_eq!(polls.load(Ordering::SeqCst), 0);
            assert_eq!(trailers.lookup("x-test"), TrailerValue::Pending);
            let mut actual = Vec::new();
            while let Some(chunk) = chunks.next().await {
                actual.extend_from_slice(&chunk.unwrap());
            }
            assert_eq!(actual, bytes);
            assert_eq!(polls.load(Ordering::SeqCst), 3);
            assert_eq!(trailers.lookup("x-test"), TrailerValue::Missing);
            let count = polls.load(Ordering::SeqCst);
            assert!(chunks.next().await.is_none());
            assert_eq!(polls.load(Ordering::SeqCst), count);
        }
    }

    #[tokio::test]
    async fn legacy_producer_error_keeps_its_concrete_type_and_is_not_eof() {
        let body = from_legacy(StreamingBlob::new(LegacyProbe {
            frames: VecDeque::from([Some(Err(
                Box::new(std::io::Error::from(std::io::ErrorKind::ConnectionReset)) as StdError
            ))]),
            length: None,
            polls: Arc::new(AtomicUsize::new(0)),
        }));
        let (mut chunks, trailers) = BodyChunks::new(body);
        let error = chunks.next().await.unwrap().unwrap_err();
        assert_eq!(
            error.downcast_ref::<std::io::Error>().unwrap().kind(),
            std::io::ErrorKind::ConnectionReset
        );
        assert_eq!(trailers.lookup("x-test"), TrailerValue::Pending);
        assert!(chunks.next().await.is_none());
    }

    struct NativeProbe(Option<Result<PayloadRead, StreamError>>);
    impl PayloadStream for NativeProbe {
        fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<PayloadRead, StreamError>> {
            Poll::Ready(self.0.take().unwrap_or_else(|| Err(StreamError::polled_after_eof())))
        }
        fn caps(&self) -> PayloadCaps {
            PayloadCaps::PUSH
        }
        fn len_hint(&self) -> Option<u64> {
            None
        }
    }

    #[tokio::test]
    async fn native_trailers_are_published_only_at_eof() {
        let mut headers = HeaderMap::new();
        headers.insert("x-test", http::HeaderValue::from_static("value"));
        let body = Body::from_stream(NativeProbe(Some(Ok(PayloadRead::Eof {
            trailers: TrailingHeaders::from_header_map(headers),
        }))))
        .unwrap();
        let (mut chunks, trailers) = BodyChunks::new(body);
        assert_eq!(trailers.lookup("x-test"), TrailerValue::Pending);
        assert!(chunks.next().await.is_none());
        assert_eq!(trailers.lookup("x-test"), TrailerValue::Present("value".to_owned()));
        assert_eq!(trailers.lookup("absent"), TrailerValue::Missing);
    }

    #[tokio::test]
    async fn native_framing_and_classified_errors_are_not_unwrapped() {
        use rustfs_gateway_stream::ProducerFailureClass;
        for error in [
            StreamError::incomplete_body(),
            StreamError::length_mismatch(2, 1),
            StreamError::classified_upstream(
                Box::new(std::io::Error::from(std::io::ErrorKind::Interrupted)),
                ProducerFailureClass::Cancelled,
            ),
        ] {
            let error = error.with_bytes_before_error(7);
            let class = error.producer_failure_class();
            let message = error.to_string();
            let (mut chunks, trailers) = BodyChunks::new(Body::from_stream(NativeProbe(Some(Err(error)))).unwrap());
            let error = chunks.next().await.unwrap().unwrap_err();
            let error = error.downcast_ref::<StreamError>().unwrap();
            assert_eq!(error.producer_failure_class(), class);
            assert_eq!(error.bytes_before_error(), 7);
            assert_eq!(error.to_string(), message);
            assert_eq!(trailers.lookup("x-test"), TrailerValue::Pending);
        }
    }

    #[derive(Debug)]
    struct InternalProducerFailure;

    impl std::fmt::Display for InternalProducerFailure {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str("internal producer failure")
        }
    }

    impl std::error::Error for InternalProducerFailure {}

    #[tokio::test]
    async fn internal_producer_error_preserves_io_kind_and_nested_source_after_data() {
        for kind in [
            std::io::ErrorKind::ConnectionReset,
            std::io::ErrorKind::TimedOut,
            std::io::ErrorKind::BrokenPipe,
        ] {
            let body = from_io_stream(futures::stream::iter([
                Ok(Bytes::from_static(b"partial")),
                Err(std::io::Error::new(kind, InternalProducerFailure)),
            ]));
            let (mut chunks, trailers) = BodyChunks::new(body);
            assert_eq!(chunks.next().await.unwrap().unwrap(), Bytes::from_static(b"partial"));
            let error = chunks.next().await.unwrap().unwrap_err();
            let error = error
                .downcast_ref::<std::io::Error>()
                .expect("the original io::Error reaches RIO");
            assert_eq!(error.kind(), kind);
            assert!(error.get_ref().unwrap().is::<InternalProducerFailure>());
            assert_eq!(error.to_string(), "internal producer failure");
            assert_eq!(trailers.lookup("x-test"), TrailerValue::Pending);
            assert!(chunks.next().await.is_none());
        }
    }

    #[test]
    fn reader_boundary_is_send_and_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<BodyChunks>();
    }
}
