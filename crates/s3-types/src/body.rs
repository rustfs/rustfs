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

//! The RustFS-owned plain HTTP body.
//!
//! Responsible for: `Body`, a type-erased `Bytes` body for the requests and
//! responses that RustFS builds itself, its constructors (empty, in-memory, and a
//! chunk stream of unknown length), and its `http_body::Body` implementation,
//! which forwards frames, errors, end-of-stream and the size hint of whatever it
//! wraps unchanged.
//! Not responsible for: any S3 semantics. Trailers, checksums, aws-chunked
//! framing and read limits belong to the layers that own them, never to this
//! type. The s3s conversions live in `crate::compat_s3s`.
//! Upstream: `bytes`, `futures-core`, `http-body`, `http-body-util`. Downstream:
//! the outbound client crates (`rustfs-s3-client`), the Swift handlers in
//! `rustfs-protocols`, and the legacy edge through `compat_s3s`.

use crate::StdError;
use bytes::Bytes;
use futures_core::Stream;
use http_body::{Frame, SizeHint};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll, ready};

/// A plain HTTP body: an owned, `Send + Sync`, type-erased stream of `Bytes`
/// frames whose errors are [`StdError`].
pub struct Body(BoxBody<Bytes, StdError>);

impl Body {
    /// A body with no data: it yields no frame and reports an exact length of 0.
    #[must_use]
    pub fn empty() -> Self {
        Self(BoxBody::default())
    }

    /// A body that yields the chunks of `stream`, for a payload whose length is
    /// not known up front.
    ///
    /// Each `Ok` chunk becomes one data frame, in order and unchanged; an empty
    /// chunk stays an empty frame. The first `Err` is yielded as the body's
    /// error and ends the body: the stream is not polled again. The size hint
    /// is unknown and `is_end_stream` stays false until a poll observes the
    /// end, because `Stream::size_hint` counts items, not bytes. Dropping the
    /// body drops the stream, which cancels whatever the stream drives.
    pub fn from_stream<S, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Sync + 'static,
        E: Into<StdError>,
    {
        Self(BoxBody::new(StreamBody::new(DataFrames { stream, failed: false })))
    }

    /// Wraps any `Bytes` body, keeping its frames, errors and size hint.
    #[cfg(any(test, feature = "compat-s3s"))]
    pub(crate) fn from_http_body<B>(body: B) -> Self
    where
        B: http_body::Body<Data = Bytes> + Send + Sync + 'static,
        B::Error: Into<StdError>,
    {
        Self(BoxBody::new(body.map_err(Into::into)))
    }
}

impl From<Bytes> for Body {
    fn from(bytes: Bytes) -> Self {
        // `Full` holds nothing for an empty buffer, so it ends at once and
        // reports an exact length of 0, the same as `Body::empty()`.
        Self(BoxBody::new(Full::new(bytes).map_err(|never| match never {})))
    }
}

impl From<Vec<u8>> for Body {
    fn from(bytes: Vec<u8>) -> Self {
        Self::from(Bytes::from(bytes))
    }
}

impl From<String> for Body {
    fn from(text: String) -> Self {
        Self::from(Bytes::from(text))
    }
}

impl From<&'static str> for Body {
    fn from(text: &'static str) -> Self {
        Self::from(Bytes::from_static(text.as_bytes()))
    }
}

impl http_body::Body for Body {
    type Data = Bytes;
    type Error = StdError;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, StdError>>> {
        Pin::new(&mut self.get_mut().0).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        http_body::Body::is_end_stream(&self.0)
    }

    fn size_hint(&self) -> SizeHint {
        http_body::Body::size_hint(&self.0)
    }
}

pin_project_lite::pin_project! {
    /// Maps a chunk stream onto data frames and ends it at its first error.
    struct DataFrames<S> {
        #[pin]
        stream: S,
        failed: bool,
    }
}

impl<S, E> Stream for DataFrames<S>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: Into<StdError>,
{
    type Item = Result<Frame<Bytes>, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.failed {
            return Poll::Ready(None);
        }
        match ready!(this.stream.poll_next(cx)) {
            Some(Ok(chunk)) => Poll::Ready(Some(Ok(Frame::data(chunk)))),
            Some(Err(error)) => {
                *this.failed = true;
                Poll::Ready(Some(Err(error.into())))
            }
            None => Poll::Ready(None),
        }
    }
}

/// Shows the length the body reports, never its bytes: they are not buffered
/// here, and a payload has no business in a log line.
impl fmt::Debug for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Body")
            .field("size_hint", &http_body::Body::size_hint(&self.0))
            .finish()
    }
}

/// Drives bodies without an async runtime: the scripted bodies used here are
/// ready unless a script pauses, and a scripted pause wakes itself, so a no-op
/// waker is enough and a body that stays pending is a test failure.
#[cfg(test)]
pub(crate) mod test_support {
    use crate::StdError;
    use bytes::Bytes;
    use futures_core::Stream;
    use http_body::{Frame, SizeHint};
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::task::{Context, Poll, Waker};

    /// A body that yields a fixed script of data frames and errors and reports
    /// a fixed size hint, so forwarding can be observed exactly.
    pub(crate) struct Scripted {
        frames: VecDeque<Result<Bytes, StdError>>,
        hint: SizeHint,
    }

    impl Scripted {
        pub(crate) fn new(frames: Vec<Result<Bytes, StdError>>, hint: SizeHint) -> Self {
            Self {
                frames: frames.into(),
                hint,
            }
        }

        pub(crate) fn chunks(chunks: &[&'static [u8]]) -> Self {
            let total = chunks
                .iter()
                .map(|chunk| u64::try_from(chunk.len()).expect("a chunk length fits in u64"))
                .sum();
            Self::new(
                chunks.iter().map(|chunk| Ok(Bytes::from_static(chunk))).collect(),
                SizeHint::with_exact(total),
            )
        }
    }

    impl http_body::Body for Scripted {
        type Data = Bytes;
        type Error = StdError;

        fn poll_frame(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>, StdError>>> {
            Poll::Ready(self.frames.pop_front().map(|frame| frame.map(Frame::data)))
        }

        fn size_hint(&self) -> SizeHint {
            self.hint
        }
    }

    /// What a body yielded until it ended or failed: its data frames in order,
    /// and the error that stopped it, if any.
    pub(crate) struct Drained<E> {
        pub(crate) chunks: Vec<Bytes>,
        pub(crate) error: Option<E>,
    }

    impl<E> Drained<E> {
        pub(crate) fn bytes(&self) -> Vec<u8> {
            self.chunks.iter().flat_map(|chunk| chunk.iter().copied()).collect()
        }
    }

    pub(crate) fn drain<B>(body: B) -> Drained<B::Error>
    where
        B: http_body::Body<Data = Bytes>,
    {
        drain_pinned(Box::pin(body).as_mut())
    }

    /// Polls `body` once with a no-op waker.
    pub(crate) fn poll_frame<B>(body: Pin<&mut B>) -> Poll<Option<Result<Frame<Bytes>, B::Error>>>
    where
        B: http_body::Body<Data = Bytes> + ?Sized,
    {
        body.poll_frame(&mut Context::from_waker(Waker::noop()))
    }

    /// Like `drain`, but leaves the body with the caller, so it can be polled
    /// again after it ended or failed.
    pub(crate) fn drain_pinned<B>(mut body: Pin<&mut B>) -> Drained<B::Error>
    where
        B: http_body::Body<Data = Bytes> + ?Sized,
    {
        let mut chunks = Vec::new();
        let mut paused = false;
        loop {
            let polled = poll_frame(body.as_mut());
            if polled.is_ready() {
                paused = false;
            }
            match polled {
                Poll::Ready(None) => return Drained { chunks, error: None },
                Poll::Ready(Some(Ok(frame))) => {
                    let data = frame.into_data().unwrap_or_else(|_| panic!("only data frames are scripted"));
                    chunks.push(data);
                }
                Poll::Ready(Some(Err(error))) => {
                    return Drained {
                        chunks,
                        error: Some(error),
                    };
                }
                // A script may pause once at a time; it wakes itself, so the
                // next poll makes progress. A second pause in a row is a bug.
                Poll::Pending if !paused => paused = true,
                Poll::Pending => panic!("a scripted body pauses at most once in a row"),
            }
        }
    }

    /// A stream that plays a fixed script of poll results, counts how often it
    /// is polled, claims a fixed `Stream::size_hint`, and raises a flag when
    /// it is dropped, so `Body::from_stream` can be observed exactly.
    pub(crate) struct ScriptedStream<E = std::io::Error> {
        script: VecDeque<Poll<Option<Result<Bytes, E>>>>,
        claim: (usize, Option<usize>),
        polls: Counter,
        dropped: Flag,
    }

    /// A poll counter shared between a scripted stream and its test.
    #[derive(Clone, Default)]
    pub(crate) struct Counter(Arc<AtomicUsize>);

    impl Counter {
        pub(crate) fn get(&self) -> usize {
            self.0.load(Ordering::SeqCst)
        }
    }

    /// A drop flag shared between a scripted stream and its test.
    #[derive(Clone, Default)]
    pub(crate) struct Flag(Arc<AtomicBool>);

    impl Flag {
        pub(crate) fn get(&self) -> bool {
            self.0.load(Ordering::SeqCst)
        }
    }

    impl<E> ScriptedStream<E> {
        pub(crate) fn new(script: Vec<Poll<Option<Result<Bytes, E>>>>) -> (Self, Counter) {
            let polls = Counter::default();
            let stream = Self {
                script: script.into(),
                claim: (0, None),
                polls: polls.clone(),
                dropped: Flag::default(),
            };
            (stream, polls)
        }

        /// Makes the stream claim `claim` as its item-count size hint.
        pub(crate) fn claiming(mut self, claim: (usize, Option<usize>)) -> Self {
            self.claim = claim;
            self
        }

        pub(crate) fn drop_flag(&self) -> Flag {
            self.dropped.clone()
        }
    }

    impl<E: Unpin> Stream for ScriptedStream<E> {
        type Item = Result<Bytes, E>;

        fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            // Every field is Unpin, so the stream is too.
            let this = self.get_mut();
            this.polls.0.fetch_add(1, Ordering::SeqCst);
            match this.script.pop_front() {
                Some(Poll::Pending) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Some(Poll::Ready(item)) => Poll::Ready(item),
                None => Poll::Ready(None),
            }
        }

        fn size_hint(&self) -> (usize, Option<usize>) {
            self.claim
        }
    }

    impl<E> Drop for ScriptedStream<E> {
        fn drop(&mut self) {
            self.dropped.0.store(true, Ordering::SeqCst);
        }
    }

    pub(crate) fn io_error(kind: std::io::ErrorKind, text: &'static str) -> StdError {
        Box::new(std::io::Error::new(kind, text))
    }

    /// The `io::Error` a body error carries, if it is one.
    pub(crate) fn as_io(error: &StdError) -> Option<&std::io::Error> {
        error.downcast_ref::<std::io::Error>()
    }
}

#[cfg(test)]
mod tests {
    use super::Body;
    use super::test_support::{Scripted, ScriptedStream, as_io, drain, drain_pinned, io_error, poll_frame};
    use crate::StdError;
    use bytes::Bytes;
    use http_body::Body as _;
    use http_body::SizeHint;
    use std::io::{self, ErrorKind};
    use std::task::Poll;

    fn exact(body: &Body) -> Option<u64> {
        body.size_hint().exact()
    }

    // ---- positive: what goes in comes out ----

    #[test]
    fn empty_yields_no_frame_and_reports_an_exact_zero_length() {
        let body = Body::empty();
        assert!(body.is_end_stream());
        assert_eq!(exact(&body), Some(0));
        let drained = drain(body);
        assert!(drained.chunks.is_empty(), "an empty body yielded {:?}", drained.chunks);
        assert!(drained.error.is_none());
    }

    #[test]
    fn in_memory_constructors_yield_their_bytes_in_one_frame_with_an_exact_length() {
        let cases: [(&str, Body); 4] = [
            ("Bytes", Body::from(Bytes::from_static(b"hello"))),
            ("Vec<u8>", Body::from(b"hello".to_vec())),
            ("String", Body::from(String::from("hello"))),
            ("&'static str", Body::from("hello")),
        ];
        for (name, body) in cases {
            assert!(!body.is_end_stream(), "{name}: data remains before the first poll");
            assert_eq!(exact(&body), Some(5), "{name}");
            let drained = drain(body);
            assert_eq!(drained.chunks, [Bytes::from_static(b"hello")], "{name}");
            assert!(drained.error.is_none(), "{name}");
        }
    }

    #[test]
    fn a_multi_chunk_body_yields_every_chunk_in_order() {
        let body = Body::from_http_body(Scripted::chunks(&[b"ab", b"", b"cde", b"f"]));
        assert_eq!(exact(&body), Some(6));
        let drained = drain(body);
        assert_eq!(
            drained.chunks,
            [
                Bytes::from_static(b"ab"),
                Bytes::new(),
                Bytes::from_static(b"cde"),
                Bytes::from_static(b"f")
            ]
        );
        assert_eq!(drained.bytes(), b"abcdef");
        assert!(drained.error.is_none());
    }

    // ---- negative and boundary: nothing invented, nothing lost ----

    #[test]
    fn an_empty_buffer_behaves_exactly_like_empty() {
        let cases: [(&str, Body); 4] = [
            ("Bytes", Body::from(Bytes::new())),
            ("Vec<u8>", Body::from(Vec::new())),
            ("String", Body::from(String::new())),
            ("&'static str", Body::from("")),
        ];
        for (name, body) in cases {
            assert!(body.is_end_stream(), "{name}");
            assert_eq!(exact(&body), Some(0), "{name}");
            let drained = drain(body);
            assert!(drained.chunks.is_empty(), "{name}: yielded {:?}", drained.chunks);
            assert!(drained.error.is_none(), "{name}");
        }
    }

    #[test]
    fn a_ranged_size_hint_is_forwarded_not_narrowed() {
        let mut hint = SizeHint::new();
        hint.set_lower(3);
        hint.set_upper(10);
        let body = Body::from_http_body(Scripted::new(vec![Ok(Bytes::from_static(b"abcd"))], hint));
        assert_eq!(body.size_hint().lower(), 3);
        assert_eq!(body.size_hint().upper(), Some(10));
        assert_eq!(exact(&body), None);
    }

    #[test]
    fn an_unknown_length_stays_unknown() {
        let body = Body::from_http_body(Scripted::new(vec![Ok(Bytes::from_static(b"x"))], SizeHint::new()));
        assert_eq!(body.size_hint().lower(), 0);
        assert_eq!(body.size_hint().upper(), None);
        assert!(!body.is_end_stream());
    }

    #[test]
    fn an_error_after_data_surfaces_on_the_frame_that_failed() {
        let body = Body::from_http_body(Scripted::new(
            vec![
                Ok(Bytes::from_static(b"partial")),
                Err(io_error(ErrorKind::TimedOut, "peer stalled")),
                Ok(Bytes::from_static(b"never read")),
            ],
            SizeHint::new(),
        ));
        let drained = drain(body);
        assert_eq!(drained.bytes(), b"partial");
        let error = drained.error.expect("the scripted error must surface");
        let io = as_io(&error).expect("the error keeps its concrete type");
        assert_eq!(io.kind(), ErrorKind::TimedOut);
        assert_eq!(error.to_string(), "peer stalled");
    }

    #[test]
    fn an_error_before_any_data_surfaces_first() {
        let body = Body::from_http_body(Scripted::new(
            vec![Err(io_error(ErrorKind::ConnectionReset, "reset before data"))],
            SizeHint::with_exact(4),
        ));
        let drained = drain(body);
        assert!(drained.chunks.is_empty());
        let error = drained.error.expect("the scripted error must surface");
        assert_eq!(as_io(&error).map(std::io::Error::kind), Some(ErrorKind::ConnectionReset));
    }

    // ---- from_stream: a chunk stream of unknown length ----

    fn chunk(bytes: &'static [u8]) -> Poll<Option<io::Result<Bytes>>> {
        Poll::Ready(Some(Ok(Bytes::from_static(bytes))))
    }

    fn failure(kind: ErrorKind, text: &'static str) -> Poll<Option<io::Result<Bytes>>> {
        Poll::Ready(Some(Err(io::Error::new(kind, text))))
    }

    #[test]
    fn from_stream_yields_every_chunk_as_one_frame_in_order() {
        let (stream, _) = ScriptedStream::new(vec![chunk(b"ab"), chunk(b""), chunk(b"cde"), chunk(b"f")]);
        let drained = drain(Body::from_stream(stream));
        assert_eq!(
            drained.chunks,
            [
                Bytes::from_static(b"ab"),
                Bytes::new(),
                Bytes::from_static(b"cde"),
                Bytes::from_static(b"f")
            ],
            "chunks are neither merged, split nor dropped, an empty one included"
        );
        assert!(drained.error.is_none());
    }

    #[test]
    fn from_stream_surfaces_an_error_after_the_data_before_it_and_then_ends() {
        let (stream, polls) = ScriptedStream::new(vec![
            chunk(b"partial"),
            failure(ErrorKind::TimedOut, "peer stalled"),
            chunk(b"never read"),
        ]);
        let mut body = Box::pin(Body::from_stream(stream));
        let drained = drain_pinned(body.as_mut());
        assert_eq!(drained.bytes(), b"partial");
        let error = drained.error.expect("the stream error must surface");
        assert_eq!(as_io(&error).map(io::Error::kind), Some(ErrorKind::TimedOut));
        assert_eq!(error.to_string(), "peer stalled");
        assert_eq!(polls.get(), 2, "the stream is polled up to its error");
        assert!(matches!(poll_frame(body.as_mut()), Poll::Ready(None)), "the body ends at its error");
        assert_eq!(polls.get(), 2, "the stream is not polled past its error");
    }

    #[test]
    fn from_stream_surfaces_an_error_before_any_data() {
        let (stream, _) = ScriptedStream::new(vec![failure(ErrorKind::ConnectionReset, "reset"), chunk(b"late")]);
        let mut body = Box::pin(Body::from_stream(stream));
        let drained = drain_pinned(body.as_mut());
        assert!(drained.chunks.is_empty(), "yielded {:?}", drained.chunks);
        assert_eq!(
            drained.error.as_ref().and_then(as_io).map(io::Error::kind),
            Some(ErrorKind::ConnectionReset)
        );
        assert!(matches!(poll_frame(body.as_mut()), Poll::Ready(None)));
    }

    #[test]
    fn from_stream_of_an_empty_stream_ends_at_the_first_poll() {
        let (stream, polls) = ScriptedStream::<io::Error>::new(Vec::new());
        let mut body = Box::pin(Body::from_stream(stream));
        assert!(matches!(poll_frame(body.as_mut()), Poll::Ready(None)));
        assert_eq!(polls.get(), 1, "one poll observes the end");
    }

    #[test]
    fn from_stream_waits_while_the_stream_is_pending() {
        let (stream, _) = ScriptedStream::new(vec![Poll::Pending, chunk(b"late")]);
        let mut body = Box::pin(Body::from_stream(stream));
        assert!(poll_frame(body.as_mut()).is_pending(), "a pending stream is neither the end nor a frame");
        let drained = drain_pinned(body.as_mut());
        assert_eq!(drained.bytes(), b"late");
    }

    #[test]
    fn from_stream_reports_an_unknown_length_whatever_the_stream_claims() {
        // `Stream::size_hint` counts items, not bytes, and a stream may claim
        // anything; only the bytes on the wire count, and they are unknown.
        let claims = [(0, Some(0)), (1, Some(1)), (3, None), (usize::MAX, Some(usize::MAX))];
        for claim in claims {
            let (stream, _) = ScriptedStream::new(vec![chunk(b"abc")]);
            let body = Body::from_stream(stream.claiming(claim));
            assert_eq!(body.size_hint().lower(), 0, "{claim:?}");
            assert_eq!(body.size_hint().upper(), None, "{claim:?}");
            assert!(!body.is_end_stream(), "{claim:?}: the end is learnt by polling");
        }
        let (stream, _) = ScriptedStream::<io::Error>::new(Vec::new());
        let empty = Body::from_stream(stream.claiming((0, Some(0))));
        assert_eq!(exact(&empty), None, "an empty stream is not known to be empty before it is polled");
        assert!(!empty.is_end_stream());
    }

    #[test]
    fn from_stream_keeps_the_concrete_error_type() {
        let (stream, _) = ScriptedStream::new(vec![failure(ErrorKind::PermissionDenied, "denied")]);
        let error = drain(Body::from_stream(stream)).error.expect("the stream error must surface");
        assert!(error.is::<io::Error>(), "the io::Error is boxed, not re-wrapped: {error:?}");

        let boxed = stream_of_std_errors();
        let error = drain(Body::from_stream(boxed)).error.expect("a boxed error must surface");
        assert_eq!(error.to_string(), "already boxed");
    }

    #[test]
    fn dropping_a_from_stream_body_drops_the_stream() {
        let (stream, _) = ScriptedStream::new(vec![chunk(b"first"), chunk(b"second")]);
        let dropped = stream.drop_flag();
        let mut body = Box::pin(Body::from_stream(stream));
        let first = poll_frame(body.as_mut());
        assert!(matches!(first, Poll::Ready(Some(Ok(_)))), "one frame is read before the drop");
        assert!(!dropped.get(), "the body owns the stream while it lives");
        drop(body);
        assert!(dropped.get(), "dropping the body must drop, and so cancel, the stream");
    }

    fn stream_of_std_errors() -> ScriptedStream<StdError> {
        ScriptedStream::new(vec![Poll::Ready(Some(Err(StdError::from("already boxed"))))]).0
    }

    #[test]
    fn the_body_is_send_sync_unpin_and_static_as_hyper_and_s3s_require() {
        fn require<T: Send + Sync + Unpin + 'static>() {}
        require::<Body>();
    }
}
