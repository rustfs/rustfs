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
//! Responsible for: `Body`, a type-erased `Bytes` body for requests that RustFS
//! builds and sends itself, its constructors (empty and in-memory), and its
//! `http_body::Body` implementation, which forwards frames, errors, end-of-stream
//! and the size hint of whatever it wraps unchanged.
//! Not responsible for: any S3 semantics. Trailers, checksums, aws-chunked
//! framing and read limits belong to the layers that own them, never to this
//! type. The s3s conversions live in `crate::compat_s3s`.
//! Upstream: `bytes`, `http-body`, `http-body-util`. Downstream: the outbound
//! client crates (`rustfs-s3-client`) and the legacy edge through `compat_s3s`.

use crate::StdError;
use bytes::Bytes;
use http_body::{Frame, SizeHint};
use http_body_util::combinators::BoxBody;
use http_body_util::{BodyExt, Full};
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

/// A plain HTTP body: an owned, `Send + Sync`, type-erased stream of `Bytes`
/// frames whose errors are [`StdError`].
pub struct Body(BoxBody<Bytes, StdError>);

impl Body {
    /// A body with no data: it yields no frame and reports an exact length of 0.
    #[must_use]
    pub fn empty() -> Self {
        Self(BoxBody::default())
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
/// always ready, so a no-op waker is enough and `Pending` is a test failure.
#[cfg(test)]
pub(crate) mod test_support {
    use crate::StdError;
    use bytes::Bytes;
    use http_body::{Frame, SizeHint};
    use std::collections::VecDeque;
    use std::pin::Pin;
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
        let mut body = Box::pin(body);
        let mut cx = Context::from_waker(Waker::noop());
        let mut chunks = Vec::new();
        loop {
            match body.as_mut().poll_frame(&mut cx) {
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
                Poll::Pending => panic!("a scripted body is always ready"),
            }
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
    use super::test_support::{Scripted, as_io, drain, io_error};
    use bytes::Bytes;
    use http_body::Body as _;
    use http_body::SizeHint;
    use std::io::ErrorKind;

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

    #[test]
    fn the_body_is_send_sync_unpin_and_static_as_hyper_and_s3s_require() {
        fn require<T: Send + Sync + Unpin + 'static>() {}
        require::<Body>();
    }
}
