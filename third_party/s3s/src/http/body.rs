use crate::error::StdError;
use crate::stream::ByteStream;
use crate::stream::DynByteStream;
use crate::stream::RemainingLength;

use std::fmt;
use std::mem;
use std::pin::Pin;
use std::sync::Mutex;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::Stream;

use http_body::Frame;

type BoxBody = http_body_util::combinators::BoxBody<Bytes, StdError>;
type UnsyncBoxBody = http_body_util::combinators::UnsyncBoxBody<Bytes, StdError>;

pin_project_lite::pin_project! {
    #[derive(Default)]
    pub struct Body {
        #[pin]
        kind: Kind,
    }
}

pin_project_lite::pin_project! {
    #[project = KindProj]
    #[derive(Default)]
    enum Kind {
        #[default]
        Empty,
        Once {
            inner: Bytes,
        },
        Hyper {
            #[pin]
            inner: hyper::body::Incoming,
        },
        BoxBody {
            #[pin]
            inner: BoxBody,
        },
        UnsyncBoxBody {
            #[pin]
            inner: Mutex<UnsyncBoxBody>,
        },
        DynStream {
            #[pin]
            inner: DynByteStream
        }
    }
}

impl Body {
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    fn once(bytes: Bytes) -> Self {
        Self {
            kind: Kind::Once { inner: bytes },
        }
    }

    fn hyper(body: hyper::body::Incoming) -> Self {
        Self {
            kind: Kind::Hyper { inner: body },
        }
    }

    fn dyn_stream(stream: DynByteStream) -> Self {
        Self {
            kind: Kind::DynStream { inner: stream },
        }
    }

    #[must_use]
    pub fn http_body<B>(body: B) -> Self
    where
        B: http_body::Body<Data = Bytes> + Send + Sync + 'static,
        StdError: From<B::Error>,
    {
        Self {
            kind: Kind::BoxBody {
                inner: BoxBody::new(http_body_util::BodyExt::map_err(body, From::from)),
            },
        }
    }

    #[must_use]
    pub fn http_body_unsync<B>(body: B) -> Self
    where
        B: http_body::Body<Data = Bytes> + Send + 'static,
        StdError: From<B::Error>,
    {
        Self {
            kind: Kind::UnsyncBoxBody {
                inner: Mutex::new(UnsyncBoxBody::new(http_body_util::BodyExt::map_err(body, From::from))),
            },
        }
    }
}

impl From<Bytes> for Body {
    fn from(bytes: Bytes) -> Self {
        Self::once(bytes)
    }
}

impl From<Vec<u8>> for Body {
    fn from(value: Vec<u8>) -> Self {
        Self::once(value.into())
    }
}

impl From<String> for Body {
    fn from(value: String) -> Self {
        Self::once(value.into())
    }
}

impl From<hyper::body::Incoming> for Body {
    fn from(body: hyper::body::Incoming) -> Self {
        Self::hyper(body)
    }
}

impl From<DynByteStream> for Body {
    fn from(stream: DynByteStream) -> Self {
        Self::dyn_stream(stream)
    }
}

impl http_body::Body for Body {
    type Data = Bytes;

    type Error = StdError;

    fn poll_frame(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        let mut this = self.project();
        match this.kind.as_mut().project() {
            KindProj::Empty => {
                Poll::Ready(None) //
            }
            KindProj::Once { inner } => {
                let bytes = mem::take(inner);
                this.kind.set(Kind::Empty);
                if bytes.is_empty() {
                    Poll::Ready(None)
                } else {
                    Poll::Ready(Some(Ok(Frame::data(bytes))))
                }
            }
            KindProj::Hyper { inner } => {
                http_body::Body::poll_frame(inner, cx).map_err(From::from)
                //
            }
            KindProj::BoxBody { inner } => {
                http_body::Body::poll_frame(inner, cx)
                //
            }
            KindProj::UnsyncBoxBody { inner } => {
                let mut inner = inner.lock().unwrap();
                http_body::Body::poll_frame(Pin::new(&mut *inner), cx)
                //
            }
            KindProj::DynStream { inner } => {
                Stream::poll_next(inner, cx).map_ok(Frame::data)
                //
            }
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.kind {
            Kind::Empty => true,
            Kind::Once { inner } => inner.is_empty(),
            Kind::Hyper { inner } => http_body::Body::is_end_stream(inner),
            Kind::BoxBody { inner } => http_body::Body::is_end_stream(inner),
            Kind::UnsyncBoxBody { inner } => inner.lock().unwrap().is_end_stream(),
            Kind::DynStream { inner } => inner.remaining_length().exact() == Some(0),
        }
    }

    fn size_hint(&self) -> http_body::SizeHint {
        match &self.kind {
            Kind::Empty => http_body::SizeHint::with_exact(0),
            Kind::Once { inner } => http_body::SizeHint::with_exact(inner.len() as u64),
            Kind::Hyper { inner } => http_body::Body::size_hint(inner),
            Kind::BoxBody { inner } => http_body::Body::size_hint(inner),
            Kind::UnsyncBoxBody { inner } => inner.lock().unwrap().size_hint(),
            Kind::DynStream { inner } => inner.remaining_length().into(),
        }
    }
}

impl Stream for Body {
    type Item = Result<Bytes, StdError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match std::task::ready!(http_body::Body::poll_frame(self.as_mut(), cx)?) {
                Some(frame) => match frame.into_data() {
                    Ok(data) => return Poll::Ready(Some(Ok(data))),
                    Err(_frame) => continue,
                },
                None => return Poll::Ready(None),
            };
        }
    }
}

impl ByteStream for Body {
    fn remaining_length(&self) -> RemainingLength {
        match &self.kind {
            Kind::Empty => RemainingLength::new_exact(0),
            Kind::Once { inner } => RemainingLength::new_exact(inner.len()),
            Kind::Hyper { inner } => http_body::Body::size_hint(inner).into(),
            Kind::BoxBody { inner } => http_body::Body::size_hint(inner).into(),
            Kind::UnsyncBoxBody { inner } => http_body::Body::size_hint(&*inner.lock().unwrap()).into(),
            Kind::DynStream { inner } => inner.remaining_length(),
        }
    }
}

impl fmt::Debug for Body {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut d = f.debug_struct("Body");
        match &self.kind {
            Kind::Empty => {}
            Kind::Once { inner } => {
                d.field("once", inner);
            }
            Kind::Hyper { inner } => {
                d.field("hyper", inner);
            }
            Kind::BoxBody { inner } => {
                d.field("body", &"{..}");
                d.field("remaining_length", &http_body::Body::size_hint(inner));
            }
            Kind::UnsyncBoxBody { inner } => {
                d.field("body", &"{..}");
                d.field("remaining_length", &http_body::Body::size_hint(&*inner.lock().unwrap()));
            }
            Kind::DynStream { inner } => {
                d.field("dyn_stream", &"{..}");
                d.field("remaining_length", &inner.remaining_length());
            }
        }
        d.finish()
    }
}

/// Error returned when body size exceeds the limit.
#[derive(Debug, Clone, thiserror::Error)]
#[error("body size {size} exceeds limit {limit}")]
pub struct BodySizeLimitExceeded {
    /// The actual body size.
    pub size: usize,
    /// The maximum allowed size.
    pub limit: usize,
}

impl Body {
    /// Stores all bytes in memory with a size limit.
    ///
    /// # Errors
    /// Returns an error if the body exceeds `limit` bytes or if reading fails.
    pub async fn store_all_limited(&mut self, limit: usize) -> Result<Bytes, StdError> {
        if let Some(bytes) = self.bytes() {
            if bytes.len() > limit {
                return Err(Box::new(BodySizeLimitExceeded {
                    size: bytes.len(),
                    limit,
                }));
            }
            return Ok(bytes);
        }
        let body = mem::take(self);
        let limited = http_body_util::Limited::new(body, limit);
        let bytes: Bytes = http_body_util::BodyExt::collect(limited).await?.to_bytes();
        // Store bytes in self (Bytes::clone is O(1) due to reference counting)
        *self = Self::from(bytes.clone());
        Ok(bytes)
    }

    pub fn bytes(&self) -> Option<Bytes> {
        match &self.kind {
            Kind::Empty => Some(Bytes::new()),
            Kind::Once { inner } => Some(inner.clone()),
            _ => None,
        }
    }

    pub fn take_bytes(&mut self) -> Option<Bytes> {
        match mem::take(&mut self.kind) {
            Kind::Empty => Some(Bytes::new()),
            Kind::Once { inner } => Some(inner),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::StreamExt;

    #[tokio::test]
    async fn test_store_all_limited_success() {
        let data = b"hello world";
        let mut body = Body::from(Bytes::from_static(data));
        let result = body.store_all_limited(20).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap().as_ref(), data);
    }

    #[tokio::test]
    async fn test_store_all_limited_exceeds() {
        let data = b"hello world";
        let mut body = Body::from(Bytes::from_static(data));
        let result = body.store_all_limited(5).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_store_all_limited_empty() {
        let mut body = Body::empty();
        let result = body.store_all_limited(10).await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_empty());
    }

    #[test]
    fn body_empty_is_end_stream() {
        let body = Body::empty();
        assert!(http_body::Body::is_end_stream(&body));
    }

    #[test]
    fn body_once_is_end_stream() {
        let body = Body::from(Bytes::from_static(b"data"));
        assert!(!http_body::Body::is_end_stream(&body));

        let body = Body::from(Bytes::new());
        assert!(http_body::Body::is_end_stream(&body));
    }

    #[test]
    fn body_empty_size_hint() {
        let body = Body::empty();
        let hint = http_body::Body::size_hint(&body);
        assert_eq!(hint.lower(), 0);
        assert_eq!(hint.upper(), Some(0));
    }

    #[test]
    fn body_once_size_hint() {
        let body = Body::from(Bytes::from_static(b"hello"));
        let hint = http_body::Body::size_hint(&body);
        assert_eq!(hint.lower(), 5);
        assert_eq!(hint.upper(), Some(5));
    }

    #[test]
    fn body_empty_poll_frame() {
        let mut body = Body::empty();
        let frame = http_body::Body::poll_frame(
            Pin::new(&mut body),
            &mut std::task::Context::from_waker(futures::task::noop_waker_ref()),
        );
        assert!(matches!(frame, Poll::Ready(None)));
    }

    #[test]
    fn body_once_poll_frame() {
        let mut body = Body::from(Bytes::from_static(b"hello"));
        let waker = futures::task::noop_waker_ref();
        let mut cx = std::task::Context::from_waker(waker);
        let frame = http_body::Body::poll_frame(Pin::new(&mut body), &mut cx);
        match frame {
            Poll::Ready(Some(Ok(frame))) => {
                let data = frame.into_data().unwrap();
                assert_eq!(data.as_ref(), b"hello");
            }
            _ => panic!("expected data frame"),
        }
        // Second poll should return None
        let frame = http_body::Body::poll_frame(Pin::new(&mut body), &mut cx);
        assert!(matches!(frame, Poll::Ready(None)));
    }

    #[test]
    fn body_once_empty_bytes_poll_frame() {
        let mut body = Body::from(Bytes::new());
        let waker = futures::task::noop_waker_ref();
        let mut cx = std::task::Context::from_waker(waker);
        let frame = http_body::Body::poll_frame(Pin::new(&mut body), &mut cx);
        assert!(matches!(frame, Poll::Ready(None)));
    }

    #[test]
    fn body_from_vec() {
        let body = Body::from(vec![1u8, 2, 3]);
        assert!(!http_body::Body::is_end_stream(&body));
        let hint = http_body::Body::size_hint(&body);
        assert_eq!(hint.lower(), 3);
    }

    #[test]
    fn body_from_string() {
        let body = Body::from("hello".to_string());
        assert!(!http_body::Body::is_end_stream(&body));
        let hint = http_body::Body::size_hint(&body);
        assert_eq!(hint.lower(), 5);
    }

    #[test]
    fn body_bytes_empty() {
        let body = Body::empty();
        let bytes = body.bytes();
        assert_eq!(bytes, Some(Bytes::new()));
    }

    #[test]
    fn body_bytes_once() {
        let body = Body::from(Bytes::from_static(b"hello"));
        let bytes = body.bytes();
        assert_eq!(bytes, Some(Bytes::from_static(b"hello")));
    }

    #[test]
    fn body_take_bytes_empty() {
        let mut body = Body::empty();
        let bytes = body.take_bytes();
        assert_eq!(bytes, Some(Bytes::new()));
    }

    #[test]
    fn body_take_bytes_once() {
        let mut body = Body::from(Bytes::from_static(b"hello"));
        let bytes = body.take_bytes();
        assert_eq!(bytes, Some(Bytes::from_static(b"hello")));
        // After take, body should be empty
        assert!(http_body::Body::is_end_stream(&body));
    }

    #[test]
    fn body_debug_empty() {
        let body = Body::empty();
        let debug = format!("{body:?}");
        assert!(debug.contains("Body"));
    }

    #[test]
    fn body_debug_once() {
        let body = Body::from(Bytes::from_static(b"hi"));
        let debug = format!("{body:?}");
        assert!(debug.contains("Body"));
        assert!(debug.contains("once"));
    }

    #[tokio::test]
    async fn body_stream_impl() {
        let mut body = Body::from(Bytes::from_static(b"stream"));
        let mut collected = Vec::new();
        while let Some(chunk) = body.next().await {
            collected.push(chunk.unwrap());
        }
        assert_eq!(collected, vec![Bytes::from_static(b"stream")]);
    }

    #[tokio::test]
    async fn body_stream_empty() {
        let mut body = Body::empty();
        let next = body.next().await;
        assert!(next.is_none());
    }

    #[test]
    fn body_byte_stream_remaining_length_empty() {
        let body = Body::empty();
        let rl = ByteStream::remaining_length(&body);
        assert_eq!(rl.exact(), Some(0));
    }

    #[test]
    fn body_byte_stream_remaining_length_once() {
        let body = Body::from(Bytes::from_static(b"12345"));
        let rl = ByteStream::remaining_length(&body);
        assert_eq!(rl.exact(), Some(5));
    }

    #[test]
    fn body_http_body_boxed() {
        let inner = http_body_util::Full::new(Bytes::from_static(b"boxed"));
        let body = Body::http_body(inner);
        let hint = http_body::Body::size_hint(&body);
        assert_eq!(hint.lower(), 5);
        assert!(!http_body::Body::is_end_stream(&body));
    }

    #[test]
    fn body_http_body_unsync() {
        let inner = http_body_util::Full::new(Bytes::from_static(b"unsync"));
        let body = Body::http_body_unsync(inner);
        let hint = http_body::Body::size_hint(&body);
        assert_eq!(hint.lower(), 6);
        assert!(!http_body::Body::is_end_stream(&body));
    }

    #[test]
    fn body_debug_box_body() {
        let inner = http_body_util::Full::new(Bytes::from_static(b"test"));
        let body = Body::http_body(inner);
        let debug = format!("{body:?}");
        assert!(debug.contains("Body"));
        assert!(debug.contains("remaining_length"));
    }

    #[test]
    fn body_debug_unsync_box_body() {
        let inner = http_body_util::Full::new(Bytes::from_static(b"test"));
        let body = Body::http_body_unsync(inner);
        let debug = format!("{body:?}");
        assert!(debug.contains("Body"));
        assert!(debug.contains("remaining_length"));
    }

    #[test]
    fn body_from_dyn_byte_stream() {
        let inner = Body::from(Bytes::from_static(b"dyn"));
        let dyn_stream: DynByteStream = Box::pin(inner);
        let body = Body::from(dyn_stream);
        let debug = format!("{body:?}");
        assert!(debug.contains("dyn_stream"));
    }

    #[test]
    fn body_size_limit_exceeded_display() {
        let err = BodySizeLimitExceeded { size: 100, limit: 50 };
        let msg = format!("{err}");
        assert!(msg.contains("100"));
        assert!(msg.contains("50"));
    }

    #[tokio::test]
    async fn body_boxed_poll_frame() {
        let inner = http_body_util::Full::new(Bytes::from_static(b"boxed"));
        let mut body = Body::http_body(inner);
        let mut collected = Vec::new();
        while let Some(chunk) = body.next().await {
            collected.push(chunk.unwrap());
        }
        assert_eq!(collected, vec![Bytes::from_static(b"boxed")]);
    }

    #[tokio::test]
    async fn body_unsync_poll_frame() {
        let inner = http_body_util::Full::new(Bytes::from_static(b"unsync"));
        let mut body = Body::http_body_unsync(inner);
        let mut collected = Vec::new();
        while let Some(chunk) = body.next().await {
            collected.push(chunk.unwrap());
        }
        assert_eq!(collected, vec![Bytes::from_static(b"unsync")]);
    }

    #[test]
    fn body_bytes_returns_none_for_box_body() {
        let inner = http_body_util::Full::new(Bytes::from_static(b"test"));
        let body = Body::http_body(inner);
        assert!(body.bytes().is_none());
    }

    #[test]
    fn body_take_bytes_returns_none_for_box_body() {
        let inner = http_body_util::Full::new(Bytes::from_static(b"test"));
        let mut body = Body::http_body(inner);
        assert!(body.take_bytes().is_none());
    }
}
