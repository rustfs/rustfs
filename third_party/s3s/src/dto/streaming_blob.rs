//! Streaming blob

use crate::error::StdError;
use crate::http::Body;
use crate::stream::*;

use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::Stream;
use hyper::body::Bytes;

pub struct StreamingBlob {
    inner: DynByteStream,
}

impl StreamingBlob {
    pub fn new<S>(stream: S) -> Self
    where
        S: ByteStream<Item = Result<Bytes, StdError>> + Send + Sync + 'static,
    {
        Self { inner: Box::pin(stream) }
    }

    pub fn wrap<S, E>(stream: S) -> Self
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Sync + 'static,
        E: std::error::Error + Send + Sync + 'static,
    {
        Self { inner: wrap(stream) }
    }

    fn into_inner(self) -> DynByteStream {
        self.inner
    }
}

impl fmt::Debug for StreamingBlob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamingBlob")
            .field("remaining_length", &self.remaining_length())
            .finish_non_exhaustive()
    }
}

impl Stream for StreamingBlob {
    type Item = Result<Bytes, StdError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.inner).poll_next(cx)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl ByteStream for StreamingBlob {
    fn remaining_length(&self) -> RemainingLength {
        self.inner.remaining_length()
    }
}

impl From<StreamingBlob> for DynByteStream {
    fn from(value: StreamingBlob) -> Self {
        value.into_inner()
    }
}

impl From<DynByteStream> for StreamingBlob {
    fn from(value: DynByteStream) -> Self {
        Self { inner: value }
    }
}

impl From<StreamingBlob> for Body {
    fn from(value: StreamingBlob) -> Self {
        Body::from(value.into_inner())
    }
}

impl From<Body> for StreamingBlob {
    fn from(value: Body) -> Self {
        Self::new(value)
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct StreamWrapper<S> {
        #[pin]
        inner: S
    }
}

impl<S, E> Stream for StreamWrapper<S>
where
    S: Stream<Item = Result<Bytes, E>> + Send + Sync + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        this.inner.poll_next(cx).map_err(|e| Box::new(e) as StdError)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<S> ByteStream for StreamWrapper<S>
where
    StreamWrapper<S>: Stream<Item = Result<Bytes, StdError>>,
{
    fn remaining_length(&self) -> RemainingLength {
        RemainingLength::unknown()
    }
}

fn wrap<S>(inner: S) -> DynByteStream
where
    StreamWrapper<S>: ByteStream<Item = Result<Bytes, StdError>> + Send + Sync + 'static,
{
    Box::pin(StreamWrapper { inner })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use http_body::Body as HttpBody;

    #[tokio::test]
    async fn streaming_blob_new_and_poll() {
        let body = Body::from(Bytes::from_static(b"hello world"));
        let mut blob = StreamingBlob::new(body);
        let mut collected = Vec::new();
        while let Some(chunk) = blob.next().await {
            collected.push(chunk.unwrap());
        }
        assert_eq!(collected, vec![Bytes::from_static(b"hello world")]);
    }

    #[tokio::test]
    async fn streaming_blob_wrap() {
        let data = vec![
            Ok::<_, std::io::Error>(Bytes::from_static(b"abc")),
            Ok(Bytes::from_static(b"def")),
        ];
        let stream = futures::stream::iter(data);
        let mut blob = StreamingBlob::wrap(stream);
        let mut collected = Vec::new();
        while let Some(chunk) = blob.next().await {
            collected.push(chunk.unwrap());
        }
        assert_eq!(collected, vec![Bytes::from_static(b"abc"), Bytes::from_static(b"def")]);
    }

    #[test]
    fn streaming_blob_debug() {
        let body = Body::from(Bytes::from_static(b"test"));
        let blob = StreamingBlob::new(body);
        let debug = format!("{blob:?}");
        assert!(debug.contains("StreamingBlob"));
        assert!(debug.contains("remaining_length"));
    }

    #[test]
    fn streaming_blob_remaining_length() {
        let body = Body::from(Bytes::from_static(b"hello"));
        let blob = StreamingBlob::new(body);
        let rl = blob.remaining_length();
        assert_eq!(rl.exact(), Some(5));
    }

    #[test]
    fn streaming_blob_from_body_roundtrip() {
        let body = Body::from(Bytes::from_static(b"data"));
        let blob = StreamingBlob::from(body);
        let body_back: Body = Body::from(blob);
        assert!(!HttpBody::is_end_stream(&body_back));
    }

    #[test]
    fn streaming_blob_into_dyn_byte_stream() {
        let body = Body::from(Bytes::from_static(b"test"));
        let blob = StreamingBlob::new(body);
        let _dyn_stream: DynByteStream = blob.into();
    }

    #[test]
    fn streaming_blob_from_dyn_byte_stream() {
        let body = Body::from(Bytes::from_static(b"test"));
        let dyn_stream: DynByteStream = Box::pin(body);
        let _blob = StreamingBlob::from(dyn_stream);
    }

    #[test]
    fn streaming_blob_size_hint() {
        let body = Body::from(Bytes::from_static(b"12345"));
        let blob = StreamingBlob::new(body);
        let (lower, upper) = blob.size_hint();
        if let Some(upper) = upper {
            assert!(lower <= upper);
        }
    }

    #[tokio::test]
    async fn streaming_blob_empty() {
        let body = Body::empty();
        let mut blob = StreamingBlob::new(body);
        let next = blob.next().await;
        assert!(next.is_none());
    }
}
