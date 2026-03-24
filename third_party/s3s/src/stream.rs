//! Byte-stream types for S3 request and response bodies.
//!
//! This module defines the [`ByteStream`] trait, the [`DynByteStream`] type
//! alias for heap-allocated streams, and [`RemainingLength`] which
//! communicates a known or estimated byte count remaining in a stream.

use crate::error::StdError;

use std::collections::VecDeque;
use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
use futures::Stream;

pub trait ByteStream: Stream {
    fn remaining_length(&self) -> RemainingLength {
        RemainingLength::unknown()
    }
}

pub type DynByteStream = Pin<Box<dyn ByteStream<Item = Result<Bytes, StdError>> + Send + Sync + 'static>>;

pub struct RemainingLength {
    lower: usize,
    upper: Option<usize>,
}

impl RemainingLength {
    /// Creates a new `RemainingLength` with the given lower and upper bounds.
    ///
    /// # Panics
    /// This function asserts that `lower <= upper`.
    #[must_use]
    pub fn new(lower: usize, upper: Option<usize>) -> Self {
        if let Some(upper) = upper {
            assert!(lower <= upper);
        }
        Self { lower, upper }
    }

    #[must_use]
    pub fn unknown() -> Self {
        Self { lower: 0, upper: None }
    }

    #[must_use]
    pub fn new_exact(n: usize) -> Self {
        Self {
            lower: n,
            upper: Some(n),
        }
    }

    #[must_use]
    pub fn exact(&self) -> Option<usize> {
        self.upper.filter(|&upper| upper == self.lower)
    }

    #[must_use]
    fn into_size_hint(self) -> http_body::SizeHint {
        let mut sz = http_body::SizeHint::new();
        sz.set_lower(self.lower as u64);
        if let Some(upper) = self.upper {
            sz.set_upper(upper as u64);
        }
        sz
    }

    #[must_use]
    fn from_size_hint(sz: &http_body::SizeHint) -> Self {
        // inaccurate conversion on 32-bit platforms
        let lower = usize::try_from(sz.lower()).unwrap_or(usize::MAX);
        let upper = sz.upper().and_then(|x| usize::try_from(x).ok());
        Self { lower, upper }
    }
}

impl From<RemainingLength> for http_body::SizeHint {
    fn from(value: RemainingLength) -> Self {
        value.into_size_hint()
    }
}

impl From<http_body::SizeHint> for RemainingLength {
    fn from(value: http_body::SizeHint) -> Self {
        Self::from_size_hint(&value)
    }
}

impl fmt::Debug for RemainingLength {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(exact) = self.exact() {
            return write!(f, "{exact}");
        }
        match self.upper {
            Some(upper) => write!(f, "({}..={})", self.lower, upper),
            None => write!(f, "({}..)", self.lower),
        }
    }
}

pub(crate) fn into_dyn<S, E>(s: S) -> DynByteStream
where
    S: ByteStream<Item = Result<Bytes, E>> + Send + Sync + Unpin + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    Box::pin(Wrapper(s))
}

struct Wrapper<S>(S);

impl<S, E> Stream for Wrapper<S>
where
    S: ByteStream<Item = Result<Bytes, E>> + Send + Sync + Unpin + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<Bytes, StdError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = Pin::new(&mut self.0);
        this.poll_next(cx).map_err(|e| Box::new(e) as StdError)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.0.size_hint()
    }
}

impl<S, E> ByteStream for Wrapper<S>
where
    S: ByteStream<Item = Result<Bytes, E>> + Send + Sync + Unpin + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    fn remaining_length(&self) -> RemainingLength {
        self.0.remaining_length()
    }
}

pub(crate) struct VecByteStream {
    queue: VecDeque<Bytes>,
    remaining_bytes: usize,
}

impl VecByteStream {
    pub fn new(v: Vec<Bytes>) -> Self {
        let total = v
            .iter()
            .map(Bytes::len)
            .try_fold(0, usize::checked_add)
            .expect("length overflow");

        Self {
            queue: v.into(),
            remaining_bytes: total,
        }
    }

    pub fn exact_remaining_length(&self) -> usize {
        self.remaining_bytes
    }
}

impl Stream for VecByteStream {
    type Item = Result<Bytes, StdError>;

    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = Pin::into_inner(self);
        match this.queue.pop_front() {
            Some(b) => {
                this.remaining_bytes -= b.len();
                Poll::Ready(Some(Ok(b)))
            }
            None => Poll::Ready(None),
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let cnt = self.queue.len();
        (cnt, Some(cnt))
    }
}

impl ByteStream for VecByteStream {
    fn remaining_length(&self) -> RemainingLength {
        RemainingLength::new_exact(self.remaining_bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use futures::StreamExt;

    // --- RemainingLength tests ---

    #[test]
    fn remaining_length_unknown() {
        let rl = RemainingLength::unknown();
        assert_eq!(rl.exact(), None);
        assert_eq!(format!("{rl:?}"), "(0..)");
    }

    #[test]
    fn remaining_length_exact() {
        let rl = RemainingLength::new_exact(42);
        assert_eq!(rl.exact(), Some(42));
        assert_eq!(format!("{rl:?}"), "42");
    }

    #[test]
    fn remaining_length_range() {
        let rl = RemainingLength::new(10, Some(20));
        assert_eq!(rl.exact(), None);
        assert_eq!(format!("{rl:?}"), "(10..=20)");
    }

    #[test]
    fn remaining_length_lower_only() {
        let rl = RemainingLength::new(5, None);
        assert_eq!(rl.exact(), None);
        assert_eq!(format!("{rl:?}"), "(5..)");
    }

    #[test]
    #[should_panic(expected = "lower <= upper")]
    fn remaining_length_invalid() {
        let _ = RemainingLength::new(20, Some(10));
    }

    #[test]
    fn remaining_length_into_size_hint() {
        let rl = RemainingLength::new(10, Some(20));
        let sh: http_body::SizeHint = rl.into();
        assert_eq!(sh.lower(), 10);
        assert_eq!(sh.upper(), Some(20));
    }

    #[test]
    fn remaining_length_from_size_hint() {
        let mut sh = http_body::SizeHint::new();
        sh.set_lower(5);
        sh.set_upper(15);
        let rl: RemainingLength = sh.into();
        assert_eq!(rl.exact(), None);
        assert_eq!(format!("{rl:?}"), "(5..=15)");
    }

    #[test]
    fn remaining_length_from_size_hint_no_upper() {
        let sh = http_body::SizeHint::new();
        let rl: RemainingLength = sh.into();
        assert_eq!(rl.exact(), None);
    }

    // --- VecByteStream tests ---

    #[tokio::test]
    async fn vec_byte_stream_empty() {
        let mut s = VecByteStream::new(vec![]);
        assert_eq!(s.exact_remaining_length(), 0);
        assert_eq!(s.remaining_length().exact(), Some(0));
        let (lo, hi) = s.size_hint();
        assert_eq!(lo, 0);
        assert_eq!(hi, Some(0));
        assert!(s.next().await.is_none());
    }

    #[tokio::test]
    async fn vec_byte_stream_single_chunk() {
        let data = Bytes::from_static(b"hello");
        let mut s = VecByteStream::new(vec![data.clone()]);
        assert_eq!(s.exact_remaining_length(), 5);
        let item = s.next().await.unwrap().unwrap();
        assert_eq!(item, data);
        assert_eq!(s.exact_remaining_length(), 0);
        assert!(s.next().await.is_none());
    }

    #[tokio::test]
    async fn vec_byte_stream_multiple_chunks() {
        let c1 = Bytes::from_static(b"ab");
        let c2 = Bytes::from_static(b"cde");
        let mut s = VecByteStream::new(vec![c1.clone(), c2.clone()]);
        assert_eq!(s.exact_remaining_length(), 5);

        let (lo, hi) = s.size_hint();
        assert_eq!(lo, 2);
        assert_eq!(hi, Some(2));

        let item1 = s.next().await.unwrap().unwrap();
        assert_eq!(item1, c1);
        assert_eq!(s.exact_remaining_length(), 3);

        let item2 = s.next().await.unwrap().unwrap();
        assert_eq!(item2, c2);
        assert_eq!(s.exact_remaining_length(), 0);

        assert!(s.next().await.is_none());
    }

    // --- into_dyn / Wrapper tests ---

    // A concrete-error ByteStream for testing `into_dyn`
    struct TestByteStream(VecDeque<Bytes>);

    impl Stream for TestByteStream {
        type Item = Result<Bytes, std::io::Error>;
        fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Ready(self.0.pop_front().map(Ok))
        }
        fn size_hint(&self) -> (usize, Option<usize>) {
            let n = self.0.len();
            (n, Some(n))
        }
    }

    impl ByteStream for TestByteStream {
        fn remaining_length(&self) -> RemainingLength {
            let total: usize = self.0.iter().map(Bytes::len).sum();
            RemainingLength::new_exact(total)
        }
    }

    #[tokio::test]
    async fn into_dyn_stream() {
        let mut q = VecDeque::new();
        q.push_back(Bytes::from_static(b"test"));
        let mut ds = into_dyn(TestByteStream(q));
        let item = ds.next().await.unwrap().unwrap();
        assert_eq!(item, Bytes::from_static(b"test"));
        assert!(ds.next().await.is_none());
    }

    #[tokio::test]
    async fn into_dyn_remaining_length() {
        let mut q = VecDeque::new();
        q.push_back(Bytes::from_static(b"abc"));
        let ds = into_dyn(TestByteStream(q));
        assert_eq!(ds.remaining_length().exact(), Some(3));
    }

    #[tokio::test]
    async fn into_dyn_size_hint() {
        let mut q = VecDeque::new();
        q.push_back(Bytes::from_static(b"a"));
        q.push_back(Bytes::from_static(b"b"));
        let ds = into_dyn(TestByteStream(q));
        let (lo, hi) = ds.size_hint();
        assert_eq!(lo, 2);
        assert_eq!(hi, Some(2));
    }
}
