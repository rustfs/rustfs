use std::fmt;
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Bytes;
#[cfg(all(feature = "http2", feature = "server"))]
use futures_channel::oneshot;
#[cfg(all(
    any(feature = "http1", feature = "http2"),
    any(feature = "client", feature = "server")
))]
use futures_core::ready;
use http_body::{Body, Frame, SizeHint};

#[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
use super::chan;
#[cfg(all(
    any(feature = "http1", feature = "http2"),
    any(feature = "client", feature = "server")
))]
use super::DecodedLength;
#[cfg(all(feature = "http2", any(feature = "client", feature = "server")))]
use crate::proto::h2::ping;

/// A stream of `Bytes`, used when receiving bodies from the network.
///
/// Note that Users should not instantiate this struct directly. When working with the hyper client,
/// `Incoming` is returned to you in responses. Similarly, when operating with the hyper server,
/// it is provided within requests.
///
/// # Examples
///
/// ```rust,ignore
/// async fn echo(
///    req: Request<hyper::body::Incoming>,
/// ) -> Result<Response<BoxBody<Bytes, hyper::Error>>, hyper::Error> {
///    //Here, you can process `Incoming`
/// }
/// ```
#[must_use = "streams do nothing unless polled"]
pub struct Incoming {
    kind: Kind,
}

enum Kind {
    Empty,
    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    Chan {
        content_length: DecodedLength,
        rx: chan::Receiver,
    },
    #[cfg(all(feature = "http2", any(feature = "client", feature = "server")))]
    H2 {
        content_length: DecodedLength,
        data_done: bool,
        ping: ping::Recorder,
        recv: h2::RecvStream,
    },
    #[cfg(all(feature = "http2", feature = "server"))]
    ExpectContinue(Box<ExpectContinue>),
    #[cfg(feature = "ffi")]
    Ffi(crate::ffi::UserBody),
}

#[cfg(all(feature = "http2", feature = "server"))]
struct ExpectContinue {
    body: Incoming,
    tx: oneshot::Sender<()>,
}

/// A sender half created through [`Body::channel()`].
///
/// Useful when wanting to stream chunks from another thread.
///
/// ## Body Closing
///
/// Note that the request body will always be closed normally when the sender is dropped (meaning
/// that the empty terminating chunk will be sent to the remote). If you desire to close the
/// connection with an incomplete response (e.g. in the case of an error during asynchronous
/// processing), call the [`Sender::abort()`] method to abort the body in an abnormal fashion.
///
/// [`Body::channel()`]: struct.Body.html#method.channel
/// [`Sender::abort()`]: struct.Sender.html#method.abort
#[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
pub(crate) use super::chan::Sender;

impl Incoming {
    /// Create a `Body` stream with an associated sender half.
    ///
    /// Useful when wanting to stream chunks from another thread.
    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    #[inline]
    #[cfg(test)]
    pub(crate) fn channel() -> (Sender, Incoming) {
        Self::new_channel(DecodedLength::CHUNKED, /*wanter =*/ false)
    }

    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    pub(crate) fn new_channel(content_length: DecodedLength, wanter: bool) -> (Sender, Incoming) {
        let (tx, rx) = chan::channel(wanter);
        let rx = Incoming::new(Kind::Chan { content_length, rx });

        (tx, rx)
    }

    fn new(kind: Kind) -> Incoming {
        Incoming { kind }
    }

    #[allow(dead_code)]
    pub(crate) fn empty() -> Incoming {
        Incoming::new(Kind::Empty)
    }

    #[cfg(feature = "ffi")]
    pub(crate) fn ffi() -> Incoming {
        Incoming::new(Kind::Ffi(crate::ffi::UserBody::new()))
    }

    #[cfg(all(feature = "http2", any(feature = "client", feature = "server")))]
    pub(crate) fn h2(
        recv: h2::RecvStream,
        mut content_length: DecodedLength,
        ping: ping::Recorder,
    ) -> Self {
        // If the stream is already EOS, then the "unknown length" is clearly
        // actually ZERO.
        if !content_length.is_exact() && recv.is_end_stream() {
            content_length = DecodedLength::ZERO;
        }

        Incoming::new(Kind::H2 {
            data_done: false,
            ping,
            content_length,
            recv,
        })
    }

    #[cfg(all(feature = "http2", feature = "server"))]
    pub(crate) fn with_expect_continue(self, tx: oneshot::Sender<()>) -> Self {
        Incoming::new(Kind::ExpectContinue(Box::new(ExpectContinue {
            body: self,
            tx,
        })))
    }

    #[cfg(feature = "ffi")]
    pub(crate) fn as_ffi_mut(&mut self) -> &mut crate::ffi::UserBody {
        if !matches!(self.kind, Kind::Ffi(_)) {
            self.kind = Kind::Ffi(crate::ffi::UserBody::new());
        }

        match &mut self.kind {
            Kind::Ffi(body) => body,
            _ => unreachable!(),
        }
    }
}

impl Body for Incoming {
    type Data = Bytes;
    type Error = crate::Error;

    fn poll_frame(
        #[cfg_attr(
            not(all(
                any(feature = "http1", feature = "http2"),
                any(feature = "client", feature = "server")
            )),
            allow(unused_mut)
        )]
        mut self: Pin<&mut Self>,
        #[cfg_attr(
            not(all(
                any(feature = "http1", feature = "http2"),
                any(feature = "client", feature = "server")
            )),
            allow(unused_variables)
        )]
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        match &mut self.kind {
            Kind::Empty => Poll::Ready(None),
            #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
            Kind::Chan {
                content_length: len,
                rx,
            } => {
                if let Some(chunk) = ready!(rx.poll_next(cx)?) {
                    len.sub_if(chunk.len() as u64);
                    return Poll::Ready(Some(Ok(Frame::data(chunk))));
                }
                Poll::Ready(rx.take_trailers().map(Frame::trailers).map(Ok))
            }
            #[cfg(all(feature = "http2", any(feature = "client", feature = "server")))]
            Kind::H2 {
                data_done,
                ping,
                recv: h2,
                content_length: len,
            } => {
                if !*data_done {
                    match ready!(h2.poll_data(cx)) {
                        Some(Ok(bytes)) => {
                            let _ = h2.flow_control().release_capacity(bytes.len());
                            len.sub_if(bytes.len() as u64);
                            ping.record_data(bytes.len());
                            return Poll::Ready(Some(Ok(Frame::data(bytes))));
                        }
                        Some(Err(e)) => {
                            if let Some(h2::Reason::NO_ERROR) = e.reason() {
                                // As mentioned in RFC 7540 Section 8.1, a RST_STREAM with NO_ERROR
                                // indicates an early response, and should cause the body reading
                                // to stop, but not fail it:
                                return Poll::Ready(None);
                            } else {
                                return Poll::Ready(Some(Err(crate::Error::new_body(e))));
                            }
                        }
                        None => {
                            *data_done = true;
                            // fall through to trailers
                        }
                    }
                }

                // after data, check trailers
                match ready!(h2.poll_trailers(cx)) {
                    Ok(t) => {
                        ping.record_non_data();
                        Poll::Ready(Ok(t.map(Frame::trailers)).transpose())
                    }
                    Err(e) => {
                        if let Some(h2::Reason::NO_ERROR) = e.reason() {
                            // Same as above, a RST_STREAM with NO_ERROR indicates an early
                            // response, and should cause reading the trailers to stop, but
                            // not fail it:
                            Poll::Ready(None)
                        } else {
                            Poll::Ready(Some(Err(crate::Error::new_h2(e))))
                        }
                    }
                }
            }

            #[cfg(all(feature = "http2", feature = "server"))]
            Kind::ExpectContinue(_) => {
                let expect = match std::mem::replace(&mut self.kind, Kind::Empty) {
                    Kind::ExpectContinue(expect) => expect,
                    _ => unreachable!(),
                };
                let ExpectContinue { body, tx } = *expect;
                *self = body;
                let res = self.as_mut().poll_frame(cx);
                // Only ask the client to continue if the body hasn't already arrived.
                if res.is_pending() {
                    let _ = tx.send(());
                }
                res
            }

            #[cfg(feature = "ffi")]
            Kind::Ffi(body) => body.poll_data(cx),
        }
    }

    fn is_end_stream(&self) -> bool {
        match &self.kind {
            Kind::Empty => true,
            #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
            Kind::Chan { content_length, .. } => *content_length == DecodedLength::ZERO,
            #[cfg(all(feature = "http2", any(feature = "client", feature = "server")))]
            Kind::H2 { recv: h2, .. } => h2.is_end_stream(),
            #[cfg(all(feature = "http2", feature = "server"))]
            Kind::ExpectContinue(expect) => expect.body.is_end_stream(),
            #[cfg(feature = "ffi")]
            Kind::Ffi(..) => false,
        }
    }

    fn size_hint(&self) -> SizeHint {
        #[cfg(all(
            any(feature = "http1", feature = "http2"),
            any(feature = "client", feature = "server")
        ))]
        fn opt_len(decoded_length: DecodedLength) -> SizeHint {
            if let Some(content_length) = decoded_length.into_opt() {
                SizeHint::with_exact(content_length)
            } else {
                SizeHint::default()
            }
        }

        match &self.kind {
            Kind::Empty => SizeHint::with_exact(0),
            #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
            Kind::Chan { content_length, .. } => opt_len(*content_length),
            #[cfg(all(feature = "http2", any(feature = "client", feature = "server")))]
            Kind::H2 { content_length, .. } => opt_len(*content_length),
            #[cfg(all(feature = "http2", feature = "server"))]
            Kind::ExpectContinue(expect) => expect.body.size_hint(),
            #[cfg(feature = "ffi")]
            Kind::Ffi(..) => SizeHint::default(),
        }
    }
}

impl fmt::Debug for Incoming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        #[cfg(any(
            all(
                any(feature = "http1", feature = "http2"),
                any(feature = "client", feature = "server")
            ),
            feature = "ffi"
        ))]
        #[derive(Debug)]
        struct Streaming;
        #[derive(Debug)]
        struct Empty;

        let mut builder = f.debug_tuple("Body");
        match self.kind {
            Kind::Empty => builder.field(&Empty),
            #[cfg(any(
                all(
                    any(feature = "http1", feature = "http2"),
                    any(feature = "client", feature = "server")
                ),
                feature = "ffi"
            ))]
            _ => builder.field(&Streaming),
        };

        builder.finish()
    }
}

#[cfg(test)]
mod tests {
    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    use std::mem;
    #[cfg(all(feature = "nightly", not(miri)))]
    use std::pin::Pin;
    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    use std::task::Poll;

    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    use super::{Body, Incoming, SizeHint};
    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    use super::{DecodedLength, Sender};
    #[cfg(all(feature = "nightly", not(miri)))]
    use bytes::Bytes;
    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    use http_body_util::BodyExt;

    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    #[test]
    fn test_size_of() {
        // These are mostly to help catch *accidentally* increasing
        // the size by too much.

        let body_size = mem::size_of::<Incoming>();
        let body_expected_size = mem::size_of::<u64>() * 5;
        assert!(
            body_size <= body_expected_size,
            "Body size = {} <= {}",
            body_size,
            body_expected_size,
        );

        assert_eq!(
            mem::size_of::<Sender>(),
            mem::size_of::<usize>() * 2,
            "Sender"
        );

        assert_eq!(
            mem::size_of::<Sender>(),
            mem::size_of::<Option<Sender>>(),
            "Option<Sender>"
        );
    }

    #[cfg(all(feature = "nightly", not(miri)))]
    #[bench]
    fn bench_channel_create_and_drop(b: &mut test::Bencher) {
        b.iter(|| {
            let _ = test::black_box(Incoming::new_channel(
                DecodedLength::CHUNKED,
                /* wanter = */ false,
            ));
        });
    }

    #[cfg(all(feature = "nightly", not(miri)))]
    #[bench]
    fn bench_channel_data_handoff(b: &mut test::Bencher) {
        let (mut tx, mut body) =
            Incoming::new_channel(DecodedLength::CHUNKED, /* wanter = */ false);
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());

        b.iter(|| {
            assert!(tx.poll_ready(&mut cx).is_ready());
            tx.try_send_data(Bytes::from_static(b"hello world"))
                .unwrap();
            let frame = match Pin::new(&mut body).poll_frame(&mut cx) {
                Poll::Ready(Some(Ok(frame))) => frame,
                unexpected => panic!("unexpected body poll: {unexpected:?}"),
            };
            test::black_box(frame);
        });
    }

    #[cfg(all(feature = "nightly", not(miri)))]
    #[bench]
    fn bench_channel_want_transition(b: &mut test::Bencher) {
        let mut cx = std::task::Context::from_waker(std::task::Waker::noop());

        b.iter(|| {
            let (mut tx, mut body) =
                Incoming::new_channel(DecodedLength::CHUNKED, /* wanter = */ true);
            assert!(tx.poll_ready(&mut cx).is_pending());
            assert!(Pin::new(&mut body).poll_frame(&mut cx).is_pending());
            assert!(tx.poll_ready(&mut cx).is_ready());
            let _ = test::black_box((tx, body));
        });
    }

    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    #[test]
    fn size_hint() {
        fn eq(body: Incoming, b: SizeHint, note: &str) {
            let a = body.size_hint();
            assert_eq!(a.lower(), b.lower(), "lower for {:?}", note);
            assert_eq!(a.upper(), b.upper(), "upper for {:?}", note);
        }

        eq(Incoming::empty(), SizeHint::with_exact(0), "empty");

        eq(Incoming::channel().1, SizeHint::new(), "channel");

        eq(
            Incoming::new_channel(DecodedLength::new(4), /*wanter =*/ false).1,
            SizeHint::with_exact(4),
            "channel with length",
        );
    }

    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    #[cfg(not(miri))]
    #[tokio::test]
    async fn channel_abort() {
        let (tx, mut rx) = Incoming::channel();

        tx.abort();

        let err = rx.frame().await.unwrap().unwrap_err();
        assert!(err.is_body_write_aborted(), "{:?}", err);
    }

    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    #[cfg(all(not(miri), feature = "http1"))]
    #[tokio::test]
    async fn channel_abort_when_buffer_is_full() {
        let (mut tx, mut rx) = Incoming::channel();

        tx.try_send_data("chunk 1".into()).expect("send 1");
        // buffer is full, but can still send abort
        tx.abort();

        let chunk1 = rx
            .frame()
            .await
            .expect("item 1")
            .expect("chunk 1")
            .into_data()
            .unwrap();
        assert_eq!(chunk1, "chunk 1");

        let err = rx.frame().await.unwrap().unwrap_err();
        assert!(err.is_body_write_aborted(), "{:?}", err);
    }

    #[cfg(feature = "http1")]
    #[test]
    fn channel_buffers_one() {
        let (mut tx, _rx) = Incoming::channel();

        tx.try_send_data("chunk 1".into()).expect("send 1");

        // buffer is now full
        let chunk2 = tx.try_send_data("chunk 2".into()).expect_err("send 2");
        assert_eq!(chunk2, "chunk 2");
    }

    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    #[cfg(not(miri))]
    #[tokio::test]
    async fn channel_empty() {
        let (_, mut rx) = Incoming::channel();

        assert!(rx.frame().await.is_none());
    }

    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    #[test]
    fn channel_ready() {
        let (mut tx, _rx) = Incoming::new_channel(DecodedLength::CHUNKED, /*wanter = */ false);

        let mut tx_ready = tokio_test::task::spawn(tx.ready());

        assert!(tx_ready.poll().is_ready(), "tx is ready immediately");
    }

    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    #[test]
    fn channel_wanter() {
        let (mut tx, mut rx) =
            Incoming::new_channel(DecodedLength::CHUNKED, /*wanter = */ true);

        let mut tx_ready = tokio_test::task::spawn(tx.ready());
        let mut rx_data = tokio_test::task::spawn(rx.frame());

        assert!(
            tx_ready.poll().is_pending(),
            "tx isn't ready before rx has been polled"
        );

        assert!(rx_data.poll().is_pending(), "poll rx.data");
        assert!(tx_ready.is_woken(), "rx poll wakes tx");

        assert!(
            tx_ready.poll().is_ready(),
            "tx is ready after rx has been polled"
        );
    }

    #[cfg(all(feature = "http1", any(feature = "client", feature = "server")))]
    #[test]
    fn channel_notices_closure() {
        let (mut tx, rx) = Incoming::new_channel(DecodedLength::CHUNKED, /*wanter = */ true);

        let mut tx_ready = tokio_test::task::spawn(tx.ready());

        assert!(
            tx_ready.poll().is_pending(),
            "tx isn't ready before rx has been polled"
        );

        drop(rx);
        assert!(tx_ready.is_woken(), "dropping rx wakes tx");

        match &tx_ready.poll() {
            Poll::Ready(Err(e)) if e.is_closed() => (),
            unexpected => panic!("tx poll ready unexpected: {:?}", unexpected),
        }
    }
}
