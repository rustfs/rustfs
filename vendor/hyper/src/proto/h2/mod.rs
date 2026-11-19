use std::error::Error as StdError;
use std::future::Future;
use std::io::{Cursor, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::Buf;
use futures_core::ready;
use h2::SendStream;
use http::header::{HeaderName, CONNECTION, TRANSFER_ENCODING, UPGRADE};
use http::HeaderMap;
use pin_project_lite::pin_project;

use crate::body::Body;

pub(crate) mod ping;
pub(crate) mod upgrade;

cfg_client! {
    pub(crate) mod client;
    pub(crate) use self::client::ClientTask;
}

cfg_server! {
    pub(crate) mod server;
    pub(crate) use self::server::Server;
}

/// Default initial stream window size defined in HTTP2 spec.
pub(crate) const SPEC_WINDOW_SIZE: u32 = 65_535;

// List of connection headers from RFC 9110 Section 7.6.1
//
// TE headers are allowed in HTTP/2 requests as long as the value is "trailers", so they're
// tested separately.
static CONNECTION_HEADERS: [HeaderName; 4] = [
    HeaderName::from_static("keep-alive"),
    HeaderName::from_static("proxy-connection"),
    TRANSFER_ENCODING,
    UPGRADE,
];

enum MessageKind {
    #[cfg(feature = "client")]
    Request,
    #[cfg(feature = "server")]
    Response,
}

fn strip_connection_headers(headers: &mut HeaderMap, kind: MessageKind) {
    for header in &CONNECTION_HEADERS {
        if headers.remove(header).is_some() {
            warn!("Connection header illegal in HTTP/2: {}", header.as_str());
        }
    }

    #[cfg(not(feature = "client"))]
    let _ = kind;
    #[cfg(feature = "client")]
    if matches!(kind, MessageKind::Request) {
        if headers
            .get(http::header::TE)
            .map_or(false, |te_header| te_header != "trailers")
        {
            warn!("TE headers not set to \"trailers\" are illegal in HTTP/2 requests");
            headers.remove(http::header::TE);
        }
    } else if headers.remove(http::header::TE).is_some() {
        warn!("TE headers illegal in HTTP/2 responses");
    }

    if let Some(header) = headers.remove(CONNECTION) {
        warn!(
            "Connection header illegal in HTTP/2: {}",
            CONNECTION.as_str()
        );
        // A `Connection` header may have a comma-separated list of names of other headers that
        // are meant for only this specific connection.
        //
        // Iterate these names and remove them as headers. Connection-specific headers are
        // forbidden in HTTP2, as that information has been moved into frame types of the h2
        // protocol.
        if let Ok(header_contents) = header.to_str() {
            for name in header_contents.split(',') {
                let name = name.trim();
                headers.remove(name);
            }
        }
    }
}

// body adapters used by both Client and Server

pin_project! {
    pub(crate) struct PipeToSendStream<S>
    where
        S: Body,
    {
        body_tx: SendStream<SendBuf<S::Data>>,
        data_done: bool,
        // A data chunk that has been polled from the body but is still waiting
        // for stream-level capacity before it can be shipped. Stored here so
        // it survives across `Poll::Pending` returns from `poll_capacity`; if
        // we left the chunk in a local, it would be dropped on every repoll.
        buffered_data: Option<Peeked<S::Data>>,
        #[pin]
        stream: S,
    }
}

struct Peeked<D> {
    data: D,
    is_eos: bool,
}

impl<S> PipeToSendStream<S>
where
    S: Body,
{
    fn new(stream: S, tx: SendStream<SendBuf<S::Data>>) -> PipeToSendStream<S> {
        PipeToSendStream {
            body_tx: tx,
            data_done: false,
            buffered_data: None,
            stream,
        }
    }

    #[cfg(feature = "client")]
    fn send_reset(self: Pin<&mut Self>, reason: h2::Reason) {
        self.project().body_tx.send_reset(reason);
    }
}

impl<S> Future for PipeToSendStream<S>
where
    S: Body,
    S::Error: Into<Box<dyn StdError + Send + Sync>>,
{
    type Output = crate::Result<()>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut me = self.project();
        loop {
            // Register for RST_STREAM notification while we wait for the next
            // body chunk or for send capacity, so the task wakes up if the
            // peer resets the stream.
            if let Poll::Ready(reason) = me
                .body_tx
                .poll_reset(cx)
                .map_err(crate::Error::new_body_write)?
            {
                debug!("stream received RST_STREAM: {:?}", reason);
                return Poll::Ready(Err(crate::Error::new_body_write(::h2::Error::from(reason))));
            }

            // If a previously-polled chunk is still waiting for stream-level
            // send capacity, drive that to completion before touching the
            // body again.
            if me.buffered_data.is_some() {
                while me.body_tx.capacity() == 0 {
                    match ready!(me.body_tx.poll_capacity(cx)) {
                        Some(Ok(0)) => {}
                        Some(Ok(_)) => break,
                        Some(Err(e)) => return Poll::Ready(Err(crate::Error::new_body_write(e))),
                        None => {
                            // None means the stream is no longer in a
                            // streaming state, we either finished it
                            // somehow, or the remote reset us.
                            return Poll::Ready(Err(crate::Error::new_body_write(
                                "send stream capacity unexpectedly closed",
                            )));
                        }
                    }
                }

                let peeked = me.buffered_data.take().expect("checked is_some above");
                let buf = SendBuf::Buf(peeked.data);
                me.body_tx
                    .send_data(buf, peeked.is_eos)
                    .map_err(crate::Error::new_body_write)?;

                if peeked.is_eos {
                    return Poll::Ready(Ok(()));
                }
                continue;
            }

            // Poll for the next body frame *before* reserving any connection
            // flow-control capacity. Reserving capacity speculatively (even a
            // single byte) pins that capacity on the connection-level window,
            // which can deadlock a second stream when talking to peers that
            // only emit WINDOW_UPDATE once their receive window is fully
            // exhausted. See #4003.
            match ready!(me.stream.as_mut().poll_frame(cx)) {
                Some(Ok(frame)) => {
                    if frame.is_data() {
                        let chunk = frame.into_data().unwrap_or_else(|_| unreachable!());
                        let is_eos = me.stream.is_end_stream();
                        let len = chunk.remaining();
                        trace!("send body chunk: {} bytes, eos={}", len, is_eos);

                        if len == 0 {
                            // Zero-length data frames need no capacity; send
                            // them straight through so trailing empty frames
                            // (e.g. an explicit end-of-stream marker) are
                            // delivered.
                            let buf = SendBuf::Buf(chunk);
                            me.body_tx
                                .send_data(buf, is_eos)
                                .map_err(crate::Error::new_body_write)?;

                            if is_eos {
                                return Poll::Ready(Ok(()));
                            }
                            continue;
                        }

                        // Reserve a minimal claim on the connection-level
                        // flow-control window rather than the whole chunk. The
                        // chunk is already in hand, so this still cannot pin
                        // capacity against a body that never produces data
                        // (#4003), and h2 raises the request to the buffered
                        // length inside `send_data`, so the demand eventually
                        // signalled to the peer is unchanged. Claiming the full
                        // length up front instead makes every in-flight stream a
                        // heavyweight claimant while it waits, which is costly
                        // once the streams on a connection collectively demand
                        // more than the window the peer advertises. Stash the
                        // chunk in `self` so it survives the upcoming
                        // `poll_capacity` wait even if it returns
                        // `Poll::Pending`.
                        me.body_tx.reserve_capacity(1);
                        *me.buffered_data = Some(Peeked {
                            data: chunk,
                            is_eos,
                        });
                    } else if frame.is_trailers() {
                        // no more DATA, so give any capacity back
                        me.body_tx.reserve_capacity(0);
                        me.body_tx
                            .send_trailers(frame.into_trailers().unwrap_or_else(|_| unreachable!()))
                            .map_err(crate::Error::new_body_write)?;
                        return Poll::Ready(Ok(()));
                    } else {
                        trace!("discarding unknown frame");
                        // loop again
                    }
                }
                Some(Err(e)) => return Poll::Ready(Err(me.body_tx.on_user_err(e))),
                None => {
                    // no more frames means we're done here
                    // but at this point, we haven't sent an EOS DATA, or
                    // any trailers, so send an empty EOS DATA.
                    return Poll::Ready(me.body_tx.send_eos_frame());
                }
            }
        }
    }
}

trait SendStreamExt {
    fn on_user_err<E>(&mut self, err: E) -> crate::Error
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>;
    fn send_eos_frame(&mut self) -> crate::Result<()>;
}

impl<B: Buf> SendStreamExt for SendStream<SendBuf<B>> {
    fn on_user_err<E>(&mut self, err: E) -> crate::Error
    where
        E: Into<Box<dyn std::error::Error + Send + Sync>>,
    {
        let err = crate::Error::new_user_body(err);
        debug!("send body user stream error: {}", err);
        self.send_reset(err.h2_reason());
        err
    }

    fn send_eos_frame(&mut self) -> crate::Result<()> {
        trace!("send body eos");
        self.send_data(SendBuf::None, true)
            .map_err(crate::Error::new_body_write)
    }
}

#[repr(usize)]
enum SendBuf<B> {
    Buf(B),
    Cursor(Cursor<Box<[u8]>>),
    None,
}

impl<B: Buf> Buf for SendBuf<B> {
    #[inline]
    fn remaining(&self) -> usize {
        match self {
            Self::Buf(b) => b.remaining(),
            Self::Cursor(c) => Buf::remaining(c),
            Self::None => 0,
        }
    }

    #[inline]
    fn chunk(&self) -> &[u8] {
        match self {
            Self::Buf(b) => b.chunk(),
            Self::Cursor(c) => c.chunk(),
            Self::None => &[],
        }
    }

    #[inline]
    fn advance(&mut self, cnt: usize) {
        match self {
            Self::Buf(b) => b.advance(cnt),
            Self::Cursor(c) => c.advance(cnt),
            Self::None => {}
        }
    }

    fn chunks_vectored<'data>(&'data self, dst: &mut [IoSlice<'data>]) -> usize {
        match self {
            Self::Buf(b) => b.chunks_vectored(dst),
            Self::Cursor(c) => c.chunks_vectored(dst),
            Self::None => 0,
        }
    }
}
