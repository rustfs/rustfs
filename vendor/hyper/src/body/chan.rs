use std::fmt;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use atomic_waker::AtomicWaker;
use bytes::Bytes;
use http::HeaderMap;

use crate::common::lock::LockResultExt;

pub(crate) fn channel(wanter: bool) -> (Sender, Receiver) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            item: None,
            pending_error: None,
            trailers: None,
            sender_open: true,
            receiver_open: true,
            want: !wanter,
        }),
        sender_waker: AtomicWaker::new(),
        receiver_waker: AtomicWaker::new(),
    });

    (
        Sender {
            shared: Arc::clone(&shared),
            trailers_sent: false,
        },
        Receiver {
            shared,
            terminated: false,
        },
    )
}

#[must_use = "Sender does nothing unless sent on"]
pub(crate) struct Sender {
    shared: Arc<Shared>,
    trailers_sent: bool,
}

pub(crate) struct Receiver {
    shared: Arc<Shared>,
    terminated: bool,
}

struct Shared {
    state: Mutex<State>,
    sender_waker: AtomicWaker,
    receiver_waker: AtomicWaker,
}

struct State {
    item: Option<Result<Bytes, crate::Error>>,
    // An error must not displace data which was already accepted. The old
    // mpsc channel achieved this by sending the error from a cloned sender.
    pending_error: Option<crate::Error>,
    trailers: Option<HeaderMap>,
    sender_open: bool,
    receiver_open: bool,
    want: bool,
}

impl Sender {
    pub(crate) fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<crate::Result<()>> {
        self.shared.sender_waker.register(cx.waker());
        let state = self.shared.state.lock().panic_if_poisoned();
        if !state.receiver_open {
            Poll::Ready(Err(crate::Error::new_closed()))
        } else if state.want && state.item.is_none() && state.pending_error.is_none() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    #[cfg(test)]
    pub(crate) async fn ready(&mut self) -> crate::Result<()> {
        futures_util::future::poll_fn(|cx| self.poll_ready(cx)).await
    }

    #[cfg(test)]
    #[allow(dead_code)]
    pub(crate) async fn send_data(&mut self, chunk: Bytes) -> crate::Result<()> {
        self.ready().await?;
        self.try_send_data(chunk)
            .map_err(|_| crate::Error::new_closed())
    }

    pub(crate) fn try_send_data(&mut self, chunk: Bytes) -> Result<(), Bytes> {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        if !state.receiver_open
            || !state.want
            || state.item.is_some()
            || state.pending_error.is_some()
        {
            return Err(chunk);
        }
        state.item = Some(Ok(chunk));
        drop(state);
        self.shared.receiver_waker.wake();
        Ok(())
    }

    pub(crate) fn try_send_trailers(
        &mut self,
        trailers: HeaderMap,
    ) -> Result<(), Option<HeaderMap>> {
        if self.trailers_sent {
            return Err(None);
        }
        self.trailers_sent = true;

        let mut state = self.shared.state.lock().panic_if_poisoned();
        if !state.receiver_open {
            return Err(Some(trailers));
        }
        state.trailers = Some(trailers);
        drop(state);
        self.shared.receiver_waker.wake();
        Ok(())
    }

    #[allow(dead_code)]
    #[allow(clippy::unused_async_trait_impl)]
    #[allow(clippy::map_err_ignore)]
    pub(crate) async fn send_trailers(&mut self, trailers: HeaderMap) -> crate::Result<()> {
        self.try_send_trailers(trailers)
            .map_err(|_| crate::Error::new_closed())
    }

    pub(crate) fn send_error(&mut self, err: crate::Error) {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        if !state.receiver_open {
            return;
        }
        if state.item.is_none() {
            state.item = Some(Err(err));
        } else if state.pending_error.is_none() {
            state.pending_error = Some(err);
        }
        drop(state);
        self.shared.receiver_waker.wake();
    }

    #[cfg(test)]
    pub(crate) fn abort(mut self) {
        self.send_error(crate::Error::new_body_write_aborted());
    }

    fn is_closed(&self) -> bool {
        !self.shared.state.lock().panic_if_poisoned().receiver_open
    }
}

impl Receiver {
    pub(crate) fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<bytes::Bytes, crate::Error>>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        self.shared.receiver_waker.register(cx.waker());
        let mut state = self.shared.state.lock().panic_if_poisoned();
        let wake_sender = if !state.want {
            state.want = true;
            true
        } else {
            false
        };
        if let Some(item) = state.item.take() {
            drop(state);
            self.shared.sender_waker.wake();
            return Poll::Ready(Some(item));
        }
        if let Some(err) = state.pending_error.take() {
            drop(state);
            if wake_sender {
                self.shared.sender_waker.wake();
            }
            return Poll::Ready(Some(Err(err)));
        }
        let sender_open = state.sender_open;
        drop(state);
        if wake_sender {
            self.shared.sender_waker.wake();
        }
        if sender_open {
            Poll::Pending
        } else {
            self.terminated = true;
            Poll::Ready(None)
        }
    }

    pub(crate) fn take_trailers(&mut self) -> Option<HeaderMap> {
        debug_assert!(self.terminated, "data channel still open before trailers");
        let mut state = self.shared.state.lock().panic_if_poisoned();
        state.trailers.take()
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        state.sender_open = false;
        let receiver_open = state.receiver_open;
        drop(state);
        if receiver_open {
            self.shared.receiver_waker.wake();
        }
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        state.receiver_open = false;
        let sender_open = state.sender_open;
        drop(state);
        if sender_open {
            self.shared.sender_waker.wake();
        }
    }
}

impl fmt::Debug for Sender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_closed() {
            f.debug_tuple("Sender").field(&"Closed").finish()
        } else {
            f.debug_tuple("Sender").field(&"Open").finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        task::{Wake, Waker},
    };

    use super::*;

    async fn recv(rx: &mut Receiver) -> Option<Result<Bytes, crate::Error>> {
        futures_util::future::poll_fn(|cx| rx.poll_next(cx)).await
    }

    #[tokio::test]
    async fn error_queued_behind_accepted_data() {
        let (mut tx, mut rx) = channel(false);
        tx.try_send_data(Bytes::from_static(b"data")).unwrap();
        tx.send_error(crate::Error::new_incomplete());
        drop(tx);

        assert_eq!(recv(&mut rx).await.unwrap().unwrap(), "data");
        assert!(recv(&mut rx).await.unwrap().is_err());
        assert!(recv(&mut rx).await.is_none());
    }

    #[tokio::test]
    async fn trailers_follow_data_close() {
        let (mut tx, mut rx) = channel(false);
        let mut trailers = HeaderMap::new();
        trailers.insert("x-trailer", "value".parse().unwrap());
        tx.try_send_trailers(trailers).unwrap();
        drop(tx);

        assert!(recv(&mut rx).await.is_none());
        assert_eq!(rx.take_trailers().unwrap()["x-trailer"], "value");
    }

    #[test]
    fn drop_wakes_only_an_open_peer() {
        struct Count(AtomicUsize);

        impl Wake for Count {
            fn wake(self: Arc<Self>) {
                self.0.fetch_add(1, Ordering::Relaxed);
            }
        }

        let count = Arc::new(Count(AtomicUsize::new(0)));
        let waker = Waker::from(count.clone());
        let mut cx = Context::from_waker(&waker);
        let wakes = || count.0.load(Ordering::Relaxed);

        // A receiver dropped after the sender does not wake the sender's stale waker.
        let (mut tx, rx) = channel(true);
        assert!(tx.poll_ready(&mut cx).is_pending());
        drop(tx);
        drop(rx);
        assert_eq!(wakes(), 0);

        // Nor does a sender dropped after the receiver.
        let (tx, mut rx) = channel(false);
        assert!(rx.poll_next(&mut cx).is_pending());
        drop(rx);
        drop(tx);
        assert_eq!(wakes(), 0);

        // An open peer is still woken to observe the close.
        let (mut tx, rx) = channel(true);
        assert!(tx.poll_ready(&mut cx).is_pending());
        drop(rx);
        assert_eq!(wakes(), 1);
        let (tx, mut rx) = channel(false);
        assert!(rx.poll_next(&mut cx).is_pending());
        drop(tx);
        assert_eq!(wakes(), 2);
    }
}
