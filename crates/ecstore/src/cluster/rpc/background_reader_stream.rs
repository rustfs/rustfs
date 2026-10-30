// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use crate::disk::background_memory::{BackgroundMemoryBudget, Reservation};
use bytes::{Bytes, BytesMut};
use futures::Stream;
use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio_util::io::ReaderStream;

struct ReservedReadBytes {
    // Free each payload view before releasing its shared allocation receipt.
    bytes: Bytes,
    _reservation: Arc<Reservation>,
}

impl AsRef<[u8]> for ReservedReadBytes {
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

struct ReaderBuffer {
    // These initialized bytes are spare storage, not unconsumed source data.
    bytes: BytesMut,
    reservation: Option<Arc<Reservation>>,
    budget: Arc<BackgroundMemoryBudget>,
    capacity: usize,
}

impl ReaderBuffer {
    fn new(budget: Arc<BackgroundMemoryBudget>, capacity: usize) -> Self {
        Self {
            bytes: BytesMut::new(),
            reservation: None,
            budget,
            capacity: capacity.max(1),
        }
    }

    fn clear(&mut self) {
        self.bytes = BytesMut::new();
        self.reservation = None;
    }

    fn ensure_readable(&mut self) -> io::Result<()> {
        if !self.bytes.is_empty() {
            return Ok(());
        }
        if self.reservation.is_some() {
            if self.bytes.try_reclaim(self.capacity) {
                self.bytes.resize(self.capacity, 0);
                return Ok(());
            }
            // A live output owns this allocation. Drop our empty view before
            // its receipt, then reserve independent replacement storage.
            self.clear();
        }
        let reservation = self.budget.reserve(self.capacity)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(self.capacity)
            .map_err(|_| io::ErrorKind::OutOfMemory)?;
        bytes.resize(self.capacity, 0);
        // Fresh ordinary Bytes is unique and spans the complete allocation.
        // Unlike from_owner output, this conversion cannot require a copy.
        let bytes = Bytes::from(bytes.into_boxed_slice())
            .try_into_mut()
            .map_err(|_| io::Error::other("fresh reader storage was not unique"))?;
        self.bytes = bytes;
        self.reservation = Some(Arc::new(reservation));
        Ok(())
    }

    fn take_bytes(&mut self, length: usize) -> io::Result<Bytes> {
        let reservation = self
            .reservation
            .as_ref()
            .cloned()
            .ok_or_else(|| io::Error::other("reader storage has no memory reservation"))?;
        Ok(Bytes::from_owner(ReservedReadBytes {
            bytes: self.bytes.split_to(length).freeze(),
            _reservation: reservation,
        }))
    }
}

/// RPC response chunks retain their background memory reservation until the
/// last clone or slice is dropped. Foreground readers use the existing stream.
pub struct BackgroundReaderStream<R> {
    inner: ReaderState<R>,
}

enum ReaderState<R> {
    Foreground(ReaderStream<R>),
    Background { reader: Option<R>, buffer: ReaderBuffer },
}

impl<R: AsyncRead> BackgroundReaderStream<R> {
    /// Capture memory ownership inside the admitted scope, before moving the
    /// response body out of it. This stream retains no node or device capacity.
    pub fn with_capacity(reader: R, capacity: usize) -> Self {
        let memory = crate::disk::current_resource_unit().and_then(|resources| resources.memory_budget());
        let inner = match memory {
            Some(memory) => ReaderState::Background {
                reader: Some(reader),
                buffer: ReaderBuffer::new(memory, capacity),
            },
            None => ReaderState::Foreground(ReaderStream::with_capacity(reader, capacity)),
        };
        Self { inner }
    }
}

impl<R: AsyncRead + Unpin> Stream for BackgroundReaderStream<R> {
    type Item = io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match &mut this.inner {
            ReaderState::Foreground(reader) => Pin::new(reader).poll_next(cx),
            ReaderState::Background { reader, buffer } => {
                let Some(inner) = reader.as_mut() else {
                    return Poll::Ready(None);
                };
                if let Err(error) = buffer.ensure_readable() {
                    *reader = None;
                    return Poll::Ready(Some(Err(error)));
                }
                let mut output = ReadBuf::new(&mut buffer.bytes);
                match std::task::ready!(Pin::new(inner).poll_read(cx, &mut output)) {
                    Ok(()) => {
                        let length = output.filled().len();
                        if length == 0 {
                            buffer.clear();
                            *reader = None;
                            Poll::Ready(None)
                        } else {
                            let bytes = buffer.take_bytes(length);
                            if bytes.is_err() {
                                buffer.clear();
                                *reader = None;
                            }
                            Poll::Ready(Some(bytes))
                        }
                    }
                    Err(error) => {
                        buffer.clear();
                        *reader = None;
                        Poll::Ready(Some(Err(error)))
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::{BackgroundMemoryBudget, test_resource_units, with_optional_resource_unit};
    use futures::StreamExt;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::io::AsyncWriteExt;

    struct ObservedReader {
        polls: Arc<AtomicUsize>,
        fail: bool,
    }

    impl AsyncRead for ObservedReader {
        fn poll_read(self: Pin<&mut Self>, _cx: &mut Context<'_>, output: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                Poll::Ready(Err(io::Error::from(io::ErrorKind::PermissionDenied)))
            } else {
                output.put_slice(b"data");
                Poll::Ready(Ok(()))
            }
        }
    }

    #[tokio::test]
    async fn background_frames_keep_capacity_until_the_last_slice() {
        let memory = BackgroundMemoryBudget::new();
        memory.set_sampled_limit(16, Duration::from_secs(30));
        let (resources, gates) = test_resource_units(2);
        let mut stream = with_optional_resource_unit(Some(resources.with_memory_budget(memory.clone())), async {
            BackgroundReaderStream::with_capacity(io::Cursor::new(b"data"), 16)
        })
        .await;
        assert!(
            gates.iter().all(|gate| gate.snapshot().active == 0),
            "the stream captures only memory ownership"
        );
        assert_eq!(memory.snapshot().used, 0, "construction does not allocate payload storage");
        let bytes = stream.next().await.expect("first frame").expect("read frame");
        assert_eq!(bytes.as_ref(), b"data");
        assert_eq!(memory.snapshot().used, 16, "charge full capacity rather than visible length");
        let slice = bytes.slice(1..3);
        memory.set_sampled_limit(0, Duration::ZERO);
        drop(bytes);
        drop(stream);
        assert_eq!(slice.as_ref(), b"at");
        assert_eq!(memory.snapshot().used, 16);
        drop(slice);
        assert_eq!(memory.snapshot().used, 0);
    }

    #[tokio::test]
    async fn background_denial_happens_before_reading_and_never_falls_back() {
        for sample in [None, Some((7, Duration::from_secs(30))), Some((8, Duration::ZERO))] {
            let memory = BackgroundMemoryBudget::new();
            if let Some((limit, validity)) = sample {
                memory.set_sampled_limit(limit, validity);
            }
            let polls = Arc::new(AtomicUsize::new(0));
            let (resources, _) = test_resource_units(1);
            let mut stream = with_optional_resource_unit(Some(resources.with_memory_budget(memory.clone())), async {
                BackgroundReaderStream::with_capacity(
                    ObservedReader {
                        polls: polls.clone(),
                        fail: false,
                    },
                    8,
                )
            })
            .await;
            let error = stream
                .next()
                .await
                .expect("allocation failure")
                .expect_err("background allocation denied");
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            assert_eq!(polls.load(Ordering::SeqCst), 0, "denial cannot consume source bytes");
            assert!(stream.next().await.is_none());
            assert_eq!(memory.snapshot().used, 0);
        }
    }

    #[tokio::test]
    async fn background_pending_read_keeps_storage_through_shrink_or_cancellation() {
        for cancelled in [false, true] {
            let memory = BackgroundMemoryBudget::new();
            memory.set_sampled_limit(16, Duration::from_secs(30));
            let (resources, _) = test_resource_units(1);
            let (mut sender, reader) = tokio::io::duplex(4);
            let mut stream = with_optional_resource_unit(Some(resources.with_memory_budget(memory.clone())), async {
                BackgroundReaderStream::with_capacity(reader, 16)
            })
            .await;
            assert!(futures::poll!(stream.next()).is_pending());
            memory.set_sampled_limit(0, Duration::ZERO);
            assert_eq!(memory.snapshot().used, 16);
            let bytes = if cancelled {
                None
            } else {
                sender.write_all(b"data").await.expect("release pending read");
                Some(
                    stream
                        .next()
                        .await
                        .expect("pending frame")
                        .expect("existing allocation survives shrink"),
                )
            };
            drop(stream);
            if let Some(bytes) = bytes {
                assert_eq!(bytes.as_ref(), b"data");
                assert_eq!(memory.snapshot().used, 16);
                drop(bytes);
            }
            assert_eq!(memory.snapshot().used, 0);
        }
    }

    #[tokio::test]
    async fn background_read_error_and_eof_release_storage() {
        for fail in [false, true] {
            let memory = BackgroundMemoryBudget::new();
            memory.set_sampled_limit(8, Duration::from_secs(30));
            let (resources, _) = test_resource_units(1);
            let mut stream = with_optional_resource_unit(Some(resources.with_memory_budget(memory.clone())), async {
                let reader: Box<dyn AsyncRead + Unpin> = if fail {
                    Box::new(ObservedReader {
                        polls: Arc::new(AtomicUsize::new(0)),
                        fail: true,
                    })
                } else {
                    Box::new(tokio::io::empty())
                };
                BackgroundReaderStream::with_capacity(reader, 8)
            })
            .await;
            if fail {
                let error = stream.next().await.expect("read failure").expect_err("preserve reader error");
                assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
            }
            assert!(stream.next().await.is_none());
            assert_eq!(memory.snapshot().used, 0);
        }
    }

    #[tokio::test]
    async fn retained_frame_blocks_new_storage_until_released() {
        let memory = BackgroundMemoryBudget::new();
        memory.set_sampled_limit(8, Duration::from_secs(30));
        let (resources, _) = test_resource_units(1);
        let mut stream = with_optional_resource_unit(Some(resources.with_memory_budget(memory.clone())), async {
            BackgroundReaderStream::with_capacity(io::Cursor::new(b"firstonefollowup"), 8)
        })
        .await;
        let first = stream.next().await.expect("first frame").expect("fits exact budget");
        assert_eq!(first.as_ref(), b"firstone");
        let error = stream
            .next()
            .await
            .expect("next allocation")
            .expect_err("held frame exhausts budget");
        assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
        assert_eq!(memory.snapshot().used, 8);
        drop(stream);
        drop(first);
        assert_eq!(memory.snapshot().used, 0);
    }

    #[tokio::test]
    async fn consumed_frames_keep_reusable_capacity_until_eof() {
        let memory = BackgroundMemoryBudget::new();
        memory.set_sampled_limit(8, Duration::from_secs(30));
        let (resources, _) = test_resource_units(1);
        let mut stream = with_optional_resource_unit(Some(resources.with_memory_budget(memory.clone())), async {
            BackgroundReaderStream::with_capacity(io::Cursor::new(b"firstonefollowup"), 8)
        })
        .await;
        for expected in [b"firstone", b"followup"] {
            let bytes = stream.next().await.expect("next frame").expect("released previous capacity");
            assert_eq!(bytes.as_ref(), expected);
            assert_eq!(memory.snapshot().used, 8);
            drop(bytes);
            assert_eq!(memory.snapshot().used, 8, "the reusable allocation remains owned by the stream");
        }
        assert!(stream.next().await.is_none());
        assert_eq!(memory.snapshot().used, 0);
    }

    #[tokio::test]
    async fn foreground_stream_and_unpolled_cancellation_preserve_existing_behavior() {
        let mut stream = BackgroundReaderStream::with_capacity(io::Cursor::new(b"foreground"), 4);
        assert!(matches!(stream.inner, ReaderState::Foreground(_)));
        let mut bytes = Vec::new();
        while let Some(chunk) = stream.next().await {
            bytes.extend_from_slice(&chunk.expect("foreground chunk"));
        }
        assert_eq!(bytes, b"foreground");

        let memory = BackgroundMemoryBudget::new();
        let (resources, _) = test_resource_units(1);
        let stream = with_optional_resource_unit(Some(resources.with_memory_budget(memory.clone())), async {
            BackgroundReaderStream::with_capacity(tokio::io::empty(), 8)
        })
        .await;
        drop(stream);
        assert_eq!(memory.snapshot().used, 0);
    }
}

#[cfg(test)]
mod reuse_tests {
    use super::*;
    use bytes::Bytes;
    use futures::StreamExt;
    use std::sync::Arc;
    use std::time::Duration;
    use std::{
        io,
        pin::Pin,
        task::{Context, Poll},
    };
    use tokio::io::{AsyncRead, ReadBuf};

    static DATA: [u8; 1024] = [0x37; 1024];
    struct ShortReader {
        offset: usize,
        length: usize,
        chunk: usize,
        fail_at: Option<usize>,
    }
    impl AsyncRead for ShortReader {
        fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>, out: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            if self.fail_at == Some(self.offset) {
                return Poll::Ready(Err(io::ErrorKind::PermissionDenied.into()));
            }
            let n = (self.length - self.offset).min(self.chunk).min(out.remaining());
            out.put_slice(&DATA[self.offset..self.offset + n]);
            self.offset += n;
            Poll::Ready(Ok(()))
        }
    }
    fn budget(limit: u64) -> Arc<crate::disk::BackgroundMemoryBudget> {
        let b = crate::disk::BackgroundMemoryBudget::new();
        b.set_sampled_limit(limit, Duration::from_secs(30));
        b
    }
    async fn stream<R: AsyncRead>(
        budget: &Arc<crate::disk::BackgroundMemoryBudget>,
        reader: R,
        capacity: usize,
    ) -> BackgroundReaderStream<R> {
        let (resources, _) = crate::disk::test_resource_units(1);
        crate::disk::with_optional_resource_unit(Some(resources.with_memory_budget(budget.clone())), async {
            BackgroundReaderStream::with_capacity(reader, capacity)
        })
        .await
    }
    #[tokio::test]
    async fn complete_consumed_frames_reclaim_the_same_storage_after_expiry() {
        let b = budget(64);
        let mut s = stream(&b, io::Cursor::new(&DATA[..]), 64).await;
        let mut pointer = None;
        for _ in 0..16 {
            let frame = s.next().await.expect("next full frame").expect("reclaimed backing");
            assert_eq!(frame.len(), 64);
            assert_eq!(frame.as_ref(), &DATA[..64]);
            if let Some(p) = pointer {
                assert_eq!(frame.as_ptr(), p);
            } else {
                pointer = Some(frame.as_ptr());
            }
            b.set_sampled_limit(0, Duration::ZERO);
            drop(frame);
            assert_eq!(b.snapshot().used, 64);
        }
        assert!(s.next().await.is_none());
        assert_eq!(b.snapshot().used, 0);
    }
    #[tokio::test]
    async fn short_live_frames_use_disjoint_tail_then_reserve_separate_storage() {
        let b = budget(32);
        let mut s = stream(
            &b,
            ShortReader {
                offset: 0,
                length: 32,
                chunk: 4,
                fail_at: None,
            },
            16,
        )
        .await;
        let mut frames: Vec<Bytes> = Vec::new();
        for i in 0..4 {
            let frame = s.next().await.expect("short frame").expect("remaining initialized tail");
            if i != 0 {
                assert_eq!(frame.as_ptr(), frames[0].as_ptr().wrapping_add(i * 4));
            }
            assert_eq!(frame.as_ref(), &DATA[..4]);
            frames.push(frame);
            assert_eq!(b.snapshot().used, 16);
        }
        let next = s.next().await.expect("independent frame").expect("second backing fits");
        assert_ne!(next.as_ptr(), frames[0].as_ptr());
        assert_eq!(b.snapshot().used, 32);
        drop(frames);
        assert_eq!(b.snapshot().used, 16);
        drop(s);
        assert_eq!(b.snapshot().used, 16);
        drop(next);
        assert_eq!(b.snapshot().used, 0);
    }
    #[tokio::test]
    async fn final_slice_prevents_reclaim_and_keeps_old_plus_new_charged() {
        let b = budget(32);
        let mut s = stream(&b, io::Cursor::new(&DATA[..32]), 16).await;
        let first = s.next().await.expect("old frame").expect("old backing");
        let last = first.slice(15..);
        drop(first);
        let next = s.next().await.expect("new frame").expect("second backing fits");
        assert_eq!(last.as_ref(), &DATA[..1]);
        assert_ne!(next.as_ptr(), last.as_ptr());
        assert_eq!(b.snapshot().used, 32);
        drop(s);
        assert_eq!(b.snapshot().used, 32);
        drop(next);
        assert_eq!(b.snapshot().used, 16);
        drop(last);
        assert_eq!(b.snapshot().used, 0);
    }
    #[tokio::test]
    async fn expiry_uses_tail_but_rejects_new_storage_before_consuming_source() {
        let b = budget(16);
        let mut s = stream(
            &b,
            ShortReader {
                offset: 0,
                length: 32,
                chunk: 4,
                fail_at: None,
            },
            16,
        )
        .await;
        let first = s.next().await.expect("first frame").expect("initial backing");
        b.set_sampled_limit(0, Duration::ZERO);
        for _ in 0..3 {
            drop(s.next().await.expect("tail frame").expect("reuse charged storage"));
        }
        let error = s
            .next()
            .await
            .expect("new backing denied")
            .expect_err("live frame prevents reclaim");
        assert!(crate::disk::background_memory::is_memory_pressure(&error));
        assert!(s.next().await.is_none());
        assert_eq!(b.snapshot().used, 16);
        drop(first);
        assert_eq!(b.snapshot().used, 0);
    }
    #[tokio::test]
    async fn eof_and_error_release_spare_but_keep_live_output_charged() {
        for fail_at in [None, Some(4)] {
            let b = budget(16);
            let mut s = stream(
                &b,
                ShortReader {
                    offset: 0,
                    length: 4,
                    chunk: 4,
                    fail_at,
                },
                16,
            )
            .await;
            let frame = s.next().await.expect("first short frame").expect("payload");
            match fail_at {
                None => assert!(s.next().await.is_none()),
                Some(_) => assert_eq!(
                    s.next()
                        .await
                        .expect("read error")
                        .expect_err("preserve source failure")
                        .kind(),
                    io::ErrorKind::PermissionDenied
                ),
            }
            assert!(s.next().await.is_none());
            assert_eq!(b.snapshot().used, 16);
            assert_eq!(frame.as_ref(), &DATA[..4]);
            drop(frame);
            assert_eq!(b.snapshot().used, 0);
        }
    }
    #[test]
    fn runtime_shutdown_drops_a_pending_stream_without_refunding_early() {
        for abort in [false, true] {
            let b = budget(16);
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("runtime");
            let (_tx, reader) = tokio::io::duplex(4);
            let (began, wait) = tokio::sync::oneshot::channel();
            let memory = b.clone();
            let handle = rt.spawn(async move {
                let mut s = stream(&memory, reader, 16).await;
                assert!(futures::poll!(s.next()).is_pending());
                assert_eq!(memory.snapshot().used, 16);
                began.send(()).expect("notify pending allocation");
                let _ = s.next().await;
            });
            rt.block_on(wait).expect("stream pending");
            assert_eq!(b.snapshot().used, 16);
            if abort {
                handle.abort();
                let _ = rt.block_on(handle);
            }
            drop(rt);
            assert_eq!(b.snapshot().used, 0);
        }
    }
}
