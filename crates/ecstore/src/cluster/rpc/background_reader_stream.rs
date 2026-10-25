// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use crate::disk::background_memory::BackgroundBuffer;
use bytes::Bytes;
use futures::Stream;
use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};
use tokio_util::io::ReaderStream;

/// RPC response chunks retain their background memory reservation until the
/// last clone or slice is dropped. Foreground readers use the existing stream.
pub struct BackgroundReaderStream<R> {
    inner: ReaderState<R>,
}

enum ReaderState<R> {
    Foreground(ReaderStream<R>),
    Background {
        reader: Option<R>,
        buffer: BackgroundBuffer,
        capacity: usize,
    },
}

impl<R: AsyncRead> BackgroundReaderStream<R> {
    /// Capture memory ownership inside the admitted scope, before moving the
    /// response body out of it. This stream retains no node or device capacity.
    pub fn with_capacity(reader: R, capacity: usize) -> Self {
        let memory = crate::disk::current_resource_unit().and_then(|resources| resources.memory_budget());
        let inner = match memory {
            Some(memory) => ReaderState::Background {
                reader: Some(reader),
                buffer: BackgroundBuffer::new(Some(memory)),
                capacity: capacity.max(1),
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
            ReaderState::Background {
                reader,
                buffer,
                capacity,
            } => {
                let Some(inner) = reader.as_mut() else {
                    return Poll::Ready(None);
                };
                if let Err(error) = buffer.ensure_len(*capacity) {
                    *reader = None;
                    return Poll::Ready(Some(Err(error)));
                }
                let mut output = ReadBuf::new(buffer.as_mut_slice());
                match std::task::ready!(Pin::new(inner).poll_read(cx, &mut output)) {
                    Ok(()) => {
                        let length = output.filled().len();
                        let bytes = buffer.take_bytes(length);
                        if length == 0 {
                            *reader = None;
                            Poll::Ready(None)
                        } else {
                            Poll::Ready(Some(Ok(bytes)))
                        }
                    }
                    Err(error) => {
                        drop(buffer.take_bytes(0));
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
    async fn consumed_frames_release_capacity_for_the_next_read() {
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
            assert_eq!(memory.snapshot().used, 0);
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
