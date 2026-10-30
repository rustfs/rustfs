// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use crate::disk::background_memory::BackgroundBuffer;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker, ready};
use tokio::io::{AsyncRead, AsyncWrite, DuplexStream, ReadBuf};

pub struct BackgroundPipeReader {
    inner: PipeEnd,
}

pub struct BackgroundPipeWriter {
    inner: PipeEnd,
}

impl BackgroundPipeWriter {
    /// Capture only the payload owner, without retaining node or physical units.
    pub fn memory_budget(&self) -> Option<Arc<crate::disk::BackgroundMemoryBudget>> {
        match &self.inner {
            PipeEnd::Foreground(_) => None,
            PipeEnd::Background(state) => state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .buffer
                .memory_budget(),
        }
    }
}

enum PipeEnd {
    Foreground(DuplexStream),
    Background(Arc<Mutex<PipeState>>),
}

// The pipe lock may reserve memory. The memory budget never locks a pipe.
struct PipeState {
    buffer: BackgroundBuffer,
    read_offset: usize,
    readable: usize,
    capacity: usize,
    reader_closed: bool,
    writer_closed: bool,
    admission_failure: Option<io::ErrorKind>,
    read_waker: Option<Waker>,
    write_waker: Option<Waker>,
}

/// Capture only memory ownership while the request is admitted. Payload storage
/// is reserved and initialized once, before the first nonempty background write.
pub fn background_pipe(capacity: usize) -> (BackgroundPipeReader, BackgroundPipeWriter) {
    let capacity = capacity.max(1);
    let memory = crate::disk::current_resource_unit().and_then(|resources| resources.memory_budget());
    match memory {
        Some(memory) => {
            let state = Arc::new(Mutex::new(PipeState {
                buffer: BackgroundBuffer::new(Some(memory)),
                read_offset: 0,
                readable: 0,
                capacity,
                reader_closed: false,
                writer_closed: false,
                admission_failure: None,
                read_waker: None,
                write_waker: None,
            }));
            (
                BackgroundPipeReader {
                    inner: PipeEnd::Background(state.clone()),
                },
                BackgroundPipeWriter {
                    inner: PipeEnd::Background(state),
                },
            )
        }
        None => {
            let (reader, writer) = tokio::io::duplex(capacity);
            (
                BackgroundPipeReader {
                    inner: PipeEnd::Foreground(reader),
                },
                BackgroundPipeWriter {
                    inner: PipeEnd::Foreground(writer),
                },
            )
        }
    }
}

fn advance_ring(offset: usize, count: usize, capacity: usize) -> usize {
    let to_end = capacity - offset;
    if count < to_end { offset + count } else { count - to_end }
}

fn poll_background_read(state: &Mutex<PipeState>, cx: &mut Context<'_>, output: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
    if output.remaining() == 0 {
        return Poll::Ready(Ok(()));
    }
    let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.readable == 0 {
        if let Some(kind) = state.admission_failure.take() {
            drop(state);
            return Poll::Ready(Err(io::Error::new(kind, "background pipe storage admission failed")));
        }
        if state.writer_closed {
            return Poll::Ready(Ok(()));
        }
        let replaced = state.read_waker.replace(cx.waker().clone());
        drop(state);
        drop(replaced);
        return Poll::Pending;
    }
    let length = state.readable.min(output.remaining());
    let first = length.min(state.capacity - state.read_offset);
    output.put_slice(&state.buffer.as_slice()[state.read_offset..state.read_offset + first]);
    output.put_slice(&state.buffer.as_slice()[..length - first]);
    state.read_offset = advance_ring(state.read_offset, length, state.capacity);
    state.readable -= length;
    let wake = state.write_waker.take();
    drop(state);
    if let Some(wake) = wake {
        wake.wake();
    }
    Poll::Ready(Ok(()))
}

fn poll_background_write(state: &Mutex<PipeState>, cx: &mut Context<'_>, input: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
    let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
    if state.reader_closed || state.writer_closed {
        return Poll::Ready(Err(io::ErrorKind::BrokenPipe.into()));
    }
    if input.iter().all(|part| part.is_empty()) {
        return Poll::Ready(Ok(0));
    }
    let available = state.capacity - state.readable;
    if available == 0 {
        let replaced = state.write_waker.replace(cx.waker().clone());
        drop(state);
        drop(replaced);
        return Poll::Pending;
    }
    let capacity = state.capacity;
    if let Err(error) = state.buffer.ensure_len(capacity) {
        // Legacy walk completion may suppress ordinary producer errors. A new
        // pipe admission failure must also reach the reader instead of EOF.
        state.admission_failure = Some(error.kind());
        state.writer_closed = true;
        let wake = state.read_waker.take();
        drop(state);
        if let Some(wake) = wake {
            wake.wake();
        }
        return Poll::Ready(Err(error));
    }
    let mut offset = advance_ring(state.read_offset, state.readable, capacity);
    let mut written = 0;
    for part in input {
        let length = part.len().min(available - written);
        let first = length.min(capacity - offset);
        state.buffer.as_mut_slice()[offset..offset + first].copy_from_slice(&part[..first]);
        state.buffer.as_mut_slice()[..length - first].copy_from_slice(&part[first..length]);
        offset = advance_ring(offset, length, capacity);
        written += length;
        if written == available {
            break;
        }
    }
    state.readable += written;
    let wake = state.read_waker.take();
    drop(state);
    if let Some(wake) = wake {
        wake.wake();
    }
    Poll::Ready(Ok(written))
}

impl AsyncRead for BackgroundPipeReader {
    fn poll_read(self: Pin<&mut Self>, cx: &mut Context<'_>, output: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            PipeEnd::Foreground(reader) => Pin::new(reader).poll_read(cx, output),
            PipeEnd::Background(state) => {
                let coop = ready!(tokio::task::coop::poll_proceed(cx));
                let result = poll_background_read(state, cx, output);
                if result.is_ready() {
                    coop.made_progress();
                }
                result
            }
        }
    }
}

impl AsyncWrite for BackgroundPipeWriter {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, input: &[u8]) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            PipeEnd::Foreground(writer) => Pin::new(writer).poll_write(cx, input),
            PipeEnd::Background(state) => {
                let coop = ready!(tokio::task::coop::poll_proceed(cx));
                let result = poll_background_write(state, cx, &[IoSlice::new(input)]);
                if result.is_ready() {
                    coop.made_progress();
                }
                result
            }
        }
    }

    fn poll_write_vectored(self: Pin<&mut Self>, cx: &mut Context<'_>, input: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        match &mut self.get_mut().inner {
            PipeEnd::Foreground(writer) => Pin::new(writer).poll_write_vectored(cx, input),
            PipeEnd::Background(state) => {
                let coop = ready!(tokio::task::coop::poll_proceed(cx));
                let result = poll_background_write(state, cx, input);
                if result.is_ready() {
                    coop.made_progress();
                }
                result
            }
        }
    }

    fn is_write_vectored(&self) -> bool {
        match &self.inner {
            PipeEnd::Foreground(writer) => writer.is_write_vectored(),
            PipeEnd::Background(_) => true,
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            PipeEnd::Foreground(writer) => Pin::new(writer).poll_flush(cx),
            PipeEnd::Background(_) => Poll::Ready(Ok(())),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match &mut self.get_mut().inner {
            PipeEnd::Foreground(writer) => Pin::new(writer).poll_shutdown(cx),
            PipeEnd::Background(state) => {
                let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                state.writer_closed = true;
                let wake = state.read_waker.take();
                drop(state);
                if let Some(wake) = wake {
                    wake.wake();
                }
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl Drop for BackgroundPipeReader {
    fn drop(&mut self) {
        if let PipeEnd::Background(state) = &self.inner {
            let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.reader_closed = true;
            let wake = state.write_waker.take();
            let cancelled = state.read_waker.take();
            drop(state);
            drop(cancelled);
            if let Some(wake) = wake {
                wake.wake();
            }
        }
    }
}

impl Drop for BackgroundPipeWriter {
    fn drop(&mut self) {
        if let PipeEnd::Background(state) = &self.inner {
            let mut state = state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            state.writer_closed = true;
            let wake = state.read_waker.take();
            let cancelled = state.write_waker.take();
            drop(state);
            drop(cancelled);
            if let Some(wake) = wake {
                wake.wake();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::disk::BackgroundMemoryBudget;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    pub(super) fn budget(capacity: usize) -> Arc<BackgroundMemoryBudget> {
        let budget = BackgroundMemoryBudget::new();
        budget.set_sampled_limit(u64::try_from(capacity).expect("test capacity fits u64"), Duration::from_secs(30));
        budget
    }

    pub(super) fn local_pipe(
        memory: &Arc<BackgroundMemoryBudget>,
        capacity: usize,
    ) -> (BackgroundPipeReader, BackgroundPipeWriter) {
        let state = Arc::new(Mutex::new(PipeState {
            buffer: BackgroundBuffer::new(Some(memory.clone())),
            read_offset: 0,
            readable: 0,
            capacity,
            reader_closed: false,
            writer_closed: false,
            admission_failure: None,
            read_waker: None,
            write_waker: None,
        }));
        (
            BackgroundPipeReader {
                inner: PipeEnd::Background(state.clone()),
            },
            BackgroundPipeWriter {
                inner: PipeEnd::Background(state),
            },
        )
    }

    pub(super) fn write_now(writer: &mut (impl AsyncWrite + Unpin), bytes: &[u8]) -> Poll<io::Result<usize>> {
        Pin::new(writer).poll_write(&mut Context::from_waker(Waker::noop()), bytes)
    }

    pub(super) fn read_now(reader: &mut (impl AsyncRead + Unpin), bytes: &mut [u8]) -> Poll<io::Result<usize>> {
        let mut output = ReadBuf::new(bytes);
        match Pin::new(reader).poll_read(&mut Context::from_waker(Waker::noop()), &mut output) {
            Poll::Ready(Ok(())) => Poll::Ready(Ok(output.filled().len())),
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => Poll::Pending,
        }
    }
    #[test]
    fn fixed_ring_allocates_lazily_once_and_keeps_last_owner_charged() {
        for capacity in [1, 7, 64 * 1024, 1024 * 1024] {
            let memory = budget(capacity);
            let (mut reader, mut writer) = local_pipe(&memory, capacity);
            assert_eq!(memory.snapshot().used, 0);
            assert!(matches!(write_now(&mut writer, &[]), Poll::Ready(Ok(0))));
            assert_eq!(memory.snapshot().used, 0);
            assert!(matches!(write_now(&mut writer, b"x"), Poll::Ready(Ok(1))));
            assert_eq!(memory.snapshot().used, capacity as u64);
            memory.set_sampled_limit(0, Duration::ZERO);
            let mut output = [0];
            assert!(matches!(read_now(&mut reader, &mut output), Poll::Ready(Ok(1))));
            assert_eq!(output, *b"x");
            assert!(matches!(write_now(&mut writer, b"y"), Poll::Ready(Ok(1))));
            drop(writer);
            assert_eq!(memory.snapshot().used, capacity as u64, "reader still owns the real ring");
            assert!(matches!(read_now(&mut reader, &mut output), Poll::Ready(Ok(1))));
            assert_eq!(output, *b"y");
            assert!(matches!(read_now(&mut reader, &mut output), Poll::Ready(Ok(0))));
            assert_eq!(memory.snapshot().used, capacity as u64, "draining does not destroy allocation ownership");
            drop(reader);
            assert_eq!(memory.snapshot().used, 0);
        }
    }

    #[test]
    fn admission_error_reaches_writer_and_reader_once_without_payload() {
        for sample in [None, Some((6, Duration::from_secs(30))), Some((7, Duration::ZERO))] {
            let memory = BackgroundMemoryBudget::new();
            if let Some((limit, validity)) = sample {
                memory.set_sampled_limit(limit, validity);
            }
            let (mut reader, mut writer) = local_pipe(&memory, 7);
            let error = match write_now(&mut writer, b"x") {
                Poll::Ready(Err(e)) => e,
                _ => panic!("reject before copying first payload"),
            };
            assert!(
                crate::disk::background_memory::is_memory_pressure(&error),
                "writer retains the original budget marker"
            );
            assert_eq!(memory.snapshot().used, 0);
            let error = match read_now(&mut reader, &mut [0]) {
                Poll::Ready(Err(e)) => e,
                _ => panic!("reader must see a pipe admission failure"),
            };
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            assert!(matches!(read_now(&mut reader, &mut [0]), Poll::Ready(Ok(0))), "failure is delivered once");
            assert!(matches!(write_now(&mut writer,b"x"),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::BrokenPipe));
            drop((reader, writer));
            assert_eq!(memory.snapshot().used, 0);
        }
    }

    #[test]
    fn allocation_overflow_is_a_reader_failure_without_a_leaked_reservation() {
        let memory = BackgroundMemoryBudget::new();
        memory.set_sampled_limit(u64::MAX, Duration::from_secs(30));
        let (mut reader, mut writer) = local_pipe(&memory, usize::MAX);
        assert!(matches!(write_now(&mut writer,b"x"),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::OutOfMemory));
        assert_eq!(memory.snapshot().used, 0);
        assert!(matches!(read_now(&mut reader,&mut[0]),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::OutOfMemory));
    }

    #[test]
    fn scalar_and_vectored_wrap_match_a_reference_queue() {
        use std::collections::VecDeque;
        for capacity in [1, 3, 7, 31] {
            let memory = budget(capacity);
            let (mut reader, mut writer) = local_pipe(&memory, capacity);
            let mut expected = VecDeque::new();
            let mut next = 0u8;
            for turn in 0..300 {
                let input: Vec<_> = (0..(turn % 11 + 1))
                    .map(|_| {
                        let byte = next;
                        next = next.wrapping_add(1);
                        byte
                    })
                    .collect();
                let available = capacity - expected.len();
                let written = if turn % 2 == 0 {
                    write_now(&mut writer, &input)
                } else {
                    let split = input.len() / 2;
                    Pin::new(&mut writer).poll_write_vectored(
                        &mut Context::from_waker(Waker::noop()),
                        &[
                            io::IoSlice::new(&[]),
                            io::IoSlice::new(&input[..split]),
                            io::IoSlice::new(&input[split..]),
                        ],
                    )
                };
                if available == 0 {
                    assert!(written.is_pending());
                } else {
                    let accepted = input.len().min(available);
                    assert!(matches!(written,Poll::Ready(Ok(n)) if n==accepted));
                    expected.extend(&input[..accepted]);
                }
                let mut output = vec![0; turn % 5 + 1];
                let count = output.len().min(expected.len());
                assert!(matches!(read_now(&mut reader,&mut output),Poll::Ready(Ok(n)) if n==count));
                for byte in &output[..count] {
                    assert_eq!(Some(*byte), expected.pop_front());
                }
                assert_eq!(memory.snapshot().used, capacity as u64);
            }
            drop(writer);
            let mut output = vec![0; capacity];
            let count = expected.len();
            assert!(matches!(read_now(&mut reader,&mut output),Poll::Ready(Ok(n)) if n==count));
            assert_eq!(&output[..count], expected.into_iter().collect::<Vec<_>>().as_slice());
            assert!(matches!(read_now(&mut reader, &mut output), Poll::Ready(Ok(0))));
            drop(reader);
            assert_eq!(memory.snapshot().used, 0);
        }
    }

    #[tokio::test]
    async fn reader_drop_wakes_blocked_writer_and_writer_drop_wakes_empty_reader() {
        let memory = budget(7);
        let (reader, mut writer) = local_pipe(&memory, 7);
        writer.write_all(b"1234567").await.expect("fill the ring");
        let task = tokio::spawn(async move { writer.write_all(b"blocked").await });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        drop(reader);
        let error = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("reader drop wakes writer")
            .expect("writer task joins")
            .expect_err("closed reader is BrokenPipe");
        assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
        assert_eq!(memory.snapshot().used, 0);
        let (mut reader, writer) = local_pipe(&memory, 7);
        let task = tokio::spawn(async move { reader.read(&mut [0]).await });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        drop(writer);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), task)
                .await
                .expect("writer drop wakes reader")
                .expect("reader task joins")
                .expect("clean EOF"),
            0
        );
    }

    #[tokio::test]
    async fn read_wakes_a_full_writer_without_changing_byte_limit() {
        let memory = budget(7);
        let (mut reader, mut writer) = local_pipe(&memory, 7);
        writer.write_all(b"1234567").await.expect("fill initial byte limit");
        let task = tokio::spawn(async move {
            writer.write_all(b"x").await.expect("resume after one byte is read");
            writer
        });
        tokio::task::yield_now().await;
        assert!(!task.is_finished());
        let mut first = [0];
        reader.read_exact(&mut first).await.expect("read one byte");
        let writer = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .expect("read wakes writer")
            .expect("writer joins");
        assert_eq!(&first, b"1");
        drop(writer);
        let mut all = Vec::new();
        reader.read_to_end(&mut all).await.expect("drain wrapped bytes");
        assert_eq!(all, b"234567x");
        drop(reader);
        assert_eq!(memory.snapshot().used, 0);
    }

    #[tokio::test]
    async fn shutdown_drains_then_eof_and_cancelled_writes_accept_nothing() {
        let memory = budget(7);
        let (mut reader, mut writer) = local_pipe(&memory, 7);
        writer
            .write_all(b"1234567")
            .await
            .expect("fill before cancelling a blocked write");
        {
            let blocked = writer.write(b"lost");
            tokio::pin!(blocked);
            assert!(futures::poll!(&mut blocked).is_pending());
        }
        writer.shutdown().await.expect("shutdown does not discard buffered bytes");
        assert!(matches!(write_now(&mut writer,b"x"),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::BrokenPipe));
        let mut out = Vec::new();
        reader.read_to_end(&mut out).await.expect("drain after shutdown");
        assert_eq!(out, b"1234567");
        drop(reader);
        assert_eq!(memory.snapshot().used, 7, "writer still holds the allocation");
        drop(writer);
        assert_eq!(memory.snapshot().used, 0);
    }

    #[test]
    fn foreground_keeps_native_duplex_semantics() {
        let (reader, mut writer) = background_pipe(7);
        assert!(matches!(write_now(&mut writer, b"12345678"), Poll::Ready(Ok(7))));
        assert!(write_now(&mut writer, b"x").is_pending());
        drop(reader);
        assert!(matches!(write_now(&mut writer,b"x"),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::BrokenPipe));
        let (mut reader, writer) = background_pipe(7);
        drop(writer);
        assert!(matches!(read_now(&mut reader, &mut [0]), Poll::Ready(Ok(0))));
    }

    #[test]
    fn cancelled_reader_leaves_the_real_ring_charged_until_writer_drop() {
        let memory = budget(7);
        let (reader, mut writer) = local_pipe(&memory, 7);
        assert!(matches!(write_now(&mut writer, b"x"), Poll::Ready(Ok(1))));
        drop(reader);
        assert_eq!(memory.snapshot().used, 7, "the surviving writer still owns the ring allocation");
        assert!(matches!(write_now(&mut writer,b"y"),Poll::Ready(Err(e)) if e.kind()==io::ErrorKind::BrokenPipe));
        drop(writer);
        assert_eq!(memory.snapshot().used, 0);
    }

    #[test]
    fn runtime_shutdown_releases_queued_writer_but_not_the_surviving_reader() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("build isolated runtime");
        let memory = budget(7);
        let (reader, mut writer) = local_pipe(&memory, 7);
        runtime.block_on(async {
            writer.write_all(b"x").await.expect("allocate ring before runtime shutdown");
            tokio::spawn(async move {
                let _writer = writer;
                std::future::pending::<()>().await;
            });
            tokio::task::yield_now().await;
        });
        drop(runtime);
        assert_eq!(memory.snapshot().used, 7, "reader holds the actual allocation after runtime cancellation");
        drop(reader);
        assert_eq!(memory.snapshot().used, 0);
    }

    #[tokio::test]
    async fn ready_operations_keep_tokio_cooperative_yields() {
        let memory = budget(7);
        let (_reader, mut writer) = local_pipe(&memory, 7);
        let observed = Arc::new(AtomicUsize::new(0));
        let flag = observed.clone();
        let task = tokio::spawn(async move {
            flag.store(1, Ordering::SeqCst);
        });
        for _ in 0..1000 {
            assert_eq!(writer.write(&[]).await.expect("empty operation remains successful"), 0);
        }
        assert_eq!(
            observed.load(Ordering::SeqCst),
            1,
            "ready pipe operations must not starve other ready tasks"
        );
        task.await.expect("observer task joins");
    }
}

#[cfg(test)]
mod waker_tests {
    use super::*;
    use std::sync::{
        Weak,
        atomic::{AtomicUsize, Ordering},
    };
    use std::task::Wake;

    struct LockCheck {
        state: Weak<Mutex<PipeState>>,
        wakes: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }
    impl LockCheck {
        fn unlocked(&self) {
            if let Some(state) = self.state.upgrade() {
                assert!(state.try_lock().is_ok(), "waker code must run outside the pipe lock");
            }
        }
    }
    impl Wake for LockCheck {
        fn wake(self: Arc<Self>) {
            self.unlocked();
            self.wakes.fetch_add(1, Ordering::SeqCst);
        }
    }
    impl Drop for LockCheck {
        fn drop(&mut self) {
            self.unlocked();
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn replaced_and_woken_reader_and_writer_wakers_run_outside_the_lock() {
        for waiting_reader in [true, false] {
            let memory = super::tests::budget(7);
            let (mut reader, mut writer) = super::tests::local_pipe(&memory, 7);
            let PipeEnd::Background(state) = &reader.inner else { panic!("background fixture") };
            let weak = Arc::downgrade(state);
            let wakes = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            if !waiting_reader {
                assert!(matches!(super::tests::write_now(&mut writer, b"1234567"), Poll::Ready(Ok(7))));
            }
            for _ in 0..2 {
                let waker = Waker::from(Arc::new(LockCheck {
                    state: weak.clone(),
                    wakes: wakes.clone(),
                    drops: drops.clone(),
                }));
                let mut cx = Context::from_waker(&waker);
                if waiting_reader {
                    let mut bytes = [0];
                    assert!(
                        Pin::new(&mut reader)
                            .poll_read(&mut cx, &mut ReadBuf::new(&mut bytes))
                            .is_pending()
                    );
                } else {
                    assert!(Pin::new(&mut writer).poll_write(&mut cx, b"x").is_pending());
                }
                drop(waker);
            }
            assert_eq!(drops.load(Ordering::SeqCst), 1, "the replaced waker has been dropped");
            if waiting_reader {
                assert!(matches!(super::tests::write_now(&mut writer, b"x"), Poll::Ready(Ok(1))));
            } else {
                assert!(matches!(super::tests::read_now(&mut reader, &mut [0]), Poll::Ready(Ok(1))));
            }
            assert_eq!(wakes.load(Ordering::SeqCst), 1);
            assert_eq!(drops.load(Ordering::SeqCst), 2);
        }
    }
}
