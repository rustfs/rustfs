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

//! Per-local-disk foreground-priority admission for scanner I/O.

use std::collections::VecDeque;
use std::future::Future;
use std::io;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use metrics::{counter, gauge, histogram};
use rustfs_rio::ChunkReader;
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::Notify;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

const BACKGROUND_FOREGROUND_BURST: u32 = 8;
const BACKGROUND_MAX_WAIT: Duration = Duration::from_millis(100);

tokio::task_local! {
    static DISK_IO_CONTEXT: DiskIoContext;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum DiskIoClass {
    Foreground,
    Background,
}

#[derive(Clone, Debug)]
pub(crate) struct DiskIoContext {
    pub(crate) class: DiskIoClass,
    pub(crate) cancellation: CancellationToken,
    pub(crate) admission: Option<Arc<DiskIoAdmission>>,
    pub(crate) permit: Option<DiskIoPermit>,
}

/// Mark disk I/O issued by `future` as background work for local-disk admission.
#[doc(hidden)]
pub async fn with_background_disk_io<F: Future>(cancellation: CancellationToken, future: F) -> F::Output {
    with_disk_io_context(
        DiskIoContext {
            class: DiskIoClass::Background,
            cancellation,
            admission: None,
            permit: None,
        },
        future,
    )
    .await
}

pub(crate) async fn with_disk_io_context<F: Future>(context: DiskIoContext, future: F) -> F::Output {
    DISK_IO_CONTEXT.scope(context, future).await
}

pub(crate) async fn with_disk_io_permit<F: Future>(permit: Option<DiskIoPermit>, future: F) -> F::Output {
    let Some(permit) = permit else {
        return future.await;
    };
    let mut context = current_disk_io_context().unwrap_or_else(|| DiskIoContext {
        class: DiskIoClass::Foreground,
        cancellation: CancellationToken::new(),
        admission: None,
        permit: None,
    });
    context.admission = Some(permit.admission());
    with_disk_io_context(
        DiskIoContext {
            permit: Some(permit),
            ..context
        },
        future,
    )
    .await
}

pub(crate) async fn with_optional_disk_io_context<F: Future>(context: Option<DiskIoContext>, future: F) -> F::Output {
    match context {
        Some(context) => with_disk_io_context(context, future).await,
        None => future.await,
    }
}

pub(crate) fn current_disk_io_context() -> Option<DiskIoContext> {
    DISK_IO_CONTEXT.try_with(Clone::clone).ok()
}

/// Acquire one low-level permit only when the current walker explicitly owns a gate.
/// Ordinary wrapper calls use their outer operation permit instead.
pub(crate) async fn acquire_context_disk_io_permit() -> io::Result<Option<DiskIoPermit>> {
    let Some(context) = current_disk_io_context() else {
        return Ok(None);
    };
    let Some(admission) = context.admission else {
        return Ok(None);
    };
    if let Some(permit) = context.permit
        && permit.is_for(&admission)
    {
        return Ok(Some(permit));
    }
    match context.class {
        DiskIoClass::Foreground => Ok(Some(admission.foreground())),
        DiskIoClass::Background => Ok(Some(admission.background(&context.cancellation).await?)),
    }
}

#[derive(Debug, Default)]
struct AdmissionState {
    foreground_completions: u32,
    next_background_ticket: u64,
    background_queue: VecDeque<u64>,
    background_front_since: Option<Instant>,
}

/// One gate is shared by all handles to a local disk, including reconnects.
/// Foreground I/O never waits on this gate. Background I/O runs exclusively
/// while the disk is idle, or after a bounded foreground burst/wait.
#[derive(Debug)]
pub(crate) struct DiskIoAdmission {
    state: Mutex<AdmissionState>,
    changed: Notify,
    disk: String,
    foreground_active: AtomicUsize,
    background_active: AtomicBool,
    background_waiters: AtomicUsize,
    #[cfg(test)]
    background_admissions: AtomicUsize,
}

impl DiskIoAdmission {
    pub(crate) fn new(disk: impl Into<String>) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(AdmissionState::default()),
            changed: Notify::new(),
            disk: disk.into(),
            foreground_active: AtomicUsize::new(0),
            background_active: AtomicBool::new(false),
            background_waiters: AtomicUsize::new(0),
            #[cfg(test)]
            background_admissions: AtomicUsize::new(0),
        })
    }

    #[cfg(test)]
    pub(crate) fn background_admissions_for_tests(&self) -> usize {
        self.background_admissions.load(Ordering::Relaxed)
    }

    #[cfg(test)]
    pub(crate) fn background_active_for_tests(&self) -> bool {
        self.background_active.load(Ordering::Acquire)
    }

    #[cfg(test)]
    fn foreground_active_for_tests(&self) -> usize {
        self.foreground_active.load(Ordering::Acquire)
    }

    pub(crate) fn foreground(self: &Arc<Self>) -> DiskIoPermit {
        self.foreground_active.fetch_add(1, Ordering::AcqRel);
        if self.background_waiters.load(Ordering::Acquire) > 0 {
            self.record_state();
        }
        DiskIoPermit {
            lease: Arc::new(DiskIoPermitLease {
                admission: self.clone(),
                class: DiskIoClass::Foreground,
            }),
        }
    }

    pub(crate) async fn background(self: &Arc<Self>, cancellation: &CancellationToken) -> std::io::Result<DiskIoPermit> {
        let started_at = Instant::now();
        let ticket = {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            let ticket = state.next_background_ticket;
            state.next_background_ticket = state.next_background_ticket.wrapping_add(1);
            if state.background_queue.is_empty() && !self.background_active.load(Ordering::Acquire) {
                state.background_front_since = Some(Instant::now());
            }
            state.background_queue.push_back(ticket);
            self.background_waiters.fetch_add(1, Ordering::Release);
            ticket
        };
        self.record_state();

        let mut waiter = BackgroundWaiter {
            admission: self.clone(),
            ticket,
            waiting: true,
        };
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();

            let granted = {
                let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                let is_front = state.background_queue.front() == Some(&ticket);
                let bounded_wait_elapsed = is_front
                    && state
                        .background_front_since
                        .is_some_and(|front_since| front_since.elapsed() >= BACKGROUND_MAX_WAIT);
                if is_front
                    && !self.background_active.load(Ordering::Acquire)
                    && (self.foreground_active.load(Ordering::Acquire) == 0
                        || state.foreground_completions >= BACKGROUND_FOREGROUND_BURST
                        || bounded_wait_elapsed)
                {
                    self.background_active.store(true, Ordering::Release);
                    state.background_queue.pop_front();
                    state.background_front_since = None;
                    self.background_waiters.fetch_sub(1, Ordering::AcqRel);
                    state.foreground_completions = 0;
                    true
                } else {
                    false
                }
            };

            if granted {
                waiter.waiting = false;
                #[cfg(test)]
                self.background_admissions.fetch_add(1, Ordering::Relaxed);
                histogram!("rustfs_disk_io_admission_wait_seconds", "disk" => self.disk.clone(), "class" => "background")
                    .record(started_at.elapsed().as_secs_f64());
                counter!("rustfs_disk_io_admission_total", "disk" => self.disk.clone(), "class" => "background").increment(1);
                self.record_state();
                return Ok(DiskIoPermit {
                    lease: Arc::new(DiskIoPermitLease {
                        admission: self.clone(),
                        class: DiskIoClass::Background,
                    }),
                });
            }

            let remaining = {
                let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
                if state.background_queue.front() == Some(&ticket) {
                    state
                        .background_front_since
                        .map(|front_since| BACKGROUND_MAX_WAIT.saturating_sub(front_since.elapsed()))
                } else {
                    None
                }
            };
            if let Some(remaining) = remaining.filter(|remaining| !remaining.is_zero()) {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "background disk I/O admission cancelled"));
                    }
                    _ = tokio::time::sleep(remaining) => {
                        self.changed.notify_waiters();
                    }
                    _ = &mut notified => {}
                }
            } else {
                tokio::select! {
                    _ = cancellation.cancelled() => {
                        return Err(std::io::Error::new(std::io::ErrorKind::Interrupted, "background disk I/O admission cancelled"));
                    }
                    _ = &mut notified => {}
                }
            }
        }
    }

    fn record_state(&self) {
        let (foreground_active, background_waiters) = {
            let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            (
                if state.background_queue.is_empty() {
                    0
                } else {
                    self.foreground_active.load(Ordering::Acquire)
                },
                state.background_queue.len(),
            )
        };
        let foreground_active = u32::try_from(foreground_active).unwrap_or(u32::MAX);
        let background_waiters = u32::try_from(background_waiters).unwrap_or(u32::MAX);
        gauge!("rustfs_disk_io_admission_active", "disk" => self.disk.clone(), "class" => "foreground")
            .set(f64::from(foreground_active));
        gauge!("rustfs_disk_io_admission_active", "disk" => self.disk.clone(), "class" => "background").set(
            if self.background_active.load(Ordering::Acquire) {
                1.0
            } else {
                0.0
            },
        );
        gauge!("rustfs_disk_io_admission_waiters", "disk" => self.disk.clone(), "class" => "background")
            .set(f64::from(background_waiters));
    }
}

struct BackgroundWaiter {
    admission: Arc<DiskIoAdmission>,
    ticket: u64,
    waiting: bool,
}

impl Drop for BackgroundWaiter {
    fn drop(&mut self) {
        if !self.waiting {
            return;
        }
        let mut state = self.admission.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(index) = state.background_queue.iter().position(|ticket| *ticket == self.ticket) {
            state.background_queue.remove(index);
            if index == 0 {
                state.background_front_since =
                    if state.background_queue.is_empty() || self.admission.background_active.load(Ordering::Acquire) {
                        None
                    } else {
                        Some(Instant::now())
                    };
            }
            self.admission.background_waiters.fetch_sub(1, Ordering::AcqRel);
        }
        drop(state);
        self.admission.record_state();
        self.admission.changed.notify_waiters();
    }
}

#[derive(Debug, Clone)]
pub(crate) struct DiskIoPermit {
    lease: Arc<DiskIoPermitLease>,
}

impl DiskIoPermit {
    pub(crate) fn is_for(&self, admission: &Arc<DiskIoAdmission>) -> bool {
        Arc::ptr_eq(&self.lease.admission, admission)
    }

    fn admission(&self) -> Arc<DiskIoAdmission> {
        self.lease.admission.clone()
    }
}

#[derive(Debug)]
struct DiskIoPermitLease {
    admission: Arc<DiskIoAdmission>,
    class: DiskIoClass,
}

pub(crate) fn spawn_blocking_with_disk_io_permit<T, F>(
    io_permit: Option<DiskIoPermit>,
    operation: F,
) -> tokio::task::JoinHandle<T>
where
    T: Send + 'static,
    F: FnOnce() -> T + Send + 'static,
{
    tokio::task::spawn_blocking(move || {
        let _io_permit = io_permit;
        operation()
    })
}

type AdmissionWait = Pin<Box<dyn Future<Output = io::Result<DiskIoPermit>> + Send>>;

impl Drop for DiskIoPermitLease {
    fn drop(&mut self) {
        match self.class {
            DiskIoClass::Foreground => {
                self.admission.foreground_active.fetch_sub(1, Ordering::AcqRel);
                let record_state = self.admission.background_waiters.load(Ordering::Acquire) > 0;
                if record_state {
                    let mut state = self.admission.state.lock().unwrap_or_else(|error| error.into_inner());
                    if !state.background_queue.is_empty() {
                        state.foreground_completions = state.foreground_completions.saturating_add(1);
                    }
                }
                if record_state {
                    self.admission.record_state();
                    self.admission.changed.notify_waiters();
                }
            }
            DiskIoClass::Background => {
                let mut state = self.admission.state.lock().unwrap_or_else(|error| error.into_inner());
                self.admission.background_active.store(false, Ordering::Release);
                state.background_front_since = if state.background_queue.is_empty() {
                    None
                } else {
                    Some(Instant::now())
                };
                drop(state);
                self.admission.record_state();
                self.admission.changed.notify_waiters();
            }
        }
    }
}

pub(crate) struct AdmissionReader<R> {
    inner: R,
    admission: Arc<DiskIoAdmission>,
    context: DiskIoContext,
    wait: Mutex<Option<AdmissionWait>>,
    permit: Option<DiskIoPermit>,
}

impl<R> AdmissionReader<R> {
    pub(crate) fn new(inner: R, admission: Arc<DiskIoAdmission>, context: DiskIoContext) -> Self {
        Self {
            inner,
            admission,
            context,
            wait: Mutex::new(None),
            permit: None,
        }
    }

    fn poll_permit(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.permit.is_some() {
            return Poll::Ready(Ok(()));
        }

        if self.context.class == DiskIoClass::Foreground {
            self.permit = Some(self.admission.foreground());
            return Poll::Ready(Ok(()));
        }

        let mut wait = self.wait.lock().unwrap_or_else(|error| error.into_inner());
        if wait.is_none() {
            let admission = self.admission.clone();
            let cancellation = self.context.cancellation.clone();
            *wait = Some(Box::pin(async move { admission.background(&cancellation).await }));
        }
        let Some(wait_future) = wait.as_mut() else {
            return Poll::Ready(Err(io::Error::other("background disk I/O admission waiter is missing")));
        };
        let result = wait_future.as_mut().poll(cx);
        match result {
            Poll::Ready(Ok(permit)) => {
                wait.take();
                drop(wait);
                self.permit = Some(permit);
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => {
                wait.take();
                Poll::Ready(Err(error))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn finish_poll<T>(&mut self, result: Poll<io::Result<T>>) -> Poll<io::Result<T>> {
        if result.is_ready() {
            self.permit = None;
        }
        result
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for AdmissionReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        match self.poll_permit(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        let result = Pin::new(&mut self.inner).poll_read(cx, buf);
        self.finish_poll(result)
    }
}

impl<R: ChunkReader + Unpin> ChunkReader for AdmissionReader<R> {
    fn poll_read_chunk(mut self: Pin<&mut Self>, cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<bytes::Bytes>>> {
        match self.poll_permit(cx) {
            Poll::Ready(Ok(())) => {}
            Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
            Poll::Pending => return Poll::Pending,
        }
        let result = Pin::new(&mut self.inner).poll_read_chunk(cx, max);
        self.finish_poll(result)
    }
}

pub(crate) struct BoxedChunkReader(pub(crate) rustfs_rio::ChunkReaderBox);

impl AsyncRead for BoxedChunkReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl ChunkReader for BoxedChunkReader {
    fn poll_read_chunk(self: Pin<&mut Self>, cx: &mut Context<'_>, max: usize) -> Poll<io::Result<Option<bytes::Bytes>>> {
        Pin::new(self.get_mut().0.as_mut()).poll_read_chunk(cx, max)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    struct BlockingWorker(tokio::task::JoinHandle<()>);

    async fn wait_for_background_waiters(admission: &DiskIoAdmission, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let waiters = admission
                    .state
                    .lock()
                    .unwrap_or_else(|error| error.into_inner())
                    .background_queue
                    .len();
                if waiters == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("background admission waiter should reach expected count");
    }

    async fn wait_for_foreground_active(admission: &DiskIoAdmission, expected: usize) {
        tokio::time::timeout(Duration::from_secs(1), async {
            loop {
                let active = admission.foreground_active.load(Ordering::Acquire);
                if active == expected {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("foreground disk read should retain its permit while pending");
    }

    #[tokio::test]
    async fn foreground_bypasses_background_and_background_progress_is_bounded() {
        let admission = DiskIoAdmission::new("disk-a");
        let foreground = admission.foreground();
        let background_admission = admission.clone();
        let cancellation = CancellationToken::new();
        let background = tokio::spawn(async move { background_admission.background(&cancellation).await });

        wait_for_background_waiters(&admission, 1).await;
        for _ in 0..BACKGROUND_FOREGROUND_BURST {
            drop(admission.foreground());
        }

        let background_permit = tokio::time::timeout(Duration::from_secs(1), background)
            .await
            .expect("background admission should progress after the foreground burst")
            .expect("background waiter task should complete")
            .expect("background admission should not be cancelled");
        assert_eq!(
            admission.foreground_active.load(Ordering::Acquire),
            1,
            "foreground operations remain admitted while one background operation runs"
        );
        drop(background_permit);
        drop(foreground);
    }

    #[tokio::test]
    async fn cancellation_removes_a_queued_background_waiter() {
        let admission = DiskIoAdmission::new("disk-a");
        let background = admission
            .background(&CancellationToken::new())
            .await
            .expect("first background operation should be admitted");
        let cancellation = CancellationToken::new();
        let waiter_admission = admission.clone();
        let waiter_cancellation = cancellation.clone();
        let waiter = tokio::spawn(async move { waiter_admission.background(&waiter_cancellation).await });
        wait_for_background_waiters(&admission, 1).await;

        cancellation.cancel();
        let error = tokio::time::timeout(Duration::from_secs(1), waiter)
            .await
            .expect("cancelled background waiter should stop")
            .expect("background waiter task should complete")
            .expect_err("cancelled admission must not start disk I/O");
        assert_eq!(error.kind(), io::ErrorKind::Interrupted);
        assert_eq!(
            admission
                .state
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .background_queue
                .len(),
            0
        );
        drop(background);
    }

    #[tokio::test]
    async fn background_admission_is_isolated_per_disk() {
        let disk_a = DiskIoAdmission::new("disk-a");
        let disk_b = DiskIoAdmission::new("disk-b");
        let _disk_a_permit = disk_a
            .background(&CancellationToken::new())
            .await
            .expect("disk A background operation should be admitted");
        let _disk_b_permit = tokio::time::timeout(Duration::from_secs(1), disk_b.background(&CancellationToken::new()))
            .await
            .expect("disk A must not block disk B")
            .expect("disk B background operation should be admitted");
    }

    #[tokio::test]
    async fn streamed_read_holds_admission_until_disk_read_resolves() {
        let admission = DiskIoAdmission::new("disk-a");
        let (mut writer, reader) = tokio::io::duplex(8);
        let mut reader = AdmissionReader::new(
            reader,
            admission.clone(),
            DiskIoContext {
                class: DiskIoClass::Foreground,
                cancellation: CancellationToken::new(),
                admission: None,
                permit: None,
            },
        );
        let read = tokio::spawn(async move {
            let mut bytes = [0; 4];
            reader
                .read_exact(&mut bytes)
                .await
                .expect("streamed disk read should complete");
            bytes
        });

        wait_for_foreground_active(&admission, 1).await;
        writer.write_all(b"rust").await.expect("test bytes should be written");
        assert_eq!(read.await.expect("reader task should finish"), *b"rust");
        wait_for_foreground_active(&admission, 0).await;
    }

    #[tokio::test]
    async fn blocking_worker_keeps_permit_after_caller_timeout() {
        let admission = DiskIoAdmission::new("disk-a");
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let worker = spawn_blocking_with_disk_io_permit(Some(admission.foreground()), move || {
            started_tx.send(()).expect("worker start should be observed");
            std::thread::sleep(Duration::from_millis(100));
        });

        started_rx.await.expect("blocking worker should start");
        assert!(tokio::time::timeout(Duration::from_millis(10), worker).await.is_err());
        assert_eq!(admission.foreground_active_for_tests(), 1);
        wait_for_foreground_active(&admission, 0).await;
    }

    #[tokio::test]
    async fn cloned_permit_remains_active_until_all_workers_finish() {
        let admission = DiskIoAdmission::new("disk-a");
        let permit = admission.foreground();
        let worker_permit = permit.clone();
        drop(permit);
        assert_eq!(admission.foreground_active_for_tests(), 1);

        drop(worker_permit);
        assert_eq!(admission.foreground_active_for_tests(), 0);
    }

    #[tokio::test]
    async fn scoped_foreground_permit_is_inherited_by_blocking_worker() {
        let admission = DiskIoAdmission::new("disk-a");
        let permit = admission.foreground();
        let worker = with_disk_io_permit(Some(permit), async {
            let worker_permit = acquire_context_disk_io_permit()
                .await
                .expect("context permit should be available")
                .expect("scoped permit should be inherited");
            let (started_tx, started_rx) = tokio::sync::oneshot::channel();
            let worker = spawn_blocking_with_disk_io_permit(Some(worker_permit), move || {
                started_tx.send(()).expect("worker start should be observed");
                std::thread::sleep(Duration::from_millis(100));
            });
            started_rx.await.expect("blocking worker should start");
            BlockingWorker(worker)
        })
        .await;

        assert_eq!(admission.foreground_active_for_tests(), 1);
        assert!(tokio::time::timeout(Duration::from_millis(10), worker.0).await.is_err());
        wait_for_foreground_active(&admission, 0).await;
    }
}
