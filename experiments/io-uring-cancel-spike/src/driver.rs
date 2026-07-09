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

use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io;
use std::os::fd::AsRawFd;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::Duration;

use io_uring::{IoUring, opcode, types};
use tokio::sync::oneshot;

/// user_data bit marking the CQE of an `AsyncCancel` SQE itself (as opposed
/// to the CQE of the read op it targets).
const CANCEL_BIT: u64 = 1 << 63;

/// `offset` value meaning "use the file's current position" (read(2)
/// semantics); required for pipes/sockets where pread returns ESPIPE.
const CURRENT_POSITION: u64 = u64::MAX;

/// Kernel single-read cap: `MAX_RW_COUNT = INT_MAX & PAGE_MASK` (2 GiB − 4 KiB
/// on 4 KiB pages). io_uring's READ length field is a u32, and any request
/// above this short-reads. We reject beyond it in `submit` so a `len as u32`
/// truncation can never silently turn a huge read into a 0-byte "EOF" (C6,
/// rustfs/backlog#1057); P2 must chunk reads larger than this.
const MAX_READ_LEN: usize = 0x7fff_f000;

/// Why the probe refused to start the io_uring driver.
///
/// Mirrors the P2 degradation contract (backlog#894): a restricted
/// environment must be recognized and answered with a silent fallback to the
/// std backend, never surfaced to callers.
#[derive(Debug)]
pub enum ProbeFailure {
    /// `io_uring_setup` itself failed (seccomp/gVisor/old kernel).
    Setup(io::Error),
    /// The ring was created but a real `IORING_OP_READ` did not complete
    /// correctly (gVisor accepts setup but fails ops; also covers silent
    /// data corruption, which we treat as "unusable").
    ReadOp(io::Error),
}

impl ProbeFailure {
    /// True when the **probe-time** errno belongs to the "expected
    /// restriction" class that P2 maps to permanent per-disk fallback:
    /// EACCES/EPERM/ENOSYS/EINVAL/EOPNOTSUPP. Anything else is a genuine bug
    /// worth surfacing.
    ///
    /// IMPORTANT (C7, rustfs/backlog#1059): this classification is valid ONLY
    /// for a one-shot startup probe, where these errnos unambiguously mean
    /// "io_uring is unusable here" (gVisor/seccomp/old kernel). Runtime
    /// per-op errnos have different semantics and MUST NOT reuse this class.
    /// In particular EINVAL is triple-meaning at runtime — offset > i64::MAX
    /// (signed loff_t), O_DIRECT buffer/offset/len misalignment (P2 will use
    /// O_DIRECT), and setup `entries` over the cap — none of which imply the
    /// disk should be permanently degraded off io_uring. P2's degradation
    /// contract must split errnos into three classes:
    ///
    ///   * probe-time restriction  -> degrade this disk to the std backend;
    ///   * runtime parameter error -> return the error to the caller (and,
    ///     for a suspected bug, re-verify once via std pread) — never latch;
    ///   * transient (EINTR/EAGAIN) -> retry, never surface.
    ///
    /// See `submit` for the offset guard that keeps a caller arithmetic bug
    /// from ever reaching the kernel as a runtime EINVAL.
    pub fn is_expected_restriction(&self) -> bool {
        let err = match self {
            ProbeFailure::Setup(e) | ProbeFailure::ReadOp(e) => e,
        };
        matches!(
            err.raw_os_error(),
            Some(libc::EACCES) | Some(libc::EPERM) | Some(libc::ENOSYS) | Some(libc::EINVAL) | Some(libc::EOPNOTSUPP)
        )
    }
}

#[derive(Default)]
struct DriverStats {
    submitted: AtomicU64,
    delivered: AtomicU64,
    orphan_reclaimed: AtomicU64,
    in_flight: AtomicU64,
}

/// Point-in-time copy of the driver counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsSnapshot {
    /// Read ops handed to the kernel.
    pub submitted: u64,
    /// CQEs whose result was received by a live caller.
    pub delivered: u64,
    /// CQEs whose caller had dropped the future: the buffer stayed in the
    /// pending table the whole time and was reclaimed here, at the CQE.
    pub orphan_reclaimed: u64,
    /// Ops submitted but not yet completed. The kernel may still write into
    /// their buffers.
    pub in_flight: u64,
}

enum Msg {
    Read {
        id: u64,
        file: Arc<File>,
        offset: u64,
        len: usize,
        done: oneshot::Sender<io::Result<Vec<u8>>>,
    },
    Cancel {
        id: u64,
    },
    Shutdown,
}

/// One in-flight read. This struct — not the caller — owns everything the
/// kernel touches:
///
/// - `buf`: the destination buffer. Its heap allocation must stay put until
///   the CQE; the `Vec` itself may move (HashMap rehash) since that never
///   relocates the heap block. It is never read, resized, or dropped before
///   the CQE handler removes this entry.
/// - `_file`: keeps the fd open even if every caller-side clone is dropped.
///   Without this, dropping the future could close the fd mid-read and a
///   recycled fd number would receive the kernel's write (spike finding:
///   the orphan table must own the file handle, not just the buffer).
struct Pending {
    buf: Vec<u8>,
    _file: Arc<File>,
    done: Option<oneshot::Sender<io::Result<Vec<u8>>>>,
}

/// Handle to a submitted read. Await it for the result.
///
/// Dropping it before completion abandons the result only; by default it
/// also submits `IORING_OP_ASYNC_CANCEL` (best effort) so the CQE — and with
/// it the buffer reclamation — arrives sooner. `without_cancel_on_drop`
/// disables that to model the bare "quorum drops the future" case.
pub struct ReadHandle {
    id: u64,
    rx: oneshot::Receiver<io::Result<Vec<u8>>>,
    tx: mpsc::Sender<Msg>,
    finished: bool,
    cancel_on_drop: bool,
}

impl ReadHandle {
    pub fn without_cancel_on_drop(mut self) -> Self {
        self.cancel_on_drop = false;
        self
    }
}

impl Future for ReadHandle {
    type Output = io::Result<Vec<u8>>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match Pin::new(&mut self.rx).poll(cx) {
            Poll::Ready(res) => {
                self.finished = true;
                Poll::Ready(match res {
                    Ok(inner) => inner,
                    Err(_) => Err(io::Error::other("uring driver shut down before completion")),
                })
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for ReadHandle {
    fn drop(&mut self) {
        // The buffer is deliberately NOT touched here: the driver owns it
        // until the CQE. All we may do is ask the kernel to hurry up.
        if !self.finished && self.cancel_on_drop {
            let _ = self.tx.send(Msg::Cancel { id: self.id });
        }
    }
}

/// Process-level io_uring driver: one ring, one driver thread.
pub struct UringDriver {
    tx: mpsc::Sender<Msg>,
    handle: Option<JoinHandle<()>>,
    stats: Arc<DriverStats>,
    next_id: AtomicU64,
}

impl UringDriver {
    /// Create the ring AND verify a real `IORING_OP_READ` round-trip on a
    /// temp file before accepting work. `io_uring_setup` succeeding is not
    /// enough: gVisor/seccomp environments can create a ring whose ops then
    /// fail with ENOSYS/EINVAL (backlog#894 probe design).
    pub fn probe_and_start(entries: u32) -> Result<Self, ProbeFailure> {
        let mut ring = IoUring::new(entries).map_err(ProbeFailure::Setup)?;
        probe_real_read(&mut ring).map_err(ProbeFailure::ReadOp)?;

        let (tx, rx) = mpsc::channel();
        let stats = Arc::new(DriverStats::default());
        let thread_stats = Arc::clone(&stats);
        let handle = std::thread::Builder::new()
            .name("uring-spike-driver".into())
            .spawn(move || drive(ring, rx, thread_stats))
            .expect("spawn driver thread");

        Ok(Self {
            tx,
            handle: Some(handle),
            stats,
            next_id: AtomicU64::new(1),
        })
    }

    /// Positioned read (pread semantics) — regular files.
    pub fn read_at(&self, file: Arc<File>, offset: u64, len: usize) -> ReadHandle {
        assert_ne!(offset, CURRENT_POSITION, "offset u64::MAX is reserved");
        self.submit(file, offset, len)
    }

    /// Read at the file's current position (read(2) semantics) — pipes.
    pub fn read_current(&self, file: Arc<File>, len: usize) -> ReadHandle {
        self.submit(file, CURRENT_POSITION, len)
    }

    fn submit(&self, file: Arc<File>, offset: u64, len: usize) -> ReadHandle {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        assert_eq!(id & CANCEL_BIT, 0, "op id overflowed into the cancel bit");
        let (done, rx) = oneshot::channel();

        // Reject an offset the kernel would answer with a runtime EINVAL that
        // must NOT be mistaken for an environment restriction (C7,
        // rustfs/backlog#1059). The kernel reads `off` as a signed loff_t, so
        // offset > i64::MAX becomes a negative ki_pos → EINVAL. A caller
        // offset-arithmetic bug has to surface as an error here, never as a
        // permanent per-disk fallback. CURRENT_POSITION is the reserved
        // read(2) sentinel and bypasses this check.
        if offset != CURRENT_POSITION && offset > i64::MAX as u64 {
            let _ = done.send(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "offset exceeds i64::MAX (kernel loff_t is signed)",
            )));
            return ReadHandle {
                id,
                rx,
                tx: self.tx.clone(),
                finished: false,
                cancel_on_drop: false,
            };
        }

        // Reject a length the kernel would short-read past MAX_RW_COUNT and
        // that the SQE's u32 `len` field would silently truncate: len == 2^32
        // becomes a 0-byte read the caller decodes as a false EOF (C6,
        // rustfs/backlog#1057). Failing fast here also removes the caller-
        // controlled `vec![0u8; len]` capacity-overflow panic that made the
        // unwind-UAF (rustfs/backlog#1054) reachable. P2 must chunk instead.
        if len > MAX_READ_LEN {
            let _ = done.send(Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read length exceeds MAX_RW_COUNT (2 GiB - 4 KiB); caller must chunk",
            )));
            return ReadHandle {
                id,
                rx,
                tx: self.tx.clone(),
                finished: false,
                cancel_on_drop: false,
            };
        }

        // If the driver already shut down, the send fails and `done` is
        // dropped, which the caller observes as a driver-gone error.
        let _ = self.tx.send(Msg::Read {
            id,
            file,
            offset,
            len,
            done,
        });
        ReadHandle {
            id,
            rx,
            tx: self.tx.clone(),
            finished: false,
            cancel_on_drop: true,
        }
    }

    pub fn stats(&self) -> StatsSnapshot {
        StatsSnapshot {
            submitted: self.stats.submitted.load(Ordering::SeqCst),
            delivered: self.stats.delivered.load(Ordering::SeqCst),
            orphan_reclaimed: self.stats.orphan_reclaimed.load(Ordering::SeqCst),
            in_flight: self.stats.in_flight.load(Ordering::SeqCst),
        }
    }

    /// Stop accepting work, cancel all in-flight ops, drain the ring to
    /// `in_flight == 0`, then join the driver thread. Only after that is the
    /// ring dropped/unmapped — the shutdown ordering P2 requires.
    pub fn shutdown(mut self) -> StatsSnapshot {
        let _ = self.tx.send(Msg::Shutdown);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
        let snap = self.stats();
        assert_eq!(snap.in_flight, 0, "driver exited with ops still in flight");
        snap
    }
}

impl Drop for UringDriver {
    fn drop(&mut self) {
        if let Some(h) = self.handle.take() {
            let _ = self.tx.send(Msg::Shutdown);
            let _ = h.join();
        }
    }
}

fn probe_real_read(ring: &mut IoUring) -> io::Result<()> {
    static PROBE_SEQ: AtomicU64 = AtomicU64::new(0);
    let pattern: Vec<u8> = (0..512u32).map(|i| (i * 7 + 13) as u8).collect();
    let path = std::env::temp_dir().join(format!(
        "uring-spike-probe-{}-{}",
        std::process::id(),
        PROBE_SEQ.fetch_add(1, Ordering::Relaxed)
    ));

    // File setup runs BEFORE any SQE is submitted, so its errors early-return
    // safely — nothing is in flight yet.
    let file = match (|| -> io::Result<File> {
        std::fs::write(&path, &pattern)?;
        File::open(&path)
    })() {
        Ok(f) => f,
        Err(e) => {
            let _ = std::fs::remove_file(&path);
            return Err(e);
        }
    };

    let mut buf = vec![0u8; pattern.len()];
    let sqe = opcode::Read::new(types::Fd(file.as_raw_fd()), buf.as_mut_ptr(), buf.len() as u32)
        .offset(0)
        .build()
        .user_data(0xB0BE);

    // SAFETY: a push failure means the kernel never accepted the SQE, so
    // `buf`/`file` may be dropped safely on this early return.
    if unsafe { ring.submission().push(&sqe) }.is_err() {
        let _ = std::fs::remove_file(&path);
        return Err(io::Error::other("probe: submission queue full"));
    }

    // C1 (backlog#1053): once the SQE is handed to the kernel, the read may be
    // punted to io-wq and write into `buf` at ANY later point. Until its CQE
    // arrives, `buf`/`file` must NOT be dropped and the ring must NOT be
    // unmapped — otherwise the kernel writes into freed memory (UAF). The
    // probe path has no pending-table backstop, so we must drain to the CQE
    // here, and any early exit first leaks the buffer ("leak over UAF").
    let res = match drain_probe_cqe(ring) {
        Ok(res) => res,
        Err(e) => {
            // We could not confirm the op terminated. Leak `buf` (the actual
            // UAF hazard: the kernel may still write 512 bytes into it) and,
            // defensively, `file`. Leaking one 512-byte startup-probe buffer
            // is trivially cheaper than a silent heap corruption. The ring is
            // dropped by the caller's `?`; unmapping the ring is safe on its
            // own (closing the ring fd triggers kernel-side teardown) — only
            // the user buffer must survive.
            std::mem::forget(buf);
            std::mem::forget(file);
            let _ = std::fs::remove_file(&path);
            return Err(e);
        }
    };

    // The CQE has arrived: the kernel is done with `buf`, so dropping it and
    // `file` below is now safe.
    let outcome = if res < 0 {
        Err(io::Error::from_raw_os_error(-res))
    } else if res as usize != pattern.len() || buf != pattern {
        Err(io::Error::other("probe: read completed but data mismatched"))
    } else {
        Ok(())
    };
    let _ = std::fs::remove_file(&path);
    outcome
}

/// Wait for the probe SQE's CQE and return its raw result.
///
/// The SQE has already been pushed; this only drains it. `submit_and_wait`
/// interrupted by a signal returns EINTR — since the kernel consumed the SQE
/// atomically before the wait phase, we retry the WAIT only and never re-push
/// (C8, backlog#1059). A bounded attempt count keeps a probe that hit a hung
/// device from blocking forever; exhausting it returns an error that drives
/// the caller's leak-over-UAF fallback.
fn drain_probe_cqe(ring: &mut IoUring) -> io::Result<i32> {
    const MAX_WAIT_ATTEMPTS: u32 = 4096;
    for _ in 0..MAX_WAIT_ATTEMPTS {
        match ring.submit_and_wait(1) {
            Ok(_) => {}
            // Signal interrupted the wait; the SQE is already in flight, so
            // just wait again (do NOT re-push).
            Err(e) if e.raw_os_error() == Some(libc::EINTR) => {}
            Err(e) => return Err(e),
        }
        if let Some(cqe) = ring.completion().next() {
            return Ok(cqe.result());
        }
    }
    Err(io::Error::other("probe: no CQE after bounded wait"))
}

/// Owns everything the kernel can still be writing into: the ring, the
/// pending (orphan) table of in-flight buffers, and the SQE backlog.
///
/// C2 (rustfs/backlog#1054): the "CQE is the only reclamation point"
/// invariant holds only while the driver thread does NOT unwind. On a panic,
/// Rust would drop the pending table (freeing every in-flight buffer) while
/// the kernel may still write into them → mass UAF; reversing drop order does
/// not help because io_uring teardown on ring drop is asynchronous and does
/// not wait for in-flight ops. So this type's `Drop` refuses to run field
/// destructors during an unwind: it aborts the process first, leaving the
/// ring mapped and the buffers allocated (leak over UAF). A storage read path
/// silently corrupting memory is worse than a crash.
struct DriverState {
    ring: IoUring,
    pending: HashMap<u64, Pending>,
    backlog: VecDeque<io_uring::squeue::Entry>,
}

impl Drop for DriverState {
    fn drop(&mut self) {
        if std::thread::panicking() {
            // Abort BEFORE any field destructor runs: the ring stays mapped
            // and the in-flight buffers stay allocated, so the kernel can
            // never write into freed memory.
            eprintln!(
                "uring-spike driver thread panicked with {} ops in flight; \
                 aborting to avoid UAF of in-flight buffers",
                self.pending.len()
            );
            std::process::abort();
        }
        // Normal drop: the shutdown invariant guarantees pending/backlog are
        // empty and in_flight == 0, so unmapping the ring here is safe.
    }
}

fn drive(ring: IoUring, rx: mpsc::Receiver<Msg>, stats: Arc<DriverStats>) {
    let mut state = DriverState {
        ring,
        pending: HashMap::new(),
        backlog: VecDeque::new(),
    };
    let mut shutting_down = false;

    loop {
        // 1. Intake. Block (with timeout) only when fully idle; once ops are
        //    in flight we must keep reaping, so only try_recv.
        loop {
            let idle = state.pending.is_empty() && state.backlog.is_empty() && !shutting_down;
            let msg = if idle {
                match rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(m) => m,
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(RecvTimeoutError::Disconnected) => {
                        shutting_down = true;
                        break;
                    }
                }
            } else {
                match rx.try_recv() {
                    Ok(m) => m,
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        shutting_down = true;
                        break;
                    }
                }
            };
            match msg {
                Msg::Read {
                    id,
                    file,
                    offset,
                    len,
                    done,
                } => {
                    if shutting_down {
                        let _ = done.send(Err(io::Error::other("uring driver shutting down")));
                        continue;
                    }
                    let mut buf = vec![0u8; len];
                    // The raw pointer is captured before `buf` moves into the
                    // table; moving the Vec never relocates its heap block,
                    // and the entry is only removed at the CQE. `len as u32`
                    // is lossless: `submit` rejected anything > MAX_READ_LEN.
                    let sqe = opcode::Read::new(types::Fd(file.as_raw_fd()), buf.as_mut_ptr(), len as u32)
                        .offset(offset)
                        .build()
                        .user_data(id);
                    state.pending.insert(
                        id,
                        Pending {
                            buf,
                            _file: file,
                            done: Some(done),
                        },
                    );
                    stats.submitted.fetch_add(1, Ordering::SeqCst);
                    stats.in_flight.fetch_add(1, Ordering::SeqCst);
                    state.backlog.push_back(sqe);
                }
                Msg::Cancel { id } => {
                    if state.pending.contains_key(&id) {
                        state
                            .backlog
                            .push_back(opcode::AsyncCancel::new(id).build().user_data(id | CANCEL_BIT));
                    }
                }
                Msg::Shutdown => {
                    shutting_down = true;
                    for id in state.pending.keys() {
                        state
                            .backlog
                            .push_back(opcode::AsyncCancel::new(*id).build().user_data(*id | CANCEL_BIT));
                    }
                }
            }
        }

        // 2. Push backlog into the SQ (stop when full; retry next turn).
        {
            let mut sq = state.ring.submission();
            while let Some(sqe) = state.backlog.pop_front() {
                // SAFETY: read SQEs point into `pending`-owned buffers that
                // live until their CQE; cancel SQEs carry no pointers.
                if unsafe { sq.push(&sqe) }.is_err() {
                    state.backlog.push_front(sqe);
                    break;
                }
            }
        }
        if state.ring.submit().is_err() {
            // EINTR and friends: retry on the next loop turn.
        }

        // 3. Reap. This is the ONLY place a Pending entry (and thus its
        //    buffer) is dropped.
        while let Some(cqe) = state.ring.completion().next() {
            let ud = cqe.user_data();
            if ud & CANCEL_BIT != 0 {
                // Result of the AsyncCancel op itself; the read's own CQE
                // (ECANCELED or success) still arrives separately.
                continue;
            }
            let res = cqe.result();
            let Some(mut p) = state.pending.remove(&ud) else {
                continue;
            };
            let outcome = if res < 0 {
                Err(io::Error::from_raw_os_error(-res))
            } else {
                p.buf.truncate(res as usize);
                Ok(std::mem::take(&mut p.buf))
            };
            match p.done.take().expect("done sender set at submit").send(outcome) {
                Ok(()) => stats.delivered.fetch_add(1, Ordering::SeqCst),
                // Caller dropped the future: the buffer survived in the
                // table until this very CQE and is reclaimed here.
                Err(_) => stats.orphan_reclaimed.fetch_add(1, Ordering::SeqCst),
            };
            stats.in_flight.fetch_sub(1, Ordering::SeqCst);
        }

        // 4. Exit only when drained: kernel no longer references any buffer,
        //    so dropping the ring (unmap) is safe.
        if shutting_down && state.pending.is_empty() && state.backlog.is_empty() {
            return;
        }

        // Spike-grade pacing: production replaces this poll with eventfd +
        // tokio AsyncFd reaping (backlog#894).
        if !state.pending.is_empty() {
            std::thread::sleep(Duration::from_micros(200));
        }
    }
}
