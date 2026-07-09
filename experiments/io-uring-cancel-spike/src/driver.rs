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
use std::io::Write as _;
use std::os::fd::{AsRawFd, FromRawFd};
use std::os::unix::ffi::OsStrExt;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, TryRecvError};
use std::sync::{Arc, Condvar, Mutex};
use std::task::{Context, Poll};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use io_uring::{IoUring, opcode, types};

/// Upper bound on how long shutdown waits for in-flight ops to drain before
/// leaking the ring+buffers and exiting (C4, rustfs/backlog#1055). ASYNC_CANCEL
/// cannot interrupt an in-execution regular-file read on a D-state/NFS-hung
/// disk, so drain-to-zero can be non-terminating; this bounds it.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(5);
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

/// Submission-side backpressure (C10, rustfs/backlog#1060).
///
/// Bounds in-flight ops so the count can never exceed CQ capacity, and — the
/// load-bearing part — releases a permit at the CQE (pending-table removal),
/// NOT at future drop. Tying a permit to the future (the natural RAII shape)
/// would let a quorum dropping many futures return permits while their orphan
/// buffers still sit in the pending table awaiting slow-disk CQEs, decoupling
/// the permit count from resident memory and reopening the memory-DoS surface.
/// P2 uses a tokio `Semaphore` with `acquire_owned`; the RELEASE POINT is what
/// matters and must stay at the CQE.
struct Backpressure {
    state: Mutex<BpState>,
    cv: Condvar,
}

struct BpState {
    permits: usize,
    shutdown: bool,
}

impl Backpressure {
    fn new(permits: usize) -> Self {
        Self {
            state: Mutex::new(BpState {
                permits,
                shutdown: false,
            }),
            cv: Condvar::new(),
        }
    }

    /// Block until a permit is free. Returns false if the driver has shut down
    /// (so submit stops blocking forever once the driver is gone).
    fn acquire(&self) -> bool {
        let mut g = self.state.lock().expect("backpressure mutex poisoned");
        loop {
            if g.shutdown {
                return false;
            }
            if g.permits > 0 {
                g.permits -= 1;
                return true;
            }
            g = self.cv.wait(g).expect("backpressure mutex poisoned");
        }
    }

    fn release(&self) {
        let mut g = self.state.lock().expect("backpressure mutex poisoned");
        g.permits += 1;
        self.cv.notify_one();
    }

    fn shutdown(&self) {
        let mut g = self.state.lock().expect("backpressure mutex poisoned");
        g.shutdown = true;
        self.cv.notify_all();
    }
}

#[derive(Default)]
struct DriverStats {
    submitted: AtomicU64,
    delivered: AtomicU64,
    orphan_reclaimed: AtomicU64,
    in_flight: AtomicU64,
    cancel_succeeded: AtomicU64,
    cancel_not_found: AtomicU64,
    cancel_already: AtomicU64,
    cq_overflow: AtomicU64,
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
    /// ASYNC_CANCEL CQEs that reported the target op was canceled (res == 0).
    pub cancel_succeeded: u64,
    /// ASYNC_CANCEL CQEs that reported the target was not found (-ENOENT):
    /// the op had already completed.
    pub cancel_not_found: u64,
    /// ASYNC_CANCEL CQEs that reported the target was already executing and
    /// could not be interrupted (-EALREADY). A rising count is the hung-disk
    /// signal that makes drain-to-zero non-terminating (C4,
    /// rustfs/backlog#1055).
    pub cancel_already: u64,
    /// Kernel CQ-ring overflow counter. MUST stay 0: a non-zero value means
    /// CQEs were lost, so their pending entries are never reclaimed and drain
    /// never completes. Treated as fatal (C5, rustfs/backlog#1056).
    pub cq_overflow: u64,
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

/// One in-flight LOGICAL read. This struct — not the caller — owns everything
/// the kernel touches:
///
/// - `buf`: the destination buffer. Its heap allocation must stay put until
///   the final CQE; the `Vec` itself may move (HashMap rehash) since that
///   never relocates the heap block. It is never resized or dropped before
///   the CQE handler removes this entry.
/// - `file`: keeps the fd open even if every caller-side clone is dropped, and
///   supplies the fd for short-read resubmission. Without it, dropping the
///   future could close the fd while an SQE built from that fd still sits in
///   the backlog (SQE construction → io_uring_enter window), and a recycled
///   fd number would make the kernel read the WRONG file (spike finding, with
///   the corrected mechanism per rustfs/backlog#1063).
/// - `offset`/`nread`: track a short-read resubmit loop (C9,
///   rustfs/backlog#1058). io_uring may legally short-read a regular file;
///   the driver resubmits the remainder into `buf[nread..]` until the request
///   is fully satisfied or a real EOF (res == 0) is seen, so reclamation
///   happens only at the FINAL CQE of the logical read.
struct Pending {
    buf: Vec<u8>,
    file: Arc<File>,
    done: Option<oneshot::Sender<io::Result<Vec<u8>>>>,
    offset: u64,
    nread: usize,
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
    bp: Arc<Backpressure>,
}

impl UringDriver {
    /// Create the ring AND verify a real `IORING_OP_READ` round-trip on a
    /// temp file before accepting work. `io_uring_setup` succeeding is not
    /// enough: gVisor/seccomp environments can create a ring whose ops then
    /// fail with ENOSYS/EINVAL (backlog#894 probe design).
    pub fn probe_and_start(entries: u32) -> Result<Self, ProbeFailure> {
        let mut ring = IoUring::new(entries).map_err(ProbeFailure::Setup)?;
        // Require the NODROP feature (kernel >= 5.5). Without it, CQ overflow
        // silently drops CQEs, stranding pending entries forever and hanging
        // shutdown (C5, rustfs/backlog#1056). ENOSYS is in the expected-
        // restriction class, so this degrades to the std backend cleanly.
        if !ring.params().is_feature_nodrop() {
            return Err(ProbeFailure::Setup(io::Error::from_raw_os_error(libc::ENOSYS)));
        }
        probe_real_read(&mut ring).map_err(ProbeFailure::ReadOp)?;

        let (tx, rx) = mpsc::channel();
        let stats = Arc::new(DriverStats::default());
        let thread_stats = Arc::clone(&stats);
        // Cap in-flight at the SQ depth (entries), which is < CQ capacity
        // (2*entries), so CQ overflow is structurally unreachable (C5/C10).
        let bp = Arc::new(Backpressure::new(entries as usize));
        let thread_bp = Arc::clone(&bp);
        let handle = std::thread::Builder::new()
            .name("uring-spike-driver".into())
            .spawn(move || drive(ring, rx, thread_stats, thread_bp))
            .expect("spawn driver thread");

        Ok(Self {
            tx,
            handle: Some(handle),
            stats,
            next_id: AtomicU64::new(1),
            bp,
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

        // Acquire a backpressure permit BEFORE handing the op to the driver;
        // the driver releases it at the CQE (C10, rustfs/backlog#1060). This
        // blocks the caller once `entries` ops are in flight, bounding both
        // in-flight count (< CQ capacity) and resident buffer memory.
        if !self.bp.acquire() {
            let _ = done.send(Err(io::Error::other("uring driver shut down")));
            return ReadHandle {
                id,
                rx,
                tx: self.tx.clone(),
                finished: false,
                cancel_on_drop: false,
            };
        }

        // If the driver shut down between acquire and send, give the permit
        // back — no CQE will ever release it. `done` is dropped with the
        // returned Msg, which the caller observes as a driver-gone error.
        if self
            .tx
            .send(Msg::Read {
                id,
                file,
                offset,
                len,
                done,
            })
            .is_err()
        {
            self.bp.release();
            return ReadHandle {
                id,
                rx,
                tx: self.tx.clone(),
                finished: false,
                cancel_on_drop: false,
            };
        }
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
            cancel_succeeded: self.stats.cancel_succeeded.load(Ordering::SeqCst),
            cancel_not_found: self.stats.cancel_not_found.load(Ordering::SeqCst),
            cancel_already: self.stats.cancel_already.load(Ordering::SeqCst),
            cq_overflow: self.stats.cq_overflow.load(Ordering::SeqCst),
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
        // A clean drain leaves in_flight == 0. A non-zero count here means the
        // bounded drain bailed out on a hung device and leaked the ring+buffers
        // to stay memory-safe (C4, rustfs/backlog#1055) — a degraded but safe
        // outcome, not a panic. Callers/tests that require a clean drain assert
        // on the returned snapshot themselves.
        if snap.in_flight != 0 {
            eprintln!(
                "uring-spike shutdown: {} ops still in flight (bounded-drain bailout on a hung device)",
                snap.in_flight
            );
        }
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
    let pattern: Vec<u8> = (0..512u32).map(|i| (i * 7 + 13) as u8).collect();

    // Open an anonymous probe file seeded with the pattern. File setup runs
    // BEFORE any SQE, so its errors early-return safely — nothing is in flight.
    let file = open_probe_file(&pattern)?;

    let mut buf = vec![0u8; pattern.len()];
    let sqe = opcode::Read::new(types::Fd(file.as_raw_fd()), buf.as_mut_ptr(), buf.len() as u32)
        .offset(0)
        .build()
        .user_data(0xB0BE);

    // SAFETY: a push failure means the kernel never accepted the SQE, so
    // `buf`/`file` may be dropped safely on this early return.
    if unsafe { ring.submission().push(&sqe) }.is_err() {
        return Err(io::Error::other("probe: submission queue full"));
    }

    // C1 (rustfs/backlog#1053): once the SQE is handed to the kernel, the read
    // may be punted to io-wq and write into `buf` at ANY later point. Until its
    // CQE arrives, `buf`/`file` must NOT be dropped and the ring must NOT be
    // unmapped — otherwise the kernel writes into freed memory (UAF). The probe
    // path has no pending-table backstop, so we must drain to the CQE here, and
    // any early exit first leaks the buffer ("leak over UAF").
    let res = match drain_probe_cqe(ring) {
        Ok(res) => res,
        Err(e) => {
            // Could not confirm the op terminated: leak `buf` (the real UAF
            // hazard — the kernel may still write 512 bytes into it) and,
            // defensively, `file`. Leaking one 512-byte startup-probe buffer is
            // trivially cheaper than a silent heap corruption.
            std::mem::forget(buf);
            std::mem::forget(file);
            return Err(e);
        }
    };

    // The CQE has arrived: the kernel is done with `buf`, so dropping it and
    // `file` below is now safe.
    if res < 0 {
        Err(io::Error::from_raw_os_error(-res))
    } else if res as usize != pattern.len() || buf != pattern {
        Err(io::Error::other("probe: read completed but data mismatched"))
    } else {
        Ok(())
    }
}

/// Open a probe file seeded with `pattern`, avoiding the symlink/TOCTOU/
/// leftover hazards of a predictable temp path (C3, rustfs/backlog#1061).
///
/// Primary: `O_TMPFILE` — an anonymous inode with no name at all, so there is
/// nothing for an attacker to pre-plant a symlink at, no TOCTOU window, and no
/// leftover file. Fallback (filesystems without O_TMPFILE): create in the temp
/// dir with `O_CREAT|O_EXCL|O_NOFOLLOW` + 0600 + a per-process nonce, then
/// unlink immediately so no attacker-planted symlink is followed and no named
/// file survives.
fn open_probe_file(pattern: &[u8]) -> io::Result<File> {
    let dir = std::env::temp_dir();
    let c_dir = std::ffi::CString::new(dir.as_os_str().as_bytes()).map_err(|_| io::Error::other("probe dir path has NUL"))?;
    // SAFETY: `c_dir` is a valid NUL-terminated path; O_TMPFILE requires a
    // directory and O_RDWR/O_WRONLY. On success we own the returned fd.
    let fd = unsafe { libc::open(c_dir.as_ptr(), libc::O_TMPFILE | libc::O_RDWR | libc::O_CLOEXEC, 0o600) };
    if fd >= 0 {
        let mut file = unsafe { File::from_raw_fd(fd) };
        file.write_all(pattern)?;
        return Ok(file);
    }
    open_probe_file_exclusive(&dir, pattern)
}

fn open_probe_file_exclusive(dir: &std::path::Path, pattern: &[u8]) -> io::Result<File> {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let nonce = SEQ.fetch_add(1, Ordering::Relaxed);
    let path = dir.join(format!("uring-spike-probe-{}-{}", std::process::id(), nonce));
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| io::Error::other("probe path has NUL"))?;
    // O_EXCL refuses a pre-existing file; O_NOFOLLOW refuses a symlink; 0600 is
    // owner-only. SAFETY: `c_path` is a valid NUL-terminated path; on success
    // we own the fd.
    let fd = unsafe {
        libc::open(
            c_path.as_ptr(),
            libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_RDWR | libc::O_CLOEXEC,
            0o600,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    let mut file = unsafe { File::from_raw_fd(fd) };
    file.write_all(pattern)?;
    // Unlink now: the fd stays valid, no named leftover remains.
    // SAFETY: `c_path` is still a valid NUL-terminated path.
    unsafe {
        libc::unlink(c_path.as_ptr());
    }
    Ok(file)
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

/// What to do with a pending entry after its CQE (C9, rustfs/backlog#1058).
enum ReapStep {
    /// The logical read is done: remove the entry and deliver this result.
    Finish(io::Result<Vec<u8>>),
    /// Short read, not EOF: re-queue this SQE for the remainder; keep the entry.
    Resubmit(io_uring::squeue::Entry),
}

fn drive(ring: IoUring, rx: mpsc::Receiver<Msg>, stats: Arc<DriverStats>, bp: Arc<Backpressure>) {
    let mut state = DriverState {
        ring,
        pending: HashMap::new(),
        backlog: VecDeque::new(),
    };
    let mut shutting_down = false;
    let mut drain_deadline: Option<Instant> = None;

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
                        // The op never became in-flight; return its permit.
                        bp.release();
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
                            file,
                            done: Some(done),
                            offset,
                            nread: 0,
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
        match state.ring.submit() {
            Ok(_) => {}
            Err(e) if e.raw_os_error() == Some(libc::EBUSY) => {
                // CQ-overflow backpressure on pre-5.19 NODROP kernels: the
                // kernel refuses new submissions until we reap. Keep the
                // backlog and reap this turn instead of spinning (C5,
                // rustfs/backlog#1056).
            }
            Err(_) => {
                // EINTR and friends: retry on the next loop turn.
            }
        }

        // 3. Reap. A Pending entry (and thus its buffer) is dropped ONLY when
        //    the logical read finishes; a short read is resubmitted for the
        //    remainder and the entry stays put (C9, rustfs/backlog#1058).
        while let Some(cqe) = state.ring.completion().next() {
            let ud = cqe.user_data();
            if ud & CANCEL_BIT != 0 {
                // Result of the AsyncCancel op itself; the read's own CQE
                // (ECANCELED or success) still arrives separately. Record the
                // three-state outcome for diagnosability (C4,
                // rustfs/backlog#1055): EALREADY means the read is executing
                // and cannot be interrupted, i.e. its CQE may never come on a
                // hung device — the signal the bounded drain below relies on.
                match cqe.result() {
                    0 => stats.cancel_succeeded.fetch_add(1, Ordering::SeqCst),
                    r if r == -libc::ENOENT => stats.cancel_not_found.fetch_add(1, Ordering::SeqCst),
                    r if r == -libc::EALREADY => stats.cancel_already.fetch_add(1, Ordering::SeqCst),
                    _ => 0,
                };
                continue;
            }
            let res = cqe.result();
            if !state.pending.contains_key(&ud) {
                continue;
            }

            // Decide the next step while borrowing the entry, then act after
            // the borrow ends (finish removes it; resubmit re-queues an SQE).
            let step = {
                let p = state.pending.get_mut(&ud).expect("checked above");
                if res < 0 {
                    // Error (incl. ECANCELED) terminates the logical read.
                    ReapStep::Finish(Err(io::Error::from_raw_os_error(-res)))
                } else if res == 0 {
                    // Real EOF: deliver what was read so far.
                    p.buf.truncate(p.nread);
                    ReapStep::Finish(Ok(std::mem::take(&mut p.buf)))
                } else {
                    p.nread += res as usize;
                    // Only POSITIONED reads (read_at, whole-range pread
                    // contract) resubmit a short read. CURRENT_POSITION reads
                    // (read_current on pipes/streams) follow read(2) semantics:
                    // a short read is a valid final result and must be
                    // delivered as-is — resubmitting would block forever
                    // waiting for stream data that may never come.
                    let is_stream = p.offset == CURRENT_POSITION;
                    if is_stream || p.nread >= p.buf.len() {
                        p.buf.truncate(p.nread);
                        ReapStep::Finish(Ok(std::mem::take(&mut p.buf)))
                    } else {
                        // Positioned short read, not EOF: resubmit the
                        // remainder into buf[nread..]. The buffer stays owned by
                        // the driver and in_flight is unchanged — one logical op.
                        let remaining = p.buf.len() - p.nread;
                        // SAFETY: buf[nread..] is in bounds (nread < len) and
                        // stays alive in the pending table until the CQE.
                        let ptr = unsafe { p.buf.as_mut_ptr().add(p.nread) };
                        let next_off = if p.offset == CURRENT_POSITION {
                            CURRENT_POSITION
                        } else {
                            p.offset + p.nread as u64
                        };
                        let sqe = opcode::Read::new(types::Fd(p.file.as_raw_fd()), ptr, remaining as u32)
                            .offset(next_off)
                            .build()
                            .user_data(ud);
                        ReapStep::Resubmit(sqe)
                    }
                }
            };

            match step {
                ReapStep::Finish(outcome) => {
                    // Content hygiene (C12, rustfs/backlog#1062): the delivered
                    // bytes are ⊆ [0, res) — buf was freshly zeroed per op and
                    // truncated to res. When P3 reuses a driver-owned slab
                    // across requests, this ⊆ [0, res) property MUST be
                    // preserved or a previous tenant's object bytes leak.
                    let mut p = state.pending.remove(&ud).expect("checked above");
                    match p.done.take().expect("done sender set at submit").send(outcome) {
                        Ok(()) => stats.delivered.fetch_add(1, Ordering::SeqCst),
                        // Caller dropped the future: the buffer survived in
                        // the table until this final CQE and is reclaimed here.
                        Err(_) => stats.orphan_reclaimed.fetch_add(1, Ordering::SeqCst),
                    };
                    stats.in_flight.fetch_sub(1, Ordering::SeqCst);
                    // Release the permit HERE, at the CQE (pending removed),
                    // not at future drop (C10, rustfs/backlog#1060).
                    bp.release();
                }
                ReapStep::Resubmit(sqe) => state.backlog.push_back(sqe),
            }
        }

        // Monitor CQ overflow. With NODROP (asserted at probe) the crate's
        // submit() auto-flushes the kernel overflow list, so this should stay
        // 0; any non-zero value means CQEs were lost — pending entries would
        // never be reclaimed. Record it as a fatal signal (C5,
        // rustfs/backlog#1056).
        let overflow = state.ring.completion().overflow();
        if overflow != 0 {
            stats.cq_overflow.store(overflow as u64, Ordering::SeqCst);
            eprintln!("uring-spike driver: CQ overflow = {overflow}; CQEs lost — treat as fatal in P2");
        }

        // 4. Exit when drained: the kernel no longer references any buffer, so
        //    dropping the ring (unmap) is safe. If a hung device keeps a CQE
        //    from ever arriving, bail out under a bounded deadline instead of
        //    blocking forever (C4, rustfs/backlog#1055).
        if shutting_down {
            if state.pending.is_empty() && state.backlog.is_empty() {
                bp.shutdown(); // unblock any submit() waiter before we exit
                return; // clean drain: DriverState drops normally, ring unmaps.
            }
            let deadline = *drain_deadline.get_or_insert_with(|| Instant::now() + DRAIN_TIMEOUT);
            if Instant::now() >= deadline {
                // A CQE may never arrive (ASYNC_CANCEL cannot interrupt an
                // in-execution regular-file read on a hung disk). We must NOT
                // unmap the ring or free the still-in-flight buffers — leak the
                // whole state (leak over UAF) and exit so shutdown() returns.
                eprintln!(
                    "uring-spike driver: bounded drain timed out with {} ops still in flight; \
                     leaking ring + buffers to stay memory-safe",
                    state.pending.len()
                );
                bp.shutdown(); // unblock any submit() waiter
                std::mem::forget(state);
                return;
            }
        }

        // Spike-grade pacing: production replaces this poll with eventfd +
        // tokio AsyncFd reaping (backlog#894).
        if !state.pending.is_empty() {
            std::thread::sleep(Duration::from_micros(200));
        }
    }
}
