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
    /// True when the errno belongs to the "expected restriction" class that
    /// P2 maps to permanent per-disk fallback: EACCES/EPERM/ENOSYS/EINVAL/
    /// EOPNOTSUPP. Anything else would be a genuine bug worth surfacing.
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
    let result = (|| {
        std::fs::write(&path, &pattern)?;
        let file = File::open(&path)?;
        let mut buf = vec![0u8; pattern.len()];
        let sqe = opcode::Read::new(types::Fd(file.as_raw_fd()), buf.as_mut_ptr(), buf.len() as u32)
            .offset(0)
            .build()
            .user_data(0xB0BE);
        // SAFETY: `buf` and `file` outlive the synchronous wait below.
        unsafe {
            ring.submission()
                .push(&sqe)
                .map_err(|_| io::Error::other("probe: submission queue full"))?;
        }
        ring.submit_and_wait(1)?;
        let cqe = ring.completion().next().ok_or_else(|| io::Error::other("probe: no CQE"))?;
        if cqe.result() < 0 {
            return Err(io::Error::from_raw_os_error(-cqe.result()));
        }
        if cqe.result() as usize != pattern.len() || buf != pattern {
            return Err(io::Error::other("probe: read completed but data mismatched"));
        }
        Ok(())
    })();
    let _ = std::fs::remove_file(&path);
    result
}

fn drive(mut ring: IoUring, rx: mpsc::Receiver<Msg>, stats: Arc<DriverStats>) {
    let mut pending: HashMap<u64, Pending> = HashMap::new();
    let mut backlog: VecDeque<io_uring::squeue::Entry> = VecDeque::new();
    let mut shutting_down = false;

    loop {
        // 1. Intake. Block (with timeout) only when fully idle; once ops are
        //    in flight we must keep reaping, so only try_recv.
        loop {
            let idle = pending.is_empty() && backlog.is_empty() && !shutting_down;
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
                    // and the entry is only removed at the CQE.
                    let sqe = opcode::Read::new(types::Fd(file.as_raw_fd()), buf.as_mut_ptr(), len as u32)
                        .offset(offset)
                        .build()
                        .user_data(id);
                    pending.insert(
                        id,
                        Pending {
                            buf,
                            _file: file,
                            done: Some(done),
                        },
                    );
                    stats.submitted.fetch_add(1, Ordering::SeqCst);
                    stats.in_flight.fetch_add(1, Ordering::SeqCst);
                    backlog.push_back(sqe);
                }
                Msg::Cancel { id } => {
                    if pending.contains_key(&id) {
                        backlog.push_back(opcode::AsyncCancel::new(id).build().user_data(id | CANCEL_BIT));
                    }
                }
                Msg::Shutdown => {
                    shutting_down = true;
                    for id in pending.keys() {
                        backlog.push_back(opcode::AsyncCancel::new(*id).build().user_data(*id | CANCEL_BIT));
                    }
                }
            }
        }

        // 2. Push backlog into the SQ (stop when full; retry next turn).
        {
            let mut sq = ring.submission();
            while let Some(sqe) = backlog.pop_front() {
                // SAFETY: read SQEs point into `pending`-owned buffers that
                // live until their CQE; cancel SQEs carry no pointers.
                if unsafe { sq.push(&sqe) }.is_err() {
                    backlog.push_front(sqe);
                    break;
                }
            }
        }
        if ring.submit().is_err() {
            // EINTR and friends: retry on the next loop turn.
        }

        // 3. Reap. This is the ONLY place a Pending entry (and thus its
        //    buffer) is dropped.
        while let Some(cqe) = ring.completion().next() {
            let ud = cqe.user_data();
            if ud & CANCEL_BIT != 0 {
                // Result of the AsyncCancel op itself; the read's own CQE
                // (ECANCELED or success) still arrives separately.
                continue;
            }
            let Some(mut p) = pending.remove(&ud) else {
                continue;
            };
            let res = cqe.result();
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
        if shutting_down && pending.is_empty() && backlog.is_empty() {
            return;
        }

        // Spike-grade pacing: production replaces this poll with eventfd +
        // tokio AsyncFd reaping (backlog#894).
        if !pending.is_empty() {
            std::thread::sleep(Duration::from_micros(200));
        }
    }
}
