//! Spike 0 for rustfs/backlog#894 (P2 io_uring read backend).
//!
//! Proves the cancel-safety ownership model before any production work:
//!
//! - The read buffer and the file handle are owned by the driver's pending
//!   (orphan) table from SQE submission until the CQE arrives. The kernel may
//!   write into the buffer at any point in that window, so nothing else is
//!   allowed to free or move its heap allocation.
//! - Dropping the caller-side future only abandons the *result*. It never
//!   touches the buffer. Optionally it submits `IORING_OP_ASYNC_CANCEL` to
//!   accelerate the CQE; reclamation still happens only at the CQE.
//! - Driver shutdown cancels all in-flight ops and drains the ring to
//!   `in_flight == 0` before the ring is dropped (unmapped).
//!
//! NOT production code: the driver thread uses a coarse poll loop instead of
//! eventfd + `AsyncFd` reaping, there is no SQ-depth semaphore backpressure,
//! no O_DIRECT alignment, and only one read shape. See SPIKE.md.

#[cfg(target_os = "linux")]
mod driver;

#[cfg(target_os = "linux")]
pub use driver::{ProbeFailure, StatsSnapshot, UringDriver};
