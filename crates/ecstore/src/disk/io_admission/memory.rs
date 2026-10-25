// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use std::io;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::time::Instant;

static BACKGROUND_MEMORY: OnceLock<Arc<BackgroundMemoryBudget>> = OnceLock::new();

/// Process-wide payload storage retained by admitted background I/O. The
/// existing resource sampler publishes limits; this owner starts no workers.
#[derive(Debug)]
pub struct BackgroundMemoryBudget {
    // This short critical section only changes counters and a deadline. It
    // never allocates, calls storage, or acquires another lock while held.
    state: Mutex<State>,
}

#[derive(Debug)]
struct State {
    limit: u64,
    used: u64,
    valid_until: Instant,
}

/// A coherent view of the leased cap and retained payload storage.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BackgroundMemorySnapshot {
    /// Zero when the latest resource observation has expired.
    pub limit: u64,
    /// Existing storage remains charged even after the cap shrinks below it.
    pub used: u64,
}

impl BackgroundMemoryBudget {
    /// Start closed until the existing resource sampler publishes a limit.
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(State {
                limit: 0,
                used: 0,
                valid_until: Instant::now(),
            }),
        })
    }

    /// Install the sole process owner before starting adaptive background work.
    /// Returns false if an owner was already installed.
    pub fn install(self: &Arc<Self>) -> bool {
        BACKGROUND_MEMORY.set(self.clone()).is_ok()
    }

    pub(super) fn current() -> Option<Arc<Self>> {
        BACKGROUND_MEMORY.get().cloned()
    }

    /// Update admission without revoking storage retained by native tasks or
    /// readers. Missing observations must publish zero capacity.
    pub fn set_sampled_limit(&self, limit: u64, valid_for: Duration) {
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        state.limit = limit;
        let now = Instant::now();
        state.valid_until = now.checked_add(valid_for).unwrap_or(now);
    }

    /// Read the cap and retained byte count under the same short lock.
    pub fn snapshot(&self) -> BackgroundMemorySnapshot {
        let state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        BackgroundMemorySnapshot {
            limit: if Instant::now() < state.valid_until { state.limit } else { 0 },
            used: state.used,
        }
    }

    fn reserve(self: &Arc<Self>, bytes: usize) -> io::Result<Reservation> {
        let bytes = u64::try_from(bytes).map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        let used = state
            .used
            .checked_add(bytes)
            .ok_or_else(|| io::Error::from(io::ErrorKind::WouldBlock))?;
        if bytes != 0 && (Instant::now() >= state.valid_until || used > state.limit) {
            return Err(io::Error::new(io::ErrorKind::WouldBlock, "background buffer memory budget exhausted"));
        }
        state.used = used;
        Ok(Reservation {
            owner: self.clone(),
            bytes,
        })
    }
}

struct Reservation {
    owner: Arc<BackgroundMemoryBudget>,
    bytes: u64,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.owner
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .used -= self.bytes;
    }
}

/// Reusable initialized payload storage. The reservation is embedded in the
/// existing native job state, with no per-read owner allocation or payload copy.
pub(crate) struct BackgroundBuffer {
    // Field order releases the allocation before refunding its reservation.
    bytes: Box<[u8]>,
    reservation: Option<Reservation>,
    budget: Option<Arc<BackgroundMemoryBudget>>,
}

impl BackgroundBuffer {
    pub(crate) fn new(budget: Option<Arc<BackgroundMemoryBudget>>) -> Self {
        Self {
            bytes: Box::default(),
            reservation: None,
            budget,
        }
    }

    /// Call only after the previous contents have been consumed. Growth charges
    /// both allocations until the old buffer is freed, including after shrink.
    pub(crate) fn ensure_len(&mut self, length: usize) -> io::Result<()> {
        if self.bytes.len() >= length {
            return Ok(());
        }
        let reservation = self.budget.as_ref().map(|budget| budget.reserve(length)).transpose()?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(length)
            .map_err(|_| io::Error::from(io::ErrorKind::OutOfMemory))?;
        bytes.resize(length, 0);
        self.bytes = bytes.into_boxed_slice();
        self.reservation = reservation;
        Ok(())
    }

    pub(crate) fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.bytes
    }

    pub(crate) fn as_slice(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn shrink_and_expiry_keep_retained_storage_charged() {
        let owner = BackgroundMemoryBudget::new();
        assert!(owner.reserve(1).is_err());
        owner.set_sampled_limit(16, Duration::from_secs(3));
        let mut buffer = BackgroundBuffer::new(Some(owner.clone()));
        buffer.ensure_len(8).expect("initial allocation");
        let pointer = buffer.as_slice().as_ptr();
        owner.set_sampled_limit(4, Duration::from_secs(3));
        buffer.ensure_len(8).expect("existing storage can be reused after shrink");
        assert_eq!(buffer.as_slice().as_ptr(), pointer);
        assert_eq!(owner.snapshot(), BackgroundMemorySnapshot { limit: 4, used: 8 });
        assert_eq!(buffer.ensure_len(9).expect_err("growth must be denied").kind(), io::ErrorKind::WouldBlock);
        assert_eq!(owner.snapshot().used, 8, "failed growth preserves the existing reservation");
        tokio::time::advance(Duration::from_secs(3)).await;
        assert_eq!(owner.snapshot(), BackgroundMemorySnapshot { limit: 0, used: 8 });
        assert!(owner.reserve(1).is_err(), "an expired sampler cannot grant memory");
        drop(buffer);
        assert_eq!(owner.snapshot().used, 0);
    }

    #[test]
    fn growth_accounts_for_both_allocations_and_refunds_failed_attempts() {
        let owner = BackgroundMemoryBudget::new();
        owner.set_sampled_limit(12, Duration::from_secs(3));
        let mut buffer = BackgroundBuffer::new(Some(owner.clone()));
        buffer.ensure_len(4).expect("first allocation");
        buffer.as_mut_slice().copy_from_slice(b"data");
        assert!(buffer.ensure_len(9).is_err(), "four old plus nine new bytes exceed the cap");
        assert_eq!(buffer.as_slice(), b"data");
        buffer.ensure_len(8).expect("four old plus eight new bytes fit");
        assert_eq!(owner.snapshot().used, 8);
        assert_eq!(buffer.as_slice(), &[0; 8]);
        drop(buffer);
        assert_eq!(owner.snapshot().used, 0);
    }

    #[test]
    fn concurrent_reservations_share_one_cap() {
        let owner = BackgroundMemoryBudget::new();
        owner.set_sampled_limit(1024, Duration::from_secs(30));
        let ready = Arc::new(std::sync::Barrier::new(17));
        let release = Arc::new(std::sync::Barrier::new(17));
        let workers: Vec<_> = (0..16)
            .map(|_| {
                let owner = owner.clone();
                let ready = ready.clone();
                let release = release.clone();
                std::thread::spawn(move || {
                    let reservation = owner.reserve(128).ok();
                    ready.wait();
                    release.wait();
                    reservation.is_some()
                })
            })
            .collect();
        ready.wait();
        let retained = owner.snapshot().used;
        release.wait();
        let granted = workers
            .into_iter()
            .map(|worker| worker.join().expect("reservation worker"))
            .filter(|granted| *granted)
            .count();
        assert_eq!(retained, 1024);
        assert_eq!(granted, 8);
        assert_eq!(owner.snapshot().used, 0);
    }
}
