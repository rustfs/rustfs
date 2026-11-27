// Copyright 2026 RustFS Team
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

//! Process-wide RAII byte budget for buffered local shard reads.
//!
//! Local batch positioned reads (`batch_shard_pread`) and mmap-copy shard reads
//! materialize full shard ranges in memory before returning a `ShardReader`.
//! Without a process-wide ceiling, concurrent unread or slow-consumed
//! `GetObject` responses can pin unbounded heap memory across requests.
//!
//! Callers reserve bytes *before* allocating shard buffers and bind the
//! resulting [`ReadMemoryPermit`] to the returned [`Bytes`] via
//! [`Bytes::from_owner`]. Dropping or cancelling the reader (and its last slice)
//! frees the buffer first and immediately returns the reserved bytes to the
//! budget. When the budget cannot admit a request, callers fall back to the
//! streaming reader path.

use bytes::Bytes;
use rustfs_config::{DEFAULT_OBJECT_MMAP_READ_MAX_INFLIGHT_BYTES, ENV_OBJECT_MMAP_READ_MAX_INFLIGHT_BYTES};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

/// Process-wide in-flight byte budget shared by buffered local shard reads.
static GLOBAL_IN_FLIGHT_READ_BUDGET: OnceLock<Arc<InFlightReadBudget>> = OnceLock::new();

const IN_FLIGHT_READ_BUDGET_EXHAUSTED_MSG: &str = "in-flight read memory budget exhausted";

/// Sentinel error returned by `LocalDisk::read_file_mmap_copy_with_metrics`
/// when the process-wide in-flight read memory budget cannot admit a read.
pub(crate) fn in_flight_read_budget_exhausted_error() -> crate::disk::error::DiskError {
    crate::disk::error::DiskError::Io(std::io::Error::new(std::io::ErrorKind::WouldBlock, IN_FLIGHT_READ_BUDGET_EXHAUSTED_MSG))
}

/// Returns `true` when `err` is the in-flight read memory budget exhaustion
/// sentinel produced by [`in_flight_read_budget_exhausted_error`].
pub(crate) fn is_in_flight_read_budget_exhausted(err: &crate::disk::error::DiskError) -> bool {
    matches!(
        err,
        crate::disk::error::DiskError::Io(io_err)
            if io_err.kind() == std::io::ErrorKind::WouldBlock
                && io_err.to_string() == IN_FLIGHT_READ_BUDGET_EXHAUSTED_MSG
    )
}

/// Configured process-wide in-flight read byte limit.
///
/// Cached via `OnceLock` in non-test builds to keep the hot read path free of
/// process-global environment locks; read directly in test builds so `temp_env`
/// overrides take effect immediately.
pub(crate) fn object_mmap_read_max_inflight_bytes() -> usize {
    #[cfg(test)]
    {
        rustfs_utils::get_env_usize(ENV_OBJECT_MMAP_READ_MAX_INFLIGHT_BYTES, DEFAULT_OBJECT_MMAP_READ_MAX_INFLIGHT_BYTES)
    }
    #[cfg(not(test))]
    {
        static CACHED: OnceLock<usize> = OnceLock::new();
        *CACHED.get_or_init(|| {
            rustfs_utils::get_env_usize(ENV_OBJECT_MMAP_READ_MAX_INFLIGHT_BYTES, DEFAULT_OBJECT_MMAP_READ_MAX_INFLIGHT_BYTES)
        })
    }
}

/// Returns the process-wide in-flight read memory budget.
pub(crate) fn global_in_flight_read_budget() -> &'static Arc<InFlightReadBudget> {
    GLOBAL_IN_FLIGHT_READ_BUDGET.get_or_init(|| Arc::new(InFlightReadBudget::new_global()))
}

/// Lock-free byte budget tracking in-flight buffered shard allocations.
#[derive(Debug)]
pub(crate) struct InFlightReadBudget {
    in_flight_bytes: AtomicUsize,
    limit_override: Option<usize>,
}

impl InFlightReadBudget {
    fn new_global() -> Self {
        Self {
            in_flight_bytes: AtomicUsize::new(0),
            limit_override: None,
        }
    }

    /// Create an isolated budget with an explicit fixed limit (used by unit tests).
    #[cfg(test)]
    pub(crate) fn with_limit(limit: usize) -> Arc<Self> {
        Arc::new(Self {
            in_flight_bytes: AtomicUsize::new(0),
            limit_override: Some(limit),
        })
    }

    /// Current configured byte ceiling.
    pub(crate) fn limit(&self) -> usize {
        self.limit_override.unwrap_or_else(object_mmap_read_max_inflight_bytes)
    }

    /// Currently reserved in-flight bytes.
    #[cfg(test)]
    pub(crate) fn in_flight_bytes(&self) -> usize {
        self.in_flight_bytes.load(Ordering::Acquire)
    }

    /// Attempt to reserve `bytes` from this budget without blocking.
    ///
    /// Returns `None` when the configured limit is `0` (buffered reads disabled)
    /// or when admitting `bytes` would overflow or exceed the current limit.
    pub(crate) fn try_reserve(self: &Arc<Self>, bytes: usize) -> Option<ReadMemoryPermit> {
        let limit = self.limit();
        if limit == 0 || bytes > limit {
            return None;
        }
        if bytes == 0 {
            return Some(ReadMemoryPermit {
                budget: Arc::clone(self),
                bytes: 0,
            });
        }

        let mut current = self.in_flight_bytes.load(Ordering::Acquire);
        loop {
            let next = current.checked_add(bytes)?;
            if next > limit {
                return None;
            }
            match self
                .in_flight_bytes
                .compare_exchange_weak(current, next, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => {
                    return Some(ReadMemoryPermit {
                        budget: Arc::clone(self),
                        bytes,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    /// Atomically reserve every shard length in `sizes` as an all-or-nothing
    /// batch, returning one independently droppable permit per entry.
    pub(crate) fn try_reserve_batch(self: &Arc<Self>, sizes: &[usize]) -> Option<Vec<ReadMemoryPermit>> {
        let total = sizes.iter().try_fold(0usize, |acc, &size| acc.checked_add(size))?;
        let mut batch_permit = self.try_reserve(total)?;
        let mut permits = Vec::with_capacity(sizes.len());
        for &size in sizes {
            permits.push(batch_permit.split(size)?);
        }
        debug_assert_eq!(batch_permit.bytes, 0, "batch permit must be fully partitioned");
        Some(permits)
    }
}

/// RAII reservation of in-flight shard read bytes.
///
/// Dropping this permit immediately returns its reserved bytes to the owning
/// [`InFlightReadBudget`].
#[derive(Debug)]
pub(crate) struct ReadMemoryPermit {
    budget: Arc<InFlightReadBudget>,
    bytes: usize,
}

impl ReadMemoryPermit {
    /// Bytes reserved by this permit.
    #[cfg(test)]
    pub(crate) fn bytes(&self) -> usize {
        self.bytes
    }

    /// Carve `amount` bytes off this permit into a child permit while conserving
    /// the total reserved byte count across parent and child.
    fn split(&mut self, amount: usize) -> Option<Self> {
        let remaining = self.bytes.checked_sub(amount)?;
        self.bytes = remaining;
        Some(Self {
            budget: Arc::clone(&self.budget),
            bytes: amount,
        })
    }

    /// Bind this permit to an owned `Vec<u8>` buffer via [`Bytes::from_owner`].
    ///
    /// If the buffer is empty, the permit is released immediately and an empty
    /// `Bytes` is returned. Otherwise the permit remains held until the last
    /// `Bytes` slice referencing the allocation is dropped, freeing the backing
    /// buffer before refunding the budget.
    pub(crate) fn wrap_vec(self, data: Vec<u8>) -> Bytes {
        if data.is_empty() {
            drop(data);
            drop(self);
            return Bytes::new();
        }
        Bytes::from_owner(BudgetedOwner { data, _permit: self })
    }

    /// Bind this permit to an already-constructed [`Bytes`] via
    /// [`Bytes::from_owner`].
    pub(crate) fn wrap_bytes(self, data: Bytes) -> Bytes {
        if data.is_empty() {
            drop(data);
            drop(self);
            return Bytes::new();
        }
        Bytes::from_owner(BudgetedOwner { data, _permit: self })
    }
}

impl Drop for ReadMemoryPermit {
    fn drop(&mut self) {
        if self.bytes > 0 {
            let prev = self.budget.in_flight_bytes.fetch_sub(self.bytes, Ordering::AcqRel);
            debug_assert!(prev >= self.bytes, "in-flight read budget underflow");
        }
    }
}

/// Owner wrapper passed to [`Bytes::from_owner`].
///
/// Field declaration order is significant: `data` is dropped first so heap
/// memory is released before `_permit` refunds the global budget.
struct BudgetedOwner<T> {
    data: T,
    _permit: ReadMemoryPermit,
}

impl<T: AsRef<[u8]>> AsRef<[u8]> for BudgetedOwner<T> {
    fn as_ref(&self) -> &[u8] {
        self.data.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Barrier;

    #[test]
    fn budget_reserves_enforces_cap_and_reclaims_on_drop() {
        let budget = InFlightReadBudget::with_limit(1024);
        assert_eq!(budget.in_flight_bytes(), 0);

        let p1 = budget.try_reserve(600).expect("first reservation under cap");
        assert_eq!(p1.bytes(), 600);
        assert_eq!(budget.in_flight_bytes(), 600);

        assert!(
            budget.try_reserve(500).is_none(),
            "reservation exceeding remaining capacity must fail without overshoot"
        );
        assert_eq!(budget.in_flight_bytes(), 600);

        let p2 = budget.try_reserve(424).expect("exact remaining capacity must succeed");
        assert_eq!(budget.in_flight_bytes(), 1024);
        assert!(budget.try_reserve(1).is_none());

        drop(p1);
        assert_eq!(budget.in_flight_bytes(), 424);
        drop(p2);
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[test]
    fn zero_limit_disables_all_reservations_including_empty() {
        let budget = InFlightReadBudget::with_limit(0);
        assert!(budget.try_reserve(0).is_none());
        assert!(budget.try_reserve(1).is_none());
        assert!(budget.try_reserve_batch(&[0, 0]).is_none());
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[test]
    fn batch_reservation_is_atomic_and_splits_independently() {
        let budget = InFlightReadBudget::with_limit(100);
        assert!(
            budget.try_reserve_batch(&[40, 40, 40]).is_none(),
            "all-or-nothing batch over limit must leave budget untouched"
        );
        assert_eq!(budget.in_flight_bytes(), 0);

        assert!(
            budget.try_reserve_batch(&[usize::MAX, 1]).is_none(),
            "overflowing batch sum must be rejected safely"
        );
        assert_eq!(budget.in_flight_bytes(), 0);

        let mut permits = budget.try_reserve_batch(&[30, 50, 20]).expect("exact batch fits");
        assert_eq!(budget.in_flight_bytes(), 100);
        assert_eq!(permits.len(), 3);

        let last = permits.pop().expect("third permit");
        drop(last);
        assert_eq!(budget.in_flight_bytes(), 80);

        drop(permits);
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[test]
    fn bytes_owner_holds_permit_across_slices_and_releases_on_last_drop() {
        let budget = InFlightReadBudget::with_limit(64);
        let permit = budget.try_reserve(64).expect("reserve 64 bytes");
        let bytes = permit.wrap_vec(vec![0xAB; 64]);
        assert_eq!(budget.in_flight_bytes(), 64);

        let slice = bytes.slice(8..16);
        drop(bytes);
        assert_eq!(
            budget.in_flight_bytes(),
            64,
            "permit must stay held while a non-empty slice pins the backing allocation"
        );
        assert_eq!(slice.as_ref(), &[0xAB; 8]);

        drop(slice);
        assert_eq!(
            budget.in_flight_bytes(),
            0,
            "dropping the final slice must release the full reservation immediately"
        );
    }

    #[test]
    fn empty_buffer_releases_permit_immediately() {
        let budget = InFlightReadBudget::with_limit(64);
        let permit = budget.try_reserve(16).expect("reserve");
        let bytes = permit.wrap_vec(Vec::new());
        assert!(bytes.is_empty());
        assert_eq!(budget.in_flight_bytes(), 0);
    }

    #[test]
    fn concurrent_reservations_never_exceed_limit_or_leak() {
        const LIMIT: usize = 4096;
        const THREADS: usize = 8;
        const CHUNK: usize = 256;

        let budget = InFlightReadBudget::with_limit(LIMIT);
        let barrier = Arc::new(Barrier::new(THREADS));
        let mut handles = Vec::with_capacity(THREADS);

        for _ in 0..THREADS {
            let budget = Arc::clone(&budget);
            let barrier = Arc::clone(&barrier);
            handles.push(std::thread::spawn(move || {
                barrier.wait();
                let mut held = Vec::new();
                for _ in 0..200 {
                    if let Some(permit) = budget.try_reserve(CHUNK) {
                        assert!(
                            budget.in_flight_bytes() <= LIMIT,
                            "in-flight bytes must never overshoot the configured limit"
                        );
                        held.push(permit.wrap_vec(vec![1u8; CHUNK]));
                    } else {
                        held.pop();
                    }
                }
                drop(held);
            }));
        }

        for handle in handles {
            handle.join().expect("worker thread must not panic");
        }
        assert_eq!(budget.in_flight_bytes(), 0, "all reservations must be reclaimed");
    }
}
