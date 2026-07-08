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

//! Worker slot limiter used by long-running background workflows.

use std::sync::Arc;
use tokio::sync::{Semaphore, watch};
use tracing::{debug, trace};

/// Cooperative worker-slot controller for async tasks.
///
/// Slot acquisition is backed by a fair FIFO [`Semaphore`], while drain
/// waiters observe the in-use count through a dedicated [`watch`] channel.
/// The two wakeup sources are deliberately separate so a drain waiter can
/// never consume a wakeup destined for a pending [`take`](Self::take).
pub struct Workers {
    semaphore: Semaphore,         // Available working slots
    in_use: watch::Sender<usize>, // Slots currently held; drain waiters watch it
    limit: usize,                 // Maximum number of concurrent jobs
}

impl Workers {
    /// Create a [`Workers`] object that allows up to `n` jobs to execute concurrently.
    pub fn new(n: usize) -> Result<Arc<Self>, &'static str> {
        if n == 0 {
            return Err("n must be > 0");
        }
        if n > Semaphore::MAX_PERMITS {
            return Err("n exceeds the maximum supported number of slots");
        }

        Ok(Arc::new(Self {
            semaphore: Semaphore::new(n),
            in_use: watch::Sender::new(0),
            limit: n,
        }))
    }

    /// Acquire a worker slot, waiting until one becomes available.
    pub async fn take(&self) {
        let permit = match self.semaphore.acquire().await {
            Ok(permit) => permit,
            // The semaphore is owned by `self` and never closed.
            Err(_) => unreachable!("worker semaphore is never closed"),
        };
        permit.forget(); // returned manually via give()
        self.in_use.send_modify(|held| *held += 1);
        trace!(
            event = "worker_slot.acquire",
            component = "concurrency",
            subsystem = "workers",
            state = "granted",
            available_slots = self.semaphore.available_permits(),
            total_slots = self.limit,
            permits_in_use = *self.in_use.borrow(),
            "worker slot updated"
        );
    }

    /// Release a worker slot.
    ///
    /// A `give()` that is not paired with a prior [`take`](Self::take) is
    /// clamped: it never inflates capacity beyond `limit`.
    pub async fn give(&self) {
        let mut released = false;
        self.in_use.send_modify(|held| {
            if *held > 0 {
                *held -= 1;
                released = true;
            }
        });
        if released {
            self.semaphore.add_permits(1);
        }
        trace!(
            event = "worker_slot.release",
            component = "concurrency",
            subsystem = "workers",
            state = "released",
            available_slots = self.semaphore.available_permits(),
            total_slots = self.limit,
            permits_in_use = *self.in_use.borrow(),
            "worker slot updated"
        );
    }

    /// Wait until all worker slots are released.
    pub async fn wait(&self) {
        let mut rx = self.in_use.subscribe();
        // The sender is owned by `self`, so it outlives this borrow.
        let _ = rx.wait_for(|&held| held == 0).await;
        debug!(
            event = "worker_slot.wait",
            component = "concurrency",
            subsystem = "workers",
            state = "drained",
            available_slots = self.limit,
            total_slots = self.limit,
            permits_in_use = 0,
            "worker drain complete"
        );
    }

    /// Return the current number of available worker slots.
    pub async fn available(&self) -> usize {
        self.limit.saturating_sub(*self.in_use.borrow())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;
    use tokio::time::{sleep, timeout};

    #[tokio::test]
    async fn test_workers() {
        let workers = Workers::new(5).unwrap();

        for _ in 0..5 {
            let workers = workers.clone();
            tokio::spawn(async move {
                workers.take().await;
                sleep(Duration::from_millis(50)).await;
                workers.give().await;
            });
        }

        for _ in 0..5 {
            workers.give().await;
        }
        // Sleep: wait for spawn task started
        sleep(Duration::from_millis(20)).await;
        workers.wait().await;
        assert_eq!(workers.available().await, workers.limit);
    }

    #[tokio::test]
    async fn test_workers_over_release_is_clamped() {
        let workers = Workers::new(2).unwrap();

        workers.take().await;
        workers.give().await;
        workers.give().await;
        workers.give().await;

        assert_eq!(workers.available().await, 2);
    }

    /// Regression for S08: with limit=1, a pending drain waiter must never
    /// steal the wakeup destined for a pending taker.
    #[tokio::test]
    async fn test_drain_waiter_does_not_steal_take_wakeup() {
        let workers = Workers::new(1).unwrap();
        workers.take().await;

        let taker = {
            let workers = workers.clone();
            tokio::spawn(async move { workers.take().await })
        };
        let drainer = {
            let workers = workers.clone();
            tokio::spawn(async move { workers.wait().await })
        };
        // Let both tasks block before releasing the slot.
        sleep(Duration::from_millis(50)).await;
        workers.give().await;

        timeout(Duration::from_secs(1), taker)
            .await
            .expect("taker must be woken by give()")
            .unwrap();

        workers.give().await;
        timeout(Duration::from_secs(1), drainer)
            .await
            .expect("drain waiter must complete once all slots are free")
            .unwrap();
        assert_eq!(workers.available().await, 1);
    }

    /// Regression for S17: two rapid give() calls must wake two pending
    /// takers; permits must not merge into a single wakeup.
    #[tokio::test]
    async fn test_rapid_gives_wake_all_pending_takers() {
        let workers = Workers::new(2).unwrap();
        workers.take().await;
        workers.take().await;

        let takers: Vec<_> = (0..2)
            .map(|_| {
                let workers = workers.clone();
                tokio::spawn(async move { workers.take().await })
            })
            .collect();
        // Let both takers queue up before any slot is released.
        sleep(Duration::from_millis(50)).await;
        workers.give().await;
        workers.give().await;

        for taker in takers {
            timeout(Duration::from_secs(1), taker)
                .await
                .expect("every pending taker must be woken")
                .unwrap();
        }
        assert_eq!(workers.available().await, 0);
    }
}
