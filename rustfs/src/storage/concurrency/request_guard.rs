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

//! Request tracking module with RAII guard for concurrent request management.

use std::sync::atomic::Ordering;
use std::time::Instant;

use super::io_schedule::ACTIVE_GET_REQUESTS;

/// RAII guard for tracking active GetObject requests.
#[derive(Debug)]
pub struct GetObjectGuard {
    start_time: Instant,
}

impl GetObjectGuard {
    /// Create a new guard and increment the concurrent request counter.
    pub fn new() -> Self {
        ACTIVE_GET_REQUESTS.fetch_add(1, Ordering::Relaxed);

        #[cfg(all(feature = "metrics", not(test)))]
        if !std::thread::panicking() {
            use metrics::counter;
            counter!("rustfs.get.object.requests.started").increment(1);
        }

        Self {
            start_time: Instant::now(),
        }
    }

    /// Get the elapsed time since this guard was created.
    pub fn elapsed(&self) -> std::time::Duration {
        self.start_time.elapsed()
    }

    /// Get the current number of concurrent GetObject requests.
    pub fn concurrent_count() -> usize {
        ACTIVE_GET_REQUESTS.load(Ordering::Relaxed)
    }

    /// Get the current number of concurrent requests (alias for concurrent_count).
    pub fn concurrent_requests() -> usize {
        Self::concurrent_count()
    }
}

impl Default for GetObjectGuard {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for GetObjectGuard {
    fn drop(&mut self) {
        if let Err(previous) =
            ACTIVE_GET_REQUESTS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_sub(1))
        {
            debug_assert_eq!(
                previous, 0,
                "ACTIVE_GET_REQUESTS underflow attempt in GetObjectGuard::drop; previous value = {}",
                previous
            );
        }

        #[cfg(all(feature = "metrics", not(test)))]
        if !std::thread::panicking() {
            use metrics::{counter, histogram};
            counter!("rustfs.get.object.requests.completed").increment(1);
            histogram!("rustfs.get.object.duration.seconds").record(self.elapsed().as_secs_f64());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guard_increments_counter() {
        let initial = GetObjectGuard::concurrent_count();
        {
            let _guard = GetObjectGuard::new();
            assert_eq!(GetObjectGuard::concurrent_count(), initial + 1);
        }
        assert_eq!(GetObjectGuard::concurrent_count(), initial);
    }

    #[test]
    fn test_guard_elapsed() {
        let guard = GetObjectGuard::new();
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(guard.elapsed().as_millis() >= 10);
    }
}
