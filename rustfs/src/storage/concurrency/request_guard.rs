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
use rustfs_io_metrics::{record_get_object_request_result, record_get_object_request_start};

/// RAII guard for tracking active GetObject requests.
#[derive(Debug)]
pub struct GetObjectGuard {
    start_time: Instant,
    /// Final status set by the caller; if None when dropped, reported as "unknown".
    result: Option<&'static str>,
}

impl GetObjectGuard {
    /// Create a new guard and increment the concurrent request counter.
    pub fn new() -> Self {
        ACTIVE_GET_REQUESTS.fetch_add(1, Ordering::Relaxed);

        // Record metrics for a started GetObject request. Capture the
        // concurrent request count AFTER increment to reflect the current
        // active requests.
        let concurrent = ACTIVE_GET_REQUESTS.load(Ordering::Relaxed);
        record_get_object_request_start(concurrent);

        Self {
            start_time: Instant::now(),
            result: None,
        }
    }

    /// Mark the request as completed successfully.
    ///
    /// Call this before the guard is dropped to record the correct status.
    pub fn finish_ok(&mut self) {
        self.result = Some("ok");
    }

    /// Mark the request as failed.
    ///
    /// Call this before the guard is dropped to record the correct status.
    pub fn finish_err(&mut self) {
        self.result = Some("error");
    }

    /// Get the elapsed time since this guard was created.
    #[allow(dead_code)]
    // This helper is primarily used by unit tests to assert timing.
    // It's intentionally kept public for callers that may want to inspect
    // a guard's duration without dropping it.
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
        // Record duration of this request before decrementing the global
        // counter. This ensures `start_time` is actually used and the
        // `elapsed()` method remains meaningful for tests and callers.
        let duration_secs = self.start_time.elapsed().as_secs_f64();
        // Use the caller-set status, or "unknown" if the result was never set
        // (e.g., the future was cancelled or the guard dropped without explicit completion).
        let status = self.result.unwrap_or("unknown");
        record_get_object_request_result(status, duration_secs);

        if let Err(previous) =
            ACTIVE_GET_REQUESTS.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| current.checked_sub(1))
        {
            debug_assert_eq!(
                previous, 0,
                "ACTIVE_GET_REQUESTS underflow attempt in GetObjectGuard::drop; previous value = {}",
                previous
            );
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
