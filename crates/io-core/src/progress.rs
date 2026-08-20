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

//! Progress tracking for long-running I/O operations.
//!
//! Re-exported as `rustfs_concurrency::OperationProgress` for the storage
//! timeout implementation, which uses `is_stale` to tell a slow transfer
//! apart from a stalled one.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Operation progress tracker.
#[derive(Debug)]
pub struct OperationProgress {
    /// Total size (if known).
    pub total_size: Option<u64>,
    /// Bytes processed.
    bytes_processed: AtomicU64,
    /// Last update time.
    last_update: std::sync::Mutex<Instant>,
    /// Stale timeout.
    stale_timeout: Duration,
    /// Start time for transfer rate calculation.
    start_time: Instant,
}

impl OperationProgress {
    /// Create new operation progress.
    pub fn new(total_size: Option<u64>, stale_timeout: Duration) -> Self {
        Self {
            total_size,
            bytes_processed: AtomicU64::new(0),
            last_update: std::sync::Mutex::new(Instant::now()),
            stale_timeout,
            start_time: Instant::now(),
        }
    }

    /// Update progress.
    pub fn update(&self, bytes: u64) {
        self.bytes_processed.store(bytes, Ordering::Relaxed);
        if let Ok(mut last) = self.last_update.lock() {
            *last = Instant::now();
        }
    }

    /// Add to progress.
    pub fn add(&self, bytes: u64) {
        self.bytes_processed.fetch_add(bytes, Ordering::Relaxed);
        if let Ok(mut last) = self.last_update.lock() {
            *last = Instant::now();
        }
    }

    /// Get current progress.
    pub fn current(&self) -> u64 {
        self.bytes_processed.load(Ordering::Relaxed)
    }

    /// Check if progress is stale.
    pub fn is_stale(&self) -> bool {
        if let Ok(last) = self.last_update.lock() {
            last.elapsed() > self.stale_timeout
        } else {
            false
        }
    }

    /// Get progress percentage.
    pub fn progress_percent(&self) -> Option<f64> {
        self.total_size.map(|total| {
            if total == 0 {
                100.0
            } else {
                let processed = self.bytes_processed.load(Ordering::Relaxed);
                (processed as f64 / total as f64 * 100.0).min(100.0)
            }
        })
    }

    /// Get remaining bytes.
    pub fn remaining(&self) -> Option<u64> {
        self.total_size.map(|total| {
            let processed = self.bytes_processed.load(Ordering::Relaxed);
            total.saturating_sub(processed)
        })
    }

    /// Calculate transfer rate in bytes per second.
    ///
    /// Returns 0 if no time has elapsed or no data transferred.
    pub fn transfer_rate(&self) -> u64 {
        let processed = self.bytes_processed.load(Ordering::Relaxed);
        if processed == 0 {
            return 0;
        }

        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed > 0.0 {
            (processed as f64 / elapsed) as u64
        } else {
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_progress() {
        let progress = OperationProgress::new(Some(1000), Duration::from_secs(5));

        assert_eq!(progress.current(), 0);
        assert_eq!(progress.progress_percent(), Some(0.0));

        progress.update(500);
        assert_eq!(progress.current(), 500);
        assert_eq!(progress.progress_percent(), Some(50.0));

        progress.add(300);
        assert_eq!(progress.current(), 800);
        assert_eq!(progress.remaining(), Some(200));
    }
}
