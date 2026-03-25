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

//! Global performance metrics instance for RustFS.
//!
//! This module provides a singleton instance of `PerformanceMetrics`
//! that can be accessed from anywhere in the codebase for consistent
//! performance monitoring.

use std::sync::{Arc, OnceLock};
use crate::monitoring::PerformanceMetrics;

// Global performance metrics instance.
// This singleton is initialized once and shared across all components
// that need to record performance metrics.
static GLOBAL_PERFORMANCE_METRICS: OnceLock<Arc<PerformanceMetrics>> = OnceLock::new();

/// Get a reference to the global performance metrics instance.
///
/// # Example
///
/// ```rust
/// use rustfs::storage::concurrency::global_metrics::get_global_metrics;
///
/// let metrics = get_global_metrics();
/// metrics.record_cache_hit();
/// ```
pub fn get_global_metrics() -> Arc<PerformanceMetrics> {
    GLOBAL_PERFORMANCE_METRICS
        .get_or_init(|| Arc::new(PerformanceMetrics::new()))
        .clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_global_metrics_instance() {
        let metrics1 = get_global_metrics();
        let metrics2 = get_global_metrics();

        // Both should point to the same instance
        assert!(Arc::ptr_eq(&metrics1, &metrics2));
    }

    #[test]
    fn test_global_metrics_recording() {
        let metrics = get_global_metrics();

        // Record some metrics
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        // Verify they were recorded
        let hits = metrics.cache_hits.load(std::sync::atomic::Ordering::Relaxed);
        let misses = metrics.cache_misses.load(std::sync::atomic::Ordering::Relaxed);

        assert!(hits >= 2);
        assert!(misses >= 1);
    }
}
