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

//! Zero-copy metrics helpers for RustFS.
//!
//! This crate provides lightweight metrics recording functions for zero-copy
//! and buffer pool operations. It has no dependencies on other RustFS crates, making it safe to
//! use from anywhere in the codebase without creating cyclic dependencies.

/// Record a zero-copy read operation.
///
/// # Arguments
///
/// * `size_bytes` - Size of the data read in bytes
/// * `duration_ms` - Time taken for the read operation in milliseconds
#[inline(always)]
pub fn record_zero_copy_read(size_bytes: usize, duration_ms: f64) {
    use metrics::{counter, histogram};

    counter!("rustfs.zero_copy.reads.total").increment(1);
    histogram!("rustfs.zero_copy.read.size.bytes").record(size_bytes as f64);
    histogram!("rustfs.zero_copy.read.duration.ms").record(duration_ms);
}

/// Record memory copies avoided by using zero-copy.
///
/// # Arguments
///
/// * `bytes_saved` - Number of bytes that would have been copied without zero-copy
#[inline(always)]
pub fn record_memory_copy_saved(bytes_saved: usize) {
    use metrics::counter;
    counter!("rustfs.zero_copy.memory.saved.bytes").increment(bytes_saved as u64);
}

/// Record a fallback from zero-copy to regular read.
///
/// This happens when zero-copy read fails (e.g., mmap not available,
/// file too large, etc.) and the system falls back to regular I/O.
///
/// # Arguments
///
/// * `reason` - Reason for the fallback (e.g., "mmap_unavailable", "file_too_large")
#[inline(always)]
pub fn record_zero_copy_fallback(reason: &str) {
    use metrics::counter;
    counter!("rustfs.zero_copy.fallback.total", "reason" => reason.to_string()).increment(1);
}

// ============================================================================
// BytesPool Metrics
// ============================================================================

/// Record BytesPool buffer acquisition.
///
/// # Arguments
///
/// * `tier` - Pool tier ("small", "medium", "large", "xlarge")
/// * `size` - Buffer size acquired
/// * `from_pool` - Whether buffer was reused from pool
#[inline(always)]
pub fn record_bytes_pool_acquire(tier: &str, size: usize, from_pool: bool) {
    use metrics::{counter, gauge};

    counter!("rustfs.bytes.pool.acquisitions.total", "tier" => tier.to_string()).increment(1);
    gauge!("rustfs.bytes.pool.size.bytes", "tier" => tier.to_string()).set(size as f64);

    if from_pool {
        counter!("rustfs.bytes.pool.hits.total", "tier" => tier.to_string()).increment(1);
    } else {
        counter!("rustfs.bytes.pool.misses.total", "tier" => tier.to_string()).increment(1);
    }
}

/// Record BytesPool buffer return.
///
/// # Arguments
///
/// * `tier` - Pool tier ("small", "medium", "large", "xlarge")
#[inline(always)]
pub fn record_bytes_pool_return(tier: &str) {
    use metrics::counter;
    counter!("rustfs.bytes.pool.returns.total", "tier" => tier.to_string()).increment(1);
}

/// Record current BytesPool allocated bytes.
///
/// # Arguments
///
/// * `tier` - Pool tier
/// * `bytes` - Currently allocated bytes
#[inline(always)]
pub fn record_bytes_pool_allocated(tier: &str, bytes: u64) {
    use metrics::gauge;
    gauge!("rustfs.bytes.pool.allocated.bytes", "tier" => tier.to_string()).set(bytes as f64);
}

/// Get BytesPool hit rate as a gauge metric.
///
/// # Arguments
///
/// * `tier` - Pool tier
/// * `hit_rate` - Hit rate (0.0 - 1.0)
#[inline(always)]
pub fn record_bytes_pool_hit_rate(tier: &str, hit_rate: f64) {
    use metrics::gauge;
    gauge!("rustfs.bytes.pool.hit.rate", "tier" => tier.to_string()).set(hit_rate * 100.0);
}

/// Record zero-copy write operation.
///
/// # Arguments
///
/// * `size_bytes` - Size of the data written in bytes
/// * `duration_ms` - Time taken for the write operation in milliseconds
#[inline(always)]
pub fn record_zero_copy_write(size_bytes: usize, duration_ms: f64) {
    use metrics::{counter, histogram};

    counter!("rustfs.zero_copy.write.total").increment(1);
    histogram!("rustfs.zero_copy.write.size.bytes").record(size_bytes as f64);
    histogram!("rustfs.zero_copy.write.duration.ms").record(duration_ms);
}

/// Record zero-copy write fallback.
///
/// This happens when zero-copy write fails and the system falls back to regular I/O.
///
/// # Arguments
///
/// * `reason` - Reason for the fallback
#[inline(always)]
pub fn record_zero_copy_write_fallback(reason: &str) {
    use metrics::counter;
    counter!("rustfs.zero_copy.write.fallback.total", "reason" => reason.to_string()).increment(1);
}

/// Record bytes saved from zero-copy.
///
/// # Arguments
///
/// * `size_bytes` - Number of bytes saved from zero-copy
#[inline(always)]
pub fn record_bytes_saved(size_bytes: usize) {
    use metrics::counter;
    counter!("rustfs.zero_copy.bytes.saved.total").increment(size_bytes as u64);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_record_zero_copy_read() {
        // These should not panic
        record_zero_copy_read(1024, 10.5);
        record_memory_copy_saved(1024);
        record_zero_copy_fallback("test");
    }

    #[test]
    fn test_record_bytes_pool_metrics() {
        // These should not panic
        record_bytes_pool_acquire("small", 4096, true);
        record_bytes_pool_return("small");
        record_bytes_pool_allocated("small", 4096);
        record_bytes_pool_hit_rate("small", 0.85);
    }

    #[test]
    fn test_record_zero_copy_write() {
        // These should not panic
        record_zero_copy_write(1024, 10.5);
        record_zero_copy_write_fallback("test");
        record_bytes_saved(1024);
    }
}
