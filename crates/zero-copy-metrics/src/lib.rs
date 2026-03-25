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
//! operations. It has no dependencies on other RustFS crates, making it safe to
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
}
