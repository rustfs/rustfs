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

//! Capacity metrics recording helpers.

use metrics::{counter, gauge, histogram};
use std::time::Duration;

/// Record capacity cache hit.
#[inline(always)]
pub fn record_capacity_cache_hit() {
    counter!("rustfs.capacity.cache.hits").increment(1);
}

/// Record capacity cache miss.
#[inline(always)]
pub fn record_capacity_cache_miss() {
    counter!("rustfs.capacity.cache.misses").increment(1);
}

/// Record current capacity gauge.
#[inline(always)]
pub fn record_capacity_current_bytes(used_bytes: u64) {
    gauge!("rustfs.capacity.current").set(used_bytes as f64);
}

/// Record capacity update completion.
#[inline(always)]
pub fn record_capacity_update_completed(source: &'static str, duration: Duration, used_bytes: u64, is_estimated: bool) {
    counter!("rustfs.capacity.update.total", "source" => source).increment(1);
    histogram!("rustfs.capacity.update.duration.seconds", "source" => source).record(duration.as_secs_f64());
    histogram!("rustfs.capacity.update.bytes", "source" => source).record(used_bytes as f64);
    counter!(
        "rustfs.capacity.update.estimated.total",
        "source" => source,
        "estimated" => if is_estimated { "true" } else { "false" }
    )
    .increment(1);
}

/// Record failed capacity update.
#[inline(always)]
pub fn record_capacity_update_failed(source: &'static str) {
    counter!("rustfs.capacity.update.failures", "source" => source).increment(1);
}

/// Record capacity write activity.
#[inline(always)]
pub fn record_capacity_write_operation(write_frequency: usize) {
    counter!("rustfs.capacity.write.operations").increment(1);
    gauge!("rustfs.capacity.write.frequency").set(write_frequency as f64);
}

/// Record symlink accounting.
#[inline(always)]
pub fn record_capacity_symlink(size_bytes: u64) {
    counter!("rustfs.capacity.symlinks.encountered").increment(1);
    histogram!("rustfs.capacity.symlinks.size.bytes").record(size_bytes as f64);
}

/// Record timeout fallback event.
#[inline(always)]
pub fn record_capacity_timeout_fallback() {
    counter!("rustfs.capacity.timeout.fallback").increment(1);
}

/// Record stall detection event.
#[inline(always)]
pub fn record_capacity_stall_detected() {
    counter!("rustfs.capacity.timeout.stall").increment(1);
}

/// Record dynamic timeout usage.
#[inline(always)]
pub fn record_capacity_dynamic_timeout(timeout: Duration) {
    counter!("rustfs.capacity.timeout.dynamic").increment(1);
    histogram!("rustfs.capacity.timeout.dynamic.seconds").record(timeout.as_secs_f64());
}

/// Record scan sampling outcome.
#[inline(always)]
pub fn record_capacity_scan_sampling(sampled_count: usize, estimated: bool) {
    histogram!("rustfs.capacity.scan.sampled.count").record(sampled_count as f64);
    counter!(
        "rustfs.capacity.scan.estimated.total",
        "estimated" => if estimated { "true" } else { "false" }
    )
    .increment(1);
}
