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

/// Record how capacity cache was served to the caller.
#[inline(always)]
pub fn record_capacity_cache_served(state: &'static str) {
    counter!("rustfs.capacity.cache.served.total", "state" => state).increment(1);
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

/// Record a capacity refresh request.
#[inline(always)]
pub fn record_capacity_refresh_request(mode: &'static str, source: &'static str) {
    counter!("rustfs.capacity.refresh.requests.total", "mode" => mode, "source" => source).increment(1);
}

/// Record a refresh joiner waiting for an inflight refresh.
#[inline(always)]
pub fn record_capacity_refresh_joiner(source: &'static str) {
    counter!("rustfs.capacity.refresh.joiners.total", "source" => source).increment(1);
}

/// Record the number of inflight capacity refreshes.
#[inline(always)]
pub fn record_capacity_refresh_inflight(count: usize) {
    gauge!("rustfs.capacity.refresh.inflight").set(count as f64);
}

/// Record the final result of a capacity refresh.
#[inline(always)]
pub fn record_capacity_refresh_result(source: &'static str, result: &'static str, duration: Duration) {
    counter!("rustfs.capacity.refresh.result.total", "source" => source, "result" => result).increment(1);
    histogram!("rustfs.capacity.refresh.duration.seconds", "source" => source, "result" => result).record(duration.as_secs_f64());
}

/// Record the refresh scope selected for a capacity refresh.
#[inline(always)]
pub fn record_capacity_refresh_scope(scope: &'static str, disk_count: usize) {
    counter!("rustfs.capacity.refresh.scope.total", "scope" => scope).increment(1);
    histogram!("rustfs.capacity.refresh.scope.disks", "scope" => scope).record(disk_count as f64);
}

/// Record the current number of dirty disks tracked by capacity management.
#[inline(always)]
pub fn record_capacity_dirty_disk_count(count: usize) {
    gauge!("rustfs.capacity.dirty.disks").set(count as f64);
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

/// Record the scan mode used for a capacity result.
#[inline(always)]
pub fn record_capacity_scan_mode(mode: &'static str) {
    counter!("rustfs.capacity.scan.mode.total", "mode" => mode).increment(1);
}

/// Record per-disk capacity scan statistics.
#[inline(always)]
pub fn record_capacity_scan_disk(
    disk: &str,
    duration: Duration,
    file_count: usize,
    sampled_count: usize,
    estimated: bool,
    partial_errors: bool,
) {
    histogram!("rustfs.capacity.scan.disk.duration.seconds", "disk" => disk.to_owned()).record(duration.as_secs_f64());
    histogram!("rustfs.capacity.scan.disk.files", "disk" => disk.to_owned()).record(file_count as f64);
    histogram!("rustfs.capacity.scan.disk.sampled", "disk" => disk.to_owned()).record(sampled_count as f64);
    counter!(
        "rustfs.capacity.scan.disk.estimated.total",
        "disk" => disk.to_owned(),
        "estimated" => if estimated { "true" } else { "false" }
    )
    .increment(1);
    if partial_errors {
        counter!("rustfs.capacity.scan.disk.partial_errors.total", "disk" => disk.to_owned()).increment(1);
    }
}
