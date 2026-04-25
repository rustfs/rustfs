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
    counter!("rustfs_capacity_cache_hits").increment(1);
}

/// Record capacity cache miss.
#[inline(always)]
pub fn record_capacity_cache_miss() {
    counter!("rustfs_capacity_cache_misses").increment(1);
}

/// Record how capacity cache was served to the caller.
#[inline(always)]
pub fn record_capacity_cache_served(state: &'static str) {
    counter!("rustfs_capacity_cache_served_total", "state" => state).increment(1);
}

/// Record current capacity gauge.
#[inline(always)]
pub fn record_capacity_current_bytes(used_bytes: u64) {
    gauge!("rustfs_capacity_current").set(used_bytes as f64);
}

/// Record capacity update completion.
#[inline(always)]
pub fn record_capacity_update_completed(source: &'static str, duration: Duration, used_bytes: u64, is_estimated: bool) {
    counter!("rustfs_capacity_update_total", "source" => source).increment(1);
    histogram!("rustfs_capacity_update_duration_seconds", "source" => source).record(duration.as_secs_f64());
    histogram!("rustfs_capacity_update_bytes", "source" => source).record(used_bytes as f64);
    counter!(
        "rustfs_capacity_update_estimated_total",
        "source" => source,
        "estimated" => if is_estimated { "true" } else { "false" }
    )
    .increment(1);
}

/// Record failed capacity update.
#[inline(always)]
pub fn record_capacity_update_failed(source: &'static str) {
    counter!("rustfs_capacity_update_failures", "source" => source).increment(1);
}

/// Record a capacity refresh request.
#[inline(always)]
pub fn record_capacity_refresh_request(mode: &'static str, source: &'static str) {
    counter!("rustfs_capacity_refresh_requests_total", "mode" => mode, "source" => source).increment(1);
}

/// Record a refresh joiner waiting for an inflight refresh.
#[inline(always)]
pub fn record_capacity_refresh_joiner(source: &'static str) {
    counter!("rustfs_capacity_refresh_joiners_total", "source" => source).increment(1);
}

/// Record the number of inflight capacity refreshes.
#[inline(always)]
pub fn record_capacity_refresh_inflight(count: usize) {
    gauge!("rustfs_capacity_refresh_inflight").set(count as f64);
}

/// Record the final result of a capacity refresh.
#[inline(always)]
pub fn record_capacity_refresh_result(source: &'static str, result: &'static str, duration: Duration) {
    counter!("rustfs_capacity_refresh_result_total", "source" => source, "result" => result).increment(1);
    histogram!("rustfs_capacity_refresh_duration_seconds", "source" => source, "result" => result).record(duration.as_secs_f64());
}

/// Record the refresh scope selected for a capacity refresh.
#[inline(always)]
pub fn record_capacity_refresh_scope(scope: &'static str, disk_count: usize) {
    counter!("rustfs_capacity_refresh_scope_total", "scope" => scope).increment(1);
    histogram!("rustfs_capacity_refresh_scope_disks", "scope" => scope).record(disk_count as f64);
}

/// Record the current number of dirty disks tracked by capacity management.
#[inline(always)]
pub fn record_capacity_dirty_disk_count(count: usize) {
    gauge!("rustfs_capacity_dirty_disks").set(count as f64);
}

/// Record capacity write activity.
#[inline(always)]
pub fn record_capacity_write_operation(write_frequency: usize) {
    counter!("rustfs_capacity_write_operations").increment(1);
    gauge!("rustfs_capacity_write_frequency").set(write_frequency as f64);
}

/// Record symlink accounting.
#[inline(always)]
pub fn record_capacity_symlink(size_bytes: u64) {
    counter!("rustfs_capacity_symlinks_encountered").increment(1);
    histogram!("rustfs_capacity_symlinks_size_bytes").record(size_bytes as f64);
}

/// Record timeout fallback event.
#[inline(always)]
pub fn record_capacity_timeout_fallback() {
    counter!("rustfs_capacity_timeout_fallback").increment(1);
}

/// Record stall detection event.
#[inline(always)]
pub fn record_capacity_stall_detected() {
    counter!("rustfs_capacity_timeout_stall").increment(1);
}

/// Record dynamic timeout usage.
#[inline(always)]
pub fn record_capacity_dynamic_timeout(timeout: Duration) {
    counter!("rustfs_capacity_timeout_dynamic").increment(1);
    histogram!("rustfs_capacity_timeout_dynamic_seconds").record(timeout.as_secs_f64());
}

/// Record scan sampling outcome.
#[inline(always)]
pub fn record_capacity_scan_sampling(sampled_count: usize, estimated: bool) {
    histogram!("rustfs_capacity_scan_sampled_count").record(sampled_count as f64);
    counter!(
        "rustfs_capacity_scan_estimated_total",
        "estimated" => if estimated { "true" } else { "false" }
    )
    .increment(1);
}

/// Record the scan mode used for a capacity result.
#[inline(always)]
pub fn record_capacity_scan_mode(mode: &'static str) {
    counter!("rustfs_capacity_scan_mode_total", "mode" => mode).increment(1);
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
    histogram!("rustfs_capacity_scan_disk_duration_seconds", "disk" => disk.to_owned()).record(duration.as_secs_f64());
    histogram!("rustfs_capacity_scan_disk_files", "disk" => disk.to_owned()).record(file_count as f64);
    histogram!("rustfs_capacity_scan_disk_sampled", "disk" => disk.to_owned()).record(sampled_count as f64);
    counter!(
        "rustfs_capacity_scan_disk_estimated_total",
        "disk" => disk.to_owned(),
        "estimated" => if estimated { "true" } else { "false" }
    )
    .increment(1);
    if partial_errors {
        counter!("rustfs_capacity_scan_disk_partial_errors_total", "disk" => disk.to_owned()).increment(1);
    }
}
