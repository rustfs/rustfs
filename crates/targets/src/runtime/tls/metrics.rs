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

//! Target-level TLS reload metrics.

use ::metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

const TARGET_TLS_RELOAD_TOTAL: &str = "rustfs_target_tls_reload_total";
const TARGET_TLS_RELOAD_SKIPPED_TOTAL: &str = "rustfs_target_tls_reload_skipped_total";
const TARGET_TLS_GENERATION: &str = "rustfs_target_tls_generation";
const TARGET_TLS_RELOAD_DURATION_SECONDS: &str = "rustfs_target_tls_reload_duration_seconds";
const TARGET_TLS_PUBLICATION_FAIL_TOTAL: &str = "rustfs_target_tls_publication_fail_total";

/// Describes all target TLS metrics. Call once during initialization.
pub fn init_target_tls_metrics() {
    describe_counter!(TARGET_TLS_RELOAD_TOTAL, "Total number of TLS reload attempts per target");
    describe_counter!(
        TARGET_TLS_RELOAD_SKIPPED_TOTAL,
        "Number of TLS reloads skipped per target (unchanged, etc.)"
    );
    describe_gauge!(TARGET_TLS_GENERATION, "Current TLS generation per target");
    describe_histogram!(TARGET_TLS_RELOAD_DURATION_SECONDS, "Duration of TLS reload attempts per target");
    describe_counter!(TARGET_TLS_PUBLICATION_FAIL_TOTAL, "Number of TLS reload publication failures per target");
}

/// Records a TLS reload result (success or failure).
pub fn record_target_tls_reload_result(target: &str, result: &str, duration_secs: f64, generation: u64) {
    counter!(TARGET_TLS_RELOAD_TOTAL, "target_id" => target.to_string(), "result" => result.to_string()).increment(1);
    histogram!(TARGET_TLS_RELOAD_DURATION_SECONDS, "target_id" => target.to_string(), "result" => result.to_string())
        .record(duration_secs);
    gauge!(TARGET_TLS_GENERATION, "target_id" => target.to_string()).set(generation as f64);
}

/// Records a skipped reload (typically because fingerprint was unchanged).
pub fn record_target_tls_reload_skipped(target: &str, reason: &str) {
    counter!(TARGET_TLS_RELOAD_SKIPPED_TOTAL, "target_id" => target.to_string(), "reason" => reason.to_string()).increment(1);
}

/// Records a TLS reload publication failure.
pub fn record_target_tls_publication_fail(target: &str) {
    counter!(TARGET_TLS_PUBLICATION_FAIL_TOTAL, "target_id" => target.to_string()).increment(1);
}
