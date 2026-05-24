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

use metrics::{counter, describe_counter, describe_gauge, describe_histogram, gauge, histogram};

pub const TLS_RUNTIME_FOUNDATION_CONSUMER: &str = "tls_runtime_foundation";
pub const TLS_OUTBOUND_GLOBAL_CONSUMER: &str = "outbound_global";

const CONSUMER_LABEL: &str = "consumer";
const RESULT_LABEL: &str = "result";
const REASON_LABEL: &str = "reason";

const TLS_OUTBOUND_PUBLICATIONS_TOTAL: &str = "rustfs_tls_outbound_publications_total";
const TLS_OUTBOUND_GENERATION: &str = "rustfs_tls_outbound_generation";
const TLS_OUTBOUND_HAS_ROOT_CA: &str = "rustfs_tls_outbound_has_root_ca";
const TLS_OUTBOUND_HAS_MTLS_IDENTITY: &str = "rustfs_tls_outbound_has_mtls_identity";
const TLS_GENERATION: &str = "rustfs_tls_generation";
const TLS_RELOAD_TOTAL: &str = "rustfs_tls_reload_total";
const TLS_RELOAD_DURATION_SECONDS: &str = "rustfs_tls_reload_duration_seconds";
const TLS_RELOAD_GENERATION: &str = "rustfs_tls_reload_generation";
const TLS_RELOAD_SKIPPED_TOTAL: &str = "rustfs_tls_reload_skipped_total";
const TLS_PUBLICATION_FAIL_TOTAL: &str = "rustfs_tls_publication_fail_total";
const TLS_CONSUMER_STALE_GENERATION_TOTAL: &str = "rustfs_tls_consumer_stale_generation_total";

pub fn record_outbound_tls_publication(generation: u64, has_root_ca: bool, has_mtls_identity: bool) {
    counter!(TLS_OUTBOUND_PUBLICATIONS_TOTAL, "result" => "ok").increment(1);
    gauge!(TLS_OUTBOUND_GENERATION).set(generation as f64);
    gauge!(TLS_OUTBOUND_HAS_ROOT_CA).set(if has_root_ca { 1.0 } else { 0.0 });
    gauge!(TLS_OUTBOUND_HAS_MTLS_IDENTITY).set(if has_mtls_identity { 1.0 } else { 0.0 });
    record_tls_generation(TLS_OUTBOUND_GLOBAL_CONSUMER, generation);
}

pub fn record_tls_generation(consumer: &'static str, generation: u64) {
    gauge!(TLS_GENERATION, CONSUMER_LABEL => consumer).set(generation as f64);
}

pub fn record_tls_reload_result(
    consumer: &'static str,
    result: &'static str,
    duration_secs: Option<f64>,
    generation: Option<u64>,
) {
    counter!(TLS_RELOAD_TOTAL, CONSUMER_LABEL => consumer, RESULT_LABEL => result).increment(1);
    if let Some(duration_secs) = duration_secs {
        histogram!(TLS_RELOAD_DURATION_SECONDS, CONSUMER_LABEL => consumer).record(duration_secs);
    }
    if let Some(generation) = generation {
        gauge!(TLS_RELOAD_GENERATION, CONSUMER_LABEL => consumer).set(generation as f64);
        record_tls_generation(consumer, generation);
    }
}

pub fn record_tls_reload_skipped(consumer: &'static str, reason: &'static str) {
    counter!(TLS_RELOAD_SKIPPED_TOTAL, CONSUMER_LABEL => consumer, REASON_LABEL => reason).increment(1);
}

pub fn record_tls_publication_fail(consumer: &'static str) {
    counter!(TLS_PUBLICATION_FAIL_TOTAL, CONSUMER_LABEL => consumer).increment(1);
}

pub fn record_tls_consumer_stale_generation(consumer: &'static str) {
    counter!(TLS_CONSUMER_STALE_GENERATION_TOTAL, CONSUMER_LABEL => consumer).increment(1);
}

pub fn init_tls_metrics() {
    describe_counter!(TLS_OUTBOUND_PUBLICATIONS_TOTAL, "Total TLS outbound publications, labeled by result.");
    describe_gauge!(TLS_OUTBOUND_GENERATION, "Current outbound TLS generation.");
    describe_gauge!(TLS_OUTBOUND_HAS_ROOT_CA, "Whether outbound TLS roots are configured.");
    describe_gauge!(TLS_OUTBOUND_HAS_MTLS_IDENTITY, "Whether outbound mTLS identity is configured.");
    describe_gauge!(TLS_GENERATION, "Current TLS generation by consumer.");
    describe_counter!(TLS_RELOAD_TOTAL, "Total TLS reload attempts, labeled by consumer and result.");
    describe_histogram!(TLS_RELOAD_DURATION_SECONDS, "TLS reload duration by consumer (seconds).");
    describe_gauge!(TLS_RELOAD_GENERATION, "TLS generation after reload by consumer.");
    describe_counter!(TLS_RELOAD_SKIPPED_TOTAL, "Total skipped TLS reloads, labeled by consumer and reason.");
    describe_counter!(TLS_PUBLICATION_FAIL_TOTAL, "Total TLS publication failures by consumer.");
    describe_counter!(TLS_CONSUMER_STALE_GENERATION_TOTAL, "Total stale TLS consumer generations observed.");
}
