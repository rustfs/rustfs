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

use metrics::{counter, gauge, histogram};

const TLS_OUTBOUND_PUBLICATIONS_TOTAL: &str = "rustfs_tls_outbound_publications_total";
const TLS_OUTBOUND_GENERATION: &str = "rustfs_tls_outbound_generation";
const TLS_OUTBOUND_HAS_ROOT_CA: &str = "rustfs_tls_outbound_has_root_ca";
const TLS_OUTBOUND_HAS_MTLS_IDENTITY: &str = "rustfs_tls_outbound_has_mtls_identity";
const TLS_RELOAD_TOTAL: &str = "rustfs_tls_reload_total";
const TLS_RELOAD_DURATION_SECONDS: &str = "rustfs_tls_reload_duration_seconds";
const TLS_RELOAD_GENERATION: &str = "rustfs_tls_reload_generation";
const TLS_RELOAD_SKIPPED_TOTAL: &str = "rustfs_tls_reload_skipped_total";

pub fn record_outbound_tls_publication(generation: u64, has_root_ca: bool, has_mtls_identity: bool) {
    counter!(TLS_OUTBOUND_PUBLICATIONS_TOTAL, "result" => "ok").increment(1);
    gauge!(TLS_OUTBOUND_GENERATION).set(generation as f64);
    gauge!(TLS_OUTBOUND_HAS_ROOT_CA).set(if has_root_ca { 1.0 } else { 0.0 });
    gauge!(TLS_OUTBOUND_HAS_MTLS_IDENTITY).set(if has_mtls_identity { 1.0 } else { 0.0 });
}

pub fn record_tls_reload_result(result: &'static str, duration_secs: Option<f64>, generation: Option<u64>) {
    counter!(TLS_RELOAD_TOTAL, "result" => result.to_string()).increment(1);
    if let Some(duration_secs) = duration_secs {
        histogram!(TLS_RELOAD_DURATION_SECONDS).record(duration_secs);
    }
    if let Some(generation) = generation {
        gauge!(TLS_RELOAD_GENERATION).set(generation as f64);
    }
}

pub fn record_tls_reload_skipped(reason: &'static str) {
    counter!(TLS_RELOAD_SKIPPED_TOTAL, "reason" => reason.to_string()).increment(1);
}
