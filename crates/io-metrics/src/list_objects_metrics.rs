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

use metrics::{counter, describe_counter, describe_histogram, histogram};
use std::sync::OnceLock;

use crate::get_stage_metrics_enabled;

pub const LIST_OBJECTS_SOURCE_WALKER: &str = "walker";
pub const LIST_OBJECTS_GATHER_OUTCOME_LIMIT_REACHED: &str = "limit_reached";
pub const LIST_OBJECTS_GATHER_OUTCOME_INPUT_CLOSED: &str = "input_closed";
pub const LIST_OBJECTS_MERGE_OUTCOME_STARTED: &str = "started";

const LIST_OBJECTS_GATHER_TOTAL: &str = "rustfs_s3_list_objects_gather_total";
const LIST_OBJECTS_GATHER_DURATION_MS: &str = "rustfs_s3_list_objects_gather_duration_ms";
const LIST_OBJECTS_GATHER_SCANNED_ENTRIES: &str = "rustfs_s3_list_objects_gather_scanned_entries";
const LIST_OBJECTS_GATHER_RETURNED_ENTRIES: &str = "rustfs_s3_list_objects_gather_returned_entries";
const LIST_OBJECTS_GATHER_FILTERED_ENTRIES: &str = "rustfs_s3_list_objects_gather_filtered_entries";
const LIST_OBJECTS_GATHER_SCAN_AMPLIFICATION: &str = "rustfs_s3_list_objects_gather_scan_amplification";
const LIST_OBJECTS_GATHER_LIMIT: &str = "rustfs_s3_list_objects_gather_limit";
const LIST_OBJECTS_MERGE_FAN_IN: &str = "rustfs_s3_list_objects_merge_fan_in";
const LIST_OBJECTS_MERGE_READ_QUORUM: &str = "rustfs_s3_list_objects_merge_read_quorum";
const LIST_OBJECTS_INDEX_ATTEMPT_TOTAL: &str = "rustfs_s3_list_objects_index_attempt_total";
const LIST_OBJECTS_INDEX_FALLBACK_TOTAL: &str = "rustfs_s3_list_objects_index_fallback_total";
const LIST_OBJECTS_INDEX_SERVED_TOTAL: &str = "rustfs_s3_list_objects_index_served_total";
const LIST_OBJECTS_INDEX_CANDIDATE_KEYS: &str = "rustfs_s3_list_objects_index_candidate_keys";
const LIST_OBJECTS_INDEX_LIVE_VERIFY_ATTEMPTS: &str = "rustfs_s3_list_objects_index_live_verify_attempts";
const LIST_OBJECTS_INDEX_LIVE_VERIFY_HITS: &str = "rustfs_s3_list_objects_index_live_verify_hits";
const LIST_OBJECTS_INDEX_LIVE_VERIFY_MISSES: &str = "rustfs_s3_list_objects_index_live_verify_misses";
const LIST_OBJECTS_INDEX_LIVE_VERIFY_FAILURE_TOTAL: &str = "rustfs_s3_list_objects_index_live_verify_failure_total";
const LIST_OBJECTS_INDEX_RETURNED_OBJECTS: &str = "rustfs_s3_list_objects_index_returned_objects";
const LIST_OBJECTS_INDEX_RETURNED_PREFIXES: &str = "rustfs_s3_list_objects_index_returned_prefixes";
const LIST_OBJECTS_INDEX_VERIFICATION_IO_AMPLIFICATION: &str = "rustfs_s3_list_objects_index_verification_io_amplification";

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ListObjectsGatherObservation {
    pub source: &'static str,
    pub outcome: &'static str,
    pub limit: i32,
    pub scanned_entries: usize,
    pub returned_entries: usize,
    pub duration_ms: f64,
    pub has_prefix: bool,
    pub has_delimiter: bool,
    pub has_marker: bool,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ListObjectsIndexPageObservation {
    pub source: &'static str,
    pub provider: &'static str,
    pub candidate_keys: usize,
    pub live_verify_attempts: usize,
    pub live_verify_hits: usize,
    pub live_verify_misses: usize,
    pub returned_objects: usize,
    pub returned_prefixes: usize,
    pub is_truncated: bool,
}

#[inline(always)]
fn bool_label(value: bool) -> &'static str {
    if value { "true" } else { "false" }
}

#[inline(always)]
fn count_as_f64(value: usize) -> f64 {
    value as f64
}

#[inline(always)]
fn limit_as_f64(value: i32) -> f64 {
    value.max(0) as f64
}

pub fn init_list_objects_metrics() {
    static METRICS_DESC_INIT: OnceLock<()> = OnceLock::new();
    METRICS_DESC_INIT.get_or_init(|| {
        describe_counter!(
            LIST_OBJECTS_GATHER_TOTAL,
            "Total number of ListObjects gather phases by source, outcome, and request shape."
        );
        describe_histogram!(LIST_OBJECTS_GATHER_DURATION_MS, "ListObjects gather phase duration in milliseconds.");
        describe_histogram!(
            LIST_OBJECTS_GATHER_SCANNED_ENTRIES,
            "Number of entries consumed by ListObjects gather before producing a page."
        );
        describe_histogram!(
            LIST_OBJECTS_GATHER_RETURNED_ENTRIES,
            "Number of entries returned by ListObjects gather before pagination trimming."
        );
        describe_histogram!(
            LIST_OBJECTS_GATHER_FILTERED_ENTRIES,
            "Number of entries filtered by ListObjects gather before producing a page."
        );
        describe_histogram!(
            LIST_OBJECTS_GATHER_SCAN_AMPLIFICATION,
            "Ratio of scanned entries to returned entries in ListObjects gather."
        );
        describe_histogram!(LIST_OBJECTS_GATHER_LIMIT, "Requested internal ListObjects gather page limit.");
        describe_histogram!(LIST_OBJECTS_MERGE_FAN_IN, "Number of input streams merged for ListObjects pages.");
        describe_histogram!(LIST_OBJECTS_MERGE_READ_QUORUM, "Read quorum used while merging ListObjects entries.");
        describe_counter!(
            LIST_OBJECTS_INDEX_ATTEMPT_TOTAL,
            "Total number of opt-in ListObjects index serving attempts by source, provider, and request shape."
        );
        describe_counter!(
            LIST_OBJECTS_INDEX_FALLBACK_TOTAL,
            "Total number of opt-in ListObjects index attempts that fell back to the live walker."
        );
        describe_counter!(
            LIST_OBJECTS_INDEX_SERVED_TOTAL,
            "Total number of ListObjects pages served by an opt-in index provider."
        );
        describe_histogram!(
            LIST_OBJECTS_INDEX_CANDIDATE_KEYS,
            "Number of candidate keys proposed by the opt-in ListObjects index provider per page."
        );
        describe_histogram!(
            LIST_OBJECTS_INDEX_LIVE_VERIFY_ATTEMPTS,
            "Number of live xl.meta verification attempts made by opt-in ListObjects index serving per page."
        );
        describe_histogram!(
            LIST_OBJECTS_INDEX_LIVE_VERIFY_HITS,
            "Number of opt-in ListObjects index candidates accepted by live xl.meta verification per page."
        );
        describe_histogram!(
            LIST_OBJECTS_INDEX_LIVE_VERIFY_MISSES,
            "Number of opt-in ListObjects index candidates rejected as stale or missing by live xl.meta verification per page."
        );
        describe_counter!(
            LIST_OBJECTS_INDEX_LIVE_VERIFY_FAILURE_TOTAL,
            "Total number of opt-in ListObjects index live xl.meta verification failures."
        );
        describe_histogram!(
            LIST_OBJECTS_INDEX_RETURNED_OBJECTS,
            "Number of objects returned by opt-in ListObjects index serving per page."
        );
        describe_histogram!(
            LIST_OBJECTS_INDEX_RETURNED_PREFIXES,
            "Number of common prefixes returned by opt-in ListObjects index serving per page."
        );
        describe_histogram!(
            LIST_OBJECTS_INDEX_VERIFICATION_IO_AMPLIFICATION,
            "Ratio of live verification attempts to returned objects for opt-in ListObjects index serving."
        );
    });
}

pub fn record_list_objects_gather(observation: ListObjectsGatherObservation) {
    if !get_stage_metrics_enabled() {
        return;
    }

    let filtered_entries = observation.scanned_entries.saturating_sub(observation.returned_entries);
    let scan_amplification = if observation.returned_entries == 0 {
        count_as_f64(observation.scanned_entries)
    } else {
        count_as_f64(observation.scanned_entries) / count_as_f64(observation.returned_entries)
    };

    counter!(
        LIST_OBJECTS_GATHER_TOTAL,
        "source" => observation.source,
        "outcome" => observation.outcome,
        "has_prefix" => bool_label(observation.has_prefix),
        "has_delimiter" => bool_label(observation.has_delimiter),
        "has_marker" => bool_label(observation.has_marker),
    )
    .increment(1);
    histogram!(
        LIST_OBJECTS_GATHER_DURATION_MS,
        "source" => observation.source,
        "outcome" => observation.outcome
    )
    .record(observation.duration_ms);
    histogram!(LIST_OBJECTS_GATHER_SCANNED_ENTRIES, "source" => observation.source)
        .record(count_as_f64(observation.scanned_entries));
    histogram!(LIST_OBJECTS_GATHER_RETURNED_ENTRIES, "source" => observation.source)
        .record(count_as_f64(observation.returned_entries));
    histogram!(LIST_OBJECTS_GATHER_FILTERED_ENTRIES, "source" => observation.source).record(count_as_f64(filtered_entries));
    histogram!(LIST_OBJECTS_GATHER_SCAN_AMPLIFICATION, "source" => observation.source).record(scan_amplification);
    histogram!(LIST_OBJECTS_GATHER_LIMIT, "source" => observation.source).record(limit_as_f64(observation.limit));
}

pub fn record_list_objects_merge(source: &'static str, input_channels: usize, read_quorum: usize) {
    if !get_stage_metrics_enabled() {
        return;
    }

    histogram!(
        LIST_OBJECTS_MERGE_FAN_IN,
        "source" => source,
        "outcome" => LIST_OBJECTS_MERGE_OUTCOME_STARTED
    )
    .record(count_as_f64(input_channels));
    histogram!(
        LIST_OBJECTS_MERGE_READ_QUORUM,
        "source" => source,
        "outcome" => LIST_OBJECTS_MERGE_OUTCOME_STARTED
    )
    .record(count_as_f64(read_quorum));
}

pub fn record_list_objects_index_fallback(source: &'static str, reason: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }

    counter!(
        LIST_OBJECTS_INDEX_FALLBACK_TOTAL,
        "source" => source,
        "reason" => reason
    )
    .increment(1);
}

pub fn record_list_objects_index_attempt(
    source: &'static str,
    provider: &'static str,
    has_prefix: bool,
    has_delimiter: bool,
    has_marker: bool,
) {
    if !get_stage_metrics_enabled() {
        return;
    }

    counter!(
        LIST_OBJECTS_INDEX_ATTEMPT_TOTAL,
        "source" => source,
        "provider" => provider,
        "has_prefix" => bool_label(has_prefix),
        "has_delimiter" => bool_label(has_delimiter),
        "has_marker" => bool_label(has_marker),
    )
    .increment(1);
}

pub fn record_list_objects_index_live_verify_failure(source: &'static str, reason: &'static str) {
    if !get_stage_metrics_enabled() {
        return;
    }

    counter!(
        LIST_OBJECTS_INDEX_LIVE_VERIFY_FAILURE_TOTAL,
        "source" => source,
        "reason" => reason,
    )
    .increment(1);
}

pub fn record_list_objects_index_served(observation: ListObjectsIndexPageObservation) {
    if !get_stage_metrics_enabled() {
        return;
    }

    let returned_objects = count_as_f64(observation.returned_objects);
    let verification_io_amplification = if observation.returned_objects == 0 {
        count_as_f64(observation.live_verify_attempts)
    } else {
        count_as_f64(observation.live_verify_attempts) / returned_objects
    };

    counter!(
        LIST_OBJECTS_INDEX_SERVED_TOTAL,
        "source" => observation.source,
        "provider" => observation.provider,
        "is_truncated" => bool_label(observation.is_truncated),
    )
    .increment(1);
    histogram!(
        LIST_OBJECTS_INDEX_CANDIDATE_KEYS,
        "source" => observation.source,
        "provider" => observation.provider,
    )
    .record(count_as_f64(observation.candidate_keys));
    histogram!(
        LIST_OBJECTS_INDEX_LIVE_VERIFY_ATTEMPTS,
        "source" => observation.source,
        "provider" => observation.provider,
    )
    .record(count_as_f64(observation.live_verify_attempts));
    histogram!(
        LIST_OBJECTS_INDEX_LIVE_VERIFY_HITS,
        "source" => observation.source,
        "provider" => observation.provider,
    )
    .record(count_as_f64(observation.live_verify_hits));
    histogram!(
        LIST_OBJECTS_INDEX_LIVE_VERIFY_MISSES,
        "source" => observation.source,
        "provider" => observation.provider,
    )
    .record(count_as_f64(observation.live_verify_misses));
    histogram!(
        LIST_OBJECTS_INDEX_RETURNED_OBJECTS,
        "source" => observation.source,
        "provider" => observation.provider,
    )
    .record(returned_objects);
    histogram!(
        LIST_OBJECTS_INDEX_RETURNED_PREFIXES,
        "source" => observation.source,
        "provider" => observation.provider,
    )
    .record(count_as_f64(observation.returned_prefixes));
    histogram!(
        LIST_OBJECTS_INDEX_VERIFICATION_IO_AMPLIFICATION,
        "source" => observation.source,
        "provider" => observation.provider,
    )
    .record(verification_io_amplification);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_gather_observation_handles_empty_page() {
        init_list_objects_metrics();
        record_list_objects_gather(ListObjectsGatherObservation {
            source: LIST_OBJECTS_SOURCE_WALKER,
            outcome: LIST_OBJECTS_GATHER_OUTCOME_INPUT_CLOSED,
            limit: 1001,
            scanned_entries: 42,
            returned_entries: 0,
            duration_ms: 3.5,
            has_prefix: true,
            has_delimiter: false,
            has_marker: true,
        });
    }

    #[test]
    fn record_merge_observation_accepts_zero_quorum() {
        init_list_objects_metrics();
        record_list_objects_merge(LIST_OBJECTS_SOURCE_WALKER, 4, 0);
    }

    #[test]
    fn record_index_fallback_observation_accepts_reason() {
        init_list_objects_metrics();
        record_list_objects_index_fallback("index_key_only", "unsupported_request");
    }

    #[test]
    fn record_index_attempt_and_served_observations_accept_counts() {
        init_list_objects_metrics();
        record_list_objects_index_attempt("index_key_only", "walker_key_only", true, true, false);
        record_list_objects_index_served(ListObjectsIndexPageObservation {
            source: "index_key_only",
            provider: "walker_key_only",
            candidate_keys: 1000,
            live_verify_attempts: 700,
            live_verify_hits: 650,
            live_verify_misses: 50,
            returned_objects: 600,
            returned_prefixes: 10,
            is_truncated: true,
        });
        record_list_objects_index_live_verify_failure("index_key_only", "read_error");
    }
}
