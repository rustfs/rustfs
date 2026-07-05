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
const LIST_OBJECTS_INDEX_FALLBACK_TOTAL: &str = "rustfs_s3_list_objects_index_fallback_total";

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
            LIST_OBJECTS_INDEX_FALLBACK_TOTAL,
            "Total number of opt-in ListObjects index attempts that fell back to the live walker."
        );
    });
}

pub fn record_list_objects_gather(observation: ListObjectsGatherObservation) {
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
    counter!(
        LIST_OBJECTS_INDEX_FALLBACK_TOTAL,
        "source" => source,
        "reason" => reason
    )
    .increment(1);
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
}
