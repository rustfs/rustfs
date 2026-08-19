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
const LIST_OBJECTS_LOCAL_READ_DIR_TOTAL: &str = "rustfs_s3_list_objects_local_read_dir_total";
const LIST_OBJECTS_LOCAL_READ_DIR_DURATION_MS: &str = "rustfs_s3_list_objects_local_read_dir_duration_ms";
const LIST_OBJECTS_LOCAL_READ_DIR_ENTRIES: &str = "rustfs_s3_list_objects_local_read_dir_entries";
const LIST_OBJECTS_LOCAL_READ_DIR_LIMIT: &str = "rustfs_s3_list_objects_local_read_dir_limit";

pub const LIST_OBJECTS_LOCAL_READ_DIR_OUTCOME_OK: &str = "ok";
pub const LIST_OBJECTS_LOCAL_READ_DIR_OUTCOME_ERROR: &str = "error";

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

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ListObjectsLocalReadDirObservation {
    pub outcome: &'static str,
    pub requested_count: i32,
    pub returned_entries: usize,
    pub duration_ms: f64,
    pub is_root: bool,
    pub has_filter_prefix: bool,
    pub has_forward: bool,
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

#[inline(always)]
fn count_mode_label(count: i32) -> &'static str {
    if count < 0 { "whole" } else { "bounded" }
}

#[inline(always)]
fn read_dir_count_as_f64(value: i32) -> f64 {
    f64::from(value)
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
        describe_counter!(
            LIST_OBJECTS_LOCAL_READ_DIR_TOTAL,
            "Total number of local read_dir calls made while serving live-walker ListObjects pages."
        );
        describe_histogram!(
            LIST_OBJECTS_LOCAL_READ_DIR_DURATION_MS,
            "Duration in milliseconds of local read_dir calls made while serving live-walker ListObjects pages."
        );
        describe_histogram!(
            LIST_OBJECTS_LOCAL_READ_DIR_ENTRIES,
            "Number of immediate directory entries returned by local read_dir while serving live-walker ListObjects pages."
        );
        describe_histogram!(
            LIST_OBJECTS_LOCAL_READ_DIR_LIMIT,
            "Requested local read_dir count used while serving live-walker ListObjects pages; negative counts mean whole-directory enumeration."
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

pub fn record_list_objects_local_read_dir(observation: ListObjectsLocalReadDirObservation) {
    if !get_stage_metrics_enabled() {
        return;
    }

    let count_mode = count_mode_label(observation.requested_count);

    counter!(
        LIST_OBJECTS_LOCAL_READ_DIR_TOTAL,
        "outcome" => observation.outcome,
        "count_mode" => count_mode,
        "is_root" => bool_label(observation.is_root),
        "has_filter_prefix" => bool_label(observation.has_filter_prefix),
        "has_forward" => bool_label(observation.has_forward),
    )
    .increment(1);
    histogram!(
        LIST_OBJECTS_LOCAL_READ_DIR_DURATION_MS,
        "outcome" => observation.outcome,
        "count_mode" => count_mode,
    )
    .record(observation.duration_ms);
    histogram!(
        LIST_OBJECTS_LOCAL_READ_DIR_ENTRIES,
        "outcome" => observation.outcome,
        "count_mode" => count_mode,
    )
    .record(count_as_f64(observation.returned_entries));
    histogram!(LIST_OBJECTS_LOCAL_READ_DIR_LIMIT, "count_mode" => count_mode)
        .record(read_dir_count_as_f64(observation.requested_count));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::set_get_stage_metrics_enabled;
    use crate::tests::{METRICS_FLAG_LOCK, counter_total, emitted_names, histogram_samples};
    use metrics_util::debugging::DebuggingRecorder;

    /// Run `body` against a local recorder with stage metrics on, and return the
    /// snapshot rows.
    ///
    /// Enabling the flag is the point: every `record_*` here returns early when
    /// `get_stage_metrics_enabled()` is false, which defaults to false. The
    /// previous versions of these tests never set it, so they exercised nothing
    /// but the early return (rustfs/backlog#1836).
    fn recorded(body: impl FnOnce()) -> Vec<crate::tests::MetricRow> {
        let _guard = METRICS_FLAG_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            init_list_objects_metrics();
            set_get_stage_metrics_enabled(true);
            body();
            set_get_stage_metrics_enabled(false);
        });
        snapshotter.snapshot().into_vec()
    }

    #[test]
    fn gather_scan_amplification_falls_back_to_the_scan_count_on_an_empty_page() {
        let rows = recorded(|| {
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
            })
        });

        assert_eq!(counter_total(&rows, LIST_OBJECTS_GATHER_TOTAL), Some(1));
        // Zero returned entries must not divide: the amplification reports the
        // scan count itself rather than an infinity or a NaN.
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_GATHER_SCAN_AMPLIFICATION), vec![42.0]);
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_GATHER_FILTERED_ENTRIES), vec![42.0]);
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_GATHER_RETURNED_ENTRIES), vec![0.0]);
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_GATHER_DURATION_MS), vec![3.5]);
    }

    #[test]
    fn merge_records_a_zero_read_quorum_rather_than_skipping_it() {
        let rows = recorded(|| record_list_objects_merge(LIST_OBJECTS_SOURCE_WALKER, 4, 0));

        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_MERGE_FAN_IN), vec![4.0]);
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_MERGE_READ_QUORUM), vec![0.0]);
    }

    #[test]
    fn index_fallback_counts_once_per_reason() {
        let rows = recorded(|| {
            record_list_objects_index_fallback("index_key_only", "unsupported_request");
            record_list_objects_index_fallback("index_key_only", "unsupported_request");
        });

        assert_eq!(counter_total(&rows, LIST_OBJECTS_INDEX_FALLBACK_TOTAL), Some(2));
    }

    #[test]
    fn index_serving_reports_verification_amplification_against_returned_objects() {
        let rows = recorded(|| {
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
        });

        assert_eq!(counter_total(&rows, LIST_OBJECTS_INDEX_ATTEMPT_TOTAL), Some(1));
        assert_eq!(counter_total(&rows, LIST_OBJECTS_INDEX_SERVED_TOTAL), Some(1));
        assert_eq!(counter_total(&rows, LIST_OBJECTS_INDEX_LIVE_VERIFY_FAILURE_TOTAL), Some(1));
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_INDEX_CANDIDATE_KEYS), vec![1000.0]);
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_INDEX_LIVE_VERIFY_HITS), vec![650.0]);
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_INDEX_LIVE_VERIFY_MISSES), vec![50.0]);
        assert_eq!(
            histogram_samples(&rows, LIST_OBJECTS_INDEX_VERIFICATION_IO_AMPLIFICATION),
            vec![700.0 / 600.0]
        );
    }

    #[test]
    fn local_read_dir_passes_the_whole_directory_sentinel_through_as_the_limit() {
        let rows = recorded(|| {
            record_list_objects_local_read_dir(ListObjectsLocalReadDirObservation {
                outcome: LIST_OBJECTS_LOCAL_READ_DIR_OUTCOME_OK,
                requested_count: -1,
                returned_entries: 4096,
                duration_ms: 12.5,
                is_root: true,
                has_filter_prefix: false,
                has_forward: false,
            });
            record_list_objects_local_read_dir(ListObjectsLocalReadDirObservation {
                outcome: LIST_OBJECTS_LOCAL_READ_DIR_OUTCOME_ERROR,
                requested_count: -1,
                returned_entries: 0,
                duration_ms: 5000.0,
                is_root: true,
                has_filter_prefix: false,
                has_forward: true,
            });
        });

        assert_eq!(counter_total(&rows, LIST_OBJECTS_LOCAL_READ_DIR_TOTAL), Some(2));
        // `-1` is the "read the whole directory" sentinel and must reach the
        // limit histogram unchanged rather than being clamped to zero.
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_LOCAL_READ_DIR_LIMIT), vec![-1.0, -1.0]);
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_LOCAL_READ_DIR_ENTRIES), vec![0.0, 4096.0]);
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_LOCAL_READ_DIR_DURATION_MS), vec![12.5, 5000.0]);
        assert!(emitted_names(&rows).contains(LIST_OBJECTS_LOCAL_READ_DIR_TOTAL));
    }
}
