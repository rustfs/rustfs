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

/// Bounded caller class for a ListObjects walker gather.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum ListObjectsGatherCaller {
    #[default]
    Unattributed,
    S3List,
    S3ListVersions,
    Lifecycle,
    Other,
}

impl ListObjectsGatherCaller {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Unattributed => "unattributed",
            Self::S3List => "s3_list",
            Self::S3ListVersions => "s3_list_versions",
            Self::Lifecycle => "lifecycle",
            Self::Other => "other",
        }
    }
}

const LIST_OBJECTS_GATHER_TOTAL: &str = "rustfs_s3_list_objects_gather_total";
const LIST_OBJECTS_GATHER_CALLER_TOTAL: &str = "rustfs_s3_list_objects_gather_caller_total";
const LIST_OBJECTS_GATHER_DURATION_MS: &str = "rustfs_s3_list_objects_gather_duration_ms";
const LIST_OBJECTS_GATHER_SCANNED_ENTRIES: &str = "rustfs_s3_list_objects_gather_scanned_entries";
const LIST_OBJECTS_GATHER_RETURNED_ENTRIES: &str = "rustfs_s3_list_objects_gather_returned_entries";
const LIST_OBJECTS_GATHER_FILTERED_ENTRIES: &str = "rustfs_s3_list_objects_gather_filtered_entries";
const LIST_OBJECTS_GATHER_SCAN_AMPLIFICATION: &str = "rustfs_s3_list_objects_gather_scan_amplification";
const LIST_OBJECTS_GATHER_LIMIT: &str = "rustfs_s3_list_objects_gather_limit";
const LIST_OBJECTS_MERGE_FAN_IN: &str = "rustfs_s3_list_objects_merge_fan_in";
const LIST_OBJECTS_MERGE_READ_QUORUM: &str = "rustfs_s3_list_objects_merge_read_quorum";
const LIST_OBJECTS_LOCAL_READ_DIR_TOTAL: &str = "rustfs_s3_list_objects_local_read_dir_total";
const LIST_OBJECTS_LOCAL_READ_DIR_DURATION_MS: &str = "rustfs_s3_list_objects_local_read_dir_duration_ms";
const LIST_OBJECTS_LOCAL_READ_DIR_ENTRIES: &str = "rustfs_s3_list_objects_local_read_dir_entries";
const LIST_OBJECTS_LOCAL_READ_DIR_LIMIT: &str = "rustfs_s3_list_objects_local_read_dir_limit";

pub const LIST_OBJECTS_LOCAL_READ_DIR_OUTCOME_OK: &str = "ok";
pub const LIST_OBJECTS_LOCAL_READ_DIR_OUTCOME_ERROR: &str = "error";

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ListObjectsGatherObservation {
    pub source: &'static str,
    pub caller: ListObjectsGatherCaller,
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
    let api_operation = crate::s3_http_metrics::current_s3_http_operation_name().unwrap_or("unattributed");

    counter!(
        LIST_OBJECTS_GATHER_TOTAL,
        "source" => observation.source,
        "outcome" => observation.outcome,
        "has_prefix" => bool_label(observation.has_prefix),
        "has_delimiter" => bool_label(observation.has_delimiter),
        "has_marker" => bool_label(observation.has_marker),
    )
    .increment(1);
    counter!(
        LIST_OBJECTS_GATHER_CALLER_TOTAL,
        "api_operation" => api_operation,
        "caller" => observation.caller.as_str(),
        "source" => observation.source,
        "outcome" => observation.outcome,
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
                caller: ListObjectsGatherCaller::Unattributed,
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
        assert_eq!(counter_total(&rows, LIST_OBJECTS_GATHER_CALLER_TOTAL), Some(1));
        let caller_labels = rows
            .iter()
            .find(|(composite, _, _, _)| composite.key().name() == LIST_OBJECTS_GATHER_CALLER_TOTAL)
            .expect("internal caller counter should be emitted")
            .0
            .key()
            .labels()
            .map(|label| (label.key().to_string(), label.value().to_string()))
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(caller_labels.get("api_operation").map(String::as_str), Some("unattributed"));
        // Zero returned entries must not divide: the amplification reports the
        // scan count itself rather than an infinity or a NaN.
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_GATHER_SCAN_AMPLIFICATION), vec![42.0]);
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_GATHER_FILTERED_ENTRIES), vec![42.0]);
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_GATHER_RETURNED_ENTRIES), vec![0.0]);
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_GATHER_DURATION_MS), vec![3.5]);
    }

    #[test]
    fn gather_caller_metric_attributes_the_s3_api_operation() {
        use rustfs_s3_ops::S3Operation;

        let rows = recorded(|| {
            let mut request = crate::s3_http_metrics::S3HttpRequestGuard::new("GET");
            request.in_scope(|| {
                crate::s3_http_metrics::observe_s3_http_operation(S3Operation::ListObjectsV2);
                record_list_objects_gather(ListObjectsGatherObservation {
                    source: LIST_OBJECTS_SOURCE_WALKER,
                    caller: ListObjectsGatherCaller::S3List,
                    outcome: LIST_OBJECTS_GATHER_OUTCOME_INPUT_CLOSED,
                    limit: 1000,
                    scanned_entries: 1,
                    returned_entries: 1,
                    duration_ms: 1.0,
                    has_prefix: false,
                    has_delimiter: false,
                    has_marker: false,
                });
            });
        });

        assert_eq!(counter_total(&rows, LIST_OBJECTS_GATHER_CALLER_TOTAL), Some(1));
        let labels = rows
            .iter()
            .find(|(composite, _, _, _)| composite.key().name() == LIST_OBJECTS_GATHER_CALLER_TOTAL)
            .expect("caller counter should be emitted")
            .0
            .key()
            .labels()
            .map(|label| (label.key().to_string(), label.value().to_string()))
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(labels.get("api_operation").map(String::as_str), Some("s3:ListObjectsV2"));
        assert_eq!(labels.get("caller").map(String::as_str), Some("s3_list"));
        assert_eq!(labels.get("source").map(String::as_str), Some(LIST_OBJECTS_SOURCE_WALKER));
    }

    #[test]
    fn merge_records_a_zero_read_quorum_rather_than_skipping_it() {
        let rows = recorded(|| record_list_objects_merge(LIST_OBJECTS_SOURCE_WALKER, 4, 0));

        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_MERGE_FAN_IN), vec![4.0]);
        assert_eq!(histogram_samples(&rows, LIST_OBJECTS_MERGE_READ_QUORUM), vec![0.0]);
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
