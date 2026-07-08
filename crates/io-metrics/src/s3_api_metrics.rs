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

use rustfs_s3_ops::S3Operation;
use std::sync::OnceLock;

const S3_OPS_METRIC: &str = "rustfs_s3_operations_total";

/// Record a handled S3 API operation.
///
/// The series is labeled by `op` only. The bucket name is deliberately NOT a
/// label: it is client-controlled and unbounded, so labeling by bucket would
/// let any client explode metric cardinality (a Prometheus/OTEL cardinality
/// DoS that grows the in-process OTEL aggregation store even without a scrape).
/// This mirrors MinIO, which never labels its default operation counters with
/// bucket. The `op` dimension is bounded (<= 122 variants).
pub fn record_s3_op(op: S3Operation) {
    counter!(S3_OPS_METRIC, "op" => op.as_str()).increment(1);
}

pub fn init_s3_metrics() {
    static METRICS_DESC_INIT: OnceLock<()> = OnceLock::new();
    METRICS_DESC_INIT.get_or_init(|| {
        describe_counter!(S3_OPS_METRIC, "Total number of S3 API operations handled");
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use metrics::with_local_recorder;
    use metrics_util::debugging::DebuggingRecorder;
    use std::collections::HashSet;

    /// Collect the label-key sets recorded against `rustfs_s3_operations_total`.
    fn ops_metric_label_key_sets(recorder: &DebuggingRecorder) -> Vec<HashSet<String>> {
        let snapshotter = recorder.snapshotter();
        with_local_recorder(recorder, || {
            record_s3_op(S3Operation::GetObject);
        });
        snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(composite, _, _, _)| composite.key().name() == S3_OPS_METRIC)
            .map(|(composite, _, _, _)| composite.key().labels().map(|label| label.key().to_string()).collect())
            .collect()
    }

    #[test]
    fn record_s3_op_labels_by_op_only_no_bucket() {
        let recorder = DebuggingRecorder::new();
        let label_key_sets = ops_metric_label_key_sets(&recorder);

        assert_eq!(label_key_sets.len(), 1, "exactly one series expected for a single op");
        let keys = &label_key_sets[0];
        assert_eq!(
            keys,
            &HashSet::from(["op".to_string()]),
            "series must carry the op label only; the client-controlled bucket label must be absent"
        );
        assert!(!keys.contains("bucket"), "bucket label must never be emitted (cardinality DoS)");
    }

    #[test]
    fn record_s3_op_cardinality_bounded_by_distinct_ops() {
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // Same op recorded twice + two other ops => distinct series == distinct ops.
        let ops = [
            S3Operation::GetObject,
            S3Operation::GetObject,
            S3Operation::PutObject,
            S3Operation::ListObjectsV2,
        ];
        with_local_recorder(&recorder, || {
            for op in ops {
                record_s3_op(op);
            }
        });

        let series: HashSet<String> = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .filter(|(composite, _, _, _)| composite.key().name() == S3_OPS_METRIC)
            .map(|(composite, _, _, _)| {
                composite
                    .key()
                    .labels()
                    .map(|label| format!("{}={}", label.key(), label.value()))
                    .collect::<Vec<_>>()
                    .join(",")
            })
            .collect();

        let distinct_ops: HashSet<&str> = ops.iter().map(|op| op.as_str()).collect();
        assert_eq!(
            series.len(),
            distinct_ops.len(),
            "series count must equal the number of distinct ops, never the bucket count"
        );
    }
}
