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

use crate::S3Operation;
use metrics::{counter, describe_counter};
use std::sync::OnceLock;

const S3_OPS_METRIC: &str = "rustfs_s3_operations_total";

/// Records an S3 operation in the metrics system.
/// This function should be called whenever an S3 API operation is handled, allowing us to track the usage of different S3 operations across buckets.
///
/// # Arguments
/// * `op` - The S3 operation being recorded.
/// * `bucket` - The name of the bucket associated with the operation, used as a label for more granular metrics analysis.
///
/// Example usage:
/// ```ignore
/// record_s3_op(S3Operation::GetObject, "my-bucket");
/// ```
pub fn record_s3_op(op: S3Operation, bucket: &str) {
    counter!(S3_OPS_METRIC, "op" => op.as_str(), "bucket" => bucket.to_owned()).increment(1);
}

/// One-time registration of indicator meta information
/// This function ensures that metric descriptors are registered only once.
pub fn init_s3_metrics() {
    static METRICS_DESC_INIT: OnceLock<()> = OnceLock::new();
    METRICS_DESC_INIT.get_or_init(|| {
        describe_counter!(S3_OPS_METRIC, "Total number of S3 API operations handled");
    });
}
