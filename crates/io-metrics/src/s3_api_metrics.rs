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

pub fn record_s3_op(op: S3Operation, bucket: &str) {
    counter!(S3_OPS_METRIC, "op" => op.as_str(), "bucket" => bucket.to_owned()).increment(1);
}

pub fn init_s3_metrics() {
    static METRICS_DESC_INIT: OnceLock<()> = OnceLock::new();
    METRICS_DESC_INIT.get_or_init(|| {
        describe_counter!(S3_OPS_METRIC, "Total number of S3 API operations handled");
    });
}
