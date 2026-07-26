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

#![allow(dead_code)]

use crate::{MetricDescriptor, MetricName, MetricSubsystem, new_gauge_md};
use std::sync::LazyLock;

const BUCKET_LABEL: &str = "bucket";

/// Cluster-wide logical bytes used by the bucket.
pub static BUCKET_USAGE_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("usage_bytes".to_string()),
        "Cluster-wide logical bytes used by the bucket",
        &[BUCKET_LABEL],
        MetricSubsystem::new("/bucket/usage"),
    )
});

/// Cluster-wide logical object count in the bucket.
pub static BUCKET_OBJECTS_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("objects_total".to_string()),
        "Cluster-wide logical object count in the bucket",
        &[BUCKET_LABEL],
        MetricSubsystem::new("/bucket/usage"),
    )
});

/// Quota limit in bytes for the bucket
pub static BUCKET_QUOTA_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::Custom("quota_bytes".to_string()),
        "Quota limit in bytes for the bucket",
        &[BUCKET_LABEL],
        MetricSubsystem::new("/bucket/usage"),
    )
});

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bucket_usage_descriptors_use_bucket_usage_subsystem() {
        assert_eq!(BUCKET_USAGE_BYTES_MD.subsystem.path(), "/bucket/usage");
        assert_eq!(BUCKET_OBJECTS_TOTAL_MD.subsystem.path(), "/bucket/usage");
        assert_eq!(BUCKET_QUOTA_BYTES_MD.subsystem.path(), "/bucket/usage");
        assert!(BUCKET_USAGE_BYTES_MD.help.contains("Cluster-wide logical"));
        assert!(BUCKET_OBJECTS_TOTAL_MD.help.contains("Cluster-wide logical"));
    }
}
