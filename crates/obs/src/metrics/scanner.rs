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

/// Scanner-related metric descriptors
use crate::metrics::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

pub static SCANNER_BUCKET_SCANS_FINISHED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerBucketScansFinished,
        "Total number of bucket scans finished since server start",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_BUCKET_SCANS_STARTED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerBucketScansStarted,
        "Total number of bucket scans started since server start",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_DIRECTORIES_SCANNED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerDirectoriesScanned,
        "Total number of directories scanned since server start",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_OBJECTS_SCANNED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerObjectsScanned,
        "Total number of unique objects scanned since server start",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_VERSIONS_SCANNED_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ScannerVersionsScanned,
        "Total number of object versions scanned since server start",
        &[],
        subsystems::SCANNER,
    )
});

pub static SCANNER_LAST_ACTIVITY_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ScannerLastActivitySeconds,
        "Time elapsed (in seconds) since last scan activity.",
        &[],
        subsystems::SCANNER,
    )
});
