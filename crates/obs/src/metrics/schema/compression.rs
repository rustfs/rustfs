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

use crate::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

pub static COMPRESSION_BYTES_ORIGINAL_TOTAL: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::OriginedBytesTotal,
        "Total bytes before compression (original)",
        &["compression"],
        subsystems::COMPRESSION,
    )
});

pub static COMPRESSION_BYTES_COMPRESSED_TOTAL: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::CompressedBytesTotal,
        "Total bytes after compression",
        &["compression"],
        subsystems::COMPRESSION,
    )
});

pub static COMPRESSION_BYTES_SAVED_TOTAL: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::SavedBytesTotal,
        "Total bytes saved by compression",
        &["compression"],
        subsystems::COMPRESSION,
    )
});

pub static COMPRESSION_RATIO: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::CompressionRatio,
        "Compression ratio (0.0 - 1.0)",
        &["compression"],
        subsystems::COMPRESSION,
    )
});

pub static COMPRESSION_OPERATIONS_TOTAL: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::CompressionOperationsTotal,
        "Total number of compression operations performed",
        &["compression"],
        subsystems::COMPRESSION,
    )
});
