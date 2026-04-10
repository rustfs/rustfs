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

//! Metric name constants for consistent naming across the codebase.

/// Request-level data plane metric names introduced by ADR 0001.
pub mod data_plane {
    /// Total number of selected request paths.
    pub const PATH_SELECTED_TOTAL: &str = "rustfs.io.path.selected_total";

    /// Total bytes observed for a given effective copy mode.
    pub const COPY_MODE_BYTES_TOTAL: &str = "rustfs.io.copy_mode.bytes_total";

    /// Total number of data plane fallbacks.
    pub const FALLBACK_TOTAL: &str = "rustfs.io.zero_copy.fallback_total";

    /// Current active local-disk mmap bytes held by chunk fast paths.
    pub const LOCAL_DISK_ACTIVE_MMAP_BYTES: &str = "rustfs.io.local_disk.active_mmap.bytes";

    /// Total pooled chunks produced or consumed by LocalDisk compatibility paths.
    pub const LOCAL_DISK_POOLED_CHUNKS_TOTAL: &str = "rustfs.io.local_disk.pooled_chunks.total";

    /// Total pooled bytes produced or consumed by LocalDisk compatibility paths.
    pub const LOCAL_DISK_POOLED_BYTES_TOTAL: &str = "rustfs.io.local_disk.pooled_bytes.total";

    /// Total number of compatibility chunk-stream aggregations performed for LocalDisk reads.
    pub const LOCAL_DISK_COMPAT_COLLECT_TOTAL: &str = "rustfs.io.local_disk.compat_collect.total";

    /// Chunk count distribution for LocalDisk compatibility chunk aggregation.
    pub const LOCAL_DISK_COMPAT_COLLECT_CHUNKS: &str = "rustfs.io.local_disk.compat_collect.chunks";

    /// Byte distribution for LocalDisk compatibility chunk aggregation.
    pub const LOCAL_DISK_COMPAT_COLLECT_BYTES: &str = "rustfs.io.local_disk.compat_collect.bytes";

    /// Total number of attempted PUT fast paths.
    pub const PUT_FAST_PATH_ATTEMPTS_TOTAL: &str = "rustfs.io.put.fast_path.attempts_total";

    /// Size distribution for attempted PUT fast paths.
    pub const PUT_FAST_PATH_ATTEMPT_SIZE_BYTES: &str = "rustfs.io.put.fast_path.attempt.size.bytes";

    /// Total number of transformed PUT selections grouped by transform kind and ingress path.
    pub const PUT_TRANSFORM_SELECTED_TOTAL: &str = "rustfs.io.put.transform.selected_total";

    /// Size distribution for transformed PUT selections.
    pub const PUT_TRANSFORM_SIZE_BYTES: &str = "rustfs.io.put.transform.size.bytes";

    /// Total number of selected GET chunk fast paths.
    pub const GET_FAST_PATH_SELECTED_TOTAL: &str = "rustfs.io.get.fast_path.selected_total";

    /// Total number of GET chunk fast path probe failures before response commit.
    pub const GET_FAST_PATH_PROBE_FAILED_TOTAL: &str = "rustfs.io.get.fast_path.probe_failed_total";

    /// Total number of GET chunk fast path mid-stream errors after response commit.
    pub const GET_FAST_PATH_MIDSTREAM_ERROR_TOTAL: &str = "rustfs.io.get.fast_path.midstream_error_total";

    /// Byte distribution promised by GET chunk fast path selections or failures.
    pub const GET_FAST_PATH_PROMISED_BYTES: &str = "rustfs.io.get.fast_path.promised.bytes";

    /// Byte distribution already sent when a GET chunk fast path fails mid-stream.
    pub const GET_FAST_PATH_MIDSTREAM_SENT_BYTES: &str = "rustfs.io.get.fast_path.midstream_sent.bytes";
}
