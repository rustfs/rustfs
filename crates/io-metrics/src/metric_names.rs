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

/// Zero-copy operation metric names.
pub mod zero_copy {
    /// Total number of zero-copy buffer operations
    pub const BUFFER_OPERATIONS_TOTAL: &str = "rustfs_zero_copy_buffer_operations_total";

    /// Total bytes processed by zero-copy buffer operations
    pub const BUFFER_BYTES_TOTAL: &str = "rustfs_zero_copy_buffer_bytes_total";

    /// Total number of memory copies
    pub const MEMORY_COPY_TOTAL: &str = "rustfs_memory_copy_total";

    /// Total bytes copied in memory
    pub const MEMORY_COPY_BYTES_TOTAL: &str = "rustfs_memory_copy_bytes_total";

    /// Total number of shared reference operations
    pub const SHARED_REF_OPERATIONS_TOTAL: &str = "rustfs_shared_ref_operations_total";

    /// Total number of BufReader layers eliminated
    pub const BUFREADER_LAYERS_ELIMINATED_TOTAL: &str = "rustfs_bufreader_layers_eliminated_total";

    /// BufReader buffer size distribution
    pub const BUFREADER_BUFFER_SIZE_BYTES: &str = "rustfs_bufreader_buffer_size_bytes";

    /// Total number of Direct I/O operations
    pub const DIRECT_IO_OPERATIONS_TOTAL: &str = "rustfs_direct_io_operations_total";

    /// Total bytes processed by Direct I/O
    pub const DIRECT_IO_BYTES_TOTAL: &str = "rustfs_direct_io_bytes_total";

    /// Average copy count per operation
    pub const AVG_COPY_COUNT: &str = "rustfs_zero_copy_avg_copy_count";

    /// Throughput in MB/s
    pub const THROUGHPUT_MBPS: &str = "rustfs_zero_copy_throughput_mbps";

    /// Memory saved by zero-copy in bytes
    pub const MEMORY_SAVED_BYTES: &str = "rustfs_zero_copy_memory_saved_bytes";
}
