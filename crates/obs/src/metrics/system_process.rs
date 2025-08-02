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

/// Process related metric descriptors
///
/// This module defines various system process metrics used for monitoring
/// the RustFS process performance, resource usage, and system integration.
/// Metrics are implemented using std::sync::LazyLock for thread-safe lazy initialization.
use crate::metrics::{MetricDescriptor, MetricName, new_counter_md, new_gauge_md, subsystems};
use std::sync::LazyLock;

/// Number of current READ locks on this peer
pub static PROCESS_LOCKS_READ_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessLocksReadTotal,
        "Number of current READ locks on this peer",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Number of current WRITE locks on this peer
pub static PROCESS_LOCKS_WRITE_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessLocksWriteTotal,
        "Number of current WRITE locks on this peer",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Total user and system CPU time spent in seconds
pub static PROCESS_CPU_TOTAL_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProcessCPUTotalSeconds,
        "Total user and system CPU time spent in seconds",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Total number of go routines running
pub static PROCESS_GO_ROUTINE_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessGoRoutineTotal,
        "Total number of go routines running",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Total bytes read by the process from the underlying storage system including cache
pub static PROCESS_IO_RCHAR_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProcessIORCharBytes,
        "Total bytes read by the process from the underlying storage system including cache, /proc/[pid]/io rchar",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Total bytes read by the process from the underlying storage system
pub static PROCESS_IO_READ_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProcessIOReadBytes,
        "Total bytes read by the process from the underlying storage system, /proc/[pid]/io read_bytes",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Total bytes written by the process to the underlying storage system including page cache
pub static PROCESS_IO_WCHAR_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProcessIOWCharBytes,
        "Total bytes written by the process to the underlying storage system including page cache, /proc/[pid]/io wchar",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Total bytes written by the process to the underlying storage system
pub static PROCESS_IO_WRITE_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProcessIOWriteBytes,
        "Total bytes written by the process to the underlying storage system, /proc/[pid]/io write_bytes",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Start time for RustFS process in seconds since Unix epoch
pub static PROCESS_START_TIME_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessStartTimeSeconds,
        "Start time for RustFS process in seconds since Unix epoch",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Uptime for RustFS process in seconds
pub static PROCESS_UPTIME_SECONDS_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessUptimeSeconds,
        "Uptime for RustFS process in seconds",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Limit on total number of open file descriptors for the RustFS Server process
pub static PROCESS_FILE_DESCRIPTOR_LIMIT_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessFileDescriptorLimitTotal,
        "Limit on total number of open file descriptors for the RustFS Server process",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Total number of open file descriptors by the RustFS Server process
pub static PROCESS_FILE_DESCRIPTOR_OPEN_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessFileDescriptorOpenTotal,
        "Total number of open file descriptors by the RustFS Server process",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Total read SysCalls to the kernel
pub static PROCESS_SYSCALL_READ_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProcessSyscallReadTotal,
        "Total read SysCalls to the kernel. /proc/[pid]/io syscr",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Total write SysCalls to the kernel
pub static PROCESS_SYSCALL_WRITE_TOTAL_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_counter_md(
        MetricName::ProcessSyscallWriteTotal,
        "Total write SysCalls to the kernel. /proc/[pid]/io syscw",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Resident memory size in bytes
pub static PROCESS_RESIDENT_MEMORY_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessResidentMemoryBytes,
        "Resident memory size in bytes",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Virtual memory size in bytes
pub static PROCESS_VIRTUAL_MEMORY_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessVirtualMemoryBytes,
        "Virtual memory size in bytes",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});

/// Maximum virtual memory size in bytes
pub static PROCESS_VIRTUAL_MEMORY_MAX_BYTES_MD: LazyLock<MetricDescriptor> = LazyLock::new(|| {
    new_gauge_md(
        MetricName::ProcessVirtualMemoryMaxBytes,
        "Maximum virtual memory size in bytes",
        &[],
        subsystems::SYSTEM_PROCESS,
    )
});
