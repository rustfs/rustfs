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

use std::sync::{Mutex, OnceLock};
use sysinfo::{Pid, ProcessRefreshKind, ProcessStatus, ProcessesToUpdate, System};

static PROCESS_SYSTEM: OnceLock<Mutex<System>> = OnceLock::new();

#[inline]
fn current_pid() -> Pid {
    Pid::from_u32(std::process::id())
}

#[inline]
fn process_system() -> &'static Mutex<System> {
    PROCESS_SYSTEM.get_or_init(|| {
        let pid = current_pid();
        let mut system = System::new();
        system.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, ProcessRefreshKind::everything());
        Mutex::new(system)
    })
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ProcessResourceSnapshot {
    pub cpu_percent: f64,
    pub memory_bytes: u64,
    pub uptime_seconds: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ProcessStatusSnapshot {
    Running = 0,
    Sleeping = 1,
    Zombie = 2,
    #[default]
    Other = 3,
}

impl From<ProcessStatus> for ProcessStatusSnapshot {
    fn from(status: ProcessStatus) -> Self {
        match status {
            ProcessStatus::Run => ProcessStatusSnapshot::Running,
            ProcessStatus::Sleep => ProcessStatusSnapshot::Sleeping,
            ProcessStatus::Zombie => ProcessStatusSnapshot::Zombie,
            _ => ProcessStatusSnapshot::Other,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ProcessSystemSnapshot {
    pub locks_read_total: u64,
    pub locks_write_total: u64,
    pub cpu_total_seconds: f64,
    pub go_routine_total: u64,
    pub disk_read_bytes: u64,
    pub disk_write_bytes: u64,
    pub io_rchar_bytes: u64,
    pub io_read_bytes: u64,
    pub io_wchar_bytes: u64,
    pub io_write_bytes: u64,
    pub start_time_seconds: u64,
    pub uptime_seconds: u64,
    pub file_descriptor_limit_total: u64,
    pub file_descriptor_open_total: u64,
    pub syscall_read_total: u64,
    pub syscall_write_total: u64,
    pub resident_memory_bytes: u64,
    pub virtual_memory_bytes: u64,
    pub virtual_memory_max_bytes: u64,
    pub status: ProcessStatusSnapshot,
    pub status_value: i64,
}

/// Collect resource-only process snapshot for the current process.
#[inline]
pub fn snapshot_process_resource() -> ProcessResourceSnapshot {
    snapshot_process_resource_and_system().0
}

/// Collect system-level process snapshot for the current process.
#[inline]
pub fn snapshot_process_system() -> ProcessSystemSnapshot {
    snapshot_process_resource_and_system().1
}

/// Collect both resource and system snapshots in one sysinfo refresh.
#[inline]
pub fn snapshot_process_resource_and_system() -> (ProcessResourceSnapshot, ProcessSystemSnapshot) {
    let platform_stats = crate::snapshot_process_platform_stats();
    let lock_snapshot = crate::snapshot_process_lock_counts();
    let pid = current_pid();
    let mut sys = process_system().lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[pid]), true, ProcessRefreshKind::everything());

    if let Some(process) = sys.process(pid) {
        let disk_usage = process.disk_usage();
        let status = ProcessStatusSnapshot::from(process.status());
        let uptime_seconds = process.run_time();

        let resource_stats = ProcessResourceSnapshot {
            cpu_percent: process.cpu_usage() as f64,
            memory_bytes: process.memory(),
            uptime_seconds,
        };

        let process_stats = ProcessSystemSnapshot {
            locks_read_total: lock_snapshot.read_locks_held,
            locks_write_total: lock_snapshot.write_locks_held,
            cpu_total_seconds: process.accumulated_cpu_time() as f64 / 1000.0,
            disk_read_bytes: disk_usage.read_bytes,
            disk_write_bytes: disk_usage.written_bytes,
            file_descriptor_limit_total: process.open_files_limit().map_or(0, |value| value as u64),
            file_descriptor_open_total: process.open_files().map_or(0, |value| value as u64),
            go_routine_total: process.tasks().map_or(0, |tasks| tasks.len() as u64),
            io_rchar_bytes: platform_stats.io_rchar_bytes.unwrap_or(disk_usage.total_read_bytes),
            io_read_bytes: platform_stats.io_read_bytes.unwrap_or(disk_usage.total_read_bytes),
            io_wchar_bytes: platform_stats.io_wchar_bytes.unwrap_or(disk_usage.total_written_bytes),
            io_write_bytes: platform_stats.io_write_bytes.unwrap_or(disk_usage.total_written_bytes),
            resident_memory_bytes: process.memory(),
            start_time_seconds: process.start_time(),
            status,
            status_value: status as i64,
            syscall_read_total: platform_stats.syscall_read_total.unwrap_or(0),
            syscall_write_total: platform_stats.syscall_write_total.unwrap_or(0),
            uptime_seconds,
            virtual_memory_bytes: process.virtual_memory(),
            virtual_memory_max_bytes: platform_stats.virtual_memory_max_bytes.unwrap_or(0),
        };

        (resource_stats, process_stats)
    } else {
        (ProcessResourceSnapshot::default(), ProcessSystemSnapshot::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn process_status_snapshot_maps_sysinfo_values() {
        assert_eq!(ProcessStatusSnapshot::from(ProcessStatus::Run), ProcessStatusSnapshot::Running);
        assert_eq!(ProcessStatusSnapshot::from(ProcessStatus::Sleep), ProcessStatusSnapshot::Sleeping);
        assert_eq!(ProcessStatusSnapshot::from(ProcessStatus::Zombie), ProcessStatusSnapshot::Zombie);
    }

    #[test]
    fn process_snapshots_are_collectable() {
        let _ = snapshot_process_resource();
        let _ = snapshot_process_system();
        let _ = snapshot_process_resource_and_system();
    }
}
