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

use sysinfo::System;

/// Source used to resolve the effective memory limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemoryBasis {
    /// Limits come from host memory reported by sysinfo.
    Host,
    /// Limits come from a constraining cgroup (container) memory limit.
    Cgroup,
}

impl MemoryBasis {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Host => "host",
            Self::Cgroup => "cgroup",
        }
    }
}

/// Effective memory totals after reconciling host memory with cgroup limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct EffectiveMemory {
    /// Effective total memory in bytes.
    pub total_bytes: u64,
    /// Effective available memory in bytes.
    pub available_bytes: u64,
    /// Whether the limits are host- or cgroup-derived.
    pub basis: MemoryBasis,
}

/// Reconciles host memory with an optional cgroup limit.
///
/// `cgroup` carries `(total_memory, free_memory)` as reported for the cgroup
/// hierarchy (sysinfo already caps `total_memory` at the host total). A cgroup
/// only counts when it actually constrains below the host, so an unlimited
/// cgroup transparently falls back to host values.
pub fn select_effective_memory(host_total: u64, host_available: u64, cgroup: Option<(u64, u64)>) -> EffectiveMemory {
    match cgroup {
        Some((cgroup_total, cgroup_free)) if cgroup_total > 0 && cgroup_total < host_total => EffectiveMemory {
            total_bytes: cgroup_total,
            available_bytes: cgroup_free.min(cgroup_total),
            basis: MemoryBasis::Cgroup,
        },
        _ => EffectiveMemory {
            total_bytes: host_total,
            available_bytes: host_available,
            basis: MemoryBasis::Host,
        },
    }
}

/// Resolves the effective memory from an already-refreshed system handle.
///
/// On Linux, the current process cgroup and its ancestors are read on each call,
/// including limits imposed by systemd services. System-wide cgroup limits are
/// used if process information is unavailable. Other platforms use host values.
pub fn effective_memory_from_system(system: &System) -> EffectiveMemory {
    #[cfg(target_os = "linux")]
    let limits = sysinfo::get_current_pid()
        .ok()
        .and_then(|pid| {
            let mut processes = System::new().ok()?;
            processes.refresh_processes_specifics(
                sysinfo::ProcessesToUpdate::Some(&[pid]),
                false,
                sysinfo::ProcessRefreshKind::nothing().with_memory(),
            );
            processes.process(pid)?.cgroup_limits()
        })
        .or_else(|| system.cgroup_limits());
    #[cfg(not(target_os = "linux"))]
    let limits = system.cgroup_limits();
    let cgroup = limits.map(|limits| (limits.total_memory, limits.free_memory));
    select_effective_memory(system.total_memory(), system.available_memory(), cgroup)
}

/// Resolves the effective memory using a fresh, memory-refreshed system handle.
pub fn resolve_effective_memory() -> EffectiveMemory {
    let Ok(mut system) = System::new() else {
        return select_effective_memory(0, 0, None);
    };
    system.refresh_memory();
    effective_memory_from_system(&system)
}

#[cfg(test)]
mod tests {
    use super::{MemoryBasis, select_effective_memory};

    const GIB: u64 = 1024 * 1024 * 1024;

    #[cfg(target_os = "linux")]
    #[test]
    fn effective_memory_uses_current_process_cgroup() {
        use sysinfo::{ProcessRefreshKind, ProcessesToUpdate, System, get_current_pid};

        let pid = get_current_pid().expect("current process ID");
        let mut system = System::new().expect("system memory handle");
        system.refresh_memory();
        let mut process_system = System::new().expect("process memory handle");
        process_system.refresh_processes_specifics(
            ProcessesToUpdate::Some(&[pid]),
            false,
            ProcessRefreshKind::nothing().with_memory(),
        );
        let limits = process_system.process(pid).expect("current process entry").cgroup_limits();
        let expected = select_effective_memory(
            system.total_memory(),
            system.available_memory(),
            limits.map(|limits| (limits.total_memory, limits.free_memory)),
        );
        let actual = super::effective_memory_from_system(&system);
        assert_eq!(actual.basis, expected.basis);
        assert_eq!(actual.total_bytes, expected.total_bytes);
        assert!(actual.available_bytes <= actual.total_bytes);
    }

    #[test]
    fn select_effective_memory_prefers_constraining_cgroup() {
        let effective = select_effective_memory(64 * GIB, 40 * GIB, Some((2 * GIB, GIB)));

        assert_eq!(effective.basis, MemoryBasis::Cgroup);
        assert_eq!(effective.total_bytes, 2 * GIB);
        assert_eq!(effective.available_bytes, GIB);
    }

    #[test]
    fn select_effective_memory_ignores_non_constraining_cgroup() {
        let effective = select_effective_memory(64 * GIB, 40 * GIB, Some((64 * GIB, 10 * GIB)));

        assert_eq!(effective.basis, MemoryBasis::Host);
        assert_eq!(effective.total_bytes, 64 * GIB);
        assert_eq!(effective.available_bytes, 40 * GIB);
    }

    #[test]
    fn select_effective_memory_falls_back_to_host_without_cgroup() {
        let effective = select_effective_memory(8 * GIB, 4 * GIB, None);

        assert_eq!(effective.basis, MemoryBasis::Host);
        assert_eq!(effective.total_bytes, 8 * GIB);
        assert_eq!(effective.available_bytes, 4 * GIB);
    }

    #[test]
    fn select_effective_memory_caps_available_at_total() {
        let effective = select_effective_memory(64 * GIB, 40 * GIB, Some((2 * GIB, 3 * GIB)));

        assert_eq!(effective.total_bytes, 2 * GIB);
        assert_eq!(effective.available_bytes, 2 * GIB);
    }
}
