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

use std::sync::atomic::{AtomicU64, Ordering};
use std::process::Command;

#[cfg(target_os = "linux")]
use std::fs;

static READ_LOCKS_HELD: AtomicU64 = AtomicU64::new(0);
static WRITE_LOCKS_HELD: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProcessLockSnapshot {
    pub read_locks_held: u64,
    pub write_locks_held: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProcessPlatformSnapshot {
    pub syscall_read_total: Option<u64>,
    pub syscall_write_total: Option<u64>,
    pub virtual_memory_max_bytes: Option<u64>,
}

#[inline(always)]
pub fn record_read_lock_held_acquire() {
    READ_LOCKS_HELD.fetch_add(1, Ordering::Relaxed);
}

#[inline(always)]
pub fn record_write_lock_held_acquire() {
    WRITE_LOCKS_HELD.fetch_add(1, Ordering::Relaxed);
}

#[inline(always)]
pub fn record_read_lock_held_release() {
    let _ = READ_LOCKS_HELD.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| Some(value.saturating_sub(1)));
}

#[inline(always)]
pub fn record_write_lock_held_release() {
    let _ = WRITE_LOCKS_HELD.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| Some(value.saturating_sub(1)));
}

#[inline(always)]
pub fn snapshot_process_lock_counts() -> ProcessLockSnapshot {
    ProcessLockSnapshot {
        read_locks_held: READ_LOCKS_HELD.load(Ordering::Relaxed),
        write_locks_held: WRITE_LOCKS_HELD.load(Ordering::Relaxed),
    }
}

#[inline]
pub fn snapshot_process_platform_stats() -> ProcessPlatformSnapshot {
    platform::snapshot()
}

fn run_command(command: &str, args: &[&str]) -> Option<String> {
    let output = Command::new(command).args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if stdout.is_empty() { None } else { Some(stdout) }
}

#[cfg(any(target_os = "linux", target_os = "windows", test))]
fn parse_kv_u64(content: &str, key: &str) -> Option<u64> {
    for line in content.lines() {
        let (k, v) = line.split_once(':')?;
        if k.trim().eq_ignore_ascii_case(key) {
            return v.trim().parse::<u64>().ok();
        }
    }
    None
}

#[cfg(target_os = "linux")]
mod platform {
    use super::*;

    pub(super) fn snapshot() -> ProcessPlatformSnapshot {
        let io = fs::read_to_string("/proc/self/io").ok();
        let status = fs::read_to_string("/proc/self/status").ok();

        ProcessPlatformSnapshot {
            syscall_read_total: io.as_deref().and_then(|v| parse_kv_u64(v, "syscr")),
            syscall_write_total: io.as_deref().and_then(|v| parse_kv_u64(v, "syscw")),
            virtual_memory_max_bytes: status.as_deref().and_then(parse_vm_peak_bytes),
        }
    }

    fn parse_vm_peak_bytes(content: &str) -> Option<u64> {
        for line in content.lines() {
            let Some(rest) = line.strip_prefix("VmPeak:") else {
                continue;
            };
            let mut parts = rest.split_whitespace();
            let value = parts.next()?.parse::<u64>().ok()?;
            let unit = parts.next().unwrap_or_default();
            return Some(match unit {
                "kB" | "KB" | "kb" => value.saturating_mul(1024),
                _ => value,
            });
        }
        None
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parse_vm_peak() {
            let status = "Name:\trustfs\nVmPeak:\t  2048 kB\nVmRSS:\t 1024 kB\n";
            assert_eq!(parse_vm_peak_bytes(status), Some(2048 * 1024));
        }
    }
}

#[cfg(target_os = "windows")]
mod platform {
    use super::*;

    pub(super) fn snapshot() -> ProcessPlatformSnapshot {
        let pid = std::process::id();
        let script = format!(
            "Get-CimInstance Win32_Process -Filter \"ProcessId = {pid}\" | \
             Format-List -Property ReadOperationCount,WriteOperationCount,PeakVirtualSize"
        );

        let output = run_command("powershell", &["-NoProfile", "-Command", &script]);
        ProcessPlatformSnapshot {
            syscall_read_total: output.as_deref().and_then(|v| parse_kv_u64(v, "ReadOperationCount")),
            syscall_write_total: output.as_deref().and_then(|v| parse_kv_u64(v, "WriteOperationCount")),
            virtual_memory_max_bytes: output.as_deref().and_then(|v| parse_kv_u64(v, "PeakVirtualSize")),
        }
    }
}

#[cfg(target_os = "macos")]
mod platform {
    use super::*;

    pub(super) fn snapshot() -> ProcessPlatformSnapshot {
        let pid = std::process::id().to_string();
        let output = run_command("ps", &["-o", "inblock=", "-o", "oublock=", "-o", "vsz=", "-p", &pid]);
        parse_ps_stats(output.as_deref())
    }

    fn parse_ps_stats(output: Option<&str>) -> ProcessPlatformSnapshot {
        let Some(output) = output else {
            return ProcessPlatformSnapshot::default();
        };

        let Some(line) = output.lines().find(|line| !line.trim().is_empty()) else {
            return ProcessPlatformSnapshot::default();
        };

        let mut parts = line.split_whitespace();
        let inblock = parts.next().and_then(|v| v.parse::<u64>().ok());
        let oublock = parts.next().and_then(|v| v.parse::<u64>().ok());
        let vsz_kb = parts.next().and_then(|v| v.parse::<u64>().ok());

        ProcessPlatformSnapshot {
            syscall_read_total: inblock,
            syscall_write_total: oublock,
            virtual_memory_max_bytes: vsz_kb.map(|v| v.saturating_mul(1024)),
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn parse_ps_line() {
            let snapshot = parse_ps_stats(Some("12 34 5678"));
            assert_eq!(snapshot.syscall_read_total, Some(12));
            assert_eq!(snapshot.syscall_write_total, Some(34));
            assert_eq!(snapshot.virtual_memory_max_bytes, Some(5678 * 1024));
        }
    }
}

#[cfg(any(target_os = "freebsd", target_os = "openbsd", target_os = "netbsd", target_os = "dragonfly"))]
mod platform {
    use super::*;

    pub(super) fn snapshot() -> ProcessPlatformSnapshot {
        let pid = std::process::id().to_string();
        let output = run_command("ps", &["-o", "inblk=", "-o", "oublk=", "-o", "vsz=", "-p", &pid]);
        parse_ps_stats(output.as_deref())
    }

    fn parse_ps_stats(output: Option<&str>) -> ProcessPlatformSnapshot {
        let Some(output) = output else {
            return ProcessPlatformSnapshot::default();
        };

        let Some(line) = output.lines().find(|line| !line.trim().is_empty()) else {
            return ProcessPlatformSnapshot::default();
        };

        let mut parts = line.split_whitespace();
        let inblock = parts.next().and_then(|v| v.parse::<u64>().ok());
        let oublock = parts.next().and_then(|v| v.parse::<u64>().ok());
        let vsz_kb = parts.next().and_then(|v| v.parse::<u64>().ok());

        ProcessPlatformSnapshot {
            syscall_read_total: inblock,
            syscall_write_total: oublock,
            virtual_memory_max_bytes: vsz_kb.map(|v| v.saturating_mul(1024)),
        }
    }
}

#[cfg(not(any(
    target_os = "linux",
    target_os = "windows",
    target_os = "macos",
    target_os = "freebsd",
    target_os = "openbsd",
    target_os = "netbsd",
    target_os = "dragonfly"
)))]
mod platform {
    use super::*;

    pub(super) fn snapshot() -> ProcessPlatformSnapshot {
        ProcessPlatformSnapshot::default()
    }
}

#[cfg(test)]
pub fn reset_process_lock_counts() {
    READ_LOCKS_HELD.store(0, Ordering::Relaxed);
    WRITE_LOCKS_HELD.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn held_lock_counters_round_trip() {
        reset_process_lock_counts();

        record_read_lock_held_acquire();
        record_read_lock_held_acquire();
        record_write_lock_held_acquire();

        assert_eq!(
            snapshot_process_lock_counts(),
            ProcessLockSnapshot {
                read_locks_held: 2,
                write_locks_held: 1,
            }
        );

        record_read_lock_held_release();
        record_write_lock_held_release();
        record_write_lock_held_release();

        assert_eq!(
            snapshot_process_lock_counts(),
            ProcessLockSnapshot {
                read_locks_held: 1,
                write_locks_held: 0,
            }
        );
    }

    #[test]
    fn parse_kv_u64_round_trip() {
        let content = "syscr: 123\nsyscw: 456\n";
        assert_eq!(parse_kv_u64(content, "syscr"), Some(123));
        assert_eq!(parse_kv_u64(content, "syscw"), Some(456));
        assert_eq!(parse_kv_u64(content, "missing"), None);
    }
}
