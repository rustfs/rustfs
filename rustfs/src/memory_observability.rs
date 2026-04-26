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

use rustfs_io_metrics::{
    record_cgroup_memory_split, record_cpu_usage, record_memory_usage, record_process_memory_split,
    snapshot_process_resource_and_system,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use sysinfo::System;
use tokio_util::sync::CancellationToken;
use tracing::debug;

static MEMORY_SYSTEM: OnceLock<Mutex<System>> = OnceLock::new();

const ENV_MEMORY_OBSERVABILITY_INTERVAL_SECS: &str = "RUSTFS_MEMORY_OBSERVABILITY_INTERVAL_SECS";
const DEFAULT_MEMORY_OBSERVABILITY_INTERVAL_SECS: u64 = 15;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct CgroupMemorySnapshot {
    current_bytes: Option<u64>,
    limit_bytes: Option<u64>,
    anon_bytes: Option<u64>,
    file_bytes: Option<u64>,
    active_file_bytes: Option<u64>,
    inactive_file_bytes: Option<u64>,
}

fn memory_system() -> &'static Mutex<System> {
    MEMORY_SYSTEM.get_or_init(|| Mutex::new(System::new()))
}

fn refresh_total_memory() -> u64 {
    let mut system = memory_system().lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    system.refresh_memory();
    system.total_memory()
}

fn read_optional_u64(path: &Path) -> Option<u64> {
    let content = std::fs::read_to_string(path).ok()?;
    let trimmed = content.trim();
    if trimmed.is_empty() || trimmed == "max" {
        return None;
    }
    trimmed.parse::<u64>().ok()
}

fn parse_kv_stats(content: &str) -> HashMap<String, u64> {
    content
        .lines()
        .filter_map(|line| {
            let mut parts = line.split_whitespace();
            let key = parts.next()?;
            let value = parts.next()?.parse::<u64>().ok()?;
            Some((key.to_string(), value))
        })
        .collect()
}

fn read_cgroup_v2() -> Option<CgroupMemorySnapshot> {
    let root = Path::new("/sys/fs/cgroup");
    let stat_path = root.join("memory.stat");
    if !stat_path.exists() {
        return None;
    }

    let stats = parse_kv_stats(&std::fs::read_to_string(&stat_path).ok()?);
    Some(CgroupMemorySnapshot {
        current_bytes: read_optional_u64(&root.join("memory.current")),
        limit_bytes: read_optional_u64(&root.join("memory.max")),
        anon_bytes: stats.get("anon").copied(),
        file_bytes: stats.get("file").copied(),
        active_file_bytes: stats.get("active_file").copied(),
        inactive_file_bytes: stats.get("inactive_file").copied(),
    })
}

fn read_cgroup_v1() -> Option<CgroupMemorySnapshot> {
    let root = Path::new("/sys/fs/cgroup/memory");
    let stat_path = root.join("memory.stat");
    if !stat_path.exists() {
        return None;
    }

    let stats = parse_kv_stats(&std::fs::read_to_string(&stat_path).ok()?);
    Some(CgroupMemorySnapshot {
        current_bytes: read_optional_u64(&root.join("memory.usage_in_bytes")),
        limit_bytes: read_optional_u64(&root.join("memory.limit_in_bytes")),
        anon_bytes: stats.get("total_rss").copied().or_else(|| stats.get("rss").copied()),
        file_bytes: stats.get("total_cache").copied().or_else(|| stats.get("cache").copied()),
        active_file_bytes: stats
            .get("total_active_file")
            .copied()
            .or_else(|| stats.get("active_file").copied()),
        inactive_file_bytes: stats
            .get("total_inactive_file")
            .copied()
            .or_else(|| stats.get("inactive_file").copied()),
    })
}

fn read_cgroup_memory_snapshot() -> Option<CgroupMemorySnapshot> {
    read_cgroup_v2().or_else(read_cgroup_v1)
}

async fn record_memory_snapshot() {
    match tokio::task::spawn_blocking(|| {
        let (resource, process) = snapshot_process_resource_and_system();
        let total_memory = refresh_total_memory();
        let cgroup = read_cgroup_memory_snapshot();
        (resource, process, total_memory, cgroup)
    })
    .await
    {
        Ok((resource, process, total_memory, cgroup)) => {
            record_memory_usage(process.resident_memory_bytes, total_memory);
            record_cpu_usage(resource.cpu_percent);
            record_process_memory_split(process.resident_memory_bytes, process.virtual_memory_bytes);

            if let Some(cgroup) = cgroup {
                record_cgroup_memory_split(
                    cgroup.current_bytes,
                    cgroup.limit_bytes,
                    cgroup.anon_bytes,
                    cgroup.file_bytes,
                    cgroup.active_file_bytes,
                    cgroup.inactive_file_bytes,
                );
            }
        }
        Err(err) => {
            debug!(error = ?err, "memory observability sampler task failed");
        }
    }
}

pub fn init_memory_observability(ctx: CancellationToken) {
    let interval_secs =
        rustfs_utils::get_env_u64(ENV_MEMORY_OBSERVABILITY_INTERVAL_SECS, DEFAULT_MEMORY_OBSERVABILITY_INTERVAL_SECS);
    let interval = Duration::from_secs(interval_secs.max(1));

    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = ctx.cancelled() => {
                    debug!("memory observability sampler cancelled");
                    break;
                }
                _ = ticker.tick() => {
                    record_memory_snapshot().await;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::{CgroupMemorySnapshot, parse_kv_stats, read_optional_u64};
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn parse_kv_stats_extracts_numeric_pairs() {
        let parsed = parse_kv_stats("anon 12\nfile 34\nactive_file 56\n");
        assert_eq!(parsed.get("anon").copied(), Some(12));
        assert_eq!(parsed.get("file").copied(), Some(34));
        assert_eq!(parsed.get("active_file").copied(), Some(56));
    }

    #[test]
    fn read_optional_u64_parses_numeric_and_max_values() {
        let tempdir = tempfile::tempdir().expect("tempdir");
        let value_path: PathBuf = tempdir.path().join("value");
        let max_path: PathBuf = tempdir.path().join("max");
        fs::write(&value_path, "123\n").expect("write numeric");
        fs::write(&max_path, "max\n").expect("write max");

        assert_eq!(read_optional_u64(&value_path), Some(123));
        assert_eq!(read_optional_u64(&max_path), None);
    }

    #[test]
    fn cgroup_memory_snapshot_defaults_are_empty() {
        let snapshot = CgroupMemorySnapshot::default();
        assert_eq!(snapshot.current_bytes, None);
        assert_eq!(snapshot.limit_bytes, None);
        assert_eq!(snapshot.anon_bytes, None);
        assert_eq!(snapshot.file_bytes, None);
        assert_eq!(snapshot.active_file_bytes, None);
        assert_eq!(snapshot.inactive_file_bytes, None);
    }
}
