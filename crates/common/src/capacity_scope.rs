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

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};
use uuid::Uuid;

const CAPACITY_SCOPE_REGISTRY_SOFT_LIMIT: usize = 2_048;
const CAPACITY_SCOPE_TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CapacityScopeDisk {
    pub endpoint: String,
    pub drive_path: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct CapacityScope {
    pub disks: Vec<CapacityScopeDisk>,
}

#[derive(Debug, Clone)]
struct CapacityScopeEntry {
    scope: CapacityScope,
    recorded_at: Instant,
}

fn capacity_scope_registry() -> &'static Mutex<HashMap<Uuid, CapacityScopeEntry>> {
    static REGISTRY: OnceLock<Mutex<HashMap<Uuid, CapacityScopeEntry>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn global_dirty_scope_registry() -> &'static Mutex<HashSet<CapacityScopeDisk>> {
    static REGISTRY: OnceLock<Mutex<HashSet<CapacityScopeDisk>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashSet::new()))
}

fn prune_expired_entries(entries: &mut HashMap<Uuid, CapacityScopeEntry>, now: Instant) {
    entries.retain(|_, entry| now.duration_since(entry.recorded_at) <= CAPACITY_SCOPE_TTL);
}

fn merge_capacity_scopes(existing: &mut CapacityScope, incoming: CapacityScope) {
    let mut seen: HashSet<CapacityScopeDisk> = existing.disks.iter().cloned().collect();
    for disk in incoming.disks {
        if seen.insert(disk.clone()) {
            existing.disks.push(disk);
        }
    }
}

pub fn record_capacity_scope(token: Uuid, scope: CapacityScope) {
    let now = Instant::now();
    let mut entries = capacity_scope_registry().lock().expect("capacity scope registry poisoned");
    if entries.len() >= CAPACITY_SCOPE_REGISTRY_SOFT_LIMIT {
        prune_expired_entries(&mut entries, now);
    }
    if let Some(entry) = entries.get_mut(&token) {
        merge_capacity_scopes(&mut entry.scope, scope);
        entry.recorded_at = now;
    } else {
        entries.insert(token, CapacityScopeEntry { scope, recorded_at: now });
    }
}

pub fn take_capacity_scope(token: Uuid) -> Option<CapacityScope> {
    let now = Instant::now();
    let mut entries = capacity_scope_registry().lock().expect("capacity scope registry poisoned");
    let entry = entries.remove(&token)?;
    if now.duration_since(entry.recorded_at) > CAPACITY_SCOPE_TTL {
        return None;
    }
    Some(entry.scope)
}

pub fn record_global_dirty_scope(scope: CapacityScope) {
    if scope.disks.is_empty() {
        return;
    }

    let mut dirty_scopes = global_dirty_scope_registry()
        .lock()
        .expect("global dirty scope registry poisoned");
    dirty_scopes.extend(scope.disks);
}

pub fn drain_global_dirty_scopes() -> Vec<CapacityScopeDisk> {
    let mut dirty_scopes = global_dirty_scope_registry()
        .lock()
        .expect("global dirty scope registry poisoned");
    dirty_scopes.drain().collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_and_take_capacity_scope_round_trips() {
        let token = Uuid::new_v4();
        let scope = CapacityScope {
            disks: vec![CapacityScopeDisk {
                endpoint: "node-a".to_string(),
                drive_path: "/tmp/disk-a".to_string(),
            }],
        };

        record_capacity_scope(token, scope.clone());

        assert_eq!(take_capacity_scope(token), Some(scope));
        assert_eq!(take_capacity_scope(token), None);
    }

    #[test]
    fn record_capacity_scope_merges_disks_for_same_token() {
        let token = Uuid::new_v4();
        record_capacity_scope(
            token,
            CapacityScope {
                disks: vec![CapacityScopeDisk {
                    endpoint: "node-a".to_string(),
                    drive_path: "/tmp/disk-a".to_string(),
                }],
            },
        );
        record_capacity_scope(
            token,
            CapacityScope {
                disks: vec![
                    CapacityScopeDisk {
                        endpoint: "node-b".to_string(),
                        drive_path: "/tmp/disk-b".to_string(),
                    },
                    CapacityScopeDisk {
                        endpoint: "node-a".to_string(),
                        drive_path: "/tmp/disk-a".to_string(),
                    },
                ],
            },
        );

        let scope = take_capacity_scope(token).expect("scope should exist");
        assert_eq!(scope.disks.len(), 2);
        assert!(scope.disks.iter().any(|disk| disk.endpoint == "node-a"));
        assert!(scope.disks.iter().any(|disk| disk.endpoint == "node-b"));
    }

    #[test]
    fn record_and_drain_global_dirty_scope_round_trips() {
        record_global_dirty_scope(CapacityScope {
            disks: vec![CapacityScopeDisk {
                endpoint: "node-a".to_string(),
                drive_path: "/tmp/disk-a".to_string(),
            }],
        });
        record_global_dirty_scope(CapacityScope {
            disks: vec![
                CapacityScopeDisk {
                    endpoint: "node-b".to_string(),
                    drive_path: "/tmp/disk-b".to_string(),
                },
                CapacityScopeDisk {
                    endpoint: "node-a".to_string(),
                    drive_path: "/tmp/disk-a".to_string(),
                },
            ],
        });

        let drained = drain_global_dirty_scopes();
        assert_eq!(drained.len(), 2);
        assert!(drained.iter().any(|disk| disk.endpoint == "node-a"));
        assert!(drained.iter().any(|disk| disk.endpoint == "node-b"));
        assert!(drain_global_dirty_scopes().is_empty());
    }
}
