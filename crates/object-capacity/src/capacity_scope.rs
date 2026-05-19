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
const CAPACITY_SCOPE_REGISTRY_HARD_LIMIT: usize = 4_096;
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

fn enforce_hard_limit(entries: &mut HashMap<Uuid, CapacityScopeEntry>, max_len: usize) {
    if entries.len() < max_len {
        return;
    }

    let evict_count = entries.len() - max_len + 1;
    let mut eviction_order: Vec<_> = entries.iter().map(|(token, entry)| (*token, entry.recorded_at)).collect();
    eviction_order.sort_unstable_by_key(|(_, recorded_at)| *recorded_at);

    for (token, _) in eviction_order.into_iter().take(evict_count) {
        entries.remove(&token);
    }
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
    let mut entries = capacity_scope_registry().lock().unwrap_or_else(|p| p.into_inner());
    if !entries.contains_key(&token) && entries.len() >= CAPACITY_SCOPE_REGISTRY_SOFT_LIMIT {
        prune_expired_entries(&mut entries, now);
        enforce_hard_limit(&mut entries, CAPACITY_SCOPE_REGISTRY_HARD_LIMIT);
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
    let mut entries = capacity_scope_registry().lock().unwrap_or_else(|p| p.into_inner());
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

    let mut dirty_scopes = global_dirty_scope_registry().lock().unwrap_or_else(|p| p.into_inner());
    dirty_scopes.extend(scope.disks);
}

pub fn drain_global_dirty_scopes() -> Vec<CapacityScopeDisk> {
    let mut dirty_scopes = global_dirty_scope_registry().lock().unwrap_or_else(|p| p.into_inner());
    dirty_scopes.drain().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;
    use std::thread;

    fn test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn clear_capacity_scope_registry_for_test() {
        capacity_scope_registry()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
        global_dirty_scope_registry()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clear();
    }

    fn poison_capacity_scope_registry_for_test() {
        let _ = thread::spawn(|| {
            let _guard = capacity_scope_registry()
                .lock()
                .expect("capacity scope registry lock should succeed");
            panic!("poison capacity scope registry");
        })
        .join();
    }

    fn poison_global_dirty_scope_registry_for_test() {
        let _ = thread::spawn(|| {
            let _guard = global_dirty_scope_registry()
                .lock()
                .expect("global dirty scope registry lock should succeed");
            panic!("poison global dirty scope registry");
        })
        .join();
    }

    #[test]
    fn record_and_take_capacity_scope_round_trips() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        clear_capacity_scope_registry_for_test();
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
        clear_capacity_scope_registry_for_test();
    }

    #[test]
    fn record_capacity_scope_merges_disks_for_same_token() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        clear_capacity_scope_registry_for_test();
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
        clear_capacity_scope_registry_for_test();
    }

    #[test]
    fn record_capacity_scope_enforces_hard_limit() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        clear_capacity_scope_registry_for_test();

        for _ in 0..(CAPACITY_SCOPE_REGISTRY_HARD_LIMIT + 32) {
            record_capacity_scope(
                Uuid::new_v4(),
                CapacityScope {
                    disks: vec![CapacityScopeDisk {
                        endpoint: "node-a".to_string(),
                        drive_path: "/tmp/disk-a".to_string(),
                    }],
                },
            );
        }

        let entries = capacity_scope_registry()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        assert!(entries.len() <= CAPACITY_SCOPE_REGISTRY_HARD_LIMIT);
        drop(entries);
        clear_capacity_scope_registry_for_test();
    }

    #[test]
    fn record_capacity_scope_recovers_from_poisoned_registry() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        clear_capacity_scope_registry_for_test();
        poison_capacity_scope_registry_for_test();
        let token = Uuid::new_v4();
        let scope = CapacityScope {
            disks: vec![CapacityScopeDisk {
                endpoint: "node-a".to_string(),
                drive_path: "/tmp/disk-a".to_string(),
            }],
        };

        record_capacity_scope(token, scope.clone());

        assert_eq!(take_capacity_scope(token), Some(scope));
        clear_capacity_scope_registry_for_test();
    }

    #[test]
    fn record_and_drain_global_dirty_scope_round_trips() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        clear_capacity_scope_registry_for_test();
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
        clear_capacity_scope_registry_for_test();
    }

    #[test]
    fn record_global_dirty_scope_recovers_from_poisoned_registry() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        clear_capacity_scope_registry_for_test();
        poison_global_dirty_scope_registry_for_test();

        record_global_dirty_scope(CapacityScope {
            disks: vec![CapacityScopeDisk {
                endpoint: "node-a".to_string(),
                drive_path: "/tmp/disk-a".to_string(),
            }],
        });

        let drained = drain_global_dirty_scopes();
        assert_eq!(drained.len(), 1);
        assert_eq!(drained[0].endpoint, "node-a");
        clear_capacity_scope_registry_for_test();
    }
}
