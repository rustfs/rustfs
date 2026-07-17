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
use std::sync::atomic::{AtomicU64, Ordering};
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

/// Monotonic generation of the global dirty-scope registry.
///
/// Advanced every time a non-empty drain removes disks from the registry. A
/// storage-set that recorded its disks at generation `g` can safely skip the
/// registry mutex on subsequent writes as long as the generation is still `g`:
/// any drain that could have removed its disks would have advanced the counter,
/// forcing the set to re-mark (backlog#1315). The generation is loaded and
/// advanced only while the registry mutex is held, so a set that observes
/// `generation == g` at record time is guaranteed its disks are still present
/// until the next drain.
static DIRTY_GENERATION: AtomicU64 = AtomicU64::new(0);

/// Test-only counter of how many times the global registry mutex was upgraded
/// to insert dirty disks. Steady-state writes must reuse an existing generation
/// mark and leave this untouched; only the first write of each generation
/// bumps it. Used by white-box tests to prove the per-generation skip holds and
/// to fail closed if the optimization regresses (backlog#1315).
static GLOBAL_DIRTY_UPGRADE_COUNT: AtomicU64 = AtomicU64::new(0);

/// Current global dirty-scope generation. See [`DIRTY_GENERATION`].
pub fn current_dirty_generation() -> u64 {
    DIRTY_GENERATION.load(Ordering::Acquire)
}

/// Number of times the global dirty registry mutex was upgraded to record
/// disks. Test/observability hook (backlog#1315).
pub fn global_dirty_upgrade_count() -> u64 {
    GLOBAL_DIRTY_UPGRADE_COUNT.load(Ordering::Relaxed)
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
        // An expired entry would already be discarded by take_capacity_scope;
        // merging into it (and refreshing recorded_at) would resurrect stale
        // disks. Replace it with the fresh scope instead (backlog#1022 #35).
        if now.duration_since(entry.recorded_at) > CAPACITY_SCOPE_TTL {
            *entry = CapacityScopeEntry { scope, recorded_at: now };
        } else {
            merge_capacity_scopes(&mut entry.scope, scope);
            entry.recorded_at = now;
        }
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

/// Record dirty disks in the global registry, returning the registry generation
/// observed while the mutex was held.
///
/// Callers cache the returned generation and, on subsequent writes, compare it
/// against [`current_dirty_generation`]: while it is unchanged the disks are
/// still queued for the next drain and the registry mutex can be skipped
/// entirely (backlog#1315). An empty scope is a no-op that returns the current
/// generation without touching the mutex.
pub fn record_global_dirty_scope(scope: CapacityScope) -> u64 {
    if scope.disks.is_empty() {
        return current_dirty_generation();
    }

    let mut dirty_scopes = global_dirty_scope_registry().lock().unwrap_or_else(|p| p.into_inner());
    dirty_scopes.extend(scope.disks);
    GLOBAL_DIRTY_UPGRADE_COUNT.fetch_add(1, Ordering::Relaxed);
    // Load under the registry lock so the value is coherent with any concurrent
    // drain (which advances the generation under the same lock). A set that
    // stores this value only re-marks once a later drain moves past it.
    DIRTY_GENERATION.load(Ordering::Acquire)
}

pub fn drain_global_dirty_scopes() -> Vec<CapacityScopeDisk> {
    let mut dirty_scopes = global_dirty_scope_registry().lock().unwrap_or_else(|p| p.into_inner());
    if dirty_scopes.is_empty() {
        // Nothing to drain: leave the generation untouched so sets that already
        // marked keep their skip fast-path. Advancing here would only force
        // redundant re-marks without changing correctness.
        return Vec::new();
    }
    // Advance before draining so the new generation is visible under the lock;
    // any set that recorded at the old generation will observe the change and
    // re-mark on its next write, and the disks it just wrote are read by the
    // refresh that consumes this drain (the write commits before it records the
    // scope, and this bump is ordered after that record in the registry's
    // modification order).
    DIRTY_GENERATION.fetch_add(1, Ordering::AcqRel);
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
        DIRTY_GENERATION.store(0, Ordering::Release);
        GLOBAL_DIRTY_UPGRADE_COUNT.store(0, Ordering::Release);
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
    fn record_capacity_scope_replaces_expired_entry_instead_of_merging() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        clear_capacity_scope_registry_for_test();
        let token = Uuid::new_v4();
        let stale_disk = CapacityScopeDisk {
            endpoint: "node-old".to_string(),
            drive_path: "/tmp/disk-old".to_string(),
        };
        let fresh_disk = CapacityScopeDisk {
            endpoint: "node-new".to_string(),
            drive_path: "/tmp/disk-new".to_string(),
        };

        record_capacity_scope(token, CapacityScope { disks: vec![stale_disk] });
        // Backdate the entry beyond the TTL: take_capacity_scope would drop
        // it, so a new record for the same token must not resurrect it.
        capacity_scope_registry()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .get_mut(&token)
            .expect("entry must exist")
            .recorded_at = Instant::now() - CAPACITY_SCOPE_TTL - Duration::from_secs(1);

        record_capacity_scope(
            token,
            CapacityScope {
                disks: vec![fresh_disk.clone()],
            },
        );

        assert_eq!(take_capacity_scope(token), Some(CapacityScope { disks: vec![fresh_disk] }));
        clear_capacity_scope_registry_for_test();
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
    fn record_global_dirty_scope_generation_is_stable_until_drain() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        clear_capacity_scope_registry_for_test();

        let disk = CapacityScopeDisk {
            endpoint: "node-a".to_string(),
            drive_path: "/tmp/disk-a".to_string(),
        };
        let scope = CapacityScope {
            disks: vec![disk.clone()],
        };

        // First record upgrades the registry and returns the current generation.
        let gen0 = record_global_dirty_scope(scope.clone());
        assert_eq!(gen0, current_dirty_generation());
        assert_eq!(global_dirty_upgrade_count(), 1);

        // A subsequent record while the generation is unchanged still returns
        // the same generation; the caller's skip fast-path keys off equality.
        let gen1 = record_global_dirty_scope(scope);
        assert_eq!(gen1, gen0);
        assert_eq!(global_dirty_upgrade_count(), 2);

        // Draining advances the generation so a caller that cached gen0 is
        // forced to re-mark on its next write.
        let drained = drain_global_dirty_scopes();
        assert_eq!(drained, vec![disk]);
        assert_ne!(current_dirty_generation(), gen0);
        assert_eq!(current_dirty_generation(), gen0 + 1);

        clear_capacity_scope_registry_for_test();
    }

    #[test]
    fn drain_empty_registry_does_not_advance_generation() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        clear_capacity_scope_registry_for_test();

        let before = current_dirty_generation();
        assert!(drain_global_dirty_scopes().is_empty());
        assert_eq!(current_dirty_generation(), before);

        clear_capacity_scope_registry_for_test();
    }

    #[test]
    fn record_empty_scope_is_noop_without_upgrade() {
        let _guard = test_lock().lock().expect("test lock poisoned");
        clear_capacity_scope_registry_for_test();

        let observed = record_global_dirty_scope(CapacityScope::default());
        assert_eq!(observed, current_dirty_generation());
        assert_eq!(global_dirty_upgrade_count(), 0);
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
