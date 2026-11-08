// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::{FdCache, FdCacheEntry, FdKey};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;

fn key(volume: &str, path: &str, direct: bool) -> FdKey {
    FdKey {
        volume: volume.to_owned(),
        path: path.to_owned(),
        direct,
    }
}

fn fixture() -> (tempfile::TempDir, Arc<File>) {
    let directory = tempfile::tempdir().expect("create cache fixture directory");
    let path = directory.path().join("part");
    std::fs::write(&path, b"old").expect("write original cache fixture");
    let file = Arc::new(File::open(path).expect("open original cache fixture"));
    (directory, file)
}

// Both key variants intentionally use normal files. These tests exercise the
// real Moka cache's identity/invalidation contract, not native O_DIRECT I/O.
#[tokio::test]
async fn uring_fd_cache_exact_invalidation_removes_both_modes_and_preserves_other_paths() {
    let (_directory, file) = fixture();
    let cache = FdCache::new();
    let paths = [("v", "a/b"), ("v", "a/b/part.1"), ("v", "a/bc"), ("other", "a/b")];
    for (volume, path) in paths {
        for direct in [false, true] {
            cache.insert(key(volume, path, direct), Arc::clone(&file)).await;
        }
    }
    assert_eq!(cache.entry_count().await, 8, "all mode/scope fixtures must exist before invalidation");
    cache.invalidate_exact("v", "a/b").await;
    for (volume, path) in paths {
        for direct in [false, true] {
            let hit = cache.get(&key(volume, path, direct)).await.is_some();
            assert_eq!(
                hit,
                (volume, path) != ("v", "a/b"),
                "wrong invalidation scope: {volume}/{path}, direct={direct}"
            );
        }
    }
}

#[tokio::test]
async fn uring_fd_cache_exact_invalidation_bumps_one_generation_and_rejects_old_epoch_inserts() {
    let (_directory, file) = fixture();
    let cache = FdCache::new();
    let entry = Arc::new(FdCacheEntry {
        file,
        len: 3,
        invalidated: AtomicBool::new(false),
    });
    let old_generation = cache.generation();
    cache.invalidate_exact("v", "part").await;
    let current_generation = cache.generation();
    assert_eq!(current_generation, old_generation + 1, "one logical invalidation must advance once");
    for direct in [false, true] {
        let key = key("v", "part", direct);
        cache.insert_if_fresh(key.clone(), Arc::clone(&entry), old_generation).await;
        assert!(cache.get(&key).await.is_none(), "old-epoch insert resurrected direct={direct}");
        cache
            .insert_if_fresh(key.clone(), Arc::clone(&entry), current_generation)
            .await;
        assert!(cache.get(&key).await.is_some(), "current epoch must allow refill, direct={direct}");
    }
}

#[tokio::test]
async fn uring_fd_cache_exact_invalidation_accepts_missing_or_single_mode_entries() {
    let (_directory, file) = fixture();
    for present in [None, Some(false), Some(true)] {
        let cache = FdCache::new();
        if let Some(direct) = present {
            cache.insert(key("v", "part", direct), Arc::clone(&file)).await;
            assert!(cache.get(&key("v", "part", direct)).await.is_some());
        }
        cache.invalidate_exact("v", "part").await;
        for direct in [false, true] {
            assert!(cache.get(&key("v", "part", direct)).await.is_none(), "mode survived: {present:?}");
        }
        assert_eq!(cache.generation(), 1);
        cache.invalidate_exact("v", "part").await;
        assert_eq!(cache.generation(), 2, "repeated invalidation still fences concurrent miss opens");
    }
}

#[tokio::test]
async fn uring_fd_cache_prefix_invalidation_covers_both_modes_without_crossing_components() {
    let (_directory, file) = fixture();
    let cache = FdCache::new();
    let paths = [("v", "a/b"), ("v", "a/b/part.1"), ("v", "a/bc"), ("other", "a/b")];
    for (volume, path) in paths {
        for direct in [false, true] {
            cache.insert(key(volume, path, direct), Arc::clone(&file)).await;
            assert!(cache.get(&key(volume, path, direct)).await.is_some());
        }
    }
    cache.invalidate_under("v", "a/b/");
    for (volume, path) in paths {
        for direct in [false, true] {
            let hit = cache.get(&key(volume, path, direct)).await.is_some();
            let removed = volume == "v" && matches!(path, "a/b" | "a/b/part.1");
            assert_eq!(hit, !removed, "wrong prefix scope: {volume}/{path}, direct={direct}");
        }
    }
}

#[tokio::test]
async fn uring_fd_cache_volume_and_clear_invalidation_cover_both_modes() {
    let (_directory, file) = fixture();
    let cache = FdCache::new();
    for volume in ["v", "other"] {
        for direct in [false, true] {
            cache.insert(key(volume, "part", direct), Arc::clone(&file)).await;
            assert!(cache.get(&key(volume, "part", direct)).await.is_some());
        }
    }
    cache.invalidate_volume("v");
    for direct in [false, true] {
        assert!(cache.get(&key("v", "part", direct)).await.is_none());
        assert!(cache.get(&key("other", "part", direct)).await.is_some());
        cache.insert(key("v", "part", direct), Arc::clone(&file)).await;
        assert!(
            cache.get(&key("v", "part", direct)).await.is_some(),
            "new entries must survive the earlier volume invalidation"
        );
    }
    cache.clear();
    for volume in ["v", "other"] {
        for direct in [false, true] {
            assert!(cache.get(&key(volume, "part", direct)).await.is_none());
        }
    }
}

#[tokio::test]
async fn uring_fd_cache_invalidation_keeps_borrowed_old_inode_alive_and_allows_replacement_refill() {
    let (directory, file) = fixture();
    let cache = FdCache::new();
    let mut borrowed = Vec::new();
    for direct in [false, true] {
        let key = key("v", "part", direct);
        cache.insert(key.clone(), Arc::clone(&file)).await;
        borrowed.push(cache.get(&key).await.expect("borrow original inode before replacement"));
    }
    drop(file);
    let replacement = directory.path().join("replacement");
    std::fs::write(&replacement, b"replacement").expect("write replacement inode");
    std::fs::rename(&replacement, directory.path().join("part")).expect("replace cached path with new inode");
    cache.invalidate_exact("v", "part").await;
    for (entry, direct) in borrowed.iter().zip([false, true]) {
        assert!(cache.get(&key("v", "part", direct)).await.is_none());
        let mut bytes = [0; 3];
        entry
            .file
            .read_exact_at(&mut bytes, 0)
            .expect("borrowed descriptor remains readable after invalidation");
        assert_eq!(&bytes, b"old");
        assert_eq!(entry.len, 3, "borrowed size snapshot belongs to the old inode");
    }
    let replacement = Arc::new(FdCacheEntry {
        file: Arc::new(File::open(directory.path().join("part")).expect("open replacement inode")),
        len: 11,
        invalidated: AtomicBool::new(false),
    });
    for direct in [false, true] {
        let key = key("v", "part", direct);
        cache
            .insert_if_fresh(key.clone(), Arc::clone(&replacement), cache.generation())
            .await;
        let fresh = cache.get(&key).await.expect("replacement should populate the current epoch");
        let mut bytes = [0; 11];
        fresh.file.read_exact_at(&mut bytes, 0).expect("read replacement inode");
        assert_eq!(&bytes, b"replacement");
        assert_eq!(fresh.len, 11);
    }
}

#[tokio::test]
async fn fd_cache_tracker_skips_unmatched_invalidations_and_handles_deep_prefixes() {
    let (_directory, file) = fixture();
    let mut cache = FdCache::new();

    // Unmatched invalidations on an empty cache must not spawn the maintenance task.
    cache.invalidate_under("v", "missing/prefix");
    cache.invalidate_volume("v");
    cache.invalidate_exact("v", "missing/prefix/part.1").await;
    cache.clear();
    assert!(
        cache.maintenance_task.get().is_none(),
        "empty-cache invalidations must not spawn maintenance"
    );

    // Populate a normal entry and verify matched vs unmatched filter gates directly,
    // and ensure unmatched invalidations do not enqueue pending removals.
    let cached_key = key("v", "obj-1/0198a3f0/part.1", false);
    cache.insert(cached_key.clone(), Arc::clone(&file)).await;
    assert_eq!(cache.tracker.live_entries(), 1);
    assert!(cache.tracker.may_match_volume("v"));
    assert!(!cache.tracker.may_match_volume("other-volume"));
    assert!(cache.tracker.may_match_prefix("v", "obj-1"));
    assert!(cache.tracker.may_match_prefix("v", "obj-1/0198a3f0"));
    assert!(!cache.tracker.may_match_prefix("v", "obj-2/0198a3f0"));
    assert!(cache.tracker.may_match_exact("v", "obj-1/0198a3f0/part.1"));
    assert!(!cache.tracker.may_match_exact("v", "obj-1/0198a3f0/xl.meta"));

    cache.invalidate_volume("other-volume");
    cache.invalidate_under("v", "obj-2/0198a3f0");
    cache.invalidate_exact("v", "obj-1/0198a3f0/xl.meta").await;
    assert!(
        !cache
            .tracker
            .has_pending_invalidations
            .load(std::sync::atomic::Ordering::SeqCst),
        "unmatched invalidations must not enqueue maintenance removals"
    );
    assert!(cache.get(&cached_key).await.is_some());

    // Deeply nested path (> FD_CACHE_MAX_INDEXED_PREFIXES slashes) must fall back
    // to per-volume unindexed_counts without degrading other volumes.
    let other_vol_key = key("v2", "shallow/part.1", false);
    cache.insert(other_vol_key.clone(), Arc::clone(&file)).await;
    let deep_path = "c1/c2/c3/c4/c5/c6/c7/c8/c9/c10/c11/c12/c13/c14/part.1";
    let deep_key = key("v", deep_path, false);
    cache.insert(deep_key.clone(), Arc::clone(&file)).await;
    assert_eq!(
        cache
            .tracker
            .unindexed_counts
            .iter()
            .map(|c| c.load(std::sync::atomic::Ordering::Acquire))
            .sum::<u32>(),
        1
    );
    assert!(
        !cache.tracker.may_match_prefix("v2", "unmatched/prefix"),
        "deep key in volume v must not disable prefix filtering in volume v2"
    );
    cache.invalidate_under("v", "c1/c2/c3/c4/c5/c6/c7/c8/c9/c10/c11/c12/c13");
    assert!(cache.get(&deep_key).await.is_none());
    cache.invalidate_volume("v2");
    assert!(cache.get(&other_vol_key).await.is_none());

    // Replacing an invalidated key before maintenance drains pending_invalidations
    // must release the old descriptor immediately and keep the replacement live
    // even when a strong reference to the old CachedFdEntry shell survives until
    // maintenance runs its Arc::ptr_eq check.
    let old_file = Arc::new(tempfile::tempfile().expect("create replaced descriptor"));
    let old_weak = Arc::downgrade(&old_file);
    cache.insert(cached_key.clone(), old_file).await;
    let held_old_shell = cache.cache.get(&cached_key).expect("cached shell must exist");
    cache.invalidate_under("v", "obj-1/0198a3f0");
    assert!(cache.get(&cached_key).await.is_none());
    cache.insert(cached_key.clone(), Arc::clone(&file)).await;
    assert!(
        old_weak.upgrade().is_none(),
        "replacing an entry must retire and drop the previous descriptor immediately"
    );
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while cache
            .tracker
            .has_pending_invalidations
            .load(std::sync::atomic::Ordering::SeqCst)
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("maintenance should drain pending invalidations");
    drop(held_old_shell);
    assert!(
        cache.get(&cached_key).await.is_some(),
        "stale pending invalidation must not evict a newer replacement"
    );
    assert_eq!(
        cache
            .tracker
            .unindexed_counts
            .iter()
            .map(|c| c.load(std::sync::atomic::Ordering::Acquire))
            .sum::<u32>(),
        0
    );

    // Dropping a cache while a live non-invalidated entry is present (even if a
    // cloned Moka handle outlives FdCache and the maintenance task is aborted)
    // must retire the descriptor and release FdCacheTracker without cycles.
    let live_drop_file = Arc::new(tempfile::tempfile().expect("create live drop descriptor"));
    let live_drop_weak = Arc::downgrade(&live_drop_file);
    let shutdown_file = Arc::new(tempfile::tempfile().expect("create shutdown descriptor"));
    let shutdown_weak = Arc::downgrade(&shutdown_file);
    let tracker_weak = Arc::downgrade(&cache.tracker);
    let cloned_moka = cache.cache.clone();
    cache.insert(key("v", "live-on-drop/part.1", false), live_drop_file).await;
    cache.insert(key("v", "shutdown/part.1", false), shutdown_file).await;
    cache.invalidate_under("v", "shutdown");
    if let Some(handle) = cache.maintenance_task.take() {
        handle.abort();
        let _ = handle.await;
    }
    drop(cache);
    assert!(
        live_drop_weak.upgrade().is_none(),
        "Drop for FdCache must retire live descriptors even if a cloned Moka handle survives"
    );
    assert!(
        shutdown_weak.upgrade().is_none(),
        "dropping cache when maintenance is aborted must still release descriptors"
    );
    assert!(
        tracker_weak.upgrade().is_none(),
        "dropping cache must release FdCacheTracker without reference cycles"
    );
    drop(cloned_moka);

    // Short-TTL expired entries (including mixed expired and fresh entries) must
    // be retired by invalidate_under and clear() even though Moka's iterator skips
    // expired entries.
    let ttl_cache = FdCache::with_ttl(std::time::Duration::from_millis(15));
    let expired_file = Arc::new(tempfile::tempfile().expect("create short-ttl descriptor"));
    let expired_weak = Arc::downgrade(&expired_file);
    ttl_cache.insert(key("v", "ttl-obj/part.1", false), expired_file).await;
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let fresh_file = Arc::new(tempfile::tempfile().expect("create fresh descriptor"));
    let fresh_weak = Arc::downgrade(&fresh_file);
    ttl_cache.insert(key("v", "ttl-obj/part.2", false), fresh_file).await;
    ttl_cache.invalidate_under("v", "ttl-obj");
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while expired_weak.upgrade().is_some() || fresh_weak.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("invalidate_under must retire both expired and fresh matching descriptors");
    assert_eq!(ttl_cache.tracker.live_entries(), 0);

    let clear_expired_file = Arc::new(tempfile::tempfile().expect("create clear short-ttl descriptor"));
    let clear_expired_weak = Arc::downgrade(&clear_expired_file);
    ttl_cache
        .insert(key("v", "ttl-clear/part.1", false), clear_expired_file)
        .await;
    tokio::time::sleep(std::time::Duration::from_millis(30)).await;
    let clear_live_file = Arc::new(tempfile::tempfile().expect("create clear live descriptor"));
    let clear_live_weak = Arc::downgrade(&clear_live_file);
    ttl_cache.insert(key("v", "ttl-clear/part.2", false), clear_live_file).await;
    ttl_cache.clear();
    assert!(
        clear_expired_weak.upgrade().is_none() && clear_live_weak.upgrade().is_none(),
        "clear() must synchronously retire both TTL-expired and live descriptors"
    );
    assert_eq!(ttl_cache.tracker.live_entries(), 0);
}

#[tokio::test]
async fn fd_cache_lockstep_oracle_equivalence_and_counter_conservation() {
    use std::collections::HashMap;
    use std::sync::atomic::Ordering;

    let (_directory, file) = fixture();
    let cache = FdCache::with_ttl(std::time::Duration::from_secs(3600));
    let mut oracle: HashMap<FdKey, Arc<FdCacheEntry>> = HashMap::new();

    let volumes = ["vol-a", "vol-b", "vol-c", ".rustfs.sys/multipart"];
    let paths = [
        "a/b",
        "a/b/part.1",
        "a/b/dir/part.2",
        "a/bc",
        "a/bc/part.1",
        "prefix/obj-0001.bin/0198a3f0/part.1",
        "prefix/obj-0002.bin/0198a3f0/part.1",
        "d1/d2/d3/d4/d5/d6/d7/d8/d9/d10/d11/d12/d13/part.1",
    ];
    let prefixes = [
        "a",
        "a/",
        "a/b",
        "a/b/",
        "a/b/dir",
        "a/bc",
        "prefix",
        "prefix/obj-0001.bin",
        "prefix/obj-0001.bin/0198a3f0",
        "d1/d2/d3/d4/d5/c_missing",
        "d1/d2/d3/d4/d5/d6/d7/d8/d9/d10/d11/d12/d13",
        "nonexistent/prefix",
    ];

    let mut candidate_keys = Vec::new();
    for v in volumes {
        for p in paths {
            for direct in [false, true] {
                candidate_keys.push(key(v, p, direct));
            }
        }
    }

    let mut rng = 0x9e37_79b9_7f4a_7c15u64;
    let mut next_u64 = || {
        rng ^= rng << 13;
        rng ^= rng >> 7;
        rng ^= rng << 17;
        rng
    };

    for step in 0..1000 {
        let op = next_u64() % 100;
        if op < 45 {
            // Insert or replace (either fresh or stale generation)
            let k = candidate_keys[(next_u64() as usize) % candidate_keys.len()].clone();
            let entry = Arc::new(FdCacheEntry {
                file: Arc::clone(&file),
                len: step,
                invalidated: AtomicBool::new(false),
            });
            let cur_gen = cache.generation();
            let stale = (next_u64() % 5) == 0 && cur_gen > 0;
            let gen_to_use = if stale { cur_gen - 1 } else { cur_gen };
            cache.insert_if_fresh(k.clone(), Arc::clone(&entry), gen_to_use).await;
            if !stale {
                oracle.insert(k, entry);
            }
        } else if op < 65 {
            // Exact invalidation
            let v = volumes[(next_u64() as usize) % volumes.len()];
            let p = paths[(next_u64() as usize) % paths.len()];
            cache.invalidate_exact(v, p).await;
            oracle.remove(&key(v, p, false));
            oracle.remove(&key(v, p, true));
        } else if op < 88 {
            // Prefix invalidation
            let v = volumes[(next_u64() as usize) % volumes.len()];
            let raw_prefix = prefixes[(next_u64() as usize) % prefixes.len()];
            cache.invalidate_under(v, raw_prefix);
            let trimmed = raw_prefix.trim_end_matches('/');
            oracle.retain(|k, _| {
                !(k.volume == v && (k.path == trimmed || k.path.strip_prefix(trimmed).is_some_and(|r| r.starts_with('/'))))
            });
        } else if op < 97 {
            // Volume invalidation
            let v = volumes[(next_u64() as usize) % volumes.len()];
            cache.invalidate_volume(v);
            oracle.retain(|k, _| k.volume != v);
        } else {
            // Full clear
            cache.clear();
            oracle.clear();
        }

        // Verify lockstep equivalence against the reference oracle after every operation.
        for k in &candidate_keys {
            let got = cache.get(k).await;
            let expected = oracle.get(k);
            match (got, expected) {
                (Some(g), Some(e)) => assert!(
                    Arc::ptr_eq(&g, e),
                    "step {step}: pointer mismatch for key ({}, {}, {})",
                    k.volume,
                    k.path,
                    k.direct
                ),
                (None, None) => {}
                (g, e) => panic!(
                    "step {step}: oracle mismatch for key ({}, {}, {}): got={}, expected={}",
                    k.volume,
                    k.path,
                    k.direct,
                    g.is_some(),
                    e.is_some()
                ),
            }
        }
    }

    // Clear and wait for maintenance to drain all entries, then verify exact counter conservation.
    cache.clear();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while cache.tracker.live_entries() != 0 || cache.tracker.has_pending_invalidations.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("clear must drain all live entries and pending invalidations");

    assert_eq!(cache.tracker.live_entries(), 0);
    assert!(cache.tracker.unindexed_counts.iter().all(|c| c.load(Ordering::SeqCst) == 0));
    assert!(cache.tracker.volume_counts.iter().all(|c| c.load(Ordering::SeqCst) == 0));
    assert!(cache.tracker.exact_counts.iter().all(|c| c.load(Ordering::SeqCst) == 0));
    assert!(cache.tracker.prefix_counts.iter().all(|c| c.load(Ordering::SeqCst) == 0));
    assert!(cache.tracker.prefix_shards.iter().all(|s| s.lock().is_empty()));
    assert!(cache.tracker.entry_shards.iter().all(|s| s.lock().is_empty()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fd_cache_concurrent_insert_and_invalidation_stress() {
    use std::sync::atomic::{AtomicU64, Ordering};

    let (_directory, file) = fixture();
    let cache = Arc::new(FdCache::new());

    // Deterministic Dekker-partner interleaving check: pause `CachedFdEntry::new`
    // at `entry_shards[shard].lock()` after it has passed the initial generation
    // check and incremented `volume_counts`, bump generation via an unmatched
    // invalidation, and verify the post-`new()` recheck refuses publication and
    // preserves the existing entry.
    let seam_key = key("v-seam", "obj-1/part.1", false);
    cache.insert(seam_key.clone(), Arc::clone(&file)).await;
    let initial_entry = cache.get(&seam_key).await.expect("initial entry must exist");
    let (vol_hash, vol_slot) = super::fd_cache_volume_hash_and_slot("v-seam");
    let entry_shard = (vol_hash as usize) & (super::FD_CACHE_PREFIX_SHARDS - 1);
    let stale_gen = cache.generation();
    let stale_entry = Arc::new(FdCacheEntry {
        file: Arc::clone(&file),
        len: 999_999,
        invalidated: AtomicBool::new(false),
    });
    let blocked_insert = {
        let hold = cache.tracker.entry_shards[entry_shard].lock();
        let cache_clone = Arc::clone(&cache);
        let seam_key_clone = seam_key.clone();
        let task = tokio::spawn(async move {
            cache_clone.insert_if_fresh(seam_key_clone, stale_entry, stale_gen).await;
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        while cache.tracker.volume_counts[usize::from(vol_slot)].load(Ordering::SeqCst) < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "insert never reached CachedFdEntry::new with the previous entry still live"
            );
            std::thread::yield_now();
        }
        cache.invalidate_under("v-seam", "unmatched-prefix");
        drop(hold);
        task
    };
    blocked_insert.await.expect("blocked insert task should complete");
    let preserved = cache
        .get(&seam_key)
        .await
        .expect("refused stale insert must not retire a valid previous entry");
    assert!(Arc::ptr_eq(&preserved, &initial_entry));
    cache.invalidate_volume("v-seam");

    let floor_v1 = Arc::new(AtomicU64::new(0));
    let floor_v2 = Arc::new(AtomicU64::new(0));
    let mut handles = Vec::new();

    for t in 0..8usize {
        let cache = Arc::clone(&cache);
        let file = Arc::clone(&file);
        let floor_v1 = Arc::clone(&floor_v1);
        let floor_v2 = Arc::clone(&floor_v2);
        handles.push(tokio::spawn(async move {
            for i in 0..250u64 {
                let is_v1 = (t + i as usize).is_multiple_of(2);
                let vol = if is_v1 { "v1" } else { "v2" };
                let path = format!("dir-{}/obj-{}/part.1", t % 4, i % 16);
                let k = key(vol, &path, i.is_multiple_of(2));
                let g = cache.generation();
                let entry = Arc::new(FdCacheEntry {
                    file: Arc::clone(&file),
                    len: g,
                    invalidated: AtomicBool::new(false),
                });
                cache.insert_if_fresh(k.clone(), entry, g).await;
                let min_gen = if is_v1 {
                    floor_v1.load(Ordering::SeqCst)
                } else {
                    floor_v2.load(Ordering::SeqCst)
                };
                if let Some(hit) = cache.get(&k).await {
                    assert!(
                        hit.len >= min_gen,
                        "stale descriptor served after volume invalidation: opened_gen={}, floor_gen={min_gen}",
                        hit.len
                    );
                }
            }
        }));
    }

    for t in 0..4usize {
        let cache = Arc::clone(&cache);
        let floor_v1 = Arc::clone(&floor_v1);
        let floor_v2 = Arc::clone(&floor_v2);
        handles.push(tokio::spawn(async move {
            for i in 0..250u64 {
                let is_v1 = (t + i as usize).is_multiple_of(2);
                let vol = if is_v1 { "v1" } else { "v2" };
                match i % 4 {
                    0 => cache.invalidate_under(vol, &format!("dir-{}/obj-{}", t % 4, i % 16)),
                    1 => {
                        let p = format!("dir-{}/obj-{}/part.1", t % 4, i % 16);
                        cache.invalidate_exact(vol, &p).await;
                    }
                    2 => {
                        let before = cache.generation();
                        cache.invalidate_volume(vol);
                        let floor = before + 1;
                        if is_v1 {
                            floor_v1.fetch_max(floor, Ordering::SeqCst);
                        } else {
                            floor_v2.fetch_max(floor, Ordering::SeqCst);
                        }
                    }
                    _ => cache.invalidate_under("uncached-vol", "uncached/prefix"),
                }
                tokio::task::yield_now().await;
            }
        }));
    }

    for h in handles {
        h.await.expect("worker task should succeed");
    }

    cache.clear();
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        while cache.tracker.live_entries() != 0 || cache.tracker.has_pending_invalidations.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("clear after concurrent stress must drain all live entries");

    assert_eq!(cache.tracker.live_entries(), 0);
    assert!(cache.tracker.unindexed_counts.iter().all(|c| c.load(Ordering::SeqCst) == 0));
    assert!(cache.tracker.volume_counts.iter().all(|c| c.load(Ordering::SeqCst) == 0));
    assert!(cache.tracker.exact_counts.iter().all(|c| c.load(Ordering::SeqCst) == 0));
    assert!(cache.tracker.prefix_counts.iter().all(|c| c.load(Ordering::SeqCst) == 0));
    assert!(cache.tracker.prefix_shards.iter().all(|s| s.lock().is_empty()));
    assert!(cache.tracker.entry_shards.iter().all(|s| s.lock().is_empty()));
}
