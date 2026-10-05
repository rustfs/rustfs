// Copyright 2024 RustFS Team
// SPDX-License-Identifier: Apache-2.0

use super::{FdCache, FdCacheEntry, FdKey};
use std::fs::File;
use std::os::unix::fs::FileExt;
use std::sync::Arc;

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
    let entry = Arc::new(FdCacheEntry { file, len: 3 });
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
            "new entries must survive the earlier volume predicate"
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
