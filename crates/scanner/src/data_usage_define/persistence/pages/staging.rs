// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use std::any::Any;
use std::sync::{Mutex, Weak};
use std::time::Instant as WallInstant;

/// A manifest commit can be interrupted after any group of immutable PUTs.
/// Reuse only receipts from the same store, epoch and predecessor revision.
struct Attempt {
    store: Weak<dyn Any + Send + Sync>,
    path: String,
    bundle: uuid::Uuid,
    epoch: u64,
    revision: DataUsageCacheRevision,
    seen: Arc<Mutex<HashSet<Digest>>>,
    last_used: WallInstant,
}

const MAX_ATTEMPTS: usize = 32;
static ATTEMPTS: LazyLock<Mutex<Vec<Attempt>>> = LazyLock::new(|| Mutex::new(Vec::new()));

pub(super) struct Staging(Arc<Mutex<HashSet<Digest>>>);

impl Staging {
    pub(super) fn open<S: ScannerObjectIO>(
        store: &Arc<S>,
        path: &str,
        bundle: uuid::Uuid,
        epoch: u64,
        revision: &DataUsageCacheRevision,
    ) -> Self {
        let store: Arc<dyn Any + Send + Sync> = store.clone();
        let owner = Arc::downgrade(&store);
        let mut attempts = ATTEMPTS.lock().unwrap_or_else(|p| p.into_inner());
        attempts.retain(|job| job.store.strong_count() != 0 && job.last_used.elapsed() < Duration::from_secs(3600));
        if let Some(attempt) = attempts.iter_mut().find(|attempt| {
            attempt.store.ptr_eq(&owner)
                && attempt.path == path
                && attempt.bundle == bundle
                && attempt.epoch == epoch
                && &attempt.revision == revision
        }) {
            attempt.last_used = WallInstant::now();
            return Self(attempt.seen.clone());
        }
        // A successfully committed predecessor naturally retires the receipts
        // for its previous ETag. Concurrent older writers retain their own Arc.
        attempts.retain(|attempt| !attempt.store.ptr_eq(&owner) || attempt.path != path);
        if attempts.len() >= MAX_ATTEMPTS
            && let Some((index, _)) = attempts.iter().enumerate().min_by_key(|(_, job)| job.last_used)
        {
            attempts.swap_remove(index);
        }
        let seen = Arc::new(Mutex::new(HashSet::new()));
        attempts.push(Attempt {
            store: owner,
            path: path.to_owned(),
            bundle,
            epoch,
            revision: revision.clone(),
            seen: seen.clone(),
            last_used: WallInstant::now(),
        });
        Self(seen)
    }

    pub(super) fn contains(&self, key: &Digest) -> bool {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).contains(key)
    }

    pub(super) fn record(&self, key: Digest) {
        self.0.lock().unwrap_or_else(|p| p.into_inner()).insert(key);
    }
}
