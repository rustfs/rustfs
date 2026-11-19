// Copyright 2026 RustFS Team
// Licensed under the Apache License, Version 2.0.

use super::*;
use crate::scanner_budget::ScannerCycleBudget;
use std::any::Any;
use std::sync::{Mutex, Weak};
use tokio_util::sync::CancellationToken;

// Keep only a failed root's identity while its independent backup is being
// recovered. A fresh primary reader still obtains the current ETag every time.
pub(super) struct PendingBackup {
    owner: Weak<dyn Any + Send + Sync>,
    name: String,
    failed_etag: String,
    last_polled: std::time::Instant,
}

static PENDING_BACKUPS: Mutex<Vec<PendingBackup>> = Mutex::new(Vec::new());
const MAX_PENDING_BACKUPS: usize = 32;

impl PendingBackup {
    fn owner<S: ScannerObjectIO>(store: &Arc<S>) -> Weak<dyn Any + Send + Sync> {
        let erased: Arc<dyn Any + Send + Sync> = store.clone();
        Arc::downgrade(&erased)
    }

    pub(super) fn needs_backup<S: ScannerObjectIO>(store: &Arc<S>, name: &str, etag: &str) -> bool {
        let owner = Self::owner(store);
        let mut pending = PENDING_BACKUPS.lock().unwrap_or_else(|error| error.into_inner());
        let Some(index) = pending
            .iter()
            .position(|phase| phase.owner.ptr_eq(&owner) && phase.name == name)
        else {
            return false;
        };
        if pending[index].failed_etag == etag {
            pending[index].last_polled = std::time::Instant::now();
            true
        } else {
            pending.swap_remove(index);
            false
        }
    }

    pub(super) fn begin<S: ScannerObjectIO>(store: &Arc<S>, name: &str, etag: &str) {
        let owner = Self::owner(store);
        let mut pending = PENDING_BACKUPS.lock().unwrap_or_else(|error| error.into_inner());
        pending.retain(|phase| {
            phase.owner.strong_count() > 0
                && phase.last_polled.elapsed() < Duration::from_secs(3600)
                && !(phase.owner.ptr_eq(&owner) && phase.name == name)
        });
        if pending.len() < MAX_PENDING_BACKUPS {
            pending.push(Self {
                owner,
                name: name.to_owned(),
                failed_etag: etag.to_owned(),
                last_polled: std::time::Instant::now(),
            });
        }
    }

    pub(super) fn finish<S: ScannerObjectIO>(store: &Arc<S>, name: &str, etag: &str) {
        let owner = Self::owner(store);
        PENDING_BACKUPS
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .retain(|phase| !(phase.owner.ptr_eq(&owner) && phase.name == name && phase.failed_etag == etag));
    }
}

struct DecodedCheckpoint {
    owner: Weak<dyn Any + Send + Sync>,
    name: String,
    parked_at: std::time::Instant,
    cache: DataUsageCache,
    revisions: DataUsageCacheRevisions,
}

// The scanner may finish a cold restore with too little time for the walk.
// Move that exact value to the next admission. Only one decoded bucket can
// occupy the handoff and it never owns storage, a permit, or a publication fence.
static DECODED: Mutex<Option<DecodedCheckpoint>> = Mutex::new(None);

impl DataUsageCache {
    pub(crate) async fn load_for_scan<S: ScannerObjectIO>(
        &mut self,
        store: Arc<S>,
        name: &str,
        ctx: &CancellationToken,
        budget: &ScannerCycleBudget,
    ) -> StorageResult<Option<DataUsageCacheRevisions>> {
        tokio::select! {
            biased;
            _ = ctx.cancelled() => Ok(None),
            result = self.load_for_scan_window(store, name, budget) => result,
        }
    }

    async fn load_for_scan_window<S: ScannerObjectIO>(
        &mut self,
        store: Arc<S>,
        name: &str,
        budget: &ScannerCycleBudget,
    ) -> StorageResult<Option<DataUsageCacheRevisions>> {
        let erased: Arc<dyn Any + Send + Sync> = store.clone();
        let owner = Arc::downgrade(&erased);
        let is_ours = |ready: &DecodedCheckpoint| ready.owner.ptr_eq(&owner) && ready.name == name;
        let cached = {
            let mut decoded = DECODED.lock().unwrap_or_else(|error| error.into_inner());
            // Give a retired or deleted bucket's slot back to other active
            // readers. A matching admission can consume it even after a long
            // scheduled interval, subject to the revision checks below.
            decoded.take_if(|ready| {
                ready.owner.strong_count() == 0 || (!is_ours(ready) && ready.parked_at.elapsed() >= Duration::from_secs(3600))
            });
            decoded.as_ref().is_some_and(is_ours)
        };
        if cached {
            let path = path_join_buf(&[BUCKET_META_PREFIX, name]);
            let main = read_config_revision(store.clone(), &path).await;
            let backup = Self::backup_revision(store.clone(), name).await;
            let ready = DECODED
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .take_if(|ready| is_ours(ready));
            if let (Some(ready), Ok(main)) = (ready, main)
                && ready.revisions.main == main
                && ready.revisions.backup == backup
            {
                *self = ready.cache;
                return Ok(Some(ready.revisions));
            }
        }

        let started = tokio::time::Instant::now();
        // Obtain the optional backup head before the expensive decode. A budget
        // stop after the decoder finishes must not discard the materialized tree
        // while waiting for an unrelated backup lookup.
        let backup = Self::backup_revision(store.clone(), name).await;
        let loaded = Box::pin(Self::load_cache(store, name)).await?;
        let revisions = DataUsageCacheRevisions {
            main: loaded.main_revision,
            backup: loaded.backup_revision.or(backup),
            damaged_pages: loaded.damaged_pages,
        };
        *self = loaded.cache;
        if self.info.scan_identity.is_some()
            && !self.cache.is_empty()
            && budget
                .remaining_config()
                .max_duration
                .is_some_and(|remaining| started.elapsed() > remaining)
        {
            let mut slot = DECODED.lock().unwrap_or_else(|error| error.into_inner());
            if slot.as_ref().is_none_or(|ready| ready.owner.strong_count() == 0) {
                *slot = Some(DecodedCheckpoint {
                    owner,
                    name: name.to_owned(),
                    parked_at: std::time::Instant::now(),
                    cache: std::mem::take(self),
                    revisions,
                });
                return Ok(None);
            }
        }
        Ok(Some(revisions))
    }
}
