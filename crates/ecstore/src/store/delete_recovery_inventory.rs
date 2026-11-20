// Copyright 2026 RustFS Team
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

//! Read-only paged observation of physical delete evidence. Sources can report
//! the same transaction; only an exact-key inspection can classify the set.

use super::ECStore;
use crate::bucket::utils::{check_bucket_and_object_names, check_valid_bucket_name_strict, is_meta_bucketname};
use crate::disk::error::DiskError;
use crate::disk::{DeleteRecoveryCursor, DeleteRecoveryPage, DeleteTransactionId, Disk, DiskStore, MAX_DELETE_RESIDUE_PAGE_SIZE};
use crate::error::{Result, StorageError};
use crate::set_disk::SetDisks;
use rustfs_utils::path::GLOBAL_DIR_SUFFIX;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio::time::Instant;
use uuid::Uuid;

const MAX_SESSIONS: usize = 8;
const CURSOR_TTL: Duration = Duration::from_secs(300);

/// Coordinates of the single configured drive examined for this page.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
pub struct DeleteResidueSource {
    pub pool: usize,
    pub set: usize,
    pub drive: usize,
}

impl DeleteResidueSource {
    fn erasure_set(self, store: &ECStore) -> Result<&SetDisks> {
        store
            .pools
            .get(self.pool)
            .and_then(|pool| pool.disk_set.get(self.set))
            .filter(|set| self.drive < set.set_drive_count)
            .map(Arc::as_ref)
            .ok_or(StorageError::ErasureReadQuorum)
    }

    fn next(self, store: &ECStore) -> Result<Option<Self>> {
        let set = self.erasure_set(store)?;
        if self.drive + 1 < set.set_drive_count {
            return Ok(Some(Self {
                drive: self.drive + 1,
                ..self
            }));
        }
        if self.set + 1 < store.pools[self.pool].disk_set.len() {
            return Ok(Some(Self {
                pool: self.pool,
                set: self.set + 1,
                drive: 0,
            }));
        }
        Ok((self.pool + 1 < store.pools.len()).then_some(Self {
            pool: self.pool + 1,
            ..Self::default()
        }))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DeleteResidueSourceStatus {
    Scanned,
    Unavailable,
    Unreadable,
}

/// One physical discovery page. A candidate may appear on other drives or
/// pages. Completion describes traversal coverage, never the outcome of a delete.
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DeleteResidueInventoryPage {
    pub bucket: String,
    pub incarnation: Uuid,
    pub source: DeleteResidueSource,
    pub source_status: DeleteResidueSourceStatus,
    pub candidates: Vec<DeleteTransactionId>,
    pub next_cursor: Option<Uuid>,
    /// Cumulative count of sources skipped because their contents could not be read.
    pub incomplete_sources: usize,
    pub complete: bool,
}

struct PhysicalContinuation {
    disk: Weak<Disk>,
    id: Uuid,
    cursor: DeleteRecoveryCursor,
}

struct InventorySession {
    store: Uuid,
    bucket: String,
    incarnation: Uuid,
    source: DeleteResidueSource,
    physical: Option<PhysicalContinuation>,
    incomplete_sources: usize,
    expires: Instant,
    _permit: OwnedSemaphorePermit,
}

/// Tokens are single-use, node-local capabilities. The permit follows the
/// session during I/O so concurrent first-page requests also share the bound.
pub(crate) struct DeleteResidueCursors {
    sessions: Mutex<HashMap<Uuid, InventorySession>>,
    capacity: Arc<Semaphore>,
}

impl Default for DeleteResidueCursors {
    fn default() -> Self {
        Self {
            sessions: Mutex::default(),
            capacity: Arc::new(Semaphore::new(MAX_SESSIONS)),
        }
    }
}

impl DeleteResidueCursors {
    fn take(&self, token: Option<Uuid>, store: Uuid, bucket: &str) -> Result<Option<InventorySession>> {
        let now = Instant::now();
        let mut sessions = self.sessions.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        sessions.retain(|_, session| session.expires > now);
        let Some(token) = token else { return Ok(None) };
        if !sessions.get(&token).is_some_and(|s| s.store == store && s.bucket == bucket) {
            return Err(StorageError::PreconditionFailed);
        }
        Ok(sessions.remove(&token))
    }

    fn save(&self, mut session: InventorySession) -> Uuid {
        let token = Uuid::new_v4();
        session.expires = Instant::now() + CURSOR_TTL;
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(token, session);
        token
    }
}

async fn source_disk(set: &SetDisks, drive: usize) -> Result<Option<DiskStore>> {
    set.disks
        .read()
        .await
        .get(drive)
        .cloned()
        .ok_or(StorageError::ErasureReadQuorum)
}

async fn observe_source(
    set: &SetDisks,
    session: &mut InventorySession,
    expected_id: Option<Uuid>,
    limit: u32,
) -> Result<(DeleteResidueSourceStatus, Vec<DeleteTransactionId>)> {
    let continuation = session.physical.take();
    let Some(disk) = source_disk(set, session.source.drive).await? else {
        if continuation.is_some() {
            return Err(StorageError::PreconditionFailed);
        }
        return Ok((DeleteResidueSourceStatus::Unavailable, Vec::new()));
    };
    let id = expected_id.filter(|id| !id.is_nil());
    if let Some(prior) = &continuation
        && (Some(prior.id) != id || !prior.disk.ptr_eq(&Arc::downgrade(&disk)))
    {
        return Err(StorageError::PreconditionFailed);
    }
    let Some(id) = id else {
        return Ok((DeleteResidueSourceStatus::Unreadable, Vec::new()));
    };
    let resuming = continuation.is_some();
    let page = disk
        .list_delete_residue_on_disk(&session.bucket, id, continuation.map(|state| state.cursor), limit)
        .await;
    let DeleteRecoveryPage { mut transactions, next } = match page {
        Ok(page) => page,
        Err(DiskError::FileNotFound | DiskError::InvalidPath | DiskError::OutdatedXLMeta | DiskError::InconsistentDisk)
            if resuming =>
        {
            return Err(StorageError::PreconditionFailed);
        }
        Err(_) => return Ok((DeleteResidueSourceStatus::Unreadable, Vec::new())),
    };
    if transactions.len() > usize::try_from(limit).map_err(|_| StorageError::FileCorrupt)?
        || next.as_ref().is_some_and(|cursor| cursor.token.is_nil())
    {
        return Err(StorageError::FileCorrupt);
    }
    for candidate in &mut transactions {
        // A literal suffix can precede the final on-disk directory marker.
        // Remove exactly one so re-encoding the listed key selects the same physical path.
        if let Some(prefix) = candidate.object.strip_suffix(GLOBAL_DIR_SUFFIX) {
            candidate.object = format!("{prefix}/");
        }
        if candidate.transaction.is_nil()
            || candidate.object.len() > 1024
            || check_bucket_and_object_names(&session.bucket, &candidate.object).is_err()
        {
            return Err(StorageError::FileCorrupt);
        }
    }
    if !source_disk(set, session.source.drive)
        .await?
        .is_some_and(|current| Arc::ptr_eq(&current, &disk))
    {
        return Err(StorageError::PreconditionFailed);
    }
    session.physical = next.map(|cursor| PhysicalContinuation {
        disk: Arc::downgrade(&disk),
        id,
        cursor,
    });
    Ok((DeleteResidueSourceStatus::Scanned, transactions))
}

impl ECStore {
    /// Lists candidates from one bounded raw disk page of a user bucket. Pass
    /// the single-use continuation to the same node until it returns no token.
    /// Later disk mutations can change the observations between pages.
    ///
    /// # Errors
    /// Returns a precondition error for a stale continuation or changed source,
    /// a throttling error when the cursor limit is reached, or a storage error
    /// when the bucket identity or peer response cannot be validated. A skipped
    /// disk is reported explicitly and prevents a complete terminal result.
    pub async fn list_delete_residue_candidates(
        self: &Arc<Self>,
        bucket: &str,
        cursor: Option<Uuid>,
        limit: u32,
    ) -> Result<DeleteResidueInventoryPage> {
        if is_meta_bucketname(bucket) || check_valid_bucket_name_strict(bucket).is_err() {
            return Err(StorageError::BucketNameInvalid(bucket.to_owned()));
        }
        if !(1..=MAX_DELETE_RESIDUE_PAGE_SIZE).contains(&limit) {
            return Err(StorageError::InvalidArgument(
                bucket.to_owned(),
                String::new(),
                "limit must be 1..=64".to_owned(),
            ));
        }
        let cursors = &self.ctx.delete_residue_cursors;
        let mut session = match cursors.take(cursor, self.id, bucket)? {
            Some(session) => session,
            None => {
                let permit = Arc::clone(&cursors.capacity)
                    .try_acquire_owned()
                    .map_err(|_| StorageError::SlowDown)?;
                InventorySession {
                    store: self.id,
                    bucket: bucket.to_owned(),
                    incarnation: self.bucket_incarnation_id_from_disk(bucket).await?,
                    source: DeleteResidueSource::default(),
                    physical: None,
                    incomplete_sources: 0,
                    expires: Instant::now(),
                    _permit: permit,
                }
            }
        };
        let fence = self.acquire_bucket_incarnation_fence(bucket, session.incarnation).await?;
        let incarnation = session.incarnation;
        let source = session.source;
        let set = source.erasure_set(self)?;
        let expected_id = self.pools[source.pool]
            .format
            .erasure
            .sets
            .get(source.set)
            .and_then(|ids| ids.get(source.drive))
            .copied();
        let (source_status, candidates) = observe_source(set, &mut session, expected_id, limit).await?;
        if fence.is_lock_lost() {
            return Err(StorageError::PreconditionFailed);
        }
        if source_status != DeleteResidueSourceStatus::Scanned {
            session.incomplete_sources += 1;
        }
        let incomplete_sources = session.incomplete_sources;
        let next_source = if session.physical.is_some() {
            Some(source)
        } else {
            source.next(self)?
        };
        let next_cursor = next_source.map(|next| {
            session.source = next;
            cursors.save(session)
        });
        Ok(DeleteResidueInventoryPage {
            bucket: bucket.to_owned(),
            incarnation,
            source,
            source_status,
            candidates,
            next_cursor,
            incomplete_sources,
            complete: next_cursor.is_none() && incomplete_sources == 0,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bucket::metadata_sys::{init_bucket_metadata_sys, test_support::isolated_store_over_temp_disks};
    use crate::disk::DiskAPI;
    use crate::storage_api_contracts::bucket::{BucketOperations, MakeBucketOptions};

    async fn fixture(bucket: &str) -> (Vec<tempfile::TempDir>, Arc<ECStore>) {
        let (dirs, store) = isolated_store_over_temp_disks().await;
        store.background_cancel_token().expect("fixture background token").cancel();
        tokio::task::yield_now().await;
        init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("create discovery bucket");
        (dirs, store)
    }

    fn seed_residue(disk: &DiskStore, bucket: &str, key: &str, txn: Uuid) -> std::path::PathBuf {
        let data = disk.path().join(bucket).join(key).join(Uuid::new_v4().to_string());
        std::fs::create_dir_all(&data).expect("create historical data directory");
        let marker = data.join(format!("reserve-delete-data.{txn}"));
        std::fs::write(&marker, []).expect("seed undiscoverable-by-S3 reservation");
        marker
    }

    #[tokio::test]
    async fn delete_residue_inventory_pages_cover_configured_sources_and_preserve_hidden_evidence() {
        let bucket = "residue-inventory";
        let (_dirs, store) = fixture(bucket).await;
        let disks = store.pools[0].disk_set[0].disk_inventory().await;
        let transaction = Uuid::new_v4();
        let mut markers = Vec::new();
        for disk in disks.iter().flatten() {
            markers.push(seed_residue(disk, bucket, "hidden", transaction));
        }
        let mut cursor = None;
        let mut observed = std::collections::HashSet::new();
        for _ in 0..(disks.len() * 4) {
            let page = store
                .list_delete_residue_candidates(bucket, cursor, 1)
                .await
                .expect("read disk discovery page");
            assert!(page.candidates.len() <= 1);
            assert_eq!(page.source_status, DeleteResidueSourceStatus::Scanned);
            for item in page.candidates {
                assert_eq!((item.object.as_str(), item.transaction), ("hidden", transaction));
                observed.insert(page.source.drive);
            }
            cursor = page.next_cursor;
            if cursor.is_none() {
                assert!(page.complete);
                break;
            }
            assert!(!page.complete);
        }
        assert!(cursor.is_none(), "traversal must reach a terminal page");
        assert_eq!(observed.len(), disks.len(), "scan every participant independently");
        for marker in markers {
            assert!(std::fs::read(marker).expect("retain legacy marker bytes").is_empty());
        }
    }

    #[tokio::test]
    async fn delete_residue_inventory_wrong_disk_never_certifies_complete() {
        let bucket = "residue-wrong-disk";
        let (_dirs, store) = fixture(bucket).await;
        let set = &store.pools[0].disk_set[0];
        let disks = set.disk_inventory().await;
        let other = disks[1].as_ref().expect("second fixture drive");
        seed_residue(other, bucket, "only-on-second-drive", Uuid::new_v4());
        set.disks.write().await[0] = Some(other.clone());

        let page = store
            .list_delete_residue_candidates(bucket, None, 4)
            .await
            .expect("report unverified slot");
        assert_eq!(page.source.drive, 0);
        assert_eq!(page.source_status, DeleteResidueSourceStatus::Unreadable);
        assert_eq!(page.incomplete_sources, 1);
        assert!(!page.complete);
        assert!(page.candidates.is_empty(), "never attribute the other slot's evidence to this one");
    }

    #[tokio::test]
    async fn delete_residue_inventory_preserves_literal_suffix_in_directory_key() {
        use rustfs_utils::path::encode_dir_object;
        let bucket = "residue-dir-suffix";
        let (_dirs, store) = fixture(bucket).await;
        let disk = store.pools[0].disk_set[0].disk_inventory().await[0]
            .clone()
            .expect("first drive");
        let key = format!("object{GLOBAL_DIR_SUFFIX}/");
        let physical = encode_dir_object(&key);
        seed_residue(&disk, bucket, &physical, Uuid::new_v4());
        let page = store
            .list_delete_residue_candidates(bucket, None, 4)
            .await
            .expect("list directory key");
        assert_eq!(page.candidates.len(), 1);
        assert_eq!(page.candidates[0].object, key);
        assert_eq!(encode_dir_object(&page.candidates[0].object), physical);
    }

    #[tokio::test]
    async fn delete_residue_inventory_missing_source_never_certifies_empty() {
        let bucket = "residue-offline";
        let (_dirs, store) = fixture(bucket).await;
        let set = &store.pools[0].disk_set[0];
        let failed = set.set_drive_count - 1;
        set.disks.write().await[failed] = None;
        let mut cursor = None;
        for index in 0..set.set_drive_count {
            let page = store
                .list_delete_residue_candidates(bucket, cursor, 1)
                .await
                .expect("page with an offline source");
            assert_eq!(page.source.drive, index);
            assert!(page.candidates.is_empty());
            assert!(!page.complete);
            if index == failed {
                assert_eq!(page.source_status, DeleteResidueSourceStatus::Unavailable);
                assert_eq!(page.incomplete_sources, 1);
            }
            cursor = page.next_cursor;
        }
        assert!(cursor.is_none(), "failed source is explicitly accounted for at end of walk");
    }

    #[tokio::test]
    async fn delete_residue_inventory_rejects_replaced_source_during_page() {
        let bucket = "residue-source-change";
        let (_dirs, store) = fixture(bucket).await;
        let set = &store.pools[0].disk_set[0];
        let disks = set.disk_inventory().await;
        let first = disks[0].as_ref().expect("first fixture participant");
        seed_residue(first, bucket, "first", Uuid::new_v4());
        seed_residue(first, bucket, "second", Uuid::new_v4());
        let page = store
            .list_delete_residue_candidates(bucket, None, 1)
            .await
            .expect("open cursor on first drive");
        assert_eq!(page.candidates.len(), 1);
        let cursor = page.next_cursor.expect("additional physical pages");
        assert!(matches!(
            store
                .list_delete_residue_candidates("different-bucket", Some(cursor), 1)
                .await,
            Err(StorageError::PreconditionFailed)
        ));
        set.disks.write().await[0] = None;
        assert!(matches!(
            store.list_delete_residue_candidates(bucket, Some(cursor), 1).await,
            Err(StorageError::PreconditionFailed)
        ));
        assert!(matches!(
            store.list_delete_residue_candidates(bucket, Some(cursor), 1).await,
            Err(StorageError::PreconditionFailed)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn delete_residue_inventory_expired_cursor_releases_capacity() {
        let cursors = DeleteResidueCursors::default();
        let store = Uuid::new_v4();
        let mut token = Uuid::nil();
        for _ in 0..MAX_SESSIONS {
            token = cursors.save(InventorySession {
                store,
                bucket: "scope".to_owned(),
                incarnation: Uuid::new_v4(),
                source: DeleteResidueSource::default(),
                physical: None,
                incomplete_sources: 0,
                expires: Instant::now(),
                _permit: cursors.capacity.clone().try_acquire_owned().expect("bounded capacity"),
            });
        }
        assert!(cursors.capacity.clone().try_acquire_owned().is_err());
        assert!(matches!(
            cursors.take(Some(token), Uuid::new_v4(), "scope"),
            Err(StorageError::PreconditionFailed)
        ));
        tokio::time::advance(CURSOR_TTL).await;
        assert!(matches!(cursors.take(Some(token), store, "scope"), Err(StorageError::PreconditionFailed)));
        assert_eq!(cursors.capacity.available_permits(), MAX_SESSIONS);
    }
}
