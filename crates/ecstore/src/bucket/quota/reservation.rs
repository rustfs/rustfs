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

use super::QUOTA_RESERVATION_PROTOCOL_V2;
use crate::bucket::metadata_sys;
use crate::config::com::{CONFIG_PREFIX, read_config_no_lock, save_config_with_opts};
use crate::data_usage::compute_bucket_usage;
use crate::disk::RUSTFS_META_BUCKET;
use crate::disk::{DiskAPI, error::DiskError};
use crate::error::{Result, StorageError, is_err_object_not_found, is_err_version_not_found};
use crate::object_api::{ObjectInfo, ObjectOptions, QuotaAdmission};
use crate::set_disk::{SetDisks, get_lock_acquire_timeout};
use crate::storage_api_contracts::namespace::NamespaceLocking;
use crate::storage_api_contracts::{list::ListOperations as _, object::ObjectOperations};
use crate::store::ECStore;
use futures::{StreamExt, stream};
use rustfs_lock::NamespaceLockGuard;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;
use time::OffsetDateTime;
use tracing::warn;
use uuid::Uuid;

const QUOTA_LEDGER_FORMAT_VERSION: u8 = 1;
const MAX_ORPHANS_REAPED_PER_WRITE: usize = 64;
const MAX_ORPHAN_PROBES_PER_WRITE: usize = 128;
const ORPHAN_PROBE_CONCURRENCY: usize = 32;
const EVENT_QUOTA_LEDGER_SETTLEMENT: &str = "quota_ledger_settlement";
const EVENT_QUOTA_ADMISSION: &str = "quota_admission";
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_QUOTA: &str = "quota";

const SHARDED_LEDGER_FORMAT_VERSION: u8 = 1;
const SHARDED_LEDGER_COUNT: u16 = 16;

#[cfg(any(test, feature = "test-util"))]
static FAIL_NEXT_LEDGER_SAVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

// Lock order: caller-held destination object/upload, bucket metadata
// transaction (read), operation reservation, then quota ledger. Protocol v2
// uses allocator(read) -> shard(write); credit refill releases the operation
// and shard guards before taking allocator(write), and orphan probes never
// hold allocator/shard guards while acquiring an operation lock.
// When Object Lock already holds the metadata transaction read lock, reuse
// that guard: reacquiring it behind a waiting metadata writer would deadlock.

#[cfg(not(any(test, feature = "test-util")))]
const ORPHAN_MIN_AGE_SECONDS: i64 = 30;
#[cfg(any(test, feature = "test-util"))]
const ORPHAN_MIN_AGE_SECONDS: i64 = 0;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedReservation {
    object: String,
    old_size: u64,
    new_size: u64,
    created_at: i64,
    #[serde(default)]
    pool_index: Option<usize>,
    #[serde(default)]
    set_index: Option<usize>,
    #[serde(default)]
    commit_started: bool,
}

impl PersistedReservation {
    fn growth(&self) -> u64 {
        self.new_size.saturating_sub(self.old_size)
    }

    fn target(&self) -> Option<(usize, usize)> {
        self.pool_index.zip(self.set_index)
    }

    fn matches_expected(&self, expected: &Self) -> bool {
        self.object == expected.object && self.old_size == expected.old_size && self.new_size == expected.new_size
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct QuotaLedger {
    version: u8,
    bucket_incarnation: Uuid,
    quota_revision_unix_nanos: i128,
    accounted_usage: u64,
    reservations: BTreeMap<Uuid, PersistedReservation>,
    #[serde(default)]
    reconcile_required: bool,
    #[serde(default)]
    reap_cursor: Option<Uuid>,
}

impl QuotaLedger {
    fn new(bucket_incarnation: Uuid, quota_revision: OffsetDateTime, accounted_usage: u64) -> Self {
        Self {
            version: QUOTA_LEDGER_FORMAT_VERSION,
            bucket_incarnation,
            quota_revision_unix_nanos: quota_revision.unix_timestamp_nanos(),
            accounted_usage,
            reservations: BTreeMap::new(),
            reconcile_required: false,
            reap_cursor: None,
        }
    }

    fn matches(&self, bucket_incarnation: Uuid, quota_revision: OffsetDateTime) -> bool {
        self.bucket_incarnation == bucket_incarnation && self.quota_revision_unix_nanos == quota_revision.unix_timestamp_nanos()
    }

    fn admitted_usage(&self) -> Result<u64> {
        let reserved_growth = self.reservations.values().try_fold(0_u64, |total, reservation| {
            total
                .checked_add(reservation.growth())
                .ok_or(StorageError::PartMissingOrCorrupt)
        })?;
        if reserved_growth > self.accounted_usage {
            return Err(StorageError::PartMissingOrCorrupt);
        }
        Ok(self.accounted_usage)
    }

    fn reserve(&mut self, operation_id: Uuid, reservation: PersistedReservation) -> Result<()> {
        self.accounted_usage = self
            .accounted_usage
            .checked_add(reservation.growth())
            .ok_or(StorageError::PartMissingOrCorrupt)?;
        self.reservations.insert(operation_id, reservation);
        Ok(())
    }

    fn commit(&mut self, operation_id: Uuid, expected: &PersistedReservation) -> Result<()> {
        let Some(reservation) = self.reservations.remove(&operation_id) else {
            return Err(StorageError::PartMissingOrCorrupt);
        };
        if !reservation.matches_expected(expected) {
            return Err(StorageError::PartMissingOrCorrupt);
        }
        if reservation.new_size < reservation.old_size {
            self.reconcile_required = true;
        }
        Ok(())
    }

    fn abort(&mut self, operation_id: Uuid, expected: &PersistedReservation) -> Result<()> {
        let Some(reservation) = self.reservations.remove(&operation_id) else {
            return Ok(());
        };
        if !reservation.matches_expected(expected) {
            return Err(StorageError::PartMissingOrCorrupt);
        }
        self.accounted_usage = self
            .accounted_usage
            .checked_sub(reservation.growth())
            .ok_or(StorageError::PartMissingOrCorrupt)?;
        Ok(())
    }

    fn mark_commit_started(&mut self, operation_id: Uuid, expected: &PersistedReservation) -> Result<()> {
        let reservation = self
            .reservations
            .get_mut(&operation_id)
            .ok_or(StorageError::PartMissingOrCorrupt)?;
        if !reservation.matches_expected(expected) {
            return Err(StorageError::PartMissingOrCorrupt);
        }
        reservation.commit_started = true;
        Ok(())
    }

    fn should_reconcile_after_denial(&self) -> bool {
        self.reservations.is_empty()
    }

    fn reap_candidates(&self, now: i64) -> (Vec<Uuid>, Option<Uuid>) {
        let aged = self
            .reservations
            .iter()
            .filter(|(_, reservation)| {
                reservation.created_at > now || now.saturating_sub(reservation.created_at) >= ORPHAN_MIN_AGE_SECONDS
            })
            .map(|(operation_id, _)| *operation_id)
            .collect::<Vec<_>>();
        let start = self
            .reap_cursor
            .and_then(|cursor| aged.iter().position(|operation_id| *operation_id > cursor))
            .unwrap_or(0);
        let candidates = aged
            .iter()
            .cycle()
            .skip(start)
            .take(aged.len().min(MAX_ORPHAN_PROBES_PER_WRITE))
            .copied()
            .collect::<Vec<_>>();
        let next_cursor = candidates.last().copied();
        (candidates, next_cursor)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AllocatorGrant {
    shard_index: u16,
    amount: u64,
    #[serde(default)]
    initial_usage: u64,
}

#[derive(Debug, Serialize, Deserialize)]
struct QuotaAllocatorLedger {
    version: u8,
    bucket_incarnation: Uuid,
    quota_revision_unix_nanos: i128,
    quota_limit: u64,
    generation: u64,
    grants: BTreeMap<Uuid, AllocatorGrant>,
}

impl QuotaAllocatorLedger {
    fn new(bucket_incarnation: Uuid, quota_revision: OffsetDateTime, quota_limit: u64) -> Self {
        Self {
            version: SHARDED_LEDGER_FORMAT_VERSION,
            bucket_incarnation,
            quota_revision_unix_nanos: quota_revision.unix_timestamp_nanos(),
            quota_limit,
            generation: 0,
            grants: BTreeMap::new(),
        }
    }

    fn matches(&self, bucket_incarnation: Uuid, quota_revision: OffsetDateTime, quota_limit: u64) -> bool {
        self.bucket_incarnation == bucket_incarnation
            && self.quota_revision_unix_nanos == quota_revision.unix_timestamp_nanos()
            && self.quota_limit == quota_limit
    }

    fn issued_bytes(&self) -> Result<u64> {
        self.grants.values().try_fold(0_u64, |total, grant| {
            total.checked_add(grant.amount).ok_or(StorageError::PartMissingOrCorrupt)
        })
    }

    fn grants_for(&self, shard_index: u16) -> BTreeMap<Uuid, AllocatorGrant> {
        self.grants
            .iter()
            .filter_map(|(grant_id, grant)| (grant.shard_index == shard_index).then_some((*grant_id, grant.clone())))
            .collect()
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct QuotaShardLedger {
    version: u8,
    bucket_incarnation: Uuid,
    quota_revision_unix_nanos: i128,
    shard_index: u16,
    allocator_generation: u64,
    accounted_usage: u64,
    grants: BTreeMap<Uuid, u64>,
    reservations: BTreeMap<Uuid, PersistedReservation>,
    #[serde(default)]
    reconcile_required: bool,
    #[serde(default)]
    reap_cursor: Option<Uuid>,
}

impl QuotaShardLedger {
    fn new(bucket_incarnation: Uuid, quota_revision: OffsetDateTime, shard_index: u16) -> Self {
        Self {
            version: SHARDED_LEDGER_FORMAT_VERSION,
            bucket_incarnation,
            quota_revision_unix_nanos: quota_revision.unix_timestamp_nanos(),
            shard_index,
            allocator_generation: 0,
            accounted_usage: 0,
            grants: BTreeMap::new(),
            reservations: BTreeMap::new(),
            reconcile_required: false,
            reap_cursor: None,
        }
    }

    fn matches(&self, bucket_incarnation: Uuid, quota_revision: OffsetDateTime, shard_index: u16) -> bool {
        self.bucket_incarnation == bucket_incarnation
            && self.quota_revision_unix_nanos == quota_revision.unix_timestamp_nanos()
            && self.shard_index == shard_index
    }

    fn credit_limit(&self) -> Result<u64> {
        self.grants.values().try_fold(0_u64, |total, amount| {
            total.checked_add(*amount).ok_or(StorageError::PartMissingOrCorrupt)
        })
    }

    fn available_credit(&self) -> Result<u64> {
        self.credit_limit()?
            .checked_sub(self.accounted_usage)
            .ok_or(StorageError::PartMissingOrCorrupt)
    }

    fn adopt_grants(&mut self, allocator: &QuotaAllocatorLedger) -> Result<()> {
        let expected = allocator.grants_for(self.shard_index);
        for (grant_id, amount) in &self.grants {
            if expected.get(grant_id).map(|grant| grant.amount) != Some(*amount) {
                return Err(StorageError::PartMissingOrCorrupt);
            }
        }
        if self.grants.is_empty() && self.accounted_usage == 0 {
            self.accounted_usage = expected.values().try_fold(0_u64, |total, grant| {
                total
                    .checked_add(grant.initial_usage)
                    .ok_or(StorageError::PartMissingOrCorrupt)
            })?;
        }
        self.grants = expected
            .into_iter()
            .map(|(grant_id, grant)| (grant_id, grant.amount))
            .collect();
        self.credit_limit()?
            .checked_sub(self.accounted_usage)
            .ok_or(StorageError::PartMissingOrCorrupt)?;
        self.allocator_generation = allocator.generation;
        Ok(())
    }

    fn reserve(&mut self, operation_id: Uuid, reservation: PersistedReservation) -> Result<()> {
        self.accounted_usage = self
            .accounted_usage
            .checked_add(reservation.growth())
            .ok_or(StorageError::PartMissingOrCorrupt)?;
        if self.accounted_usage > self.credit_limit()? {
            return Err(StorageError::PartMissingOrCorrupt);
        }
        self.reservations.insert(operation_id, reservation);
        Ok(())
    }

    fn commit(&mut self, operation_id: Uuid, expected: &PersistedReservation) -> Result<()> {
        let Some(reservation) = self.reservations.remove(&operation_id) else {
            return Err(StorageError::PartMissingOrCorrupt);
        };
        if !reservation.matches_expected(expected) {
            return Err(StorageError::PartMissingOrCorrupt);
        }
        if reservation.new_size < reservation.old_size {
            self.reconcile_required = true;
        }
        Ok(())
    }

    fn abort(&mut self, operation_id: Uuid, expected: &PersistedReservation) -> Result<()> {
        let Some(reservation) = self.reservations.remove(&operation_id) else {
            return Ok(());
        };
        if !reservation.matches_expected(expected) {
            return Err(StorageError::PartMissingOrCorrupt);
        }
        self.accounted_usage = self
            .accounted_usage
            .checked_sub(reservation.growth())
            .ok_or(StorageError::PartMissingOrCorrupt)?;
        Ok(())
    }

    fn mark_commit_started(&mut self, operation_id: Uuid, expected: &PersistedReservation) -> Result<()> {
        let reservation = self
            .reservations
            .get_mut(&operation_id)
            .ok_or(StorageError::PartMissingOrCorrupt)?;
        if !reservation.matches_expected(expected) {
            return Err(StorageError::PartMissingOrCorrupt);
        }
        reservation.commit_started = true;
        Ok(())
    }

    fn reap_candidates(&self, now: i64) -> (Vec<Uuid>, Option<Uuid>) {
        let aged = self
            .reservations
            .iter()
            .filter(|(_, reservation)| {
                reservation.created_at > now || now.saturating_sub(reservation.created_at) >= ORPHAN_MIN_AGE_SECONDS
            })
            .map(|(operation_id, _)| *operation_id)
            .collect::<Vec<_>>();
        let start = self
            .reap_cursor
            .and_then(|cursor| aged.iter().position(|operation_id| *operation_id > cursor))
            .unwrap_or(0);
        let candidates = aged
            .iter()
            .cycle()
            .skip(start)
            .take(aged.len().min(MAX_ORPHAN_PROBES_PER_WRITE))
            .copied()
            .collect::<Vec<_>>();
        (candidates.clone(), candidates.last().copied())
    }
}

pub(crate) struct QuotaContext {
    store: Option<Arc<ECStore>>,
    bucket: String,
    object: String,
    ledger_object: String,
    bucket_incarnation: Option<Uuid>,
    quota_revision: Option<OffsetDateTime>,
    quota_limit: Option<u64>,
    reservation_protocol: Option<u32>,
    capability_proof: Option<crate::services::notification_sys::CrossPoolFenceFleetProofToken>,
    snapshot_admission: Option<QuotaAdmission>,
    legacy_data_movement: bool,
    metadata_guard: Option<Arc<NamespaceLockGuard>>,
    pool_index: Option<usize>,
    set_index: Option<usize>,
}

impl QuotaContext {
    pub(crate) fn is_enforced(&self) -> bool {
        self.quota_limit.is_some()
    }

    pub(crate) async fn reserve(self, old_size: u64, new_size: u64) -> Result<QuotaReservation> {
        let Some(quota_limit) = self.quota_limit else {
            return Ok(QuotaReservation::unlimited(self.metadata_guard));
        };
        if let Some(admission) = self.snapshot_admission {
            let growth = new_size.saturating_sub(old_size);
            if growth > admission.remaining() {
                return Err(StorageError::QuotaExceeded {
                    current: admission.current_usage(),
                    limit: admission.quota_limit(),
                });
            }
            return Ok(QuotaReservation::unlimited(self.metadata_guard));
        }
        if self.legacy_data_movement {
            if new_size > old_size {
                return Err(StorageError::PartMissingOrCorrupt);
            }
            return Ok(QuotaReservation::unlimited(self.metadata_guard));
        }
        if self.reservation_protocol == Some(QUOTA_RESERVATION_PROTOCOL_V2) {
            return reserve_sharded(self, old_size, new_size).await;
        }
        let store = self.store.ok_or(StorageError::PartMissingOrCorrupt)?;
        let bucket_incarnation = self.bucket_incarnation.ok_or(StorageError::PartMissingOrCorrupt)?;
        let quota_revision = self.quota_revision.ok_or(StorageError::PartMissingOrCorrupt)?;
        let operation_id = Uuid::new_v4();
        let operation_lock_object = operation_lock_object(&self.ledger_object, operation_id);
        let operation_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &operation_lock_object).await?;
        let operation_guard = operation_lock.get_write_lock(get_lock_acquire_timeout()).await?;
        let reservation = PersistedReservation {
            object: self.object,
            old_size,
            new_size,
            created_at: OffsetDateTime::now_utc().unix_timestamp(),
            pool_index: self.pool_index,
            set_index: self.set_index,
            commit_started: false,
        };
        let ledger_data = LedgerReservationData {
            store: Arc::clone(&store),
            bucket: self.bucket,
            ledger_object: self.ledger_object,
            operation_id,
            reservation: reservation.clone(),
        };
        let metadata_guard = self.metadata_guard;
        let capability_proof = self.capability_proof;

        tokio::spawn(async move {
            reap_stale_reservations(Arc::clone(&store), &ledger_data.bucket, &ledger_data.ledger_object).await?;

            let ledger_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &ledger_data.ledger_object).await?;
            let ledger_guard = Arc::new(ledger_lock.get_write_lock(get_lock_acquire_timeout()).await?);
            fence_namespace_mutations(&store, RUSTFS_META_BUCKET, &ledger_data.ledger_object, None).await?;
            let mut ledger = load_current_ledger_locked(
                Arc::clone(&store),
                &ledger_data.bucket,
                &ledger_data.ledger_object,
                bucket_incarnation,
                quota_revision,
            )
            .await?;

            let growth = reservation.growth();
            let mut current_usage = ledger.admitted_usage()?;
            let mut expected_usage = current_usage.checked_add(growth).ok_or(StorageError::PartMissingOrCorrupt)?;
            if growth > 0 && expected_usage > quota_limit && growth <= quota_limit && ledger.should_reconcile_after_denial() {
                reconcile_exact(&store, &ledger_data.bucket, &mut ledger).await?;
                current_usage = ledger.admitted_usage()?;
                expected_usage = current_usage.checked_add(growth).ok_or(StorageError::PartMissingOrCorrupt)?;
            }
            if growth > 0 && expected_usage > quota_limit {
                return Err(StorageError::QuotaExceeded {
                    current: current_usage,
                    limit: quota_limit,
                });
            }
            if operation_guard.is_lock_lost() || metadata_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
                return Err(StorageError::NamespaceLockQuorumUnavailable {
                    mode: "quota_reservation",
                    bucket: ledger_data.bucket.clone(),
                    object: ledger_data.ledger_object.clone(),
                    required: 1,
                    achieved: 0,
                });
            }
            ledger.reserve(operation_id, reservation)?;
            save_ledger_locked(Arc::clone(&store), &ledger_data.ledger_object, &ledger, &ledger_guard).await?;

            Ok(QuotaReservation {
                ledger: Some(ReservationLedgerData::Single(ledger_data)),
                operation_guard: Some(operation_guard),
                metadata_guard,
                capability_proof,
                state: ReservationState::Pending,
            })
        })
        .await
        .map_err(|err| StorageError::other_with_context("quota ledger reservation task failed", err))?
    }
}

async fn reserve_sharded(context: QuotaContext, old_size: u64, new_size: u64) -> Result<QuotaReservation> {
    let quota_limit = context.quota_limit.ok_or(StorageError::PartMissingOrCorrupt)?;
    let store = context.store.ok_or(StorageError::PartMissingOrCorrupt)?;
    let bucket_incarnation = context.bucket_incarnation.ok_or(StorageError::PartMissingOrCorrupt)?;
    let quota_revision = context.quota_revision.ok_or(StorageError::PartMissingOrCorrupt)?;
    let operation_id = Uuid::new_v4();
    let shard_index = shard_index(&context.object);
    let allocator_object = allocator_object(&context.ledger_object, bucket_incarnation, quota_revision);
    let shard_object = shard_object(&context.ledger_object, shard_index, bucket_incarnation, quota_revision);
    let growth = new_size.saturating_sub(old_size);
    let reservation = PersistedReservation {
        object: context.object,
        old_size,
        new_size,
        created_at: now_unix(),
        pool_index: context.pool_index,
        set_index: context.set_index,
        commit_started: false,
    };
    let metadata_guard = context.metadata_guard;
    let capability_proof = context.capability_proof;
    let data = ShardedReservationData {
        store: Arc::clone(&store),
        bucket: context.bucket,
        ledger_object: context.ledger_object,
        allocator_object,
        shard_object,
        shard_index,
        bucket_incarnation,
        quota_revision,
        operation_id,
        reservation,
    };
    let bootstrap = ensure_sharded_allocator(Arc::clone(&store), &data, growth, quota_limit).await?;
    let operation_lock_object = operation_lock_object(&data.shard_object, operation_id);
    let operation_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &operation_lock_object).await?;
    let operation_guard = operation_lock.get_write_lock(get_lock_acquire_timeout()).await?;

    for attempt in 0..=2 {
        let allocator_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.allocator_object).await?;
        let allocator_guard = allocator_lock.get_read_lock(get_lock_acquire_timeout()).await?;
        if allocator_guard.is_lock_lost() {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "quota_allocator",
                bucket: RUSTFS_META_BUCKET.to_string(),
                object: data.allocator_object.clone(),
                required: 1,
                achieved: 0,
            });
        }
        let allocator = load_current_allocator_locked(
            Arc::clone(&store),
            &data.allocator_object,
            bucket_incarnation,
            quota_revision,
            quota_limit,
        )
        .await?;
        let allow_create = shard_may_be_created(&allocator, data.shard_index, bootstrap);
        let shard_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.shard_object).await?;
        let shard_guard = Arc::new(shard_lock.get_write_lock(get_lock_acquire_timeout()).await?);
        let mut shard = load_current_shard_locked(
            Arc::clone(&store),
            &data.shard_object,
            bucket_incarnation,
            quota_revision,
            data.shard_index,
            allow_create,
            &allocator,
        )
        .await?;
        let growth = data.reservation.growth();
        if growth == 0 || shard.available_credit()? >= growth {
            if operation_guard.is_lock_lost()
                || allocator_guard.is_lock_lost()
                || shard_guard.is_lock_lost()
                || metadata_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
            {
                return Err(StorageError::NamespaceLockQuorumUnavailable {
                    mode: "quota_sharded_reservation",
                    bucket: data.bucket.clone(),
                    object: data.shard_object.clone(),
                    required: 1,
                    achieved: 0,
                });
            }
            fence_namespace_mutations(&store, RUSTFS_META_BUCKET, &data.shard_object, None).await?;
            if allocator_guard.is_lock_lost() || shard_guard.is_lock_lost() {
                return Err(StorageError::NamespaceLockQuorumUnavailable {
                    mode: "quota_sharded_reservation",
                    bucket: data.bucket.clone(),
                    object: data.shard_object.clone(),
                    required: 1,
                    achieved: 0,
                });
            }
            shard.reserve(data.operation_id, data.reservation.clone())?;
            save_shard_locked(Arc::clone(&store), &data.shard_object, &shard, &shard_guard).await?;
            return Ok(QuotaReservation {
                ledger: Some(ReservationLedgerData::Sharded(data)),
                operation_guard: Some(operation_guard),
                metadata_guard,
                capability_proof,
                state: ReservationState::Pending,
            });
        }
        if shard.grants.is_empty() && shard.reservations.is_empty() && shard.accounted_usage == 0 {
            fence_namespace_mutations(&store, RUSTFS_META_BUCKET, &data.shard_object, None).await?;
            save_shard_locked(Arc::clone(&store), &data.shard_object, &shard, &shard_guard).await?;
        }
        drop(shard_guard);
        drop(allocator_guard);

        if attempt == 2 {
            return Err(StorageError::QuotaExceeded {
                current: quota_limit,
                limit: quota_limit,
            });
        }
        if attempt < 2
            && reap_sharded_reservations(Arc::clone(&store), &data, bucket_incarnation, quota_revision, quota_limit).await?
        {
            continue;
        }
        refill_sharded_credit(
            Arc::clone(&store),
            &data.allocator_object,
            data.shard_index,
            growth,
            bucket_incarnation,
            quota_revision,
            quota_limit,
        )
        .await?;
    }

    Err(StorageError::QuotaExceeded {
        current: quota_limit,
        limit: quota_limit,
    })
}

#[derive(Clone)]
struct LedgerReservationData {
    store: Arc<ECStore>,
    bucket: String,
    ledger_object: String,
    operation_id: Uuid,
    reservation: PersistedReservation,
}

#[derive(Clone)]
enum ReservationLedgerData {
    Single(LedgerReservationData),
    Sharded(ShardedReservationData),
}

#[derive(Clone)]
struct ShardedReservationData {
    store: Arc<ECStore>,
    bucket: String,
    ledger_object: String,
    allocator_object: String,
    shard_object: String,
    shard_index: u16,
    bucket_incarnation: Uuid,
    quota_revision: OffsetDateTime,
    operation_id: Uuid,
    reservation: PersistedReservation,
}

impl ReservationLedgerData {
    fn store(&self) -> &Arc<ECStore> {
        match self {
            Self::Single(data) => &data.store,
            Self::Sharded(data) => &data.store,
        }
    }

    fn bucket(&self) -> &str {
        match self {
            Self::Single(data) => &data.bucket,
            Self::Sharded(data) => &data.bucket,
        }
    }

    fn error_object(&self) -> &str {
        match self {
            Self::Single(data) => &data.ledger_object,
            Self::Sharded(data) => &data.shard_object,
        }
    }
}

pub(crate) struct QuotaReservation {
    ledger: Option<ReservationLedgerData>,
    operation_guard: Option<NamespaceLockGuard>,
    metadata_guard: Option<Arc<NamespaceLockGuard>>,
    capability_proof: Option<crate::services::notification_sys::CrossPoolFenceFleetProofToken>,
    state: ReservationState,
}

#[derive(Clone, Copy)]
enum ReservationState {
    Pending,
    CommitStarted,
    Committed,
    FenceReleaseUncertain,
}

impl QuotaReservation {
    fn unlimited(metadata_guard: Option<Arc<NamespaceLockGuard>>) -> Self {
        Self {
            ledger: None,
            operation_guard: None,
            metadata_guard,
            capability_proof: None,
            state: ReservationState::Pending,
        }
    }

    pub(crate) fn is_lock_lost(&self) -> bool {
        self.operation_guard.as_ref().is_some_and(NamespaceLockGuard::is_lock_lost)
            || self.metadata_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
    }

    pub(crate) fn capability_proof_matches(&self) -> bool {
        self.capability_proof
            .as_ref()
            .is_none_or(crate::services::notification_sys::cross_pool_fence_fleet_proof_matches)
    }

    pub(crate) async fn mark_commit_started(&mut self) -> Result<()> {
        if !self.capability_proof_matches() {
            let ledger = self.ledger.as_ref().ok_or(StorageError::PartMissingOrCorrupt)?;
            return Err(quota_capability_error(ledger.bucket(), ledger.error_object()));
        }
        if let Some(ledger) = self.ledger.as_ref() {
            mark_commit_started(ledger).await?;
        }
        self.state = ReservationState::CommitStarted;
        Ok(())
    }

    pub(crate) async fn commit(mut self) {
        self.state = ReservationState::Committed;
        let Some(ledger) = self.ledger.as_ref() else {
            return;
        };
        crate::store::list_objects::observe_list_objects_mutation(ledger.store(), ledger.bucket()).await;
        match settle(ledger, true).await {
            Ok(()) => self.ledger = None,
            Err(err) => log_deferred_settlement(ledger, "commit_deferred", &err),
        }
    }

    pub(crate) async fn abort(mut self) {
        let Some(ledger) = self.ledger.as_ref() else {
            return;
        };
        match settle(ledger, false).await {
            Ok(()) => self.ledger = None,
            Err(err) => log_deferred_settlement(ledger, "abort_deferred", &err),
        }
    }

    pub(crate) fn defer_after_fence(mut self) {
        self.state = ReservationState::FenceReleaseUncertain;
    }
}

fn should_settle_on_drop(state: ReservationState) -> bool {
    !matches!(state, ReservationState::CommitStarted | ReservationState::FenceReleaseUncertain)
}

impl Drop for QuotaReservation {
    fn drop(&mut self) {
        let Some(ledger) = self.ledger.take() else {
            return;
        };
        if !should_settle_on_drop(self.state) {
            return;
        }
        let committed = matches!(self.state, ReservationState::Committed);
        let operation_guard = self.operation_guard.take();
        let metadata_guard = self.metadata_guard.take();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        runtime.spawn(async move {
            let _operation_guard = operation_guard;
            let _metadata_guard = metadata_guard;
            if let Err(err) = settle(&ledger, committed).await {
                log_deferred_settlement(&ledger, "background_retry_failed", &err);
            }
        });
    }
}

pub(crate) async fn begin(
    ctx: &crate::runtime::instance::InstanceContext,
    bucket: &str,
    object: &str,
    opts: &ObjectOptions,
    pool_index: usize,
    set_index: usize,
) -> Result<QuotaContext> {
    let snapshot_admission = opts.quota_admission;
    let data_movement = opts.data_movement;
    if crate::bucket::utils::is_meta_bucketname(bucket) {
        return Ok(QuotaContext {
            store: None,
            bucket: bucket.to_string(),
            object: object.to_string(),
            ledger_object: ledger_object(bucket),
            bucket_incarnation: None,
            quota_revision: None,
            quota_limit: None,
            reservation_protocol: None,
            capability_proof: None,
            snapshot_admission: None,
            legacy_data_movement: false,
            metadata_guard: None,
            pool_index: None,
            set_index: None,
        });
    }
    #[cfg(test)]
    if let Some(snapshot_admission) = snapshot_admission {
        return Ok(QuotaContext {
            store: None,
            bucket: bucket.to_string(),
            object: object.to_string(),
            ledger_object: ledger_object(bucket),
            bucket_incarnation: None,
            quota_revision: None,
            quota_limit: Some(snapshot_admission.quota_limit()),
            reservation_protocol: None,
            capability_proof: None,
            snapshot_admission: Some(snapshot_admission),
            legacy_data_movement: false,
            metadata_guard: None,
            pool_index: Some(pool_index),
            set_index: Some(set_index),
        });
    }
    #[cfg(any(test, feature = "test-util"))]
    if ctx.bucket_metadata_sys().is_none() {
        return Ok(QuotaContext {
            store: None,
            bucket: bucket.to_string(),
            object: object.to_string(),
            ledger_object: ledger_object(bucket),
            bucket_incarnation: None,
            quota_revision: None,
            quota_limit: None,
            reservation_protocol: None,
            capability_proof: None,
            snapshot_admission: None,
            legacy_data_movement: false,
            metadata_guard: None,
            pool_index: None,
            set_index: None,
        });
    }

    let metadata_guard = metadata_sys::acquire_bucket_metadata_transaction_read_lock_for_options_in(ctx, bucket, opts).await?;
    let (quota, bucket_incarnation, quota_revision) =
        metadata_sys::get_quota_config_and_incarnation_from_disk_for_options_in(ctx, bucket, opts, &metadata_guard).await?;
    if metadata_guard.is_lock_lost() {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "quota_config",
            bucket: bucket.to_string(),
            object: ledger_object(bucket),
            required: 1,
            achieved: 0,
        });
    }
    if quota
        .as_ref()
        .is_some_and(|quota| quota.has_unsupported_reservation_protocol())
    {
        log_admission_rejected(bucket, object, "unsupported_reservation_protocol");
        return Err(StorageError::PartMissingOrCorrupt);
    }
    let durable_quota = quota.as_ref().filter(|quota| quota.uses_durable_reservations());
    let capability_proof = if durable_quota.is_some() {
        Some(
            crate::services::notification_sys::acquire_cross_pool_fence_fleet_proof()
                .ok_or_else(|| quota_capability_error(bucket, &ledger_object(bucket)))?,
        )
    } else {
        None
    };
    let durable_quota_limit = durable_quota.and_then(|quota| quota.quota);
    let reservation_protocol = durable_quota.and_then(|quota| quota.reservation_protocol);
    let snapshot_admission = match quota.as_ref().filter(|quota| !quota.uses_durable_reservations()) {
        Some(quota) => match (quota.quota, snapshot_admission) {
            (Some(limit), Some(admission)) if admission.quota_limit() == limit => Some(admission),
            (Some(_), None) if data_movement => None,
            (Some(_), _) => {
                // A snapshot-protocol quota requires the request handler's
                // admission on every write. Missing or mismatched admission
                // means a caller rebuilt `ObjectOptions` without carrying it
                // over (rustfs/rustfs#7674 lost it on CopyObject); fail closed
                // but leave a diagnosable trace, because the storage error is
                // the generic `PartMissingOrCorrupt`.
                log_admission_rejected(bucket, object, "snapshot_quota_admission_missing");
                return Err(StorageError::PartMissingOrCorrupt);
            }
            (None, _) => None,
        },
        None => None,
    };
    let legacy_data_movement = durable_quota_limit.is_none()
        && quota.as_ref().and_then(|quota| quota.quota).is_some()
        && snapshot_admission.is_none()
        && data_movement;
    let quota_limit = durable_quota_limit
        .or_else(|| snapshot_admission.map(QuotaAdmission::quota_limit))
        .or_else(|| {
            legacy_data_movement
                .then(|| quota.as_ref().and_then(|quota| quota.quota))
                .flatten()
        });
    let store = if durable_quota_limit.is_some() {
        Some(metadata_sys::object_store_in(ctx).await?)
    } else {
        None
    };
    Ok(QuotaContext {
        store,
        bucket: bucket.to_string(),
        object: object.to_string(),
        ledger_object: ledger_object(bucket),
        bucket_incarnation: Some(bucket_incarnation),
        quota_revision: Some(quota_revision),
        quota_limit,
        reservation_protocol,
        capability_proof,
        snapshot_admission,
        legacy_data_movement,
        metadata_guard: Some(metadata_guard),
        pool_index: Some(pool_index),
        set_index: Some(set_index),
    })
}

fn quota_capability_error(bucket: &str, object: &str) -> StorageError {
    StorageError::NamespaceLockQuorumUnavailable {
        mode: "quota_capability",
        bucket: bucket.to_string(),
        object: object.to_string(),
        required: 1,
        achieved: 0,
    }
}

pub(crate) async fn replaced_logical_size(set_disks: &SetDisks, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<u64> {
    if opts.versioned && !opts.version_suspended && opts.version_id.is_none() {
        return Ok(0);
    }
    let version_id = opts
        .version_id
        .clone()
        .or_else(|| opts.version_suspended.then(|| Uuid::nil().to_string()));
    let lookup_opts = ObjectOptions {
        version_id,
        no_lock: true,
        metadata_cache_safe: false,
        versioned: opts.versioned,
        version_suspended: opts.version_suspended,
        ..Default::default()
    };
    match set_disks.get_object_info(bucket, object, &lookup_opts).await {
        Ok(info) if info.delete_marker => Ok(0),
        Ok(info) => logical_object_size(&info),
        Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => Ok(0),
        Err(err) => Err(err),
    }
}

fn logical_object_size(info: &ObjectInfo) -> Result<u64> {
    crate::data_usage::quota_object_size(info)
}

async fn mark_commit_started(data: &ReservationLedgerData) -> Result<()> {
    if let ReservationLedgerData::Sharded(data) = data {
        return mark_commit_started_sharded(data).await;
    }
    let ReservationLedgerData::Single(data) = data else {
        unreachable!();
    };
    let data = data.clone();
    tokio::spawn(async move {
        let ledger_lock = data.store.new_ns_lock(RUSTFS_META_BUCKET, &data.ledger_object).await?;
        let ledger_guard = Arc::new(ledger_lock.get_write_lock(get_lock_acquire_timeout()).await?);
        fence_namespace_mutations(&data.store, RUSTFS_META_BUCKET, &data.ledger_object, None).await?;
        let mut ledger = load_ledger_locked(Arc::clone(&data.store), &data.ledger_object).await?;
        ledger.mark_commit_started(data.operation_id, &data.reservation)?;
        save_ledger_locked(Arc::clone(&data.store), &data.ledger_object, &ledger, &ledger_guard).await
    })
    .await
    .map_err(|err| StorageError::other_with_context("quota commit marker task failed", err))?
}

async fn settle(data: &ReservationLedgerData, committed: bool) -> Result<()> {
    if let ReservationLedgerData::Sharded(data) = data {
        return settle_sharded(data, committed).await;
    }
    let ReservationLedgerData::Single(data) = data else {
        unreachable!();
    };
    let store = Arc::clone(&data.store);
    let ledger_object = data.ledger_object.clone();
    let operation_id = data.operation_id;
    let reservation = data.reservation.clone();
    tokio::spawn(async move {
        // The commit/abort path releases its own object fence before settlement.
        // Do not revoke all tokens here: a deferred retry can run after the
        // object lock is released and would otherwise revoke a later write's
        // newly acquired fence for the same object.
        let ledger_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &ledger_object).await?;
        let ledger_guard = Arc::new(ledger_lock.get_write_lock(get_lock_acquire_timeout()).await?);
        fence_namespace_mutations(&store, RUSTFS_META_BUCKET, &ledger_object, None).await?;
        let mut ledger = load_ledger_locked(Arc::clone(&store), &ledger_object).await?;
        if committed {
            ledger.commit(operation_id, &reservation)?;
        } else {
            ledger.abort(operation_id, &reservation)?;
        }
        save_ledger_locked(Arc::clone(&store), &ledger_object, &ledger, &ledger_guard).await
    })
    .await
    .map_err(|err| StorageError::other_with_context("quota ledger settlement task failed", err))?
}

async fn load_current_ledger_locked(
    store: Arc<ECStore>,
    bucket: &str,
    ledger_object: &str,
    bucket_incarnation: Uuid,
    quota_revision: OffsetDateTime,
) -> Result<QuotaLedger> {
    match load_ledger_locked(Arc::clone(&store), ledger_object).await {
        Ok(ledger) if ledger.matches(bucket_incarnation, quota_revision) => Ok(ledger),
        Ok(ledger) if ledger.reservations.is_empty() && !ledger.reconcile_required => {
            let usage = exact_bucket_usage(&store, bucket).await?;
            Ok(QuotaLedger::new(bucket_incarnation, quota_revision, usage))
        }
        Ok(_) => Err(StorageError::PartMissingOrCorrupt),
        Err(StorageError::ConfigNotFound) => {
            let usage = exact_bucket_usage(&store, bucket).await?;
            Ok(QuotaLedger::new(bucket_incarnation, quota_revision, usage))
        }
        Err(err) => Err(err),
    }
}

async fn reap_stale_reservations(store: Arc<ECStore>, bucket: &str, ledger_object: &str) -> Result<()> {
    let now = now_unix();
    let (candidates, reconcile_required, next_cursor) = {
        let ledger_lock = store.new_ns_lock(RUSTFS_META_BUCKET, ledger_object).await?;
        let _ledger_guard = ledger_lock.get_write_lock(get_lock_acquire_timeout()).await?;
        match load_ledger_locked(Arc::clone(&store), ledger_object).await {
            Ok(ledger) => {
                let (candidates, next_cursor) = ledger.reap_candidates(now);
                (candidates, ledger.reconcile_required, next_cursor)
            }
            Err(StorageError::ConfigNotFound) => (Vec::new(), false, None),
            Err(err) => return Err(err),
        }
    };
    if candidates.is_empty() && !reconcile_required {
        return Ok(());
    }

    let probe_results = stream::iter(candidates)
        .map(|operation_id| {
            let store = Arc::clone(&store);
            async move {
                let lock_object = operation_lock_object(ledger_object, operation_id);
                let operation_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &lock_object).await?;
                Ok::<_, StorageError>(
                    operation_lock
                        .get_write_lock_quiet(Duration::from_millis(50))
                        .await
                        .ok()
                        .map(|guard| (operation_id, guard)),
                )
            }
        })
        .buffer_unordered(ORPHAN_PROBE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    let mut orphan_guards = Vec::new();
    for result in probe_results {
        if let Some(guard) = result? {
            orphan_guards.push(guard);
            if orphan_guards.len() == MAX_ORPHANS_REAPED_PER_WRITE {
                break;
            }
        }
    }

    let ledger_lock = store.new_ns_lock(RUSTFS_META_BUCKET, ledger_object).await?;
    let ledger_guard = Arc::new(ledger_lock.get_write_lock(get_lock_acquire_timeout()).await?);
    fence_namespace_mutations(&store, RUSTFS_META_BUCKET, ledger_object, None).await?;
    let mut ledger = load_ledger_locked(Arc::clone(&store), ledger_object).await?;
    let cursor_changed = next_cursor.is_some() && ledger.reap_cursor != next_cursor;
    if next_cursor.is_some() {
        ledger.reap_cursor = next_cursor;
    }
    let orphan_ids = orphan_guards
        .iter()
        .map(|(operation_id, _)| *operation_id)
        .collect::<Vec<_>>();
    let orphan_commit_targets = orphan_ids
        .iter()
        .filter_map(|operation_id| ledger.reservations.get(operation_id))
        .filter(|reservation| reservation.commit_started)
        .map(|reservation| (reservation.object.clone(), reservation.target()))
        .collect::<Vec<_>>();
    for (object, target) in orphan_commit_targets {
        fence_namespace_mutations(&store, bucket, &object, target).await?;
    }
    let mut removed = remove_orphan_reservations(&mut ledger, &orphan_ids)?;
    if ledger.reservations.is_empty() && ledger.reconcile_required {
        reconcile_exact(&store, bucket, &mut ledger).await?;
        removed = true;
    }
    if !removed && !cursor_changed {
        return Ok(());
    }
    save_ledger_locked(store, ledger_object, &ledger, &ledger_guard).await
}

fn remove_orphan_reservations(ledger: &mut QuotaLedger, operation_ids: &[Uuid]) -> Result<bool> {
    let mut removed = false;
    for operation_id in operation_ids {
        let Some(reservation) = ledger.reservations.get(operation_id).cloned() else {
            continue;
        };
        if reservation.commit_started {
            ledger.reservations.remove(operation_id);
            ledger.reconcile_required = true;
        } else {
            ledger.abort(*operation_id, &reservation)?;
        }
        removed = true;
    }
    Ok(removed)
}

async fn reconcile_exact(store: &Arc<ECStore>, bucket: &str, ledger: &mut QuotaLedger) -> Result<()> {
    if !ledger.reservations.is_empty() {
        return Err(StorageError::PartMissingOrCorrupt);
    }
    ledger.accounted_usage = exact_bucket_usage(store, bucket).await?;
    ledger.reconcile_required = false;
    Ok(())
}

async fn exact_bucket_usage(store: &Arc<ECStore>, bucket: &str) -> Result<u64> {
    crate::store::list_objects::observe_list_objects_mutation(store, bucket).await;
    Ok(compute_bucket_usage(Arc::clone(store), bucket).await?.size)
}

async fn reconcile_shard_exact(store: &Arc<ECStore>, bucket: &str, shard: &mut QuotaShardLedger) -> Result<()> {
    if !shard.reservations.is_empty() {
        return Err(StorageError::PartMissingOrCorrupt);
    }
    shard.accounted_usage = exact_shard_usage(store, bucket, shard.shard_index).await?;
    if shard.accounted_usage > shard.credit_limit()? {
        return Err(StorageError::PartMissingOrCorrupt);
    }
    shard.reconcile_required = false;
    Ok(())
}

async fn exact_shard_usage(store: &Arc<ECStore>, bucket: &str, target_shard: u16) -> Result<u64> {
    let usages = exact_shard_usages(store, bucket).await?;
    usages
        .get(usize::from(target_shard))
        .copied()
        .ok_or(StorageError::PartMissingOrCorrupt)
}

async fn exact_shard_usages(store: &Arc<ECStore>, bucket: &str) -> Result<Vec<u64>> {
    let mut marker = None;
    let mut version_marker = None;
    let mut usages = vec![0_u64; usize::from(SHARDED_LEDGER_COUNT)];
    loop {
        let page = Arc::clone(store)
            .list_object_versions(bucket, "", marker, version_marker, None, 1_000)
            .await?;
        for object in page.objects {
            let shard = usize::from(shard_index(object.name.as_str()));
            usages[shard] = usages[shard]
                .checked_add(logical_object_size(&object)?)
                .ok_or(StorageError::PartMissingOrCorrupt)?;
        }
        if !page.is_truncated {
            return Ok(usages);
        }
        marker = page.next_marker;
        version_marker = page.next_version_idmarker;
        if marker.is_none() && version_marker.is_none() {
            return Err(StorageError::PartMissingOrCorrupt);
        }
    }
}

fn ledger_object(bucket: &str) -> String {
    format!("{CONFIG_PREFIX}/quota-ledger/{bucket}.json")
}

fn operation_lock_object(ledger_object: &str, operation_id: Uuid) -> String {
    format!("{ledger_object}.operations/{operation_id}")
}

fn allocator_object(ledger_object: &str, bucket_incarnation: Uuid, quota_revision: OffsetDateTime) -> String {
    format!(
        "{ledger_object}.allocator.{bucket_incarnation}.{}.json",
        quota_revision.unix_timestamp_nanos()
    )
}

fn shard_object(ledger_object: &str, shard_index: u16, bucket_incarnation: Uuid, quota_revision: OffsetDateTime) -> String {
    format!(
        "{ledger_object}.shards/{bucket_incarnation}/{}.{shard_index:02}.json",
        quota_revision.unix_timestamp_nanos()
    )
}

fn shard_index(object: &str) -> u16 {
    let digest = Sha256::digest(object.as_bytes());
    u16::from_be_bytes([digest[0], digest[1]]) % SHARDED_LEDGER_COUNT
}

fn shard_may_be_created(allocator: &QuotaAllocatorLedger, shard_index: u16, bootstrap: bool) -> bool {
    if bootstrap {
        return true;
    }
    let grants = allocator.grants_for(shard_index);
    grants.is_empty() || grants.values().all(|grant| grant.initial_usage > 0)
}

fn credit_grant_amount(issued: u64, quota_limit: u64, growth: u64) -> Result<u64> {
    let available = quota_limit.saturating_sub(issued);
    if growth > available {
        return Err(StorageError::QuotaExceeded {
            current: issued,
            limit: quota_limit,
        });
    }
    let fair_chunk = quota_limit.div_ceil(SHARDED_LEDGER_COUNT as u64).max(1);
    Ok(growth.max(fair_chunk).min(available))
}

async fn ensure_sharded_allocator(
    store: Arc<ECStore>,
    data: &ShardedReservationData,
    growth: u64,
    quota_limit: u64,
) -> Result<bool> {
    let allocator_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.allocator_object).await?;
    let allocator_guard = allocator_lock.get_read_lock(get_lock_acquire_timeout()).await?;
    match load_allocator_locked(Arc::clone(&store), &data.allocator_object).await {
        Ok(allocator) if allocator.matches(data.bucket_incarnation, data.quota_revision, quota_limit) => return Ok(false),
        Ok(_) | Err(StorageError::ConfigNotFound) => {}
        Err(err) => return Err(err),
    }
    drop(allocator_guard);

    let allocator_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.allocator_object).await?;
    let allocator_guard = Arc::new(allocator_lock.get_write_lock(get_lock_acquire_timeout()).await?);
    match load_allocator_locked(Arc::clone(&store), &data.allocator_object).await {
        Ok(allocator) if allocator.matches(data.bucket_incarnation, data.quota_revision, quota_limit) => return Ok(false),
        Ok(allocator) if !allocator.grants.is_empty() => return Err(StorageError::PartMissingOrCorrupt),
        Ok(_) | Err(StorageError::ConfigNotFound) => {}
        Err(err) => return Err(err),
    }
    for candidate in 0..SHARDED_LEDGER_COUNT {
        let object = shard_object(&data.ledger_object, candidate, data.bucket_incarnation, data.quota_revision);
        match read_config_no_lock(Arc::clone(&store), &object).await {
            Ok(_) => return Err(StorageError::PartMissingOrCorrupt),
            Err(StorageError::ConfigNotFound) => {}
            Err(err) => return Err(err),
        }
    }
    if allocator_guard.is_lock_lost() {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "quota_allocator_bootstrap",
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: data.allocator_object.clone(),
            required: 1,
            achieved: 0,
        });
    }
    fence_namespace_mutations(&store, RUSTFS_META_BUCKET, &data.allocator_object, None).await?;
    let shard_usages = exact_shard_usages(&store, &data.bucket).await?;
    let usage = shard_usages
        .iter()
        .try_fold(0_u64, |total, usage| total.checked_add(*usage).ok_or(StorageError::PartMissingOrCorrupt))?;
    let required = usage.checked_add(growth).ok_or(StorageError::PartMissingOrCorrupt)?;
    let _ = credit_grant_amount(0, quota_limit, required)?;
    let mut allocator = QuotaAllocatorLedger::new(data.bucket_incarnation, data.quota_revision, quota_limit);
    for (shard_index, initial_usage) in shard_usages.into_iter().enumerate() {
        let shard_index = u16::try_from(shard_index).map_err(|_| StorageError::PartMissingOrCorrupt)?;
        let growth_for_shard = if shard_index == data.shard_index { growth } else { 0 };
        let amount = initial_usage
            .checked_add(growth_for_shard)
            .ok_or(StorageError::PartMissingOrCorrupt)?;
        if amount == 0 {
            continue;
        }
        allocator.grants.insert(
            Uuid::new_v4(),
            AllocatorGrant {
                shard_index,
                amount,
                initial_usage,
            },
        );
    }
    allocator.generation = 1;
    save_allocator_locked(Arc::clone(&store), &data.allocator_object, &allocator, &allocator_guard).await?;
    Ok(true)
}

async fn load_allocator_locked(store: Arc<ECStore>, object: &str) -> Result<QuotaAllocatorLedger> {
    let data = read_config_no_lock(store, object).await?;
    let allocator: QuotaAllocatorLedger = serde_json::from_slice(&data)?;
    if allocator.version != SHARDED_LEDGER_FORMAT_VERSION {
        return Err(StorageError::CorruptedFormat);
    }
    allocator.issued_bytes()?;
    Ok(allocator)
}

async fn load_current_allocator_locked(
    store: Arc<ECStore>,
    object: &str,
    bucket_incarnation: Uuid,
    quota_revision: OffsetDateTime,
    quota_limit: u64,
) -> Result<QuotaAllocatorLedger> {
    match load_allocator_locked(Arc::clone(&store), object).await {
        Ok(allocator) if allocator.matches(bucket_incarnation, quota_revision, quota_limit) => Ok(allocator),
        Ok(allocator) if allocator.grants.is_empty() => {
            Ok(QuotaAllocatorLedger::new(bucket_incarnation, quota_revision, quota_limit))
        }
        Ok(_) => Err(StorageError::PartMissingOrCorrupt),
        Err(StorageError::ConfigNotFound) => Err(StorageError::PartMissingOrCorrupt),
        Err(err) => Err(err),
    }
}

async fn load_shard_locked(store: Arc<ECStore>, object: &str) -> Result<QuotaShardLedger> {
    let data = read_config_no_lock(store, object).await?;
    let shard: QuotaShardLedger = serde_json::from_slice(&data)?;
    if shard.version != SHARDED_LEDGER_FORMAT_VERSION {
        return Err(StorageError::CorruptedFormat);
    }
    shard
        .credit_limit()?
        .checked_sub(shard.accounted_usage)
        .ok_or(StorageError::PartMissingOrCorrupt)?;
    Ok(shard)
}

async fn load_current_shard_locked(
    store: Arc<ECStore>,
    object: &str,
    bucket_incarnation: Uuid,
    quota_revision: OffsetDateTime,
    shard_index: u16,
    allow_create: bool,
    allocator: &QuotaAllocatorLedger,
) -> Result<QuotaShardLedger> {
    let mut shard = match load_shard_locked(Arc::clone(&store), object).await {
        Ok(shard) if shard.matches(bucket_incarnation, quota_revision, shard_index) => shard,
        Ok(_) => return Err(StorageError::PartMissingOrCorrupt),
        Err(StorageError::ConfigNotFound) if allow_create => {
            QuotaShardLedger::new(bucket_incarnation, quota_revision, shard_index)
        }
        Err(StorageError::ConfigNotFound) => return Err(StorageError::PartMissingOrCorrupt),
        Err(err) => return Err(err),
    };
    shard.adopt_grants(allocator)?;
    Ok(shard)
}

async fn save_allocator_locked(
    store: Arc<ECStore>,
    object: &str,
    allocator: &QuotaAllocatorLedger,
    guard: &Arc<NamespaceLockGuard>,
) -> Result<()> {
    if guard.is_lock_lost() {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "quota_allocator",
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: object.to_string(),
            required: 1,
            achieved: 0,
        });
    }
    let mut opts = ObjectOptions {
        max_parity: true,
        no_lock: true,
        ..Default::default()
    };
    let _ = opts.set_quota_admission(0, u64::MAX);
    opts.add_owned_write_lock(Arc::clone(guard), RUSTFS_META_BUCKET, object);
    opts.write_completion = crate::object_api::WriteCompletion::TailDrained;
    save_config_with_opts(store, object, serde_json::to_vec(allocator)?, &opts).await
}

async fn save_shard_locked(
    store: Arc<ECStore>,
    object: &str,
    shard: &QuotaShardLedger,
    guard: &Arc<NamespaceLockGuard>,
) -> Result<()> {
    if guard.is_lock_lost() {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "quota_shard",
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: object.to_string(),
            required: 1,
            achieved: 0,
        });
    }
    let mut opts = ObjectOptions {
        max_parity: true,
        no_lock: true,
        ..Default::default()
    };
    let _ = opts.set_quota_admission(0, u64::MAX);
    opts.add_owned_write_lock(Arc::clone(guard), RUSTFS_META_BUCKET, object);
    opts.write_completion = crate::object_api::WriteCompletion::TailDrained;
    save_config_with_opts(store, object, serde_json::to_vec(shard)?, &opts).await
}

async fn refill_sharded_credit(
    store: Arc<ECStore>,
    allocator_object: &str,
    shard_index: u16,
    growth: u64,
    bucket_incarnation: Uuid,
    quota_revision: OffsetDateTime,
    quota_limit: u64,
) -> Result<()> {
    let allocator_lock = store.new_ns_lock(RUSTFS_META_BUCKET, allocator_object).await?;
    let allocator_guard = Arc::new(allocator_lock.get_write_lock(get_lock_acquire_timeout()).await?);
    if allocator_guard.is_lock_lost() {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "quota_allocator",
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: allocator_object.to_string(),
            required: 1,
            achieved: 0,
        });
    }
    fence_namespace_mutations(&store, RUSTFS_META_BUCKET, allocator_object, None).await?;
    let mut allocator =
        load_current_allocator_locked(Arc::clone(&store), allocator_object, bucket_incarnation, quota_revision, quota_limit)
            .await?;
    let issued = allocator.issued_bytes()?;
    let amount = credit_grant_amount(issued, quota_limit, growth)?;
    let grant_id = Uuid::new_v4();
    allocator.grants.insert(
        grant_id,
        AllocatorGrant {
            shard_index,
            amount,
            initial_usage: 0,
        },
    );
    allocator.generation = allocator
        .generation
        .checked_add(1)
        .ok_or(StorageError::PartMissingOrCorrupt)?;
    save_allocator_locked(Arc::clone(&store), allocator_object, &allocator, &allocator_guard).await
}

async fn reap_sharded_reservations(
    store: Arc<ECStore>,
    data: &ShardedReservationData,
    bucket_incarnation: Uuid,
    quota_revision: OffsetDateTime,
    quota_limit: u64,
) -> Result<bool> {
    let allocator_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.allocator_object).await?;
    let allocator_guard = allocator_lock.get_read_lock(get_lock_acquire_timeout()).await?;
    if allocator_guard.is_lock_lost() {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "quota_allocator",
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: data.allocator_object.clone(),
            required: 1,
            achieved: 0,
        });
    }
    let allocator = load_current_allocator_locked(
        Arc::clone(&store),
        &data.allocator_object,
        bucket_incarnation,
        quota_revision,
        quota_limit,
    )
    .await?;
    let shard_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.shard_object).await?;
    let shard_guard = shard_lock.get_write_lock(get_lock_acquire_timeout()).await?;
    let shard = load_current_shard_locked(
        Arc::clone(&store),
        &data.shard_object,
        bucket_incarnation,
        quota_revision,
        data.shard_index,
        false,
        &allocator,
    )
    .await?;
    let (candidates, next_cursor) = shard.reap_candidates(now_unix());
    drop(shard_guard);
    drop(allocator_guard);
    if candidates.is_empty() && next_cursor.is_none() {
        return Ok(false);
    }
    let probe_results = stream::iter(candidates)
        .map(|operation_id| {
            let store = Arc::clone(&store);
            let object = data.shard_object.clone();
            async move {
                let lock_object = operation_lock_object(&object, operation_id);
                let operation_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &lock_object).await?;
                Ok::<_, StorageError>(
                    operation_lock
                        .get_write_lock_quiet(Duration::from_millis(50))
                        .await
                        .ok()
                        .map(|guard| (operation_id, guard)),
                )
            }
        })
        .buffer_unordered(ORPHAN_PROBE_CONCURRENCY)
        .collect::<Vec<_>>()
        .await;
    let mut orphan_guards = Vec::new();
    for result in probe_results {
        if let Some(guard) = result? {
            orphan_guards.push(guard);
        }
    }
    let had_orphan_guards = !orphan_guards.is_empty();
    let allocator_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.allocator_object).await?;
    let allocator_guard = allocator_lock.get_read_lock(get_lock_acquire_timeout()).await?;
    if allocator_guard.is_lock_lost() {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "quota_allocator",
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: data.allocator_object.clone(),
            required: 1,
            achieved: 0,
        });
    }
    let allocator = load_current_allocator_locked(
        Arc::clone(&store),
        &data.allocator_object,
        bucket_incarnation,
        quota_revision,
        quota_limit,
    )
    .await?;
    let shard_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.shard_object).await?;
    let shard_guard = Arc::new(shard_lock.get_write_lock(get_lock_acquire_timeout()).await?);
    let mut shard = load_current_shard_locked(
        Arc::clone(&store),
        &data.shard_object,
        bucket_incarnation,
        quota_revision,
        data.shard_index,
        false,
        &allocator,
    )
    .await?;
    let mut removed = false;
    for (operation_id, _guard) in &orphan_guards {
        let Some(reservation) = shard.reservations.get(operation_id).cloned() else {
            continue;
        };
        if reservation.commit_started {
            shard.reservations.remove(operation_id);
            shard.reconcile_required = true;
        } else {
            shard.abort(*operation_id, &reservation)?;
        }
        removed = true;
    }
    let cursor_changed = next_cursor.is_some() && shard.reap_cursor != next_cursor;
    if next_cursor.is_some() {
        shard.reap_cursor = next_cursor;
    }
    if !removed && !cursor_changed {
        return Ok(false);
    }
    if shard.reservations.is_empty() && shard.reconcile_required {
        reconcile_shard_exact(&store, &data.bucket, &mut shard).await?;
    }
    fence_namespace_mutations(&store, RUSTFS_META_BUCKET, &data.shard_object, None).await?;
    if allocator_guard.is_lock_lost() || shard_guard.is_lock_lost() {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "quota_shard_reap",
            bucket: data.bucket.clone(),
            object: data.shard_object.clone(),
            required: 1,
            achieved: 0,
        });
    }
    save_shard_locked(Arc::clone(&store), &data.shard_object, &shard, &shard_guard).await?;
    Ok(removed || cursor_changed || had_orphan_guards)
}

async fn mark_commit_started_sharded(data: &ShardedReservationData) -> Result<()> {
    let store = Arc::clone(&data.store);
    let data = data.clone();
    tokio::spawn(async move {
        let allocator_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.allocator_object).await?;
        let allocator_guard = allocator_lock.get_read_lock(get_lock_acquire_timeout()).await?;
        if allocator_guard.is_lock_lost() {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "quota_allocator",
                bucket: RUSTFS_META_BUCKET.to_string(),
                object: data.allocator_object.clone(),
                required: 1,
                achieved: 0,
            });
        }
        let allocator = load_allocator_locked(Arc::clone(&store), &data.allocator_object).await?;
        let shard_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.shard_object).await?;
        let shard_guard = Arc::new(shard_lock.get_write_lock(get_lock_acquire_timeout()).await?);
        let mut shard = load_current_shard_locked(
            Arc::clone(&store),
            &data.shard_object,
            data.bucket_incarnation,
            data.quota_revision,
            data.shard_index,
            false,
            &allocator,
        )
        .await?;
        fence_namespace_mutations(&store, RUSTFS_META_BUCKET, &data.shard_object, None).await?;
        if allocator_guard.is_lock_lost() || shard_guard.is_lock_lost() {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "quota_sharded_commit_marker",
                bucket: data.bucket.clone(),
                object: data.shard_object.clone(),
                required: 1,
                achieved: 0,
            });
        }
        shard.mark_commit_started(data.operation_id, &data.reservation)?;
        save_shard_locked(Arc::clone(&store), &data.shard_object, &shard, &shard_guard).await
    })
    .await
    .map_err(|err| StorageError::other_with_context("quota sharded commit marker task failed", err))?
}

async fn settle_sharded(data: &ShardedReservationData, committed: bool) -> Result<()> {
    let store = Arc::clone(&data.store);
    let data = data.clone();
    tokio::spawn(async move {
        let allocator_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.allocator_object).await?;
        let allocator_guard = allocator_lock.get_read_lock(get_lock_acquire_timeout()).await?;
        if allocator_guard.is_lock_lost() {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "quota_allocator",
                bucket: RUSTFS_META_BUCKET.to_string(),
                object: data.allocator_object.clone(),
                required: 1,
                achieved: 0,
            });
        }
        let allocator = load_allocator_locked(Arc::clone(&store), &data.allocator_object).await?;
        let shard_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.shard_object).await?;
        let shard_guard = Arc::new(shard_lock.get_write_lock(get_lock_acquire_timeout()).await?);
        let mut shard = load_current_shard_locked(
            Arc::clone(&store),
            &data.shard_object,
            data.bucket_incarnation,
            data.quota_revision,
            data.shard_index,
            false,
            &allocator,
        )
        .await?;
        fence_namespace_mutations(&store, RUSTFS_META_BUCKET, &data.shard_object, None).await?;
        if allocator_guard.is_lock_lost() || shard_guard.is_lock_lost() {
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "quota_sharded_settlement",
                bucket: data.bucket.clone(),
                object: data.shard_object.clone(),
                required: 1,
                achieved: 0,
            });
        }
        if committed {
            shard.commit(data.operation_id, &data.reservation)?;
        } else {
            shard.abort(data.operation_id, &data.reservation)?;
        }
        if shard.reservations.is_empty() && shard.reconcile_required {
            reconcile_shard_exact(&store, &data.bucket, &mut shard).await?;
        }
        save_shard_locked(Arc::clone(&store), &data.shard_object, &shard, &shard_guard).await
    })
    .await
    .map_err(|err| StorageError::other_with_context("quota sharded settlement task failed", err))?
}

fn now_unix() -> i64 {
    OffsetDateTime::now_utc().unix_timestamp()
}

async fn fence_namespace_mutations(
    store: &Arc<ECStore>,
    bucket: &str,
    object: &str,
    target: Option<(usize, usize)>,
) -> Result<()> {
    crate::bucket::utils::check_object_args(bucket, object)?;
    let sets = match target {
        Some((pool_index, set_index)) => {
            let set = store
                .pools
                .get(pool_index)
                .and_then(|pool| pool.disk_set.get(set_index))
                .cloned()
                .ok_or(StorageError::PartMissingOrCorrupt)?;
            vec![set]
        }
        None => store.pools.iter().map(|pool| pool.get_disks_by_key(object)).collect(),
    };
    for set in sets {
        let write_quorum = set.default_write_quorum();
        let disks = set.disks.read().await.iter().flatten().cloned().collect::<Vec<_>>();
        let fence_path = crate::disk::quota_mutation_fence_path(bucket, object);
        let revoke_results = stream::iter(disks)
            .map(|disk| {
                let fence_path = fence_path.clone();
                async move {
                    let result = disk
                        .release_snapshot_lease(RUSTFS_META_BUCKET, &fence_path, crate::disk::SnapshotLeaseToken::revoke_all())
                        .await;
                    (disk, result)
                }
            })
            .buffer_unordered(ORPHAN_PROBE_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        let revoked_disks = revoke_results
            .into_iter()
            .filter_map(|(disk, result)| result.is_ok().then_some(disk))
            .collect::<Vec<_>>();
        if revoked_disks.len() < write_quorum {
            return Err(StorageError::ErasureWriteQuorum);
        }

        let drain_results = stream::iter(revoked_disks)
            .map(|disk| async move {
                match disk.acquire_snapshot_lease(bucket, object).await {
                    Ok(token) => disk.release_snapshot_lease(bucket, object, token).await,
                    Err(DiskError::FileNotFound | DiskError::VolumeNotFound) => Ok(()),
                    Err(err) => Err(err),
                }
            })
            .buffer_unordered(ORPHAN_PROBE_CONCURRENCY)
            .collect::<Vec<_>>()
            .await;
        if drain_results.iter().filter(|result| result.is_ok()).count() < write_quorum {
            return Err(StorageError::ErasureWriteQuorum);
        }
    }
    Ok(())
}

#[cfg(test)]
pub(crate) async fn fence_namespace_mutations_for_test(
    store: &Arc<ECStore>,
    bucket: &str,
    object: &str,
    target: Option<(usize, usize)>,
) -> Result<()> {
    fence_namespace_mutations(store, bucket, object, target).await
}

async fn load_ledger_locked(store: Arc<ECStore>, ledger_object: &str) -> Result<QuotaLedger> {
    let data = read_config_no_lock(store, ledger_object).await?;
    let ledger: QuotaLedger = serde_json::from_slice(&data)?;
    if ledger.version != QUOTA_LEDGER_FORMAT_VERSION {
        return Err(StorageError::CorruptedFormat);
    }
    ledger.admitted_usage()?;
    Ok(ledger)
}

async fn save_ledger_locked(
    store: Arc<ECStore>,
    ledger_object: &str,
    ledger: &QuotaLedger,
    ledger_guard: &Arc<NamespaceLockGuard>,
) -> Result<()> {
    if ledger_guard.is_lock_lost() {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "quota_ledger",
            bucket: RUSTFS_META_BUCKET.to_string(),
            object: ledger_object.to_string(),
            required: 1,
            achieved: 0,
        });
    }
    #[cfg(any(test, feature = "test-util"))]
    if FAIL_NEXT_LEDGER_SAVE.swap(false, std::sync::atomic::Ordering::SeqCst) {
        return Err(StorageError::Unexpected);
    }
    let mut opts = ObjectOptions {
        max_parity: true,
        no_lock: true,
        ..Default::default()
    };
    let _ = opts.set_quota_admission(0, u64::MAX);
    opts.add_owned_write_lock(Arc::clone(ledger_guard), RUSTFS_META_BUCKET, ledger_object);
    opts.write_completion = crate::object_api::WriteCompletion::TailDrained;
    save_config_with_opts(store, ledger_object, serde_json::to_vec(ledger)?, &opts).await
}

#[cfg(any(test, feature = "test-util"))]
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub fn fail_next_quota_ledger_save_for_test() {
    FAIL_NEXT_LEDGER_SAVE.store(true, std::sync::atomic::Ordering::SeqCst);
}

fn log_admission_rejected(bucket: &str, object: &str, state: &'static str) {
    warn!(
        event = EVENT_QUOTA_ADMISSION,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_QUOTA,
        state,
        bucket = %bucket,
        object = %object,
        "quota admission rejected the write before commit"
    );
}

fn log_deferred_settlement(data: &ReservationLedgerData, state: &'static str, err: &StorageError) {
    let operation_id = match data {
        ReservationLedgerData::Single(data) => data.operation_id,
        ReservationLedgerData::Sharded(data) => data.operation_id,
    };
    warn!(
        event = EVENT_QUOTA_LEDGER_SETTLEMENT,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_QUOTA,
        state,
        bucket = %data.bucket(),
        operation_id = %operation_id,
        error = %err,
        "quota ledger settlement deferred"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ledger(accounted_usage: u64) -> QuotaLedger {
        QuotaLedger::new(Uuid::new_v4(), OffsetDateTime::now_utc(), accounted_usage)
    }

    #[test]
    fn ledger_rejects_reserved_growth_overflow() {
        let mut ledger = ledger(u64::MAX);
        let result = ledger.reserve(
            Uuid::new_v4(),
            PersistedReservation {
                object: "object".to_string(),
                old_size: 0,
                new_size: 1,
                created_at: 0,
                pool_index: Some(0),
                set_index: Some(0),
                commit_started: false,
            },
        );

        assert!(matches!(result, Err(StorageError::PartMissingOrCorrupt)));
    }

    #[test]
    fn legacy_reservation_without_topology_uses_conservative_fallback() {
        let reservation: PersistedReservation =
            serde_json::from_str(r#"{"object":"object","old_size":0,"new_size":1,"created_at":0,"commit_started":true}"#)
                .expect("legacy reservation should deserialize");

        assert_eq!(reservation.target(), None);
    }

    #[test]
    fn ledger_rejects_persisted_reservations_larger_than_accounted_usage() {
        let mut ledger = ledger(0);
        ledger.reservations.insert(
            Uuid::new_v4(),
            PersistedReservation {
                object: "object".to_string(),
                old_size: 0,
                new_size: 1,
                created_at: 0,
                pool_index: Some(0),
                set_index: Some(0),
                commit_started: false,
            },
        );

        assert!(matches!(ledger.admitted_usage(), Err(StorageError::PartMissingOrCorrupt)));
    }

    #[test]
    fn ledger_accounts_overwrite_delta_and_reserved_growth() {
        let mut ledger = ledger(10);
        let operation_id = Uuid::new_v4();
        let overwrite = PersistedReservation {
            object: "object".to_string(),
            old_size: 8,
            new_size: 5,
            created_at: 0,
            pool_index: Some(0),
            set_index: Some(0),
            commit_started: false,
        };
        ledger
            .reserve(operation_id, overwrite.clone())
            .expect("shrinking overwrite should reserve");
        ledger
            .reserve(
                Uuid::new_v4(),
                PersistedReservation {
                    object: "new-object".to_string(),
                    old_size: 0,
                    new_size: 7,
                    created_at: 0,
                    pool_index: Some(0),
                    set_index: Some(0),
                    commit_started: false,
                },
            )
            .expect("new object should reserve positive growth");

        assert_eq!(
            ledger
                .admitted_usage()
                .expect("ledger usage should count positive growth only"),
            17
        );
        ledger
            .commit(operation_id, &overwrite)
            .expect("overwrite should settle exactly");
        assert_eq!(ledger.accounted_usage, 17);
        assert!(ledger.reconcile_required);
        assert_eq!(ledger.admitted_usage().expect("remaining reservation should stay counted"), 17);
    }

    #[test]
    fn commit_started_reservation_stays_precharged_until_reconciled() {
        let mut ledger = ledger(10);
        let operation_id = Uuid::new_v4();
        ledger
            .reserve(
                operation_id,
                PersistedReservation {
                    object: "object".to_string(),
                    old_size: 0,
                    new_size: 7,
                    created_at: 0,
                    pool_index: Some(0),
                    set_index: Some(0),
                    commit_started: false,
                },
            )
            .expect("new object should reserve positive growth");
        assert_eq!(ledger.accounted_usage, 17);

        let expected = ledger
            .reservations
            .get(&operation_id)
            .expect("reservation should exist")
            .clone();
        ledger
            .mark_commit_started(operation_id, &expected)
            .expect("commit marker should persist");

        assert_eq!(ledger.accounted_usage, 17);
        assert!(!ledger.should_reconcile_after_denial());
    }

    #[test]
    fn commit_started_orphans_make_progress_across_bounded_batches() {
        let mut ledger = ledger(65);
        let operation_ids = (0..65).map(|_| Uuid::new_v4()).collect::<Vec<_>>();
        for operation_id in &operation_ids {
            ledger.reservations.insert(
                *operation_id,
                PersistedReservation {
                    object: format!("object-{operation_id}"),
                    old_size: 0,
                    new_size: 1,
                    created_at: 0,
                    pool_index: Some(0),
                    set_index: Some(0),
                    commit_started: true,
                },
            );
        }

        assert!(remove_orphan_reservations(&mut ledger, &operation_ids[..64]).expect("first orphan batch should apply"));
        assert_eq!(ledger.reservations.len(), 1);
        assert!(ledger.reconcile_required);
        assert!(remove_orphan_reservations(&mut ledger, &operation_ids[64..]).expect("final orphan batch should apply"));
        assert!(ledger.reservations.is_empty());
    }

    #[test]
    fn orphan_probe_cursor_rotates_across_the_bounded_window() {
        let mut ledger = ledger(129);
        let operation_ids = (1..=129).map(Uuid::from_u128).collect::<Vec<_>>();
        for operation_id in &operation_ids {
            ledger.reservations.insert(
                *operation_id,
                PersistedReservation {
                    object: format!("object-{operation_id}"),
                    old_size: 0,
                    new_size: 1,
                    created_at: 0,
                    pool_index: Some(0),
                    set_index: Some(0),
                    commit_started: false,
                },
            );
        }

        let (first, cursor) = ledger.reap_candidates(1);
        assert_eq!(first.len(), MAX_ORPHAN_PROBES_PER_WRITE);
        assert_eq!(first.first(), operation_ids.first());
        ledger.reap_cursor = cursor;

        let (second, _) = ledger.reap_candidates(1);
        assert_eq!(second.first(), operation_ids.last());
    }

    #[test]
    fn uncertain_fence_release_does_not_schedule_abort_on_drop() {
        assert!(!should_settle_on_drop(ReservationState::FenceReleaseUncertain));
        assert!(!should_settle_on_drop(ReservationState::CommitStarted));
        assert!(should_settle_on_drop(ReservationState::Pending));
        assert!(should_settle_on_drop(ReservationState::Committed));
    }

    #[test]
    fn allocator_never_issues_credit_above_the_hard_limit() {
        let revision = OffsetDateTime::now_utc();
        let mut allocator = QuotaAllocatorLedger::new(Uuid::new_v4(), revision, 100);
        allocator.grants.insert(
            Uuid::new_v4(),
            AllocatorGrant {
                shard_index: 0,
                amount: 64,
                initial_usage: 0,
            },
        );
        assert_eq!(allocator.issued_bytes().expect("issued bytes should sum"), 64);
        assert_eq!(credit_grant_amount(64, allocator.quota_limit, 36).expect("exact remaining credit"), 36);
        assert!(matches!(
            credit_grant_amount(64, allocator.quota_limit, 37),
            Err(StorageError::QuotaExceeded { .. })
        ));
    }

    #[test]
    fn shard_adopts_a_durable_grant_after_allocator_crash_window() {
        let revision = OffsetDateTime::now_utc();
        let incarnation = Uuid::new_v4();
        let grant_id = Uuid::new_v4();
        let mut allocator = QuotaAllocatorLedger::new(incarnation, revision, 1024);
        allocator.grants.insert(
            grant_id,
            AllocatorGrant {
                shard_index: 3,
                amount: 128,
                initial_usage: 0,
            },
        );
        allocator.generation = 7;
        let mut shard = QuotaShardLedger::new(incarnation, revision, 3);
        shard.adopt_grants(&allocator).expect("shard should recover the issued grant");
        assert_eq!(shard.grants.get(&grant_id), Some(&128));
        assert_eq!(shard.allocator_generation, 7);

        let operation_id = Uuid::new_v4();
        let reservation = PersistedReservation {
            object: "object".to_string(),
            old_size: 0,
            new_size: 64,
            created_at: 0,
            pool_index: None,
            set_index: None,
            commit_started: false,
        };
        shard
            .reserve(operation_id, reservation.clone())
            .expect("credit should admit reservation");
        assert_eq!(shard.available_credit().expect("available credit"), 64);
        shard
            .abort(operation_id, &reservation)
            .expect("abort should release shard credit");
        assert_eq!(shard.available_credit().expect("available credit"), 128);
    }

    #[test]
    fn shard_bootstrap_usage_stays_partitioned_by_object_shard() {
        let revision = OffsetDateTime::now_utc();
        let incarnation = Uuid::new_v4();
        let mut allocator = QuotaAllocatorLedger::new(incarnation, revision, 100_000);
        allocator.grants.insert(
            Uuid::new_v4(),
            AllocatorGrant {
                shard_index: 1,
                amount: 60_000,
                initial_usage: 60_000,
            },
        );
        allocator.grants.insert(
            Uuid::new_v4(),
            AllocatorGrant {
                shard_index: 7,
                amount: 30_000,
                initial_usage: 30_000,
            },
        );

        let mut first = QuotaShardLedger::new(incarnation, revision, 1);
        first.adopt_grants(&allocator).expect("first shard should adopt its grant");
        let mut second = QuotaShardLedger::new(incarnation, revision, 7);
        second.adopt_grants(&allocator).expect("second shard should adopt its grant");

        assert_eq!(first.accounted_usage, 60_000);
        assert_eq!(second.accounted_usage, 30_000);
        assert_eq!(first.available_credit().expect("first shard credit"), 0);
        assert_eq!(second.available_credit().expect("second shard credit"), 0);
    }

    #[test]
    fn untouched_bootstrap_shard_remains_creatable_after_allocator_refill() {
        let revision = OffsetDateTime::now_utc();
        let incarnation = Uuid::new_v4();
        let mut allocator = QuotaAllocatorLedger::new(incarnation, revision, 100_000);
        allocator.generation = 2;
        allocator.grants.insert(
            Uuid::new_v4(),
            AllocatorGrant {
                shard_index: 7,
                amount: 100,
                initial_usage: 100,
            },
        );
        assert!(shard_may_be_created(&allocator, 7, false));

        allocator.grants.insert(
            Uuid::new_v4(),
            AllocatorGrant {
                shard_index: 7,
                amount: 100,
                initial_usage: 0,
            },
        );
        assert!(!shard_may_be_created(&allocator, 7, false));
        assert!(shard_may_be_created(&allocator, 1, false));
    }

    #[test]
    fn shard_rejects_a_grant_not_owned_by_the_allocator_epoch() {
        let revision = OffsetDateTime::now_utc();
        let incarnation = Uuid::new_v4();
        let mut allocator = QuotaAllocatorLedger::new(incarnation, revision, 1024);
        allocator.generation = 2;
        let mut shard = QuotaShardLedger::new(incarnation, revision, 1);
        shard.grants.insert(Uuid::new_v4(), 64);
        assert!(matches!(shard.adopt_grants(&allocator), Err(StorageError::PartMissingOrCorrupt)));
    }

    #[test]
    fn shard_commit_started_shrink_requires_exact_reconciliation() {
        let revision = OffsetDateTime::now_utc();
        let incarnation = Uuid::new_v4();
        let mut allocator = QuotaAllocatorLedger::new(incarnation, revision, 256);
        let grant_id = Uuid::new_v4();
        allocator.grants.insert(
            grant_id,
            AllocatorGrant {
                shard_index: 0,
                amount: 256,
                initial_usage: 128,
            },
        );
        let mut shard = QuotaShardLedger::new(incarnation, revision, 0);
        shard.adopt_grants(&allocator).expect("grant should be adopted");
        let operation_id = Uuid::new_v4();
        let reservation = PersistedReservation {
            object: "object".to_string(),
            old_size: 128,
            new_size: 1,
            created_at: 0,
            pool_index: None,
            set_index: None,
            commit_started: true,
        };
        shard.reservations.insert(operation_id, reservation.clone());
        shard.commit(operation_id, &reservation).expect("commit marker should settle");
        assert!(shard.reconcile_required);
    }
}
