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
use crate::object_api::{ObjectInfo, ObjectLockConfigSnapshot, ObjectOptions, QuotaAdmission};
use crate::set_disk::{SetDisks, get_lock_acquire_timeout};
use crate::storage_api_contracts::namespace::NamespaceLocking;
use crate::storage_api_contracts::{list::ListOperations as _, object::ObjectOperations};
use crate::store::ECStore;
use futures::{StreamExt, stream};
use rustfs_lock::NamespaceLockGuard;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet, hash_map::Entry};
use std::sync::{Arc, Mutex, OnceLock, PoisonError};
use std::time::Duration;
use time::OffsetDateTime;
use tokio::sync::oneshot;
use tokio::time::Instant;
use tracing::{Instrument, Span, warn};
use uuid::Uuid;

const QUOTA_LEDGER_FORMAT_VERSION: u8 = 1;
const RECONCILE_RETRY_SECONDS: i64 = 30;
const EVENT_QUOTA_RECONCILIATION: &str = "quota_reconciliation";
const MAX_ORPHANS_REAPED_PER_WRITE: usize = 64;
const MAX_ORPHAN_PROBES_PER_WRITE: usize = 128;
const ORPHAN_PROBE_CONCURRENCY: usize = 32;
const MAX_V1_LEDGER_BATCH: usize = 64;
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
    /// Changes on every durable write, including zero-growth reservations.
    /// Missing on an older writer's output, which invalidates a concurrent scan.
    #[serde(default)]
    writer_revision: Uuid,
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
            writer_revision: Uuid::nil(),
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
        let Some(reservation) = self.reservations.get(&operation_id) else {
            return Err(StorageError::PartMissingOrCorrupt);
        };
        if !reservation.matches_expected(expected) {
            return Err(StorageError::PartMissingOrCorrupt);
        }
        if reservation.new_size < reservation.old_size {
            self.reconcile_required = true;
        }
        self.reservations.remove(&operation_id);
        Ok(())
    }

    fn abort(&mut self, operation_id: Uuid, expected: &PersistedReservation) -> Result<()> {
        let Some(reservation) = self.reservations.get(&operation_id) else {
            return Ok(());
        };
        if !reservation.matches_expected(expected) {
            return Err(StorageError::PartMissingOrCorrupt);
        }
        self.accounted_usage = self
            .accounted_usage
            .checked_sub(reservation.growth())
            .ok_or(StorageError::PartMissingOrCorrupt)?;
        self.reservations.remove(&operation_id);
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

#[derive(Debug, Clone, Serialize, Deserialize)]
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
        // The commit owner and settlement retries may run in detached tasks.
        let diagnostic_span = if crate::set_disk::is_object_lock_diag_enabled() {
            Span::current()
        } else {
            Span::none()
        };
        let ledger_data = LedgerReservationData {
            store: Arc::clone(&store),
            bucket: self.bucket,
            ledger_object: self.ledger_object,
            operation_id,
            reservation: reservation.clone(),
            diagnostic_span: diagnostic_span.clone(),
            operation_guard: Arc::new(operation_guard),
        };
        let metadata_guard = self.metadata_guard;

        let admit = async move {
            let (ledger_guard, ledger) = lock_and_reap_v1_ledger(&store, &ledger_data.bucket, &ledger_data.ledger_object).await?;
            let ledger = current_v1_ledger(ledger, bucket_incarnation, quota_revision)?;
            let job = ReconcileJob::new(Arc::clone(&store), ledger_data.bucket.clone(), bucket_incarnation, quota_revision);
            let Some(mut ledger) = ledger else {
                // Bootstrap has no authoritative usage yet. Release every
                // request-owned fence by returning; the worker owns no request.
                job.schedule();
                return Err(StorageError::SlowDown);
            };
            let growth = reservation.growth();
            let current_usage = ledger.admitted_usage()?;
            let expected_usage = current_usage.checked_add(growth).ok_or(StorageError::PartMissingOrCorrupt)?;
            if ledger.reconcile_required
                || (growth > 0 && expected_usage > quota_limit && growth <= quota_limit && ledger.should_reconcile_after_denial())
            {
                job.schedule();
            }
            if growth > 0 && expected_usage > quota_limit {
                return Err(StorageError::QuotaExceeded {
                    current: current_usage,
                    limit: quota_limit,
                });
            }
            if ledger_data.operation_guard.is_lock_lost() || metadata_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
                return Err(StorageError::NamespaceLockQuorumUnavailable {
                    mode: "quota_reservation",
                    bucket: ledger_data.bucket.clone(),
                    object: ledger_data.ledger_object.clone(),
                    required: 1,
                    achieved: 0,
                });
            }
            ledger.reserve(operation_id, reservation)?;
            save_ledger_locked(Arc::clone(&store), &ledger_data.ledger_object, &mut ledger, &ledger_guard).await?;

            Ok(QuotaReservation {
                ledger: Some(ReservationLedgerData::Single(ledger_data)),
                shard_operation_guard: None,
                metadata_guard,
                state: ReservationState::Pending,
            })
        };
        tokio::spawn(admit.instrument(diagnostic_span))
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
    #[cfg(test)]
    tests::pause_after_allocator_ready(&data.bucket, &data.reservation.object).await;
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
        // Initialization released its lock; a concurrent refill may have added grants.
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
        let (mut shard, shard_created) = load_current_shard_locked(
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
                shard_operation_guard: Some(operation_guard),
                metadata_guard,
                state: ReservationState::Pending,
            });
        }
        let shard_was_empty = shard.grants.is_empty() && shard.reservations.is_empty() && shard.accounted_usage == 0;
        if shard_created || shard_was_empty {
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
        if !shard_was_empty
            && attempt < 2
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
    diagnostic_span: Span,
    operation_guard: Arc<NamespaceLockGuard>,
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
    fn bucket(&self) -> &str {
        match self {
            Self::Single(data) => &data.bucket,
            Self::Sharded(data) => &data.bucket,
        }
    }
}

pub(crate) struct QuotaReservation {
    ledger: Option<ReservationLedgerData>,
    shard_operation_guard: Option<NamespaceLockGuard>,
    metadata_guard: Option<Arc<NamespaceLockGuard>>,
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
            shard_operation_guard: None,
            metadata_guard,
            state: ReservationState::Pending,
        }
    }

    pub(crate) fn is_lock_lost(&self) -> bool {
        matches!(self.ledger.as_ref(), Some(ReservationLedgerData::Single(data)) if data.operation_guard.is_lock_lost())
            || self
                .shard_operation_guard
                .as_ref()
                .is_some_and(NamespaceLockGuard::is_lock_lost)
            || self.metadata_guard.as_ref().is_some_and(|guard| guard.is_lock_lost())
    }

    pub(crate) async fn mark_commit_started(&mut self) -> Result<()> {
        match self.ledger.as_ref() {
            Some(ReservationLedgerData::Single(data)) => {
                v1_ledger_update(data, V1LedgerUpdateKind::MarkCommitStarted, self.metadata_guard.as_ref()).await?;
            }
            Some(ReservationLedgerData::Sharded(data)) => mark_commit_started_sharded(data).await?,
            None => {}
        }
        self.state = ReservationState::CommitStarted;
        Ok(())
    }

    pub(crate) async fn commit(mut self) {
        self.state = ReservationState::Committed;
        let Some(ledger) = self.ledger.as_ref() else {
            return;
        };
        crate::store::list_objects::observe_scanner_namespace_mutations(ledger.bucket(), 1);
        match settle(ledger, true, self.metadata_guard.as_ref()).await {
            Ok(()) => self.ledger = None,
            Err(err) => log_deferred_settlement(ledger, "commit_deferred", &err),
        }
    }

    pub(crate) async fn abort(mut self) {
        let Some(ledger) = self.ledger.as_ref() else {
            return;
        };
        match settle(ledger, false, self.metadata_guard.as_ref()).await {
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
        let shard_operation_guard = self.shard_operation_guard.take();
        let metadata_guard = self.metadata_guard.take();
        let Ok(runtime) = tokio::runtime::Handle::try_current() else {
            return;
        };
        runtime.spawn(async move {
            let _shard_operation_guard = shard_operation_guard;
            if let Err(err) = settle(&ledger, committed, metadata_guard.as_ref()).await {
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
    // A lockless cache PUT is only taken when the bucket has no quota, so it
    // reserves nothing and needs no metadata-transaction lock.
    let lockless_cache = opts.is_lockless_cache_write() && snapshot_admission.is_none();
    if lockless_cache || crate::bucket::utils::is_meta_bucketname(bucket) {
        if !lockless_cache {
            let meta_bucket_fast_path_started = rustfs_io_metrics::put_stage_timer();
            rustfs_io_metrics::record_put_object_stage_duration_from(
                rustfs_io_metrics::PUT_STAGE_PUT_OBJECT_QUOTA_BEGIN_META_BUCKET_FAST_PATH,
                meta_bucket_fast_path_started,
            );
        }
        return Ok(QuotaContext {
            store: None,
            bucket: bucket.to_string(),
            object: object.to_string(),
            ledger_object: ledger_object(bucket),
            bucket_incarnation: None,
            quota_revision: None,
            quota_limit: None,
            reservation_protocol: None,
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
            snapshot_admission: None,
            legacy_data_movement: false,
            metadata_guard: None,
            pool_index: None,
            set_index: None,
        });
    }

    let metadata_lock_started = rustfs_io_metrics::put_stage_timer();
    let metadata_guard = metadata_sys::acquire_bucket_metadata_transaction_read_lock_for_options_in(
        ctx,
        bucket,
        opts,
        "quota_context_metadata_read",
    )
    .await?;
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_PUT_OBJECT_QUOTA_BEGIN_METADATA_LOCK,
        metadata_lock_started,
    );
    let cache_lookup_started = rustfs_io_metrics::put_stage_timer();
    let cached_quota_snapshot = if rustfs_io_metrics::put_stage_metrics_enabled() {
        match metadata_sys::get_cached_quota_config_and_incarnation_in(ctx, bucket).await {
            Ok(Some(snapshot)) => {
                rustfs_io_metrics::record_put_object_quota_cache_decision(
                    "candidate",
                    quota_cache_reason(snapshot.quota.as_ref()),
                );
                Some(snapshot)
            }
            Ok(None) => {
                rustfs_io_metrics::record_put_object_quota_cache_decision("miss", "no_authoritative_cache");
                None
            }
            Err(_) => {
                rustfs_io_metrics::record_put_object_quota_cache_decision("miss", "invalid_cached_quota");
                None
            }
        }
    } else {
        None
    };
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_PUT_OBJECT_QUOTA_BEGIN_CACHE_LOOKUP,
        cache_lookup_started,
    );
    let config_read_started = rustfs_io_metrics::put_stage_timer();
    let (quota, bucket_incarnation, quota_revision) =
        metadata_sys::get_quota_config_and_incarnation_from_disk_for_options_in(ctx, bucket, opts, &metadata_guard).await?;
    if let Some(cached) = cached_quota_snapshot.as_ref() {
        let reason = quota_cache_reason(quota.as_ref());
        if cached.quota == quota && cached.bucket_incarnation == bucket_incarnation && cached.quota_revision == quota_revision {
            rustfs_io_metrics::record_put_object_quota_cache_decision("authoritative_match", reason);
        } else {
            rustfs_io_metrics::record_put_object_quota_cache_decision("authoritative_mismatch", reason);
            if cached.quota != quota {
                rustfs_io_metrics::record_put_object_quota_cache_decision("authoritative_mismatch_field", "quota");
            }
            if cached.bucket_incarnation != bucket_incarnation {
                rustfs_io_metrics::record_put_object_quota_cache_decision("authoritative_mismatch_field", "bucket_incarnation");
            }
            if cached.quota_revision != quota_revision {
                rustfs_io_metrics::record_put_object_quota_cache_decision("authoritative_mismatch_field", "quota_revision");
            }
        }
    }
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_PUT_OBJECT_QUOTA_BEGIN_CONFIG_READ,
        config_read_started,
    );
    let lock_lost_check_started = rustfs_io_metrics::put_stage_timer();
    if metadata_guard.is_lock_lost() {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "quota_config",
            bucket: bucket.to_string(),
            object: ledger_object(bucket),
            required: 1,
            achieved: 0,
        });
    }
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_PUT_OBJECT_QUOTA_BEGIN_LOCK_LOST_CHECK,
        lock_lost_check_started,
    );
    let policy_evaluate_started = rustfs_io_metrics::put_stage_timer();
    if quota
        .as_ref()
        .is_some_and(|quota| quota.has_unsupported_reservation_protocol())
    {
        log_admission_rejected(bucket, object, "unsupported_reservation_protocol");
        return Err(StorageError::PartMissingOrCorrupt);
    }
    let durable_quota = quota.as_ref().filter(|quota| quota.uses_durable_reservations());
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_PUT_OBJECT_QUOTA_BEGIN_POLICY_EVALUATE,
        policy_evaluate_started,
    );
    // Durable writes are fenced by a write quorum of participating disks that
    // enforce the configured topology. Fleet-wide proof remains a configuration gate;
    // losing an unrelated peer must not revoke an admitted object write.
    let durable_quota_limit = durable_quota.and_then(|quota| quota.quota);
    let snapshot_admission_started = rustfs_io_metrics::put_stage_timer();
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
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_PUT_OBJECT_QUOTA_BEGIN_SNAPSHOT_ADMISSION,
        snapshot_admission_started,
    );
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
    let object_store_lookup_started = rustfs_io_metrics::put_stage_timer();
    let store = if durable_quota_limit.is_some() {
        Some(metadata_sys::object_store_in(ctx).await?)
    } else {
        None
    };
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_PUT_OBJECT_QUOTA_BEGIN_OBJECT_STORE_LOOKUP,
        object_store_lookup_started,
    );
    let context_build_started = rustfs_io_metrics::put_stage_timer();
    rustfs_io_metrics::record_put_object_stage_duration_from(
        rustfs_io_metrics::PUT_STAGE_PUT_OBJECT_QUOTA_BEGIN_CONTEXT_BUILD,
        context_build_started,
    );
    Ok(QuotaContext {
        store,
        bucket: bucket.to_string(),
        object: object.to_string(),
        ledger_object: ledger_object(bucket),
        bucket_incarnation: Some(bucket_incarnation),
        quota_revision: Some(quota_revision),
        quota_limit,
        reservation_protocol,
        snapshot_admission,
        legacy_data_movement,
        metadata_guard: Some(metadata_guard),
        pool_index: Some(pool_index),
        set_index: Some(set_index),
    })
}

fn quota_cache_reason(quota: Option<&crate::bucket::quota::BucketQuota>) -> &'static str {
    match quota {
        None => "no_quota",
        Some(quota) if quota.uses_durable_reservations() => "durable_quota",
        Some(quota) if quota.quota.is_some() => "snapshot_quota",
        Some(_) => "quota_without_limit",
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

async fn settle(ledger: &ReservationLedgerData, committed: bool, metadata_guard: Option<&Arc<NamespaceLockGuard>>) -> Result<()> {
    match ledger {
        ReservationLedgerData::Sharded(data) => settle_sharded(data, committed).await,
        ReservationLedgerData::Single(data) => {
            // Settlement must never revoke the object's tokens: a deferred
            // retry may run after a later writer has acquired its own tokens.
            v1_ledger_update(data, V1LedgerUpdateKind::Settle { committed }, metadata_guard).await
        }
    }
}

#[derive(Clone, Copy)]
enum V1LedgerUpdateKind {
    MarkCommitStarted,
    Settle { committed: bool },
}

impl V1LedgerUpdateKind {
    fn diagnostic_operation(self) -> &'static str {
        match self {
            Self::MarkCommitStarted => "quota_ledger_mark_commit_started",
            Self::Settle { .. } => "quota_ledger_settlement",
        }
    }
}

struct V1LedgerUpdate {
    kind: V1LedgerUpdateKind,
    operation_id: Uuid,
    reservation: PersistedReservation,
    _operation_guard: Arc<NamespaceLockGuard>,
    _metadata_guard: Option<Arc<NamespaceLockGuard>>,
    diagnostic_span: Span,
    submitted_at: Instant,
    timeout: Duration,
    reply: oneshot::Sender<Result<()>>,
}

impl V1LedgerUpdate {
    async fn lock(&self, store: &Arc<ECStore>, object: &str) -> Result<Arc<NamespaceLockGuard>> {
        let remaining = self.timeout.saturating_sub(self.submitted_at.elapsed());
        if remaining.is_zero() && !self.timeout.is_zero() {
            let key = rustfs_lock::ObjectKey::new(RUSTFS_META_BUCKET, object);
            return Err(rustfs_lock::error::LockError::timeout(key.to_string(), self.timeout).into());
        }
        lock_v1_ledger(store, object, self.kind.diagnostic_operation(), remaining)
            .instrument(self.diagnostic_span.clone())
            .await
    }

    fn apply(&self, ledger: &mut QuotaLedger) -> Result<()> {
        match self.kind {
            V1LedgerUpdateKind::MarkCommitStarted => ledger.mark_commit_started(self.operation_id, &self.reservation),
            V1LedgerUpdateKind::Settle { committed: true } => ledger.commit(self.operation_id, &self.reservation),
            V1LedgerUpdateKind::Settle { committed: false } => ledger.abort(self.operation_id, &self.reservation),
        }
    }
}

type V1LedgerKey = (Uuid, String);
type V1LedgerLanes = HashMap<V1LedgerKey, Vec<V1LedgerUpdate>>;

fn v1_ledger_lanes() -> std::sync::MutexGuard<'static, V1LedgerLanes> {
    static LANES: OnceLock<Mutex<V1LedgerLanes>> = OnceLock::new();
    LANES
        .get_or_init(Mutex::default)
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
}

struct V1LedgerLane(Option<V1LedgerKey>);

impl V1LedgerLane {
    fn drain(mut self) -> Vec<V1LedgerUpdate> {
        self.0
            .take()
            .and_then(|key| v1_ledger_lanes().remove(&key))
            .unwrap_or_default()
    }
}

impl Drop for V1LedgerLane {
    fn drop(&mut self) {
        if let Some(key) = self.0.as_ref() {
            let _pending = v1_ledger_lanes().remove(key);
        }
    }
}

async fn v1_ledger_update(
    data: &LedgerReservationData,
    kind: V1LedgerUpdateKind,
    metadata_guard: Option<&Arc<NamespaceLockGuard>>,
) -> Result<()> {
    enqueue_v1_ledger_update(data, kind, metadata_guard, get_lock_acquire_timeout())
        .await
        .map_err(|err| StorageError::other_with_context("quota ledger update task failed", err))?
}

fn enqueue_v1_ledger_update(
    data: &LedgerReservationData,
    kind: V1LedgerUpdateKind,
    metadata_guard: Option<&Arc<NamespaceLockGuard>>,
    timeout: Duration,
) -> oneshot::Receiver<Result<()>> {
    let (reply, receiver) = oneshot::channel();
    let mut lanes = v1_ledger_lanes();
    let update = V1LedgerUpdate {
        kind,
        operation_id: data.operation_id,
        reservation: data.reservation.clone(),
        // The waiter can be cancelled while the detached batch is in flight.
        _operation_guard: Arc::clone(&data.operation_guard),
        _metadata_guard: metadata_guard.cloned(),
        diagnostic_span: data.diagnostic_span.clone(),
        submitted_at: Instant::now(),
        timeout,
        reply,
    };
    let lane = match lanes.entry((data.store.id, data.ledger_object.clone())) {
        Entry::Occupied(mut lane) if lane.get().len() < MAX_V1_LEDGER_BATCH - 1 => {
            lane.get_mut().push(update);
            return receiver;
        }
        Entry::Occupied(_) => None, // A full batch leaves excess requests on the same direct lock path.
        Entry::Vacant(lane) => {
            let key = lane.key().clone();
            lane.insert(Vec::new());
            Some(V1LedgerLane(Some(key)))
        }
    };
    drop(lanes);
    tokio::spawn(flush_v1_ledger(Arc::clone(&data.store), data.ledger_object.clone(), update, lane));
    receiver
}

async fn flush_v1_ledger(store: Arc<ECStore>, object: String, first: V1LedgerUpdate, lane: Option<V1LedgerLane>) {
    // Join only operations already waiting for the same ledger. Never delay
    // a lock holder to wait for followers, or acquire operation locks here.
    let mut leader = first;
    let mut acquired = leader.lock(&store, &object).await;
    let mut followers = lane.map_or_else(Vec::new, V1LedgerLane::drain).into_iter();
    let guard = loop {
        match acquired {
            Err(err @ StorageError::Lock(rustfs_lock::error::LockError::Timeout { .. })) => {
                // A follower must not inherit an earlier writer's deadline.
                let _ = leader.reply.send(Err(err));
                let Some(next) = followers.next() else {
                    return;
                };
                leader = next;
                acquired = leader.lock(&store, &object).await;
            }
            other => break other,
        }
    };
    let authority = async {
        let guard = guard?;
        let ledger = load_ledger_locked(Arc::clone(&store), &object).await?;
        Ok::<_, StorageError>((guard, ledger))
    }
    .await;
    let (guard, mut ledger) = match authority {
        Ok(authority) => authority,
        Err(err) => {
            for update in std::iter::once(leader).chain(followers) {
                let _ = update.reply.send(Err(err.clone()));
            }
            return;
        }
    };

    let first_result = leader.apply(&mut ledger);
    let outcomes: Vec<_> = followers.as_slice().iter().map(|update| update.apply(&mut ledger)).collect();
    let saved = if first_result.is_ok() || outcomes.iter().any(Result::is_ok) {
        save_ledger_locked(store, &object, &mut ledger, &guard)
            .instrument(leader.diagnostic_span.clone())
            .await
    } else {
        Ok(())
    };
    drop(guard);
    let _ = leader.reply.send(first_result.and(saved.clone()));
    for (update, result) in followers.zip(outcomes) {
        let _ = update.reply.send(result.and(saved.clone()));
    }
}

fn current_v1_ledger(
    ledger: Option<QuotaLedger>,
    bucket_incarnation: Uuid,
    quota_revision: OffsetDateTime,
) -> Result<Option<QuotaLedger>> {
    match ledger {
        Some(ledger) if ledger.matches(bucket_incarnation, quota_revision) => Ok(Some(ledger)),
        Some(ledger) if !ledger.reservations.is_empty() => Err(StorageError::PartMissingOrCorrupt),
        _ => Ok(None),
    }
}

/// Remove the single-ledger reservation state for an incarnation after its
/// durable bucket retirement has been published. The caller holds the bucket
/// lifecycle and metadata transaction write fences, so no live reservation
/// owner can outlast this cleanup. Keep the ledger namespace lock as the local
/// serialization boundary and delete only the exact V1 object key.
pub(crate) async fn cleanup_retired_bucket_ledger(store: Arc<ECStore>, bucket: &str, incarnation: Uuid) -> Result<()> {
    let object = ledger_object(bucket);
    let ledger_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &object).await?;
    let guard = ledger_lock.get_write_lock(get_lock_acquire_timeout()).await?;
    let ledger = match load_ledger_locked(Arc::clone(&store), &object).await {
        Ok(ledger) if ledger.bucket_incarnation == incarnation => Some(ledger.bucket_incarnation),
        Ok(ledger) if crate::bucket::retirement::is_retired(store.clone(), bucket, ledger.bucket_incarnation).await? => {
            Some(ledger.bucket_incarnation)
        }
        Ok(_) => return Err(StorageError::PreconditionFailed),
        Err(StorageError::ConfigNotFound) => None,
        Err(error) => return Err(error),
    };
    // The reconciliation throttle object of the retiring incarnation goes first,
    // so a crash after the ledger delete cannot leave it behind; the requested
    // incarnation is cleaned even when its ledger is already gone.
    for retired in [Some(incarnation), ledger.filter(|retired| *retired != incarnation)]
        .into_iter()
        .flatten()
    {
        let control = reconcile_control_object(&object, retired);
        match store
            .delete_object(
                RUSTFS_META_BUCKET,
                &control,
                ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(_) => {}
            Err(error) if is_err_object_not_found(&error) => {}
            Err(error) => return Err(error),
        }
    }
    if ledger.is_none() {
        return Ok(());
    }
    if guard.is_lock_lost() {
        return Err(StorageError::NamespaceLockQuorumUnavailable {
            mode: "quota_ledger_retirement",
            bucket: RUSTFS_META_BUCKET.to_string(),
            object,
            required: 1,
            achieved: 0,
        });
    }
    let mut options = ObjectOptions {
        no_lock: true,
        ..Default::default()
    };
    options.add_namespace_lock_guard(&guard);
    match store.delete_object(RUSTFS_META_BUCKET, &object, options).await {
        Ok(_) => Ok(()),
        Err(error) if is_err_object_not_found(&error) => Ok(()),
        Err(error) => Err(error),
    }
}

/// The reconciliation throttle object of one bucket incarnation. One object per
/// incarnation: a quota revision change replaces the throttle instead of
/// leaving the previous revision's object behind.
fn reconcile_control_object(ledger_object: &str, incarnation: Uuid) -> String {
    format!("{ledger_object}.reconcile/{incarnation}")
}

async fn lock_v1_ledger(
    store: &Arc<ECStore>,
    ledger_object: &str,
    op: &'static str,
    timeout: Duration,
) -> Result<Arc<NamespaceLockGuard>> {
    let lock = store.new_ns_lock(RUSTFS_META_BUCKET, ledger_object).await?;
    let mut attempt = crate::diagnostics::object_lock::ObjectLockAttempt::start(
        op,
        RUSTFS_META_BUCKET,
        ledger_object,
        None,
        &lock,
        "write",
        timeout,
    );
    let guard = lock.get_write_lock(timeout).await;
    attempt.observe_acquisition(&guard);
    let guard = Arc::new(guard?);
    fence_namespace_mutations(store, RUSTFS_META_BUCKET, ledger_object, None).await?;
    Ok(guard)
}

async fn load_v1_ledger_if_present(store: &Arc<ECStore>, ledger_object: &str) -> Result<Option<QuotaLedger>> {
    match load_ledger_locked(Arc::clone(store), ledger_object).await {
        Ok(ledger) => Ok(Some(ledger)),
        Err(StorageError::ConfigNotFound) => Ok(None),
        Err(err) => Err(err),
    }
}

async fn lock_and_reap_v1_ledger(
    store: &Arc<ECStore>,
    bucket: &str,
    ledger_object: &str,
) -> Result<(Arc<NamespaceLockGuard>, Option<QuotaLedger>)> {
    let ledger_guard = lock_v1_ledger(store, ledger_object, "quota_ledger_admission", get_lock_acquire_timeout()).await?;
    let Some(ledger) = load_v1_ledger_if_present(store, ledger_object).await? else {
        return Ok((ledger_guard, None));
    };
    let (candidates, next_cursor) = ledger.reap_candidates(now_unix());
    if candidates.is_empty() {
        return Ok((ledger_guard, Some(ledger)));
    }

    // Probe operation owners without the ledger lock. Admission and
    // settlement take the operation lock before the ledger lock.
    let epoch = (ledger.bucket_incarnation, ledger.quota_revision_unix_nanos);
    drop(ledger);
    drop(ledger_guard);

    let probe_results = stream::iter(candidates)
        .map(|operation_id| async move {
            let lock_object = operation_lock_object(ledger_object, operation_id);
            let operation_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &lock_object).await?;
            Ok::<_, StorageError>(
                operation_lock
                    .get_write_lock_quiet(Duration::from_millis(50))
                    .await
                    .ok()
                    .map(|guard| (operation_id, guard)),
            )
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

    let ledger_guard = lock_v1_ledger(store, ledger_object, "quota_ledger_reap", get_lock_acquire_timeout()).await?;
    let Some(mut ledger) = load_v1_ledger_if_present(store, ledger_object).await? else {
        return Ok((ledger_guard, None));
    };
    if epoch != (ledger.bucket_incarnation, ledger.quota_revision_unix_nanos) {
        return Ok((ledger_guard, Some(ledger)));
    }
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
        fence_namespace_mutations(store, bucket, &object, target).await?;
    }
    let removed = remove_orphan_reservations(&mut ledger, &orphan_ids)?;
    if removed || cursor_changed {
        // Persist recovery even when admission of the incoming write fails.
        save_ledger_locked(Arc::clone(store), ledger_object, &mut ledger, &ledger_guard).await?;
    }
    Ok((ledger_guard, Some(ledger)))
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

#[derive(Debug, Serialize, Deserialize)]
struct ReconcileControl {
    bucket_incarnation: Uuid,
    quota_revision_unix_nanos: i128,
    next_attempt_at: i64,
}

#[derive(Clone)]
struct ReconcileJob {
    store: Arc<ECStore>,
    bucket: String,
    bucket_incarnation: Uuid,
    quota_revision: OffsetDateTime,
}

type ReconcileKey = (Uuid, String, Uuid, i128);

fn active_reconciliations() -> &'static std::sync::Mutex<HashSet<ReconcileKey>> {
    static ACTIVE: std::sync::OnceLock<std::sync::Mutex<HashSet<ReconcileKey>>> = std::sync::OnceLock::new();
    ACTIVE.get_or_init(Default::default)
}

struct ActiveReconciliation(ReconcileKey);

impl Drop for ActiveReconciliation {
    fn drop(&mut self) {
        active_reconciliations()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&self.0);
    }
}

impl ReconcileJob {
    fn new(store: Arc<ECStore>, bucket: String, bucket_incarnation: Uuid, quota_revision: OffsetDateTime) -> Self {
        Self {
            store,
            bucket,
            bucket_incarnation,
            quota_revision,
        }
    }

    fn schedule(self) {
        let key = (
            self.store.id,
            self.bucket.clone(),
            self.bucket_incarnation,
            self.quota_revision.unix_timestamp_nanos(),
        );
        if !active_reconciliations()
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(key.clone())
        {
            return;
        }
        let owner = ActiveReconciliation(key);
        // The owner carries no object, metadata, lifecycle or reservation guard.
        // Cancellation of the rejected request cannot cancel its reconciliation.
        tokio::spawn(async move {
            let _owner = owner;
            if let Err(error) = self.run().await {
                warn!(event = EVENT_QUOTA_RECONCILIATION, component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_QUOTA, state = "deferred", bucket = %self.bucket,
                    error = %error, "Quota reconciliation deferred");
            }
        });
    }

    fn control_object(&self) -> String {
        reconcile_control_object(&ledger_object(&self.bucket), self.bucket_incarnation)
    }

    async fn save_control(&self, object: &str, control: &ReconcileControl, guard: &Arc<NamespaceLockGuard>) -> Result<()> {
        if guard.is_lock_lost() {
            return Err(StorageError::SlowDown);
        }
        let mut opts = ObjectOptions {
            max_parity: true,
            no_lock: true,
            ..Default::default()
        };
        let _ = opts.set_quota_admission(0, u64::MAX);
        opts.add_owned_write_lock(Arc::clone(guard), RUSTFS_META_BUCKET, object);
        opts.write_completion = crate::object_api::WriteCompletion::TailDrained;
        save_config_with_opts(Arc::clone(&self.store), object, serde_json::to_vec(control)?, &opts).await
    }

    async fn run(&self) -> Result<()> {
        let control_object = self.control_object();
        let lock = self.store.new_ns_lock(RUSTFS_META_BUCKET, &control_object).await?;
        let Ok(guard) = lock.get_write_lock_quiet(Duration::from_millis(50)).await else {
            return Ok(());
        };
        let guard = Arc::new(guard);
        let now = now_unix();
        match read_config_no_lock(Arc::clone(&self.store), &control_object).await {
            Ok(bytes) => {
                let control: ReconcileControl = serde_json::from_slice(&bytes)?;
                if control.bucket_incarnation != self.bucket_incarnation {
                    return Err(StorageError::PartMissingOrCorrupt);
                }
                // A previous quota revision's throttle does not apply: the new
                // revision needs its own scan and overwrites the object below.
                if control.quota_revision_unix_nanos == self.quota_revision.unix_timestamp_nanos()
                    && control.next_attempt_at > now
                {
                    return Ok(());
                }
            }
            Err(StorageError::ConfigNotFound) => {}
            Err(error) => return Err(error),
        }
        fence_namespace_mutations(&self.store, RUSTFS_META_BUCKET, &control_object, None).await?;
        let mut control = ReconcileControl {
            bucket_incarnation: self.bucket_incarnation,
            quota_revision_unix_nanos: self.quota_revision.unix_timestamp_nanos(),
            next_attempt_at: now.saturating_add(RECONCILE_RETRY_SECONDS),
        };
        // Persist the throttle before work, including bootstrap and failed scans.
        self.save_control(&control_object, &control, &guard).await?;
        let result = self.scan_and_publish(&guard).await;
        // The throttle outlives a successful scan on purpose: denied requests
        // keep scheduling the job, and they must not rescan a full bucket more
        // than once per interval. A scan refused because the bucket generation
        // or its quota no longer matches writes nothing back: the retirement
        // cleanup may already have removed this object.
        if !matches!(result, Err(StorageError::PreconditionFailed)) {
            control.next_attempt_at = now_unix().saturating_add(RECONCILE_RETRY_SECONDS);
            self.save_control(&control_object, &control, &guard).await?;
        }
        result
    }

    /// Take a short authoritative configuration snapshot without recursively
    /// acquiring a reader behind a queued metadata/lifecycle writer.
    async fn configuration_snapshot(&self) -> Result<Arc<ObjectLockConfigSnapshot>> {
        let snapshot = self.store.object_lock_config_snapshot_for_single_put(&self.bucket).await?;
        let guard = snapshot
            .metadata_transaction_guard_for(self.store.id, &self.bucket, Some(self.bucket_incarnation))
            .ok_or(StorageError::PreconditionFailed)?;
        let opts = ObjectOptions {
            expected_bucket_incarnation_id: Some(self.bucket_incarnation),
            object_lock_config_snapshot: Some(Arc::clone(&snapshot)),
            ..Default::default()
        };
        let (quota, incarnation, revision) =
            metadata_sys::get_quota_config_and_incarnation_from_disk_for_options_in(&self.store.ctx, &self.bucket, &opts, &guard)
                .await?;
        if incarnation != self.bucket_incarnation
            || revision != self.quota_revision
            || !quota.as_ref().is_some_and(|quota| {
                quota.uses_durable_reservations() && quota.reservation_protocol != Some(QUOTA_RESERVATION_PROTOCOL_V2)
            })
        {
            return Err(StorageError::PreconditionFailed);
        }
        Ok(snapshot)
    }

    async fn scan_and_publish(&self, owner: &NamespaceLockGuard) -> Result<()> {
        let ledger_object = ledger_object(&self.bucket);
        let expected_revision = {
            // Lock order: reconcile owner, lifecycle, metadata, ledger.
            // Request paths never wait for the reconcile owner.
            let _configuration = self.configuration_snapshot().await?;
            let lock = self.store.new_ns_lock(RUSTFS_META_BUCKET, &ledger_object).await?;
            let guard = Arc::new(lock.get_write_lock(get_lock_acquire_timeout()).await?);
            match load_ledger_locked(Arc::clone(&self.store), &ledger_object).await {
                Ok(mut ledger) => {
                    if !ledger.reservations.is_empty() {
                        return Ok(());
                    }
                    // Seed a non-nil token even for a legacy ledger. An old
                    // node drops this field on write and cannot pass the CAS.
                    fence_namespace_mutations(&self.store, RUSTFS_META_BUCKET, &ledger_object, None).await?;
                    save_ledger_locked(Arc::clone(&self.store), &ledger_object, &mut ledger, &guard).await?;
                    Some(ledger.writer_revision)
                }
                Err(StorageError::ConfigNotFound) => None,
                Err(error) => return Err(error),
            }
        };
        #[cfg(test)]
        tests::pause_reconciliation(&self.bucket, tests::ReconcilePauseStage::BeforeScan).await?;
        // Only the independent reconcile owner is held during this whole-bucket
        // walk. Concurrent reserve/settle/reap writes invalidate its result.
        let usage = exact_bucket_usage(&self.store, &self.bucket).await?;
        #[cfg(test)]
        tests::pause_reconciliation(&self.bucket, tests::ReconcilePauseStage::BeforePublish).await?;
        let configuration = self.configuration_snapshot().await?;
        let lock = self.store.new_ns_lock(RUSTFS_META_BUCKET, &ledger_object).await?;
        let guard = Arc::new(lock.get_write_lock(get_lock_acquire_timeout()).await?);
        let unchanged = match load_ledger_locked(Arc::clone(&self.store), &ledger_object).await {
            Ok(ledger) => {
                expected_revision == Some(ledger.writer_revision)
                    && !ledger.writer_revision.is_nil()
                    && ledger.reservations.is_empty()
            }
            Err(StorageError::ConfigNotFound) => expected_revision.is_none(),
            Err(error) => return Err(error),
        };
        if !unchanged {
            return Ok(());
        }
        if owner.is_lock_lost()
            || !configuration.is_valid_for_destructive_put(self.store.id, &self.bucket, self.bucket_incarnation)
        {
            return Err(StorageError::SlowDown);
        }
        fence_namespace_mutations(&self.store, RUSTFS_META_BUCKET, &ledger_object, None).await?;
        let mut ledger = QuotaLedger::new(self.bucket_incarnation, self.quota_revision, usage);
        save_ledger_locked(Arc::clone(&self.store), &ledger_object, &mut ledger, &guard).await
    }
}

async fn exact_bucket_usage(store: &Arc<ECStore>, bucket: &str) -> Result<u64> {
    crate::store::list_objects::observe_scanner_namespace_mutations(bucket, 1);
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
    let object = rustfs_utils::path::encode_dir_object_ref(object);
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
        Ok(allocator) if allocator.matches(data.bucket_incarnation, data.quota_revision, quota_limit) => {
            return Ok(false);
        }
        Ok(_) | Err(StorageError::ConfigNotFound) => {}
        Err(err) => return Err(err),
    }
    drop(allocator_guard);

    let allocator_lock = store.new_ns_lock(RUSTFS_META_BUCKET, &data.allocator_object).await?;
    let allocator_guard = Arc::new(allocator_lock.get_write_lock(get_lock_acquire_timeout()).await?);
    match load_allocator_locked(Arc::clone(&store), &data.allocator_object).await {
        Ok(allocator) if allocator.matches(data.bucket_incarnation, data.quota_revision, quota_limit) => {
            return Ok(false);
        }
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
) -> Result<(QuotaShardLedger, bool)> {
    let (mut shard, created) = match load_shard_locked(Arc::clone(&store), object).await {
        Ok(shard) if shard.matches(bucket_incarnation, quota_revision, shard_index) => (shard, false),
        Ok(_) => return Err(StorageError::PartMissingOrCorrupt),
        Err(StorageError::ConfigNotFound) if allow_create => {
            (QuotaShardLedger::new(bucket_incarnation, quota_revision, shard_index), true)
        }
        Err(StorageError::ConfigNotFound) => return Err(StorageError::PartMissingOrCorrupt),
        Err(err) => return Err(err),
    };
    shard.adopt_grants(allocator)?;
    Ok((shard, created))
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
    let (shard, _) = load_current_shard_locked(
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
    let (mut shard, _) = load_current_shard_locked(
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
        let (mut shard, _) = load_current_shard_locked(
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
        let (mut shard, _) = load_current_shard_locked(
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
    ledger: &mut QuotaLedger,
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
    ledger.writer_revision = Uuid::new_v4();
    save_config_with_opts(store, ledger_object, serde_json::to_vec(ledger)?, &opts).await
}

/// Advance the persisted reconciliation retry deadline for deterministic fixtures.
/// The production reaper, ownership fence, scan and compare-and-swap still run.
#[cfg(any(test, feature = "test-util"))]
pub async fn retry_quota_reconciliation_for_test(store: Arc<ECStore>, bucket: &str) -> Result<()> {
    let (_, incarnation, revision) = store.bucket_quota_and_incarnation_from_disk(bucket).await?;
    let job = ReconcileJob::new(Arc::clone(&store), bucket.to_owned(), incarnation, revision);
    let key = (store.id, bucket.to_owned(), incarnation, revision.unix_timestamp_nanos());
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let active = active_reconciliations()
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains(&key);
            if !active {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .map_err(|_| StorageError::SlowDown)?;
    drop(lock_and_reap_v1_ledger(&store, bucket, &ledger_object(bucket)).await?);
    let object = job.control_object();
    {
        let lock = store.new_ns_lock(RUSTFS_META_BUCKET, &object).await?;
        let guard = Arc::new(lock.get_write_lock(get_lock_acquire_timeout()).await?);
        match read_config_no_lock(Arc::clone(&store), &object).await {
            Ok(bytes) => {
                let mut control: ReconcileControl = serde_json::from_slice(&bytes)?;
                control.next_attempt_at = 0;
                fence_namespace_mutations(&store, RUSTFS_META_BUCKET, &object, None).await?;
                job.save_control(&object, &control, &guard).await?;
            }
            Err(StorageError::ConfigNotFound) => {}
            Err(error) => return Err(error),
        }
    }
    job.run().await
}

/// Bootstrap a test bucket through the real background reconciliation path.
/// Fixtures call this after enabling V1 quota, before issuing writes whose
/// contract is independent of the initial retryable admission response.
#[cfg(any(test, feature = "test-util"))]
pub async fn initialize_quota_ledger_for_test(store: Arc<ECStore>, bucket: &str) -> Result<()> {
    let (quota, incarnation, revision) = store.bucket_quota_and_incarnation_from_disk(bucket).await?;
    if !quota
        .as_ref()
        .is_some_and(|quota| quota.uses_durable_reservations() && !quota.uses_sharded_reservations())
    {
        return Ok(());
    }
    ReconcileJob::new(Arc::clone(&store), bucket.to_owned(), incarnation, revision)
        .run()
        .await?;
    match current_v1_ledger(load_v1_ledger_if_present(&store, &ledger_object(bucket)).await?, incarnation, revision)? {
        Some(_) => Ok(()),
        None => Err(StorageError::SlowDown),
    }
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
    #[test]
    fn quota_shard_agrees_with_directory_listing_name() {
        for listed in ["photos/", "reports/june/"] {
            assert_eq!(shard_index(listed), shard_index(&rustfs_utils::path::encode_dir_object(listed)));
        }
    }
    use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
    use serial_test::serial;

    #[derive(Clone, Copy, PartialEq, Eq)]
    pub(super) enum ReconcilePauseStage {
        BeforeScan,
        BeforePublish,
    }

    struct ReconcilePause {
        bucket: String,
        stage: ReconcilePauseStage,
        armed: std::sync::atomic::AtomicBool,
        scans: std::sync::atomic::AtomicUsize,
        fail_scan: std::sync::atomic::AtomicBool,
        reached: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    static RECONCILE_PAUSE: std::sync::Mutex<Option<Arc<ReconcilePause>>> = std::sync::Mutex::new(None);

    struct ReconcilePauseGuard(Arc<ReconcilePause>);

    impl ReconcilePauseGuard {
        fn install(bucket: &str, stage: ReconcilePauseStage) -> Self {
            let pause = Arc::new(ReconcilePause {
                bucket: bucket.to_owned(),
                stage,
                armed: true.into(),
                scans: 0.into(),
                fail_scan: false.into(),
                reached: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            });
            *RECONCILE_PAUSE.lock().expect("reconcile hooks") = Some(Arc::clone(&pause));
            Self(pause)
        }

        async fn reached(&self) {
            tokio::time::timeout(Duration::from_secs(10), self.0.reached.notified())
                .await
                .expect("reconciliation reached barrier");
        }
    }

    impl Drop for ReconcilePauseGuard {
        fn drop(&mut self) {
            self.0.release.notify_one();
            let mut hook = RECONCILE_PAUSE.lock().expect("reconcile hooks");
            if hook.as_ref().is_some_and(|hook| Arc::ptr_eq(hook, &self.0)) {
                *hook = None;
            }
        }
    }

    pub(super) async fn pause_reconciliation(bucket: &str, stage: ReconcilePauseStage) -> Result<()> {
        use std::sync::atomic::Ordering;
        let hook = RECONCILE_PAUSE
            .lock()
            .expect("reconcile hooks")
            .as_ref()
            .filter(|hook| hook.bucket == bucket)
            .cloned();
        if let Some(hook) = hook {
            if stage == ReconcilePauseStage::BeforeScan {
                hook.scans.fetch_add(1, Ordering::SeqCst);
            }
            if stage == hook.stage && hook.armed.swap(false, Ordering::SeqCst) {
                hook.reached.notify_one();
                hook.release.notified().await;
            }
            if stage == ReconcilePauseStage::BeforeScan && hook.fail_scan.load(Ordering::SeqCst) {
                return Err(StorageError::other("injected quota usage failure"));
            }
        }
        Ok(())
    }

    async fn reconciliation_fixture(limit: u64) -> (Vec<tempfile::TempDir>, ReconcileJob) {
        use crate::object_api::PutObjReader;
        use crate::storage_api_contracts::object::ObjectIO;
        let (directories, store) = metadata_sys::test_support::isolated_store_over_temp_disks().await;
        metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
        crate::services::notification_sys::install_cross_pool_fence_fleet_proof_for_test();
        let bucket = format!("quota-reconcile-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket");
        let mut seed = PutObjReader::from_vec(vec![1; 10]);
        store
            .put_object(&bucket, "seed", &mut seed, &ObjectOptions::default())
            .await
            .expect("existing usage");
        store
            .update_bucket_metadata_config(
                &bucket,
                "quota.json",
                serde_json::to_vec(&super::super::BucketQuota::new(Some(limit))).expect("quota JSON"),
            )
            .await
            .expect("enable quota");
        let (_, incarnation, revision) = store
            .bucket_quota_and_incarnation_from_disk(&bucket)
            .await
            .expect("quota identity");
        (directories, ReconcileJob::new(store, bucket, incarnation, revision))
    }

    async fn quota_reservation(job: &ReconcileJob, old: u64, new: u64) -> Result<QuotaReservation> {
        begin(&job.store.ctx, &job.bucket, "candidate", &ObjectOptions::default(), 0, 0)
            .await?
            .reserve(old, new)
            .await
    }

    async fn read_test_ledger(job: &ReconcileJob) -> QuotaLedger {
        load_ledger_locked(Arc::clone(&job.store), &ledger_object(&job.bucket))
            .await
            .expect("ledger")
    }

    async fn drain_reconciliation_scheduler(job: &ReconcileJob) {
        let key = (
            job.store.id,
            job.bucket.clone(),
            job.bucket_incarnation,
            job.quota_revision.unix_timestamp_nanos(),
        );
        tokio::time::timeout(Duration::from_secs(10), async {
            while active_reconciliations().lock().expect("active workers").contains(&key) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("owned reconciliation finished");
    }

    async fn expire_reconciliation_backoff(job: &ReconcileJob) {
        drain_reconciliation_scheduler(job).await;
        let object = job.control_object();
        let lock = job.store.new_ns_lock(RUSTFS_META_BUCKET, &object).await.expect("owner lock");
        let guard = Arc::new(lock.get_write_lock(get_lock_acquire_timeout()).await.expect("owner guard"));
        // No control object exists before the first scan took the owner lock,
        // or after a retirement cleanup; there is nothing to expire then.
        let Ok(bytes) = read_config_no_lock(Arc::clone(&job.store), &object).await else {
            return;
        };
        let mut control: ReconcileControl = serde_json::from_slice(&bytes).expect("control JSON");
        control.next_attempt_at = 0;
        fence_namespace_mutations(&job.store, RUSTFS_META_BUCKET, &object, None)
            .await
            .expect("control fence");
        job.save_control(&object, &control, &guard).await.expect("advance test clock");
    }

    async fn wait_for_reconcile_owner(job: &ReconcileJob) {
        let object = job.control_object();
        let lock = job.store.new_ns_lock(RUSTFS_META_BUCKET, &object).await.expect("owner lock");
        let _guard = tokio::time::timeout(Duration::from_secs(10), lock.get_write_lock(get_lock_acquire_timeout()))
            .await
            .expect("worker finished")
            .expect("owner guard");
    }

    #[tokio::test]
    #[serial]
    async fn v1_group_commit_shares_durability_and_isolates_rejected_updates() {
        let (_directories, job) = reconciliation_fixture(100).await;
        job.run().await.expect("bootstrap");
        let first = quota_reservation(&job, 0, 2).await.expect("first reservation");
        let second = quota_reservation(&job, 0, 3).await.expect("second reservation");
        let Some(ReservationLedgerData::Single(first_data)) = first.ledger.as_ref() else {
            panic!("first v1 reservation");
        };
        let Some(ReservationLedgerData::Single(second_data)) = second.ledger.as_ref() else {
            panic!("second v1 reservation");
        };
        let object = ledger_object(&job.bucket);
        let mark = V1LedgerUpdateKind::MarkCommitStarted;
        let enqueue = |reservation: &QuotaReservation, data, kind| {
            enqueue_v1_ledger_update(data, kind, reservation.metadata_guard.as_ref(), get_lock_acquire_timeout())
        };

        let guard = lock_v1_ledger(&job.store, &object, "quota_ledger_test_hold", get_lock_acquire_timeout())
            .await
            .expect("hold the ledger while submitting both markers");
        let mut first_result = enqueue(&first, first_data, mark);
        let mut second_result = enqueue(&second, second_data, mark);
        assert!(matches!(first_result.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
        assert!(matches!(second_result.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
        fail_next_quota_ledger_save_for_test();
        drop(guard);
        assert!(matches!(first_result.await.expect("first response"), Err(StorageError::Unexpected)));
        assert!(matches!(second_result.await.expect("second response"), Err(StorageError::Unexpected)));
        let ledger = read_test_ledger(&job).await;
        assert_eq!(ledger.accounted_usage, 15);
        assert!(ledger.reservations.values().all(|reservation| !reservation.commit_started));

        let guard = lock_v1_ledger(&job.store, &object, "quota_ledger_test_hold", get_lock_acquire_timeout())
            .await
            .expect("hold the ledger across the first request's deadline");
        let expired = enqueue_v1_ledger_update(first_data, mark, first.metadata_guard.as_ref(), Duration::from_millis(25));
        let mut follower = enqueue(&second, second_data, mark);
        assert!(matches!(
            expired.await.expect("leader timeout response"),
            Err(StorageError::Lock(rustfs_lock::error::LockError::Timeout { .. }))
        ));
        assert!(matches!(follower.try_recv(), Err(oneshot::error::TryRecvError::Empty)));
        drop(guard);
        follower
            .await
            .expect("follower response")
            .expect("follower retains its own deadline");
        let ledger = read_test_ledger(&job).await;
        assert!(!ledger.reservations[&first_data.operation_id].commit_started);
        assert!(ledger.reservations[&second_data.operation_id].commit_started);

        let guard = lock_v1_ledger(&job.store, &object, "quota_ledger_test_hold", get_lock_acquire_timeout())
            .await
            .expect("hold the ledger while submitting invalid and valid updates");
        let mut wrong_size = first_data.clone();
        wrong_size.reservation.new_size = 99;
        let invalid = enqueue(&first, &wrong_size, V1LedgerUpdateKind::Settle { committed: false });
        let valid = enqueue(&second, second_data, V1LedgerUpdateKind::Settle { committed: false });
        drop(guard);
        assert!(matches!(
            invalid.await.expect("rejected update response"),
            Err(StorageError::PartMissingOrCorrupt)
        ));
        valid.await.expect("valid update response").expect("durable settlement");
        let ledger = read_test_ledger(&job).await;
        assert_eq!(ledger.accounted_usage, 12);
        assert!(!ledger.reservations[&first_data.operation_id].commit_started);
        assert!(!ledger.reservations.contains_key(&second_data.operation_id));
        first.abort().await;
        second.defer_after_fence();
        assert_eq!(read_test_ledger(&job).await.accounted_usage, 10);
    }

    #[tokio::test]
    #[serial]
    async fn v1_group_commit_keeps_operation_owner_after_waiter_cancellation() {
        let (_directories, job) = reconciliation_fixture(100).await;
        job.run().await.expect("bootstrap");
        let reservation = quota_reservation(&job, 0, 2).await.expect("reserve");
        let Some(ReservationLedgerData::Single(data)) = reservation.ledger.as_ref() else {
            panic!("v1 reservation");
        };
        let operation_id = data.operation_id;
        let operation_lock = job
            .store
            .new_ns_lock(RUSTFS_META_BUCKET, &operation_lock_object(&data.ledger_object, operation_id))
            .await
            .expect("operation lock");
        let guard = lock_v1_ledger(&job.store, &data.ledger_object, "quota_ledger_test_hold", get_lock_acquire_timeout())
            .await
            .expect("hold the ledger during cancellation");
        let result = enqueue_v1_ledger_update(
            data,
            V1LedgerUpdateKind::MarkCommitStarted,
            reservation.metadata_guard.as_ref(),
            get_lock_acquire_timeout(),
        );
        drop(result);
        reservation.defer_after_fence();
        assert!(
            operation_lock.get_write_lock_quiet(Duration::from_millis(10)).await.is_err(),
            "the batch must retain the reservation owner while the marker is in flight"
        );
        drop(guard);
        let _operation_owner = operation_lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .expect("batch releases the operation owner after persistence");
        assert!(read_test_ledger(&job).await.reservations[&operation_id].commit_started);
    }

    #[tokio::test]
    #[serial]
    async fn v1_admission_reaps_an_old_revision_before_bootstrapping() {
        let (_directories, job) = reconciliation_fixture(11).await;
        let object = ledger_object(&job.bucket);
        let guard = lock_v1_ledger(&job.store, &object, "quota_ledger_test_seed", get_lock_acquire_timeout())
            .await
            .expect("ledger lock");
        let previous_revision = job.quota_revision - time::Duration::seconds(1);
        let mut ledger = QuotaLedger::new(job.bucket_incarnation, previous_revision, 10);
        ledger
            .reserve(
                Uuid::new_v4(),
                PersistedReservation {
                    object: "abandoned".to_owned(),
                    old_size: 0,
                    new_size: 1,
                    created_at: now_unix() - 60,
                    pool_index: Some(0),
                    set_index: Some(0),
                    commit_started: false,
                },
            )
            .expect("precharge the old reservation");
        save_ledger_locked(Arc::clone(&job.store), &object, &mut ledger, &guard)
            .await
            .expect("persist the old ledger");
        drop(guard);

        assert!(matches!(quota_reservation(&job, 0, 1).await, Err(StorageError::SlowDown)));
        drain_reconciliation_scheduler(&job).await;
        let ledger = read_test_ledger(&job).await;
        assert!(ledger.matches(job.bucket_incarnation, job.quota_revision));
        assert!(ledger.reservations.is_empty());
        assert_eq!(ledger.accounted_usage, 10);

        let reservation = quota_reservation(&job, 0, 1).await.expect("admit the remaining byte");
        assert!(matches!(
            quota_reservation(&job, 0, 1).await,
            Err(StorageError::QuotaExceeded { current: 11, limit: 11 })
        ));
        reservation.abort().await;
        let ledger = read_test_ledger(&job).await;
        assert_eq!(ledger.accounted_usage, 10);
        assert!(ledger.reservations.is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn quota_reconciliation_bootstrap_is_single_flight_without_request_guards() {
        use std::sync::atomic::Ordering;
        let (_directories, job) = reconciliation_fixture(10).await;
        let pause = ReconcilePauseGuard::install(&job.bucket, ReconcilePauseStage::BeforeScan);
        assert!(
            matches!(quota_reservation(&job, 0, 1).await, Err(StorageError::SlowDown)),
            "unknown usage cannot admit a reservation"
        );
        pause.reached().await;
        assert!(
            matches!(
                load_ledger_locked(Arc::clone(&job.store), &ledger_object(&job.bucket)).await,
                Err(StorageError::ConfigNotFound)
            ),
            "bootstrap must not publish zero usage"
        );
        assert!(matches!(quota_reservation(&job, 0, 1).await, Err(StorageError::SlowDown)));
        // Bypass the local scheduler, exercising the cross-node ownership boundary.
        tokio::time::timeout(Duration::from_secs(2), job.run())
            .await
            .expect("contender must not wait for a scan")
            .expect("another owner is active");
        let lifecycle = tokio::time::timeout(Duration::from_secs(2), job.store.acquire_bucket_lifecycle_write_lock(&job.bucket))
            .await
            .expect("scan does not retain lifecycle readers")
            .expect("lifecycle writer");
        let metadata = tokio::time::timeout(
            Duration::from_secs(2),
            metadata_sys::acquire_bucket_metadata_transaction_lock_in(&job.store.ctx, &job.bucket),
        )
        .await
        .expect("scan does not retain metadata readers")
        .expect("metadata writer");
        drop(metadata);
        drop(lifecycle);
        pause.0.release.notify_one();
        wait_for_reconcile_owner(&job).await;
        let ledger = read_test_ledger(&job).await;
        assert_eq!(ledger.accounted_usage, 10);
        assert!(ledger.reservations.is_empty());
        assert!(!ledger.writer_revision.is_nil());
        for _ in 0..3 {
            assert!(matches!(quota_reservation(&job, 0, 1).await, Err(StorageError::QuotaExceeded { .. })));
            job.run().await.expect("persisted cooldown");
        }
        drain_reconciliation_scheduler(&job).await;
        assert_eq!(
            pause.0.scans.load(Ordering::SeqCst),
            1,
            "denied requests cannot repeatedly scan a full bucket"
        );
    }

    #[tokio::test]
    #[serial]
    async fn quota_reconciliation_rejects_concurrent_zero_growth_settle_reap_and_legacy_writes() {
        let (_directories, job) = reconciliation_fixture(100).await;
        job.run().await.expect("bootstrap");
        for mutation in ["abort", "commit", "reap", "legacy"] {
            expire_reconciliation_backoff(&job).await;
            let pause = ReconcilePauseGuard::install(&job.bucket, ReconcilePauseStage::BeforePublish);
            let worker = tokio::spawn({
                let job = job.clone();
                async move { job.run().await }
            });
            pause.reached().await;
            let initial = read_test_ledger(&job).await.writer_revision;
            if mutation == "legacy" {
                let object = ledger_object(&job.bucket);
                let lock = job.store.new_ns_lock(RUSTFS_META_BUCKET, &object).await.expect("ledger lock");
                let guard = Arc::new(lock.get_write_lock(get_lock_acquire_timeout()).await.expect("ledger writer"));
                let mut document = serde_json::to_value(read_test_ledger(&job).await).expect("ledger JSON");
                document.as_object_mut().expect("ledger map").remove("writer_revision");
                fence_namespace_mutations(&job.store, RUSTFS_META_BUCKET, &object, None)
                    .await
                    .expect("ledger fence");
                let mut opts = ObjectOptions {
                    max_parity: true,
                    no_lock: true,
                    ..Default::default()
                };
                let _ = opts.set_quota_admission(0, u64::MAX);
                opts.add_owned_write_lock(Arc::clone(&guard), RUSTFS_META_BUCKET, &object);
                opts.write_completion = crate::object_api::WriteCompletion::TailDrained;
                save_config_with_opts(
                    Arc::clone(&job.store),
                    &object,
                    serde_json::to_vec(&document).expect("old node JSON"),
                    &opts,
                )
                .await
                .expect("old node drops new field");
            } else {
                let mut reservation = quota_reservation(&job, 10, 10)
                    .await
                    .expect("zero-growth admission while scanning");
                let reserved = read_test_ledger(&job).await.writer_revision;
                assert_ne!(reserved, initial, "even zero-growth reserve must invalidate a scan");
                reservation.mark_commit_started().await.expect("commit marker");
                assert_ne!(read_test_ledger(&job).await.writer_revision, reserved);
                match mutation {
                    "abort" => reservation.abort().await,
                    "commit" => reservation.commit().await,
                    "reap" => {
                        reservation.defer_after_fence();
                        drop(
                            lock_and_reap_v1_ledger(&job.store, &job.bucket, &ledger_object(&job.bucket))
                                .await
                                .expect("reap orphan"),
                        );
                    }
                    _ => unreachable!(),
                }
            }
            let written = read_test_ledger(&job).await;
            assert!(written.reservations.is_empty());
            assert_ne!(written.writer_revision, initial);
            pause.0.release.notify_one();
            worker.await.expect("worker join").expect("stale scan discarded");
            let current = read_test_ledger(&job).await;
            assert_eq!(
                current.writer_revision, written.writer_revision,
                "{mutation}: stale usage must not replace a concurrent writer"
            );
            assert_eq!(current.accounted_usage, written.accounted_usage);
        }
    }

    #[tokio::test]
    #[serial]
    async fn quota_reconciliation_failure_is_throttled_and_deletions_eventually_reclaim_usage() {
        use std::sync::atomic::Ordering;
        let (_directories, job) = reconciliation_fixture(10).await;
        let pause = ReconcilePauseGuard::install(&job.bucket, ReconcilePauseStage::BeforeScan);
        pause.0.fail_scan.store(true, Ordering::SeqCst);
        pause.0.release.notify_one();
        assert!(job.run().await.is_err());
        assert!(matches!(
            load_ledger_locked(Arc::clone(&job.store), &ledger_object(&job.bucket)).await,
            Err(StorageError::ConfigNotFound)
        ));
        job.run().await.expect("failure cooldown");
        assert_eq!(pause.0.scans.load(Ordering::SeqCst), 1);
        pause.0.fail_scan.store(false, Ordering::SeqCst);
        expire_reconciliation_backoff(&job).await;
        job.run().await.expect("recover after failure");
        assert_eq!(read_test_ledger(&job).await.accounted_usage, 10);
        job.store
            .delete_object(&job.bucket, "seed", ObjectOptions::default())
            .await
            .expect("delete stored usage");
        assert!(
            matches!(quota_reservation(&job, 0, 1).await, Err(StorageError::QuotaExceeded { .. })),
            "stale usage remains conservative during cooldown"
        );
        expire_reconciliation_backoff(&job).await;
        job.run().await.expect("refresh deleted usage");
        assert_eq!(read_test_ledger(&job).await.accounted_usage, 0);
        quota_reservation(&job, 0, 1)
            .await
            .expect("delete must not leave bucket permanently full")
            .abort()
            .await;
    }

    #[tokio::test]
    #[serial]
    async fn quota_reconciliation_survives_waiter_cancellation_and_rechecks_configuration() {
        let (_directories, job) = reconciliation_fixture(100).await;
        let pause = ReconcilePauseGuard::install(&job.bucket, ReconcilePauseStage::BeforePublish);
        let caller = tokio::spawn({
            let job = job.clone();
            async move {
                job.schedule();
                std::future::pending::<()>().await
            }
        });
        pause.reached().await;
        caller.abort();
        assert!(caller.await.expect_err("cancel caller").is_cancelled());
        job.store
            .update_bucket_metadata_config(
                &job.bucket,
                "quota.json",
                serde_json::to_vec(&super::super::BucketQuota::new(Some(200))).expect("updated quota"),
            )
            .await
            .expect("quota changes while scanning");
        pause.0.release.notify_one();
        wait_for_reconcile_owner(&job).await;
        assert!(
            matches!(
                load_ledger_locked(Arc::clone(&job.store), &ledger_object(&job.bucket)).await,
                Err(StorageError::ConfigNotFound)
            ),
            "old config cannot publish bootstrap usage"
        );
        drop(pause);
        let (_, incarnation, revision) = job
            .store
            .bucket_quota_and_incarnation_from_disk(&job.bucket)
            .await
            .expect("current configuration");
        let current = ReconcileJob::new(Arc::clone(&job.store), job.bucket.clone(), incarnation, revision);
        let pause = ReconcilePauseGuard::install(&job.bucket, ReconcilePauseStage::BeforeScan);
        let caller = tokio::spawn({
            let job = current.clone();
            async move {
                job.schedule();
                std::future::pending::<()>().await
            }
        });
        pause.reached().await;
        caller.abort();
        assert!(caller.await.expect_err("cancel new caller").is_cancelled());
        pause.0.release.notify_one();
        wait_for_reconcile_owner(&current).await;
        assert!(
            read_test_ledger(&current).await.matches(incarnation, revision),
            "owned scan publishes after its waiter is canceled"
        );
    }

    #[tokio::test]
    #[serial]
    async fn quota_reconciliation_cannot_publish_into_recreated_bucket() {
        use crate::storage_api_contracts::bucket::DeleteBucketOptions;
        let (_directories, job) = reconciliation_fixture(100).await;
        let pause = ReconcilePauseGuard::install(&job.bucket, ReconcilePauseStage::BeforePublish);
        let worker = tokio::spawn({
            let job = job.clone();
            async move { job.run().await }
        });
        pause.reached().await;
        job.store
            .delete_object(&job.bucket, "seed", ObjectOptions::default())
            .await
            .expect("empty old bucket");
        job.store
            .delete_bucket(&job.bucket, &DeleteBucketOptions::default())
            .await
            .expect("retire bucket while scan is paused");
        job.store
            .make_bucket(&job.bucket, &MakeBucketOptions::default())
            .await
            .expect("replacement bucket");
        job.store
            .update_bucket_metadata_config(
                &job.bucket,
                "quota.json",
                serde_json::to_vec(&super::super::BucketQuota::new(Some(100))).expect("new quota"),
            )
            .await
            .expect("replacement quota");
        pause.0.release.notify_one();
        assert!(worker.await.expect("old worker join").is_err(), "old incarnation must not publish");
        assert!(matches!(
            load_ledger_locked(Arc::clone(&job.store), &ledger_object(&job.bucket)).await,
            Err(StorageError::ConfigNotFound)
        ));
        drop(pause);
        initialize_quota_ledger_for_test(Arc::clone(&job.store), &job.bucket)
            .await
            .expect("replacement can bootstrap independently");
        let replacement = read_test_ledger(&job).await;
        assert_ne!(replacement.bucket_incarnation, job.bucket_incarnation);
        assert_eq!(replacement.accounted_usage, 0);
    }

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

    #[tokio::test]
    #[serial]
    async fn retired_bucket_cleanup_removes_stale_v1_reservation_before_new_admission() {
        let (_directories, store) = metadata_sys::test_support::isolated_store_over_temp_disks().await;
        metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
        let bucket = format!("quota-orphan-recovery-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create old bucket generation");
        let old_incarnation = store
            .bucket_incarnation_id_from_disk(&bucket)
            .await
            .expect("old bucket incarnation");
        let new_incarnation = Uuid::new_v4();
        let ledger_path = ledger_object(&bucket);
        let mut stale = QuotaLedger::new(old_incarnation, OffsetDateTime::now_utc(), 0);
        stale
            .reserve(
                Uuid::new_v4(),
                PersistedReservation {
                    object: "crashed-put".to_string(),
                    old_size: 0,
                    new_size: 100,
                    created_at: now_unix(),
                    pool_index: Some(0),
                    set_index: Some(0),
                    commit_started: false,
                },
            )
            .expect("crash-left reservation is structurally valid");
        crate::config::com::save_config(Arc::clone(&store), &ledger_path, serde_json::to_vec(&stale).expect("encode ledger"))
            .await
            .expect("persist crash-left reservation fixture");
        // A throttled reconciliation of the old generation left its control object behind.
        let stale_control =
            ReconcileJob::new(Arc::clone(&store), bucket.clone(), old_incarnation, OffsetDateTime::now_utc()).control_object();
        let control = ReconcileControl {
            bucket_incarnation: old_incarnation,
            quota_revision_unix_nanos: 0,
            next_attempt_at: i64::MAX,
        };
        crate::config::com::save_config(
            Arc::clone(&store),
            &stale_control,
            serde_json::to_vec(&control).expect("encode control"),
        )
        .await
        .expect("persist stale reconciliation control");

        assert!(matches!(
            current_v1_ledger(
                load_v1_ledger_if_present(&store, &ledger_path).await.expect("load ledger"),
                new_incarnation,
                OffsetDateTime::now_utc(),
            ),
            Err(StorageError::PartMissingOrCorrupt)
        ));
        assert!(matches!(
            cleanup_retired_bucket_ledger(Arc::clone(&store), &bucket, Uuid::new_v4()).await,
            Err(StorageError::PreconditionFailed)
        ));
        crate::bucket::retirement::commit_retirement(Arc::clone(&store), &bucket, old_incarnation, &ObjectOptions::default())
            .await
            .expect("publish predecessor generation retirement proof");
        cleanup_retired_bucket_ledger(Arc::clone(&store), &bucket, new_incarnation)
            .await
            .expect("a proven retired predecessor ledger cannot block the replacement generation");
        assert!(matches!(
            load_ledger_locked(Arc::clone(&store), &ledger_path).await,
            Err(StorageError::ConfigNotFound)
        ));
        assert!(
            matches!(
                read_config_no_lock(Arc::clone(&store), &stale_control).await,
                Err(StorageError::ConfigNotFound)
            ),
            "the retired generation's reconciliation control object goes with its ledger"
        );
        // A control object written back after the ledger was already gone (a scan
        // that was mid-flight during retirement) is removed by the next cleanup.
        crate::config::com::save_config(
            Arc::clone(&store),
            &stale_control,
            serde_json::to_vec(&control).expect("encode control"),
        )
        .await
        .expect("persist late reconciliation control");
        cleanup_retired_bucket_ledger(Arc::clone(&store), &bucket, old_incarnation)
            .await
            .expect("cleanup without a ledger still removes the generation's control object");
        assert!(matches!(
            read_config_no_lock(Arc::clone(&store), &stale_control).await,
            Err(StorageError::ConfigNotFound)
        ));
        let replacement = current_v1_ledger(
            load_v1_ledger_if_present(&store, &ledger_path).await.expect("load ledger"),
            new_incarnation,
            OffsetDateTime::now_utc(),
        )
        .expect("new quota admission must not inherit a fresh stale reservation");
        assert!(replacement.is_none(), "replacement must await authoritative bootstrap before admission");
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

    struct AllocatorPause {
        bucket: String,
        object: String,
        reached: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    static ALLOCATOR_PAUSE: std::sync::Mutex<Option<Arc<AllocatorPause>>> = std::sync::Mutex::new(None);

    struct AllocatorPauseGuard(Arc<AllocatorPause>);

    impl Drop for AllocatorPauseGuard {
        fn drop(&mut self) {
            self.0.release.notify_one();
            let mut pause = ALLOCATOR_PAUSE.lock().expect("allocator pause registry");
            if pause.as_ref().is_some_and(|current| Arc::ptr_eq(current, &self.0)) {
                *pause = None;
            }
        }
    }

    pub(super) async fn pause_after_allocator_ready(bucket: &str, object: &str) {
        let pause = ALLOCATOR_PAUSE
            .lock()
            .expect("allocator pause registry")
            .as_ref()
            .filter(|pause| pause.bucket == bucket && pause.object == object)
            .cloned();
        if let Some(pause) = pause {
            pause.reached.notify_one();
            pause.release.notified().await;
        }
    }

    // Its writes read the process-wide usage snapshot cache through the
    // degraded-baseline fallback, which the data-usage cache tests rewrite.
    #[tokio::test]
    #[serial]
    async fn sharded_reservation_reloads_allocator_after_concurrent_refill() {
        use crate::object_api::PutObjReader;
        use crate::storage_api_contracts::bucket::{BucketOperations, MakeBucketOptions};
        use crate::storage_api_contracts::object::ObjectIO;

        let (_directories, store) = metadata_sys::test_support::isolated_store_over_temp_disks().await;
        metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
        crate::services::notification_sys::install_cross_pool_fence_fleet_proof_for_test();
        let bucket = format!("quota-refill-{}", Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create quota bucket");
        store
            .update_bucket_metadata_config(
                &bucket,
                "quota.json",
                serde_json::to_vec(&super::super::BucketQuota::new_sharded(100_000)).expect("serialize v2 quota"),
            )
            .await
            .expect("enable v2 quota");
        let names = (0..1000)
            .map(|index| format!("quota-object-{index}"))
            .filter(|name| shard_index(name) == 0)
            .take(3)
            .collect::<Vec<_>>();
        assert_eq!(names.len(), 3);
        let mut seed = PutObjReader::from_vec(vec![1]);
        store
            .put_object(&bucket, &names[0], &mut seed, &ObjectOptions::default())
            .await
            .expect("seed a one-byte initial grant");

        let pause = Arc::new(AllocatorPause {
            bucket: bucket.clone(),
            object: names[1].clone(),
            reached: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let _pause_guard = AllocatorPauseGuard(Arc::clone(&pause));
        *ALLOCATOR_PAUSE.lock().expect("install allocator pause") = Some(Arc::clone(&pause));
        let waiting_put = tokio::spawn({
            let store = Arc::clone(&store);
            let bucket = bucket.clone();
            let object = names[1].clone();
            async move {
                let mut body = PutObjReader::from_vec(vec![2]);
                store.put_object(&bucket, &object, &mut body, &ObjectOptions::default()).await
            }
        });
        tokio::time::timeout(Duration::from_secs(30), pause.reached.notified())
            .await
            .expect("first PUT reached the gap after allocator initialization");

        // This PUT refills the allocator and persists the new grant in the same shard.
        let mut body = PutObjReader::from_vec(vec![3, 4]);
        let refill_result = store
            .put_object(&bucket, &names[2], &mut body, &ObjectOptions::default())
            .await;
        pause.release.notify_one();
        let waiting_result = tokio::time::timeout(Duration::from_secs(30), waiting_put)
            .await
            .expect("paused PUT completed")
            .expect("paused PUT task joined");
        assert_eq!(refill_result.expect("refill PUT remains below quota").size, 2);
        assert_eq!(
            waiting_result
                .expect("paused PUT must adopt the current allocator grants")
                .size,
            1
        );
    }

    #[tokio::test]
    #[serial]
    async fn sharded_reservation_persists_a_populated_lazy_shard_before_refill() {
        use crate::object_api::PutObjReader;
        use crate::storage_api_contracts::object::ObjectIO;

        let (_directories, store) = metadata_sys::test_support::isolated_store_over_temp_disks().await;
        metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
        crate::services::notification_sys::install_cross_pool_fence_fleet_proof_for_test();
        let bucket = format!("quota-populated-bootstrap-{}", Uuid::new_v4().simple());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create quota bucket");

        let mut seed_shard = None;
        let mut seed_name = None;
        let mut candidate_name = None;
        let mut bootstrap_name = None;
        for index in 0..10_000 {
            let object = format!("quota-populated-object-{index}");
            let object_shard = shard_index(&object);
            if seed_shard.is_none() {
                seed_shard = Some(object_shard);
                seed_name = Some(object);
            } else if Some(object_shard) == seed_shard && candidate_name.is_none() {
                candidate_name = Some(object);
            } else if Some(object_shard) != seed_shard && bootstrap_name.is_none() {
                bootstrap_name = Some(object);
            }
            if candidate_name.is_some() && bootstrap_name.is_some() {
                break;
            }
        }
        let seed_name = seed_name.expect("find a seeded shard");
        let candidate_name = candidate_name.expect("find a second object in the seeded shard");
        let bootstrap_name = bootstrap_name.expect("find an empty bootstrap shard");

        let mut seed = PutObjReader::from_vec(vec![1]);
        store
            .put_object(&bucket, &seed_name, &mut seed, &ObjectOptions::default())
            .await
            .expect("seed existing usage before enabling v2");
        store
            .update_bucket_metadata_config(
                &bucket,
                "quota.json",
                serde_json::to_vec(&super::super::BucketQuota::new_sharded(100_000)).expect("serialize v2 quota"),
            )
            .await
            .expect("enable v2 quota");

        let mut bootstrap = PutObjReader::from_vec(vec![2]);
        store
            .put_object(&bucket, &bootstrap_name, &mut bootstrap, &ObjectOptions::default())
            .await
            .expect("bootstrap an empty shard");

        let mut candidate = PutObjReader::from_vec(vec![3]);
        let result = store
            .put_object(&bucket, &candidate_name, &mut candidate, &ObjectOptions::default())
            .await
            .expect("a populated shard must persist before refill and accept its first growth");
        assert_eq!(result.size, 1);
    }
}
