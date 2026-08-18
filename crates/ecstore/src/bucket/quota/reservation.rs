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

use crate::bucket::metadata_sys;
use crate::config::com::{CONFIG_PREFIX, read_config_no_lock, save_config_with_opts};
use crate::data_usage::compute_bucket_usage;
use crate::disk::RUSTFS_META_BUCKET;
use crate::disk::{DiskAPI, error::DiskError};
use crate::error::{Result, StorageError, is_err_object_not_found, is_err_version_not_found};
use crate::object_api::{ObjectInfo, ObjectOptions, QuotaAdmission};
use crate::set_disk::{SetDisks, get_lock_acquire_timeout};
use crate::storage_api_contracts::namespace::NamespaceLocking;
use crate::storage_api_contracts::object::ObjectOperations;
use crate::store::ECStore;
use futures::{StreamExt, stream};
use rustfs_lock::NamespaceLockGuard;
use serde::{Deserialize, Serialize};
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
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_QUOTA: &str = "quota";

#[cfg(any(test, feature = "test-util"))]
static FAIL_NEXT_LEDGER_SAVE: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);

// Lock order: caller-held destination object/upload, bucket metadata
// transaction (read), operation reservation, then quota ledger.

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

pub(crate) struct QuotaContext {
    store: Option<Arc<ECStore>>,
    bucket: String,
    object: String,
    ledger_object: String,
    bucket_incarnation: Option<Uuid>,
    quota_revision: Option<OffsetDateTime>,
    quota_limit: Option<u64>,
    capability_proof: Option<crate::services::notification_sys::CrossPoolFenceFleetProofToken>,
    snapshot_admission: Option<QuotaAdmission>,
    legacy_data_movement: bool,
    metadata_guard: Option<NamespaceLockGuard>,
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
            let ledger_guard = ledger_lock.get_write_lock(get_lock_acquire_timeout()).await?;
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
            if operation_guard.is_lock_lost() || metadata_guard.as_ref().is_some_and(NamespaceLockGuard::is_lock_lost) {
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
                ledger: Some(ledger_data),
                operation_guard: Some(operation_guard),
                metadata_guard,
                capability_proof,
                state: ReservationState::Pending,
            })
        })
        .await
        .map_err(|err| StorageError::other(format!("quota ledger reservation task failed: {err}")))?
    }
}

#[derive(Clone)]
struct LedgerReservationData {
    store: Arc<ECStore>,
    bucket: String,
    ledger_object: String,
    operation_id: Uuid,
    reservation: PersistedReservation,
}

pub(crate) struct QuotaReservation {
    ledger: Option<LedgerReservationData>,
    operation_guard: Option<NamespaceLockGuard>,
    metadata_guard: Option<NamespaceLockGuard>,
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
    fn unlimited(metadata_guard: Option<NamespaceLockGuard>) -> Self {
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
            || self.metadata_guard.as_ref().is_some_and(NamespaceLockGuard::is_lock_lost)
    }

    pub(crate) fn capability_proof_matches(&self) -> bool {
        self.capability_proof
            .as_ref()
            .is_none_or(crate::services::notification_sys::cross_pool_fence_fleet_proof_matches)
    }

    pub(crate) async fn mark_commit_started(&mut self) -> Result<()> {
        if !self.capability_proof_matches() {
            let ledger = self.ledger.as_ref().ok_or(StorageError::PartMissingOrCorrupt)?;
            return Err(quota_capability_error(&ledger.bucket, &ledger.ledger_object));
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
        crate::store::list_objects::observe_list_objects_mutation(&ledger.store, &ledger.bucket).await;
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
    snapshot_admission: Option<QuotaAdmission>,
    data_movement: bool,
    pool_index: usize,
    set_index: usize,
) -> Result<QuotaContext> {
    if crate::bucket::utils::is_meta_bucketname(bucket) {
        return Ok(QuotaContext {
            store: None,
            bucket: bucket.to_string(),
            object: object.to_string(),
            ledger_object: ledger_object(bucket),
            bucket_incarnation: None,
            quota_revision: None,
            quota_limit: None,
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
            capability_proof: None,
            snapshot_admission: None,
            legacy_data_movement: false,
            metadata_guard: None,
            pool_index: None,
            set_index: None,
        });
    }

    let metadata_guard = metadata_sys::acquire_bucket_metadata_transaction_read_lock_in(ctx, bucket).await?;
    let (quota, bucket_incarnation, quota_revision) =
        metadata_sys::get_quota_config_and_incarnation_from_disk_in(ctx, bucket).await?;
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
    let snapshot_admission = match quota.as_ref().filter(|quota| !quota.uses_durable_reservations()) {
        Some(quota) => match (quota.quota, snapshot_admission) {
            (Some(limit), Some(admission)) if admission.quota_limit() == limit => Some(admission),
            (Some(_), None) if data_movement => None,
            (Some(_), _) => return Err(StorageError::PartMissingOrCorrupt),
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

async fn mark_commit_started(data: &LedgerReservationData) -> Result<()> {
    let data = data.clone();
    tokio::spawn(async move {
        let ledger_lock = data.store.new_ns_lock(RUSTFS_META_BUCKET, &data.ledger_object).await?;
        let ledger_guard = ledger_lock.get_write_lock(get_lock_acquire_timeout()).await?;
        fence_namespace_mutations(&data.store, RUSTFS_META_BUCKET, &data.ledger_object, None).await?;
        let mut ledger = load_ledger_locked(Arc::clone(&data.store), &data.ledger_object).await?;
        ledger.mark_commit_started(data.operation_id, &data.reservation)?;
        save_ledger_locked(Arc::clone(&data.store), &data.ledger_object, &ledger, &ledger_guard).await
    })
    .await
    .map_err(|err| StorageError::other(format!("quota commit marker task failed: {err}")))?
}

async fn settle(data: &LedgerReservationData, committed: bool) -> Result<()> {
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
        let ledger_guard = ledger_lock.get_write_lock(get_lock_acquire_timeout()).await?;
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
    .map_err(|err| StorageError::other(format!("quota ledger settlement task failed: {err}")))?
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
    let ledger_guard = ledger_lock.get_write_lock(get_lock_acquire_timeout()).await?;
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

fn ledger_object(bucket: &str) -> String {
    format!("{CONFIG_PREFIX}/quota-ledger/{bucket}.json")
}

fn operation_lock_object(ledger_object: &str, operation_id: Uuid) -> String {
    format!("{ledger_object}.operations/{operation_id}")
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
    ledger_guard: &NamespaceLockGuard,
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
    opts.add_namespace_lock_guard(ledger_guard);
    save_config_with_opts(store, ledger_object, serde_json::to_vec(ledger)?, &opts).await
}

#[cfg(any(test, feature = "test-util"))]
#[allow(dead_code, reason = "asserted by this file's tests (backlog#1823)")]
pub fn fail_next_quota_ledger_save_for_test() {
    FAIL_NEXT_LEDGER_SAVE.store(true, std::sync::atomic::Ordering::SeqCst);
}

fn log_deferred_settlement(data: &LedgerReservationData, state: &'static str, err: &StorageError) {
    warn!(
        event = EVENT_QUOTA_LEDGER_SETTLEMENT,
        component = LOG_COMPONENT_ECSTORE,
        subsystem = LOG_SUBSYSTEM_QUOTA,
        state,
        bucket = %data.bucket,
        operation_id = %data.operation_id,
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
}
