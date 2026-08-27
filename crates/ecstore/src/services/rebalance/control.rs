use super::meta::{
    RebalanceMetaMergeOutcome, clone_first_arc, clone_rebalance_pool_stats, defer_bucket_in_rebalance_queue,
    ensure_valid_rebalance_pool_index, invalid_rebalance_pool_index_error, is_rebalance_actively_running,
    is_rebalance_conflicting_with_decommission, mark_rebalance_bucket_done, merge_rebalance_meta, percent_free_ratio,
    rebalance_metadata_not_initialized_error, record_rebalance_cleanup_warning_in_meta,
    record_rebalance_stop_propagation_snapshot, resolve_next_rebalance_bucket, rollback_rebalance_start_meta_snapshot_for_id,
    should_accept_rebalance_stats_update, should_pool_participate, stop_rebalance_meta_snapshot_for_id,
    validate_init_rebalance_state,
};
use super::worker::{
    rebalance_meta_lock_error, resolve_load_rebalance_stats_update_result, resolve_rebalance_meta_load_result,
    resolve_rebalance_meta_save_result,
};
use super::{
    DiskStat, EVENT_REBALANCE_BUCKET, EVENT_REBALANCE_STATE, LOG_COMPONENT_ECSTORE, LOG_SUBSYSTEM_REBALANCE, REBAL_META_NAME,
    RebalStatus, RebalanceInfo, RebalanceMeta, RebalanceStats, RebalanceStopPropagationRecord,
    encode_rebalance_stop_propagation_record,
};
use crate::core::pools::{
    PoolMeta, PoolRebalanceActivationFence, acquire_pool_activation_fleet_proof, acquire_pool_rebalance_activation_locks,
    pool_meta_has_active_decommission,
};
use crate::error::{Error, Result};
use crate::object_api::ObjectOptions;
use crate::set_disk::get_lock_acquire_timeout;
use crate::storage_api_contracts::{
    admin::StorageAdminApi,
    namespace::NamespaceLocking as StorageNamespaceLocking,
    object::{EcstoreObjectIO, EcstoreObjectOperations},
};
use crate::store::ECStore;
use rustfs_filemeta::FileInfo;
use std::sync::Arc;
use time::OffsetDateTime;
use tracing::{debug, info};
use uuid::Uuid;

#[cfg(test)]
static FAIL_NEXT_REBALANCE_ACTIVATION_SAVE: std::sync::Mutex<Option<String>> = std::sync::Mutex::new(None);

#[cfg(test)]
static REBALANCE_DISK_STATS_OVERRIDES: std::sync::OnceLock<std::sync::Mutex<std::collections::HashMap<Uuid, Vec<DiskStat>>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
pub(super) fn fail_next_rebalance_activation_save_for_test(rebalance_id: &str) {
    *FAIL_NEXT_REBALANCE_ACTIVATION_SAVE
        .lock()
        .expect("rebalance activation save failure hook should not be poisoned") = Some(rebalance_id.to_string());
}

#[cfg(test)]
fn set_rebalance_disk_stats_override_for_test(store_id: Uuid, disk_stats: Vec<DiskStat>) {
    REBALANCE_DISK_STATS_OVERRIDES
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
        .lock()
        .expect("rebalance disk stats override should not be poisoned")
        .insert(store_id, disk_stats);
}

#[cfg(test)]
fn take_rebalance_disk_stats_override_for_test(store_id: Uuid) -> Option<Vec<DiskStat>> {
    REBALANCE_DISK_STATS_OVERRIDES
        .get_or_init(|| std::sync::Mutex::new(std::collections::HashMap::new()))
        .lock()
        .expect("rebalance disk stats override should not be poisoned")
        .remove(&store_id)
}

fn ensure_rebalance_activation_pool_meta_allowed(meta: &PoolMeta) -> Result<()> {
    if pool_meta_has_active_decommission(meta) {
        return Err(Error::DecommissionAlreadyRunning);
    }

    Ok(())
}

pub(super) enum RebalanceWorkerActivationFence {
    Ready(Box<PoolRebalanceActivationFence>),
    NotStartedTerminal,
}

pub(super) struct RebalanceRunGuard {
    _local_guard: tokio::sync::OwnedRwLockReadGuard<()>,
    persisted_guard: rustfs_lock::NamespaceLockGuard,
}

#[cfg(any(test, feature = "test-util"))]
struct RebalanceStopWaitProbeState {
    expected_id: String,
    attempted: std::sync::atomic::AtomicBool,
    notify: tokio::sync::Notify,
}

#[cfg(any(test, feature = "test-util"))]
static REBALANCE_STOP_WAIT_PROBES: std::sync::OnceLock<std::sync::Mutex<Vec<Arc<RebalanceStopWaitProbeState>>>> =
    std::sync::OnceLock::new();

#[cfg(any(test, feature = "test-util"))]
pub(super) struct RebalanceStopWaitProbe {
    state: Arc<RebalanceStopWaitProbeState>,
}

#[cfg(any(test, feature = "test-util"))]
impl RebalanceStopWaitProbe {
    pub(super) fn install(expected_id: &str) -> Self {
        let state = Arc::new(RebalanceStopWaitProbeState {
            expected_id: expected_id.to_string(),
            attempted: std::sync::atomic::AtomicBool::new(false),
            notify: tokio::sync::Notify::new(),
        });
        REBALANCE_STOP_WAIT_PROBES
            .get_or_init(|| std::sync::Mutex::new(Vec::new()))
            .lock()
            .expect("rebalance stop wait probe should not be poisoned")
            .push(Arc::clone(&state));
        Self { state }
    }

    pub(super) async fn wait_until_attempted(&self) {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            while !self.state.attempted.load(std::sync::atomic::Ordering::Acquire) {
                self.state.notify.notified().await;
            }
        })
        .await
        .expect("rebalance stop should reach the deterministic activation wait probe");
    }
}

#[cfg(any(test, feature = "test-util"))]
impl Drop for RebalanceStopWaitProbe {
    fn drop(&mut self) {
        let mut probes = REBALANCE_STOP_WAIT_PROBES
            .get_or_init(|| std::sync::Mutex::new(Vec::new()))
            .lock()
            .expect("rebalance stop wait probe should not be poisoned");
        probes.retain(|state| !Arc::ptr_eq(state, &self.state));
    }
}

#[cfg(any(test, feature = "test-util"))]
fn observe_rebalance_stop_wait_attempt(expected_id: Option<&str>) {
    let probes = REBALANCE_STOP_WAIT_PROBES
        .get_or_init(|| std::sync::Mutex::new(Vec::new()))
        .lock()
        .expect("rebalance stop wait probe should not be poisoned")
        .clone();
    for state in probes {
        if expected_id == Some(state.expected_id.as_str()) {
            state.attempted.store(true, std::sync::atomic::Ordering::Release);
            state.notify.notify_one();
        }
    }
}

impl RebalanceRunGuard {
    pub(super) fn ensure_held(&self, stage: &str) -> Result<()> {
        #[cfg(test)]
        let forced_lost = self
            .persisted_guard
            .lock_lost_signal()
            .is_some_and(|signal| crate::object_api::namespace_lock_signal_test_fence_is_lost(&signal));
        #[cfg(not(test))]
        let forced_lost = false;
        if self.persisted_guard.is_lock_lost() || forced_lost {
            return Err(Error::other(format!("rebalance distributed run fence lost during {stage}")));
        }
        Ok(())
    }

    pub(super) fn lock_lost_signal(&self) -> Option<Arc<rustfs_lock::distributed_lock::LockLostSignal>> {
        self.persisted_guard.lock_lost_signal()
    }
}

async fn acquire_persisted_rebalance_run_guard<S>(
    pool: Arc<S>,
    expected_id: &str,
    stage: &str,
) -> Result<rustfs_lock::NamespaceLockGuard>
where
    S: EcstoreObjectIO
        + EcstoreObjectOperations
        + StorageNamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>,
{
    let ns_lock = pool.new_ns_lock(crate::disk::RUSTFS_META_BUCKET, REBAL_META_NAME).await?;
    let guard = ns_lock
        .get_read_lock(get_lock_acquire_timeout())
        .await
        .map_err(|err| rebalance_meta_lock_error(err, "read"))?;
    let mut opts = ObjectOptions {
        no_lock: true,
        ..Default::default()
    };
    opts.add_namespace_lock_guard(&guard);
    let object_info = pool
        .get_object_info(crate::disk::RUSTFS_META_BUCKET, REBAL_META_NAME, &opts)
        .await;
    let persisted_run_id = match object_info {
        Ok(object_info) => {
            let metadata = object_info.user_defined.as_ref();
            let run_id = rustfs_utils::http::metadata_compat::get_consistent_str(
                metadata,
                rustfs_utils::http::metadata_compat::SUFFIX_REBALANCE_RUN_ID,
            );
            if run_id.is_none()
                && rustfs_utils::http::metadata_compat::contains_key_str(
                    metadata,
                    rustfs_utils::http::metadata_compat::SUFFIX_REBALANCE_RUN_ID,
                )
            {
                return Err(Error::other(format!("rebalance run marker is inconsistent during {stage}")));
            }
            run_id.map(str::to_string)
        }
        Err(err) if crate::error::is_err_object_not_found(&err) => None,
        Err(err) => return Err(err),
    };
    if let Some(persisted_run_id) = persisted_run_id {
        if persisted_run_id != expected_id {
            return Err(Error::other(format!(
                "stale rebalance run rejected during {stage}: expected {expected_id}, found {persisted_run_id}"
            )));
        }
    } else {
        // Metadata written before the fixed-size run marker was introduced
        // remains readable during rolling upgrades.
        let mut persisted = RebalanceMeta::new();
        persisted.load_with_opts(pool, opts).await?;
        ensure_rebalance_worker_active(Some(&persisted), expected_id, stage)?;
    }
    if guard.is_lock_lost() {
        return Err(Error::other(format!("rebalance distributed run fence lost during {stage}")));
    }
    Ok(guard)
}

pub(super) async fn merge_and_save_rebalance_meta_no_lock<S>(
    pool: Arc<S>,
    local_snapshot: &RebalanceMeta,
    stage: &str,
    mut opts: ObjectOptions,
    activation_fence: Option<&PoolRebalanceActivationFence>,
    expected_id: Option<&str>,
) -> Result<()>
where
    S: EcstoreObjectIO,
{
    let mut merged = RebalanceMeta::new();
    match merged.load_with_opts(pool.clone(), opts.clone()).await {
        Ok(()) => {
            if let Some(expected_id) = expected_id {
                ensure_rebalance_run_id(Some(&merged), expected_id, stage)?;
            }
            if merge_rebalance_meta(&mut merged, local_snapshot) == RebalanceMetaMergeOutcome::RejectedActiveConflict {
                return Err(Error::RebalanceAlreadyRunning);
            }
        }
        Err(Error::ConfigNotFound) => {
            if expected_id.is_some() {
                return Err(rebalance_metadata_not_initialized_error(stage));
            }
            merged = local_snapshot.clone();
        }
        Err(err) => return Err(Error::other(format!("rebalance meta load before save failed during {stage}: {err}"))),
    }

    if let Some(fence) = activation_fence {
        fence.add_namespace_lock_fence(&mut opts);
        fence.ensure_held()?;
    }
    #[cfg(test)]
    let barrier_pool = pool.clone();
    merged.save_with_opts(pool, opts).await?;
    #[cfg(test)]
    if let Some(fence) = activation_fence {
        crate::core::pools::pause_pool_activation_after_durable_save(&barrier_pool, fence).await;
    }
    // With an activation fence, a successful save is the commit point. Lease
    // loss after this point cannot make the durable activation uncommitted.
    Ok(())
}

pub(super) fn ensure_rebalance_run_id(meta: Option<&RebalanceMeta>, expected_id: &str, stage: &str) -> Result<()> {
    let Some(meta) = meta else {
        return Err(rebalance_metadata_not_initialized_error(stage));
    };
    if meta.id != expected_id {
        return Err(Error::other(format!(
            "stale rebalance worker rejected during {stage}: expected {expected_id}, found {}",
            meta.id
        )));
    }
    Ok(())
}

pub(super) fn ensure_rebalance_worker_active(meta: Option<&RebalanceMeta>, expected_id: &str, stage: &str) -> Result<()> {
    ensure_rebalance_run_id(meta, expected_id, stage)?;
    let Some(meta) = meta else {
        return Err(rebalance_metadata_not_initialized_error(stage));
    };
    if meta.stopped_at.is_some()
        || meta
            .cancel
            .as_ref()
            .is_some_and(tokio_util::sync::CancellationToken::is_cancelled)
        || !is_rebalance_conflicting_with_decommission(meta)
    {
        return Err(Error::other(format!("inactive rebalance worker rejected during {stage}: {expected_id}")));
    }
    Ok(())
}

pub(super) fn validate_rebalance_disk_stats_coverage(disk_stats: &[DiskStat]) -> Result<()> {
    for (idx, disk_stat) in disk_stats.iter().enumerate() {
        if disk_stat.total_space == 0 {
            return Err(Error::other(format!(
                "rebalance storage info is incomplete: pool {idx} has no reported capacity"
            )));
        }
    }

    Ok(())
}

fn pool_rebalance_status_from_meta(meta: Option<&RebalanceMeta>, pool_index: usize) -> (RebalStatus, bool) {
    meta.and_then(|meta| meta.pool_stats.get(pool_index))
        .filter(|pool_stat| pool_stat.participating)
        .map(|pool_stat| (pool_stat.info.status, pool_stat.info.stopping))
        .unwrap_or_default()
}

fn merge_rebalance_status_refresh(current: &mut Option<RebalanceMeta>, persisted: RebalanceMeta) -> bool {
    if persisted.id.is_empty() && persisted.pool_stats.is_empty() {
        return clear_rebalance_status_refresh(current);
    }

    let before = current.clone();
    match current.as_mut() {
        Some(current_meta) => {
            if merge_rebalance_meta(current_meta, &persisted) == RebalanceMetaMergeOutcome::RejectedActiveConflict
                && !is_rebalance_actively_running(current_meta)
            {
                *current = Some(persisted);
            }
        }
        None => {
            *current = Some(persisted);
        }
    }

    match (before.as_ref(), current.as_ref()) {
        (None, None) => false,
        (None, Some(_)) | (Some(_), None) => true,
        (Some(before), Some(after)) => rebalance_movement_snapshot_changed(Some(before), after),
    }
}

fn clear_rebalance_status_refresh(current: &mut Option<RebalanceMeta>) -> bool {
    if current.as_ref().is_none_or(|meta| !is_rebalance_actively_running(meta)) {
        current.take().is_some()
    } else {
        false
    }
}

fn rebalance_movement_snapshot_changed(current: Option<&RebalanceMeta>, persisted: &RebalanceMeta) -> bool {
    let Some(current) = current else {
        return true;
    };

    current.id != persisted.id
        || current.stopped_at != persisted.stopped_at
        || current.pool_stats.len() != persisted.pool_stats.len()
        || current
            .pool_stats
            .iter()
            .zip(persisted.pool_stats.iter())
            .any(|(current, persisted)| {
                current.participating != persisted.participating
                    || current.info.status != persisted.info.status
                    || current.info.stopping != persisted.info.stopping
            })
}

impl ECStore {
    // Transition order is start_gate -> activation_gate -> movement gate -> rebalance_meta;
    // the probe read below is released before the activation gate.
    pub(super) async fn rebalance_activation_write_guard(
        &self,
        expected_id: Option<&str>,
        stage: &str,
    ) -> Result<Option<tokio::sync::OwnedRwLockWriteGuard<()>>> {
        let activation_gate = {
            let meta = self.rebalance_meta.read().await;
            if let Some(expected_id) = expected_id {
                ensure_rebalance_run_id(meta.as_ref(), expected_id, stage)?;
            }
            meta.as_ref().map(|meta| Arc::clone(&meta.activation_gate))
        };
        Ok(match activation_gate {
            Some(gate) => Some(gate.write_owned().await),
            None => None,
        })
    }

    pub(super) async fn rebalance_run_guard(&self, expected_id: &str, stage: &str) -> Result<RebalanceRunGuard> {
        // Runtime fence order is activation_gate -> rebalance.bin.
        let activation_gate = {
            let meta = self.rebalance_meta.read().await;
            ensure_rebalance_worker_active(meta.as_ref(), expected_id, stage)?;
            Arc::clone(
                &meta
                    .as_ref()
                    .ok_or_else(|| rebalance_metadata_not_initialized_error(stage))?
                    .activation_gate,
            )
        };
        let guard = Arc::clone(&activation_gate).read_owned().await;
        let meta = self.rebalance_meta.read().await;
        ensure_rebalance_worker_active(meta.as_ref(), expected_id, stage)?;
        let current_gate = &meta
            .as_ref()
            .ok_or_else(|| rebalance_metadata_not_initialized_error(stage))?
            .activation_gate;
        if !Arc::ptr_eq(&activation_gate, current_gate) {
            return Err(Error::other(format!(
                "stale rebalance activation gate rejected during {stage}: {expected_id}"
            )));
        }
        drop(meta);
        let pool = clone_first_arc(self.pools.as_slice(), "rebalance run fence: no pools available")?;
        let persisted_guard = acquire_persisted_rebalance_run_guard(pool, expected_id, stage).await?;
        Ok(RebalanceRunGuard {
            _local_guard: guard,
            persisted_guard,
        })
    }

    pub(super) async fn save_rebalance_meta_with_merge<S>(
        &self,
        pool: Arc<S>,
        local_snapshot: &RebalanceMeta,
        stage: &str,
    ) -> Result<()>
    where
        S: EcstoreObjectIO + StorageNamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>,
    {
        self.save_rebalance_meta_with_merge_for_id(pool, local_snapshot, stage, None)
            .await
    }

    pub(super) async fn save_rebalance_meta_for_id_with_merge<S>(
        &self,
        pool: Arc<S>,
        local_snapshot: &RebalanceMeta,
        stage: &str,
        expected_id: &str,
    ) -> Result<()>
    where
        S: EcstoreObjectIO + StorageNamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>,
    {
        self.save_rebalance_meta_with_merge_for_id(pool, local_snapshot, stage, Some(expected_id))
            .await
    }

    pub(super) async fn save_rebalance_meta_under_activation_fence<S>(
        &self,
        pool: Arc<S>,
        local_snapshot: &RebalanceMeta,
        stage: &str,
        activation_fence: &PoolRebalanceActivationFence,
        expected_id: &str,
    ) -> Result<()>
    where
        S: EcstoreObjectIO,
    {
        #[cfg(test)]
        {
            let mut fail_id = FAIL_NEXT_REBALANCE_ACTIVATION_SAVE
                .lock()
                .expect("rebalance activation save failure hook should not be poisoned");
            if fail_id.as_deref() == Some(local_snapshot.id.as_str()) {
                fail_id.take();
                return Err(Error::other("injected rebalance activation save failure"));
            }
        }
        merge_and_save_rebalance_meta_no_lock(
            pool,
            local_snapshot,
            stage,
            ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
            Some(activation_fence),
            Some(expected_id),
        )
        .await
    }

    async fn save_rebalance_meta_with_merge_for_id<S>(
        &self,
        pool: Arc<S>,
        local_snapshot: &RebalanceMeta,
        stage: &str,
        expected_id: Option<&str>,
    ) -> Result<()>
    where
        S: EcstoreObjectIO + StorageNamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>,
    {
        let ns_lock = pool.new_ns_lock(crate::disk::RUSTFS_META_BUCKET, REBAL_META_NAME).await?;
        let guard = ns_lock
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(|err| rebalance_meta_lock_error(err, "write"))?;
        let mut opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        opts.add_namespace_lock_guard(&guard);

        merge_and_save_rebalance_meta_no_lock(pool, local_snapshot, stage, opts, None, expected_id).await?;
        if guard.is_lock_lost() {
            return Err(Error::other("rebalance metadata lock lost during metadata commit"));
        }
        Ok(())
    }

    async fn save_rebalance_activation_meta_with_merge<S>(
        &self,
        pool: Arc<S>,
        local_snapshot: &RebalanceMeta,
        stage: &str,
    ) -> Result<()>
    where
        S: EcstoreObjectIO + StorageNamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>,
    {
        #[cfg(test)]
        crate::core::pools::observe_pool_activation_start_attempt(crate::core::pools::PoolActivationStartKind::Rebalance);
        let fleet_proof = acquire_pool_activation_fleet_proof(&self.ctx).await?;
        let mut pool_meta_guard = self.pool_meta_save_gate.lock().await;
        pool_meta_guard.ensure_write_safe(stage)?;
        let activation_fence = acquire_pool_rebalance_activation_locks(pool.clone(), fleet_proof).await?;
        let pool_meta = self
            .load_runtime_pool_meta_under_activation_fence(&mut pool_meta_guard, &activation_fence, stage)
            .await?;
        ensure_rebalance_activation_pool_meta_allowed(&pool_meta)?;

        merge_and_save_rebalance_meta_no_lock(
            pool,
            local_snapshot,
            stage,
            ObjectOptions {
                no_lock: true,
                ..Default::default()
            },
            Some(&activation_fence),
            None,
        )
        .await
    }

    pub(super) async fn fence_rebalance_worker_activation<S>(
        &self,
        pool: Arc<S>,
        expected_id: &str,
    ) -> Result<RebalanceWorkerActivationFence>
    where
        S: EcstoreObjectIO + StorageNamespaceLocking<Error = Error, NamespaceLock = rustfs_lock::NamespaceLockWrapper>,
    {
        // Lock order: pool_meta_save_gate -> pool.bin -> rebalance.bin.
        let mut pool_meta_guard = self.pool_meta_save_gate.lock().await;
        pool_meta_guard.ensure_write_safe("rebalance worker activation")?;
        // Classify the durable rebalance record while holding both namespace
        // fences. A terminal record is a no-op and must not depend on the
        // notification subsystem having published a fleet proof yet.
        let mut activation_fence = acquire_pool_rebalance_activation_locks(pool.clone(), None).await?;
        let pool_meta = self
            .load_runtime_pool_meta_under_activation_fence(&mut pool_meta_guard, &activation_fence, "rebalance worker activation")
            .await?;
        ensure_rebalance_activation_pool_meta_allowed(&pool_meta)?;

        let mut persisted = RebalanceMeta::new();
        persisted
            .load_with_opts(
                pool,
                ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await?;
        if persisted.id != expected_id {
            return Err(Error::other(format!(
                "rebalance metadata changed before worker activation: expected {expected_id}, found {}",
                persisted.id
            )));
        }

        activation_fence.ensure_held()?;
        if !crate::services::rebalance::rebalance_requires_worker_activation(&persisted) {
            return Ok(RebalanceWorkerActivationFence::NotStartedTerminal);
        }

        // Active worker admission still requires the fail-closed fleet proof.
        // Attach it immediately before the final fence validation so expiry or
        // topology changes are checked again at every later commit boundary.
        let fleet_proof = acquire_pool_activation_fleet_proof(&self.ctx).await?;
        activation_fence.set_fleet_proof(fleet_proof);
        activation_fence.ensure_held()?;

        Ok(RebalanceWorkerActivationFence::Ready(Box::new(activation_fence)))
    }

    #[tracing::instrument(skip_all)]
    pub async fn load_rebalance_meta(&self) -> Result<()> {
        let _start_guard = self.start_gate.lock().await;
        self.load_rebalance_meta_under_start_gate().await
    }

    /// Cancels local admission before refreshing the persisted stop target under the start gate.
    pub async fn prepare_rebalance_stop(&self) -> Result<Option<String>> {
        let _start_guard = self.start_gate.lock().await;

        {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            if let Some(meta) = rebalance_meta.as_mut()
                && is_rebalance_conflicting_with_decommission(meta)
            {
                meta.cancel
                    .get_or_insert_with(tokio_util::sync::CancellationToken::new)
                    .cancel();
                #[cfg(any(test, feature = "test-util"))]
                observe_rebalance_stop_wait_attempt(Some(meta.id.as_str()));
            }
        }

        self.load_rebalance_meta_under_start_gate().await?;

        let mut rebalance_meta = self.rebalance_meta.write().await;
        let Some(meta) = rebalance_meta.as_mut() else {
            return Ok(None);
        };
        if !is_rebalance_conflicting_with_decommission(meta) {
            return Ok(None);
        }
        if meta.id.is_empty() {
            return Err(Error::other("active rebalance metadata has no activation id"));
        }
        meta.cancel
            .get_or_insert_with(tokio_util::sync::CancellationToken::new)
            .cancel();
        Ok(Some(meta.id.clone()))
    }

    pub(crate) async fn load_rebalance_meta_under_start_gate(&self) -> Result<()> {
        let mut meta = RebalanceMeta::new();
        debug!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "metadata_loading",
            "Loading rebalance metadata"
        );
        let pool = clone_first_arc(&self.pools, "rebalanceMeta: no pools available")?;
        let _activation_guard = self.rebalance_activation_write_guard(None, "load rebalance metadata").await?;
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
        let loaded = resolve_rebalance_meta_load_result(meta.load(pool).await)?;
        if loaded {
            let movement_changed = rebalance_movement_snapshot_changed(self.rebalance_meta.read().await.as_ref(), &meta);
            {
                let mut rebalance_meta = self.rebalance_meta.write().await;

                *rebalance_meta = Some(meta);

                drop(rebalance_meta);
            }

            if movement_changed {
                self.ctx.advance_data_movement_operation_epoch();
            }
            drop(_movement_guard);
            resolve_load_rebalance_stats_update_result(self.update_rebalance_stats().await)?;
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "metadata_loaded",
                "Loaded rebalance metadata"
            );
        } else {
            let movement_changed = self.rebalance_meta.read().await.is_some();
            {
                let mut rebalance_meta = self.rebalance_meta.write().await;
                *rebalance_meta = None;
            }
            if movement_changed {
                self.ctx.advance_data_movement_operation_epoch();
            }
            drop(_movement_guard);
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "metadata_missing",
                reason = "rebalance_not_started",
                "Rebalance metadata not found"
            );
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn refresh_rebalance_status_meta(&self) -> Result<()> {
        let _start_guard = self.start_gate.lock().await;
        let pool = clone_first_arc(&self.pools, "refresh_rebalance_status_meta: no pools available")?;
        let mut persisted = RebalanceMeta::new();
        let _activation_guard = self
            .rebalance_activation_write_guard(None, "refresh rebalance metadata")
            .await?;
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
        let loaded = persisted.load(pool).await;
        match loaded {
            Ok(()) => {
                let mut rebalance_meta = self.rebalance_meta.write().await;
                if merge_rebalance_status_refresh(&mut rebalance_meta, persisted) {
                    self.ctx.advance_data_movement_operation_epoch();
                }
            }
            Err(Error::ConfigNotFound) => {
                let mut rebalance_meta = self.rebalance_meta.write().await;
                if clear_rebalance_status_refresh(&mut rebalance_meta) {
                    self.ctx.advance_data_movement_operation_epoch();
                }
            }
            Err(err) => {
                return Err(Error::other(format!("rebalance metadata refresh failed during pool status: {err}")));
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip_all)]
    pub async fn update_rebalance_stats(&self) -> Result<()> {
        let mut ok = false;

        let pool_stats = {
            let rebalance_meta = self.rebalance_meta.read().await;
            clone_rebalance_pool_stats(rebalance_meta.as_ref())?
        };

        debug!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_count = pool_stats.len(),
            "Refreshing rebalance stats snapshot"
        );

        for i in 0..self.pools.len() {
            if pool_stats.get(i).is_none() {
                let mut rebalance_meta = self.rebalance_meta.write().await;
                debug!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index = i,
                    state = "pool_stat_missing",
                    "Adding missing rebalance pool stats entry"
                );
                if let Some(meta) = rebalance_meta.as_mut() {
                    meta.pool_stats.push(RebalanceStats::default());
                }
                ok = true;
                drop(rebalance_meta);
            }
        }

        if ok {
            debug!(
                event = EVENT_REBALANCE_STATE,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_REBALANCE,
                state = "metadata_saving",
                "Saving rebalance metadata after stats refresh"
            );

            let rebalance_meta = self.rebalance_meta.read().await;
            if let Some(meta) = rebalance_meta.as_ref() {
                let pool = clone_first_arc(&self.pools, "update_rebalance_stats: no pools available")?;
                resolve_rebalance_meta_save_result(
                    self.save_rebalance_meta_for_id_with_merge(pool, meta, "update_rebalance_stats", meta.id.as_str())
                        .await,
                    "update_rebalance_stats",
                )?;
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn init_rebalance_meta(&self, bucktes: Vec<String>) -> Result<String> {
        info!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "initializing",
            bucket_count = bucktes.len(),
            "Initializing rebalance metadata"
        );
        let si = StorageAdminApi::storage_info(self).await;

        let mut disk_stats = vec![DiskStat::default(); self.pools.len()];

        let mut total_cap = 0;
        let mut total_free = 0;
        for disk in si.disks.iter() {
            if disk.pool_index < 0 || disk_stats.len() <= disk.pool_index as usize {
                continue;
            }

            total_cap += disk.total_space;
            total_free += disk.available_space;

            disk_stats[disk.pool_index as usize].total_space += disk.total_space;
            disk_stats[disk.pool_index as usize].available_space += disk.available_space;
        }

        #[cfg(test)]
        if let Some(overridden) = take_rebalance_disk_stats_override_for_test(self.id) {
            disk_stats = overridden;
            total_cap = disk_stats.iter().map(|stat| stat.total_space).sum();
            total_free = disk_stats.iter().map(|stat| stat.available_space).sum();
        }

        let percent_free_goal = percent_free_ratio(total_free, total_cap);
        validate_rebalance_disk_stats_coverage(&disk_stats)?;

        let mut pool_stats = Vec::with_capacity(self.pools.len());

        let now = OffsetDateTime::now_utc();

        for disk_stat in disk_stats.iter() {
            let mut pool_stat = RebalanceStats {
                init_free_space: disk_stat.available_space,
                init_capacity: disk_stat.total_space,
                buckets: bucktes.clone(),
                rebalanced_buckets: Vec::with_capacity(bucktes.len()),
                ..Default::default()
            };

            if should_pool_participate(disk_stat.available_space, disk_stat.total_space, percent_free_goal) {
                pool_stat.participating = true;
                pool_stat.info = RebalanceInfo {
                    start_time: Some(now),
                    status: RebalStatus::Started,
                    ..Default::default()
                };
            }

            pool_stats.push(pool_stat);
        }

        let meta = RebalanceMeta {
            id: Uuid::new_v4().to_string(),
            percent_free_goal,
            pool_stats,
            ..Default::default()
        };

        let pool = clone_first_arc(&self.pools, "init_rebalance_meta: no pools available")?;
        resolve_rebalance_meta_save_result(
            self.save_rebalance_activation_meta_with_merge(pool, &meta, "init_rebalance_meta")
                .await,
            "init_rebalance_meta",
        )?;

        info!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "metadata_initialized",
            bucket_count = bucktes.len(),
            "Rebalance metadata initialized"
        );

        let id = meta.id.clone();

        {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            *rebalance_meta = Some(meta);
            drop(rebalance_meta);
        }

        Ok(id)
    }

    #[tracing::instrument(skip(self, bucktes))]
    pub async fn init_and_start_rebalance(self: &Arc<Self>, bucktes: Vec<String>) -> Result<String> {
        let id = self.init_rebalance_start(bucktes).await?;
        if let Err(start_err) = self.start_rebalance_for_id(&id).await {
            if let Err(rollback_err) = self
                .rollback_rebalance_start_without_worker_for_id(Some(&id), start_err.to_string())
                .await
            {
                return Err(Error::other(format!(
                    "failed to start rebalance after metadata initialized for {id}: {start_err}; rollback failed: {rollback_err}"
                )));
            }

            return Err(Error::other(format!(
                "failed to start rebalance after metadata initialized for {id}; local metadata was finalized as failed: {start_err}"
            )));
        }

        Ok(id)
    }

    #[tracing::instrument(skip(self, bucktes))]
    pub async fn init_rebalance_start(self: &Arc<Self>, bucktes: Vec<String>) -> Result<String> {
        let _start_guard = self.start_gate.lock().await;
        {
            let decommission_running = self.is_decommission_running().await;
            let rebalance_meta = self.rebalance_meta.read().await;
            validate_init_rebalance_state(decommission_running, rebalance_meta.as_ref())?;
        }
        let _activation_guard = self
            .rebalance_activation_write_guard(None, "initialize replacement rebalance")
            .await?;
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;

        let id = self.init_rebalance_meta(bucktes).await?;
        self.ctx.advance_data_movement_operation_epoch();
        Ok(id)
    }

    #[tracing::instrument(skip(self))]
    pub async fn start_rebalance_for_id(self: &Arc<Self>, expected_id: &str) -> Result<()> {
        let _start_guard = self.start_gate.lock().await;
        let _activation_guard = self
            .rebalance_activation_write_guard(Some(expected_id), "start rebalance")
            .await?;
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;

        {
            let rebalance_meta = self.rebalance_meta.read().await;
            let Some(meta) = rebalance_meta.as_ref() else {
                return Err(Error::ConfigNotFound);
            };
            if meta.id != expected_id {
                return Err(Error::other(format!(
                    "rebalance metadata changed before start: expected {expected_id}, found {}",
                    meta.id
                )));
            }
            if meta.stopped_at.is_some() {
                return Err(Error::other(format!("rebalance {expected_id} was stopped before start")));
            }
        }

        if self.start_rebalance_under_gate().await? {
            self.ctx.advance_data_movement_operation_epoch();
        }
        Ok(())
    }

    pub async fn rollback_rebalance_start_for_id(self: &Arc<Self>, expected_id: Option<&str>, start_error: String) -> Result<()> {
        self.rollback_rebalance_start_without_worker_for_id(expected_id, start_error)
            .await
    }

    #[tracing::instrument(skip(self, fi))]
    pub async fn update_pool_stats(&self, pool_index: usize, bucket: String, fi: &FileInfo) -> Result<()> {
        self.update_pool_stats_batch(pool_index, bucket, &[fi]).await
    }

    #[tracing::instrument(skip(self, versions))]
    pub async fn update_pool_stats_batch(&self, pool_index: usize, bucket: String, versions: &[&FileInfo]) -> Result<()> {
        self.update_pool_stats_batch_for_id(pool_index, bucket, versions, None).await
    }

    pub(super) async fn update_pool_stats_batch_for_rebalance(
        &self,
        pool_index: usize,
        bucket: String,
        versions: &[&FileInfo],
        expected_id: &str,
    ) -> Result<()> {
        self.update_pool_stats_batch_for_id(pool_index, bucket, versions, Some(expected_id))
            .await
    }

    async fn update_pool_stats_batch_for_id(
        &self,
        pool_index: usize,
        bucket: String,
        versions: &[&FileInfo],
        expected_id: Option<&str>,
    ) -> Result<()> {
        if versions.is_empty() {
            return Ok(());
        }

        let mut rebalance_meta = self.rebalance_meta.write().await;
        if let Some(expected_id) = expected_id {
            ensure_rebalance_worker_active(rebalance_meta.as_ref(), expected_id, "update rebalance pool stats")?;
        }
        if let Some(meta) = rebalance_meta.as_mut() {
            if !should_accept_rebalance_stats_update(meta, pool_index) {
                return Ok(());
            }

            if let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) {
                pool_stat.update_batch(bucket, versions);
            }
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn next_rebal_bucket(&self, pool_index: usize, expected_id: &str) -> Result<Option<String>> {
        let rebalance_meta = self.rebalance_meta.read().await;
        ensure_rebalance_worker_active(rebalance_meta.as_ref(), expected_id, "next rebalance bucket")?;
        debug!(
            event = EVENT_REBALANCE_BUCKET,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            pool_index,
            has_meta = rebalance_meta.is_some(),
            state = "next_bucket_lookup",
            "Rebalance next bucket lookup"
        );
        resolve_next_rebalance_bucket(rebalance_meta.as_ref(), pool_index)
    }

    #[tracing::instrument(skip(self))]
    pub async fn bucket_rebalance_done(&self, pool_index: usize, bucket: String, expected_id: &str) -> Result<()> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        ensure_rebalance_worker_active(rebalance_meta.as_ref(), expected_id, "mark rebalance bucket done")?;
        mark_rebalance_bucket_done(rebalance_meta.as_mut(), pool_index, &bucket)
    }

    pub(super) async fn record_rebalance_cleanup_warning(
        &self,
        pool_index: usize,
        bucket: &str,
        object: &str,
        message: String,
        expected_id: &str,
    ) -> Result<()> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        ensure_rebalance_worker_active(rebalance_meta.as_ref(), expected_id, "record rebalance cleanup warning")?;
        record_rebalance_cleanup_warning_in_meta(
            rebalance_meta.as_mut(),
            pool_index,
            bucket,
            object,
            message,
            OffsetDateTime::now_utc(),
        )
    }

    pub(super) async fn defer_rebalance_bucket(
        &self,
        pool_index: usize,
        bucket: String,
        last_error: String,
        expected_id: &str,
    ) -> Result<()> {
        let mut rebalance_meta = self.rebalance_meta.write().await;
        ensure_rebalance_worker_active(rebalance_meta.as_ref(), expected_id, "defer rebalance bucket")?;
        let Some(meta) = rebalance_meta.as_mut() else {
            return Err(rebalance_metadata_not_initialized_error("defer rebalance bucket"));
        };
        let pool_count = meta.pool_stats.len();
        ensure_valid_rebalance_pool_index(pool_count, pool_index)?;
        let Some(pool_stat) = meta.pool_stats.get_mut(pool_index) else {
            return Err(invalid_rebalance_pool_index_error(pool_index, pool_count));
        };

        defer_bucket_in_rebalance_queue(pool_stat, &bucket)?;
        pool_stat.info.last_error = Some(last_error);
        meta.last_refreshed_at = Some(OffsetDateTime::now_utc());
        Ok(())
    }

    pub async fn is_rebalance_started(&self) -> bool {
        let rebalance_meta = self.rebalance_meta.read().await;
        if let Some(meta) = rebalance_meta.as_ref() {
            meta.pool_stats.iter().enumerate().for_each(|(i, v)| {
                debug!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    pool_index = i,
                    participating = v.participating,
                    status = ?v.info.status,
                    state = "status_inspected",
                    "Rebalance status inspected"
                );
            });

            let started = is_rebalance_conflicting_with_decommission(meta);
            if started {
                debug!(
                    event = EVENT_REBALANCE_STATE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_REBALANCE,
                    state = "running",
                    "Rebalance is running"
                );
                return true;
            }
        }

        debug!(
            event = EVENT_REBALANCE_STATE,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_REBALANCE,
            state = "not_running",
            "Rebalance is not running"
        );
        false
    }

    pub async fn is_rebalance_conflicting_with_decommission(&self) -> bool {
        let rebalance_meta = self.rebalance_meta.read().await;
        rebalance_meta
            .as_ref()
            .is_some_and(is_rebalance_conflicting_with_decommission)
    }

    pub async fn is_pool_rebalancing(&self, pool_index: usize) -> bool {
        let rebalance_meta = self.rebalance_meta.read().await;
        if let Some(ref meta) = *rebalance_meta {
            if meta.stopped_at.is_some() {
                return false;
            }

            if let Some(pool_stat) = meta.pool_stats.get(pool_index) {
                return pool_stat.participating && pool_stat.info.status == RebalStatus::Started;
            }
        }

        false
    }

    pub async fn pool_rebalance_status(&self, pool_index: usize) -> (RebalStatus, bool) {
        let rebalance_meta = self.rebalance_meta.read().await;
        pool_rebalance_status_from_meta(rebalance_meta.as_ref(), pool_index)
    }

    pub async fn current_rebalance_id(&self) -> Option<String> {
        let rebalance_meta = self.rebalance_meta.read().await;
        rebalance_meta
            .as_ref()
            .and_then(|meta| (!meta.id.is_empty()).then(|| meta.id.clone()))
    }

    pub async fn cancel_rebalance_admission_for_id(&self, expected_id: &str) -> Result<()> {
        let _start_guard = self.start_gate.lock().await;
        let mut rebalance_meta = self.rebalance_meta.write().await;
        ensure_rebalance_run_id(rebalance_meta.as_ref(), expected_id, "cancel rebalance admission")?;
        let meta = rebalance_meta
            .as_mut()
            .ok_or_else(|| rebalance_metadata_not_initialized_error("cancel rebalance admission"))?;
        if meta.stopped_at.is_some() || !is_rebalance_conflicting_with_decommission(meta) {
            return Err(Error::other(format!(
                "inactive rebalance rejected while cancelling admission: {expected_id}"
            )));
        }
        meta.cancel
            .get_or_insert_with(tokio_util::sync::CancellationToken::new)
            .cancel();
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub async fn stop_rebalance(self: &Arc<Self>) -> Result<()> {
        self.stop_rebalance_for_id(None).await
    }

    #[tracing::instrument(skip(self))]
    pub async fn stop_rebalance_for_id(self: &Arc<Self>, expected_id: Option<&str>) -> Result<()> {
        let _start_guard = self.start_gate.lock().await;
        let activation_gate = {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            if let Some(expected_id) = expected_id {
                ensure_rebalance_run_id(rebalance_meta.as_ref(), expected_id, "stop rebalance")?;
            }
            rebalance_meta.as_mut().map(|meta| {
                let cancel = meta.cancel.get_or_insert_with(tokio_util::sync::CancellationToken::new);
                cancel.cancel();
                Arc::clone(&meta.activation_gate)
            })
        };
        let _activation_guard = match activation_gate {
            Some(gate) => {
                #[cfg(any(test, feature = "test-util"))]
                observe_rebalance_stop_wait_attempt(expected_id);
                Some(gate.write_owned().await)
            }
            None => None,
        };
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
        let (previous_meta, meta_to_save) = {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            let previous_meta = rebalance_meta.clone();
            let meta_to_save =
                stop_rebalance_meta_snapshot_for_id(rebalance_meta.as_mut(), OffsetDateTime::now_utc(), expected_id)?;
            (previous_meta, meta_to_save)
        };

        if let Some(meta_to_save) = meta_to_save {
            let pool = clone_first_arc(self.pools.as_slice(), "stop_rebalance: no pools available")?;
            let save_result = resolve_rebalance_meta_save_result(
                self.save_rebalance_meta_for_id_with_merge(pool, &meta_to_save, "stop_rebalance", meta_to_save.id.as_str())
                    .await,
                "stop_rebalance",
            );
            if let Err(err) = save_result {
                *self.rebalance_meta.write().await = previous_meta;
                return Err(err);
            }
            self.ctx.advance_data_movement_operation_epoch();
        }

        Ok(())
    }

    async fn rollback_rebalance_start_without_worker_for_id(
        self: &Arc<Self>,
        expected_id: Option<&str>,
        start_error: String,
    ) -> Result<()> {
        let _start_guard = self.start_gate.lock().await;
        let _activation_guard = self
            .rebalance_activation_write_guard(expected_id, "rollback rebalance start")
            .await?;
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
        let meta_to_save = {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            rollback_rebalance_start_meta_snapshot_for_id(
                rebalance_meta.as_mut(),
                OffsetDateTime::now_utc(),
                expected_id,
                start_error,
            )
        };

        if let Some(meta_to_save) = meta_to_save {
            let pool = clone_first_arc(self.pools.as_slice(), "rollback_rebalance_start: no pools available")?;
            resolve_rebalance_meta_save_result(
                self.save_rebalance_meta_for_id_with_merge(
                    pool,
                    &meta_to_save,
                    "rollback_rebalance_start",
                    meta_to_save.id.as_str(),
                )
                .await,
                "rollback_rebalance_start",
            )?;
            self.ctx.advance_data_movement_operation_epoch();
        }

        Ok(())
    }

    pub async fn record_rebalance_stop_propagation(
        self: &Arc<Self>,
        expected_id: &str,
        record: RebalanceStopPropagationRecord,
    ) -> Result<()> {
        if !record.has_failures() {
            return Ok(());
        }

        let _start_guard = self.start_gate.lock().await;
        let _activation_guard = self
            .rebalance_activation_write_guard(Some(expected_id), "record rebalance stop propagation")
            .await?;
        let movement_gate = self.ctx.data_movement_operation_gate();
        let _movement_guard = movement_gate.write().await;
        let encoded_error = encode_rebalance_stop_propagation_record(&record);
        let meta_to_save = {
            let mut rebalance_meta = self.rebalance_meta.write().await;
            ensure_rebalance_run_id(rebalance_meta.as_ref(), expected_id, "record rebalance stop propagation")?;
            record_rebalance_stop_propagation_snapshot(rebalance_meta.as_mut(), encoded_error, OffsetDateTime::now_utc())
        };

        if let Some(meta_to_save) = meta_to_save {
            let pool = clone_first_arc(self.pools.as_slice(), "record_rebalance_stop_propagation: no pools available")?;
            resolve_rebalance_meta_save_result(
                self.save_rebalance_meta_for_id_with_merge(pool, &meta_to_save, "record_rebalance_stop_propagation", expected_id)
                    .await,
                "record_rebalance_stop_propagation",
            )?;
            self.ctx.advance_data_movement_operation_epoch();
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::com::delete_config;
    use crate::core::pools::{
        POOL_META_NAME, PoolActivationDurableSaveBarrier, PoolActivationStartKind, PoolActivationStartProbe, PoolMetaWriteState,
        persist_pool_meta_identity_for_startup,
    };
    use crate::object_api::NamespaceLockFence;
    use crate::set_disk::{PutObjectCommitBarrier, PutObjectCommitPause, hermetic_set_disks_isolated};

    async fn persist_initialized_identity_then_remove_pool_meta(store: &Arc<ECStore>) {
        let mut write_state = PoolMetaWriteState::for_startup(store.id, false);
        persist_pool_meta_identity_for_startup(store.pools.clone(), &mut write_state, true)
            .await
            .expect("initialized pool metadata identity should persist");
        *store.pool_meta_save_gate.lock().await = write_state;
        for pool in &store.pools {
            delete_config(pool.clone(), POOL_META_NAME)
                .await
                .expect("every pool metadata replica should be removed");
        }
    }

    async fn assert_activation_locks_released(store: &Arc<ECStore>) {
        let fleet_proof = acquire_pool_activation_fleet_proof(&store.ctx)
            .await
            .expect("fleet proof should remain available");
        tokio::time::timeout(
            std::time::Duration::from_secs(5),
            acquire_pool_rebalance_activation_locks(store.pools[0].clone(), fleet_proof),
        )
        .await
        .expect("a rejected activation must release namespace fences promptly")
        .expect("a rejected activation must release both namespace fences");
    }

    #[tokio::test]
    async fn rebalance_stop_wait_probe_matches_run_id() {
        let probe = RebalanceStopWaitProbe::install("rebalance-stop-current");

        observe_rebalance_stop_wait_attempt(Some("rebalance-stop-stale"));
        assert!(!probe.state.attempted.load(std::sync::atomic::Ordering::Acquire));

        observe_rebalance_stop_wait_attempt(Some("rebalance-stop-current"));
        probe.wait_until_attempted().await;
    }

    #[tokio::test]
    async fn cancel_rebalance_admission_is_id_checked_and_idempotent() {
        let rebalance_id = "rebalance-admission-current";
        let cancel = tokio_util::sync::CancellationToken::new();
        let (_temp_dirs, store) = crate::services::rebalance::test_store_with_persisted_rebalance_meta(RebalanceMeta {
            id: rebalance_id.to_string(),
            percent_free_goal: 1.0,
            cancel: Some(cancel.clone()),
            pool_stats: vec![RebalanceStats {
                participating: true,
                init_capacity: 100,
                info: RebalanceInfo {
                    start_time: Some(OffsetDateTime::now_utc()),
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        })
        .await;

        let error = store
            .cancel_rebalance_admission_for_id("rebalance-admission-replacement")
            .await
            .expect_err("a stale admin stop must not cancel the current run");
        assert!(error.to_string().contains("expected rebalance-admission-replacement"));
        assert!(!cancel.is_cancelled());

        store
            .cancel_rebalance_admission_for_id(rebalance_id)
            .await
            .expect("the current run admission should close");
        store
            .cancel_rebalance_admission_for_id(rebalance_id)
            .await
            .expect("retrying admission cancellation should be idempotent");
        assert!(cancel.is_cancelled());
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn rebalance_activation_rejects_initialized_cluster_with_all_pool_meta_missing() {
        let (_temp_dirs, store, _other_store) = crate::services::rebalance::test_two_pool_stores(None).await;
        persist_initialized_identity_then_remove_pool_meta(&store).await;
        set_rebalance_disk_stats_override_for_test(
            store.id,
            vec![
                DiskStat {
                    total_space: 100,
                    available_space: 0,
                },
                DiskStat {
                    total_space: 100,
                    available_space: 100,
                },
            ],
        );

        let err = store
            .init_rebalance_start(vec!["missing-pool-meta".to_string()])
            .await
            .expect_err("rebalance activation must fail closed when every pool.bin is missing");
        assert!(err.to_string().contains("initialized cluster identity exists"));
        store
            .ensure_pool_meta_side_effects_safe("rebalance activation after missing pool metadata")
            .await
            .expect_err("the missing metadata observation must latch the shared runtime gate");

        let mut persisted = RebalanceMeta::new();
        assert!(
            matches!(persisted.load(store.pools[0].clone()).await, Err(Error::ConfigNotFound)),
            "rejected activation must not create rebalance metadata"
        );
        assert_activation_locks_released(&store).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn rebalance_worker_rejects_initialized_cluster_with_all_pool_meta_missing() {
        let rebalance_id = "missing-pool-meta-worker";
        let active = RebalanceMeta {
            id: rebalance_id.to_string(),
            percent_free_goal: 0.5,
            pool_stats: vec![
                RebalanceStats {
                    participating: true,
                    init_capacity: 100,
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    participating: true,
                    init_capacity: 100,
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let (_temp_dirs, store, _other_store) = crate::services::rebalance::test_two_pool_stores(Some(active)).await;
        persist_initialized_identity_then_remove_pool_meta(&store).await;

        let err = match store
            .fence_rebalance_worker_activation(store.pools[0].clone(), rebalance_id)
            .await
        {
            Ok(_) => panic!("worker activation must not return a fence when every pool.bin is missing"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("initialized cluster identity exists"));
        store
            .ensure_pool_meta_side_effects_safe("rebalance worker after missing pool metadata")
            .await
            .expect_err("worker validation must latch the shared runtime gate");
        assert_activation_locks_released(&store).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn rebalance_worker_skips_terminal_metadata_without_fleet_proof() {
        let rebalance_id = "terminal-metadata-without-proof";
        let completed = RebalanceMeta {
            id: rebalance_id.to_string(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        let (_temp_dirs, store, _other_store) = crate::services::rebalance::test_two_pool_stores(Some(completed)).await;
        let _proof_guard = crate::services::notification_sys::without_cross_pool_fence_fleet_proof_for_test();

        let activation = store
            .fence_rebalance_worker_activation(store.pools[0].clone(), rebalance_id)
            .await
            .expect("terminal metadata should not require a fleet proof");
        assert!(matches!(activation, RebalanceWorkerActivationFence::NotStartedTerminal));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn rebalance_worker_still_requires_fleet_proof_for_active_metadata() {
        let rebalance_id = "active-metadata-without-proof";
        let active = RebalanceMeta {
            id: rebalance_id.to_string(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        let (_temp_dirs, store, _other_store) = crate::services::rebalance::test_two_pool_stores(Some(active)).await;
        let _proof_guard = crate::services::notification_sys::without_cross_pool_fence_fleet_proof_for_test();

        let err = match store
            .fence_rebalance_worker_activation(store.pools[0].clone(), rebalance_id)
            .await
        {
            Ok(_) => panic!("active metadata must not be admitted without a fleet proof"),
            Err(err) => err,
        };
        assert!(
            err.to_string()
                .contains("pool activation requires a live fleet capability proof")
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn rebalance_activation_adopts_commit_after_post_save_fence_loss() {
        let (_temp_dirs, store, _other_store) = crate::services::rebalance::test_two_pool_stores(None).await;
        set_rebalance_disk_stats_override_for_test(
            store.id,
            vec![
                DiskStat {
                    total_space: 100,
                    available_space: 0,
                },
                DiskStat {
                    total_space: 100,
                    available_space: 100,
                },
            ],
        );
        let barrier = PoolActivationDurableSaveBarrier::install(&store.pools[0]);
        let start_store = Arc::clone(&store);
        let start_task = tokio::spawn(async move {
            start_store
                .init_rebalance_start(vec!["post-commit-fence-loss".to_string()])
                .await
        });

        barrier.wait_until_paused().await;
        barrier.release_after_fence_loss();
        let rebalance_id = tokio::time::timeout(std::time::Duration::from_secs(30), start_task)
            .await
            .expect("rebalance activation should finish after its durable commit")
            .expect("rebalance activation task should not panic")
            .expect("post-commit fence loss must not report the committed activation as failed");

        let local_meta = store.rebalance_meta.read().await;
        let local = local_meta
            .as_ref()
            .expect("the committed rebalance metadata should be installed locally");
        assert_eq!(local.id, rebalance_id);
        assert!(is_rebalance_conflicting_with_decommission(local));
        drop(local_meta);

        let mut persisted = RebalanceMeta::new();
        persisted
            .load(store.pools[0].clone())
            .await
            .expect("the committed rebalance metadata should remain readable");
        assert_eq!(persisted.id, rebalance_id);
        assert!(is_rebalance_conflicting_with_decommission(&persisted));
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn rebalance_worker_admission_installs_committed_candidate_after_post_save_fence_loss() {
        let rebalance_id = "post-save-worker-admission";
        let active = RebalanceMeta {
            id: rebalance_id.to_string(),
            percent_free_goal: 0.5,
            pool_stats: vec![
                RebalanceStats {
                    participating: true,
                    init_free_space: 50,
                    init_capacity: 100,
                    buckets: vec!["completed-pool-bucket".to_string()],
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    participating: true,
                    init_capacity: 100,
                    buckets: vec!["active-pool-bucket".to_string()],
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let (_temp_dirs, store, _other_store) = crate::services::rebalance::test_two_pool_stores(Some(active)).await;
        let barrier = PoolActivationDurableSaveBarrier::install(&store.pools[0]);
        let start_store = Arc::clone(&store);
        let start_task = tokio::spawn(async move { start_store.start_rebalance().await });

        barrier.wait_until_paused().await;
        barrier.release_after_fence_loss();
        tokio::time::timeout(std::time::Duration::from_secs(30), start_task)
            .await
            .expect("rebalance worker admission should finish after its durable commit")
            .expect("rebalance worker admission task should not panic")
            .expect("post-commit fence loss must not reject the committed worker candidate");

        let local_meta = store.rebalance_meta.read().await;
        let local = local_meta
            .as_ref()
            .expect("the committed worker candidate should remain installed locally");
        assert_eq!(local.id, rebalance_id);
        assert_eq!(local.pool_stats[0].info.status, RebalStatus::Completed);
        let admitted_cancel = local
            .cancel
            .clone()
            .expect("the committed worker candidate should install its cancellation token");
        assert!(!admitted_cancel.is_cancelled());
        drop(local_meta);

        admitted_cancel.cancel();
        assert!(admitted_cancel.is_cancelled());

        let mut persisted = RebalanceMeta::new();
        persisted
            .load(store.pools[0].clone())
            .await
            .expect("the committed worker candidate should remain readable");
        assert_eq!(persisted.id, rebalance_id);
        assert_eq!(persisted.pool_stats[0].info.status, RebalStatus::Completed);
    }

    async fn assert_real_activation_start_race(paused_kind: PoolActivationStartKind) {
        let (_temp_dirs, rebalance_store, decommission_store) =
            crate::services::rebalance::test_two_pool_stores_with_isolated_node_contexts(None).await;
        let disk_stats = vec![
            DiskStat {
                total_space: 100,
                available_space: 0,
            },
            DiskStat {
                total_space: 100,
                available_space: 100,
            },
        ];
        set_rebalance_disk_stats_override_for_test(rebalance_store.id, disk_stats.clone());
        set_rebalance_disk_stats_override_for_test(decommission_store.id, disk_stats);
        crate::core::pools::set_decommission_space_info_override_for_test(
            decommission_store.id,
            vec![
                (
                    0,
                    crate::core::pools::PoolSpaceInfo {
                        free: 0,
                        total: 100,
                        used: 100,
                    },
                ),
                (
                    1,
                    crate::core::pools::PoolSpaceInfo {
                        free: 200,
                        total: 200,
                        used: 0,
                    },
                ),
            ],
        );
        let (first_object, competing_object, competing_kind) = match paused_kind {
            PoolActivationStartKind::Rebalance => {
                (REBAL_META_NAME, crate::core::pools::POOL_META_NAME, PoolActivationStartKind::Decommission)
            }
            PoolActivationStartKind::Decommission => {
                (crate::core::pools::POOL_META_NAME, REBAL_META_NAME, PoolActivationStartKind::Rebalance)
            }
        };
        let first_barrier = PutObjectCommitBarrier::install(
            crate::disk::RUSTFS_META_BUCKET,
            first_object,
            PutObjectCommitPause::BeforeQuotaRename,
        );
        let competing_barrier = PutObjectCommitBarrier::install(
            crate::disk::RUSTFS_META_BUCKET,
            competing_object,
            PutObjectCommitPause::BeforeQuotaRename,
        );

        let mut rebalance_task;
        let mut decommission_task;
        let mut observed_rebalance_result = None;
        let mut observed_decommission_result = None;
        let reached_activation = if paused_kind == PoolActivationStartKind::Rebalance {
            let store = Arc::clone(&rebalance_store);
            rebalance_task =
                tokio::spawn(async move { store.init_rebalance_start(vec!["bucket".to_string()]).await.map(|_| ()) });
            tokio::select! {
                _ = first_barrier.wait_until_paused() => {}
                result = &mut rebalance_task => {
                    panic!("real rebalance start finished before its activation commit: {:?}", result.expect("rebalance activation task should not panic"));
                }
            }
            let probe = PoolActivationStartProbe::install(competing_kind);
            let store = Arc::clone(&decommission_store);
            decommission_task = tokio::spawn(async move { store.start_decommission(vec![0]).await });
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                tokio::select! {
                    _ = probe.wait_until_attempted() => true,
                    result = &mut decommission_task => {
                        observed_decommission_result = Some(result.expect("decommission activation task should not panic"));
                        false
                    }
                }
            })
            .await
            .unwrap_or(false)
        } else {
            let store = Arc::clone(&decommission_store);
            decommission_task = tokio::spawn(async move { store.start_decommission(vec![0]).await });
            tokio::select! {
                _ = first_barrier.wait_until_paused() => {}
                result = &mut decommission_task => {
                    panic!("real decommission start finished before its activation commit: {:?}", result.expect("decommission activation task should not panic"));
                }
            }
            let probe = PoolActivationStartProbe::install(competing_kind);
            let store = Arc::clone(&rebalance_store);
            rebalance_task =
                tokio::spawn(async move { store.init_rebalance_start(vec!["bucket".to_string()]).await.map(|_| ()) });
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                tokio::select! {
                    _ = probe.wait_until_attempted() => true,
                    result = &mut rebalance_task => {
                        observed_rebalance_result = Some(result.expect("rebalance activation task should not panic"));
                        false
                    }
                }
            })
            .await
            .unwrap_or(false)
        };

        // A competing start may be fenced while refreshing persisted metadata,
        // before it reaches the activation save, or at the activation commit.
        let competing_reached_commit = if reached_activation {
            tokio::time::timeout(std::time::Duration::from_secs(15), async {
                if paused_kind == PoolActivationStartKind::Rebalance {
                    tokio::select! {
                        _ = competing_barrier.wait_until_paused() => true,
                        result = &mut decommission_task => {
                            observed_decommission_result = Some(result.expect("decommission activation task should not panic"));
                            false
                        }
                    }
                } else {
                    tokio::select! {
                        _ = competing_barrier.wait_until_paused() => true,
                        result = &mut rebalance_task => {
                            observed_rebalance_result = Some(result.expect("rebalance activation task should not panic"));
                            false
                        }
                    }
                }
            })
            .await
            .expect("competing activation should either finish or reach its commit")
        } else {
            false
        };
        if competing_reached_commit {
            competing_barrier.release();
        }
        drop(competing_barrier);
        first_barrier.release();
        drop(first_barrier);

        let rebalance_result = match observed_rebalance_result {
            Some(result) => result,
            None => tokio::time::timeout(std::time::Duration::from_secs(15), &mut rebalance_task)
                .await
                .expect("real rebalance activation should finish")
                .expect("rebalance activation task should not panic"),
        };
        let decommission_result = match observed_decommission_result {
            Some(result) => result,
            None => tokio::time::timeout(std::time::Duration::from_secs(15), &mut decommission_task)
                .await
                .expect("real decommission activation should finish")
                .expect("decommission activation task should not panic"),
        };
        assert!(
            !competing_reached_commit,
            "the competing real start reached its commit while the other activation lock was held"
        );
        assert_ne!(
            rebalance_result.is_ok(),
            decommission_result.is_ok(),
            "exactly one real activation entry may commit"
        );

        let mut persisted_rebalance = RebalanceMeta::new();
        let rebalance_committed = persisted_rebalance
            .load(rebalance_store.pools[0].clone())
            .await
            .is_ok_and(|_| is_rebalance_conflicting_with_decommission(&persisted_rebalance));
        let mut persisted_pool = PoolMeta::default();
        persisted_pool
            .load_no_lock_from_replicas(rebalance_store.pools.clone())
            .await
            .expect("persisted pool metadata should remain readable");
        let decommission_committed = pool_meta_has_active_decommission(&persisted_pool);
        assert_eq!(
            usize::from(rebalance_committed) + usize::from(decommission_committed),
            1,
            "at most one persisted activation state may be active"
        );
        assert_eq!(rebalance_committed, paused_kind == PoolActivationStartKind::Rebalance);
        assert_eq!(decommission_committed, paused_kind == PoolActivationStartKind::Decommission);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_rebalance_and_decommission_starts_commit_at_most_one_side() {
        temp_env::async_with_vars([(rustfs_config::ENV_OBJECT_LOCK_ACQUIRE_TIMEOUT, Some("1"))], async {
            assert_real_activation_start_race(PoolActivationStartKind::Rebalance).await;
            assert_real_activation_start_race(PoolActivationStartKind::Decommission).await;
        })
        .await;
    }

    #[test]
    fn rebalance_activation_rejects_persisted_decommission_despite_idle_local_snapshot() {
        let persisted = PoolMeta {
            pools: vec![crate::core::pools::PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission: Some(crate::core::pools::PoolDecommissionInfo {
                    start_time: Some(OffsetDateTime::UNIX_EPOCH),
                    ..Default::default()
                }),
            }],
            ..Default::default()
        };

        let err = ensure_rebalance_activation_pool_meta_allowed(&persisted)
            .expect_err("persisted decommission must block a stale rebalance admission");
        assert!(matches!(err, Error::DecommissionAlreadyRunning));
    }

    #[test]
    fn rebalance_activation_allows_persisted_idle_pool_meta() {
        assert!(ensure_rebalance_activation_pool_meta_allowed(&PoolMeta::default()).is_ok());
    }

    #[tokio::test]
    async fn rebalance_merge_save_does_not_commit_after_namespace_fence_loss() {
        let (_temp_dirs, _disk_stores, set_disks) = hermetic_set_disks_isolated(4).await;

        let persisted = RebalanceMeta {
            id: "rebalance-a".to_string(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                bytes: 1,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        persisted
            .save(set_disks.clone())
            .await
            .expect("baseline rebalance metadata should be saved");

        let mut update = persisted.clone();
        update.pool_stats[0].bytes = 999;
        let err = merge_and_save_rebalance_meta_no_lock(
            set_disks.clone(),
            &update,
            "lost namespace fence test",
            ObjectOptions {
                no_lock: true,
                namespace_lock_fence: Some(NamespaceLockFence::lost_for_test()),
                ..Default::default()
            },
            None,
            Some(persisted.id.as_str()),
        )
        .await
        .expect_err("lost namespace fence must reject the metadata commit");
        assert!(matches!(err, Error::NamespaceLockQuorumUnavailable { .. }));

        let mut after = RebalanceMeta::new();
        after
            .load(set_disks)
            .await
            .expect("baseline rebalance metadata should remain readable");
        assert_eq!(after.id, persisted.id);
        assert_eq!(after.pool_stats[0].bytes, 1);
    }

    #[tokio::test]
    async fn persisted_rebalance_run_fence_rejects_stale_active_node_after_remote_stop() {
        let (_temp_dirs, _disk_stores, set_disks) = hermetic_set_disks_isolated(4).await;
        let active = RebalanceMeta {
            id: "rebalance-a".to_string(),
            cancel: Some(tokio_util::sync::CancellationToken::new()),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };
        active
            .save(set_disks.clone())
            .await
            .expect("active rebalance metadata should be saved");
        ensure_rebalance_worker_active(Some(&active), active.id.as_str(), "stale node precondition")
            .expect("the stale node snapshot should still look active locally");

        let mut stopped = active.clone();
        stopped.stopped_at = Some(OffsetDateTime::now_utc());
        stopped.pool_stats[0].info.stopping = true;
        stopped
            .save(set_disks.clone())
            .await
            .expect("remote stop should replace persisted metadata");

        let err = acquire_persisted_rebalance_run_guard(set_disks, active.id.as_str(), "cross-node stale snapshot")
            .await
            .expect_err("persisted stop must fence a node that missed stop propagation");
        assert!(err.to_string().contains("inactive rebalance worker rejected"));
    }

    #[test]
    fn pool_rebalance_status_ignores_non_participating_pool_state() {
        let meta = RebalanceMeta {
            pool_stats: vec![
                RebalanceStats {
                    participating: false,
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        stopping: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                RebalanceStats {
                    participating: true,
                    info: RebalanceInfo {
                        status: RebalStatus::Started,
                        ..Default::default()
                    },
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        assert_eq!(pool_rebalance_status_from_meta(Some(&meta), 0), (RebalStatus::None, false));
        assert_eq!(pool_rebalance_status_from_meta(Some(&meta), 1), (RebalStatus::Started, false));
        assert_eq!(pool_rebalance_status_from_meta(Some(&meta), 2), (RebalStatus::None, false));
    }

    #[test]
    fn rebalance_status_refresh_applies_persisted_terminal_state() {
        let rebalance_id = "rebalance-id".to_string();
        let now = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let mut current = Some(RebalanceMeta {
            id: rebalance_id.clone(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });
        let persisted = RebalanceMeta {
            id: rebalance_id,
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    start_time: Some(now),
                    end_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(merge_rebalance_status_refresh(&mut current, persisted));

        let refreshed = current.as_ref().expect("refresh should keep rebalance metadata");
        assert_eq!(refreshed.pool_stats[0].info.status, RebalStatus::Completed);
        assert_eq!(refreshed.pool_stats[0].info.end_time, Some(now));
    }

    #[test]
    fn rebalance_status_refresh_preserves_runtime_cancel_token() {
        let rebalance_id = "rebalance-id".to_string();
        let now = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let mut current = Some(RebalanceMeta {
            id: rebalance_id.clone(),
            cancel: Some(tokio_util::sync::CancellationToken::new()),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });
        let persisted = RebalanceMeta {
            id: rebalance_id,
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        assert!(!merge_rebalance_status_refresh(&mut current, persisted));

        assert!(
            current.as_ref().and_then(|meta| meta.cancel.as_ref()).is_some(),
            "status refresh must not drop the runtime cancellation token"
        );
    }

    #[test]
    fn rebalance_status_refresh_preserves_local_active_different_id_conflict() {
        let now = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let mut current = Some(RebalanceMeta {
            id: "old-active-id".to_string(),
            cancel: Some(tokio_util::sync::CancellationToken::new()),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });
        let persisted = RebalanceMeta {
            id: "new-terminal-id".to_string(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    start_time: Some(now),
                    end_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        merge_rebalance_status_refresh(&mut current, persisted);

        let refreshed = current.as_ref().expect("local active metadata should remain visible");
        assert_eq!(refreshed.id, "old-active-id");
        assert_eq!(refreshed.pool_stats[0].info.status, RebalStatus::Started);
        assert!(
            refreshed.cancel.is_some(),
            "status refresh must not drop a live runtime cancellation token"
        );
    }

    #[test]
    fn rebalance_status_refresh_replaces_stale_memory_without_runtime_token() {
        let now = OffsetDateTime::from_unix_timestamp(1_000).expect("test timestamp should be valid");
        let mut current = Some(RebalanceMeta {
            id: "old-stale-id".to_string(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    start_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });
        let persisted = RebalanceMeta {
            id: "new-terminal-id".to_string(),
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    start_time: Some(now),
                    end_time: Some(now),
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        };

        merge_rebalance_status_refresh(&mut current, persisted);

        let refreshed = current.as_ref().expect("persisted metadata should replace stale memory");
        assert_eq!(refreshed.id, "new-terminal-id");
        assert_eq!(refreshed.pool_stats[0].info.status, RebalStatus::Completed);
        assert!(refreshed.cancel.is_none());
    }
}
