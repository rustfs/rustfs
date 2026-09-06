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

use super::*;
use crate::core::pools::{
    PoolMetaBootstrapAuthority, PoolMetaReplicaState, PoolMetaWriteState, load_pool_meta_identity_observing,
    local_decommission_queue_prefix, persist_pool_meta_identity_for_startup, pool_meta_has_active_decommission,
};
use crate::runtime::instance::InstanceContext;
use crate::runtime::sources as runtime_sources;
use crate::storage_api_contracts::object::EcstoreObjectIO;
use rustfs_config::server_config::KVS;
use rustfs_credentials::{RPC_SECRET_REQUIRED_OPERATOR_MESSAGE, try_get_rpc_token};
use std::future::Future;
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_STORE_INIT: &str = "store_init";
const EVENT_DECOMMISSION_RESUME_RETRY: &str = "decommission_resume_retry";
const EVENT_STORE_FORMAT_RETRY: &str = "store_format_retry";
const EVENT_ECSTORE_INIT_STATUS: &str = "ecstore_init_status";
const EVENT_STORE_RPC_SECRET_PREFLIGHT_FAILED: &str = "store_rpc_secret_preflight_failed";

fn pool_first_endpoint_is_local(pool: &crate::layout::endpoints::PoolEndpoints) -> bool {
    pool.endpoints.as_ref().first().is_some_and(|endpoint| endpoint.is_local)
}

fn startup_pool_drive_counts(endpoint_pools: &EndpointServerPools) -> Vec<usize> {
    endpoint_pools.as_ref().iter().map(|pool| pool.drives_per_set).collect()
}

fn resolve_startup_pool_defaults(endpoint_pools: &EndpointServerPools) -> Result<Vec<usize>> {
    resolve_startup_pool_defaults_with(endpoint_pools, ECStore::validate_startup_storage_class)
}

fn resolve_startup_pool_defaults_with(
    endpoint_pools: &EndpointServerPools,
    validate: impl FnOnce(&EndpointServerPools) -> Result<()>,
) -> Result<Vec<usize>> {
    validate(endpoint_pools)?;
    let drive_counts = startup_pool_drive_counts(endpoint_pools);
    drive_counts.into_iter().map(ec_drives_no_config).collect()
}

/// Fail fast when the topology spans remote nodes but no internode RPC secret
/// resolves. Every remote format read would otherwise fail client-side with
/// "No valid auth token" and startup would retry for minutes before dying with
/// a misleading "erasure read quorum" error (issues #4939, #5153).
fn preflight_startup_rpc_secret(endpoint_pools: &EndpointServerPools) -> Result<()> {
    preflight_startup_rpc_secret_with(endpoint_pools, try_get_rpc_token)
}

fn preflight_startup_rpc_secret_with(
    endpoint_pools: &EndpointServerPools,
    resolve_rpc_token: impl FnOnce() -> std::io::Result<String>,
) -> Result<()> {
    let has_remote_endpoint = endpoint_pools
        .as_ref()
        .iter()
        .flat_map(|pool| pool.endpoints.as_ref().iter())
        .any(|endpoint| !endpoint.is_local);

    if !has_remote_endpoint {
        return Ok(());
    }

    match resolve_rpc_token() {
        Ok(_) => Ok(()),
        Err(err) => {
            // The message prefix must stay aligned with the runtime log in
            // cluster/rpc/http_auth.rs: the log-analyzer `rpc-secret-resolution`
            // rule anchors on it, and this preflight aborts before that log
            // would ever be emitted.
            error!(
                event = EVENT_STORE_RPC_SECRET_PREFLIGHT_FAILED,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_STORE_INIT,
                "RPC auth secret resolution failed: {err}; {RPC_SECRET_REQUIRED_OPERATOR_MESSAGE}"
            );
            Err(Error::other(format!(
                "store init aborted: endpoints include remote nodes but {err}; {RPC_SECRET_REQUIRED_OPERATOR_MESSAGE}"
            )))
        }
    }
}

const LOCAL_DECOMMISSION_INITIAL_RESUME_DELAY: Duration = Duration::from_secs(60 * 3);
const LOCAL_DECOMMISSION_RESUME_RETRY_DELAY: Duration = Duration::from_secs(30);
const LOCAL_DECOMMISSION_WATCHDOG_INTERVAL: Duration = Duration::from_secs(30);
const LOCAL_DECOMMISSION_WATCHDOG_MAX_RETRY_DELAY: Duration = Duration::from_secs(60 * 5);
const REBALANCE_INITIAL_RESUME_DELAY: Duration = Duration::from_secs(10);
const REBALANCE_RESUME_RETRY_DELAY: Duration = Duration::from_secs(10);

fn should_retry_format_load(err: &Error) -> bool {
    !matches!(err, Error::CorruptedFormat)
}

fn should_auto_start_rebalance_after_init(decommission_running: bool, rebalance_resume_required: bool) -> bool {
    rebalance_resume_required && !decommission_running
}

fn should_defer_rebalance_auto_start(distributed: bool, fleet_proof_available: bool) -> bool {
    distributed && !fleet_proof_available
}

async fn wait_for_local_decommission_resume_delay(rx: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        _ = rx.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

fn local_decommission_watchdog_retry_delay(consecutive_failures: u32) -> Duration {
    let exponent = consecutive_failures.saturating_sub(1).min(4);
    LOCAL_DECOMMISSION_RESUME_RETRY_DELAY
        .saturating_mul(1_u32 << exponent)
        .min(LOCAL_DECOMMISSION_WATCHDOG_MAX_RETRY_DELAY)
}

async fn wait_for_rebalance_resume_delay(rx: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        _ = rx.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

async fn wait_for_rebalance_resume_retry(rx: &CancellationToken) -> bool {
    wait_for_rebalance_resume_delay(rx, REBALANCE_RESUME_RETRY_DELAY).await
}

fn resolve_store_init_stage_result(result: Result<()>, stage: &str) -> Result<()> {
    result.map_err(|err| Error::other(format!("store init failed during {stage}: {err}")))
}

async fn load_pool_meta_for_startup<S>(
    pools: Vec<Arc<S>>,
    write_state: &mut PoolMetaWriteState,
) -> Result<(PoolMeta, PoolMetaReplicaState)>
where
    S: EcstoreObjectIO,
{
    load_pool_meta_identity_observing(pools.clone(), write_state)
        .await
        .map_err(|err| Error::other(format!("store init failed during load_pool_meta_identity: {err}")))?;
    let mut meta = PoolMeta::default();
    let replica_state = meta
        .load_no_lock_from_replicas_observing(pools, write_state)
        .await
        .map_err(|err| Error::other(format!("store init failed during load_pool_meta: {err}")))?;
    write_state.observe_replicas(replica_state);
    write_state
        .ensure_missing_metadata_can_initialize()
        .map_err(|err| Error::other(format!("store init failed during classify_pool_meta_absence: {err}")))?;
    Ok((meta, replica_state))
}

async fn establish_pool_meta_bootstrap_identity_if_proven<S>(
    pools: Vec<Arc<S>>,
    write_state: &mut PoolMetaWriteState,
    elected_writer: bool,
) -> Result<()>
where
    S: EcstoreObjectIO,
{
    if elected_writer && write_state.bootstrap_identity_proven() {
        persist_pool_meta_identity_for_startup(pools, write_state, false).await?;
    }
    Ok(())
}

async fn save_validated_pool_meta_for_startup<S>(
    meta: &PoolMeta,
    pools: Vec<Arc<S>>,
    write_state: &mut PoolMetaWriteState,
) -> Result<PoolMeta>
where
    S: EcstoreObjectIO,
{
    meta.save_for_startup_observing(pools, write_state)
        .await
        .map_err(|err| Error::other(format!("store init failed during save_validated_pool_meta: {err}")))
}

async fn persist_pool_meta_for_startup_if_safe<S>(
    meta: &PoolMeta,
    pools: Vec<Arc<S>>,
    replica_state: PoolMetaReplicaState,
    write_state: &mut PoolMetaWriteState,
    topology_update: bool,
    elected_writer: bool,
) -> Result<PoolMeta>
where
    S: EcstoreObjectIO,
{
    if !elected_writer {
        return Ok(meta.clone());
    }
    let should_write = topology_update || (replica_state.needs_repair && replica_state.repair_write_safe);
    if topology_update {
        replica_state.ensure_write_safe("store init failed during save_validated_pool_meta")?;
    }
    if should_write || write_state.identity_requires_repair() {
        write_state.ensure_write_safe("store init failed during save_validated_pool_meta")?;
    }
    let mut committed = meta.clone();
    if should_write {
        if write_state.bootstrap_identity_proven() || write_state.identity_is_pending() {
            persist_pool_meta_identity_for_startup(pools.clone(), write_state, false)
                .await
                .map_err(|err| Error::other(format!("store init failed during prepare_pool_meta_identity: {err}")))?;
        }
        committed = save_validated_pool_meta_for_startup(meta, pools.clone(), write_state).await?;
    }
    if should_write || write_state.identity_requires_repair() {
        persist_pool_meta_identity_for_startup(pools, write_state, true)
            .await
            .map_err(|err| Error::other(format!("store init failed during commit_pool_meta_identity: {err}")))?;
    }
    Ok(committed)
}

async fn run_local_decommission_watchdog<F, Fut>(rx: CancellationToken, mut reconcile: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<()>>,
{
    let mut consecutive_failures = 0_u32;

    loop {
        if rx.is_cancelled() {
            return;
        }

        let delay = match reconcile().await {
            Ok(()) => {
                consecutive_failures = 0;
                LOCAL_DECOMMISSION_WATCHDOG_INTERVAL
            }
            Err(err) => {
                consecutive_failures = consecutive_failures.saturating_add(1);
                let retry_delay = local_decommission_watchdog_retry_delay(consecutive_failures);
                warn!(
                    event = EVENT_DECOMMISSION_RESUME_RETRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_STORE_INIT,
                    consecutive_failures,
                    retry_delay_secs = retry_delay.as_secs(),
                    error = %err,
                    "Retrying decommission worker recovery"
                );
                retry_delay
            }
        };

        if !wait_for_local_decommission_resume_delay(&rx, delay).await {
            return;
        }
    }
}

async fn supervise_local_decommission_after_init(store: Arc<ECStore>, rx: CancellationToken) {
    run_local_decommission_watchdog(rx.clone(), || {
        let store = store.clone();
        let worker_rx = rx.clone();
        async move {
            store
                .ensure_pool_meta_side_effects_safe("decommission worker recovery blocked while pool metadata requires recovery")
                .await?;
            if store.has_active_local_decommission_worker().await {
                return Ok(());
            }
            store.refresh_pool_status_meta().await?;
            store.spawn_missing_local_decommission_routines_with_token(worker_rx).await
        }
    })
    .await;
}

async fn resume_rebalance_after_init(store: Arc<ECStore>, rx: CancellationToken) {
    if !wait_for_rebalance_resume_delay(&rx, REBALANCE_INITIAL_RESUME_DELAY).await {
        return;
    }

    loop {
        if rx.is_cancelled() {
            return;
        }

        let resume_required = store
            .rebalance_meta
            .read()
            .await
            .as_ref()
            .is_some_and(crate::services::rebalance::rebalance_requires_worker_activation);
        if !resume_required {
            return;
        }

        if should_defer_rebalance_auto_start(
            store.ctx.is_dist_erasure().await,
            crate::services::notification_sys::acquire_cross_pool_fence_fleet_proof().is_some(),
        ) {
            if !wait_for_rebalance_resume_retry(&rx).await {
                return;
            }
            continue;
        }

        match store.start_rebalance().await {
            Ok(()) => return,
            Err(err) if crate::core::pools::is_pool_activation_fleet_proof_error(&err) => {
                warn!(
                    event = EVENT_ECSTORE_INIT_STATUS,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_STORE_INIT,
                    stage = "start_rebalance",
                    state = "retrying",
                    reason = "fleet_capability_proof_unavailable",
                    error = %err,
                    retry_delay_secs = REBALANCE_RESUME_RETRY_DELAY.as_secs(),
                    "Retrying deferred rebalance auto-start"
                );
                if !wait_for_rebalance_resume_retry(&rx).await {
                    return;
                }
            }
            Err(err) => {
                error!(
                    event = EVENT_ECSTORE_INIT_STATUS,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_STORE_INIT,
                    stage = "start_rebalance",
                    state = "failed",
                    reason = "deferred_resume_failed",
                    error = %err,
                    "Failed to resume rebalance after store initialization"
                );
                return;
            }
        }
    }
}

impl ECStore {
    /// Shutdown token owned by this store instance.
    pub fn background_cancel_token(&self) -> Option<CancellationToken> {
        self.ctx.background_cancel_token()
    }

    /// Validate topology and process storage-class overrides before any disk is opened.
    pub fn validate_startup_storage_class(endpoint_pools: &EndpointServerPools) -> Result<()> {
        let drive_counts = startup_pool_drive_counts(endpoint_pools);
        storageclass::lookup_config_for_pools(&KVS::new(), &drive_counts).map(|_| ())
    }

    #[allow(clippy::new_ret_no_self)]
    #[instrument(level = "debug", skip(endpoint_pools))]
    pub async fn new(address: SocketAddr, endpoint_pools: EndpointServerPools, ctx: CancellationToken) -> Result<Arc<Self>> {
        Self::new_with_instance_ctx(address, endpoint_pools, ctx, crate::runtime::instance::bootstrap_ctx()).await
    }

    /// Build a store around an explicit instance context (Phase 5 follow-up,
    /// backlog#1052). The legacy [`ECStore::new`] entry adopts the process
    /// bootstrap context, keeping single-instance startup byte-for-byte
    /// unchanged; a caller that owns its own context (a future second embedded
    /// server) passes it here so every construction-time write — pool sets,
    /// local-disk registry, deployment id — lands on that context instead of
    /// the shared bootstrap one.
    #[allow(clippy::new_ret_no_self)]
    #[instrument(level = "debug", skip(endpoint_pools, instance_ctx))]
    pub async fn new_with_instance_ctx(
        address: SocketAddr,
        endpoint_pools: EndpointServerPools,
        ctx: CancellationToken,
        instance_ctx: Arc<InstanceContext>,
    ) -> Result<Arc<Self>> {
        Box::pin(Self::new_with_instance_ctx_inner(address, endpoint_pools, ctx, instance_ctx)).await
    }

    async fn new_with_instance_ctx_inner(
        address: SocketAddr,
        endpoint_pools: EndpointServerPools,
        ctx: CancellationToken,
        instance_ctx: Arc<InstanceContext>,
    ) -> Result<Arc<Self>> {
        instance_ctx.bind_background_cancel_token(ctx.clone());

        // let layouts = DisksLayout::from_volumes(endpoints.as_slice())?;

        // Validate topology and environment overrides before opening any disk.
        // The values stored on SetDisks remain pure per-pool topology defaults;
        // payload writes use the runtime storage-class snapshot published later
        // from config before the store is marked ready.
        let default_pool_parities = resolve_startup_pool_defaults(&endpoint_pools)?;

        preflight_startup_rpc_secret(&endpoint_pools)?;

        let mut deployment_id = None;
        let mut pool_meta_bootstrap_authority = None;

        // let (endpoint_pools, _) = EndpointServerPools::create_server_endpoints(address.as_str(), &layouts)?;

        let mut pools = Vec::with_capacity(endpoint_pools.as_ref().len());
        let mut disk_map = HashMap::with_capacity(endpoint_pools.as_ref().len());

        let mut local_disks = Vec::new();

        debug!(
            event = EVENT_ECSTORE_INIT_STATUS,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_STORE_INIT,
            address = %address,
            "Initializing ECStore address"
        );
        let mut host = address.ip().to_string();
        if host.is_empty() {
            host = runtime_sources::rustfs_host().await
        }
        let mut port = address.port().to_string();
        if port.is_empty() {
            port = runtime_sources::rustfs_port().to_string()
        }
        debug!(
            event = EVENT_ECSTORE_INIT_STATUS,
            component = LOG_COMPONENT_ECSTORE,
            subsystem = LOG_SUBSYSTEM_STORE_INIT,
            host = %host,
            port = %port,
            "Initializing ECStore host"
        );
        init_local_peer(&endpoint_pools, &host, &port).await;

        // debug!("endpoint_pools: {:?}", endpoint_pools);

        for (i, pool_eps) in endpoint_pools.as_ref().iter().enumerate() {
            let pool_first_is_local = pool_first_endpoint_is_local(pool_eps);
            let parity_drives = default_pool_parities
                .get(i)
                .copied()
                .ok_or_else(|| Error::other(format!("store init failed to resolve default parity for pool {i}")))?;

            // validate_parity(parity_count, pool_eps.drives_per_set)?;

            // Build disks with health monitoring available, but do not start
            // periodic monitoring until format loading succeeds. Startup RPC
            // failures can still spawn recovery probes for peers that come up
            // after this node.
            let (mut disks, errs) = init_format::init_disks(
                &pool_eps.endpoints,
                &DiskOption {
                    cleanup: true,
                    health_check: true,
                },
            )
            .await;

            check_disk_fatal_errs(&errs)?;

            let loaded_format = {
                let mut times = 0;
                let mut interval = 1;
                loop {
                    match init_format::connect_load_init_formats_with_instance_ctx(
                        &instance_ctx,
                        pool_first_is_local,
                        &mut disks,
                        pool_eps.set_count,
                        pool_eps.drives_per_set,
                        deployment_id,
                    )
                    .await
                    {
                        Ok(fm) => break Ok(fm),
                        Err(e) if !should_retry_format_load(&e) => break Err(e),
                        // Wrap the final error if we are giving up
                        Err(e) if times >= 10 => {
                            break Err(Error::other(format!("store init failed to load formats after {times} retries: {e}")));
                        }
                        // Retrying so just drop the error
                        Err(_) => {}
                    }
                    times += 1;
                    if interval < 16 {
                        interval *= 2;
                    }
                    debug!(
                        event = EVENT_STORE_FORMAT_RETRY,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_STORE_INIT,
                        retry_count = times,
                        retry_delay_secs = interval,
                        "Retrying storage format load"
                    );
                    select! {
                        _ = tokio::signal::ctrl_c() => {
                            info!(
                                event = EVENT_STORE_FORMAT_RETRY,
                                component = LOG_COMPONENT_ECSTORE,
                                subsystem = LOG_SUBSYSTEM_STORE_INIT,
                                reason = "ctrl_c",
                                "Interrupted storage format retry loop"
                            );
                            exit(0);
                        }
                        _ = sleep(Duration::from_secs(interval)) => {
                        }
                    }
                    // After waiting for peers, clear transient faulty marks so the next attempt can open RPCs again
                    // (these `DiskStore` handles are reused; `is_faulty()` would otherwise short-circuit).
                    for disk in disks.iter().flatten() {
                        disk.reset_health_for_store_init_retry();
                    }
                }
            }?;
            pool_meta_bootstrap_authority = Some(pool_meta_bootstrap_authority.map_or(
                loaded_format.pool_meta_bootstrap_authority,
                |authority: PoolMetaBootstrapAuthority| {
                    authority.combine_across_pools(loaded_format.pool_meta_bootstrap_authority)
                },
            ));
            let fm = loaded_format.format;

            // Format loading succeeded, enable health monitoring on all disks
            for disk in disks.iter().flatten() {
                disk.enable_health_check();
            }

            if deployment_id.is_none() {
                deployment_id = Some(fm.id);
            }

            if deployment_id != Some(fm.id) {
                return Err(Error::other("store init failed: deployment IDs do not match across pools"));
            }

            if deployment_id.is_some_and(|id| id.is_nil()) {
                deployment_id = Some(Uuid::new_v4());
            }

            for disk in disks.iter() {
                if disk.is_some() && disk.as_ref().expect("operation should succeed").is_local() {
                    local_disks.push(disk.as_ref().expect("operation should succeed").clone());
                }
            }

            let sets = Sets::new_with_instance_ctx(disks.clone(), pool_eps, &fm, i, parity_drives, instance_ctx.clone()).await?;
            pools.push(sets);

            disk_map.insert(i, disks);
        }

        // Replace the local disk
        if !instance_ctx.is_dist_erasure().await {
            runtime_sources::record_local_disks(&instance_ctx, local_disks).await;
        }

        let deployment_id = deployment_id.ok_or_else(|| Error::other("store init failed: deployment id is not initialized"))?;
        let peer_sys = S3PeerSys::new_with_instance_ctx(&endpoint_pools, instance_ctx.clone());
        let mut pool_meta = PoolMeta::new(&pools, &PoolMeta::default());
        pool_meta.dont_save = true;
        let pool_meta_write_state = PoolMetaWriteState::for_startup_with_bootstrap_authority(
            deployment_id,
            pool_meta_bootstrap_authority.unwrap_or_default(),
        );

        let decommission_cancelers = RwLock::new(vec![None; pools.len()]);
        let ec = Arc::new(ECStore {
            id: deployment_id,
            disk_map,
            pools,
            peer_sys,
            pool_meta: RwLock::new(pool_meta),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers,
            start_gate: Mutex::new(()),
            pool_meta_save_gate: Mutex::new(pool_meta_write_state),
            // Adopt the caller's context (the process bootstrap one on the
            // legacy path) so startup writes (erasure type recorded before
            // this point) and later reads share one cell.
            ctx: instance_ctx.clone(),
            bucket_fence_registry: std::sync::Arc::default(),
        });

        // Only set it when this instance's deployment ID is not yet configured
        if instance_ctx.deployment_id().is_none() {
            instance_ctx.set_deployment_id(deployment_id);
        }

        let wait_sec = 5;
        let mut exit_count = 0;
        loop {
            if let Err(err) = ec.init(ctx.clone()).await {
                error!("init err: {}", err);
                error!("retry after  {} second", wait_sec);
                sleep(Duration::from_secs(wait_sec)).await;

                if exit_count > 10 {
                    return Err(Error::other("store init failed: init retry budget exhausted"));
                }

                exit_count += 1;

                continue;
            }

            break;
        }

        runtime_sources::publish_object_store(ec.clone()).await;

        Ok(ec)
    }

    #[instrument(level = "debug", skip(self, rx))]
    pub async fn init(self: &Arc<Self>, rx: CancellationToken) -> Result<()> {
        runtime_sources::ensure_boot_time().await;

        let should_persist_pool_meta = self
            .pools
            .first()
            .is_some_and(|pool| pool_first_endpoint_is_local(&pool.endpoints));
        let (meta, pool_meta_replica_state) = {
            let mut write_state = self.pool_meta_save_gate.lock().await;
            establish_pool_meta_bootstrap_identity_if_proven(self.pools.clone(), &mut write_state, should_persist_pool_meta)
                .await
                .map_err(|err| Error::other(format!("store init failed during establish_pool_meta_bootstrap_identity: {err}")))?;
            load_pool_meta_for_startup(self.pools.clone(), &mut write_state).await?
        };
        let update = meta.validate(self.pools.clone())?;
        let endpoints = runtime_sources::endpoint_pools_or_default();

        let mut installed_pool_meta = if update {
            PoolMeta::new(&self.pools, &meta)
        } else {
            meta.clone()
        };
        // Only one local node should persist validated pool metadata here; otherwise
        // distributed startup can race on the same lock and replay the prior init bug.
        {
            let mut write_state = self.pool_meta_save_gate.lock().await;
            installed_pool_meta = persist_pool_meta_for_startup_if_safe(
                &installed_pool_meta,
                self.pools.clone(),
                pool_meta_replica_state,
                &mut write_state,
                update,
                should_persist_pool_meta,
            )
            .await?;
        }

        {
            let mut pool_meta = self.pool_meta.write().await;
            *pool_meta = installed_pool_meta.clone();
        }

        resolve_store_init_stage_result(self.load_rebalance_meta().await, "load_rebalance_meta")?;
        let rebalance_resume_required = {
            let rebalance_meta = self.rebalance_meta.read().await;
            rebalance_meta
                .as_ref()
                .is_some_and(crate::services::rebalance::rebalance_requires_worker_activation)
        };
        let decommission_running =
            pool_meta_has_active_decommission(&installed_pool_meta) || self.is_decommission_running().await;
        let distributed = self.ctx.is_dist_erasure().await;
        let fleet_proof_available = crate::services::notification_sys::acquire_cross_pool_fence_fleet_proof().is_some();
        let mut rebalance_auto_start_deferred = false;
        if should_auto_start_rebalance_after_init(decommission_running, rebalance_resume_required) {
            if should_defer_rebalance_auto_start(distributed, fleet_proof_available) {
                rebalance_auto_start_deferred = true;
                warn!(
                    event = EVENT_ECSTORE_INIT_STATUS,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_STORE_INIT,
                    stage = "start_rebalance",
                    state = "deferred",
                    reason = "fleet_capability_proof_unavailable",
                    retry_delay_secs = REBALANCE_RESUME_RETRY_DELAY.as_secs(),
                    "Deferred rebalance auto-start until a live fleet capability proof is available"
                );
            } else if let Err(err) = self.start_rebalance().await {
                if crate::core::pools::is_pool_activation_fleet_proof_error(&err) {
                    rebalance_auto_start_deferred = true;
                    warn!(
                        event = EVENT_ECSTORE_INIT_STATUS,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_STORE_INIT,
                        stage = "start_rebalance",
                        state = "deferred",
                        reason = "fleet_capability_proof_changed",
                        error = %err,
                        retry_delay_secs = REBALANCE_RESUME_RETRY_DELAY.as_secs(),
                        "Deferred rebalance auto-start after the fleet capability proof changed"
                    );
                } else {
                    return resolve_store_init_stage_result(Err(err), "start_rebalance");
                }
            }
        } else if decommission_running && rebalance_resume_required {
            warn!(
                event = EVENT_ECSTORE_INIT_STATUS,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_STORE_INIT,
                stage = "start_rebalance",
                reason = "active_decommission",
                "Deferred rebalance auto-start during store init because decommission is active"
            );
        }

        // Initialize the storage-owned scanner publication state only after
        // both movement metadata sources have been loaded. SetDisks cache
        // writers remain fail-closed until this snapshot is available.
        let _ = self.scanner_data_usage_publication_blocked().await;

        let pools = installed_pool_meta.return_resumable_pools();
        let mut pool_indices = Vec::with_capacity(pools.len());

        for p in pools.iter() {
            if let Some(idx) = endpoints.get_pool_idx(&p.cmd_line) {
                pool_indices.push(idx);
            } else {
                return Err(Error::other(format!(
                    "store init failed to resolve resumable decommission pool `{}` from current endpoints",
                    p.cmd_line
                )));
            }
        }

        let local_pool_indices = local_decommission_queue_prefix(&endpoints, &pool_indices)?;
        let has_local_decommission_leadership = endpoints.as_ref().iter().any(pool_first_endpoint_is_local);
        let pool_meta_write_safe = self
            .ensure_pool_meta_side_effects_safe("decommission resume blocked while pool metadata requires recovery")
            .await
            .is_ok();
        if !pool_meta_replica_state.repair_write_safe || !pool_meta_write_safe {
            warn!(
                event = EVENT_DECOMMISSION_RESUME_RETRY,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_STORE_INIT,
                state = "blocked",
                pool_indices = ?local_pool_indices,
                reason = "pool_meta_write_blocked",
                "Decommission watchdog waiting for pool metadata recovery"
            );
        }
        if has_local_decommission_leadership {
            let store = self.clone();
            let decommission_rx = rx.clone();
            tokio::spawn(async move {
                if !wait_for_local_decommission_resume_delay(&decommission_rx, LOCAL_DECOMMISSION_INITIAL_RESUME_DELAY).await {
                    return;
                }
                supervise_local_decommission_after_init(store, decommission_rx).await;
            });
        }

        runtime_sources::init_bucket_monitor_for_current_endpoints();
        crate::bucket::bucket_target_sys::BucketTargetSys::get().start_heartbeat();

        init_background_expiry(self.clone()).await;
        crate::bucket::lifecycle::bucket_lifecycle_ops::init_background_stale_multipart_upload_cleanup(self.clone());

        TransitionState::init(self.clone()).await;
        crate::services::tier::tier::try_migrate_tiering_config(self.clone()).await;

        if let Err(err) = runtime_sources::init_tier_config_mgr(self.clone()).await {
            info!("TierConfigMgr init error: {}", err);
        }

        if rebalance_auto_start_deferred {
            let store = self.clone();
            tokio::spawn(resume_rebalance_after_init(store, rx));
        }

        Ok(())
    }

    pub fn init_local_disks() {}

    pub fn single_pool(&self) -> bool {
        self.pools.len() == 1
    }

    /// The set-local create-only check is atomic only when every object
    /// mutation uses that same, enabled namespace lock domain.
    pub fn supports_atomic_create_only_write_back(&self) -> bool {
        !self.ctx.lock_manager().is_disabled() && self.pools.len() == 1 && self.pools[0].disk_set.len() == 1
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LOCAL_DECOMMISSION_RESUME_RETRY_DELAY, LOCAL_DECOMMISSION_WATCHDOG_MAX_RETRY_DELAY, PoolMetaWriteState,
        establish_pool_meta_bootstrap_identity_if_proven, load_pool_meta_for_startup, local_decommission_watchdog_retry_delay,
        persist_pool_meta_for_startup_if_safe, pool_first_endpoint_is_local, pool_meta_has_active_decommission,
        preflight_startup_rpc_secret_with, resolve_startup_pool_defaults_with, resolve_store_init_stage_result,
        run_local_decommission_watchdog, save_validated_pool_meta_for_startup, should_auto_start_rebalance_after_init,
        should_defer_rebalance_auto_start, should_retry_format_load, wait_for_local_decommission_resume_delay,
    };
    use crate::core::pools::PoolMetaBootstrapAuthority;
    #[cfg(feature = "test-util")]
    use crate::disk::DiskAPI;
    #[cfg(feature = "test-util")]
    use crate::{
        bucket::lifecycle::{
            DurableIlmRecordCheckpoint, ILM_META_PREFIX, ValidatedDurableIlmRecord,
            bucket_lifecycle_ops::{ExpiryState, ManualTransitionRunOptions, recover_manual_transition_jobs_once},
            lifecycle::{TRANSITION_PENDING, TransitionOptions},
            manual_transition_job::{
                ManualTransitionJobRecord, ManualTransitionScopeAdmission, ManualTransitionTaskRecord,
                ManualTransitionWorkerResult, ManualTransitionWorkerResultRecord, manual_transition_job_record_object_name,
                manual_transition_scope_record_object_name, manual_transition_task_object_name,
                manual_transition_worker_result_object_name, manual_transition_worker_result_task_key,
            },
            recovery_control::{
                IlmRecoveryClassification, IlmRecoveryControl, IlmRecoveryControlIdentity, IlmRecoveryErrorCode,
                IlmRecoveryProtocol, MAX_RECOVERY_ATTEMPTS, list_recovery_controls, load_recovery_control,
                observe_recovery_source, recovery_control_record_object_name, save_recovery_control_if_absent,
            },
            recovery_disposition::{
                IlmRecoveryDispositionExecutionOutcome, IlmRecoveryDispositionState, RecoveryDispositionCrashStage,
                dry_run_recovery_disposition, execute_recovery_disposition, inject_recovery_disposition_crash_once,
                load_recovery_disposition,
            },
            recovery_disposition_runtime::garbage_collect_completed_recovery_disposition,
            recovery_export::{
                create_recovery_export, inspect_recovery_export_observation, load_recovery_export,
                recovery_export_record_object_name,
            },
            tier_delete_journal::{
                DecommissionCheckpointTargetFailureHook, TIER_DELETE_DISPATCH_MANIFEST_PREFIX, TIER_DELETE_JOURNAL_PREFIX,
                TierDeleteChunkTestBarrier, TierDeleteChunkTestStage, TierDeleteDispatchBatchLimitGuard,
                TierDeleteDispatchManifestState, TierDeleteDispatchMemberReadTestHook, TierDeleteDispatchMemberReadTestStage,
                TierDeleteDispatchRollbackTestHook, complete_tier_delete_dispatch, encode_tier_delete_journal_entry,
                install_test_tier_delete_dispatch_fixture, persist_tier_delete_journal_entry, prepare_tier_delete_dispatch,
                recover_test_tier_delete_dispatch_manifest, recover_test_tier_delete_dispatch_manifest_with_page_budget,
                recover_test_tier_delete_journal_pass_with_budget, recover_tier_delete_dispatch_manifests,
                recover_tier_delete_journal_entries, test_tier_delete_dispatch_manifest_checkpoint,
                test_tier_delete_dispatch_manifest_state, tier_delete_dispatch_manifest_operation_lock_held_for_test,
                tier_delete_dispatch_manifest_recovery_count_for_test, tier_delete_dispatch_manifest_recovery_inflight_for_test,
                tier_delete_journal_object_name,
            },
            tier_free_version_recovery::recover_tier_free_versions,
            tier_sweeper::{
                Jentry, TierDeleteJournalState, TierDeleteSourceIdentity, transitioned_delete_journal_entry_for_source,
            },
            transition_transaction::{
                TRANSITION_TRANSACTION_RECORD_PREFIX, TransitionCleanupDecision, TransitionCleanupProof, TransitionOperatorError,
                TransitionOperatorProbe, TransitionRecoveryClaimBarrier, TransitionRecoveryTerminalBarrier,
                TransitionRemoteVersion, TransitionSourceIdentity, TransitionSourceVersionMode, TransitionTransaction,
                TransitionTransactionInit, TransitionTransactionState, delete_transition_candidate_for_operator,
                finalize_missing_transition_transaction_for_operator, inspect_transition_recovery_retry_for_operator,
                inspect_transition_transaction_for_operator, load_transition_transaction_record,
                recover_transition_transaction_records, recover_transition_transaction_records_at,
                retry_transition_recovery_for_operator, save_transition_transaction_record,
                save_transition_transaction_record_if_current, transition_recovery_control_id,
                transition_transaction_record_object_name,
            },
            validate_durable_ilm_record,
        },
        bucket::metadata::{BUCKET_LIFECYCLE_CONFIG, BUCKET_VERSIONING_CONFIG},
        config::com,
        core::pools::DecomBucketInfo,
        data_movement::SourceCleanupDeleteBarrier,
        disk::{BUCKET_META_PREFIX, RUSTFS_META_BUCKET, STORAGE_FORMAT_FILE},
        runtime::{global::set_object_store_resolver, sources as runtime_sources},
        services::notification_sys::acquire_tier_delete_journal_fleet_proof,
        services::tier::{
            test_util::{MockWarmBackend, MockWarmOp, TransitionCleanupStoreBarrier, register_mock_tier},
            tier::{
                ERR_TIER_BACKEND_IN_USE, ERR_TIER_INVALID_CONFIG, TIER_CONFIG_FILE, TierConfigMgr,
                ensure_no_authoritative_tier_references_for_test, tier_config_candidate_digest,
            },
            tier_config::{TierConfig, TierType, TierWasabi},
            tier_mutation_intent::{
                TIER_MUTATION_INTENT_RECORD_PREFIX, TierMutationIntent, TierMutationIntentKind, TierMutationIntentState,
                TierMutationIntentTarget, advance_tier_mutation_intent_record_idempotent, delete_tier_mutation_intent_record,
                list_tier_mutation_intent_records, load_tier_mutation_intent_record, load_tier_mutation_intent_record_with_etag,
                save_tier_mutation_intent_record, save_tier_mutation_intent_record_if_current,
            },
            tier_mutation_peer::{TierMutationPeerError, TierMutationPeerState, handle_tier_mutation_peer_request},
            tier_probe_intent::{
                TierProbeIntent, TierProbeIntentState, TierProbeOperationIdentity, TierProbeOwnerFence, TierProbeRemoteVersion,
                delete_tier_probe_intent_record_if_current, load_tier_probe_intent_record,
                save_tier_probe_intent_record_if_absent, save_tier_probe_intent_record_if_current,
            },
            warm_backend::{TransitionCandidateProbe, WarmBackend},
        },
        set_disk::SetDiskTransitionUploadedCommitBarrier as TransitionUploadedCommitBarrier,
        storage_api_contracts::list::ListOperations as _,
    };
    #[cfg(feature = "test-util")]
    use crate::{bucket::replication::ReplicationObjectBridge, storage_api_contracts::bucket::DeleteBucketOptions};
    use crate::{
        bucket::replication::{ReplicationState, ReplicationStatusType, replication_statuses_map},
        core::pools::{
            DecommissionErasureLayout, DecommissionPoolCapacityInfo, POOL_META_IDENTITY_NAME, POOL_META_NAME, POOL_META_VERSION,
            PoolDecommissionInfo, PoolMeta, PoolStatus, pool_meta_identity_initialized_for_test,
            pool_meta_v3_commit_state_for_test, set_decommission_capacity_info_overrides_for_test,
        },
        disk::endpoint::Endpoint,
        error::{Error, Result, StorageError},
        io_support::rio::{WritePlan, compression_metadata_value},
        layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
        object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader},
        services::rebalance::{RebalStatus, RebalanceInfo, RebalanceMeta, RebalanceStats},
        storage_api_contracts::{
            bucket::{BucketOperations as _, MakeBucketOptions},
            multipart::MultipartOperations as _,
            object::{ObjectIO, ObjectOperations as _, ObjectToDelete},
            range::HTTPRangeSpec,
        },
    };
    #[cfg(feature = "test-util")]
    use futures::{StreamExt as _, TryStreamExt as _};
    use http::HeaderMap;
    use rustfs_config::server_config::KVS;
    #[cfg(feature = "test-util")]
    use rustfs_filemeta::{FileInfo, FileMeta};
    use rustfs_filemeta::{FileInfoVersions, MetaCacheEntry};
    #[cfg(feature = "test-util")]
    use rustfs_protos::{TIER_MUTATION_RPC_PROTOCOL_VERSION, TierMutationRpcPhase};
    use rustfs_rio::{Checksum, ChecksumType};
    #[cfg(feature = "test-util")]
    use rustfs_s3_client::transition_api::ReaderImpl;
    use rustfs_utils::{
        CompressionAlgorithm,
        http::{SUFFIX_COMPRESSION, insert_str},
    };
    use std::{
        collections::HashMap,
        future::Future,
        io::Cursor,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicUsize, Ordering},
        },
        time::Duration,
    };
    use time::OffsetDateTime;
    use tokio::io::AsyncReadExt;
    use tokio_util::sync::CancellationToken;

    fn run_large_stack_async_test<C, F>(name: &str, case: C)
    where
        C: FnOnce() -> F + Send + 'static,
        F: Future<Output = ()> + 'static,
    {
        const STACK_SIZE: usize = if cfg!(debug_assertions) {
            8 * rustfs_config::DEFAULT_THREAD_STACK_SIZE
        } else if cfg!(target_os = "macos") {
            2 * rustfs_config::DEFAULT_THREAD_STACK_SIZE
        } else {
            rustfs_config::DEFAULT_THREAD_STACK_SIZE
        };
        std::thread::Builder::new()
            .name(name.to_string())
            .stack_size(STACK_SIZE)
            .spawn(move || {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .thread_stack_size(STACK_SIZE)
                    .build()
                    .expect("large-stack store test runtime should build");
                runtime.block_on(case());
            })
            .expect("large-stack store test thread should spawn")
            .join()
            .expect("large-stack store test thread should complete");
    }
    use uuid::Uuid;

    #[test]
    fn new_with_instance_ctx_future_remains_stack_bounded() {
        let future = crate::store::ECStore::new_with_instance_ctx(
            std::net::SocketAddr::from(([127, 0, 0, 1], 0)),
            EndpointServerPools(Vec::new()),
            CancellationToken::new(),
            Arc::new(crate::runtime::instance::InstanceContext::new()),
        );
        let future_size = std::mem::size_of_val(&future);

        assert!(
            future_size <= 4 * 1024,
            "ECStore constructor future must remain stack-bounded; measured {future_size} bytes"
        );
    }

    fn startup_pool_meta_payload(meta: &PoolMeta) -> Vec<u8> {
        meta.encode_config_data_for_test().expect("pool metadata should encode")
    }

    #[derive(Debug)]
    struct StartupPoolMetaStorage {
        read_payload: Vec<u8>,
        read_error: bool,
        read_without_lock: AtomicBool,
        wrote_without_lock: AtomicBool,
        wrote_with_max_parity: AtomicBool,
        written_payload: Mutex<Option<Vec<u8>>>,
        revision: AtomicUsize,
        pool_meta_write_attempts: AtomicUsize,
        fail_pool_meta_write_at: AtomicUsize,
        pool_meta_written_versions: Mutex<Vec<u16>>,
        objects: Mutex<HashMap<String, (Vec<u8>, String)>>,
    }

    impl StartupPoolMetaStorage {
        fn new(read_payload: Vec<u8>) -> Self {
            let mut objects = HashMap::new();
            if !read_payload.is_empty() {
                objects.insert(POOL_META_NAME.to_string(), (read_payload.clone(), "startup-pool-meta-0".to_string()));
            }
            Self {
                read_payload,
                read_error: false,
                read_without_lock: AtomicBool::new(false),
                wrote_without_lock: AtomicBool::new(false),
                wrote_with_max_parity: AtomicBool::new(false),
                written_payload: Mutex::new(None),
                revision: AtomicUsize::new(0),
                pool_meta_write_attempts: AtomicUsize::new(0),
                fail_pool_meta_write_at: AtomicUsize::new(0),
                pool_meta_written_versions: Mutex::new(Vec::new()),
                objects: Mutex::new(objects),
            }
        }

        fn unreadable() -> Self {
            Self {
                read_payload: Vec::new(),
                read_error: true,
                read_without_lock: AtomicBool::new(false),
                wrote_without_lock: AtomicBool::new(false),
                wrote_with_max_parity: AtomicBool::new(false),
                written_payload: Mutex::new(None),
                revision: AtomicUsize::new(0),
                pool_meta_write_attempts: AtomicUsize::new(0),
                fail_pool_meta_write_at: AtomicUsize::new(0),
                pool_meta_written_versions: Mutex::new(Vec::new()),
                objects: Mutex::new(HashMap::new()),
            }
        }

        fn object_info(&self, bucket: &str, object: &str, size: usize, etag: String) -> ObjectInfo {
            ObjectInfo {
                bucket: bucket.to_string(),
                name: object.to_string(),
                size: size as i64,
                actual_size: size as i64,
                etag: Some(etag),
                ..Default::default()
            }
        }
    }

    #[async_trait::async_trait]
    impl ObjectIO for StartupPoolMetaStorage {
        type Error = Error;
        type RangeSpec = HTTPRangeSpec;
        type HeaderMap = HeaderMap;
        type ObjectOptions = ObjectOptions;
        type ObjectInfo = ObjectInfo;
        type GetObjectReader = GetObjectReader;
        type PutObjectReader = PutObjReader;

        async fn get_object_reader(
            &self,
            bucket: &str,
            object: &str,
            _range: Option<HTTPRangeSpec>,
            _h: HeaderMap,
            opts: &ObjectOptions,
        ) -> Result<GetObjectReader> {
            assert!(opts.no_lock, "store init pool metadata load must not require namespace locks");
            self.read_without_lock.store(true, Ordering::SeqCst);
            if self.read_error {
                return Err(Error::other("pool metadata read quorum unavailable"));
            }
            let Some((payload, etag)) = self
                .objects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(object)
                .cloned()
            else {
                return Err(Error::FileNotFound);
            };

            Ok(GetObjectReader {
                stream: Box::new(Cursor::new(payload.clone())),
                object_info: self.object_info(bucket, object, payload.len(), etag),
                buffered_body: None,
                body_source: Default::default(),
            })
        }

        async fn put_object(
            &self,
            bucket: &str,
            object: &str,
            data: &mut PutObjReader,
            opts: &ObjectOptions,
        ) -> Result<ObjectInfo> {
            assert!(opts.no_lock, "store init pool metadata save must not require namespace locks");
            self.wrote_without_lock.store(true, Ordering::SeqCst);
            self.wrote_with_max_parity.store(opts.max_parity, Ordering::SeqCst);
            if object == POOL_META_NAME {
                let attempt = self.pool_meta_write_attempts.fetch_add(1, Ordering::SeqCst) + 1;
                if self.fail_pool_meta_write_at.load(Ordering::SeqCst) == attempt {
                    return Err(Error::Timeout);
                }
            }
            let current_etag = self
                .objects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(object)
                .map(|(_, etag)| etag.clone());
            if opts
                .http_preconditions
                .as_ref()
                .and_then(|preconditions| preconditions.if_none_match_value())
                == Some("*")
                && current_etag.is_some()
            {
                return Err(Error::PreconditionFailed);
            }
            if let Some(expected) = opts
                .http_preconditions
                .as_ref()
                .and_then(|preconditions| preconditions.if_match_value())
                && current_etag.as_deref() != Some(expected)
            {
                return Err(Error::PreconditionFailed);
            }
            let mut payload = Vec::new();
            data.stream.read_to_end(&mut payload).await?;
            let size = payload.len();
            if object == POOL_META_NAME {
                *self.written_payload.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(payload.clone());
                if payload.len() >= 4 {
                    self.pool_meta_written_versions
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .push(u16::from_le_bytes([payload[2], payload[3]]));
                }
            }
            let etag = format!("startup-pool-meta-{}", self.revision.fetch_add(1, Ordering::SeqCst) + 1);
            self.objects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(object.to_string(), (payload, etag.clone()));
            Ok(self.object_info(bucket, object, size, etag))
        }
    }

    fn init_test_pool_meta(decommission: Option<PoolDecommissionInfo>) -> PoolMeta {
        PoolMeta {
            version: POOL_META_VERSION,
            pools: vec![PoolStatus {
                id: 0,
                cmd_line: "pool-0".to_string(),
                last_update: OffsetDateTime::UNIX_EPOCH,
                decommission,
            }],
            dont_save: false,
        }
    }

    #[tokio::test]
    async fn test_fresh_bootstrap_pool_meta_save_reaches_v3_cas_without_blocking_itself() {
        let storage = Arc::new(StartupPoolMetaStorage::new(Vec::new()));
        let mut write_state = PoolMetaWriteState::for_startup(Uuid::new_v4(), true);
        establish_pool_meta_bootstrap_identity_if_proven(vec![storage.clone()], &mut write_state, true)
            .await
            .expect("the elected fresh bootstrap should persist its identity before loading pool metadata");

        let (loaded, replica_state) = load_pool_meta_for_startup(vec![storage.clone()], &mut write_state)
            .await
            .expect("startup pool metadata load should tolerate missing metadata without locks");
        assert!(loaded.pools.is_empty());
        assert!(!replica_state.needs_repair);
        assert!(replica_state.repair_write_safe);
        assert!(storage.read_without_lock.load(Ordering::SeqCst));

        let meta = PoolMeta {
            version: POOL_META_VERSION,
            pools: Vec::new(),
            dont_save: false,
        };
        save_validated_pool_meta_for_startup(&meta, vec![storage.clone()], &mut write_state)
            .await
            .expect("startup pool metadata save should bypass locks");
        assert!(storage.wrote_without_lock.load(Ordering::SeqCst));
        assert!(storage.wrote_with_max_parity.load(Ordering::SeqCst));
        assert_eq!(
            storage.pool_meta_write_attempts.load(Ordering::SeqCst),
            2,
            "fresh bootstrap should reach both V3 prepare and commit CAS writes"
        );
        assert_eq!(
            *storage
                .pool_meta_written_versions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
            vec![3, 3]
        );
        write_state
            .ensure_write_safe("fresh bootstrap publication")
            .expect("successful startup publication must disarm the transaction guard");
    }

    #[tokio::test]
    async fn test_legacy_adoption_pool_meta_bootstrap_reaches_v3_cas() {
        let deployment_id = Uuid::new_v4();
        let storage = Arc::new(StartupPoolMetaStorage::new(Vec::new()));
        let mut write_state =
            PoolMetaWriteState::for_startup_with_bootstrap_authority(deployment_id, PoolMetaBootstrapAuthority::LegacyAdoption);

        establish_pool_meta_bootstrap_identity_if_proven(vec![storage.clone()], &mut write_state, true)
            .await
            .expect("verified legacy adoption should persist a nonce-bound identity before loading pool metadata");
        let (loaded, replica_state) = load_pool_meta_for_startup(vec![storage.clone()], &mut write_state)
            .await
            .expect("verified legacy adoption should authorize initially missing pool metadata");
        assert!(loaded.pools.is_empty());

        let committed = persist_pool_meta_for_startup_if_safe(
            &init_test_pool_meta(None),
            vec![storage.clone()],
            replica_state,
            &mut write_state,
            true,
            true,
        )
        .await
        .expect("verified legacy adoption should publish initial pool metadata");
        assert_eq!(committed.pools[0].cmd_line, "pool-0");

        let objects = storage.objects.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
        assert!(objects.contains_key(POOL_META_NAME));
        let identity = objects
            .get(POOL_META_IDENTITY_NAME)
            .map(|(payload, _)| payload.clone())
            .expect("legacy adoption should commit the bootstrap identity");
        assert!(pool_meta_identity_initialized_for_test(&identity).expect("decode committed identity"));
    }

    #[tokio::test]
    async fn test_non_elected_legacy_adoption_cannot_initialize_pool_meta() {
        let storage = Arc::new(StartupPoolMetaStorage::new(Vec::new()));
        let mut write_state =
            PoolMetaWriteState::for_startup_with_bootstrap_authority(Uuid::new_v4(), PoolMetaBootstrapAuthority::LegacyAdoption);

        establish_pool_meta_bootstrap_identity_if_proven(vec![storage.clone()], &mut write_state, false)
            .await
            .expect("a non-elected distributed node must not create legacy adoption authority");
        let err = load_pool_meta_for_startup(vec![storage.clone()], &mut write_state)
            .await
            .expect_err("legacy adoption still requires the elected writer to create a durable identity");
        assert!(err.to_string().contains("no durable bootstrap identity"));
        assert!(
            !storage
                .objects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(POOL_META_NAME),
            "classification must not create pool metadata on non-elected nodes"
        );
    }

    #[tokio::test]
    async fn test_unproven_pending_identity_cannot_authorize_all_missing_pool_meta() {
        let deployment_id = Uuid::new_v4();
        let storage = Arc::new(StartupPoolMetaStorage::new(Vec::new()));
        let mut proven_bootstrap = PoolMetaWriteState::for_startup(deployment_id, true);
        establish_pool_meta_bootstrap_identity_if_proven(vec![storage.clone()], &mut proven_bootstrap, true)
            .await
            .expect("fresh topology proof should persist a nonce-bound pending identity");

        let identity = storage
            .objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(POOL_META_IDENTITY_NAME)
            .map(|(payload, _)| payload.clone())
            .expect("pending identity should be durable");
        assert!(!pool_meta_identity_initialized_for_test(&identity).expect("decode pending identity"));

        let mut unproven_restart = PoolMetaWriteState::for_startup(deployment_id, false);
        let err = load_pool_meta_for_startup(vec![storage.clone()], &mut unproven_restart)
            .await
            .expect_err("a pending identity alone must not authorize an all-missing restart");
        assert!(err.to_string().contains("no verified fresh-bootstrap proof"));
        unproven_restart
            .ensure_write_safe("unproven pending identity restart")
            .expect_err("the rejected restart must latch the write gate");
        assert!(
            !storage
                .objects
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .contains_key(POOL_META_NAME),
            "classification must not create pool metadata"
        );
    }

    #[tokio::test]
    async fn test_nonfresh_identity_repair_crash_never_persists_pending_bootstrap_authority() {
        let deployment_id = Uuid::new_v4();
        let initial = init_test_pool_meta(None);
        let storage = Arc::new(StartupPoolMetaStorage::new(startup_pool_meta_payload(&initial)));
        let mut write_state = PoolMetaWriteState::for_startup(deployment_id, false);
        let (loaded, replica_state) = load_pool_meta_for_startup(vec![storage.clone()], &mut write_state)
            .await
            .expect("an existing pool metadata snapshot may repair its missing identity");

        storage.fail_pool_meta_write_at.store(1, Ordering::SeqCst);
        persist_pool_meta_for_startup_if_safe(&loaded, vec![storage.clone()], replica_state, &mut write_state, true, true)
            .await
            .expect_err("the injected crash boundary should stop before the pool metadata replacement");

        let identity = storage
            .objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(POOL_META_IDENTITY_NAME)
            .map(|(payload, _)| payload.clone())
            .expect("identity repair should be durable before the failed topology write");
        assert!(
            pool_meta_identity_initialized_for_test(&identity).expect("decode repaired identity"),
            "an existing cluster must never leave pending bootstrap authority at this crash boundary"
        );

        storage
            .objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(POOL_META_NAME);
        let mut restarted = PoolMetaWriteState::for_startup(deployment_id, false);
        let err = load_pool_meta_for_startup(vec![storage], &mut restarted)
            .await
            .expect_err("wiping pool metadata after the crash must require recovery");
        assert!(err.to_string().contains("initialized cluster identity exists"));
    }

    #[tokio::test]
    #[serial_test::serial(pool_meta_version_env)]
    async fn test_existing_legacy_identity_and_replica_repair_respects_disabled_v3_gates() {
        temp_env::async_with_vars(
            [
                (rustfs_config::ENV_POOL_META_V3_WRITE, None::<&str>),
                (rustfs_config::ENV_POOL_META_V3_FLEET_CONFIRMED, None::<&str>),
            ],
            async {
                let deployment_id = Uuid::new_v4();
                let corrupt = Arc::new(StartupPoolMetaStorage::new(vec![0, 1, 2]));
                let legacy = init_test_pool_meta(None);
                let valid = Arc::new(StartupPoolMetaStorage::new(startup_pool_meta_payload(&legacy)));
                let mut write_state = PoolMetaWriteState::for_startup(deployment_id, false);
                let (loaded, replica_state) = load_pool_meta_for_startup(vec![corrupt.clone(), valid.clone()], &mut write_state)
                    .await
                    .expect("existing V2 metadata should remain readable while its identity is missing");
                assert!(replica_state.needs_repair);

                let committed = persist_pool_meta_for_startup_if_safe(
                    &loaded,
                    vec![corrupt.clone(), valid.clone()],
                    replica_state,
                    &mut write_state,
                    true,
                    true,
                )
                .await
                .expect("legacy replica and identity repair should succeed without crossing the V3 gate");
                assert_eq!(committed.version, POOL_META_VERSION);
                for storage in [corrupt, valid] {
                    assert_eq!(
                        *storage
                            .pool_meta_written_versions
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner),
                        vec![POOL_META_VERSION],
                        "legacy repair must write exactly one V2 snapshot"
                    );
                    let identity = storage
                        .objects
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get(POOL_META_IDENTITY_NAME)
                        .map(|(payload, _)| payload.clone())
                        .expect("identity repair should persist on every pool");
                    assert!(pool_meta_identity_initialized_for_test(&identity).expect("decode repaired identity"));
                }
            },
        )
        .await;
    }

    #[tokio::test]
    async fn test_store_init_distinguishes_fresh_deployment_from_wiped_lagging_node() {
        let deployment_id = Uuid::new_v4();
        let storage = Arc::new(StartupPoolMetaStorage::new(Vec::new()));
        let mut fresh_state = PoolMetaWriteState::for_startup(deployment_id, true);
        establish_pool_meta_bootstrap_identity_if_proven(vec![storage.clone()], &mut fresh_state, true)
            .await
            .expect("fresh topology proof should become a durable pending identity");
        let (_, replica_state) = load_pool_meta_for_startup(vec![storage.clone()], &mut fresh_state)
            .await
            .expect("new formats with no identity may initialize pool metadata exactly once");
        let committed = persist_pool_meta_for_startup_if_safe(
            &init_test_pool_meta(None),
            vec![storage.clone()],
            replica_state,
            &mut fresh_state,
            true,
            true,
        )
        .await
        .expect("fresh deployment should durably commit identity and pool metadata");
        assert_eq!(committed.pools[0].cmd_line, "pool-0");
        {
            let objects = storage.objects.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            assert!(objects.contains_key(POOL_META_NAME));
            assert!(objects.contains_key(POOL_META_IDENTITY_NAME));
        }

        storage
            .objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(POOL_META_NAME);
        let mut wiped_pool_state = PoolMetaWriteState::for_startup(deployment_id, false);
        let err = load_pool_meta_for_startup(vec![storage.clone()], &mut wiped_pool_state)
            .await
            .expect_err("an initialized identity must prevent an empty pool metadata rebuild");
        assert!(err.to_string().contains("initialized cluster identity exists"));

        storage
            .objects
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(POOL_META_IDENTITY_NAME);
        let mut wiped_all_state = PoolMetaWriteState::for_startup(deployment_id, false);
        let err = load_pool_meta_for_startup(vec![storage], &mut wiped_all_state)
            .await
            .expect_err("existing formats without identity or pool metadata must require recovery");
        assert!(err.to_string().contains("no durable bootstrap identity"));

        let distributed_node = Arc::new(StartupPoolMetaStorage::new(Vec::new()));
        let mut non_elected_state = PoolMetaWriteState::for_startup(Uuid::new_v4(), true);
        establish_pool_meta_bootstrap_identity_if_proven(vec![distributed_node.clone()], &mut non_elected_state, false)
            .await
            .expect("a non-elected distributed node must not create bootstrap authority");
        let err = load_pool_meta_for_startup(vec![distributed_node], &mut non_elected_state)
            .await
            .expect_err("a distributed node without durable identity or pool metadata must require recovery");
        assert!(err.to_string().contains("no durable bootstrap identity"));
    }

    #[tokio::test]
    async fn test_store_init_resumes_pending_v3_bootstrap_without_legacy_overwrite() {
        let deployment_id = Uuid::new_v4();
        let storage = Arc::new(StartupPoolMetaStorage::new(Vec::new()));
        let mut first_bootstrap = PoolMetaWriteState::for_startup(deployment_id, true);
        establish_pool_meta_bootstrap_identity_if_proven(vec![storage.clone()], &mut first_bootstrap, true)
            .await
            .expect("fresh bootstrap should persist a pending identity");
        let (_, replica_state) = load_pool_meta_for_startup(vec![storage.clone()], &mut first_bootstrap)
            .await
            .expect("the durable pending identity should authorize the initial pool metadata write");

        storage.fail_pool_meta_write_at.store(2, Ordering::SeqCst);
        persist_pool_meta_for_startup_if_safe(
            &init_test_pool_meta(None),
            vec![storage.clone()],
            replica_state,
            &mut first_bootstrap,
            true,
            true,
        )
        .await
        .expect_err("the injected crash boundary should leave only the V3 prepare record");

        let (pending_pool, pending_identity) = {
            let objects = storage.objects.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                objects
                    .get(POOL_META_NAME)
                    .map(|(payload, _)| payload.clone())
                    .expect("the V3 prepare record should be durable"),
                objects
                    .get(POOL_META_IDENTITY_NAME)
                    .map(|(payload, _)| payload.clone())
                    .expect("the bootstrap identity should be durable"),
            )
        };
        assert_eq!(pool_meta_v3_commit_state_for_test(pending_pool).expect("decode pending V3"), (1, false));
        assert!(!pool_meta_identity_initialized_for_test(&pending_identity).expect("decode pending identity"));

        storage.fail_pool_meta_write_at.store(0, Ordering::SeqCst);
        let mut restarted = PoolMetaWriteState::for_startup(deployment_id, false);
        let (loaded, replica_state) = load_pool_meta_for_startup(vec![storage.clone()], &mut restarted)
            .await
            .expect("restart should recover the predecessor embedded in the pending V3 record");
        assert!(loaded.pools.is_empty());
        assert!(replica_state.needs_repair);
        let committed = persist_pool_meta_for_startup_if_safe(
            &init_test_pool_meta(None),
            vec![storage.clone()],
            replica_state,
            &mut restarted,
            true,
            true,
        )
        .await
        .expect("restart should finish generation 1 before promoting the identity");
        assert_eq!(committed.version, 3);

        let (committed_pool, committed_identity) = {
            let objects = storage.objects.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
            (
                objects
                    .get(POOL_META_NAME)
                    .map(|(payload, _)| payload.clone())
                    .expect("the committed V3 record should be durable"),
                objects
                    .get(POOL_META_IDENTITY_NAME)
                    .map(|(payload, _)| payload.clone())
                    .expect("the committed identity should be durable"),
            )
        };
        assert_eq!(
            pool_meta_v3_commit_state_for_test(committed_pool).expect("decode committed V3"),
            (1, true)
        );
        assert!(pool_meta_identity_initialized_for_test(&committed_identity).expect("decode committed identity"));
        assert!(
            storage
                .pool_meta_written_versions
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .iter()
                .all(|version| *version == 3),
            "bootstrap recovery must never overwrite a pending V3 record with legacy metadata"
        );
    }

    #[tokio::test]
    async fn test_store_init_pool_meta_falls_back_from_corrupt_first_replica() {
        let corrupt = Arc::new(StartupPoolMetaStorage::new(vec![0, 1, 2]));
        let expected = init_test_pool_meta(None);
        let backup = Arc::new(StartupPoolMetaStorage::new(startup_pool_meta_payload(&expected)));
        let mut write_state = PoolMetaWriteState::default();

        let (loaded, replica_state) = load_pool_meta_for_startup(vec![corrupt.clone(), backup.clone()], &mut write_state)
            .await
            .expect("startup should select the validated backup replica");

        assert!(replica_state.needs_repair);
        assert!(replica_state.repair_write_safe);
        assert_eq!(loaded.pools.len(), 1);
        assert_eq!(loaded.pools[0].cmd_line, expected.pools[0].cmd_line);
        assert!(corrupt.read_without_lock.load(Ordering::SeqCst));
        assert!(backup.read_without_lock.load(Ordering::SeqCst));

        persist_pool_meta_for_startup_if_safe(
            &loaded,
            vec![corrupt.clone(), backup.clone()],
            replica_state,
            &mut write_state,
            false,
            true,
        )
        .await
        .expect("the elected startup writer should repair validated corrupt replicas");

        let corrupt_write = corrupt
            .written_payload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .expect("corrupt replica should be repaired");
        let backup_write = backup
            .written_payload
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
            .expect("backup replica should receive the same canonical snapshot");
        assert_eq!(corrupt_write, backup_write);
        assert_ne!(corrupt_write, corrupt.read_payload);
    }

    #[tokio::test]
    async fn test_store_init_pool_meta_does_not_repair_unreadable_replica() {
        let valid = Arc::new(StartupPoolMetaStorage::new(startup_pool_meta_payload(&init_test_pool_meta(None))));
        let unreadable = Arc::new(StartupPoolMetaStorage::unreadable());
        let mut write_state = PoolMetaWriteState::default();

        let (loaded, replica_state) = load_pool_meta_for_startup(vec![valid.clone(), unreadable.clone()], &mut write_state)
            .await
            .expect("startup should use a validated replica without overwriting an unreadable copy");
        assert!(replica_state.needs_repair);
        assert!(!replica_state.repair_write_safe);

        persist_pool_meta_for_startup_if_safe(
            &loaded,
            vec![valid.clone(), unreadable.clone()],
            replica_state,
            &mut write_state,
            false,
            true,
        )
        .await
        .expect("an unreadable copy should defer repair when no topology write is needed");
        assert!(!valid.wrote_without_lock.load(Ordering::SeqCst));
        assert!(!unreadable.wrote_without_lock.load(Ordering::SeqCst));

        let err = persist_pool_meta_for_startup_if_safe(
            &loaded,
            vec![valid.clone(), unreadable.clone()],
            replica_state,
            &mut write_state,
            true,
            true,
        )
        .await
        .expect_err("a topology update must not overwrite an unreadable replica");
        assert!(err.to_string().contains("cannot overwrite an unreadable replica"));
        assert!(!valid.wrote_without_lock.load(Ordering::SeqCst));
        assert!(!unreadable.wrote_without_lock.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn test_store_init_pool_meta_stays_blocked_after_all_replicas_were_unreadable() {
        let unreadable_a = Arc::new(StartupPoolMetaStorage::unreadable());
        let unreadable_b = Arc::new(StartupPoolMetaStorage::unreadable());
        let mut write_state = PoolMetaWriteState::default();

        load_pool_meta_for_startup(vec![unreadable_a, unreadable_b], &mut write_state)
            .await
            .expect_err("startup must fail when no readable pool metadata replica exists");

        let repaired = Arc::new(StartupPoolMetaStorage::new(startup_pool_meta_payload(&init_test_pool_meta(None))));
        let (loaded, replica_state) = load_pool_meta_for_startup(vec![repaired.clone()], &mut write_state)
            .await
            .expect("a later startup retry may read the repaired replica");
        assert!(replica_state.repair_write_safe);

        let err =
            persist_pool_meta_for_startup_if_safe(&loaded, vec![repaired.clone()], replica_state, &mut write_state, true, true)
                .await
                .expect_err("the same store instance must not write after observing unreadable replicas");
        assert!(
            err.to_string()
                .contains("restart after all replicas are readable and consistent")
        );
        assert!(!repaired.wrote_without_lock.load(Ordering::SeqCst));
    }

    #[test]
    fn test_pool_first_endpoint_is_local_respects_local_flag() {
        let mut local_endpoint = Endpoint::try_from("http://127.0.0.1:9000/data").expect("endpoint should parse");
        local_endpoint.is_local = true;
        let pool = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: Endpoints::from(vec![local_endpoint]),
            cmd_line: "pool-0".to_string(),
            platform: String::new(),
        };

        assert!(pool_first_endpoint_is_local(&pool));
    }

    #[test]
    fn test_pool_first_endpoint_is_local_rejects_missing_endpoint() {
        let pool = PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: Endpoints::from(Vec::<Endpoint>::new()),
            cmd_line: "pool-0".to_string(),
            platform: String::new(),
        };

        assert!(!pool_first_endpoint_is_local(&pool));
    }

    #[test]
    fn test_local_decommission_watchdog_retry_delay_is_bounded() {
        assert_eq!(local_decommission_watchdog_retry_delay(1), LOCAL_DECOMMISSION_RESUME_RETRY_DELAY);
        assert_eq!(
            local_decommission_watchdog_retry_delay(u32::MAX),
            LOCAL_DECOMMISSION_WATCHDOG_MAX_RETRY_DELAY
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_local_decommission_watchdog_retries_general_failures_until_cancelled() {
        let rx = CancellationToken::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(run_local_decommission_watchdog(rx.clone(), {
            let attempts = attempts.clone();
            let rx = rx.clone();
            move || {
                let attempts = attempts.clone();
                let rx = rx.clone();
                async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        Err(StorageError::SlowDown)
                    } else {
                        rx.cancel();
                        Ok(())
                    }
                }
            }
        }));

        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        tokio::time::advance(LOCAL_DECOMMISSION_RESUME_RETRY_DELAY).await;
        task.await.expect("watchdog task should exit after cancellation");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn test_local_decommission_watchdog_rescans_after_success() {
        let rx = CancellationToken::new();
        let attempts = Arc::new(AtomicUsize::new(0));
        let task = tokio::spawn(run_local_decommission_watchdog(rx.clone(), {
            let attempts = attempts.clone();
            let rx = rx.clone();
            move || {
                let attempts = attempts.clone();
                let rx = rx.clone();
                async move {
                    if attempts.fetch_add(1, Ordering::SeqCst) == 1 {
                        rx.cancel();
                    }
                    Ok(())
                }
            }
        }));

        tokio::task::yield_now().await;
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        tokio::time::advance(super::LOCAL_DECOMMISSION_WATCHDOG_INTERVAL).await;
        task.await.expect("watchdog task should exit after cancellation");
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn test_should_retry_format_load_rejects_permanent_corruption() {
        assert!(!should_retry_format_load(&StorageError::CorruptedFormat));
        assert!(should_retry_format_load(&StorageError::ErasureReadQuorum));
        assert!(should_retry_format_load(&StorageError::FirstDiskWait));
    }

    #[test]
    fn test_should_auto_start_rebalance_after_init_allows_active_rebalance_without_decommission() {
        assert!(should_auto_start_rebalance_after_init(false, true));
    }

    #[test]
    fn test_should_auto_start_rebalance_after_init_rejects_active_decommission() {
        assert!(!should_auto_start_rebalance_after_init(true, true));
    }

    #[test]
    fn test_should_auto_start_rebalance_after_init_rejects_terminal_or_missing_rebalance() {
        assert!(!should_auto_start_rebalance_after_init(false, false));
    }

    #[test]
    fn test_should_defer_rebalance_auto_start_only_without_distributed_fleet_proof() {
        assert!(should_defer_rebalance_auto_start(true, false));
        assert!(!should_defer_rebalance_auto_start(true, true));
        assert!(!should_defer_rebalance_auto_start(false, false));
    }

    #[test]
    fn test_store_init_recovery_skips_rebalance_when_decommission_metadata_is_active() {
        let pool_meta = init_test_pool_meta(Some(PoolDecommissionInfo {
            start_time: Some(OffsetDateTime::UNIX_EPOCH),
            complete: false,
            failed: false,
            canceled: false,
            ..Default::default()
        }));
        let rebalance_meta = Some(RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });

        assert!(!should_auto_start_rebalance_after_init(
            pool_meta_has_active_decommission(&pool_meta),
            rebalance_meta
                .as_ref()
                .is_some_and(crate::services::rebalance::rebalance_requires_worker_activation)
        ));
    }

    #[test]
    fn test_store_init_recovery_allows_active_rebalance_without_decommission() {
        let pool_meta = init_test_pool_meta(None);
        let rebalance_meta = Some(RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Started,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });

        assert!(should_auto_start_rebalance_after_init(
            pool_meta_has_active_decommission(&pool_meta),
            rebalance_meta
                .as_ref()
                .is_some_and(crate::services::rebalance::rebalance_requires_worker_activation)
        ));
    }

    #[test]
    fn test_store_init_recovery_skips_completed_rebalance_metadata() {
        let pool_meta = init_test_pool_meta(None);
        let rebalance_meta = Some(RebalanceMeta {
            pool_stats: vec![RebalanceStats {
                participating: true,
                info: RebalanceInfo {
                    status: RebalStatus::Completed,
                    ..Default::default()
                },
                ..Default::default()
            }],
            ..Default::default()
        });

        assert!(!should_auto_start_rebalance_after_init(
            pool_meta_has_active_decommission(&pool_meta),
            rebalance_meta
                .as_ref()
                .is_some_and(crate::services::rebalance::rebalance_requires_worker_activation)
        ));
    }

    #[test]
    fn test_resolve_store_init_stage_result_passthrough_ok() {
        resolve_store_init_stage_result(Ok(()), "load_rebalance_meta").expect("successful stage should pass through");
    }

    #[test]
    fn test_resolve_store_init_stage_result_wraps_error_context() {
        let err = resolve_store_init_stage_result(Err(StorageError::SlowDown), "start_rebalance")
            .expect_err("failed stage should be wrapped");
        let err_message = err.to_string();
        assert!(err_message.contains("store init failed during start_rebalance"));
        assert!(err_message.contains(&StorageError::SlowDown.to_string()));
    }

    #[tokio::test]
    async fn test_wait_for_local_decommission_resume_delay_returns_true_after_delay() {
        let rx = CancellationToken::new();
        assert!(wait_for_local_decommission_resume_delay(&rx, Duration::from_millis(1)).await);
    }

    #[tokio::test]
    async fn test_wait_for_local_decommission_resume_delay_returns_false_when_cancelled() {
        let rx = CancellationToken::new();
        rx.cancel();
        assert!(!wait_for_local_decommission_resume_delay(&rx, Duration::from_secs(1)).await);
    }

    #[test]
    fn test_pool_first_endpoint_is_local_uses_pool_scope_for_expansion() {
        let mut remote_endpoint = Endpoint::try_from("http://127.0.0.2:9000/data1").expect("remote endpoint should parse");
        remote_endpoint.is_local = false;

        let mut local_endpoint = Endpoint::try_from("http://127.0.0.1:9000/data1").expect("local endpoint should parse");
        local_endpoint.is_local = true;

        let endpoints = EndpointServerPools::from(vec![
            PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 1,
                endpoints: Endpoints::from(vec![remote_endpoint]),
                cmd_line: "pool-0".to_string(),
                platform: String::new(),
            },
            PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set: 1,
                endpoints: Endpoints::from(vec![local_endpoint]),
                cmd_line: "pool-1".to_string(),
                platform: String::new(),
            },
        ]);

        assert!(!endpoints.first_local(), "cluster first endpoint is intentionally remote");
        assert!(
            pool_first_endpoint_is_local(endpoints.as_ref().get(1).expect("second pool should exist")),
            "the expanded pool should be initialized by its own first local endpoint"
        );
    }

    fn rpc_preflight_pools(pools: Vec<Vec<Endpoint>>) -> EndpointServerPools {
        EndpointServerPools::from(
            pools
                .into_iter()
                .enumerate()
                .map(|(pool_index, endpoints)| PoolEndpoints {
                    legacy: false,
                    set_count: 1,
                    drives_per_set: endpoints.len(),
                    endpoints: Endpoints::from(endpoints),
                    cmd_line: format!("pool-{pool_index}"),
                    platform: String::new(),
                })
                .collect::<Vec<_>>(),
        )
    }

    fn parsed_local_endpoint() -> Endpoint {
        let mut endpoint = Endpoint::try_from("http://127.0.0.1:9000/data").expect("local endpoint should parse");
        endpoint.is_local = true;
        endpoint
    }

    fn parsed_remote_endpoint() -> Endpoint {
        let mut endpoint = Endpoint::try_from("http://10.0.0.2:9000/data").expect("remote endpoint should parse");
        endpoint.is_local = false;
        endpoint
    }

    #[test]
    fn test_rpc_secret_preflight_aborts_for_remote_endpoints_without_secret() {
        // The remote endpoint intentionally sits behind a local one: the
        // preflight must scan every endpoint, not just the first.
        let pools = rpc_preflight_pools(vec![vec![parsed_local_endpoint(), parsed_remote_endpoint()]]);

        let err = preflight_startup_rpc_secret_with(&pools, || {
            Err(std::io::Error::other(rustfs_credentials::RPC_SECRET_REQUIRED_MESSAGE))
        })
        .expect_err("remote endpoints without an RPC secret must abort startup before the format retry loop");

        let message = err.to_string();
        assert!(message.contains("store init aborted"), "unexpected error: {message}");
        assert!(
            message.contains(rustfs_credentials::RPC_SECRET_REQUIRED_MESSAGE),
            "error must state the resolution failure: {message}"
        );
        assert!(
            message.contains(rustfs_credentials::RPC_SECRET_REQUIRED_OPERATOR_MESSAGE),
            "error must carry the operator remediation guidance: {message}"
        );
    }

    #[test]
    fn test_rpc_secret_preflight_aborts_for_remote_endpoint_in_later_pool() {
        // The remote endpoint hides in a later, otherwise-local pool: a
        // first-pool shortcut (the `first_local` shape) must not pass.
        let pools = rpc_preflight_pools(vec![vec![parsed_local_endpoint()], vec![parsed_remote_endpoint()]]);

        let err = preflight_startup_rpc_secret_with(&pools, || {
            Err(std::io::Error::other(rustfs_credentials::RPC_SECRET_REQUIRED_MESSAGE))
        })
        .expect_err("a remote endpoint in a later pool must abort startup without an RPC secret");

        let message = err.to_string();
        assert!(message.contains("store init aborted"), "unexpected error: {message}");
    }

    #[test]
    fn test_rpc_secret_preflight_skips_resolution_for_local_only_endpoints() {
        preflight_startup_rpc_secret_with(&rpc_preflight_pools(vec![vec![parsed_local_endpoint()]]), || {
            panic!("single-node topology must not resolve an RPC secret")
        })
        .expect("local-only topology must start without an RPC secret");
    }

    #[test]
    fn test_rpc_secret_preflight_accepts_remote_endpoints_with_resolved_secret() {
        preflight_startup_rpc_secret_with(&rpc_preflight_pools(vec![vec![parsed_remote_endpoint()]]), || {
            Ok("resolved-secret".to_string())
        })
        .expect("a resolvable RPC secret must not block startup");
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn test_new_with_instance_ctx_aborts_before_format_retry_loop_without_rpc_secret() {
        // Guard: under a shared-process `cargo test` run another test may have
        // already pinned the process-global RPC secret (ensure_test_rpc_secret),
        // which would let init proceed past the preflight into remote disk
        // init. A failing probe pins nothing, so it cannot poison later tests;
        // under nextest's process-per-test model (CI) the guard never fires.
        if rustfs_credentials::try_get_rpc_token().is_ok() {
            return;
        }

        let mut remote_endpoint = parsed_remote_endpoint();
        remote_endpoint.set_pool_index(0);
        remote_endpoint.set_set_index(0);
        remote_endpoint.set_disk_index(0);

        let err = crate::store::ECStore::new_with_instance_ctx(
            "127.0.0.1:0".parse().expect("test address"),
            rpc_preflight_pools(vec![vec![remote_endpoint]]),
            CancellationToken::new(),
            Arc::new(crate::runtime::instance::InstanceContext::new()),
        )
        .await
        .expect_err("distributed init without an RPC secret must abort before the format retry loop");

        let message = err.to_string();
        assert!(message.contains("store init aborted"), "unexpected error: {message}");
        assert!(
            message.contains(rustfs_credentials::RPC_SECRET_REQUIRED_OPERATOR_MESSAGE),
            "abort must carry the operator remediation guidance: {message}"
        );
    }

    fn endpoint_pools_with_drive_counts(counts: &[usize]) -> EndpointServerPools {
        EndpointServerPools::from(
            counts
                .iter()
                .enumerate()
                .map(|(pool_index, &drives_per_set)| PoolEndpoints {
                    legacy: false,
                    set_count: 1,
                    drives_per_set,
                    endpoints: Endpoints::from(Vec::new()),
                    cmd_line: format!("pool-{pool_index}"),
                    platform: String::new(),
                })
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn startup_pool_defaults_are_resolved_per_pool() {
        let validate = |pools: &EndpointServerPools| {
            let drive_counts: Vec<_> = pools.as_ref().iter().map(|pool| pool.drives_per_set).collect();
            crate::config::storageclass::lookup_config_for_pools_without_env(&KVS::new(), &drive_counts).map(|_| ())
        };
        let defaults = resolve_startup_pool_defaults_with(&endpoint_pools_with_drive_counts(&[4, 2]), validate)
            .expect("heterogeneous topology should resolve");
        assert_eq!(defaults, vec![2, 1]);

        let defaults = resolve_startup_pool_defaults_with(&endpoint_pools_with_drive_counts(&[4, 6]), validate)
            .expect("heterogeneous topology should resolve");
        assert_eq!(defaults, vec![2, 3]);
    }

    #[test]
    fn startup_pool_defaults_validate_explicit_environment_for_every_pool() {
        let validate = |pools: &EndpointServerPools| {
            let drive_counts: Vec<_> = pools.as_ref().iter().map(|pool| pool.drives_per_set).collect();
            let mut kvs = KVS::new();
            kvs.insert(crate::config::storageclass::CLASS_STANDARD.to_string(), "EC:2".to_string());
            crate::config::storageclass::lookup_config_for_pools_without_env(&kvs, &drive_counts).map(|_| ())
        };
        let err = resolve_startup_pool_defaults_with(&endpoint_pools_with_drive_counts(&[4, 2]), validate)
            .expect_err("explicit EC:2 must fail before any two-drive pool I/O");
        assert!(err.to_string().contains("pool 1") && err.to_string().contains("2 drives"));
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn startup_pool_defaults_validate_environment_without_changing_metadata_fallback() {
        temp_env::with_vars(
            [
                (crate::config::storageclass::STANDARD_ENV, Some("EC:1")),
                (crate::config::storageclass::RRS_ENV, None),
                (crate::config::storageclass::OPTIMIZE_ENV, None),
                (crate::config::storageclass::INLINE_BLOCK_ENV, None),
            ],
            || {
                let runtime = crate::config::storageclass::lookup_config_for_pools(&KVS::new(), &[6, 4])
                    .expect("explicit standard parity must resolve the runtime candidate");
                assert_eq!(runtime.parities_for_sc(crate::config::storageclass::STANDARD), Some(vec![1, 1]));

                let defaults = super::resolve_startup_pool_defaults(&endpoint_pools_with_drive_counts(&[6, 4]))
                    .expect("explicit standard parity must validate for every pool");
                assert_eq!(defaults, vec![3, 2]);
            },
        );
    }

    async fn without_storage_class_env<F: Future>(future: F) -> F::Output {
        temp_env::async_with_vars(
            [
                (crate::config::storageclass::STANDARD_ENV, None::<&str>),
                (crate::config::storageclass::RRS_ENV, None::<&str>),
                (crate::config::storageclass::OPTIMIZE_ENV, None::<&str>),
                (crate::config::storageclass::INLINE_BLOCK_ENV, None::<&str>),
            ],
            future,
        )
        .await
    }

    // Build a real local store over a temp dir around a fresh instance context.
    async fn build_isolated_test_store(
        temp_dir: &std::path::Path,
        cmd_line: &str,
        pool_drive_counts: &[usize],
    ) -> (
        Arc<crate::runtime::instance::InstanceContext>,
        Arc<crate::store::ECStore>,
        CancellationToken,
    ) {
        build_isolated_test_store_with_shutdown(temp_dir, cmd_line, pool_drive_counts, CancellationToken::new()).await
    }

    async fn build_isolated_test_store_with_shutdown(
        temp_dir: &std::path::Path,
        cmd_line: &str,
        pool_drive_counts: &[usize],
        shutdown: CancellationToken,
    ) -> (
        Arc<crate::runtime::instance::InstanceContext>,
        Arc<crate::store::ECStore>,
        CancellationToken,
    ) {
        let pool_layouts = pool_drive_counts
            .iter()
            .map(|&drives_per_set| (1, drives_per_set))
            .collect::<Vec<_>>();
        build_isolated_test_store_with_layout(temp_dir, cmd_line, &pool_layouts, shutdown, None).await
    }

    async fn build_isolated_test_store_with_layout(
        temp_dir: &std::path::Path,
        cmd_line: &str,
        pool_layouts: &[(usize, usize)],
        shutdown: CancellationToken,
        instance_ctx: Option<Arc<crate::runtime::instance::InstanceContext>>,
    ) -> (
        Arc<crate::runtime::instance::InstanceContext>,
        Arc<crate::store::ECStore>,
        CancellationToken,
    ) {
        let mut pools = Vec::with_capacity(pool_layouts.len());
        for (pool_index, &(set_count, drives_per_set)) in pool_layouts.iter().enumerate() {
            let mut endpoints = Vec::with_capacity(set_count * drives_per_set);
            for set_index in 0..set_count {
                for disk_index in 0..drives_per_set {
                    let path = temp_dir.join(format!("pool{pool_index}/set{set_index}/disk{disk_index}"));
                    tokio::fs::create_dir_all(&path).await.expect("create disk dir");
                    let mut endpoint =
                        Endpoint::try_from(path.to_str().expect("disk path should be utf-8")).expect("local endpoint");
                    endpoint.set_pool_index(pool_index);
                    endpoint.set_set_index(set_index);
                    endpoint.set_disk_index(disk_index);
                    endpoints.push(endpoint);
                }
            }
            pools.push(PoolEndpoints {
                legacy: false,
                set_count,
                drives_per_set,
                endpoints: Endpoints::from(endpoints),
                cmd_line: format!("{cmd_line}-pool-{pool_index}"),
                platform: "test".to_string(),
            });
        }
        let endpoint_pools = EndpointServerPools(pools);
        crate::services::notification_sys::install_cross_pool_fence_fleet_proof_for_test();

        let instance_ctx = instance_ctx.unwrap_or_else(|| Arc::new(crate::runtime::instance::InstanceContext::new()));
        crate::store::init_local_disks_with_instance_ctx(&instance_ctx, endpoint_pools.clone())
            .await
            .expect("register local disks into the fresh context");

        let store = crate::store::ECStore::new_with_instance_ctx(
            "127.0.0.1:0".parse().expect("test address"),
            endpoint_pools,
            shutdown.clone(),
            instance_ctx.clone(),
        )
        .await
        .expect("store should build around the fresh context");

        // Capacity admission in these local fixtures must not depend on the
        // host volume's statvfs values. Keep enough identical snapshots for
        // startup, recovery, and the mutation probes exercised by each test.
        let layout = DecommissionErasureLayout { data: 2, parity: 2 };
        let snapshot: Vec<DecommissionPoolCapacityInfo> = store
            .pools
            .iter()
            .enumerate()
            .map(|(pool_index, _)| DecommissionPoolCapacityInfo::for_test(pool_index, layout, 1 << 40, 1 << 40, 1 << 30))
            .collect();
        set_decommission_capacity_info_overrides_for_test(store.id, (0..128).map(|_| snapshot.clone()).collect());

        (instance_ctx, store, shutdown)
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn cross_key_materialized_copy_does_not_publish_remote_tuple_ownership() {
        let temp_dir = tempfile::tempdir().expect("create materialized-copy store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "copy-materialized", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;

        let bucket = format!("copy-materialized-{}", Uuid::new_v4());
        let source_object = "source-restored.bin";
        let target_object = "target-local.bin";
        let payload = b"materialized copy must own only its new local bytes".to_vec();
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("copy test bucket should be created");

        let tier_name = "COPY-MATERIALIZED-TIER";
        register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let lease = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("mock tier generation should be available");
        let backend_identity = lease.backend_identity();
        drop(lease);

        let mut source_metadata = HashMap::from([
            ("content-type".to_string(), "application/octet-stream".to_string()),
            (
                "x-amz-restore".to_string(),
                "ongoing-request=\"false\", expiry-date=\"2099-01-01T00:00:00Z\"".to_string(),
            ),
        ]);
        for (suffix, value) in [
            (
                rustfs_utils::http::SUFFIX_TRANSITION_STATUS,
                crate::bucket::lifecycle::core::TRANSITION_COMPLETE.to_string(),
            ),
            (
                rustfs_utils::http::SUFFIX_TRANSITIONED_OBJECTNAME,
                "remote/source-restored.bin".to_string(),
            ),
            (rustfs_utils::http::SUFFIX_TRANSITION_TIER, tier_name.to_string()),
            (rustfs_utils::http::SUFFIX_TRANSITIONED_VERSION_ID, "remote-source-version".to_string()),
            (
                rustfs_utils::http::SUFFIX_TRANSITIONED_VERSION_STATE,
                rustfs_filemeta::TransitionVersionState::Exact.as_str().to_string(),
            ),
            (
                rustfs_utils::http::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
                rustfs_utils::crypto::hex(backend_identity),
            ),
            (rustfs_utils::http::SUFFIX_TRANSITION_TRANSACTION_ID, Uuid::new_v4().to_string()),
        ] {
            rustfs_utils::http::metadata_compat::insert_str(&mut source_metadata, suffix, value);
        }

        let mut source_body = PutObjReader::from_vec(payload.clone());
        store
            .put_object(
                &bucket,
                source_object,
                &mut source_body,
                &ObjectOptions {
                    user_defined: source_metadata,
                    ..Default::default()
                },
            )
            .await
            .expect("restored transitioned source should be written");

        let source_opts = ObjectOptions::default();
        let mut source_info = store
            .get_object_info(&bucket, source_object, &source_opts)
            .await
            .expect("source metadata should be readable");
        assert_eq!(
            source_info.transitioned_object.status,
            crate::bucket::lifecycle::core::TRANSITION_COMPLETE
        );
        assert_eq!(source_info.transitioned_object.tier, tier_name);
        assert!(source_info.data_dir.is_some(), "restored source must retain local data");
        source_info.put_object_reader = Some(PutObjReader::from_vec(payload.clone()));

        let copied = store
            .copy_object(
                &bucket,
                source_object,
                &bucket,
                target_object,
                &mut source_info,
                &source_opts,
                &ObjectOptions::default(),
            )
            .await
            .expect("cross-key materialized copy should succeed");
        assert!(copied.transitioned_object.status.is_empty());
        assert!(copied.transitioned_object.name.is_empty());
        assert!(copied.transitioned_object.version_id.is_empty());
        assert!(copied.transitioned_object.tier.is_empty());
        assert!(!copied.transitioned_object.free_version);
        for suffix in [
            rustfs_utils::http::SUFFIX_TRANSITION_STATUS,
            rustfs_utils::http::SUFFIX_TRANSITIONED_OBJECTNAME,
            rustfs_utils::http::SUFFIX_TRANSITIONED_VERSION_ID,
            rustfs_utils::http::SUFFIX_TRANSITIONED_VERSION_STATE,
            rustfs_utils::http::SUFFIX_TRANSITION_TIER,
            rustfs_utils::http::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::http::SUFFIX_TRANSITION_TRANSACTION_ID,
        ] {
            assert!(
                !rustfs_utils::http::metadata_compat::contains_key_str(copied.user_defined.as_ref(), suffix),
                "target retained protected source suffix {suffix}"
            );
        }
        assert_eq!(
            copied.user_defined.get("content-type").map(String::as_str),
            Some("application/octet-stream")
        );

        let mut target_reader = store
            .get_object_reader(&bucket, target_object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("target object should be readable");
        let mut target_body = Vec::new();
        target_reader
            .stream
            .read_to_end(&mut target_body)
            .await
            .expect("target body should stream");
        assert_eq!(target_body, payload);

        let source_after = store
            .get_object_info(&bucket, source_object, &source_opts)
            .await
            .expect("source metadata should remain readable after copy");
        assert_eq!(
            source_after.transitioned_object.status,
            crate::bucket::lifecycle::core::TRANSITION_COMPLETE
        );
        assert_eq!(source_after.transitioned_object.tier, tier_name);
        assert_eq!(source_after.transitioned_object.name, "remote/source-restored.bin");
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn copy_object_immediately_reads_small_completed_multipart_source() {
        let temp_dir = tempfile::tempdir().expect("create small multipart copy store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "small-multipart-copy", &[1])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;

        let bucket = format!("small-multipart-copy-{}", Uuid::new_v4());
        let source_object = "docker/registry/v2/repositories/example/_uploads/upload-id/data";
        let target_object = "docker/registry/v2/blobs/sha256/c0/digest/data";
        let payload = vec![0xAB; 273];

        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket for small multipart copy");
        let upload = store
            .new_multipart_upload(&bucket, source_object, &ObjectOptions::default())
            .await
            .expect("create source multipart upload");
        let mut part_reader = PutObjReader::from_vec(payload.clone());
        let part = store
            .put_object_part(&bucket, source_object, &upload.upload_id, 1, &mut part_reader, &ObjectOptions::default())
            .await
            .expect("stage small multipart source part");
        let completed = store
            .clone()
            .complete_multipart_upload(
                &bucket,
                source_object,
                &upload.upload_id,
                vec![crate::storage_api_contracts::multipart::CompletePart {
                    part_num: part.part_num,
                    etag: part.etag,
                    ..Default::default()
                }],
                &ObjectOptions::default(),
            )
            .await
            .expect("complete the small multipart source");
        assert_eq!(completed.get_actual_size().expect("completed object logical size"), payload.len() as i64);

        let source_reader = store
            .get_object_reader(&bucket, source_object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("completed multipart source should be immediately readable");
        let mut copy_info = source_reader.object_info.clone();
        let actual_size = copy_info.get_actual_size().expect("copy source logical size should resolve");
        assert_eq!(actual_size, payload.len() as i64);
        let copy_reader = rustfs_rio::HashReader::from_stream(source_reader.stream, actual_size, actual_size, None, None, false)
            .expect("copy source hash reader should build");
        copy_info.put_object_reader = Some(PutObjReader::new(copy_reader));

        store
            .copy_object(
                &bucket,
                source_object,
                &bucket,
                target_object,
                &mut copy_info,
                &ObjectOptions::default(),
                &ObjectOptions::default(),
            )
            .await
            .expect("CopyObject should accept a freshly completed multipart source");

        let mut target_reader = store
            .get_object_reader(&bucket, target_object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("copied target should be readable");
        let mut target_body = Vec::new();
        target_reader
            .stream
            .read_to_end(&mut target_body)
            .await
            .expect("target body should stream");
        assert_eq!(target_body, payload);
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn complete_multipart_waits_for_tail_rename_before_copy_source_visibility() {
        let temp_dir = tempfile::tempdir().expect("create early-ack multipart copy store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "early-ack-multipart-copy", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;

        let bucket = format!("early-ack-multipart-copy-{}", Uuid::new_v4());
        let source_object = "docker/registry/v2/repositories/example/_uploads/upload-id/data";
        let target_object = "docker/registry/v2/blobs/sha256/c0/digest/data";
        let payload = vec![0xCD; 273];

        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket for early-ack multipart copy");
        let upload = store
            .new_multipart_upload(&bucket, source_object, &ObjectOptions::default())
            .await
            .expect("create source multipart upload");
        let mut part_reader = PutObjReader::from_vec(payload.clone());
        let part = store
            .put_object_part(&bucket, source_object, &upload.upload_id, 1, &mut part_reader, &ObjectOptions::default())
            .await
            .expect("stage small multipart source part");
        let completed_parts = vec![crate::storage_api_contracts::multipart::CompletePart {
            part_num: part.part_num,
            etag: part.etag,
            ..Default::default()
        }];

        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let rename_tasks = crate::set_disk::rename_fanout_barrier::observe_tasks(source_object);
            let rename_barrier = crate::set_disk::rename_fanout_barrier::arm(
                source_object,
                0,
                crate::set_disk::rename_fanout_barrier::PHASE_RENAME,
            );
            let complete_store = Arc::clone(&store);
            let complete_bucket = bucket.clone();
            let complete_upload_id = upload.upload_id.clone();
            let mut complete = tokio::spawn(async move {
                complete_store
                    .complete_multipart_upload(
                        &complete_bucket,
                        source_object,
                        &complete_upload_id,
                        completed_parts,
                        &ObjectOptions::default(),
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(30), rename_barrier.wait_until_paused())
                .await
                .expect("multipart completion should pause one tail disk during rename");
            assert!(
                rename_tasks.running() >= 1,
                "the paused multipart tail disk must remain in flight before completion returns"
            );
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut complete).await.is_err(),
                "CompleteMultipartUpload must not return while a copy source rename tail is still pending"
            );
            rename_barrier.release();
            complete
                .await
                .expect("multipart completion task should join")
                .expect("multipart completion should return after every rename tail finishes");

            let source_reader = store
                .get_object_reader(&bucket, source_object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("completed multipart source should be immediately readable after success");
            let mut copy_info = source_reader.object_info.clone();
            let actual_size = copy_info.get_actual_size().expect("copy source logical size should resolve");
            assert_eq!(actual_size, payload.len() as i64);
            let copy_reader =
                rustfs_rio::HashReader::from_stream(source_reader.stream, actual_size, actual_size, None, None, false)
                    .expect("copy source hash reader should build");
            copy_info.put_object_reader = Some(PutObjReader::new(copy_reader));

            store
                .copy_object(
                    &bucket,
                    source_object,
                    &bucket,
                    target_object,
                    &mut copy_info,
                    &ObjectOptions::default(),
                    &ObjectOptions::default(),
                )
                .await
                .expect("CopyObject should accept a freshly completed multipart source");
        })
        .await;

        let mut target_reader = store
            .get_object_reader(&bucket, target_object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("copied target should be readable after tail release");
        let mut target_body = Vec::new();
        target_reader
            .stream
            .read_to_end(&mut target_body)
            .await
            .expect("target body should stream");
        assert_eq!(target_body, payload);
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn early_ack_put_tails_block_scanner_publication_until_all_renames_finish() {
        use crate::storage_api_contracts::namespace::NamespaceLocking as _;

        let temp_dir = tempfile::tempdir().expect("create scanner PUT tail store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "scanner-put-tails", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
        let bucket = format!("scanner-put-tails-{}", Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create scanner PUT tail bucket");
        let set = &store.pools[0].disk_set[0];
        let objects = [("scanner-tail-a", vec![0xA1; 273]), ("scanner-tail-b", vec![0xB2; 379])];

        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let (active, blocked, movement_generation) = store.scanner_data_movement_activity().await;
            assert!(!active && !blocked);
            assert!(ctx.scanner_publication_state_allowed(), "the set admission cache should start allowed");
            let (old_lease, _) = store
                .acquire_scanner_publication_lease(movement_generation, crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL)
                .await
                .expect("publication lease should be admitted before either PUT starts");

            let barriers: Vec<_> = objects
                .iter()
                .map(|(object, _)| {
                    crate::set_disk::rename_fanout_barrier::arm(object, 0, crate::set_disk::rename_fanout_barrier::PHASE_RENAME)
                })
                .collect();
            let trackers: Vec<_> = objects
                .iter()
                .map(|(object, _)| crate::set_disk::rename_fanout_barrier::observe_tasks(object))
                .collect();
            let puts: Vec<_> = objects
                .iter()
                .map(|(object, body)| {
                    let put_store = Arc::clone(&store);
                    let put_bucket = bucket.clone();
                    let object = *object;
                    let body = body.clone();
                    tokio::spawn(async move {
                        let mut reader = PutObjReader::from_vec(body);
                        put_store
                            .put_object(&put_bucket, object, &mut reader, &ObjectOptions::default())
                            .await
                    })
                })
                .collect();
            let committed = tokio::time::timeout(Duration::from_secs(30), async {
                for barrier in &barriers {
                    barrier.wait_until_paused().await;
                }
                let mut committed = Vec::with_capacity(puts.len());
                for put in puts {
                    committed.push(
                        put.await
                            .expect("early-ACK PUT task should join while its tail is paused")
                            .expect("root PUT should return after quorum without waiting for its tail"),
                    );
                }
                committed
            })
            .await
            .expect("both root PUTs must quorum-ACK while their tail disks remain paused");

            assert!(trackers.iter().all(|tracker| tracker.running() >= 1));
            assert!(ctx.namespace_commits_pending());
            assert!(
                ctx.scanner_publication_state_allowed(),
                "pending PUT tails must not disable scanner namespace walks"
            );
            let (active, blocked, observed_movement_generation) = store.scanner_data_movement_activity().await;
            assert!(!active, "ordinary PUT tails are not decommission or rebalance work");
            assert!(!blocked, "ordinary PUT tails must not block the movement-only scan baseline");
            assert_eq!(observed_movement_generation, movement_generation);
            assert!(store.scanner_data_usage_publication_blocked().await);
            assert!(store.scanner_data_usage_publication_admission_guard().await.is_some());
            assert!(set.scanner_data_usage_publication_admission_guard().await.is_some());
            for error in [
                store
                    .acquire_scanner_publication_lease(
                        movement_generation,
                        crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL,
                    )
                    .await
                    .expect_err("a new remote publication lease must reject pending PUT tails"),
                store
                    .validate_scanner_publication_lease(old_lease, movement_generation)
                    .await
                    .expect_err("an existing remote lease must not bypass pending PUT tails"),
                store
                    .acquire_scanner_publication_lease_guard(old_lease)
                    .await
                    .expect_err("target-side publication admission must reject pending PUT tails"),
            ] {
                assert!(
                    error.to_string().contains("blocked"),
                    "publication must fail because of active tails: {error}"
                );
            }
            store.release_scanner_publication_lease(old_lease).await;

            for (index, barrier) in barriers.iter().enumerate() {
                let commit_generation = ctx.namespace_commit_generation();
                let namespace_generation = store.scanner_namespace_mutation_generation();
                barrier.release();
                tokio::time::timeout(Duration::from_secs(30), async {
                    while trackers[index].running() != 0 || ctx.namespace_commit_generation() <= commit_generation {
                        tokio::task::yield_now().await;
                    }
                    if index + 1 == barriers.len() {
                        while ctx.namespace_commits_pending() {
                            tokio::task::yield_now().await;
                        }
                    }
                })
                .await
                .expect("released tail must drain and publish its terminal namespace generation");
                assert!(store.scanner_namespace_mutation_generation() > namespace_generation);
                let pending = index + 1 < barriers.len();
                assert_eq!(ctx.namespace_commits_pending(), pending);
                assert_eq!(store.scanner_data_usage_publication_blocked().await, pending);
                assert!(!store.scanner_data_movement_activity().await.1);
                assert!(store.scanner_data_usage_publication_admission_guard().await.is_some());
                assert!(set.scanner_data_usage_publication_admission_guard().await.is_some());
            }

            let (lease, generation) = store
                .acquire_scanner_publication_lease(movement_generation, crate::runtime::instance::SCANNER_PUBLICATION_LEASE_TTL)
                .await
                .expect("remote publication lease should resume after both tails drain");
            store
                .validate_scanner_publication_lease(lease, generation)
                .await
                .expect("a resumed remote publication lease should validate");
            drop(
                store
                    .acquire_scanner_publication_lease_guard(lease)
                    .await
                    .expect("target-side publication admission should resume after both tails drain"),
            );
            assert!(store.release_scanner_publication_lease(lease).await);

            let disks = set.disk_inventory().await;
            assert_eq!(disks.len(), 4);
            for ((object, body), committed) in objects.iter().zip(&committed) {
                let logical_size = i64::try_from(body.len()).expect("fixture payload size should fit i64");
                let etag = committed.etag.as_ref().expect("root PUT should return a committed ETag");
                for (disk_index, disk) in disks.iter().enumerate() {
                    let file_info = disk
                        .as_ref()
                        .expect("every fixture disk should remain online")
                        .read_version(
                            "",
                            &bucket,
                            object,
                            "",
                            &crate::disk::ReadOptions {
                                read_data: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap_or_else(|err| panic!("disk {disk_index} should publish {object} after its tail finishes: {err}"));
                    assert_eq!(file_info.size, logical_size);
                    assert_eq!(file_info.metadata.get(http::header::ETAG.as_str()), Some(etag));
                    assert!(
                        file_info.inline_data(),
                        "small fixture payloads should have an inline shard on every disk"
                    );
                    let inline_data = file_info.data.as_ref().expect("every disk should retain its inline shard");
                    let erasure = crate::erasure::coding::Erasure::try_new_with_options(
                        file_info.erasure.data_blocks,
                        file_info.erasure.parity_blocks,
                        file_info.erasure.block_size,
                        file_info.uses_legacy_checksum,
                    )
                    .expect("persisted erasure geometry should be valid");
                    let shard_size =
                        usize::try_from(erasure.shard_file_size(logical_size)).expect("fixture shard size should fit usize");
                    crate::erasure::coding::bitrot_verify(
                        Cursor::new(inline_data.clone()),
                        inline_data.len(),
                        shard_size,
                        rustfs_utils::HashAlgorithm::HighwayHash256S,
                        erasure.shard_size(),
                    )
                    .await
                    .unwrap_or_else(|err| panic!("disk {disk_index} should retain a complete valid shard for {object}: {err}"));
                }
                let mut reader = store
                    .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                    .await
                    .expect("fully drained PUT should be readable");
                let mut actual = Vec::new();
                reader.stream.read_to_end(&mut actual).await.expect("PUT body should drain");
                assert_eq!(&actual, body);
            }

            let generation_before_internal_put = ctx.namespace_commit_generation();
            let internal_object = "scanner-tail-regression/internal-metadata";
            let internal_body = b"scanner metadata must not invalidate its own publication";
            let mut internal_reader = PutObjReader::from_vec(internal_body.to_vec());
            store
                .put_object(RUSTFS_META_BUCKET, internal_object, &mut internal_reader, &ObjectOptions::default())
                .await
                .expect("internal metadata PUT should commit without scanner self-invalidation");
            let internal_lock = set
                .new_ns_lock(RUSTFS_META_BUCKET, internal_object)
                .await
                .expect("internal metadata tail lock should be available");
            drop(
                internal_lock
                    .get_write_lock(Duration::from_secs(30))
                    .await
                    .expect("internal metadata tail should drain"),
            );
            assert_eq!(ctx.namespace_commit_generation(), generation_before_internal_put);
            assert!(!ctx.namespace_commits_pending());
            assert!(store.scanner_data_usage_publication_admission_guard().await.is_some());
            assert!(set.scanner_data_usage_publication_admission_guard().await.is_some());
            let mut internal_reader = store
                .get_object_reader(RUSTFS_META_BUCKET, internal_object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("internal metadata should remain readable");
            let mut actual = Vec::new();
            internal_reader
                .stream
                .read_to_end(&mut actual)
                .await
                .expect("internal metadata body should drain");
            assert_eq!(actual, internal_body);
        })
        .await;
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn cancelled_early_ack_put_keeps_scanner_publication_blocked_until_tail_finishes() {
        let temp_dir = tempfile::tempdir().expect("create cancelled scanner PUT tail store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "scanner-cancelled-put-tail", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;
        let bucket = format!("scanner-cancelled-put-tail-{}", Uuid::new_v4());
        let object = "scanner-cancelled-tail";
        let body = vec![0xC3; 273];
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create cancelled scanner PUT tail bucket");

        temp_env::async_with_vars([(crate::set_disk::ENV_RUSTFS_PUT_RENAME_EARLY_ACK_ENABLE, Some("true"))], async {
            let tracker = crate::set_disk::rename_fanout_barrier::observe_tasks(object);
            let tail =
                crate::set_disk::rename_fanout_barrier::arm(object, 0, crate::set_disk::rename_fanout_barrier::PHASE_RENAME);
            let quorum = crate::set_disk::PutObjectCommitBarrier::install(
                &bucket,
                object,
                crate::set_disk::PutObjectCommitPause::AfterRenameQuorum,
            );
            let handoff = crate::set_disk::PutObjectCommitBarrier::install(
                &bucket,
                object,
                crate::set_disk::PutObjectCommitPause::AfterRenameHandoff,
            );
            let put_store = Arc::clone(&store);
            let put_bucket = bucket.clone();
            let put_body = body.clone();
            let put = tokio::spawn(async move {
                let mut reader = PutObjReader::from_vec(put_body);
                put_store
                    .put_object(&put_bucket, object, &mut reader, &ObjectOptions::default())
                    .await
            });
            tokio::time::timeout(Duration::from_secs(30), tail.wait_until_paused())
                .await
                .expect("cancelled PUT should pause one disk before rename");
            quorum.wait_until_paused().await;
            put.abort();
            assert!(
                put.await
                    .expect_err("caller should be cancelled after rename quorum")
                    .is_cancelled()
            );
            quorum.release();
            handoff.wait_until_paused().await;
            assert!(tracker.running() >= 1);
            assert!(ctx.namespace_commits_pending());
            assert!(!store.scanner_data_movement_activity().await.1);
            assert!(store.scanner_data_usage_publication_blocked().await);
            assert!(store.scanner_data_usage_publication_admission_guard().await.is_some());
            assert!(
                store.pools[0].disk_set[0]
                    .scanner_data_usage_publication_admission_guard()
                    .await
                    .is_some()
            );
            let generation = store.scanner_namespace_mutation_generation();

            handoff.release();
            tail.release();
            tokio::time::timeout(Duration::from_secs(30), async {
                while tracker.running() != 0 || ctx.namespace_commits_pending() {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .expect("cancelled request's detached fanout must release scanner admission after finishing");
            assert!(store.scanner_namespace_mutation_generation() > generation);
            assert!(!store.scanner_data_usage_publication_blocked().await);
            assert!(store.scanner_data_usage_publication_admission_guard().await.is_some());
            for (disk_index, disk) in store.pools[0].disk_set[0].disk_inventory().await.iter().enumerate() {
                let file_info = disk
                    .as_ref()
                    .expect("cancelled PUT fixture disk should remain online")
                    .read_version("", &bucket, object, "", &crate::disk::ReadOptions::default())
                    .await
                    .unwrap_or_else(|err| panic!("cancelled PUT must still publish on disk {disk_index}: {err}"));
                assert_eq!(file_info.size, i64::try_from(body.len()).expect("fixture body size should fit i64"));
            }
            let mut reader = store
                .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                .await
                .expect("a cancelled caller must not discard its quorum-committed object");
            let mut actual = Vec::new();
            reader
                .stream
                .read_to_end(&mut actual)
                .await
                .expect("cancelled PUT body should drain");
            assert_eq!(actual, body);
        })
        .await;
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[test]
    #[serial_test::serial(storage_class_env)]
    fn data_movement_multipart_part_staging_holds_no_publication_lock_or_tier_lease() {
        run_large_stack_async_test(
            "multipart-part-staging-publication-fence",
            data_movement_multipart_part_staging_holds_no_publication_lock_or_tier_lease_case,
        );
    }

    #[cfg(feature = "test-util")]
    async fn data_movement_multipart_part_staging_holds_no_publication_lock_or_tier_lease_case() {
        let temp_dir = tempfile::tempdir().expect("create multipart staging-fence store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "mpu-staging-publication", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(Arc::clone(&store), Vec::new()).await;

        let bucket = format!("mpu-staging-publication-{}", Uuid::new_v4());
        let object = "remote-owned.bin";
        let payload = b"multipart part staging must not publish an object".to_vec();
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("multipart staging test bucket should be created");

        let tier_name = "MPU-STAGING-TIER";
        register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let lease = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("mock tier generation should be available");
        let backend_identity = lease.backend_identity();
        drop(lease);

        let mut source_metadata = HashMap::from([(
            "x-amz-restore".to_string(),
            "ongoing-request=\"false\", expiry-date=\"2099-01-01T00:00:00Z\"".to_string(),
        )]);
        for (suffix, value) in [
            (
                rustfs_utils::http::SUFFIX_TRANSITION_STATUS,
                crate::bucket::lifecycle::core::TRANSITION_COMPLETE.to_string(),
            ),
            (rustfs_utils::http::SUFFIX_TRANSITIONED_OBJECTNAME, "remote/mpu.bin".to_string()),
            (rustfs_utils::http::SUFFIX_TRANSITION_TIER, tier_name.to_string()),
            (rustfs_utils::http::SUFFIX_TRANSITIONED_VERSION_ID, "remote-mpu-version".to_string()),
            (
                rustfs_utils::http::SUFFIX_TRANSITIONED_VERSION_STATE,
                rustfs_filemeta::TransitionVersionState::Exact.as_str().to_string(),
            ),
            (
                rustfs_utils::http::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
                rustfs_utils::crypto::hex(backend_identity),
            ),
        ] {
            rustfs_utils::http::metadata_compat::insert_str(&mut source_metadata, suffix, value);
        }
        let mut source_body = PutObjReader::from_vec(payload.clone());
        store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut source_body,
                &ObjectOptions {
                    user_defined: source_metadata,
                    ..Default::default()
                },
            )
            .await
            .expect("remote-owned source should be written to the source pool");
        let source = store.pools[0]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("remote-owned source metadata should be readable");
        let publication = store
            .acquire_remote_tuple_publication_fence(&bucket, 0, &source, true)
            .await
            .expect("multipart migration should capture its publication capability");
        assert_eq!(
            TierConfigMgr::active_operation_lease_count(&ctx.tier_config_mgr(), tier_name).await,
            0,
            "capturing the commit-late capability must not pin the tier generation"
        );

        let bucket_incarnation_id = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("bucket incarnation should resolve");
        mark_test_pool_decommissioning(&store, 0).await;
        let capacity_owner = test_decommission_capacity_owner(store.as_ref(), 0)
            .await
            .with_mutation_id(Uuid::new_v4());
        let upload_metadata = source.user_defined.as_ref().clone();
        let mut staging_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: source.version_id.is_some(),
            version_id: source.version_id.map(|version_id| version_id.to_string()),
            mod_time: source.mod_time,
            user_defined: upload_metadata,
            expected_bucket_incarnation_id: Some(bucket_incarnation_id),
            ..Default::default()
        };
        let upload_identity = crate::data_movement::data_movement_upload_identity_from_options(&staging_opts);
        rustfs_utils::http::insert_str(
            &mut staging_opts.user_defined,
            rustfs_utils::http::SUFFIX_DATA_MOVEMENT_UPLOAD,
            upload_identity,
        );
        capacity_owner.apply_to(&mut staging_opts);
        let (upload, target_pool_idx, staged_incarnation_id) = store
            .handle_new_multipart_upload_with_pool_idx(&bucket, object, &staging_opts, None)
            .await
            .expect("target multipart upload should be staged");
        assert_eq!(target_pool_idx, 1, "the active pool must receive the staged upload");
        assert_eq!(staged_incarnation_id, Some(bucket_incarnation_id));

        let barrier = crate::set_disk::MultipartCommitBarrier::install(
            &bucket,
            object,
            crate::set_disk::MultipartCommitPause::PutPartBeforeLockLost,
        );
        let part_store = Arc::clone(&store);
        let part_bucket = bucket.clone();
        let upload_id = upload.upload_id.clone();
        let part_payload = payload.clone();
        let part = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(part_payload);
            let mut part_opts = ObjectOptions {
                data_movement: true,
                src_pool_idx: 0,
                part_number: Some(1),
                expected_bucket_incarnation_id: Some(bucket_incarnation_id),
                ..Default::default()
            };
            capacity_owner.apply_to(&mut part_opts);
            part_store
                .put_object_part_for_data_movement(1, &part_bucket, object, &upload_id, &mut reader, &part_opts)
                .await
        });
        barrier.wait_until_paused().await;

        assert_eq!(
            TierConfigMgr::active_operation_lease_count(&ctx.tier_config_mgr(), tier_name).await,
            0,
            "UploadPart staging must not pin the tier generation"
        );
        let encoded = rustfs_utils::path::encode_dir_object(object);
        let mut proof_opts = ObjectOptions::default();
        let proof = tokio::time::timeout(
            Duration::from_millis(250),
            store.acquire_all_physical_object_read_locks("tier_delete_journal_recovery", &bucket, &encoded, &mut proof_opts),
        )
        .await
        .expect("UploadPart staging must not hold any object publication write lock")
        .expect("the all-physical recovery proof should acquire every object namespace");
        let source_head = store.pools[0]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("source HEAD should remain available while the part commit is paused");
        assert_eq!(source_head.etag, source.etag);
        assert!(
            store.pools[1]
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .is_err(),
            "UploadPart staging must not publish target object metadata"
        );
        drop(proof);

        barrier.release();
        drop(barrier);
        part.await
            .expect("UploadPart staging task should join")
            .expect("UploadPart staging should commit after the observation releases");
        let mut abort_opts = ObjectOptions {
            data_movement: true,
            src_pool_idx: 0,
            versioned: source.version_id.is_some(),
            version_id: source.version_id.map(|version_id| version_id.to_string()),
            mod_time: source.mod_time,
            expected_bucket_incarnation_id: Some(bucket_incarnation_id),
            ..Default::default()
        };
        capacity_owner.apply_to(&mut abort_opts);
        store
            .abort_multipart_upload_for_data_movement(1, &bucket, object, &upload.upload_id, &abort_opts)
            .await
            .expect("staged target upload should be removable without publishing");
        drop(publication);
        assert_eq!(TierConfigMgr::active_operation_lease_count(&ctx.tier_config_mgr(), tier_name).await, 0);
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    struct OfflineTestDisks {
        disks: Vec<crate::disk::DiskStore>,
    }

    #[cfg(feature = "test-util")]
    impl Drop for OfflineTestDisks {
        fn drop(&mut self) {
            for disk in &self.disks {
                disk.reset_health_for_store_init_retry();
            }
        }
    }

    #[cfg(feature = "test-util")]
    async fn force_set_disks_offline_for_test(set: &Arc<crate::set_disk::SetDisks>) -> OfflineTestDisks {
        let disks = set
            .disks
            .read()
            .await
            .iter()
            .map(|disk| disk.clone().expect("fault-injection set should start fully online"))
            .collect::<Vec<_>>();
        for disk in &disks {
            disk.close().await.expect("fault injection should stop per-disk monitoring");
            disk.force_runtime_state_for_test(crate::disk::health_state::RuntimeDriveHealthState::Offline);
        }

        // Sets has an independent endpoint monitor that renews missing slots.
        // Keep each slot populated by the original, deliberately faulty handle
        // and prove an explicit reconnect pass cannot heal this test fault.
        set.connect_disks().await;
        let current = set.disks.read().await.clone();
        assert_eq!(current.len(), disks.len());
        for (slot, expected) in current.iter().zip(&disks) {
            let disk = slot.as_ref().expect("offline test slot must remain populated");
            assert!(Arc::ptr_eq(disk, expected), "endpoint monitor must not replace an offline test handle");
            assert_eq!(disk.runtime_state(), crate::disk::health_state::RuntimeDriveHealthState::Offline);
            assert!(
                disk.is_online().await,
                "the valid disk identity must prevent endpoint renewal while the IO health gate remains offline"
            );
        }
        OfflineTestDisks { disks }
    }

    fn active_rebalance_meta_for_pool(pool_count: usize, active_pool_idx: usize) -> RebalanceMeta {
        let now = OffsetDateTime::now_utc();
        let mut pool_stats = vec![RebalanceStats::default(); pool_count];
        pool_stats[active_pool_idx] = RebalanceStats {
            participating: true,
            info: RebalanceInfo {
                start_time: Some(now),
                status: RebalStatus::Started,
                ..Default::default()
            },
            ..Default::default()
        };

        RebalanceMeta {
            id: uuid::Uuid::new_v4().to_string(),
            pool_stats,
            ..Default::default()
        }
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn quota_object_fence_ignores_an_unrelated_offline_pool() {
        let temp_dir = tempfile::tempdir().expect("create quota fence store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "quota-object-fence", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = format!("quota-object-fence-{}", uuid::Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create quota fence bucket");
        store.pools[1].disk_set[0].disks.write().await.fill(None);

        crate::bucket::quota::reservation::fence_namespace_mutations_for_test(&store, &bucket, object, Some((0, 0)))
            .await
            .expect("the selected pool fence should ignore an unrelated offline pool");
        let err = crate::bucket::quota::reservation::fence_namespace_mutations_for_test(&store, &bucket, object, None)
            .await
            .expect_err("legacy reservations must conservatively fence every pool");
        assert!(matches!(err, StorageError::ErasureWriteQuorum));

        shutdown.cancel();
    }

    async fn migrate_versioned_decommission_test_object(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        payload: &[u8],
        op_label: &'static str,
    ) -> (uuid::Uuid, FileInfoVersions) {
        let mut source = PutObjReader::from_vec(payload.to_vec());
        let source_info = store.pools[0]
            .put_object(
                bucket,
                object,
                &mut source,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("write versioned source to the pool being decommissioned");
        let source_version = source_info.version_id.expect("versioned source must have a version ID");
        let expected_source_versions = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("source versions should be readable before migration")
            .expect("source versions should exist before migration");
        {
            let mut pool_meta = store.pool_meta.write().await;
            pool_meta.pools[0].decommission = Some(PoolDecommissionInfo {
                start_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            });
        }

        let barrier = crate::set_disk::PutObjectCommitBarrier::install(
            bucket,
            object,
            crate::set_disk::PutObjectCommitPause::AfterNamespace,
        );
        let migration_store = Arc::clone(store);
        let migration_bucket = bucket.to_string();
        let migration_object = object.to_string();
        let migration = tokio::spawn(async move {
            let source_reader = migration_store.pools[0]
                .get_object_reader(
                    &migration_bucket,
                    &migration_object,
                    None,
                    HeaderMap::new(),
                    &ObjectOptions {
                        versioned: true,
                        version_id: Some(source_version.to_string()),
                        no_lock: true,
                        data_movement: true,
                        raw_data_movement_read: true,
                        ..Default::default()
                    },
                )
                .await?;
            crate::data_movement::migrate_decommission_object(
                migration_store,
                0,
                migration_bucket,
                source_reader,
                None,
                op_label,
                None,
            )
            .await
        });
        barrier.wait_until_paused().await;
        barrier.release();
        migration
            .await
            .expect("versioned decommission migration task should join")
            .expect("versioned decommission migration should commit");

        (source_version, expected_source_versions)
    }

    fn set_test_decommission_capacity_override(store: &Arc<crate::store::ECStore>, pool_idx: usize) {
        let layout = DecommissionErasureLayout { data: 2, parity: 2 };
        let source_physical_bytes = 1024 * 1024 * 1024;
        let target_physical_bytes = source_physical_bytes * 8;
        let capacity: Vec<DecommissionPoolCapacityInfo> = store
            .pools
            .iter()
            .enumerate()
            .map(|(index, _)| {
                if index == pool_idx {
                    DecommissionPoolCapacityInfo::for_test(index, layout, 0, source_physical_bytes, source_physical_bytes)
                } else {
                    DecommissionPoolCapacityInfo::for_test(index, layout, target_physical_bytes, target_physical_bytes, 0)
                }
            })
            .collect();
        set_decommission_capacity_info_overrides_for_test(store.id, (0..128).map(|_| capacity.clone()).collect());
    }

    async fn mark_test_pool_decommissioning(store: &Arc<crate::store::ECStore>, pool_idx: usize) {
        set_test_decommission_capacity_override(store, pool_idx);
        store
            .save_current_pool_meta_for_decommission_start(&[pool_idx], Vec::new())
            .await
            .expect("test decommission capacity reservation should activate");
    }

    #[cfg(feature = "test-util")]
    async fn mark_test_pool_decommissioning_with_split_targets(store: &Arc<crate::store::ECStore>, pool_idx: usize) {
        let layout = DecommissionErasureLayout { data: 2, parity: 2 };
        let source_physical_bytes = 1024 * 1024 * 1024;
        let capacity = store
            .pools
            .iter()
            .enumerate()
            .map(|(index, _)| {
                if index == pool_idx {
                    DecommissionPoolCapacityInfo::for_test(index, layout, 0, source_physical_bytes, source_physical_bytes)
                } else {
                    // The reservation peak is twice the predicted target size.
                    // Give each target only half so the real allocator must
                    // authorize both receipt-bearing pools.
                    DecommissionPoolCapacityInfo::for_test(index, layout, source_physical_bytes, source_physical_bytes, 0)
                }
            })
            .collect::<Vec<_>>();
        set_decommission_capacity_info_overrides_for_test(store.id, (0..128).map(|_| capacity.clone()).collect());
        store
            .save_current_pool_meta_for_decommission_start(&[pool_idx], Vec::new())
            .await
            .expect("split-target decommission capacity reservation should activate");
    }

    #[cfg(feature = "test-util")]
    async fn test_decommission_capacity_owner(
        store: &crate::store::ECStore,
        source_pool_index: usize,
    ) -> crate::core::pools::DecommissionCapacityOwner {
        let pool_meta = store.pool_meta.read().await;
        let reservation = pool_meta.pools[source_pool_index]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("test decommission capacity reservation should exist");
        crate::core::pools::DecommissionCapacityOwner {
            source_pool_index,
            operation_id: reservation.operation_id,
            generation: reservation.generation,
            owner_nonce: reservation.owner_nonce,
            mutation_id: None,
        }
    }

    const DECOMMISSION_TEST_FAULT_STAGE_DELETE_MARKER: &str = "delete_marker_copy";
    const DECOMMISSION_TEST_FAULT_STAGE_MIGRATE_OBJECT: &str = "migrate_object";
    #[cfg(feature = "test-util")]
    const DECOMMISSION_TEST_FAULT_STAGE_TIERED: &str = "decommission_tiered_object";

    fn decommission_retry_fault_hook(
        bucket: &str,
        object: &str,
        faults: Arc<AtomicUsize>,
    ) -> crate::core::pools::DecommissionTestFaultDecision {
        let target_bucket = bucket.to_string();
        let target_object = object.to_string();
        Arc::new(move |stage, bucket, object, attempt, succeeded| {
            if !succeeded
                || attempt >= crate::core::pools::DECOMMISSION_VERSION_COPY_ATTEMPTS
                || stage != DECOMMISSION_TEST_FAULT_STAGE_MIGRATE_OBJECT
                || bucket != target_bucket
                || object != target_object
            {
                return false;
            }

            // Entry retries reset the local attempt; real copy errors can skip
            // successful attempts. Only injected faults spend this global budget.
            // A real failure may consume an attempt, so preserve the final chance.
            faults
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |faults| {
                    (faults < crate::core::pools::DECOMMISSION_VERSION_COPY_ATTEMPTS.saturating_sub(1))
                        .then_some(faults.saturating_add(1))
                })
                .is_ok()
        })
    }

    async fn seed_decommission_source(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        body: Vec<u8>,
        opts: &ObjectOptions,
    ) {
        let mut reader = PutObjReader::from_vec(body);
        store.pools[0]
            .put_object(bucket, object, &mut reader, opts)
            .await
            .expect("seed decommission source object");
    }

    async fn run_decommission_entry_retry_test(
        store: &Arc<crate::store::ECStore>,
        rx: CancellationToken,
        bucket: &str,
        object: &str,
        expected_bucket_incarnation_id: Option<uuid::Uuid>,
        source_changed_exhaustions: Arc<AtomicUsize>,
    ) -> crate::error::Result<()> {
        let source_set = store.pools[0].get_disks_by_key(object);
        store
            .decommission_entry_with_retry_state_for_test(
                rx,
                0,
                MetaCacheEntry {
                    name: object.to_string(),
                    ..Default::default()
                },
                bucket.to_string(),
                source_set,
                expected_bucket_incarnation_id,
                source_changed_exhaustions,
            )
            .await
    }

    async fn read_decommission_target_body(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Vec<u8> {
        let mut reader = store.pools[1]
            .get_object_reader(bucket, object, None, HeaderMap::new(), opts)
            .await
            .expect("read decommission target object");
        let mut body = Vec::new();
        reader
            .stream
            .read_to_end(&mut body)
            .await
            .expect("drain decommission target body");
        body
    }

    async fn assert_decommission_source_absent(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) {
        let err = store.pools[0]
            .get_object_info(bucket, object, opts)
            .await
            .expect_err("decommission source must be retained until target commit, then removed");
        assert!(
            matches!(err, StorageError::ObjectNotFound(_, _) | StorageError::VersionNotFound(_, _, _)),
            "unexpected decommission source result: {err:?}"
        );
    }

    #[cfg(feature = "test-util")]
    async fn seed_transitioned_free_version(
        ctx: &Arc<crate::runtime::instance::InstanceContext>,
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
    ) -> (uuid::Uuid, uuid::Uuid) {
        let tier_name = format!("DECOMFREE{}", uuid::Uuid::new_v4().simple());
        register_mock_tier(&ctx.tier_config_mgr(), &tier_name).await;

        let mut reader = PutObjReader::from_vec(b"transitioned source bytes".to_vec());
        let source = store.pools[0]
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("write transitioned decommission source");
        let source_version = source.version_id.expect("transitioned source must be versioned");
        store.pools[0]
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.to_string()),
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name,
                        etag: source.etag.clone().expect("transitioned source must have an ETag"),
                        ..Default::default()
                    },
                    mod_time: source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("transition source before decommission");
        store.pools[0]
            .delete_object(
                bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("delete transitioned source version");

        let versions = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("source versions should decode after transition delete")
            .expect("source free version should remain after transition delete");
        let free_version = versions
            .versions
            .iter()
            .find(|version| version.tier_free_version())
            .and_then(|version| version.version_id)
            .expect("transition delete should create a free version");
        (source_version, free_version)
    }

    async fn write_decommission_test_multipart_source(
        store: &Arc<crate::store::ECStore>,
        pool_idx: usize,
        bucket: &str,
        object: &str,
    ) {
        let pool = &store.pools[pool_idx];
        let upload = pool
            .new_multipart_upload(bucket, object, &ObjectOptions::default())
            .await
            .expect("create decommission multipart source upload");
        let first_part = vec![b'm'; 5 * 1024 * 1024];
        let second_part = b"decommission multipart tail".to_vec();
        let mut completed_parts = Vec::with_capacity(2);
        for (part_number, body) in [(1, first_part), (2, second_part)] {
            let mut reader = PutObjReader::from_vec(body);
            let part = pool
                .put_object_part(bucket, object, &upload.upload_id, part_number, &mut reader, &ObjectOptions::default())
                .await
                .expect("write decommission multipart source part");
            completed_parts.push(crate::storage_api_contracts::multipart::CompletePart {
                part_num: part.part_num,
                etag: part.etag,
                ..Default::default()
            });
        }
        pool.clone()
            .complete_multipart_upload(bucket, object, &upload.upload_id, completed_parts, &ObjectOptions::default())
            .await
            .expect("complete decommission multipart source object");
    }

    struct DataMovementSourceSeed {
        pool_idx: usize,
        version_id: Option<Uuid>,
        body: Vec<u8>,
        multipart_split: Option<usize>,
        mod_time: OffsetDateTime,
        user_defined: HashMap<String, String>,
    }

    async fn seed_data_movement_source_reader(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        seed: DataMovementSourceSeed,
    ) -> GetObjectReader {
        let DataMovementSourceSeed {
            pool_idx,
            version_id,
            body,
            multipart_split,
            mod_time,
            user_defined,
        } = seed;
        let pool = &store.pools[pool_idx];
        let write_opts = ObjectOptions {
            versioned: version_id.is_some(),
            version_id: version_id.map(|version_id| version_id.to_string()),
            mod_time: Some(mod_time),
            user_defined,
            ..Default::default()
        };
        if let Some(split) = multipart_split {
            assert!(split > 0 && split < body.len(), "multipart source split must create two non-empty parts");
            let upload = pool
                .new_multipart_upload(bucket, object, &write_opts)
                .await
                .expect("create physical data-movement source upload");
            let mut completed_parts = Vec::with_capacity(2);
            for (part_number, bytes) in [(1, &body[..split]), (2, &body[split..])] {
                let mut reader = PutObjReader::from_vec(bytes.to_vec());
                let part = pool
                    .put_object_part(bucket, object, &upload.upload_id, part_number, &mut reader, &ObjectOptions::default())
                    .await
                    .expect("write physical data-movement source part");
                completed_parts.push(crate::storage_api_contracts::multipart::CompletePart {
                    part_num: part.part_num,
                    etag: part.etag,
                    ..Default::default()
                });
            }
            pool.clone()
                .complete_multipart_upload(bucket, object, &upload.upload_id, completed_parts, &write_opts)
                .await
                .expect("complete physical data-movement source object");
        } else {
            let mut reader = PutObjReader::from_vec(body);
            pool.put_object(bucket, object, &mut reader, &write_opts)
                .await
                .expect("write physical data-movement source object");
        }

        pool.get_object_reader(
            bucket,
            object,
            None,
            HeaderMap::new(),
            &ObjectOptions {
                versioned: version_id.is_some(),
                version_id: version_id.map(|version_id| version_id.to_string()),
                raw_data_movement_read: true,
                include_part_checksums: true,
                ..Default::default()
            },
        )
        .await
        .expect("read the physical data-movement source snapshot")
    }

    async fn assert_pool_object_present(pool: &Arc<crate::core::sets::Sets>, bucket: &str, object: &str) {
        pool.get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("expected object generation must remain present");
    }

    async fn assert_pool_object_absent(pool: &Arc<crate::core::sets::Sets>, bucket: &str, object: &str) {
        let err = pool
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect_err("fenced decommission target must remain absent");
        assert!(
            matches!(err, StorageError::ObjectNotFound(_, _) | StorageError::VersionNotFound(_, _, _)),
            "unexpected fenced target result: {err:?}"
        );
    }

    async fn write_suspended_decommission_source(store: &Arc<crate::store::ECStore>, bucket: &str, object: &str) {
        let mut reader = PutObjReader::from_vec(b"suspended source generation".to_vec());
        let source = store.pools[0]
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    version_suspended: true,
                    mod_time: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND),
                    ..Default::default()
                },
            )
            .await
            .expect("write suspended null source version");
        assert!(
            source.version_id.is_none_or(|version_id| version_id.is_nil()),
            "suspended source must use the null version identity"
        );
    }

    async fn assert_suspended_null_source_present(store: &Arc<crate::store::ECStore>, bucket: &str, object: &str) {
        let versions = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("suspended source versions should be readable")
            .expect("suspended source must exist before worker convergence");
        assert!(
            versions
                .versions
                .iter()
                .any(|version| !version.deleted && version.version_id.is_none_or(|version_id| version_id.is_nil())),
            "the source pool must retain its null data version while DELETE owns the fixed fence"
        );
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tag_updates_skip_active_rebalance_source_pool() {
        let temp_dir = tempfile::tempdir().expect("create writer-fencing store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "writer-fencing-tags", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("writer-fencing-tags-{}", uuid::Uuid::new_v4());
        let object = "tagged-object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create writer fencing bucket");

        let old_time = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixed timestamp should be valid");
        let newer_time = old_time + time::Duration::seconds(10);
        let mut source_reader = PutObjReader::from_vec(b"source-body".to_vec());
        store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut source_reader,
                &ObjectOptions {
                    mod_time: Some(newer_time),
                    ..Default::default()
                },
            )
            .await
            .expect("write newer source object");
        let mut target_reader = PutObjReader::from_vec(b"target-body".to_vec());
        store.pools[1]
            .put_object(
                &bucket,
                object,
                &mut target_reader,
                &ObjectOptions {
                    mod_time: Some(old_time),
                    ..Default::default()
                },
            )
            .await
            .expect("write older target object");

        *store.rebalance_meta.write().await = Some(active_rebalance_meta_for_pool(store.pools.len(), 0));
        assert!(store.is_pool_rebalancing(0).await, "pool 0 must be marked as an active rebalance source");

        let tags = "rebalance=target";
        assert_ne!(
            store.pools[0]
                .get_object_tags(&bucket, object, &ObjectOptions::default())
                .await
                .expect("source object tags should be readable before update"),
            tags,
            "source object must start without the target tag"
        );
        assert_ne!(
            store.pools[1]
                .get_object_tags(&bucket, object, &ObjectOptions::default())
                .await
                .expect("target object tags should be readable before update"),
            tags,
            "target object must start without the target tag"
        );
        let selected_pool = store
            .get_pool_idx_existing_with_opts(
                &bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_chg: true,
                    skip_decommissioned: true,
                    skip_rebalancing: true,
                    ..Default::default()
                },
            )
            .await
            .expect("writer lookup should select an existing non-rebalancing pool");
        assert_eq!(selected_pool, 1, "writer lookup must skip active rebalance pool 0");

        let updated = store
            .put_object_tags(&bucket, object, tags, &ObjectOptions::default())
            .await
            .expect("tag update should use the non-rebalancing target pool");
        assert_eq!(
            updated.mod_time,
            Some(old_time),
            "tag update must return the non-rebalancing pool object rather than the newer active source"
        );

        let target_tags = store.pools[1]
            .get_object_tags(&bucket, object, &ObjectOptions::default())
            .await
            .expect("target object tags should be readable");
        assert_eq!(target_tags, tags, "non-rebalancing pool must receive writer tag updates");

        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn multipart_listing_skips_active_rebalance_source_pool() {
        let temp_dir = tempfile::tempdir().expect("create multipart writer-fencing store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "writer-fencing-multipart", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("writer-fencing-multipart-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create multipart writer fencing bucket");

        let incarnation = store.bucket_incarnation_id(&bucket).await.expect("read bucket incarnation");
        let lifecycle_guard = store
            .acquire_bucket_lifecycle_read_lock(&bucket)
            .await
            .expect("acquire multipart test lifecycle fence");
        let mut upload_opts = ObjectOptions {
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        upload_opts.add_bucket_lifecycle_lock_guard(&lifecycle_guard);
        let source_upload = store.pools[0]
            .new_multipart_upload(&bucket, "source-only.bin", &upload_opts)
            .await
            .expect("create source upload");
        let target_upload = store.pools[1]
            .new_multipart_upload(&bucket, "target-visible.bin", &upload_opts)
            .await
            .expect("create target upload");

        *store.rebalance_meta.write().await = Some(active_rebalance_meta_for_pool(store.pools.len(), 0));
        assert!(store.is_pool_rebalancing(0).await, "pool 0 must be marked as an active rebalance source");

        let listed = store
            .list_multipart_uploads(&bucket, "", None, None, None, 100)
            .await
            .expect("list multipart uploads");
        let listed_uploads: Vec<(&str, &str)> = listed
            .uploads
            .iter()
            .map(|upload| (upload.object.as_str(), upload.upload_id.as_str()))
            .collect();

        assert!(
            !listed_uploads.contains(&("source-only.bin", source_upload.upload_id.as_str())),
            "active source pool upload must be hidden from multipart listing"
        );
        assert!(
            listed_uploads.contains(&("target-visible.bin", target_upload.upload_id.as_str())),
            "non-rebalancing pool upload must remain visible"
        );

        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn suspended_decommission_source_multipart_remains_operable_until_drained() {
        let temp_dir = tempfile::tempdir().expect("create decommission multipart drain store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "decommission-multipart-drain", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("decommission-mp-drain-{}", uuid::Uuid::new_v4());
        let complete_object = "complete.bin";
        let abort_object = "abort.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create decommission multipart drain bucket");

        let incarnation = store.bucket_incarnation_id(&bucket).await.expect("read bucket incarnation");
        let lifecycle_guard = store
            .acquire_bucket_lifecycle_read_lock(&bucket)
            .await
            .expect("acquire multipart creation lifecycle fence");
        let mut upload_opts = ObjectOptions {
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        upload_opts.add_bucket_lifecycle_lock_guard(&lifecycle_guard);
        let complete_upload = store.pools[0]
            .new_multipart_upload(&bucket, complete_object, &upload_opts)
            .await
            .expect("create source upload to complete");
        let abort_upload = store.pools[0]
            .new_multipart_upload(&bucket, abort_object, &upload_opts)
            .await
            .expect("create source upload to abort");
        drop(lifecycle_guard);

        mark_test_pool_decommissioning(&store, 0).await;
        let err = store
            .ensure_decommission_multipart_uploads_drained_for_test(0)
            .await
            .expect_err("an unresolved source multipart upload must block final decommission");
        let drain_error = err.to_string();
        assert!(
            drain_error.contains("still contains multipart upload") && drain_error.contains(&bucket),
            "the drain error must identify both the upload path and user bucket: {drain_error}"
        );

        let listed = store
            .list_multipart_uploads(&bucket, "", None, None, None, 100)
            .await
            .expect("list uploads from suspended decommission source");
        assert!(
            listed
                .uploads
                .iter()
                .any(|upload| upload.upload_id.as_str() == complete_upload.upload_id.as_str()),
            "the upload selected before suspension must remain visible"
        );
        store
            .get_multipart_info(&bucket, complete_object, &complete_upload.upload_id, &ObjectOptions::default())
            .await
            .expect("read upload metadata from suspended decommission source");

        let mut part_reader = PutObjReader::from_vec(b"multipart body".to_vec());
        let part = store
            .put_object_part(
                &bucket,
                complete_object,
                &complete_upload.upload_id,
                1,
                &mut part_reader,
                &ObjectOptions::default(),
            )
            .await
            .expect("write part to suspended decommission source");
        let parts = store
            .list_object_parts(&bucket, complete_object, &complete_upload.upload_id, None, 100, &ObjectOptions::default())
            .await
            .expect("list parts from suspended decommission source");
        assert_eq!(parts.parts.len(), 1);
        assert_eq!(parts.parts[0].etag.as_deref(), part.etag.as_deref());

        store
            .clone()
            .complete_multipart_upload(
                &bucket,
                complete_object,
                &complete_upload.upload_id,
                vec![crate::storage_api_contracts::multipart::CompletePart {
                    part_num: part.part_num,
                    etag: part.etag,
                    ..Default::default()
                }],
                &ObjectOptions::default(),
            )
            .await
            .expect("complete upload on suspended decommission source");
        store
            .abort_multipart_upload(&bucket, abort_object, &abort_upload.upload_id, &ObjectOptions::default())
            .await
            .expect("abort upload on suspended decommission source");

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match store.ensure_decommission_multipart_uploads_drained_for_test(0).await {
                    Ok(()) => break,
                    Err(err) if err.to_string().contains("still contains multipart upload") => {
                        tokio::time::sleep(Duration::from_millis(10)).await;
                    }
                    Err(err) => panic!("unexpected final decommission drain error: {err:?}"),
                }
            }
        })
        .await
        .expect("final decommission gate should open after upload cleanup converges");
        assert_pool_object_present(&store.pools[0], &bucket, complete_object).await;

        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn active_multipart_upload_routes_before_faulted_suspended_source() {
        use sha2::Digest;

        let temp_dir = tempfile::tempdir().expect("create active-first multipart routing store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "active-first-multipart-routing", &[4, 4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("active-first-mp-route-{}", uuid::Uuid::new_v4());
        let object = "target-upload.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create active-first multipart routing bucket");

        let incarnation = store.bucket_incarnation_id(&bucket).await.expect("read bucket incarnation");
        let lifecycle_guard = store
            .acquire_bucket_lifecycle_read_lock(&bucket)
            .await
            .expect("acquire multipart creation lifecycle fence");
        let mut upload_opts = ObjectOptions {
            expected_bucket_incarnation_id: Some(incarnation),
            ..Default::default()
        };
        upload_opts.add_bucket_lifecycle_lock_guard(&lifecycle_guard);
        let upload = store.pools[1]
            .new_multipart_upload(&bucket, object, &upload_opts)
            .await
            .expect("create upload in active target pool");
        drop(lifecycle_guard);

        mark_test_pool_decommissioning(&store, 0).await;
        // Corrupt only the source pool's entry for this UploadID. Taking the
        // whole source set offline would also make the bucket-incarnation
        // sidecar unreadable before multipart routing is reached.
        let upload_sha =
            hex_simd::encode_to_string(sha2::Sha256::digest(format!("{bucket}/{object}").as_bytes()), hex_simd::AsciiCase::Lower);
        let upload_uuid = crate::runtime::sources::upload_uuid_suffix(&upload.upload_id);
        for disk_index in 0..4 {
            let metadata_path = temp_dir
                .path()
                .join(format!("pool0/set0/disk{disk_index}"))
                .join(crate::disk::RUSTFS_META_MULTIPART_BUCKET)
                .join(&upload_sha)
                .join(&upload_uuid)
                .join(crate::disk::STORAGE_FORMAT_FILE);
            tokio::fs::create_dir_all(metadata_path.parent().expect("multipart metadata path should have a parent"))
                .await
                .expect("create corrupt source upload directory");
            tokio::fs::write(metadata_path, b"not-xl-meta")
                .await
                .expect("inject source upload metadata read failure");
        }

        let source_result = store.pools[0]
            .get_multipart_info(&bucket, object, &upload.upload_id, &ObjectOptions::default())
            .await;
        let routed_result = store
            .get_multipart_info(&bucket, object, &upload.upload_id, &ObjectOptions::default())
            .await;

        assert!(
            source_result
                .as_ref()
                .is_err_and(|err| !crate::error::is_err_invalid_upload_id(err)),
            "the suspended source must expose the injected hard read failure: {source_result:?}"
        );
        let routed = routed_result.expect("the active target UploadID must be resolved before the faulted suspended source");
        assert_eq!(routed.upload_id, upload.upload_id);

        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn delete_objects_skips_active_rebalance_source_pool() {
        let temp_dir = tempfile::tempdir().expect("create batch-delete writer-fencing store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "batch-delete-rebalance", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("batch-delete-rebalance-{}", uuid::Uuid::new_v4());
        let object = "delete-me.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create batch delete rebalance bucket");

        let mut source_reader = PutObjReader::from_vec(b"source-body".to_vec());
        store.pools[0]
            .put_object(&bucket, object, &mut source_reader, &ObjectOptions::default())
            .await
            .expect("write object on active source pool");
        let mut target_reader = PutObjReader::from_vec(b"target-body".to_vec());
        store.pools[1]
            .put_object(&bucket, object, &mut target_reader, &ObjectOptions::default())
            .await
            .expect("write object on non-rebalancing target pool");

        let mut pool_stats = vec![RebalanceStats::default(); store.pools.len()];
        pool_stats[0] = RebalanceStats {
            participating: true,
            info: RebalanceInfo {
                start_time: Some(OffsetDateTime::now_utc()),
                status: RebalStatus::Started,
                ..Default::default()
            },
            ..Default::default()
        };
        *store.rebalance_meta.write().await = Some(RebalanceMeta {
            id: uuid::Uuid::new_v4().to_string(),
            pool_stats,
            ..Default::default()
        });
        assert!(store.is_pool_rebalancing(0).await, "pool 0 must be marked as an active rebalance source");

        let (deleted, errs) = store
            .delete_objects(
                &bucket,
                vec![crate::storage_api_contracts::object::ObjectToDelete {
                    object_name: object.to_string(),
                    ..Default::default()
                }],
                ObjectOptions::default(),
            )
            .await;
        assert!(matches!(errs.as_slice(), [None]), "batch delete must not fail: {errs:?}");
        assert!(
            matches!(deleted.as_slice(), [deleted] if deleted.found && deleted.object_name == object),
            "batch delete must report the non-rebalancing pool deletion"
        );

        store.pools[0]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("active source pool object must not be deleted by DeleteObjects");
        let target_err = store.pools[1]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect_err("non-rebalancing target pool object must be deleted");
        assert!(
            matches!(target_err, StorageError::ObjectNotFound(_, _)),
            "target pool should report object not found after DeleteObjects, got {target_err:?}"
        );

        shutdown.cancel();
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn data_movement_conflicts_and_multipart_retries_converge_safely() {
        // Run this large async scenario on a larger dedicated stack so debug
        // test threads do not overflow before the regression assertions.
        std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .build()
                    .expect("build data movement test runtime");
                runtime.block_on(async move {
                    let temp_dir = tempfile::tempdir().expect("create data movement store dir");
                    let (_ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
                        temp_dir.path(),
                        "data-movement-conflict-convergence",
                        &[4, 4],
                    ))
                    .await;
                    crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

                    let bucket = format!("data-movement-conflict-{}", uuid::Uuid::new_v4());
                    store
                        .make_bucket(&bucket, &MakeBucketOptions::default())
                        .await
                        .expect("create data movement bucket");
                    let source_mod_time = OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND;
                    let target_mod_time = source_mod_time + time::Duration::SECOND;

                    let object = "single-object";
                    let target_body = b"newer client body".to_vec();
                    let mut target_reader = PutObjReader::from_vec(target_body.clone());
                    store.pools[1]
                        .put_object(
                            &bucket,
                            object,
                            &mut target_reader,
                            &ObjectOptions {
                                mod_time: Some(target_mod_time),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("write newer single-part target");

                    let source_body = b"stale migration body".to_vec();
                    let source_reader = seed_data_movement_source_reader(
                        &store,
                        &bucket,
                        object,
                        DataMovementSourceSeed {
                            pool_idx: 0,
                            version_id: None,
                            body: source_body,
                            multipart_split: None,
                            mod_time: source_mod_time,
                            user_defined: HashMap::new(),
                        },
                    )
                    .await;
                    crate::data_movement::migrate_object(
                        store.clone(),
                        0,
                        bucket.clone(),
                        source_reader,
                        None,
                        "test_data_movement",
                    )
                    .await
                    .expect("newer single-part target should converge migration");

                    let mut reader = store.pools[1]
                        .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
                        .await
                        .expect("read converged single-part target");
                    let mut body = Vec::new();
                    reader.stream.read_to_end(&mut body).await.expect("drain single-part target");
                    assert_eq!(body, target_body);

                    let multipart_object = "multipart-object";
                    let multipart_target_body = b"newer multipart client body".to_vec();
                    let mut multipart_target_reader = PutObjReader::from_vec(multipart_target_body.clone());
                    store.pools[1]
                        .put_object(
                            &bucket,
                            multipart_object,
                            &mut multipart_target_reader,
                            &ObjectOptions {
                                mod_time: Some(target_mod_time),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("write newer multipart target");

                    let first_part_size = 5 * 1024 * 1024;
                    let mut multipart_source_body = vec![b'a'; first_part_size];
                    multipart_source_body.push(b'b');
                    let multipart_source_reader = seed_data_movement_source_reader(
                        &store,
                        &bucket,
                        multipart_object,
                        DataMovementSourceSeed {
                            pool_idx: 0,
                            version_id: None,
                            body: multipart_source_body,
                            multipart_split: Some(first_part_size),
                            mod_time: source_mod_time,
                            user_defined: HashMap::new(),
                        },
                    )
                    .await;
                    crate::data_movement::migrate_object(
                        store.clone(),
                        0,
                        bucket.clone(),
                        multipart_source_reader,
                        None,
                        "test_data_movement",
                    )
                    .await
                    .expect("newer multipart target should converge migration");

                    let uploads = store.pools[1]
                        .list_multipart_uploads(&bucket, multipart_object, None, None, None, 100)
                        .await
                        .expect("list target pool multipart uploads");
                    assert!(uploads.uploads.is_empty(), "superseded migration staging must be aborted");

                    let mut reader = store.pools[1]
                        .get_object_reader(&bucket, multipart_object, None, HeaderMap::new(), &ObjectOptions::default())
                        .await
                        .expect("read converged multipart target");
                    let mut body = Vec::new();
                    reader.stream.read_to_end(&mut body).await.expect("drain multipart target");
                    assert_eq!(body, multipart_target_body);

                    let retry_object = "multipart-retry-object";
                    let retry_object_etag = "0123456789abcdef0123456789abcdef".to_string();
                    let retry_first_part_size = 5 * 1024 * 1024;
                    let retry_object_mod_time =
                        OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixed retry timestamp should be valid");
                    let mut retry_source_body = vec![b'c'; retry_first_part_size];
                    retry_source_body.push(b'd');
                    let retry_first_checksum =
                        Checksum::new_from_data(ChecksumType::CRC32C, &retry_source_body[..retry_first_part_size])
                            .expect("first source part checksum should be created");
                    let retry_second_checksum =
                        Checksum::new_from_data(ChecksumType::CRC32C, &retry_source_body[retry_first_part_size..])
                            .expect("second source part checksum should be created");
                    let mut retry_checksum_parts = retry_first_checksum.raw.clone();
                    retry_checksum_parts.extend_from_slice(&retry_second_checksum.raw);
                    let mut retry_checksum_type = ChecksumType::CRC32C;
                    retry_checksum_type
                        .merge(ChecksumType::MULTIPART)
                        .merge(ChecksumType::INCLUDES_MULTIPART);
                    let retry_object_checksum = Checksum::new_from_data(retry_checksum_type, &retry_checksum_parts)
                        .expect("source multipart checksum should be created");
                    let retry_object_checksum_bytes = retry_object_checksum.to_bytes(&retry_checksum_parts);
                    let mut retry_metadata = HashMap::from([("x-amz-meta-retry".to_string(), "stable".to_string())]);
                    insert_str(
                        &mut retry_metadata,
                        SUFFIX_COMPRESSION,
                        compression_metadata_value(CompressionAlgorithm::default()),
                    );
                    let retry_upload = store.pools[0]
                        .new_multipart_upload(
                            &bucket,
                            retry_object,
                            &ObjectOptions {
                                user_defined: retry_metadata,
                                want_checksum: Some(retry_object_checksum.clone()),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("create source multipart upload");
                    let mut retry_parts = Vec::with_capacity(2);
                    for (part_number, plaintext, checksum) in [
                        (1, &retry_source_body[..retry_first_part_size], &retry_first_checksum),
                        (2, &retry_source_body[retry_first_part_size..], &retry_second_checksum),
                    ] {
                        let part_size = i64::try_from(plaintext.len()).expect("source part size should fit i64");
                        let mut plaintext_reader = rustfs_rio::HashReader::from_stream(
                            Cursor::new(plaintext.to_vec()),
                            part_size,
                            part_size,
                            None,
                            None,
                            false,
                        )
                        .expect("create source part plaintext reader");
                        plaintext_reader
                            .add_non_trailing_checksum(Some(checksum.clone()), false)
                            .expect("set source part checksum");
                        let compressed = WritePlan::new()
                            .with_compression(CompressionAlgorithm::default())
                            .apply(plaintext_reader, part_size)
                            .expect("compress source part");
                        let mut reader = PutObjReader::new(compressed);
                        let part = store.pools[0]
                            .put_object_part(
                                &bucket,
                                retry_object,
                                &retry_upload.upload_id,
                                part_number,
                                &mut reader,
                                &ObjectOptions::default(),
                            )
                            .await
                            .expect("write source multipart part");
                        retry_parts.push(crate::storage_api_contracts::multipart::CompletePart {
                            part_num: part.part_num,
                            etag: part.etag,
                            checksum_crc32c: Some(checksum.encoded.clone()),
                            ..Default::default()
                        });
                    }
                    store.pools[0]
                        .clone()
                        .complete_multipart_upload(
                            &bucket,
                            retry_object,
                            &retry_upload.upload_id,
                            retry_parts,
                            &ObjectOptions {
                                mod_time: Some(retry_object_mod_time),
                                want_checksum: Some(retry_object_checksum),
                                preserve_etag: Some(retry_object_etag.clone()),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("complete source multipart object");

                    *store.rebalance_meta.write().await = Some(active_rebalance_meta_for_pool(store.pools.len(), 0));
                    assert!(store.is_pool_rebalancing(0).await, "pool 0 must be marked as an active rebalance source");

                    let mut retry_reader = store.pools[0]
                        .get_object_reader(
                            &bucket,
                            retry_object,
                            None,
                            HeaderMap::new(),
                            &ObjectOptions {
                                raw_data_movement_read: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read source multipart object for first migration");
                    let mut retry_source_info = retry_reader.object_info.clone();
                    let mut retry_source_parts = retry_source_info.parts.as_ref().clone();
                    for (part, checksum) in retry_source_parts
                        .iter_mut()
                        .zip([&retry_first_checksum, &retry_second_checksum])
                    {
                        part.mod_time = Some(OffsetDateTime::UNIX_EPOCH);
                        part.checksums = Some(HashMap::from([(ChecksumType::CRC32C.to_string(), checksum.encoded.clone())]));
                    }
                    retry_source_info.parts = Arc::new(retry_source_parts);
                    assert_eq!(retry_source_info.etag.as_deref(), Some(retry_object_etag.as_str()));
                    assert!(retry_source_info.is_multipart());
                    assert!(retry_source_info.parts.iter().all(|part| part.checksums.is_some()));
                    assert_eq!(retry_source_info.checksum.as_deref(), Some(retry_object_checksum_bytes.as_ref()));
                    assert!(
                        !retry_source_info
                            .user_defined
                            .contains_key(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM)
                    );
                    assert!(
                        !retry_source_info
                            .user_defined
                            .contains_key(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE)
                    );
                    retry_reader.object_info = retry_source_info.clone();
                    temp_env::async_with_vars(
                        [
                            (rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_WRITE, None::<&str>),
                            (rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_FLEET_CONFIRMED, None::<&str>),
                        ],
                        crate::data_movement::migrate_object(
                            store.clone(),
                            0,
                            bucket.clone(),
                            retry_reader,
                            None,
                            "test_data_movement_compatible_retry",
                        ),
                    )
                    .await
                    .expect("default multipart migration should not require the checksum sidecar capability");

                    let compatible_target = store.pools[1]
                        .get_object_info(
                            &bucket,
                            retry_object,
                            &ObjectOptions {
                                include_part_checksums: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read compatible multipart target metadata");
                    assert_eq!(compatible_target.checksum, retry_source_info.checksum);
                    assert!(compatible_target.parts.iter().all(|part| part.checksums.is_none()));
                    assert_eq!(
                        rustfs_utils::http::get_consistent_str(
                            &compatible_target.user_defined,
                            rustfs_utils::http::SUFFIX_DATA_MOVED
                        ),
                        Some("true")
                    );
                    assert!(!rustfs_utils::http::contains_key_str(
                        &compatible_target.user_defined,
                        rustfs_utils::http::SUFFIX_PART_CHECKSUMS
                    ));
                    let mut compatible_reader = store.pools[1]
                        .get_object_reader(&bucket, retry_object, None, HeaderMap::new(), &ObjectOptions::default())
                        .await
                        .expect("read compatible multipart target body");
                    let mut compatible_body = Vec::new();
                    compatible_reader
                        .stream
                        .read_to_end(&mut compatible_body)
                        .await
                        .expect("drain compatible multipart target body");
                    assert_eq!(compatible_body, retry_source_body);

                    let mut compatible_retry_reader = store.pools[0]
                        .get_object_reader(
                            &bucket,
                            retry_object,
                            None,
                            HeaderMap::new(),
                            &ObjectOptions {
                                raw_data_movement_read: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read source multipart object for compatible retry");
                    compatible_retry_reader.object_info = retry_source_info.clone();
                    temp_env::async_with_vars(
                        [
                            (rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_WRITE, Some("true")),
                            (rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_FLEET_CONFIRMED, Some("true")),
                        ],
                        crate::data_movement::migrate_object(
                            store.clone(),
                            0,
                            bucket.clone(),
                            compatible_retry_reader,
                            None,
                            "test_data_movement_compatible_retry",
                        ),
                    )
                    .await
                    .expect("pre-gate multipart target should converge after enabling checksum persistence");
                    let compatible_target_after_retry = store.pools[1]
                        .get_object_info(
                            &bucket,
                            retry_object,
                            &ObjectOptions {
                                include_part_checksums: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read pre-gate target after fleet-confirmed retry");
                    assert_eq!(compatible_target_after_retry.data_dir, compatible_target.data_dir);
                    assert_eq!(compatible_target_after_retry.parts, compatible_target.parts);
                    let compatible_uploads = store.pools[1]
                        .list_multipart_uploads(&bucket, retry_object, None, None, None, 100)
                        .await
                        .expect("list compatible target multipart uploads");
                    assert!(compatible_uploads.uploads.is_empty(), "compatible retry staging must be aborted");

                    store.pools[1]
                        .delete_object(&bucket, retry_object, ObjectOptions::default())
                        .await
                        .expect("remove compatible target before fleet-confirmed migration");
                    let mut retry_reader = store.pools[0]
                        .get_object_reader(
                            &bucket,
                            retry_object,
                            None,
                            HeaderMap::new(),
                            &ObjectOptions {
                                raw_data_movement_read: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read source multipart object for fleet-confirmed migration");
                    retry_reader.object_info = retry_source_info.clone();
                    temp_env::async_with_vars(
                        [
                            (rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_WRITE, Some("true")),
                            (rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_FLEET_CONFIRMED, Some("true")),
                        ],
                        crate::data_movement::migrate_object(
                            store.clone(),
                            0,
                            bucket.clone(),
                            retry_reader,
                            None,
                            "test_data_movement_retry",
                        ),
                    )
                    .await
                    .expect("first multipart migration should succeed");

                    let target_before_retry = store.pools[1]
                        .get_object_info(
                            &bucket,
                            retry_object,
                            &ObjectOptions {
                                include_part_checksums: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read target metadata before retry");
                    assert_eq!(target_before_retry.checksum, retry_source_info.checksum);
                    assert_eq!(
                        target_before_retry
                            .parts
                            .iter()
                            .map(|part| (&part.number, &part.checksums))
                            .collect::<Vec<_>>(),
                        retry_source_info
                            .parts
                            .iter()
                            .map(|part| (&part.number, &part.checksums))
                            .collect::<Vec<_>>()
                    );
                    assert_eq!(retry_source_info.mod_time, target_before_retry.mod_time);
                    assert!(
                        retry_source_info.parts.iter().any(|source_part| {
                            target_before_retry
                                .parts
                                .iter()
                                .find(|target_part| target_part.number == source_part.number)
                                .is_some_and(|target_part| target_part.mod_time != source_part.mod_time)
                        }),
                        "multipart migration must rewrite at least one target part timestamp"
                    );
                    let mut retry_reader = store.pools[0]
                        .get_object_reader(
                            &bucket,
                            retry_object,
                            None,
                            HeaderMap::new(),
                            &ObjectOptions {
                                raw_data_movement_read: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read source multipart object for retry");
                    retry_reader.object_info = retry_source_info;
                    temp_env::async_with_vars(
                        [
                            (rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_WRITE, Some("true")),
                            (rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_FLEET_CONFIRMED, Some("true")),
                        ],
                        crate::data_movement::migrate_object(
                            store.clone(),
                            0,
                            bucket.clone(),
                            retry_reader,
                            None,
                            "test_data_movement_retry",
                        ),
                    )
                    .await
                    .expect("equivalent multipart migration retry should converge");

                    let target_after_retry = store.pools[1]
                        .get_object_info(&bucket, retry_object, &ObjectOptions::default())
                        .await
                        .expect("read target metadata after retry");
                    assert_eq!(target_after_retry.data_dir, target_before_retry.data_dir);
                    assert_eq!(target_after_retry.etag, target_before_retry.etag);
                    assert_eq!(target_after_retry.checksum, target_before_retry.checksum);
                    assert_eq!(target_after_retry.mod_time, target_before_retry.mod_time);
                    assert!(!rustfs_utils::http::contains_key_str(
                        &target_after_retry.user_defined,
                        rustfs_utils::http::SUFFIX_PART_CHECKSUMS
                    ));
                    assert!(target_after_retry.parts.iter().all(|part| part.checksums.is_none()));
                    let cached_target_after_retry = store.pools[1]
                        .get_object_info(&bucket, retry_object, &ObjectOptions::default())
                        .await
                        .expect("read target metadata from the ordinary cache path");
                    assert!(!rustfs_utils::http::contains_key_str(
                        &cached_target_after_retry.user_defined,
                        rustfs_utils::http::SUFFIX_PART_CHECKSUMS
                    ));
                    assert!(cached_target_after_retry.parts.iter().all(|part| part.checksums.is_none()));
                    let hydrated_target_after_retry = store.pools[1]
                        .get_object_info(
                            &bucket,
                            retry_object,
                            &ObjectOptions {
                                include_part_checksums: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("hydrate target metadata after ordinary cache reads");
                    assert_eq!(hydrated_target_after_retry.user_defined, target_before_retry.user_defined);
                    assert_eq!(hydrated_target_after_retry.parts, target_before_retry.parts);

                    let mut target_reader = store.pools[1]
                        .get_object_reader(&bucket, retry_object, None, HeaderMap::new(), &ObjectOptions::default())
                        .await
                        .expect("read target multipart body after retry");
                    let mut target_body = Vec::new();
                    target_reader
                        .stream
                        .read_to_end(&mut target_body)
                        .await
                        .expect("drain target multipart body after retry");
                    assert_eq!(target_body, retry_source_body);

                    let uploads = store.pools[1]
                        .list_multipart_uploads(&bucket, retry_object, None, None, None, 100)
                        .await
                        .expect("list retry target multipart uploads");
                    assert!(uploads.uploads.is_empty(), "retry migration staging must be aborted");

                    Box::pin(async {
                        store.pools[0]
                            .delete_object(&bucket, retry_object, ObjectOptions::default())
                            .await
                            .expect("remove first-hop source before second migration");
                        *store.rebalance_meta.write().await = Some(active_rebalance_meta_for_pool(store.pools.len(), 1));
                        let second_hop_reader = store.pools[1]
                            .get_object_reader(
                                &bucket,
                                retry_object,
                                None,
                                HeaderMap::new(),
                                &ObjectOptions {
                                    data_movement: true,
                                    raw_data_movement_read: true,
                                    ..Default::default()
                                },
                            )
                            .await
                            .expect("read first target as the second migration source");
                        assert!(
                            second_hop_reader
                                .object_info
                                .parts
                                .iter()
                                .all(|part| part.checksums.is_some()),
                            "data-movement source reads must hydrate persisted part checksums"
                        );
                        temp_env::async_with_vars(
                            [
                                (rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_WRITE, Some("true")),
                                (rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_FLEET_CONFIRMED, Some("true")),
                            ],
                            crate::data_movement::migrate_object(
                                store.clone(),
                                1,
                                bucket.clone(),
                                second_hop_reader,
                                None,
                                "test_data_movement_second_hop",
                            ),
                        )
                        .await
                        .expect("second multipart migration should preserve part checksums");

                        let second_hop_target = store.pools[0]
                            .get_object_info(
                                &bucket,
                                retry_object,
                                &ObjectOptions {
                                    include_part_checksums: true,
                                    ..Default::default()
                                },
                            )
                            .await
                            .expect("read second-hop target metadata");
                        assert_eq!(second_hop_target.checksum, target_before_retry.checksum);
                        assert_eq!(
                            second_hop_target
                                .parts
                                .iter()
                                .map(|part| (&part.number, &part.checksums))
                                .collect::<Vec<_>>(),
                            target_before_retry
                                .parts
                                .iter()
                                .map(|part| (&part.number, &part.checksums))
                                .collect::<Vec<_>>()
                        );
                    })
                    .await;
                });
            })
            .expect("spawn data movement test thread")
            .join()
            .expect("join data movement test thread");
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn data_movement_multipart_replaces_only_unlocked_owned_generation() {
        std::thread::Builder::new()
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .build()
                    .expect("build data movement replacement test runtime");
                runtime.block_on(async move {
                    let temp_dir = tempfile::tempdir().expect("create data movement replacement store dir");
                    let (_ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
                        temp_dir.path(),
                        "data-movement-stale-replacement",
                        &[4, 4],
                    ))
                    .await;
                    crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

                    let bucket = format!("dm-stale-replacement-{}", uuid::Uuid::new_v4());
                    store
                        .make_bucket(
                            &bucket,
                            &MakeBucketOptions {
                                lock_enabled: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("create data movement replacement bucket");
                    *store.rebalance_meta.write().await = Some(active_rebalance_meta_for_pool(store.pools.len(), 0));

                    let version_id = uuid::Uuid::new_v4();
                    let old_time = OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND;
                    let new_time = old_time + time::Duration::SECOND;
                    let first_part_size = 5 * 1024 * 1024;
                    let mut old_body = vec![b'a'; first_part_size];
                    old_body.push(b'b');
                    let mut new_body = vec![b'c'; first_part_size];
                    new_body.push(b'd');

                    let source_reader = |name: &'static str, body: Vec<u8>, mod_time, metadata: HashMap<String, String>| {
                        seed_data_movement_source_reader(
                            &store,
                            &bucket,
                            name,
                            DataMovementSourceSeed {
                                pool_idx: 0,
                                version_id: Some(version_id),
                                body,
                                multipart_split: Some(first_part_size),
                                mod_time,
                                user_defined: metadata,
                            },
                        )
                    };

                    let replaceable = "replaceable.bin";
                    crate::data_movement::migrate_object(
                        store.clone(),
                        0,
                        bucket.clone(),
                        source_reader(
                            replaceable,
                            old_body.clone(),
                            old_time,
                            HashMap::from([("x-amz-meta-generation".to_string(), "old".to_string())]),
                        )
                        .await,
                        None,
                        "test_stale_target_seed",
                    )
                    .await
                    .expect("seed old migrated target");
                    let seeded_target = store.pools[1]
                        .get_object_info(
                            &bucket,
                            replaceable,
                            &ObjectOptions {
                                versioned: true,
                                version_id: Some(version_id.to_string()),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read old migrated target");
                    let replacement_opts = ObjectOptions {
                        data_movement: true,
                        versioned: true,
                        version_id: Some(version_id.to_string()),
                        mod_time: Some(new_time),
                        http_preconditions: Some(crate::data_movement::data_movement_target_precondition()),
                        ..Default::default()
                    };
                    assert!(
                        crate::data_movement::can_replace_stale_data_movement_target(&seeded_target, &replacement_opts),
                        "seeded target should be replaceable: {seeded_target:?}"
                    );
                    crate::data_movement::migrate_object(
                        store.clone(),
                        0,
                        bucket.clone(),
                        source_reader(
                            replaceable,
                            new_body.clone(),
                            new_time,
                            HashMap::from([("x-amz-meta-generation".to_string(), "new".to_string())]),
                        )
                        .await,
                        None,
                        "test_stale_target_replace",
                    )
                    .await
                    .expect("newer source generation should replace the old migrated target");

                    let replacement = store.pools[1]
                        .get_object_info(
                            &bucket,
                            replaceable,
                            &ObjectOptions {
                                versioned: true,
                                version_id: Some(version_id.to_string()),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read replaced target metadata");
                    assert_eq!(replacement.mod_time, Some(new_time));
                    assert_eq!(replacement.user_defined.get("x-amz-meta-generation").map(String::as_str), Some("new"));
                    let mut replacement_reader = store.pools[1]
                        .get_object_reader(
                            &bucket,
                            replaceable,
                            None,
                            HeaderMap::new(),
                            &ObjectOptions {
                                versioned: true,
                                version_id: Some(version_id.to_string()),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read replaced target body");
                    let mut replacement_body = Vec::new();
                    replacement_reader
                        .stream
                        .read_to_end(&mut replacement_body)
                        .await
                        .expect("drain replaced target body");
                    assert_eq!(replacement_body, new_body);

                    let client_target = "client-target.bin";
                    let client_body = b"client-owned exact version".to_vec();
                    let mut client_reader = PutObjReader::from_vec(client_body.clone());
                    store.pools[1]
                        .put_object(
                            &bucket,
                            client_target,
                            &mut client_reader,
                            &ObjectOptions {
                                versioned: true,
                                version_id: Some(version_id.to_string()),
                                mod_time: Some(old_time),
                                user_defined: HashMap::from([("x-amz-meta-owner".to_string(), "client".to_string())]),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("seed client-owned exact version");
                    let err = crate::data_movement::migrate_object(
                        store.clone(),
                        0,
                        bucket.clone(),
                        source_reader(client_target, new_body.clone(), new_time, HashMap::new()).await,
                        None,
                        "test_client_target_reject",
                    )
                    .await
                    .expect_err("data movement must not replace a client-owned exact version");
                    assert!(err.to_string().contains("complete_multipart_upload"), "unexpected migration error: {err}");
                    let mut preserved_reader = store.pools[1]
                        .get_object_reader(
                            &bucket,
                            client_target,
                            None,
                            HeaderMap::new(),
                            &ObjectOptions {
                                versioned: true,
                                version_id: Some(version_id.to_string()),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("read preserved client target");
                    let mut preserved_body = Vec::new();
                    preserved_reader
                        .stream
                        .read_to_end(&mut preserved_body)
                        .await
                        .expect("drain preserved client target");
                    assert_eq!(preserved_body, client_body);

                    let acknowledged_target = "acknowledged-target.bin";
                    crate::data_movement::migrate_object(
                        store.clone(),
                        0,
                        bucket.clone(),
                        source_reader(
                            acknowledged_target,
                            old_body.clone(),
                            old_time,
                            HashMap::from([("x-amz-meta-generation".to_string(), "old".to_string())]),
                        )
                        .await,
                        None,
                        "test_acknowledged_target_seed",
                    )
                    .await
                    .expect("seed migrated target before tag acknowledgement");
                    let target_version_opts = ObjectOptions {
                        versioned: true,
                        version_id: Some(version_id.to_string()),
                        ..Default::default()
                    };
                    store
                        .put_object_tags(&bucket, acknowledged_target, "acknowledged=true", &target_version_opts)
                        .await
                        .expect("tag update should acknowledge the migrated target");
                    let acknowledged = store.pools[1]
                        .get_object_info(&bucket, acknowledged_target, &target_version_opts)
                        .await
                        .expect("read acknowledged migrated target");
                    let rustfs_data_moved_key = rustfs_utils::http::internal_key_rustfs(rustfs_utils::http::SUFFIX_DATA_MOVED);
                    let minio_data_moved_key =
                        format!("{}{}", rustfs_utils::http::MINIO_INTERNAL_PREFIX, rustfs_utils::http::SUFFIX_DATA_MOVED);
                    assert_eq!(acknowledged.user_defined.get(&rustfs_data_moved_key).map(String::as_str), Some(""));
                    assert_eq!(acknowledged.user_defined.get(&minio_data_moved_key).map(String::as_str), Some(""));
                    assert_eq!(
                        rustfs_utils::http::get_consistent_str(&acknowledged.user_defined, rustfs_utils::http::SUFFIX_DATA_MOVED),
                        None
                    );

                    let err = crate::data_movement::migrate_object(
                        store.clone(),
                        0,
                        bucket.clone(),
                        source_reader(
                            acknowledged_target,
                            new_body.clone(),
                            new_time,
                            HashMap::from([("x-amz-meta-generation".to_string(), "new".to_string())]),
                        )
                        .await,
                        None,
                        "test_acknowledged_target_reject",
                    )
                    .await
                    .expect_err("data movement must not replace a target acknowledged by a tag update");
                    assert!(err.to_string().contains("complete_multipart_upload"), "unexpected migration error: {err}");

                    let acknowledged_tags = store.pools[1]
                        .get_object_tags(&bucket, acknowledged_target, &target_version_opts)
                        .await
                        .expect("read preserved acknowledged target tags");
                    assert_eq!(acknowledged_tags, "acknowledged=true");
                    let mut acknowledged_reader = store.pools[1]
                        .get_object_reader(&bucket, acknowledged_target, None, HeaderMap::new(), &target_version_opts)
                        .await
                        .expect("read preserved acknowledged target body");
                    let mut acknowledged_body = Vec::new();
                    acknowledged_reader
                        .stream
                        .read_to_end(&mut acknowledged_body)
                        .await
                        .expect("drain preserved acknowledged target body");
                    assert_eq!(acknowledged_body, old_body);

                    let metadata_acknowledged_target = "metadata-acknowledged-target.bin";
                    crate::data_movement::migrate_object(
                        store.clone(),
                        0,
                        bucket.clone(),
                        source_reader(
                            metadata_acknowledged_target,
                            old_body.clone(),
                            old_time,
                            HashMap::from([("x-amz-meta-generation".to_string(), "old".to_string())]),
                        )
                        .await,
                        None,
                        "test_metadata_acknowledged_target_seed",
                    )
                    .await
                    .expect("seed migrated target before metadata acknowledgement");
                    store
                        .put_object_metadata(
                            &bucket,
                            metadata_acknowledged_target,
                            &ObjectOptions {
                                eval_metadata: Some(HashMap::from([(
                                    s3s::header::X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str().to_string(),
                                    s3s::dto::ObjectLockLegalHoldStatus::OFF.to_string(),
                                )])),
                                ..target_version_opts.clone()
                            },
                        )
                        .await
                        .expect("legal hold metadata update should acknowledge the migrated target");
                    let metadata_acknowledged = store.pools[1]
                        .get_object_info(&bucket, metadata_acknowledged_target, &target_version_opts)
                        .await
                        .expect("read metadata-acknowledged migrated target");
                    assert_eq!(
                        metadata_acknowledged
                            .user_defined
                            .get(&rustfs_data_moved_key)
                            .map(String::as_str),
                        Some("")
                    );
                    assert_eq!(
                        metadata_acknowledged
                            .user_defined
                            .get(&minio_data_moved_key)
                            .map(String::as_str),
                        Some("")
                    );
                    assert_eq!(
                        metadata_acknowledged
                            .user_defined
                            .get(s3s::header::X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str())
                            .map(String::as_str),
                        Some("OFF")
                    );
                    let err = crate::data_movement::migrate_object(
                        store.clone(),
                        0,
                        bucket.clone(),
                        source_reader(
                            metadata_acknowledged_target,
                            new_body.clone(),
                            new_time,
                            HashMap::from([("x-amz-meta-generation".to_string(), "new".to_string())]),
                        )
                        .await,
                        None,
                        "test_metadata_acknowledged_target_reject",
                    )
                    .await
                    .expect_err("data movement must not replace a target acknowledged by a metadata update");
                    assert!(err.to_string().contains("complete_multipart_upload"), "unexpected migration error: {err}");

                    let retain_until = (OffsetDateTime::now_utc() + time::Duration::days(1))
                        .format(&time::format_description::well_known::Rfc3339)
                        .expect("retain-until date should format");
                    for (object, mode) in [
                        ("compliance-target.bin", s3s::dto::ObjectLockRetentionMode::COMPLIANCE),
                        ("governance-target.bin", s3s::dto::ObjectLockRetentionMode::GOVERNANCE),
                    ] {
                        let retained_metadata = HashMap::from([
                            (s3s::header::X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(), mode.to_string()),
                            (
                                s3s::header::X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(),
                                retain_until.clone(),
                            ),
                        ]);
                        crate::data_movement::migrate_object(
                            store.clone(),
                            0,
                            bucket.clone(),
                            source_reader(object, old_body.clone(), old_time, retained_metadata.clone()).await,
                            None,
                            "test_retained_target_seed",
                        )
                        .await
                        .expect("seed retained migrated target");
                        let err = crate::data_movement::migrate_object(
                            store.clone(),
                            0,
                            bucket.clone(),
                            source_reader(object, new_body.clone(), new_time, retained_metadata).await,
                            None,
                            "test_retained_target_replace",
                        )
                        .await
                        .expect_err("active retention must block stale target replacement");
                        assert!(err.to_string().contains("complete_multipart_upload"), "unexpected retention error: {err}");

                        let mut retained_reader = store.pools[1]
                            .get_object_reader(
                                &bucket,
                                object,
                                None,
                                HeaderMap::new(),
                                &ObjectOptions {
                                    versioned: true,
                                    version_id: Some(version_id.to_string()),
                                    ..Default::default()
                                },
                            )
                            .await
                            .expect("read retained target");
                        let mut retained_body = Vec::new();
                        retained_reader
                            .stream
                            .read_to_end(&mut retained_body)
                            .await
                            .expect("drain retained target");
                        assert_eq!(retained_body, old_body);
                    }

                    for object in [
                        replaceable,
                        client_target,
                        acknowledged_target,
                        metadata_acknowledged_target,
                        "compliance-target.bin",
                        "governance-target.bin",
                    ] {
                        let uploads = store.pools[1]
                            .list_multipart_uploads(&bucket, object, None, None, None, 100)
                            .await
                            .expect("list target multipart staging");
                        assert!(uploads.uploads.is_empty(), "data movement staging must be cleaned for {object}");
                    }
                });
            })
            .expect("spawn data movement replacement test thread")
            .join()
            .expect("join data movement replacement test thread");
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn data_movement_delete_marker_retries_converge_safely() {
        let temp_dir = tempfile::tempdir().expect("create delete-marker data movement store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "data-movement-delete-marker", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("dm-delete-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create delete-marker data movement bucket");
        let object = "delete-marker-retry";
        let version = uuid::Uuid::new_v4();
        let mod_time = OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND;
        let replica_timestamp = mod_time + time::Duration::SECOND;
        let replication_timestamp = mod_time;
        let replica_timestamp_string = replica_timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .expect("replica timestamp should format as RFC3339");
        let replication_timestamp_string = replication_timestamp
            .format(&time::format_description::well_known::Rfc3339)
            .expect("replication timestamp should format as RFC3339");
        let replication_status = "arn:minio:replication::TenantA:bucket=COMPLETED;";
        store.pools[0]
            .delete_object(
                &bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    version_id: Some(version.to_string()),
                    delete_marker: true,
                    mod_time: Some(mod_time),
                    delete_replication: Some(ReplicationState {
                        replica_status: ReplicationStatusType::Replica,
                        replica_timestamp: Some(replica_timestamp),
                        replication_status_internal: Some(replication_status.to_string()),
                        replication_timestamp: Some(replication_timestamp),
                        targets: replication_statuses_map(replication_status),
                        delete_marker: true,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await
            .expect("write source delete marker");
        let source = store.pools[0]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("read source delete marker replication metadata");
        assert_eq!(
            rustfs_utils::http::get_str(&source.user_defined, rustfs_utils::http::SUFFIX_REPLICA_STATUS).as_deref(),
            Some("REPLICA"),
            "source metadata: {:?}",
            source.user_defined
        );

        *store.rebalance_meta.write().await = Some(active_rebalance_meta_for_pool(store.pools.len(), 0));
        let missing_object = "missing-delete-marker";
        let missing_version = uuid::Uuid::new_v4();
        let missing_opts = ObjectOptions {
            versioned: true,
            version_id: Some(missing_version.to_string()),
            delete_marker: true,
            mod_time: Some(mod_time),
            src_pool_idx: 0,
            data_movement: true,
            skip_decommissioned: true,
            ..Default::default()
        };
        let missing_source_err = store
            .delete_object(&bucket, missing_object, missing_opts.clone())
            .await
            .expect_err("data movement must not recreate a delete marker missing from the source");
        assert!(matches!(missing_source_err, StorageError::DataMovementOverwriteErr(_, _, _)));
        let missing_target_err = store.pools[1]
            .get_object_info(&bucket, missing_object, &missing_opts)
            .await
            .expect_err("source-missing preflight must not write a target delete marker");
        assert!(matches!(
            missing_target_err,
            StorageError::ObjectNotFound(_, _) | StorageError::VersionNotFound(_, _, _)
        ));

        let movement_opts = ObjectOptions {
            versioned: true,
            version_id: Some(version.to_string()),
            delete_marker: true,
            mod_time: Some(mod_time),
            src_pool_idx: 0,
            data_movement: true,
            skip_decommissioned: true,
            ..Default::default()
        };
        for attempt in 1..=2 {
            let moved = store
                .delete_object(&bucket, object, movement_opts.clone())
                .await
                .unwrap_or_else(|err| panic!("delete marker migration attempt {attempt} should converge: {err}"));
            assert!(moved.delete_marker);
            assert_eq!(moved.version_id, Some(version));
        }
        let target = store.pools[1]
            .get_object_info(&bucket, object, &movement_opts)
            .await
            .expect("retry must retain the target delete marker");
        assert!(target.delete_marker);
        assert_eq!(target.mod_time, Some(mod_time));
        assert_eq!(
            rustfs_utils::http::get_str(&target.user_defined, rustfs_utils::http::SUFFIX_REPLICA_STATUS).as_deref(),
            Some("REPLICA")
        );
        assert_eq!(
            rustfs_utils::http::get_str(&target.user_defined, rustfs_utils::http::SUFFIX_REPLICA_TIMESTAMP).as_deref(),
            Some(replica_timestamp_string.as_str())
        );
        assert_eq!(
            rustfs_utils::http::get_str(&target.user_defined, rustfs_utils::http::SUFFIX_REPLICATION_STATUS).as_deref(),
            Some(replication_status)
        );
        assert_eq!(
            rustfs_utils::http::get_str(&target.user_defined, rustfs_utils::http::SUFFIX_REPLICATION_TIMESTAMP).as_deref(),
            Some(replication_timestamp_string.as_str())
        );

        let null_object = "null-delete-marker-retry";
        let null_mod_time = mod_time + time::Duration::seconds(3);
        let null_marker = store.pools[0]
            .delete_object(
                &bucket,
                null_object,
                ObjectOptions {
                    version_suspended: true,
                    version_id: Some(uuid::Uuid::nil().to_string()),
                    data_movement: true,
                    delete_marker: true,
                    mod_time: Some(null_mod_time),
                    ..Default::default()
                },
            )
            .await
            .expect("write source null delete marker");
        assert_eq!(null_marker.version_id, Some(uuid::Uuid::nil()));
        let null_opts = ObjectOptions {
            version_suspended: true,
            version_id: Some(uuid::Uuid::nil().to_string()),
            delete_marker: true,
            mod_time: Some(null_mod_time),
            src_pool_idx: 0,
            data_movement: true,
            skip_decommissioned: true,
            ..Default::default()
        };
        for attempt in 1..=2 {
            let moved = store
                .delete_object(&bucket, null_object, null_opts.clone())
                .await
                .unwrap_or_else(|err| panic!("null delete marker migration attempt {attempt} should converge: {err}"));
            assert!(moved.delete_marker);
            assert_eq!(moved.version_id, Some(uuid::Uuid::nil()));
        }
        let target_null_marker = store.pools[1]
            .get_object_info(&bucket, null_object, &null_opts)
            .await
            .expect("target must retain the null delete marker identity");
        assert!(target_null_marker.delete_marker);
        assert_eq!(target_null_marker.version_id, Some(uuid::Uuid::nil()));

        let explicit_object = "suspended-explicit-version-delete";
        let explicit_version = uuid::Uuid::new_v4();
        let mut explicit_reader = PutObjReader::from_vec(b"explicit version".to_vec());
        store.pools[0]
            .put_object(
                &bucket,
                explicit_object,
                &mut explicit_reader,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(explicit_version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("write explicit version before suspended delete");
        store.pools[0]
            .delete_object(
                &bucket,
                explicit_object,
                ObjectOptions {
                    version_suspended: true,
                    version_id: Some(explicit_version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("delete explicit version from suspended bucket");
        store.pools[0]
            .get_object_info(
                &bucket,
                explicit_object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(explicit_version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("suspended delete must remove the requested UUID version");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn decommission_entry_carries_migration_and_cleanup_mutation_fences() {
        let temp_dir = tempfile::tempdir().expect("create decommission delete-fence store dir");
        let (_ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store_with_layout(
            temp_dir.path(),
            "decommission-delete-fence",
            &[(2, 4), (1, 4)],
            CancellationToken::new(),
            None,
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("decom-delete-fence-{}", uuid::Uuid::new_v4());
        let object = (0..128)
            .map(|index| format!("object-{index}.bin"))
            .find(|candidate| store.pools[0].get_disks_by_key(candidate).set_index == 1)
            .expect("the deterministic object search should select source set 1");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create decommission delete-fence bucket");
        let mut source = PutObjReader::from_vec(b"source generation".to_vec());
        store.pools[0]
            .put_object(&bucket, &object, &mut source, &ObjectOptions::default())
            .await
            .expect("write source object to the pool being decommissioned");
        mark_test_pool_decommissioning(&store, 0).await;
        assert!(store.is_suspended(0).await, "pool 0 must be a suspended decommission source");

        let barrier = crate::set_disk::PutObjectCommitBarrier::install(
            &bucket,
            &object,
            crate::set_disk::PutObjectCommitPause::AfterNamespace,
        );
        let cleanup_barrier = crate::data_movement::SourceCleanupDeleteBarrier::install(&bucket, &object);
        let source_set = store.pools[0].get_disks_by_key(&object);
        assert_eq!(source_set.set_index, 1, "the source entry must exercise the non-fixed set cleanup lock");
        let worker_store = Arc::clone(&store);
        let worker_bucket = bucket.clone();
        let worker_object = object.clone();
        let worker = tokio::spawn(async move {
            worker_store
                .decommission_entry_for_test(
                    0,
                    MetaCacheEntry {
                        name: worker_object,
                        ..Default::default()
                    },
                    worker_bucket,
                    source_set,
                )
                .await
        });
        barrier.wait_until_paused().await;

        let delete_barrier = crate::store::object::DeleteAfterObjectLockSnapshotBarrier::install(&bucket);
        let delete_store = Arc::clone(&store);
        let delete_bucket = bucket.clone();
        let delete_object = object.clone();
        let delete = tokio::spawn(async move {
            delete_store
                .delete_object(&delete_bucket, &delete_object, ObjectOptions::default())
                .await
        });
        delete_barrier.wait_until_paused().await;
        delete_barrier.release_and_wait_until_namespace_pending().await;
        assert!(
            !delete_barrier.namespace_acquired() && !delete.is_finished(),
            "DELETE must remain before namespace acquisition behind the decommission worker's target-commit mutation fence"
        );

        barrier.release();
        cleanup_barrier.wait_until_paused().await;
        drop(barrier);

        let fixed_set = Arc::clone(&store.pools[0].disk_set[0]);
        let fixed_mutation_barrier = crate::set_disk::PutObjectCommitBarrier::install(
            &bucket,
            &object,
            crate::set_disk::PutObjectCommitPause::BeforeNamespace,
        );
        let mutation_bucket = bucket.clone();
        let mutation_object = object.clone();
        let fixed_mutation = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(b"fixed-domain replacement".to_vec());
            fixed_set
                .put_object(&mutation_bucket, &mutation_object, &mut reader, &ObjectOptions::default())
                .await
        });
        fixed_mutation_barrier.wait_until_paused().await;
        fixed_mutation_barrier.release_and_wait_until_namespace_pending().await;
        assert!(
            !fixed_mutation_barrier.namespace_acquired() && !fixed_mutation.is_finished(),
            "the source cleanup must retain the fixed mutation fence before the set-0 mutation acquires its namespace"
        );
        fixed_mutation.abort();
        assert!(
            fixed_mutation
                .await
                .expect_err("the fixed-domain mutation should be canceled")
                .is_cancelled(),
            "the competing fixed-domain mutation must remain cancelable while blocked"
        );
        drop(fixed_mutation_barrier);

        cleanup_barrier.release();
        worker
            .await
            .expect("decommission entry worker should join")
            .expect("decommission entry should migrate and clean its source");
        delete
            .await
            .expect("DELETE task should join")
            .expect("DELETE should remove the source and migrated target generations");

        for pool in &store.pools {
            let err = pool
                .get_object_info(&bucket, &object, &ObjectOptions::default())
                .await
                .expect_err("DELETE must remove the source and migrated target copies");
            assert!(
                matches!(err, StorageError::ObjectNotFound(_, _)),
                "unexpected post-delete pool result: {err:?}"
            );
        }
        let err = store
            .get_object_info(&bucket, &object, &ObjectOptions::default())
            .await
            .expect_err("the deleted generation must not become visible again");
        assert!(
            matches!(err, StorageError::ObjectNotFound(_, _)),
            "unexpected post-delete store result: {err:?}"
        );

        shutdown.cancel();
    }

    #[test]
    fn decommission_retry_fault_budget_counts_successes_across_attempt_changes() {
        let cases: &[&[(usize, bool, bool)]] = &[
            &[(1, true, true), (2, true, true), (3, true, false)],
            &[(1, true, true), (1, true, true), (2, true, false)],
            &[(1, true, true), (3, true, false), (3, true, false)],
            &[(1, true, true), (2, false, false), (1, true, true), (2, true, false)],
            &[(1, true, true), (2, false, false), (3, true, false)],
            &[(3, true, false), (4, true, false)],
        ];
        for case in cases {
            let faults = Arc::new(AtomicUsize::new(0));
            let hook = decommission_retry_fault_hook("bucket", "object", Arc::clone(&faults));

            for (stage, bucket, object, succeeded) in [
                ("other-stage", "bucket", "object", true),
                (DECOMMISSION_TEST_FAULT_STAGE_MIGRATE_OBJECT, "other-bucket", "object", true),
                (DECOMMISSION_TEST_FAULT_STAGE_MIGRATE_OBJECT, "bucket", "other-object", true),
                (DECOMMISSION_TEST_FAULT_STAGE_MIGRATE_OBJECT, "bucket", "object", false),
            ] {
                assert!(!hook(stage, bucket, object, 1, succeeded));
            }
            assert_eq!(faults.load(Ordering::SeqCst), 0, "unrelated or failed copies must not consume faults");

            let mut expected_faults = 0;
            for &(attempt, succeeded, expected) in *case {
                assert_eq!(
                    hook(DECOMMISSION_TEST_FAULT_STAGE_MIGRATE_OBJECT, "bucket", "object", attempt, succeeded),
                    expected,
                    "fault plan {case:?} at attempt {attempt}"
                );
                expected_faults += usize::from(expected);
                assert_eq!(faults.load(Ordering::SeqCst), expected_faults);
            }
        }
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn decommission_entry_retries_source_changed_without_canceling_other_bucket() {
        let handle = std::thread::Builder::new()
            .name("decommission_entry_retries_source_changed_without_canceling_other_bucket".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .build()
                    .expect("test runtime should build");
                runtime.block_on(async {
                    let temp_dir = tempfile::tempdir().expect("create decommission retry store dir");
                    let (_ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(
                        temp_dir.path(),
                        "decommission-entry-retry",
                        &[4, 4],
                    ))
                    .await;
                    crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

                    let changed_bucket = format!("decom-retry-a-{}", uuid::Uuid::new_v4());
                    let other_bucket = format!("decom-retry-b-{}", uuid::Uuid::new_v4());
                    for bucket in [&changed_bucket, &other_bucket] {
                        store
                            .make_bucket(bucket, &MakeBucketOptions::default())
                            .await
                            .expect("create decommission retry bucket");
                    }

                    let changed_object = "changed.bin";
                    let first_version = uuid::Uuid::new_v4();
                    let second_version = uuid::Uuid::new_v4();
                    let base_time = OffsetDateTime::now_utc();
                    seed_decommission_source(
                        &store,
                        &changed_bucket,
                        changed_object,
                        b"first generation".to_vec(),
                        &ObjectOptions {
                            versioned: true,
                            version_id: Some(first_version.to_string()),
                            mod_time: Some(base_time),
                            ..Default::default()
                        },
                    )
                    .await;
                    let other_object = "other.bin";
                    seed_decommission_source(
                        &store,
                        &other_bucket,
                        other_object,
                        b"other bucket generation".to_vec(),
                        &ObjectOptions::default(),
                    )
                    .await;
                    mark_test_pool_decommissioning(&store, 0).await;

                    let mutation_calls = Arc::new(AtomicUsize::new(0));
                    let mutation_calls_for_hook = Arc::clone(&mutation_calls);
                    let mutation_store = Arc::clone(&store);
                    let mutation_bucket = changed_bucket.clone();
                    let _mutation_guard = crate::core::pools::DecommissionCleanupMutationGuard::install(Arc::new(
                        move |bucket, object, attempt| {
                            let is_target = bucket == mutation_bucket.as_str() && object == changed_object;
                            let calls = Arc::clone(&mutation_calls_for_hook);
                            let store = Arc::clone(&mutation_store);
                            let bucket = mutation_bucket.clone();
                            Box::pin(async move {
                                if !is_target {
                                    return;
                                }
                                calls.fetch_add(1, Ordering::SeqCst);
                                if attempt == 1 {
                                    seed_decommission_source(
                                        &store,
                                        &bucket,
                                        changed_object,
                                        b"second generation".to_vec(),
                                        &ObjectOptions {
                                            versioned: true,
                                            version_id: Some(second_version.to_string()),
                                            mod_time: Some(base_time + time::Duration::seconds(1)),
                                            ..Default::default()
                                        },
                                    )
                                    .await;
                                }
                            })
                        },
                    ));

                    let ordinary_faults = Arc::new(AtomicUsize::new(0));
                    let fault_hook = decommission_retry_fault_hook(&other_bucket, other_object, Arc::clone(&ordinary_faults));
                    let _fault_guard = crate::core::pools::DecommissionTestFaultGuard::install(fault_hook);

                    let rx = CancellationToken::new();
                    let source_changed_exhaustions = Arc::new(AtomicUsize::new(0));
                    let changed_incarnation = Some(
                        store
                            .bucket_incarnation_id(&changed_bucket)
                            .await
                            .expect("changed bucket incarnation"),
                    );
                    let other_incarnation = Some(
                        store
                            .bucket_incarnation_id(&other_bucket)
                            .await
                            .expect("other bucket incarnation"),
                    );
                    let (changed_result, other_result) = tokio::join!(
                        run_decommission_entry_retry_test(
                            &store,
                            rx.clone(),
                            &changed_bucket,
                            changed_object,
                            changed_incarnation,
                            Arc::clone(&source_changed_exhaustions),
                        ),
                        run_decommission_entry_retry_test(
                            &store,
                            rx.clone(),
                            &other_bucket,
                            other_object,
                            other_incarnation,
                            Arc::clone(&source_changed_exhaustions),
                        )
                    );
                    changed_result.expect("SourceChanged entry retry must converge");
                    other_result.expect("other bucket entry must continue through ordinary copy retries");

                    assert_eq!(
                        store.pool_meta.read().await.pools[0]
                            .decommission
                            .as_ref()
                            .expect("decommission progress should remain available")
                            .items_decommission_failed,
                        0,
                        "entry completion must not hide an exhausted copy failure"
                    );
                    assert!(!rx.is_cancelled(), "entry-level SourceChanged must not cancel the shared worker token");
                    assert_eq!(mutation_calls.load(Ordering::SeqCst), 2, "entry must be re-listed after SourceChanged");
                    assert_eq!(ordinary_faults.load(Ordering::SeqCst), 2, "ordinary copy must consume the retry budget");
                    assert_eq!(source_changed_exhaustions.load(Ordering::SeqCst), 0);

                    for (version_id, expected_body) in [
                        (first_version, b"first generation".as_slice()),
                        (second_version, b"second generation".as_slice()),
                    ] {
                        let opts = ObjectOptions {
                            versioned: true,
                            version_id: Some(version_id.to_string()),
                            ..Default::default()
                        };
                        assert_decommission_source_absent(&store, &changed_bucket, changed_object, &opts).await;
                        assert_eq!(
                            read_decommission_target_body(&store, &changed_bucket, changed_object, &opts).await,
                            expected_body
                        );
                    }
                    assert_decommission_source_absent(&store, &other_bucket, other_object, &ObjectOptions::default()).await;
                    assert_eq!(
                        read_decommission_target_body(&store, &other_bucket, other_object, &ObjectOptions::default()).await,
                        b"other bucket generation"
                    );

                    shutdown.cancel();
                });
            })
            .expect("spawn decommission retry test thread");
        if let Err(payload) = handle.join() {
            std::panic::resume_unwind(payload);
        }
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn decommission_entry_exhausted_source_changed_retains_source_and_records_failure() {
        let handle = std::thread::Builder::new()
            .name("decommission_entry_exhausted_source_changed_retains_source_and_records_failure".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .build()
                    .expect("test runtime should build");
                runtime.block_on(async {
                    let temp_dir = tempfile::tempdir().expect("create decommission exhaustion store dir");
                    let (_ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(
                        temp_dir.path(),
                        "decommission-entry-exhaustion",
                        &[4, 4],
                    ))
                    .await;
                    crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

                    let bucket = format!("decom-exhausted-{}", uuid::Uuid::new_v4());
                    let object = "exhausted.bin";
                    store
                        .make_bucket(&bucket, &MakeBucketOptions::default())
                        .await
                        .expect("create decommission exhaustion bucket");
                    let original_version = uuid::Uuid::new_v4();
                    let base_time = OffsetDateTime::now_utc();
                    seed_decommission_source(
                        &store,
                        &bucket,
                        object,
                        b"original generation".to_vec(),
                        &ObjectOptions {
                            versioned: true,
                            version_id: Some(original_version.to_string()),
                            mod_time: Some(base_time),
                            ..Default::default()
                        },
                    )
                    .await;
                    mark_test_pool_decommissioning(&store, 0).await;

                    let mutation_calls = Arc::new(AtomicUsize::new(0));
                    let mutation_calls_for_hook = Arc::clone(&mutation_calls);
                    let mutation_store = Arc::clone(&store);
                    let mutation_bucket = bucket.clone();
                    let _mutation_guard = crate::core::pools::DecommissionCleanupMutationGuard::install(Arc::new(
                        move |called_bucket, called_object, _attempt| {
                            let is_target = called_bucket == mutation_bucket.as_str() && called_object == object;
                            let calls = Arc::clone(&mutation_calls_for_hook);
                            let store = Arc::clone(&mutation_store);
                            let bucket = mutation_bucket.clone();
                            Box::pin(async move {
                                if !is_target {
                                    return;
                                }
                                let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                                let offset = i64::try_from(call).expect("entry retry count should fit i64");
                                seed_decommission_source(
                                    &store,
                                    &bucket,
                                    object,
                                    format!("concurrent generation {call}").into_bytes(),
                                    &ObjectOptions {
                                        versioned: true,
                                        version_id: Some(uuid::Uuid::new_v4().to_string()),
                                        mod_time: Some(base_time + time::Duration::seconds(offset)),
                                        ..Default::default()
                                    },
                                )
                                .await;
                            })
                        },
                    ));

                    let rx = CancellationToken::new();
                    let source_changed_exhaustions = Arc::new(AtomicUsize::new(0));
                    let incarnation = Some(store.bucket_incarnation_id(&bucket).await.expect("bucket incarnation"));
                    run_decommission_entry_retry_test(
                        &store,
                        rx.clone(),
                        &bucket,
                        object,
                        incarnation,
                        Arc::clone(&source_changed_exhaustions),
                    )
                    .await
                    .expect("entry-level exhaustion must stay local below the pool threshold");

                    assert!(!rx.is_cancelled(), "one exhausted entry must not cancel other bucket workers");
                    assert_eq!(mutation_calls.load(Ordering::SeqCst), crate::core::pools::DECOMMISSION_ENTRY_MAX_ATTEMPTS);
                    assert_eq!(source_changed_exhaustions.load(Ordering::SeqCst), 1);
                    store.pools[0]
                        .get_object_info(
                            &bucket,
                            object,
                            &ObjectOptions {
                                versioned: true,
                                version_id: Some(original_version.to_string()),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("retry exhaustion must retain the original source version");
                    let pool_meta = store.pool_meta.read().await;
                    let info = pool_meta.pools[0]
                        .decommission
                        .as_ref()
                        .expect("decommission progress must be initialized");
                    assert_eq!(info.items_decommission_failed, 1, "exhausted entry must be visible as failed");
                    drop(pool_meta);

                    shutdown.cancel();
                });
            })
            .expect("spawn decommission retry test thread");
        if let Err(payload) = handle.join() {
            std::panic::resume_unwind(payload);
        }
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn decommission_entry_delete_marker_copy_retries_real_path() {
        let handle = std::thread::Builder::new()
            .name("decommission_entry_delete_marker_copy_retries_real_path".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .build()
                    .expect("test runtime should build");
                runtime.block_on(async {
                    let temp_dir = tempfile::tempdir().expect("create delete marker retry store dir");
                    let (_ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(
                        temp_dir.path(),
                        "decommission-delete-marker-retry",
                        &[4, 4],
                    ))
                    .await;
                    crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

                    let bucket = format!("decom-marker-{}", uuid::Uuid::new_v4());
                    let object = "marker.bin";
                    store
                        .make_bucket(&bucket, &MakeBucketOptions::default())
                        .await
                        .expect("create delete marker retry bucket");
                    let data_version = uuid::Uuid::new_v4();
                    let marker_version = uuid::Uuid::new_v4();
                    let base_time = OffsetDateTime::now_utc();
                    seed_decommission_source(
                        &store,
                        &bucket,
                        object,
                        b"delete marker data".to_vec(),
                        &ObjectOptions {
                            versioned: true,
                            version_id: Some(data_version.to_string()),
                            mod_time: Some(base_time),
                            ..Default::default()
                        },
                    )
                    .await;
                    store.pools[0]
                        .delete_object(
                            &bucket,
                            object,
                            ObjectOptions {
                                versioned: true,
                                version_id: Some(marker_version.to_string()),
                                delete_marker: true,
                                mod_time: Some(base_time + time::Duration::seconds(1)),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("seed source delete marker");
                    mark_test_pool_decommissioning(&store, 0).await;

                    let fault_calls = Arc::new(AtomicUsize::new(0));
                    let fault_calls_for_hook = Arc::clone(&fault_calls);
                    let fault_bucket = bucket.clone();
                    let _fault_guard = crate::core::pools::DecommissionTestFaultGuard::install(Arc::new(
                        move |stage, called_bucket, called_object, attempt, succeeded| {
                            let injected = succeeded
                                && stage == DECOMMISSION_TEST_FAULT_STAGE_DELETE_MARKER
                                && called_bucket == fault_bucket.as_str()
                                && called_object == object
                                && attempt < crate::core::pools::DECOMMISSION_VERSION_COPY_ATTEMPTS;
                            if injected {
                                fault_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                            }
                            injected
                        },
                    ));

                    let incarnation = Some(store.bucket_incarnation_id(&bucket).await.expect("bucket incarnation"));
                    run_decommission_entry_retry_test(
                        &store,
                        CancellationToken::new(),
                        &bucket,
                        object,
                        incarnation,
                        Arc::new(AtomicUsize::new(0)),
                    )
                    .await
                    .expect("delete marker copy retries must converge");

                    assert_eq!(
                        fault_calls.load(Ordering::SeqCst),
                        crate::core::pools::DECOMMISSION_VERSION_COPY_ATTEMPTS - 1
                    );
                    let marker_opts = ObjectOptions {
                        versioned: true,
                        version_id: Some(marker_version.to_string()),
                        ..Default::default()
                    };
                    let target_marker = store.pools[1]
                        .get_object_info(&bucket, object, &marker_opts)
                        .await
                        .expect("target delete marker must exist");
                    assert!(target_marker.delete_marker);
                    assert_decommission_source_absent(&store, &bucket, object, &marker_opts).await;

                    let data_opts = ObjectOptions {
                        versioned: true,
                        version_id: Some(data_version.to_string()),
                        ..Default::default()
                    };
                    assert_eq!(
                        read_decommission_target_body(&store, &bucket, object, &data_opts).await,
                        b"delete marker data"
                    );
                    assert_decommission_source_absent(&store, &bucket, object, &data_opts).await;

                    shutdown.cancel();
                });
            })
            .expect("spawn decommission retry test thread");
        if let Err(payload) = handle.join() {
            std::panic::resume_unwind(payload);
        }
    }

    #[cfg(feature = "test-util")]
    #[test]
    #[serial_test::serial(storage_class_env)]
    fn decommission_entry_tiered_copy_retries_real_path() {
        let handle = std::thread::Builder::new()
            .name("decommission_entry_tiered_copy_retries_real_path".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .build()
                    .expect("test runtime should build");
                runtime.block_on(async {
                    let temp_dir = tempfile::tempdir().expect("create tiered retry store dir");
                    let (ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(
                        temp_dir.path(),
                        "decommission-tiered-retry",
                        &[4, 4],
                    ))
                    .await;
                    crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

                    let bucket = format!("decom-tiered-{}", uuid::Uuid::new_v4());
                    let object = "tiered.bin";
                    store
                        .make_bucket(&bucket, &MakeBucketOptions::default())
                        .await
                        .expect("create tiered retry bucket");
                    let mut reader = PutObjReader::from_vec(b"tiered generation".to_vec());
                    let original = store.pools[0]
                        .put_object(&bucket, object, &mut reader, &ObjectOptions::default())
                        .await
                        .expect("seed tiered source object");
                    let tier_name = format!("DECOM{}", &uuid::Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
                    register_mock_tier(&ctx.tier_config_mgr(), &tier_name).await;
                    store.pools[0]
                        .transition_object(
                            &bucket,
                            object,
                            &ObjectOptions {
                                transition: TransitionOptions {
                                    status: TRANSITION_PENDING.to_string(),
                                    tier: tier_name,
                                    etag: original.etag.clone().expect("tiered source ETag"),
                                    ..Default::default()
                                },
                                version_id: original.version_id.map(|version_id| version_id.to_string()),
                                mod_time: original.mod_time,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("transition source object to mock tier");
                    mark_test_pool_decommissioning(&store, 0).await;

                    let fault_calls = Arc::new(AtomicUsize::new(0));
                    let fault_calls_for_hook = Arc::clone(&fault_calls);
                    let fault_bucket = bucket.clone();
                    let _fault_guard = crate::core::pools::DecommissionTestFaultGuard::install(Arc::new(
                        move |stage, called_bucket, called_object, attempt, succeeded| {
                            let injected = succeeded
                                && stage == DECOMMISSION_TEST_FAULT_STAGE_TIERED
                                && called_bucket == fault_bucket.as_str()
                                && called_object == object
                                && attempt < crate::core::pools::DECOMMISSION_VERSION_COPY_ATTEMPTS;
                            if injected {
                                fault_calls_for_hook.fetch_add(1, Ordering::SeqCst);
                            }
                            injected
                        },
                    ));

                    let incarnation = Some(store.bucket_incarnation_id(&bucket).await.expect("bucket incarnation"));
                    run_decommission_entry_retry_test(
                        &store,
                        CancellationToken::new(),
                        &bucket,
                        object,
                        incarnation,
                        Arc::new(AtomicUsize::new(0)),
                    )
                    .await
                    .expect("tiered copy retries must converge");

                    assert_eq!(
                        fault_calls.load(Ordering::SeqCst),
                        crate::core::pools::DECOMMISSION_VERSION_COPY_ATTEMPTS - 1
                    );
                    let target = store.pools[1]
                        .get_object_info(&bucket, object, &ObjectOptions::default())
                        .await
                        .expect("tiered target metadata must exist");
                    assert_eq!(target.transitioned_object.status, rustfs_filemeta::TRANSITION_COMPLETE);
                    assert_decommission_source_absent(&store, &bucket, object, &ObjectOptions::default()).await;

                    shutdown.cancel();
                });
            })
            .expect("spawn decommission retry test thread");
        if let Err(payload) = handle.join() {
            std::panic::resume_unwind(payload);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn decommission_outer_fence_loss_blocks_target_put_commit() {
        let temp_dir = tempfile::tempdir().expect("create decommission PUT fence-loss store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "decommission-put-fence-loss", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("decom-put-fence-loss-{}", uuid::Uuid::new_v4());
        let object = "ordinary.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create decommission PUT fence-loss bucket");
        let mut source = PutObjReader::from_vec(b"source generation".to_vec());
        store.pools[0]
            .put_object(&bucket, object, &mut source, &ObjectOptions::default())
            .await
            .expect("write decommission PUT source");
        mark_test_pool_decommissioning(&store, 0).await;

        let loss_hook = crate::store::object::DecommissionMutationFenceLossHook::install(
            &bucket,
            object,
            crate::store::object::DecommissionMutationFenceTestPhase::Migration,
        );
        let barrier = crate::set_disk::PutObjectCommitBarrier::install(
            &bucket,
            object,
            crate::set_disk::PutObjectCommitPause::BeforeQuotaRename,
        );
        let source_set = store.pools[0].get_disks_by_key(object);
        let worker_store = Arc::clone(&store);
        let worker_bucket = bucket.clone();
        let worker = tokio::spawn(async move {
            worker_store
                .decommission_entry_for_test(
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    worker_bucket,
                    source_set,
                )
                .await
        });

        barrier.wait_until_paused().await;
        loss_hook.mark_lost();
        barrier.release();
        drop(barrier);
        worker
            .await
            .expect("decommission PUT fence-loss worker should join")
            .expect("a fenced migration failure should remain retryable at entry scope");

        assert_pool_object_absent(&store.pools[1], &bucket, object).await;
        assert_pool_object_present(&store.pools[0], &bucket, object).await;
        shutdown.cancel();
    }

    #[test]
    #[serial_test::serial(storage_class_env)]
    fn decommission_outer_fence_loss_blocks_multipart_commits() {
        run_large_stack_async_test(
            "decommission-multipart-outer-fence-loss",
            decommission_outer_fence_loss_blocks_multipart_commits_case,
        );
    }

    async fn decommission_outer_fence_loss_blocks_multipart_commits_case() {
        for (object, pause) in [
            ("complete.bin", crate::set_disk::MultipartCommitPause::BeforeLockLost),
            ("new-upload.bin", crate::set_disk::MultipartCommitPause::NewUploadBeforeLockLost),
        ] {
            // Each injected commit failure intentionally leaves a capacity intent for
            // reconciliation.  Isolate the scenarios so one failure cannot turn the
            // next scenario into a capacity-recovery test before its barrier is hit.
            let temp_dir = tempfile::tempdir().expect("create decommission multipart fence-loss store dir");
            let (_ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(
                temp_dir.path(),
                "decommission-multipart-fence-loss",
                &[4, 4],
            ))
            .await;
            crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

            let bucket = format!("decom-mpu-fence-loss-{}", uuid::Uuid::new_v4());
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create decommission multipart fence-loss bucket");
            write_decommission_test_multipart_source(&store, 0, &bucket, object).await;
            mark_test_pool_decommissioning(&store, 0).await;

            let loss_hook = crate::store::object::DecommissionMutationFenceLossHook::install(
                &bucket,
                object,
                crate::store::object::DecommissionMutationFenceTestPhase::Migration,
            );
            let commit_observation = (pause == crate::set_disk::MultipartCommitPause::NewUploadBeforeLockLost)
                .then(|| crate::set_disk::NewMultipartUploadCommitObservation::install(&bucket, object));
            let barrier = crate::set_disk::MultipartCommitBarrier::install(&bucket, object, pause);
            let source_set = store.pools[0].get_disks_by_key(object);
            let recovery_source_set = Arc::clone(&source_set);
            let worker_store = Arc::clone(&store);
            let worker_bucket = bucket.clone();
            let mut worker = tokio::spawn(async move {
                worker_store
                    .decommission_entry_for_test(
                        0,
                        MetaCacheEntry {
                            name: object.to_string(),
                            ..Default::default()
                        },
                        worker_bucket,
                        source_set,
                    )
                    .await
            });

            tokio::select! {
                _ = barrier.wait_until_paused() => {}
                result = &mut worker => {
                    panic!("decommission multipart worker finished before the requested {pause:?} commit barrier: {result:?}");
                }
            }
            loss_hook.mark_lost();
            barrier.release();
            drop(barrier);
            worker
                .await
                .expect("decommission multipart fence-loss worker should join")
                .expect("a fenced multipart migration failure should remain retryable at entry scope");

            if let Some(commit_observation) = commit_observation {
                assert!(
                    !commit_observation.committed(),
                    "NewMultipart must reject a lost outer decommission/capacity fence even though it does not acquire the final remote-tuple publication fence"
                );
            }
            assert_pool_object_absent(&store.pools[1], &bucket, object).await;
            assert_pool_object_present(&store.pools[0], &bucket, object).await;
            let uploads = store.pools[1]
                .list_multipart_uploads(&bucket, object, None, None, None, 100)
                .await
                .expect("list target multipart uploads after fenced migration");
            assert!(uploads.uploads.is_empty(), "fenced multipart migration must not retain target staging");
            drop(loss_hook);
            store
                .decommission_entry_for_test(
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    bucket.clone(),
                    recovery_source_set,
                )
                .await
                .expect("same-mutation retry should recover the durable capacity intent");

            shutdown.cancel();
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn decommission_outer_fence_loss_blocks_source_cleanup_delete_commit() {
        let temp_dir = tempfile::tempdir().expect("create decommission cleanup fence-loss store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "decommission-cleanup-fence-loss", &[4, 4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("decom-cleanup-fence-loss-{}", uuid::Uuid::new_v4());
        let object = "cleanup.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create decommission cleanup fence-loss bucket");
        let mut source = PutObjReader::from_vec(b"source generation".to_vec());
        store.pools[0]
            .put_object(&bucket, object, &mut source, &ObjectOptions::default())
            .await
            .expect("write decommission cleanup source");
        mark_test_pool_decommissioning(&store, 0).await;

        let loss_hook = crate::store::object::DecommissionMutationFenceLossHook::install(
            &bucket,
            object,
            crate::store::object::DecommissionMutationFenceTestPhase::SourceCleanup,
        );
        let barrier = crate::data_movement::SourceCleanupDeleteBarrier::install(&bucket, object);
        let source_set = store.pools[0].get_disks_by_key(object);
        let worker_store = Arc::clone(&store);
        let worker_bucket = bucket.clone();
        let worker = tokio::spawn(async move {
            worker_store
                .decommission_entry_for_test(
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    worker_bucket,
                    source_set,
                )
                .await
        });

        barrier.wait_until_paused().await;
        loss_hook.mark_lost();
        barrier.release();
        drop(barrier);
        let err = worker
            .await
            .expect("decommission cleanup fence-loss worker should join")
            .expect_err("source cleanup must fail after its outer fence is lost");
        assert!(
            err.to_string().contains("delete_object_commit"),
            "cleanup failure must come from the delete commit fence: {err:?}"
        );

        assert_pool_object_present(&store.pools[0], &bucket, object).await;
        assert_pool_object_present(&store.pools[1], &bucket, object).await;
        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn reverse_decommission_reuses_fixed_target_fence_for_put_and_multipart() {
        let temp_dir = tempfile::tempdir().expect("create reverse decommission store dir");
        let (_ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store_with_layout(
            temp_dir.path(),
            "reverse-decommission-fixed-target",
            &[(1, 4), (1, 4)],
            CancellationToken::new(),
            None,
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("reverse-decom-fixed-target-{}", uuid::Uuid::new_v4());
        let object = "ordinary.bin";
        let object_body = b"reverse ordinary generation".to_vec();
        let self_copy_object = "source-only-self-copy.bin";
        let self_copy_body = b"source only copy generation".to_vec();
        let multipart_object = "multipart.bin";
        let first_part = vec![b'm'; 5 * 1024 * 1024];
        let second_part = b"reverse multipart tail".to_vec();
        let mut multipart_body = first_part.clone();
        multipart_body.extend_from_slice(&second_part);

        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create reverse decommission bucket");
        let mut source = PutObjReader::from_vec(object_body.clone());
        store.pools[1]
            .put_object(&bucket, object, &mut source, &ObjectOptions::default())
            .await
            .expect("write ordinary source object to pool 1");
        let mut self_copy_source = PutObjReader::from_vec(self_copy_body.clone());
        store.pools[1]
            .put_object(&bucket, self_copy_object, &mut self_copy_source, &ObjectOptions::default())
            .await
            .expect("write self-copy source object to pool 1");

        let upload = store.pools[1]
            .new_multipart_upload(&bucket, multipart_object, &ObjectOptions::default())
            .await
            .expect("create source multipart upload in pool 1");
        let mut completed_parts = Vec::with_capacity(2);
        for (part_number, bytes) in [(1, first_part.as_slice()), (2, second_part.as_slice())] {
            let mut reader = PutObjReader::from_vec(bytes.to_vec());
            let part = store.pools[1]
                .put_object_part(
                    &bucket,
                    multipart_object,
                    &upload.upload_id,
                    part_number,
                    &mut reader,
                    &ObjectOptions::default(),
                )
                .await
                .expect("write source multipart part");
            completed_parts.push(crate::storage_api_contracts::multipart::CompletePart {
                part_num: part.part_num,
                etag: part.etag,
                ..Default::default()
            });
        }
        store.pools[1]
            .clone()
            .complete_multipart_upload(&bucket, multipart_object, &upload.upload_id, completed_parts, &ObjectOptions::default())
            .await
            .expect("complete source multipart object in pool 1");

        {
            let mut pool_meta = store.pool_meta.write().await;
            pool_meta.pools[1].decommission = Some(PoolDecommissionInfo {
                start_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            });
        }
        assert!(store.is_suspended(1).await, "pool 1 must be the reverse decommission source");

        let self_copy_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let mut self_copy_reader = store
            .get_object_reader(&bucket, self_copy_object, None, HeaderMap::new(), &self_copy_opts)
            .await
            .expect("read the suspended source before its target is committed");
        let mut self_copy_info = self_copy_reader.object_info.clone();
        let mut self_copy_source_body = Vec::new();
        self_copy_reader
            .stream
            .read_to_end(&mut self_copy_source_body)
            .await
            .expect("drain the suspended self-copy source");
        assert_eq!(self_copy_source_body, self_copy_body);
        self_copy_info.metadata_only = true;
        self_copy_info.put_object_reader = Some(PutObjReader::from_vec(self_copy_source_body));
        store
            .copy_object(
                &bucket,
                self_copy_object,
                &bucket,
                self_copy_object,
                &mut self_copy_info,
                &self_copy_opts,
                &self_copy_opts,
            )
            .await
            .expect("self-copy should read the suspended source and commit to an active pool");
        assert_pool_object_present(&store.pools[0], &bucket, self_copy_object).await;
        assert_pool_object_present(&store.pools[1], &bucket, self_copy_object).await;

        let mut active_copy_reader = store
            .get_object_reader(&bucket, self_copy_object, None, HeaderMap::new(), &self_copy_opts)
            .await
            .expect("read the active self-copy target while the source remains");
        let active_copy_data_dir = active_copy_reader.object_info.data_dir;
        let mut active_copy_info = active_copy_reader.object_info.clone();
        let mut active_copy_body = Vec::new();
        active_copy_reader
            .stream
            .read_to_end(&mut active_copy_body)
            .await
            .expect("drain the active self-copy target");
        assert_eq!(active_copy_body, self_copy_body);
        active_copy_info.metadata_only = true;
        active_copy_info.put_object_reader = Some(PutObjReader::from_vec(active_copy_body));
        let active_copy_result = store
            .copy_object(
                &bucket,
                self_copy_object,
                &bucket,
                self_copy_object,
                &mut active_copy_info,
                &self_copy_opts,
                &self_copy_opts,
            )
            .await
            .expect("self-copy should keep using the committed active target");
        assert_eq!(active_copy_result.data_dir, active_copy_data_dir);

        {
            let mut pool_meta = store.pool_meta.write().await;
            pool_meta.pools[1].decommission = None;
        }
        mark_test_pool_decommissioning(&store, 1).await;

        let cleanup_barrier = crate::data_movement::SourceCleanupDeleteBarrier::install(&bucket, object);
        let commit_barrier = crate::set_disk::PutObjectCommitBarrier::install(
            &bucket,
            object,
            crate::set_disk::PutObjectCommitPause::AfterNamespace,
        );
        let source_set = store.pools[1].get_disks_by_key(object);
        let worker_store = Arc::clone(&store);
        let worker_bucket = bucket.clone();
        let worker = tokio::spawn(async move {
            worker_store
                .decommission_entry_for_test(
                    1,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    worker_bucket,
                    source_set,
                )
                .await
        });
        commit_barrier.wait_until_paused().await;

        let delete_barrier = crate::store::object::DeleteAfterObjectLockSnapshotBarrier::install(&bucket);
        let delete_store = Arc::clone(&store);
        let delete_bucket = bucket.clone();
        let delete = tokio::spawn(async move {
            delete_store
                .delete_object(&delete_bucket, object, ObjectOptions::default())
                .await
        });
        delete_barrier.wait_until_paused().await;
        delete_barrier.release_and_wait_until_namespace_pending().await;
        assert!(
            !delete_barrier.namespace_acquired() && !delete.is_finished(),
            "the reverse target commit must keep DELETE behind the fixed read fence"
        );
        delete.abort();
        assert!(
            delete
                .await
                .expect_err("the blocked DELETE should be canceled")
                .is_cancelled(),
            "canceling the blocked DELETE must not mutate either pool"
        );
        drop(delete_barrier);

        commit_barrier.release();
        cleanup_barrier.wait_until_paused().await;
        drop(commit_barrier);

        let read_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let target_info = store.pools[0]
            .get_object_info(&bucket, object, &read_opts)
            .await
            .expect("read the committed active target before source cleanup");
        store.pools[1]
            .get_object_info(&bucket, object, &read_opts)
            .await
            .expect("the source must remain present until cleanup is released");
        let routed_info = store
            .get_object_info(&bucket, object, &read_opts)
            .await
            .expect("HEAD routing should prefer the active target before source cleanup");
        assert_eq!(routed_info.data_dir, target_info.data_dir);

        let mut ranged_reader = store
            .get_object_reader(
                &bucket,
                object,
                Some(HTTPRangeSpec {
                    is_suffix_length: false,
                    start: 8,
                    end: 15,
                }),
                HeaderMap::new(),
                &read_opts,
            )
            .await
            .expect("ranged GET routing should prefer the active target before source cleanup");
        assert_eq!(ranged_reader.object_info.data_dir, target_info.data_dir);
        let mut ranged_body = Vec::new();
        ranged_reader
            .stream
            .read_to_end(&mut ranged_body)
            .await
            .expect("drain the routed target range");
        assert_eq!(ranged_body, object_body[8..=15]);

        cleanup_barrier.release();
        drop(cleanup_barrier);
        tokio::time::timeout(Duration::from_secs(60), worker)
            .await
            .expect("reverse ordinary decommission must not self-deadlock on the fixed target set")
            .expect("reverse ordinary decommission worker should join")
            .expect("reverse ordinary decommission should complete");

        let mut ordinary_reader = store.pools[0]
            .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("read the ordinary object from the fixed target set");
        let mut ordinary_target_body = Vec::new();
        ordinary_reader
            .stream
            .read_to_end(&mut ordinary_target_body)
            .await
            .expect("drain the ordinary target body");
        assert_eq!(ordinary_target_body, object_body, "ordinary migration must preserve the full body");
        let ordinary_source_err = store.pools[1]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect_err("ordinary source generation must be cleaned after migration");
        assert!(matches!(ordinary_source_err, StorageError::ObjectNotFound(_, _)));

        let multipart_source_set = store.pools[1].get_disks_by_key(multipart_object);
        let multipart_store = Arc::clone(&store);
        let multipart_bucket = bucket.clone();
        let multipart_worker = tokio::spawn(async move {
            multipart_store
                .decommission_entry_for_test(
                    1,
                    MetaCacheEntry {
                        name: multipart_object.to_string(),
                        ..Default::default()
                    },
                    multipart_bucket,
                    multipart_source_set,
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(60), multipart_worker)
            .await
            .expect("reverse multipart decommission must not self-deadlock on new or complete")
            .expect("reverse multipart decommission worker should join")
            .expect("reverse multipart decommission should complete");

        let target_info = store.pools[0]
            .get_object_info(&bucket, multipart_object, &ObjectOptions::default())
            .await
            .expect("read migrated multipart metadata from the fixed target set");
        assert!(target_info.is_multipart(), "migration must retain multipart identity");
        let mut multipart_reader = store.pools[0]
            .get_object_reader(&bucket, multipart_object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("read migrated multipart object from the fixed target set");
        let mut multipart_target_body = Vec::new();
        multipart_reader
            .stream
            .read_to_end(&mut multipart_target_body)
            .await
            .expect("drain the multipart target body");
        assert_eq!(multipart_target_body, multipart_body, "multipart migration must preserve the full body");
        let multipart_source_err = store.pools[1]
            .get_object_info(&bucket, multipart_object, &ObjectOptions::default())
            .await
            .expect_err("multipart source generation must be cleaned after migration");
        assert!(matches!(multipart_source_err, StorageError::ObjectNotFound(_, _)));

        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn batch_delete_real_path_preserves_source_pool_errors_in_any_pool_order() {
        let temp_dir = tempfile::tempdir().expect("create batch delete pool-error store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "batch-delete-pool-errors", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        for source_pool_idx in [0, 1] {
            {
                let mut pool_meta = store.pool_meta.write().await;
                for pool in &mut pool_meta.pools {
                    pool.decommission = None;
                }
            }

            let bucket = format!("batch-del-pool-error-{source_pool_idx}-{}", uuid::Uuid::new_v4());
            let object_names = vec![
                format!("third-{source_pool_idx}.bin"),
                format!("first-{source_pool_idx}.bin"),
                format!("second-{source_pool_idx}.bin"),
            ];
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("create batch delete pool-error bucket");
            for pool in &store.pools {
                for object_name in &object_names {
                    let mut reader = PutObjReader::from_vec(format!("pool {} {object_name}", pool.pool_idx).into_bytes());
                    pool.put_object(&bucket, object_name, &mut reader, &ObjectOptions::default())
                        .await
                        .expect("seed each object in both the source and active pools");
                }
            }
            {
                let mut pool_meta = store.pool_meta.write().await;
                pool_meta.pools[source_pool_idx].decommission = Some(PoolDecommissionInfo {
                    start_time: Some(OffsetDateTime::now_utc()),
                    ..Default::default()
                });
            }
            assert!(
                store.is_suspended(source_pool_idx).await,
                "the injected error pool must be the decommission source"
            );

            let expected_errors = [
                StorageError::ErasureWriteQuorum,
                StorageError::NamespaceLockQuorumUnavailable {
                    mode: "delete_objects_commit",
                    bucket: bucket.clone(),
                    object: object_names[1].clone(),
                    required: 3,
                    achieved: 2,
                },
                StorageError::ErasureWriteQuorum,
            ];
            let injection = crate::store::object::BatchDeletePoolErrorInjection::install(
                &bucket,
                source_pool_idx,
                object_names.iter().cloned().zip(expected_errors.iter().cloned()).collect(),
            );
            let requests = object_names
                .iter()
                .map(|object_name| ObjectToDelete {
                    object_name: object_name.clone(),
                    ..Default::default()
                })
                .collect();

            let (deleted, errors) = store.delete_objects(&bucket, requests, ObjectOptions::default()).await;

            assert_eq!(
                injection.observed(),
                object_names.len(),
                "the source pool must first complete every real delete"
            );
            assert_eq!(
                errors,
                expected_errors.iter().cloned().map(Some).collect::<Vec<_>>(),
                "a successful pool must not clear a source pool failure at any request index"
            );
            assert_eq!(
                deleted.iter().map(|object| object.object_name.as_str()).collect::<Vec<_>>(),
                object_names.iter().map(String::as_str).collect::<Vec<_>>(),
                "DeleteObjects must preserve request index mapping while aggregating pool failures"
            );
            assert!(
                deleted.iter().all(|object| object.found),
                "the injected source results must retain real delete success data"
            );

            for pool in &store.pools {
                for object_name in &object_names {
                    let error = pool
                        .get_object_info(&bucket, object_name, &ObjectOptions::default())
                        .await
                        .expect_err("both the active and source pool delete calls must execute");
                    assert!(
                        matches!(error, StorageError::ObjectNotFound(_, _)),
                        "unexpected residual object: {error:?}"
                    );
                }
            }
            drop(injection);
        }

        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn decommission_source_cleanup_holds_hashed_set_lock_across_preflight() {
        let temp_dir = tempfile::tempdir().expect("create multi-set decommission cleanup store dir");
        let (_ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store_with_layout(
            temp_dir.path(),
            "multi-set-decommission-source-cleanup",
            &[(2, 4)],
            CancellationToken::new(),
            None,
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("decom-source-cleanup-lock-{}", uuid::Uuid::new_v4());
        let object = (0..128)
            .map(|index| format!("object-{index}.bin"))
            .find(|candidate| store.pools[0].get_disks_by_key(candidate).set_index == 1)
            .expect("the deterministic object search should select source set 1");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create multi-set decommission cleanup bucket");
        let mut source = PutObjReader::from_vec(b"source generation".to_vec());
        store.pools[0]
            .put_object(&bucket, &object, &mut source, &ObjectOptions::default())
            .await
            .expect("write the source generation to set 1");
        let source_set = store.pools[0].get_disks_by_key(&object);
        assert_eq!(source_set.set_index, 1, "the source must not share the fixed set-0 namespace");
        let expected_source_versions = source_set
            .load_file_info_versions_exact(&bucket, &object)
            .await
            .expect("source versions should be readable")
            .expect("the source generation should exist");

        let cleanup_barrier = crate::data_movement::SourceCleanupDeleteBarrier::install(&bucket, &object);
        let cleanup_store = Arc::clone(&store);
        let cleanup_bucket = bucket.clone();
        let cleanup_object = object.clone();
        let cleanup = tokio::spawn(async move {
            let mutation_fence = cleanup_store
                .acquire_decommission_source_cleanup_fence(&cleanup_bucket, &cleanup_object, source_set.as_ref())
                .await?;
            crate::data_movement::cleanup_source_entry_if_unchanged(
                source_set,
                &cleanup_bucket,
                &cleanup_object,
                &expected_source_versions,
                &[],
                crate::data_movement::SourceCleanupBucketFence {
                    object_mutation_fence: Some(&mutation_fence),
                    ..Default::default()
                },
                "test_multi_set_decommission_source_cleanup",
            )
            .await
        });
        cleanup_barrier.wait_until_paused().await;

        let put_barrier = crate::set_disk::PutObjectCommitBarrier::install(
            &bucket,
            &object,
            crate::set_disk::PutObjectCommitPause::BeforeNamespace,
        );
        let mutation_pool = Arc::clone(&store.pools[0]);
        let mutation_bucket = bucket.clone();
        let mutation_object = object.clone();
        let replacement = b"replacement generation".to_vec();
        let expected_replacement = replacement.clone();
        let mutation = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(replacement);
            mutation_pool
                .put_object(&mutation_bucket, &mutation_object, &mut reader, &ObjectOptions::default())
                .await
        });
        put_barrier.wait_until_paused().await;
        put_barrier.release_and_wait_until_namespace_pending().await;
        assert!(!mutation.is_finished(), "a source mutation must wait behind cleanup's set-1 write lock");

        cleanup_barrier.release();
        cleanup
            .await
            .expect("source cleanup task should join")
            .expect("source cleanup should remove only the preflight generation");
        mutation
            .await
            .expect("source mutation task should join")
            .expect("source mutation should commit after cleanup releases the set lock");

        let mut reader = store.pools[0]
            .get_object_reader(&bucket, &object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("the replacement generation must remain readable");
        let mut actual = Vec::new();
        reader
            .stream
            .read_to_end(&mut actual)
            .await
            .expect("read the replacement generation");
        assert_eq!(actual, expected_replacement, "cleanup must not delete the replacement generation");

        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn versioned_delete_marker_survives_decommission_source_cleanup() {
        let temp_dir = tempfile::tempdir().expect("create versioned decommission delete-fence store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "versioned-decommission-delete-fence", &[4, 4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("versioned-decom-delete-{}", uuid::Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create versioned decommission delete-fence bucket");
        let (source_version, expected_source_versions) = migrate_versioned_decommission_test_object(
            &store,
            &bucket,
            object,
            b"source generation",
            "test_versioned_decommission_delete_fence",
        )
        .await;

        let delete_barrier = crate::store::object::VersionedDeleteMarkerCommitBarrier::install(&bucket, object);
        let delete_store = Arc::clone(&store);
        let delete_bucket = bucket.clone();
        let delete = tokio::spawn(async move {
            delete_store
                .delete_object(
                    &delete_bucket,
                    object,
                    ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
        });
        delete_barrier.wait_until_paused().await;
        let cleanup_set = store.pools[0].get_disks_by_key(object);
        crate::data_movement::ensure_source_cleanup_versions_unchanged(
            Arc::clone(&cleanup_set),
            &bucket,
            object,
            &expected_source_versions,
            &[],
            "test_versioned_decommission_delete_fence",
        )
        .await
        .expect("the committed delete marker must not be published to the suspended source pool");

        let cleanup_delete_barrier = crate::data_movement::SourceCleanupDeleteBarrier::install(&bucket, object);
        let cleanup_store = Arc::clone(&store);
        let cleanup_bucket = bucket.clone();
        let cleanup = tokio::spawn(async move {
            let mutation_fence = cleanup_store
                .acquire_decommission_source_cleanup_fence(&cleanup_bucket, object, cleanup_set.as_ref())
                .await?;
            crate::data_movement::cleanup_source_entry_if_unchanged(
                cleanup_set,
                &cleanup_bucket,
                object,
                &expected_source_versions,
                &[],
                crate::data_movement::SourceCleanupBucketFence {
                    object_mutation_fence: Some(&mutation_fence),
                    ..Default::default()
                },
                "test_versioned_decommission_delete_fence",
            )
            .await
        });
        cleanup_delete_barrier.wait_until_fence_pending().await;
        assert!(
            !cleanup_delete_barrier.is_paused(),
            "source cleanup must wait for the versioned DELETE mutation fence"
        );

        delete_barrier.release();
        let marker = delete
            .await
            .expect("versioned DELETE task should join")
            .expect("versioned DELETE should publish a delete marker after migration");
        assert!(marker.delete_marker, "versioned DELETE must publish a delete marker");
        assert!(
            marker.version_id.is_some_and(|version_id| !version_id.is_nil()),
            "the delete marker must have a non-nil version ID"
        );

        cleanup_delete_barrier.wait_until_paused().await;
        cleanup_delete_barrier.release();
        cleanup
            .await
            .expect("source cleanup task should join")
            .expect("source cleanup should preserve the active-pool delete marker");

        let err = store
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("the post-migration delete marker must hide the migrated version");
        assert!(
            matches!(err, StorageError::ObjectNotFound(_, _)),
            "unexpected latest-version result: {err:?}"
        );
        store
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("the migrated source version must remain addressable below the delete marker");
        store.pools[0]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("source cleanup must remove the decommissioned source versions");

        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn decommission_entry_skips_cleanup_only_marker_when_free_version_is_present() {
        let temp_dir = tempfile::tempdir().expect("create free-version decommission store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "decommission-free-marker", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = format!("decom-free-marker-{}", uuid::Uuid::new_v4());
        let object = "free-marker-object";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create free-version decommission bucket");
        let (_, free_version) = seed_transitioned_free_version(&ctx, &store, &bucket, object).await;
        let source_free = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(&bucket, object)
            .await
            .expect("source free-version metadata should decode")
            .and_then(|versions| {
                versions
                    .versions
                    .into_iter()
                    .find(|version| version.version_id == Some(free_version) && version.tier_free_version())
            })
            .expect("source free-version identity should be present before decommission");
        let mut target_reader = PutObjReader::from_vec(b"target ordinary bytes".to_vec());
        let target_w = store.pools[1]
            .put_object(
                &bucket,
                object,
                &mut target_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("write unrelated target version");
        let target_w_version = target_w.version_id.expect("target version should have an id");
        let marker = store.pools[0]
            .delete_object(
                &bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("write cleanup-only delete marker");
        assert!(marker.delete_marker);

        mark_test_pool_decommissioning(&store, 0).await;
        let source_set = store.pools[0].get_disks_by_key(object);
        store
            .decommission_entry_for_test_with_bucket_incarnation(
                0,
                MetaCacheEntry {
                    name: object.to_string(),
                    ..Default::default()
                },
                bucket.clone(),
                source_set.clone(),
            )
            .await
            .expect("real decommission entry should migrate the free version");

        let target_versions = store.pools[1]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(&bucket, object)
            .await
            .expect("target versions should decode")
            .expect("target free version should be present");
        assert!(
            target_versions
                .versions
                .iter()
                .any(|version| { version.version_id == Some(free_version) && version.tier_free_version() })
        );
        let migrated_free = target_versions
            .versions
            .iter()
            .find(|version| version.version_id == Some(free_version) && version.tier_free_version())
            .expect("migrated free-version identity should remain readable from target disks");
        assert!(
            crate::store::tiered_data_movement_source_matches(&source_free, migrated_free)
                .expect("migrated free-version identity should decode")
        );
        let retained_w = target_versions
            .versions
            .iter()
            .find(|version| version.version_id == Some(target_w_version))
            .expect("unrelated target version should remain");
        assert!(!retained_w.deleted && !retained_w.tier_free_version());
        assert_eq!(retained_w.size, target_w.size);
        assert_eq!(retained_w.get_etag(), target_w.etag);
        assert!(
            target_versions
                .versions
                .iter()
                .all(|version| { version.tier_free_version() || !version.deleted })
        );
        assert!(
            source_set
                .load_file_info_versions_exact(&bucket, object)
                .await
                .expect("source versions should be readable after cleanup")
                .is_none(),
            "successful free-version migration should permit source cleanup"
        );
        let (heal_versions, _, _) = store
            .heal_walk_versions_page(1, 0, &bucket, "", None, 2, 16, true)
            .await
            .expect("heal walk should decode the migrated free version");
        let free_version_string = free_version.to_string();
        let healed_free = heal_versions
            .iter()
            .find(|version| version.version_id.as_deref() == Some(free_version_string.as_str()))
            .expect("heal walk should surface the migrated free version");
        let healed_info = healed_free
            .lifecycle_object_info
            .as_ref()
            .expect("heal walk should retain lifecycle identity for the migrated free version");
        assert!(healed_info.transitioned_object.free_version);
        assert_eq!(healed_info.transitioned_object.tier, source_free.transition_tier);
        assert_eq!(healed_info.transitioned_object.name, source_free.transitioned_objname);
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn decommission_entry_allows_free_version_consumed_before_source_lock() {
        let temp_dir = tempfile::tempdir().expect("create consumed free-version store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "decommission-free-consumed", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = format!("decom-free-consumed-{}", uuid::Uuid::new_v4());
        let object = "free-consumed-object";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create consumed free-version bucket");
        let (_, free_version) = seed_transitioned_free_version(&ctx, &store, &bucket, object).await;

        mark_test_pool_decommissioning(&store, 0).await;
        let source_set = store.pools[0].get_disks_by_key(object);
        let barrier = crate::store::object::DecommissionFreeVersionSourceRaceBarrier::install(&bucket, object);
        let decommission = tokio::spawn({
            let store = store.clone();
            let bucket = bucket.clone();
            let source_set = source_set.clone();
            async move {
                store
                    .decommission_entry_for_test_with_bucket_incarnation(
                        0,
                        MetaCacheEntry {
                            name: object.to_string(),
                            ..Default::default()
                        },
                        bucket,
                        source_set,
                    )
                    .await
            }
        });

        barrier.wait_until_paused().await;
        store.pools[0]
            .delete_object(
                &bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    version_id: Some(free_version.to_string()),
                    incl_free_versions: true,
                    ..Default::default()
                },
            )
            .await
            .expect("lifecycle should consume the source free version before decommission locks it");
        assert!(
            source_set
                .load_file_info_versions_exact(&bucket, object)
                .await
                .expect("consumed source metadata should remain readable")
                .is_none(),
            "the lifecycle delete should remove the source free version"
        );

        barrier.release();
        decommission
            .await
            .expect("decommission task should join")
            .expect("a concurrently consumed free version should not fail source cleanup");
        assert!(
            store.pools[1]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(&bucket, object)
                .await
                .expect("target metadata should remain readable")
                .is_none(),
            "an already consumed free version should not be recreated on the target"
        );
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn decommission_entry_rejects_subquorum_free_version_conflict_and_retains_source() {
        let temp_dir = tempfile::tempdir().expect("create sub-quorum free-version store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "decommission-free-conflict", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = format!("decom-free-conflict-{}", uuid::Uuid::new_v4());
        let object = "free-conflict-object";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create sub-quorum conflict bucket");
        let (_, free_version) = seed_transitioned_free_version(&ctx, &store, &bucket, object).await;
        let source_free = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(&bucket, object)
            .await
            .expect("source free version should decode before crash replay setup")
            .and_then(|versions| {
                versions
                    .versions
                    .into_iter()
                    .find(|version| version.version_id == Some(free_version))
            })
            .expect("source free version should be available for crash replay setup");

        let mut target_reader = PutObjReader::from_vec(b"target ordinary bytes".to_vec());
        let target = store.pools[1]
            .put_object(
                &bucket,
                object,
                &mut target_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("seed ordinary target version");
        let target_version = target.version_id.expect("target version must have an ID");
        // A regular PUT may acknowledge write quorum while its rename tail still owns the
        // target namespace lock. Drain that tail through a locked read before bypassing
        // the namespace API with the per-disk crash-replay fixture writes below.
        store.pools[1]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(target_version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("target PUT tail should drain before per-disk fixture mutation");
        let target_disks = store.pools[1].get_disks_by_key(object).disks.read().await.clone();
        for disk in target_disks.iter().skip(1) {
            disk.as_ref()
                .expect("target crash replay quorum disk should be online")
                .write_metadata("", &bucket, object, source_free.clone())
                .await
                .expect("seed an equivalent free version on the target quorum");
        }
        let conflict_path = temp_dir
            .path()
            .join(format!("pool1/set0/disk0/{bucket}/{object}/{STORAGE_FORMAT_FILE}"));
        let encoded = tokio::fs::read(&conflict_path)
            .await
            .expect("target metadata should be readable");
        let mut metadata = FileMeta::load(&encoded).expect("target metadata should decode");
        let target_index = metadata
            .versions
            .iter()
            .position(|version| version.header.version_id == Some(target_version))
            .expect("target version should be present on the conflict disk");
        let mut target_meta = metadata.versions[target_index]
            .parse_version_meta()
            .expect("target version metadata should decode");
        target_meta
            .object
            .as_mut()
            .expect("target conflict must remain an ordinary object")
            .version_id = Some(free_version);
        metadata.versions[target_index] = target_meta.try_into().expect("conflict metadata should encode");
        let expected_conflict_meta = metadata.versions[target_index].meta.clone();
        let expected_conflict = metadata.versions[target_index]
            .into_fileinfo(&bucket, object, true)
            .expect("conflict metadata should decode as an ordinary object");
        let duplicate_free: rustfs_filemeta::FileMetaShallowVersion = rustfs_filemeta::FileMetaVersion::from(source_free.clone())
            .try_into()
            .expect("duplicate free metadata should encode");
        metadata.versions.insert(target_index, duplicate_free);
        tokio::fs::write(&conflict_path, metadata.marshal_msg().expect("conflict metadata should encode"))
            .await
            .expect("write sub-quorum conflict metadata");

        let mut expected_target_metadata = Vec::with_capacity(4);
        for disk_index in 0..4 {
            let target_path = temp_dir
                .path()
                .join(format!("pool1/set0/disk{disk_index}/{bucket}/{object}/{STORAGE_FORMAT_FILE}"));
            let target_encoded = tokio::fs::read(&target_path)
                .await
                .expect("target fixture metadata should be readable before decommission");
            let target_meta = FileMeta::load(&target_encoded).expect("target fixture metadata should decode");
            let same_id = target_meta
                .versions
                .iter()
                .filter(|version| version.header.version_id == Some(free_version))
                .collect::<Vec<_>>();
            if disk_index == 0 {
                assert_eq!(same_id.len(), 2, "conflict fixture should contain both same-ID records");
                assert!(same_id[0].header.free_version());
                assert!(!same_id[1].header.free_version());
            } else {
                assert_eq!(same_id.len(), 1, "target fixture disk {disk_index} should contain one free version");
                assert!(same_id[0].header.free_version());
            }
            expected_target_metadata.push(target_encoded);
        }

        mark_test_pool_decommissioning(&store, 0).await;
        let source_set = store.pools[0].get_disks_by_key(object);
        store
            .decommission_entry_for_test_with_bucket_incarnation(
                0,
                MetaCacheEntry {
                    name: object.to_string(),
                    ..Default::default()
                },
                bucket.clone(),
                source_set.clone(),
            )
            .await
            .expect("conflicted decommission entry should retain the source and retry later");

        let source_versions = source_set
            .load_file_info_versions_exact(&bucket, object)
            .await
            .expect("retained source versions should decode")
            .expect("source free version should be retained after conflict");
        assert!(
            source_versions
                .versions
                .iter()
                .any(|version| { version.version_id == Some(free_version) && version.tier_free_version() })
        );
        let post_encoded = tokio::fs::read(&conflict_path)
            .await
            .expect("conflict metadata should remain readable");
        let post_metadata = FileMeta::load(&post_encoded).expect("post-conflict metadata should decode");
        let same_id = post_metadata
            .versions
            .iter()
            .filter(|version| version.header.version_id == Some(free_version))
            .collect::<Vec<_>>();
        assert_eq!(same_id.len(), 2, "conflict metadata should retain both same-ID records");
        assert_eq!(same_id.iter().filter(|version| version.header.free_version()).count(), 1);
        assert_eq!(same_id.iter().filter(|version| !version.header.free_version()).count(), 1);
        let post_conflict = same_id
            .into_iter()
            .find(|version| !version.header.free_version())
            .expect("ordinary conflict version must remain addressable by the source ID");
        let post_conflict_info = post_conflict
            .into_fileinfo(&bucket, object, true)
            .expect("post-conflict ordinary metadata should decode");
        assert!(!post_conflict_info.deleted && !post_conflict_info.tier_free_version());
        assert_eq!(post_conflict.meta, expected_conflict_meta);
        assert_eq!(post_conflict_info.size, expected_conflict.size);
        assert_eq!(post_conflict_info.data_dir, expected_conflict.data_dir);
        assert_eq!(post_conflict_info.metadata, expected_conflict.metadata);
        assert_eq!(post_conflict_info.get_etag(), expected_conflict.get_etag());

        for (disk_index, expected_target_encoded) in expected_target_metadata.iter().enumerate() {
            let target_path = temp_dir
                .path()
                .join(format!("pool1/set0/disk{disk_index}/{bucket}/{object}/{STORAGE_FORMAT_FILE}"));
            let target_encoded = tokio::fs::read(&target_path)
                .await
                .expect("target metadata should remain readable");
            assert_eq!(
                &target_encoded, expected_target_encoded,
                "conflicted decommission must not mutate target disk {disk_index}"
            );
            let target_meta = FileMeta::load(&target_encoded).expect("target metadata should decode");
            let same_id = target_meta
                .versions
                .iter()
                .filter(|version| version.header.version_id == Some(free_version))
                .collect::<Vec<_>>();
            if disk_index == 0 {
                assert_eq!(same_id.len(), 2);
                assert!(same_id[0].header.free_version());
                assert!(!same_id[1].header.free_version());
            } else {
                assert_eq!(same_id.len(), 1);
                assert!(same_id[0].header.free_version());
            }
        }
        let sweep_err = store
            .check_after_decommission_for_test(0)
            .await
            .expect_err("final sweep must report the retained free version");
        assert!(
            sweep_err.to_string().contains("version(s) were found"),
            "unexpected final sweep error: {sweep_err}"
        );
        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn versioned_batch_delete_marker_skips_decommission_source() {
        let temp_dir = tempfile::tempdir().expect("create versioned batch decommission store dir");
        let (_ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "versioned-batch-decommission-delete-fence",
            &[4, 4, 4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("vbatch-decom-delete-{}", uuid::Uuid::new_v4());
        let object = "batch-object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create versioned batch decommission bucket");
        let (_source_version, expected_source_versions) = migrate_versioned_decommission_test_object(
            &store,
            &bucket,
            object,
            b"batch source generation",
            "test_versioned_batch_decommission_delete_fence",
        )
        .await;

        let delete_config_snapshot =
            Arc::new(crate::bucket::replication::DeleteReplicationConfigSnapshot::from_configs_for_test(
                s3s::dto::VersioningConfiguration {
                    status: Some(s3s::dto::BucketVersioningStatus::from_static(s3s::dto::BucketVersioningStatus::ENABLED)),
                    ..Default::default()
                },
                None,
            ));
        let delete_barrier = crate::store::object::VersionedDeleteMarkerCommitBarrier::install(&bucket, object);
        let delete_store = Arc::clone(&store);
        let delete_bucket = bucket.clone();
        let delete = tokio::spawn(async move {
            delete_store
                .delete_objects(
                    &delete_bucket,
                    vec![ObjectToDelete {
                        object_name: object.to_string(),
                        ..Default::default()
                    }],
                    ObjectOptions {
                        delete_replication_config_snapshot: Some(delete_config_snapshot),
                        ..Default::default()
                    },
                )
                .await
        });
        delete_barrier.wait_until_paused().await;

        let source_set = store.pools[0].get_disks_by_key(object);
        crate::data_movement::ensure_source_cleanup_versions_unchanged(
            source_set,
            &bucket,
            object,
            &expected_source_versions,
            &[],
            "test_versioned_batch_decommission_delete_fence",
        )
        .await
        .expect("batch DELETE must not publish a marker to the suspended source");

        delete_barrier.release();
        let (deleted, errors) = delete.await.expect("versioned batch DELETE task should join");
        assert!(errors.iter().all(Option::is_none), "versioned batch DELETE should succeed: {errors:?}");
        assert_eq!(deleted.len(), 1);
        assert!(deleted[0].delete_marker, "versioned batch DELETE must return a marker");
        assert!(
            deleted[0]
                .delete_marker_version_id
                .is_some_and(|version_id| !version_id.is_nil()),
            "versioned batch DELETE marker must have a non-nil version ID"
        );

        let mut active_marker_count = 0;
        for pool in store.pools.iter().skip(1) {
            let Some(versions) = pool
                .get_disks_by_key(object)
                .load_file_info_versions_exact(&bucket, object)
                .await
                .expect("active-pool versions should be readable")
            else {
                continue;
            };
            active_marker_count += versions.versions.iter().filter(|version| version.deleted).count();
        }
        assert_eq!(active_marker_count, 1, "batch DELETE must publish exactly one active-pool marker");

        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn suspended_delete_marker_then_decommission_worker_converges_null_source() {
        let temp_dir = tempfile::tempdir().expect("create suspended decommission DELETE store dir");
        let (_ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "suspended-decommission-delete-convergence",
            &[4, 4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("suspended-decom-delete-{}", uuid::Uuid::new_v4());
        let object = "single.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create suspended decommission DELETE bucket");
        write_suspended_decommission_source(&store, &bucket, object).await;
        mark_test_pool_decommissioning(&store, 0).await;

        let delete_err = store
            .delete_object(
                &bucket,
                object,
                ObjectOptions {
                    version_suspended: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("capacity-reserved target must reject a concurrent suspended DELETE");
        assert!(
            matches!(delete_err, Error::SlowDown),
            "unexpected suspended DELETE result: {delete_err:?}"
        );
        assert_suspended_null_source_present(&store, &bucket, object).await;

        let source_set = store.pools[0].get_disks_by_key(object);
        let worker_store = Arc::clone(&store);
        let worker_bucket = bucket.clone();
        tokio::spawn(async move {
            worker_store
                .decommission_entry_for_test(
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    worker_bucket,
                    source_set,
                )
                .await
        })
        .await
        .expect("suspended decommission worker should join")
        .expect("worker must migrate the fenced suspended source");

        assert_decommission_source_absent(
            &store,
            &bucket,
            object,
            &ObjectOptions {
                version_suspended: true,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(
            read_decommission_target_body(&store, &bucket, object, &ObjectOptions::default()).await,
            b"suspended source generation"
        );
        shutdown.cancel();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    #[serial_test::serial(storage_class_env)]
    async fn suspended_batch_delete_marker_then_decommission_worker_converges_null_source() {
        let temp_dir = tempfile::tempdir().expect("create suspended batch decommission DELETE store dir");
        let (_ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "suspended-batch-decommission-delete-convergence",
            &[4, 4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("susp-batch-decom-delete-{}", uuid::Uuid::new_v4());
        let object = "batch.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create suspended batch decommission DELETE bucket");
        write_suspended_decommission_source(&store, &bucket, object).await;
        mark_test_pool_decommissioning(&store, 0).await;

        let delete_config_snapshot =
            Arc::new(crate::bucket::replication::DeleteReplicationConfigSnapshot::from_configs_for_test(
                s3s::dto::VersioningConfiguration {
                    status: Some(s3s::dto::BucketVersioningStatus::from_static(s3s::dto::BucketVersioningStatus::SUSPENDED)),
                    ..Default::default()
                },
                None,
            ));
        let (_deleted, errors) = store
            .delete_objects(
                &bucket,
                vec![ObjectToDelete {
                    object_name: object.to_string(),
                    ..Default::default()
                }],
                ObjectOptions {
                    delete_replication_config_snapshot: Some(delete_config_snapshot),
                    ..Default::default()
                },
            )
            .await;
        assert!(
            matches!(errors.as_slice(), [Some(Error::SlowDown)]),
            "unexpected suspended batch DELETE result: {errors:?}"
        );
        assert_suspended_null_source_present(&store, &bucket, object).await;

        let source_set = store.pools[0].get_disks_by_key(object);
        let worker_store = Arc::clone(&store);
        let worker_bucket = bucket.clone();
        tokio::spawn(async move {
            worker_store
                .decommission_entry_for_test(
                    0,
                    MetaCacheEntry {
                        name: object.to_string(),
                        ..Default::default()
                    },
                    worker_bucket,
                    source_set,
                )
                .await
        })
        .await
        .expect("suspended batch decommission worker should join")
        .expect("worker must migrate the batch-fenced suspended source");

        assert_decommission_source_absent(
            &store,
            &bucket,
            object,
            &ObjectOptions {
                version_suspended: true,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(
            read_decommission_target_body(&store, &bucket, object, &ObjectOptions::default()).await,
            b"suspended source generation"
        );
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tiered_data_movement_rejects_a_stale_source_snapshot_before_target_write() {
        let temp_dir = tempfile::tempdir().expect("create stale-source data movement store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "stale-tier-source", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "stale-tier-source-bucket";
        let object = "stale-tier-source-object";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        register_mock_tier(&ctx.tier_config_mgr(), "STALE-TIER").await;
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("bucket incarnation should exist");
        let version_id = uuid::Uuid::new_v4();
        let stale = FileInfo {
            volume: bucket.to_string(),
            name: object.to_string(),
            version_id: Some(version_id),
            transition_status: rustfs_filemeta::TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/stale-source".to_string(),
            transition_tier: "STALE-TIER".to_string(),
            transition_version_id: Some(uuid::Uuid::new_v4()),
            transition_version_state: rustfs_filemeta::TransitionVersionState::Exact,
            data_dir: Some(uuid::Uuid::new_v4()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            size: 1,
            metadata: HashMap::new(),
            ..Default::default()
        };
        let err = store
            .decommission_tiered_object(
                bucket,
                object,
                &stale,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version_id.to_string()),
                    src_pool_idx: 0,
                    data_movement: true,
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect_err("a source removed after queue capture must fail closed");
        assert!(matches!(err, Error::ObjectNotFound(_, _) | Error::FileNotFound));
        let target_err = store.pools[1]
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect_err("stale source rejection must not write target metadata");
        assert!(matches!(
            target_err,
            StorageError::ObjectNotFound(_, _) | StorageError::VersionNotFound(_, _, _)
        ));
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn data_movement_put_conflict_validates_only_selected_target_pool() {
        let temp_dir = tempfile::tempdir().expect("create three-pool data movement store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "data-movement-selected-target", &[4, 4, 4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("dm-selected-target-{}", uuid::Uuid::new_v4());
        let object = "object.bin";
        let source_body = b"source-and-equivalent".to_vec();
        let conflicting_body = b"newer-conflicting-target".to_vec();
        let source_mod_time = OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND;
        let version = uuid::Uuid::new_v4();
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create selected-target bucket");
        for (pool_idx, body, mod_time) in [
            (0, source_body.clone(), source_mod_time),
            (1, conflicting_body.clone(), source_mod_time + time::Duration::SECOND),
            (2, source_body.clone(), source_mod_time),
        ] {
            let mut reader = PutObjReader::from_vec(body);
            store.pools[pool_idx]
                .put_object(
                    &bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        versioned: true,
                        version_id: Some(version.to_string()),
                        mod_time: Some(mod_time),
                        ..Default::default()
                    },
                )
                .await
                .expect("seed data movement pool");
        }
        *store.rebalance_meta.write().await = Some(active_rebalance_meta_for_pool(store.pools.len(), 0));

        let source_reader = store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version.to_string()),
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read source for selected-target migration");
        crate::data_movement::migrate_object(store.clone(), 0, bucket.clone(), source_reader, None, "test_selected_target")
            .await
            .expect_err("an equivalent object in another pool must not mask the selected target conflict");

        let mut selected_target = store.pools[1]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("read selected conflicting target");
        let mut selected_body = Vec::new();
        selected_target
            .stream
            .read_to_end(&mut selected_body)
            .await
            .expect("drain selected conflicting target");
        assert_eq!(selected_body, conflicting_body);
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn data_movement_target_remains_readable_before_source_cleanup() {
        let temp_dir = tempfile::tempdir().expect("create data movement read-window store dir");
        let (_ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "data-movement-read-window", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("dm-read-window-{}", uuid::Uuid::new_v4());
        let object = "object.bin";
        let version = uuid::Uuid::new_v4();
        let mod_time = OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND;
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create data movement read-window bucket");

        let mut reader = PutObjReader::from_vec(b"source body".to_vec());
        store.pools[0]
            .put_object(
                &bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version.to_string()),
                    mod_time: Some(mod_time),
                    ..Default::default()
                },
            )
            .await
            .expect("seed data movement source");
        *store.rebalance_meta.write().await = Some(active_rebalance_meta_for_pool(store.pools.len(), 0));

        let source_reader = store.pools[0]
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version.to_string()),
                    raw_data_movement_read: true,
                    ..Default::default()
                },
            )
            .await
            .expect("read data movement source");
        let source_data_dir = source_reader.object_info.data_dir;
        crate::data_movement::migrate_object(store.clone(), 0, bucket.clone(), source_reader, None, "test_read_window")
            .await
            .expect("commit data movement target");

        let target = store.pools[1]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("read committed data movement target");
        assert_ne!(source_data_dir, target.data_dir, "the target must use its own data directory");
        assert_eq!(
            rustfs_utils::http::get_consistent_str(&target.user_defined, rustfs_utils::http::SUFFIX_DATA_MOVED),
            Some("true")
        );

        let resolved = store
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect("the equal-time migration window must remain readable");
        assert_eq!(resolved.data_dir, target.data_dir);

        shutdown.cancel();
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn data_movement_multipart_conflict_validates_exact_version_target_pool() {
        let temp_dir = tempfile::tempdir().expect("create three-pool multipart data movement store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "data-movement-multipart-target", &[4, 4, 4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("dm-multipart-target-{}", uuid::Uuid::new_v4());
        let object = "object.bin";
        let source_version = uuid::Uuid::new_v4();
        let other_version = uuid::Uuid::new_v4();
        let source_mod_time = OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND;
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create multipart selected-target bucket");
        let first_part_size = 5 * 1024 * 1024;
        let mut source_body = vec![b'a'; first_part_size];
        source_body.push(b'b');
        for (pool_idx, version_id, body, mod_time) in [
            (
                1,
                other_version,
                b"newer different version".to_vec(),
                source_mod_time + time::Duration::seconds(2),
            ),
            (2, source_version, b"conflicting exact version".to_vec(), source_mod_time),
        ] {
            let mut reader = PutObjReader::from_vec(body);
            store.pools[pool_idx]
                .put_object(
                    &bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        versioned: true,
                        version_id: Some(version_id.to_string()),
                        mod_time: Some(mod_time),
                        ..Default::default()
                    },
                )
                .await
                .expect("seed multipart data movement pool");
        }
        let source_reader = seed_data_movement_source_reader(
            &store,
            &bucket,
            object,
            DataMovementSourceSeed {
                pool_idx: 0,
                version_id: Some(source_version),
                body: source_body,
                multipart_split: Some(first_part_size),
                mod_time: source_mod_time,
                user_defined: HashMap::new(),
            },
        )
        .await;
        *store.rebalance_meta.write().await = Some(active_rebalance_meta_for_pool(store.pools.len(), 0));

        let completion_barrier = crate::store::multipart::DataMovementMultipartCompletionBarrier::install(&bucket);
        let migration_store = store.clone();
        let migration_bucket = bucket.clone();
        let migration = tokio::spawn(async move {
            crate::data_movement::migrate_object(
                migration_store,
                0,
                migration_bucket,
                source_reader,
                None,
                "test_multipart_selected_target",
            )
            .await
        });
        completion_barrier.wait_until_paused().await;
        let unrelated_set = store.pools[1].get_disks_by_key(object);
        let original_disks = {
            let mut disks = unrelated_set.disks.write().await;
            let original = disks.clone();
            for disk in disks.iter_mut().take(3) {
                *disk = None;
            }
            original
        };
        drop(completion_barrier);
        let err = migration
            .await
            .expect("multipart migration task should join")
            .expect_err("the exact-version target conflict must not be bypassed by a newer different version");
        *unrelated_set.disks.write().await = original_disks;
        let rendered = err.to_string();
        assert!(
            rendered.contains("complete_multipart_upload"),
            "the selected target should be reached despite an unrelated degraded pool: {rendered}"
        );
        assert!(
            !rendered.contains("put_object_part failed"),
            "part upload must not scan the unrelated degraded pool: {rendered}"
        );

        let unexpected_version = store.pools[1]
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    version_id: Some(source_version.to_string()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("multipart migration must not duplicate the source version into the latest-version pool");
        assert!(matches!(
            unexpected_version,
            StorageError::ObjectNotFound(_, _) | StorageError::VersionNotFound(_, _, _)
        ));
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn decommission_durable_ilm_target_read_error_is_not_masked_by_peer_success() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "durable-ilm-target-read-error", &[4, 4, 4]))
                .await;
        let job_id = uuid::Uuid::new_v4();
        let job =
            ManualTransitionJobRecord::new(job_id, "manual-target-read-error", &ManualTransitionRunOptions::default(), "owner");
        let path = manual_transition_job_record_object_name(job_id).expect("manual job path should build");
        let data = job.encode().expect("manual job should encode");
        for pool in &store.pools {
            com::save_config(pool.clone(), &path, data.clone())
                .await
                .expect("manual job fixture should persist in every pool");
        }
        store.pool_meta.write().await.pools[0].decommission = Some(PoolDecommissionInfo {
            start_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        });

        let failing_target = store.pools[2].get_disks_by_key(&path);
        let original_disks = {
            let mut disks = failing_target.disks.write().await;
            let original = disks.clone();
            for disk in disks.iter_mut().take(3) {
                *disk = None;
            }
            original
        };
        let error = store
            .verify_and_cleanup_decommissioned_durable_ilm_record_for_test(0, store.pools[0].get_disks_by_key(&path), &path)
            .await
            .expect_err("one target read-quorum error must fail closed despite another target success")
            .to_string();
        *failing_target.disks.write().await = original_disks;

        assert!(error.contains(&path));
        assert!(error.contains("pool 2"));
        assert_eq!(
            com::read_config(store.pools[0].clone(), &path)
                .await
                .expect("target read error must retain the source"),
            data
        );
        assert_eq!(
            store
                .decommission_durable_ilm_receipt_count_for_test(0)
                .await
                .expect("failed target verification should not create a receipt"),
            0
        );
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn decommission_durable_ilm_terminal_receipt_recovers_failed_source_cleanup() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "durable-ilm-terminal-receipt", &[4, 4])).await;
        let tier_name = "DECOMMISSION-RECEIPT";
        let backend = register_transition_reconcile_test_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let entry = Jentry {
            persisted_version: 0,
            obj_name: "receipt-recovery-object".to_string(),
            version_id: "receipt-recovery-version".to_string(),
            tier_name: tier_name.to_string(),
            backend_identity: Some(backend_identity),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: TierDeleteJournalState::Committed,
            source: None,
            dispatch: None,
        };
        let path = tier_delete_journal_object_name(&entry);
        let data = encode_tier_delete_journal_entry(&entry).expect("tier journal should encode");
        com::save_config(store.pools[0].clone(), &path, data.clone())
            .await
            .expect("source tier journal should persist");
        com::save_config(store.pools[1].clone(), &path, data.clone())
            .await
            .expect("target tier journal should persist");
        let active_pool_meta = {
            let mut pool_meta = store.pool_meta.write().await;
            pool_meta.pools[0].decommission = Some(PoolDecommissionInfo {
                start_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            });
            pool_meta.clone()
        };
        active_pool_meta
            .save(store.pools.clone())
            .await
            .expect("active decommission run identity should persist");

        let source_set = store.pools[0].get_disks_by_key(&path);
        let barrier = SourceCleanupDeleteBarrier::install(RUSTFS_META_BUCKET, &path);
        let cleanup_store = store.clone();
        let cleanup_set = source_set.clone();
        let cleanup_path = path.clone();
        let cleanup = tokio::spawn(async move {
            cleanup_store
                .verify_and_cleanup_decommissioned_durable_ilm_record_for_test(0, cleanup_set, &cleanup_path)
                .await
        });
        barrier.wait_until_paused().await;
        let original_source_disks = {
            let mut disks = source_set.disks.write().await;
            let original = disks.clone();
            for disk in disks.iter_mut().take(3) {
                *disk = None;
            }
            original
        };
        barrier.release();
        let cleanup_error = cleanup
            .await
            .expect("source cleanup task should not panic")
            .expect_err("injected source delete quorum failure must fail cleanup")
            .to_string();
        *source_set.disks.write().await = original_source_disks;
        drop(barrier);

        assert!(cleanup_error.contains("source durable ILM cleanup failed"));
        assert_eq!(
            store
                .decommission_durable_ilm_receipt_count_for_test(0)
                .await
                .expect("receipt should persist before source cleanup"),
            1
        );
        assert_eq!(
            com::read_config(store.pools[0].clone(), &path)
                .await
                .expect("failed cleanup must retain the source"),
            data
        );

        let mut restarted_pool_meta = PoolMeta::default();
        restarted_pool_meta
            .load(store.pools[0].clone(), store.pools.clone())
            .await
            .expect("decommission run identity should reload after restart");
        *store.pool_meta.write().await = restarted_pool_meta;
        let stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("target recovery should commit terminal proof and delete the target");
        assert_eq!((stats.scanned, stats.deleted, stats.failed), (1, 1, 0));
        assert!(matches!(
            com::read_config(store.pools[1].clone(), &path).await,
            Err(Error::ConfigNotFound)
        ));
        assert_eq!(
            com::read_config(store.pools[0].clone(), &path)
                .await
                .expect("target recovery must not delete the decommission source"),
            data
        );

        store
            .verify_and_cleanup_decommissioned_durable_ilm_record_for_test(0, source_set, &path)
            .await
            .expect("terminal receipt should authorize cleanup after target deletion");
        assert!(matches!(
            com::read_config(store.pools[0].clone(), &path).await,
            Err(Error::ConfigNotFound)
        ));
        assert!(backend.remove_versions().await.contains(&(entry.obj_name, entry.version_id)));
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn decommission_final_sweep_blocks_cancel_until_source_cleanup_finishes() {
        let temp_dir = tempfile::tempdir().expect("create final sweep gate store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "durable-ilm-final-sweep-gate", &[4, 4])).await;
        let job_id = uuid::Uuid::new_v4();
        let job = ManualTransitionJobRecord::new(job_id, "final-sweep-gate", &ManualTransitionRunOptions::default(), "owner");
        let path = manual_transition_job_record_object_name(job_id).expect("manual job path should build");
        let data = job.encode().expect("manual job should encode");
        for pool in &store.pools {
            com::save_config(pool.clone(), &path, data.clone())
                .await
                .expect("manual job fixture should persist in both pools");
        }
        let active_pool_meta = {
            let mut pool_meta = store.pool_meta.write().await;
            pool_meta.pools[0].decommission = Some(PoolDecommissionInfo {
                start_time: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            });
            pool_meta.clone()
        };
        active_pool_meta
            .save(store.pools.clone())
            .await
            .expect("active decommission run identity should persist");

        let barrier = SourceCleanupDeleteBarrier::install(RUSTFS_META_BUCKET, &path);
        let final_sweep = tokio::spawn({
            let store = store.clone();
            async move { store.check_after_decommission_for_test(0).await }
        });
        barrier.wait_until_paused().await;

        let mut cancel = tokio::spawn({
            let store = store.clone();
            async move { store.decommission_cancel(0).await }
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut cancel).await.is_err(),
            "cancel must wait for the final sweep source cleanup"
        );
        {
            let pool_meta = store.pool_meta.read().await;
            let decommission = pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("decommission state should remain present");
            assert!(
                decommission.canceled,
                "cancel must publish its durable terminal state before waiting for the final sweep"
            );
            assert!(
                decommission.start_time.is_none(),
                "the durable cancel state must clear the active run identity before quiescence"
            );
        }

        barrier.release();
        final_sweep
            .await
            .expect("final sweep task should not panic")
            .expect("final sweep should finish after the barrier releases");
        cancel
            .await
            .expect("cancel task should not panic")
            .expect("cancel should complete after the final sweep releases the operation gate");
        let pool_meta = store.pool_meta.read().await;
        let decommission = pool_meta.pools[0]
            .decommission
            .as_ref()
            .expect("decommission state should remain present");
        assert!(decommission.canceled);
        assert!(decommission.start_time.is_none());
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn decommission_durable_ilm_recovery_keeps_multiple_active_sources() {
        let temp_dir = tempfile::tempdir().expect("create multi-source recovery store dir");
        let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "durable-ilm-multi-source-recovery",
            &[4, 4, 4],
        ))
        .await;
        let tier_name = "DECOMMISSION-MULTI-SOURCE";
        let backend = register_transition_reconcile_test_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let entry = Jentry {
            persisted_version: 0,
            obj_name: "multi-source-recovery-object".to_string(),
            version_id: "multi-source-recovery-version".to_string(),
            tier_name: tier_name.to_string(),
            backend_identity: Some(backend_identity),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: TierDeleteJournalState::Committed,
            source: None,
            dispatch: None,
        };
        let path = tier_delete_journal_object_name(&entry);
        let data = encode_tier_delete_journal_entry(&entry).expect("tier journal should encode");
        for pool in &store.pools {
            com::save_config(pool.clone(), &path, data.clone())
                .await
                .expect("source and target tier journals should persist");
        }

        let active_pool_meta = {
            let mut pool_meta = store.pool_meta.write().await;
            let start_time = OffsetDateTime::now_utc();
            for pool_idx in [0, 1] {
                pool_meta.pools[pool_idx].decommission = Some(PoolDecommissionInfo {
                    start_time: Some(start_time),
                    ..Default::default()
                });
            }
            pool_meta.clone()
        };
        active_pool_meta
            .save(store.pools.clone())
            .await
            .expect("multiple active decommission runs should persist");
        let mut restarted_pool_meta = PoolMeta::default();
        restarted_pool_meta
            .load(store.pools[0].clone(), store.pools.clone())
            .await
            .expect("multiple active decommission runs should reload");
        *store.pool_meta.write().await = restarted_pool_meta;

        let record = validate_durable_ilm_record(&path, &data).expect("tier journal should validate");
        let source_zero_receipt = store
            .persist_decommission_durable_ilm_receipt_for_test(0, 1, &path, &record, false)
            .await
            .expect("source pool zero receipt should persist on the other active source");
        let source_one_receipt = store
            .persist_decommission_durable_ilm_receipt_for_test(1, 0, &path, &record, false)
            .await
            .expect("source pool one receipt should persist on the other active source");
        assert_ne!(
            source_zero_receipt, source_one_receipt,
            "active source runs must have distinct receipt paths"
        );

        let stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("cross-source receipts should not remove active source journals");
        assert_eq!((stats.scanned, stats.deleted, stats.failed), (1, 1, 0));
        for pool in &store.pools {
            assert_eq!(
                com::read_config(pool.clone(), &path)
                    .await
                    .expect("cross-source receipts alone must retain every journal copy"),
                data
            );
        }

        store
            .persist_decommission_durable_ilm_receipt_for_test(0, 2, &path, &record, false)
            .await
            .expect("source pool zero receipt should persist on the target");
        store
            .persist_decommission_durable_ilm_receipt_for_test(1, 2, &path, &record, false)
            .await
            .expect("source pool one receipt should persist on the target");

        let stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("multi-source tier journal recovery should complete");
        assert_eq!((stats.scanned, stats.deleted, stats.failed), (1, 1, 0));
        assert_eq!(
            com::read_config(store.pools[0].clone(), &path)
                .await
                .expect("first active source must remain after target recovery"),
            data
        );
        assert_eq!(
            com::read_config(store.pools[1].clone(), &path)
                .await
                .expect("second active source must remain after target recovery"),
            data
        );
        assert!(matches!(
            com::read_config(store.pools[2].clone(), &path).await,
            Err(Error::ConfigNotFound)
        ));
        assert!(backend.remove_versions().await.contains(&(entry.obj_name, entry.version_id)));
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn decommission_durable_ilm_receipt_pagination_fails_closed_on_second_page() {
        const RECEIPT_COUNT: usize = 1001;

        let temp_dir = tempfile::tempdir().expect("create paginated receipt store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "durable-ilm-receipt-pages", &[4, 4])).await;
        store.pool_meta.write().await.pools[0].decommission = Some(PoolDecommissionInfo {
            start_time: Some(OffsetDateTime::now_utc()),
            ..Default::default()
        });

        futures::stream::iter(0..RECEIPT_COUNT)
            .map(|index| {
                let store = store.clone();
                async move {
                    let id = format!("{index:064x}");
                    let source_path = format!("ilm/tier-delete-journal/{id}.json");
                    let record = ValidatedDurableIlmRecord {
                        namespace: "tier-delete-journal",
                        id_kind: "operation_id",
                        id,
                        checkpoint: DurableIlmRecordCheckpoint::TierDeleteJournal {
                            content_sha256: format!("{:064x}", index + RECEIPT_COUNT),
                            identity_sha256: "f".repeat(64),
                            committed: false,
                            dispatch_identity_sha256: None,
                            state: None,
                        },
                    };
                    store
                        .persist_decommission_durable_ilm_receipt_for_test(0, 0, &source_path, &record, true)
                        .await?;
                    store
                        .persist_decommission_durable_ilm_receipt_for_test(0, 1, &source_path, &record, true)
                        .await?;
                    Ok::<(), Error>(())
                }
            })
            .buffer_unordered(32)
            .try_collect::<Vec<_>>()
            .await
            .expect("more than one receipt page should persist");
        store
            .persist_decommission_durable_ilm_manifest_for_test(0)
            .await
            .expect("paginated source receipts should produce a manifest");

        let target_receipts = store
            .decommission_durable_ilm_receipt_paths_for_test(0)
            .await
            .expect("paginated target receipts should list");
        assert_eq!(target_receipts.len(), RECEIPT_COUNT);
        let (target_pool_idx, second_page_path) = target_receipts
            .get(1000)
            .cloned()
            .expect("the real 1000-item page boundary should expose a second page receipt");
        let receipt_bytes = com::read_config(store.pools[target_pool_idx].clone(), &second_page_path)
            .await
            .expect("second page receipt should be readable");

        com::delete_config(store.pools[target_pool_idx].clone(), &second_page_path)
            .await
            .expect("second page receipt should delete");
        let missing = store
            .complete_decommission(0)
            .await
            .expect_err("a missing second page receipt must block completion")
            .to_string();
        assert!(missing.contains(&second_page_path));
        assert!(
            !store.pool_meta.read().await.pools[0]
                .decommission
                .as_ref()
                .expect("source pool should remain in decommission")
                .complete
        );
        assert!(com::read_config(store.pools[0].clone(), &second_page_path).await.is_ok());

        let full_tail = ObjectOptions {
            max_parity: true,
            write_completion: crate::object_api::WriteCompletion::TailDrained,
            ..Default::default()
        };
        com::save_config_with_opts(store.pools[target_pool_idx].clone(), &second_page_path, receipt_bytes.clone(), &full_tail)
            .await
            .expect("second page receipt should restore");
        com::save_config_with_opts(store.pools[target_pool_idx].clone(), &second_page_path, b"{corrupt".to_vec(), &full_tail)
            .await
            .expect("second page receipt should corrupt deterministically");
        let corrupt = store
            .complete_decommission(0)
            .await
            .expect_err("a corrupt second page receipt must block completion")
            .to_string();
        assert!(corrupt.contains(&second_page_path));
        assert!(corrupt.contains("invalid"));
        assert!(
            !store.pool_meta.read().await.pools[0]
                .decommission
                .as_ref()
                .expect("source pool should remain in decommission")
                .complete
        );
        assert!(com::read_config(store.pools[0].clone(), &second_page_path).await.is_ok());
    }

    #[cfg(feature = "test-util")]
    #[test]
    #[serial_test::serial(storage_class_env)]
    fn decommission_migrates_and_verifies_registered_durable_ilm_records() {
        std::thread::Builder::new()
            .name("durable-ilm-decommission-test".to_string())
            .stack_size(16 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .build()
                    .expect("durable ILM decommission runtime should build");
                runtime.block_on(decommission_migrates_and_verifies_registered_durable_ilm_records_scenario());
            })
            .expect("durable ILM decommission scenario thread should spawn")
            .join()
            .expect("durable ILM decommission scenario should not panic");
    }

    #[cfg(feature = "test-util")]
    async fn decommission_migrates_and_verifies_registered_durable_ilm_records_scenario() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "durable-ilm-decommission", &[4, 4])).await;

        let tier_name = "DECOMMISSION-ILM";
        let backend = register_transition_reconcile_test_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let tier_entry = Jentry {
            persisted_version: 0,
            obj_name: "decommissioned-remote-object".to_string(),
            version_id: "decommissioned-remote-version".to_string(),
            tier_name: tier_name.to_string(),
            backend_identity: Some(backend_identity),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: TierDeleteJournalState::Committed,
            source: None,
            dispatch: None,
        };
        let tier_path = tier_delete_journal_object_name(&tier_entry);
        let tier_bytes = encode_tier_delete_journal_entry(&tier_entry).expect("tier journal should encode");

        let mut transaction = TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
            transaction_id: uuid::Uuid::new_v4(),
            owner_epoch: uuid::Uuid::new_v4(),
            write_id: uuid::Uuid::new_v4(),
            source: TransitionSourceIdentity {
                bucket: "source-bucket".to_string(),
                object: "source-object".to_string(),
                version_id: Some(uuid::Uuid::new_v4()),
                data_dir: uuid::Uuid::new_v4(),
                mod_time_unix_nanos: 1_770_000_000_000_000_000,
                size: 42,
                etag: "source-etag".to_string(),
                version_mode: TransitionSourceVersionMode::Versioned,
            },
            tier_name: tier_name.to_string(),
            backend_fingerprint: backend_identity,
            not_after_unix_nanos: 1_780_000_000_000_000_000,
        })
        .expect("transition transaction should build");
        let regressed_transaction = transaction.clone();
        transaction
            .advance(transaction.fence(), TransitionTransactionState::UploadOutcomeUnknown, None)
            .expect("transition transaction should advance before migration");
        let transaction_path = transition_transaction_record_object_name(transaction.transaction_id)
            .expect("transition transaction path should build");
        let transaction_bytes = transaction.encode().expect("transition transaction should encode");

        let manual_job_id = uuid::Uuid::new_v4();
        let manual_bucket = format!("manual-decommission-{}", manual_job_id.simple());
        let manual_options = ManualTransitionRunOptions {
            prefix: "logs/".to_string(),
            tier: Some(tier_name.to_string()),
            ..Default::default()
        };
        let mut manual_job = ManualTransitionJobRecord::new(manual_job_id, &manual_bucket, &manual_options, "old-owner");
        manual_job.scan_completed = true;
        manual_job.report.enqueued = 1;
        manual_job.lease_expires_at_unix_nanos = 0;
        let manual_scope = ManualTransitionScopeAdmission::from_job(&manual_job);
        let task_key = manual_transition_worker_result_task_key(&manual_bucket, "logs/a", None);
        let manual_task = ManualTransitionTaskRecord::new(manual_job_id, &task_key, &manual_bucket, "logs/a", None, tier_name);
        let manual_result =
            ManualTransitionWorkerResultRecord::new(manual_job_id, &task_key, ManualTransitionWorkerResult::Completed);

        let manual_job_path = manual_transition_job_record_object_name(manual_job_id).expect("manual job path should build");
        let manual_scope_path =
            manual_transition_scope_record_object_name(&manual_scope.scope_key).expect("manual scope path should build");
        let manual_task_path =
            manual_transition_task_object_name(manual_job_id, &task_key).expect("manual task path should build");
        let manual_result_path = manual_transition_worker_result_object_name(manual_job_id, &task_key)
            .expect("manual worker result path should build");
        let manual_job_bytes = manual_job.encode().expect("manual job should encode");
        let manual_scope_bytes = serde_json::to_vec(&manual_scope).expect("manual scope should encode");
        let manual_task_bytes = manual_task.encode().expect("manual task should encode");
        let manual_result_bytes = manual_result.encode().expect("manual result should encode");

        let records = vec![
            (tier_path.clone(), tier_bytes.clone()),
            (transaction_path.clone(), transaction_bytes.clone()),
            (manual_job_path.clone(), manual_job_bytes.clone()),
            (manual_scope_path.clone(), manual_scope_bytes.clone()),
            (manual_task_path.clone(), manual_task_bytes.clone()),
            (manual_result_path.clone(), manual_result_bytes.clone()),
        ];
        for (path, data) in &records {
            com::save_config(store.pools[0].clone(), path, data.clone())
                .await
                .expect("durable ILM source record should persist");
        }

        let legacy_queue = [com::CONFIG_PREFIX, BUCKET_META_PREFIX]
            .into_iter()
            .map(|prefix| {
                DecomBucketInfo {
                    name: RUSTFS_META_BUCKET.to_string(),
                    prefix: prefix.to_string(),
                }
                .to_string()
            })
            .collect();
        let legacy_pool_meta = {
            let mut pool_meta = store.pool_meta.write().await;
            pool_meta.pools[0].decommission = Some(PoolDecommissionInfo {
                queued: true,
                queued_buckets: legacy_queue,
                ..Default::default()
            });
            pool_meta.clone()
        };
        legacy_pool_meta
            .save(store.pools.clone())
            .await
            .expect("legacy decommission queue should persist before restart");
        let mut restarted_pool_meta = PoolMeta::default();
        restarted_pool_meta
            .load(store.pools[0].clone(), store.pools.clone())
            .await
            .expect("legacy decommission queue should reload after restart");
        *store.pool_meta.write().await = restarted_pool_meta;
        set_test_decommission_capacity_override(&store, 0);
        store
            .promote_queued_decommission_for_test(0)
            .await
            .expect("legacy queued decommission should resume");
        let expected_ilm_queue = DecomBucketInfo {
            name: RUSTFS_META_BUCKET.to_string(),
            prefix: ILM_META_PREFIX.to_string(),
        }
        .to_string();
        {
            let pool_meta = store.pool_meta.read().await;
            let decommission = pool_meta.pools[0]
                .decommission
                .as_ref()
                .expect("decommission state should remain present");
            assert!(!decommission.queued);
            assert!(decommission.queued_buckets.contains(&expected_ilm_queue));
        }

        let ilm_bucket = DecomBucketInfo {
            name: RUSTFS_META_BUCKET.to_string(),
            prefix: ILM_META_PREFIX.to_string(),
        };
        for _ in 0..2 {
            store
                .decommission_pool_for_test(CancellationToken::new(), 0, store.pools[0].clone(), ilm_bucket.clone())
                .await
                .expect("durable ILM decommission should be idempotent");
        }
        for (path, expected) in &records {
            assert_eq!(
                com::read_config(store.pools[0].clone(), path)
                    .await
                    .expect("source should remain until the final sweep"),
                *expected
            );
            assert_eq!(
                com::read_config(store.pools[1].clone(), path)
                    .await
                    .expect("target should contain the migrated record"),
                *expected
            );
        }
        assert_eq!(
            store
                .decommission_durable_ilm_receipt_count_for_test(0)
                .await
                .expect("no receipt should exist before the final sweep"),
            0
        );
        let isolated_tier_stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("tier recovery should retain a terminal record until its receipt is committed");
        assert!(isolated_tier_stats.scanned >= 1);
        assert_eq!(isolated_tier_stats.deleted, 0);
        assert!(isolated_tier_stats.failed >= 1);
        assert_eq!(
            com::read_config(store.pools[1].clone(), &tier_path)
                .await
                .expect("receipt isolation must retain the target tier journal"),
            tier_bytes
        );

        com::delete_config(store.pools[1].clone(), &manual_job_path)
            .await
            .expect("target manual job should delete");
        let missing = store
            .verify_and_cleanup_decommissioned_durable_ilm_record_for_test(
                0,
                store.pools[0].get_disks_by_key(&manual_job_path),
                &manual_job_path,
            )
            .await
            .expect_err("missing target must block source cleanup");
        let missing = missing.to_string();
        assert!(missing.contains(&manual_job_path) && missing.contains(&manual_job_id.to_string()));
        assert_eq!(
            com::read_config(store.pools[0].clone(), &manual_job_path)
                .await
                .expect("missing target must retain source"),
            manual_job_bytes
        );
        com::save_config(store.pools[1].clone(), &manual_job_path, manual_job_bytes.clone())
            .await
            .expect("target manual job should restore");

        com::save_config(store.pools[1].clone(), &transaction_path, b"{corrupt".to_vec())
            .await
            .expect("target transaction should corrupt deterministically");
        let corrupt = store
            .verify_and_cleanup_decommissioned_durable_ilm_record_for_test(
                0,
                store.pools[0].get_disks_by_key(&transaction_path),
                &transaction_path,
            )
            .await
            .expect_err("corrupt target must block source cleanup");
        let corrupt = corrupt.to_string();
        assert!(corrupt.contains(&transaction_path) && corrupt.contains(&transaction.transaction_id.to_string()));
        assert_eq!(
            com::read_config(store.pools[0].clone(), &transaction_path)
                .await
                .expect("corrupt target must retain source"),
            transaction_bytes
        );
        com::save_config(store.pools[1].clone(), &transaction_path, transaction_bytes.clone())
            .await
            .expect("target transaction should restore");

        com::save_config(store.pools[1].clone(), &manual_scope_path, manual_scope_bytes.clone())
            .await
            .expect("target scope rewrite should invalidate cached metadata before the quorum check");
        let target_scope_set = store.pools[1].get_disks_by_key(&manual_scope_path);
        let original_target_scope_disks = {
            let mut disks = target_scope_set.disks.write().await;
            let original = disks.clone();
            for disk in disks.iter_mut().take(3) {
                *disk = None;
            }
            original
        };
        let quorum_error = store
            .verify_and_cleanup_decommissioned_durable_ilm_record_for_test(
                0,
                store.pools[0].get_disks_by_key(&manual_scope_path),
                &manual_scope_path,
            )
            .await
            .expect_err("target below read quorum must block source cleanup");
        *target_scope_set.disks.write().await = original_target_scope_disks;
        let quorum_error = quorum_error.to_string();
        assert!(quorum_error.contains(&manual_scope_path) && quorum_error.contains(&manual_job_id.to_string()));
        assert!(com::read_config(store.pools[0].clone(), &manual_scope_path).await.is_ok());

        com::save_config(store.pools[1].clone(), &manual_task_path, manual_task_bytes.clone())
            .await
            .expect("target task rewrite should invalidate cached metadata before the quorum check");
        let target_task_set = store.pools[1].get_disks_by_key(&manual_task_path);
        let original_target_task_disks = {
            let mut disks = target_task_set.disks.write().await;
            let original = disks.clone();
            for disk in disks.iter_mut().take(2) {
                *disk = None;
            }
            original
        };
        let receipt_quorum_error = store
            .verify_and_cleanup_decommissioned_durable_ilm_record_for_test(
                0,
                store.pools[0].get_disks_by_key(&manual_task_path),
                &manual_task_path,
            )
            .await
            .expect_err("target read quorum without receipt write quorum must retain the source");
        *target_task_set.disks.write().await = original_target_task_disks;
        let receipt_quorum_error = receipt_quorum_error.to_string();
        assert!(receipt_quorum_error.contains("receipt"));
        assert!(receipt_quorum_error.contains(&manual_task_path));
        assert!(receipt_quorum_error.contains(&manual_job_id.to_string()));
        assert!(com::read_config(store.pools[0].clone(), &manual_task_path).await.is_ok());
        store
            .verify_and_cleanup_decommissioned_durable_ilm_record_for_test(
                0,
                store.pools[0].get_disks_by_key(&manual_task_path),
                &manual_task_path,
            )
            .await
            .expect("healthy target should persist the receipt before source cleanup");

        let unknown_path = "ilm/future-durable/jobs/one.json";
        com::save_config(store.pools[0].clone(), unknown_path, b"{}".to_vec())
            .await
            .expect("unknown durable ILM record should persist for the guard test");
        let unknown_migration = store
            .decommission_pool_for_test(CancellationToken::new(), 0, store.pools[0].clone(), ilm_bucket)
            .await
            .expect_err("unregistered durable ILM namespace must block migration");
        assert!(unknown_migration.to_string().contains(unknown_path));
        let unknown_final_sweep = store
            .check_after_decommission_for_test(0)
            .await
            .expect_err("unregistered durable ILM namespace must block completion");
        assert!(unknown_final_sweep.to_string().contains(unknown_path));
        com::delete_config(store.pools[0].clone(), unknown_path)
            .await
            .expect("unknown guard fixture should be removed before the successful final sweep");

        store
            .check_after_decommission_for_test(0)
            .await
            .expect("production final sweep should validate every target before cleanup");
        assert_eq!(
            store
                .decommission_durable_ilm_receipt_count_for_test(0)
                .await
                .expect("durable ILM receipts should be listable"),
            records.len(),
            "every cleaned source record must have a durable validation receipt"
        );
        for (path, expected) in &records {
            assert!(
                matches!(com::read_config(store.pools[0].clone(), path).await, Err(Error::ConfigNotFound)),
                "final sweep should remove the validated source `{path}`"
            );
            assert_eq!(
                com::read_config(store.pools[1].clone(), path)
                    .await
                    .expect("final sweep must preserve the target"),
                *expected
            );
        }

        let mut crash_restarted_pool_meta = PoolMeta::default();
        crash_restarted_pool_meta
            .load(store.pools[0].clone(), store.pools.clone())
            .await
            .expect("pool metadata should reload after the simulated pre-complete crash");
        *store.pool_meta.write().await = crash_restarted_pool_meta;

        let (manual_job_receipt_pool, manual_job_receipt_path) = store
            .decommission_durable_ilm_receipt_paths_for_test(0)
            .await
            .expect("durable ILM receipt paths should be listable")
            .into_iter()
            .find(|(_, path)| path.contains(&manual_job_path))
            .expect("manual job should have one target receipt");
        let manual_job_receipt_bytes = com::read_config(store.pools[manual_job_receipt_pool].clone(), &manual_job_receipt_path)
            .await
            .expect("manual job receipt should be readable before deletion");
        com::delete_config(store.pools[manual_job_receipt_pool].clone(), &manual_job_receipt_path)
            .await
            .expect("manual job receipt should delete after source cleanup");
        com::delete_config(store.pools[1].clone(), &manual_job_path)
            .await
            .expect("post-crash target manual job should delete");
        let missing_after_crash = store
            .complete_decommission(0)
            .await
            .expect_err("completion must reject a missing target after source cleanup and restart")
            .to_string();
        assert!(missing_after_crash.contains("receipt"));
        assert!(missing_after_crash.contains(&manual_job_path));
        assert!(missing_after_crash.contains(&manual_job_id.to_string()));
        assert!(
            !store.pool_meta.read().await.pools[0]
                .decommission
                .as_ref()
                .expect("decommission state should survive restart")
                .complete
        );
        com::save_config(store.pools[1].clone(), &manual_job_path, manual_job_bytes.clone())
            .await
            .expect("post-crash target manual job should restore");
        com::save_config(
            store.pools[manual_job_receipt_pool].clone(),
            &manual_job_receipt_path,
            manual_job_receipt_bytes,
        )
        .await
        .expect("manual job receipt should restore after the missing-receipt check");

        com::save_config(store.pools[1].clone(), &transaction_path, b"{corrupt".to_vec())
            .await
            .expect("post-crash target transaction should corrupt deterministically");
        let corrupt_after_crash = store
            .complete_decommission(0)
            .await
            .expect_err("completion must reject a corrupt target after source cleanup and restart")
            .to_string();
        assert!(corrupt_after_crash.contains(&transaction_path));
        assert!(corrupt_after_crash.contains(&transaction.transaction_id.to_string()));
        com::save_config(store.pools[1].clone(), &transaction_path, transaction_bytes.clone())
            .await
            .expect("post-crash target transaction should restore");

        let mut wrong_manual_job = manual_job.clone();
        wrong_manual_job.job_id = uuid::Uuid::new_v4();
        com::save_config(
            store.pools[1].clone(),
            &manual_job_path,
            wrong_manual_job.encode().expect("wrong-id job should encode"),
        )
        .await
        .expect("post-crash target manual job should accept the wrong-id fixture");
        let wrong_id_after_crash = store
            .complete_decommission(0)
            .await
            .expect_err("completion must reject a target record with the wrong id")
            .to_string();
        assert!(wrong_id_after_crash.contains(&manual_job_path));
        assert!(wrong_id_after_crash.contains(&manual_job_id.to_string()));
        com::save_config(store.pools[1].clone(), &manual_job_path, manual_job_bytes.clone())
            .await
            .expect("post-crash target manual job should restore after the wrong-id check");

        com::save_config(
            store.pools[1].clone(),
            &transaction_path,
            regressed_transaction
                .encode()
                .expect("regressed transition transaction should encode"),
        )
        .await
        .expect("post-crash target transaction should accept the regression fixture");
        let regression_after_crash = store
            .complete_decommission(0)
            .await
            .expect_err("completion must reject a lower transition transaction revision")
            .to_string();
        assert!(regression_after_crash.contains("generation mismatch"));
        assert!(regression_after_crash.contains(&transaction_path));
        assert!(regression_after_crash.contains(&transaction.transaction_id.to_string()));
        com::save_config(store.pools[1].clone(), &transaction_path, transaction_bytes.clone())
            .await
            .expect("post-crash target transaction should restore after the regression check");

        let (manual_task_receipt_pool, manual_task_receipt_path) = store
            .decommission_durable_ilm_receipt_paths_for_test(0)
            .await
            .expect("durable ILM receipt paths should be listable")
            .into_iter()
            .find(|(_, path)| path.contains(&manual_task_path))
            .expect("manual task receipt should retain its reversible source path");
        let manual_task_receipt_bytes =
            com::read_config(store.pools[manual_task_receipt_pool].clone(), &manual_task_receipt_path)
                .await
                .expect("manual task receipt should be readable before corruption");
        com::save_config(
            store.pools[manual_task_receipt_pool].clone(),
            &manual_task_receipt_path,
            b"{corrupt".to_vec(),
        )
        .await
        .expect("manual task receipt should corrupt deterministically");
        let corrupt_receipt = store
            .complete_decommission(0)
            .await
            .expect_err("completion must fail closed on a corrupt receipt")
            .to_string();
        assert!(corrupt_receipt.contains(&manual_task_path));
        assert!(corrupt_receipt.contains(&manual_job_id.to_string()));
        com::save_config(
            store.pools[manual_task_receipt_pool].clone(),
            &manual_task_receipt_path,
            manual_task_receipt_bytes,
        )
        .await
        .expect("manual task receipt should restore after the corruption check");

        let tier_stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("tier journal recovery should consume the migrated record before completion");
        assert_eq!((tier_stats.scanned, tier_stats.deleted, tier_stats.failed), (1, 1, 0));
        assert!(matches!(com::read_config(store.clone(), &tier_path).await, Err(Error::ConfigNotFound)));

        let recovered_transition_version = "recovered-transition-version".to_string();
        backend
            .set_transition_candidate_probe_override(Some(TransitionCandidateProbe::VersionedPresent(
                recovered_transition_version.clone(),
            )))
            .await;
        let transaction_stats = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("transition recovery should advance and consume the migrated transaction before completion");
        backend.set_transition_candidate_probe_override(None).await;
        assert_eq!(
            (
                transaction_stats.scanned,
                transaction_stats.recovered,
                transaction_stats.retained,
                transaction_stats.failed,
            ),
            (1, 1, 0, 0)
        );
        assert!(matches!(
            com::read_config(store.clone(), &transaction_path).await,
            Err(Error::ConfigNotFound)
        ));
        com::save_config(
            store.pools[1].clone(),
            &transaction_path,
            regressed_transaction
                .encode()
                .expect("post-terminal transition rollback should encode"),
        )
        .await
        .expect("target should accept the post-terminal rollback fixture");
        let post_terminal_regression = store
            .complete_decommission(0)
            .await
            .expect_err("terminal proof must not mask a lower transition revision")
            .to_string();
        assert!(post_terminal_regression.contains("generation mismatch"));
        assert!(post_terminal_regression.contains(&transaction_path));
        assert!(post_terminal_regression.contains(&transaction.transaction_id.to_string()));
        com::delete_config(store.pools[1].clone(), &transaction_path)
            .await
            .expect("post-terminal rollback fixture should be removed");

        let manual_stats = recover_manual_transition_jobs_once(store.clone(), 100, None)
            .await
            .expect("manual recovery should advance the migrated job and consume its scope before completion");
        assert_eq!(
            (manual_stats.scanned, manual_stats.resumed, manual_stats.skipped, manual_stats.failed,),
            (1, 1, 0, 0)
        );
        assert!(matches!(
            com::read_config(store.clone(), &manual_scope_path).await,
            Err(Error::ConfigNotFound)
        ));
        let recovered_manual_job_bytes = com::read_config(store.pools[1].clone(), &manual_job_path)
            .await
            .expect("manual recovery should retain the advanced job record");
        assert_ne!(recovered_manual_job_bytes, manual_job_bytes);

        com::save_config(store.pools[1].clone(), &manual_job_path, manual_job_bytes.clone())
            .await
            .expect("target manual job should accept the rollback fixture");
        let manual_regression = store
            .complete_decommission(0)
            .await
            .expect_err("completion must reject a manual job generation rollback")
            .to_string();
        assert!(manual_regression.contains("generation mismatch"));
        assert!(manual_regression.contains(&manual_job_path));
        assert!(manual_regression.contains(&manual_job_id.to_string()));
        com::save_config(store.pools[1].clone(), &manual_job_path, recovered_manual_job_bytes)
            .await
            .expect("target manual job should restore its recovered generation");

        store
            .complete_decommission(0)
            .await
            .expect("completion should persist before receipt cleanup");
        assert!(
            store.pool_meta.read().await.pools[0]
                .decommission
                .as_ref()
                .expect("completed decommission state should remain present")
                .complete
        );
        assert_eq!(
            store
                .decommission_durable_ilm_receipt_count_for_test(0)
                .await
                .expect("receipt cleanup should be observable"),
            0
        );
        store
            .cleanup_decommission_durable_ilm_receipts_for_test(0)
            .await
            .expect("receipt cleanup should be idempotent");
        let removed_versions = backend.remove_versions().await;
        assert!(removed_versions.contains(&(tier_entry.obj_name.clone(), tier_entry.version_id.clone())));
        assert!(removed_versions.contains(&(transaction.remote_object.clone(), recovered_transition_version)));
    }

    #[cfg(feature = "test-util")]
    async fn tier_delete_journal_count(store: Arc<crate::store::ECStore>) -> usize {
        store
            .list_objects_v2(RUSTFS_META_BUCKET, TIER_DELETE_JOURNAL_PREFIX, None, None, 100, false, None, false)
            .await
            .expect("tier delete journal should be listable")
            .objects
            .len()
    }

    #[cfg(feature = "test-util")]
    async fn tier_delete_dispatch_manifest_count(store: Arc<crate::store::ECStore>) -> usize {
        store
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                TIER_DELETE_DISPATCH_MANIFEST_PREFIX,
                None,
                None,
                100,
                false,
                None,
                false,
            )
            .await
            .expect("tier delete dispatch manifests should be listable")
            .objects
            .len()
    }

    #[cfg(feature = "test-util")]
    async fn install_committed_tier_delete_journals(
        store: Arc<crate::store::ECStore>,
        tier_name: &str,
        backend_identity: [u8; 32],
        count: usize,
    ) -> Vec<Jentry> {
        let mut entries = Vec::with_capacity(count);
        for index in 0..count {
            let entry = Jentry {
                persisted_version: 0,
                obj_name: format!("remote/pass-drain-{index:06}.bin"),
                version_id: uuid::Uuid::new_v4().to_string(),
                tier_name: tier_name.to_string(),
                backend_identity: Some(backend_identity),
                version_id_exact: true,
                version_state: rustfs_filemeta::TransitionVersionState::Exact,
                state: TierDeleteJournalState::Committed,
                source: None,
                dispatch: None,
            };
            persist_tier_delete_journal_entry(store.clone(), &entry)
                .await
                .expect("committed tier-delete journal fixture should persist");
            entries.push(entry);
        }
        entries
    }

    #[cfg(feature = "test-util")]
    fn synthetic_v6_dispatch_entry(
        bucket: &str,
        object: &str,
        tier_name: &str,
        backend_identity: [u8; 32],
        remote_version: &str,
    ) -> Jentry {
        Jentry {
            persisted_version: 0,
            obj_name: format!("remote/{object}"),
            version_id: remote_version.to_string(),
            tier_name: tier_name.to_string(),
            backend_identity: Some(backend_identity),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: TierDeleteJournalState::Prepared,
            source: Some(TierDeleteSourceIdentity {
                bucket: bucket.to_string(),
                object: object.to_string(),
                version_id: Some(uuid::Uuid::new_v4().to_string()),
                versioned: true,
                version_suspended: false,
                data_dir: Some(uuid::Uuid::new_v4().to_string()),
                etag: Some(format!("etag-{object}")),
                mod_time: Some(OffsetDateTime::UNIX_EPOCH.to_string()),
            }),
            dispatch: None,
        }
    }

    #[cfg(feature = "test-util")]
    async fn install_aborting_dispatch_fixture(
        store: Arc<crate::store::ECStore>,
        bucket: &str,
        incarnation: uuid::Uuid,
        prefix: &str,
        tier_name: &str,
        backend_identity: [u8; 32],
        journal_count: usize,
    ) -> (String, Vec<Jentry>) {
        let entries = (0..journal_count)
            .map(|index| {
                (
                    synthetic_v6_dispatch_entry(
                        bucket,
                        &format!("{prefix}{index:06}.bin"),
                        tier_name,
                        backend_identity,
                        &uuid::Uuid::new_v4().to_string(),
                    ),
                    Some(TierDeleteJournalState::Prepared),
                )
            })
            .collect();
        install_test_tier_delete_dispatch_fixture(
            store,
            bucket,
            incarnation,
            prefix,
            entries,
            TierDeleteDispatchManifestState::Aborting,
        )
        .await
        .expect("Aborting rollback fixture should persist")
    }

    #[cfg(feature = "test-util")]
    async fn drive_tier_delete_dispatch_restart_to_convergence(store: Arc<crate::store::ECStore>) {
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut stable_empty_observations = 0;
            loop {
                recover_tier_delete_dispatch_manifests(store.clone(), 100, None)
                    .await
                    .expect("restarted manifest recovery pass should finish");
                recover_tier_delete_journal_entries(store.clone(), 100, None)
                    .await
                    .expect("restarted journal recovery pass should finish");
                if tier_delete_journal_count(store.clone()).await == 0
                    && tier_delete_dispatch_manifest_count(store.clone()).await == 0
                    && tier_delete_dispatch_manifest_recovery_count_for_test(&store) == 0
                {
                    stable_empty_observations += 1;
                    if stable_empty_observations == 10 {
                        break;
                    }
                } else {
                    stable_empty_observations = 0;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("same-disk restart recovery should converge without retained dispatch state");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_delete_recovery_pass_drains_multiple_fast_manifest_pages() {
        const MANIFEST_COUNT: usize = 10;

        let temp_dir = tempfile::tempdir().expect("create fast manifest pass recovery store dir");
        let mut instance_ctx = crate::runtime::instance::InstanceContext::new();
        instance_ctx.suppress_tier_delete_journal_recovery_for_test();
        let (ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store_with_layout(
            temp_dir.path(),
            "tier-delete-fast-manifest-pass",
            &[(1, 4)],
            CancellationToken::new(),
            Some(Arc::new(instance_ctx)),
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "FAST-MANIFEST-PASS";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("fast manifest pass tier lease should resolve")
            .backend_identity();
        for index in 0..MANIFEST_COUNT {
            // Pagination must not depend on same-bucket lock wait deadlines.
            let bucket = format!("tier-delete-fast-manifest-pass-{index}");
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("fast manifest pass bucket should be created");
            let incarnation = store
                .bucket_incarnation_id(&bucket)
                .await
                .expect("fast manifest pass bucket incarnation should resolve");
            install_aborting_dispatch_fixture(
                store.clone(),
                &bucket,
                incarnation,
                &format!("manifest-page-{index:06}/"),
                tier_name,
                backend_identity,
                1,
            )
            .await;
        }
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, MANIFEST_COUNT);
        assert_eq!(tier_delete_journal_count(store.clone()).await, MANIFEST_COUNT);

        let cancel = CancellationToken::new();
        let mut manifest_marker = None;
        let mut journal_marker = None;
        let stats = recover_test_tier_delete_journal_pass_with_budget(
            store.clone(),
            &cancel,
            &mut manifest_marker,
            &mut journal_marker,
            Duration::from_secs(30),
        )
        .await;

        assert!(!stats.canceled);
        assert!(!stats.deadline_exhausted);
        assert!(
            stats.manifest_pages > 1,
            "one production pass must cross the default eight-manifest page limit"
        );
        assert_eq!(stats.manifests.scanned, MANIFEST_COUNT);
        assert_eq!(stats.manifests.deleted, MANIFEST_COUNT, "full recovery result: {stats:?}");
        assert_eq!(stats.manifests.failed, 0, "full recovery result: {stats:?}");
        assert_eq!(manifest_marker, None);
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, 0);
        assert_eq!(tier_delete_journal_count(store).await, 0);
        assert_eq!(backend.remove_count().await, 0, "rollback recovery must not call the remote tier");
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_delete_manual_pass_retains_manifest_owned_by_startup_recovery() {
        let temp_dir = tempfile::tempdir().expect("create automatic recovery ownership store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-delete-auto-owner", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "tier-delete-auto-owner-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("automatic recovery bucket should be created");
        let incarnation = store.bucket_incarnation_id(bucket).await.expect("bucket incarnation");
        let tier_name = "AUTO-OWNER";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("automatic recovery tier lease")
            .backend_identity();

        // The automatic worker must not observe a partially installed fixture.
        let lifecycle_guard = store
            .acquire_bucket_lifecycle_write_lock(bucket)
            .await
            .expect("fixture lifecycle lock");
        let (manifest_name, entries) =
            install_aborting_dispatch_fixture(store.clone(), bucket, incarnation, "auto-owner/", tier_name, identity, 1).await;
        let journal_name = tier_delete_journal_object_name(&entries[0]);
        let hook = TierDeleteDispatchRollbackTestHook::install_slow_delete(&journal_name, &journal_name);
        drop(lifecycle_guard);
        ctx.wake_tier_delete_journal_recovery();
        tokio::time::timeout(Duration::from_secs(30), hook.wait_until_delete_paused())
            .await
            .expect("startup recovery should own the manifest before a manual pass");
        assert!(tier_delete_dispatch_manifest_recovery_inflight_for_test(&store, &manifest_name));

        let stats = recover_tier_delete_dispatch_manifests(store.clone(), 8, None)
            .await
            .expect("manual recovery scan");
        assert_eq!(stats.scanned, 1, "{stats:?}");
        assert_eq!(stats.retained, 1, "{stats:?}");
        assert_eq!(stats.deleted, 0, "{stats:?}");
        assert_eq!(stats.failed, 0, "{stats:?}");
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, 1);
        assert_eq!(tier_delete_journal_count(store.clone()).await, 1);

        hook.release_delete();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let manifest_gone = matches!(com::read_config(store.clone(), &manifest_name).await, Err(Error::ConfigNotFound));
                if manifest_gone && !tier_delete_dispatch_manifest_recovery_inflight_for_test(&store, &manifest_name) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("automatic recovery should converge without a manual retry");
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, 0);
        assert_eq!(tier_delete_journal_count(store).await, 0);
        assert_eq!(backend.remove_count().await, 0, "rollback must not delete from the remote tier");
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_delete_recovery_pass_drains_multiple_fast_journal_pages() {
        const JOURNAL_COUNT: usize = 25;

        let temp_dir = tempfile::tempdir().expect("create fast pass recovery store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-delete-fast-pass", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "FAST-PASS-RECOVERY";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("fast pass tier lease should resolve")
            .backend_identity();
        let entries = install_committed_tier_delete_journals(store.clone(), tier_name, backend_identity, JOURNAL_COUNT).await;
        assert_eq!(tier_delete_journal_count(store.clone()).await, JOURNAL_COUNT);

        let cancel = CancellationToken::new();
        let mut manifest_marker = None;
        let mut journal_marker = None;
        let stats = recover_test_tier_delete_journal_pass_with_budget(
            store.clone(),
            &cancel,
            &mut manifest_marker,
            &mut journal_marker,
            Duration::from_secs(30),
        )
        .await;

        assert!(!stats.canceled);
        assert!(!stats.deadline_exhausted);
        assert!(
            stats.journal_pages > 1,
            "one production pass must cross the default eight-record page limit"
        );
        assert_eq!(stats.journals.scanned, JOURNAL_COUNT);
        assert_eq!(stats.journals.deleted, JOURNAL_COUNT);
        assert_eq!(stats.journals.failed, 0);
        assert_eq!(journal_marker, None);
        assert_eq!(tier_delete_journal_count(store).await, 0);
        assert_eq!(backend.exact_remove_count(), JOURNAL_COUNT);
        let mut removed = backend.remove_versions().await;
        removed.sort();
        let mut expected = entries
            .into_iter()
            .map(|entry| (entry.obj_name, entry.version_id))
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(removed, expected);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_delete_recovery_pass_rotates_failed_journal_pages_without_busy_loop() {
        const JOURNAL_COUNT: usize = 9;

        let temp_dir = tempfile::tempdir().expect("create failed pass recovery store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-delete-failed-pass", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "FAILED-PASS-RECOVERY";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("failed pass tier lease should resolve")
            .backend_identity();
        install_committed_tier_delete_journals(store.clone(), tier_name, backend_identity, JOURNAL_COUNT).await;
        backend.set_remove_failure(true);

        let cancel = CancellationToken::new();
        let mut manifest_marker = None;
        let mut journal_marker = None;
        let first = recover_test_tier_delete_journal_pass_with_budget(
            store.clone(),
            &cancel,
            &mut manifest_marker,
            &mut journal_marker,
            Duration::from_secs(30),
        )
        .await;
        assert_eq!(first.journal_pages, 1, "a no-progress failed page must stop the pass");
        assert_eq!(first.journals.scanned, 8);
        assert_eq!(first.journals.deleted, 0);
        assert_eq!(first.journals.failed, 8);
        assert!(journal_marker.is_some(), "a truncated failed page must retain its continuation marker");
        assert_eq!(tier_delete_journal_count(store.clone()).await, JOURNAL_COUNT);

        let second = recover_test_tier_delete_journal_pass_with_budget(
            store.clone(),
            &cancel,
            &mut manifest_marker,
            &mut journal_marker,
            Duration::from_secs(30),
        )
        .await;
        assert_eq!(second.journal_pages, 1);
        assert_eq!(second.journals.scanned, 1);
        assert_eq!(second.journals.deleted, 0);
        assert_eq!(second.journals.failed, 1);
        assert_eq!(
            journal_marker, None,
            "end-of-list rotation must revisit the failed prefix on a later pass"
        );
        assert_eq!(tier_delete_journal_count(store.clone()).await, JOURNAL_COUNT);

        backend.set_remove_failure(false);
        let third = recover_test_tier_delete_journal_pass_with_budget(
            store.clone(),
            &cancel,
            &mut manifest_marker,
            &mut journal_marker,
            Duration::from_secs(30),
        )
        .await;
        assert_eq!(third.journals.scanned, JOURNAL_COUNT);
        assert_eq!(third.journals.deleted, JOURNAL_COUNT);
        assert_eq!(third.journals.failed, 0);
        assert_eq!(tier_delete_journal_count(store).await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_delete_journal_rejects_unbound_new_prepare_without_persisting() {
        let temp_dir = tempfile::tempdir().expect("create journal admission store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-journal-unbound-prepare", &[4])).await;
        let entry = Jentry {
            persisted_version: 0,
            obj_name: "remote/object".to_string(),
            version_id: uuid::Uuid::new_v4().to_string(),
            tier_name: "COLD-A".to_string(),
            backend_identity: Some([7; 32]),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: TierDeleteJournalState::Prepared,
            source: Some(TierDeleteSourceIdentity {
                bucket: "source-bucket".to_string(),
                object: "source-object".to_string(),
                version_id: Some(uuid::Uuid::new_v4().to_string()),
                versioned: true,
                version_suspended: false,
                data_dir: Some(uuid::Uuid::new_v4().to_string()),
                etag: Some("source-etag".to_string()),
                mod_time: Some(OffsetDateTime::UNIX_EPOCH.to_string()),
            }),
            dispatch: None,
        };
        let error = persist_tier_delete_journal_entry(store.clone(), &entry)
            .await
            .expect_err("a new source-owning journal without a v6 manifest binding must fail closed");
        assert!(error.to_string().contains("dispatch manifest binding"), "unexpected error: {error}");
        assert_eq!(tier_delete_journal_count(store).await, 0, "rejected prepare must not write a journal");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatch_manifest_recovery_rolls_back_unapproved_crash_states_without_remote_io() {
        let temp_dir = tempfile::tempdir().expect("create dispatch rollback store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "dispatch-manifest-rollback", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "dispatch-manifest-rollback-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("dispatch rollback bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("bucket incarnation should resolve");
        let tier_name = "DISPATCH-ROLLBACK";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();

        let preparing = [
            ("preparing/a.bin", TierDeleteJournalState::Prepared),
            ("preparing/b.bin", TierDeleteJournalState::Dispatched),
        ]
        .into_iter()
        .map(|(object, state)| {
            (
                synthetic_v6_dispatch_entry(bucket, object, tier_name, identity, &uuid::Uuid::new_v4().to_string()),
                Some(state),
            )
        })
        .collect();
        install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "preparing/",
            preparing,
            TierDeleteDispatchManifestState::Preparing,
        )
        .await
        .expect("Preparing crash fixture should persist");

        let aborting = [
            ("aborting/missing.bin", None),
            ("aborting/prepared.bin", Some(TierDeleteJournalState::Prepared)),
            ("aborting/dispatched.bin", Some(TierDeleteJournalState::Dispatched)),
        ]
        .into_iter()
        .map(|(object, state)| {
            (
                synthetic_v6_dispatch_entry(bucket, object, tier_name, identity, &uuid::Uuid::new_v4().to_string()),
                state,
            )
        })
        .collect();
        install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "aborting/",
            aborting,
            TierDeleteDispatchManifestState::Aborting,
        )
        .await
        .expect("Aborting crash fixture should persist");
        install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "empty/",
            Vec::new(),
            TierDeleteDispatchManifestState::Completed,
        )
        .await
        .expect("empty Completed crash fixture should persist");

        let deleted = tokio::time::timeout(Duration::from_secs(30), async {
            let mut deleted = 0;
            loop {
                let stats = recover_tier_delete_dispatch_manifests(store.clone(), 100, None)
                    .await
                    .expect("manifest recovery should reconcile negative-seal crash states");
                deleted += stats.deleted;
                if tier_delete_dispatch_manifest_count(store.clone()).await == 0 {
                    break deleted;
                }
                // A different process-global test can briefly revoke the
                // fleet proof. Recovery must fail closed for that pass and
                // converge after the proof generation is republished.
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("valid negative-seal crash states should eventually converge");
        assert_eq!(deleted, 3);
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        assert_eq!(tier_delete_dispatch_manifest_count(store).await, 0);
        assert_eq!(
            backend.remove_count().await,
            0,
            "rollback recovery must perform zero remote delete operations"
        );
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatch_manifest_recovery_quarantines_impossible_committed_rollback_set() {
        let temp_dir = tempfile::tempdir().expect("create dispatch quarantine store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "dispatch-manifest-quarantine", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "dispatch-manifest-quarantine-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("dispatch quarantine bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("bucket incarnation should resolve");
        let tier_name = "DISPATCH-QUARANTINE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let entries = [TierDeleteJournalState::Prepared, TierDeleteJournalState::Committed]
            .into_iter()
            .enumerate()
            .map(|(index, state)| {
                (
                    synthetic_v6_dispatch_entry(
                        bucket,
                        &format!("quarantine/{index}.bin"),
                        tier_name,
                        identity,
                        &uuid::Uuid::new_v4().to_string(),
                    ),
                    Some(state),
                )
            })
            .collect();
        let (manifest_name, _) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "quarantine/",
            entries,
            TierDeleteDispatchManifestState::Aborting,
        )
        .await
        .expect("impossible rollback fixture should persist");

        let stats = recover_tier_delete_dispatch_manifests(store.clone(), 100, None)
            .await
            .expect("quarantined manifest scan should finish");
        assert_eq!((stats.scanned, stats.deleted, stats.failed), (1, 0, 1));
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            2,
            "validation must precede every deletion"
        );
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, 1);
        assert_eq!(
            test_tier_delete_dispatch_manifest_state(store, &manifest_name)
                .await
                .expect("quarantined manifest should remain readable"),
            Some(TierDeleteDispatchManifestState::Aborting)
        );
        assert_eq!(backend.remove_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatch_manifest_rollback_bounded_concurrency_reaches_tail_behind_slow_member() {
        const JOURNAL_COUNT: usize = 65;

        let temp_dir = tempfile::tempdir().expect("create bounded rollback store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "bounded-dispatch-rollback", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "bounded-dispatch-rollback-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bounded rollback bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("bounded rollback bucket incarnation should resolve");
        let tier_name = "BOUNDED-DISPATCH-ROLLBACK";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("bounded rollback tier lease should resolve")
            .backend_identity();
        let (manifest_name, entries) =
            install_aborting_dispatch_fixture(store.clone(), bucket, incarnation, "slow/", tier_name, identity, JOURNAL_COUNT)
                .await;
        let first_name = tier_delete_journal_object_name(entries.first().expect("slow rollback fixture should not be empty"));
        let last_name = tier_delete_journal_object_name(entries.last().expect("slow rollback fixture should not be empty"));
        let hook = TierDeleteDispatchRollbackTestHook::install_slow_delete(&first_name, &last_name);
        let worker_store = store.clone();
        let worker_manifest = manifest_name.clone();
        let worker =
            tokio::spawn(async move { recover_test_tier_delete_dispatch_manifest(worker_store, &worker_manifest).await });
        tokio::time::timeout(Duration::from_secs(30), hook.wait_until_delete_paused())
            .await
            .expect("the first rollback member should reach the slow hook");
        tokio::time::timeout(Duration::from_secs(30), hook.wait_until_delete_observed())
            .await
            .expect("a member beyond the first two concurrency windows should remain reachable");
        assert!(
            (2..=32).contains(&hook.max_in_flight()),
            "rollback delete concurrency must make progress without exceeding its bound; observed {}",
            hook.max_in_flight()
        );
        hook.release_delete();
        worker
            .await
            .expect("bounded rollback recovery task should join")
            .expect("bounded rollback recovery should converge after the slow member releases");
        drop(hook);

        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, 0);
        assert_eq!(backend.remove_count().await, 0, "rollback must not call the remote tier");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn aborted_dispatch_retry_defers_residual_scan_to_recovery() {
        const JOURNAL_COUNT: usize = 65;

        let temp_dir = tempfile::tempdir().expect("create aborted retry store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "aborted-dispatch-retry", &[4])).await;
        shutdown.cancel();
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "aborted-dispatch-retry-bucket";
        let prefix = "archive/";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("aborted retry bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("aborted retry bucket incarnation should resolve");
        let tier_name = "ABORTED-DISPATCH-RETRY";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("aborted retry tier lease should resolve")
            .backend_identity();
        let aborted_entries = (0..JOURNAL_COUNT)
            .map(|index| {
                (
                    synthetic_v6_dispatch_entry(
                        bucket,
                        &format!("{prefix}{index:06}.bin"),
                        tier_name,
                        identity,
                        &uuid::Uuid::new_v4().to_string(),
                    ),
                    Some(TierDeleteJournalState::Prepared),
                )
            })
            .collect::<Vec<_>>();
        let entries = aborted_entries.iter().map(|(entry, _)| entry.clone()).collect::<Vec<_>>();
        let (manifest_name, _) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            prefix,
            aborted_entries,
            TierDeleteDispatchManifestState::Aborted,
        )
        .await
        .expect("Aborted retry fixture should persist");

        let lifecycle_guard = store
            .acquire_bucket_lifecycle_write_lock(bucket)
            .await
            .expect("aborted retry should acquire the bucket lifecycle fence");
        let mut fence_opts = ObjectOptions::default();
        fence_opts.add_bucket_lifecycle_lock_guard(&lifecycle_guard);
        let bucket_fence = fence_opts
            .bucket_lifecycle_lock_fence
            .clone()
            .expect("aborted retry should capture the bucket lifecycle fence");
        let fleet_proof = acquire_tier_delete_journal_fleet_proof().expect("aborted retry fixture should have a fleet proof");
        let hook = TierDeleteDispatchMemberReadTestHook::install_pause(TierDeleteDispatchMemberReadTestStage::Validation);

        let error =
            match prepare_tier_delete_dispatch(store.clone(), bucket, incarnation, prefix, entries, fleet_proof, &bucket_fence)
                .await
            {
                Ok(_) => panic!("an Aborted manifest must be retained for bounded recovery cleanup"),
                Err(err) => err,
            };

        assert!(
            error.to_string().contains("bounded recovery cleanup"),
            "unexpected aborted retry error: {error}"
        );
        assert_eq!(
            hook.entry_count(),
            0,
            "request retry must not scan the retained Aborted journal set under bucket write lock"
        );
        assert_eq!(
            test_tier_delete_dispatch_manifest_state(store.clone(), &manifest_name)
                .await
                .expect("retained Aborted manifest should remain readable"),
            Some(TierDeleteDispatchManifestState::Aborted)
        );
        assert_eq!(tier_delete_journal_count(store.clone()).await, JOURNAL_COUNT);
        assert_eq!(backend.remove_count().await, 0, "retry must not call the remote tier");
        drop(hook);
        drop(lifecycle_guard);

        recover_test_tier_delete_dispatch_manifest(store.clone(), &manifest_name)
            .await
            .expect("bounded recovery should clean the retained Aborted manifest");
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        assert_eq!(tier_delete_dispatch_manifest_count(store).await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatch_manifest_page_timeout_detaches_one_deduplicated_worker_until_convergence() {
        let temp_dir = tempfile::tempdir().expect("create detached rollback store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "detached-dispatch-rollback", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "detached-dispatch-rollback-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("detached rollback bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("detached rollback bucket incarnation should resolve");
        let tier_name = "DETACHED-DISPATCH-ROLLBACK";
        let _backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("detached rollback tier lease should resolve")
            .backend_identity();
        let (manifest_name, entries) =
            install_aborting_dispatch_fixture(store.clone(), bucket, incarnation, "detached/", tier_name, identity, 1).await;
        let journal_name = tier_delete_journal_object_name(&entries[0]);
        let hook = TierDeleteDispatchRollbackTestHook::install_slow_delete(&journal_name, &journal_name);

        assert!(
            recover_test_tier_delete_dispatch_manifest_with_page_budget(
                store.clone(),
                &manifest_name,
                Duration::from_millis(10),
            )
            .await,
            "the synthetic page budget must elapse while the manifest worker continues"
        );
        tokio::time::timeout(Duration::from_secs(30), hook.wait_until_delete_paused())
            .await
            .expect("the detached manifest worker should continue into its delete phase");
        assert!(
            tier_delete_dispatch_manifest_recovery_inflight_for_test(&store, &manifest_name),
            "the detached worker must retain its per-store/object deduplication guard"
        );
        assert!(
            !recover_test_tier_delete_dispatch_manifest_with_page_budget(store.clone(), &manifest_name, Duration::from_secs(30),)
                .await,
            "a duplicate page observation should be retained without queuing another worker"
        );

        hook.release_delete();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let manifest_gone = matches!(com::read_config(store.clone(), &manifest_name).await, Err(Error::ConfigNotFound));
                if manifest_gone && !tier_delete_dispatch_manifest_recovery_inflight_for_test(&store, &manifest_name) {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the detached manifest worker should release its guard after convergence");
        drop(hook);
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatch_manifest_detached_workers_are_store_bounded_and_shutdown_cancellable() {
        const MANIFEST_COUNT: usize = 6;
        const WORKER_LIMIT: usize = 4;

        let temp_dir = tempfile::tempdir().expect("create bounded detached rollback store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "bounded-detached-dispatch-rollback", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "BOUNDED-DETACHED-DISPATCH-ROLLBACK";
        let _backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("bounded detached rollback tier lease should resolve")
            .backend_identity();
        let mut manifest_names = Vec::with_capacity(MANIFEST_COUNT);
        for index in 0..MANIFEST_COUNT {
            let bucket = format!("bounded-detached-dispatch-rollback-{index}");
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("bounded detached rollback bucket should be created");
            let incarnation = store
                .bucket_incarnation_id(&bucket)
                .await
                .expect("bounded detached rollback bucket incarnation should resolve");
            let (manifest_name, _) = install_aborting_dispatch_fixture(
                store.clone(),
                &bucket,
                incarnation,
                &format!("detached-{index}/"),
                tier_name,
                identity,
                1,
            )
            .await;
            manifest_names.push(manifest_name);
        }
        let hook = TierDeleteDispatchRollbackTestHook::install_pause_all_deletes();

        let first_pass = futures::stream::iter(manifest_names.iter().cloned().map(|manifest_name| {
            let store = store.clone();
            async move {
                recover_test_tier_delete_dispatch_manifest_with_page_budget(store, &manifest_name, Duration::from_millis(10))
                    .await
            }
        }))
        .buffer_unordered(MANIFEST_COUNT)
        .collect::<Vec<_>>()
        .await;
        tokio::time::timeout(Duration::from_secs(30), hook.wait_until_delete_pause_count(WORKER_LIMIT))
            .await
            .expect("every admitted detached worker should pause before its first mutation");
        assert_eq!(
            first_pass.into_iter().filter(|timed_out| *timed_out).count(),
            WORKER_LIMIT,
            "only the per-store worker budget may detach"
        );
        assert_eq!(
            tier_delete_dispatch_manifest_recovery_count_for_test(&store),
            WORKER_LIMIT,
            "detached workers must remain bounded per store"
        );

        let repeated_pass = futures::stream::iter(manifest_names.iter().cloned().map(|manifest_name| {
            let store = store.clone();
            async move {
                recover_test_tier_delete_dispatch_manifest_with_page_budget(store, &manifest_name, Duration::from_secs(30)).await
            }
        }))
        .buffer_unordered(MANIFEST_COUNT)
        .collect::<Vec<_>>()
        .await;
        assert!(
            repeated_pass.into_iter().all(|timed_out| !timed_out),
            "duplicates and over-budget manifests must be retained without a waiter queue"
        );
        assert_eq!(tier_delete_dispatch_manifest_recovery_count_for_test(&store), WORKER_LIMIT);

        shutdown.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            tier_delete_dispatch_manifest_recovery_count_for_test(&store),
            WORKER_LIMIT,
            "shutdown must not drop a bounded batch before its admitted work drains"
        );
        assert_eq!(tier_delete_journal_count(store.clone()).await, MANIFEST_COUNT);
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, MANIFEST_COUNT);
        hook.release_all_deletes();
        tokio::time::timeout(Duration::from_secs(30), async {
            while tier_delete_dispatch_manifest_recovery_count_for_test(&store) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("shutdown should cancel every detached worker and release its registry permit");
        assert_eq!(tier_delete_journal_count(store.clone()).await, MANIFEST_COUNT);
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, MANIFEST_COUNT);
        drop(hook);
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            MANIFEST_COUNT,
            "releasing the old hook after shutdown must not resume a cancelled mutation"
        );
        assert_eq!(
            tier_delete_dispatch_manifest_recovery_count_for_test(&store),
            0,
            "the shutdown store must leave no registry permits behind"
        );
        drop(store);
        drop(ctx);

        let (_restarted_ctx, restarted_store, restarted_shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "bounded-detached-dispatch-rollback-restart",
            &[4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(restarted_store.clone(), Vec::new()).await;
        assert!(!restarted_shutdown.is_cancelled(), "the restarted store needs a fresh shutdown token");
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let active = tier_delete_dispatch_manifest_recovery_count_for_test(&restarted_store);
                let manifests = tier_delete_dispatch_manifest_count(restarted_store.clone()).await;
                if active > 0 || manifests < MANIFEST_COUNT {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the restarted store should start a fresh manifest recovery");
        tokio::time::timeout(Duration::from_secs(30), async {
            while tier_delete_dispatch_manifest_recovery_count_for_test(&restarted_store) != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the restarted store recovery should release every worker permit");
        if tier_delete_dispatch_manifest_count(restarted_store.clone()).await != 0 {
            let stats = recover_tier_delete_dispatch_manifests(restarted_store.clone(), 100, None)
                .await
                .expect("the restarted store should retry any conservatively retained manifest");
            assert_eq!(stats.failed, 0, "the restarted recovery must not quarantine a valid manifest");
        }
        assert_eq!(tier_delete_journal_count(restarted_store.clone()).await, 0);
        assert_eq!(tier_delete_dispatch_manifest_count(restarted_store).await, 0);
        restarted_shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatch_manifest_shutdown_stops_aborting_validation_tail_reads() {
        const JOURNAL_COUNT: usize = 65;
        const ADMITTED_READS: usize = 32;

        let temp_dir = tempfile::tempdir().expect("create validation-read shutdown store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "dispatch-validation-read-shutdown", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "dispatch-validation-read-shutdown-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("validation-read shutdown bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("validation-read shutdown bucket incarnation should resolve");
        let tier_name = "VALIDATION-READ-SHUTDOWN";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("validation-read shutdown tier lease should resolve")
            .backend_identity();
        let (manifest_name, _) =
            install_aborting_dispatch_fixture(store.clone(), bucket, incarnation, "archive/", tier_name, identity, JOURNAL_COUNT)
                .await;
        let hook = TierDeleteDispatchMemberReadTestHook::install_pause(TierDeleteDispatchMemberReadTestStage::Validation);
        let worker_store = store.clone();
        let worker_manifest = manifest_name.clone();
        let worker = tokio::spawn(async move {
            recover_test_tier_delete_dispatch_manifest_with_page_budget(worker_store, &worker_manifest, Duration::from_secs(30))
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), hook.wait_until_read_pause_count(ADMITTED_READS))
            .await
            .expect("the first validation read window should be admitted");
        assert_eq!(hook.entry_count(), ADMITTED_READS);
        assert_eq!(hook.in_flight(), ADMITTED_READS);

        shutdown.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(hook.entry_count(), ADMITTED_READS, "shutdown must not admit validation tail reads");
        assert_eq!(tier_delete_dispatch_manifest_recovery_count_for_test(&store), 1);
        assert!(tier_delete_dispatch_manifest_operation_lock_held_for_test(store.clone(), &manifest_name).await);
        assert!(crate::services::notification_sys::tier_delete_journal_fleet_proof_has_inflight_for_test());
        assert_eq!(tier_delete_journal_count(store.clone()).await, JOURNAL_COUNT);
        assert_eq!(backend.remove_count().await, 0);

        hook.release_all_reads();
        assert!(
            !worker.await.expect("validation-read shutdown worker should join"),
            "cooperative shutdown should retain an unvalidated rollback"
        );
        assert_eq!(hook.entry_count(), ADMITTED_READS);
        assert_eq!(hook.in_flight(), 0);
        assert_eq!(tier_delete_dispatch_manifest_recovery_count_for_test(&store), 0);
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            JOURNAL_COUNT,
            "no rollback DELETE may start"
        );
        assert_eq!(
            test_tier_delete_dispatch_manifest_state(store.clone(), &manifest_name)
                .await
                .expect("retained Aborting manifest should remain readable"),
            Some(TierDeleteDispatchManifestState::Aborting)
        );
        drop(hook);
        drop(store);
        drop(ctx);

        let (_restarted_ctx, restarted_store, restarted_shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "dispatch-validation-read-shutdown-restart",
            &[4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(restarted_store.clone(), Vec::new()).await;
        drive_tier_delete_dispatch_restart_to_convergence(restarted_store.clone()).await;
        restarted_shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatch_manifest_shutdown_stops_authorized_tail_reads_before_receipt_or_cas() {
        const JOURNAL_COUNT: usize = 65;
        const ADMITTED_READS: usize = 32;

        let temp_dir = tempfile::tempdir().expect("create authorized-read shutdown store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "dispatch-authorized-read-shutdown", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "dispatch-authorized-read-shutdown-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("authorized-read shutdown bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("authorized-read shutdown bucket incarnation should resolve");
        let tier_name = "AUTHORIZED-READ-SHUTDOWN";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("authorized-read shutdown tier lease should resolve")
            .backend_identity();
        let entries = (0..JOURNAL_COUNT)
            .map(|index| {
                (
                    synthetic_v6_dispatch_entry(
                        bucket,
                        &format!("archive/{index:06}.bin"),
                        tier_name,
                        identity,
                        &uuid::Uuid::new_v4().to_string(),
                    ),
                    Some(TierDeleteJournalState::Committed),
                )
            })
            .collect();
        let (manifest_name, _) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "archive/",
            entries,
            TierDeleteDispatchManifestState::DispatchAuthorized,
        )
        .await
        .expect("authorized-read shutdown fixture should persist");
        let tier_config = ctx
            .tier_config_mgr()
            .read()
            .await
            .tiers
            .get(tier_name)
            .cloned()
            .expect("authorized-read tier config should remain available for restart");
        let hook = TierDeleteDispatchMemberReadTestHook::install_pause(TierDeleteDispatchMemberReadTestStage::Authorized);
        let worker_store = store.clone();
        let worker_manifest = manifest_name.clone();
        let worker = tokio::spawn(async move {
            recover_test_tier_delete_dispatch_manifest_with_page_budget(worker_store, &worker_manifest, Duration::from_secs(30))
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), hook.wait_until_read_pause_count(ADMITTED_READS))
            .await
            .expect("the first authorized read window should be admitted");

        shutdown.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(hook.entry_count(), ADMITTED_READS, "shutdown must not admit authorized tail reads");
        assert_eq!(hook.in_flight(), ADMITTED_READS);
        assert_eq!(tier_delete_dispatch_manifest_recovery_count_for_test(&store), 1);
        assert!(tier_delete_dispatch_manifest_operation_lock_held_for_test(store.clone(), &manifest_name).await);
        assert!(crate::services::notification_sys::tier_delete_journal_fleet_proof_has_inflight_for_test());

        hook.release_all_reads();
        assert!(
            !worker.await.expect("authorized-read shutdown worker should join"),
            "cooperative shutdown should retain an unscanned authorized manifest"
        );
        assert_eq!(hook.entry_count(), ADMITTED_READS);
        assert_eq!(hook.in_flight(), 0);
        assert_eq!(tier_delete_dispatch_manifest_recovery_count_for_test(&store), 0);
        assert_eq!(
            test_tier_delete_dispatch_manifest_state(store.clone(), &manifest_name)
                .await
                .expect("retained Authorized manifest should remain readable"),
            Some(TierDeleteDispatchManifestState::DispatchAuthorized),
            "cancellation before member reads must prevent the Completed CAS"
        );
        assert_eq!(
            hook.authorized_progress_count(),
            0,
            "cancellation before member reads must not attempt a decommission progress receipt"
        );
        assert_eq!(backend.remove_count().await, 0);
        drop(hook);
        drop(store);
        drop(ctx);

        let (restarted_ctx, restarted_store, restarted_shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "dispatch-authorized-read-shutdown-restart",
            &[4],
        ))
        .await;
        {
            let tier_config_mgr = restarted_ctx.tier_config_mgr();
            let mut manager = tier_config_mgr.write().await;
            manager.tiers.insert(tier_name.to_string(), tier_config);
            manager
                .install_test_driver(tier_name, Box::new(backend.clone()))
                .expect("the exact authorized-read tier driver should reinstall after restart");
        }
        crate::bucket::metadata_sys::init_bucket_metadata_sys(restarted_store.clone(), Vec::new()).await;
        drive_tier_delete_dispatch_restart_to_convergence(restarted_store.clone()).await;
        assert_eq!(backend.remove_count().await, JOURNAL_COUNT);
        restarted_shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatch_completion_cas_is_bounded_and_reaches_the_tail() {
        const JOURNAL_COUNT: usize = 65;
        const ADMITTED_COMMITS: usize = 32;

        let temp_dir = tempfile::tempdir().expect("create dispatch completion concurrency store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "dispatch-completion-concurrency", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "dispatch-completion-concurrency-bucket";
        let prefix = "archive/";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("dispatch completion concurrency bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("dispatch completion concurrency bucket incarnation should resolve");
        let tier_name = "DISPATCH-COMPLETION-CONCURRENCY";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        backend.set_remove_failure(true);
        let identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("dispatch completion concurrency tier lease should resolve")
            .backend_identity();
        let entries = (0..JOURNAL_COUNT)
            .map(|index| {
                synthetic_v6_dispatch_entry(
                    bucket,
                    &format!("{prefix}{index:06}.bin"),
                    tier_name,
                    identity,
                    &uuid::Uuid::new_v4().to_string(),
                )
            })
            .collect::<Vec<_>>();
        let (manifest_name, _) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            prefix,
            entries
                .iter()
                .cloned()
                .map(|entry| (entry, Some(TierDeleteJournalState::Dispatched)))
                .collect(),
            TierDeleteDispatchManifestState::DispatchAuthorized,
        )
        .await
        .expect("dispatch completion concurrency fixture should persist");

        let lifecycle_guard = store
            .acquire_bucket_lifecycle_write_lock(bucket)
            .await
            .expect("dispatch completion should acquire the bucket lifecycle fence");
        let mut fence_opts = ObjectOptions::default();
        fence_opts.add_bucket_lifecycle_lock_guard(&lifecycle_guard);
        let bucket_fence = fence_opts
            .bucket_lifecycle_lock_fence
            .clone()
            .expect("dispatch completion should capture the bucket lifecycle fence");
        let fleet_proof =
            acquire_tier_delete_journal_fleet_proof().expect("dispatch completion concurrency fixture should have a fleet proof");
        let prepared =
            prepare_tier_delete_dispatch(store.clone(), bucket, incarnation, prefix, entries, fleet_proof, &bucket_fence)
                .await
                .expect("the existing Authorized dispatch should reload");
        let active = prepared
            .consume(bucket, incarnation, prefix)
            .expect("the existing Authorized dispatch permit should be consumable");
        active
            .authorization()
            .mark_mutation_started(bucket, incarnation, prefix)
            .expect("the completion test should mark its synthetic local mutation");

        let hook = TierDeleteDispatchMemberReadTestHook::install_pause(TierDeleteDispatchMemberReadTestStage::Completion);
        let worker_store = store.clone();
        let worker = tokio::spawn(async move { complete_tier_delete_dispatch(worker_store, &active, &bucket_fence).await });
        tokio::time::timeout(Duration::from_secs(30), hook.wait_until_read_pause_count(ADMITTED_COMMITS))
            .await
            .expect("the first completion CAS window should be admitted");
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(hook.entry_count(), ADMITTED_COMMITS, "completion must not admit an unbounded CAS tail");
        assert_eq!(hook.in_flight(), ADMITTED_COMMITS);

        hook.release_all_reads();
        worker
            .await
            .expect("dispatch completion concurrency worker should join")
            .expect("every bounded completion CAS should succeed");
        assert_eq!(
            hook.entry_count(),
            JOURNAL_COUNT,
            "completion must eventually admit the entire journal set"
        );
        assert_eq!(hook.in_flight(), 0);
        assert_eq!(
            test_tier_delete_dispatch_manifest_state(store.clone(), &manifest_name)
                .await
                .expect("completed concurrency manifest should remain readable"),
            Some(TierDeleteDispatchManifestState::Completed)
        );
        drop(hook);
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatch_manifest_shutdown_stops_completed_missing_journal_tail_reads() {
        const JOURNAL_COUNT: usize = 65;
        const ADMITTED_READS: usize = 32;

        let temp_dir = tempfile::tempdir().expect("create completed-read shutdown store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "dispatch-completed-read-shutdown", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "dispatch-completed-read-shutdown-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("completed-read shutdown bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("completed-read shutdown bucket incarnation should resolve");
        let tier_name = "COMPLETED-READ-SHUTDOWN";
        let _backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("completed-read shutdown tier lease should resolve")
            .backend_identity();
        let entries = (0..JOURNAL_COUNT)
            .map(|index| {
                (
                    synthetic_v6_dispatch_entry(
                        bucket,
                        &format!("archive/{index:06}.bin"),
                        tier_name,
                        identity,
                        &uuid::Uuid::new_v4().to_string(),
                    ),
                    None,
                )
            })
            .collect();
        let (manifest_name, _) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "archive/",
            entries,
            TierDeleteDispatchManifestState::Completed,
        )
        .await
        .expect("completed-read shutdown fixture should persist");
        let hook = TierDeleteDispatchMemberReadTestHook::install_pause(TierDeleteDispatchMemberReadTestStage::Completed);
        let worker_store = store.clone();
        let worker_manifest = manifest_name.clone();
        let worker = tokio::spawn(async move {
            recover_test_tier_delete_dispatch_manifest_with_page_budget(worker_store, &worker_manifest, Duration::from_secs(30))
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), hook.wait_until_read_pause_count(ADMITTED_READS))
            .await
            .expect("the first Completed read window should be admitted");

        shutdown.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(hook.entry_count(), ADMITTED_READS, "shutdown must not admit later Completed chunks");
        assert_eq!(hook.in_flight(), ADMITTED_READS);
        assert_eq!(tier_delete_dispatch_manifest_recovery_count_for_test(&store), 1);
        assert!(tier_delete_dispatch_manifest_operation_lock_held_for_test(store.clone(), &manifest_name).await);
        assert!(crate::services::notification_sys::tier_delete_journal_fleet_proof_has_inflight_for_test());

        hook.release_all_reads();
        assert!(
            !worker.await.expect("completed-read shutdown worker should join"),
            "cooperative shutdown should retain a partially scanned Completed manifest"
        );
        assert_eq!(hook.entry_count(), ADMITTED_READS);
        assert_eq!(hook.in_flight(), 0);
        assert_eq!(tier_delete_dispatch_manifest_recovery_count_for_test(&store), 0);
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        assert_eq!(
            tier_delete_dispatch_manifest_count(store.clone()).await,
            1,
            "manifest DELETE must not run"
        );
        assert_eq!(
            test_tier_delete_dispatch_manifest_state(store.clone(), &manifest_name)
                .await
                .expect("retained Completed manifest should remain readable"),
            Some(TierDeleteDispatchManifestState::Completed)
        );
        drop(hook);
        drop(store);
        drop(ctx);

        let (_restarted_ctx, restarted_store, restarted_shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "dispatch-completed-read-shutdown-restart",
            &[4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(restarted_store.clone(), Vec::new()).await;
        drive_tier_delete_dispatch_restart_to_convergence(restarted_store.clone()).await;
        restarted_shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatch_manifest_shutdown_drains_preparing_cas_before_releasing_fences() {
        let temp_dir = tempfile::tempdir().expect("create preparing CAS shutdown store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "dispatch-preparing-cas-shutdown", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "dispatch-preparing-cas-shutdown-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("preparing CAS shutdown bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("preparing CAS shutdown bucket incarnation should resolve");
        let entry = synthetic_v6_dispatch_entry(
            bucket,
            "archive/preparing-cas.bin",
            "PREPARING-CAS-SHUTDOWN",
            [31; 32],
            &uuid::Uuid::new_v4().to_string(),
        );
        let (manifest_name, _) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "archive/",
            vec![(entry, Some(TierDeleteJournalState::Prepared))],
            TierDeleteDispatchManifestState::Preparing,
        )
        .await
        .expect("preparing CAS shutdown fixture should persist");
        let barrier = crate::set_disk::PutObjectCommitBarrier::install(
            RUSTFS_META_BUCKET,
            &manifest_name,
            crate::set_disk::PutObjectCommitPause::BeforeQuotaRename,
        );
        let worker_store = store.clone();
        let worker_manifest = manifest_name.clone();
        let worker = tokio::spawn(async move {
            recover_test_tier_delete_dispatch_manifest_with_page_budget(worker_store, &worker_manifest, Duration::from_secs(30))
                .await
        });
        barrier.wait_until_paused().await;

        shutdown.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            tier_delete_dispatch_manifest_recovery_count_for_test(&store),
            1,
            "a paused CAS must retain its registry permit after shutdown"
        );
        assert!(
            tier_delete_dispatch_manifest_operation_lock_held_for_test(store.clone(), &manifest_name).await,
            "a paused CAS must retain its manifest operation lock after shutdown"
        );
        assert!(
            crate::services::notification_sys::tier_delete_journal_fleet_proof_has_inflight_for_test(),
            "a paused CAS must retain its fleet-proof generation permit"
        );

        barrier.release();
        assert!(
            !worker.await.expect("preparing CAS shutdown worker should join"),
            "cooperative shutdown should retain the partially advanced manifest"
        );
        assert_eq!(tier_delete_dispatch_manifest_recovery_count_for_test(&store), 0);
        assert!(
            !tier_delete_dispatch_manifest_operation_lock_held_for_test(store.clone(), &manifest_name).await,
            "the operation lock may release only after the admitted CAS drains"
        );
        assert!(
            com::read_config(store.clone(), &manifest_name).await.is_ok(),
            "shutdown must retain the durable manifest for restart replay"
        );
        drop(barrier);
        drop(store);
        drop(ctx);

        let (_restarted_ctx, restarted_store, restarted_shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "dispatch-preparing-cas-shutdown-restart",
            &[4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(restarted_store.clone(), Vec::new()).await;
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut stable_empty_observations = 0;
            loop {
                if tier_delete_journal_count(restarted_store.clone()).await == 0
                    && tier_delete_dispatch_manifest_count(restarted_store.clone()).await == 0
                    && tier_delete_dispatch_manifest_recovery_count_for_test(&restarted_store) == 0
                {
                    stable_empty_observations += 1;
                    if stable_empty_observations == 10 {
                        break;
                    }
                } else {
                    stable_empty_observations = 0;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the restarted background recovery should replay the drained Preparing CAS");
        assert_eq!(tier_delete_journal_count(restarted_store.clone()).await, 0);
        assert_eq!(tier_delete_dispatch_manifest_count(restarted_store).await, 0);
        restarted_shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatch_manifest_shutdown_drains_admitted_delete_batch_without_tail_admission() {
        const JOURNAL_COUNT: usize = 65;
        const ADMITTED_BATCH: usize = 32;

        let temp_dir = tempfile::tempdir().expect("create delete-batch shutdown store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "dispatch-delete-batch-shutdown", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "dispatch-delete-batch-shutdown-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("delete-batch shutdown bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("delete-batch shutdown bucket incarnation should resolve");
        let (manifest_name, entries) = install_aborting_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "archive/",
            "DELETE-BATCH-SHUTDOWN",
            [32; 32],
            JOURNAL_COUNT,
        )
        .await;
        let mut journal_names = entries.iter().map(tier_delete_journal_object_name).collect::<Vec<_>>();
        journal_names.sort();
        let published_delete_name = journal_names[0].clone();
        let hook = TierDeleteDispatchRollbackTestHook::install_pause_all_except_delete(&published_delete_name);
        let barrier =
            crate::set_disk::DeleteObjectCommitBarrier::install_after_publish(RUSTFS_META_BUCKET, &published_delete_name);
        let worker_store = store.clone();
        let worker_manifest = manifest_name.clone();
        let worker = tokio::spawn(async move {
            recover_test_tier_delete_dispatch_manifest_with_page_budget(worker_store, &worker_manifest, Duration::from_secs(30))
                .await
        });
        barrier.wait_until_paused().await;
        tokio::time::timeout(Duration::from_secs(30), hook.wait_until_delete_pause_count(ADMITTED_BATCH - 1))
            .await
            .expect("the rest of the bounded delete batch should pause before mutation");
        assert_eq!(hook.delete_entry_count(), ADMITTED_BATCH);

        shutdown.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(tier_delete_dispatch_manifest_recovery_count_for_test(&store), 1);
        assert!(
            tier_delete_dispatch_manifest_operation_lock_held_for_test(store.clone(), &manifest_name).await,
            "the partially published DELETE must retain its operation lock"
        );
        assert!(
            crate::services::notification_sys::tier_delete_journal_fleet_proof_has_inflight_for_test(),
            "the partially published DELETE must retain its fleet-proof permit"
        );
        assert_eq!(
            hook.delete_entry_count(),
            ADMITTED_BATCH,
            "shutdown must stop admitting the tail behind the bounded batch"
        );

        barrier.release();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(
            tier_delete_dispatch_manifest_recovery_count_for_test(&store),
            1,
            "the worker must retain its guards until every admitted sibling drains"
        );
        hook.release_all_deletes();
        assert!(
            !worker.await.expect("delete-batch shutdown worker should join"),
            "cooperative shutdown should retain the partial rollback for restart"
        );
        assert_eq!(hook.delete_entry_count(), ADMITTED_BATCH);
        assert_eq!(tier_delete_dispatch_manifest_recovery_count_for_test(&store), 0);
        assert!(
            tier_delete_journal_count(store.clone()).await >= JOURNAL_COUNT - 1,
            "only the already-published DELETE may commit before restart"
        );
        drop(barrier);
        drop(hook);
        drop(store);
        drop(ctx);

        let (_restarted_ctx, restarted_store, restarted_shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "dispatch-delete-batch-shutdown-restart", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(restarted_store.clone(), Vec::new()).await;
        tokio::time::timeout(Duration::from_secs(30), async {
            let mut stable_empty_observations = 0;
            loop {
                if tier_delete_journal_count(restarted_store.clone()).await == 0
                    && tier_delete_dispatch_manifest_count(restarted_store.clone()).await == 0
                    && tier_delete_dispatch_manifest_recovery_count_for_test(&restarted_store) == 0
                {
                    stable_empty_observations += 1;
                    if stable_empty_observations == 10 {
                        break;
                    }
                } else {
                    stable_empty_observations = 0;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the restarted background recovery should replay the partially drained delete batch");
        assert_eq!(tier_delete_journal_count(restarted_store.clone()).await, 0);
        assert_eq!(tier_delete_dispatch_manifest_count(restarted_store).await, 0);
        restarted_shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatch_manifest_rollback_retries_delete_and_confirmation_failures() {
        const JOURNAL_COUNT: usize = 40;

        let temp_dir = tempfile::tempdir().expect("create rollback retry store dir");
        // Manual retries must own progress between fault removal and the next attempt.
        let mut instance_ctx = crate::runtime::instance::InstanceContext::new();
        instance_ctx.suppress_tier_delete_journal_recovery_for_test();
        let (ctx, store, shutdown) = without_storage_class_env(build_isolated_test_store_with_layout(
            temp_dir.path(),
            "dispatch-rollback-retry",
            &[(1, 4)],
            CancellationToken::new(),
            Some(Arc::new(instance_ctx)),
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "dispatch-rollback-retry-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("rollback retry bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("rollback retry bucket incarnation should resolve");
        let tier_name = "DISPATCH-ROLLBACK-RETRY";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("rollback retry tier lease should resolve")
            .backend_identity();

        let (delete_manifest, delete_entries) = install_aborting_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "delete-failure/",
            tier_name,
            identity,
            JOURNAL_COUNT,
        )
        .await;
        let delete_failure_name = tier_delete_journal_object_name(&delete_entries[10]);
        let delete_hook = TierDeleteDispatchRollbackTestHook::install_delete_failure(&delete_failure_name);
        let delete_error = recover_test_tier_delete_dispatch_manifest(store.clone(), &delete_manifest)
            .await
            .expect_err("an injected member delete failure must retain the manifest")
            .to_string();
        assert!(delete_error.contains("injected tier delete dispatch rollback delete failure"));
        assert!(
            tier_delete_journal_count(store.clone()).await > 0,
            "the failed member and all work not admitted after the first error must remain retryable"
        );
        drop(delete_hook);
        recover_test_tier_delete_dispatch_manifest(store.clone(), &delete_manifest)
            .await
            .expect("the delete-stage retry should converge");

        let (confirmation_manifest, confirmation_entries) = install_aborting_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "confirmation-failure/",
            tier_name,
            identity,
            JOURNAL_COUNT,
        )
        .await;
        let confirmation_failure_name = tier_delete_journal_object_name(&confirmation_entries[10]);
        let confirmation_hook = TierDeleteDispatchRollbackTestHook::install_confirmation_failure(&confirmation_failure_name);
        let confirmation_error = recover_test_tier_delete_dispatch_manifest(store.clone(), &confirmation_manifest)
            .await
            .expect_err("an injected confirmation failure must retain the manifest")
            .to_string();
        assert!(confirmation_error.contains("injected tier delete dispatch rollback confirmation failure"));
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            0,
            "confirmation failure occurs only after the bounded delete phase drains"
        );
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, 1);
        drop(confirmation_hook);
        recover_test_tier_delete_dispatch_manifest(store.clone(), &confirmation_manifest)
            .await
            .expect("the confirmation-stage retry should converge over missing members");

        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, 0);
        assert_eq!(backend.remove_count().await, 0, "rollback retries must never call the remote tier");
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[test]
    #[serial_test::serial(storage_class_env)]
    fn v6_decommission_checkpoint_no_lock_put_rejects_lost_publication_fence() {
        run_large_stack_async_test(
            "v6-checkpoint-fence-loss",
            v6_decommission_checkpoint_no_lock_put_rejects_lost_publication_fence_case,
        );
    }

    #[cfg(feature = "test-util")]
    async fn v6_decommission_checkpoint_no_lock_put_rejects_lost_publication_fence_case() {
        let temp_dir = tempfile::tempdir().expect("create v6 checkpoint fence-loss store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "v6-checkpoint-fence-loss", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "v6-checkpoint-fence-loss-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("checkpoint fence-loss bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("checkpoint fence-loss bucket incarnation should resolve");
        let entry = synthetic_v6_dispatch_entry(
            bucket,
            "archive/fence-loss.bin",
            "FENCE-LOSS-TIER",
            [18; 32],
            &uuid::Uuid::new_v4().to_string(),
        );
        let (manifest_name, entries) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "archive/",
            vec![(entry, Some(TierDeleteJournalState::Prepared))],
            TierDeleteDispatchManifestState::Preparing,
        )
        .await
        .expect("checkpoint fence-loss fixture should persist");
        let journal_name = tier_delete_journal_object_name(&entries[0]);
        let manifest_data = com::read_config(store.clone(), &manifest_name)
            .await
            .expect("checkpoint fence-loss manifest should be readable");
        let journal_data = com::read_config(store.clone(), &journal_name)
            .await
            .expect("checkpoint fence-loss journal should be readable");
        for pool in &store.pools {
            com::save_config(pool.clone(), &manifest_name, manifest_data.clone())
                .await
                .expect("manifest copy should persist in each checkpoint fence-loss pool");
            com::save_config(pool.clone(), &journal_name, journal_data.clone())
                .await
                .expect("journal copy should persist in each checkpoint fence-loss pool");
        }
        mark_test_pool_decommissioning(&store, 0).await;
        for (path, data) in [
            (&manifest_name, manifest_data.as_slice()),
            (&journal_name, journal_data.as_slice()),
        ] {
            let record = validate_durable_ilm_record(path, data).expect("checkpoint fence-loss v6 record should validate");
            store
                .persist_decommission_durable_ilm_receipt_for_test(0, 1, path, &record, false)
                .await
                .expect("checkpoint fence-loss receipt should persist");
        }

        let loss_hook = crate::store::object::DecommissionMutationFenceLossHook::install(
            RUSTFS_META_BUCKET,
            &manifest_name,
            crate::store::object::DecommissionMutationFenceTestPhase::Migration,
        );
        let barrier = crate::set_disk::PutObjectCommitBarrier::install(
            RUSTFS_META_BUCKET,
            &manifest_name,
            crate::set_disk::PutObjectCommitPause::BeforeQuotaRename,
        );
        let worker_store = store.clone();
        let worker_manifest = manifest_name.clone();
        let worker =
            tokio::spawn(async move { recover_test_tier_delete_dispatch_manifest(worker_store, &worker_manifest).await });
        barrier.wait_until_paused().await;
        loss_hook.mark_lost();
        barrier.release();
        drop(barrier);
        let error = worker
            .await
            .expect("checkpoint fence-loss recovery task should join")
            .expect_err("lost publication fence must reject the no-lock checkpoint PUT")
            .to_string();
        assert!(
            error.contains("lock") || error.contains("fence"),
            "unexpected checkpoint fence-loss error: {error}"
        );
        assert_eq!(
            com::read_config(store.pools[1].clone(), &manifest_name)
                .await
                .expect("fenced target manifest should remain readable"),
            manifest_data,
            "the target checkpoint must not commit after its publication fence is lost"
        );

        drop(loss_hook);
        recover_test_tier_delete_dispatch_manifest(store.clone(), &manifest_name)
            .await
            .expect("checkpoint recovery should converge after a fresh fence is acquired");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn v6_decommission_checkpoint_partial_target_failure_retries_without_advancing_receipts() {
        let temp_dir = tempfile::tempdir().expect("create v6 partial checkpoint store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "v6-partial-checkpoint", &[4, 4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "v6-partial-checkpoint-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("partial checkpoint bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("partial checkpoint bucket incarnation should resolve");
        let entry = synthetic_v6_dispatch_entry(
            bucket,
            "archive/partial.bin",
            "PARTIAL-CHECKPOINT-TIER",
            [19; 32],
            &uuid::Uuid::new_v4().to_string(),
        );
        let (manifest_name, entries) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "archive/",
            vec![(entry, Some(TierDeleteJournalState::Prepared))],
            TierDeleteDispatchManifestState::Preparing,
        )
        .await
        .expect("partial checkpoint fixture should persist");
        let journal_name = tier_delete_journal_object_name(&entries[0]);
        let manifest_data = com::read_config(store.clone(), &manifest_name)
            .await
            .expect("partial checkpoint manifest should be readable");
        let journal_data = com::read_config(store.clone(), &journal_name)
            .await
            .expect("partial checkpoint journal should be readable");
        for pool in &store.pools {
            com::save_config(pool.clone(), &manifest_name, manifest_data.clone())
                .await
                .expect("manifest copy should persist in each partial checkpoint pool");
            com::save_config(pool.clone(), &journal_name, journal_data.clone())
                .await
                .expect("journal copy should persist in each partial checkpoint pool");
        }
        mark_test_pool_decommissioning_with_split_targets(&store, 0).await;
        for target_pool_index in [1, 2] {
            for (path, data) in [
                (&manifest_name, manifest_data.as_slice()),
                (&journal_name, journal_data.as_slice()),
            ] {
                let record = validate_durable_ilm_record(path, data).expect("partial checkpoint v6 record should validate");
                store
                    .persist_decommission_durable_ilm_receipt_for_test(0, target_pool_index, path, &record, false)
                    .await
                    .expect("partial checkpoint receipt should persist on each target");
            }
        }
        let (aborting_data, preparing_etag) = test_tier_delete_dispatch_manifest_checkpoint(
            store.clone(),
            &manifest_name,
            TierDeleteDispatchManifestState::Aborting,
        )
        .await
        .expect("partial checkpoint Aborting generation should encode");
        let targets = store
            .decommission_durable_ilm_checkpoint_targets(&manifest_name, &aborting_data, &preparing_etag)
            .await
            .expect("partial checkpoint targets should resolve")
            .expect("the active decommission should own the partial checkpoint");
        assert_eq!(
            targets.iter().map(|target| target.target_pool_index).collect::<Vec<_>>(),
            vec![1, 2],
            "the capacity reservation must exercise two independently confirmed targets"
        );

        let failure_hook = DecommissionCheckpointTargetFailureHook::install(2);
        let error = recover_test_tier_delete_dispatch_manifest(store.clone(), &manifest_name)
            .await
            .expect_err("the injected second-target failure must reject the checkpoint");
        assert!(
            error.to_string().contains("injected decommission checkpoint target failure"),
            "unexpected partial checkpoint error: {error}"
        );
        let mut diagnostic = &error as &(dyn std::error::Error + 'static);
        let mut target_context_found = false;
        while let Some(source) = diagnostic.source() {
            if source.to_string().contains("target pool 2") {
                target_context_found = true;
                break;
            }
            diagnostic = source;
        }
        assert!(
            target_context_found,
            "the stable checkpoint error must retain the failed target in its diagnostic source chain: {error:?}"
        );
        assert_eq!(
            com::read_config(store.pools[1].clone(), &manifest_name)
                .await
                .expect("first target checkpoint should be readable"),
            aborting_data,
            "the first target should model a committed checkpoint before the later failure"
        );
        assert_eq!(
            com::read_config(store.pools[2].clone(), &manifest_name)
                .await
                .expect("second target checkpoint should be readable"),
            manifest_data,
            "the failed second target must retain the prior generation"
        );
        let retry_targets = store
            .decommission_durable_ilm_checkpoint_targets(&manifest_name, &aborting_data, &preparing_etag)
            .await
            .expect("unadvanced receipts must still authorize the exact partial checkpoint retry")
            .expect("the partial checkpoint retry should remain decommission-owned");
        assert!(retry_targets[0].already_committed);
        assert!(!retry_targets[1].already_committed);

        drop(failure_hook);
        recover_test_tier_delete_dispatch_manifest(store.clone(), &manifest_name)
            .await
            .expect("the partial checkpoint should converge after the target recovers");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn v6_decommission_checkpoint_shutdown_drains_target_put_and_replays_capacity() {
        let temp_dir = tempfile::tempdir().expect("create checkpoint shutdown store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "v6-checkpoint-shutdown", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "v6-checkpoint-shutdown-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("checkpoint shutdown bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("checkpoint shutdown bucket incarnation should resolve");
        let entry = synthetic_v6_dispatch_entry(
            bucket,
            "archive/checkpoint-shutdown.bin",
            "CHECKPOINT-SHUTDOWN",
            [33; 32],
            &uuid::Uuid::new_v4().to_string(),
        );
        let (manifest_name, entries) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "archive/",
            vec![(entry, Some(TierDeleteJournalState::Prepared))],
            TierDeleteDispatchManifestState::Preparing,
        )
        .await
        .expect("checkpoint shutdown fixture should persist");
        let journal_name = tier_delete_journal_object_name(&entries[0]);
        let manifest_data = com::read_config(store.clone(), &manifest_name)
            .await
            .expect("checkpoint shutdown manifest should be readable");
        let journal_data = com::read_config(store.clone(), &journal_name)
            .await
            .expect("checkpoint shutdown journal should be readable");
        for pool in &store.pools {
            com::save_config(pool.clone(), &manifest_name, manifest_data.clone())
                .await
                .expect("manifest copy should persist in each checkpoint shutdown pool");
            com::save_config(pool.clone(), &journal_name, journal_data.clone())
                .await
                .expect("journal copy should persist in each checkpoint shutdown pool");
        }
        mark_test_pool_decommissioning(&store, 0).await;
        for (path, data) in [
            (&manifest_name, manifest_data.as_slice()),
            (&journal_name, journal_data.as_slice()),
        ] {
            let record = validate_durable_ilm_record(path, data).expect("checkpoint shutdown v6 record should validate");
            store
                .persist_decommission_durable_ilm_receipt_for_test(0, 1, path, &record, false)
                .await
                .expect("checkpoint shutdown receipt should persist");
        }
        let (aborting_data, preparing_etag) = test_tier_delete_dispatch_manifest_checkpoint(
            store.clone(),
            &manifest_name,
            TierDeleteDispatchManifestState::Aborting,
        )
        .await
        .expect("checkpoint shutdown Aborting generation should encode");
        let targets = store
            .decommission_durable_ilm_checkpoint_targets(&manifest_name, &aborting_data, &preparing_etag)
            .await
            .expect("checkpoint shutdown target should resolve")
            .expect("the active decommission should own the checkpoint target");
        assert_eq!(targets.len(), 1);
        let target = targets[0].clone();
        let consumed_before_checkpoint = store.pool_meta.read().await.pools[0]
            .decommission
            .as_ref()
            .and_then(|info| info.capacity_reservation.as_ref())
            .expect("checkpoint shutdown capacity reservation should exist")
            .consumed_target_physical_bytes;
        let precommit_error = store
            .run_decommission_capacity_non_growing_replacement_with_capacity_lease(
                target.target_pool_index,
                Some(target.capacity_owner),
                Some(aborting_data.len()),
                |_| async { Err::<(), Error>(Error::other("injected checkpoint failure before commit")) },
            )
            .await
            .expect_err("the first checkpoint attempt should fail before writing its target")
            .to_string();
        assert!(precommit_error.contains("injected checkpoint failure before commit"));
        assert!(
            store
                .has_decommission_capacity_temporary_mutation_state(target.target_pool_index, target.capacity_owner)
                .await,
            "a failed checkpoint attempt must retain exact retry state"
        );
        let barrier = crate::set_disk::PutObjectCommitBarrier::install(
            RUSTFS_META_BUCKET,
            &manifest_name,
            crate::set_disk::PutObjectCommitPause::BeforeQuotaRename,
        );
        let worker_store = store.clone();
        let worker_manifest = manifest_name.clone();
        let worker = tokio::spawn(async move {
            recover_test_tier_delete_dispatch_manifest_with_page_budget(worker_store, &worker_manifest, Duration::from_secs(30))
                .await
        });
        barrier.wait_until_paused().await;
        assert!(
            store
                .has_decommission_capacity_temporary_mutation_state(target.target_pool_index, target.capacity_owner)
                .await,
            "the target PUT must persist its capacity intent before commit"
        );

        shutdown.cancel();
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(tier_delete_dispatch_manifest_recovery_count_for_test(&store), 1);
        assert!(
            tier_delete_dispatch_manifest_operation_lock_held_for_test(store.clone(), &manifest_name).await,
            "the target checkpoint PUT must retain its operation lock after shutdown"
        );
        assert!(
            crate::services::notification_sys::tier_delete_journal_fleet_proof_has_inflight_for_test(),
            "the target checkpoint PUT must retain its fleet-proof permit after shutdown"
        );

        barrier.release();
        assert!(
            !worker.await.expect("checkpoint shutdown worker should join"),
            "cooperative shutdown should retain the partially committed checkpoint"
        );
        assert_eq!(tier_delete_dispatch_manifest_recovery_count_for_test(&store), 0);
        assert_eq!(
            com::read_config(store.pools[target.target_pool_index].clone(), &manifest_name)
                .await
                .expect("the committed target checkpoint should be readable"),
            aborting_data,
            "the admitted target PUT must finish before its fences release"
        );
        assert!(
            !store
                .has_decommission_capacity_temporary_mutation_state(target.target_pool_index, target.capacity_owner)
                .await,
            "the admitted target PUT must drain its capacity transaction before releasing the recovery fences"
        );
        {
            let pool_meta = store.pool_meta.read().await;
            let reservation = pool_meta.pools[0]
                .decommission
                .as_ref()
                .and_then(|info| info.capacity_reservation.as_ref())
                .expect("checkpoint shutdown capacity reservation should remain active");
            let capacity_target = reservation
                .targets
                .iter()
                .find(|candidate| candidate.pool_index == target.target_pool_index)
                .expect("checkpoint shutdown capacity target should remain allocated");
            assert_eq!(
                reservation.consumed_target_physical_bytes, consumed_before_checkpoint,
                "a non-growing checkpoint replacement must not consume durable migration capacity"
            );
            assert_eq!(reservation.pending_target_physical_bytes, 0);
            assert_eq!(reservation.inflight_target_physical_bytes, 0);
            assert_eq!(capacity_target.pending_physical_bytes, 0);
            assert!(capacity_target.temporary_mutations.is_empty());
        }
        drop(barrier);
        let mut reloaded_pool_meta = PoolMeta::default();
        reloaded_pool_meta
            .load(store.pools[0].clone(), store.pools.clone())
            .await
            .expect("checkpoint shutdown pool metadata should reload from disk");
        *store.pool_meta.write().await = reloaded_pool_meta;
        recover_test_tier_delete_dispatch_manifest(store.clone(), &manifest_name)
            .await
            .expect("a fresh recovery attempt should replay the committed checkpoint and receipt");
        assert!(
            !store
                .has_decommission_capacity_temporary_mutation_state(target.target_pool_index, target.capacity_owner)
                .await,
            "durable replay must reconcile the exact capacity transaction"
        );
        assert_eq!(
            store
                .decommission_durable_ilm_receipt_count_for_test(0)
                .await
                .expect("checkpoint shutdown receipts should be listable"),
            2,
            "durable replay must advance both the manifest and journal receipts"
        );
        drop(ctx);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn v6_dispatch_recovery_advances_decommission_receipts_and_cleans_target_copies() {
        let temp_dir = tempfile::tempdir().expect("create v6 decommission receipt store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "v6-dispatch-decommission-receipts", &[4, 4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "v6-dispatch-decommission-receipts-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("receipt bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("receipt bucket incarnation should resolve");
        let entry = synthetic_v6_dispatch_entry(
            bucket,
            "archive/receipt.bin",
            "RECEIPT-TIER",
            [17; 32],
            &uuid::Uuid::new_v4().to_string(),
        );
        let (manifest_name, entries) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "archive/",
            vec![(entry, Some(TierDeleteJournalState::Prepared))],
            TierDeleteDispatchManifestState::Preparing,
        )
        .await
        .expect("Preparing v6 fixture should persist");
        let journal_name = tier_delete_journal_object_name(&entries[0]);
        let manifest_data = com::read_config(store.clone(), &manifest_name)
            .await
            .expect("manifest fixture should be readable");
        let journal_data = com::read_config(store.clone(), &journal_name)
            .await
            .expect("journal fixture should be readable");

        // Model an in-flight decommission after both records have been copied
        // to its target pool but before their state machines advance.
        for pool in &store.pools {
            com::save_config(pool.clone(), &manifest_name, manifest_data.clone())
                .await
                .expect("manifest copy should persist in each pool");
            com::save_config(pool.clone(), &journal_name, journal_data.clone())
                .await
                .expect("journal copy should persist in each pool");
        }
        // Exercise the same durable capacity reservation and identity that a
        // real decommission start installs. A hand-written active flag makes
        // every target mutation look external and masks whether recovery is
        // correctly charged to the decommission owner.
        mark_test_pool_decommissioning(&store, 0).await;

        let missing_receipt_error = recover_test_tier_delete_dispatch_manifest(store.clone(), &manifest_name)
            .await
            .expect_err("an active decommission without receipt coverage must block v6 checkpoint mutation")
            .to_string();
        assert!(
            missing_receipt_error.contains("missing receipt coverage"),
            "unexpected missing-receipt error: {missing_receipt_error}"
        );
        assert_eq!(
            com::read_config(store.pools[1].clone(), &manifest_name)
                .await
                .expect("blocked checkpoint must retain the target manifest generation"),
            manifest_data,
            "missing receipt coverage must fail before mutating a target copy"
        );

        for (path, data) in [
            (&manifest_name, manifest_data.as_slice()),
            (&journal_name, journal_data.as_slice()),
        ] {
            let record = validate_durable_ilm_record(path, data).expect("v6 durable record should validate");
            store
                .persist_decommission_durable_ilm_receipt_for_test(0, 1, path, &record, false)
                .await
                .expect("initial decommission receipt should persist");
        }

        // Model a crash after the target checkpoint PUT committed but before
        // its temporary-capacity intent was resolved. Recovery must recognize
        // the exact target bytes and finish that same deterministic intent
        // instead of treating the checkpoint as an uncharged success.
        let (aborting_data, preparing_etag) = test_tier_delete_dispatch_manifest_checkpoint(
            store.clone(),
            &manifest_name,
            TierDeleteDispatchManifestState::Aborting,
        )
        .await
        .expect("the Aborting checkpoint should encode");
        let checkpoint_targets = store
            .decommission_durable_ilm_checkpoint_targets(&manifest_name, &aborting_data, &preparing_etag)
            .await
            .expect("receipt-bearing checkpoint targets should resolve")
            .expect("an active decommission should own the checkpoint");
        assert_eq!(checkpoint_targets.len(), 1);
        let checkpoint_target = checkpoint_targets
            .into_iter()
            .next()
            .expect("one checkpoint target should exist");
        let checkpoint_owner = checkpoint_target.capacity_owner;
        let target_pool_index = checkpoint_target.target_pool_index;
        let injected_write_error = store
            .run_decommission_capacity_temporary_mutation_with_capacity_lease(
                target_pool_index,
                Some(checkpoint_owner),
                Some(aborting_data.len()),
                |_| {
                    let target_pool = store.pools[target_pool_index].clone();
                    let manifest_name = manifest_name.clone();
                    let aborting_data = aborting_data.clone();
                    async move {
                        com::save_config(target_pool, &manifest_name, aborting_data)
                            .await
                            .expect("the injected target checkpoint should commit");
                        Err::<(), Error>(Error::other("injected crash after checkpoint target commit"))
                    }
                },
            )
            .await
            .expect_err("the injected post-commit crash should leave a pending capacity intent")
            .to_string();
        assert!(injected_write_error.contains("injected crash after checkpoint target commit"));
        assert!(
            store
                .has_decommission_capacity_temporary_mutation_state(target_pool_index, checkpoint_owner)
                .await,
            "the committed target must retain its exact unresolved capacity intent"
        );

        recover_test_tier_delete_dispatch_manifest(store.clone(), &manifest_name)
            .await
            .expect("Preparing rollback should advance receipts and finish terminal cleanup");
        assert!(
            !store
                .has_decommission_capacity_temporary_mutation_state(target_pool_index, checkpoint_owner)
                .await,
            "already-committed checkpoint recovery must resolve the pending capacity intent"
        );
        let stats = recover_tier_delete_dispatch_manifests(store.clone(), 100, None)
            .await
            .expect("the terminal source checkpoint should remain non-actionable");
        assert_eq!((stats.scanned, stats.deleted, stats.retained, stats.failed), (1, 0, 1, 0));
        assert_eq!(
            store
                .decommission_durable_ilm_receipt_count_for_test(0)
                .await
                .expect("v6 terminal receipts should be listable"),
            2
        );

        {
            let _proof_guard = crate::services::notification_sys::without_cross_pool_fence_fleet_proof_for_test();
            let proof_error = store
                .verify_and_cleanup_decommissioned_durable_ilm_record_for_test(
                    0,
                    store.pools[0].get_disks_by_key(&manifest_name),
                    &manifest_name,
                )
                .await
                .expect_err("revoked v6 fleet proof must block final source cleanup")
                .to_string();
            assert!(
                proof_error.contains("fleet capability is unavailable"),
                "unexpected revoked-proof cleanup error: {proof_error}"
            );
            assert!(
                com::read_config(store.pools[0].clone(), &manifest_name).await.is_ok(),
                "revoked fleet proof must retain the last source manifest copy"
            );
        }

        for path in [&manifest_name, &journal_name] {
            store
                .verify_and_cleanup_decommissioned_durable_ilm_record_for_test(0, store.pools[0].get_disks_by_key(path), path)
                .await
                .expect("terminal receipt should authorize verified source cleanup");
        }
        let stats = recover_tier_delete_dispatch_manifests(store.clone(), 100, None)
            .await
            .expect("verified source cleanup should remove the last manifest copy");
        assert_eq!((stats.scanned, stats.deleted, stats.failed), (0, 0, 0));
        for path in [&manifest_name, &journal_name] {
            for pool in &store.pools {
                assert!(
                    matches!(com::read_config(pool.clone(), path).await, Err(Error::ConfigNotFound)),
                    "terminal receipt cleanup must remove `{path}` from every staged pool"
                );
            }
        }
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn authorized_all_committed_manifest_recovers_after_last_commit_crash() {
        let temp_dir = tempfile::tempdir().expect("create authorized manifest recovery store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "authorized-manifest-last-commit", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let bucket = "authorized-manifest-last-commit-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("authorized recovery bucket should be created");
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("bucket incarnation should resolve");
        let tier_name = "AUTHORIZED-LAST-COMMIT";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let lease = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve");
        let mut entries = Vec::new();
        for index in 0..2 {
            let remote_version = uuid::Uuid::new_v4().to_string();
            let entry = synthetic_v6_dispatch_entry(
                bucket,
                &format!("archive/{index}.bin"),
                tier_name,
                lease.backend_identity(),
                &remote_version,
            );
            backend.set_put_remote_version(Some(remote_version)).await;
            lease
                .put(&entry.obj_name, ReaderImpl::Body(bytes::Bytes::from_static(b"remote candidate")), 16)
                .await
                .expect("remote candidate should be seeded");
            entries.push((entry, Some(TierDeleteJournalState::Committed)));
        }
        let (manifest_name, _) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            "archive/",
            entries,
            TierDeleteDispatchManifestState::DispatchAuthorized,
        )
        .await
        .expect("last-commit crash fixture should persist");

        let manifest_stats = recover_tier_delete_dispatch_manifests(store.clone(), 100, None)
            .await
            .expect("all-Committed Authorized manifest should advance");
        assert_eq!((manifest_stats.scanned, manifest_stats.advanced, manifest_stats.failed), (1, 1, 0));
        assert_eq!(
            test_tier_delete_dispatch_manifest_state(store.clone(), &manifest_name)
                .await
                .expect("advanced manifest should remain readable"),
            Some(TierDeleteDispatchManifestState::Completed)
        );
        let journal_stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("Completed journal set should recover remote candidates");
        assert_eq!((journal_stats.scanned, journal_stats.deleted, journal_stats.failed), (2, 2, 0));
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, 1);
        let gc_stats = recover_tier_delete_dispatch_manifests(store.clone(), 100, None)
            .await
            .expect("manifest coordinator should remove the empty Completed manifest");
        assert_eq!((gc_stats.scanned, gc_stats.deleted, gc_stats.failed), (1, 1, 0));
        assert_eq!(tier_delete_dispatch_manifest_count(store).await, 0);
        assert_eq!(backend.object_count().await, 0);
        assert_eq!(backend.remove_versions().await.len(), 2);
    }

    #[cfg(feature = "test-util")]
    async fn transition_transaction_record_count(store: Arc<crate::store::ECStore>) -> usize {
        store
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                TRANSITION_TRANSACTION_RECORD_PREFIX,
                None,
                None,
                100,
                false,
                None,
                false,
            )
            .await
            .expect("transition transaction records should be listable")
            .objects
            .len()
    }

    #[cfg(feature = "test-util")]
    async fn register_transition_reconcile_test_tier(
        handle: &Arc<tokio::sync::RwLock<TierConfigMgr>>,
        tier_name: &str,
    ) -> MockWarmBackend {
        let backend = MockWarmBackend::new();
        let mut manager = handle.write().await;
        manager.tiers.insert(
            tier_name.to_string(),
            TierConfig {
                version: "v1".to_string(),
                tier_type: TierType::Wasabi,
                name: tier_name.to_string(),
                wasabi: Some(TierWasabi {
                    name: tier_name.to_string(),
                    endpoint: "https://s3.wasabisys.com".to_string(),
                    access_key: "test-access-key".to_string(),
                    secret_key: "test-secret-key".to_string(),
                    bucket: "mock-tier".to_string(),
                    prefix: format!("mock/{}/", uuid::Uuid::new_v4()),
                    region: "us-east-1".to_string(),
                }),
                ..Default::default()
            },
        );
        manager
            .install_test_driver(tier_name, Box::new(backend.clone()))
            .expect("mock fallback tier driver should install");
        backend
    }

    #[cfg(feature = "test-util")]
    async fn wait_for_tier_delete_journal_recovery(
        store: Arc<crate::store::ECStore>,
        backend: &MockWarmBackend,
        expected_removes: usize,
    ) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if backend.remove_versions().await.len() >= expected_removes
                    && tier_delete_journal_count(store.clone()).await == 0
                {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("tier delete journal recovery should complete");
    }

    #[cfg(feature = "test-util")]
    async fn wait_for_tier_free_version_recovery(
        store: Arc<crate::store::ECStore>,
        backend: &MockWarmBackend,
        expected_removes: usize,
    ) {
        ExpiryState::resize_workers(1, store.clone()).await;
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let stats = recover_tier_free_versions(store.clone(), 100, None, None)
                    .await
                    .expect("tier free-version recovery scan should succeed");
                if backend.remove_versions().await.len() >= expected_removes && stats.enqueued == 0 && stats.failed == 0 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("tier free-version recovery should complete");
    }

    #[cfg(feature = "test-util")]
    async fn wait_for_expiry_workers_idle(store: &crate::store::ECStore) {
        let expiry_state = store.ctx.expiry_state();
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let idle = {
                    let state = expiry_state.read().await;
                    state.pending_tasks() == 0 && state.active_tasks() == 0
                };
                if idle {
                    // Recheck after a scheduler turn so the tiny hand-off
                    // between dequeue and active-task accounting cannot make
                    // the test observe a false idle state.
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    let state = expiry_state.read().await;
                    if state.pending_tasks() == 0 && state.active_tasks() == 0 {
                        return;
                    }
                } else {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            }
        })
        .await
        .expect("lifecycle expiry workers should become idle");
    }

    #[cfg(feature = "test-util")]
    async fn run_transitioned_delete_free_version_owner_case(
        store_name: &str,
        tier_name: &str,
        bucket: &str,
        object: &str,
        restore_before_delete: bool,
        causal_enqueue: bool,
        delete_with_journal: bool,
    ) {
        let temp_dir = tempfile::tempdir().expect("create transitioned delete store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), store_name, &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("source bucket should be created");
        let mut reader = PutObjReader::from_vec(vec![b'd'; 1024 * 1024]);
        let original = store
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        store
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: original.etag.clone().expect("source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("source transition should commit");
        assert_eq!(backend.object_count().await, 1, "transition should create one remote object");
        if restore_before_delete {
            store
                .clone()
                .restore_transitioned_object(
                    bucket,
                    object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            restore_request: s3s::dto::RestoreRequest {
                                days: Some(1),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        ..Default::default()
                    },
                )
                .await
                .expect("restore should complete before transitioned delete");
            let restored = store
                .get_object_info(bucket, object, &ObjectOptions::default())
                .await
                .expect("restored transitioned source should remain readable");
            assert!(
                restored.user_defined.contains_key(s3s::header::X_AMZ_RESTORE.as_str()),
                "restore completion metadata must be present before the delete regression"
            );
        }

        if causal_enqueue {
            ExpiryState::resize_workers(1, store.clone()).await;
        }
        backend.set_remove_failure(!causal_enqueue);
        if delete_with_journal {
            store
                .delete_object_with_tier_delete_journal(bucket, object, ObjectOptions::default())
                .await
                .expect("transitioned source journal-wrapper delete should commit");
        } else {
            store
                .delete_object(bucket, object, ObjectOptions::default())
                .await
                .expect("transitioned source plain object-layer delete should commit");
        }

        if causal_enqueue {
            tokio::time::timeout(Duration::from_secs(30), async {
                loop {
                    let metadata_absent = store.pools[0]
                        .get_disks_by_key(object)
                        .load_file_info_versions_exact(bucket, object)
                        .await
                        .expect("causal free-version cleanup metadata should remain readable")
                        .is_none();
                    if metadata_absent && backend.remove_versions().await.len() == 1 {
                        return;
                    }
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .expect("committed free-version should be cleaned without a recovery scan");
            assert_eq!(backend.object_count().await, 0, "causal cleanup should remove the remote object");
            assert_eq!(
                tier_delete_journal_count(store.clone()).await,
                0,
                "ordinary causal cleanup must not create a journal"
            );
            store
                .delete_bucket(bucket, &DeleteBucketOptions::default())
                .await
                .expect("bucket delete should succeed after causal free-version cleanup");
            return;
        }

        let local_versions = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("post-delete metadata should remain decodable");
        let local_versions = local_versions.expect("ordinary delete must leave a durable free-version cleanup owner");
        assert_eq!(
            local_versions
                .versions
                .iter()
                .chain(local_versions.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            1,
            "ordinary delete must leave exactly one hidden free-version owner"
        );
        assert_eq!(
            local_versions
                .versions
                .iter()
                .filter(|version| !version.tier_free_version())
                .count(),
            0,
            "ordinary delete must remove the visible transitioned source"
        );
        let listed = store
            .clone()
            .list_object_versions(bucket, "", None, None, None, 100)
            .await
            .expect("source versions should be listable");
        assert!(listed.objects.is_empty(), "source must have no visible versions after delete");

        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            0,
            "ordinary delete must not create a journal"
        );

        backend.set_remove_failure(false);
        ExpiryState::resize_workers(1, store.clone()).await;
        let recovered = recover_tier_free_versions(store.clone(), 100, None, None)
            .await
            .expect("free-version recovery should scan the hidden owner");
        assert_eq!((recovered.enqueued, recovered.failed), (1, 0));
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let metadata_absent = store.pools[0]
                    .get_disks_by_key(object)
                    .load_file_info_versions_exact(bucket, object)
                    .await
                    .expect("free-version cleanup metadata should remain readable")
                    .is_none();
                if metadata_absent && backend.remove_versions().await.len() == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("free-version recovery should delete the remote and exact local owner");
        assert_eq!(backend.object_count().await, 0, "free-version recovery should remove the remote object");
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("ordinary bucket delete should succeed after the last transitioned object");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transitioned_delete_uses_free_version_as_cleanup_owner() {
        run_transitioned_delete_free_version_owner_case(
            "transitioned-delete-journal-owner",
            "DELETEOWNER",
            "transitioned-delete-journal-owner-bucket",
            "transition/archive.bin",
            false,
            false,
            true,
        )
        .await;
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn restored_transitioned_delete_uses_free_version_as_cleanup_owner() {
        run_transitioned_delete_free_version_owner_case(
            "restored-transitioned-delete-journal-owner",
            "RESTOREDELETEOWNER",
            "restored-transitioned-delete-journal-owner-bucket",
            "transition/archive.bin",
            true,
            false,
            true,
        )
        .await;
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transitioned_delete_causally_enqueues_free_version() {
        run_transitioned_delete_free_version_owner_case(
            "transitioned-delete-causal-enqueue",
            "DELETE-CAUSAL",
            "transitioned-delete-causal-enqueue-bucket",
            "transition/archive.bin",
            false,
            true,
            false,
        )
        .await;
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn restored_transitioned_delete_causally_enqueues_free_version() {
        run_transitioned_delete_free_version_owner_case(
            "restored-transitioned-delete-causal-enqueue",
            "RESTORE-DELETE-CAUSAL",
            "restored-transitioned-delete-causal-enqueue-bucket",
            "transition/archive.bin",
            true,
            true,
            true,
        )
        .await;
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn force_tier_remove_blocks_on_physical_free_version_hidden_by_other_pool() {
        let temp_dir = tempfile::tempdir().expect("create tier reference store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-free-reference-proof", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "FREE-PROOF";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("test tier identity should resolve")
            .backend_identity();
        let bucket = "tier-free-reference-proof-bucket";
        let object = "archive.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("source bucket should be created");
        let mut reader = PutObjReader::from_vec(vec![b'f'; 1024 * 1024]);
        let original = store.pools[0]
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        store.pools[0]
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: original.etag.clone().expect("source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("source transition should commit");
        backend.set_remove_failure(true);
        store.pools[0]
            .delete_object(
                bucket,
                object,
                ObjectOptions {
                    tier_delete_journal_api: Some(store.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("journal-less compatibility delete should retain a free-version");

        let exact = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("free-version metadata should remain decodable")
            .expect("free-version metadata should remain on disk");
        assert_eq!(
            exact
                .versions
                .iter()
                .chain(exact.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            1,
            "fixture must contain one real serialized hidden free-version"
        );

        let mut hot_reader = PutObjReader::from_vec(vec![b'h'; 1024 * 1024]);
        store.pools[1]
            .put_object(
                bucket,
                object,
                &mut hot_reader,
                &ObjectOptions {
                    mod_time: Some(OffsetDateTime::now_utc() + time::Duration::hours(1)),
                    ..Default::default()
                },
            )
            .await
            .expect("second pool hot source should be written");
        let merged = store
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("merged lookup should return the second-pool hot source");
        assert!(
            merged.transitioned_object.status.is_empty() && !merged.transitioned_object.free_version,
            "the ordinary second-pool source must hide the first-pool free-version from merged lookup"
        );

        let err = ensure_no_authoritative_tier_references_for_test(store, tier_name, backend_identity, true)
            .await
            .expect_err("force tier removal must not bypass a physical free-version cleanup obligation");
        assert_eq!(err.code, ERR_TIER_BACKEND_IN_USE.code);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_reference_proof_fails_closed_when_physical_bucket_topology_is_incomplete() {
        let temp_dir = tempfile::tempdir().expect("create incomplete tier reference store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-reference-incomplete-topology", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "INCOMPLETE-PROOF";
        register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("test tier identity should resolve")
            .backend_identity();
        store.pools[0].disk_set[0].disks.write().await[0] = None;

        let err = ensure_no_authoritative_tier_references_for_test(store, tier_name, backend_identity, true)
            .await
            .expect_err("an incomplete physical bucket enumeration must block tier mutation");
        assert_eq!(err.code, ERR_TIER_INVALID_CONFIG.code);
        assert!(
            err.message.contains("complete set quorum"),
            "unexpected reference proof error: {}",
            err.message
        );
    }

    #[cfg(feature = "test-util")]
    async fn copy_test_xlmeta_between_pools(
        root: &std::path::Path,
        source_pool: usize,
        target_pool: usize,
        bucket: &str,
        object: &str,
    ) {
        for disk_index in 0..4 {
            let source = root.join(format!("pool{source_pool}/set0/disk{disk_index}/{bucket}/{object}/{STORAGE_FORMAT_FILE}"));
            let target = root.join(format!("pool{target_pool}/set0/disk{disk_index}/{bucket}/{object}/{STORAGE_FORMAT_FILE}"));
            tokio::fs::create_dir_all(target.parent().expect("target xl.meta should have a parent"))
                .await
                .expect("target object directory should be created");
            tokio::fs::copy(&source, &target)
                .await
                .expect("xl.meta should copy exactly between pools");
        }
    }

    #[cfg(feature = "test-util")]
    async fn rewrite_transitioned_xlmeta_as_legacy_unknown(
        root: &std::path::Path,
        pool_index: usize,
        bucket: &str,
        object: &str,
        minio_unversioned: bool,
    ) {
        for disk_index in 0..4 {
            let metadata_path =
                root.join(format!("pool{pool_index}/set0/disk{disk_index}/{bucket}/{object}/{STORAGE_FORMAT_FILE}"));
            let encoded = tokio::fs::read(&metadata_path)
                .await
                .expect("transitioned xl.meta should be readable");
            let mut metadata = FileMeta::load(&encoded).expect("transitioned xl.meta should decode");
            let mut found = false;
            for shallow in &mut metadata.versions {
                let mut version = shallow
                    .parse_version_meta()
                    .expect("transitioned version record should decode");
                let Some(object_meta) = version.object.as_mut() else {
                    continue;
                };
                if rustfs_utils::http::metadata_compat::get_bytes(
                    &object_meta.meta_sys,
                    rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_STATUS,
                )
                .as_deref()
                    != Some(rustfs_filemeta::TRANSITION_COMPLETE.as_bytes())
                {
                    continue;
                }
                found = true;
                for suffix in [
                    rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
                    rustfs_utils::http::metadata_compat::SUFFIX_TRANSITIONED_VERSION_STATE,
                    rustfs_utils::http::metadata_compat::SUFFIX_TRANSITIONED_VERSION_ID,
                ] {
                    rustfs_utils::http::metadata_compat::remove_bytes(&mut object_meta.meta_sys, suffix);
                }
                if minio_unversioned {
                    object_meta
                        .meta_sys
                        .insert("x-minio-internal-transitioned-versionID".to_string(), Vec::new());
                }
                *shallow = rustfs_filemeta::FileMetaShallowVersion::try_from(version)
                    .expect("legacy transitioned version should re-encode");
            }
            assert!(found, "transitioned source should exist in serialized xl.meta");
            tokio::fs::write(&metadata_path, metadata.marshal_msg().expect("legacy transitioned xl.meta should encode"))
                .await
                .expect("legacy transitioned xl.meta should persist");
        }
    }

    #[cfg(feature = "test-util")]
    async fn read_store_body(
        store: &Arc<crate::store::ECStore>,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        opts: &ObjectOptions,
    ) -> Vec<u8> {
        let mut reader = store
            .get_object_reader(bucket, object, range, HeaderMap::new(), opts)
            .await
            .expect("object reader should open");
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("object body should drain");
        body
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn legacy_unknown_unversioned_transition_supports_head_get_and_range_without_backfill() {
        let temp_dir = tempfile::tempdir().expect("create legacy unknown unversioned store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "legacy-unknown-unversioned-read", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "LEGACY-UNKNOWN-UNVERSIONED-READ";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        backend.set_put_remote_version(Some(String::new())).await;
        let bucket = "legacy-unknown-unversioned-read-bucket";
        let object = "object.bin";
        let payload = b"legacy unversioned remote tier object remains readable".repeat(1024);
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("legacy source bucket should be created");
        let mut reader = PutObjReader::from_vec(payload.clone());
        let source = store
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("legacy source should be written");
        store
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: source.etag.clone().expect("legacy source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("legacy source should transition");
        rewrite_transitioned_xlmeta_as_legacy_unknown(temp_dir.path(), 0, bucket, object, true).await;
        backend.clear_op_log().await;

        let opts = ObjectOptions {
            metadata_cache_safe: false,
            ..Default::default()
        };
        let head = store
            .get_object_info(bucket, object, &opts)
            .await
            .expect("legacy transitioned HEAD should use local metadata");
        assert_eq!(head.transition_version_state, rustfs_filemeta::TransitionVersionState::Unknown);
        assert!(head.transitioned_object.version_id.is_empty());
        assert_eq!(
            head.user_defined
                .get("x-minio-internal-transitioned-versionID")
                .map(String::as_str),
            Some(""),
            "the MinIO empty version-key provenance must survive xl.meta decoding"
        );
        assert!(
            !rustfs_utils::http::metadata_compat::contains_key_str(
                &head.user_defined,
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITIONED_VERSION_STATE,
            ),
            "the compatibility read must not synthesize version-state metadata"
        );

        let full_body = read_store_body(&store, bucket, object, None, &opts).await;
        assert_eq!(full_body, payload);

        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 7,
            end: 38,
        };
        let ranged_body = read_store_body(&store, bucket, object, Some(range), &opts).await;
        assert_eq!(ranged_body, &payload[7..=38]);

        let after_read = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("legacy metadata should remain readable after GET")
            .expect("legacy object metadata should remain on disk")
            .versions
            .into_iter()
            .find(|version| version.transition_status == rustfs_filemeta::TRANSITION_COMPLETE)
            .expect("legacy transitioned source should remain visible after GET");
        assert_eq!(after_read.transition_version_state, rustfs_filemeta::TransitionVersionState::Unknown);
        assert!(after_read.transition_version.is_none());
        assert!(after_read.transition_version_id.is_none());
        assert_eq!(
            after_read
                .metadata
                .get("x-minio-internal-transitioned-versionID")
                .map(String::as_str),
            Some(""),
            "the MinIO empty version-key provenance must remain after GET and Range GET"
        );
        assert!(
            !rustfs_utils::http::metadata_compat::contains_key_str(
                &after_read.metadata,
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITIONED_VERSION_STATE,
            ),
            "the compatibility read must remain side-effect free"
        );

        assert_eq!(
            backend.op_log().await,
            vec![
                MockWarmOp::Probe {
                    object: after_read.transitioned_objname.clone(),
                },
                MockWarmOp::Get {
                    object: after_read.transitioned_objname.clone(),
                },
                MockWarmOp::Probe {
                    object: after_read.transitioned_objname.clone(),
                },
                MockWarmOp::Get {
                    object: after_read.transitioned_objname,
                },
            ],
            "legacy reads should probe before each unversioned GET and never mutate local metadata"
        );
        assert_eq!(backend.remove_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn legacy_unknown_transition_delete_falls_back_for_single_batch_and_blocks_prefix() {
        let temp_dir = tempfile::tempdir().expect("create legacy transitioned delete store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "legacy-unknown-transitioned-delete", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "LEGACY-UNKNOWN-DELETE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "legacy-unknown-transitioned-delete-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("legacy source bucket should be created");

        for (index, object) in ["single.bin", "batch.bin", "prefix/legacy.bin"].into_iter().enumerate() {
            let mut reader = PutObjReader::from_vec(vec![b'l' + index as u8; 1024 * 1024]);
            let source = store
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
                .expect("legacy source should be written");
            store
                .transition_object(
                    bucket,
                    object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name.to_string(),
                            etag: source.etag.clone().expect("legacy source should have an etag"),
                            ..Default::default()
                        },
                        mod_time: source.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("legacy source should transition");
            rewrite_transitioned_xlmeta_as_legacy_unknown(temp_dir.path(), 0, bucket, object, false).await;
            let legacy = store.pools[0]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("legacy source metadata should remain decodable")
                .expect("legacy source should remain on disk")
                .versions
                .into_iter()
                .find(|version| version.transition_status == rustfs_filemeta::TRANSITION_COMPLETE)
                .expect("legacy transitioned source should remain visible before delete");
            assert_eq!(legacy.transition_version_state, rustfs_filemeta::TransitionVersionState::Unknown);
            assert_eq!(
                crate::services::tier::tier::tier_destination_id_from_metadata(&legacy.metadata)
                    .expect("legacy metadata identity lookup should not fail"),
                None
            );
        }

        backend.set_remove_failure(true);
        store
            .delete_object_with_tier_delete_journal(bucket, "single.bin", ObjectOptions::default())
            .await
            .expect("legacy single delete should retain its free-version fallback");
        let free_version_prefix_error = store
            .delete_object_with_tier_delete_journal(
                bucket,
                "single.bin",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("recursive prefix delete must not discard an existing hidden cleanup owner");
        assert!(
            free_version_prefix_error
                .to_string()
                .contains("tier free-version cleanup obligation"),
            "unexpected hidden-owner prefix failure: {free_version_prefix_error}"
        );
        let (_deleted, errors) = store
            .delete_objects_with_tier_delete_journal(
                bucket,
                vec![ObjectToDelete {
                    object_name: "batch.bin".to_string(),
                    ..Default::default()
                }],
                ObjectOptions::default(),
            )
            .await;
        assert!(errors.iter().all(Option::is_none), "legacy batch delete should succeed: {errors:?}");
        let prefix_error = store
            .delete_object_with_tier_delete_journal(
                bucket,
                "prefix/",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("legacy recursive prefix delete must fail closed before physical removal");
        assert!(
            prefix_error.to_string().contains("legacy Unknown"),
            "unexpected prefix failure: {prefix_error}"
        );

        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            0,
            "legacy cleanup must not create an unsafe journal"
        );
        assert_eq!(
            backend.object_count().await,
            3,
            "failed free-version cleanup must retain all remote objects"
        );
        for object in ["single.bin", "batch.bin"] {
            let exact = store.pools[0]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("legacy post-delete metadata should decode")
                .expect("legacy cleanup owner should remain on disk");
            assert_eq!(
                exact
                    .versions
                    .iter()
                    .chain(exact.free_versions.iter())
                    .filter(|version| version.tier_free_version())
                    .count(),
                1,
                "legacy {object} must retain exactly one hidden cleanup owner"
            );
            assert_eq!(
                exact.versions.iter().filter(|version| !version.tier_free_version()).count(),
                0,
                "legacy {object} must have no visible source after delete"
            );
        }
        let retained_prefix = store.pools[0]
            .get_disks_by_key("prefix/legacy.bin")
            .load_file_info_versions_exact(bucket, "prefix/legacy.bin")
            .await
            .expect("rejected prefix metadata should decode")
            .expect("rejected prefix delete must retain its source");
        assert_eq!(
            retained_prefix
                .versions
                .iter()
                .filter(|version| !version.tier_free_version())
                .count(),
            1
        );
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn stable_transitioned_recursive_prefix_delete_uses_journal_owners() {
        let temp_dir = tempfile::tempdir().expect("create stable transitioned prefix-delete store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "stable-transitioned-prefix-delete", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "STABLE-PREFIX-DELETE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "stable-transitioned-prefix-delete-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("stable prefix source bucket should be created");

        let objects = ["prefix/first.bin", "prefix/second.bin"];
        for (index, object) in objects.into_iter().enumerate() {
            let mut reader = PutObjReader::from_vec(vec![b'a' + index as u8; 1024 * 1024]);
            let source = store
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
                .expect("stable prefix source should be written");
            store
                .transition_object(
                    bucket,
                    object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name.to_string(),
                            etag: source.etag.clone().expect("stable prefix source should have an etag"),
                            ..Default::default()
                        },
                        mod_time: source.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("stable prefix source should transition");
        }
        assert_eq!(backend.object_count().await, objects.len());
        backend.set_remove_failure(true);

        store
            .delete_object_with_tier_delete_journal(
                bucket,
                "prefix/",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect("stable recursive prefix delete should use journal ownership");

        assert_eq!(tier_delete_journal_count(store.clone()).await, objects.len());
        assert_eq!(backend.object_count().await, objects.len());
        for object in objects {
            assert!(
                store.pools[0]
                    .get_disks_by_key(object)
                    .load_file_info_versions_exact(bucket, object)
                    .await
                    .expect("stable prefix metadata lookup should succeed")
                    .is_none(),
                "stable recursive delete must leave neither a visible source nor a free-version for {object}"
            );
        }

        backend.set_remove_failure(false);
        let stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("stable prefix journals should recover");
        assert_eq!(stats.failed, 0);
        assert!(stats.scanned <= objects.len() && stats.deleted <= objects.len());
        assert_eq!(tier_delete_journal_count(store).await, 0);
        assert_eq!(backend.object_count().await, 0);
        assert_eq!(backend.remove_versions().await.len(), objects.len());
    }

    #[cfg(feature = "test-util")]
    #[test]
    #[serial_test::serial(storage_class_env)]
    fn tier_delete_prefix_limit_and_multi_chunk_batches_converge() {
        run_large_stack_async_test(
            "tier-delete-prefix-limit-and-multi-chunk",
            tier_delete_prefix_limit_and_multi_chunk_batches_converge_case,
        );
    }

    #[cfg(feature = "test-util")]
    async fn tier_delete_prefix_limit_and_multi_chunk_batches_converge_case() {
        let _batch_limit = TierDeleteDispatchBatchLimitGuard::install(2);
        let temp_dir = tempfile::tempdir().expect("create chunked prefix-delete store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "chunked-prefix-delete", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "CHUNKED-PREFIX-DELETE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "chunked-prefix-delete-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("chunked prefix source bucket should be created");

        for (prefix, count) in [("at-limit", 2), ("limit-plus-one", 3), ("multi-chunk", 5)] {
            for index in 0..count {
                let object = format!("{prefix}/object-{index}.bin");
                let mut reader = PutObjReader::from_vec(vec![b'a' + index as u8; 1024 * 1024]);
                let source = store
                    .put_object(bucket, &object, &mut reader, &ObjectOptions::default())
                    .await
                    .expect("chunked prefix source should be written");
                store
                    .transition_object(
                        bucket,
                        &object,
                        &ObjectOptions {
                            transition: TransitionOptions {
                                status: TRANSITION_PENDING.to_string(),
                                tier: tier_name.to_string(),
                                etag: source.etag.clone().expect("chunked prefix source should have an etag"),
                                ..Default::default()
                            },
                            mod_time: source.mod_time,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("chunked prefix source should transition");
            }
        }
        backend.set_remove_failure(true);

        store
            .delete_object_with_tier_delete_journal(
                bucket,
                "at-limit/",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect("exactly one batch must retain the v1 one-shot path");

        let limit_plus_one_first = store
            .delete_object_with_tier_delete_journal(
                bucket,
                "limit-plus-one/",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("limit plus one must start a bounded parent transaction");
        assert!(
            limit_plus_one_first.to_string().contains("retry the next durable batch"),
            "unexpected first limit-plus-one result: {limit_plus_one_first}"
        );
        let active_limit_plus_one_records = store
            .clone()
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                TIER_DELETE_DISPATCH_MANIFEST_PREFIX,
                None,
                None,
                10,
                false,
                None,
                false,
            )
            .await
            .expect("active limit-plus-one records should be listable");
        let mut active_limit_plus_one_parent_seen = false;
        for record in active_limit_plus_one_records
            .objects
            .iter()
            .filter(|record| !record.name.contains("/chunks/"))
        {
            let data = com::read_config(store.clone(), &record.name)
                .await
                .expect("an active dispatch root should be readable");
            let value: serde_json::Value = serde_json::from_slice(&data).expect("an active dispatch root should contain JSON");
            if value["prefix"] == "limit-plus-one/" {
                assert_eq!(value["record_type"], "chunked_parent");
                active_limit_plus_one_parent_seen = true;
            }
        }
        assert!(
            active_limit_plus_one_parent_seen,
            "limit plus one must establish the fail-closed parent at the legacy root"
        );

        let mut limit_plus_one_completed = false;
        let mut limit_plus_one_retries = 1;
        for _ in 0..3 {
            match store
                .delete_object_with_tier_delete_journal(
                    bucket,
                    "limit-plus-one/",
                    ObjectOptions {
                        delete_prefix: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => {
                    limit_plus_one_completed = true;
                    break;
                }
                Err(err) if err.to_string().contains("retry the next durable batch") => {
                    limit_plus_one_retries += 1;
                }
                Err(err) => panic!("limit-plus-one delete returned an unexpected error: {err}"),
            }
        }
        assert!(limit_plus_one_completed, "limit plus one must converge through two bounded children");
        assert_eq!(limit_plus_one_retries, 2, "limit plus one must require exactly two child batches");

        let first_batch = store
            .delete_object_with_tier_delete_journal(
                bucket,
                "multi-chunk/",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("the first bounded child must request a successor batch");
        assert!(
            first_batch.to_string().contains("retry the next durable batch"),
            "unexpected first child result: {first_batch}"
        );

        let newcomer = "multi-chunk/zzz-newcomer.bin";
        let mut newcomer_reader = PutObjReader::from_vec(vec![b'n'; 1024 * 1024]);
        let newcomer_source = store
            .put_object(bucket, newcomer, &mut newcomer_reader, &ObjectOptions::default())
            .await
            .expect("a source created between chunks should be written");
        store
            .transition_object(
                bucket,
                newcomer,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: newcomer_source
                            .etag
                            .clone()
                            .expect("the between-chunks source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: newcomer_source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("a source created between chunks should transition");

        let mut completed = false;
        let mut durable_batch_retries = 1;
        for _ in 0..7 {
            match store
                .delete_object_with_tier_delete_journal(
                    bucket,
                    "multi-chunk/",
                    ObjectOptions {
                        delete_prefix: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(_) => {
                    completed = true;
                    break;
                }
                Err(err)
                    if err.to_string().contains("retry the next durable batch")
                        || err.to_string().contains("retry the next batch") =>
                {
                    durable_batch_retries += 1;
                }
                Err(err) => panic!("chunked prefix delete returned an unexpected error: {err}"),
            }
        }
        assert!(completed, "six entries must converge through three bounded child batches");
        assert_eq!(durable_batch_retries, 3, "limit two should require exactly three child batches");
        assert_eq!(tier_delete_journal_count(store.clone()).await, 11);
        assert_eq!(
            backend.object_count().await,
            11,
            "remote cleanup must remain durable while the tier is unavailable"
        );
        let dispatch_records = store
            .clone()
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                TIER_DELETE_DISPATCH_MANIFEST_PREFIX,
                None,
                None,
                20,
                false,
                None,
                false,
            )
            .await
            .expect("bounded dispatch records should be listable");
        let mut child_batches = 0;
        let mut legacy_at_limit_seen = false;
        for record in &dispatch_records.objects {
            let data = com::read_config(store.clone(), &record.name)
                .await
                .expect("a retained dispatch record should be readable");
            let value: serde_json::Value = serde_json::from_slice(&data).expect("a retained dispatch record should contain JSON");
            if record.name.contains("/chunks/") {
                let journal_count = value["journal_count"]
                    .as_u64()
                    .expect("a retained child should declare its journal count");
                assert!(journal_count <= 2, "a child batch exceeded the configured resource bound");
                child_batches += 1;
            } else if value["prefix"] == "at-limit/" {
                assert!(
                    value.get("record_type").is_none(),
                    "the exact-limit root must remain a legacy v1 manifest"
                );
                assert_eq!(value["journal_count"], 2);
                legacy_at_limit_seen = true;
            }
        }
        assert!(
            legacy_at_limit_seen,
            "the exact-limit dispatch must retain its byte-compatible root shape"
        );
        assert_eq!(child_batches, 5, "nine chunked sources should persist exactly five bounded children");

        backend.set_remove_failure(false);
        drive_tier_delete_dispatch_restart_to_convergence(store.clone()).await;
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, 0);
        assert_eq!(backend.object_count().await, 0);
        assert_eq!(
            backend.remove_versions().await.len(),
            11,
            "each remote version must be removed exactly once"
        );
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[test]
    #[serial_test::serial(storage_class_env)]
    fn tier_delete_chunk_crash_boundaries_resume_without_skipping_sources() {
        run_large_stack_async_test(
            "tier-delete-chunk-crash-boundaries",
            tier_delete_chunk_crash_boundaries_resume_without_skipping_sources_case,
        );
    }

    #[cfg(feature = "test-util")]
    async fn tier_delete_chunk_crash_boundaries_resume_without_skipping_sources_case() {
        let _batch_limit = TierDeleteDispatchBatchLimitGuard::install(1);
        let stages = [
            TierDeleteChunkTestStage::ParentPersisted,
            TierDeleteChunkTestStage::ChildManifestPersisted,
            TierDeleteChunkTestStage::ParentBound,
            TierDeleteChunkTestStage::DispatchAuthorized,
            TierDeleteChunkTestStage::LocalReplayCompleted,
            TierDeleteChunkTestStage::ChildCompleted,
            TierDeleteChunkTestStage::ParentProgressed,
            TierDeleteChunkTestStage::FinalLocalDeletionCompleted,
            TierDeleteChunkTestStage::ParentCompleted,
        ];
        for (case, stage) in stages.into_iter().enumerate() {
            let temp_dir = tempfile::tempdir().expect("create chunk crash-boundary store dir");
            let initial_name = format!("chunk-crash-boundary-{case}");
            let (ctx, store, shutdown) =
                without_storage_class_env(build_isolated_test_store(temp_dir.path(), &initial_name, &[4])).await;
            crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
            let tier_name = format!("CHUNK-CRASH-BOUNDARY-{case}");
            let backend = register_mock_tier(&ctx.tier_config_mgr(), &tier_name).await;
            backend.set_remove_failure(true);
            let bucket = format!("chunk-crash-boundary-{case}-bucket");
            let prefix = "prefix/";
            store
                .make_bucket(&bucket, &MakeBucketOptions::default())
                .await
                .expect("chunk crash-boundary bucket should be created");
            for index in 0..2 {
                let object = format!("{prefix}object-{index}.bin");
                let mut reader = PutObjReader::from_vec(vec![b'a' + index as u8; 1024 * 1024]);
                let source = store
                    .put_object(&bucket, &object, &mut reader, &ObjectOptions::default())
                    .await
                    .expect("chunk crash-boundary source should be written");
                store
                    .transition_object(
                        &bucket,
                        &object,
                        &ObjectOptions {
                            transition: TransitionOptions {
                                status: TRANSITION_PENDING.to_string(),
                                tier: tier_name.clone(),
                                etag: source.etag.clone().expect("chunk crash-boundary source should have an etag"),
                                ..Default::default()
                            },
                            mod_time: source.mod_time,
                            ..Default::default()
                        },
                    )
                    .await
                    .expect("chunk crash-boundary source should transition");
            }
            let local_only = format!("{prefix}local-only.bin");
            let mut local_reader = PutObjReader::from_vec(vec![b'l'; 1024 * 1024]);
            store
                .put_object(&bucket, &local_only, &mut local_reader, &ObjectOptions::default())
                .await
                .expect("chunk crash-boundary local-only object should be written");
            let tier_config = ctx
                .tier_config_mgr()
                .read()
                .await
                .tiers
                .get(&tier_name)
                .expect("chunk crash-boundary tier config should remain available for restart")
                .clone_with_credentials();

            if matches!(
                stage,
                TierDeleteChunkTestStage::FinalLocalDeletionCompleted | TierDeleteChunkTestStage::ParentCompleted
            ) {
                for _ in 0..2 {
                    let retry = store
                        .delete_object_with_tier_delete_journal(
                            &bucket,
                            prefix,
                            ObjectOptions {
                                delete_prefix: true,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect_err("each bounded child must complete before the final crash boundary");
                    assert!(
                        retry.to_string().contains("retry the next durable batch"),
                        "unexpected bounded-child result before {stage:?}: {retry}"
                    );
                }
            }

            let barrier = TierDeleteChunkTestBarrier::install(stage);
            let worker_store = store.clone();
            let worker_bucket = bucket.clone();
            let worker = tokio::spawn(async move {
                worker_store
                    .delete_object_with_tier_delete_journal(
                        &worker_bucket,
                        prefix,
                        ObjectOptions {
                            delete_prefix: true,
                            ..Default::default()
                        },
                    )
                    .await
            });
            tokio::time::timeout(Duration::from_secs(30), barrier.wait_until_paused())
                .await
                .unwrap_or_else(|_| panic!("chunk delete did not reach crash boundary {stage:?}"));
            worker.abort();
            let _ = worker.await;
            drop(barrier);
            let released_bucket_guard =
                tokio::time::timeout(Duration::from_secs(5), store.acquire_bucket_lifecycle_write_lock(&bucket))
                    .await
                    .unwrap_or_else(|_| panic!("canceling at {stage:?} did not release the bucket lifecycle lock"))
                    .unwrap_or_else(|err| {
                        panic!("bucket lifecycle lock reacquire failed after cancellation at {stage:?}: {err}")
                    });
            drop(released_bucket_guard);
            shutdown.cancel();
            drop(store);
            drop(ctx);

            let restarted_name = format!("chunk-crash-boundary-{case}-restart");
            let (restarted_ctx, restarted_store, restarted_shutdown) =
                without_storage_class_env(build_isolated_test_store(temp_dir.path(), &restarted_name, &[4])).await;
            {
                let tier_config_mgr = restarted_ctx.tier_config_mgr();
                let mut manager = tier_config_mgr.write().await;
                manager.tiers.insert(tier_name.clone(), tier_config);
                manager
                    .install_test_driver(&tier_name, Box::new(backend.clone()))
                    .expect("the exact chunk crash-boundary tier driver should reinstall after restart");
            }
            crate::bucket::metadata_sys::init_bucket_metadata_sys(restarted_store.clone(), Vec::new()).await;

            let mut completed = false;
            for _ in 0..20 {
                let recovery = recover_tier_delete_dispatch_manifests(restarted_store.clone(), 100, None)
                    .await
                    .expect("chunk crash-boundary manifest recovery should remain readable");
                assert_eq!(recovery.failed, 0, "recovery must not quarantine a valid chunk boundary");
                match restarted_store
                    .delete_object_with_tier_delete_journal(
                        &bucket,
                        prefix,
                        ObjectOptions {
                            delete_prefix: true,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(_) => {
                        completed = true;
                        break;
                    }
                    Err(err)
                        if err.to_string().contains("retry")
                            || err.to_string().contains("rollback")
                            || err.to_string().contains("durable cleanup") => {}
                    Err(err) => panic!("chunk crash-boundary retry returned an unexpected error at {stage:?}: {err}"),
                }
            }
            assert!(completed, "chunk state must converge after cancellation at {stage:?}");
            for index in 0..2 {
                let object = format!("{prefix}object-{index}.bin");
                assert!(
                    restarted_store.pools[0]
                        .get_disks_by_key(&object)
                        .load_file_info_versions_exact(&bucket, &object)
                        .await
                        .expect("chunk crash-boundary source lookup should succeed")
                        .is_none(),
                    "source {object} must not be skipped after cancellation at {stage:?}"
                );
            }
            assert!(
                restarted_store.pools[0]
                    .get_disks_by_key(&local_only)
                    .load_file_info_versions_exact(&bucket, &local_only)
                    .await
                    .expect("chunk crash-boundary local-only source lookup should succeed")
                    .is_none(),
                "the final raw delete must remove the local-only source after cancellation at {stage:?}"
            );
            assert_eq!(backend.object_count().await, 2);
            backend.set_remove_failure(false);
            drive_tier_delete_dispatch_restart_to_convergence(restarted_store.clone()).await;
            assert_eq!(tier_delete_journal_count(restarted_store.clone()).await, 0);
            assert_eq!(tier_delete_dispatch_manifest_count(restarted_store.clone()).await, 0);
            assert_eq!(backend.object_count().await, 0);
            assert_eq!(
                backend.remove_versions().await.len(),
                2,
                "each crash case must retain exactly one cleanup owner per remote version"
            );
            restarted_shutdown.cancel();
        }
    }

    #[cfg(feature = "test-util")]
    #[test]
    #[serial_test::serial(storage_class_env)]
    fn tier_delete_chunk_missing_child_with_journals_fails_closed() {
        run_large_stack_async_test(
            "tier-delete-chunk-missing-child",
            tier_delete_chunk_missing_child_with_journals_fails_closed_case,
        );
    }

    #[cfg(feature = "test-util")]
    async fn tier_delete_chunk_missing_child_with_journals_fails_closed_case() {
        let _batch_limit = TierDeleteDispatchBatchLimitGuard::install(1);
        let temp_dir = tempfile::tempdir().expect("create missing-child store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "chunk-missing-child", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "CHUNK-MISSING-CHILD";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        backend.set_remove_failure(true);
        let bucket = "chunk-missing-child-bucket";
        let prefix = "prefix/";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("missing-child bucket should be created");
        for index in 0..2 {
            let object = format!("{prefix}object-{index}.bin");
            let mut reader = PutObjReader::from_vec(vec![b'm' + index as u8; 1024 * 1024]);
            let source = store
                .put_object(bucket, &object, &mut reader, &ObjectOptions::default())
                .await
                .expect("missing-child source should be written");
            store
                .transition_object(
                    bucket,
                    &object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name.to_string(),
                            etag: source.etag.clone().expect("missing-child source should have an etag"),
                            ..Default::default()
                        },
                        mod_time: source.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("missing-child source should transition");
        }

        let barrier = TierDeleteChunkTestBarrier::install(TierDeleteChunkTestStage::DispatchAuthorized);
        let worker_store = store.clone();
        let worker = tokio::spawn(async move {
            worker_store
                .delete_object_with_tier_delete_journal(
                    bucket,
                    prefix,
                    ObjectOptions {
                        delete_prefix: true,
                        ..Default::default()
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(30), barrier.wait_until_paused())
            .await
            .expect("chunk delete should persist its authorization before corruption injection");
        worker.abort();
        let _ = worker.await;
        drop(barrier);

        let records = store
            .clone()
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                TIER_DELETE_DISPATCH_MANIFEST_PREFIX,
                None,
                None,
                10,
                false,
                None,
                false,
            )
            .await
            .expect("parent and child records should be listable");
        let child = records
            .objects
            .iter()
            .find(|object| object.name.contains("/chunks/"))
            .expect("the bound child manifest should exist")
            .name
            .clone();
        com::delete_config(store.clone(), &child)
            .await
            .expect("the test should remove only the child manifest");
        assert_eq!(tier_delete_journal_count(store.clone()).await, 1);

        let error = store
            .delete_object_with_tier_delete_journal(
                bucket,
                prefix,
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("a missing child with retained journals must quarantine the parent");
        assert!(
            error.to_string().contains("missing child with retained journals"),
            "unexpected missing-child result: {error}"
        );
        assert_eq!(backend.object_count().await, 2, "fail-closed inspection must not remove a remote version");
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[test]
    #[serial_test::serial(storage_class_env)]
    fn authorized_prefix_retry_replays_predecessor_before_newcomer() {
        let handle = std::thread::Builder::new()
            .name("authorized-prefix-predecessor-replay-test".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .build()
                    .expect("authorized predecessor replay test runtime should build");
                runtime.block_on(authorized_prefix_retry_replays_predecessor_before_newcomer_case());
            })
            .expect("authorized predecessor replay test thread should spawn");
        if let Err(payload) = handle.join() {
            std::panic::resume_unwind(payload);
        }
    }

    #[cfg(feature = "test-util")]
    async fn authorized_prefix_retry_replays_predecessor_before_newcomer_case() {
        let _batch_limit = TierDeleteDispatchBatchLimitGuard::install(1);
        let temp_dir = tempfile::tempdir().expect("create authorized predecessor replay store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "authorized-prefix-predecessor-replay", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "AUTHORIZED-PREFIX-REPLAY";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let lease = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("authorized replay tier lease should resolve");
        let bucket = "authorized-prefix-predecessor-replay-bucket";
        let prefix = "prefix/";
        let predecessor = "prefix/000-predecessor.bin";
        let newcomer = "prefix/zzz-newcomer.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("authorized replay bucket should be created");

        let mut predecessor_reader = PutObjReader::from_vec(vec![b'p'; 1024 * 1024]);
        let predecessor_source = store
            .put_object(bucket, predecessor, &mut predecessor_reader, &ObjectOptions::default())
            .await
            .expect("predecessor source should be written");
        store
            .transition_object(
                bucket,
                predecessor,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: predecessor_source.etag.clone().expect("predecessor should have an etag"),
                        ..Default::default()
                    },
                    mod_time: predecessor_source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("predecessor should transition");
        let predecessor_info = store
            .get_object_info(bucket, predecessor, &ObjectOptions::default())
            .await
            .expect("transitioned predecessor should be readable");
        let mut predecessor_entry =
            transitioned_delete_journal_entry_for_source(None, false, false, bucket, predecessor, &predecessor_info)
                .expect("transitioned predecessor should produce a dispatch journal");
        predecessor_entry.backend_identity = Some(lease.backend_identity());
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("authorized replay bucket incarnation should resolve");
        let (manifest_name, _) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            prefix,
            vec![(predecessor_entry, Some(TierDeleteJournalState::Dispatched))],
            TierDeleteDispatchManifestState::DispatchAuthorized,
        )
        .await
        .expect("authorized predecessor fixture should persist");

        let mut newcomer_reader = PutObjReader::from_vec(vec![b'n'; 1024 * 1024]);
        let newcomer_source = store
            .put_object(bucket, newcomer, &mut newcomer_reader, &ObjectOptions::default())
            .await
            .expect("newcomer source should be written after authorization");
        store
            .transition_object(
                bucket,
                newcomer,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: newcomer_source.etag.clone().expect("newcomer should have an etag"),
                        ..Default::default()
                    },
                    mod_time: newcomer_source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("newcomer should transition");
        backend.set_remove_failure(true);

        let publication_epoch = store
            .scanner_data_usage_publication_epoch()
            .await
            .expect("authorized replay should observe the current scanner publication epoch");
        let publication_scope = store
            .scanner_data_usage_publication_commit_scope(
                publication_epoch,
                tokio::time::Instant::now() + Duration::from_secs(30),
                Vec::new(),
            )
            .await
            .expect("authorized replay should acquire a scanner publication scope");

        let retry = store
            .delete_object_with_tier_delete_journal(
                bucket,
                prefix,
                ObjectOptions {
                    delete_prefix: true,
                    scanner_publication_commit_scope: Some(publication_scope.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect_err("the predecessor-only replay must request a successor retry");
        assert!(
            retry.to_string().contains("retry the successor dispatch"),
            "unexpected predecessor replay result: {retry}"
        );
        assert_eq!(
            publication_scope.state(),
            crate::object_api::ScannerPublicationCommitState::Committed,
            "an exact predecessor replay must report its local mutation as committed"
        );
        assert!(publication_scope.release_movement_permit().await);
        assert!(
            store.pools[0]
                .get_disks_by_key(predecessor)
                .load_file_info_versions_exact(bucket, predecessor)
                .await
                .expect("predecessor metadata should remain readable")
                .is_none(),
            "the authorized predecessor must be removed exactly"
        );
        assert!(
            store.pools[0]
                .get_disks_by_key(newcomer)
                .load_file_info_versions_exact(bucket, newcomer)
                .await
                .expect("newcomer metadata should remain readable")
                .is_some(),
            "the predecessor authorization must not delete the newcomer"
        );
        assert_eq!(
            test_tier_delete_dispatch_manifest_state(store.clone(), &manifest_name)
                .await
                .expect("completed predecessor manifest should remain readable"),
            Some(TierDeleteDispatchManifestState::Completed)
        );

        backend.set_remove_failure(false);
        let predecessor_journals = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("completed predecessor journal should recover");
        assert_eq!(predecessor_journals.failed, 0);
        assert!(predecessor_journals.scanned <= 1 && predecessor_journals.deleted <= 1);
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        assert_eq!(backend.object_count().await, 1, "only the newcomer remote object should remain");
        assert_eq!(backend.remove_versions().await.len(), 1);
        let predecessor_manifest = recover_tier_delete_dispatch_manifests(store.clone(), 100, None)
            .await
            .expect("completed predecessor manifest should be collected");
        assert_eq!(predecessor_manifest.failed, 0);
        assert!(predecessor_manifest.deleted <= 1);
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, 0);

        store
            .delete_object_with_tier_delete_journal(
                bucket,
                prefix,
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect("a successor dispatch should delete the remaining newcomer");
        assert!(
            store.pools[0]
                .get_disks_by_key(newcomer)
                .load_file_info_versions_exact(bucket, newcomer)
                .await
                .expect("newcomer metadata lookup should succeed after successor")
                .is_none()
        );
        drive_tier_delete_dispatch_restart_to_convergence(store.clone()).await;
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        assert_eq!(tier_delete_dispatch_manifest_count(store.clone()).await, 0);
        assert_eq!(backend.object_count().await, 0);
        assert_eq!(backend.remove_versions().await.len(), 2);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn multi_pool_same_tuple_recursive_prefix_uses_one_journal_owner() {
        let temp_dir = tempfile::tempdir().expect("create shared-tuple prefix-delete store dir");
        let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "multi-pool-shared-tuple-prefix-delete",
            &[4, 4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "SHARED-PREFIX-DELETE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "multi-pool-shared-tuple-prefix-delete-bucket";
        let object = "prefix/shared.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("shared-tuple prefix bucket should be created");
        let mut reader = PutObjReader::from_vec(vec![b's'; 1024 * 1024]);
        let source = store.pools[0]
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("shared-tuple prefix source should be written");
        store.pools[0]
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: source.etag.clone().expect("shared-tuple prefix source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("shared-tuple prefix source should transition");
        copy_test_xlmeta_between_pools(temp_dir.path(), 0, 1, bucket, object).await;
        assert_eq!(backend.object_count().await, 1);
        backend.set_remove_failure(true);

        store
            .delete_object_with_tier_delete_journal(
                bucket,
                "prefix/",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect("shared-tuple recursive prefix delete should commit");

        assert_eq!(tier_delete_journal_count(store.clone()).await, 1);
        for pool_idx in 0..2 {
            assert!(
                store.pools[pool_idx]
                    .get_disks_by_key(object)
                    .load_file_info_versions_exact(bucket, object)
                    .await
                    .expect("shared-tuple post-delete metadata should decode")
                    .is_none(),
                "pool {pool_idx} must be physically empty"
            );
        }
        backend.set_remove_failure(false);
        ctx.wake_tier_delete_journal_recovery();
        wait_for_tier_delete_journal_recovery(store.clone(), &backend, 1).await;
        assert_eq!(backend.object_count().await, 0);
        assert_eq!(backend.remove_versions().await.len(), 1);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn multi_pool_recursive_prefix_rejects_legacy_or_hidden_merge_loser_before_delete() {
        let temp_dir = tempfile::tempdir().expect("create merge-loser prefix-delete store dir");
        let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "multi-pool-merge-loser-prefix-delete",
            &[4, 4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "MERGE-LOSER-PREFIX-DELETE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "multi-pool-merge-loser-prefix-delete-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("merge-loser prefix bucket should be created");

        for (object, byte) in [("legacy/item.bin", b'l'), ("hidden/item.bin", b'h')] {
            let mut reader = PutObjReader::from_vec(vec![byte; 1024 * 1024]);
            let source = store.pools[0]
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
                .expect("merge-loser source should be written");
            store.pools[0]
                .transition_object(
                    bucket,
                    object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name.to_string(),
                            etag: source.etag.clone().expect("merge-loser source should have an etag"),
                            ..Default::default()
                        },
                        mod_time: source.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("merge-loser source should transition");
            copy_test_xlmeta_between_pools(temp_dir.path(), 0, 1, bucket, object).await;
        }
        rewrite_transitioned_xlmeta_as_legacy_unknown(temp_dir.path(), 1, bucket, "legacy/item.bin", false).await;
        backend.set_remove_failure(true);
        store.pools[1]
            .delete_object(bucket, "hidden/item.bin", ObjectOptions::default())
            .await
            .expect("second-pool source delete should retain a hidden free-version");

        let legacy_error = store
            .delete_object_with_tier_delete_journal(
                bucket,
                "legacy/",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("a legacy merge loser must reject recursive deletion");
        assert!(legacy_error.to_string().contains("legacy Unknown"), "unexpected error: {legacy_error}");
        let hidden_error = store
            .delete_object_with_tier_delete_journal(
                bucket,
                "hidden/",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("a hidden free-version merge loser must reject recursive deletion");
        assert!(
            hidden_error.to_string().contains("free-version cleanup obligation"),
            "unexpected error: {hidden_error}"
        );

        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        assert_eq!(backend.object_count().await, 2);
        assert_eq!(backend.remove_count().await, 0);
        for object in ["legacy/item.bin", "hidden/item.bin"] {
            let pool0 = store.pools[0]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("winner metadata should decode")
                .expect("winner source must remain before destructive deletion");
            assert_eq!(pool0.versions.iter().filter(|version| !version.tier_free_version()).count(), 1);
        }
        let hidden_pool1 = store.pools[1]
            .get_disks_by_key("hidden/item.bin")
            .load_file_info_versions_exact(bucket, "hidden/item.bin")
            .await
            .expect("hidden loser metadata should decode")
            .expect("hidden loser cleanup owner must remain");
        assert_eq!(
            hidden_pool1
                .versions
                .iter()
                .chain(hidden_pool1.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            1
        );
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn recursive_prefix_partial_pool_failure_keeps_prepared_cleanup_owners() {
        let temp_dir = tempfile::tempdir().expect("create partial prefix-delete store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "partial-pool-prefix-delete", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "PARTIAL-PREFIX-DELETE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "partial-pool-prefix-delete-bucket";
        let object = "prefix/item.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("partial prefix bucket should be created");
        for (pool_idx, byte) in [(0, b'a'), (1, b'b')] {
            let mut reader = PutObjReader::from_vec(vec![byte; 1024 * 1024]);
            let source = store.pools[pool_idx]
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
                .expect("pool-local prefix source should be written");
            store.pools[pool_idx]
                .transition_object(
                    bucket,
                    object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name.to_string(),
                            etag: source.etag.clone().expect("pool-local source should have an etag"),
                            ..Default::default()
                        },
                        mod_time: source.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("pool-local prefix source should transition");
        }
        assert_eq!(backend.object_count().await, 2);
        backend.set_remove_failure(true);

        let barrier = crate::store::object::PrefixDeleteAfterJournalPrepareBarrier::install(bucket, "prefix/");
        let delete_store = store.clone();
        let delete = tokio::spawn(async move {
            delete_store
                .delete_object_with_tier_delete_journal(
                    bucket,
                    "prefix/",
                    ObjectOptions {
                        delete_prefix: true,
                        ..Default::default()
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), barrier.wait_until_paused())
            .await
            .expect("prefix delete should pause after every journal is Prepared");
        assert_eq!(tier_delete_journal_count(store.clone()).await, 2);

        let failed_set = store.pools[1].get_disks_by_key(object);
        let offline_disks = force_set_disks_offline_for_test(&failed_set).await;
        barrier.release();
        let error = delete
            .await
            .expect("partial prefix delete task should join")
            .expect_err("an offline second pool must fail the recursive prefix delete");
        assert!(
            matches!(
                error,
                StorageError::InsufficientWriteQuorum(_, _) | StorageError::InsufficientReadQuorum(_, _)
            ),
            "unexpected error: {error:?}"
        );
        drop(offline_disks);
        drop(barrier);

        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            2,
            "an ambiguous partial mutation must retain every Dispatched owner"
        );
        let first_pool_state = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("first-pool metadata should remain decodable after an ambiguous failure");
        if let Some(first_pool_state) = first_pool_state {
            assert_eq!(
                first_pool_state
                    .versions
                    .iter()
                    .filter(|version| !version.tier_free_version())
                    .count(),
                1,
                "a retained first-pool source must remain a visible source, not an unjournaled fallback owner"
            );
        }
        assert!(
            store.pools[1]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("failed-pool metadata should decode")
                .is_some(),
            "the failed pool should retain its live source"
        );

        let _ = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("Dispatched owners should reconcile after the failed pool returns");
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            2,
            "recovery must never abort or delete a Dispatched journal while any source is still live"
        );
        store
            .delete_object_with_tier_delete_journal(
                bucket,
                "prefix/",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect("retry should delete the remaining pool source");
        backend.set_remove_failure(false);
        for _ in 0..3 {
            let _ = recover_tier_delete_journal_entries(store.clone(), 100, None)
                .await
                .expect("remaining pool journals should settle after backend recovery");
            if tier_delete_journal_count(store.clone()).await == 0 {
                break;
            }
        }
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        assert_eq!(backend.object_count().await, 0);
        assert_eq!(backend.remove_versions().await.len(), 2);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn recursive_prefix_partial_set_failure_keeps_prepared_cleanup_owners() {
        let temp_dir = tempfile::tempdir().expect("create partial-set prefix-delete store dir");
        let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store_with_layout(
            temp_dir.path(),
            "partial-set-prefix-delete",
            &[(2, 4)],
            CancellationToken::new(),
            None,
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "PARTIAL-SET-PREFIX-DELETE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "partial-set-prefix-delete-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("partial-set prefix bucket should be created");

        let objects = (0..2)
            .map(|target_set| {
                (0..512)
                    .map(|index| format!("prefix/set-{target_set}-{index}.bin"))
                    .find(|candidate| store.pools[0].get_disks_by_key(candidate).set_index == target_set)
                    .expect("a deterministic key should map to each test set")
            })
            .collect::<Vec<_>>();
        for (index, object) in objects.iter().enumerate() {
            let mut reader = PutObjReader::from_vec(vec![b'0' + index as u8; 1024 * 1024]);
            let source = store.pools[0]
                .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                .await
                .expect("set-local prefix source should be written");
            store.pools[0]
                .transition_object(
                    bucket,
                    object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name.to_string(),
                            etag: source.etag.clone().expect("set-local source should have an etag"),
                            ..Default::default()
                        },
                        mod_time: source.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("set-local prefix source should transition");
        }
        assert_eq!(backend.object_count().await, 2);
        backend.set_remove_failure(true);

        let barrier = crate::store::object::PrefixDeleteAfterJournalPrepareBarrier::install(bucket, "prefix/");
        let delete_store = store.clone();
        let delete = tokio::spawn(async move {
            delete_store
                .delete_object_with_tier_delete_journal(
                    bucket,
                    "prefix/",
                    ObjectOptions {
                        delete_prefix: true,
                        ..Default::default()
                    },
                )
                .await
        });
        tokio::time::timeout(Duration::from_secs(10), barrier.wait_until_paused())
            .await
            .expect("set-scoped prefix delete should pause after journal Prepare");
        assert_eq!(tier_delete_journal_count(store.clone()).await, 2);

        let failed_set = store.pools[0].disk_set[1].clone();
        let offline_disks = force_set_disks_offline_for_test(&failed_set).await;
        barrier.release();
        let error = delete
            .await
            .expect("partial-set prefix delete task should join")
            .expect_err("an offline second set must fail recursive prefix deletion");
        assert!(
            matches!(
                error,
                StorageError::InsufficientWriteQuorum(_, _) | StorageError::InsufficientReadQuorum(_, _)
            ),
            "unexpected error: {error:?}"
        );
        drop(offline_disks);
        drop(barrier);

        assert_eq!(tier_delete_journal_count(store.clone()).await, 2);
        let first_set_state = store.pools[0]
            .get_disks_by_key(&objects[0])
            .load_file_info_versions_exact(bucket, &objects[0])
            .await
            .expect("first-set metadata should remain decodable after an ambiguous failure");
        if let Some(first_set_state) = first_set_state {
            assert_eq!(
                first_set_state
                    .versions
                    .iter()
                    .filter(|version| !version.tier_free_version())
                    .count(),
                1,
                "a retained first-set source must remain visible under its Dispatched journal"
            );
        }
        assert!(
            store.pools[0]
                .get_disks_by_key(&objects[1])
                .load_file_info_versions_exact(bucket, &objects[1])
                .await
                .expect("failed-set metadata should decode")
                .is_some()
        );

        let _ = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("set-scoped Dispatched owners should reconcile");
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            2,
            "recovery must retain the complete Authorized dispatch until every source is absent"
        );
        store
            .delete_object_with_tier_delete_journal(
                bucket,
                "prefix/",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect("set-scoped retry should delete the remaining source");
        backend.set_remove_failure(false);
        for _ in 0..3 {
            let _ = recover_tier_delete_journal_entries(store.clone(), 100, None)
                .await
                .expect("remaining set journals should settle after backend recovery");
            if tier_delete_journal_count(store.clone()).await == 0 {
                break;
            }
        }
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        assert_eq!(backend.object_count().await, 0);
        assert_eq!(backend.remove_versions().await.len(), 2);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn versioned_explicit_transition_delete_preserves_other_version_then_allows_bucket_delete() {
        let temp_dir = tempfile::tempdir().expect("create versioned transitioned delete store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "versioned-transitioned-delete-owner", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "VERSIONED-DELETE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "versioned-transitioned-delete-owner-bucket";
        let object = "archive.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("versioned source bucket should be created");
        crate::bucket::metadata_sys::update_in(
            &ctx,
            bucket,
            BUCKET_VERSIONING_CONFIG,
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
        )
        .await
        .expect("bucket versioning should be enabled");

        let mut first_reader = PutObjReader::from_vec(vec![b'1'; 1024 * 1024]);
        let first = store
            .put_object(
                bucket,
                object,
                &mut first_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("first version should be written");
        let first_version = first.version_id.expect("versioned PUT should return an identity");
        store
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    version_id: Some(first_version.to_string()),
                    versioned: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: first.etag.clone().expect("first version should have an etag"),
                        ..Default::default()
                    },
                    mod_time: first.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("first version should transition");
        let mut second_reader = PutObjReader::from_vec(vec![b'2'; 1024 * 1024]);
        let second = store
            .put_object(
                bucket,
                object,
                &mut second_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("second version should be written");
        let second_version = second.version_id.expect("second PUT should return an identity");

        backend.set_remove_failure(true);
        store
            .delete_object_with_tier_delete_journal(
                bucket,
                object,
                ObjectOptions {
                    version_id: Some(first_version.to_string()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("explicit transitioned version delete should commit");
        store
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    version_id: Some(second_version.to_string()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the unrelated second version must remain readable");
        let exact = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("remaining version metadata should decode")
            .expect("the second version should remain");
        assert_eq!(
            exact
                .versions
                .iter()
                .chain(exact.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            1,
            "ordinary exact-version delete must leave one hidden free-version owner"
        );
        assert!(
            exact
                .versions
                .iter()
                .any(|version| version.version_id == Some(second_version)),
            "the explicit delete must not remove the other version"
        );
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            0,
            "ordinary exact-version delete must not create a journal"
        );
        backend.set_remove_failure(false);
        wait_for_tier_free_version_recovery(store.clone(), &backend, 1).await;
        let exact = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("post-cleanup metadata should decode")
            .expect("the unrelated second version should remain after cleanup");
        assert_eq!(
            exact
                .versions
                .iter()
                .chain(exact.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            0,
            "free-version recovery must remove only its exact owner"
        );
        assert!(
            exact
                .versions
                .iter()
                .any(|version| version.version_id == Some(second_version)),
            "free-version recovery must preserve the unrelated version"
        );

        store
            .delete_object_with_tier_delete_journal(
                bucket,
                object,
                ObjectOptions {
                    version_id: Some(second_version.to_string()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("last ordinary version should be removed");
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("bucket delete should succeed after the last version is removed");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn suspended_null_transition_delete_uses_free_version_as_sole_owner() {
        let temp_dir = tempfile::tempdir().expect("create suspended transitioned delete store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "suspended-transitioned-delete-owner", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "SUSPENDED-DELETE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "suspended-transitioned-delete-owner-bucket";
        let object = "archive.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("suspended source bucket should be created");
        crate::bucket::metadata_sys::update_in(
            &ctx,
            bucket,
            BUCKET_VERSIONING_CONFIG,
            b"<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>".to_vec(),
        )
        .await
        .expect("bucket versioning should be suspended");

        let mut reader = PutObjReader::from_vec(vec![b's'; 1024 * 1024]);
        let source = store
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    version_suspended: true,
                    ..Default::default()
                },
            )
            .await
            .expect("suspended null source should be written");
        let null_version = source.version_id.expect("suspended source should expose the null identity");
        assert!(null_version.is_nil(), "suspended source identity must be null");
        store
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    version_id: Some(null_version.to_string()),
                    version_suspended: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: source.etag.clone().expect("suspended source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("suspended null source should transition");

        backend.set_remove_failure(true);
        store
            .delete_object_with_tier_delete_journal(
                bucket,
                object,
                ObjectOptions {
                    version_id: Some(null_version.to_string()),
                    version_suspended: true,
                    ..Default::default()
                },
            )
            .await
            .expect("explicit null transitioned version delete should commit");
        let exact = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("post-delete null metadata should decode")
            .expect("ordinary null-version delete must retain a cleanup owner");
        assert_eq!(
            exact
                .versions
                .iter()
                .chain(exact.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            1,
            "ordinary null-version delete must retain exactly one hidden free-version owner"
        );
        assert_eq!(
            exact.versions.iter().filter(|version| !version.tier_free_version()).count(),
            0,
            "ordinary null-version delete must remove the visible source"
        );
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        backend.set_remove_failure(false);
        wait_for_tier_free_version_recovery(store.clone(), &backend, 1).await;
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("suspended bucket should be empty after exact null-version cleanup");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn versioned_delete_marker_keeps_transitioned_source_and_remote_object() {
        let temp_dir = tempfile::tempdir().expect("create versioned delete-marker store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transitioned-delete-marker", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "DELETE-MARKER";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "transitioned-delete-marker-bucket";
        let object = "archive.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("versioned source bucket should be created");
        crate::bucket::metadata_sys::update_in(
            &ctx,
            bucket,
            BUCKET_VERSIONING_CONFIG,
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
        )
        .await
        .expect("bucket versioning should be enabled");

        let mut reader = PutObjReader::from_vec(vec![b'm'; 1024 * 1024]);
        let source = store
            .put_object(
                bucket,
                object,
                &mut reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("versioned source should be written");
        let source_version = source.version_id.expect("versioned source should have an identity");
        store
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    version_id: Some(source_version.to_string()),
                    versioned: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: source.etag.clone().expect("versioned source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("versioned source should transition");

        let marker = store
            .delete_object_with_tier_delete_journal(
                bucket,
                object,
                ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("versioned DELETE without versionId should create a marker");
        assert!(marker.delete_marker);
        let marker_version = marker.version_id.expect("delete marker should have an identity");
        assert_eq!(backend.object_count().await, 1, "delete-marker creation must retain the remote object");
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            0,
            "delete-marker creation must not schedule cleanup"
        );
        let exact = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("versioned delete-marker metadata should decode")
            .expect("source and delete marker should remain on disk");
        assert!(
            exact.versions.iter().any(|version| {
                version.version_id == Some(source_version) && version.transition_status == rustfs_filemeta::TRANSITION_COMPLETE
            }),
            "the transitioned source must remain behind the delete marker"
        );
        assert_eq!(
            exact
                .versions
                .iter()
                .chain(exact.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            0,
            "delete-marker creation must not create a cleanup free-version"
        );

        store
            .delete_object_with_tier_delete_journal(
                bucket,
                object,
                ObjectOptions {
                    version_id: Some(marker_version.to_string()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("delete marker should be removable by exact identity");
        store
            .delete_object_with_tier_delete_journal(
                bucket,
                object,
                ObjectOptions {
                    version_id: Some(source_version.to_string()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("transitioned source should be removable by exact identity");
        wait_for_tier_free_version_recovery(store.clone(), &backend, 1).await;
        if let Some(exact) = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("post-recovery version metadata should decode")
        {
            assert!(
                exact
                    .versions
                    .iter()
                    .chain(exact.free_versions.iter())
                    .all(|version| !version.tier_free_version()),
                "free-version recovery must remove the all-physical cleanup owner"
            );
        }
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("versioned source bucket should be empty after explicit cleanup");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn batch_transitioned_delete_uses_free_version_per_item() {
        let temp_dir = tempfile::tempdir().expect("create batch transitioned delete store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "batch-transitioned-delete-owner", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "BATCH-DELETE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "batch-transitioned-delete-owner-bucket";
        let transitioned = "transitioned.bin";
        let ordinary = "ordinary.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("batch source bucket should be created");
        let mut transitioned_reader = PutObjReader::from_vec(vec![b't'; 1024 * 1024]);
        let transitioned_source = store
            .put_object(bucket, transitioned, &mut transitioned_reader, &ObjectOptions::default())
            .await
            .expect("transitioned batch source should be written");
        store
            .transition_object(
                bucket,
                transitioned,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: transitioned_source
                            .etag
                            .clone()
                            .expect("transitioned batch source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: transitioned_source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("batch source transition should commit");
        let mut ordinary_reader = PutObjReader::from_vec(vec![b'o'; 1024 * 1024]);
        store
            .put_object(bucket, ordinary, &mut ordinary_reader, &ObjectOptions::default())
            .await
            .expect("ordinary batch source should be written");

        backend.set_remove_failure(true);
        let (_deleted, errors) = store
            .delete_objects_with_tier_delete_journal(
                bucket,
                vec![
                    ObjectToDelete {
                        object_name: transitioned.to_string(),
                        ..Default::default()
                    },
                    ObjectToDelete {
                        object_name: ordinary.to_string(),
                        ..Default::default()
                    },
                    ObjectToDelete {
                        object_name: "missing.bin".to_string(),
                        ..Default::default()
                    },
                ],
                ObjectOptions::default(),
            )
            .await;
        assert!(errors.iter().all(Option::is_none), "S3 batch delete should be idempotent: {errors:?}");
        let transitioned_versions = store.pools[0]
            .get_disks_by_key(transitioned)
            .load_file_info_versions_exact(bucket, transitioned)
            .await
            .expect("transitioned batch metadata should remain decodable")
            .expect("transitioned batch item must retain a cleanup owner");
        assert_eq!(
            transitioned_versions
                .versions
                .iter()
                .chain(transitioned_versions.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            1,
            "transitioned batch item must retain exactly one hidden free-version owner"
        );
        assert_eq!(
            transitioned_versions
                .versions
                .iter()
                .filter(|version| !version.tier_free_version())
                .count(),
            0,
            "transitioned batch item must have no visible source"
        );
        assert!(
            store.pools[0]
                .get_disks_by_key(ordinary)
                .load_file_info_versions_exact(bucket, ordinary)
                .await
                .expect("ordinary batch metadata should remain decodable")
                .is_none(),
            "ordinary batch item should have no local metadata"
        );
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            0,
            "ordinary batch delete must not create a tier-delete journal"
        );
        assert_eq!(backend.object_count().await, 1, "failed remote cleanup must retain its object");

        backend.set_remove_failure(false);
        wait_for_tier_free_version_recovery(store.clone(), &backend, 1).await;
        assert!(
            store.pools[0]
                .get_disks_by_key(transitioned)
                .load_file_info_versions_exact(bucket, transitioned)
                .await
                .expect("cleaned batch metadata should remain decodable")
                .is_none(),
            "free-version recovery must remove the exact cleanup owner"
        );

        let causal = "causal.bin";
        let mut causal_reader = PutObjReader::from_vec(vec![b'c'; 1024 * 1024]);
        let causal_source = store
            .put_object(bucket, causal, &mut causal_reader, &ObjectOptions::default())
            .await
            .expect("causal batch source should be written");
        store
            .transition_object(
                bucket,
                causal,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: causal_source.etag.clone().expect("causal batch source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: causal_source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("causal batch source transition should commit");
        let (_deleted, errors) = store
            .delete_objects(
                bucket,
                vec![
                    ObjectToDelete {
                        object_name: causal.to_string(),
                        ..Default::default()
                    },
                    ObjectToDelete {
                        object_name: causal.to_string(),
                        ..Default::default()
                    },
                ],
                ObjectOptions::default(),
            )
            .await;
        assert!(
            errors.iter().all(Option::is_none),
            "duplicate causal batch deletes should remain idempotent: {errors:?}"
        );
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let metadata_absent = store.pools[0]
                    .get_disks_by_key(causal)
                    .load_file_info_versions_exact(bucket, causal)
                    .await
                    .expect("causal batch cleanup metadata should remain readable")
                    .is_none();
                if metadata_absent && backend.remove_versions().await.len() >= 2 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("batch free-version receipt should converge without another recovery scan");
        assert_eq!(
            backend.remove_versions().await.len(),
            2,
            "duplicate batch requests must cause only one remote delete for the causal object"
        );

        crate::bucket::metadata_sys::update_in(
            &ctx,
            bucket,
            BUCKET_VERSIONING_CONFIG,
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
        )
        .await
        .expect("causal batch bucket versioning should be enabled");
        let versioned_causal = "versioned-causal.bin";
        let mut versioned_reader = PutObjReader::from_vec(vec![b'v'; 1024 * 1024]);
        let versioned_source = store
            .put_object(
                bucket,
                versioned_causal,
                &mut versioned_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("versioned causal batch source should be written");
        let versioned_source_id = versioned_source
            .version_id
            .expect("versioned causal batch source should have an identity");
        store
            .transition_object(
                bucket,
                versioned_causal,
                &ObjectOptions {
                    version_id: Some(versioned_source_id.to_string()),
                    versioned: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: versioned_source
                            .etag
                            .clone()
                            .expect("versioned causal batch source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: versioned_source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("versioned causal batch source transition should commit");
        let (_deleted, errors) = store
            .delete_objects(
                bucket,
                vec![ObjectToDelete {
                    object_name: versioned_causal.to_string(),
                    version_id: Some(versioned_source_id),
                    ..Default::default()
                }],
                ObjectOptions::default(),
            )
            .await;
        assert!(
            errors.iter().all(Option::is_none),
            "explicit-version causal batch delete should commit: {errors:?}"
        );
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let metadata_absent = store.pools[0]
                    .get_disks_by_key(versioned_causal)
                    .load_file_info_versions_exact(bucket, versioned_causal)
                    .await
                    .expect("versioned causal batch cleanup metadata should remain readable")
                    .is_none();
                if metadata_absent && backend.remove_versions().await.len() == 3 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("explicit-version batch receipt should converge without a recovery scan");
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("batch source bucket should be physically empty");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn batch_transitioned_delete_aggregate_error_still_enqueues_committed_free_version() {
        let temp_dir = tempfile::tempdir().expect("create aggregate-error batch delete store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "batch-transitioned-aggregate-error", &[4, 4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "BATCH-AGGREGATE-ERROR";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "batch-transitioned-aggregate-error-bucket";
        let object = "archive.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("aggregate-error source bucket should be created");
        let mut reader = PutObjReader::from_vec(vec![b'a'; 1024 * 1024]);
        let source = store.pools[0]
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("aggregate-error source should be written");
        store.pools[0]
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: source.etag.clone().expect("aggregate-error source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("aggregate-error source should transition");

        // Model a data-movement copy: both pools own the same logical source
        // and exact remote tuple, but each batch delete creates its own local
        // free-version UUID in the shared request sink.
        for disk_index in 0..4 {
            let source_meta = temp_dir
                .path()
                .join(format!("pool0/set0/disk{disk_index}/{bucket}/{object}/{STORAGE_FORMAT_FILE}"));
            let target_meta = temp_dir
                .path()
                .join(format!("pool1/set0/disk{disk_index}/{bucket}/{object}/{STORAGE_FORMAT_FILE}"));
            tokio::fs::create_dir_all(target_meta.parent().expect("target xl.meta should have a parent"))
                .await
                .expect("second-pool object directory should be created");
            tokio::fs::copy(&source_meta, &target_meta)
                .await
                .expect("transitioned xl.meta should copy exactly to the second pool");
        }

        ExpiryState::resize_workers(1, store.clone()).await;
        let injection = crate::store::object::BatchDeletePoolErrorInjection::install(
            bucket,
            1,
            vec![(object.to_string(), StorageError::ErasureWriteQuorum)],
        );
        let (deleted, errors) = store
            .delete_objects(
                bucket,
                vec![ObjectToDelete {
                    object_name: object.to_string(),
                    ..Default::default()
                }],
                ObjectOptions::default(),
            )
            .await;
        assert_eq!(injection.observed(), 1, "the second pool should inject one post-commit aggregate error");
        assert_eq!(errors, vec![Some(StorageError::ErasureWriteQuorum)]);
        assert!(deleted[0].found, "the aggregate error must retain the committed pool result");
        drop(injection);

        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let mut metadata_absent = true;
                for pool in &store.pools {
                    metadata_absent &= pool
                        .get_disks_by_key(object)
                        .load_file_info_versions_exact(bucket, object)
                        .await
                        .expect("aggregate-error cleanup metadata should remain readable")
                        .is_none();
                }
                if metadata_absent && backend.remove_count().await == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("aggregate failure must not suppress committed receipt dispatch");
        assert_eq!(backend.object_count().await, 0, "the shared remote object should be removed exactly once");
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("aggregate-error bucket should be physically empty");
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transitioned_delete_free_version_replays_after_store_restart() {
        let temp_dir = tempfile::tempdir().expect("create restarted free-version store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transitioned-delete-restart-before", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "DELETE-RESTART";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let tier_config = ctx
            .tier_config_mgr()
            .read()
            .await
            .tiers
            .get(tier_name)
            .expect("mock tier config should exist")
            .clone_with_credentials();
        let bucket = "transitioned-delete-restart-bucket";
        let object = "archive.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("restart source bucket should be created");
        let mut reader = PutObjReader::from_vec(vec![b'r'; 1024 * 1024]);
        let original = store
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("restart source object should be written");
        store
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: original.etag.clone().expect("restart source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("restart source transition should commit");
        backend.set_remove_failure(true);
        store
            .delete_object_with_tier_delete_journal(bucket, object, ObjectOptions::default())
            .await
            .expect("local source delete should commit while remote cleanup is retryable");
        let retained = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("post-delete local metadata should decode")
            .expect("ordinary delete must retain a free-version across restart");
        assert_eq!(
            retained
                .versions
                .iter()
                .chain(retained.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            1,
            "ordinary delete must retain exactly one free-version across restart"
        );
        assert_eq!(
            retained
                .versions
                .iter()
                .filter(|version| !version.tier_free_version())
                .count(),
            0,
            "ordinary delete must remove the visible source before restart"
        );
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        assert_eq!(backend.object_count().await, 1);

        shutdown.cancel();
        drop(store);
        drop(ctx);
        let (restarted_ctx, restarted_store, restarted_shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transitioned-delete-restart-after", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(restarted_store.clone(), Vec::new()).await;
        {
            let restarted_tier_config_mgr = restarted_ctx.tier_config_mgr();
            let mut manager = restarted_tier_config_mgr.write().await;
            manager.tiers.insert(tier_name.to_string(), tier_config);
            manager
                .install_test_driver(tier_name, Box::new(backend.clone()))
                .expect("restarted store should bind the original tier destination");
        }
        backend.set_remove_failure(false);
        wait_for_tier_free_version_recovery(restarted_store.clone(), &backend, 1).await;
        assert_eq!(tier_delete_journal_count(restarted_store.clone()).await, 0);
        assert_eq!(backend.object_count().await, 0);
        assert!(
            restarted_store.pools[0]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("restarted cleanup metadata should decode")
                .is_none(),
            "restarted recovery must remove the exact free-version owner"
        );
        restarted_store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("source bucket should be empty after restarted free-version recovery");
        restarted_shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transitioned_delete_local_quorum_failure_rolls_back_without_cleanup_owner() {
        let temp_dir = tempfile::tempdir().expect("create failed local delete store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transitioned-delete-local-failure", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "DELETE-LOCAL-FAIL";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "transitioned-delete-local-failure-bucket";
        let object = "archive.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("failed-delete source bucket should be created");
        let mut reader = PutObjReader::from_vec(vec![b'q'; 1024 * 1024]);
        let source = store
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("failed-delete source should be written");
        store
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: source.etag.clone().expect("failed-delete source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("failed-delete source should transition");

        let set = store.pools[0].get_disks_by_key(object);
        let saved_disk = {
            let mut disks = set.disks.write().await;
            let first = disks[0].as_ref().expect("first disk should be online");
            crate::disk::local::set_delete_version_fail_after_commit(first.path().as_path(), object);
            disks[1].take().expect("second disk should be online before fault injection")
        };
        let err = store
            .delete_object_with_tier_delete_journal(bucket, object, ObjectOptions::default())
            .await
            .expect_err("one post-commit error plus one offline disk must miss delete quorum");
        assert!(matches!(err, StorageError::Io(_)), "unexpected local delete failure: {err:?}");
        assert!(
            err.to_string()
                .contains("Storage resources are insufficient for the write operation"),
            "the caller must observe the wrapped quorum failure: {err}"
        );
        set.disks.write().await[1] = Some(saved_disk);

        let retained = store
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("failed local delete must roll back the transitioned source");
        assert_eq!(retained.transitioned_object.status, rustfs_filemeta::TRANSITION_COMPLETE);
        let retained_versions = set
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("rolled-back metadata should decode")
            .expect("rolled-back source should remain on disk");
        assert_eq!(
            retained_versions
                .versions
                .iter()
                .chain(retained_versions.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            0,
            "failed local quorum must not create a cleanup owner"
        );
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            0,
            "ordinary delete must not create a journal, including on rollback"
        );
        assert_eq!(backend.object_count().await, 1, "failed local commit must retain the remote object");
        assert_eq!(backend.remove_count().await, 0, "failed local commit must not dispatch remote cleanup");

        backend.set_remove_failure(true);
        store
            .delete_object_with_tier_delete_journal(bucket, object, ObjectOptions::default())
            .await
            .expect("retry after disk recovery should commit");
        let cleanup_owner = set
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("retry metadata should decode")
            .expect("successful retry must leave a free-version owner");
        assert_eq!(
            cleanup_owner
                .versions
                .iter()
                .chain(cleanup_owner.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            1,
            "successful retry must leave exactly one free-version owner"
        );
        assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
        backend.set_remove_failure(false);
        wait_for_tier_free_version_recovery(store.clone(), &backend, 1).await;
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("retry should leave the source bucket empty");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn batch_transitioned_delete_post_commit_failures_roll_back_without_free_version_receipt() {
        let temp_dir = tempfile::tempdir().expect("create failed batch delete store dir");
        let (ctx, store, shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "batch-delete-local-failure", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "BATCH-DELETE-LOCAL-FAIL";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "batch-delete-local-failure-bucket";
        let object = "archive.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("failed batch source bucket should be created");
        crate::bucket::metadata_sys::update_in(
            &ctx,
            bucket,
            BUCKET_VERSIONING_CONFIG,
            b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
        )
        .await
        .expect("failed batch bucket versioning should be enabled");
        let mut transitioned_reader = PutObjReader::from_vec(vec![b't'; 1024 * 1024]);
        let transitioned_source = store
            .put_object(
                bucket,
                object,
                &mut transitioned_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("failed batch transitioned version should be written");
        let transitioned_version_id = transitioned_source
            .version_id
            .expect("failed batch transitioned source should have a version identity");
        store
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    version_id: Some(transitioned_version_id.to_string()),
                    versioned: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: transitioned_source
                            .etag
                            .clone()
                            .expect("failed batch transitioned source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: transitioned_source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("failed batch source version should transition");
        let mut ordinary_reader = PutObjReader::from_vec(vec![b'o'; 1024 * 1024]);
        let ordinary_source = store
            .put_object(
                bucket,
                object,
                &mut ordinary_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("failed batch ordinary sibling should be written");
        let ordinary_version_id = ordinary_source
            .version_id
            .expect("failed batch ordinary sibling should have a version identity");
        let delete_requests = || {
            vec![
                ObjectToDelete {
                    object_name: object.to_string(),
                    version_id: Some(transitioned_version_id),
                    ..Default::default()
                },
                ObjectToDelete {
                    object_name: object.to_string(),
                    version_id: Some(ordinary_version_id),
                    ..Default::default()
                },
            ]
        };

        let set = store.pools[0].get_disks_by_key(object);
        let disks = set.disks.read().await;
        assert_eq!(disks.len(), 4, "the rollback fixture must use four disks");
        // Keep discovery fully online, then make a quorum of disks report an
        // error only after their batch metadata commit has completed.
        for disk in disks.iter().take(3) {
            let disk = disk.as_ref().expect("injected rollback disks should be online");
            crate::disk::local::set_delete_version_fail_after_commit(disk.path().as_path(), object);
        }
        drop(disks);
        let receipt_sink = crate::object_api::TierFreeVersionReceiptSink::new();
        let (_deleted, errors) = store
            .delete_objects(
                bucket,
                delete_requests(),
                ObjectOptions {
                    tier_free_version_receipt_sink: Some(receipt_sink.clone()),
                    ..Default::default()
                },
            )
            .await;
        assert_eq!(
            errors,
            vec![Some(StorageError::Unexpected), Some(StorageError::Unexpected),],
            "three post-commit disk errors must fail batch delete before receipts publish"
        );

        assert!(
            receipt_sink
                .drain()
                .expect("the test-owned failed-batch sink should drain exactly once")
                .is_empty(),
            "a rolled-back physical group must publish no cleanup receipt"
        );
        let retained_transitioned = store
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    version_id: Some(transitioned_version_id.to_string()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("failed batch delete must restore the transitioned sibling");
        assert_eq!(retained_transitioned.transitioned_object.status, rustfs_filemeta::TRANSITION_COMPLETE);
        let retained_ordinary = store
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    version_id: Some(ordinary_version_id.to_string()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("failed batch delete must restore the ordinary sibling");
        assert_ne!(retained_ordinary.transitioned_object.status, rustfs_filemeta::TRANSITION_COMPLETE);
        let retained_versions = set
            .load_file_info_versions_exact(bucket, object)
            .await
            .expect("rolled-back batch metadata should decode")
            .expect("rolled-back batch source should remain on disk");
        assert_eq!(
            retained_versions
                .versions
                .iter()
                .chain(retained_versions.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            0,
            "failed batch quorum must not retain a free-version owner"
        );
        let retained_version_ids = retained_versions
            .versions
            .iter()
            .filter_map(|version| version.version_id)
            .collect::<std::collections::HashSet<_>>();
        assert_eq!(
            retained_version_ids,
            std::collections::HashSet::from([transitioned_version_id, ordinary_version_id]),
            "the physical-group rollback must restore both explicit siblings"
        );
        assert_eq!(backend.object_count().await, 1, "failed batch commit must retain the remote object");
        assert_eq!(backend.remove_count().await, 0, "failed batch commit must not dispatch remote cleanup");

        ExpiryState::resize_workers(1, store.clone()).await;
        let (_deleted, retry_errors) = store
            .delete_objects(bucket, delete_requests(), ObjectOptions::default())
            .await;
        assert!(
            retry_errors.iter().all(Option::is_none),
            "retry after disk recovery should commit: {retry_errors:?}"
        );
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                let metadata_absent = set
                    .load_file_info_versions_exact(bucket, object)
                    .await
                    .expect("retry cleanup metadata should remain readable")
                    .is_none();
                if metadata_absent && backend.remove_count().await == 1 {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("successful batch retry should converge without a recovery scan");
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("successful batch retry should leave the bucket empty");
        shutdown.cancel();
    }

    #[cfg(feature = "test-util")]
    async fn run_multi_pool_same_remote_tuple_delete_case(batch: bool) {
        let temp_dir = tempfile::tempdir().expect("create shared-tuple multi-pool store dir");
        let case = if batch { "batch" } else { "single" };
        let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            &format!("multi-pool-same-tuple-{case}"),
            &[4, 4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = if batch { "SAME-TUPLE-BATCH" } else { "SAME-TUPLE-SINGLE" };
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = format!("multi-pool-same-tuple-{case}-bucket");
        let object = "archive.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("shared-tuple bucket should be created");
        let mut reader = PutObjReader::from_vec(vec![b's'; 1024 * 1024]);
        let source = store.pools[0]
            .put_object(&bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("first pool source should be written");
        store.pools[0]
            .transition_object(
                &bucket,
                object,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: source.etag.clone().expect("shared source should have an etag"),
                        ..Default::default()
                    },
                    mod_time: source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("first pool source should transition");
        let first = store.pools[0]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("first pool transitioned source should be readable");

        // Model the exact metadata duplication produced by data movement:
        // both pools refer to the same remote object/version/backend and carry
        // the same stable source identity.
        for disk_index in 0..4 {
            let source_meta = temp_dir
                .path()
                .join(format!("pool0/set0/disk{disk_index}/{bucket}/{object}/{STORAGE_FORMAT_FILE}"));
            let target_meta = temp_dir
                .path()
                .join(format!("pool1/set0/disk{disk_index}/{bucket}/{object}/{STORAGE_FORMAT_FILE}"));
            tokio::fs::create_dir_all(target_meta.parent().expect("target xl.meta should have a parent"))
                .await
                .expect("second pool object directory should be created");
            tokio::fs::copy(&source_meta, &target_meta)
                .await
                .expect("transitioned xl.meta should copy exactly to the second pool");
        }
        let second = store.pools[1]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("copied second-pool source should be readable");
        assert_eq!(
            (first.transitioned_object.name.clone(), first.transitioned_object.version_id.clone()),
            (second.transitioned_object.name.clone(), second.transitioned_object.version_id.clone())
        );
        assert_eq!(backend.object_count().await, 1);

        let receipt_sink = crate::object_api::TierFreeVersionReceiptSink::new();
        let mut delete_opts = ObjectOptions {
            tier_delete_journal_api: Some(store.clone()),
            tier_free_version_receipt_sink: Some(receipt_sink.clone()),
            ..Default::default()
        };
        if batch {
            delete_opts.delete_replication_config_snapshot = Some(Arc::new(
                ReplicationObjectBridge::delete_request_config_in(&store.ctx, &bucket)
                    .await
                    .expect("batch delete replication snapshot should load"),
            ));
            let (_deleted, errors, _accounting) = store.pools[0]
                .delete_objects_with_accounting(
                    &bucket,
                    vec![ObjectToDelete {
                        object_name: object.to_string(),
                        ..Default::default()
                    }],
                    delete_opts.clone(),
                )
                .await;
            assert!(errors.iter().all(Option::is_none), "first pool batch delete should commit: {errors:?}");
        } else {
            store.pools[0]
                .delete_object(&bucket, object, delete_opts.clone())
                .await
                .expect("first pool single delete should commit");
        }

        let first_pool_versions = store.pools[0]
            .get_disks_by_key(object)
            .load_file_info_versions_exact(&bucket, object)
            .await
            .expect("first-pool metadata should decode")
            .expect("first pool must retain a free-version owner");
        assert_eq!(
            first_pool_versions
                .versions
                .iter()
                .chain(first_pool_versions.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .count(),
            1,
            "first pool must retain exactly one free-version owner"
        );
        assert_eq!(
            first_pool_versions
                .versions
                .iter()
                .filter(|version| !version.tier_free_version())
                .count(),
            0,
            "first pool must remove its visible source"
        );
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            0,
            "ordinary pool-local delete must not create a journal"
        );

        ExpiryState::resize_workers(1, store.clone()).await;
        let first_recovery = recover_tier_free_versions(store.clone(), 100, None, None)
            .await
            .expect("first-pool free-version should be recoverable");
        assert_eq!(
            (first_recovery.enqueued, first_recovery.failed),
            (0, 0),
            "the live second-pool source is the merged walk winner, so its hidden loser must not dispatch cleanup"
        );
        wait_for_expiry_workers_idle(&store).await;
        assert_eq!(backend.remove_count().await, 0, "live second-pool source must gate remote DELETE");
        assert_eq!(backend.object_count().await, 1, "live source must retain its remote body");
        assert!(
            store.pools[0]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(&bucket, object)
                .await
                .expect("blocked cleanup metadata should decode")
                .is_some(),
            "blocked cleanup must retain the first-pool free-version"
        );
        let surviving = store.pools[1]
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("second-pool source must remain GET-addressable");
        assert_eq!(surviving.transitioned_object.name, first.transitioned_object.name);

        backend.set_remove_failure(true);
        if batch {
            let (_deleted, errors, _accounting) = store.pools[1]
                .delete_objects_with_accounting(
                    &bucket,
                    vec![ObjectToDelete {
                        object_name: object.to_string(),
                        ..Default::default()
                    }],
                    delete_opts,
                )
                .await;
            assert!(errors.iter().all(Option::is_none), "second pool batch retry should commit: {errors:?}");
        } else {
            store.pools[1]
                .delete_object(&bucket, object, delete_opts)
                .await
                .expect("second pool single retry should commit");
        }
        wait_for_expiry_workers_idle(&store).await;
        let mut free_version_ids = Vec::new();
        for pool_idx in 0..2 {
            let retained = store.pools[pool_idx]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(&bucket, object)
                .await
                .expect("shared-tuple free-version metadata should decode")
                .expect("remote failure must retain every shared-tuple cleanup owner");
            let owners = retained
                .versions
                .iter()
                .chain(retained.free_versions.iter())
                .filter(|version| version.tier_free_version())
                .collect::<Vec<_>>();
            assert_eq!(owners.len(), 1, "pool {pool_idx} must retain one free-version owner");
            free_version_ids.push(owners[0].version_id);
        }
        assert_ne!(
            free_version_ids[0], free_version_ids[1],
            "ordinary pool-local deletes must model distinct local free-version identities"
        );

        backend.set_remove_failure(false);
        let receipts = receipt_sink
            .drain()
            .expect("the simulated outer multi-pool wrapper should drain exactly once");
        assert_eq!(
            receipts.len(),
            1,
            "the same physical key and remote tuple must collapse to one causal task"
        );
        assert_eq!(
            crate::bucket::lifecycle::bucket_lifecycle_ops::enqueue_committed_free_versions(&store, receipts).await,
            1,
            "the committed shared-tuple task should enter the running worker"
        );
        wait_for_expiry_workers_idle(&store).await;
        assert_eq!(backend.remove_count().await, 1, "shared remote tuple should be deleted exactly once");
        for pool_idx in 0..2 {
            assert!(
                store.pools[pool_idx]
                    .get_disks_by_key(object)
                    .load_file_info_versions_exact(&bucket, object)
                    .await
                    .expect("post-delete metadata should decode")
                    .is_none(),
                "pool {pool_idx} must not retain source or free-version metadata"
            );
        }
        store
            .delete_bucket(&bucket, &DeleteBucketOptions::default())
            .await
            .expect("shared-tuple bucket should be physically empty");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn multi_pool_same_remote_tuple_single_delete_waits_for_all_sources() {
        run_multi_pool_same_remote_tuple_delete_case(false).await;
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn multi_pool_same_remote_tuple_batch_delete_waits_for_all_sources() {
        run_multi_pool_same_remote_tuple_delete_case(true).await;
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn multi_pool_transitioned_delete_persists_one_free_version_per_remote_tuple() {
        let temp_dir = tempfile::tempdir().expect("create multi-pool transitioned delete store dir");
        let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "multi-pool-transitioned-delete-owner",
            &[4, 4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "MULTIPOOL-DELETE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "multi-pool-transitioned-delete-owner-bucket";
        let object = "archive.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("multi-pool source bucket should be created");

        let mut remote_tuples = Vec::new();
        for pool_idx in 0..2 {
            let mut reader = PutObjReader::from_vec(vec![b'a' + pool_idx as u8; 1024 * 1024]);
            let source = store.pools[pool_idx]
                .put_object(
                    bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        mod_time: Some(OffsetDateTime::now_utc() + time::Duration::seconds(pool_idx as i64)),
                        ..Default::default()
                    },
                )
                .await
                .expect("pool-local source should be written");
            store.pools[pool_idx]
                .transition_object(
                    bucket,
                    object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name.to_string(),
                            etag: source.etag.clone().expect("pool-local source should have an etag"),
                            ..Default::default()
                        },
                        mod_time: source.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("pool-local source should transition");
            let transitioned = store.pools[pool_idx]
                .get_object_info(bucket, object, &ObjectOptions::default())
                .await
                .expect("pool-local transitioned source should be readable");
            remote_tuples.push((
                transitioned.transitioned_object.name.clone(),
                transitioned.transitioned_object.version_id.clone(),
            ));
        }
        assert_ne!(remote_tuples[0], remote_tuples[1], "fixture must use distinct remote cleanup tuples");
        assert_eq!(backend.object_count().await, 2);

        backend.set_remove_failure(true);
        store
            .delete_object_with_tier_delete_journal(bucket, object, ObjectOptions::default())
            .await
            .expect("store-level delete should remove both pool copies");
        for pool_idx in 0..2 {
            let retained = store.pools[pool_idx]
                .get_disks_by_key(object)
                .load_file_info_versions_exact(bucket, object)
                .await
                .expect("pool-local post-delete metadata should decode")
                .expect("each pool-specific remote tuple must retain a cleanup owner");
            assert_eq!(
                retained
                    .versions
                    .iter()
                    .chain(retained.free_versions.iter())
                    .filter(|version| version.tier_free_version())
                    .count(),
                1,
                "pool {pool_idx} must retain exactly one free-version owner"
            );
            assert_eq!(
                retained
                    .versions
                    .iter()
                    .filter(|version| !version.tier_free_version())
                    .count(),
                0,
                "pool {pool_idx} must not retain a visible source"
            );
        }
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            0,
            "ordinary multi-pool delete must not create tier-delete journals"
        );
        assert_eq!(backend.object_count().await, 2, "failed remote cleanup must retain both remote objects");

        backend.set_remove_failure(false);
        wait_for_tier_free_version_recovery(store.clone(), &backend, 2).await;
        let mut removed = backend.remove_versions().await;
        removed.sort();
        remote_tuples.sort();
        assert_eq!(removed, remote_tuples, "recovery must use each pool's exact remote tuple");
        for pool_idx in 0..2 {
            assert!(
                store.pools[pool_idx]
                    .get_disks_by_key(object)
                    .load_file_info_versions_exact(bucket, object)
                    .await
                    .expect("post-recovery pool metadata should decode")
                    .is_none(),
                "pool {pool_idx} must not retain source or free-version metadata"
            );
        }
        store
            .delete_bucket(bucket, &DeleteBucketOptions::default())
            .await
            .expect("multi-pool source bucket should be physically empty");
    }

    // Phase 5 follow-up (backlog#1052): building a real store through the
    // ctx-explicit constructor lands every construction-time write — object
    // graph adoption, local-disk registry, deployment id — on the passed
    // context, not on the process bootstrap one. This is the storage-layer
    // seam a future second embedded server needs to stay isolated.
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn new_with_instance_ctx_threads_context_through_store_graph() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (instance_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "instance-ctx-store-graph-test", &[4])).await;

        assert!(
            Arc::ptr_eq(&store.ctx, &instance_ctx),
            "the store must adopt the explicitly passed instance context"
        );
        for sets in &store.pools {
            assert!(
                Arc::ptr_eq(sets.instance_ctx(), &instance_ctx),
                "every pool's Sets must carry the passed instance context"
            );
        }
        assert_eq!(
            instance_ctx.deployment_id(),
            Some(store.id),
            "the deployment id must land on the passed context and mirror the store id"
        );

        let registered: Vec<String> = instance_ctx.local_disk_map().read().await.keys().cloned().collect();
        assert_eq!(registered.len(), 4, "the passed context must register all four local disks");
        let registered_disk_ids = instance_ctx.local_disk_id_map();
        let registered_disk_ids = registered_disk_ids.read().await;
        assert_eq!(registered_disk_ids.len(), 4, "the passed context must publish all four disk IDs");
        for endpoint in registered_disk_ids.values() {
            assert!(
                registered.contains(endpoint),
                "every disk ID in the passed context must resolve to one of its registered endpoints"
            );
        }
        drop(registered_disk_ids);
        let bootstrap = crate::runtime::instance::bootstrap_ctx();
        assert_ne!(
            bootstrap.deployment_id(),
            Some(store.id),
            "the bootstrap context must not absorb the fresh store's deployment id"
        );
        let bootstrap_map = bootstrap.local_disk_map();
        let bootstrap_map = bootstrap_map.read().await;
        for key in &registered {
            assert!(
                !bootstrap_map.contains_key(key),
                "the bootstrap context must not absorb the fresh store's disks"
            );
        }
        drop(bootstrap_map);
        let bootstrap_disk_ids = bootstrap.local_disk_id_map();
        let bootstrap_disk_ids = bootstrap_disk_ids.read().await;
        for endpoint in bootstrap_disk_ids.values() {
            assert!(
                !registered.contains(endpoint),
                "the bootstrap context must not absorb the fresh store's disk IDs"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn new_with_instance_ctx_applies_default_parity_to_each_real_pool() {
        let temp_dir = tempfile::tempdir().expect("create multi-pool store dir");
        let (_, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "pool-parity-regression", &[4, 2])).await;

        assert_eq!(store.pools.len(), 2);
        assert_eq!(store.pools[0].default_parity_count, 2);
        assert_eq!(store.pools[0].disk_set[0].default_parity_count, 2);
        assert_eq!(store.pools[1].default_parity_count, 1);
        assert_eq!(store.pools[1].disk_set[0].default_parity_count, 1);
    }

    // backlog#1052 S3: two stores in one process each initialize their own
    // bucket metadata system on their own instance context. Before this, the
    // second `init_bucket_metadata_sys` panicked on the process-global
    // OnceLock — the hard blocker for a second embedded server's services.
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn two_stores_initialize_their_own_bucket_metadata_sys() {
        let temp_a = tempfile::tempdir().expect("create temp store dir a");
        let temp_b = tempfile::tempdir().expect("create temp store dir b");
        let (ctx_a, store_a, _shutdown_a) =
            without_storage_class_env(build_isolated_test_store(temp_a.path(), "bucket-metadata-isolation-a", &[4])).await;
        let (ctx_b, store_b, _shutdown_b) =
            without_storage_class_env(build_isolated_test_store(temp_b.path(), "bucket-metadata-isolation-b", &[4])).await;

        crate::bucket::metadata_sys::init_bucket_metadata_sys(store_a.clone(), Vec::new()).await;
        // The old process-global cell would panic right here.
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store_b.clone(), Vec::new()).await;

        let sys_a = ctx_a
            .bucket_metadata_sys()
            .expect("store A's context must hold its metadata system");
        let sys_b = ctx_b
            .bucket_metadata_sys()
            .expect("store B's context must hold its metadata system");
        assert!(!Arc::ptr_eq(&sys_a, &sys_b), "each store must own a distinct bucket metadata system");
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn same_name_bucket_recursive_delete_uses_owning_instance_object_lock_state() {
        let temp_a = tempfile::tempdir().expect("create temp store dir a");
        let temp_b = tempfile::tempdir().expect("create temp store dir b");
        let (_ctx_a, store_a, _shutdown_a) =
            without_storage_class_env(build_isolated_test_store(temp_a.path(), "delete-scope-a", &[4])).await;
        let (_ctx_b, store_b, _shutdown_b) =
            without_storage_class_env(build_isolated_test_store(temp_b.path(), "delete-scope-b", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store_a.clone(), Vec::new()).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store_b.clone(), Vec::new()).await;

        let bucket = format!("same-name-delete-scope-{}", uuid::Uuid::new_v4());
        store_a
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create plain bucket in store A");
        store_b
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    lock_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create Object Lock bucket in store B");

        for store in [&store_a, &store_b] {
            let mut reader = PutObjReader::from_vec(b"payload".to_vec());
            store
                .put_object(
                    &bucket,
                    "prefix/object.bin",
                    &mut reader,
                    &ObjectOptions {
                        versioned: Arc::ptr_eq(store, &store_b),
                        ..Default::default()
                    },
                )
                .await
                .expect("put same-name object");
        }

        store_a
            .delete_object(
                &bucket,
                "prefix",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect("plain store prefix delete should remain allowed");

        let err = store_b
            .delete_object(
                &bucket,
                "prefix",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("store B must not consult store A's plain bucket metadata");
        assert!(matches!(err, StorageError::InvalidArgument(_, _, _)));
        store_b
            .get_object_info(&bucket, "prefix/object.bin", &ObjectOptions::default())
            .await
            .expect("locked store object must survive the rejected prefix delete");
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn recursive_delete_checks_object_lock_on_every_version_page() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "recursive-delete-pagination", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("recursive-page-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create plain bucket");
        for object in ["prefix/a.bin", "prefix/b.bin"] {
            let mut reader = PutObjReader::from_vec(b"plain".to_vec());
            store
                .put_object(&bucket, object, &mut reader, &ObjectOptions::default())
                .await
                .expect("write first-page object");
        }
        let locked_object = "prefix/z-locked.bin";
        let mut reader = PutObjReader::from_vec(b"locked".to_vec());
        store
            .put_object(
                &bucket,
                locked_object,
                &mut reader,
                &ObjectOptions {
                    user_defined: HashMap::from([
                        (
                            s3s::header::X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
                            s3s::dto::ObjectLockRetentionMode::COMPLIANCE.to_string(),
                        ),
                        (
                            s3s::header::X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(),
                            "2099-01-01T00:00:00Z".to_string(),
                        ),
                    ]),
                    ..Default::default()
                },
            )
            .await
            .expect("write second-page locked object");

        let first_page = store.pools[0].disk_set[0]
            .clone()
            .inner_list_object_versions_for_recursive_delete(&bucket, "prefix/", None, None, 2)
            .await
            .expect("list the first recursive-delete validation page");
        assert!(first_page.is_truncated, "the fixture must require a continuation page");
        assert!(
            first_page.objects.iter().all(|object| object.name != locked_object),
            "the locked object must not be visible on the first page"
        );
        let second_page = store.pools[0].disk_set[0]
            .clone()
            .inner_list_object_versions_for_recursive_delete(
                &bucket,
                "prefix/",
                first_page.next_marker,
                first_page.next_version_idmarker,
                2,
            )
            .await
            .expect("list the recursive-delete continuation page");
        assert!(
            second_page.objects.iter().any(|object| object.name == locked_object),
            "the continuation page must contain the locked object"
        );

        let err = store
            .delete_object(
                &bucket,
                "prefix/",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("recursive delete must scan the continuation page and find the locked object");
        assert!(
            matches!(err, StorageError::PrefixAccessDenied(ref name, ref object) if name == &bucket && object == locked_object),
            "unexpected recursive delete error: {err:?}"
        );
        for object in ["prefix/a.bin", "prefix/b.bin", locked_object] {
            store
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .expect("a rejected recursive delete must leave every page intact");
        }
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn delete_holds_bucket_incarnation_sentinel_through_object_commit() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "delete-bucket-incarnation", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("delete-bucket-incarnation-{}", uuid::Uuid::new_v4());
        let object = "same-key.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create initial plain bucket");
        let old_incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("read initial bucket incarnation");
        let mut reader = PutObjReader::from_vec(b"old generation".to_vec());
        store
            .put_object(&bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("put old-generation object");

        let barrier = crate::store::object::DeleteAfterObjectLockSnapshotBarrier::install(&bucket);
        let delete_store = Arc::clone(&store);
        let delete_bucket = bucket.clone();
        let delete = tokio::spawn(async move {
            delete_store
                .delete_object(&delete_bucket, object, ObjectOptions::default())
                .await
        });
        barrier.wait_until_paused().await;

        let recreate_store = Arc::clone(&store);
        let recreate_bucket = bucket.clone();
        let mut recreate = tokio::spawn(async move {
            recreate_store
                .delete_bucket(
                    &recreate_bucket,
                    &crate::storage_api_contracts::bucket::DeleteBucketOptions {
                        force: true,
                        ..Default::default()
                    },
                )
                .await?;
            recreate_store
                .make_bucket(
                    &recreate_bucket,
                    &MakeBucketOptions {
                        lock_enabled: true,
                        ..Default::default()
                    },
                )
                .await?;
            let mut reader = PutObjReader::from_vec(b"new protected generation".to_vec());
            recreate_store
                .put_object(
                    &recreate_bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await?;
            Ok::<_, Error>(())
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut recreate).await.is_err(),
            "bucket delete and same-name recreation must wait for the in-flight object delete"
        );
        barrier.release();
        delete
            .await
            .expect("delete task should join")
            .expect("old-generation delete should complete");
        tokio::time::timeout(Duration::from_secs(10), recreate)
            .await
            .expect("bucket recreation should finish after delete releases the sentinel")
            .expect("bucket recreation task should join")
            .expect("bucket recreation should succeed");

        let stale_delete_err = store
            .delete_object(
                &bucket,
                object,
                ObjectOptions {
                    expected_bucket_incarnation_id: Some(old_incarnation),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("an authorization snapshot from the old bucket incarnation must be rejected");
        assert!(matches!(stale_delete_err, StorageError::BucketNotFound(name) if name == bucket));

        let (_deleted, stale_batch_errors) = store
            .delete_objects(
                &bucket,
                vec![crate::storage_api_contracts::object::ObjectToDelete {
                    object_name: object.to_string(),
                    ..Default::default()
                }],
                ObjectOptions {
                    expected_bucket_incarnation_id: Some(old_incarnation),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await;
        assert!(
            matches!(stale_batch_errors.as_slice(), [Some(StorageError::BucketNotFound(name))] if name.as_str() == bucket),
            "batch delete must reject an authorization snapshot from the old bucket incarnation"
        );

        store
            .get_object_info(
                &bucket,
                object,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the new locked-bucket object must survive the old-generation delete");
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn delete_objects_holds_bucket_incarnation_sentinel_through_commit() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "batch-delete-bucket-incarnation", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("batch-incarnation-{}", uuid::Uuid::new_v4());
        let object = "same-key.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create initial bucket");
        let mut reader = PutObjReader::from_vec(b"old generation".to_vec());
        store
            .put_object(&bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("put old-generation object");

        let barrier = crate::store::object::DeleteAfterObjectLockSnapshotBarrier::install(&bucket);
        let delete_store = Arc::clone(&store);
        let delete_bucket = bucket.clone();
        let delete = tokio::spawn(async move {
            delete_store
                .delete_objects(
                    &delete_bucket,
                    vec![crate::storage_api_contracts::object::ObjectToDelete {
                        object_name: object.to_string(),
                        ..Default::default()
                    }],
                    ObjectOptions::default(),
                )
                .await
        });
        barrier.wait_until_paused().await;

        let recreate_store = Arc::clone(&store);
        let recreate_bucket = bucket.clone();
        let mut recreate = tokio::spawn(async move {
            recreate_store
                .delete_bucket(
                    &recreate_bucket,
                    &crate::storage_api_contracts::bucket::DeleteBucketOptions {
                        force: true,
                        ..Default::default()
                    },
                )
                .await?;
            recreate_store
                .make_bucket(&recreate_bucket, &MakeBucketOptions::default())
                .await?;
            let mut reader = PutObjReader::from_vec(b"new generation".to_vec());
            recreate_store
                .put_object(&recreate_bucket, object, &mut reader, &ObjectOptions::default())
                .await?;
            Ok::<_, Error>(())
        });

        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut recreate).await.is_err(),
            "bucket recreation must wait for the in-flight batch delete"
        );
        barrier.release();
        let (_deleted, errors) = delete.await.expect("batch delete task should join");
        assert!(
            errors.iter().all(Option::is_none),
            "old-generation batch delete should complete: {errors:?}"
        );
        tokio::time::timeout(Duration::from_secs(10), recreate)
            .await
            .expect("bucket recreation should finish after batch delete releases the sentinel")
            .expect("bucket recreation task should join")
            .expect("bucket recreation should succeed");
        store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("new-generation object must survive the old batch delete");
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn prefix_delete_serializes_with_concurrent_object_lock_metadata_updates() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "prefix-delete-lock-metadata", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let cases = [
            (
                "compliance",
                HashMap::from([
                    ("x-amz-object-lock-mode".to_string(), "COMPLIANCE".to_string()),
                    ("x-amz-object-lock-retain-until-date".to_string(), "2099-01-01T00:00:00Z".to_string()),
                ]),
            ),
            (
                "legal-hold",
                HashMap::from([("x-amz-object-lock-legal-hold".to_string(), "ON".to_string())]),
            ),
        ];

        for (case, eval_metadata) in cases {
            let bucket = format!("prefix-lock-{case}-{}", uuid::Uuid::new_v4());
            let object = "protected-after-scan.bin";
            store
                .make_bucket(
                    &bucket,
                    &MakeBucketOptions {
                        lock_enabled: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("create Object Lock bucket");
            let mut reader = PutObjReader::from_vec(b"body".to_vec());
            let object_info = store
                .put_object(
                    &bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
                .expect("write unprotected version");
            let version_id = object_info.version_id.expect("versioned PUT should return an ID").to_string();

            let barrier = crate::store::object::DeleteAfterObjectLockSnapshotBarrier::install(&bucket);
            let delete_store = Arc::clone(&store);
            let delete_bucket = bucket.clone();
            let delete = tokio::spawn(async move {
                delete_store
                    .delete_object(
                        &delete_bucket,
                        object,
                        ObjectOptions {
                            delete_prefix: true,
                            delete_prefix_object: true,
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await
            });
            barrier.wait_until_paused().await;

            let update_store = Arc::clone(&store);
            let update_bucket = bucket.clone();
            let mut update = tokio::spawn(async move {
                update_store
                    .put_object_metadata(
                        &update_bucket,
                        object,
                        &ObjectOptions {
                            version_id: Some(version_id),
                            versioned: true,
                            eval_metadata: Some(eval_metadata),
                            ..Default::default()
                        },
                    )
                    .await
            });
            assert!(
                tokio::time::timeout(Duration::from_millis(100), &mut update).await.is_err(),
                "{case} metadata update must wait behind the prefix-delete lifecycle write fence"
            );

            barrier.release();
            delete
                .await
                .expect("prefix delete task should join")
                .expect("prefix delete should remove the unprotected version");
            update
                .await
                .expect("metadata update task should join")
                .expect_err("metadata update must not succeed after the version was deleted");
            assert!(
                store
                    .get_object_info(
                        &bucket,
                        object,
                        &ObjectOptions {
                            versioned: true,
                            ..Default::default()
                        },
                    )
                    .await
                    .is_err(),
                "{case} case must not report a successful protected write followed by deletion"
            );
        }
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn put_rejects_same_name_bucket_recreated_after_request_snapshot() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "put-bucket-incarnation", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("put-bucket-incarnation-{}", uuid::Uuid::new_v4());
        let object = "object.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create initial bucket");
        let old_incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("read initial bucket incarnation");

        let barrier = crate::set_disk::PutObjectCommitBarrier::install(
            &bucket,
            object,
            crate::set_disk::PutObjectCommitPause::BeforeNamespace,
        );
        let put_store = Arc::clone(&store);
        let put_bucket = bucket.clone();
        let put = tokio::spawn(async move {
            let mut reader = PutObjReader::from_vec(b"old generation".to_vec());
            put_store
                .put_object(
                    &put_bucket,
                    object,
                    &mut reader,
                    &ObjectOptions {
                        versioned: true,
                        ..Default::default()
                    },
                )
                .await
        });
        barrier.wait_until_paused().await;

        store
            .delete_bucket(&bucket, &crate::storage_api_contracts::bucket::DeleteBucketOptions::default())
            .await
            .expect("delete initial empty bucket while PUT is staged");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate same-name bucket");
        let new_incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("read recreated bucket incarnation");
        assert_ne!(old_incarnation, new_incarnation);

        barrier.release();
        let err = put
            .await
            .expect("PUT task should join")
            .expect_err("old-generation PUT must not commit into the recreated bucket");
        assert!(
            matches!(err, StorageError::BucketNotFound(ref name) if name == &bucket),
            "unexpected stale PUT error: {err:?}"
        );
        assert!(
            store
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .is_err(),
            "the recreated bucket must not contain the staged old-generation object"
        );
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn exact_version_put_uses_only_its_own_bucket_object_lock_snapshot() {
        let target_temp = tempfile::tempdir().expect("create target store dir");
        let foreign_temp = tempfile::tempdir().expect("create foreign store dir");
        let (target_ctx, target_store, _target_shutdown) =
            without_storage_class_env(build_isolated_test_store(target_temp.path(), "put-object-lock-target", &[4])).await;
        let (_foreign_ctx, foreign_store, _foreign_shutdown) =
            without_storage_class_env(build_isolated_test_store(foreign_temp.path(), "put-object-lock-foreign", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(target_store.clone(), Vec::new()).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(foreign_store.clone(), Vec::new()).await;

        let bucket = format!("put-object-lock-snapshot-{}", uuid::Uuid::new_v4());
        let object = "object.bin";
        target_store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    lock_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create target Object Lock bucket");
        foreign_store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create same-name plain bucket in foreign store");
        let stale_snapshot = target_store
            .object_lock_config_snapshot(&bucket)
            .await
            .expect("load target snapshot before changing its default retention");

        let mut metadata = crate::bucket::metadata_sys::get_in(&target_ctx, &bucket)
            .await
            .expect("load target bucket metadata")
            .as_ref()
            .clone();
        metadata
            .update_config(
                crate::bucket::metadata::OBJECT_LOCK_CONFIG,
                b"<ObjectLockConfiguration><ObjectLockEnabled>Enabled</ObjectLockEnabled><Rule><DefaultRetention><Mode>COMPLIANCE</Mode><Days>1</Days></DefaultRetention></Rule></ObjectLockConfiguration>".to_vec(),
            )
            .expect("set default COMPLIANCE metadata");
        let update_started = Arc::new(tokio::sync::Barrier::new(2));
        let update_ctx = Arc::clone(&target_ctx);
        let update_started_task = Arc::clone(&update_started);
        let mut update = tokio::spawn(async move {
            update_started_task.wait().await;
            let _transaction_guard =
                crate::bucket::metadata_sys::acquire_bucket_metadata_transaction_lock_in(&update_ctx, &metadata.name).await?;
            crate::bucket::metadata_sys::set_bucket_metadata_in(&update_ctx, metadata).await
        });
        update_started.wait().await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), &mut update).await.is_err(),
            "an active Object Lock snapshot must hold the metadata revision read fence"
        );
        drop(stale_snapshot);
        update
            .await
            .expect("Object Lock metadata update task should join")
            .expect("persist target Object Lock metadata after snapshot release");

        let original_body = b"protected body".to_vec();
        let mut original_reader = PutObjReader::from_vec(original_body.clone());
        let original = target_store
            .put_object(
                &bucket,
                object,
                &mut original_reader,
                &ObjectOptions {
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("write protected target version");
        let version_id = original
            .version_id
            .expect("versioned PUT should return a version ID")
            .to_string();

        let mut replacement = PutObjReader::from_vec(b"replacement".to_vec());
        let err = target_store
            .put_object(
                &bucket,
                object,
                &mut replacement,
                &ObjectOptions {
                    version_id: Some(version_id.clone()),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("ECStore must load default COMPLIANCE before an exact-version overwrite");
        assert!(matches!(err, StorageError::PrefixAccessDenied(_, _)));

        let foreign_snapshot = foreign_store
            .object_lock_config_snapshot(&bucket)
            .await
            .expect("load same-name foreign bucket snapshot");
        let mut foreign_replacement = PutObjReader::from_vec(b"foreign replacement".to_vec());
        target_store
            .put_object(
                &bucket,
                object,
                &mut foreign_replacement,
                &ObjectOptions {
                    version_id: Some(version_id.clone()),
                    versioned: true,
                    object_lock_config_snapshot: Some(foreign_snapshot),
                    ..Default::default()
                },
            )
            .await
            .expect_err("a snapshot from another store must fail closed");

        let mut reader = target_store
            .get_object_reader(
                &bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    version_id: Some(version_id),
                    versioned: true,
                    ..Default::default()
                },
            )
            .await
            .expect("protected target version should remain readable");
        let mut body = Vec::new();
        reader
            .stream
            .read_to_end(&mut body)
            .await
            .expect("protected body should drain");
        assert_eq!(body, original_body);
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn force_create_existing_bucket_preserves_incarnation_and_inflight_request() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "force-create-incarnation", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("force-create-incarnation-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create initial bucket");
        let incarnation = store.bucket_incarnation_id(&bucket).await.expect("read initial incarnation");

        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    force_create: true,
                    ..Default::default()
                },
            )
            .await
            .expect("idempotent force-create should succeed");
        assert_eq!(
            store.bucket_incarnation_id(&bucket).await.unwrap(),
            incarnation,
            "idempotent force-create must not rotate the bucket generation"
        );

        let mut reader = PutObjReader::from_vec(b"authorized before force-create".to_vec());
        store
            .put_object(
                &bucket,
                "inflight.bin",
                &mut reader,
                &ObjectOptions {
                    expected_bucket_incarnation_id: Some(incarnation),
                    ..Default::default()
                },
            )
            .await
            .expect("a request authorized before idempotent force-create must still commit");
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn lock_enabled_create_retries_metadata_intent_without_unlocking() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "lock-create-intent-retry", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("lock-create-intent-retry-{}", uuid::Uuid::new_v4());
        let mut intent = crate::bucket::metadata::BucketMetadata::new(&bucket);
        intent.lock_enabled = true;
        crate::bucket::metadata_sys::set_new_bucket_metadata_in(&ctx, intent)
            .await
            .expect("persist pre-visibility lock intent");
        assert!(
            store
                .peer_sys
                .get_bucket_info(&bucket, &crate::storage_api_contracts::bucket::BucketOptions::default())
                .await
                .is_err(),
            "the intent must not make the physical bucket visible"
        );

        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    lock_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("retry lock-enabled creation");
        assert!(matches!(
            crate::bucket::metadata_sys::get_object_lock_config_state_in(&ctx, &bucket)
                .await
                .expect("read retried lock state"),
            crate::bucket::metadata_sys::ObjectLockConfigState::Configured { .. }
        ));
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn physical_lock_bucket_with_incomplete_metadata_fails_closed() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "lock-create-incomplete-metadata", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("lock-create-incomplete-metadata-{}", uuid::Uuid::new_v4());
        let mut metadata = crate::bucket::metadata::BucketMetadata::new(&bucket);
        metadata.lock_enabled = true;
        metadata
            .save_with_store(store.clone())
            .await
            .expect("persist lock metadata before sidecar");
        store
            .peer_sys
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("expose physical bucket after metadata phase");

        let err = crate::bucket::metadata_sys::get_object_lock_config_state_in(&ctx, &bucket)
            .await
            .expect_err("missing sidecar must not fabricate an unlocked bucket");
        assert!(err.to_string().contains("sidecar is missing"), "unexpected error: {err}");
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn successful_lock_enabled_create_has_complete_metadata_intent() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "lock-create-complete-metadata", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("lock-create-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    lock_enabled: true,
                    ..Default::default()
                },
            )
            .await
            .expect("create lock-enabled bucket");
        let (metadata, persisted) = crate::bucket::metadata_sys::get_config_from_disk_with_presence_in(&ctx, &bucket)
            .await
            .expect("read completed metadata intent");
        assert!(persisted && metadata.bucket_incarnation_sidecar && metadata.lock_enabled);
        assert!(matches!(
            crate::bucket::metadata_sys::get_object_lock_config_state_in(&ctx, &bucket)
                .await
                .expect("read completed lock state"),
            crate::bucket::metadata_sys::ObjectLockConfigState::Configured { .. }
        ));
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn lifecycle_expiry_rejects_task_from_recreated_bucket_incarnation() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "lifecycle-expiry-incarnation", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("lifecycle-incarnation-{}", uuid::Uuid::new_v4());
        let object = "same-key.bin";
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create initial bucket");
        let old_incarnation = store.bucket_incarnation_id(&bucket).await.expect("read old incarnation");
        let mut old_reader = PutObjReader::from_vec(b"old-generation".to_vec());
        store
            .put_object(&bucket, object, &mut old_reader, &ObjectOptions::default())
            .await
            .expect("put old-generation object");
        let old_object = store
            .get_object_info(&bucket, object, &ObjectOptions::default())
            .await
            .expect("snapshot old-generation object");

        store
            .delete_object(&bucket, object, ObjectOptions::default())
            .await
            .expect("delete old-generation object");
        store
            .delete_bucket(&bucket, &crate::storage_api_contracts::bucket::DeleteBucketOptions::default())
            .await
            .expect("delete old bucket");
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("recreate bucket");
        let mut new_reader = PutObjReader::from_vec(b"new-generation".to_vec());
        store
            .put_object(&bucket, object, &mut new_reader, &ObjectOptions::default())
            .await
            .expect("put replacement object");

        let applied = crate::bucket::lifecycle::bucket_lifecycle_ops::apply_expiry_on_non_transitioned_objects(
            store.clone(),
            &old_object,
            &crate::bucket::lifecycle::lifecycle::Event {
                action: crate::bucket::lifecycle::lifecycle::IlmAction::DeleteAction,
                ..Default::default()
            },
            &crate::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc::Scanner,
            old_incarnation,
        )
        .await;
        assert!(!applied, "an expiry task from the old bucket incarnation must fail closed");

        let mut reader = store
            .get_object_reader(&bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("new-generation object must survive stale expiry");
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("read replacement object");
        assert_eq!(body, b"new-generation");
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn pre_upgrade_multipart_upload_can_complete_and_abort_in_the_same_bucket_lifetime() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "multipart-upgrade-compat", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("multipart-upgrade-compat-{}", uuid::Uuid::new_v4());
        let created = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixed timestamp should be valid");
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    created_at: Some(created),
                    ..Default::default()
                },
            )
            .await
            .expect("create bucket");

        let complete_object = "complete.bin";
        let complete_upload = store.pools[0]
            .new_multipart_upload(
                &bucket,
                complete_object,
                &ObjectOptions {
                    mod_time: Some(created + time::Duration::seconds(1)),
                    ..Default::default()
                },
            )
            .await
            .expect("create a pre-upgrade multipart upload without an incarnation stamp");
        let mut part_reader = PutObjReader::from_vec(b"pre-upgrade multipart body".to_vec());
        let part = store.pools[0]
            .put_object_part(
                &bucket,
                complete_object,
                &complete_upload.upload_id,
                1,
                &mut part_reader,
                &ObjectOptions::default(),
            )
            .await
            .expect("stage a pre-upgrade multipart part");
        store
            .clone()
            .complete_multipart_upload(
                &bucket,
                complete_object,
                &complete_upload.upload_id,
                vec![crate::storage_api_contracts::multipart::CompletePart {
                    part_num: part.part_num,
                    etag: part.etag,
                    ..Default::default()
                }],
                &ObjectOptions::default(),
            )
            .await
            .expect("a pre-upgrade upload from the current bucket lifetime should complete");
        store
            .get_object_info(&bucket, complete_object, &ObjectOptions::default())
            .await
            .expect("completed pre-upgrade upload should be visible");

        let abort_object = "abort.bin";
        let abort_upload = store.pools[0]
            .new_multipart_upload(
                &bucket,
                abort_object,
                &ObjectOptions {
                    mod_time: Some(created + time::Duration::seconds(2)),
                    ..Default::default()
                },
            )
            .await
            .expect("create another pre-upgrade multipart upload");
        store
            .abort_multipart_upload(&bucket, abort_object, &abort_upload.upload_id, &ObjectOptions::default())
            .await
            .expect("a pre-upgrade upload from the current bucket lifetime should abort");
        let err = store
            .get_multipart_info(&bucket, abort_object, &abort_upload.upload_id, &ObjectOptions::default())
            .await
            .expect_err("aborted pre-upgrade upload should be removed");
        assert!(matches!(err, StorageError::InvalidUploadID(..)));
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn complete_multipart_rejects_legacy_upload_from_recreated_bucket_incarnation() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "multipart-bucket-incarnation", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("multipart-incarnation-{}", uuid::Uuid::new_v4());
        let object = "object.bin";
        let created = OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("fixed timestamp should be valid");
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    created_at: Some(created),
                    ..Default::default()
                },
            )
            .await
            .expect("create initial bucket");
        let old_incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("read initial bucket incarnation");
        let upload = store.pools[0]
            .new_multipart_upload(
                &bucket,
                object,
                &ObjectOptions {
                    mod_time: Some(created + time::Duration::seconds(1)),
                    ..Default::default()
                },
            )
            .await
            .expect("create old-generation legacy multipart upload");

        store
            .delete_bucket(&bucket, &crate::storage_api_contracts::bucket::DeleteBucketOptions::default())
            .await
            .expect("delete bucket with an incomplete multipart upload");
        store
            .make_bucket(
                &bucket,
                &MakeBucketOptions {
                    created_at: Some(created + time::Duration::seconds(2)),
                    ..Default::default()
                },
            )
            .await
            .expect("recreate same-name bucket");
        let new_incarnation = store
            .bucket_incarnation_id(&bucket)
            .await
            .expect("read recreated bucket incarnation");
        assert_ne!(old_incarnation, new_incarnation);

        let err = store
            .clone()
            .complete_multipart_upload(&bucket, object, &upload.upload_id, Vec::new(), &ObjectOptions::default())
            .await
            .expect_err("an upload initiated in the old bucket incarnation must not commit into the replacement");
        assert!(
            match &err {
                StorageError::BucketNotFound(name) => name == &bucket,
                StorageError::InvalidUploadID(name, key, _) => name == &bucket && key == object,
                _ => false,
            },
            "unexpected stale multipart completion error: {err:?}"
        );
        assert!(
            store
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .is_err(),
            "the recreated bucket must not contain the old-generation multipart upload"
        );
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn delete_after_bucket_validation_preserves_bucket_not_found() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "delete-missing-bucket", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("delete-missing-bucket-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket before validation");
        store
            .get_bucket_info(&bucket, &crate::storage_api_contracts::bucket::BucketOptions::default())
            .await
            .expect("request-level bucket validation should succeed");
        store
            .delete_bucket(&bucket, &crate::storage_api_contracts::bucket::DeleteBucketOptions::default())
            .await
            .expect("delete bucket after validation");

        let err = store
            .delete_object(&bucket, "missing.bin", ObjectOptions::default())
            .await
            .expect_err("post-validation bucket deletion must not become an object-not-found error");

        assert!(matches!(err, StorageError::BucketNotFound(name) if name == bucket));
    }

    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn object_lock_metadata_read_error_blocks_every_destructive_delete_shape() {
        let temp = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp.path(), "delete-config-read-error", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let bucket = format!("delete-config-read-error-{}", uuid::Uuid::new_v4());
        store
            .make_bucket(&bucket, &MakeBucketOptions::default())
            .await
            .expect("create bucket");
        for object in ["single.bin", "batch.bin", "prefix/child.bin", "lifecycle.bin"] {
            let mut reader = PutObjReader::from_vec(b"must survive".to_vec());
            store
                .put_object(&bucket, object, &mut reader, &ObjectOptions::default())
                .await
                .expect("put object before metadata corruption");
        }

        crate::bucket::metadata_sys::inject_object_lock_disk_read_error_in(&ctx, &bucket)
            .await
            .expect("inject single-delete metadata read failure");

        let single_err = store
            .delete_object(&bucket, "single.bin", ObjectOptions::default())
            .await
            .expect_err("single delete must propagate the metadata read failure");
        assert!(
            single_err
                .to_string()
                .contains("injected Object Lock metadata disk read failure")
        );

        crate::bucket::metadata_sys::inject_object_lock_disk_read_error_in(&ctx, &bucket)
            .await
            .expect("inject batch-delete metadata read failure");
        let (_deleted, batch_errs) = store
            .delete_objects(
                &bucket,
                vec![crate::storage_api_contracts::object::ObjectToDelete {
                    object_name: "batch.bin".to_string(),
                    ..Default::default()
                }],
                ObjectOptions::default(),
            )
            .await;
        assert!(
            batch_errs[0]
                .as_ref()
                .is_some_and(|err| err.to_string().contains("injected Object Lock metadata disk read failure"))
        );

        crate::bucket::metadata_sys::inject_object_lock_disk_read_error_in(&ctx, &bucket)
            .await
            .expect("inject prefix-delete metadata read failure");
        let prefix_err = store
            .delete_object(
                &bucket,
                "prefix",
                ObjectOptions {
                    delete_prefix: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("prefix delete must propagate the metadata read failure");
        assert!(
            prefix_err
                .to_string()
                .contains("injected Object Lock metadata disk read failure")
        );

        crate::bucket::metadata_sys::inject_object_lock_disk_read_error_in(&ctx, &bucket)
            .await
            .expect("inject lifecycle-style delete-all metadata read failure");
        let lifecycle_err = store
            .delete_object(
                &bucket,
                "lifecycle.bin",
                ObjectOptions {
                    delete_prefix: true,
                    delete_prefix_object: true,
                    ..Default::default()
                },
            )
            .await
            .expect_err("lifecycle-style delete-all must propagate the metadata read failure");
        assert!(
            lifecycle_err
                .to_string()
                .contains("injected Object Lock metadata disk read failure")
        );

        for object in ["single.bin", "batch.bin", "prefix/child.bin", "lifecycle.bin"] {
            store
                .get_object_info(&bucket, object, &ObjectOptions::default())
                .await
                .expect("metadata failure must preserve every object");
        }
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn legacy_tier_delete_journals_create_redacted_recovery_controls_without_remote_calls() {
        let temp_dir = tempfile::tempdir().expect("create legacy journal recovery store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "legacy-tier-journal-recovery", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "LEGACY-RECOVERY";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("legacy recovery tier lease should resolve")
            .backend_identity();
        let fixtures = [
            serde_json::json!({
                "version": 1,
                "obj_name": "legacy/remote-v1",
                "version_id": "opaque-v1",
                "tier_name": tier_name,
            }),
            serde_json::json!({
                "version": 2,
                "obj_name": "legacy/remote-v2",
                "version_id": "opaque-v2",
                "tier_name": tier_name,
                "backend_identity": backend_identity,
            }),
        ];
        let mut journal_paths = Vec::new();
        for fixture in &fixtures {
            let data = serde_json::to_vec(&fixture).expect("legacy journal fixture should encode");
            let entry = crate::bucket::lifecycle::tier_delete_journal::decode_tier_delete_journal_entry(&data)
                .expect("legacy journal fixture should decode");
            let path = tier_delete_journal_object_name(&entry);
            com::save_config(store.clone(), &path, data)
                .await
                .expect("legacy journal fixture should persist");
            journal_paths.push(path);
        }
        let corrupt_path = format!(
            "{TIER_DELETE_JOURNAL_PREFIX}/{}.json",
            rustfs_utils::crypto::hex_sha256(b"corrupt legacy tier journal", ToOwned::to_owned)
        );
        com::save_config(store.clone(), &corrupt_path, b"{corrupt".to_vec())
            .await
            .expect("corrupt legacy journal fixture should persist");

        let (first, concurrent) = tokio::join!(
            recover_tier_delete_journal_entries(store.clone(), 100, None),
            recover_tier_delete_journal_entries(store.clone(), 100, None),
        );
        for stats in [first, concurrent] {
            let stats = stats.expect("concurrent legacy journal recovery scan should finish");
            assert_eq!((stats.scanned, stats.deleted, stats.failed), (3, 0, 0));
        }
        assert_eq!(tier_delete_journal_count(store.clone()).await, 3);
        assert_eq!(backend.remove_count().await, 0, "legacy recovery must not call the remote tier");
        assert_eq!(backend.exact_remove_count(), 0, "legacy recovery must not issue exact remote DELETE");
        assert!(backend.op_log().await.is_empty(), "legacy recovery must not invoke any backend operation");

        let mut first_controls = list_recovery_controls(store.clone(), IlmRecoveryProtocol::TierDeleteJournal, None, 100, None)
            .await
            .expect("legacy recovery controls should be listable")
            .records;
        first_controls.sort_by(|left, right| left.control_id.cmp(&right.control_id));
        assert_eq!(first_controls.len(), 3);
        assert_eq!(
            first_controls
                .iter()
                .filter(|control| control.classification == IlmRecoveryClassification::RetainedAmbiguous)
                .count(),
            2
        );
        assert_eq!(
            first_controls
                .iter()
                .filter(|control| control.classification == IlmRecoveryClassification::Corrupt)
                .count(),
            1
        );
        for view in &first_controls {
            assert_eq!(view.protocol, IlmRecoveryProtocol::TierDeleteJournal);
            assert_eq!(view.revision, 1);
            assert_eq!(view.attempt_count, 0);
            let encoded = serde_json::to_string(view).expect("recovery control view should encode");
            for secret in ["legacy/remote-v1", "legacy/remote-v2", "opaque-v1", "opaque-v2", tier_name] {
                assert!(!encoded.contains(secret), "recovery control view must redact `{secret}`");
            }
            let persisted = load_recovery_control(store.clone(), IlmRecoveryProtocol::TierDeleteJournal, &view.control_id)
                .await
                .expect("legacy recovery control should load");
            match view.source_schema.as_str() {
                "rustfs-tier-delete-journal-v1" => {
                    assert_eq!(persisted.control.identity.record_class, "tier_delete_journal_v1");
                    assert_eq!(view.last_error_code, IlmRecoveryErrorCode::RemoteVersionUnknown);
                }
                "rustfs-tier-delete-journal-v2" => {
                    assert_eq!(persisted.control.identity.record_class, "tier_delete_journal_v2");
                    assert_eq!(view.last_error_code, IlmRecoveryErrorCode::RemoteVersionUnknown);
                }
                "rustfs-tier-delete-journal-unknown" => {
                    assert_eq!(persisted.control.identity.record_class, "tier_delete_journal_corrupt");
                    assert_eq!(view.last_error_code, IlmRecoveryErrorCode::SourceCorrupt);
                }
                schema => panic!("unexpected legacy recovery source schema: {schema}"),
            }
        }

        let creator_sha256 = rustfs_utils::crypto::hex_sha256(b"legacy-export-actor", ToOwned::to_owned);
        let mut created_exports = Vec::new();
        for exportable in first_controls
            .iter()
            .filter(|control| control.classification == IlmRecoveryClassification::RetainedAmbiguous)
        {
            let observation = inspect_recovery_export_observation(store.clone(), &exportable.control_id)
                .await
                .expect("fresh legacy recovery observation should be exportable");
            let created = create_recovery_export(store.clone(), &observation, &creator_sha256)
                .await
                .expect("legacy recovery export should be created exactly once");
            assert!(!created.replayed);
            let loaded = load_recovery_export(store.clone(), &created.export_id)
                .await
                .expect("created legacy recovery export should load");
            assert_eq!(loaded.encoded, created.encoded, "export readback must preserve the exact committed bytes");
            let replayed = create_recovery_export(store.clone(), &observation, &creator_sha256)
                .await
                .expect("the same observed generation should replay its immutable export");
            assert!(replayed.replayed);
            assert_eq!(replayed.encoded, created.encoded);
            created_exports.push(created);
        }
        assert_eq!(created_exports.len(), 2, "both v1 and v2 legacy journals must have an export path");

        let corrupt_export_id = &created_exports[0].export_id;
        let export_path = recovery_export_record_object_name(IlmRecoveryProtocol::TierDeleteJournal, corrupt_export_id)
            .expect("export path should build");
        com::save_config(store.clone(), &export_path, Vec::new())
            .await
            .expect("zero-byte corruption fixture should persist");
        let corrupt_export = load_recovery_export(store.clone(), corrupt_export_id)
            .await
            .expect_err("an existing zero-byte export must fail closed");
        assert!(!matches!(corrupt_export, Error::ConfigNotFound));

        com::save_config(
            store.clone(),
            &journal_paths[0],
            serde_json::to_vec_pretty(&fixtures[0]).expect("rewritten legacy journal fixture should encode"),
        )
        .await
        .expect("equivalent legacy journal rewrite should persist");
        let second = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("repeated legacy journal recovery scan should finish");
        assert_eq!((second.scanned, second.deleted, second.failed), (3, 0, 0));
        let mut second_controls = list_recovery_controls(store, IlmRecoveryProtocol::TierDeleteJournal, None, 100, None)
            .await
            .expect("repeated legacy recovery controls should remain listable")
            .records;
        second_controls.sort_by(|left, right| left.control_id.cmp(&right.control_id));
        assert_eq!(second_controls, first_controls, "repeated scans must not reset durable controls");
        assert_eq!(backend.remove_count().await, 0, "repeated recovery must remain remote-call free");
        assert_eq!(backend.exact_remove_count(), 0);
        assert!(
            backend.op_log().await.is_empty(),
            "repeated recovery must not invoke any backend operation"
        );
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn legacy_recovery_disposition_removes_only_local_journals_and_replays() {
        Box::pin(legacy_recovery_disposition_removes_only_local_journals_and_replays_case()).await;
    }

    #[cfg(feature = "test-util")]
    async fn legacy_recovery_disposition_removes_only_local_journals_and_replays_case() {
        let temp_dir = tempfile::tempdir().expect("create legacy disposition store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "legacy-recovery-disposition", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "LEGACY-DISPOSITION";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("legacy disposition tier lease should resolve")
            .backend_identity();
        let fixtures = [
            serde_json::json!({
                "version": 1,
                "obj_name": "legacy/disposition-v1",
                "version_id": "opaque-disposition-v1",
                "tier_name": tier_name,
            }),
            serde_json::json!({
                "version": 2,
                "obj_name": "legacy/disposition-v2",
                "version_id": "opaque-disposition-v2",
                "tier_name": tier_name,
                "backend_identity": backend_identity,
            }),
        ];
        let mut journal_paths = Vec::new();
        for fixture in &fixtures {
            let data = serde_json::to_vec(fixture).expect("legacy disposition fixture should encode");
            let entry = crate::bucket::lifecycle::tier_delete_journal::decode_tier_delete_journal_entry(&data)
                .expect("legacy disposition fixture should decode");
            let path = tier_delete_journal_object_name(&entry);
            com::save_config(store.clone(), &path, data)
                .await
                .expect("legacy disposition fixture should persist");
            journal_paths.push(path);
        }

        let recovered = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("legacy disposition recovery scan should finish");
        assert_eq!((recovered.scanned, recovered.deleted, recovered.failed), (2, 0, 0));
        assert_eq!(tier_delete_journal_count(store.clone()).await, 2);

        let mut controls = list_recovery_controls(
            store.clone(),
            IlmRecoveryProtocol::TierDeleteJournal,
            Some(IlmRecoveryClassification::RetainedAmbiguous),
            100,
            None,
        )
        .await
        .expect("legacy disposition controls should be listable")
        .records;
        controls.sort_by(|left, right| left.control_id.cmp(&right.control_id));
        assert_eq!(controls.len(), 2, "both legacy schemas must support disposition");

        let actor_sha256 = rustfs_utils::crypto::hex_sha256(b"legacy-disposition-actor", ToOwned::to_owned);
        let wrong_actor_sha256 = rustfs_utils::crypto::hex_sha256(b"different-disposition-actor", ToOwned::to_owned);
        let wrong_export_sha256 = "ff".repeat(32);
        for (index, control) in controls.iter().enumerate() {
            let observation = inspect_recovery_export_observation(store.clone(), &control.control_id)
                .await
                .expect("legacy disposition source should be observable");
            let export = create_recovery_export(store.clone(), &observation, &actor_sha256)
                .await
                .expect("legacy disposition export should persist");
            let confirmed_at_unix_nanos = i64::try_from(OffsetDateTime::now_utc().unix_timestamp_nanos())
                .expect("legacy disposition timestamp should fit i64");

            if index == 0 {
                let wrong_hash = Box::pin(execute_recovery_disposition(
                    store.clone(),
                    &observation,
                    &export.export_id,
                    &wrong_export_sha256,
                    &actor_sha256,
                    confirmed_at_unix_nanos,
                ))
                .await
                .expect_err("a mismatched export checksum must fail before local deletion");
                assert_eq!(wrong_hash, Error::PreconditionFailed);
                assert_eq!(tier_delete_journal_count(store.clone()).await, 2);
            }

            let dry_run = dry_run_recovery_disposition(
                store.clone(),
                &observation,
                &export.export_id,
                &export.content_sha256,
                &actor_sha256,
                confirmed_at_unix_nanos,
            )
            .await
            .expect("legacy disposition dry-run should validate exact local state");
            assert_eq!(dry_run.source_copy_count, observation.source_generation.copies.len());
            assert_eq!(
                tier_delete_journal_count(store.clone()).await,
                fixtures.len() - index,
                "dry-run must not delete a legacy journal"
            );
            assert!(
                matches!(
                    load_recovery_disposition(store.clone(), IlmRecoveryProtocol::TierDeleteJournal, &dry_run.disposition_id,)
                        .await,
                    Err(Error::ConfigNotFound)
                ),
                "dry-run must not persist a disposition record"
            );

            if index == 0 {
                inject_recovery_disposition_crash_once(RecoveryDispositionCrashStage::AfterLocalDelete);
                Box::pin(execute_recovery_disposition(
                    store.clone(),
                    &observation,
                    &export.export_id,
                    &export.content_sha256,
                    &actor_sha256,
                    confirmed_at_unix_nanos,
                ))
                .await
                .expect_err("the injected crash must stop after local delete commits");
                assert_eq!(tier_delete_journal_count(store.clone()).await, 1);
                let interrupted =
                    load_recovery_disposition(store.clone(), IlmRecoveryProtocol::TierDeleteJournal, &dry_run.disposition_id)
                        .await
                        .expect("the applying disposition must survive the post-delete crash");
                assert_eq!(interrupted.disposition.state, IlmRecoveryDispositionState::Applying);
                assert!(
                    interrupted.disposition.confirmed_absent.is_empty(),
                    "the crash must occur before absence progress is persisted"
                );
            } else {
                inject_recovery_disposition_crash_once(RecoveryDispositionCrashStage::AfterControlAbandon);
                Box::pin(execute_recovery_disposition(
                    store.clone(),
                    &observation,
                    &export.export_id,
                    &export.content_sha256,
                    &actor_sha256,
                    confirmed_at_unix_nanos,
                ))
                .await
                .expect_err("the injected crash must stop after control abandonment commits");
                let interrupted =
                    load_recovery_disposition(store.clone(), IlmRecoveryProtocol::TierDeleteJournal, &dry_run.disposition_id)
                        .await
                        .expect("the applying disposition must survive the post-control crash");
                assert_eq!(interrupted.disposition.state, IlmRecoveryDispositionState::Applying);
                assert_eq!(
                    interrupted.disposition.confirmed_absent.len(),
                    interrupted.disposition.identity.source_generation.copies.len()
                );

                let abandoned =
                    load_recovery_control(store.clone(), IlmRecoveryProtocol::TierDeleteJournal, &observation.control_id)
                        .await
                        .expect("the abandoned control must survive the injected crash");
                let exact_abandoned = abandoned.control.encode().expect("the exact abandoned control should encode");
                let mut wrong_history = abandoned.control;
                wrong_history.last_error_code = IlmRecoveryErrorCode::CleanupFailed;
                let control_path = recovery_control_record_object_name(observation.protocol, &observation.control_id)
                    .expect("control path should remain canonical");
                com::save_config(
                    store.clone(),
                    &control_path,
                    wrong_history
                        .encode()
                        .expect("the alternate valid control history should encode"),
                )
                .await
                .expect("the alternate control history fixture should persist");
                let wrong_history_err = Box::pin(execute_recovery_disposition(
                    store.clone(),
                    &observation,
                    &export.export_id,
                    &export.content_sha256,
                    &actor_sha256,
                    confirmed_at_unix_nanos + 1,
                ))
                .await
                .expect_err("a different abandoned control history must not bridge to completion");
                assert_eq!(wrong_history_err, Error::PreconditionFailed);
                com::save_config(store.clone(), &control_path, exact_abandoned)
                    .await
                    .expect("the exact abandoned control fixture should be restored");
            }

            let replay_confirmed_at_unix_nanos = confirmed_at_unix_nanos + 2;
            let executed = Box::pin(execute_recovery_disposition(
                store.clone(),
                &observation,
                &export.export_id,
                &export.content_sha256,
                &actor_sha256,
                replay_confirmed_at_unix_nanos,
            ))
            .await
            .expect("a later request must resume and complete the interrupted disposition");
            assert_eq!(executed.state, IlmRecoveryDispositionState::Completed);
            assert_eq!(executed.outcome, IlmRecoveryDispositionExecutionOutcome::Completed);
            assert_eq!(executed.confirmed_absent_copy_count, executed.source_copy_count);
            assert_eq!(tier_delete_journal_count(store.clone()).await, fixtures.len() - index - 1);
            assert!(matches!(
                com::read_config(store.clone(), &observation.canonical_source_path).await,
                Err(Error::ConfigNotFound)
            ));
            if index == 0 {
                let untouched = journal_paths
                    .iter()
                    .find(|path| *path != &observation.canonical_source_path)
                    .expect("the other legacy journal should remain");
                com::read_config(store.clone(), untouched)
                    .await
                    .expect("disposition must not remove a different legacy journal");
            }

            let abandoned = load_recovery_control(store.clone(), IlmRecoveryProtocol::TierDeleteJournal, &observation.control_id)
                .await
                .expect("abandoned recovery control should remain inspectable");
            assert_eq!(abandoned.control.classification, IlmRecoveryClassification::Abandoned);
            assert_eq!(abandoned.control.revision, observation.control_revision + 1);
            assert_eq!(abandoned.control.observed_source_generation, observation.source_generation);

            let persisted =
                load_recovery_disposition(store.clone(), IlmRecoveryProtocol::TierDeleteJournal, &executed.disposition_id)
                    .await
                    .expect("completed disposition should remain durable");
            assert_eq!(persisted.disposition.state, IlmRecoveryDispositionState::Completed);

            let replayed = Box::pin(execute_recovery_disposition(
                store.clone(),
                &observation,
                &export.export_id,
                &export.content_sha256,
                &actor_sha256,
                replay_confirmed_at_unix_nanos + 1,
            ))
            .await
            .expect("same actor should replay the completed disposition");
            assert_eq!(replayed.state, IlmRecoveryDispositionState::Completed);
            assert_eq!(replayed.outcome, IlmRecoveryDispositionExecutionOutcome::Replayed);

            let wrong_actor = Box::pin(execute_recovery_disposition(
                store.clone(),
                &observation,
                &export.export_id,
                &export.content_sha256,
                &wrong_actor_sha256,
                replay_confirmed_at_unix_nanos + 2,
            ))
            .await
            .expect_err("a different actor must not replay a completed disposition");
            assert_eq!(wrong_actor, Error::PreconditionFailed);

            assert!(
                !Box::pin(garbage_collect_completed_recovery_disposition(
                    store.clone(),
                    &persisted,
                    persisted.disposition.retain_until_unix_nanos - 1,
                ))
                .await
                .expect("completed disposition should remain before retention expires")
            );
            assert!(
                Box::pin(garbage_collect_completed_recovery_disposition(
                    store.clone(),
                    &persisted,
                    persisted.disposition.retain_until_unix_nanos,
                ))
                .await
                .expect("expired completed disposition should be garbage collected")
            );
            assert!(matches!(
                load_recovery_disposition(store.clone(), IlmRecoveryProtocol::TierDeleteJournal, &executed.disposition_id).await,
                Err(Error::ConfigNotFound)
            ));
            assert_eq!(backend.remove_count().await, 0, "legacy disposition must not call the remote tier");
            assert_eq!(backend.exact_remove_count(), 0, "legacy disposition must not issue exact remote DELETE");
            assert!(
                backend.op_log().await.is_empty(),
                "legacy disposition must not invoke any backend operation"
            );
        }
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn v4_tier_delete_journal_recovery_is_exact_and_store_scoped() {
        let temp_a = tempfile::tempdir().expect("create temp store dir a");
        let temp_b = tempfile::tempdir().expect("create temp store dir b");
        let (ctx_a, store_a, _shutdown_a) =
            without_storage_class_env(build_isolated_test_store(temp_a.path(), "tier-journal-recovery-a", &[4])).await;
        let (ctx_b, store_b, _shutdown_b) =
            without_storage_class_env(build_isolated_test_store(temp_b.path(), "tier-journal-recovery-b", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store_a.clone(), Vec::new()).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store_b.clone(), Vec::new()).await;

        let tier_a = "JOURNAL-A";
        let tier_b = "JOURNAL-B";
        let backend_a = register_mock_tier(&ctx_a.tier_config_mgr(), tier_a).await;
        let backend_b = register_mock_tier(&ctx_b.tier_config_mgr(), tier_b).await;
        let identity_a = TierConfigMgr::acquire_operation_lease(&ctx_a.tier_config_mgr(), tier_a)
            .await
            .expect("store A tier lease should resolve")
            .backend_identity();
        let identity_b = TierConfigMgr::acquire_operation_lease(&ctx_b.tier_config_mgr(), tier_b)
            .await
            .expect("store B tier lease should resolve")
            .backend_identity();
        let entry_a = Jentry {
            persisted_version: 0,
            obj_name: "remote-a".to_string(),
            version_id: "version-a".to_string(),
            tier_name: tier_a.to_string(),
            backend_identity: Some(identity_a),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: crate::bucket::lifecycle::tier_sweeper::TierDeleteJournalState::Committed,
            source: None,
            dispatch: None,
        };
        let entry_b = Jentry {
            persisted_version: 0,
            obj_name: "remote-b".to_string(),
            version_id: "version-b".to_string(),
            tier_name: tier_b.to_string(),
            backend_identity: Some(identity_b),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: crate::bucket::lifecycle::tier_sweeper::TierDeleteJournalState::Committed,
            source: None,
            dispatch: None,
        };
        persist_tier_delete_journal_entry(store_a.clone(), &entry_a)
            .await
            .expect("store A v4 compatibility fixture should persist");
        persist_tier_delete_journal_entry(store_b.clone(), &entry_b)
            .await
            .expect("store B v4 compatibility fixture should persist");
        let entry_a_v3 = Jentry {
            obj_name: "remote-a-v3".to_string(),
            version_id: "version-a-v3".to_string(),
            ..entry_a.clone()
        };
        let mut raw_v3: serde_json::Value = serde_json::from_slice(
            &encode_tier_delete_journal_entry(&entry_a_v3).expect("store A v3 fixture should encode from the v4 shape"),
        )
        .expect("store A v3 fixture should be JSON");
        raw_v3["version"] = serde_json::json!(3);
        raw_v3
            .as_object_mut()
            .expect("store A v3 fixture should be an object")
            .remove("version_state");
        let path_a_v3 = tier_delete_journal_object_name(&entry_a_v3);
        com::save_config(
            store_a.clone(),
            &path_a_v3,
            serde_json::to_vec(&raw_v3).expect("store A raw v3 fixture should encode"),
        )
        .await
        .expect("store A raw v3 compatibility fixture should persist");
        let path_a = tier_delete_journal_object_name(&entry_a);
        let path_b = tier_delete_journal_object_name(&entry_b);
        let recovered_a = recover_tier_delete_journal_entries(store_a.clone(), 100, None)
            .await
            .expect("store A recovery scan should finish");
        let recovered_b = recover_tier_delete_journal_entries(store_b.clone(), 100, None)
            .await
            .expect("store B recovery scan should finish");
        assert_eq!((recovered_a.scanned, recovered_a.deleted, recovered_a.failed), (2, 2, 0));
        assert_eq!((recovered_b.scanned, recovered_b.deleted, recovered_b.failed), (1, 1, 0));
        let mut removed_a = backend_a.remove_versions().await;
        removed_a.sort();
        assert_eq!(
            removed_a,
            vec![
                ("remote-a".to_string(), "version-a".to_string()),
                ("remote-a-v3".to_string(), "version-a-v3".to_string()),
            ]
        );
        assert_eq!(backend_b.remove_versions().await, vec![("remote-b".to_string(), "version-b".to_string())]);
        assert!(matches!(com::read_config(store_a.clone(), &path_a).await, Err(Error::ConfigNotFound)));
        assert!(matches!(com::read_config(store_a.clone(), &path_a_v3).await, Err(Error::ConfigNotFound)));
        assert!(matches!(com::read_config(store_b.clone(), &path_b).await, Err(Error::ConfigNotFound)));
        assert_eq!(tier_delete_journal_count(store_a).await, 0);
        assert_eq!(tier_delete_journal_count(store_b).await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn v5_tier_delete_recovery_retains_live_sources_and_aborts_only_prepared() {
        let temp_dir = tempfile::tempdir().expect("create v5 live-source store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "v5-live-source-recovery", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "V5-LIVE-SOURCE";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("v5 live-source tier lease should resolve")
            .backend_identity();
        let bucket = "v5-live-source-recovery-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("v5 live-source bucket should be created");

        let mut paths = Vec::new();
        for (index, state) in [
            TierDeleteJournalState::Prepared,
            TierDeleteJournalState::Dispatched,
            TierDeleteJournalState::Committed,
        ]
        .into_iter()
        .enumerate()
        {
            let object = format!("live-{index}.bin");
            let remote_version = uuid::Uuid::new_v4().to_string();
            backend.set_put_remote_version(Some(remote_version)).await;
            let mut reader = PutObjReader::from_vec(format!("v5 live source {index}").into_bytes());
            let source = store
                .put_object(bucket, &object, &mut reader, &ObjectOptions::default())
                .await
                .expect("v5 live source should be written");
            store
                .transition_object(
                    bucket,
                    &object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name.to_string(),
                            etag: source.etag.clone().expect("v5 live source should have an ETag"),
                            ..Default::default()
                        },
                        version_id: source.version_id.map(|version| version.to_string()),
                        mod_time: source.mod_time,
                        ..Default::default()
                    },
                )
                .await
                .expect("v5 live source should transition");
            let transitioned = store
                .get_object_info(
                    bucket,
                    &object,
                    &ObjectOptions {
                        no_lock: true,
                        metadata_cache_safe: false,
                        ..Default::default()
                    },
                )
                .await
                .expect("v5 transitioned source should be readable");
            let mut entry = transitioned_delete_journal_entry_for_source(None, false, false, bucket, &object, &transitioned)
                .expect("v5 transitioned source should produce a stable journal identity");
            entry.persisted_version = 5;
            entry.backend_identity = Some(backend_identity);
            entry.state = state;
            entry.dispatch = None;
            let path = tier_delete_journal_object_name(&entry);
            com::save_config(
                store.clone(),
                &path,
                encode_tier_delete_journal_entry(&entry).expect("raw v5 live-source journal should encode"),
            )
            .await
            .expect("raw v5 live-source journal should persist");
            paths.push((state, path));
        }

        let stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("v5 live-source recovery should finish");
        assert_eq!((stats.scanned, stats.deleted, stats.failed), (3, 1, 2));
        assert_eq!(backend.remove_count().await, 0, "a live source must block every remote DELETE");
        for (state, path) in paths {
            let observed = com::read_config(store.clone(), &path).await;
            if state == TierDeleteJournalState::Prepared {
                assert!(matches!(observed, Err(Error::ConfigNotFound)), "Prepared v5 evidence should abort");
            } else {
                assert!(observed.is_ok(), "{state:?} v5 evidence must remain while its exact source exists");
            }
        }
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn v5_tier_delete_recovery_replays_all_crash_states_without_sources() {
        let temp_dir = tempfile::tempdir().expect("create v5 crash-replay store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "v5-crash-replay", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "V5-CRASH-REPLAY";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let lease = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("v5 crash-replay tier lease should resolve");
        let backend_identity = lease.backend_identity();
        let bucket = "v5-crash-replay-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("v5 crash-replay bucket should be created");

        for (index, state, seed_remote) in [
            (0, TierDeleteJournalState::Prepared, true),
            (1, TierDeleteJournalState::Dispatched, true),
            (2, TierDeleteJournalState::Committed, true),
            // Models a restart after exact remote DELETE succeeded but before
            // the local Committed journal was removed.
            (3, TierDeleteJournalState::Committed, false),
        ] {
            let remote_version = uuid::Uuid::new_v4().to_string();
            let mut entry = synthetic_v6_dispatch_entry(
                bucket,
                &format!("missing-{index}.bin"),
                tier_name,
                backend_identity,
                &remote_version,
            );
            entry.persisted_version = 5;
            entry.state = state;
            entry.dispatch = None;
            if seed_remote {
                backend.set_put_remote_version(Some(remote_version)).await;
                let body = bytes::Bytes::from(format!("v5 remote candidate {index}"));
                lease
                    .put(
                        &entry.obj_name,
                        ReaderImpl::Body(body.clone()),
                        i64::try_from(body.len()).expect("v5 candidate length should fit i64"),
                    )
                    .await
                    .expect("v5 remote candidate should be seeded");
            }
            let path = tier_delete_journal_object_name(&entry);
            com::save_config(
                store.clone(),
                &path,
                encode_tier_delete_journal_entry(&entry).expect("raw v5 crash journal should encode"),
            )
            .await
            .expect("raw v5 crash journal should persist");
        }

        let stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("v5 crash-state recovery should finish");
        assert_eq!((stats.scanned, stats.deleted, stats.failed), (4, 4, 0));
        assert_eq!(tier_delete_journal_count(store).await, 0);
        assert_eq!(backend.object_count().await, 0);
        assert_eq!(backend.exact_remove_count(), 4, "every v5 state must use exact-version cleanup");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatched_tier_delete_recovery_finds_directory_source_on_encoded_set() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let shutdown = CancellationToken::new();
        let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store_with_layout(
            temp_dir.path(),
            "prepared-directory-recovery",
            &[(2, 4)],
            shutdown,
            None,
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let pool = &store.pools[0];
        let object = (0..10_000)
            .map(|index| format!("directory-{index}/"))
            .find(|candidate| {
                let encoded = rustfs_utils::path::encode_dir_object(candidate);
                !Arc::ptr_eq(&pool.get_disks_by_key(candidate), &pool.get_disks_by_key(&encoded))
            })
            .expect("test topology should have a directory key whose encoded form hashes to another set");
        let encoded = rustfs_utils::path::encode_dir_object(&object);
        assert!(!Arc::ptr_eq(&pool.get_disks_by_key(&object), &pool.get_disks_by_key(&encoded)));

        let tier_name = "PREPAREDDIRECTORY";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let bucket = "prepared-directory-recovery-bucket";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(b"directory source".to_vec());
        let original = store
            .put_object(bucket, &object, &mut reader, &ObjectOptions::default())
            .await
            .expect("directory source should be written");
        store
            .transition_object(
                bucket,
                &object,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: original.etag.clone().expect("source should have an ETag"),
                        ..Default::default()
                    },
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("directory transition should commit");
        let committed = store
            .get_object_info(
                bucket,
                &object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
            )
            .await
            .expect("transitioned directory source should be readable");
        let mut entry = transitioned_delete_journal_entry_for_source(None, false, false, bucket, &object, &committed)
            .expect("transitioned source should produce a prepared journal entry");
        entry.backend_identity = Some(backend_identity);
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("bucket incarnation should resolve");
        install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            &object,
            vec![(entry, Some(TierDeleteJournalState::Dispatched))],
            TierDeleteDispatchManifestState::DispatchAuthorized,
        )
        .await
        .expect("authorized dispatched journal should persist");

        let stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("prepared recovery should complete");

        assert_eq!((stats.scanned, stats.deleted, stats.failed), (1, 0, 1));
        assert_eq!(tier_delete_journal_count(store).await, 1);
        assert_eq!(backend.remove_count().await, 0);
        assert_eq!(backend.object_count().await, 1, "live directory source must retain its remote object");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatched_tier_delete_recovery_checks_later_pool_then_commits_after_source_removal() {
        let temp_dir = tempfile::tempdir().expect("create cross-pool recovery store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "prepared-cross-pool-recovery", &[4, 4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "PREPAREDCROSSPOOL";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let bucket = "prepared-cross-pool-recovery-bucket";
        let object = "object";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(b"later pool source".to_vec());
        let original = store.pools[1]
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source should be written only to the later pool");
        store.pools[1]
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: original.etag.clone().expect("source should have an ETag"),
                        ..Default::default()
                    },
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("later-pool transition should commit");
        let committed = store.pools[1]
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("transitioned source should be readable");
        let mut entry = transitioned_delete_journal_entry_for_source(None, false, false, bucket, object, &committed)
            .expect("transitioned source should produce a prepared journal");
        entry.backend_identity = Some(backend_identity);
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("bucket incarnation should resolve");
        install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            object,
            vec![(entry, Some(TierDeleteJournalState::Dispatched))],
            TierDeleteDispatchManifestState::DispatchAuthorized,
        )
        .await
        .expect("authorized dispatched journal should persist");

        let retained = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("recovery should scan the later pool");
        assert_eq!((retained.scanned, retained.deleted, retained.failed), (1, 0, 1));
        assert_eq!(backend.remove_count().await, 0);
        assert_eq!(backend.object_count().await, 1);
        assert_eq!(
            tier_delete_journal_count(store.clone()).await,
            1,
            "an authorized journal must remain while its live source is durable"
        );

        store.pools[1]
            .delete_object(
                bucket,
                object,
                ObjectOptions {
                    version_id: committed.version_id.map(|version| version.to_string()),
                    expiration: crate::storage_api_contracts::lifecycle::ExpirationOptions { expire: true },
                    skip_free_version: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source version should be removed before recovery retry");
        let committed_stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("journal recovery should commit an absent stable source");
        assert_eq!((committed_stats.scanned, committed_stats.deleted, committed_stats.failed), (1, 0, 1));
        assert_eq!(backend.remove_count().await, 0);

        let manifest_stats = recover_tier_delete_dispatch_manifests(store.clone(), 100, None)
            .await
            .expect("manifest recovery should authorize committed remote cleanup");
        assert_eq!((manifest_stats.advanced, manifest_stats.failed), (1, 0));

        let deleted = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("completed journal should delete the exact remote version");
        assert_eq!((deleted.scanned, deleted.deleted, deleted.failed), (1, 1, 0));
        assert_eq!(tier_delete_journal_count(store).await, 0);
        assert_eq!(backend.remove_count().await, 1);
        assert_eq!(backend.object_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatched_tier_delete_recovery_retains_journal_on_source_metadata_error() {
        let temp_dir = tempfile::tempdir().expect("create metadata-error recovery store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "prepared-metadata-error-recovery", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "PREPAREDMETADATAERROR";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let bucket = "prepared-metadata-error-recovery-bucket";
        let object = "object";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(b"source with unreadable metadata".to_vec());
        let original = store
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source should be written");
        store
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: original.etag.clone().expect("source should have an ETag"),
                        ..Default::default()
                    },
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("source transition should commit");
        let committed = store
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("transitioned source should be readable");
        let mut entry = transitioned_delete_journal_entry_for_source(None, false, false, bucket, object, &committed)
            .expect("transitioned source should produce a prepared journal");
        entry.backend_identity = Some(backend_identity);
        let incarnation = store
            .bucket_incarnation_id(bucket)
            .await
            .expect("bucket incarnation should resolve");
        install_test_tier_delete_dispatch_fixture(
            store.clone(),
            bucket,
            incarnation,
            object,
            vec![(entry, Some(TierDeleteJournalState::Dispatched))],
            TierDeleteDispatchManifestState::DispatchAuthorized,
        )
        .await
        .expect("authorized dispatched journal should persist");

        for disk_index in 0..4 {
            let metadata_path = temp_dir
                .path()
                .join(format!("pool0/set0/disk{disk_index}/{bucket}/{object}/{STORAGE_FORMAT_FILE}"));
            tokio::fs::write(metadata_path, b"not-xl-meta")
                .await
                .expect("source metadata should be corrupted");
        }

        let stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("recovery scan should complete despite the entry failure");

        assert_eq!((stats.scanned, stats.deleted, stats.failed), (1, 0, 1));
        assert_eq!(tier_delete_journal_count(store).await, 1, "journal must remain dispatched for retry");
        assert_eq!(backend.remove_count().await, 0, "unreadable source metadata must block remote deletion");
        assert_eq!(backend.object_count().await, 1, "remote source must remain intact");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_delete_recovery_retains_content_that_does_not_match_its_object_name() {
        let temp_dir = tempfile::tempdir().expect("create mismatched journal store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "mismatched-tier-journal", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
        let tier_name = "MISMATCHEDJOURNAL";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let lease = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve");
        let remote_version = uuid::Uuid::new_v4().to_string();
        backend.set_put_remote_version(Some(remote_version.clone())).await;
        lease
            .put("remote/original", ReaderImpl::Body(bytes::Bytes::from_static(b"remote body")), 11)
            .await
            .expect("remote body should be seeded");
        let entry = Jentry {
            persisted_version: 0,
            obj_name: "remote/original".to_string(),
            version_id: remote_version,
            tier_name: tier_name.to_string(),
            backend_identity: Some(lease.backend_identity()),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: TierDeleteJournalState::Prepared,
            source: Some(TierDeleteSourceIdentity {
                bucket: "absent-source-bucket".to_string(),
                object: "absent-source-object".to_string(),
                version_id: Some(uuid::Uuid::new_v4().to_string()),
                versioned: true,
                version_suspended: false,
                data_dir: Some(uuid::Uuid::new_v4().to_string()),
                etag: Some("etag".to_string()),
                mod_time: Some(OffsetDateTime::UNIX_EPOCH.to_string()),
            }),
            dispatch: None,
        };
        let (_manifest_name, bound) = install_test_tier_delete_dispatch_fixture(
            store.clone(),
            "absent-source-bucket",
            uuid::Uuid::new_v4(),
            "absent-source-object",
            vec![(entry, Some(TierDeleteJournalState::Dispatched))],
            TierDeleteDispatchManifestState::DispatchAuthorized,
        )
        .await
        .expect("authorized dispatched journal should persist");
        let journal_name = tier_delete_journal_object_name(&bound[0]);
        let data = com::read_config(store.clone(), &journal_name)
            .await
            .expect("prepared journal should be readable");
        let mut value: serde_json::Value = serde_json::from_slice(&data).expect("journal should contain JSON");
        value["obj_name"] = serde_json::json!("remote/replaced");
        com::save_config(
            store.clone(),
            &journal_name,
            serde_json::to_vec(&value).expect("mismatched journal should encode"),
        )
        .await
        .expect("mismatched journal content should be written under the original name");

        let stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
            .await
            .expect("recovery scan should complete");

        assert_eq!((stats.scanned, stats.deleted, stats.failed), (1, 0, 1));
        assert_eq!(tier_delete_journal_count(store).await, 1, "mismatched journal must be retained");
        assert_eq!(backend.remove_count().await, 0);
        assert_eq!(backend.object_count().await, 1);
    }

    #[cfg(feature = "test-util")]
    #[test]
    #[serial_test::serial(storage_class_env)]
    fn transitioned_history_expiry_uses_journal_or_legacy_free_version() {
        std::thread::Builder::new()
            .name("transitioned-delete-all-test".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .build()
                    .expect("test runtime should build");
                runtime.block_on(async {
                    let temp_dir = tempfile::tempdir().expect("create transitioned delete-all store dir");
                    let (ctx, store, _shutdown) =
                        without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transitioned-delete-all", &[4]))
                            .await;
                    crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
                    let tier_name = "DELETEALLTRANSITIONED";
                    let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
                    let bucket = "transitioned-delete-all-bucket";
                    let object = "object";
                    store
                        .make_bucket(bucket, &MakeBucketOptions::default())
                        .await
                        .expect("bucket should be created");
                    crate::bucket::metadata_sys::update_in(
                        &ctx,
                        bucket,
                        BUCKET_VERSIONING_CONFIG,
                        b"<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>".to_vec(),
                    )
                    .await
                    .expect("bucket versioning should be enabled");
                    crate::bucket::metadata_sys::update_in(
                        &ctx,
                        bucket,
                        BUCKET_LIFECYCLE_CONFIG,
                        br#"<LifecycleConfiguration>
  <Rule>
    <ID>delete-all-versions</ID>
    <Status>Enabled</Status>
    <Filter><Prefix></Prefix></Filter>
    <Expiration><Days>1</Days><ExpiredObjectAllVersions>true</ExpiredObjectAllVersions></Expiration>
  </Rule>
</LifecycleConfiguration>"#
                            .to_vec(),
                    )
                    .await
                    .expect("delete-all lifecycle should be configured");

                    let old_time = OffsetDateTime::now_utc() - time::Duration::days(3);
                    let mut history_reader = PutObjReader::from_vec(b"transitioned history".to_vec());
                    let history = store
                        .put_object(
                            bucket,
                            object,
                            &mut history_reader,
                            &ObjectOptions {
                                versioned: true,
                                mod_time: Some(old_time - time::Duration::hours(1)),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("historical version should be written");
                    let mut current_reader = PutObjReader::from_vec(b"current version".to_vec());
                    let current = store
                        .put_object(
                            bucket,
                            object,
                            &mut current_reader,
                            &ObjectOptions {
                                versioned: true,
                                mod_time: Some(old_time),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("current version should be written");
                    store
                        .transition_object(
                            bucket,
                            object,
                            &ObjectOptions {
                                versioned: true,
                                version_id: history.version_id.map(|version_id| version_id.to_string()),
                                transition: TransitionOptions {
                                    status: TRANSITION_PENDING.to_string(),
                                    tier: tier_name.to_string(),
                                    etag: history.etag.clone().expect("history should have an ETag"),
                                    ..Default::default()
                                },
                                mod_time: history.mod_time,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("historical version should transition");
                    assert_eq!(backend.object_count().await, 1);
                    assert_eq!(backend.put_versions().await.len(), 1);
                    backend.set_remove_failure(true);

                    let incarnation = store
                        .bucket_incarnation_id_from_disk(bucket)
                        .await
                        .expect("bucket incarnation should be available");
                    for disk_index in 0..4 {
                        let metadata_path = temp_dir
                            .path()
                            .join(format!("pool0/set0/disk{disk_index}/{bucket}/{object}/{STORAGE_FORMAT_FILE}"));
                        let encoded = tokio::fs::read(&metadata_path)
                            .await
                            .expect("transition metadata should be readable");
                        let mut metadata = FileMeta::load(&encoded).expect("transition metadata should decode");
                        let mut transitioned = metadata
                            .get_all_file_info_versions(bucket, object, true)
                            .expect("transitioned versions should decode")
                            .versions
                            .into_iter()
                            .find(|version| version.version_id == history.version_id)
                            .expect("transitioned history should exist");
                        transitioned.transition_version_state = rustfs_filemeta::TransitionVersionState::Unknown;
                        rustfs_utils::http::metadata_compat::remove_str(
                            &mut transitioned.metadata,
                            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITIONED_VERSION_STATE,
                        );
                        metadata
                            .add_version(transitioned)
                            .expect("unknown state should replace the transitioned version");
                        tokio::fs::write(
                            &metadata_path,
                            metadata.marshal_msg().expect("unknown transition metadata should encode"),
                        )
                        .await
                        .expect("unknown transition metadata should be written");
                    }
                    let lifecycle_event = crate::bucket::lifecycle::lifecycle::Event {
                        action: rustfs_scanner_metrics::metrics::IlmAction::DeleteAllVersionsAction,
                        rule_id: "delete-all-versions".to_string(),
                        ..Default::default()
                    };
                    let legacy_applied =
                        crate::bucket::lifecycle::bucket_lifecycle_ops::apply_expiry_on_non_transitioned_objects(
                            store.clone(),
                            &current,
                            &lifecycle_event,
                            &crate::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc::Scanner,
                            incarnation,
                        )
                        .await;
                    assert!(legacy_applied, "legacy Unknown transition metadata should use the free-version fallback");
                    assert_eq!(tier_delete_journal_count(store.clone()).await, 0);
                    assert_eq!(backend.remove_count().await, 0);
                    let retained = store.pools[0].disk_set[0]
                        .load_file_info_versions_exact(bucket, object)
                        .await
                        .expect("fallback delete-all metadata should remain readable")
                        .expect("fallback delete-all should retain a hidden cleanup owner");
                    assert_eq!(
                        retained
                            .versions
                            .iter()
                            .filter(|version| !version.tier_free_version())
                            .count(),
                        0
                    );
                    assert_eq!(
                        retained.versions.iter().filter(|version| version.tier_free_version()).count(),
                        1,
                        "legacy cleanup must retain exactly one hidden free-version"
                    );
                    assert_eq!(backend.object_count().await, 1, "legacy remote cleanup must remain pending");

                    // Use a distinct tier/backend so the legacy worker can be
                    // held in a deterministic remote-failure state while the
                    // exact journal path independently proves convergence.
                    let exact_tier_name = "DELETEALLTRANSITIONEDEXACT";
                    let exact_backend = register_mock_tier(&ctx.tier_config_mgr(), exact_tier_name).await;
                    let exact_object = "exact-object";
                    let mut exact_history_reader = PutObjReader::from_vec(b"exact transitioned history".to_vec());
                    let exact_history = store
                        .put_object(
                            bucket,
                            exact_object,
                            &mut exact_history_reader,
                            &ObjectOptions {
                                versioned: true,
                                mod_time: Some(old_time - time::Duration::hours(1)),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("second historical version should be written");
                    let mut exact_current_reader = PutObjReader::from_vec(b"exact current version".to_vec());
                    let exact_current = store
                        .put_object(
                            bucket,
                            exact_object,
                            &mut exact_current_reader,
                            &ObjectOptions {
                                versioned: true,
                                mod_time: Some(old_time),
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("second current version should be written");
                    store
                        .transition_object(
                            bucket,
                            exact_object,
                            &ObjectOptions {
                                versioned: true,
                                version_id: exact_history.version_id.map(|version_id| version_id.to_string()),
                                transition: TransitionOptions {
                                    status: TRANSITION_PENDING.to_string(),
                                    tier: exact_tier_name.to_string(),
                                    etag: exact_history.etag.clone().expect("second history should have an ETag"),
                                    ..Default::default()
                                },
                                mod_time: exact_history.mod_time,
                                ..Default::default()
                            },
                        )
                        .await
                        .expect("second historical version should transition exactly");
                    let exact_remote_version = exact_backend
                        .put_versions()
                        .await
                        .last()
                        .cloned()
                        .expect("second transition should publish a remote version");
                    exact_backend.set_remove_failure(true);
                    let applied = crate::bucket::lifecycle::bucket_lifecycle_ops::apply_expiry_on_non_transitioned_objects(
                        store.clone(),
                        &exact_current,
                        &lifecycle_event,
                        &crate::bucket::lifecycle::bucket_lifecycle_audit::LcEventSrc::Scanner,
                        incarnation,
                    )
                    .await;

                    assert!(applied, "delete-all should remove current and transitioned history");
                    assert!(
                        store.pools[0].disk_set[0]
                            .load_file_info_versions_exact(bucket, exact_object)
                            .await
                            .expect("exact journal-owned metadata lookup should succeed")
                            .is_none(),
                        "exact journal ownership must remove the source key without leaving a free-version"
                    );
                    let versions = store.pools[0].disk_set[0]
                        .load_file_info_versions_exact(bucket, object)
                        .await
                        .expect("remaining fallback metadata should be readable")
                        .expect("the prior legacy cleanup owner should remain");
                    assert_eq!(
                        versions
                            .versions
                            .iter()
                            .filter(|version| !version.tier_free_version())
                            .count(),
                        0,
                        "exact delete-all must remove every visible version"
                    );
                    assert_eq!(
                        versions.versions.iter().filter(|version| version.tier_free_version()).count(),
                        1,
                        "the exact v6 journal must not add another free-version"
                    );
                    assert_eq!(tier_delete_journal_count(store.clone()).await, 1);
                    assert_eq!(backend.object_count().await, 1, "legacy remote cleanup must remain pending");
                    assert_eq!(exact_backend.object_count().await, 1, "exact remote cleanup must remain journal-owned");

                    exact_backend.set_remove_failure(false);
                    let stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
                        .await
                        .expect("committed journal should recover");
                    assert_eq!(stats.failed, 0);
                    assert!(stats.scanned <= 1 && stats.deleted <= 1);
                    assert_eq!(tier_delete_journal_count(store).await, 0);
                    assert_eq!(
                        backend.object_count().await,
                        1,
                        "journal recovery must leave the legacy free-version remote"
                    );
                    assert_eq!(exact_backend.object_count().await, 0);
                    assert!(exact_backend.exact_remove_count() >= 1);
                    assert_eq!(exact_backend.remove_versions().await, vec![exact_remote_version]);
                });
            })
            .expect("test thread should spawn")
            .join()
            .expect("test thread should complete");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn odm_write_back_requires_one_set_and_enabled_namespace_locking() {
        for (layout, locking, supported) in [
            (&[(1, 4)][..], true, true),
            (&[(1, 4), (1, 4)][..], true, false),
            (&[(2, 4)][..], true, false),
            (&[(1, 4)][..], false, false),
        ] {
            temp_env::async_with_vars([("RUSTFS_LOCK_ENABLED", Some(if locking { "true" } else { "false" }))], async {
                let dir = tempfile::tempdir().expect("isolated topology");
                let shutdown = CancellationToken::new();
                let (_ctx, store, _) = without_storage_class_env(build_isolated_test_store_with_layout(
                    dir.path(),
                    "odm-topology",
                    layout,
                    shutdown.clone(),
                    None,
                ))
                .await;
                assert_eq!(
                    store.supports_atomic_create_only_write_back(),
                    supported,
                    "layout={layout:?}, locking={locking}"
                );
                shutdown.cancel();
            })
            .await;
        }
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn dispatched_tier_delete_recovery_requires_namespace_locking() {
        temp_env::async_with_vars([("RUSTFS_LOCK_ENABLED", Some("false"))], async {
            let temp_dir = tempfile::tempdir().expect("create lock-disabled store dir");
            let (ctx, store, _shutdown) =
                without_storage_class_env(build_isolated_test_store(temp_dir.path(), "prepared-recovery-lock-disabled", &[4]))
                    .await;
            crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
            assert!(ctx.lock_manager().is_disabled());

            let tier_name = "PREPAREDLOCKDISABLED";
            let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
            let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
                .await
                .expect("tier lease should resolve")
                .backend_identity();
            let entry = Jentry {
                persisted_version: 0,
                obj_name: "remote/lock-disabled".to_string(),
                version_id: uuid::Uuid::new_v4().to_string(),
                tier_name: tier_name.to_string(),
                backend_identity: Some(backend_identity),
                version_id_exact: true,
                version_state: rustfs_filemeta::TransitionVersionState::Exact,
                state: TierDeleteJournalState::Prepared,
                source: Some(TierDeleteSourceIdentity {
                    bucket: "absent-source-bucket".to_string(),
                    object: "absent-source-object".to_string(),
                    version_id: Some(uuid::Uuid::new_v4().to_string()),
                    versioned: true,
                    version_suspended: false,
                    data_dir: Some(uuid::Uuid::new_v4().to_string()),
                    etag: Some("etag".to_string()),
                    mod_time: Some(OffsetDateTime::UNIX_EPOCH.to_string()),
                }),
                dispatch: None,
            };
            install_test_tier_delete_dispatch_fixture(
                store.clone(),
                "absent-source-bucket",
                uuid::Uuid::new_v4(),
                "absent-source-object",
                vec![(entry, Some(TierDeleteJournalState::Dispatched))],
                TierDeleteDispatchManifestState::DispatchAuthorized,
            )
            .await
            .expect("authorized dispatched journal should persist");

            let stats = recover_tier_delete_journal_entries(store.clone(), 100, None)
                .await
                .expect("recovery scan should complete");

            assert_eq!((stats.scanned, stats.deleted, stats.failed), (1, 0, 1));
            assert_eq!(tier_delete_journal_count(store).await, 1, "journal must remain dispatched for retry");
            assert_eq!(backend.remove_count().await, 0, "lock-disabled recovery must not delete remotely");
        })
        .await;
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn lifecycle_delete_all_requires_namespace_locking_before_mutation() {
        temp_env::async_with_vars([("RUSTFS_LOCK_ENABLED", Some("false"))], async {
            let temp_dir = tempfile::tempdir().expect("create lock-disabled delete-all store dir");
            let (ctx, store, _shutdown) =
                without_storage_class_env(build_isolated_test_store(temp_dir.path(), "delete-all-lock-disabled", &[4])).await;
            crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;
            assert!(ctx.lock_manager().is_disabled());
            let bucket = "delete-all-lock-disabled-bucket";
            let object = "object";
            store
                .make_bucket(bucket, &MakeBucketOptions::default())
                .await
                .expect("bucket should be created");
            let mut reader = PutObjReader::from_vec(b"must survive".to_vec());
            let no_lock_opts = ObjectOptions {
                no_lock: true,
                ..Default::default()
            };
            let original = store.pools[0]
                .put_object(bucket, object, &mut reader, &no_lock_opts)
                .await
                .expect("source should be written");

            let mut delete_opts = ObjectOptions {
                delete_prefix: true,
                delete_prefix_object: true,
                lifecycle_delete_all: Some(crate::object_api::LifecycleDeleteAllRequest {
                    version_id: original.version_id,
                    delete_marker: false,
                    action: rustfs_scanner_metrics::metrics::IlmAction::DeleteAllVersionsAction,
                    rule_id: "rule".to_string(),
                    phase: crate::object_api::LifecycleDeleteAllPhase::Preflight,
                }),
                delete_replication_config_snapshot: Some(Arc::new(
                    crate::bucket::replication::DeleteReplicationConfigSnapshot::default(),
                )),
                ..Default::default()
            };
            delete_opts.ensure_lifecycle_delete_all_journal();

            let err = store
                .delete_object_with_tier_delete_journal(bucket, object, delete_opts)
                .await
                .expect_err("delete-all must reject disabled namespace locking");

            assert!(err.to_string().contains("requires namespace locking"));
            let retained = store.pools[0]
                .get_object_info(bucket, object, &no_lock_opts)
                .await
                .expect("rejected delete-all must retain the source");
            assert_eq!(retained.etag, original.etag);
            assert_eq!(tier_delete_journal_count(store).await, 0, "rejected delete-all must not prepare journals");
        })
        .await;
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_mutation_intent_record_round_trips_through_config_store() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-mutation-intent-record", &[4])).await;
        let mutation_id = uuid::Uuid::new_v4();
        let intent = TierMutationIntent {
            mutation_id,
            revision: 1,
            kind: TierMutationIntentKind::Edit,
            state: TierMutationIntentState::Prepared,
            old_config_etag: Some("old-etag".to_string()),
            committed_config_etag: None,
            candidate_digest: [3; 32],
            affected_targets: vec![TierMutationIntentTarget {
                tier_name: "COLD-A".to_string(),
                old_backend_identity: Some([1; 32]),
                new_backend_identity: Some([2; 32]),
            }],
            expires_at_unix_nanos: 1_780_000_000_000_000_000,
        };

        save_tier_mutation_intent_record(store.clone(), &intent)
            .await
            .expect("tier mutation intent record should persist");
        let loaded = load_tier_mutation_intent_record(store.clone(), mutation_id)
            .await
            .expect("tier mutation intent record should load");

        assert_eq!(loaded, intent);

        delete_tier_mutation_intent_record(store.clone(), mutation_id)
            .await
            .expect("tier mutation intent record delete should be idempotent");
        delete_tier_mutation_intent_record(store.clone(), mutation_id)
            .await
            .expect("tier mutation intent record delete should tolerate missing records");
        let err = load_tier_mutation_intent_record(store, mutation_id)
            .await
            .expect_err("deleted tier mutation intent record should not load");
        assert!(matches!(err, Error::ConfigNotFound));
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_probe_intent_store_enforces_create_cas_and_terminal_delete_preconditions() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-probe-intent-cas", &[4])).await;
        let probe_id = uuid::Uuid::new_v4();
        let creator_epoch = uuid::Uuid::new_v4();
        let initial = TierProbeIntent {
            probe_id,
            revision: 1,
            state: TierProbeIntentState::UploadOutcomeUnknown,
            operation: TierProbeOperationIdentity::Verify {
                config_etag: "config-etag".to_string(),
                backend_identity: [1; 32],
            },
            tier_name: "COLD-A".to_string(),
            destination_id: [1; 32],
            probe_object: format!("rustfs-tier-probe-{probe_id}"),
            creator_id: "node-a".to_string(),
            creator_epoch,
            created_at_unix_nanos: 1_780_000_000_000_000_000,
            owner: TierProbeOwnerFence {
                owner_id: "node-a".to_string(),
                owner_epoch: creator_epoch,
                not_after_unix_nanos: 1_780_000_900_000_000_000,
            },
            remote_version: TierProbeRemoteVersion::default(),
        };

        save_tier_probe_intent_record_if_absent(store.clone(), &initial)
            .await
            .expect("initial probe intent should persist with create-only semantics");
        let duplicate = save_tier_probe_intent_record_if_absent(store.clone(), &initial)
            .await
            .expect_err("duplicate create must fail closed");
        assert!(matches!(duplicate, Error::PreconditionFailed));

        let observed_initial = load_tier_probe_intent_record(store.clone(), probe_id)
            .await
            .expect("initial probe intent should load with an ETag");
        assert_eq!(observed_initial.intent(), &initial);

        let nonterminal_delete = delete_tier_probe_intent_record_if_current(store.clone(), &observed_initial)
            .await
            .expect_err("nonterminal evidence must not be deleted");
        assert!(nonterminal_delete.to_string().contains("must be terminal"));

        let mut fabricated_current_intent = initial.clone();
        fabricated_current_intent.tier_name = "COLD-B".to_string();
        let mut fabricated_successor = fabricated_current_intent.clone();
        fabricated_successor
            .advance(
                TierProbeIntentState::Uploaded,
                TierProbeRemoteVersion::versioned(uuid::Uuid::new_v4().to_string()),
            )
            .expect("fabricated successor should be internally valid");
        let fabricated_current = observed_initial.with_intent_for_test(fabricated_current_intent.clone());
        let crossed_cas = save_tier_probe_intent_record_if_current(store.clone(), &fabricated_current, &fabricated_successor)
            .await
            .expect_err("a live ETag must not authorize a different caller record");
        assert!(matches!(crossed_cas, Error::PreconditionFailed));
        assert_eq!(
            load_tier_probe_intent_record(store.clone(), probe_id)
                .await
                .expect("crossed CAS must retain the authoritative record")
                .intent(),
            &initial
        );

        let mut fabricated_terminal_intent = fabricated_current_intent;
        fabricated_terminal_intent
            .advance(TierProbeIntentState::AbortedNoRemote, TierProbeRemoteVersion::default())
            .expect("fabricated terminal should be internally valid");
        let fabricated_terminal = observed_initial.with_intent_for_test(fabricated_terminal_intent);
        let crossed_delete = delete_tier_probe_intent_record_if_current(store.clone(), &fabricated_terminal)
            .await
            .expect_err("a live ETag must not delete for a different caller record");
        assert!(matches!(crossed_delete, Error::PreconditionFailed));
        assert_eq!(
            load_tier_probe_intent_record(store.clone(), probe_id)
                .await
                .expect("crossed delete must retain the authoritative record")
                .intent(),
            &initial
        );

        let remote_version = TierProbeRemoteVersion::versioned(uuid::Uuid::new_v4().to_string());
        let mut uploaded = observed_initial.intent().clone();
        uploaded
            .advance(TierProbeIntentState::Uploaded, remote_version.clone())
            .expect("known PUT result should advance");
        save_tier_probe_intent_record_if_current(store.clone(), &observed_initial, &uploaded)
            .await
            .expect("the matching initial ETag should admit one successor");

        let stale_cas = save_tier_probe_intent_record_if_current(store.clone(), &observed_initial, &uploaded)
            .await
            .expect_err("a consumed ETag must not overwrite the current generation");
        assert!(matches!(stale_cas, Error::PreconditionFailed));

        let observed_uploaded = load_tier_probe_intent_record(store.clone(), probe_id)
            .await
            .expect("uploaded generation should load");
        assert_eq!(observed_uploaded.intent(), &uploaded);
        let mut cleanup = observed_uploaded.intent().clone();
        cleanup
            .advance(TierProbeIntentState::CleanupPending, remote_version.clone())
            .expect("known candidate should become cleanup-pending");
        save_tier_probe_intent_record_if_current(store.clone(), &observed_uploaded, &cleanup)
            .await
            .expect("cleanup generation should persist by exact ETag");

        let observed_cleanup = load_tier_probe_intent_record(store.clone(), probe_id)
            .await
            .expect("cleanup generation should load");
        let mut completed = observed_cleanup.intent().clone();
        completed
            .advance(TierProbeIntentState::Completed, remote_version)
            .expect("exact cleanup should become terminal");
        save_tier_probe_intent_record_if_current(store.clone(), &observed_cleanup, &completed)
            .await
            .expect("terminal generation should persist by exact ETag");

        let stale_terminal = observed_cleanup.with_intent_for_test(completed.clone());
        let stale_delete = delete_tier_probe_intent_record_if_current(store.clone(), &stale_terminal)
            .await
            .expect_err("a stale ETag must not delete terminal evidence");
        assert!(matches!(stale_delete, Error::PreconditionFailed));

        let observed_completed = load_tier_probe_intent_record(store.clone(), probe_id)
            .await
            .expect("terminal generation should remain after stale delete");
        assert_eq!(observed_completed.intent(), &completed);
        delete_tier_probe_intent_record_if_current(store.clone(), &observed_completed)
            .await
            .expect("the exact terminal ETag should delete the record");
        assert!(matches!(load_tier_probe_intent_record(store, probe_id).await, Err(Error::ConfigNotFound)));
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_mutation_intent_record_scan_retains_good_records_and_counts_bad_records() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-mutation-intent-scan", &[4])).await;
        let build_intent = |mutation_id: uuid::Uuid, tier_name: &str| TierMutationIntent {
            mutation_id,
            revision: 1,
            kind: TierMutationIntentKind::Edit,
            state: TierMutationIntentState::Prepared,
            old_config_etag: Some("old-etag".to_string()),
            committed_config_etag: None,
            candidate_digest: [3; 32],
            affected_targets: vec![TierMutationIntentTarget {
                tier_name: tier_name.to_string(),
                old_backend_identity: Some([1; 32]),
                new_backend_identity: Some([2; 32]),
            }],
            expires_at_unix_nanos: 1_780_000_000_000_000_000,
        };
        let first_id = uuid::Uuid::parse_str("12345678-1234-5678-9abc-def012345678").expect("first uuid should parse");
        let second_id = uuid::Uuid::parse_str("22345678-1234-5678-9abc-def012345678").expect("second uuid should parse");
        let first = build_intent(first_id, "COLD-A");
        let second = build_intent(second_id, "COLD-B");
        save_tier_mutation_intent_record(store.clone(), &first)
            .await
            .expect("first tier mutation intent record should persist");
        save_tier_mutation_intent_record(store.clone(), &second)
            .await
            .expect("second tier mutation intent record should persist");
        com::save_config(
            store.clone(),
            &format!("{TIER_MUTATION_INTENT_RECORD_PREFIX}/00/00/33345678123456789abcdef012345678.json"),
            b"{}".to_vec(),
        )
        .await
        .expect("malformed-shard intent record should persist");
        com::save_config(
            store.clone(),
            &format!("{TIER_MUTATION_INTENT_RECORD_PREFIX}/44/44/44445678123456789abcdef012345678.json"),
            b"{".to_vec(),
        )
        .await
        .expect("corrupt-json intent record should persist");

        let scan = list_tier_mutation_intent_records(store, 100, None)
            .await
            .expect("tier mutation intent records should scan");
        let mut loaded_ids: Vec<_> = scan.intents.into_iter().map(|intent| intent.mutation_id).collect();
        loaded_ids.sort();

        assert_eq!(scan.scanned, 4);
        assert_eq!(scan.failed, 2);
        assert_eq!(loaded_ids, vec![first_id, second_id]);
        assert!(!scan.truncated);
        assert_eq!(scan.next_marker, None);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_mutation_intent_record_advance_is_idempotent_in_config_store() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-mutation-intent-advance", &[4])).await;
        let mutation_id = uuid::Uuid::new_v4();
        let intent = TierMutationIntent {
            mutation_id,
            revision: 1,
            kind: TierMutationIntentKind::Edit,
            state: TierMutationIntentState::Prepared,
            old_config_etag: Some("old-etag".to_string()),
            committed_config_etag: None,
            candidate_digest: [3; 32],
            affected_targets: vec![TierMutationIntentTarget {
                tier_name: "COLD-A".to_string(),
                old_backend_identity: Some([1; 32]),
                new_backend_identity: Some([2; 32]),
            }],
            expires_at_unix_nanos: 1_780_000_000_000_000_000,
        };
        save_tier_mutation_intent_record(store.clone(), &intent)
            .await
            .expect("prepared tier mutation intent record should persist");
        let (loaded_before_advance, stale_etag) = load_tier_mutation_intent_record_with_etag(store.clone(), mutation_id)
            .await
            .expect("prepared tier mutation intent record should load with etag");
        assert_eq!(loaded_before_advance, intent);
        assert!(!stale_etag.is_empty());

        let (committed, first_advanced) = advance_tier_mutation_intent_record_idempotent(
            store.clone(),
            mutation_id,
            TierMutationIntentState::Committed,
            Some("new-etag".to_string()),
        )
        .await
        .expect("first commit should advance the record");
        assert!(first_advanced);
        assert_eq!(committed.state, TierMutationIntentState::Committed);
        assert_eq!(committed.revision, 2);
        assert_eq!(committed.committed_config_etag.as_deref(), Some("new-etag"));

        let (retried, retry_advanced) = advance_tier_mutation_intent_record_idempotent(
            store.clone(),
            mutation_id,
            TierMutationIntentState::Committed,
            Some("new-etag".to_string()),
        )
        .await
        .expect("same commit retry should be idempotent");
        assert!(!retry_advanced);
        assert_eq!(retried, committed);

        let conflict = advance_tier_mutation_intent_record_idempotent(
            store.clone(),
            mutation_id,
            TierMutationIntentState::Committed,
            Some("other-etag".to_string()),
        )
        .await
        .expect_err("conflicting commit retry should fail closed");
        assert!(matches!(conflict, Error::Io(_)));
        assert!(conflict.to_string().contains("committed config etag does not match"));

        let mut stale_conflict = intent;
        stale_conflict
            .advance_idempotent(TierMutationIntentState::Committed, Some("other-etag".to_string()))
            .expect("stale conflicting intent should advance locally");
        let stale_save = save_tier_mutation_intent_record_if_current(store.clone(), &stale_conflict, &stale_etag)
            .await
            .expect_err("stale etag must fail closed instead of overwriting the committed record");
        assert!(matches!(stale_save, Error::PreconditionFailed));

        let loaded = load_tier_mutation_intent_record(store.clone(), mutation_id)
            .await
            .expect("conflicting retry must not overwrite the durable record");
        assert_eq!(loaded, committed);

        let abort_id = uuid::Uuid::new_v4();
        let abort_intent = TierMutationIntent {
            mutation_id: abort_id,
            revision: 1,
            kind: TierMutationIntentKind::Edit,
            state: TierMutationIntentState::Prepared,
            old_config_etag: Some("old-etag".to_string()),
            committed_config_etag: None,
            candidate_digest: [4; 32],
            affected_targets: vec![TierMutationIntentTarget {
                tier_name: "COLD-B".to_string(),
                old_backend_identity: Some([1; 32]),
                new_backend_identity: Some([2; 32]),
            }],
            expires_at_unix_nanos: 1_780_000_000_000_000_000,
        };
        save_tier_mutation_intent_record(store.clone(), &abort_intent)
            .await
            .expect("prepared abort intent record should persist");

        let (aborted, first_abort_advanced) =
            advance_tier_mutation_intent_record_idempotent(store.clone(), abort_id, TierMutationIntentState::Aborted, None)
                .await
                .expect("first abort should advance the record");
        assert!(first_abort_advanced);
        assert_eq!(aborted.state, TierMutationIntentState::Aborted);
        assert_eq!(aborted.revision, 2);
        assert_eq!(aborted.committed_config_etag, None);

        let (aborted_retry, retry_abort_advanced) =
            advance_tier_mutation_intent_record_idempotent(store, abort_id, TierMutationIntentState::Aborted, None)
                .await
                .expect("same abort retry should be idempotent");
        assert!(!retry_abort_advanced);
        assert_eq!(aborted_retry, aborted);
    }

    #[cfg(feature = "test-util")]
    fn tier_mutation_peer_test_intent(
        mutation_id: uuid::Uuid,
        tier_name: &str,
        candidate_digest: [u8; 32],
    ) -> TierMutationIntent {
        TierMutationIntent {
            mutation_id,
            revision: 1,
            kind: TierMutationIntentKind::Edit,
            state: TierMutationIntentState::Prepared,
            old_config_etag: Some("old-etag".to_string()),
            committed_config_etag: None,
            candidate_digest,
            affected_targets: vec![TierMutationIntentTarget {
                tier_name: tier_name.to_string(),
                old_backend_identity: Some([1; 32]),
                new_backend_identity: Some([2; 32]),
            }],
            expires_at_unix_nanos: i64::MAX,
        }
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_mutation_abort_tombstone_rejects_delayed_prepare() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-mutation-abort-tombstone", &[4])).await;
        register_mock_tier(&store.tier_config_mgr(), "COLD-A").await;
        store
            .tier_config_mgr()
            .read()
            .await
            .save_tiering_config(store.clone())
            .await
            .expect("base tier config should persist");
        let config_info = store
            .get_object_info(
                RUSTFS_META_BUCKET,
                &format!("{}/{}", com::CONFIG_PREFIX, TIER_CONFIG_FILE),
                &ObjectOptions::default(),
            )
            .await
            .expect("base tier config metadata should load");
        let mutation_id = uuid::Uuid::new_v4();
        let mut intent = tier_mutation_peer_test_intent(mutation_id, "COLD-A", [7; 32]);
        intent.old_config_etag = config_info.etag;
        intent.expires_at_unix_nanos =
            i64::try_from((OffsetDateTime::now_utc() + time::Duration::hours(1)).unix_timestamp_nanos())
                .expect("future tombstone expiry should fit i64");
        let payload = intent.encode().expect("prepare identity should encode");

        let mismatch_id = uuid::Uuid::new_v4();
        let mut mismatch = intent.clone();
        mismatch.mutation_id = mismatch_id;
        mismatch.old_config_etag = Some("not-the-current-config-etag".to_string());
        let mismatch_error = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Abort,
            mismatch_id,
            &mismatch.encode().expect("proof-mismatch Abort should encode"),
        )
        .await
        .expect_err("a missing-record Abort with the wrong config proof must fail closed");
        assert!(matches!(mismatch_error, TierMutationPeerError::AbortProofMismatch));
        assert!(matches!(
            load_tier_mutation_intent_record(store.clone(), mismatch_id).await,
            Err(Error::ConfigNotFound)
        ));

        let aborted = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Abort,
            mutation_id,
            &payload,
        )
        .await
        .expect("missing-record abort should persist a tombstone");
        assert!(aborted.applied);
        assert_eq!(aborted.state, TierMutationPeerState::Aborted);
        let tombstone = load_tier_mutation_intent_record(store.clone(), mutation_id)
            .await
            .expect("abort tombstone should be durable");
        assert_eq!(tombstone.state, TierMutationIntentState::Aborted);
        assert!(tombstone.same_identity_as(&intent));

        let manager = store.tier_config_mgr();
        TierConfigMgr::reload_handle(&manager, store.clone())
            .await
            .expect("reload should retain an unexpired abort tombstone");
        let tombstone_after_reload = load_tier_mutation_intent_record(store.clone(), mutation_id)
            .await
            .expect("reload must not clean an unexpired abort tombstone");
        assert_eq!(tombstone_after_reload.state, TierMutationIntentState::Aborted);
        assert!(tombstone_after_reload.same_identity_as(&intent));

        register_mock_tier(&store.tier_config_mgr(), "COLD-B").await;
        store
            .tier_config_mgr()
            .read()
            .await
            .save_tiering_config(store.clone())
            .await
            .expect("later config change should persist before idempotent Abort replay");
        let retried_abort = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Abort,
            mutation_id,
            &payload,
        )
        .await
        .expect("terminal Abort retry must not depend on the mutable current config proof");
        assert!(!retried_abort.applied);
        assert_eq!(retried_abort.state, TierMutationPeerState::Aborted);

        let delayed = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            mutation_id,
            &payload,
        )
        .await
        .expect("delayed matching prepare should converge on the tombstone");
        assert!(!delayed.applied);
        assert_eq!(delayed.state, TierMutationPeerState::Aborted);
        TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A")
            .await
            .expect("delayed prepare must not reinstall the runtime block");

        let mut conflicting = intent.clone();
        conflicting.candidate_digest = [8; 32];
        let conflict = handle_tier_mutation_peer_request(
            store,
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            mutation_id,
            &conflicting.encode().expect("conflicting prepare should encode"),
        )
        .await
        .expect_err("different identity must not reuse an abort tombstone");
        assert!(matches!(conflict, TierMutationPeerError::ConflictingIntent));
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_mutation_expired_prepare_cannot_recreate_a_cleaned_tombstone() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-mutation-expired-replay", &[4])).await;
        register_mock_tier(&store.tier_config_mgr(), "COLD-A").await;
        let mutation_id = uuid::Uuid::new_v4();
        let mut intent = tier_mutation_peer_test_intent(mutation_id, "COLD-A", [11; 32]);
        intent.expires_at_unix_nanos =
            i64::try_from((OffsetDateTime::now_utc() - time::Duration::seconds(1)).unix_timestamp_nanos())
                .expect("expired timestamp should fit i64");
        let payload = intent.encode().expect("expired signed Prepare should still encode");

        let error = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            mutation_id,
            &payload,
        )
        .await
        .expect_err("a missing-record expired Prepare must fail closed");
        assert!(matches!(error, TierMutationPeerError::ExpiredIntent));
        assert!(matches!(
            load_tier_mutation_intent_record(store.clone(), mutation_id).await,
            Err(Error::ConfigNotFound)
        ));
        TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A")
            .await
            .expect("expired replay must not install a peer-only runtime fence");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_mutation_peer_prepare_waits_for_inflight_reference_lease() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-mutation-peer-drain", &[4])).await;
        register_mock_tier(&store.tier_config_mgr(), "COLD-A").await;
        let old_lease = TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A")
            .await
            .expect("old tier operation lease should be available");
        let mutation_id = uuid::Uuid::new_v4();
        let mut intent = tier_mutation_peer_test_intent(mutation_id, "COLD-A", [9; 32]);
        intent.affected_targets[0].old_backend_identity = Some(old_lease.backend_identity());
        let payload = intent.encode().expect("prepare intent should encode");
        let prepare_store = store.clone();
        let prepare = tokio::spawn(async move {
            handle_tier_mutation_peer_request(
                prepare_store,
                TIER_MUTATION_RPC_PROTOCOL_VERSION,
                TierMutationRpcPhase::Prepare,
                mutation_id,
                &payload,
            )
            .await
        });

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A")
                    .await
                    .is_err()
                {
                    return;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("peer Prepare should install its runtime block");
        assert!(
            !prepare.is_finished(),
            "peer Prepare must not acknowledge while a pre-existing reference creator still holds a lease"
        );

        drop(old_lease);
        let outcome = prepare
            .await
            .expect("peer Prepare task should join")
            .expect("peer Prepare should finish after the lease drains");
        assert_eq!(outcome.state, TierMutationPeerState::Prepared);
        TierConfigMgr::clear_prepared_mutation_intent_block(&store.tier_config_mgr(), mutation_id)
            .await
            .expect("test should clear the prepared runtime block");
    }

    #[cfg(feature = "test-util")]
    #[test]
    #[serial_test::serial(storage_class_env)]
    fn tier_mutation_peer_handler_applies_prepare_commit_and_abort_idempotently() {
        // This end-to-end state-machine scenario intentionally keeps many
        // durable fixtures live at once. Give its debug future the same
        // dedicated stack used by the other large ecstore scenarios so the
        // assertions run under the default test command without RUST_MIN_STACK.
        std::thread::Builder::new()
            .name("tier-mutation-peer-handler-test".to_string())
            .stack_size(32 * 1024 * 1024)
            .spawn(|| {
                let runtime = tokio::runtime::Builder::new_multi_thread()
                    .enable_all()
                    .worker_threads(2)
                    .build()
                    .expect("tier mutation peer handler test runtime should build");
                runtime.block_on(tier_mutation_peer_handler_case());
            })
            .expect("tier mutation peer handler test thread should spawn")
            .join()
            .expect("tier mutation peer handler test thread should complete");
    }

    #[cfg(feature = "test-util")]
    async fn tier_mutation_peer_handler_case() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-mutation-peer-handler", &[4])).await;
        register_mock_tier(&store.tier_config_mgr(), "COLD-A").await;
        let (candidate_digest, config_etag) = {
            let tier_config_mgr = store.tier_config_mgr();
            let manager = tier_config_mgr.read().await;
            let candidate_digest = tier_config_candidate_digest(&manager).expect("peer commit candidate digest should build");
            manager
                .save_tiering_config(store.clone())
                .await
                .expect("peer commit config fixture should persist");
            let config_info = store
                .get_object_info(
                    RUSTFS_META_BUCKET,
                    &format!("{}/{}", com::CONFIG_PREFIX, TIER_CONFIG_FILE),
                    &ObjectOptions::default(),
                )
                .await
                .expect("peer commit config fixture should load");
            (candidate_digest, config_info.etag.expect("peer commit config should carry an ETag"))
        };
        let mutation_id = uuid::Uuid::new_v4();
        let mut intent = tier_mutation_peer_test_intent(mutation_id, "COLD-A", candidate_digest);
        intent.old_config_etag = Some(config_etag.clone());
        let prepare_payload = intent.encode().expect("prepare intent should encode");

        let prepared = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            mutation_id,
            &prepare_payload,
        )
        .await
        .expect("first prepare should create the peer intent");
        assert!(prepared.applied);
        assert_eq!(prepared.state, TierMutationPeerState::Prepared);
        let blocked = match TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A").await {
            Ok(_) => panic!("prepared peer mutation should block new tier operation leases"),
            Err(err) => err,
        };
        assert!(
            blocked.message.contains("being replaced"),
            "prepared peer mutation should reuse the existing blocked-tier error: {blocked}"
        );

        let retried_prepare = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            mutation_id,
            &prepare_payload,
        )
        .await
        .expect("same prepare retry should be idempotent");
        assert!(!retried_prepare.applied);
        assert_eq!(retried_prepare.state, TierMutationPeerState::Prepared);
        let retried_blocked = match TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A").await {
            Ok(_) => panic!("prepared retry should keep blocking new tier operation leases"),
            Err(err) => err,
        };
        assert!(
            retried_blocked.message.contains("being replaced"),
            "prepared retry should keep the existing blocked-tier error: {retried_blocked}"
        );

        let mismatched_commit = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Commit,
            mutation_id,
            b"not-the-current-etag",
        )
        .await
        .expect_err("commit with a mismatched config proof must fail closed");
        assert!(matches!(mismatched_commit, TierMutationPeerError::CommitProofMismatch));

        register_mock_tier(&store.tier_config_mgr(), "COLD-C").await;
        let bad_digest_id = uuid::Uuid::new_v4();
        let mut bad_digest_intent = tier_mutation_peer_test_intent(bad_digest_id, "COLD-C", [9; 32]);
        bad_digest_intent.old_config_etag = Some(config_etag.clone());
        handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            bad_digest_id,
            &bad_digest_intent.encode().expect("bad digest prepare intent should encode"),
        )
        .await
        .expect("bad digest prepare should install a prepared intent");
        let mismatched_digest_commit = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Commit,
            bad_digest_id,
            config_etag.as_bytes(),
        )
        .await
        .expect_err("a correct ETag with a mismatched candidate digest must fail closed");
        assert!(matches!(mismatched_digest_commit, TierMutationPeerError::CommitProofMismatch));
        let bad_digest_loaded = load_tier_mutation_intent_record(store.clone(), bad_digest_id)
            .await
            .expect("mismatched digest must leave the prepared intent durable");
        assert_eq!(bad_digest_loaded.state, TierMutationIntentState::Prepared);
        let bad_digest_blocked = match TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-C").await {
            Ok(_) => panic!("mismatched digest must retain the prepared runtime fence"),
            Err(err) => err,
        };
        assert!(bad_digest_blocked.message.contains("being replaced"), "{bad_digest_blocked}");
        handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Abort,
            bad_digest_id,
            &bad_digest_intent.encode().expect("bad digest abort identity should encode"),
        )
        .await
        .expect("the negative digest proof fixture should clean up through abort");

        let committed = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Commit,
            mutation_id,
            config_etag.as_bytes(),
        )
        .await
        .expect("commit should advance the prepared peer intent");
        assert!(committed.applied);
        assert_eq!(committed.state, TierMutationPeerState::Committed);
        let committed_blocked = match TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A").await {
            Ok(_) => panic!("committed peer mutation must remain blocked until local reload publishes the config"),
            Err(err) => err,
        };
        assert!(
            committed_blocked.message.contains("being replaced"),
            "committed peer mutation should keep the existing blocked-tier error: {committed_blocked}"
        );

        let retried_commit = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Commit,
            mutation_id,
            config_etag.as_bytes(),
        )
        .await
        .expect("same commit retry should be idempotent");
        assert!(!retried_commit.applied);
        assert_eq!(retried_commit.state, TierMutationPeerState::Committed);
        let retried_commit_blocked = match TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A").await {
            Ok(_) => panic!("committed retry must keep the tier blocked until local reload"),
            Err(err) => err,
        };
        assert!(
            retried_commit_blocked.message.contains("being replaced"),
            "committed retry should keep the existing blocked-tier error: {retried_commit_blocked}"
        );

        let delayed_prepare_retry = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            mutation_id,
            &prepare_payload,
        )
        .await
        .expect("delayed duplicate prepare should report the durable committed state");
        assert!(!delayed_prepare_retry.applied);
        assert_eq!(delayed_prepare_retry.state, TierMutationPeerState::Committed);
        let delayed_prepare_blocked = match TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A").await {
            Ok(_) => panic!("delayed committed prepare retry must preserve the committed runtime block"),
            Err(err) => err,
        };
        assert!(
            delayed_prepare_blocked.message.contains("being replaced"),
            "delayed committed prepare retry should keep the existing blocked-tier error: {delayed_prepare_blocked}"
        );

        let loaded = load_tier_mutation_intent_record(store.clone(), mutation_id)
            .await
            .expect("committed peer intent should remain durable");
        assert_eq!(loaded.state, TierMutationIntentState::Committed);
        assert_eq!(loaded.committed_config_etag.as_deref(), Some(config_etag.as_str()));

        store
            .tier_config_mgr()
            .read()
            .await
            .save_tiering_config(store.clone())
            .await
            .expect("tier config should persist for cleaned intent commit proof");
        let tier_config_info = store
            .get_object_info(
                RUSTFS_META_BUCKET,
                &format!("{}/{}", com::CONFIG_PREFIX, TIER_CONFIG_FILE),
                &ObjectOptions::default(),
            )
            .await
            .expect("tier config object info should load");
        let tier_config_etag = tier_config_info.etag.expect("tier config should carry an ETag");
        delete_tier_mutation_intent_record(store.clone(), mutation_id)
            .await
            .expect("simulate another node cleaning the shared committed peer intent");
        let cleaned_commit_retry = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Commit,
            mutation_id,
            tier_config_etag.as_bytes(),
        )
        .await
        .expect("commit retry after durable cleanup should be idempotently terminal");
        assert!(!cleaned_commit_retry.applied);
        assert_eq!(cleaned_commit_retry.state, TierMutationPeerState::Committed);
        let cleaned_commit_blocked = match TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A").await {
            Ok(_) => panic!("shared intent cleanup must not clear this node's committed runtime block"),
            Err(err) => err,
        };
        assert!(
            cleaned_commit_blocked.message.contains("being replaced"),
            "commit retry after shared cleanup should keep the existing blocked-tier error: {cleaned_commit_blocked}"
        );
        let mismatched_cleaned_commit = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Commit,
            mutation_id,
            b"not-the-current-etag",
        )
        .await
        .expect_err("missing intent without a matching committed config ETag must fail closed");
        assert!(matches!(mismatched_cleaned_commit, TierMutationPeerError::Store(Error::ConfigNotFound)));
        let refresh_store = store.clone();
        let refresh_manager = store.tier_config_mgr();
        let refresh_worker = tokio::spawn(async move {
            TierConfigMgr::refresh_tier_config_handle_with(refresh_manager, refresh_store).await;
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(lease) = TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A").await {
                    drop(lease);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("matching cleaned commit should wake the refresh worker and clear the committed fence");
        refresh_worker.abort();
        let _ = refresh_worker.await;

        let abort_id = uuid::Uuid::new_v4();
        register_mock_tier(&store.tier_config_mgr(), "COLD-B").await;
        store
            .tier_config_mgr()
            .read()
            .await
            .save_tiering_config(store.clone())
            .await
            .expect("abort target tier config should persist");
        let abort_config_info = store
            .get_object_info(
                RUSTFS_META_BUCKET,
                &format!("{}/{}", com::CONFIG_PREFIX, TIER_CONFIG_FILE),
                &ObjectOptions::default(),
            )
            .await
            .expect("abort target config metadata should load");
        let abort_config_etag = abort_config_info.etag.expect("abort target config should carry an ETag");
        let mut abort_intent = tier_mutation_peer_test_intent(abort_id, "COLD-B", [4; 32]);
        abort_intent.old_config_etag = Some(abort_config_etag);
        let abort_prepare_payload = abort_intent.encode().expect("abort prepare intent should encode");
        handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            abort_id,
            &abort_prepare_payload,
        )
        .await
        .expect("abort target prepare should create the peer intent");
        let abort_blocked = match TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-B").await {
            Ok(_) => panic!("abort target prepare should block new tier operation leases"),
            Err(err) => err,
        };
        assert!(
            abort_blocked.message.contains("being replaced"),
            "abort target prepare should reuse the existing blocked-tier error: {abort_blocked}"
        );

        let aborted = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Abort,
            abort_id,
            &abort_prepare_payload,
        )
        .await
        .expect("abort should advance the prepared peer intent");
        assert!(aborted.applied);
        assert_eq!(aborted.state, TierMutationPeerState::Aborted);
        TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-B")
            .await
            .expect("durable abort must clear the peer runtime fence immediately");

        let retried_abort = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Abort,
            abort_id,
            &abort_prepare_payload,
        )
        .await
        .expect("same abort retry should be idempotent before recovery cleanup");
        assert!(!retried_abort.applied);
        assert_eq!(retried_abort.state, TierMutationPeerState::Aborted);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn late_abort_after_config_commit_keeps_fence_until_commit_recovery() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-mutation-late-abort", &[4])).await;
        register_mock_tier(&store.tier_config_mgr(), "COLD-A").await;
        store
            .tier_config_mgr()
            .read()
            .await
            .save_tiering_config(store.clone())
            .await
            .expect("base tier config should persist");
        let base_config_info = store
            .get_object_info(
                RUSTFS_META_BUCKET,
                &format!("{}/{}", com::CONFIG_PREFIX, TIER_CONFIG_FILE),
                &ObjectOptions::default(),
            )
            .await
            .expect("base tier config metadata should load");
        let base_etag = base_config_info.etag.expect("base tier config should carry an ETag");

        register_mock_tier(&store.tier_config_mgr(), "COLD-B").await;
        let candidate_digest = {
            let manager = store.tier_config_mgr();
            let manager = manager.read().await;
            tier_config_candidate_digest(&manager).expect("candidate digest should build")
        };
        let mutation_id = uuid::Uuid::new_v4();
        let mut intent = tier_mutation_peer_test_intent(mutation_id, "COLD-B", candidate_digest);
        intent.old_config_etag = Some(base_etag.clone());
        let prepare_payload = intent.encode().expect("late abort prepare intent should encode");
        handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            mutation_id,
            &prepare_payload,
        )
        .await
        .expect("prepare should install the runtime fence");

        store
            .tier_config_mgr()
            .read()
            .await
            .save_tiering_config(store.clone())
            .await
            .expect("candidate tier config should persist before the late abort");
        let committed_config_info = store
            .get_object_info(
                RUSTFS_META_BUCKET,
                &format!("{}/{}", com::CONFIG_PREFIX, TIER_CONFIG_FILE),
                &ObjectOptions::default(),
            )
            .await
            .expect("committed tier config metadata should load");
        let committed_etag = committed_config_info
            .etag
            .expect("committed tier config should carry an ETag");
        assert_ne!(committed_etag, base_etag);

        let late_abort = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Abort,
            mutation_id,
            &prepare_payload,
        )
        .await
        .expect_err("an abort after the candidate config commit must fail closed");
        assert!(matches!(late_abort, TierMutationPeerError::AbortProofMismatch));
        let prepared = load_tier_mutation_intent_record(store.clone(), mutation_id)
            .await
            .expect("rejected late abort must retain the prepared intent");
        assert_eq!(prepared.state, TierMutationIntentState::Prepared);
        let blocked = match TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-B").await {
            Ok(_) => panic!("rejected late abort must retain the runtime fence"),
            Err(err) => err,
        };
        assert!(blocked.message.contains("being replaced"), "{blocked}");

        let committed = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Commit,
            mutation_id,
            committed_etag.as_bytes(),
        )
        .await
        .expect("matching commit should converge the rejected late abort fixture");
        assert!(committed.applied);
        assert_eq!(committed.state, TierMutationPeerState::Committed);

        let refresh_store = store.clone();
        let refresh_manager = store.tier_config_mgr();
        let refresh_worker = tokio::spawn(async move {
            TierConfigMgr::refresh_tier_config_handle_with(refresh_manager, refresh_store).await;
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(lease) = TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-B").await {
                    drop(lease);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("commit recovery should publish before clearing the late-abort fence");
        refresh_worker.abort();
        let _ = refresh_worker.await;
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn missing_record_commit_promotes_prepared_fence_until_worker_publish() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-mutation-missing-record-commit", &[4]))
                .await;
        register_mock_tier(&store.tier_config_mgr(), "COLD-A").await;
        let tier_config_mgr = store.tier_config_mgr();
        let candidate_digest = {
            let manager = tier_config_mgr.read().await;
            let digest = tier_config_candidate_digest(&manager).expect("candidate digest should build");
            manager
                .save_tiering_config(store.clone())
                .await
                .expect("candidate config should persist");
            digest
        };
        let config_info = store
            .get_object_info(
                RUSTFS_META_BUCKET,
                &format!("{}/{}", com::CONFIG_PREFIX, TIER_CONFIG_FILE),
                &ObjectOptions::default(),
            )
            .await
            .expect("candidate config metadata should load");
        let config_etag = config_info.etag.expect("candidate config should carry an ETag");
        let mutation_id = uuid::Uuid::new_v4();
        let intent = tier_mutation_peer_test_intent(mutation_id, "COLD-A", candidate_digest);
        handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            mutation_id,
            &intent.encode().expect("prepare intent should encode"),
        )
        .await
        .expect("prepare should install the runtime fence");
        delete_tier_mutation_intent_record(store.clone(), mutation_id)
            .await
            .expect("simulate shared intent cleanup before the local commit arrives");

        let committed = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Commit,
            mutation_id,
            config_etag.as_bytes(),
        )
        .await
        .expect("matching commit after shared cleanup should be terminal");
        assert!(!committed.applied);
        assert_eq!(committed.state, TierMutationPeerState::Committed);
        let blocked = match TierConfigMgr::acquire_operation_lease(&tier_config_mgr, "COLD-A").await {
            Ok(_) => panic!("the promoted committed fence must block old-generation leases"),
            Err(err) => err,
        };
        assert!(blocked.message.contains("being replaced"), "{blocked}");

        let refresh_store = store.clone();
        let refresh_manager = tier_config_mgr.clone();
        let refresh_worker = tokio::spawn(async move {
            TierConfigMgr::refresh_tier_config_handle_with(refresh_manager, refresh_store).await;
        });
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                if let Ok(lease) = TierConfigMgr::acquire_operation_lease(&tier_config_mgr, "COLD-A").await {
                    drop(lease);
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the commit notification should drive publish and clear the promoted fence");
        refresh_worker.abort();
        let _ = refresh_worker.await;
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_mutation_peer_handler_rejects_conflicting_prepare_without_overwrite() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-mutation-peer-conflict", &[4])).await;
        let mutation_id = uuid::Uuid::new_v4();
        let intent = tier_mutation_peer_test_intent(mutation_id, "COLD-A", [3; 32]);
        let prepare_payload = intent.encode().expect("prepare intent should encode");
        handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            mutation_id,
            &prepare_payload,
        )
        .await
        .expect("first prepare should create the peer intent");

        let conflicting = tier_mutation_peer_test_intent(mutation_id, "COLD-A", [4; 32]);
        let conflicting_payload = conflicting.encode().expect("conflicting intent should encode");
        let conflict = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Prepare,
            mutation_id,
            &conflicting_payload,
        )
        .await
        .expect_err("conflicting prepare must fail closed");
        assert!(matches!(conflict, TierMutationPeerError::ConflictingIntent));

        let loaded = load_tier_mutation_intent_record(store, mutation_id)
            .await
            .expect("conflicting prepare must not overwrite the first record");
        assert_eq!(loaded.candidate_digest, [3; 32]);
        assert_eq!(loaded.state, TierMutationIntentState::Prepared);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_mutation_intent_record_scan_paginates_exact_limit() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-mutation-intent-scan-page", &[4])).await;
        let build_intent = |mutation_id: uuid::Uuid, tier_name: &str| TierMutationIntent {
            mutation_id,
            revision: 1,
            kind: TierMutationIntentKind::Edit,
            state: TierMutationIntentState::Prepared,
            old_config_etag: Some("old-etag".to_string()),
            committed_config_etag: None,
            candidate_digest: [3; 32],
            affected_targets: vec![TierMutationIntentTarget {
                tier_name: tier_name.to_string(),
                old_backend_identity: Some([1; 32]),
                new_backend_identity: Some([2; 32]),
            }],
            expires_at_unix_nanos: 1_780_000_000_000_000_000,
        };
        let intent_ids = vec![
            uuid::Uuid::parse_str("11345678-1234-5678-9abc-def012345678").expect("first uuid should parse"),
            uuid::Uuid::parse_str("22345678-1234-5678-9abc-def012345678").expect("second uuid should parse"),
            uuid::Uuid::parse_str("33345678-1234-5678-9abc-def012345678").expect("third uuid should parse"),
        ];
        for (index, mutation_id) in intent_ids.iter().copied().enumerate() {
            let intent = build_intent(mutation_id, &format!("COLD-{index}"));
            save_tier_mutation_intent_record(store.clone(), &intent)
                .await
                .expect("tier mutation intent record should persist");
        }

        let exact_page = list_tier_mutation_intent_records(store.clone(), 3, None)
            .await
            .expect("exact full page should scan");
        assert_eq!(exact_page.scanned, 3);
        assert_eq!(exact_page.intents.len(), 3);
        assert_eq!(exact_page.failed, 0);
        assert!(!exact_page.truncated);
        assert_eq!(exact_page.next_marker, None);

        let first_page = list_tier_mutation_intent_records(store.clone(), 2, None)
            .await
            .expect("first page should scan");
        assert_eq!(first_page.scanned, 2);
        assert_eq!(first_page.intents.len(), 2);
        assert_eq!(first_page.failed, 0);
        assert!(first_page.truncated);
        assert!(first_page.next_marker.is_some());

        let second_page = list_tier_mutation_intent_records(store, 2, first_page.next_marker)
            .await
            .expect("second page should scan");
        let mut loaded_ids: Vec<_> = first_page
            .intents
            .into_iter()
            .chain(second_page.intents.into_iter())
            .map(|intent| intent.mutation_id)
            .collect();
        loaded_ids.sort();

        assert_eq!(second_page.scanned, 1);
        assert_eq!(second_page.failed, 0);
        assert!(!second_page.truncated);
        assert_eq!(second_page.next_marker, None);
        assert_eq!(loaded_ids, intent_ids);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn cancelled_transition_cleanup_recovers_from_its_own_instance_transaction() {
        struct ResolverReset(Arc<std::sync::Mutex<Option<std::sync::Weak<crate::store::ECStore>>>>);

        impl Drop for ResolverReset {
            fn drop(&mut self) {
                *self.0.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = None;
            }
        }

        let temp_a = tempfile::tempdir().expect("create transition store dir a");
        let temp_b = tempfile::tempdir().expect("create transition store dir b");
        let shutdown_a = CancellationToken::new();
        let shutdown_b = CancellationToken::new();
        shutdown_a.cancel();
        shutdown_b.cancel();
        let (ctx_a, store_a, shutdown_a) = without_storage_class_env(build_isolated_test_store_with_shutdown(
            temp_a.path(),
            "transition-cleanup-context-a",
            &[4],
            shutdown_a,
        ))
        .await;
        let (ctx_b, store_b, shutdown_b) = without_storage_class_env(build_isolated_test_store_with_shutdown(
            temp_b.path(),
            "transition-cleanup-context-b",
            &[4],
            shutdown_b,
        ))
        .await;
        assert!(shutdown_a.is_cancelled());
        assert!(shutdown_b.is_cancelled());
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store_a.clone(), Vec::new()).await;

        let resolver_target = Arc::new(std::sync::Mutex::new(Some(Arc::downgrade(&store_b))));
        let resolver_store = resolver_target.clone();
        assert!(
            set_object_store_resolver(Arc::new(move || {
                resolver_store
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade)
            })),
            "the cross-context regression test must install the only process object-store resolver"
        );
        let _resolver_reset = ResolverReset(resolver_target);
        assert!(
            runtime_sources::object_store_handle().is_some_and(|store| Arc::ptr_eq(&store, &store_b)),
            "the process resolver must deliberately point at store B"
        );

        let tier_name = "CROSSCTXA";
        let backend = register_transition_reconcile_test_tier(&ctx_a.tier_config_mgr(), tier_name).await;
        backend.set_put_remote_version(Some(uuid::Uuid::new_v4().to_string())).await;
        backend.reject_next_non_empty_remote_version_validation();
        let remove_barrier = backend.arm_failing_remove_barrier().await;

        let bucket = "transition-cleanup-context-a";
        let object = "rejected-candidate.bin";
        store_a
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("store A bucket should be created");
        let mut reader = PutObjReader::from_vec(b"cross-context rejected transition cleanup".repeat(1024));
        let original = store_a
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("store A source object should be written");
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name.to_string(),
                etag: original.etag.clone().expect("the source object should have an ETag"),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let cleanup_store_barrier = TransitionCleanupStoreBarrier::install();
        let transition_store = store_a.clone();
        let transition = tokio::spawn(async move { transition_store.transition_object(bucket, object, &opts).await });
        cleanup_store_barrier.wait_until_paused().await;
        transition.abort();
        assert!(
            transition
                .await
                .expect_err("the transition task should observe cancellation")
                .is_cancelled()
        );

        remove_barrier.wait_until_paused().await;
        let transaction_counts = (
            transition_transaction_record_count(store_a.clone()).await,
            transition_transaction_record_count(store_b.clone()).await,
        );
        assert_eq!(
            transaction_counts,
            (1, 0),
            "the transition transaction must remain only on store A even while the process resolver points at store B"
        );
        let journal_counts = (
            tier_delete_journal_count(store_a.clone()).await,
            tier_delete_journal_count(store_b.clone()).await,
        );
        assert_eq!(
            journal_counts,
            (0, 0),
            "rejected transition uploads must not create a tier-delete journal"
        );
        assert_eq!(backend.object_count().await, 1, "failed cleanup should retain the remote candidate");
        remove_barrier.release();
        remove_barrier.wait_until_operation_dropped().await;

        let recovered = recover_transition_transaction_records(store_a.clone(), 100, None)
            .await
            .expect("store A should recover its own cancelled-transition transaction");
        assert_eq!(
            (recovered.scanned, recovered.recovered, recovered.retained, recovered.failed),
            (1, 1, 0, 0)
        );
        assert_eq!(transition_transaction_record_count(store_a.clone()).await, 0);
        assert_eq!(transition_transaction_record_count(store_b.clone()).await, 0);
        assert_eq!(tier_delete_journal_count(store_a.clone()).await, 0);
        assert_eq!(tier_delete_journal_count(store_b.clone()).await, 0);
        assert_eq!(
            backend.object_count().await,
            0,
            "store A recovery should delete the exact remote candidate"
        );
        assert!(!Arc::ptr_eq(&ctx_a, &ctx_b), "the regression requires two distinct instance contexts");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_expires_abandoned_attempt_at_budget_bound() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "transition-transaction-expired-attempt-budget",
            &[4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let transaction = TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
            transaction_id: uuid::Uuid::new_v4(),
            owner_epoch: uuid::Uuid::new_v4(),
            write_id: uuid::Uuid::new_v4(),
            source: TransitionSourceIdentity {
                bucket: "source-bucket".to_string(),
                object: "source-object".to_string(),
                version_id: Some(uuid::Uuid::new_v4()),
                data_dir: uuid::Uuid::new_v4(),
                mod_time_unix_nanos: 1_770_000_000_000_000_000,
                size: 42,
                etag: "source-etag".to_string(),
                version_mode: TransitionSourceVersionMode::Versioned,
            },
            tier_name: "UNUSEDABANDONEDTIER".to_string(),
            backend_fingerprint: [7; 32],
            not_after_unix_nanos: 1,
        })
        .expect("transaction should build");
        save_transition_transaction_record(store.clone(), &transaction)
            .await
            .expect("transaction record should persist");

        let record_name =
            transition_transaction_record_object_name(transaction.transaction_id).expect("transaction record name should derive");
        let source = observe_recovery_source(
            store.clone(),
            &record_name,
            crate::bucket::lifecycle::transition_transaction::TRANSITION_TRANSACTION_SCHEMA,
        )
        .await
        .expect("transaction source generation should be observable");
        let mut control = IlmRecoveryControl::new(
            IlmRecoveryControlIdentity {
                protocol: IlmRecoveryProtocol::TransitionTransaction,
                canonical_source_path: record_name,
                stable_operation_identity: transaction.transaction_id.to_string(),
                record_class: "transition_transaction_v1".to_string(),
            },
            source.generation,
            IlmRecoveryClassification::Retrying,
            2_000_000_000,
            IlmRecoveryErrorCode::None,
        )
        .expect("recovery control should build");
        let mut now = 3_000_000_000;
        for _ in 1..MAX_RECOVERY_ATTEMPTS {
            now = now.max(control.next_attempt_at_unix_nanos.unwrap_or(now));
            control
                .claim("cancelled-or-timed-out-owner", uuid::Uuid::new_v4(), now, 1)
                .expect("abandoned attempt should claim");
            control
                .record_expired_attempt(now + 1)
                .expect("expired attempt should consume retry budget");
            now += 2;
        }
        now = now.max(control.next_attempt_at_unix_nanos.expect("last retry should have a backoff"));
        control
            .claim("cancelled-or-timed-out-owner", uuid::Uuid::new_v4(), now, 1)
            .expect("final abandoned attempt should claim");
        save_recovery_control_if_absent(store.clone(), &control)
            .await
            .expect("claimed recovery control should persist");

        let stats = recover_transition_transaction_records_at(store.clone(), 100, None, i128::from(now + 1))
            .await
            .expect("recovery should account for the expired attempt");
        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (1, 0, 1, 0));

        let control_id = transition_recovery_control_id(&transaction).expect("control id should derive");
        let persisted = load_recovery_control(store.clone(), IlmRecoveryProtocol::TransitionTransaction, &control_id)
            .await
            .expect("expired recovery control should remain inspectable");
        assert_eq!(persisted.control.classification, IlmRecoveryClassification::OperatorRequired);
        assert_eq!(persisted.control.attempt_count, u64::from(MAX_RECOVERY_ATTEMPTS));
        assert_eq!(persisted.control.consecutive_failure_count, MAX_RECOVERY_ATTEMPTS);
        assert_eq!(persisted.control.last_error_code, IlmRecoveryErrorCode::AttemptLeaseExpired);
        assert!(persisted.control.owner.is_none());
        assert_eq!(
            transition_transaction_record_count(store).await,
            1,
            "budget exhaustion must retain the source record"
        );
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_retains_active_uploaded_candidate_until_commit() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transition-transaction-active-uploaded", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXACTIVEUPLOADED";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "transition-transaction-active-uploaded-bucket";
        let object = "object.bin";
        let payload = b"active Uploaded ownership must survive recovery until local commit".repeat(1024);
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("source bucket should be created");
        let mut reader = PutObjReader::from_vec(payload.clone());
        let source = store
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let barrier = TransitionUploadedCommitBarrier::install(bucket, object);
        let transition_store = store.clone();
        let transition = tokio::spawn(async move {
            transition_store
                .transition_object(
                    bucket,
                    object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            status: TRANSITION_PENDING.to_string(),
                            tier: tier_name.to_string(),
                            etag: source.etag.clone().expect("source object should have an ETag"),
                            ..Default::default()
                        },
                        version_id: source.version_id.map(|version_id| version_id.to_string()),
                        mod_time: source.mod_time,
                        ..Default::default()
                    },
                )
                .await
        });
        barrier.wait_until_paused().await;

        let stats = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("recovery should inspect the active Uploaded transaction");
        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (1, 0, 1, 0));
        assert_eq!(backend.object_count().await, 1, "active ownership must retain the remote candidate");
        assert_eq!(backend.remove_count().await, 0, "active ownership must fence remote DELETE");
        assert_eq!(transition_transaction_record_count(store.clone()).await, 1);

        barrier.release();
        transition
            .await
            .expect("transition task should join")
            .expect("transition should commit after the barrier is released");
        assert_eq!(transition_transaction_record_count(store.clone()).await, 0);
        assert_eq!(backend.remove_count().await, 0);

        let mut body = Vec::new();
        store
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("committed tier object should remain readable")
            .stream
            .read_to_end(&mut body)
            .await
            .expect("tier object body should drain");
        assert_eq!(body, payload);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_deletes_expired_uploaded_remote_candidate() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transition-transaction-recovery", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXRECOVERY";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let exact_version = uuid::Uuid::new_v4().to_string();
        let cases = [
            (
                "exact",
                exact_version.clone(),
                TransitionRemoteVersion::versioned(exact_version),
                TransitionSourceVersionMode::Versioned,
            ),
            (
                "suspended-null",
                "null".to_string(),
                TransitionRemoteVersion::versioned("null"),
                TransitionSourceVersionMode::VersionSuspended,
            ),
            (
                "known-unversioned",
                String::new(),
                TransitionRemoteVersion::unversioned(),
                TransitionSourceVersionMode::Unversioned,
            ),
        ];
        let mut expected_removes = Vec::new();
        let mut recovery_control_ids = Vec::new();
        for (case, put_version, remote_version, source_mode) in cases {
            let mut transaction = TransitionTransaction::new(TransitionTransactionInit {
                deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
                transaction_id: uuid::Uuid::new_v4(),
                owner_epoch: uuid::Uuid::new_v4(),
                write_id: uuid::Uuid::new_v4(),
                source: TransitionSourceIdentity {
                    bucket: "source-bucket".to_string(),
                    object: format!("source-{case}"),
                    version_id: (source_mode == TransitionSourceVersionMode::Versioned).then(uuid::Uuid::new_v4),
                    data_dir: uuid::Uuid::new_v4(),
                    mod_time_unix_nanos: 1_770_000_000_000_000_000,
                    size: 42,
                    etag: "source-etag".to_string(),
                    version_mode: source_mode,
                },
                tier_name: tier_name.to_string(),
                backend_fingerprint: backend_identity,
                not_after_unix_nanos: 1,
            })
            .expect("transaction should build");
            transaction
                .advance(transaction.fence(), TransitionTransactionState::Uploaded, Some(remote_version))
                .expect("transaction should enter uploaded state");
            backend.set_put_remote_version(Some(put_version.clone())).await;
            let candidate = bytes::Bytes::from_static(b"orphan candidate");
            backend
                .put(
                    &transaction.remote_object,
                    ReaderImpl::Body(candidate.clone()),
                    i64::try_from(candidate.len()).expect("test candidate length should fit i64"),
                )
                .await
                .expect("mock backend should accept candidate");
            save_transition_transaction_record(store.clone(), &transaction)
                .await
                .expect("transaction record should persist");
            recovery_control_ids
                .push(transition_recovery_control_id(&transaction).expect("transition recovery control id should derive"));
            expected_removes.push((transaction.remote_object, put_version));
        }

        let stats = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("transition transaction recovery should run");

        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (3, 3, 0, 0));
        assert_eq!(transition_transaction_record_count(store.clone()).await, 0);
        let mut actual_removes = backend.remove_versions().await;
        actual_removes.sort();
        expected_removes.sort();
        assert_eq!(actual_removes, expected_removes, "recovery must preserve each remote version shape");
        assert_eq!(backend.exact_remove_count(), 2);
        assert_eq!(backend.object_count().await, 0);
        for control_id in recovery_control_ids {
            let control = load_recovery_control(store.clone(), IlmRecoveryProtocol::TransitionTransaction, &control_id)
                .await
                .expect("completed recovery control should remain inspectable");
            assert_eq!(control.control.classification, IlmRecoveryClassification::Terminal);
            assert_eq!(control.control.attempt_count, 1);
            assert!(control.control.owner.is_none());
        }

        let replay = recover_transition_transaction_records(store, 100, None)
            .await
            .expect("replayed recovery should remain idempotent");
        assert_eq!((replay.scanned, replay.recovered, replay.retained, replay.failed), (0, 0, 0, 0));
        assert_eq!(
            backend.remove_versions().await.len(),
            3,
            "each expired candidate must be deleted only once"
        );
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_resumes_source_cleanup_after_terminal_crash() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transition-transaction-terminal-crash", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXTERMINALCRASH";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let remote_version = uuid::Uuid::new_v4().to_string();
        let mut transaction = TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
            transaction_id: uuid::Uuid::new_v4(),
            owner_epoch: uuid::Uuid::new_v4(),
            write_id: uuid::Uuid::new_v4(),
            source: TransitionSourceIdentity {
                bucket: "source-bucket".to_string(),
                object: "source-object".to_string(),
                version_id: Some(uuid::Uuid::new_v4()),
                data_dir: uuid::Uuid::new_v4(),
                mod_time_unix_nanos: 1_770_000_000_000_000_000,
                size: 42,
                etag: "source-etag".to_string(),
                version_mode: TransitionSourceVersionMode::Versioned,
            },
            tier_name: tier_name.to_string(),
            backend_fingerprint: backend_identity,
            not_after_unix_nanos: 1,
        })
        .expect("transaction should build");
        transaction
            .advance(
                transaction.fence(),
                TransitionTransactionState::Uploaded,
                Some(TransitionRemoteVersion::versioned(remote_version.clone())),
            )
            .expect("transaction should enter uploaded state");
        backend.set_put_remote_version(Some(remote_version)).await;
        let candidate = bytes::Bytes::from_static(b"terminal crash candidate");
        backend
            .put(
                &transaction.remote_object,
                ReaderImpl::Body(candidate.clone()),
                i64::try_from(candidate.len()).expect("test candidate length should fit i64"),
            )
            .await
            .expect("mock backend should accept candidate");
        save_transition_transaction_record(store.clone(), &transaction)
            .await
            .expect("transaction record should persist");
        let control_id = transition_recovery_control_id(&transaction).expect("control id should derive");

        let barrier = TransitionRecoveryTerminalBarrier::install(transaction.transaction_id);
        let recovery_store = store.clone();
        let recovery = tokio::spawn(async move { recover_transition_transaction_records(recovery_store, 100, None).await });
        barrier.wait_until_paused().await;
        let terminal = load_recovery_control(store.clone(), IlmRecoveryProtocol::TransitionTransaction, &control_id)
            .await
            .expect("terminal control should persist before source cleanup");
        assert_eq!(terminal.control.classification, IlmRecoveryClassification::Terminal);
        assert_eq!(transition_transaction_record_count(store.clone()).await, 1);
        assert_eq!(backend.object_count().await, 0);
        assert_eq!(backend.exact_remove_count(), 1);

        recovery.abort();
        assert!(
            recovery
                .await
                .expect_err("recovery should be cancelled at the crash boundary")
                .is_cancelled()
        );
        drop(barrier);

        let replay = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("terminal control should resume source cleanup without another remote delete");
        assert_eq!((replay.scanned, replay.recovered, replay.retained, replay.failed), (1, 1, 0, 0));
        assert_eq!(transition_transaction_record_count(store).await, 0);
        assert_eq!(backend.exact_remove_count(), 1, "terminal replay must not repeat the remote delete");
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_cas_loses_to_active_state_advance() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transition-transaction-recovery-cas", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXRECOVERYCAS";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let remote_version = uuid::Uuid::new_v4().to_string();
        let mut uploaded = TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
            transaction_id: uuid::Uuid::new_v4(),
            owner_epoch: uuid::Uuid::new_v4(),
            write_id: uuid::Uuid::new_v4(),
            source: TransitionSourceIdentity {
                bucket: "source-bucket".to_string(),
                object: "source-object".to_string(),
                version_id: Some(uuid::Uuid::new_v4()),
                data_dir: uuid::Uuid::new_v4(),
                mod_time_unix_nanos: 1_770_000_000_000_000_000,
                size: 42,
                etag: "source-etag".to_string(),
                version_mode: TransitionSourceVersionMode::Versioned,
            },
            tier_name: tier_name.to_string(),
            backend_fingerprint: backend_identity,
            not_after_unix_nanos: 1,
        })
        .expect("transaction should build");
        uploaded
            .advance(
                uploaded.fence(),
                TransitionTransactionState::Uploaded,
                Some(TransitionRemoteVersion::versioned(remote_version.clone())),
            )
            .expect("transaction should enter uploaded state");
        backend.set_put_remote_version(Some(remote_version)).await;
        let candidate = bytes::Bytes::from_static(b"CAS-owned transition remote candidate");
        backend
            .put(
                &uploaded.remote_object,
                ReaderImpl::Body(candidate.clone()),
                i64::try_from(candidate.len()).expect("test candidate length should fit i64"),
            )
            .await
            .expect("mock backend should accept candidate");
        save_transition_transaction_record(store.clone(), &uploaded)
            .await
            .expect("transaction record should persist");
        let recovery_control_id =
            transition_recovery_control_id(&uploaded).expect("transition recovery control id should derive");

        let barrier = TransitionRecoveryClaimBarrier::install(uploaded.transaction_id);
        let recovery_store = store.clone();
        let recovery = tokio::spawn(async move { recover_transition_transaction_records(recovery_store, 100, None).await });
        barrier.wait_until_paused().await;

        let mut active = uploaded.clone();
        active
            .advance(active.fence(), TransitionTransactionState::LocalCommitStarted, None)
            .expect("active owner should advance to local commit");
        save_transition_transaction_record_if_current(store.clone(), &uploaded, &active)
            .await
            .expect("active owner should win the persisted CAS");
        barrier.release();

        let stats = recovery
            .await
            .expect("recovery task should join")
            .expect("recovery should treat the lost CAS as a retained transaction");
        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (1, 0, 1, 0));
        assert_eq!(
            load_transition_transaction_record(store.clone(), uploaded.transaction_id)
                .await
                .expect("newer transaction revision must remain"),
            active
        );
        let control = load_recovery_control(store, IlmRecoveryProtocol::TransitionTransaction, &recovery_control_id)
            .await
            .expect("lost source CAS should retain a retryable recovery control");
        assert_eq!(control.control.classification, IlmRecoveryClassification::Retrying);
        assert_eq!(control.control.consecutive_failure_count, 1);
        assert_eq!(control.control.last_error_code, IlmRecoveryErrorCode::SourceGenerationChanged);
        assert_eq!(backend.object_count().await, 1, "a stale recovery must not delete the candidate");
        assert_eq!(backend.remove_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_quarantines_missing_required_fields() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "transition-transaction-recovery-corrupt",
            &[4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXRECOVERYCORRUPT";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let remote_version = uuid::Uuid::new_v4().to_string();
        let mut transaction = TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
            transaction_id: uuid::Uuid::new_v4(),
            owner_epoch: uuid::Uuid::new_v4(),
            write_id: uuid::Uuid::new_v4(),
            source: TransitionSourceIdentity {
                bucket: "source-bucket".to_string(),
                object: "source-object".to_string(),
                version_id: None,
                data_dir: uuid::Uuid::new_v4(),
                mod_time_unix_nanos: 1_770_000_000_000_000_000,
                size: 42,
                etag: "source-etag".to_string(),
                version_mode: TransitionSourceVersionMode::Unversioned,
            },
            tier_name: tier_name.to_string(),
            backend_fingerprint: backend_identity,
            not_after_unix_nanos: 1,
        })
        .expect("transaction should build");
        transaction
            .advance(
                transaction.fence(),
                TransitionTransactionState::Uploaded,
                Some(TransitionRemoteVersion::versioned(remote_version.clone())),
            )
            .expect("transaction should enter uploaded state");
        backend.set_put_remote_version(Some(remote_version)).await;
        let candidate = bytes::Bytes::from_static(b"corrupt journal candidate");
        backend
            .put(
                &transaction.remote_object,
                ReaderImpl::Body(candidate.clone()),
                i64::try_from(candidate.len()).expect("test candidate length should fit i64"),
            )
            .await
            .expect("mock backend should accept candidate");

        let mut persisted: serde_json::Value = serde_json::from_slice(&transaction.encode().expect("transaction should encode"))
            .expect("transaction should be JSON");
        persisted["transaction"]
            .as_object_mut()
            .expect("transaction payload should be an object")
            .remove("not_after_unix_nanos");
        let path =
            transition_transaction_record_object_name(transaction.transaction_id).expect("transaction path should be canonical");
        com::save_config(
            store.clone(),
            &path,
            serde_json::to_vec(&persisted).expect("corrupt fixture should encode"),
        )
        .await
        .expect("corrupt transaction fixture should persist");

        let stats = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("recovery scan should isolate a corrupt record");
        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (1, 0, 0, 1));
        assert_eq!(
            transition_transaction_record_count(store).await,
            1,
            "corrupt evidence must remain quarantined"
        );
        assert_eq!(backend.object_count().await, 1, "corrupt evidence must never authorize remote DELETE");
        assert_eq!(backend.remove_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_retries_cleanup_pending_candidate() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transition-transaction-cleanup", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXCLEANUP";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let remote_version = uuid::Uuid::new_v4().to_string();
        let mut transaction = TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
            transaction_id: uuid::Uuid::new_v4(),
            owner_epoch: uuid::Uuid::new_v4(),
            write_id: uuid::Uuid::new_v4(),
            source: TransitionSourceIdentity {
                bucket: "source-bucket".to_string(),
                object: "source-object".to_string(),
                version_id: Some(uuid::Uuid::new_v4()),
                data_dir: uuid::Uuid::new_v4(),
                mod_time_unix_nanos: 1_770_000_000_000_000_000,
                size: 42,
                etag: "source-etag".to_string(),
                version_mode: TransitionSourceVersionMode::Versioned,
            },
            tier_name: tier_name.to_string(),
            backend_fingerprint: backend_identity,
            not_after_unix_nanos: 1_780_000_000_000_000_000,
        })
        .expect("transaction should build");
        let uploaded_fence = transaction
            .advance(
                transaction.fence(),
                TransitionTransactionState::Uploaded,
                Some(TransitionRemoteVersion::versioned(remote_version.clone())),
            )
            .expect("transaction should enter uploaded state");
        transaction
            .mark_cleanup_pending(
                uploaded_fence,
                TransitionCleanupProof {
                    transaction_id: transaction.transaction_id,
                    write_id: transaction.write_id,
                    remote_object: transaction.remote_object.clone(),
                    remote_version: transaction.remote_version.clone(),
                    backend_fingerprint: transaction.backend_fingerprint,
                    decision: TransitionCleanupDecision::UploadAbortedBeforeLocalCommit,
                },
            )
            .expect("transaction should enter cleanup pending state");
        backend.set_put_remote_version(Some(remote_version.clone())).await;
        let candidate = bytes::Bytes::from_static(b"cleanup pending candidate");
        backend
            .put(
                &transaction.remote_object,
                ReaderImpl::Body(candidate.clone()),
                i64::try_from(candidate.len()).expect("test candidate length should fit i64"),
            )
            .await
            .expect("mock backend should accept candidate");
        save_transition_transaction_record(store.clone(), &transaction)
            .await
            .expect("transaction record should persist");

        let stats = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("transition transaction recovery should run");

        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (1, 1, 0, 0));
        assert_eq!(transition_transaction_record_count(store.clone()).await, 0);
        assert_eq!(
            backend.remove_versions().await,
            vec![(transaction.remote_object.clone(), remote_version)],
            "cleanup pending recovery must retry the exact remote candidate delete"
        );
        assert_eq!(backend.exact_remove_count(), 1);
        assert_eq!(backend.object_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_retries_cleanup_pending_unversioned_candidate() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "transition-transaction-cleanup-unversioned",
            &[4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXCLEANUPUNVERSIONED";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let mut transaction = TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
            transaction_id: uuid::Uuid::new_v4(),
            owner_epoch: uuid::Uuid::new_v4(),
            write_id: uuid::Uuid::new_v4(),
            source: TransitionSourceIdentity {
                bucket: "source-bucket".to_string(),
                object: "source-object".to_string(),
                version_id: None,
                data_dir: uuid::Uuid::new_v4(),
                mod_time_unix_nanos: 1_770_000_000_000_000_000,
                size: 42,
                etag: "source-etag".to_string(),
                version_mode: TransitionSourceVersionMode::Unversioned,
            },
            tier_name: tier_name.to_string(),
            backend_fingerprint: backend_identity,
            not_after_unix_nanos: 1_780_000_000_000_000_000,
        })
        .expect("transaction should build");
        let uploaded_fence = transaction
            .advance(
                transaction.fence(),
                TransitionTransactionState::Uploaded,
                Some(TransitionRemoteVersion::unversioned()),
            )
            .expect("transaction should enter uploaded state");
        transaction
            .mark_cleanup_pending(
                uploaded_fence,
                TransitionCleanupProof {
                    transaction_id: transaction.transaction_id,
                    write_id: transaction.write_id,
                    remote_object: transaction.remote_object.clone(),
                    remote_version: transaction.remote_version.clone(),
                    backend_fingerprint: transaction.backend_fingerprint,
                    decision: TransitionCleanupDecision::UploadAbortedBeforeLocalCommit,
                },
            )
            .expect("transaction should enter cleanup pending state");
        backend.set_put_remote_version(Some(String::new())).await;
        let candidate = bytes::Bytes::from_static(b"cleanup pending unversioned candidate");
        backend
            .put(
                &transaction.remote_object,
                ReaderImpl::Body(candidate.clone()),
                i64::try_from(candidate.len()).expect("test candidate length should fit i64"),
            )
            .await
            .expect("mock backend should accept candidate");
        save_transition_transaction_record(store.clone(), &transaction)
            .await
            .expect("transaction record should persist");

        let stats = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("transition transaction recovery should run");

        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (1, 1, 0, 0));
        assert_eq!(transition_transaction_record_count(store.clone()).await, 0);
        assert_eq!(
            backend.remove_versions().await,
            vec![(transaction.remote_object.clone(), String::new())],
            "unversioned cleanup must not send a synthetic remote version"
        );
        assert_eq!(backend.exact_remove_count(), 0);
        assert_eq!(backend.object_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_keeps_cleanup_pending_record_when_delete_fails() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transition-transaction-cleanup-fail", &[4]))
                .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXCLEANUPFAIL";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let remote_version = uuid::Uuid::new_v4().to_string();
        let mut transaction = TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
            transaction_id: uuid::Uuid::new_v4(),
            owner_epoch: uuid::Uuid::new_v4(),
            write_id: uuid::Uuid::new_v4(),
            source: TransitionSourceIdentity {
                bucket: "source-bucket".to_string(),
                object: "source-object".to_string(),
                version_id: Some(uuid::Uuid::new_v4()),
                data_dir: uuid::Uuid::new_v4(),
                mod_time_unix_nanos: 1_770_000_000_000_000_000,
                size: 42,
                etag: "source-etag".to_string(),
                version_mode: TransitionSourceVersionMode::Versioned,
            },
            tier_name: tier_name.to_string(),
            backend_fingerprint: backend_identity,
            not_after_unix_nanos: 1_780_000_000_000_000_000,
        })
        .expect("transaction should build");
        transaction
            .advance(
                transaction.fence(),
                TransitionTransactionState::Uploaded,
                Some(TransitionRemoteVersion::versioned(remote_version.clone())),
            )
            .expect("transaction should enter uploaded state");
        let candidate = bytes::Bytes::from_static(b"cleanup pending candidate retained after failure");
        backend.set_put_remote_version(Some(remote_version)).await;
        backend
            .put(
                &transaction.remote_object,
                ReaderImpl::Body(candidate.clone()),
                i64::try_from(candidate.len()).expect("test candidate length should fit i64"),
            )
            .await
            .expect("mock backend should accept candidate");
        save_transition_transaction_record(store.clone(), &transaction)
            .await
            .expect("transaction record should persist");
        let recovery_control_id =
            transition_recovery_control_id(&transaction).expect("transition recovery control id should derive");

        backend.set_remove_failure(true);
        let stats = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("transition transaction recovery should keep scanning after cleanup failure");

        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (1, 0, 0, 1));
        assert_eq!(
            transition_transaction_record_count(store.clone()).await,
            1,
            "failed cleanup must keep the cleanup-pending transaction for retry"
        );
        assert_eq!(backend.remove_versions().await, Vec::<(String, String)>::new());
        assert_eq!(backend.exact_remove_count(), 1);
        assert_eq!(backend.object_count().await, 1);
        let control = load_recovery_control(store.clone(), IlmRecoveryProtocol::TransitionTransaction, &recovery_control_id)
            .await
            .expect("failed recovery control should persist");
        assert_eq!(control.control.classification, IlmRecoveryClassification::Retrying);
        assert_eq!(control.control.attempt_count, 1);
        assert_eq!(control.control.consecutive_failure_count, 1);
        assert!(
            control
                .control
                .next_attempt_at_unix_nanos
                .is_some_and(|next| next > OffsetDateTime::now_utc().unix_timestamp_nanos() as i64)
        );

        backend.set_remove_failure(false);
        let replay = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("recovery before the persisted deadline should be skipped");
        assert_eq!((replay.scanned, replay.recovered, replay.retained, replay.failed), (1, 0, 1, 0));
        assert_eq!(backend.exact_remove_count(), 1, "persisted backoff must prevent an immediate retry");

        let retry_at = control
            .control
            .next_attempt_at_unix_nanos
            .expect("retry deadline should persist");
        let retried = recover_transition_transaction_records_at(store.clone(), 100, None, i128::from(retry_at) + 1)
            .await
            .expect("recovery at the persisted deadline should retry the advanced source generation");
        assert_eq!((retried.scanned, retried.recovered, retried.retained, retried.failed), (1, 1, 0, 0));
        assert_eq!(backend.exact_remove_count(), 2);
        assert_eq!(backend.object_count().await, 0);
        assert_eq!(transition_transaction_record_count(store.clone()).await, 0);
        let terminal = load_recovery_control(store, IlmRecoveryProtocol::TransitionTransaction, &recovery_control_id)
            .await
            .expect("completed retry control should remain inspectable");
        assert_eq!(terminal.control.classification, IlmRecoveryClassification::Terminal);
        assert_eq!(terminal.control.attempt_count, 2);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_keeps_cleanup_pending_local_commit() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
            temp_dir.path(),
            "transition-transaction-cleanup-committed",
            &[4],
        ))
        .await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXCLEANUPCOMMITTED";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let bucket = "transition-transaction-cleanup-committed-bucket";
        let object = "object.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(b"cleanup pending local commit record cleanup".repeat(1024));
        let original = store
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name.to_string(),
                etag: original.etag.clone().expect("source object should have etag"),
                ..Default::default()
            },
            mod_time: original.mod_time,
            ..Default::default()
        };
        store
            .transition_object(bucket, object, &opts)
            .await
            .expect("transition should commit");
        let committed = store
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
            )
            .await
            .expect("committed object info should be readable");
        let mut remote_parts = committed.transitioned_object.name.rsplit('/');
        let write_id = uuid::Uuid::parse_str(remote_parts.next().expect("remote object should contain write id"))
            .expect("write id should parse");
        let transaction_id = uuid::Uuid::parse_str(remote_parts.next().expect("remote object should contain transaction id"))
            .expect("transaction id should parse");
        let source = TransitionSourceIdentity {
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: None,
            data_dir: original.data_dir.expect("source object should have data_dir"),
            mod_time_unix_nanos: original
                .mod_time
                .expect("source object should have mod_time")
                .unix_timestamp_nanos()
                .try_into()
                .expect("test timestamp should fit i64"),
            size: original.size,
            etag: original.etag.expect("source object should have etag"),
            version_mode: TransitionSourceVersionMode::Unversioned,
        };
        let mut transaction = TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
            transaction_id,
            owner_epoch: uuid::Uuid::new_v4(),
            write_id,
            source,
            tier_name: tier_name.to_string(),
            backend_fingerprint: backend_identity,
            not_after_unix_nanos: 1_780_000_000_000_000_000,
        })
        .expect("transaction should build");
        transaction
            .advance(
                transaction.fence(),
                TransitionTransactionState::Uploaded,
                Some(TransitionRemoteVersion::known_from_put_response(
                    committed.transitioned_object.version_id.clone(),
                )),
            )
            .expect("transaction should enter uploaded state");
        let local_commit_fence = transaction
            .advance(transaction.fence(), TransitionTransactionState::LocalCommitStarted, None)
            .expect("transaction should enter local commit state");
        transaction
            .mark_cleanup_pending(
                local_commit_fence,
                TransitionCleanupProof {
                    transaction_id: transaction.transaction_id,
                    write_id: transaction.write_id,
                    remote_object: transaction.remote_object.clone(),
                    remote_version: transaction.remote_version.clone(),
                    backend_fingerprint: transaction.backend_fingerprint,
                    decision: TransitionCleanupDecision::SourceReconciledUnchanged {
                        observed_source: transaction.source.clone(),
                    },
                },
            )
            .expect("transaction should enter cleanup pending state");
        save_transition_transaction_record(store.clone(), &transaction)
            .await
            .expect("transaction record should persist");

        let stats = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("transition transaction recovery should run");

        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (1, 1, 0, 0));
        assert_eq!(transition_transaction_record_count(store.clone()).await, 0);
        assert_eq!(
            backend.object_count().await,
            1,
            "cleanup pending recovery must keep the remote body once local metadata references it"
        );
        assert_eq!(backend.remove_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_drops_record_after_confirmed_local_commit() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transition-transaction-committed", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXCOMMITTED";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let bucket = "transition-transaction-committed-bucket";
        let object = "object.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        let mut reader = PutObjReader::from_vec(b"confirmed local commit record cleanup".repeat(1024));
        let original = store
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name.to_string(),
                etag: original.etag.clone().expect("source object should have etag"),
                ..Default::default()
            },
            mod_time: original.mod_time,
            ..Default::default()
        };
        store
            .transition_object(bucket, object, &opts)
            .await
            .expect("transition should commit");
        let committed = store
            .get_object_info(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
            )
            .await
            .expect("committed object info should be readable");
        let mut remote_parts = committed.transitioned_object.name.rsplit('/');
        let write_id = uuid::Uuid::parse_str(remote_parts.next().expect("remote object should contain write id"))
            .expect("write id should parse");
        let transaction_id = uuid::Uuid::parse_str(remote_parts.next().expect("remote object should contain transaction id"))
            .expect("transaction id should parse");
        let mut transaction = TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
            transaction_id,
            owner_epoch: uuid::Uuid::new_v4(),
            write_id,
            source: TransitionSourceIdentity {
                bucket: bucket.to_string(),
                object: object.to_string(),
                version_id: None,
                data_dir: original.data_dir.expect("source object should have data_dir"),
                mod_time_unix_nanos: original
                    .mod_time
                    .expect("source object should have mod_time")
                    .unix_timestamp_nanos()
                    .try_into()
                    .expect("test timestamp should fit i64"),
                size: original.size,
                etag: original.etag.expect("source object should have etag"),
                version_mode: TransitionSourceVersionMode::Unversioned,
            },
            tier_name: tier_name.to_string(),
            backend_fingerprint: backend_identity,
            not_after_unix_nanos: 1_780_000_000_000_000_000,
        })
        .expect("transaction should build");
        transaction
            .advance(
                transaction.fence(),
                TransitionTransactionState::Uploaded,
                Some(TransitionRemoteVersion::known_from_put_response(
                    committed.transitioned_object.version_id.clone(),
                )),
            )
            .expect("transaction should enter uploaded state");
        transaction
            .advance(transaction.fence(), TransitionTransactionState::LocalCommitStarted, None)
            .expect("transaction should enter local commit state");
        save_transition_transaction_record(store.clone(), &transaction)
            .await
            .expect("transaction record should persist");
        assert_eq!(transition_transaction_record_count(store.clone()).await, 1);

        let stats = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("transition transaction recovery should run");

        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (1, 1, 0, 0));
        assert_eq!(transition_transaction_record_count(store.clone()).await, 0);
        assert_eq!(
            backend.object_count().await,
            1,
            "confirmed local commit recovery must not delete the committed remote body"
        );
        assert_eq!(backend.remove_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_retains_unproven_remote_candidates() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transition-transaction-unproven", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXUNPROVEN";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let remote_version = uuid::Uuid::new_v4().to_string();
        let new_transaction = || {
            TransitionTransaction::new(TransitionTransactionInit {
                deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
                transaction_id: uuid::Uuid::new_v4(),
                owner_epoch: uuid::Uuid::new_v4(),
                write_id: uuid::Uuid::new_v4(),
                source: TransitionSourceIdentity {
                    bucket: "absent-source-bucket".to_string(),
                    object: "source-object".to_string(),
                    version_id: None,
                    data_dir: uuid::Uuid::new_v4(),
                    mod_time_unix_nanos: 1_770_000_000_000_000_000,
                    size: 42,
                    etag: "source-etag".to_string(),
                    version_mode: TransitionSourceVersionMode::Unversioned,
                },
                tier_name: tier_name.to_string(),
                backend_fingerprint: backend_identity,
                not_after_unix_nanos: 1_780_000_000_000_000_000,
            })
            .expect("transaction should build")
        };

        let upload_started = new_transaction();
        let mut local_commit_started = new_transaction();
        local_commit_started
            .advance(
                local_commit_started.fence(),
                TransitionTransactionState::Uploaded,
                Some(TransitionRemoteVersion::versioned(remote_version.clone())),
            )
            .expect("transaction should enter uploaded state");
        local_commit_started
            .advance(local_commit_started.fence(), TransitionTransactionState::LocalCommitStarted, None)
            .expect("transaction should enter local commit state");
        let upload_started_control_id =
            transition_recovery_control_id(&upload_started).expect("upload-started control id should derive");
        let local_commit_control_id =
            transition_recovery_control_id(&local_commit_started).expect("local-commit control id should derive");

        backend.set_put_remote_version(Some(remote_version)).await;
        for transaction in [&upload_started, &local_commit_started] {
            let candidate = bytes::Bytes::from_static(b"unproven transition remote candidate");
            backend
                .put(
                    &transaction.remote_object,
                    ReaderImpl::Body(candidate.clone()),
                    i64::try_from(candidate.len()).expect("test candidate length should fit i64"),
                )
                .await
                .expect("mock backend should accept candidate");
            save_transition_transaction_record(store.clone(), transaction)
                .await
                .expect("transaction record should persist");
        }

        let stats = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("transition transaction recovery should run");

        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (2, 0, 2, 0));
        assert_eq!(
            transition_transaction_record_count(store.clone()).await,
            2,
            "an upload without completion proof or unproven local commit must remain for authoritative reconcile"
        );
        assert_eq!(backend.object_count().await, 2, "recovery must not delete an unproven remote candidate");
        assert_eq!(backend.remove_count().await, 0);
        assert_eq!(backend.exact_remove_count(), 0);
        let upload_started_control =
            load_recovery_control(store.clone(), IlmRecoveryProtocol::TransitionTransaction, &upload_started_control_id)
                .await
                .expect("upload-started control should persist");
        assert_eq!(
            upload_started_control.control.classification,
            IlmRecoveryClassification::RetainedAmbiguous
        );
        let local_commit_control =
            load_recovery_control(store.clone(), IlmRecoveryProtocol::TransitionTransaction, &local_commit_control_id)
                .await
                .expect("local-commit control should persist");
        assert_eq!(local_commit_control.control.classification, IlmRecoveryClassification::OperatorRequired);

        let upload_status = inspect_transition_recovery_retry_for_operator(store.clone(), &upload_started_control_id)
            .await
            .expect("retained upload should be inspectable for a bounded retry");
        let local_status = inspect_transition_recovery_retry_for_operator(store.clone(), &local_commit_control_id)
            .await
            .expect("operator-required local commit should be inspectable for a bounded retry");
        assert!(upload_status.retry_ready);
        assert!(local_status.retry_ready);
        assert!(matches!(
            retry_transition_recovery_for_operator(
                store.clone(),
                &upload_started_control_id,
                upload_status.control_revision + 1,
                &upload_status.source_generation_sha256,
            )
            .await,
            Err(TransitionOperatorError::StaleRecoveryControl)
        ));

        let put_count_before_retry = backend.put_count().await;
        let get_count_before_retry = backend.get_count().await;
        let remove_count_before_retry = backend.remove_count().await;
        let upload_retry = retry_transition_recovery_for_operator(
            store.clone(),
            &upload_started_control_id,
            upload_status.control_revision,
            &upload_status.source_generation_sha256,
        )
        .await
        .expect("exact retained upload generation should be rearmed");
        let local_retry = retry_transition_recovery_for_operator(
            store.clone(),
            &local_commit_control_id,
            local_status.control_revision,
            &local_status.source_generation_sha256,
        )
        .await
        .expect("exact operator-required local commit generation should be rearmed");
        assert_eq!(upload_retry.classification, IlmRecoveryClassification::Retrying);
        assert_eq!(local_retry.classification, IlmRecoveryClassification::Retrying);
        assert_eq!(upload_retry.attempt_count, upload_status.attempt_count);
        assert_eq!(local_retry.attempt_count, local_status.attempt_count);
        assert_eq!(backend.put_count().await, put_count_before_retry);
        assert_eq!(backend.get_count().await, get_count_before_retry);
        assert_eq!(backend.remove_count().await, remove_count_before_retry);
        assert_eq!(backend.exact_remove_count(), 0, "operator retry must not directly issue remote DELETE");

        let retried = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("rearmed records should be re-evaluated through normal recovery");
        assert_eq!((retried.scanned, retried.recovered, retried.retained, retried.failed), (2, 0, 2, 0));
        let upload_retained =
            load_recovery_control(store.clone(), IlmRecoveryProtocol::TransitionTransaction, &upload_started_control_id)
                .await
                .expect("upload retry result should persist");
        let local_retained = load_recovery_control(store, IlmRecoveryProtocol::TransitionTransaction, &local_commit_control_id)
            .await
            .expect("local commit retry result should persist");
        assert_eq!(upload_retained.control.classification, IlmRecoveryClassification::RetainedAmbiguous);
        assert_eq!(local_retained.control.classification, IlmRecoveryClassification::OperatorRequired);
        assert_eq!(upload_retained.control.attempt_count, upload_status.attempt_count + 1);
        assert_eq!(local_retained.control.attempt_count, local_status.attempt_count + 1);
        assert_eq!(backend.put_count().await, put_count_before_retry);
        assert_eq!(backend.get_count().await, get_count_before_retry);
        assert_eq!(backend.remove_count().await, remove_count_before_retry);
        assert_eq!(backend.exact_remove_count(), 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_handles_provider_recovered_unknown_upload() {
        let versioned_remote = uuid::Uuid::new_v4().to_string();
        let nil_remote = uuid::Uuid::nil().to_string();
        for (case, tier_name, remote_version, should_recover) in [
            ("missing", "TXPROBEMISSING", None, true),
            ("unversioned", "TXPROBEUNVERSIONED", Some(String::new()), true),
            ("versioned", "TXPROBEVERSIONED", Some(versioned_remote), true),
            ("nil-version", "TXPROBENILVERSION", Some(nil_remote), false),
        ] {
            let temp_dir = tempfile::tempdir().expect("create temp store dir");
            let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
                temp_dir.path(),
                &format!("transition-transaction-probe-{case}"),
                &[4],
            ))
            .await;
            crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

            let backend = register_transition_reconcile_test_tier(&ctx.tier_config_mgr(), tier_name).await;
            let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
                .await
                .expect("tier lease should resolve")
                .backend_identity();
            let mut transaction = TransitionTransaction::new(TransitionTransactionInit {
                deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
                transaction_id: uuid::Uuid::new_v4(),
                owner_epoch: uuid::Uuid::new_v4(),
                write_id: uuid::Uuid::new_v4(),
                source: TransitionSourceIdentity {
                    bucket: "source-bucket".to_string(),
                    object: "source-object".to_string(),
                    version_id: None,
                    data_dir: uuid::Uuid::new_v4(),
                    mod_time_unix_nanos: 1_770_000_000_000_000_000,
                    size: 42,
                    etag: "source-etag".to_string(),
                    version_mode: TransitionSourceVersionMode::Unversioned,
                },
                tier_name: tier_name.to_string(),
                backend_fingerprint: backend_identity,
                not_after_unix_nanos: 1,
            })
            .expect("transaction should build");
            transaction
                .advance(transaction.fence(), TransitionTransactionState::UploadOutcomeUnknown, None)
                .expect("transaction should enter unknown upload outcome state");

            if let Some(version) = &remote_version {
                backend.set_put_remote_version(Some(version.clone())).await;
                let candidate = bytes::Bytes::from_static(b"provider-recovered transition remote candidate");
                backend
                    .put(
                        &transaction.remote_object,
                        ReaderImpl::Body(candidate.clone()),
                        i64::try_from(candidate.len()).expect("test candidate length should fit i64"),
                    )
                    .await
                    .expect("mock backend should accept candidate");
            }
            save_transition_transaction_record(store.clone(), &transaction)
                .await
                .expect("transaction record should persist");

            let stats = recover_transition_transaction_records(store.clone(), 100, None)
                .await
                .expect("transition transaction recovery should run");

            assert_eq!(
                (stats.scanned, stats.recovered, stats.retained, stats.failed),
                if should_recover { (1, 1, 0, 0) } else { (1, 0, 1, 0) }
            );
            assert_eq!(transition_transaction_record_count(store.clone()).await, usize::from(!should_recover));
            assert_eq!(
                backend.object_count().await,
                usize::from(!should_recover),
                "case {case}: only a valid provider version state may be recovered destructively"
            );
            let removed = remote_version
                .filter(|_| should_recover)
                .map(|version| vec![(transaction.remote_object.clone(), version)])
                .unwrap_or_default();
            assert_eq!(
                backend.remove_versions().await,
                removed,
                "case {case}: recovery must delete only provider-recovered candidates"
            );
            assert_eq!(
                backend.exact_remove_count(),
                usize::from(removed.first().is_some_and(|(_, version)| !version.is_empty()))
            );
        }
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn operator_reconcile_deletes_exact_candidate_before_finalizing_record() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transition-operator-reconcile", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXOPERATOR";
        let backend = register_transition_reconcile_test_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let mut transaction = TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
            transaction_id: uuid::Uuid::new_v4(),
            owner_epoch: uuid::Uuid::new_v4(),
            write_id: uuid::Uuid::new_v4(),
            source: TransitionSourceIdentity {
                bucket: "source-bucket".to_string(),
                object: "source-object".to_string(),
                version_id: None,
                data_dir: uuid::Uuid::new_v4(),
                mod_time_unix_nanos: 1_770_000_000_000_000_000,
                size: 42,
                etag: "source-etag".to_string(),
                version_mode: TransitionSourceVersionMode::Unversioned,
            },
            tier_name: tier_name.to_string(),
            backend_fingerprint: backend_identity,
            not_after_unix_nanos: 1,
        })
        .expect("transaction should build");
        transaction
            .advance(transaction.fence(), TransitionTransactionState::UploadOutcomeUnknown, None)
            .expect("transaction should enter unknown upload outcome state");

        let remote_version = uuid::Uuid::new_v4().to_string();
        backend.set_put_remote_version(Some(remote_version.clone())).await;
        let candidate = bytes::Bytes::from_static(b"operator-confirmed transition candidate");
        backend
            .put(
                &transaction.remote_object,
                ReaderImpl::Body(candidate.clone()),
                i64::try_from(candidate.len()).expect("test candidate length should fit i64"),
            )
            .await
            .expect("mock backend should accept candidate");
        save_transition_transaction_record(store.clone(), &transaction)
            .await
            .expect("transaction record should persist");

        let status = inspect_transition_transaction_for_operator(store.clone(), transaction.transaction_id)
            .await
            .expect("operator inspection should probe the candidate");
        assert_eq!(status.probe, TransitionOperatorProbe::VersionedPresent(remote_version.clone()));

        let wrong_version = uuid::Uuid::new_v4().to_string();
        let err = delete_transition_candidate_for_operator(store.clone(), transaction.transaction_id, &wrong_version)
            .await
            .expect_err("a mismatched exact version must fail before deleting a candidate");
        assert!(matches!(
            err,
            TransitionOperatorError::CandidateVersionMismatch {
                expected,
                actual: TransitionOperatorProbe::VersionedPresent(ref observed),
            } if expected == wrong_version && observed == &remote_version
        ));
        assert!(backend.contains(&transaction.remote_object).await);
        assert_eq!(backend.exact_remove_count(), 0);
        load_transition_transaction_record(store.clone(), transaction.transaction_id)
            .await
            .expect("an incorrect exact version must retain the transaction journal");

        let result = delete_transition_candidate_for_operator(store.clone(), transaction.transaction_id, &remote_version)
            .await
            .expect("operator-confirmed exact candidate should be deleted");
        assert_eq!(result.status.probe, TransitionOperatorProbe::Missing);
        assert!(result.journal_observed_after_delete);
        assert_eq!(backend.exact_remove_count(), 1);
        assert_eq!(backend.remove_versions().await, vec![(transaction.remote_object.clone(), remote_version)]);
        load_transition_transaction_record(store.clone(), transaction.transaction_id)
            .await
            .expect("candidate deletion must retain the transaction journal");

        finalize_missing_transition_transaction_for_operator(store.clone(), transaction.transaction_id)
            .await
            .expect("a separately confirmed missing candidate should permit finalization");
        assert!(matches!(
            load_transition_transaction_record(store, transaction.transaction_id).await,
            Err(Error::ConfigNotFound)
        ));
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn operator_finalize_retains_record_without_missing_proof() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transition-operator-fail-closed", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXOPERATORFAIL";
        let backend = register_transition_reconcile_test_tier(&ctx.tier_config_mgr(), tier_name).await;
        let backend_identity = TierConfigMgr::acquire_operation_lease(&ctx.tier_config_mgr(), tier_name)
            .await
            .expect("tier lease should resolve")
            .backend_identity();
        let mut transaction = TransitionTransaction::new(TransitionTransactionInit {
            deployment_id: ctx.deployment_id().expect("test store should initialize deployment id"),
            transaction_id: uuid::Uuid::new_v4(),
            owner_epoch: uuid::Uuid::new_v4(),
            write_id: uuid::Uuid::new_v4(),
            source: TransitionSourceIdentity {
                bucket: "source-bucket".to_string(),
                object: "source-object".to_string(),
                version_id: None,
                data_dir: uuid::Uuid::new_v4(),
                mod_time_unix_nanos: 1_770_000_000_000_000_000,
                size: 42,
                etag: "source-etag".to_string(),
                version_mode: TransitionSourceVersionMode::Unversioned,
            },
            tier_name: tier_name.to_string(),
            backend_fingerprint: backend_identity,
            not_after_unix_nanos: 1,
        })
        .expect("transaction should build");
        transaction
            .advance(transaction.fence(), TransitionTransactionState::UploadOutcomeUnknown, None)
            .expect("transaction should enter unknown upload outcome state");
        save_transition_transaction_record(store.clone(), &transaction)
            .await
            .expect("transaction record should persist");
        backend
            .set_transition_candidate_probe_override(Some(TransitionCandidateProbe::Unsupported))
            .await;

        assert!(matches!(
            finalize_missing_transition_transaction_for_operator(store.clone(), transaction.transaction_id).await,
            Err(TransitionOperatorError::CandidateNotMissing(TransitionOperatorProbe::Unsupported))
        ));
        load_transition_transaction_record(store, transaction.transaction_id)
            .await
            .expect("an unsupported probe must retain the transaction journal");
        assert_eq!(backend.remove_count().await, 0);
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_response_loss_persists_unknown_outcome_for_provider_recovery() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transition-response-loss", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXRESPONSELOSS";
        let backend = register_transition_reconcile_test_tier(&ctx.tier_config_mgr(), tier_name).await;
        let bucket = "transition-response-loss-bucket";
        let object = "source.bin";
        store
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("source bucket should be created");
        let payload = b"a response-lost tier PUT must remain recoverable".repeat(1024);
        let mut reader = PutObjReader::from_vec(payload.clone());
        let source = store
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        backend.lose_next_put_response();

        let error = store
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name.to_string(),
                        etag: source.etag.clone().expect("source object should have an ETag"),
                        ..Default::default()
                    },
                    version_id: source.version_id.map(|version| version.to_string()),
                    mod_time: source.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect_err("a lost tier PUT response must fail the transition request");
        assert!(
            matches!(error, StorageError::Io(ref err) if err.kind() == std::io::ErrorKind::ConnectionReset),
            "the response-loss error must remain visible to the caller: {error:?}"
        );

        let records = store
            .clone()
            .list_objects_v2(
                RUSTFS_META_BUCKET,
                TRANSITION_TRANSACTION_RECORD_PREFIX,
                None,
                None,
                10,
                false,
                None,
                false,
            )
            .await
            .expect("transition transaction records should be listable");
        assert_eq!(records.objects.len(), 1, "response loss must leave one durable transaction record");
        let transaction_id = records.objects[0]
            .name
            .rsplit('/')
            .next()
            .and_then(|name| name.strip_suffix(".json"))
            .and_then(|name| uuid::Uuid::parse_str(name).ok())
            .expect("transaction record name should contain a UUID");
        let transaction = load_transition_transaction_record(store.clone(), transaction_id)
            .await
            .expect("response loss transaction record should load");
        assert_eq!(
            transaction.state,
            TransitionTransactionState::UploadOutcomeUnknown,
            "a response-lost PUT must not remain in UploadStarted"
        );
        assert!(
            backend.contains(&transaction.remote_object).await,
            "the test backend must retain the remote candidate"
        );

        backend
            .set_transition_candidate_probe_override(Some(TransitionCandidateProbe::Unsupported))
            .await;
        let unsupported_stats = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("active unknown ownership should remain fenced before provider recovery");
        assert_eq!(
            (
                unsupported_stats.scanned,
                unsupported_stats.recovered,
                unsupported_stats.retained,
                unsupported_stats.failed
            ),
            (1, 0, 1, 0),
            "active unknown ownership must retain the upload before the recovery deadline"
        );
        assert_eq!(transition_transaction_record_count(store.clone()).await, 1);
        let recovery_control_id =
            transition_recovery_control_id(&transaction).expect("transition recovery control id should derive");
        assert!(matches!(
            load_recovery_control(store.clone(), IlmRecoveryProtocol::TransitionTransaction, &recovery_control_id).await,
            Err(Error::ConfigNotFound)
        ));
        assert!(
            backend.contains(&transaction.remote_object).await,
            "active ownership must not delete the candidate"
        );
        assert_eq!(backend.remove_count().await, 0, "active ownership must not attempt cleanup");
        assert!(
            !backend
                .op_log()
                .await
                .iter()
                .any(|operation| matches!(operation, MockWarmOp::Probe { .. })),
            "active ownership must not probe the provider"
        );

        backend.set_transition_candidate_probe_override(None).await;
        let stats =
            recover_transition_transaction_records_at(store.clone(), 100, None, i128::from(transaction.not_after_unix_nanos) + 1)
                .await
                .expect("provider-authoritative recovery should run");
        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (1, 1, 0, 0));
        assert_eq!(transition_transaction_record_count(store.clone()).await, 0);
        assert_eq!(backend.object_count().await, 0, "recovery must delete the provider-confirmed candidate");
        let op_log = backend.op_log().await;
        assert!(
            op_log.iter().any(|operation| matches!(operation, MockWarmOp::Probe { .. })),
            "response-loss recovery must enter the provider probe branch"
        );
        assert!(
            op_log.iter().any(|operation| matches!(operation, MockWarmOp::Put { .. })),
            "response-loss fixture must record that the remote PUT reached the backend"
        );
        let source_after = store
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("recovery must preserve the local source object");
        assert_eq!(source_after.size, i64::try_from(payload.len()).expect("payload length should fit i64"));
    }
}
