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
use crate::core::pools::local_decommission_queue_prefix;
use crate::error::is_err_decommission_running;
use crate::runtime::instance::InstanceContext;
use crate::runtime::sources as runtime_sources;
use crate::storage_api_contracts::object::EcstoreObjectIO;
use rustfs_config::server_config::KVS;
use rustfs_credentials::{RPC_SECRET_REQUIRED_OPERATOR_MESSAGE, try_get_rpc_token};
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_STORE_INIT: &str = "store_init";
const EVENT_DECOMMISSION_RESUME_RETRY: &str = "decommission_resume_retry";
const EVENT_DECOMMISSION_RESUME_FAILED: &str = "decommission_resume_failed";
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

const LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES: usize = 6;
const LOCAL_DECOMMISSION_INITIAL_RESUME_DELAY: Duration = Duration::from_secs(60 * 3);
const LOCAL_DECOMMISSION_RESUME_RETRY_DELAY: Duration = Duration::from_secs(30);

fn should_retry_local_decommission_resume(err: &Error, attempt: usize) -> bool {
    matches!(err, Error::ConfigNotFound) && attempt < LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES
}

fn should_retry_format_load(err: &Error) -> bool {
    !matches!(err, Error::CorruptedFormat)
}

fn should_auto_start_rebalance_after_init(decommission_running: bool, rebalance_meta_loaded: bool) -> bool {
    rebalance_meta_loaded && !decommission_running
}

fn pool_meta_has_active_decommission(meta: &PoolMeta) -> bool {
    meta.pools.iter().any(|pool| {
        pool.decommission
            .as_ref()
            .is_some_and(|info| info.has_decommission_state() && !info.complete && !info.failed && !info.canceled)
    })
}

async fn wait_for_local_decommission_resume_delay(rx: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        _ = rx.cancelled() => false,
        _ = tokio::time::sleep(delay) => true,
    }
}

fn resolve_store_init_stage_result(result: Result<()>, stage: &str) -> Result<()> {
    result.map_err(|err| Error::other(format!("store init failed during {stage}: {err}")))
}

async fn load_pool_meta_for_startup<S>(pool: Arc<S>) -> Result<PoolMeta>
where
    S: EcstoreObjectIO,
{
    let mut meta = PoolMeta::default();
    resolve_store_init_stage_result(meta.load_for_startup(pool).await, "load_pool_meta")?;
    Ok(meta)
}

async fn save_validated_pool_meta_for_startup<S>(meta: &PoolMeta, pools: Vec<Arc<S>>) -> Result<()>
where
    S: EcstoreObjectIO,
{
    resolve_store_init_stage_result(meta.save_for_startup(pools).await, "save_validated_pool_meta")
}

async fn resume_local_decommission_after_init(store: Arc<ECStore>, rx: CancellationToken, pool_indices: Vec<usize>) {
    for attempt in 0..=LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES {
        if rx.is_cancelled() {
            return;
        }

        let result = if pool_indices.len() > 1 {
            store
                .spawn_decommission_routines(store.clone(), rx.clone(), pool_indices.clone())
                .await
        } else {
            store.decommission(rx.clone(), pool_indices.clone()).await
        };

        match result {
            Ok(()) => return,
            Err(err) if is_err_decommission_running(&err) => {
                if let Err(spawn_err) = store
                    .spawn_decommission_routines(store.clone(), rx.clone(), pool_indices.clone())
                    .await
                {
                    error!(
                        event = EVENT_DECOMMISSION_RESUME_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_STORE_INIT,
                        pool_indices = ?pool_indices,
                        error = %spawn_err,
                        reason = "spawn_workers_failed",
                        "Failed to resume decommission workers"
                    );
                }
                return;
            }
            Err(err) if should_retry_local_decommission_resume(&err, attempt) => {
                warn!(
                    event = EVENT_DECOMMISSION_RESUME_RETRY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_STORE_INIT,
                    pool_indices = ?pool_indices,
                    retry_count = attempt + 1,
                    retry_limit = LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES + 1,
                    error = %err,
                    "Retrying decommission resume after missing config"
                );
                tokio::select! {
                    _ = rx.cancelled() => return,
                    _ = tokio::time::sleep(LOCAL_DECOMMISSION_RESUME_RETRY_DELAY) => {}
                }
            }
            Err(err) => {
                error!(
                    event = EVENT_DECOMMISSION_RESUME_FAILED,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_STORE_INIT,
                    pool_indices = ?pool_indices,
                    error = %err,
                    reason = "resume_failed",
                    "Failed to resume decommission"
                );
                return;
            }
        }
    }
}

impl ECStore {
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
        instance_ctx.bind_background_cancel_token(ctx.clone());

        // let layouts = DisksLayout::from_volumes(endpoints.as_slice())?;

        // Validate topology and environment overrides before opening any disk.
        // The values stored on SetDisks remain pure per-pool topology defaults;
        // payload writes use the runtime storage-class snapshot published later
        // from config before the store is marked ready.
        let default_pool_parities = resolve_startup_pool_defaults(&endpoint_pools)?;

        preflight_startup_rpc_secret(&endpoint_pools)?;

        let mut deployment_id = None;

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

            let fm = {
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

        let peer_sys = S3PeerSys::new_with_instance_ctx(&endpoint_pools, instance_ctx.clone());
        let mut pool_meta = PoolMeta::new(&pools, &PoolMeta::default());
        pool_meta.dont_save = true;

        let decommission_cancelers = RwLock::new(vec![None; pools.len()]);
        let ec = Arc::new(ECStore {
            id: deployment_id.ok_or_else(|| Error::other("store init failed: deployment id is not initialized"))?,
            disk_map,
            pools,
            peer_sys,
            pool_meta: RwLock::new(pool_meta),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers,
            start_gate: Mutex::new(()),
            pool_meta_save_gate: Mutex::new(()),
            // Adopt the caller's context (the process bootstrap one on the
            // legacy path) so startup writes (erasure type recorded before
            // this point) and later reads share one cell.
            ctx: instance_ctx.clone(),
            bucket_fence_registry: std::sync::Arc::default(),
        });

        // Only set it when this instance's deployment ID is not yet configured
        if let Some(dep_id) = deployment_id
            && instance_ctx.deployment_id().is_none()
        {
            instance_ctx.set_deployment_id(dep_id);
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

        let meta = load_pool_meta_for_startup(
            self.pools
                .first()
                .cloned()
                .ok_or_else(|| Error::other("store init failed: no storage pools available"))?,
        )
        .await?;
        let update = meta.validate(self.pools.clone())?;
        let endpoints = runtime_sources::endpoint_pools_or_default();
        let should_persist_pool_meta = runtime_sources::first_cluster_node_is_local().await;

        let installed_pool_meta = if !update {
            meta.clone()
        } else {
            let new_meta = PoolMeta::new(&self.pools, &meta);
            // Only one local node should persist validated pool metadata here; otherwise
            // distributed startup can race on the same lock and replay the prior init bug.
            if should_persist_pool_meta {
                save_validated_pool_meta_for_startup(&new_meta, self.pools.clone()).await?;
            }
            new_meta
        };

        {
            let mut pool_meta = self.pool_meta.write().await;
            *pool_meta = installed_pool_meta.clone();
        }

        resolve_store_init_stage_result(self.load_rebalance_meta().await, "load_rebalance_meta")?;
        let rebalance_meta_loaded = self.rebalance_meta.read().await.is_some();
        let decommission_running =
            pool_meta_has_active_decommission(&installed_pool_meta) || self.is_decommission_running().await;
        if should_auto_start_rebalance_after_init(decommission_running, rebalance_meta_loaded) {
            resolve_store_init_stage_result(self.start_rebalance().await, "start_rebalance")?;
        } else if decommission_running && rebalance_meta_loaded {
            warn!(
                event = EVENT_ECSTORE_INIT_STATUS,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_STORE_INIT,
                stage = "start_rebalance",
                reason = "active_decommission",
                "Deferred rebalance auto-start during store init because decommission is active"
            );
        }

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
        if !local_pool_indices.is_empty() {
            let store = self.clone();

            tokio::spawn(async move {
                if !wait_for_local_decommission_resume_delay(&rx, LOCAL_DECOMMISSION_INITIAL_RESUME_DELAY).await {
                    return;
                }
                resume_local_decommission_after_init(store, rx, local_pool_indices).await;
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

        Ok(())
    }

    pub fn init_local_disks() {}

    pub fn single_pool(&self) -> bool {
        self.pools.len() == 1
    }
}

#[cfg(test)]
mod tests {
    use super::{
        LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES, load_pool_meta_for_startup, pool_first_endpoint_is_local,
        pool_meta_has_active_decommission, preflight_startup_rpc_secret_with, resolve_startup_pool_defaults_with,
        resolve_store_init_stage_result, save_validated_pool_meta_for_startup, should_auto_start_rebalance_after_init,
        should_retry_format_load, should_retry_local_decommission_resume, wait_for_local_decommission_resume_delay,
    };
    #[cfg(feature = "test-util")]
    use crate::{
        bucket::lifecycle::{
            lifecycle::{TRANSITION_PENDING, TransitionOptions},
            tier_delete_journal::{
                TIER_DELETE_JOURNAL_PREFIX, persist_tier_delete_journal_entry, recover_tier_delete_journal_entries,
            },
            tier_sweeper::Jentry,
            transition_transaction::{
                TRANSITION_TRANSACTION_RECORD_PREFIX, TransitionCleanupDecision, TransitionCleanupProof, TransitionOperatorError,
                TransitionOperatorProbe, TransitionRemoteVersion, TransitionSourceIdentity, TransitionSourceVersionMode,
                TransitionTransaction, TransitionTransactionInit, TransitionTransactionState,
                delete_transition_candidate_for_operator, finalize_missing_transition_transaction_for_operator,
                inspect_transition_transaction_for_operator, load_transition_transaction_record,
                recover_transition_transaction_records, save_transition_transaction_record,
            },
        },
        client::transition_api::ReaderImpl,
        config::com,
        disk::RUSTFS_META_BUCKET,
        runtime::{global::set_object_store_resolver, sources as runtime_sources},
        services::tier::{
            test_util::{MockWarmBackend, MockWarmOp, TransitionCleanupStoreBarrier, register_mock_tier},
            tier::{TIER_CONFIG_FILE, TierConfigMgr},
            tier_config::{TierConfig, TierType, TierWasabi},
            tier_mutation_intent::{
                TIER_MUTATION_INTENT_RECORD_PREFIX, TierMutationIntent, TierMutationIntentKind, TierMutationIntentState,
                TierMutationIntentTarget, advance_tier_mutation_intent_record_idempotent, delete_tier_mutation_intent_record,
                list_tier_mutation_intent_records, load_tier_mutation_intent_record, load_tier_mutation_intent_record_with_etag,
                save_tier_mutation_intent_record, save_tier_mutation_intent_record_if_current,
            },
            tier_mutation_peer::{TierMutationPeerError, TierMutationPeerState, handle_tier_mutation_peer_request},
            warm_backend::{TransitionCandidateProbe, WarmBackend},
        },
        storage_api_contracts::list::ListOperations as _,
    };
    use crate::{
        bucket::replication::{ReplicationState, ReplicationStatusType, replication_statuses_map},
        core::pools::{POOL_META_VERSION, PoolDecommissionInfo, PoolMeta, PoolStatus},
        disk::endpoint::Endpoint,
        error::{Error, Result, StorageError},
        io_support::rio::{WritePlan, compression_metadata_value},
        layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
        object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader},
        services::rebalance::{RebalStatus, RebalanceInfo, RebalanceMeta, RebalanceStats},
        storage_api_contracts::{
            bucket::{BucketOperations as _, MakeBucketOptions},
            multipart::MultipartOperations as _,
            object::{ObjectIO, ObjectOperations as _},
            range::HTTPRangeSpec,
        },
    };
    use http::HeaderMap;
    use rustfs_config::server_config::KVS;
    use rustfs_filemeta::ObjectPartInfo;
    #[cfg(feature = "test-util")]
    use rustfs_protos::{TIER_MUTATION_RPC_PROTOCOL_VERSION, TierMutationRpcPhase};
    use rustfs_rio::{Checksum, ChecksumType};
    use rustfs_utils::{
        CompressionAlgorithm,
        http::{SUFFIX_COMPRESSION, insert_str},
    };
    use std::{
        collections::HashMap,
        future::Future,
        io::Cursor,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use time::OffsetDateTime;
    use tokio::io::AsyncReadExt;
    use tokio_util::sync::CancellationToken;

    #[derive(Debug)]
    struct StartupPoolMetaStorage {
        read_payload: Vec<u8>,
        read_without_lock: AtomicBool,
        wrote_without_lock: AtomicBool,
        wrote_with_max_parity: AtomicBool,
    }

    impl StartupPoolMetaStorage {
        fn new(read_payload: Vec<u8>) -> Self {
            Self {
                read_payload,
                read_without_lock: AtomicBool::new(false),
                wrote_without_lock: AtomicBool::new(false),
                wrote_with_max_parity: AtomicBool::new(false),
            }
        }

        fn object_info(&self, bucket: &str, object: &str, size: usize) -> ObjectInfo {
            ObjectInfo {
                bucket: bucket.to_string(),
                name: object.to_string(),
                size: size as i64,
                actual_size: size as i64,
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

            Ok(GetObjectReader {
                stream: Box::new(Cursor::new(self.read_payload.clone())),
                object_info: self.object_info(bucket, object, self.read_payload.len()),
                buffered_body: None,
                body_source: Default::default(),
            })
        }

        async fn put_object(
            &self,
            bucket: &str,
            object: &str,
            _data: &mut PutObjReader,
            opts: &ObjectOptions,
        ) -> Result<ObjectInfo> {
            assert!(opts.no_lock, "store init pool metadata save must not require namespace locks");
            self.wrote_without_lock.store(true, Ordering::SeqCst);
            self.wrote_with_max_parity.store(opts.max_parity, Ordering::SeqCst);
            Ok(self.object_info(bucket, object, 0))
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
    async fn test_store_init_pool_meta_io_bypasses_namespace_lock_surface() {
        let storage = Arc::new(StartupPoolMetaStorage::new(Vec::new()));

        let loaded = load_pool_meta_for_startup(storage.clone())
            .await
            .expect("startup pool metadata load should tolerate missing metadata without locks");
        assert!(loaded.pools.is_empty());
        assert!(storage.read_without_lock.load(Ordering::SeqCst));

        let meta = PoolMeta {
            version: POOL_META_VERSION,
            pools: Vec::new(),
            dont_save: false,
        };
        save_validated_pool_meta_for_startup(&meta, vec![storage.clone()])
            .await
            .expect("startup pool metadata save should bypass locks");
        assert!(storage.wrote_without_lock.load(Ordering::SeqCst));
        assert!(storage.wrote_with_max_parity.load(Ordering::SeqCst));
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
    fn test_should_retry_local_decommission_resume_accepts_config_not_found_before_retry_limit() {
        assert!(should_retry_local_decommission_resume(&StorageError::ConfigNotFound, 0));
    }

    #[test]
    fn test_should_retry_local_decommission_resume_rejects_config_not_found_at_retry_limit() {
        assert!(!should_retry_local_decommission_resume(
            &StorageError::ConfigNotFound,
            LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES
        ));
    }

    #[test]
    fn test_should_retry_local_decommission_resume_rejects_non_config_errors() {
        assert!(!should_retry_local_decommission_resume(&StorageError::SlowDown, 0));
    }

    #[test]
    fn test_should_retry_format_load_rejects_permanent_corruption() {
        assert!(!should_retry_format_load(&StorageError::CorruptedFormat));
        assert!(should_retry_format_load(&StorageError::ErasureReadQuorum));
        assert!(should_retry_format_load(&StorageError::FirstDiskWait));
    }

    #[test]
    fn test_should_auto_start_rebalance_after_init_allows_loaded_rebalance_without_decommission() {
        assert!(should_auto_start_rebalance_after_init(false, true));
    }

    #[test]
    fn test_should_auto_start_rebalance_after_init_rejects_active_decommission() {
        assert!(!should_auto_start_rebalance_after_init(true, true));
    }

    #[test]
    fn test_should_auto_start_rebalance_after_init_rejects_missing_rebalance_meta() {
        assert!(!should_auto_start_rebalance_after_init(false, false));
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
        let rebalance_meta = Some(RebalanceMeta::default());

        assert!(!should_auto_start_rebalance_after_init(
            pool_meta_has_active_decommission(&pool_meta),
            rebalance_meta.is_some()
        ));
    }

    #[test]
    fn test_store_init_recovery_allows_rebalance_when_only_rebalance_metadata_exists() {
        let pool_meta = init_test_pool_meta(None);
        let rebalance_meta = Some(RebalanceMeta::default());

        assert!(should_auto_start_rebalance_after_init(
            pool_meta_has_active_decommission(&pool_meta),
            rebalance_meta.is_some()
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
        let mut pools = Vec::with_capacity(pool_drive_counts.len());
        for (pool_index, &drives_per_set) in pool_drive_counts.iter().enumerate() {
            let mut endpoints = Vec::with_capacity(drives_per_set);
            for disk_index in 0..drives_per_set {
                let path = temp_dir.join(format!("pool{pool_index}/disk{disk_index}"));
                tokio::fs::create_dir_all(&path).await.expect("create disk dir");
                let mut endpoint = Endpoint::try_from(path.to_str().expect("disk path should be utf-8")).expect("local endpoint");
                endpoint.set_pool_index(pool_index);
                endpoint.set_set_index(0);
                endpoint.set_disk_index(disk_index);
                endpoints.push(endpoint);
            }
            pools.push(PoolEndpoints {
                legacy: false,
                set_count: 1,
                drives_per_set,
                endpoints: Endpoints::from(endpoints),
                cmd_line: format!("{cmd_line}-pool-{pool_index}"),
                platform: "test".to_string(),
            });
        }
        let endpoint_pools = EndpointServerPools(pools);

        let instance_ctx = Arc::new(crate::runtime::instance::InstanceContext::new());
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

        (instance_ctx, store, shutdown)
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
                    let source_mod_time = OffsetDateTime::UNIX_EPOCH;
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
                    crate::data_movement::migrate_object(
                        store.clone(),
                        0,
                        bucket.clone(),
                        GetObjectReader {
                            stream: Box::new(Cursor::new(source_body.clone())),
                            object_info: ObjectInfo {
                                bucket: bucket.clone(),
                                name: object.to_string(),
                                size: i64::try_from(source_body.len()).expect("single source size should fit i64"),
                                actual_size: i64::try_from(source_body.len()).expect("single source size should fit i64"),
                                etag: Some("0123456789abcdef0123456789abcdef".to_string()),
                                mod_time: Some(source_mod_time),
                                ..Default::default()
                            },
                            buffered_body: None,
                            body_source: Default::default(),
                        },
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
                    let multipart_source_size =
                        i64::try_from(multipart_source_body.len()).expect("multipart source size should fit i64");
                    crate::data_movement::migrate_object(
                        store.clone(),
                        0,
                        bucket.clone(),
                        GetObjectReader {
                            stream: Box::new(Cursor::new(multipart_source_body)),
                            object_info: ObjectInfo {
                                bucket: bucket.clone(),
                                name: multipart_object.to_string(),
                                size: multipart_source_size,
                                actual_size: multipart_source_size,
                                etag: Some("source-multipart-etag-2".to_string()),
                                mod_time: Some(source_mod_time),
                                parts: Arc::new(vec![
                                    ObjectPartInfo {
                                        number: 1,
                                        size: first_part_size,
                                        actual_size: i64::try_from(first_part_size).expect("first part size should fit i64"),
                                        etag: "source-part-1".to_string(),
                                        ..Default::default()
                                    },
                                    ObjectPartInfo {
                                        number: 2,
                                        size: 1,
                                        actual_size: 1,
                                        etag: "source-part-2".to_string(),
                                        ..Default::default()
                                    },
                                ]),
                                ..Default::default()
                            },
                            buffered_body: None,
                            body_source: Default::default(),
                        },
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
                    retry_source_info.etag = Some("0123456789abcdef0123456789abcdef".to_string());
                    assert!(!retry_source_info.is_multipart());
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
                            (rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_WRITE, None::<&str>),
                            (rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_FLEET_CONFIRMED, None::<&str>),
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
                    .expect("compatible multipart migration retry should converge");
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

                    let source_reader = |name: &str, body: Vec<u8>, mod_time, metadata: HashMap<String, String>| {
                        let size = i64::try_from(body.len()).expect("source body size should fit i64");
                        GetObjectReader {
                            stream: Box::new(Cursor::new(body)),
                            object_info: ObjectInfo {
                                bucket: bucket.clone(),
                                name: name.to_string(),
                                version_id: Some(version_id),
                                size,
                                actual_size: size,
                                etag: Some(format!("{name}-multipart-etag-2")),
                                mod_time: Some(mod_time),
                                user_defined: Arc::new(metadata),
                                parts: Arc::new(vec![
                                    ObjectPartInfo {
                                        number: 1,
                                        size: first_part_size,
                                        actual_size: i64::try_from(first_part_size).expect("first part size should fit i64"),
                                        etag: format!("{name}-part-1"),
                                        ..Default::default()
                                    },
                                    ObjectPartInfo {
                                        number: 2,
                                        size: 1,
                                        actual_size: 1,
                                        etag: format!("{name}-part-2"),
                                        ..Default::default()
                                    },
                                ]),
                                ..Default::default()
                            },
                            buffered_body: None,
                            body_source: Default::default(),
                        }
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
                        ),
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
                        ),
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
                        source_reader(client_target, new_body.clone(), new_time, HashMap::new()),
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
                        ),
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
                        ),
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
                        ),
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
                        ),
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
                            source_reader(object, old_body.clone(), old_time, retained_metadata.clone()),
                            None,
                            "test_retained_target_seed",
                        )
                        .await
                        .expect("seed retained migrated target");
                        let err = crate::data_movement::migrate_object(
                            store.clone(),
                            0,
                            bucket.clone(),
                            source_reader(object, new_body.clone(), new_time, retained_metadata),
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
        for (pool_idx, version_id, body, mod_time) in [
            (0, source_version, b"source version".to_vec(), source_mod_time),
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
        *store.rebalance_meta.write().await = Some(active_rebalance_meta_for_pool(store.pools.len(), 0));

        let first_part_size = 5 * 1024 * 1024;
        let mut source_body = vec![b'a'; first_part_size];
        source_body.push(b'b');
        let source_size = i64::try_from(source_body.len()).expect("multipart source size should fit i64");
        let completion_barrier = crate::store::multipart::DataMovementMultipartCompletionBarrier::install(&bucket);
        let migration_store = store.clone();
        let migration_bucket = bucket.clone();
        let migration = tokio::spawn(async move {
            let object_bucket = migration_bucket.clone();
            crate::data_movement::migrate_object(
                migration_store,
                0,
                migration_bucket,
                GetObjectReader {
                    stream: Box::new(Cursor::new(source_body)),
                    object_info: ObjectInfo {
                        bucket: object_bucket,
                        name: object.to_string(),
                        version_id: Some(source_version),
                        size: source_size,
                        actual_size: source_size,
                        etag: Some("source-multipart-etag-2".to_string()),
                        mod_time: Some(source_mod_time),
                        parts: Arc::new(vec![
                            ObjectPartInfo {
                                number: 1,
                                size: first_part_size,
                                actual_size: i64::try_from(first_part_size).expect("first part size should fit i64"),
                                etag: "source-part-1".to_string(),
                                ..Default::default()
                            },
                            ObjectPartInfo {
                                number: 2,
                                size: 1,
                                actual_size: 1,
                                etag: "source-part-2".to_string(),
                                ..Default::default()
                            },
                        ]),
                        ..Default::default()
                    },
                    buffered_body: None,
                    body_source: Default::default(),
                },
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
    async fn tier_delete_journal_count(store: Arc<crate::store::ECStore>) -> usize {
        store
            .list_objects_v2(RUSTFS_META_BUCKET, TIER_DELETE_JOURNAL_PREFIX, None, None, 100, false, None, false)
            .await
            .expect("tier delete journal should be listable")
            .objects
            .len()
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
    async fn tier_delete_journal_recovery_spawns_for_each_store() {
        let temp_a = tempfile::tempdir().expect("create temp store dir a");
        let temp_b = tempfile::tempdir().expect("create temp store dir b");
        let (ctx_a, store_a, shutdown_a) =
            without_storage_class_env(build_isolated_test_store(temp_a.path(), "tier-journal-recovery-a", &[4])).await;
        let (ctx_b, store_b, shutdown_b) =
            without_storage_class_env(build_isolated_test_store(temp_b.path(), "tier-journal-recovery-b", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store_a.clone(), Vec::new()).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store_b.clone(), Vec::new()).await;

        assert!(
            !ctx_a.mark_tier_delete_journal_recovery_started(store_a.id),
            "store A should have claimed its production recovery worker"
        );
        assert!(
            !ctx_b.mark_tier_delete_journal_recovery_started(store_b.id),
            "store B should have claimed its production recovery worker"
        );
        assert!(!shutdown_a.is_cancelled());
        assert!(!shutdown_b.is_cancelled());

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
            obj_name: "remote-a".to_string(),
            version_id: "version-a".to_string(),
            tier_name: tier_a.to_string(),
            backend_identity: Some(identity_a),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: crate::bucket::lifecycle::tier_sweeper::TierDeleteJournalState::Committed,
            source: None,
        };
        let entry_b = Jentry {
            obj_name: "remote-b".to_string(),
            version_id: "version-b".to_string(),
            tier_name: tier_b.to_string(),
            backend_identity: Some(identity_b),
            version_id_exact: true,
            version_state: rustfs_filemeta::TransitionVersionState::Exact,
            state: crate::bucket::lifecycle::tier_sweeper::TierDeleteJournalState::Committed,
            source: None,
        };
        let remove_a = backend_a.arm_failing_remove_barrier().await;
        persist_tier_delete_journal_entry(store_a.clone(), &entry_a)
            .await
            .expect("store A journal should persist");
        persist_tier_delete_journal_entry(store_b.clone(), &entry_b)
            .await
            .expect("store B journal should persist");

        ctx_a.wake_tier_delete_journal_recovery();
        ctx_b.wake_tier_delete_journal_recovery();
        remove_a.wait_until_paused().await;
        wait_for_tier_delete_journal_recovery(store_b.clone(), &backend_b, 1).await;

        shutdown_a.cancel();
        remove_a.wait_until_operation_dropped().await;
        assert!(
            ctx_a
                .background_cancel_token()
                .expect("store A shutdown token should be bound")
                .is_cancelled()
        );
        assert!(
            !ctx_b
                .background_cancel_token()
                .expect("store B shutdown token should be bound")
                .is_cancelled(),
            "cancelling store A must not stop store B"
        );
        assert_eq!(tier_delete_journal_count(store_a.clone()).await, 1);

        let recovered_a = recover_tier_delete_journal_entries(store_a.clone(), 100, None)
            .await
            .expect("the cancelled store A worker must leave its journal recoverable");
        assert_eq!((recovered_a.scanned, recovered_a.deleted, recovered_a.failed), (1, 1, 0));
        assert_eq!(backend_a.remove_versions().await, vec![("remote-a".to_string(), "version-a".to_string())]);

        let second_entry_b = Jentry {
            obj_name: "remote-b-2".to_string(),
            version_id: "version-b-2".to_string(),
            ..entry_b
        };
        persist_tier_delete_journal_entry(store_b.clone(), &second_entry_b)
            .await
            .expect("store B second journal should persist");
        ctx_b.wake_tier_delete_journal_recovery();
        wait_for_tier_delete_journal_recovery(store_b.clone(), &backend_b, 2).await;
        assert_eq!(
            backend_b.remove_versions().await,
            vec![
                ("remote-b".to_string(), "version-b".to_string()),
                ("remote-b-2".to_string(), "version-b-2".to_string()),
            ]
        );

        shutdown_b.cancel();
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
            expires_at_unix_nanos: 1_780_000_000_000_000_000,
        }
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn tier_mutation_peer_handler_applies_prepare_commit_and_abort_idempotently() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (_ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "tier-mutation-peer-handler", &[4])).await;
        let mutation_id = uuid::Uuid::new_v4();
        let intent = tier_mutation_peer_test_intent(mutation_id, "COLD-A", [3; 32]);
        let prepare_payload = intent.encode().expect("prepare intent should encode");
        register_mock_tier(&store.tier_config_mgr(), "COLD-A").await;

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

        let committed = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Commit,
            mutation_id,
            b"new-etag",
        )
        .await
        .expect("commit should advance the prepared peer intent");
        assert!(committed.applied);
        assert_eq!(committed.state, TierMutationPeerState::Committed);
        drop(
            TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A")
                .await
                .expect("committed peer mutation should clear the prepared runtime block"),
        );

        let retried_commit = handle_tier_mutation_peer_request(
            store.clone(),
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Commit,
            mutation_id,
            b"new-etag",
        )
        .await
        .expect("same commit retry should be idempotent");
        assert!(!retried_commit.applied);
        assert_eq!(retried_commit.state, TierMutationPeerState::Committed);

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
        drop(
            TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-A")
                .await
                .expect("delayed committed prepare retry must not recreate a runtime block"),
        );

        let loaded = load_tier_mutation_intent_record(store.clone(), mutation_id)
            .await
            .expect("committed peer intent should remain durable");
        assert_eq!(loaded.state, TierMutationIntentState::Committed);
        assert_eq!(loaded.committed_config_etag.as_deref(), Some("new-etag"));

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
            .expect("committed peer intent cleanup should persist");
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

        let abort_id = uuid::Uuid::new_v4();
        let abort_intent = tier_mutation_peer_test_intent(abort_id, "COLD-B", [4; 32]);
        let abort_prepare_payload = abort_intent.encode().expect("abort prepare intent should encode");
        register_mock_tier(&store.tier_config_mgr(), "COLD-B").await;
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
            b"",
        )
        .await
        .expect("abort should advance the prepared peer intent");
        assert!(aborted.applied);
        assert_eq!(aborted.state, TierMutationPeerState::Aborted);
        drop(
            TierConfigMgr::acquire_operation_lease(&store.tier_config_mgr(), "COLD-B")
                .await
                .expect("aborted peer mutation should clear the prepared runtime block"),
        );

        let retried_abort = handle_tier_mutation_peer_request(
            store,
            TIER_MUTATION_RPC_PROTOCOL_VERSION,
            TierMutationRpcPhase::Abort,
            abort_id,
            b"",
        )
        .await
        .expect("same abort retry should be idempotent");
        assert!(!retried_abort.applied);
        assert_eq!(retried_abort.state, TierMutationPeerState::Aborted);
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
    async fn cancelled_transition_cleanup_journals_to_its_own_instance_store() {
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
        let backend = register_mock_tier(&ctx_a.tier_config_mgr(), tier_name).await;
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
        let journal_counts = (
            tier_delete_journal_count(store_a.clone()).await,
            tier_delete_journal_count(store_b.clone()).await,
        );
        assert_eq!(
            journal_counts,
            (1, 0),
            "the journal must land only on store A even while the process resolver points at store B"
        );
        assert_eq!(backend.object_count().await, 1, "failed cleanup should retain the remote candidate");
        remove_barrier.release();
        remove_barrier.wait_until_operation_dropped().await;

        let recovered = recover_tier_delete_journal_entries(store_a.clone(), 100, None)
            .await
            .expect("store A should recover its own cancelled-transition journal");
        assert_eq!((recovered.scanned, recovered.deleted, recovered.failed), (1, 1, 0));
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
    async fn transition_transaction_recovery_deletes_uploaded_remote_candidate() {
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
        backend.set_put_remote_version(Some(remote_version.clone())).await;
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

        let stats = recover_transition_transaction_records(store.clone(), 100, None)
            .await
            .expect("transition transaction recovery should run");

        assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (1, 1, 0, 0));
        assert_eq!(transition_transaction_record_count(store.clone()).await, 0);
        assert_eq!(
            backend.remove_versions().await,
            vec![(transaction.remote_object.clone(), remote_version)],
            "recovery must delete the exact uploaded candidate"
        );
        assert_eq!(backend.exact_remove_count(), 1);
        assert_eq!(backend.object_count().await, 0);
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
        let uploaded_fence = transaction
            .advance(
                transaction.fence(),
                TransitionTransactionState::Uploaded,
                Some(TransitionRemoteVersion::versioned(remote_version)),
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
        let candidate = bytes::Bytes::from_static(b"cleanup pending candidate retained after failure");
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
            data_dir: uuid::Uuid::new_v4(),
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
                data_dir: uuid::Uuid::new_v4(),
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
    }

    #[cfg(feature = "test-util")]
    #[tokio::test]
    #[serial_test::serial(storage_class_env)]
    async fn transition_transaction_recovery_deletes_provider_recovered_unknown_upload() {
        let versioned_remote = uuid::Uuid::new_v4().to_string();
        let nil_remote = uuid::Uuid::nil().to_string();
        for (case, tier_name, remote_version) in [
            ("missing", "TXPROBEMISSING", None),
            ("unversioned", "TXPROBEUNVERSIONED", Some(String::new())),
            ("versioned", "TXPROBEVERSIONED", Some(versioned_remote)),
            ("nil-version", "TXPROBENILVERSION", Some(nil_remote)),
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
                not_after_unix_nanos: 1_780_000_000_000_000_000,
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

            assert_eq!((stats.scanned, stats.recovered, stats.retained, stats.failed), (1, 1, 0, 0));
            assert_eq!(transition_transaction_record_count(store.clone()).await, 0);
            assert_eq!(
                backend.object_count().await,
                0,
                "case {case}: recovered unknown upload candidate must be absent"
            );
            let removed = remote_version
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
            .expect("unsupported provider recovery should fail closed");
        assert_eq!(
            (
                unsupported_stats.scanned,
                unsupported_stats.recovered,
                unsupported_stats.retained,
                unsupported_stats.failed
            ),
            (1, 0, 1, 0),
            "an unsupported provider probe must retain the unknown upload"
        );
        assert_eq!(transition_transaction_record_count(store.clone()).await, 1);
        assert!(
            backend.contains(&transaction.remote_object).await,
            "unsupported recovery must not delete the candidate"
        );
        assert_eq!(backend.remove_count().await, 0, "unsupported recovery must not attempt cleanup");

        backend.set_transition_candidate_probe_override(None).await;
        let stats = recover_transition_transaction_records(store.clone(), 100, None)
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
