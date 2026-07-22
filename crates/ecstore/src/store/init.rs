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
use tracing::{debug, error, info, warn};

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_STORE_INIT: &str = "store_init";
const EVENT_DECOMMISSION_RESUME_RETRY: &str = "decommission_resume_retry";
const EVENT_DECOMMISSION_RESUME_FAILED: &str = "decommission_resume_failed";
const EVENT_STORE_FORMAT_RETRY: &str = "store_format_retry";
const EVENT_ECSTORE_INIT_STATUS: &str = "ecstore_init_status";

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

fn should_resume_local_decommission(endpoints: &EndpointServerPools, idx: usize) -> Result<bool> {
    let pool = endpoints.as_ref().get(idx).ok_or_else(|| {
        Error::other(format!(
            "store init failed to resolve decommission resume pool index {idx} from current endpoints"
        ))
    })?;
    let endpoint = pool.endpoints.as_ref().first().ok_or_else(|| {
        Error::other(format!(
            "store init failed to resolve decommission resume pool index {idx}: no endpoints available"
        ))
    })?;

    Ok(endpoint.is_local)
}

const LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES: usize = 6;
const LOCAL_DECOMMISSION_INITIAL_RESUME_DELAY: Duration = Duration::from_secs(60 * 3);
const LOCAL_DECOMMISSION_RESUME_RETRY_DELAY: Duration = Duration::from_secs(30);

fn should_retry_local_decommission_resume(err: &Error, attempt: usize) -> bool {
    matches!(err, Error::ConfigNotFound) && attempt < LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES
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

fn should_auto_start_rebalance_after_recovered_meta(pool_meta: &PoolMeta, rebalance_meta_loaded: bool) -> bool {
    should_auto_start_rebalance_after_init(pool_meta_has_active_decommission(pool_meta), rebalance_meta_loaded)
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
            let (disks, errs) = init_format::init_disks(
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
                    match init_format::connect_load_init_formats(
                        pool_first_is_local,
                        &disks,
                        pool_eps.set_count,
                        pool_eps.drives_per_set,
                        deployment_id,
                    )
                    .await
                    {
                        Ok(fm) => break Ok(fm),
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
        resolve_startup_pool_defaults_with, resolve_store_init_stage_result, save_validated_pool_meta_for_startup,
        should_auto_start_rebalance_after_init, should_auto_start_rebalance_after_recovered_meta,
        should_resume_local_decommission, should_retry_local_decommission_resume, wait_for_local_decommission_resume_delay,
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
                TRANSITION_TRANSACTION_RECORD_PREFIX, TransitionCleanupDecision, TransitionCleanupProof, TransitionRemoteVersion,
                TransitionSourceIdentity, TransitionSourceVersionMode, TransitionTransaction, TransitionTransactionInit,
                TransitionTransactionState, load_transition_transaction_record, recover_transition_transaction_records,
                save_transition_transaction_record,
            },
        },
        client::transition_api::ReaderImpl,
        config::com,
        disk::RUSTFS_META_BUCKET,
        runtime::{global::set_object_store_resolver, sources as runtime_sources},
        services::tier::{
            test_util::{MockWarmBackend, MockWarmOp, TransitionCleanupStoreBarrier, register_mock_tier},
            tier::{TIER_CONFIG_FILE, TierConfigMgr},
            tier_mutation_intent::{
                TIER_MUTATION_INTENT_RECORD_PREFIX, TierMutationIntent, TierMutationIntentKind, TierMutationIntentState,
                TierMutationIntentTarget, advance_tier_mutation_intent_record_idempotent, delete_tier_mutation_intent_record,
                list_tier_mutation_intent_records, load_tier_mutation_intent_record, load_tier_mutation_intent_record_with_etag,
                save_tier_mutation_intent_record, save_tier_mutation_intent_record_if_current,
            },
            tier_mutation_peer::{TierMutationPeerError, TierMutationPeerState, handle_tier_mutation_peer_request},
            warm_backend::{TransitionCandidateProbe, WarmBackend},
        },
        storage_api_contracts::{
            bucket::{BucketOperations as _, MakeBucketOptions},
            list::ListOperations as _,
            object::ObjectOperations as _,
        },
    };
    use crate::{
        core::pools::{POOL_META_VERSION, PoolDecommissionInfo, PoolMeta, PoolStatus},
        disk::endpoint::Endpoint,
        error::{Error, Result, StorageError},
        layout::endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
        object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader},
        services::rebalance::RebalanceMeta,
        storage_api_contracts::{object::ObjectIO, range::HTTPRangeSpec},
    };
    use http::HeaderMap;
    use rustfs_config::server_config::KVS;
    #[cfg(feature = "test-util")]
    use rustfs_protos::{TIER_MUTATION_RPC_PROTOCOL_VERSION, TierMutationRpcPhase};
    use std::{
        future::Future,
        io::Cursor,
        sync::{
            Arc,
            atomic::{AtomicBool, Ordering},
        },
        time::Duration,
    };
    use time::OffsetDateTime;
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
    fn test_should_resume_local_decommission_respects_local_flag() {
        let mut local_endpoint = Endpoint::try_from("http://127.0.0.1:9000/data").expect("endpoint should parse");
        local_endpoint.is_local = true;
        let endpoints = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: Endpoints::from(vec![local_endpoint]),
            cmd_line: "pool-0".to_string(),
            platform: String::new(),
        }]);

        assert!(should_resume_local_decommission(&endpoints, 0).expect("local endpoint should resume"));
    }

    #[test]
    fn test_should_resume_local_decommission_rejects_unresolvable_pool() {
        let endpoints = EndpointServerPools::default();
        let err = should_resume_local_decommission(&endpoints, 0).expect_err("missing pool should error");
        assert_eq!(
            err.to_string(),
            "Io error: store init failed to resolve decommission resume pool index 0 from current endpoints"
        );
    }

    #[test]
    fn test_should_resume_local_decommission_rejects_missing_endpoint() {
        let endpoints = EndpointServerPools::from(vec![PoolEndpoints {
            legacy: false,
            set_count: 1,
            drives_per_set: 1,
            endpoints: Endpoints::from(Vec::<Endpoint>::new()),
            cmd_line: "pool-0".to_string(),
            platform: String::new(),
        }]);
        let err = should_resume_local_decommission(&endpoints, 0).expect_err("missing endpoint should error");
        assert_eq!(
            err.to_string(),
            "Io error: store init failed to resolve decommission resume pool index 0: no endpoints available"
        );
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

        assert!(!should_auto_start_rebalance_after_recovered_meta(&pool_meta, rebalance_meta.is_some()));
    }

    #[test]
    fn test_store_init_recovery_allows_rebalance_when_only_rebalance_metadata_exists() {
        let pool_meta = init_test_pool_meta(None);
        let rebalance_meta = Some(RebalanceMeta::default());

        assert!(should_auto_start_rebalance_after_recovered_meta(&pool_meta, rebalance_meta.is_some()));
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
            version_id_exact: false,
        };
        let entry_b = Jentry {
            obj_name: "remote-b".to_string(),
            version_id: "version-b".to_string(),
            tier_name: tier_b.to_string(),
            backend_identity: Some(identity_b),
            version_id_exact: false,
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
        backend.set_reject_non_empty_remote_versions(true);
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
        for (case, tier_name, remote_version) in [
            ("missing", "TXPROBEMISSING", None),
            ("unversioned", "TXPROBEUNVERSIONED", Some(String::new())),
            ("versioned", "TXPROBEVERSIONED", Some(versioned_remote)),
        ] {
            let temp_dir = tempfile::tempdir().expect("create temp store dir");
            let (ctx, store, _shutdown) = without_storage_class_env(build_isolated_test_store(
                temp_dir.path(),
                &format!("transition-transaction-probe-{case}"),
                &[4],
            ))
            .await;
            crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

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
    async fn transition_response_loss_persists_unknown_outcome_for_provider_recovery() {
        let temp_dir = tempfile::tempdir().expect("create temp store dir");
        let (ctx, store, _shutdown) =
            without_storage_class_env(build_isolated_test_store(temp_dir.path(), "transition-response-loss", &[4])).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(store.clone(), Vec::new()).await;

        let tier_name = "TXRESPONSELOSS";
        let backend = register_mock_tier(&ctx.tier_config_mgr(), tier_name).await;
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
