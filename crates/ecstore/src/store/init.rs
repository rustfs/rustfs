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
use crate::error::is_err_decommission_running;
use crate::global::is_first_cluster_node_local;

fn missing_deployment_id_error() -> Error {
    Error::other("store init failed: deployment id is not initialized")
}

fn no_storage_pools_available_error() -> Error {
    Error::other("store init failed: no storage pools available")
}

fn store_init_load_formats_exhausted_error(retries: usize, err: &Error) -> Error {
    Error::other(format!("store init failed to load formats after {retries} retries: {err}"))
}

fn store_init_deployment_id_mismatch_error() -> Error {
    Error::other("store init failed: deployment IDs do not match across pools")
}

fn store_init_retry_budget_exhausted_error() -> Error {
    Error::other("store init failed: init retry budget exhausted")
}

fn store_init_resume_pool_not_found_error(cmd_line: &str) -> Error {
    Error::other(format!(
        "store init failed to resolve resumable decommission pool `{cmd_line}` from current endpoints"
    ))
}

fn store_init_resume_pool_index_not_found_error(idx: usize) -> Error {
    Error::other(format!(
        "store init failed to resolve decommission resume pool index {idx} from current endpoints"
    ))
}

fn store_init_resume_pool_without_endpoints_error(idx: usize) -> Error {
    Error::other(format!(
        "store init failed to resolve decommission resume pool index {idx}: no endpoints available"
    ))
}

fn format_store_init_decommission_resume_error(pool_indices: &[usize], err: &Error) -> String {
    format!("store init failed to resume decommission for pools {:?}: {}", pool_indices, err)
}

fn format_store_init_decommission_resume_spawn_error(pool_indices: &[usize], err: &Error) -> String {
    format!("store init failed to resume decommission workers for pools {:?}: {}", pool_indices, err)
}

fn require_deployment_id(deployment_id: Option<Uuid>) -> Result<Uuid> {
    deployment_id.ok_or_else(missing_deployment_id_error)
}

fn clone_first_store_pool<T: Clone>(pools: &[T]) -> Result<T> {
    pools.first().cloned().ok_or_else(no_storage_pools_available_error)
}

fn should_resume_local_decommission(endpoints: &EndpointServerPools, idx: usize) -> Result<bool> {
    let pool = endpoints
        .as_ref()
        .get(idx)
        .ok_or_else(|| store_init_resume_pool_index_not_found_error(idx))?;
    let endpoint = pool
        .endpoints
        .as_ref()
        .first()
        .ok_or_else(|| store_init_resume_pool_without_endpoints_error(idx))?;

    Ok(endpoint.is_local)
}

const LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES: usize = 6;
const LOCAL_DECOMMISSION_INITIAL_RESUME_DELAY: Duration = Duration::from_secs(60 * 3);
const LOCAL_DECOMMISSION_RESUME_RETRY_DELAY: Duration = Duration::from_secs(30);

fn should_retry_local_decommission_resume(err: &Error, attempt: usize) -> bool {
    matches!(err, Error::ConfigNotFound) && attempt < LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES
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

async fn resume_local_decommission_after_init(store: Arc<ECStore>, rx: CancellationToken, pool_indices: Vec<usize>) {
    for attempt in 0..=LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES {
        if rx.is_cancelled() {
            return;
        }

        match store.decommission(rx.clone(), pool_indices.clone()).await {
            Ok(()) => return,
            Err(err) if is_err_decommission_running(&err) => {
                if let Err(spawn_err) = store
                    .spawn_decommission_routines(store.clone(), rx.clone(), pool_indices.clone())
                    .await
                {
                    error!("{}", format_store_init_decommission_resume_spawn_error(&pool_indices, &spawn_err));
                }
                return;
            }
            Err(err) if should_retry_local_decommission_resume(&err, attempt) => {
                warn!(
                    "store init decommission resume missing config for pools {:?}, retry {}/{}: {}",
                    pool_indices,
                    attempt + 1,
                    LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES + 1,
                    err
                );
                tokio::select! {
                    _ = rx.cancelled() => return,
                    _ = tokio::time::sleep(LOCAL_DECOMMISSION_RESUME_RETRY_DELAY) => {}
                }
            }
            Err(err) => {
                error!("{}", format_store_init_decommission_resume_error(&pool_indices, &err));
                return;
            }
        }
    }
}

impl ECStore {
    #[allow(clippy::new_ret_no_self)]
    #[instrument(level = "debug", skip(endpoint_pools))]
    pub async fn new(address: SocketAddr, endpoint_pools: EndpointServerPools, ctx: CancellationToken) -> Result<Arc<Self>> {
        // let layouts = DisksLayout::from_volumes(endpoints.as_slice())?;

        let mut deployment_id = None;

        // let (endpoint_pools, _) = EndpointServerPools::create_server_endpoints(address.as_str(), &layouts)?;

        let mut pools = Vec::with_capacity(endpoint_pools.as_ref().len());
        let mut disk_map = HashMap::with_capacity(endpoint_pools.as_ref().len());

        let first_is_local = endpoint_pools.first_local();

        let mut local_disks = Vec::new();

        info!("ECStore new address: {}", address.to_string());
        let mut host = address.ip().to_string();
        if host.is_empty() {
            host = GLOBAL_RUSTFS_HOST.read().await.to_string()
        }
        let mut port = address.port().to_string();
        if port.is_empty() {
            port = GLOBAL_RUSTFS_PORT.read().await.to_string()
        }
        info!("ECStore new host: {}, port: {}", host, port);
        init_local_peer(&endpoint_pools, &host, &port).await;

        // debug!("endpoint_pools: {:?}", endpoint_pools);

        let mut common_parity_drives = 0;

        for (i, pool_eps) in endpoint_pools.as_ref().iter().enumerate() {
            if common_parity_drives == 0 {
                let parity_drives = ec_drives_no_config(pool_eps.drives_per_set)?;
                storageclass::validate_parity(parity_drives, pool_eps.drives_per_set)?;
                common_parity_drives = parity_drives;
            }

            // validate_parity(parity_count, pool_eps.drives_per_set)?;

            // Initialize disks without health monitoring so that remote peers
            // are not immediately marked as faulty before they have a chance to
            // start up. Health monitoring is enabled after format loading succeeds.
            let (disks, errs) = store_init::init_disks(
                &pool_eps.endpoints,
                &DiskOption {
                    cleanup: true,
                    health_check: false,
                },
            )
            .await;

            check_disk_fatal_errs(&errs)?;

            let fm = {
                let mut times = 0;
                let mut interval = 1;
                loop {
                    match store_init::connect_load_init_formats(
                        first_is_local,
                        &disks,
                        pool_eps.set_count,
                        pool_eps.drives_per_set,
                        deployment_id,
                    )
                    .await
                    {
                        Ok(fm) => break Ok(fm),
                        // Wrap the final error if we are giving up
                        Err(e) if times >= 10 => break Err(store_init_load_formats_exhausted_error(times, &e)),
                        // Retrying so just drop the error
                        Err(_) => {}
                    }
                    times += 1;
                    if interval < 16 {
                        interval *= 2;
                    }
                    info!("retrying get formats after {:?}", interval);
                    select! {
                        _ = tokio::signal::ctrl_c() => {
                            info!("got ctrl+c, exits");
                            exit(0);
                        }
                        _ = sleep(Duration::from_secs(interval)) => {
                        }
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
                return Err(store_init_deployment_id_mismatch_error());
            }

            if deployment_id.is_some_and(|id| id.is_nil()) {
                deployment_id = Some(Uuid::new_v4());
            }

            for disk in disks.iter() {
                if disk.is_some() && disk.as_ref().unwrap().is_local() {
                    local_disks.push(disk.as_ref().unwrap().clone());
                }
            }

            let sets = Sets::new(disks.clone(), pool_eps, &fm, i, common_parity_drives).await?;
            pools.push(sets);

            disk_map.insert(i, disks);
        }

        // Replace the local disk
        if !is_dist_erasure().await {
            let mut global_local_disk_map = GLOBAL_LOCAL_DISK_MAP.write().await;
            for disk in local_disks {
                let path = disk.endpoint().to_string();
                global_local_disk_map.insert(path, Some(disk.clone()));
            }
        }

        let peer_sys = S3PeerSys::new(&endpoint_pools);
        let mut pool_meta = PoolMeta::new(&pools, &PoolMeta::default());
        pool_meta.dont_save = true;

        let decommission_cancelers = RwLock::new(vec![None; pools.len()]);
        let ec = Arc::new(ECStore {
            id: require_deployment_id(deployment_id)?,
            disk_map,
            pools,
            peer_sys,
            pool_meta: RwLock::new(pool_meta),
            rebalance_meta: RwLock::new(None),
            decommission_cancelers,
        });

        // Only set it when the global deployment ID is not yet configured
        if let Some(dep_id) = deployment_id
            && get_global_deployment_id().is_none()
        {
            set_global_deployment_id(dep_id);
        }

        let wait_sec = 5;
        let mut exit_count = 0;
        loop {
            if let Err(err) = ec.init(ctx.clone()).await {
                error!("init err: {}", err);
                error!("retry after  {} second", wait_sec);
                sleep(Duration::from_secs(wait_sec)).await;

                if exit_count > 10 {
                    return Err(store_init_retry_budget_exhausted_error());
                }

                exit_count += 1;

                continue;
            }

            break;
        }

        set_object_layer(ec.clone()).await;

        Ok(ec)
    }

    #[instrument(level = "debug", skip(self, rx))]
    pub async fn init(self: &Arc<Self>, rx: CancellationToken) -> Result<()> {
        GLOBAL_BOOT_TIME.get_or_init(|| async { SystemTime::now() }).await;

        resolve_store_init_stage_result(self.load_rebalance_meta().await, "load_rebalance_meta")?;
        if self.rebalance_meta.read().await.is_some() {
            resolve_store_init_stage_result(self.start_rebalance().await, "start_rebalance")?;
        }

        let mut meta = PoolMeta::default();
        resolve_store_init_stage_result(
            meta.load(clone_first_store_pool(&self.pools)?, self.pools.clone()).await,
            "load_pool_meta",
        )?;
        let update = meta.validate(self.pools.clone())?;
        let endpoints = get_global_endpoints();
        let should_persist_pool_meta = is_first_cluster_node_local().await;

        if !update {
            {
                let mut pool_meta = self.pool_meta.write().await;
                *pool_meta = meta.clone();
            }
        } else {
            let new_meta = PoolMeta::new(&self.pools, &meta);
            // Only one local node should persist validated pool metadata here; otherwise
            // distributed startup can race on the same lock and replay the prior init bug.
            if should_persist_pool_meta {
                resolve_store_init_stage_result(new_meta.save(self.pools.clone()).await, "save_validated_pool_meta")?;
            }
            {
                let mut pool_meta = self.pool_meta.write().await;
                *pool_meta = new_meta;
            }
        }

        let pools = meta.return_resumable_pools();
        let mut pool_indices = Vec::with_capacity(pools.len());

        for p in pools.iter() {
            if let Some(idx) = endpoints.get_pool_idx(&p.cmd_line) {
                pool_indices.push(idx);
            } else {
                return Err(store_init_resume_pool_not_found_error(&p.cmd_line));
            }
        }

        if !pool_indices.is_empty() {
            let idx = pool_indices[0];
            if should_resume_local_decommission(&endpoints, idx)? {
                let store = self.clone();

                tokio::spawn(async move {
                    if !wait_for_local_decommission_resume_delay(&rx, LOCAL_DECOMMISSION_INITIAL_RESUME_DELAY).await {
                        return;
                    }
                    resume_local_decommission_after_init(store, rx, pool_indices).await;
                });
            }
        }

        let num_nodes = get_global_endpoints().get_nodes().len() as u64;
        init_global_bucket_monitor(num_nodes);

        init_background_expiry(self.clone()).await;

        TransitionState::init(self.clone()).await;
        crate::tier::tier::try_migrate_tiering_config(self.clone()).await;

        if let Err(err) = GLOBAL_TierConfigMgr.write().await.init(self.clone()).await {
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
        LOCAL_DECOMMISSION_RESUME_MAX_CONFIG_RETRIES, clone_first_store_pool, require_deployment_id,
        resolve_store_init_stage_result, should_resume_local_decommission, should_retry_local_decommission_resume,
        store_init_deployment_id_mismatch_error, store_init_load_formats_exhausted_error, store_init_resume_pool_not_found_error,
        store_init_retry_budget_exhausted_error, wait_for_local_decommission_resume_delay,
    };
    use crate::{
        disk::endpoint::Endpoint,
        endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
        error::StorageError,
    };
    use std::time::Duration;
    use tokio_util::sync::CancellationToken;
    use uuid::Uuid;

    #[test]
    fn test_require_deployment_id_returns_uuid() {
        let id = Uuid::new_v4();
        let resolved = require_deployment_id(Some(id)).expect("deployment id should pass through");
        assert_eq!(resolved, id);
    }

    #[test]
    fn test_require_deployment_id_rejects_missing_id() {
        let err = require_deployment_id(None).expect_err("missing deployment id should error");
        assert!(
            err.to_string()
                .contains("store init failed: deployment id is not initialized")
        );
    }

    #[test]
    fn test_clone_first_store_pool_returns_first_pool() {
        let first = clone_first_store_pool(&[1_u8, 2, 3]).expect("first pool should be returned");
        assert_eq!(first, 1);
    }

    #[test]
    fn test_clone_first_store_pool_rejects_empty_pools() {
        let err = clone_first_store_pool::<u8>(&[]).expect_err("empty pool list should error");
        assert!(err.to_string().contains("store init failed: no storage pools available"));
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

    #[test]
    fn test_format_store_init_decommission_resume_error_includes_pool_context() {
        let message = super::format_store_init_decommission_resume_error(&[1, 3], &StorageError::SlowDown);

        assert!(message.contains("resume decommission for pools [1, 3]"));
        assert!(message.contains(&StorageError::SlowDown.to_string()));
    }

    #[test]
    fn test_format_store_init_decommission_resume_spawn_error_includes_pool_context() {
        let message = super::format_store_init_decommission_resume_spawn_error(&[2], &StorageError::SlowDown);

        assert!(message.contains("resume decommission workers for pools [2]"));
        assert!(message.contains(&StorageError::SlowDown.to_string()));
    }

    #[test]
    fn test_store_init_load_formats_exhausted_error_includes_retry_count_and_source() {
        let err = store_init_load_formats_exhausted_error(10, &StorageError::SlowDown);
        let rendered = err.to_string();

        assert!(rendered.contains("store init failed to load formats after 10 retries"), "{rendered}");
        assert!(rendered.contains(&StorageError::SlowDown.to_string()), "{rendered}");
    }

    #[test]
    fn test_store_init_deployment_id_mismatch_error_formats_context() {
        let err = store_init_deployment_id_mismatch_error();
        let rendered = err.to_string();

        assert!(
            rendered.contains("store init failed: deployment IDs do not match across pools"),
            "{rendered}"
        );
    }

    #[test]
    fn test_store_init_retry_budget_exhausted_error_formats_context() {
        let err = store_init_retry_budget_exhausted_error();
        let rendered = err.to_string();

        assert!(rendered.contains("store init failed: init retry budget exhausted"), "{rendered}");
    }

    #[test]
    fn test_store_init_resume_pool_not_found_error_formats_cmdline_context() {
        let err = store_init_resume_pool_not_found_error("pool-a");
        let rendered = err.to_string();

        assert!(
            rendered.contains("store init failed to resolve resumable decommission pool `pool-a`"),
            "{rendered}"
        );
        assert!(rendered.contains("current endpoints"), "{rendered}");
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
}
