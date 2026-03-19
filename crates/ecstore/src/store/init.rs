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

fn require_deployment_id(deployment_id: Option<Uuid>) -> Result<Uuid> {
    deployment_id.ok_or_else(|| Error::other("deployment id is not initialized"))
}

fn clone_first_store_pool<T: Clone>(pools: &[T]) -> Result<T> {
    pools
        .first()
        .cloned()
        .ok_or_else(|| Error::other("no storage pools available"))
}

fn should_resume_local_decommission(endpoints: &EndpointServerPools, idx: usize) -> Result<bool> {
    let pool = endpoints
        .as_ref()
        .get(idx)
        .ok_or_else(|| Error::other(format!("decommission resume pool index {idx} not found")))?;
    let endpoint = pool
        .endpoints
        .as_ref()
        .first()
        .ok_or_else(|| Error::other(format!("decommission resume pool index {idx} has no endpoints")))?;

    Ok(endpoint.is_local)
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
                        Err(e) if times >= 10 => {
                            break Err(Error::other(format!("can not get formats after {} retries, last error: {e}", times)));
                        }
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
                return Err(Error::other("deployment_id not same in one pool"));
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
                    return Err(Error::other("ec init failed"));
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

        self.load_rebalance_meta().await?;
        if self.rebalance_meta.read().await.is_some() {
            self.start_rebalance().await?;
        }

        let mut meta = PoolMeta::default();
        meta.load(clone_first_store_pool(&self.pools)?, self.pools.clone()).await?;
        let update = meta.validate(self.pools.clone())?;

        if !update {
            {
                let mut pool_meta = self.pool_meta.write().await;
                *pool_meta = meta.clone();
            }
        } else {
            let new_meta = PoolMeta::new(&self.pools, &meta);
            new_meta.save(self.pools.clone()).await?;
            {
                let mut pool_meta = self.pool_meta.write().await;
                *pool_meta = new_meta;
            }
        }

        let pools = meta.return_resumable_pools();
        let mut pool_indices = Vec::with_capacity(pools.len());

        let endpoints = get_global_endpoints();

        for p in pools.iter() {
            if let Some(idx) = endpoints.get_pool_idx(&p.cmd_line) {
                pool_indices.push(idx);
            } else {
                return Err(Error::other(format!(
                    "unexpected state present for decommission status pool({}) not found",
                    p.cmd_line
                )));
            }
        }

        if !pool_indices.is_empty() {
            let idx = pool_indices[0];
            if should_resume_local_decommission(&endpoints, idx)? {
                let store = self.clone();

                tokio::spawn(async move {
                    // wait  3 minutes for cluster init
                    tokio::time::sleep(Duration::from_secs(60 * 3)).await;

                    if let Err(err) = store.decommission(rx.clone(), pool_indices.clone()).await {
                        if is_err_decommission_running(&err) {
                            if let Err(spawn_err) = store
                                .spawn_decommission_routines(store.clone(), rx.clone(), pool_indices.clone())
                                .await
                            {
                                error!("store init spawn_decommission_routines err: {}", spawn_err);
                            }
                            return;
                        }

                        error!("store init decommission err: {}", err);

                        // TODO: check config err
                    }
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
    use super::{clone_first_store_pool, require_deployment_id, should_resume_local_decommission};
    use crate::{
        disk::endpoint::Endpoint,
        endpoints::{EndpointServerPools, Endpoints, PoolEndpoints},
    };
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
        assert!(err.to_string().contains("deployment id is not initialized"));
    }

    #[test]
    fn test_clone_first_store_pool_returns_first_pool() {
        let first = clone_first_store_pool(&[1_u8, 2, 3]).expect("first pool should be returned");
        assert_eq!(first, 1);
    }

    #[test]
    fn test_clone_first_store_pool_rejects_empty_pools() {
        let err = clone_first_store_pool::<u8>(&[]).expect_err("empty pool list should error");
        assert!(err.to_string().contains("no storage pools available"));
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
        assert!(err.to_string().contains("pool index 0 not found"));
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
        assert!(err.to_string().contains("pool index 0 has no endpoints"));
    }
}
