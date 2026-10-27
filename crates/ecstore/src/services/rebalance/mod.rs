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

#[cfg(any(test, feature = "test-util"))]
use crate::disk::DiskAPI;
use crate::error::{Error, Result};
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use tokio::time::Duration;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_REBALANCE: &str = "rebalance";
const EVENT_REBALANCE_STATE: &str = "rebalance_state";
const EVENT_REBALANCE_BUCKET: &str = "rebalance_bucket";
const EVENT_REBALANCE_ENTRY: &str = "rebalance_entry";
const EVENT_REBALANCE_LISTING: &str = "rebalance_listing";

const REBAL_META_FMT: u16 = 1; // Replace with actual format value
const REBAL_META_VER: u16 = 1; // Replace with actual version value
pub(crate) const REBAL_META_NAME: &str = "rebalance.bin";
const DEFAULT_REBALANCE_MAX_ATTEMPTS: usize = 3;
pub(crate) const REBALANCE_SOURCE_CLEANUP_MAX_DEFERS: usize = 8;
const REBALANCE_MAX_ATTEMPTS_ENV: &str = "RUSTFS_REBALANCE_MAX_ATTEMPTS";
const REBALANCE_STOP_PROPAGATION_ERROR_PREFIX: &str = "rebalance stop propagation incomplete: ";
const REBALANCE_LISTING_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
const REBALANCE_MIGRATION_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
const REBALANCE_MIGRATION_LOCK_RETRY_CAP: Duration = Duration::from_secs(10);
const REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX: &str = "deferred transient rebalance entry failure:";
pub(crate) const REBALANCE_SOURCE_CLEANUP_DEFERRED_ERROR_PREFIX: &str = "deferred rebalance source cleanup conflict:";
const REBALANCE_CLEANUP_WARNING_ENTRY_LIMIT: usize = 10;

mod control;
#[cfg(test)]
pub(crate) use control::{REBALANCE_REFRESH_ACTIVATION_GATE, REBALANCE_REFRESH_MOVEMENT_GATE, REBALANCE_REFRESH_START_GATE};
mod entry;
mod meta;
mod migration;
mod runtime;
mod types;
mod worker;

#[cfg(feature = "test-util")]
pub use entry::test_util::PausedRebalanceEntryTestFixture;
pub use meta::{decode_rebalance_stop_propagation_record, encode_rebalance_stop_propagation_record};
pub(crate) use meta::{is_rebalance_conflicting_with_decommission, rebalance_requires_worker_activation};
pub use types::{
    DiskStat, RebalSaveOpt, RebalStatus, RebalanceCleanupWarningEntry, RebalanceCleanupWarnings, RebalanceInfo, RebalanceMeta,
    RebalanceStats, RebalanceStopPropagationRecord,
};
use types::{RebalanceBucketConfigs, RebalanceBucketOutcome, RebalanceDeferKind, RebalanceEntryOutcome};

#[cfg(any(test, feature = "test-util"))]
pub async fn test_store_with_persisted_rebalance_meta(
    meta: RebalanceMeta,
) -> (Vec<tempfile::TempDir>, std::sync::Arc<crate::store::ECStore>) {
    let ctx = std::sync::Arc::new(crate::runtime::instance::InstanceContext::new());
    let (temp_dirs, pool) = crate::core::sets::make_local_two_set_sets_with_ctx(ctx.clone()).await;
    let pools = vec![pool.clone()];
    let pool_meta = crate::core::pools::PoolMeta::new(&pools, &crate::core::pools::PoolMeta::default())
        .expect("new pool topology should produce valid pool metadata");
    pool_meta
        .save_for_startup(pools.clone())
        .await
        .expect("rebalance test pool metadata should be persisted");
    meta.save(pool.clone())
        .await
        .expect("rebalance test metadata should be persisted");
    let endpoint_pools: crate::layout::endpoints::EndpointServerPools = vec![pool.endpoints.clone()].into();
    let authority = ctx
        .bind_multipart_proof_topology(&endpoint_pools)
        .expect("single-pool fixture must register its complete local topology");
    for set in &pool.disk_set {
        let disks = set.disks.read().await;
        crate::disk::multipart_source::bind_multipart_proof_snapshot(&disks, &authority)
            .expect("single-pool native handles must bind to their actual instance");
        for (slot, disk) in disks.iter().enumerate() {
            assert_eq!(
                disk.as_ref().expect("single-pool fixture disk").get_disk_id().await.unwrap(),
                Some(pool.format.erasure.sets[set.set_index][slot]),
                "single-pool fixture must bind every formatted physical slot"
            );
        }
    }
    let store = std::sync::Arc::new(crate::store::ECStore {
        id: uuid::Uuid::new_v4(),
        disk_map: std::collections::HashMap::new(),
        pools,
        peer_sys: crate::cluster::rpc::S3PeerSys::new_with_instance_ctx(&endpoint_pools, ctx.clone()),
        pool_meta: tokio::sync::RwLock::new(pool_meta),
        rebalance_meta: tokio::sync::RwLock::new(Some(meta)),
        decommission_cancelers: tokio::sync::RwLock::new(vec![None]),
        start_gate: tokio::sync::Mutex::new(()),
        pool_meta_save_gate: tokio::sync::Mutex::default(),
        ctx,
        bucket_fence_registry: std::sync::Arc::default(),
    });
    (temp_dirs, store)
}

#[cfg(test)]
pub(crate) async fn test_two_pool_stores(
    rebalance_meta: Option<RebalanceMeta>,
) -> (
    Vec<tempfile::TempDir>,
    std::sync::Arc<crate::store::ECStore>,
    std::sync::Arc<crate::store::ECStore>,
) {
    test_pool_stores_with_contexts(rebalance_meta, false, 2, 2).await
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) async fn test_three_pool_stores(
    rebalance_meta: Option<RebalanceMeta>,
) -> (
    Vec<tempfile::TempDir>,
    std::sync::Arc<crate::store::ECStore>,
    std::sync::Arc<crate::store::ECStore>,
) {
    test_pool_stores_with_contexts(rebalance_meta, false, 3, 2).await
}

#[cfg(test)]
pub(crate) async fn test_two_pool_stores_with_isolated_node_contexts(
    rebalance_meta: Option<RebalanceMeta>,
) -> (
    Vec<tempfile::TempDir>,
    std::sync::Arc<crate::store::ECStore>,
    std::sync::Arc<crate::store::ECStore>,
) {
    test_pool_stores_with_contexts(rebalance_meta, true, 2, 2).await
}

#[cfg(test)]
pub(crate) async fn promote_test_pool_meta_to_v2(store: &std::sync::Arc<crate::store::ECStore>) {
    let mut pool_meta = store.pool_meta.read().await.clone();
    pool_meta.version = crate::core::pools::POOL_META_VERSION;
    pool_meta
        .save(store.pools.clone())
        .await
        .expect("test pool metadata should be promoted to V2");
    *store.pool_meta.write().await = pool_meta;
}

#[cfg(test)]
pub(crate) async fn test_three_pool_stores_with_isolated_node_contexts(
    rebalance_meta: Option<RebalanceMeta>,
) -> (
    Vec<tempfile::TempDir>,
    std::sync::Arc<crate::store::ECStore>,
    std::sync::Arc<crate::store::ECStore>,
) {
    test_pool_stores_with_contexts(rebalance_meta, true, 3, 2).await
}

/// Four drives per set, so a set can hold metadata on fewer than its read
/// quorum of drives while each drive stays online.
#[cfg(all(test, feature = "test-util"))]
pub(crate) async fn test_three_pool_stores_with_four_disk_sets_with_isolated_node_contexts(
    rebalance_meta: Option<RebalanceMeta>,
) -> (
    Vec<tempfile::TempDir>,
    std::sync::Arc<crate::store::ECStore>,
    std::sync::Arc<crate::store::ECStore>,
) {
    test_pool_stores_with_contexts(rebalance_meta, true, 3, 4).await
}

#[cfg(all(test, feature = "test-util"))]
pub(crate) async fn test_three_pool_stores_with_three_disk_sets_with_isolated_node_contexts(
    rebalance_meta: Option<RebalanceMeta>,
) -> (
    Vec<tempfile::TempDir>,
    std::sync::Arc<crate::store::ECStore>,
    std::sync::Arc<crate::store::ECStore>,
) {
    test_pool_stores_with_contexts(rebalance_meta, true, 3, 3).await
}

#[cfg(test)]
async fn test_pool_stores_with_contexts(
    rebalance_meta: Option<RebalanceMeta>,
    isolate_node_contexts: bool,
    pool_count: usize,
    set_drive_count: usize,
) -> (
    Vec<tempfile::TempDir>,
    std::sync::Arc<crate::store::ECStore>,
    std::sync::Arc<crate::store::ECStore>,
) {
    crate::services::notification_sys::install_cross_pool_fence_fleet_proof_for_test();
    use crate::core::pools::{POOL_META_VERSION, PoolMeta, PoolMetaWriteState, persist_pool_meta_identity_for_startup};
    use crate::layout::endpoints::{EndpointServerPools, SetupType};

    let ctx = std::sync::Arc::new(crate::runtime::instance::InstanceContext::new());
    ctx.update_erasure_type(SetupType::DistErasure).await;
    let deployment_id = uuid::Uuid::new_v4();
    ctx.set_deployment_id(deployment_id);
    let mut temp_dirs = Vec::new();
    let mut pools = Vec::with_capacity(pool_count);
    for pool_index in 0..pool_count {
        let (pool_temp_dirs, pool) = crate::core::sets::make_local_two_set_sets_for_pool_with_drive_count_and_ctx(
            std::sync::Arc::clone(&ctx),
            pool_index,
            set_drive_count,
        )
        .await;
        temp_dirs.extend(pool_temp_dirs);
        pools.push(pool);
    }
    {
        let local_disk_map = ctx.local_disk_map();
        let mut local_disk_map = local_disk_map.write().await;
        for pool in &pools {
            for set in &pool.disk_set {
                for disk in set.disks.read().await.iter().flatten() {
                    local_disk_map.insert(disk.endpoint().to_string(), Some(disk.clone()));
                }
            }
        }
    }
    let mut pool_meta =
        PoolMeta::new(&pools, &PoolMeta::default()).expect("new pool topology should produce valid pool metadata");
    pool_meta.version = POOL_META_VERSION;
    pool_meta
        .save_for_startup(pools.clone())
        .await
        .expect("baseline pool metadata should be persisted");
    let mut pool_meta_write_state = PoolMetaWriteState::for_startup(deployment_id, true);
    persist_pool_meta_identity_for_startup(pools.clone(), &mut pool_meta_write_state, false)
        .await
        .expect("pending pool metadata identity should be persisted");
    let replica_state = pool_meta
        .load_no_lock_from_replicas_observing(pools.clone(), &mut pool_meta_write_state)
        .await
        .expect("baseline pool metadata should remain readable");
    pool_meta_write_state.observe_replicas(replica_state);
    persist_pool_meta_identity_for_startup(pools.clone(), &mut pool_meta_write_state, true)
        .await
        .expect("initialized pool metadata identity should be persisted");
    if let Some(meta) = rebalance_meta.as_ref() {
        meta.save(pools[0].clone())
            .await
            .expect("active rebalance metadata should be persisted");
    }
    let endpoint_pools: EndpointServerPools = pools.iter().map(|pool| pool.endpoints.clone()).collect::<Vec<_>>().into();
    ctx.set_endpoints(endpoint_pools.clone());
    let multipart_authority = ctx
        .bind_multipart_proof_topology(&endpoint_pools)
        .expect("the fixture's complete topology should contain only its local disks");
    for pool in &pools {
        for set in &pool.disk_set {
            let disks = set.disks.read().await;
            crate::disk::multipart_source::bind_multipart_proof_snapshot(&disks, &multipart_authority)
                .expect("fixture disks should bind to their complete instance topology");
            for (slot, disk) in disks.iter().enumerate() {
                assert_eq!(
                    disk.as_ref().expect("fixture disk").get_disk_id().await.unwrap(),
                    Some(pool.format.erasure.sets[set.set_index][slot]),
                    "fixture must bind its actual formatted cohort"
                );
            }
        }
    }
    let other_ctx = if isolate_node_contexts {
        let other_ctx = std::sync::Arc::new(crate::runtime::instance::InstanceContext::new());
        other_ctx.update_erasure_type(SetupType::DistErasure).await;
        other_ctx.set_deployment_id(deployment_id);
        other_ctx.set_endpoints(endpoint_pools.clone());
        other_ctx
    } else {
        std::sync::Arc::clone(&ctx)
    };
    let other_pools = if isolate_node_contexts {
        let peer_authority = other_ctx
            .bind_multipart_proof_topology(&endpoint_pools)
            .expect("the simulated peer should validate the same complete physical topology");
        let mut peer_pools = pools.clone();
        let mut peer_disk_map = std::collections::HashMap::new();
        for pool in &mut peer_pools {
            let pool = std::sync::Arc::make_mut(pool);
            pool.set_instance_ctx_for_test(std::sync::Arc::clone(&other_ctx));
            for set in &mut pool.disk_set {
                let set = std::sync::Arc::make_mut(set);
                let mut peer_disks = Vec::with_capacity(set.set_endpoints.len());
                let source_disks = set.disks.read().await;
                for (slot, endpoint) in set.set_endpoints.iter().enumerate() {
                    // Share durable contents and lock clients, while each node
                    // context owns its independently opened native handles.
                    let disk = crate::disk::new_disk(
                        endpoint,
                        &crate::disk::DiskOption {
                            cleanup: false,
                            health_check: false,
                        },
                    )
                    .await
                    .expect("open the peer's independent local disk handle");
                    assert!(!std::sync::Arc::ptr_eq(&disk, source_disks[slot].as_ref().expect("source fixture disk")));
                    assert_eq!(disk.get_disk_id().await.unwrap(), Some(pool.format.erasure.sets[set.set_index][slot]));
                    peer_disk_map.insert(endpoint.to_string(), Some(disk.clone()));
                    peer_disks.push(Some(disk));
                }
                drop(source_disks);
                crate::disk::multipart_source::bind_multipart_proof_snapshot(&peer_disks, &peer_authority)
                    .expect("independent peer handles should bind only to their own instance");
                set.disks = std::sync::Arc::new(tokio::sync::RwLock::new(peer_disks));
            }
        }
        *other_ctx.local_disk_map().write().await = peer_disk_map;
        peer_pools
    } else {
        pools.clone()
    };
    let make_store = |store_ctx: std::sync::Arc<crate::runtime::instance::InstanceContext>,
                      mut store_pools: Vec<std::sync::Arc<crate::core::sets::Sets>>| {
        for pool in &mut store_pools {
            std::sync::Arc::make_mut(pool).set_instance_ctx_for_test(std::sync::Arc::clone(&store_ctx));
        }
        std::sync::Arc::new(crate::store::ECStore {
            id: uuid::Uuid::new_v4(),
            disk_map: std::collections::HashMap::new(),
            pools: store_pools,
            peer_sys: crate::cluster::rpc::S3PeerSys::new_with_instance_ctx(&endpoint_pools, std::sync::Arc::clone(&store_ctx)),
            pool_meta: tokio::sync::RwLock::new(pool_meta.clone()),
            rebalance_meta: tokio::sync::RwLock::new(rebalance_meta.clone()),
            decommission_cancelers: tokio::sync::RwLock::new(vec![None; pool_count]),
            start_gate: tokio::sync::Mutex::new(()),
            pool_meta_save_gate: tokio::sync::Mutex::new(pool_meta_write_state.independent_clone_for_test()),
            ctx: store_ctx,
            bucket_fence_registry: std::sync::Arc::default(),
        })
    };
    let store = make_store(ctx, pools);
    let other_store = make_store(other_ctx, other_pools);
    if isolate_node_contexts {
        crate::bucket::metadata_sys::init_bucket_metadata_sys(std::sync::Arc::clone(&store), Vec::new()).await;
        crate::bucket::metadata_sys::init_bucket_metadata_sys(std::sync::Arc::clone(&other_store), Vec::new()).await;
    } else {
        crate::bucket::metadata_sys::init_bucket_metadata_sys(std::sync::Arc::clone(&store), Vec::new()).await;
    }
    (temp_dirs, store, other_store)
}

#[cfg(test)]
mod rebalance_unit_tests;
