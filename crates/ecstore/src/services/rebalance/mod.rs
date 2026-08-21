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
pub(crate) const REBALANCE_SOURCE_CLEANUP_MAX_DEFERS: usize = 3;
const REBALANCE_MAX_ATTEMPTS_ENV: &str = "RUSTFS_REBALANCE_MAX_ATTEMPTS";
const REBALANCE_STOP_PROPAGATION_ERROR_PREFIX: &str = "rebalance stop propagation incomplete: ";
const REBALANCE_LISTING_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
const REBALANCE_MIGRATION_RETRY_BASE_DELAY: Duration = Duration::from_millis(250);
const REBALANCE_MIGRATION_LOCK_RETRY_CAP: Duration = Duration::from_secs(10);
const REBALANCE_DEFERRED_ENTRY_ERROR_PREFIX: &str = "deferred transient rebalance entry failure:";
pub(crate) const REBALANCE_SOURCE_CLEANUP_DEFERRED_ERROR_PREFIX: &str = "deferred rebalance source cleanup conflict:";
const REBALANCE_CLEANUP_WARNING_ENTRY_LIMIT: usize = 10;

mod control;
mod entry;
mod meta;
mod migration;
mod runtime;
mod types;
mod worker;

pub(crate) use meta::is_rebalance_conflicting_with_decommission;
pub use meta::{decode_rebalance_stop_propagation_record, encode_rebalance_stop_propagation_record};
pub use types::{
    DiskStat, RebalSaveOpt, RebalStatus, RebalanceCleanupWarningEntry, RebalanceCleanupWarnings, RebalanceInfo, RebalanceMeta,
    RebalanceStats, RebalanceStopPropagationRecord,
};
use types::{RebalanceBucketConfigs, RebalanceBucketOutcome, RebalanceEntryOutcome};

#[cfg(any(test, feature = "test-util"))]
pub(crate) async fn test_store_with_persisted_rebalance_meta(
    meta: RebalanceMeta,
) -> (Vec<tempfile::TempDir>, std::sync::Arc<crate::store::ECStore>) {
    let ctx = std::sync::Arc::new(crate::runtime::instance::InstanceContext::new());
    let (temp_dirs, pool) = crate::core::sets::make_local_two_set_sets_with_ctx(ctx.clone()).await;
    meta.save(pool.clone())
        .await
        .expect("rebalance test metadata should be persisted");
    let endpoint_pools: crate::layout::endpoints::EndpointServerPools = vec![pool.endpoints.clone()].into();
    let store = std::sync::Arc::new(crate::store::ECStore {
        id: uuid::Uuid::new_v4(),
        disk_map: std::collections::HashMap::new(),
        pools: vec![pool],
        peer_sys: crate::cluster::rpc::S3PeerSys::new_with_instance_ctx(&endpoint_pools, ctx.clone()),
        pool_meta: tokio::sync::RwLock::new(crate::core::pools::PoolMeta::default()),
        rebalance_meta: tokio::sync::RwLock::new(Some(meta)),
        decommission_cancelers: tokio::sync::RwLock::new(vec![None]),
        start_gate: tokio::sync::Mutex::new(()),
        pool_meta_save_gate: tokio::sync::Mutex::new(()),
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
    use crate::core::pools::PoolMeta;
    use crate::layout::endpoints::{EndpointServerPools, SetupType};

    let ctx = std::sync::Arc::new(crate::runtime::instance::InstanceContext::new());
    ctx.update_erasure_type(SetupType::DistErasure).await;
    let (mut temp_dirs, first_pool) =
        crate::core::sets::make_local_two_set_sets_for_pool_with_ctx(std::sync::Arc::clone(&ctx), 0).await;
    let (second_temp_dirs, second_pool) =
        crate::core::sets::make_local_two_set_sets_for_pool_with_ctx(std::sync::Arc::clone(&ctx), 1).await;
    temp_dirs.extend(second_temp_dirs);
    let pools = vec![first_pool, second_pool];
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
    let pool_meta = PoolMeta::new(&pools, &PoolMeta::default());
    pool_meta
        .save(pools.clone())
        .await
        .expect("baseline pool metadata should be persisted");
    if let Some(meta) = rebalance_meta.as_ref() {
        meta.save(pools[0].clone())
            .await
            .expect("active rebalance metadata should be persisted");
    }
    let endpoint_pools: EndpointServerPools = pools.iter().map(|pool| pool.endpoints.clone()).collect::<Vec<_>>().into();
    ctx.set_endpoints(endpoint_pools.clone());
    let make_store = || {
        std::sync::Arc::new(crate::store::ECStore {
            id: uuid::Uuid::new_v4(),
            disk_map: std::collections::HashMap::new(),
            pools: pools.clone(),
            peer_sys: crate::cluster::rpc::S3PeerSys::new_with_instance_ctx(&endpoint_pools, std::sync::Arc::clone(&ctx)),
            pool_meta: tokio::sync::RwLock::new(pool_meta.clone()),
            rebalance_meta: tokio::sync::RwLock::new(rebalance_meta.clone()),
            decommission_cancelers: tokio::sync::RwLock::new(vec![None, None]),
            start_gate: tokio::sync::Mutex::new(()),
            pool_meta_save_gate: tokio::sync::Mutex::new(()),
            ctx: std::sync::Arc::clone(&ctx),
            bucket_fence_registry: std::sync::Arc::default(),
        })
    };
    (temp_dirs, make_store(), make_store())
}

#[cfg(test)]
mod rebalance_unit_tests;
