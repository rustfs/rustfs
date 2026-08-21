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

#[cfg(test)]
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
mod rebalance_unit_tests;
