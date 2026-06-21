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

use std::sync::Arc;

use rustfs_ecstore::api::bucket as ecstore_bucket;

pub(crate) fn get_global_bucket_metadata_sys() -> Option<Arc<tokio::sync::RwLock<ecstore_bucket::metadata_sys::BucketMetadataSys>>>
{
    ecstore_bucket::metadata_sys::get_global_bucket_metadata_sys()
}

pub(crate) fn get_global_replication_pool() -> Option<Arc<ecstore_bucket::replication::DynReplicationPool>> {
    ecstore_bucket::replication::get_global_replication_pool()
}

pub(crate) fn replication_queue_current_count() -> Option<i64> {
    ecstore_bucket::replication::GLOBAL_REPLICATION_STATS.get().and_then(|stats| {
        stats
            .q_cache
            .try_lock()
            .ok()
            .map(|cache| cache.sr_queue_stats.curr.get_current_count())
    })
}
