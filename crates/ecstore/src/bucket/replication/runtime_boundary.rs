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

use crate::bucket::bandwidth::monitor::Monitor;
use crate::bucket::replication::replication_pool::DynReplicationPool;
use crate::bucket::replication::replication_state::ReplicationStats;
use crate::runtime::sources;
use crate::store::ECStore;

pub(crate) fn object_store_handle() -> Option<Arc<ECStore>> {
    sources::object_store_handle()
}

pub(crate) fn default_local_node_name() -> String {
    sources::default_local_node_name()
}

pub(crate) fn replication_pool() -> Option<Arc<DynReplicationPool>> {
    sources::replication_pool()
}

pub(crate) fn replication_stats() -> Option<Arc<ReplicationStats>> {
    sources::replication_stats()
}

pub(crate) fn replication_runtime_initialized() -> bool {
    sources::replication_runtime_initialized()
}

pub(crate) fn bucket_monitor() -> Option<Arc<Monitor>> {
    sources::bucket_monitor()
}
