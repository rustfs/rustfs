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

use rustfs_filemeta::ReplicateDecision;
use s3s::dto::ReplicationConfiguration;

use crate::bucket::lifecycle::lifecycle::ObjectOpts;
use crate::bucket::replication::{ReplicationLifecycleBridge, ReplicationLifecycleConfig};
use crate::object_api::{ObjectInfo, ObjectOptions};
use crate::storage_api_contracts::object::{DeletedObject, ObjectToDelete};

pub(crate) type LifecycleReplicationConfig = ReplicationLifecycleConfig;

pub(crate) fn new_replication_config(config: ReplicationConfiguration) -> LifecycleReplicationConfig {
    ReplicationLifecycleBridge::new_config(config)
}

pub(crate) fn has_pending_version_purge(config: &LifecycleReplicationConfig, obj: &ObjectOpts) -> bool {
    ReplicationLifecycleBridge::has_pending_version_purge(config, obj.name.as_str(), !obj.version_purge_status.is_empty())
}

pub(crate) async fn check_delete_replication(
    bucket: &str,
    object: ObjectToDelete,
    source: &ObjectInfo,
    opts: &ObjectOptions,
) -> ReplicateDecision {
    ReplicationLifecycleBridge::check_delete_replication(bucket, &object, source, opts).await
}

pub(crate) async fn schedule_delete(bucket: String, delete_object: DeletedObject) {
    ReplicationLifecycleBridge::schedule_delete(bucket, delete_object).await;
}
