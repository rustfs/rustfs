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

mod config;
pub mod datatypes;
mod replication_bandwidth_boundary;
mod replication_config_store;
mod replication_error_boundary;
mod replication_event_sink;
mod replication_filemeta_boundary;
mod replication_lifecycle_bridge;
mod replication_lock_boundary;
mod replication_metadata_boundary;
mod replication_msgp_boundary;
pub(crate) mod replication_pool;
mod replication_resyncer;
mod replication_scanner_bridge;
mod replication_state;
mod replication_storage_boundary;
mod replication_tagging_boundary;
mod replication_target_boundary;
mod replication_versioning_boundary;
mod rule;
mod runtime_boundary;

pub use config::{ObjectOpts, ReplicationConfigurationExt};
pub use datatypes::ResyncStatusType;
pub(crate) use replication_lifecycle_bridge::{ReplicationLifecycleBridge, ReplicationLifecycleConfig};
pub use replication_pool::{
    DynReplicationPool, ReplicationHealQueueResult, ReplicationPoolTrait, ReplicationQueueAdmission, get_global_replication_pool,
    get_global_replication_stats, init_background_replication, schedule_replication, schedule_replication_delete,
};
pub use replication_resyncer::{
    BucketReplicationResyncStatus, DeletedObjectReplicationInfo, MustReplicateOptions, ReplicationConfig, ResyncOpts,
    TargetReplicationResyncStatus, check_replicate_delete, get_must_replicate_options, must_replicate,
};
pub(crate) use replication_resyncer::{decode_resync_file, encode_resync_file};
pub use replication_scanner_bridge::ReplicationScannerBridge;
pub use replication_state::{BucketStats, ReplicationStats};
pub use replication_storage_boundary::{ReplicationObjectIO, ReplicationStorage};
