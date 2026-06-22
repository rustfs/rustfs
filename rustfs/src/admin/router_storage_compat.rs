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

pub(crate) use crate::admin::storage_compat::{
    AdminReplicationConfigExt, AdminVersioningConfigExt, GLOBAL_BOOT_TIME, PeerRestClient, StorageError,
    get_global_bucket_monitor, get_global_deployment_id, get_global_notification_sys, get_global_region,
    read_admin_config_without_migrate,
};

pub(crate) mod bandwidth {
    pub(crate) mod monitor {
        pub(crate) use crate::admin::storage_compat::bandwidth::monitor::BandwidthDetails;
    }
}

pub(crate) mod bucket_target_sys {
    pub(crate) use crate::admin::storage_compat::bucket_target_sys::{
        AdvancedPutOptions, BucketTargetSys, PutObjectOptions, RemoveObjectOptions, S3ClientError, TargetClient,
    };
}

pub(crate) mod metadata {
    pub(crate) use crate::admin::storage_compat::metadata::BUCKET_TARGETS_FILE;
}

pub(crate) use crate::admin::storage_compat::metadata_sys;

pub(crate) mod replication {
    pub(crate) use crate::admin::storage_compat::replication::{
        BucketReplicationResyncStatus, BucketStats, GLOBAL_REPLICATION_STATS, ObjectOpts, ResyncOpts, get_global_replication_pool,
    };

    #[cfg(test)]
    pub(crate) use crate::admin::storage_compat::replication::{ResyncStatusType, TargetReplicationResyncStatus};
}

pub(crate) mod target {
    pub(crate) use crate::admin::storage_compat::target::{BucketTarget, BucketTargetType, BucketTargets};
}

pub(crate) mod versioning_sys {
    pub(crate) use crate::admin::storage_compat::versioning_sys::BucketVersioningSys;
}
