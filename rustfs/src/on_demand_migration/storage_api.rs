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

//! Storage capabilities used by on-demand migration orchestration.

#[cfg(test)]
pub(super) use crate::storage_api::on_demand_migration::test_support;
pub(super) use crate::storage_api::on_demand_migration::{
    BUCKET_CONFIG_PUBLISH_HOOK, BUCKET_META_PREFIX, BUCKET_ON_DEMAND_MIGRATION_CONFIG, ECStore, HTTPPreconditions, HTTPRangeSpec,
    NamespaceLocking, ObjectOperations, ObjectOptions, RUSTFS_META_BUCKET, StorageError, WriteCompletion,
    get_lock_acquire_timeout, get_on_demand_migration_config, get_on_demand_migration_config_in, read_config_with_metadata,
    remote_s3_client, save_config_with_opts,
};

pub(super) async fn local_node_name() -> String {
    rustfs_common::get_global_local_node_name().await
}
