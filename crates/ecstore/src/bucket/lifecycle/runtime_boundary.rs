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

use s3s::dto::BucketLifecycleConfiguration;
use tokio::sync::RwLock;

use crate::bucket::lifecycle::bucket_lifecycle_ops::{ExpiryState, TransitionState};
use crate::runtime::sources;
use crate::services::tier::tier::TierConfigMgr;
use crate::store::ECStore;

pub(crate) fn expiry_state_handle() -> Arc<RwLock<ExpiryState>> {
    sources::expiry_state_handle()
}

pub(crate) fn transition_state_handle() -> Arc<TransitionState> {
    sources::transition_state_handle()
}

pub(crate) fn tier_config_mgr_handle() -> Arc<RwLock<TierConfigMgr>> {
    sources::tier_config_mgr_handle()
}

pub(crate) fn object_store_handle() -> Option<Arc<ECStore>> {
    sources::object_store_handle()
}

pub(crate) fn default_local_node_name() -> String {
    sources::default_local_node_name()
}

pub(crate) fn deployment_id() -> Option<String> {
    sources::deployment_id()
}

pub(crate) async fn bucket_lifecycle_config(bucket: &str) -> Option<BucketLifecycleConfiguration> {
    sources::bucket_lifecycle_config(bucket).await
}
