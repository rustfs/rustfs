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

use super::replication_error_boundary::Result;
use crate::bucket::{versioning::VersioningApi as _, versioning_sys::BucketVersioningSys};
#[cfg(test)]
use s3s::dto::VersioningConfiguration;
#[cfg(test)]
use std::{collections::HashMap, sync::LazyLock, sync::Mutex};

#[cfg(test)]
static PREFIX_STATE_TEST_CONFIGS: LazyLock<Mutex<HashMap<String, VersioningConfiguration>>> =
    LazyLock::new(|| Mutex::new(HashMap::new()));

pub(crate) struct ReplicationVersioningStore;

impl ReplicationVersioningStore {
    #[cfg(test)]
    pub(crate) fn install_prefix_state_test_config(bucket: &str, config: VersioningConfiguration) {
        PREFIX_STATE_TEST_CONFIGS
            .lock()
            .expect("replication versioning test config lock should not be poisoned")
            .insert(bucket.to_string(), config);
    }

    pub(crate) async fn prefix_enabled(bucket: &str, prefix: &str) -> bool {
        BucketVersioningSys::prefix_enabled(bucket, prefix).await
    }

    pub(crate) async fn prefix_suspended(bucket: &str, prefix: &str) -> bool {
        BucketVersioningSys::prefix_suspended(bucket, prefix).await
    }

    pub(crate) async fn prefix_state(bucket: &str, prefix: &str) -> Result<(bool, bool)> {
        #[cfg(test)]
        if let Some(config) = PREFIX_STATE_TEST_CONFIGS
            .lock()
            .expect("replication versioning test config lock should not be poisoned")
            .remove(bucket)
        {
            return Ok((config.prefix_enabled(prefix), config.prefix_suspended(prefix)));
        }

        let config = BucketVersioningSys::get(bucket).await?;
        Ok((config.prefix_enabled(prefix), config.prefix_suspended(prefix)))
    }
}
