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

use super::replication_pool::{ReplicationHealQueueResult, queue_replication_heal_internal};
use super::replication_resyncer::ReplicationConfig;
use super::replication_storage_boundary::ObjectInfo;

pub struct ReplicationScannerBridge;

impl ReplicationScannerBridge {
    pub async fn queue_heal(
        bucket: &str,
        oi: ObjectInfo,
        rcfg: ReplicationConfig,
        retry_count: u32,
    ) -> ReplicationHealQueueResult {
        queue_replication_heal_internal(bucket, oi, rcfg, retry_count).await
    }
}

#[cfg(test)]
mod tests {
    use super::super::replication_pool::ReplicationQueueAdmission;
    use super::*;

    #[tokio::test]
    async fn scanner_bridge_preserves_empty_config_skip_behavior() {
        let result = ReplicationScannerBridge::queue_heal("bucket", ObjectInfo::default(), ReplicationConfig::default(), 0).await;

        assert_eq!(result.admission, ReplicationQueueAdmission::Skipped);
    }
}
