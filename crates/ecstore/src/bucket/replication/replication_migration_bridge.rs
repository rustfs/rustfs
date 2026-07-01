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
use super::replication_resyncer::{BucketReplicationResyncStatus, decode_resync_file, encode_resync_file};

pub(crate) struct ReplicationMigrationBridge;

impl ReplicationMigrationBridge {
    pub(crate) fn decode_resync_status(data: &[u8]) -> Result<BucketReplicationResyncStatus> {
        decode_resync_file(data)
    }

    pub(crate) fn encode_resync_status(status: &BucketReplicationResyncStatus) -> Result<Vec<u8>> {
        encode_resync_file(status)
    }
}
