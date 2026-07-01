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

use crate::bucket::bucket_target_sys::{BucketTargetError, BucketTargetSys};

pub(crate) use crate::bucket::bucket_target_sys::{
    AdvancedPutOptions, PutObjectOptions, PutObjectPartOptions, RemoveObjectOptions, TargetClient,
};
pub(crate) use crate::bucket::target::BucketTargets;

pub(crate) struct ReplicationTargetStore;

impl ReplicationTargetStore {
    pub(crate) async fn list_bucket_targets(bucket: &str) -> Result<BucketTargets, BucketTargetError> {
        BucketTargetSys::get().list_bucket_targets(bucket).await
    }

    pub(crate) async fn remote_target_client(bucket: &str, arn: &str) -> Option<Arc<TargetClient>> {
        BucketTargetSys::get().get_remote_target_client(bucket, arn).await
    }

    pub(crate) async fn target_is_offline(target_client: &TargetClient) -> bool {
        BucketTargetSys::get().is_offline(&target_client.to_url()).await
    }
}
