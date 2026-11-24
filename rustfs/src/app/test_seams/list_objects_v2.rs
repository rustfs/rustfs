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

use crate::app::bucket_usecase::DefaultBucketUsecase;

/// Calls `DefaultBucketUsecase::execute_list_objects_v2` with owned inputs.
pub(crate) async fn list_objects_v2_owned(
    usecase: &DefaultBucketUsecase,
    envelope: crate::app::RequestEnvelope,
    input: rustfs_gateway::dto::ListObjectsV2Input,
) -> rustfs_s3_types::S3Result<rustfs_gateway::dto::ListObjectsV2Output> {
    usecase.execute_list_objects_v2(envelope, input).await
}
