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
use crate::app::storage_api::s3::{ListObjectsV2Input, ListObjectsV2Output, S3Request, S3Response, S3Result};
use std::future::Future;

/// Calls `DefaultBucketUsecase::execute_list_objects_v2` with today's arguments.
///
/// Owned by the ListObjectsV2 batch (B2). Tests outside that batch's files call this seam instead of the use-case
/// method; when batch B2 changes the use-case signature it changes only this body.
pub(crate) fn list_objects_v2(
    usecase: &DefaultBucketUsecase,
    req: S3Request<ListObjectsV2Input>,
) -> impl Future<Output = S3Result<S3Response<ListObjectsV2Output>>> + '_ {
    usecase.execute_list_objects_v2(req)
}
