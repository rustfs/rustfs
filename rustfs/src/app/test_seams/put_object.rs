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

use crate::app::RequestEnvelope;
use crate::app::object_usecase::{DefaultObjectUsecase, PutObjectWriteInput, PutObjectWriteOutput};
use std::future::Future;

/// Calls `DefaultObjectUsecase::execute_put_object_owned`, the entry point the gateway handler calls.
///
/// Owned by the PutObject batch (D). Tests outside that batch's files call this seam instead of the use-case method;
/// when batch D changes the use-case signature it changes only this body.
pub(crate) fn put_object(
    usecase: &DefaultObjectUsecase,
    envelope: RequestEnvelope,
    input: PutObjectWriteInput,
) -> impl Future<Output = rustfs_s3_types::S3Result<PutObjectWriteOutput>> + '_ {
    Box::pin(usecase.execute_put_object_owned(envelope, input))
}
