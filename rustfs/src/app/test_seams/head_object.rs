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

use crate::app::object_usecase::DefaultObjectUsecase;
use crate::app::storage_api::s3::{HeadObjectInput, HeadObjectOutput, S3Request, S3Response, S3Result};

/// Calls `DefaultObjectUsecase::execute_head_object` with today's arguments.
///
/// Owned by the HeadObject batch (C). Tests outside that batch's files call this seam instead of the use-case method;
/// when batch C changes the use-case signature it changes only this body.
pub(crate) async fn head_object(
    usecase: &DefaultObjectUsecase,
    req: S3Request<HeadObjectInput>,
) -> S3Result<S3Response<HeadObjectOutput>> {
    use crate::app::object_usecase::HeadObjectFields;
    use crate::app::{Answered, RequestEnvelope};
    rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::HeadObject);
    let envelope = RequestEnvelope::from_s3s(&req);
    let Answered { output, status, headers } = usecase
        .execute_head_object_fields(envelope, HeadObjectFields::from_s3s(req.input))
        .await?;
    let mut response = S3Response::new(output.into_s3s().map_err(crate::app::dto_values::seam_error)?);
    response.status = status;
    response.headers = headers;
    Ok(response)
}
