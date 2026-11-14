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

//! CompleteMultipartUpload gateway registration and its shared authorization.

use super::{OpRow, Served, row};
use crate::app::{RequestEnvelope, multipart_usecase::DefaultMultipartUsecase};
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::access_owned::object_commit::authorize_complete_multipart_upload;
use rustfs_gateway::{Req, Resp, dto};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3Result, s3_error};
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[row::<dto::CompleteMultipartUpload>()];

impl Served for dto::CompleteMultipartUpload {
    const METRIC: S3Operation = S3Operation::CompleteMultipartUpload;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let server = envelope
            .server_context()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "server context is not initialized"))?;
        let input = request.input();
        let specific_etag = input.if_match.as_deref().is_some_and(|value| value.trim() != "*");
        Ok(
            authorize_complete_multipart_upload(envelope, &server, input.bucket.as_str(), input.key.as_str(), specific_etag)
                .await?,
        )
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultMultipartUsecase::with_context(context)
            .execute_complete_multipart_upload(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}
