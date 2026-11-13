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

//! The gateway operations of migration batch E2 (rustfs/backlog#2765): DeleteObject, DeleteObjects, RestoreObject and SelectObjectContent.
//! DeleteObject is served; the batch appends the others here.

use super::{OpRow, Served, row};
use crate::app::RequestEnvelope;
use crate::app::object_usecase::DefaultObjectUsecase;
use crate::runtime_sources::AppContext;
use crate::storage_api::server::http::access::access_owned::object_misc::authorize_delete_object;
use rustfs_gateway::{Req, Resp, dto};
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3Result, s3_error};
use std::sync::Arc;

pub(super) const ROWS: &[OpRow] = &[row::<dto::DeleteObject>()];

impl Served for dto::DeleteObject {
    const METRIC: S3Operation = S3Operation::DeleteObject;

    async fn authorize(envelope: &mut RequestEnvelope, request: &Req<Self>) -> S3Result<()> {
        let input = request.input();
        // Cloned out first: the decision borrows the envelope mutably.
        let server_ctx = envelope
            .server_context()
            .cloned()
            .ok_or_else(|| s3_error!(InternalError, "server context is missing"))?;
        let version_id = input.version_id.as_deref();
        Ok(authorize_delete_object(&server_ctx, envelope, input.bucket.as_str(), input.key.as_str(), version_id).await?)
    }

    async fn execute(context: Option<Arc<AppContext>>, envelope: RequestEnvelope, request: Req<Self>) -> S3Result<Resp<Self>> {
        let output = DefaultObjectUsecase::with_context(context)
            .execute_delete_object(envelope, request.into_input())
            .await?;
        Ok(Resp::new(output))
    }
}
