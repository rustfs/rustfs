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

//! Authorization shapes that only migration batch C (rustfs/backlog#2765) uses: GetObject, HeadObject and GetObjectAttributes.
//!
//! Responsible for: the owned authorization function of each such operation, generic over
//! `AuthorizationRequest`, so the legacy `S3Access` hook and `Served::authorize` call the same code.
//! Not responsible for: shapes more than one batch shares (those stay in `storage/access.rs`), the
//! policy evaluation itself (`authorize_request`), or the operation's use case.
//! Upstream: the legacy hook in `storage/access.rs` and the batch's `server/gateway_handlers/object_read.rs`.
//! Downstream: the shared shapes in `storage/access.rs`.

use crate::storage::access::{
    AuthorizationRequest, S3Result, ServerContextSlot, authorize_request, capture_odm_read_generation, req_info_mut,
    versioned_read_action,
};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_utils::http::{SUFFIX_SOURCE_REPLICATION_CHECK, get_header};

/// Both HEAD edges retain the captured ODM generation and the additional replication decision.
pub(crate) async fn authorize_head_object<R: AuthorizationRequest + ?Sized>(
    server_ctx: &ServerContextSlot,
    req: &mut R,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<()> {
    let source_generation = capture_odm_read_generation(server_ctx, req, bucket).await;
    let info = req_info_mut(req)?;
    info.bucket = Some(bucket.to_owned());
    info.object = Some(key.to_owned());
    info.version_id = version_id.map(str::to_owned);
    authorize_request(req, versioned_read_action(version_id)).await?;
    if get_header(req.headers(), SUFFIX_SOURCE_REPLICATION_CHECK).as_deref() == Some("true") {
        authorize_request(req, Action::S3Action(S3Action::ReplicateObjectAction)).await?;
        req_info_mut(req)?.replication_request_authorized = true;
    }
    req.install_odm_read_generation(source_generation);
    Ok(())
}

#[cfg(test)]
#[path = "object_read_test.rs"]
mod tests;
