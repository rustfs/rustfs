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

//! The access phase of a gateway handler: the legacy access hooks in their owned form.
//!
//! Responsible for: [`check`], what the legacy access phase runs once per request before any
//! operation hook. Per-operation hooks join it here as their operations are registered.
//! NOT responsible for: the decisions themselves. Every function here calls the function the
//! legacy access phase calls (`storage/access.rs`), so the two stacks cannot answer differently.
//! Upstream: each `impl Handler`. Downstream: the storage layer's access functions.
//!
//! The caller is resolved here on every request, and again by the gateway's `Authorizer` before
//! it: the pinned gateway hands a handler no policy snapshot, so one resolution cannot be carried
//! from the authorization stages to the handler.

use crate::app::RequestEnvelope;
use crate::runtime_sources::ServerContextSlot;
use crate::storage_api::server::http::access::{admit_operation, resolve_req_info};
use rustfs_s3_types::S3Result;
use std::sync::Arc;

/// Runs the per-request access check for `operation` and installs the caller in `envelope`.
///
/// The caller is the credential IAM holds for the access key the gateway verified, checked
/// against the presented session token; an anonymous request resolves to no caller. The signed
/// upload-size capabilities are then refused on any operation but their own, and the license is
/// checked.
///
/// # Errors
///
/// What the legacy access phase answers: the credential refusals of the key lookup,
/// `InvalidRequest` for a misplaced capability query, `AccessDenied` for a refused license, and
/// `InternalError` when IAM or the license cannot be read.
pub(crate) async fn check(
    server_ctx: &Arc<ServerContextSlot>,
    envelope: &mut RequestEnvelope,
    operation: &'static str,
) -> S3Result<()> {
    let credentials = envelope.credentials();
    let access_key = credentials.map(|credentials| credentials.access_key().to_owned());
    let session_token = credentials
        .and_then(|credentials| credentials.session_token())
        .unwrap_or_default()
        .to_owned();
    let request_context = envelope.request_context().cloned();
    let req_info = resolve_req_info(server_ctx, access_key.as_deref(), &session_token, request_context).await?;
    envelope.install_req_info(req_info);
    admit_operation(envelope.headers(), envelope.uri().query(), operation)?;
    Ok(())
}
