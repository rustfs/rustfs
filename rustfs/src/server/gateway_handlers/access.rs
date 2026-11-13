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

//! The shared access phase of the gateway edge: what the legacy `S3Access::check` runs for every
//! operation, fed from the gateway's verdict.
//!
//! Responsible for: [`admit`], which builds the envelope with the access markers, installs the
//! caller and runs the operation-aware admission, each through the function the legacy check
//! calls (`storage/access.rs`).
//! NOT responsible for: an operation's own authorization (`Served::authorize`), or verifying the
//! signature (the gateway did, and only a verified caller has a principal).
//! Upstream: `GatewayBackend::serve`. Downstream: the storage layer's access functions.

use crate::app::RequestEnvelope;
use crate::runtime_sources::ServerContextSlot;
use crate::storage_api::server::http::access::{VerifiedSignature, admit_operation, resolve_req_info};
use rustfs_gateway::sig::{SigFamily, SigLocation};
use rustfs_gateway::{RequestContextView, RequestPrincipal};
use rustfs_s3_types::S3Result;
use std::sync::Arc;

/// Runs the shared access phase for one accepted request and returns its envelope.
///
/// # Errors
///
/// What the legacy access check answers: the credential refusals of the key lookup,
/// `InvalidRequest` for a misplaced capability query, `AccessDenied` for a refused license, and
/// `InternalError` when IAM or the license cannot be read or the request cannot be read back.
pub(super) async fn admit(server_ctx: &Arc<ServerContextSlot>, context: &RequestContextView) -> S3Result<RequestEnvelope> {
    let signature = verified_signature(context);
    let mut envelope = RequestEnvelope::from_gateway(context, server_ctx, signature)?;
    let credentials = envelope.credentials();
    let access_key = credentials.map(|credentials| credentials.access_key().to_owned());
    let session_token = credentials
        .and_then(|credentials| credentials.session_token())
        .unwrap_or_default()
        .to_owned();
    let req_info =
        resolve_req_info(server_ctx, access_key.as_deref(), &session_token, envelope.request_context().cloned()).await?;
    envelope.install_req_info(req_info);
    admit_operation(envelope.headers(), envelope.uri().query(), context.operation(), signature)?;
    Ok(envelope)
}

/// The gateway's verdict as the access markers read it: a SigV4 signature in the header or the
/// query. SigV2, a POST policy and an anonymous request mark nothing, as on the legacy edge.
pub(super) fn verified_signature(context: &RequestContextView) -> VerifiedSignature {
    match context.principal().map(RequestPrincipal::scheme) {
        Some(scheme) if scheme.family() == SigFamily::V4 => VerifiedSignature {
            sigv4: matches!(scheme.location(), SigLocation::Header | SigLocation::Query),
            presigned: scheme.location() == SigLocation::Query,
        },
        _ => VerifiedSignature::default(),
    }
}
