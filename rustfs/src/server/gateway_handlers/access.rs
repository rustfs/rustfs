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
//! Also the gateway-edge form of the one authorization shape several families share:
//! [`authorize_bucket_config_mutation`].
//! NOT responsible for: an operation's own authorization (`Served::authorize`), or verifying the
//! signature (the gateway did, and only a verified caller has a principal).
//! Upstream: `GatewayBackend::serve`. Downstream: the storage layer's access functions.

use crate::app::RequestEnvelope;
use crate::runtime_sources::ServerContextSlot;
use crate::storage_api::server::http::access::{
    VerifiedSignature, admit_operation, authorize_bucket_config_mutation as authorize_config_mutation, resolve_req_info,
};
use rustfs_gateway::sig::{SigFamily, SigLocation};
use rustfs_gateway::{RequestContextView, RequestPrincipal};
use rustfs_policy::policy::action::Action;
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

/// The bucket-configuration mutation shape on the gateway edge: the shared function the legacy
/// hook calls, with the server context the envelope carries.
///
/// # Errors
///
/// `InternalError` when the envelope carries no server context; otherwise what the shared
/// function answers.
pub(super) async fn authorize_bucket_config_mutation(
    envelope: &mut RequestEnvelope,
    bucket: &str,
    action: Action,
) -> S3Result<()> {
    let server_ctx = envelope
        .server_context()
        .cloned()
        .ok_or_else(|| rustfs_s3_types::s3_error!(InternalError, "the request carries no server context"))?;
    Ok(authorize_config_mutation(&server_ctx, envelope, bucket, action).await?)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::RequestHead;
    use http::{Extensions, HeaderMap, Method};
    use rustfs_policy::policy::action::S3Action;

    /// Without a server context there is no lock to take and no incarnation to snapshot, so the
    /// mutation is refused rather than decided.
    #[tokio::test]
    async fn a_bucket_config_mutation_without_a_server_context_is_refused() {
        let head = RequestHead {
            method: Method::PUT,
            uri: "/bucket?cors".parse().expect("valid URI"),
            headers: HeaderMap::new(),
            credentials: None,
            region: None,
            service: None,
        };
        let mut envelope = RequestEnvelope::from_parts(head, Extensions::new());

        let action = Action::S3Action(S3Action::PutBucketCorsAction);
        let refusal = authorize_bucket_config_mutation(&mut envelope, "bucket", action)
            .await
            .unwrap_err();

        assert_eq!(refusal.code(), &rustfs_s3_types::S3ErrorCode::InternalError);
        assert_eq!(refusal.message(), Some("the request carries no server context"));
    }
}
