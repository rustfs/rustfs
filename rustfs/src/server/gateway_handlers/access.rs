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

//! The access phase for S3 operations served by the gateway.
//!
//! Responsible for: [`admit`], which builds the envelope with the access markers, installs the
//! caller and runs the operation-aware admission checks.
//! Also the gateway-edge form of the one authorization shape several families share:
//! [`authorize_bucket_config_mutation`].
//! NOT responsible for: an operation's own authorization (`Served::authorize`), or verifying the
//! signature (the gateway did, and only a verified caller has a principal).
//! Upstream: `GatewayBackend::serve`. Downstream: the storage layer's access functions.

use crate::app::RequestEnvelope;
use crate::runtime_sources::ServerContextSlot;
use crate::storage_api::server::http::access::{
    VerifiedSignature, admit_operation, authorize_bucket_config_mutation as authorize_config_mutation,
    authorize_bucket_tag_deletion as authorize_tag_deletion, resolve_req_info,
};
use rustfs_gateway::sig::{SigFamily, SigLocation};
use rustfs_gateway::{RequestContextView, RequestPrincipal};
use rustfs_policy::policy::action::Action;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};
use std::future::Future;
use std::sync::Arc;
use zeroize::Zeroizing;

/// Runs the shared access phase for one accepted request and returns its envelope.
///
/// # Errors
///
/// Credential errors from the key lookup, `MethodNotAllowed` for ACL deletion,
/// `InvalidRequest` for a misplaced capability query, `MissingContentMD5` for a SigV2
/// control-plane mutation without a digest, `AccessDenied` for a refused license, and
/// `InternalError` when IAM or the license cannot be read or the request cannot be read back.
pub(super) fn admit<'a>(
    server_ctx: &'a Arc<ServerContextSlot>,
    context: &RequestContextView,
) -> impl Future<Output = S3Result<RequestEnvelope>> + Send + use<'a> {
    let signature = verified_signature(context);
    let is_sigv2 = context
        .principal()
        .map(RequestPrincipal::scheme)
        .is_some_and(|scheme| scheme.family() == SigFamily::V2);
    let operation = context.operation();
    let envelope = RequestEnvelope::from_gateway(context, server_ctx, signature);
    let keystone = context.header_token_principal().map(|_| {
        super::envelope::keystone_identity(context).map(|identity| (identity.credentials().clone(), identity.had_session_token()))
    });
    async move {
        let mut envelope = envelope?;
        let credentials = envelope.credentials();
        let access_key = credentials.map(|credentials| credentials.access_key().to_owned());
        let session_token = Zeroizing::new(
            credentials
                .and_then(|credentials| credentials.session_token())
                .unwrap_or_default()
                .to_owned(),
        );
        let req_info = if let Some(identity) = keystone {
            let (credentials, had_session_token) = identity?;
            let mut info = resolve_req_info(server_ctx, None, "", envelope.request_context().cloned()).await?;
            info.is_owner = crate::auth::keystone_is_owner(&credentials);
            info.cred = Some(credentials);
            info.keystone_session_present = Some(had_session_token);
            info
        } else {
            resolve_req_info(server_ctx, access_key.as_deref(), &session_token, envelope.request_context().cloned()).await?
        };
        envelope.install_req_info(req_info);
        admit_operation(envelope.method(), envelope.headers(), envelope.uri().query(), operation, signature)?;
        enforce_sigv2_body_digest_requirement(is_sigv2, envelope.method(), envelope.headers(), operation)?;
        Ok(envelope)
    }
}

/// SigV2 does not sign request bodies, and `rustfs_profile()` waives the AWS checksum-required
/// check globally for legacy clients. Require a signed `Content-MD5` post-auth on SigV2
/// `DeleteObjects` and non-empty configuration `PUT`s so a signed request cannot be replayed
/// with a tampered XML body; the gateway verifies the supplied `Content-MD5` post-auth.
fn enforce_sigv2_body_digest_requirement(
    is_sigv2: bool,
    method: &http::Method,
    headers: &http::HeaderMap,
    operation: &str,
) -> S3Result<()> {
    if !is_sigv2 {
        return Ok(());
    }
    let has_content_md5 = headers
        .get("content-md5")
        .is_some_and(|value| !value.as_bytes().iter().all(u8::is_ascii_whitespace));
    if has_content_md5 {
        return Ok(());
    }
    if operation == "DeleteObjects" {
        return Err(missing_content_md5_error());
    }
    if method == http::Method::PUT && operation.starts_with("Put") && operation != "PutObject" {
        let declared_empty = headers
            .get(http::header::CONTENT_LENGTH)
            .and_then(|value| value.to_str().ok())
            .is_some_and(|value| value.trim() == "0");
        if !declared_empty {
            return Err(missing_content_md5_error());
        }
    }
    Ok(())
}

fn missing_content_md5_error() -> S3Error {
    let mut err = S3Error::with_message(
        S3ErrorCode::Custom("MissingContentMD5".into()),
        "A Content-MD5 header is required for this request.",
    );
    err.set_status_code(http::StatusCode::BAD_REQUEST);
    err
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
    let server_ctx = require_server_context(envelope)?;
    authorize_config_mutation(&server_ctx, envelope, bucket, action).await
}

pub(super) async fn authorize_bucket_tag_deletion(envelope: &mut RequestEnvelope, bucket: &str) -> S3Result<()> {
    let server_ctx = require_server_context(envelope)?;
    authorize_tag_deletion(&server_ctx, envelope, bucket).await
}

fn require_server_context(envelope: &RequestEnvelope) -> S3Result<Arc<ServerContextSlot>> {
    envelope
        .server_context()
        .cloned()
        .ok_or_else(|| rustfs_s3_types::s3_error!(InternalError, "the request carries no server context"))
}

/// The gateway's verdict as the access markers read it: a SigV4 signature in the header or the
/// query. SigV2, a POST policy and an anonymous request mark nothing, as on the legacy edge.
pub(in crate::server) fn verified_signature(context: &RequestContextView) -> VerifiedSignature {
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

    #[test]
    fn sigv2_body_digest_requirement_enforces_content_md5_on_delete_objects_and_config_puts() {
        let empty_headers = HeaderMap::new();
        let err = enforce_sigv2_body_digest_requirement(true, &Method::POST, &empty_headers, "DeleteObjects")
            .expect_err("SigV2 DeleteObjects without Content-MD5 must fail");
        assert_eq!(err.code().as_str(), "MissingContentMD5");
        assert_eq!(err.status_code(), Some(http::StatusCode::BAD_REQUEST));

        let mut blank_md5 = HeaderMap::new();
        blank_md5.insert("content-md5", http::HeaderValue::from_static("   "));
        let err = enforce_sigv2_body_digest_requirement(true, &Method::POST, &blank_md5, "DeleteObjects")
            .expect_err("blank Content-MD5 must not satisfy SigV2 DeleteObjects");
        assert_eq!(err.code().as_str(), "MissingContentMD5");

        let mut checksum_only = HeaderMap::new();
        checksum_only.insert("x-amz-checksum-sha256", http::HeaderValue::from_static("placeholder"));
        let err = enforce_sigv2_body_digest_requirement(true, &Method::POST, &checksum_only, "DeleteObjects")
            .expect_err("x-amz-checksum alone must not satisfy SigV2 DeleteObjects");
        assert_eq!(err.code().as_str(), "MissingContentMD5");

        let mut with_md5 = HeaderMap::new();
        with_md5.insert("content-md5", http::HeaderValue::from_static("1B2M2Y8AsgTpgAmY7PhCfg=="));
        assert!(enforce_sigv2_body_digest_requirement(true, &Method::POST, &with_md5, "DeleteObjects").is_ok());

        let err = enforce_sigv2_body_digest_requirement(true, &Method::PUT, &empty_headers, "PutBucketVersioning")
            .expect_err("non-empty SigV2 config PUT without Content-MD5 must fail");
        assert_eq!(err.code().as_str(), "MissingContentMD5");
        assert_eq!(err.status_code(), Some(http::StatusCode::BAD_REQUEST));

        let mut declared_empty = HeaderMap::new();
        declared_empty.insert(http::header::CONTENT_LENGTH, http::HeaderValue::from_static("0"));
        assert!(enforce_sigv2_body_digest_requirement(true, &Method::PUT, &declared_empty, "PutBucketAcl").is_ok());
        assert!(enforce_sigv2_body_digest_requirement(true, &Method::PUT, &with_md5, "PutBucketVersioning").is_ok());

        // SigV2 PutObject streaming uploads and SigV4 operations remain unaffected.
        assert!(enforce_sigv2_body_digest_requirement(true, &Method::PUT, &empty_headers, "PutObject").is_ok());
        assert!(enforce_sigv2_body_digest_requirement(false, &Method::POST, &empty_headers, "DeleteObjects").is_ok());
        assert!(enforce_sigv2_body_digest_requirement(false, &Method::PUT, &empty_headers, "PutBucketVersioning").is_ok());
    }
}
