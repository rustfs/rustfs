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

//! The access phase every S3 request passes before its own authorization.
//!
//! Responsible for: resolving the caller of a request into `ReqInfo` ([`resolve_req_info`]),
//! the markers the phase installs for later readers ([`AccessMarkers`], [`VerifiedSignature`]),
//! and the operation-aware admission checks ([`admit_operation`]).
//! Not responsible for: verifying a signature (each edge does), rejecting unsigned `x-amz-*`
//! headers (`crate::auth`), or any per-operation policy decision (`decision`).
//! Upstream: the legacy `S3Access::check` hook and the gateway edge's `access::admit`.
//! Downstream: `ReqInfo` readers and the use cases that read the installed markers.

mod request_validation;

#[cfg(test)]
use crate::auth::AuthType;
use crate::auth::{
    RUSTFS_MAX_CONTENT_LENGTH_QUERY, RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY, VerifiedPresignedRequest, VerifiedSigV4Request,
    check_key_valid_with_context, parse_presigned_multipart_max_total_object_size, parse_presigned_put_max_content_length,
};
use crate::license::license_check;
use crate::storage::req_info::ReqInfo;
use crate::storage::request_context::RequestContext;
use crate::storage::request_markers::MultipartStagingOperation;
use crate::storage::storage_api::runtime_sources_consumer::{ServerContextSlot, runtime_sources};
use http::HeaderMap;
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result, s3_error};
use std::sync::Arc;

/// What an edge's signature verification established about a request, as the access phase
/// records it for the readers of [`VerifiedSigV4Request`] and [`VerifiedPresignedRequest`].
///
/// Each edge derives it from its own verdict; the legacy edge from the signature form s3s verified.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct VerifiedSignature {
    /// A SigV4 signature covered the request line, in the header or the query.
    pub(crate) sigv4: bool,
    /// The SigV4 signature was carried in the query.
    pub(crate) presigned: bool,
}

#[cfg(test)]
impl VerifiedSignature {
    /// The legacy edge's reading: s3s verified the signature form these headers name.
    pub(crate) fn from_auth_type(auth_type: AuthType) -> Self {
        Self {
            sigv4: matches!(auth_type, AuthType::Presigned | AuthType::Signed),
            presigned: matches!(auth_type, AuthType::Presigned),
        }
    }
}

/// The payloads the access phase hands every later reader of a request: the signature markers
/// and, for an operation that stages an upload, [`MultipartStagingOperation`].
#[derive(Clone, Copy, Debug)]
pub(crate) struct AccessMarkers {
    staging: Option<MultipartStagingOperation>,
    signature: VerifiedSignature,
}

impl AccessMarkers {
    /// The markers of the S3 operation named `operation`, signed as `signature` says.
    pub(crate) fn new(operation: &str, signature: VerifiedSignature) -> Self {
        Self {
            staging: MultipartStagingOperation::for_operation(operation),
            signature,
        }
    }

    pub(crate) fn install(self, extensions: &mut http::Extensions) {
        if let Some(staging) = self.staging {
            extensions.insert(staging);
        }
        if self.signature.presigned {
            extensions.insert(VerifiedPresignedRequest);
        }
        if self.signature.sigv4 {
            extensions.insert(VerifiedSigV4Request);
        }
    }
}

/// Resolves the caller of one request as the access phase of either edge needs it: the credential
/// IAM holds for `access_key` (validated against `session_token`), whether it owns the deployment,
/// and the request-scoped facts later authorization steps read.
///
/// `access_key` is the key whose signature the edge verified; `None` is an anonymous request.
/// Resolved against `server_ctx` so each embedded server reads its own IAM domain.
///
/// # Errors
///
/// What [`check_key_valid_with_context`] refuses: an unknown, disabled or expired credential, a
/// session token that does not belong to it, or an IAM system that is not initialized.
pub(crate) async fn resolve_req_info(
    server_ctx: &Arc<ServerContextSlot>,
    access_key: Option<&str>,
    session_token: &str,
    request_context: Option<RequestContext>,
) -> S3Result<ReqInfo> {
    let app_context = server_ctx.app_context();
    let (cred, is_owner) = match access_key {
        Some(access_key) => {
            let (cred, is_owner) = check_key_valid_with_context(session_token, access_key, app_context.as_deref()).await?;
            (Some(cred), is_owner)
        }
        None => (None, false),
    };
    let region = match app_context.as_deref() {
        Some(context) => context.region().get(),
        None => runtime_sources::current_region(),
    };
    Ok(ReqInfo {
        cred,
        is_owner,
        region,
        request_context,
        ..Default::default()
    })
}

// POST policies cover multipart fields, not the surrounding HTTP request.
// Check before authorization can hoist query metadata into the wire headers.
fn validate_post_object_metadata(headers: &HeaderMap, raw_query: Option<&str>) -> S3Result<()> {
    fn object_property(name: &str) -> bool {
        name == "x-amz-storage-class"
            || name == "x-amz-snowball-auto-extract"
            || ["x-amz-meta-", "x-minio-meta-", "x-rustfs-meta-"]
                .iter()
                .any(|prefix| name.starts_with(prefix))
    }
    if headers.keys().any(|name| object_property(name.as_str()))
        || raw_query.is_some_and(|query| {
            url::form_urlencoded::parse(query.as_bytes()).any(|(name, _)| object_property(&name.to_ascii_lowercase()))
        })
    {
        return Err(s3_error!(AccessDenied, "POST object properties must be supplied as form fields"));
    }
    Ok(())
}

/// The operation-aware admission every S3 request passes after its caller is resolved and before
/// its own authorization. It checks ACL deletion, POST metadata, signed upload-size capabilities,
/// and the license.
///
/// # Errors
///
/// `MethodNotAllowed` for ACL deletion, `InvalidRequest` for a misplaced capability query,
/// `AccessDenied` for out-of-form POST metadata or a refused license, and `InternalError` when
/// the license cannot be checked.
pub(crate) fn admit_operation(
    method: &http::Method,
    headers: &HeaderMap,
    raw_query: Option<&str>,
    operation: &str,
    signature: VerifiedSignature,
) -> S3Result<()> {
    request_validation::validate_method_subresource(method, raw_query).map_err(|violation| match violation {
        request_validation::RequestViolation::DeleteAcl => S3Error::new(S3ErrorCode::MethodNotAllowed),
    })?;
    if operation == "PostObject" {
        validate_post_object_metadata(headers, raw_query)?;
    }
    // The size capability is intentionally scoped to the single-object
    // PutObject operation. Validate this at the operation-aware access
    // boundary so unsupported GET/HEAD/DELETE/bucket routes cannot silently
    // ignore a signed capability query.
    if parse_presigned_put_max_content_length(headers, raw_query, signature.presigned)?.is_some() && operation != "PutObject" {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("{RUSTFS_MAX_CONTENT_LENGTH_QUERY} is only supported for presigned PutObject"),
        ));
    }
    if parse_presigned_multipart_max_total_object_size(headers, raw_query, signature.sigv4)?.is_some()
        && operation != "CreateMultipartUpload"
    {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidRequest,
            format!("{RUSTFS_MAX_TOTAL_OBJECT_SIZE_QUERY} is only supported for CreateMultipartUpload"),
        ));
    }
    license_check().map_err(|er| match er.kind() {
        std::io::ErrorKind::PermissionDenied => s3_error!(AccessDenied, "{er}"),
        _ => {
            tracing::error!("license check failed due to unexpected error: {er}");
            s3_error!(InternalError, "License validation failed")
        }
    })
}

#[cfg(test)]
mod access_phase_tests {
    use super::{
        AccessMarkers, AuthType, MultipartStagingOperation, VerifiedPresignedRequest, VerifiedSigV4Request, VerifiedSignature,
    };

    #[test]
    fn admission_rejects_delete_acl_before_operation_authorization() {
        for operation in ["DeleteObject", "DeleteBucket", "DeleteObjectTagging", "AbortMultipartUpload"] {
            let error = super::admit_operation(
                &http::Method::DELETE,
                &http::HeaderMap::new(),
                Some("%61cl&versionId=version-1"),
                operation,
                VerifiedSignature::default(),
            )
            .expect_err("ACL DELETE must be refused by the shared admission phase");
            assert_eq!(error.code(), &rustfs_s3_types::S3ErrorCode::MethodNotAllowed);
        }
    }

    #[test]
    fn post_object_metadata_checks_only_out_of_form_object_properties() {
        let mut headers = http::HeaderMap::new();
        headers.insert(http::header::CONTENT_TYPE, "multipart/form-data; boundary=test".parse().unwrap());
        super::validate_post_object_metadata(&headers, Some("client=browser")).unwrap();
        for query in [
            "X-Amz-Meta-Label=test",
            "X%2DAmz-Storage-Class=STANDARD",
            "x-minio-meta-label=test",
            "x-rustfs-meta-label=test",
            "X-Amz-Snowball-Auto-Extract=true",
            "x-amz-meta-snowball-auto-extract=true",
        ] {
            assert!(super::validate_post_object_metadata(&headers, Some(query)).is_err());
        }
        for name in [
            "x-amz-meta-label",
            "x-minio-meta-label",
            "x-rustfs-meta-label",
            "x-amz-storage-class",
            "x-amz-snowball-auto-extract",
            "x-amz-meta-snowball-auto-extract",
        ] {
            headers.insert(name, "test".parse().unwrap());
            assert!(super::validate_post_object_metadata(&headers, None).is_err());
            headers.remove(name);
        }
    }

    #[test]
    fn only_a_sigv4_header_or_query_signature_is_marked_verified() {
        for (auth_type, expected) in [
            (AuthType::Signed, (true, false)),
            (AuthType::Presigned, (true, true)),
            (AuthType::SignedV2, (false, false)),
            (AuthType::PresignedV2, (false, false)),
            (AuthType::StreamingSigned, (false, false)),
            (AuthType::PostPolicy, (false, false)),
            (AuthType::Anonymous, (false, false)),
        ] {
            let signature = VerifiedSignature::from_auth_type(auth_type.clone());
            assert_eq!((signature.sigv4, signature.presigned), expected, "{auth_type:?}");
        }
    }

    #[test]
    fn each_marker_is_installed_exactly_when_its_fact_holds() {
        for (operation, sigv4, presigned) in [
            ("ListBuckets", false, false),
            ("ListBuckets", true, false),
            ("ListBuckets", true, true),
            ("CreateMultipartUpload", false, false),
            ("UploadPart", true, true),
            ("UploadPartCopy", true, false),
            ("CompleteMultipartUpload", true, false),
            ("PutObject", true, true),
        ] {
            let mut extensions = http::Extensions::new();
            AccessMarkers::new(operation, VerifiedSignature { sigv4, presigned }).install(&mut extensions);

            let staging = matches!(operation, "CreateMultipartUpload" | "UploadPart" | "UploadPartCopy");
            assert_eq!(extensions.get::<MultipartStagingOperation>().is_some(), staging, "{operation}");
            assert_eq!(extensions.get::<VerifiedSigV4Request>().is_some(), sigv4, "{operation}");
            assert_eq!(extensions.get::<VerifiedPresignedRequest>().is_some(), presigned, "{operation}");
        }
    }
}
