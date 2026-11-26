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

//! Authorization shapes that only migration batch D (rustfs/backlog#2765) uses: PutObject, UploadPart and PostObject.
//!
//! Responsible for: the owned authorization function of each such operation, generic over
//! `AuthorizationRequest`, so the legacy `S3Access` hook and `Served::authorize` call the same code.
//! Not responsible for: shapes more than one batch shares (those stay in `storage/access.rs`), the
//! policy evaluation itself (`authorize_request`), or the operation's use case.
//! Upstream: the legacy hook in `storage/access.rs` and the batch's `server/gateway_handlers/object_write.rs`.
//! Downstream: the shared shapes in `storage/access.rs`.

#[cfg(test)]
use crate::error::ApiError;
use crate::storage::authorization::{
    AuthorizationRequest, authorize_replication_only_put_headers, authorize_request, authorize_staged_multipart_part,
    authorize_write_etag_condition, has_write_offset_bytes_header, load_bucket_generation, missing_req_info_error,
    set_requested_object_lock_retain_until,
};
use crate::storage::authorization_actions::post_object_authorize_action;
use crate::storage::request_markers::PostObjectPolicyHeaders;
use crate::storage::storage_api::runtime_sources_consumer::ServerContextSlot;
use http::{HeaderName, HeaderValue};
use rustfs_policy::policy::action::{Action, S3Action};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};
use time::OffsetDateTime;

pub(crate) async fn authorize_upload_part<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    server: &ServerContextSlot,
    bucket: &str,
    key: &str,
) -> S3Result<()> {
    let generation = load_bucket_generation(server, req, bucket).await;
    let info = req.req_info_mut().ok_or_else(missing_req_info_error)?;
    info.bucket = Some(bucket.to_owned());
    info.object = Some(key.to_owned());
    authorize_staged_multipart_part(req).await?;
    req.install_bucket_generation(generation?);
    Ok(())
}

/// Request facts needed by both PutObject authorization edges.
pub(crate) struct PutObjectAuthorization<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) key: &'a str,
    pub(crate) version_id: Option<&'a str>,
    pub(crate) retain_until: Option<time::OffsetDateTime>,
    pub(crate) specific_etag: bool,
    pub(crate) legal_hold: bool,
    pub(crate) retention: bool,
    pub(crate) post: bool,
}

/// Preserves write refusal ordering and captures the generation authorized for publication.
pub(crate) async fn authorize_put_object<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    server: &ServerContextSlot,
    input: PutObjectAuthorization<'_>,
    prepare_policy: impl FnOnce(&mut R) -> S3Result<()> + Send,
) -> S3Result<()> {
    let version = crate::storage::options::destination_version_id(input.version_id, req.headers());
    let info = req.req_info_mut().ok_or_else(missing_req_info_error)?;
    info.bucket = Some(input.bucket.to_owned());
    info.object = Some(input.key.to_owned());
    info.version_id = version;
    if has_write_offset_bytes_header(req.headers()) {
        return Err(S3Error::with_message(
            S3ErrorCode::NotImplemented,
            "A header you provided implies functionality that is not implemented".to_string(),
        ));
    }
    // Capture first, but let authorization refusals precede bucket-state failures.
    let generation = load_bucket_generation(server, req, input.bucket).await;
    #[cfg(test)]
    let generation = if crate::storage::authorization::PUT_GENERATION_READ_QUORUM_FAILURE
        .try_with(|_| ())
        .is_ok()
    {
        Err(ApiError::from(crate::storage::StorageError::ErasureReadQuorum).into())
    } else {
        generation
    };
    set_requested_object_lock_retain_until(req, input.retain_until);
    prepare_policy(req)?;
    authorize_request(req, Action::S3Action(S3Action::PutObjectAction)).await?;
    authorize_write_etag_condition(req, input.specific_etag).await?;
    req.install_bucket_generation(generation?);
    if !input.post {
        authorize_replication_only_put_headers(req).await?;
    }
    super::object_commit::authorize_write_request_tagging(req, input.post).await?;
    if input.legal_hold {
        authorize_request(req, Action::S3Action(S3Action::PutObjectLegalHoldAction)).await?;
    }
    if input.retention {
        authorize_request(req, Action::S3Action(S3Action::PutObjectRetentionAction)).await?;
    }
    Ok(())
}

/// The final PostObject check uses the form resource and policy projection.
pub(crate) async fn authorize_post_object<R: AuthorizationRequest + ?Sized>(
    req: &mut R,
    bucket: &str,
    key: &str,
    version_id: Option<&str>,
) -> S3Result<()> {
    let info = req.req_info_mut().ok_or_else(missing_req_info_error)?;
    info.bucket = Some(bucket.to_owned());
    info.object = Some(key.to_owned());
    info.version_id = version_id.map(str::to_owned);
    req.install_post_object_marker();
    authorize_request(req, post_object_authorize_action()).await
}

/// Form fields that shadow wire headers in the PutObject policy decision.
#[derive(Default)]
pub(crate) struct PostObjectPolicyFields<'a> {
    pub(crate) lock_mode: Option<&'a str>,
    pub(crate) legal_hold: Option<&'a str>,
    pub(crate) retain_until: Option<OffsetDateTime>,
    pub(crate) tagging: Option<&'a str>,
    pub(crate) acl: Option<&'a str>,
    pub(crate) storage_class: Option<&'a str>,
    pub(crate) encryption: Option<&'a str>,
    pub(crate) kms_key_id: Option<&'a str>,
    pub(crate) customer_algorithm: Option<&'a str>,
    pub(crate) grant_full_control: Option<&'a str>,
    pub(crate) grant_read: Option<&'a str>,
    pub(crate) grant_read_acp: Option<&'a str>,
    pub(crate) grant_write_acp: Option<&'a str>,
    pub(crate) if_match: Option<HeaderValue>,
    pub(crate) if_none_match: Option<HeaderValue>,
}

/// Owns validated form values while authorization waits for IAM or bucket state.
pub(crate) struct PostObjectFormPolicy(Vec<(HeaderName, Option<HeaderValue>)>);

impl PostObjectFormPolicy {
    pub(crate) fn new(fields: PostObjectPolicyFields<'_>) -> S3Result<Self> {
        let retain_until = fields
            .retain_until
            .map(|date| date.format(&time::format_description::well_known::Rfc3339))
            .transpose()
            .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid retain-until date"))?;
        let form_values = [
            ("x-amz-object-lock-mode", fields.lock_mode),
            ("x-amz-object-lock-legal-hold", fields.legal_hold),
            ("x-amz-object-lock-retain-until-date", retain_until.as_deref()),
            ("x-amz-tagging", fields.tagging),
            ("x-amz-acl", fields.acl),
            ("x-amz-storage-class", fields.storage_class),
            ("x-amz-server-side-encryption", fields.encryption),
            ("x-amz-server-side-encryption-aws-kms-key-id", fields.kms_key_id),
            ("x-amz-server-side-encryption-customer-algorithm", fields.customer_algorithm),
            ("x-amz-grant-full-control", fields.grant_full_control),
            ("x-amz-grant-read", fields.grant_read),
            ("x-amz-grant-write", None),
            ("x-amz-grant-read-acp", fields.grant_read_acp),
            ("x-amz-grant-write-acp", fields.grant_write_acp),
        ];
        let mut values = Vec::with_capacity(form_values.len() + 2);
        for (name, value) in form_values {
            let value = value
                .map(HeaderValue::from_str)
                .transpose()
                .map_err(|_| invalid_post_form_field())?;
            values.push((HeaderName::from_static(name), value));
        }
        values.extend([
            (HeaderName::from_static("if-match"), fields.if_match),
            (HeaderName::from_static("if-none-match"), fields.if_none_match),
        ]);
        Ok(Self(values))
    }

    pub(crate) fn install<R: AuthorizationRequest + ?Sized>(self, req: &mut R) {
        let mut headers = req.headers().clone();
        for (name, value) in self.0 {
            headers.remove(&name);
            if let Some(value) = value {
                headers.insert(name, value);
            }
        }
        req.install_post_object_policy_headers(PostObjectPolicyHeaders(headers));
    }

    pub(crate) fn install_gateway<R: AuthorizationRequest + ?Sized>(mut self, req: &mut R) {
        for (name, value) in &mut self.0 {
            match name.as_str() {
                "if-match" | "if-none-match" => *value = req.headers().get(&*name).cloned(),
                "x-amz-server-side-encryption" | "x-amz-server-side-encryption-customer-algorithm" if value.is_none() => {
                    *value = req.headers().get(&*name).cloned();
                }
                _ => {}
            }
        }
        self.install(req);
    }
}

pub(crate) fn invalid_post_form_field() -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid form field")
}
