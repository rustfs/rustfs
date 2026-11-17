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

//! Browser uploads on the gateway's authorization-before-success-control registration.
//! The gateway owns the form and its response; the owned PutObject use case owns the write.

use super::{GatewayBackend, OpRow, access, handler_error, object_write::retain_until_with_offset};
use crate::app::RequestEnvelope;
use crate::app::object::post_body::stage_post_body;
use crate::app::object::post_headers::{post_checksum_headers, post_metadata_headers};
use crate::app::object_usecase::{DefaultObjectUsecase, PutObjectWriteInput};
use crate::storage_api::server::http::access::access_owned::object_write::{
    PostObjectFormPolicy, PostObjectPolicyFields, PutObjectAuthorization, authorize_post_object, authorize_put_object,
    invalid_post_form_field,
};
use http::HeaderValue;
use rustfs_gateway::{BoxFuture, HandlerError, HandlerResult, Operation, PostObjectHandler, Req, Resp, ServiceBuilder, dto};
use rustfs_io_metrics::record_s3_op;
use rustfs_s3_ops::S3Operation;
use rustfs_s3_types::{S3Result, s3_error};
use std::sync::Arc;

pub(super) const ROW: OpRow = OpRow {
    name: dto::PostObject::NAME,
    register,
};

fn register(builder: ServiceBuilder, backend: &Arc<GatewayBackend>) -> ServiceBuilder {
    builder.register_post_object_after_authorization(Arc::clone(backend))
}

impl PostObjectHandler for GatewayBackend {
    type Authorization = RequestEnvelope;

    fn authorize(&self, request: &Req<dto::PostObject>) -> BoxFuture<'_, Result<RequestEnvelope, HandlerError>> {
        let admission = access::admit(&self.server_ctx, request.context());
        let input = request.input();
        let bucket = input.bucket.as_str().to_owned();
        let key = input.key.as_str().to_owned();
        let expected_owner = input.fields.expected_bucket_owner.clone();
        let form = post_retain_until(request).map(|date| (date, form_policy(&input.fields, date)));
        let form_if_match = is_specific_etag(input.fields.if_match.as_deref());
        let legal_hold = input.fields.object_lock_legal_hold_status.is_some();
        let retention = input.fields.object_lock_mode.is_some() || input.fields.object_lock_retain_until_date.is_some();
        Box::pin(async move {
            let mut envelope = admission.await.map_err(handler_error)?;
            let (retain_until, policy) = form.map_err(handler_error)?;
            // The write evaluates a wire If-Match even when IAM sees the form's projection.
            let wire_if_match = envelope
                .headers()
                .get_all(http::header::IF_MATCH)
                .iter()
                .any(|value| value.to_str().map_or(true, |value| value.trim() != "*"));
            let facts = PutObjectAuthorization {
                bucket: &bucket,
                key: &key,
                version_id: None,
                retain_until,
                specific_etag: form_if_match || wire_if_match,
                legal_hold,
                retention,
                post: true,
            };
            authorize_put_object(&mut envelope, &self.server_ctx, facts, |envelope| {
                policy?.install_gateway(envelope);
                Ok(())
            })
            .await
            .map_err(|error| handler_error(error.into()))?;
            if let Some(expected) = expected_owner.as_deref().filter(|value| !value.is_empty())
                && crate::app::dto_values::owner().id.as_deref() != Some(expected)
            {
                return Err(handler_error(s3_error!(AccessDenied)));
            }
            Ok(envelope)
        })
    }

    fn execute(
        &self,
        mut envelope: RequestEnvelope,
        request: Req<dto::PostObject>,
    ) -> BoxFuture<'_, HandlerResult<dto::PostObject>> {
        Box::pin(async move {
            let retain_until = post_retain_until(&request).map_err(handler_error)?;
            let input = request.into_input();
            // The legacy POST hook checks the same form resource after success-control validation.
            authorize_post_object(&mut envelope, input.bucket.as_str(), input.key.as_str(), None)
                .await
                .map_err(|error| handler_error(error.into()))?;
            record_s3_op(S3Operation::PostObject);

            let checksum_headers = post_checksum_headers(&envelope, &input.fields).map_err(handler_error)?;
            if crate::app::object::is_put_object_extract_requested(envelope.headers()) {
                return Err(handler_error(s3_error!(NotImplemented, "POST archive extraction is not supported")));
            }
            let metadata_headers =
                post_metadata_headers(&envelope, input.content_type.as_deref(), input.fields.content_encoding.as_deref())
                    .map_err(handler_error)?;
            let original_generation = envelope
                .take_bucket_generation()
                .ok_or_else(|| handler_error(s3_error!(InternalError, "The POST bucket generation is missing")))?;
            let bucket = input.bucket.as_str().to_owned();
            let key = input.key.as_str().to_owned();
            let policy = form_policy(&input.fields, retain_until).map_err(handler_error)?;
            let specific_etag = is_specific_etag(input.fields.if_match.as_deref())
                || envelope
                    .headers()
                    .get_all(http::header::IF_MATCH)
                    .iter()
                    .any(|value| value.to_str().map_or(true, |value| value.trim() != "*"));
            let legal_hold = input.fields.object_lock_legal_hold_status.is_some();
            let retention = input.fields.object_lock_mode.is_some() || input.fields.object_lock_retain_until_date.is_some();
            // Preflight guards must not hold table publication locks while the
            // client streams. Reauthorize under fresh guards before publication.
            envelope.release_table_publication_guards();
            let mut input = write_input(&envelope, input, retain_until).await.map_err(handler_error)?;
            input.metadata_headers = Some(metadata_headers);
            input.checksum_headers = checksum_headers;
            let facts = PutObjectAuthorization {
                bucket: &bucket,
                key: &key,
                version_id: None,
                retain_until,
                specific_etag,
                legal_hold,
                retention,
                post: true,
            };
            authorize_put_object(&mut envelope, &self.server_ctx, facts, |envelope| {
                policy.install_gateway(envelope);
                Ok(())
            })
            .await
            .map_err(|error| handler_error(error.into()))?;
            // A recreated bucket must not inherit the preflight authorization.
            let generation = envelope
                .bucket_generation()
                .ok_or_else(|| handler_error(s3_error!(InternalError, "The POST bucket generation is missing")))?;
            if generation != &original_generation {
                return Err(handler_error(s3_error!(NoSuchBucket)));
            }
            authorize_post_object(&mut envelope, &bucket, &key, None)
                .await
                .map_err(|error| handler_error(error.into()))?;
            let output = DefaultObjectUsecase::with_context(self.server_ctx.app_context())
                .execute_put_object_owned(envelope, input)
                .await
                .map_err(handler_error)?;
            let dto = dto::PostObjectOutput {
                e_tag: output.e_tag.as_deref().map(crate::app::dto_values::stored_etag),
                version_id: output.version_id,
            };
            Ok(Resp::new(dto).with_extra_headers(output.headers))
        })
    }
}

fn is_specific_etag(value: Option<&str>) -> bool {
    value.is_some_and(|value| value != "*")
}

fn post_retain_until(request: &Req<dto::PostObject>) -> S3Result<Option<time::OffsetDateTime>> {
    let Some(date) = request.input().fields.object_lock_retain_until_date.as_ref() else {
        return Ok(None);
    };
    let offset = request
        .context()
        .object_lock_retain_until_offset_seconds()
        .ok_or_else(|| s3_error!(InternalError, "The POST retain-until date is missing its validated offset"))?;
    retain_until_with_offset(Some(date), Some(offset))
}

fn form_policy(fields: &dto::PostObjectFields, retain_until: Option<time::OffsetDateTime>) -> S3Result<PostObjectFormPolicy> {
    Ok(PostObjectFormPolicy::new(PostObjectPolicyFields {
        lock_mode: fields.object_lock_mode.as_ref().map(|value| value.as_str()),
        legal_hold: fields.object_lock_legal_hold_status.as_ref().map(|value| value.as_str()),
        retain_until,
        tagging: fields.tagging.as_deref(),
        acl: fields.acl.as_ref().map(|value| value.as_str()),
        storage_class: fields.storage_class.as_ref().map(|value| value.as_str()),
        encryption: fields.server_side_encryption.as_ref().map(|value| value.as_str()),
        kms_key_id: fields.ssekms_key_id.as_deref(),
        customer_algorithm: fields.sse_customer_algorithm.as_deref(),
        grant_full_control: fields.grant_full_control.as_deref(),
        grant_read: fields.grant_read.as_deref(),
        grant_read_acp: fields.grant_read_acp.as_deref(),
        grant_write_acp: fields.grant_write_acp.as_deref(),
        if_match: fields.if_match.as_deref().map(form_condition).transpose()?,
        if_none_match: fields.if_none_match.as_deref().map(form_condition).transpose()?,
    })?)
}

fn form_condition(value: &str) -> S3Result<HeaderValue> {
    // The form decoder has already validated its legacy entity-tag grammar.
    let header = if value == "*" || value.starts_with('"') || value.starts_with("W/\"") {
        HeaderValue::from_str(value)
    } else {
        HeaderValue::from_str(&format!("\"{value}\""))
    };
    header.map_err(|_| invalid_post_form_field().into())
}

async fn write_input(
    envelope: &RequestEnvelope,
    input: dto::PostObjectInput,
    retain_until: Option<time::OffsetDateTime>,
) -> S3Result<PutObjectWriteInput> {
    let dto::PostObjectInput {
        bucket,
        key,
        body,
        content_length,
        content_type,
        metadata,
        fields,
    } = input;
    let body = body.into_body();
    let (body, length) = match content_length {
        Some(length) => {
            let length = i64::try_from(length).map_err(|_| s3_error!(EntityTooLarge, "The object is too large"))?;
            (body, length)
        }
        None => stage_post_body(envelope, bucket.as_str(), key.as_str(), body, None).await?,
    };

    Ok(PutObjectWriteInput {
        bucket: bucket.as_str().to_owned(),
        key: key.as_str().to_owned(),
        body: Some(body),
        content_length: Some(length),
        metadata: Some(metadata.into_iter().collect()),
        specific_if_none_match: is_specific_etag(fields.if_none_match.as_deref()),
        object_lock_retain_until_date: retain_until,
        cache_control: fields.cache_control,
        content_disposition: fields.content_disposition,
        content_encoding: fields.content_encoding,
        content_language: fields.content_language,
        content_type,
        expires: fields.expires.map(|value| value.as_str().to_owned()),
        tagging: fields.tagging,
        server_side_encryption: fields.server_side_encryption.map(|value| value.as_str().to_owned()),
        sse_customer_algorithm: fields.sse_customer_algorithm,
        sse_customer_key: fields.sse_customer_key.map(|value| value.expose_secret().to_owned()),
        sse_customer_key_md5: fields.sse_customer_key_md5,
        ssekms_key_id: fields.ssekms_key_id,
        content_md5: fields.content_md5,
        object_lock_legal_hold_status: fields.object_lock_legal_hold_status.map(|value| value.as_str().to_owned()),
        object_lock_mode: fields.object_lock_mode.map(|value| value.as_str().to_owned()),
        storage_class: fields.storage_class.map(|value| value.as_str().to_owned()),
        website_redirect_location: fields.website_redirect_location,
        checksum_algorithm: fields.checksum_algorithm.map(|value| value.as_str().to_owned()),
        checksum_crc32: fields.checksum_crc32,
        checksum_crc32c: fields.checksum_crc32c,
        checksum_crc64nvme: fields.checksum_crc64nvme,
        checksum_sha1: fields.checksum_sha1,
        checksum_sha256: fields.checksum_sha256,
        ..Default::default()
    })
}
