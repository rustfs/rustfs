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

//! Request and response projections for protocol storage operations.
//! Protocol clients retain their session checks; use cases own storage and bucket visibility.

use super::{ProtocolStorageClient, RequestParams};
use crate::app::object_usecase::{DefaultObjectUsecase, GetObjectBody, GetObjectOutputFacts, HeadObjectOutputFacts};
use crate::app::{RequestCredentials, RequestEnvelope, RequestHead};
use crate::runtime_sources::current_action_credentials;
use crate::storage_api::protocols::client::{ReqInfo, RequestContext};
use http::{HeaderMap, Method, Uri};
use rustfs_gateway_types::{ETag, Timestamp, dto};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};

impl ProtocolStorageClient {
    pub(super) async fn get_object_owned(
        &self,
        bucket: &str,
        key: &str,
        credentials: &rustfs_credentials::Credentials,
        range: Option<rustfs_gateway_types::ByteRange>,
    ) -> S3Result<dto::GetObjectOutput> {
        let uri = super::build_object_uri(bucket, key, &[])?;
        let envelope = Self::create_envelope(
            Method::GET,
            uri,
            RequestParams {
                bucket: Some(bucket.to_owned()),
                object: Some(key.to_owned()),
                credentials,
            },
        );
        rustfs_io_metrics::record_s3_op(rustfs_s3_ops::S3Operation::GetObject);
        self.owned_usecase()
            .execute_get_object_fields(
                envelope,
                crate::app::object_usecase::GetObjectFields {
                    bucket: bucket.to_owned(),
                    key: key.to_owned(),
                    version_id: None,
                    part_number: None,
                    range,
                    sse_customer_key: None,
                    sse_customer_key_md5: None,
                },
            )
            .await
            .and_then(|answered| get_output(answered.output))
    }

    pub(super) fn owned_bucket_usecase(&self) -> crate::app::bucket_usecase::DefaultBucketUsecase {
        crate::app::bucket_usecase::DefaultBucketUsecase::with_context(self.fs.server_ctx().app_context())
    }

    pub(super) fn owned_usecase(&self) -> DefaultObjectUsecase {
        DefaultObjectUsecase::with_context(self.fs.server_ctx().app_context())
    }

    pub(super) fn create_envelope(method: Method, uri: Uri, params: RequestParams<'_>) -> RequestEnvelope {
        Self::create_envelope_with_headers(method, uri, params, HeaderMap::new())
    }

    pub(super) fn create_envelope_with_headers(
        method: Method,
        uri: Uri,
        params: RequestParams<'_>,
        headers: HeaderMap,
    ) -> RequestEnvelope {
        let is_owner = current_action_credentials().is_some_and(|root| params.credentials.access_key == root.access_key);
        let credentials =
            RequestCredentials::new(params.credentials.access_key.clone(), params.credentials.secret_key.clone(), None)
                .unwrap_or_else(|_| RequestCredentials::without_secret_key(params.credentials.access_key.clone(), None));
        let mut extensions = http::Extensions::new();
        extensions.insert(ReqInfo {
            cred: Some(params.credentials.clone()),
            keystone_session_present: None,
            is_owner,
            bucket: params.bucket,
            object: params.object,
            version_id: None,
            replication_request_authorized: false,
            region: None,
            request_context: Some(RequestContext::fallback()),
            suppress_denial_log: false,
            suppress_denial_audit: false,
        });
        RequestEnvelope::from_parts(
            RequestHead {
                method,
                uri,
                headers,
                credentials: Some(credentials),
                region: None,
                service: None,
            },
            extensions,
        )
    }
}

fn etag(opaque: String, weak: bool) -> S3Result<ETag> {
    if weak { ETag::new_weak(opaque) } else { ETag::new(opaque) }
        .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "Object response contains an invalid ETag"))
}

// Protocol clients received millisecond timestamps through the legacy response codec.
fn millisecond_timestamp(value: Timestamp) -> Timestamp {
    Timestamp::from_secs_nanos(value.secs(), value.subsec_millis() * 1_000_000)
        .expect("a millisecond remainder is below one second") // subsec_millis is at most 999.
}

pub(super) fn get_output(mut output: GetObjectOutputFacts) -> S3Result<dto::GetObjectOutput> {
    output.dto.last_modified = output.dto.last_modified.map(millisecond_timestamp);
    output.dto.object_lock_retain_until_date = output.dto.object_lock_retain_until_date.map(millisecond_timestamp);
    output.dto.e_tag = output.etag.map(|tag| etag(tag.opaque, tag.weak)).transpose()?;
    output.dto.body = output
        .body
        .map(|body| match body {
            GetObjectBody::Bytes(bytes) => Ok(rustfs_gateway_stream::ByteStream::from_bytes(bytes)),
            GetObjectBody::Stream(stream) => rustfs_gateway_stream::ByteStream::new(stream)
                .map_err(|_| S3Error::with_message(S3ErrorCode::InternalError, "Invalid GetObject body capabilities")),
        })
        .transpose()?;
    Ok(output.dto)
}

pub(super) fn head_output(mut output: HeadObjectOutputFacts) -> S3Result<dto::HeadObjectOutput> {
    output.dto.last_modified = output.dto.last_modified.map(millisecond_timestamp);
    output.dto.object_lock_retain_until_date = output.dto.object_lock_retain_until_date.map(millisecond_timestamp);
    output.dto.e_tag = output.etag.map(|tag| etag(tag.opaque, tag.weak)).transpose()?;
    Ok(output.dto)
}

fn put_input_error(field: &str, reason: &str) -> S3Error {
    super::conversion_error("PutObject input", format!("{field}: {reason}"))
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

// Protocol DTOs used this narrower condition grammar before reaching storage.
fn put_condition(value: Option<&str>, field: &str) -> S3Result<bool> {
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Ok(false);
    };
    if value == "*" {
        return Ok(false);
    }
    let quoted = value.strip_prefix("W/").unwrap_or(value);
    let valid = if quoted.starts_with('"') && quoted.ends_with('"') && quoted.len() >= 2 {
        quoted.bytes().all(|byte| byte == b'\t' || (b' '..=b'~').contains(&byte))
    } else {
        value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    };
    if !valid {
        return Err(put_input_error(field, "not an entity-tag condition the s3s input can hold"));
    }
    Ok(true)
}

pub(super) fn put_input(input: dto::PutObjectInput) -> S3Result<crate::app::object_usecase::PutObjectWriteInput> {
    use crate::app::object_usecase::PutObjectWriteInput;
    use rustfs_gateway_types::ChecksumAlgorithm;
    for (field, named) in [
        (
            "object_lock_event_hold",
            input
                .object_lock_event_hold
                .as_ref()
                .is_some_and(|value| !value.as_str().is_empty()),
        ),
        (
            "object_lock_event_hold_duration_days",
            input.object_lock_event_hold_duration_days.is_some(),
        ),
        (
            "object_lock_event_hold_duration_years",
            input.object_lock_event_hold_duration_years.is_some(),
        ),
    ] {
        if named {
            return Err(put_input_error(
                field,
                "an Object Lock event hold, which the pinned s3s input cannot hold and RustFS does not store",
            ));
        }
    }
    put_condition(input.if_match.as_deref(), "if_match")?;
    let specific_if_none_match = put_condition(input.if_none_match.as_deref(), "if_none_match")?;
    let checksum = |algorithm| {
        input
            .checksum_spec
            .as_ref()
            .filter(|value| value.algorithm() == algorithm)
            .map(|value| value.render_base64().to_owned())
    };
    let retain_until = input
        .object_lock_retain_until_date
        .map(|at| {
            time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(at.secs()) * 1_000_000_000 + i128::from(at.subsec_nanos()))
                .map_err(|_| put_input_error("object_lock_retain_until_date", "an instant outside what the s3s input can hold"))
        })
        .transpose()?;
    Ok(PutObjectWriteInput {
        checksum_crc32: checksum(ChecksumAlgorithm::Crc32),
        checksum_crc32c: checksum(ChecksumAlgorithm::Crc32c),
        checksum_sha1: checksum(ChecksumAlgorithm::Sha1),
        checksum_sha256: checksum(ChecksumAlgorithm::Sha256),
        checksum_crc64nvme: checksum(ChecksumAlgorithm::Crc64Nvme),
        bucket: input.bucket.as_str().to_owned(),
        key: input.key.as_str().to_owned(),
        body: input.body.map(|body| body.into_body()),
        trailing_headers: None,
        frame_trailers: None,
        content_length: (input.content_length >= 0).then_some(input.content_length),
        metadata: (!input.metadata.is_empty()).then(|| input.metadata.into_iter().collect()),
        specific_if_none_match,
        object_lock_retain_until_date: retain_until,
        cache_control: nonempty(input.cache_control),
        content_disposition: nonempty(input.content_disposition),
        content_encoding: nonempty(input.content_encoding),
        content_language: nonempty(input.content_language),
        content_type: nonempty(input.content_type),
        expires: nonempty(input.expires.map(|value| value.as_str().to_owned())),
        tagging: nonempty(input.tagging),
        version_id: None,
        server_side_encryption: nonempty(input.server_side_encryption.map(|value| value.as_str().to_owned())),
        sse_customer_algorithm: nonempty(input.sse_customer_algorithm),
        sse_customer_key: input.sse_customer_key.map(|value| value.expose_secret().to_owned()),
        sse_customer_key_md5: nonempty(input.sse_customer_key_md5),
        ssekms_key_id: nonempty(input.ssekms_key_id),
        content_md5: nonempty(input.content_md5),
        object_lock_legal_hold_status: nonempty(input.object_lock_legal_hold_status.map(|value| value.as_str().to_owned())),
        object_lock_mode: nonempty(input.object_lock_mode.map(|value| value.as_str().to_owned())),
        storage_class: nonempty(input.storage_class.map(|value| value.as_str().to_owned())),
        website_redirect_location: nonempty(input.website_redirect_location),
        checksum_algorithm: nonempty(input.checksum_algorithm.map(|value| value.as_str().to_owned())),
    })
}

pub(super) fn put_output(output: crate::app::object_usecase::PutObjectWriteOutput) -> S3Result<dto::PutObjectOutput> {
    use rustfs_gateway_types::{ChecksumAlgorithm, ChecksumSpec};
    let error = |field, reason| super::conversion_error("PutObject output", format!("{field}: {reason}"));
    let mut checksums = [
        ("checksum_crc32", ChecksumAlgorithm::Crc32, output.checksum_crc32),
        ("checksum_crc32c", ChecksumAlgorithm::Crc32c, output.checksum_crc32c),
        ("checksum_crc64nvme", ChecksumAlgorithm::Crc64Nvme, output.checksum_crc64nvme),
        ("checksum_sha1", ChecksumAlgorithm::Sha1, output.checksum_sha1),
        ("checksum_sha256", ChecksumAlgorithm::Sha256, output.checksum_sha256),
    ]
    .into_iter()
    .filter_map(|(field, algorithm, value)| value.map(|value| (field, algorithm, value)));
    let checksum_spec = match (checksums.next(), checksums.next()) {
        (None, _) => None,
        (Some(_), Some(_)) => {
            return Err(error(
                "checksum_spec",
                "the gateway output carries one checksum, so a second would be lost",
            ));
        }
        (Some((field, algorithm, value)), None) => Some(
            ChecksumSpec::parse_header(algorithm.header_name(), &value)
                .map_err(|_| error(field, "not a checksum value of this algorithm's width"))?,
        ),
    };
    let raw_etag = output
        .e_tag
        .ok_or_else(|| error("e_tag", "a PutObject answer carries an entity tag and the gateway output cannot omit one"))?;
    let e_tag = if let Some(value) = raw_etag.strip_prefix("W/\"") {
        ETag::new_weak(value.strip_suffix('"').unwrap_or(value).to_owned())
    } else {
        ETag::new(
            raw_etag
                .strip_prefix('"')
                .and_then(|value| value.strip_suffix('"'))
                .unwrap_or(&raw_etag)
                .to_owned(),
        )
    }
    .map_err(|_| error("e_tag", "not an entity tag the gateway can write"))?;
    Ok(dto::PutObjectOutput {
        e_tag,
        checksum_spec,
        version_id: output.version_id,
        expiration: output.expiration.map(Into::into),
        server_side_encryption: output.server_side_encryption.map(dto::ServerSideEncryption::custom),
        sse_customer_algorithm: output.sse_customer_algorithm,
        sse_customer_key_md5: output.sse_customer_key_md5,
        ssekms_key_id: output.ssekms_key_id,
        ..Default::default()
    })
}

pub(super) fn put_error(mut error: S3Error) -> S3Error {
    if error.message().is_some() {
        return error;
    }
    let message = match error.code() {
        S3ErrorCode::InvalidStorageClass => "The storage class you specified is not valid.",
        S3ErrorCode::IncompleteBody => "You did not provide the number of bytes specified by the Content-Length HTTP header",
        S3ErrorCode::UnexpectedContent => "This request does not support content.",
        _ => return error,
    };
    error.set_message(message);
    error
}

pub(super) fn head_bucket_input(bucket: &str) -> S3Result<dto::HeadBucketInput> {
    Ok(dto::HeadBucketInput {
        bucket: if bucket.is_empty() {
            // Protocol adapters use the empty bucket for the storage root.
            Default::default()
        } else {
            crate::app::dto_values::stored_bucket_name(bucket).map_err(|_| {
                S3Error::with_message(
                    S3ErrorCode::InternalError,
                    "failed to convert through the gateway compatibility seam: bucket: not a bucket name the gateway can write",
                )
            })?
        },
        expected_bucket_owner: None,
    })
}

pub(super) fn bucket_listing_output(mut output: dto::ListBucketsOutput) -> dto::ListBucketsOutput {
    output
        .buckets
        .retain(|bucket| rustfs_gateway_types::BucketName::new(bucket.name.as_str()).is_ok());
    for bucket in &mut output.buckets {
        bucket.creation_date = millisecond_timestamp(bucket.creation_date);
    }
    output
}

// Protocol listings keep the legacy edge's entry encoding and XML entity-tag spelling.
pub(super) fn listing_output(mut output: dto::ListObjectsV2Output) -> S3Result<dto::ListObjectsV2Output> {
    let error = |field, reason| super::conversion_error("ListObjectsV2 output", format!("{field}: {reason}"));
    let encode = |value: &str| {
        if output
            .encoding_type
            .as_ref()
            .is_some_and(|encoding| encoding.as_str() == "url")
        {
            value
                .split('/')
                .map(|part| urlencoding::encode(part).into_owned())
                .collect::<Vec<_>>()
                .join("/")
        } else {
            value.to_owned()
        }
    };
    for object in &mut output.contents {
        object.key = rustfs_gateway_types::ObjectKey::new(encode(object.key.as_str()))
            .map_err(|_| error("key", "not an object key the gateway can write"))?;
        object.last_modified = millisecond_timestamp(object.last_modified);
        object.e_tag = object
            .e_tag
            .take()
            .map(|tag| {
                let value = quick_xml::escape::escape(tag.opaque_tag()).into_owned();
                if tag.is_weak() {
                    ETag::new_weak(value)
                } else {
                    ETag::new(value)
                }
                .map_err(|_| error("e_tag", "not an entity tag the gateway can write"))
            })
            .transpose()?;
    }
    for prefix in &mut output.common_prefixes {
        prefix.prefix = encode(&prefix.prefix);
    }
    output.rustfs_on_demand_migration_list = None;
    super::require_listed_etags(output).map_err(|error| super::conversion_error("ListObjectsV2 output", error))
}

pub(super) fn create_bucket_input(bucket: &str) -> S3Result<dto::CreateBucketInput> {
    Ok(dto::CreateBucketInput {
        bucket: crate::app::dto_values::stored_bucket_name(bucket).map_err(|_| {
            S3Error::with_message(
                S3ErrorCode::InternalError,
                "failed to convert through the gateway compatibility seam: bucket: not a bucket name the gateway can write",
            )
        })?,
        ..Default::default()
    })
}
