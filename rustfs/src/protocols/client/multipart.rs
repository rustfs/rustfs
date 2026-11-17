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

//! Legacy-compatible multipart projections for protocol storage clients.
//! The shared multipart use cases own storage and validation; protocol sessions own authorization.

use super::ProtocolStorageClient;
use rustfs_gateway_types::{ETag, Timestamp, dto};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};

impl ProtocolStorageClient {
    pub(super) fn owned_multipart_usecase(&self) -> crate::app::multipart_usecase::DefaultMultipartUsecase {
        crate::app::multipart_usecase::DefaultMultipartUsecase::with_context(self.fs.server_ctx().app_context())
    }
}

fn nonempty(value: Option<String>) -> Option<String> {
    value.filter(|value| !value.is_empty())
}

pub(super) fn multipart_timestamp(value: Timestamp, operation: &str, field: &'static str) -> S3Result<time::OffsetDateTime> {
    let nanos = i128::from(value.subsec_nanos());
    time::OffsetDateTime::from_unix_timestamp_nanos(i128::from(value.secs()) * 1_000_000_000 + nanos)
        .map_err(|_| super::conversion_error(operation, format!("{field}: an instant outside what the s3s shape can hold")))
}

pub(super) fn create_multipart_input(
    input: dto::CreateMultipartUploadInput,
) -> S3Result<crate::app::multipart_usecase::CreateMultipartUploadFields> {
    for (field, present) in [
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
        if present {
            return Err(super::conversion_error(
                "CreateMultipartUpload input",
                format!("{field}: an Object Lock event hold, which no pinned s3s shape holds and RustFS does not store"),
            ));
        }
    }
    Ok(crate::app::multipart_usecase::CreateMultipartUploadFields {
        bucket: input.bucket.as_str().to_owned(),
        key: input.key.as_str().to_owned(),
        metadata: (!input.metadata.is_empty()).then(|| input.metadata.into_iter().collect()),
        tagging: nonempty(input.tagging),
        storage_class: nonempty(input.storage_class.map(|value| value.as_str().to_owned())),
        server_side_encryption: nonempty(input.server_side_encryption.map(|value| value.as_str().to_owned())),
        sse_customer_algorithm: nonempty(input.sse_customer_algorithm),
        sse_customer_key: nonempty(input.sse_customer_key.map(|value| value.expose_secret().to_owned())),
        sse_customer_key_md5: nonempty(input.sse_customer_key_md5),
        ssekms_key_id: nonempty(input.ssekms_key_id),
        object_lock_legal_hold_status: nonempty(input.object_lock_legal_hold_status.map(|value| value.as_str().to_owned())),
        object_lock_mode: nonempty(input.object_lock_mode.map(|value| value.as_str().to_owned())),
        object_lock_retain_until_date: input
            .object_lock_retain_until_date
            .map(|value| multipart_timestamp(value, "CreateMultipartUpload input", "object_lock_retain_until_date"))
            .transpose()?,
    })
}

pub(super) fn create_multipart_output(
    output: crate::app::multipart_usecase::CreateMultipartUploadResult,
) -> S3Result<dto::CreateMultipartUploadOutput> {
    Ok(dto::CreateMultipartUploadOutput {
        bucket: crate::app::dto_values::stored_bucket_name(&output.bucket)?,
        key: crate::app::dto_values::stored_object_key(&output.key)?,
        upload_id: output.upload_id,
        server_side_encryption: output.server_side_encryption.map(dto::ServerSideEncryption::custom),
        sse_customer_algorithm: output.sse_customer_algorithm,
        sse_customer_key_md5: output.sse_customer_key_md5,
        ssekms_key_id: output.ssekms_key_id,
        checksum_algorithm: output.checksum_algorithm.map(dto::ChecksumAlgorithm::custom),
        checksum_type: output.checksum_type.map(dto::ChecksumType::custom),
        ..Default::default()
    })
}

fn complete_condition(value: Option<String>, field: &'static str) -> S3Result<Option<String>> {
    let Some(value) = nonempty(value) else { return Ok(None) };
    if value == "*" {
        return Ok(Some(value));
    }
    let quoted = value.strip_prefix("W/").unwrap_or(&value);
    let valid = if quoted.starts_with('"') && quoted.ends_with('"') && quoted.len() >= 2 {
        quoted.bytes().all(|byte| byte == b'\t' || (b' '..=b'~').contains(&byte))
    } else {
        value.bytes().all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    };
    if !valid {
        return Err(super::conversion_error(
            "CompleteMultipartUpload input",
            format!("{field}: not an entity-tag condition the s3s input can hold"),
        ));
    }
    Ok(Some(if quoted.starts_with('"') {
        value
    } else {
        format!("\"{value}\"")
    }))
}

pub(super) fn complete_multipart_input(
    mut input: dto::CompleteMultipartUploadInput,
) -> S3Result<dto::CompleteMultipartUploadInput> {
    input.if_match = complete_condition(input.if_match, "if_match")?;
    input.if_none_match = complete_condition(input.if_none_match, "if_none_match")?;
    input.checksum_spec = None;
    input.checksum_type = input.checksum_type.filter(|value| !value.as_str().is_empty());
    input.sse_customer_algorithm = nonempty(input.sse_customer_algorithm);
    input.sse_customer_key = input.sse_customer_key.filter(|value| !value.expose_secret().is_empty());
    input.sse_customer_key_md5 = nonempty(input.sse_customer_key_md5);
    for part in &mut input.multipart_upload.parts {
        part.e_tag = part
            .e_tag
            .take()
            .map(|value| ETag::from_stored(value.opaque_tag().to_owned()));
    }
    Ok(input)
}

pub(super) fn complete_multipart_output(
    mut output: dto::CompleteMultipartUploadOutput,
) -> S3Result<dto::CompleteMultipartUploadOutput> {
    for (field, value) in [
        ("x-amz-checksum-md5", output.checksum_header_md5.take()),
        ("x-amz-checksum-sha512", output.checksum_header_sha512.take()),
        ("x-amz-checksum-xxhash3", output.checksum_header_xxhash3.take()),
        ("x-amz-checksum-xxhash64", output.checksum_header_xxhash64.take()),
        ("x-amz-checksum-xxhash128", output.checksum_header_xxhash128.take()),
        ("x-amz-server-side-encryption-customer-algorithm", output.sse_customer_algorithm.take()),
        ("x-amz-server-side-encryption-customer-key-md5", output.sse_customer_key_md5.take()),
    ] {
        if let Some(value) = value {
            http::HeaderValue::from_str(&value).map_err(|_| {
                crate::app::dto_values::seam_error(rustfs_gateway_types::compat::ConversionError {
                    field,
                    reason: "not a header value",
                })
            })?;
        }
    }
    output.e_tag = output
        .e_tag
        .map(|value| {
            let escaped = quick_xml::escape::escape(value.opaque_tag()).into_owned();
            if value.is_weak() {
                ETag::new_weak(escaped)
            } else {
                ETag::new(escaped)
            }
            .map_err(|_| {
                super::conversion_error("CompleteMultipartUpload output", "e_tag: not an entity tag the gateway can write")
            })
        })
        .transpose()?;
    Ok(output)
}

pub(super) fn complete_multipart_error(mut error: S3Error) -> S3Error {
    if error.message().is_none() && matches!(error.code(), S3ErrorCode::PreconditionFailed) {
        error.set_message("At least one of the preconditions you specified did not hold.");
    }
    error
}

pub(super) fn abort_multipart_input(input: &dto::AbortMultipartUploadInput) -> S3Result<()> {
    input
        .if_match_initiated_time
        .map(|value| multipart_timestamp(value, "AbortMultipartUpload input", "if_match_initiated_time"))
        .transpose()?;
    Ok(())
}

pub(super) fn upload_part_input(
    input: dto::UploadPartInput,
    upload_id: String,
) -> crate::app::multipart_usecase::UploadPartWriteInput {
    use crate::app::multipart_usecase::{UploadPartTarget, UploadPartWriteInput};
    use rustfs_gateway_types::ChecksumAlgorithm;
    let checksum = |algorithm| {
        input
            .checksum_spec
            .as_ref()
            .filter(|value| value.algorithm() == algorithm)
            .map(|value| value.render_base64().to_owned())
    };
    UploadPartWriteInput {
        checksum_crc32: checksum(ChecksumAlgorithm::Crc32),
        checksum_crc32c: checksum(ChecksumAlgorithm::Crc32c),
        checksum_crc64nvme: checksum(ChecksumAlgorithm::Crc64Nvme),
        checksum_sha1: checksum(ChecksumAlgorithm::Sha1),
        checksum_sha256: checksum(ChecksumAlgorithm::Sha256),
        target: UploadPartTarget::Legacy {
            bucket: input.bucket.as_str().to_owned(),
            key: input.key.as_str().to_owned(),
            upload_id,
        },
        part_number: input.part_number,
        content_length: Some(input.content_length),
        body: input.body.map(|body| body.into_body()),
        trailing_headers: None,
        frame_trailers: None,
        content_md5: nonempty(input.content_md5),
        checksum_algorithm: nonempty(input.checksum_algorithm.map(|value| value.as_str().to_owned())),
        sse_customer_algorithm: nonempty(input.sse_customer_algorithm),
        sse_customer_key: input.sse_customer_key.map(|value| value.expose_secret().to_owned()),
        sse_customer_key_md5: nonempty(input.sse_customer_key_md5),
    }
}

pub(super) fn upload_part_output(
    output: crate::app::multipart_usecase::UploadPartWriteOutput,
) -> S3Result<dto::UploadPartOutput> {
    use rustfs_gateway_types::{ChecksumAlgorithm, ChecksumSpec};
    let error = |field, reason| super::conversion_error("UploadPart output", format!("{field}: {reason}"));
    let raw_etag = output
        .e_tag
        .ok_or_else(|| error("e_tag", "the gateway shape requires this member"))?;
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
                "the gateway shape carries one checksum, so a second would be lost",
            ));
        }
        (Some((field, algorithm, value)), None) => Some(
            ChecksumSpec::parse_header(algorithm.header_name(), &value)
                .map_err(|_| error(field, "not a checksum value of this algorithm's width"))?,
        ),
    };
    Ok(dto::UploadPartOutput {
        e_tag,
        checksum_spec,
        server_side_encryption: output.server_side_encryption.map(dto::ServerSideEncryption::custom),
        ssekms_key_id: output.ssekms_key_id,
        sse_customer_algorithm: output.sse_customer_algorithm,
        sse_customer_key_md5: output.sse_customer_key_md5,
        ..Default::default()
    })
}

pub(super) fn upload_part_error(error: S3Error) -> S3Error {
    let mut error = super::owned::put_error(error);
    if error.message().is_none() && matches!(error.code(), S3ErrorCode::EntityTooLarge) {
        error.set_message("Your proposed upload exceeds the maximum allowed object size.");
    }
    error
}

pub(super) fn upload_part_copy_input(
    input: dto::UploadPartCopyInput,
) -> S3Result<crate::app::multipart_usecase::UploadPartCopyWriteInput> {
    super::owned::validate_copy_source(&input.copy_source, "UploadPartCopy copy_source")?;
    let copy_source_if_match =
        super::owned::copy_condition(input.copy_source_if_match.as_deref(), "copy_source_if_match", "UploadPartCopy input")?;
    // Dates must be representable at the edge; storage does not evaluate date preconditions.
    // Dates must fit the edge codec; the storage path does not evaluate date preconditions.
    input
        .copy_source_if_modified_since
        .map(|at| multipart_timestamp(at, "UploadPartCopy input", "copy_source_if_modified_since"))
        .transpose()?;
    let copy_source_if_none_match = super::owned::copy_condition(
        input.copy_source_if_none_match.as_deref(),
        "copy_source_if_none_match",
        "UploadPartCopy input",
    )?;
    input
        .copy_source_if_unmodified_since
        .map(|at| multipart_timestamp(at, "UploadPartCopy input", "copy_source_if_unmodified_since"))
        .transpose()?;
    Ok(crate::app::multipart_usecase::UploadPartCopyWriteInput {
        bucket: input.bucket.as_str().to_owned(),
        key: input.key.as_str().to_owned(),
        upload_id: super::gateway_s3s_compat::leaf::upload_id_to_s3s(&input.upload_id),
        part_number: input.part_number,
        copy_source: input.copy_source,
        copy_source_range: nonempty(input.copy_source_range),
        copy_source_if_match,
        copy_source_if_none_match,
        sse_customer_algorithm: nonempty(input.sse_customer_algorithm),
        sse_customer_key: input.sse_customer_key.map(|value| value.expose_secret().to_owned()),
        sse_customer_key_md5: nonempty(input.sse_customer_key_md5),
        copy_source_sse_customer_algorithm: nonempty(input.copy_source_sse_customer_algorithm),
        copy_source_sse_customer_key: input
            .copy_source_sse_customer_key
            .map(|value| value.expose_secret().to_owned()),
        copy_source_sse_customer_key_md5: nonempty(input.copy_source_sse_customer_key_md5),
    })
}

pub(super) fn upload_part_copy_output(
    output: crate::app::multipart_usecase::UploadPartCopyWriteOutput,
) -> S3Result<dto::UploadPartCopyOutput> {
    let raw = output
        .e_tag
        .ok_or_else(|| super::conversion_error("UploadPartCopy output", "e_tag: the gateway shape requires this member"))?;
    Ok(dto::UploadPartCopyOutput {
        e_tag: super::owned::copy_xml_etag(&raw, "UploadPartCopy output")?,
        last_modified: output
            .last_modified
            .map(crate::app::dto_values::timestamp)
            .map(super::owned::millisecond_timestamp),
        copy_source_version_id: output.copy_source_version_id,
        server_side_encryption: output.server_side_encryption.map(dto::ServerSideEncryption::custom),
        ssekms_key_id: output.ssekms_key_id,
        sse_customer_algorithm: output.sse_customer_algorithm,
        sse_customer_key_md5: output.sse_customer_key_md5,
        checksum_crc32: output.checksum_crc32,
        checksum_crc32c: output.checksum_crc32c,
        checksum_sha1: output.checksum_sha1,
        checksum_sha256: output.checksum_sha256,
        checksum_crc64nvme: output.checksum_crc64nvme,
        ..Default::default()
    })
}

#[cfg(test)]
#[path = "multipart_test.rs"]
mod tests;
