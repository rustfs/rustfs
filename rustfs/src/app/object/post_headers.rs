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

//! Effective metadata and checksum inputs for an authorized browser upload.

use crate::app::RequestEnvelope;
use crate::app::storage_api::object_usecase::compression::DISK_COMPRESSION_EXCLUDED_CONTENT_TYPES;
use http::{HeaderMap, HeaderValue};
use rustfs_gateway::dto::PostObjectFields;
use rustfs_s3_types::{S3Result, s3_error};

pub(crate) fn post_effective_headers(envelope: &RequestEnvelope) -> &HeaderMap {
    envelope
        .post_object_policy_headers()
        .map(|headers| &headers.0)
        .unwrap_or_else(|| envelope.headers())
}

pub(crate) fn post_metadata_headers(
    envelope: &RequestEnvelope,
    content_type: Option<&str>,
    content_encoding: Option<&str>,
) -> S3Result<HeaderMap> {
    let policy = &envelope
        .post_object_policy_headers()
        .ok_or_else(|| s3_error!(InternalError, "POST policy headers are missing"))?
        .0;
    let mut headers = HeaderMap::new();
    for name in [
        "x-amz-object-lock-mode",
        "x-amz-object-lock-legal-hold",
        "x-amz-object-lock-retain-until-date",
        "x-amz-tagging",
        "x-amz-storage-class",
    ] {
        if let Some(value) = policy.get(name) {
            headers.insert(name, value.clone());
        }
    }
    if let Some(value) = content_type {
        let wire = HeaderValue::from_str(value).map_err(|_| s3_error!(InvalidArgument, "Invalid POST content field"))?;
        let base = value.split(';').next().unwrap_or(value).trim();
        if !base.is_ascii() {
            return Err(s3_error!(InvalidArgument, "Invalid POST content field"));
        }
        let base = base.to_ascii_lowercase();
        let content_type =
            if wire.to_str().is_err() || rustfs_utils::string::has_pattern(DISK_COMPRESSION_EXCLUDED_CONTENT_TYPES, &base) {
                HeaderValue::from_str(&base).map_err(|_| s3_error!(InvalidArgument, "Invalid POST content field"))?
            } else {
                wire
            };
        headers.insert("content-type", content_type);
    }
    if let Some(value) = content_encoding {
        let value = HeaderValue::from_str(value).map_err(|_| s3_error!(InvalidArgument, "Invalid POST content field"))?;
        value
            .to_str()
            .map_err(|_| s3_error!(InvalidArgument, "Invalid POST content field"))?;
        headers.insert("content-encoding", value);
    }
    Ok(headers)
}

pub(crate) fn post_checksum_headers(envelope: &RequestEnvelope, fields: &PostObjectFields) -> S3Result<Option<HeaderMap>> {
    post_checksum_headers_inner(envelope, fields, false)
}

pub(crate) fn post_archive_checksum_headers(
    envelope: &RequestEnvelope,
    fields: &PostObjectFields,
) -> S3Result<Option<HeaderMap>> {
    post_checksum_headers_inner(envelope, fields, true)
}

fn post_checksum_headers_inner(
    envelope: &RequestEnvelope,
    fields: &PostObjectFields,
    allow_archive: bool,
) -> S3Result<Option<HeaderMap>> {
    let checksums = [
        ("x-amz-checksum-crc32", "CRC32", fields.checksum_crc32.as_deref()),
        ("x-amz-checksum-crc32c", "CRC32C", fields.checksum_crc32c.as_deref()),
        ("x-amz-checksum-crc64nvme", "CRC64NVME", fields.checksum_crc64nvme.as_deref()),
        ("x-amz-checksum-md5", "MD5", fields.checksum_md5.as_deref()),
        ("x-amz-checksum-sha1", "SHA1", fields.checksum_sha1.as_deref()),
        ("x-amz-checksum-sha256", "SHA256", fields.checksum_sha256.as_deref()),
        ("x-amz-checksum-sha512", "SHA512", fields.checksum_sha512.as_deref()),
        ("x-amz-checksum-xxhash3", "XXHASH3", fields.checksum_xxhash3.as_deref()),
        ("x-amz-checksum-xxhash64", "XXHASH64", fields.checksum_xxhash64.as_deref()),
        ("x-amz-checksum-xxhash128", "XXHASH128", fields.checksum_xxhash128.as_deref()),
    ];
    let invalid = || s3_error!(InvalidRequest, "The POST checksum declarations conflict");
    let headers = envelope.headers();
    let mut form = None;
    let mut wire = None;
    for (name, algorithm, value) in checksums {
        if let Some(value) = value
            && form.replace((name, algorithm, value)).is_some()
        {
            return Err(invalid());
        }
        for value in headers.get_all(name) {
            let value = value.to_str().map_err(|_| invalid())?;
            if wire.replace((name, algorithm, value)).is_some() {
                return Err(invalid());
            }
        }
    }
    if form.is_some() && wire.is_some() && form != wire {
        return Err(invalid());
    }
    let algorithm = fields.checksum_algorithm.as_ref().map(|value| value.as_str());
    let header_algorithm = headers.get("x-amz-checksum-algorithm");
    let sdk_algorithm = headers.get("x-amz-sdk-checksum-algorithm");
    if headers.get_all("x-amz-checksum-algorithm").iter().count() > 1
        || headers.get_all("x-amz-sdk-checksum-algorithm").iter().count() > 1
    {
        return Err(invalid());
    }
    let trailer = ["x-amz-trailer", "trailer"].iter().any(|header| {
        headers.get_all(*header).iter().any(|value| {
            value.to_str().map_or(true, |value| {
                value.split(',').any(|name| {
                    checksums
                        .iter()
                        .any(|(checksum, _, _)| checksum.eq_ignore_ascii_case(name.trim()))
                })
            })
        })
    });
    if trailer {
        return Err(invalid());
    }
    let selected = form.or(wire);
    if !allow_archive
        && super::is_put_object_extract_requested(post_effective_headers(envelope))
        && (selected.is_some()
            || algorithm.is_some()
            || header_algorithm.is_some()
            || sdk_algorithm.is_some()
            || fields.content_md5.is_some()
            || headers.contains_key("content-md5"))
    {
        return Err(s3_error!(InvalidRequest, "POST archive extraction cannot use a checksum"));
    }
    let Some((name, expected_algorithm, value)) = selected else {
        return if algorithm.is_some() || header_algorithm.is_some() || sdk_algorithm.is_some() {
            Err(invalid())
        } else {
            Ok(None)
        };
    };
    if value.is_empty() || value.contains('-') || algorithm.is_some_and(|value| !value.eq_ignore_ascii_case(expected_algorithm)) {
        return Err(invalid());
    }
    for header in ["x-amz-checksum-algorithm", "x-amz-sdk-checksum-algorithm"] {
        if let Some(value) = headers.get(header)
            && !value
                .to_str()
                .is_ok_and(|value| value.eq_ignore_ascii_case(expected_algorithm))
        {
            return Err(invalid());
        }
    }
    let value = HeaderValue::from_str(value).map_err(|_| invalid())?;
    let mut validation = HeaderMap::new();
    validation.insert(name, value.clone());
    if !matches!(rustfs_rio::get_content_checksum(&validation), Ok(Some(_))) {
        return Err(invalid());
    }
    Ok(form.map(|_| {
        let mut projected = HeaderMap::new();
        for name in ["x-amz-checksum-algorithm", "x-amz-checksum-type"] {
            if let Some(value) = headers.get(name) {
                projected.insert(name, value.clone());
            }
        }
        projected.insert(name, value);
        projected
    }))
}

pub(crate) fn post_archive_content_md5(envelope: &RequestEnvelope, fields: &PostObjectFields) -> S3Result<Option<String>> {
    let mut values = envelope.headers().get_all("content-md5").iter();
    let wire = values
        .next()
        .map(|value| value.to_str().map_err(|_| s3_error!(InvalidDigest)))
        .transpose()?;
    if values.next().is_some()
        || wire
            .zip(fields.content_md5.as_deref())
            .is_some_and(|(wire, form)| wire != form)
    {
        return Err(s3_error!(InvalidRequest, "The POST content-MD5 declarations conflict"));
    }
    Ok(fields.content_md5.as_deref().or(wire).map(str::to_owned))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::RequestHead;
    use crate::app::storage_api::context::PostObjectPolicyHeaders;
    use http::{Extensions, Method, Uri};
    use rustfs_s3_types::S3ErrorCode;

    fn request(headers: HeaderMap, policy: HeaderMap) -> RequestEnvelope {
        let mut extensions = Extensions::new();
        extensions.insert(PostObjectPolicyHeaders(policy));
        RequestEnvelope::from_parts(
            RequestHead {
                method: Method::POST,
                uri: Uri::from_static("/bucket"),
                headers,
                credentials: None,
                region: None,
                service: None,
            },
            extensions,
        )
    }

    #[test]
    fn post_metadata_projection_cannot_replace_signed_fields_or_audit_headers() {
        let mut wire = HeaderMap::new();
        wire.insert("x-amz-tagging", HeaderValue::from_static("classification=private"));
        wire.insert("x-amz-storage-class", HeaderValue::from_static("REDUCED_REDUNDANCY"));
        wire.insert("x-amz-object-lock-mode", HeaderValue::from_static("GOVERNANCE"));
        wire.insert("x-amz-meta-purpose", HeaderValue::from_static("wire"));
        wire.insert("content-encoding", HeaderValue::from_static("wire"));
        let mut policy = wire.clone();
        policy.insert("x-amz-tagging", HeaderValue::from_static("classification=public"));
        policy.insert("x-amz-storage-class", HeaderValue::from_static("STANDARD"));
        policy.remove("x-amz-object-lock-mode");
        let request = request(wire, policy);

        let headers = post_metadata_headers(&request, Some("text/plain"), Some("gzip")).expect("project metadata");
        assert_eq!(headers["x-amz-tagging"], "classification=public");
        assert_eq!(headers["x-amz-storage-class"], "STANDARD");
        assert!(!headers.contains_key("x-amz-object-lock-mode"));
        assert!(!headers.contains_key("x-amz-meta-purpose"));
        assert_eq!(headers["content-type"], "text/plain");
        assert_eq!(headers["content-encoding"], "gzip");
        assert_eq!(request.headers()["x-amz-tagging"], "classification=private");
        assert_eq!(request.headers()["x-amz-meta-purpose"], "wire");
        assert_eq!(request.headers()["content-encoding"], "wire");
        let absent = post_metadata_headers(&request, None, None).expect("project absent content fields");
        assert!(!absent.contains_key("content-type"));
        assert!(!absent.contains_key("content-encoding"));
        let archive = post_metadata_headers(&request, Some("Application/ZIP; charset=binary"), None).expect("archive MIME");
        assert_eq!(archive["content-type"], "application/zip");
        let custom = post_metadata_headers(&request, Some("text/plain; charset=utf-8"), None).expect("custom MIME");
        assert_eq!(custom["content-type"], "text/plain; charset=utf-8");
        let unicode =
            post_metadata_headers(&request, Some("application/zip; name=\"café\""), None).expect("UTF-8 MIME parameter");
        assert_eq!(unicode["content-type"], "application/zip");
        assert!(
            post_metadata_headers(&request, None, Some("gzip, é")).is_err(),
            "non-ASCII content encoding must not bypass validation"
        );
    }

    #[test]
    fn post_checksum_projection_rejects_ambiguous_sources() {
        let fields = PostObjectFields {
            checksum_crc32: Some("NhCmhg==".to_owned()),
            ..Default::default()
        };
        let mut wire = HeaderMap::new();
        wire.insert("x-amz-server-side-encryption-customer-key", HeaderValue::from_static("secret"));
        let envelope = request(wire.clone(), HeaderMap::new());
        let projected = post_checksum_headers(&envelope, &fields)
            .expect("valid form checksum")
            .expect("checksum projection");
        assert_eq!(projected["x-amz-checksum-crc32"], "NhCmhg==");
        assert!(!projected.contains_key("x-amz-server-side-encryption-customer-key"));
        assert!(!envelope.headers().contains_key("x-amz-checksum-crc32"));

        for (name, value) in [
            ("x-amz-checksum-crc32", "AAAAAA=="),
            ("x-amz-checksum-algorithm", "SHA256"),
            ("x-amz-trailer", "x-amz-checksum-crc32"),
            ("trailer", "X-Amz-Checksum-Crc32"),
        ] {
            let mut conflict = wire.clone();
            conflict.insert(name, HeaderValue::from_str(value).expect("test header"));
            let error = post_checksum_headers(&request(conflict, HeaderMap::new()), &fields).expect_err("conflicting checksum");
            assert_eq!(error.code(), &S3ErrorCode::InvalidRequest, "{name}");
        }

        let mut mismatch = wire;
        mismatch.insert("x-amz-checksum-crc32", HeaderValue::from_static("AAAAAA=="));
        mismatch.insert("x-amz-checksum-algorithm", HeaderValue::from_static("SHA256"));
        let error = post_checksum_headers(&request(mismatch, HeaderMap::new()), &PostObjectFields::default())
            .expect_err("wire algorithm cannot hide a different checksum");
        assert_eq!(error.code(), &S3ErrorCode::InvalidRequest);
    }

    #[test]
    fn post_archive_checksum_projection_preserves_closed_guard_and_rejects_md5_conflicts() {
        let mut wire = HeaderMap::new();
        wire.insert("content-md5", HeaderValue::from_static("XUFAKrxLKna5cZ2REBfFkg=="));
        let mut policy = HeaderMap::new();
        policy.insert("x-amz-meta-snowball-auto-extract", HeaderValue::from_static("true"));
        let fields = PostObjectFields {
            content_md5: Some("XUFAKrxLKna5cZ2REBfFkg==".to_owned()),
            checksum_crc32: Some("NhCmhg==".to_owned()),
            ..Default::default()
        };
        let envelope = request(wire.clone(), policy.clone());
        assert_eq!(
            post_checksum_headers(&envelope, &fields)
                .expect_err("closed archive refuses checksums")
                .code(),
            &S3ErrorCode::InvalidRequest
        );
        let headers = post_archive_checksum_headers(&envelope, &fields)
            .expect("valid archive checksums")
            .expect("form checksum projection");
        assert_eq!(headers["x-amz-checksum-crc32"], "NhCmhg==");
        assert_eq!(
            post_archive_content_md5(&envelope, &fields).expect("matching MD5").as_deref(),
            Some("XUFAKrxLKna5cZ2REBfFkg==")
        );

        wire.insert("content-md5", HeaderValue::from_static("AAAAAAAAAAAAAAAAAAAAAA=="));
        let conflict = request(wire.clone(), policy.clone());
        assert_eq!(
            post_archive_content_md5(&conflict, &fields)
                .expect_err("different MD5 sources")
                .code(),
            &S3ErrorCode::InvalidRequest
        );
        wire.append("content-md5", HeaderValue::from_static("AAAAAAAAAAAAAAAAAAAAAA=="));
        let duplicate = request(wire, policy);
        assert_eq!(
            post_archive_content_md5(&duplicate, &PostObjectFields::default())
                .expect_err("duplicate MD5")
                .code(),
            &S3ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn post_checksum_projection_carries_each_supported_form_algorithm() {
        let mut failures = Vec::new();
        for (name, length) in [
            ("crc32", 4),
            ("crc32c", 4),
            ("crc64nvme", 8),
            ("md5", 16),
            ("sha1", 20),
            ("sha256", 32),
            ("sha512", 64),
            ("xxhash3", 8),
            ("xxhash64", 8),
            ("xxhash128", 16),
        ] {
            let encoded = base64_simd::STANDARD.encode_to_string(vec![0; length]);
            let mut fields = PostObjectFields::default();
            match name {
                "crc32" => fields.checksum_crc32 = Some(encoded.clone()),
                "crc32c" => fields.checksum_crc32c = Some(encoded.clone()),
                "crc64nvme" => fields.checksum_crc64nvme = Some(encoded.clone()),
                "md5" => fields.checksum_md5 = Some(encoded.clone()),
                "sha1" => fields.checksum_sha1 = Some(encoded.clone()),
                "sha256" => fields.checksum_sha256 = Some(encoded.clone()),
                "sha512" => fields.checksum_sha512 = Some(encoded.clone()),
                "xxhash3" => fields.checksum_xxhash3 = Some(encoded.clone()),
                "xxhash64" => fields.checksum_xxhash64 = Some(encoded.clone()),
                "xxhash128" => fields.checksum_xxhash128 = Some(encoded.clone()),
                _ => unreachable!(),
            }
            let envelope = request(HeaderMap::new(), HeaderMap::new());
            let header = format!("x-amz-checksum-{name}");
            let actual = match post_checksum_headers(&envelope, &fields) {
                Ok(Some(projected)) => projected
                    .get(header.as_str())
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned),
                _ => None,
            };
            if actual.as_deref() != Some(encoded.as_str()) {
                failures.push((name, actual));
            }
        }
        assert!(failures.is_empty(), "checksum projections failed: {failures:?}");
    }
}
