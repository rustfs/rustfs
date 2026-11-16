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

//! Read the stored object attributes requested by either S3 edge.

use super::head::ResponseEtag;
use super::*;
use crate::app::{Answered, RequestEnvelope};
use rustfs_s3_types::{S3Error, S3ErrorCode, S3Result};

const ETAG: &str = "ETag";
const CHECKSUM: &str = "Checksum";
const OBJECT_PARTS: &str = "ObjectParts";
const STORAGE_CLASS: &str = "StorageClass";
const OBJECT_SIZE: &str = "ObjectSize";

/// Decoded inputs preserve the version spelling and the raw attribute choices until use.
/// Customer key material must never be rendered as Debug output.
pub(crate) struct GetObjectAttributesFields {
    pub(crate) bucket: String,
    pub(crate) key: String,
    pub(crate) version_id: Option<String>,
    pub(crate) object_attributes: Vec<String>,
    pub(crate) max_parts: Option<i32>,
    pub(crate) part_number_marker: Option<String>,
    pub(crate) sse_customer_key: Option<String>,
    pub(crate) sse_customer_key_md5: Option<String>,
}

/// Stored values are encoded by the response edge after the operation completes.
pub(crate) struct GetObjectAttributesOutputFacts {
    pub(crate) e_tag: Option<ResponseEtag>,
    pub(crate) last_modified: Option<OffsetDateTime>,
    pub(crate) version_id: Option<String>,
    pub(crate) storage_class: Option<String>,
    pub(crate) object_size: Option<i64>,
    pub(crate) checksum: Option<ResponseChecksums>,
    pub(crate) object_parts: Option<AttributesPartsFacts>,
}

pub(crate) struct AttributesPartsFacts {
    pub(crate) is_truncated: bool,
    pub(crate) max_parts: i32,
    pub(crate) next_part_number_marker: Option<i32>,
    pub(crate) part_number_marker: Option<i32>,
    pub(crate) parts: Vec<AttributesPartFacts>,
    pub(crate) total_parts_count: i32,
}

pub(crate) struct AttributesPartFacts {
    pub(crate) part_number: Option<i32>,
    pub(crate) size: i64,
    pub(crate) checksums: ResponseChecksums,
}

impl DefaultObjectUsecase {
    pub(crate) async fn execute_get_object_attributes(
        &self,
        envelope: RequestEnvelope,
        input: GetObjectAttributesFields,
    ) -> S3Result<Answered<GetObjectAttributesOutputFacts>> {
        if let Some(context) = &self.context {
            let _ = context.object_store();
        }

        let mut helper = OperationHelper::from_view(
            &envelope.audit_view(),
            EventName::ObjectAccessedAttributes,
            S3Operation::GetObjectAttributes,
        )
        .suppress_event();
        let GetObjectAttributesFields {
            bucket,
            key,
            version_id,
            object_attributes,
            max_parts,
            part_number_marker,
            sse_customer_key,
            sse_customer_key_md5,
        } = input;

        let Some(store) = self.object_store() else {
            return Err(S3Error::with_message(S3ErrorCode::InternalError, "Not init"));
        };

        let headers = envelope.headers();
        let mut opts = get_opts(&bucket, &key, version_id, None, headers)
            .await
            .map_err(ApiError::from)?;
        let requested = |name| object_attributes_requested(&object_attributes, name);
        opts.include_part_checksums = requested(OBJECT_PARTS);

        let info = match store.get_object_info(&bucket, &key, &opts).await {
            Ok(info) => info,
            Err(err) => {
                if is_err_object_not_found(&err) || is_err_version_not_found(&err) {
                    if is_dir_object(&key) {
                        let has_children = match probe_prefix_has_children(store, &bucket, &key, false).await {
                            Ok(has_children) => has_children,
                            Err(e) => {
                                error!(
                                    "Failed to probe children for object attributes (bucket: {}, key: {}): {}",
                                    bucket, key, e
                                );
                                false
                            }
                        };
                        let msg = head_prefix_not_found_message(&bucket, &key, has_children);
                        return Err(S3Error::with_message(S3ErrorCode::NoSuchKey, msg));
                    }
                    return Err(S3Error::new(S3ErrorCode::NoSuchKey));
                }
                return Err(ApiError::from(err).into());
            }
        };

        if info.delete_marker {
            if opts.version_id.is_none() {
                return Err(S3Error::new(S3ErrorCode::NoSuchKey));
            }
            return Err(S3Error::new(S3ErrorCode::MethodNotAllowed));
        }

        validate_ssec_for_read(&info.user_defined, sse_customer_key.as_ref(), sse_customer_key_md5.as_ref())?;

        let storage_class = response_storage_class_for_object_attributes(&info, &info.user_defined, requested(STORAGE_CLASS));
        let e_tag = if requested(ETAG) {
            info.etag.as_deref().map(ResponseEtag::from_stored)
        } else {
            None
        };
        let object_size = if requested(OBJECT_SIZE) {
            Some(info.get_actual_size().map_err(ApiError::from)?)
        } else {
            None
        };
        let checksum = if requested(CHECKSUM) {
            let (checksums, is_multipart) = info.decrypt_checksums(0, headers).map_err(ApiError::from)?;
            Some(classify_response_checksums(checksums, is_multipart))
        } else {
            None
        };
        let object_parts = if requested(OBJECT_PARTS) && info.is_multipart() {
            Some(read_parts(&info, headers, part_number_marker.as_deref(), max_parts)?)
        } else {
            None
        };

        let version_id = s3_response_version_id(info.version_id);
        let output = GetObjectAttributesOutputFacts {
            e_tag,
            last_modified: info.mod_time,
            version_id: version_id.clone(),
            storage_class,
            object_size,
            checksum,
            object_parts,
        };

        helper = helper.object(info).version_id(version_id.unwrap_or_default());
        let result = Ok(Answered::new(output));
        let _ = helper.complete_owned(&result, StatusCode::OK, &HeaderMap::new());
        result
    }
}

fn read_parts(
    info: &ObjectInfo,
    headers: &HeaderMap,
    part_number_marker: Option<&str>,
    max_parts: Option<i32>,
) -> S3Result<AttributesPartsFacts> {
    // Legacy headers have already decoded this as an i32. Native marker text is
    // only parsed when a stored multipart object actually needs a page.
    let part_number_marker = part_number_marker
        .map(str::parse::<i32>)
        .transpose()
        .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid part-number-marker"))?;
    let params = parse_list_parts_params(part_number_marker, max_parts)?;
    let mut parts = Vec::new();
    let mut marker = params.part_number_marker;
    let mut start_at = 0;

    if let Some(value) = marker {
        if let Some(index) = info.parts.iter().position(|part| part.number == value) {
            start_at = index + 1;
        } else {
            marker = None;
        }
    }

    let max_parts = i32::try_from(params.max_parts)
        .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "max-parts value is out of range"))?;
    let end = (start_at + params.max_parts).min(info.parts.len());

    for part in &info.parts[start_at..end] {
        let (checksums, is_multipart) = info.decrypt_checksums(part.number, headers).map_err(ApiError::from)?;
        let checksums = classify_response_checksums(checksums, is_multipart);
        let size = if part.actual_size > 0 {
            part.actual_size
        } else {
            part.size
                .try_into()
                .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "Part size value is out of range"))?
        };
        parts.push(AttributesPartFacts {
            part_number: i32::try_from(part.number).ok(),
            size,
            checksums,
        });
    }

    Ok(AttributesPartsFacts {
        is_truncated: end < info.parts.len(),
        max_parts,
        next_part_number_marker: parts.last().and_then(|part| part.part_number),
        part_number_marker: marker.and_then(|value| i32::try_from(value).ok()),
        parts,
        total_parts_count: i32::try_from(info.parts.len())
            .map_err(|_| S3Error::with_message(S3ErrorCode::InvalidArgument, "Part count is out of range"))?,
    })
}

fn object_attributes_requested(object_attributes: &[String], name: &str) -> bool {
    object_attributes.iter().any(|value| {
        value.split(',').any(|part| {
            part.trim_matches(|c: char| c.is_whitespace() || c == '"' || c == '\'')
                .eq_ignore_ascii_case(name)
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn execute_get_object_attributes_returns_internal_error_when_store_uninitialized() {
        let fields = GetObjectAttributesFields {
            bucket: "test-bucket".to_owned(),
            key: "test-key".to_owned(),
            version_id: None,
            object_attributes: vec![],
            max_parts: None,
            part_number_marker: None,
            sse_customer_key: None,
            sse_customer_key_md5: None,
        };
        let request = build_request((), http::Method::GET);
        let envelope = RequestEnvelope::from_s3s(&request);
        let error = DefaultObjectUsecase::without_context()
            .execute_get_object_attributes(envelope, fields)
            .await
            .err()
            .expect("uninitialized store");
        assert_eq!(error.code(), &S3ErrorCode::InternalError);
    }

    #[test]
    fn object_attributes_requested_with_single_value() {
        let selection = vec![ETAG.to_owned()];
        assert!(object_attributes_requested(&selection, ETAG));
        assert!(!object_attributes_requested(&selection, OBJECT_SIZE));
    }

    #[test]
    fn object_attributes_requested_with_comma_separated_values() {
        let selection = vec!["ObjectParts,etag".to_owned(), "StorageClass".to_owned()];
        assert!(object_attributes_requested(&selection, OBJECT_PARTS));
        assert!(object_attributes_requested(&selection, ETAG));
        assert!(!object_attributes_requested(&selection, OBJECT_SIZE));
    }

    #[test]
    fn object_attributes_requested_with_quotes_and_spaces() {
        let selection = vec!["'ObjectSize', \"Checksum\" , \"Etag\"".to_owned()];
        assert!(object_attributes_requested(&selection, OBJECT_SIZE));
        assert!(object_attributes_requested(&selection, CHECKSUM));
        assert!(object_attributes_requested(&selection, ETAG));
    }

    #[test]
    fn object_attributes_requested_returns_false_for_missing_name() {
        let selection = vec!["Checksum".to_owned()];
        assert!(!object_attributes_requested(&selection, OBJECT_SIZE));
    }
}
