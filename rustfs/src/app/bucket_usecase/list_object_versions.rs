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

//! ListObjectVersions: one page of a bucket's versions and its answer as the gateway's output types
//! hold it.
//!
//! Responsible for: reading the page (plain or table-isolated) and building the answer from the
//! stored values, raw.
//! NOT responsible for: `encoding-type=url` and the `max-keys` ceiling (the edge that writes the
//! answer), the authorization and its table snapshot (`storage/access_owned/listing_multipart.rs`),
//! or the `?metadata=true` listing (`listing.rs`).
//! Upstream: both S3 edges. Downstream: the store, `table_list_isolation`.

use super::list_objects::{listed_etag, listed_last_modified, listed_storage_class};
use super::*;
use crate::app::storage_api::bucket_usecase::contract::list::ListOperations as _;
use rustfs_gateway_types::OpaqueString;
use rustfs_s3_types::S3Result;

type ListObjectVersionsInfo = StorageListObjectVersionsInfo<ObjectInfo>;

impl DefaultBucketUsecase {
    /// One page of ListObjectVersions.
    ///
    /// # Errors
    ///
    /// `InvalidArgument` for a negative `max-keys`; `NoSuchBucket`; otherwise the store's error.
    #[instrument(level = "debug", skip(self, envelope, input))]
    pub async fn execute_list_object_versions(
        &self,
        envelope: RequestEnvelope,
        input: gateway_dto::ListObjectVersionsInput,
    ) -> S3Result<gateway_dto::ListObjectVersionsOutput> {
        let bucket = input.bucket.as_str().to_owned();
        let params = parse_list_object_versions_params(
            input.prefix,
            input.delimiter,
            input.key_marker,
            input.version_id_marker.map(|marker| marker.as_str().to_owned()),
            input.max_keys,
        )?;
        let store = get_validated_store(&bucket).await?;
        let info = match envelope.table_list_access() {
            Some(access) => {
                table_list_isolation::list_object_versions(
                    store.clone(),
                    access,
                    table_list_isolation::ListObjectVersionsRequest {
                        bucket: &bucket,
                        prefix: &params.prefix,
                        key_marker: params.key_marker.as_deref(),
                        version_id_marker: params.version_id_marker.as_deref(),
                        delimiter: params.delimiter.as_deref(),
                        max_keys: params.max_keys,
                    },
                )
                .await?
            }
            None => store
                .list_object_versions(
                    &bucket,
                    &params.prefix,
                    params.key_marker.clone(),
                    params.version_id_marker.clone(),
                    params.delimiter.clone(),
                    params.max_keys,
                )
                .await
                .map_err(ApiError::from)?,
        };
        list_object_versions_output(info, &bucket, params, input.encoding_type)
    }
}

/// A null version surfaces as `Some(Uuid::nil())` on the client-facing `ObjectInfo`; AWS writes it
/// as the literal `null`, and a nil UUID never reaches the wire (issue #6745).
fn listed_version_id(version_id: Option<uuid::Uuid>) -> OpaqueString {
    OpaqueString::from(
        version_id
            .filter(|id| !id.is_nil())
            .map_or_else(|| "null".to_owned(), |id| id.to_string()),
    )
}

/// The ListObjectVersions answer for one page the store returned.
///
/// # Errors
///
/// `InternalError` for a stored value the answer cannot carry.
fn list_object_versions_output(
    info: ListObjectVersionsInfo,
    bucket: &str,
    params: ListObjectVersionsParams,
    encoding_type: Option<gateway_dto::EncodingType>,
) -> S3Result<gateway_dto::ListObjectVersionsOutput> {
    let mut versions = Vec::new();
    let mut delete_markers = Vec::new();
    for object in info.objects.iter().filter(|object| !object.name.is_empty()) {
        let key = dto_values::stored_object_key(&object.name)?;
        let last_modified = listed_last_modified(object.mod_time);
        if object.delete_marker {
            delete_markers.push(gateway_dto::DeleteMarkerEntry {
                key,
                version_id: listed_version_id(object.version_id),
                is_latest: object.is_latest,
                last_modified,
                ..Default::default()
            });
        } else {
            versions.push(gateway_dto::ObjectVersion {
                key,
                last_modified,
                size: object.size,
                version_id: listed_version_id(object.version_id),
                is_latest: object.is_latest,
                e_tag: listed_etag(object),
                storage_class: listed_storage_class(object.storage_class.as_deref()),
                ..Default::default()
            });
        }
    }
    Ok(gateway_dto::ListObjectVersionsOutput {
        is_truncated: info.is_truncated,
        max_keys: params.max_keys,
        delimiter: params.delimiter,
        encoding_type,
        key_marker: params.key_marker.unwrap_or_default(),
        version_id_marker: OpaqueString::from(params.version_id_marker.unwrap_or_default()),
        name: dto_values::stored_bucket_name(bucket)?,
        prefix: params.prefix,
        common_prefixes: info
            .prefixes
            .into_iter()
            .map(|prefix| gateway_dto::CommonPrefix { prefix })
            .collect(),
        versions,
        delete_markers,
        // Only a marker that is set: an empty one breaks S3 clients that page on it.
        next_key_marker: info.next_marker.filter(|marker| !marker.is_empty()),
        next_version_id_marker: info
            .next_version_idmarker
            .filter(|marker| !marker.is_empty())
            .map(OpaqueString::from),
        ..Default::default()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::app::bucket_usecase::test_support::build_request;
    use rustfs_gateway_types::BucketName;
    use rustfs_s3_types::S3ErrorCode;

    fn entry(name: &str) -> ObjectInfo {
        ObjectInfo {
            name: name.to_owned(),
            mod_time: Some(time::OffsetDateTime::UNIX_EPOCH),
            storage_class: Some("STANDARD".to_owned()),
            ..Default::default()
        }
    }

    fn params(key_marker: Option<&str>, version_id_marker: Option<&str>, max_keys: i32) -> ListObjectVersionsParams {
        parse_list_object_versions_params(
            Some("prefix-a".to_owned()),
            Some("/".to_owned()),
            key_marker.map(str::to_owned),
            version_id_marker.map(str::to_owned),
            Some(max_keys),
        )
        .expect("valid parameters")
    }

    fn output(info: ListObjectVersionsInfo, params: ListObjectVersionsParams) -> gateway_dto::ListObjectVersionsOutput {
        list_object_versions_output(info, "bucket-a", params, None).expect("a representable page")
    }

    #[test]
    fn versions_delete_markers_and_markers_are_answered_raw() {
        let info = ListObjectVersionsInfo {
            is_truncated: true,
            next_marker: Some(String::new()),
            next_version_idmarker: Some("next-version-id".to_owned()),
            objects: vec![
                ObjectInfo {
                    size: 10,
                    etag: Some("etag-a".to_owned()),
                    version_id: Some(uuid::Uuid::nil()),
                    is_latest: true,
                    ..entry("logs and more/obj-a")
                },
                ObjectInfo {
                    delete_marker: true,
                    ..entry("obj-delete-marker")
                },
                ObjectInfo {
                    delete_marker: true,
                    version_id: Some(uuid::Uuid::nil()),
                    is_latest: true,
                    ..entry("obj-null-delete-marker")
                },
                entry(""),
            ],
            prefixes: vec!["photos and more/".to_owned()],
        };

        let output = output(info, params(Some("marker a"), Some("version-marker-a"), 123));

        assert!(output.is_truncated);
        assert_eq!(output.max_keys, 123);
        assert_eq!(output.name.as_str(), "bucket-a");
        assert_eq!(output.prefix, "prefix-a");
        assert_eq!(output.delimiter.as_deref(), Some("/"));
        assert_eq!(output.key_marker, "marker a");
        assert_eq!(output.version_id_marker.as_str(), "version-marker-a");
        assert_eq!(output.next_key_marker, None, "an empty marker is left out");
        assert_eq!(output.next_version_id_marker.as_ref().map(OpaqueString::as_str), Some("next-version-id"));
        assert_eq!(output.versions.len(), 1);
        assert_eq!(output.versions[0].key.as_str(), "logs and more/obj-a");
        // A null version's synthesized nil UUID is the literal `null` (issue #6745).
        assert_eq!(output.versions[0].version_id.as_str(), "null");
        assert_eq!(output.versions[0].size, 10);
        let markers: Vec<_> = output
            .delete_markers
            .iter()
            .map(|marker| (marker.key.as_str(), marker.version_id.as_str(), marker.is_latest))
            .collect();
        assert_eq!(markers, [("obj-delete-marker", "null", false), ("obj-null-delete-marker", "null", true)]);
        assert_eq!(output.common_prefixes[0].prefix, "photos and more/");
    }

    #[test]
    fn next_key_marker_is_the_plain_last_key() {
        let info = ListObjectVersionsInfo {
            is_truncated: true,
            next_marker: Some("obj-b".to_owned()),
            next_version_idmarker: Some("null".to_owned()),
            objects: vec![entry("obj-a"), entry("obj-b")],
            prefixes: Vec::new(),
        };

        let output = output(info, params(None, None, 2));

        assert_eq!(output.next_key_marker.as_deref(), Some("obj-b"));
        assert_eq!(output.next_version_id_marker.as_ref().map(OpaqueString::as_str), Some("null"));
        assert_eq!((output.key_marker.as_str(), output.version_id_marker.as_str()), ("", ""));
    }

    #[test]
    fn an_encryption_fingerprint_and_a_legacy_storage_label_are_never_listed() {
        let fingerprinted = ObjectInfo {
            etag: Some("0123456789abcdef0123456789abcdef".to_owned()),
            user_defined: Arc::new(HashMap::from([("x-amz-server-side-encryption".to_owned(), "aws:kms".to_owned())])),
            ..entry("kms")
        };
        let file_info = rustfs_filemeta::FileInfo {
            name: "legacy-object".to_owned(),
            version_id: Some(uuid::Uuid::from_u128(1)),
            mod_time: Some(time::OffsetDateTime::UNIX_EPOCH),
            metadata: HashMap::from([(rustfs_utils::http::headers::AMZ_STORAGE_CLASS.to_owned(), "STANDARD_IA".to_owned())]),
            ..Default::default()
        };
        let labelled = ObjectInfo::from_file_info(&file_info, "bucket", "legacy-object", true);
        let info = ListObjectVersionsInfo {
            objects: vec![fingerprinted, labelled],
            ..Default::default()
        };

        let output = output(info, params(None, None, 1000));

        assert!(output.versions[0].e_tag.is_none());
        assert_eq!(output.versions[1].storage_class.as_str(), "STANDARD");
    }

    #[test]
    fn an_unheld_fact_of_a_version_or_delete_marker_stays_unheld() {
        use rustfs_gateway_types::WirePlaceholder as _;
        let info = ListObjectVersionsInfo {
            objects: vec![
                ObjectInfo {
                    mod_time: None,
                    storage_class: None,
                    ..entry("version")
                },
                ObjectInfo {
                    mod_time: None,
                    delete_marker: true,
                    ..entry("marker")
                },
            ],
            ..Default::default()
        };

        let output = output(info, params(None, None, 1000));

        assert!(output.versions[0].last_modified.is_wire_placeholder());
        assert!(output.versions[0].storage_class.is_wire_placeholder());
        assert!(output.delete_markers[0].last_modified.is_wire_placeholder());
    }

    #[tokio::test]
    async fn list_object_versions_refuses_a_negative_max_keys_before_the_store() {
        let input = gateway_dto::ListObjectVersionsInput {
            bucket: BucketName::new("test-bucket").expect("bucket"),
            max_keys: Some(-1),
            ..Default::default()
        };

        let err = DefaultBucketUsecase::without_context()
            .execute_list_object_versions(RequestEnvelope::from_s3s(&build_request((), http::Method::GET)), input)
            .await
            .expect_err("a negative page size");
        assert_eq!(err.code(), &S3ErrorCode::InvalidArgument);
    }

    #[tokio::test]
    async fn list_object_versions_returns_internal_error_when_the_store_is_not_initialized() {
        let input = gateway_dto::ListObjectVersionsInput {
            bucket: BucketName::new("test-bucket").expect("bucket"),
            ..Default::default()
        };

        let err = DefaultBucketUsecase::without_context()
            .execute_list_object_versions(RequestEnvelope::from_s3s(&build_request((), http::Method::GET)), input)
            .await
            .expect_err("no store");
        assert_eq!(err.code(), &S3ErrorCode::InternalError);
    }
}
