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

use crate::app::storage_api::bucket_usecase::access::{
    TABLE_DATA_PLANE_LIST_CURSOR_PREFIX, TableDataPlaneListAccess, TableDataPlaneListCursorPosition,
};
use crate::app::storage_api::bucket_usecase::contract::list::{ListObjectVersionsInfo, ListObjectsV2Info, ListOperations as _};
use crate::app::storage_api::bucket_usecase::{ECStore, StorageObjectInfo as ObjectInfo};
use crate::app::storage_api::multipart_usecase::contract::multipart::{ListMultipartsInfo, MultipartInfo};
use crate::app::storage_api::s3::{S3Error, S3ErrorCode, S3Result};
use crate::error::ApiError;
use std::collections::HashSet;
use std::sync::Arc;
use uuid::Uuid;

const TABLE_LIST_RAW_PAGE_SIZE: i32 = 1000;
const TABLE_LIST_RAW_PAGE_SIZE_USIZE: usize = 1000;
const TABLE_LIST_MAX_RAW_PAGES: usize = 16;
const LIST_OBJECTS_V2_OPERATION: &str = "ListObjectsV2";
const LIST_OBJECTS_V2_INCLUDE_DELETED_OPERATION: &str = "ListObjectsV2:include-deleted";
const LIST_OBJECT_VERSIONS_OPERATION: &str = "ListObjectVersions";
const LIST_MULTIPART_UPLOADS_OPERATION: &str = "ListMultipartUploads";

enum ObjectListEntry {
    Object,
    Prefix(String),
}

pub(crate) struct ListObjectsV2Request<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) prefix: &'a str,
    pub(crate) continuation_token: Option<&'a str>,
    pub(crate) delimiter: Option<&'a str>,
    pub(crate) max_keys: i32,
    pub(crate) start_after: Option<&'a str>,
    pub(crate) incl_deleted: bool,
    pub(crate) opaque_cursor_supported: bool,
}

pub(crate) struct ListObjectVersionsRequest<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) prefix: &'a str,
    pub(crate) key_marker: Option<&'a str>,
    pub(crate) version_id_marker: Option<&'a str>,
    pub(crate) delimiter: Option<&'a str>,
    pub(crate) max_keys: i32,
}

pub(crate) struct ListMultipartUploadsRequest<'a> {
    pub(crate) bucket: &'a str,
    pub(crate) prefix: &'a str,
    pub(crate) key_marker: Option<&'a str>,
    pub(crate) upload_id_marker: Option<&'a str>,
    pub(crate) delimiter: Option<&'a str>,
    pub(crate) max_uploads: usize,
    pub(crate) expected_incarnation_id: Uuid,
}

fn table_list_scan_limit_error() -> S3Error {
    S3Error::with_message(
        S3ErrorCode::SlowDown,
        "Table listing exceeded the protected scan limit; retry with a narrower prefix".to_string(),
    )
}

fn invalid_table_list_cursor() -> S3Error {
    S3Error::with_message(S3ErrorCode::InvalidArgument, "Invalid table listing continuation token".to_string())
}

fn list_objects_v2_operation(incl_deleted: bool) -> &'static str {
    if incl_deleted {
        LIST_OBJECTS_V2_INCLUDE_DELETED_OPERATION
    } else {
        LIST_OBJECTS_V2_OPERATION
    }
}

fn object_list_entry(object: &ObjectInfo, prefix: &str, delimiter: Option<&str>) -> Option<ObjectListEntry> {
    if object.name.is_empty() {
        return None;
    }
    let Some(delimiter) = delimiter else {
        return Some(ObjectListEntry::Object);
    };
    let suffix = object.name.strip_prefix(prefix)?;
    match suffix.find(delimiter) {
        Some(index) => Some(ObjectListEntry::Prefix(format!("{prefix}{}", &suffix[..index + delimiter.len()]))),
        None => Some(ObjectListEntry::Object),
    }
}

fn multipart_list_entry(upload: MultipartInfo, prefix: &str, delimiter: Option<&str>) -> Option<MultipartListEntry> {
    if upload.object.is_empty() {
        return None;
    }
    let Some(delimiter) = delimiter else {
        return Some(MultipartListEntry::Upload(upload));
    };
    let suffix = upload.object.strip_prefix(prefix)?;
    match suffix.find(delimiter) {
        Some(index) => Some(MultipartListEntry::Prefix(format!("{prefix}{}", &suffix[..index + delimiter.len()]))),
        None => Some(MultipartListEntry::Upload(upload)),
    }
}

enum MultipartListEntry {
    Upload(MultipartInfo),
    Prefix(String),
}

fn table_list_cursor_from_markers(
    access: &TableDataPlaneListAccess,
    operation: &str,
    prefix: &str,
    delimiter: Option<&str>,
    max_keys: usize,
    marker: Option<&str>,
    version_marker: Option<&str>,
) -> S3Result<(Option<String>, Option<String>, Option<String>)> {
    let Some(marker) = marker else {
        return Ok((None, None, None));
    };
    if !marker.starts_with(TABLE_DATA_PLANE_LIST_CURSOR_PREFIX) || version_marker != Some(marker) {
        return Ok((Some(marker.to_string()), version_marker.map(str::to_string), None));
    }
    let cursor = access.decode_cursor(operation, prefix, delimiter, max_keys, marker)?;
    Ok((
        cursor.marker().map(str::to_string),
        cursor.version_marker().map(str::to_string),
        cursor.common_prefix().map(str::to_string),
    ))
}

pub(crate) async fn list_objects_v2(
    store: Arc<ECStore>,
    access: &TableDataPlaneListAccess,
    request: ListObjectsV2Request<'_>,
) -> S3Result<ListObjectsV2Info<ObjectInfo>> {
    let ListObjectsV2Request {
        bucket,
        prefix,
        continuation_token,
        delimiter,
        max_keys,
        start_after,
        incl_deleted,
        opaque_cursor_supported,
    } = request;
    if max_keys == 0 {
        return Ok(ListObjectsV2Info::default());
    }
    let max_keys = usize::try_from(max_keys).map_err(|_| invalid_table_list_cursor())?;
    let operation = list_objects_v2_operation(incl_deleted);
    let (mut marker, mut last_visible_common_prefix) = match continuation_token {
        Some(token) => {
            if !token.starts_with(TABLE_DATA_PLANE_LIST_CURSOR_PREFIX) {
                return Err(invalid_table_list_cursor());
            }
            let cursor = access.decode_cursor(operation, prefix, delimiter, max_keys, token)?;
            (cursor.marker().map(str::to_string), cursor.common_prefix().map(str::to_string))
        }
        None => (None, None),
    };
    let common_prefix_floor = continuation_token.is_none().then_some(start_after).flatten();
    let mut raw_start_after = continuation_token
        .is_none()
        .then(|| start_after.map(str::to_string))
        .flatten();
    let mut objects = Vec::new();
    let mut prefixes = Vec::new();
    let mut emitted_prefixes = HashSet::new();
    if let Some(common_prefix) = &last_visible_common_prefix {
        emitted_prefixes.insert(common_prefix.clone());
    }

    for page_index in 0..TABLE_LIST_MAX_RAW_PAGES {
        let raw = store
            .clone()
            .list_objects_v2(
                bucket,
                prefix,
                marker.clone(),
                None,
                TABLE_LIST_RAW_PAGE_SIZE,
                false,
                raw_start_after.take(),
                incl_deleted,
            )
            .await
            .map_err(ApiError::from)?;

        let mut marker_after_visible = marker.clone();
        let mut marker_after_scanned = marker.clone();
        let mut found_more = false;
        for object in raw.objects {
            let object_name = object.name.clone();
            let next_marker = Some(object_name.clone());
            marker_after_scanned.clone_from(&next_marker);
            let Some(entry) = object_list_entry(&object, prefix, delimiter) else {
                continue;
            };
            if matches!(&entry, ObjectListEntry::Prefix(prefix) if common_prefix_floor.is_some_and(|floor| prefix.as_str() <= floor))
            {
                marker_after_visible.clone_from(&next_marker);
                continue;
            }
            if matches!(&entry, ObjectListEntry::Prefix(prefix) if emitted_prefixes.contains(prefix)) {
                marker_after_visible.clone_from(&next_marker);
                continue;
            }
            if !access.allows_object(&object_name) {
                continue;
            }
            match entry {
                ObjectListEntry::Object if objects.len() + prefixes.len() < max_keys => {
                    objects.push(object);
                    marker_after_visible.clone_from(&next_marker);
                    last_visible_common_prefix = None;
                }
                ObjectListEntry::Prefix(prefix) if objects.len() + prefixes.len() < max_keys => {
                    emitted_prefixes.insert(prefix.clone());
                    prefixes.push(prefix);
                    marker_after_visible.clone_from(&next_marker);
                    last_visible_common_prefix = prefixes.last().cloned();
                }
                _ => {
                    found_more = true;
                    break;
                }
            }
        }

        if found_more {
            let next_continuation_token = access.encode_cursor(
                operation,
                prefix,
                delimiter,
                max_keys,
                TableDataPlaneListCursorPosition {
                    marker: marker_after_visible,
                    version_marker: None,
                    common_prefix: last_visible_common_prefix,
                },
            )?;
            return Ok(ListObjectsV2Info {
                is_truncated: true,
                next_continuation_token: Some(next_continuation_token),
                objects,
                prefixes,
                ..Default::default()
            });
        }
        if !raw.is_truncated {
            return Ok(ListObjectsV2Info {
                objects,
                prefixes,
                ..Default::default()
            });
        }
        if marker_after_scanned == marker {
            return Err(table_list_scan_limit_error());
        }
        marker = marker_after_scanned;
        if page_index + 1 == TABLE_LIST_MAX_RAW_PAGES {
            if !opaque_cursor_supported {
                return Err(table_list_scan_limit_error());
            }
            let next_continuation_token = access.encode_cursor(
                operation,
                prefix,
                delimiter,
                max_keys,
                TableDataPlaneListCursorPosition {
                    marker,
                    version_marker: None,
                    common_prefix: last_visible_common_prefix,
                },
            )?;
            return Ok(ListObjectsV2Info {
                is_truncated: true,
                next_continuation_token: Some(next_continuation_token),
                objects,
                prefixes,
                ..Default::default()
            });
        }
    }

    Err(table_list_scan_limit_error())
}

pub(crate) async fn list_object_versions(
    store: Arc<ECStore>,
    access: &TableDataPlaneListAccess,
    request: ListObjectVersionsRequest<'_>,
) -> S3Result<ListObjectVersionsInfo<ObjectInfo>> {
    let ListObjectVersionsRequest {
        bucket,
        prefix,
        key_marker,
        version_id_marker,
        delimiter,
        max_keys,
    } = request;
    if max_keys == 0 {
        return Ok(ListObjectVersionsInfo::default());
    }
    let max_keys = usize::try_from(max_keys).map_err(|_| invalid_table_list_cursor())?;
    let (mut marker, mut version_marker, mut last_visible_common_prefix) = table_list_cursor_from_markers(
        access,
        LIST_OBJECT_VERSIONS_OPERATION,
        prefix,
        delimiter,
        max_keys,
        key_marker,
        version_id_marker,
    )?;
    let common_prefix_floor = key_marker.filter(|marker| !marker.starts_with(TABLE_DATA_PLANE_LIST_CURSOR_PREFIX));
    let mut objects = Vec::new();
    let mut prefixes = Vec::new();
    let mut emitted_prefixes = HashSet::new();
    if let Some(common_prefix) = &last_visible_common_prefix {
        emitted_prefixes.insert(common_prefix.clone());
    }

    for page_index in 0..TABLE_LIST_MAX_RAW_PAGES {
        let raw = store
            .clone()
            .list_object_versions(bucket, prefix, marker.clone(), version_marker.clone(), None, TABLE_LIST_RAW_PAGE_SIZE)
            .await
            .map_err(ApiError::from)?;

        let mut marker_after_visible = marker.clone();
        let mut version_after_visible = version_marker.clone();
        let mut marker_after_scanned = marker.clone();
        let mut version_after_scanned = version_marker.clone();
        let mut found_more = false;
        for object in raw.objects {
            let object_name = object.name.clone();
            let next_marker = Some(object_name.clone());
            let next_version = Some(
                object
                    .version_id
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "null".to_string()),
            );
            marker_after_scanned.clone_from(&next_marker);
            version_after_scanned.clone_from(&next_version);
            let Some(entry) = object_list_entry(&object, prefix, delimiter) else {
                continue;
            };
            if matches!(&entry, ObjectListEntry::Prefix(prefix) if common_prefix_floor.is_some_and(|floor| prefix.as_str() <= floor))
            {
                marker_after_visible.clone_from(&next_marker);
                version_after_visible.clone_from(&next_version);
                continue;
            }
            if matches!(&entry, ObjectListEntry::Prefix(prefix) if emitted_prefixes.contains(prefix)) {
                marker_after_visible.clone_from(&next_marker);
                version_after_visible.clone_from(&next_version);
                continue;
            }
            if !access.allows_object(&object_name) {
                continue;
            }
            match entry {
                ObjectListEntry::Object if objects.len() + prefixes.len() < max_keys => {
                    objects.push(object);
                    marker_after_visible.clone_from(&next_marker);
                    version_after_visible.clone_from(&next_version);
                    last_visible_common_prefix = None;
                }
                ObjectListEntry::Prefix(prefix) if objects.len() + prefixes.len() < max_keys => {
                    emitted_prefixes.insert(prefix.clone());
                    prefixes.push(prefix);
                    marker_after_visible.clone_from(&next_marker);
                    version_after_visible.clone_from(&next_version);
                    last_visible_common_prefix = prefixes.last().cloned();
                }
                _ => {
                    found_more = true;
                    break;
                }
            }
        }

        if found_more {
            let token = access.encode_cursor(
                LIST_OBJECT_VERSIONS_OPERATION,
                prefix,
                delimiter,
                max_keys,
                TableDataPlaneListCursorPosition {
                    marker: marker_after_visible,
                    version_marker: version_after_visible,
                    common_prefix: last_visible_common_prefix,
                },
            )?;
            return Ok(ListObjectVersionsInfo {
                is_truncated: true,
                next_marker: Some(token.clone()),
                next_version_idmarker: Some(token),
                objects,
                prefixes,
            });
        }
        if !raw.is_truncated {
            return Ok(ListObjectVersionsInfo {
                objects,
                prefixes,
                ..Default::default()
            });
        }
        if marker_after_scanned == marker && version_after_scanned == version_marker {
            return Err(table_list_scan_limit_error());
        }
        marker = marker_after_scanned;
        version_marker = version_after_scanned;
        if page_index + 1 == TABLE_LIST_MAX_RAW_PAGES {
            let token = access.encode_cursor(
                LIST_OBJECT_VERSIONS_OPERATION,
                prefix,
                delimiter,
                max_keys,
                TableDataPlaneListCursorPosition {
                    marker,
                    version_marker,
                    common_prefix: last_visible_common_prefix,
                },
            )?;
            return Ok(ListObjectVersionsInfo {
                is_truncated: true,
                next_marker: Some(token.clone()),
                next_version_idmarker: Some(token),
                objects,
                prefixes,
            });
        }
    }

    Err(table_list_scan_limit_error())
}

pub(crate) async fn list_multipart_uploads(
    store: Arc<ECStore>,
    access: &TableDataPlaneListAccess,
    request: ListMultipartUploadsRequest<'_>,
) -> S3Result<ListMultipartsInfo> {
    let ListMultipartUploadsRequest {
        bucket,
        prefix,
        key_marker,
        upload_id_marker,
        delimiter,
        max_uploads,
        expected_incarnation_id,
    } = request;
    if max_uploads == 0 {
        return Ok(ListMultipartsInfo {
            key_marker: key_marker.map(str::to_string),
            upload_id_marker: upload_id_marker.map(str::to_string),
            max_uploads,
            prefix: prefix.to_string(),
            delimiter: delimiter.map(str::to_string),
            ..Default::default()
        });
    }
    let (mut marker, mut upload_marker, mut last_visible_common_prefix) = table_list_cursor_from_markers(
        access,
        &format!("{LIST_MULTIPART_UPLOADS_OPERATION}:{expected_incarnation_id}"),
        prefix,
        delimiter,
        max_uploads,
        key_marker,
        upload_id_marker,
    )?;
    let common_prefix_floor = key_marker.filter(|marker| !marker.starts_with(TABLE_DATA_PLANE_LIST_CURSOR_PREFIX));
    let mut uploads = Vec::new();
    let mut common_prefixes = Vec::new();
    let mut emitted_prefixes = HashSet::new();
    if let Some(common_prefix) = &last_visible_common_prefix {
        emitted_prefixes.insert(common_prefix.clone());
    }

    for page_index in 0..TABLE_LIST_MAX_RAW_PAGES {
        let raw = store
            .list_multipart_uploads_for_bucket_incarnation(
                bucket,
                prefix,
                marker.clone(),
                upload_marker.clone(),
                None,
                TABLE_LIST_RAW_PAGE_SIZE_USIZE,
                expected_incarnation_id,
            )
            .await
            .map_err(ApiError::from)?;

        let mut marker_after_visible = marker.clone();
        let mut upload_after_visible = upload_marker.clone();
        let mut marker_after_scanned = marker.clone();
        let mut upload_after_scanned = upload_marker.clone();
        let mut found_more = false;
        for upload in raw.uploads {
            let object_name = upload.object.clone();
            let next_marker = Some(object_name.clone());
            let next_upload = Some(upload.upload_id.clone());
            marker_after_scanned.clone_from(&next_marker);
            upload_after_scanned.clone_from(&next_upload);
            let Some(entry) = multipart_list_entry(upload, prefix, delimiter) else {
                continue;
            };
            if matches!(&entry, MultipartListEntry::Prefix(prefix) if common_prefix_floor.is_some_and(|floor| prefix.as_str() <= floor))
            {
                marker_after_visible.clone_from(&next_marker);
                upload_after_visible.clone_from(&next_upload);
                continue;
            }
            if matches!(&entry, MultipartListEntry::Prefix(prefix) if emitted_prefixes.contains(prefix)) {
                marker_after_visible.clone_from(&next_marker);
                upload_after_visible.clone_from(&next_upload);
                continue;
            }
            if !access.allows_object(&object_name) {
                continue;
            }
            match entry {
                MultipartListEntry::Upload(upload) if uploads.len() + common_prefixes.len() < max_uploads => {
                    uploads.push(upload);
                    marker_after_visible.clone_from(&next_marker);
                    upload_after_visible.clone_from(&next_upload);
                    last_visible_common_prefix = None;
                }
                MultipartListEntry::Prefix(prefix) if uploads.len() + common_prefixes.len() < max_uploads => {
                    emitted_prefixes.insert(prefix.clone());
                    common_prefixes.push(prefix);
                    marker_after_visible.clone_from(&next_marker);
                    upload_after_visible.clone_from(&next_upload);
                    last_visible_common_prefix = common_prefixes.last().cloned();
                }
                _ => {
                    found_more = true;
                    break;
                }
            }
        }

        if found_more {
            let token = access.encode_cursor(
                &format!("{LIST_MULTIPART_UPLOADS_OPERATION}:{expected_incarnation_id}"),
                prefix,
                delimiter,
                max_uploads,
                TableDataPlaneListCursorPosition {
                    marker: marker_after_visible,
                    version_marker: upload_after_visible,
                    common_prefix: last_visible_common_prefix,
                },
            )?;
            return Ok(ListMultipartsInfo {
                key_marker: key_marker.map(str::to_string),
                upload_id_marker: upload_id_marker.map(str::to_string),
                next_key_marker: Some(token.clone()),
                next_upload_id_marker: Some(token),
                max_uploads,
                is_truncated: true,
                uploads,
                prefix: prefix.to_string(),
                delimiter: delimiter.map(str::to_string),
                common_prefixes,
            });
        }
        if !raw.is_truncated {
            return Ok(ListMultipartsInfo {
                key_marker: key_marker.map(str::to_string),
                upload_id_marker: upload_id_marker.map(str::to_string),
                max_uploads,
                uploads,
                prefix: prefix.to_string(),
                delimiter: delimiter.map(str::to_string),
                common_prefixes,
                ..Default::default()
            });
        }
        if marker_after_scanned == marker && upload_after_scanned == upload_marker {
            return Err(table_list_scan_limit_error());
        }
        marker = marker_after_scanned;
        upload_marker = upload_after_scanned;
        if page_index + 1 == TABLE_LIST_MAX_RAW_PAGES {
            let token = access.encode_cursor(
                &format!("{LIST_MULTIPART_UPLOADS_OPERATION}:{expected_incarnation_id}"),
                prefix,
                delimiter,
                max_uploads,
                TableDataPlaneListCursorPosition {
                    marker,
                    version_marker: upload_marker,
                    common_prefix: last_visible_common_prefix,
                },
            )?;
            return Ok(ListMultipartsInfo {
                key_marker: key_marker.map(str::to_string),
                upload_id_marker: upload_id_marker.map(str::to_string),
                next_key_marker: Some(token.clone()),
                next_upload_id_marker: Some(token),
                max_uploads,
                is_truncated: true,
                uploads,
                prefix: prefix.to_string(),
                delimiter: delimiter.map(str::to_string),
                common_prefixes,
            });
        }
    }

    Err(table_list_scan_limit_error())
}

#[cfg(test)]
mod tests {
    use super::{MultipartListEntry, ObjectListEntry, multipart_list_entry, object_list_entry};
    use crate::app::storage_api::bucket_usecase::StorageObjectInfo as ObjectInfo;
    use crate::app::storage_api::multipart_usecase::contract::multipart::MultipartInfo;

    #[test]
    fn object_list_entry_keeps_an_object_without_a_delimiter() {
        let object = ObjectInfo {
            name: "tables/orders/data/file.parquet".to_string(),
            ..Default::default()
        };
        let entry = object_list_entry(&object, "tables/", None);

        assert!(matches!(entry, Some(ObjectListEntry::Object)));
    }

    #[test]
    fn object_list_entry_folds_only_visible_keys_into_common_prefixes() {
        let object = ObjectInfo {
            name: "tables/orders/data/file.parquet".to_string(),
            ..Default::default()
        };
        let entry = object_list_entry(&object, "tables/", Some("/"));

        assert!(matches!(entry, Some(ObjectListEntry::Prefix(prefix)) if prefix == "tables/orders/"));
    }

    #[test]
    fn multipart_list_entry_folds_only_visible_uploads_into_common_prefixes() {
        let entry = multipart_list_entry(
            MultipartInfo {
                object: "tables/orders/data/file.parquet".to_string(),
                ..Default::default()
            },
            "tables/",
            Some("/"),
        );

        assert!(matches!(entry, Some(MultipartListEntry::Prefix(prefix)) if prefix == "tables/orders/"));
    }
}
