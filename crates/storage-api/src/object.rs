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

use std::fmt;
use std::sync::Arc;

use crate::replication::{
    ReplicationState, ReplicationStatusType, VersionPurgeStatusType, replication_statuses_map, version_purge_statuses_map,
};
use time::OffsetDateTime;
use uuid::Uuid;

const NULL_VERSION_MARKER: &str = "null";

#[derive(Debug, Default, Clone)]
pub struct HTTPPreconditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<OffsetDateTime>,
    pub if_unmodified_since: Option<OffsetDateTime>,
}

impl HTTPPreconditions {
    pub fn if_match_value(&self) -> Option<&str> {
        non_empty_condition_value(self.if_match.as_deref())
    }

    pub fn if_none_match_value(&self) -> Option<&str> {
        non_empty_condition_value(self.if_none_match.as_deref())
    }
}

#[derive(Debug, Default, Clone)]
pub struct ObjectLockRetentionOptions {
    pub mode: Option<String>,
    pub retain_until: Option<OffsetDateTime>,
    pub bypass_governance: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ExpirationOptions {
    pub expire: bool,
}

#[derive(Debug, Default, Clone)]
pub struct TransitionedObject {
    pub name: String,
    pub version_id: String,
    pub tier: String,
    pub free_version: bool,
    pub status: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObjectPreconditionPart {
    pub number: usize,
    pub exists: bool,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct ObjectPreconditionState<'a> {
    pub etag: Option<&'a str>,
    pub mod_time: Option<OffsetDateTime>,
    pub requested_part: Option<ObjectPreconditionPart>,
}

impl ObjectPreconditionState<'_> {
    pub fn check(self, preconditions: Option<&HTTPPreconditions>) -> Result<(), ObjectPreconditionError> {
        if let Some(part) = self.requested_part
            && part.number > 1
            && !part.exists
        {
            return Err(ObjectPreconditionError::InvalidPartNumber(part.number));
        }

        let Some(preconditions) = preconditions else {
            return Ok(());
        };

        let has_valid_mod_time = self.mod_time.is_some_and(|t| t != OffsetDateTime::UNIX_EPOCH);
        let if_none_match = preconditions.if_none_match_value();
        let if_match = preconditions.if_match_value();

        if let Some(if_none_match) = if_none_match
            && let Some(etag) = self.etag
            && etag_matches(etag, if_none_match)
        {
            return Err(ObjectPreconditionError::NotModified);
        }

        if has_valid_mod_time
            && let Some(if_modified_since) = &preconditions.if_modified_since
            && let Some(mod_time) = &self.mod_time
            && !is_modified_since(mod_time, if_modified_since)
        {
            return Err(ObjectPreconditionError::NotModified);
        }

        if let Some(if_match) = if_match {
            if let Some(etag) = self.etag {
                if !etag_matches(etag, if_match) {
                    return Err(ObjectPreconditionError::PreconditionFailed);
                }
            } else {
                return Err(ObjectPreconditionError::PreconditionFailed);
            }
        }

        if has_valid_mod_time
            && if_match.is_none()
            && let Some(if_unmodified_since) = &preconditions.if_unmodified_since
            && let Some(mod_time) = &self.mod_time
            && is_modified_since(mod_time, if_unmodified_since)
        {
            return Err(ObjectPreconditionError::PreconditionFailed);
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObjectPreconditionError {
    InvalidPartNumber(usize),
    NotModified,
    PreconditionFailed,
}

impl fmt::Display for ObjectPreconditionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPartNumber(part_number) => write!(f, "invalid part number {part_number}"),
            Self::NotModified => f.write_str("object not modified"),
            Self::PreconditionFailed => f.write_str("object precondition failed"),
        }
    }
}

impl std::error::Error for ObjectPreconditionError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionMarker {
    Null,
    Version(Uuid),
}

impl VersionMarker {
    pub fn parse(marker: impl AsRef<str>) -> Result<Self, uuid::Error> {
        let marker = marker.as_ref();
        if marker == NULL_VERSION_MARKER {
            Ok(Self::Null)
        } else {
            Ok(Self::Version(Uuid::parse_str(marker)?))
        }
    }
}

#[derive(Clone, Default, PartialEq, Eq)]
pub enum WalkVersionsSortOrder {
    #[default]
    Ascending,
    Descending,
}

#[derive(Debug, Default, Clone)]
pub struct ObjectToDelete {
    pub object_name: String,
    pub version_id: Option<Uuid>,
    pub delete_marker_replication_status: Option<String>,
    pub version_purge_status: Option<VersionPurgeStatusType>,
    pub version_purge_statuses: Option<String>,
    pub replicate_decision_str: Option<String>,
}

impl ObjectToDelete {
    pub fn replication_state(&self) -> ReplicationState {
        ReplicationState {
            replication_status_internal: self.delete_marker_replication_status.clone(),
            version_purge_status_internal: self.version_purge_statuses.clone(),
            replicate_decision_str: self.replicate_decision_str.clone().unwrap_or_default(),
            targets: replication_statuses_map(self.delete_marker_replication_status.as_deref().unwrap_or_default()),
            purge_targets: version_purge_statuses_map(self.version_purge_statuses.as_deref().unwrap_or_default()),
            ..Default::default()
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct DeletedObject {
    pub delete_marker: bool,
    pub delete_marker_version_id: Option<Uuid>,
    pub object_name: String,
    pub version_id: Option<Uuid>,
    pub delete_marker_mtime: Option<OffsetDateTime>,
    pub replication_state: Option<ReplicationState>,
    pub found: bool,
    pub force_delete: bool,
}

impl DeletedObject {
    pub fn version_purge_status(&self) -> VersionPurgeStatusType {
        self.replication_state
            .as_ref()
            .map(|v| v.composite_version_purge_status())
            .unwrap_or(VersionPurgeStatusType::Empty)
    }

    pub fn delete_marker_replication_status(&self) -> ReplicationStatusType {
        self.replication_state
            .as_ref()
            .map(|v| v.composite_replication_status())
            .unwrap_or(ReplicationStatusType::Empty)
    }
}

#[derive(Clone)]
pub struct WalkOptions<Filter> {
    pub filter: Option<Filter>,
    pub marker: Option<String>,
    pub latest_only: bool,
    pub ask_disks: String,
    pub versions_sort: WalkVersionsSortOrder,
    pub limit: usize,
    pub include_free_versions: bool,
}

impl<Filter> Default for WalkOptions<Filter> {
    fn default() -> Self {
        Self {
            filter: None,
            marker: None,
            latest_only: false,
            ask_disks: String::new(),
            versions_sort: WalkVersionsSortOrder::default(),
            limit: 0,
            include_free_versions: false,
        }
    }
}

#[async_trait::async_trait]
pub trait ObjectIO: Send + Sync + fmt::Debug + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type RangeSpec: Send + 'static;
    type HeaderMap: Send + 'static;
    type ObjectOptions: Send + Sync + 'static;
    type ObjectInfo: Send + 'static;
    type GetObjectReader: Send + 'static;
    type PutObjectReader: Send + 'static;

    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<Self::RangeSpec>,
        h: Self::HeaderMap,
        opts: &Self::ObjectOptions,
    ) -> Result<Self::GetObjectReader, Self::Error>;

    async fn put_object(
        &self,
        bucket: &str,
        object: &str,
        data: &mut Self::PutObjectReader,
        opts: &Self::ObjectOptions,
    ) -> Result<Self::ObjectInfo, Self::Error>;
}

#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
pub trait ObjectOperations: Send + Sync + fmt::Debug {
    type Error: std::error::Error + Send + Sync + 'static;
    type ObjectInfo: Send + 'static;
    type ObjectOptions: Send + Sync + 'static;
    type FileInfo: Send + Sync + 'static;
    type ObjectToDelete: Send + 'static;
    type DeletedObject: Send + 'static;

    async fn get_object_info(
        &self,
        bucket: &str,
        object: &str,
        opts: &Self::ObjectOptions,
    ) -> Result<Self::ObjectInfo, Self::Error>;
    async fn verify_object_integrity(&self, bucket: &str, object: &str, opts: &Self::ObjectOptions) -> Result<(), Self::Error>;
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        src_info: &mut Self::ObjectInfo,
        src_opts: &Self::ObjectOptions,
        dst_opts: &Self::ObjectOptions,
    ) -> Result<Self::ObjectInfo, Self::Error>;
    async fn delete_object_version(
        &self,
        bucket: &str,
        object: &str,
        fi: &Self::FileInfo,
        force_del_marker: bool,
    ) -> Result<(), Self::Error>;
    async fn delete_object(&self, bucket: &str, object: &str, opts: Self::ObjectOptions)
    -> Result<Self::ObjectInfo, Self::Error>;
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<Self::ObjectToDelete>,
        opts: Self::ObjectOptions,
    ) -> (Vec<Self::DeletedObject>, Vec<Option<Self::Error>>);
    async fn put_object_metadata(
        &self,
        bucket: &str,
        object: &str,
        opts: &Self::ObjectOptions,
    ) -> Result<Self::ObjectInfo, Self::Error>;
    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &Self::ObjectOptions) -> Result<String, Self::Error>;
    async fn put_object_tags(
        &self,
        bucket: &str,
        object: &str,
        tags: &str,
        opts: &Self::ObjectOptions,
    ) -> Result<Self::ObjectInfo, Self::Error>;
    async fn delete_object_tags(
        &self,
        bucket: &str,
        object: &str,
        opts: &Self::ObjectOptions,
    ) -> Result<Self::ObjectInfo, Self::Error>;
    async fn add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<(), Self::Error>;
    async fn transition_object(&self, bucket: &str, object: &str, opts: &Self::ObjectOptions) -> Result<(), Self::Error>;
    async fn restore_transitioned_object(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        opts: &Self::ObjectOptions,
    ) -> Result<(), Self::Error>;
}

#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
pub trait ListOperations: Send + Sync + fmt::Debug {
    type Error: std::error::Error + Send + Sync + 'static;
    type ListObjectsV2Info: Send + 'static;
    type ListObjectVersionsInfo: Send + 'static;
    type ObjectInfoOrErr: Send + 'static;
    type WalkOptions: Send + 'static;
    type WalkCancellation: Send + 'static;
    type WalkResultSender: Send + 'static;

    async fn list_objects_v2(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        continuation_token: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
        fetch_owner: bool,
        start_after: Option<String>,
        incl_deleted: bool,
    ) -> Result<Self::ListObjectsV2Info, Self::Error>;

    async fn list_object_versions(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<Self::ListObjectVersionsInfo, Self::Error>;

    async fn walk(
        self: Arc<Self>,
        rx: Self::WalkCancellation,
        bucket: &str,
        prefix: &str,
        result: Self::WalkResultSender,
        opts: Self::WalkOptions,
    ) -> Result<(), Self::Error>;
}

#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
pub trait MultipartOperations: Send + Sync + fmt::Debug {
    type Error: std::error::Error + Send + Sync + 'static;
    type ObjectInfo: Send + 'static;
    type ObjectOptions: Send + Sync + 'static;
    type PutObjectReader: Send + 'static;
    type CompletePart: Send + 'static;
    type ListMultipartsInfo: Send + 'static;
    type MultipartUploadResult: Send + 'static;
    type PartInfo: Send + 'static;
    type MultipartInfo: Send + 'static;
    type ListPartsInfo: Send + 'static;

    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
        max_uploads: usize,
    ) -> Result<Self::ListMultipartsInfo, Self::Error>;
    async fn new_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        opts: &Self::ObjectOptions,
    ) -> Result<Self::MultipartUploadResult, Self::Error>;
    async fn copy_object_part(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        upload_id: &str,
        part_id: usize,
        start_offset: i64,
        length: i64,
        src_info: &Self::ObjectInfo,
        src_opts: &Self::ObjectOptions,
        dst_opts: &Self::ObjectOptions,
    ) -> Result<(), Self::Error>;
    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: &mut Self::PutObjectReader,
        opts: &Self::ObjectOptions,
    ) -> Result<Self::PartInfo, Self::Error>;
    async fn get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &Self::ObjectOptions,
    ) -> Result<Self::MultipartInfo, Self::Error>;
    async fn list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: Option<usize>,
        max_parts: usize,
        opts: &Self::ObjectOptions,
    ) -> Result<Self::ListPartsInfo, Self::Error>;
    async fn abort_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &Self::ObjectOptions,
    ) -> Result<(), Self::Error>;
    async fn complete_multipart_upload(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<Self::CompletePart>,
        opts: &Self::ObjectOptions,
    ) -> Result<Self::ObjectInfo, Self::Error>;
}

#[async_trait::async_trait]
pub trait HealOperations: Send + Sync + fmt::Debug {
    type Error: std::error::Error + Send + Sync + 'static;
    type HealResultItem: Send + 'static;
    type HealOptions: Send + Sync + 'static;

    async fn heal_format(&self, dry_run: bool) -> Result<(Self::HealResultItem, Option<Self::Error>), Self::Error>;
    async fn heal_bucket(&self, bucket: &str, opts: &Self::HealOptions) -> Result<Self::HealResultItem, Self::Error>;
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &Self::HealOptions,
    ) -> Result<(Self::HealResultItem, Option<Self::Error>), Self::Error>;
    async fn get_pool_and_set(&self, id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>), Self::Error>;
    async fn check_abandoned_parts(&self, bucket: &str, object: &str, opts: &Self::HealOptions) -> Result<(), Self::Error>;
}

#[async_trait::async_trait]
pub trait NamespaceLocking: Send + Sync + fmt::Debug + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    type NamespaceLock: Send + 'static;

    async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<Self::NamespaceLock, Self::Error>;
}

#[derive(Debug, Default)]
pub struct ListObjectsInfo<ObjectItem> {
    pub is_truncated: bool,
    pub next_marker: Option<String>,
    pub objects: Vec<ObjectItem>,
    pub prefixes: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ListObjectsV2Info<ObjectItem> {
    pub is_truncated: bool,
    pub continuation_token: Option<String>,
    pub next_continuation_token: Option<String>,
    pub objects: Vec<ObjectItem>,
    pub prefixes: Vec<String>,
}

#[derive(Debug, Default, Clone)]
pub struct ListObjectVersionsInfo<ObjectItem> {
    pub is_truncated: bool,
    pub next_marker: Option<String>,
    pub next_version_idmarker: Option<String>,
    pub objects: Vec<ObjectItem>,
    pub prefixes: Vec<String>,
}

#[derive(Debug)]
pub struct ObjectInfoOrErr<ObjectItem, ListError> {
    pub item: Option<ObjectItem>,
    pub err: Option<ListError>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HTTPRangeError {
    InvalidRangeSpec(String),
}

impl fmt::Display for HTTPRangeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRangeSpec(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for HTTPRangeError {}

#[derive(Debug, Clone)]
pub struct HTTPRangeSpec {
    pub is_suffix_length: bool,
    pub start: i64,
    pub end: i64,
}

impl HTTPRangeSpec {
    pub fn from_part_sizes(object_size: i64, part_number: usize, part_sizes: impl IntoIterator<Item = i64>) -> Option<Self> {
        if object_size == 0 || part_number == 0 {
            return None;
        }

        let mut start = 0_i64;
        let mut end = -1_i64;
        let mut parts = part_sizes.into_iter();
        for _ in 0..part_number {
            let part_size = parts.next()?;
            start = end.checked_add(1)?;
            end = start.checked_add(part_size)?.checked_sub(1)?;
        }

        Some(Self {
            is_suffix_length: false,
            start,
            end,
        })
    }

    pub fn get_offset_length(&self, res_size: i64) -> Result<(usize, i64), HTTPRangeError> {
        let len = self.get_length(res_size)?;

        let mut start = self.start;
        if self.is_suffix_length {
            let suffix_len = if self.start < 0 {
                self.start
                    .checked_neg()
                    .ok_or_else(|| HTTPRangeError::InvalidRangeSpec("range value invalid: suffix length overflow".to_string()))?
            } else {
                self.start
            };
            start = res_size - suffix_len;
            if start < 0 {
                start = 0;
            }
        }
        let offset = usize::try_from(start)
            .map_err(|_| HTTPRangeError::InvalidRangeSpec("range value invalid: start offset overflow".to_string()))?;
        Ok((offset, len))
    }

    pub fn get_length(&self, res_size: i64) -> Result<i64, HTTPRangeError> {
        if res_size < 0 {
            return Err(HTTPRangeError::InvalidRangeSpec("The requested range is not satisfiable".to_string()));
        }

        if self.is_suffix_length {
            let specified_len = if self.start < 0 {
                self.start
                    .checked_neg()
                    .ok_or_else(|| HTTPRangeError::InvalidRangeSpec("range value invalid: suffix length overflow".to_string()))?
            } else {
                self.start
            };
            let mut range_length = specified_len;

            if specified_len > res_size {
                range_length = res_size;
            }

            return Ok(range_length);
        }

        if self.start >= res_size {
            return Err(HTTPRangeError::InvalidRangeSpec("The requested range is not satisfiable".to_string()));
        }

        if self.end > -1 {
            let mut end = self.end;
            if res_size <= end {
                end = res_size - 1;
            }

            let range_length = end - self.start + 1;
            return Ok(range_length);
        }

        if self.end == -1 {
            let range_length = res_size - self.start;
            return Ok(range_length);
        }

        Err(HTTPRangeError::InvalidRangeSpec(format!(
            "range value invalid: start={}, end={}, expected start <= end and end >= -1",
            self.start, self.end
        )))
    }
}

fn non_empty_condition_value(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn etag_matches(object_etag: &str, condition_etag: &str) -> bool {
    let object_etag = object_etag.trim_matches('"');
    let condition_etag = condition_etag.trim_matches('"');
    condition_etag == "*" || object_etag == condition_etag
}

fn is_modified_since(mod_time: &OffsetDateTime, given_time: &OffsetDateTime) -> bool {
    mod_time.unix_timestamp() > given_time.unix_timestamp()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn http_preconditions_ignore_empty_etag_headers() {
        let opts = HTTPPreconditions {
            if_match: Some("  ".to_owned()),
            if_none_match: Some(" * ".to_owned()),
            ..Default::default()
        };

        assert_eq!(opts.if_match_value(), None);
        assert_eq!(opts.if_none_match_value(), Some("*"));
    }

    #[test]
    fn object_lock_retention_defaults_preserve_false_bypass() {
        let opts = ObjectLockRetentionOptions::default();

        assert!(opts.mode.is_none());
        assert!(opts.retain_until.is_none());
        assert!(!opts.bypass_governance);
    }

    #[test]
    fn object_precondition_state_rejects_missing_requested_part() {
        let state = ObjectPreconditionState {
            requested_part: Some(ObjectPreconditionPart {
                number: 3,
                exists: false,
            }),
            ..Default::default()
        };

        assert_eq!(state.check(None), Err(ObjectPreconditionError::InvalidPartNumber(3)));
    }

    #[test]
    fn object_precondition_state_keeps_existing_etag_priority() {
        let preconditions = HTTPPreconditions {
            if_match: Some("\"other\"".to_owned()),
            if_none_match: Some("\"abc\"".to_owned()),
            ..Default::default()
        };
        let state = ObjectPreconditionState {
            etag: Some("\"abc\""),
            ..Default::default()
        };

        assert_eq!(state.check(Some(&preconditions)), Err(ObjectPreconditionError::NotModified));
    }

    #[test]
    fn object_precondition_state_requires_etag_for_if_match() {
        let preconditions = HTTPPreconditions {
            if_match: Some("\"abc\"".to_owned()),
            ..Default::default()
        };

        assert_eq!(
            ObjectPreconditionState::default().check(Some(&preconditions)),
            Err(ObjectPreconditionError::PreconditionFailed)
        );
    }

    #[test]
    fn object_precondition_state_checks_modification_dates() {
        let mod_time = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(100);
        let preconditions = HTTPPreconditions {
            if_modified_since: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(100)),
            ..Default::default()
        };
        let state = ObjectPreconditionState {
            mod_time: Some(mod_time),
            ..Default::default()
        };

        assert_eq!(state.check(Some(&preconditions)), Err(ObjectPreconditionError::NotModified));

        let preconditions = HTTPPreconditions {
            if_unmodified_since: Some(OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(99)),
            ..Default::default()
        };

        assert_eq!(state.check(Some(&preconditions)), Err(ObjectPreconditionError::PreconditionFailed));
    }

    #[test]
    fn version_marker_parses_null_and_uuid_markers() {
        assert_eq!(VersionMarker::parse("null").expect("null marker should parse"), VersionMarker::Null);

        let marker = "550e8400-e29b-41d4-a716-446655440000";
        assert_eq!(
            VersionMarker::parse(marker).expect("uuid marker should parse"),
            VersionMarker::Version(Uuid::parse_str(marker).expect("test uuid should parse"))
        );
    }

    #[test]
    fn walk_versions_sort_order_defaults_to_ascending() {
        assert!(matches!(WalkVersionsSortOrder::default(), WalkVersionsSortOrder::Ascending));
    }

    #[test]
    fn walk_options_defaults_preserve_existing_contract() {
        type TestFilter = fn(&str) -> bool;

        let opts = WalkOptions::<TestFilter>::default();

        assert!(opts.filter.is_none());
        assert!(opts.marker.is_none());
        assert!(!opts.latest_only);
        assert!(opts.ask_disks.is_empty());
        assert!(matches!(opts.versions_sort, WalkVersionsSortOrder::Ascending));
        assert_eq!(opts.limit, 0);
        assert!(!opts.include_free_versions);
    }

    #[test]
    fn object_to_delete_builds_replication_state() {
        let object = ObjectToDelete {
            object_name: "photo.jpg".to_owned(),
            delete_marker_replication_status: Some("PENDING".to_owned()),
            version_purge_statuses: Some("arn:minio:replication:::target=PENDING".to_owned()),
            replicate_decision_str: Some("arn:minio:replication:::target=PENDING".to_owned()),
            ..Default::default()
        };

        let state = object.replication_state();

        assert_eq!(state.replication_status_internal.as_deref(), Some("PENDING"));
        assert_eq!(
            state.version_purge_status_internal.as_deref(),
            Some("arn:minio:replication:::target=PENDING")
        );
        assert_eq!(state.replicate_decision_str, "arn:minio:replication:::target=PENDING");
    }

    #[test]
    fn deleted_object_status_helpers_default_to_empty() {
        let object = DeletedObject::default();

        assert_eq!(object.version_purge_status(), VersionPurgeStatusType::Empty);
        assert_eq!(object.delete_marker_replication_status(), ReplicationStatusType::Empty);
    }

    #[test]
    fn lifecycle_helper_defaults_preserve_existing_contracts() {
        let expiration = ExpirationOptions::default();
        let transitioned = TransitionedObject::default();

        assert!(!expiration.expire);
        assert!(transitioned.name.is_empty());
        assert!(transitioned.version_id.is_empty());
        assert!(transitioned.tier.is_empty());
        assert!(!transitioned.free_version);
        assert!(transitioned.status.is_empty());
    }

    #[derive(Debug)]
    struct TestListBackend;

    #[derive(Debug)]
    struct TestListError;

    impl fmt::Display for TestListError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("test list error")
        }
    }

    impl std::error::Error for TestListError {}

    #[async_trait::async_trait]
    impl ListOperations for TestListBackend {
        type Error = TestListError;
        type ListObjectsV2Info = ListObjectsV2Info<&'static str>;
        type ListObjectVersionsInfo = ListObjectVersionsInfo<&'static str>;
        type ObjectInfoOrErr = ObjectInfoOrErr<&'static str, TestListError>;
        type WalkOptions = WalkOptions<fn(&str) -> bool>;
        type WalkCancellation = ();
        type WalkResultSender = ();

        async fn list_objects_v2(
            self: Arc<Self>,
            bucket: &str,
            prefix: &str,
            continuation_token: Option<String>,
            delimiter: Option<String>,
            max_keys: i32,
            fetch_owner: bool,
            start_after: Option<String>,
            incl_deleted: bool,
        ) -> Result<Self::ListObjectsV2Info, Self::Error> {
            assert_eq!(bucket, "bucket");
            assert_eq!(prefix, "photos/");
            assert_eq!(continuation_token.as_deref(), Some("token"));
            assert_eq!(delimiter.as_deref(), Some("/"));
            assert_eq!(max_keys, 10);
            assert!(fetch_owner);
            assert_eq!(start_after.as_deref(), Some("photos/0001.jpg"));
            assert!(!incl_deleted);

            Ok(ListObjectsV2Info {
                is_truncated: true,
                continuation_token,
                next_continuation_token: Some("next".to_owned()),
                objects: vec!["photos/0002.jpg"],
                prefixes: vec!["photos/2024/".to_owned()],
            })
        }

        async fn list_object_versions(
            self: Arc<Self>,
            bucket: &str,
            prefix: &str,
            marker: Option<String>,
            version_marker: Option<String>,
            delimiter: Option<String>,
            max_keys: i32,
        ) -> Result<Self::ListObjectVersionsInfo, Self::Error> {
            assert_eq!(bucket, "bucket");
            assert_eq!(prefix, "photos/");
            assert_eq!(marker.as_deref(), Some("photos/0001.jpg"));
            assert_eq!(version_marker.as_deref(), Some("version"));
            assert_eq!(delimiter.as_deref(), Some("/"));
            assert_eq!(max_keys, 10);

            Ok(ListObjectVersionsInfo {
                is_truncated: false,
                next_marker: None,
                next_version_idmarker: None,
                objects: vec!["photos/0001.jpg"],
                prefixes: Vec::new(),
            })
        }

        async fn walk(
            self: Arc<Self>,
            _rx: Self::WalkCancellation,
            bucket: &str,
            prefix: &str,
            _result: Self::WalkResultSender,
            opts: Self::WalkOptions,
        ) -> Result<(), Self::Error> {
            assert_eq!(bucket, "bucket");
            assert_eq!(prefix, "photos/");
            assert_eq!(opts.marker.as_deref(), Some("photos/0001.jpg"));
            Ok(())
        }
    }

    #[tokio::test]
    async fn list_operations_trait_exposes_generic_list_contract() -> Result<(), TestListError> {
        let backend = Arc::new(TestListBackend);

        let listed = backend
            .clone()
            .list_objects_v2(
                "bucket",
                "photos/",
                Some("token".to_owned()),
                Some("/".to_owned()),
                10,
                true,
                Some("photos/0001.jpg".to_owned()),
                false,
            )
            .await?;
        assert!(listed.is_truncated);
        assert_eq!(listed.objects, vec!["photos/0002.jpg"]);
        assert_eq!(listed.next_continuation_token.as_deref(), Some("next"));

        let versions = backend
            .clone()
            .list_object_versions(
                "bucket",
                "photos/",
                Some("photos/0001.jpg".to_owned()),
                Some("version".to_owned()),
                Some("/".to_owned()),
                10,
            )
            .await?;
        assert_eq!(versions.objects, vec!["photos/0001.jpg"]);

        let opts = WalkOptions::<fn(&str) -> bool> {
            marker: Some("photos/0001.jpg".to_owned()),
            ..Default::default()
        };
        backend.walk((), "bucket", "photos/", (), opts).await?;

        Ok(())
    }

    #[test]
    fn object_list_response_contracts_default_to_empty_collections() {
        let v1 = ListObjectsInfo::<()>::default();
        assert!(!v1.is_truncated);
        assert!(v1.next_marker.is_none());
        assert!(v1.objects.is_empty());
        assert!(v1.prefixes.is_empty());

        let v2 = ListObjectsV2Info::<()>::default();
        assert!(!v2.is_truncated);
        assert!(v2.continuation_token.is_none());
        assert!(v2.next_continuation_token.is_none());
        assert!(v2.objects.is_empty());
        assert!(v2.prefixes.is_empty());

        let versions = ListObjectVersionsInfo::<()>::default();
        assert!(!versions.is_truncated);
        assert!(versions.next_marker.is_none());
        assert!(versions.next_version_idmarker.is_none());
        assert!(versions.objects.is_empty());
        assert!(versions.prefixes.is_empty());
    }

    #[test]
    fn object_info_or_err_keeps_optional_item_and_error_slots() {
        let item = ObjectInfoOrErr::<usize, &str> {
            item: Some(7),
            err: None,
        };

        assert_eq!(item.item, Some(7));
        assert!(item.err.is_none());
    }

    #[test]
    fn http_range_spec_offset_length_handles_suffix_and_bounds() {
        let range = HTTPRangeSpec {
            is_suffix_length: false,
            start: 5,
            end: 10,
        };

        assert_eq!(range.get_offset_length(20).expect("range should fit"), (5, 6));

        let suffix = HTTPRangeSpec {
            is_suffix_length: true,
            start: -5,
            end: -1,
        };

        assert_eq!(suffix.get_offset_length(20).expect("suffix range should fit"), (15, 5));
    }

    #[test]
    fn http_range_spec_from_part_sizes_keeps_part_boundaries() {
        let spec = HTTPRangeSpec::from_part_sizes(100, 3, [10, 15, 20]).expect("third part should exist");

        assert_eq!(spec.start, 25);
        assert_eq!(spec.end, 44);
        assert!(HTTPRangeSpec::from_part_sizes(100, 0, [10]).is_none());
        assert!(HTTPRangeSpec::from_part_sizes(100, 2, [10]).is_none());
    }
}
