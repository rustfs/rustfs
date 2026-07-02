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

use crate::tagging::ReplicationTagFilter;
use rustfs_filemeta::{ReplicationAction, ReplicationType};
use rustfs_utils::http::{
    AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_MODE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE, AMZ_OBJECT_TAGGING,
    AMZ_WEBSITE_REDIRECT_LOCATION, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, EXPIRES,
};
use rustfs_utils::path::trim_etag;
use rustfs_utils::string::strings_has_prefix_fold;
use std::collections::HashMap;
use time::OffsetDateTime;

const AMZ_META_PREFIX: &str = "X-Amz-Meta-";
const CONTENT_ENCODING_LOWER: &str = "content-encoding";
const REPLICATION_METADATA_COMPARE_KEYS: [&str; 9] = [
    EXPIRES,
    CACHE_CONTROL,
    CONTENT_LANGUAGE,
    CONTENT_DISPOSITION,
    AMZ_OBJECT_LOCK_MODE,
    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
    AMZ_OBJECT_LOCK_LEGAL_HOLD,
    AMZ_WEBSITE_REDIRECT_LOCATION,
    AMZ_META_PREFIX,
];

#[derive(Debug, Clone)]
pub struct ReplicationSourceObject<'a> {
    pub mod_time: Option<OffsetDateTime>,
    pub version_id: Option<String>,
    pub etag: Option<&'a str>,
    pub actual_size: i64,
    pub delete_marker: bool,
    pub content_type: Option<&'a str>,
    pub content_encoding: Option<&'a str>,
    pub user_tags: &'a str,
    pub user_defined: &'a HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct ReplicationTargetObject<'a> {
    pub last_modified: Option<OffsetDateTime>,
    pub version_id: Option<&'a str>,
    pub etag: Option<&'a str>,
    pub content_length: i64,
    pub delete_marker: bool,
    pub content_type: Option<&'a str>,
    pub metadata: Option<&'a HashMap<String, String>>,
    pub tag_count: i32,
}

pub fn content_matches_by_etag(source: &ReplicationSourceObject<'_>, target: &ReplicationTargetObject<'_>) -> bool {
    replication_etags_match(source.etag, target.etag)
}

pub fn replication_etags_match(source: Option<&str>, target: Option<&str>) -> bool {
    let source_etag = source.map(trim_etag);
    let target_etag = target.map(trim_etag);
    source_etag.is_some() && source_etag == target_etag
}

pub fn target_is_newer_than_source_null_version(
    source: &ReplicationSourceObject<'_>,
    target: &ReplicationTargetObject<'_>,
) -> bool {
    target
        .last_modified
        .is_some_and(|target_mod_time| target_mod_time > source.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH))
        && source.version_id.is_none()
}

pub fn replication_action_for_target(
    source: &ReplicationSourceObject<'_>,
    target: &ReplicationTargetObject<'_>,
    op_type: ReplicationType,
) -> ReplicationAction {
    if op_type == ReplicationType::ExistingObject && target_is_newer_than_source_null_version(source, target) {
        return ReplicationAction::None;
    }

    if source.etag.map(trim_etag) != target.etag.map(trim_etag)
        || source.version_id.as_deref() != target.version_id
        || source.actual_size != target.content_length
        || source.delete_marker != target.delete_marker
        || source.mod_time != target.last_modified
    {
        return ReplicationAction::All;
    }

    if source.content_type != target.content_type {
        return ReplicationAction::Metadata;
    }

    if content_encoding_differs(source, target) {
        return ReplicationAction::Metadata;
    }

    if tag_metadata_differs(source, target) {
        return ReplicationAction::Metadata;
    }

    if comparable_metadata(Some(source.user_defined)) != comparable_metadata(target.metadata) {
        return ReplicationAction::Metadata;
    }

    ReplicationAction::None
}

fn content_encoding_differs(source: &ReplicationSourceObject<'_>, target: &ReplicationTargetObject<'_>) -> bool {
    if let Some(content_encoding) = source.content_encoding {
        return target
            .metadata
            .and_then(|metadata| {
                metadata
                    .get(CONTENT_ENCODING)
                    .or_else(|| metadata.get(CONTENT_ENCODING_LOWER))
            })
            .is_none_or(|enc| enc != content_encoding);
    }
    false
}

fn tag_metadata_differs(source: &ReplicationSourceObject<'_>, target: &ReplicationTargetObject<'_>) -> bool {
    let source_tags = ReplicationTagFilter::decode_tags_to_map(source.user_tags);
    let target_tagging = target
        .metadata
        .and_then(|metadata| metadata.get(AMZ_OBJECT_TAGGING).map(String::as_str))
        .unwrap_or_default();
    let target_tags = ReplicationTagFilter::decode_tags_to_map(target_tagging);
    let source_tag_count = match i32::try_from(source_tags.len()) {
        Ok(count) => count,
        Err(_) => i32::MAX,
    };

    (target.tag_count > 0 && source_tags != target_tags) || target.tag_count != source_tag_count
}

fn comparable_metadata(metadata: Option<&HashMap<String, String>>) -> HashMap<String, String> {
    let mut comparable = HashMap::new();
    for (key, value) in metadata.into_iter().flatten() {
        if REPLICATION_METADATA_COMPARE_KEYS
            .iter()
            .any(|prefix| strings_has_prefix_fold(key, prefix))
        {
            comparable.insert(key.to_lowercase(), value.clone());
        }
    }
    comparable
}

#[cfg(test)]
mod tests {
    use super::{
        ReplicationSourceObject, ReplicationTargetObject, content_matches_by_etag, replication_action_for_target,
        replication_etags_match, target_is_newer_than_source_null_version,
    };
    use rustfs_filemeta::{ReplicationAction, ReplicationType};
    use std::collections::HashMap;
    use time::{Duration, OffsetDateTime};

    fn source_object(user_defined: &HashMap<String, String>) -> ReplicationSourceObject<'_> {
        ReplicationSourceObject {
            mod_time: Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(10)),
            version_id: Some("source-version".to_string()),
            etag: Some("\"abc\""),
            actual_size: 10,
            delete_marker: false,
            content_type: Some("text/plain"),
            content_encoding: None,
            user_tags: "a=1",
            user_defined,
        }
    }

    fn target_object(metadata: &HashMap<String, String>) -> ReplicationTargetObject<'_> {
        ReplicationTargetObject {
            last_modified: Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(10)),
            version_id: Some("source-version"),
            etag: Some("abc"),
            content_length: 10,
            delete_marker: false,
            content_type: Some("text/plain"),
            metadata: Some(metadata),
            tag_count: 1,
        }
    }

    #[test]
    fn content_matches_by_etag_ignores_version_ids() {
        let source_metadata = HashMap::new();
        let target_metadata = HashMap::new();
        let source = ReplicationSourceObject {
            version_id: Some("source-version".to_string()),
            etag: Some("\"abc\""),
            user_defined: &source_metadata,
            ..source_object(&source_metadata)
        };
        let target = ReplicationTargetObject {
            version_id: Some("different-version"),
            etag: Some("abc"),
            metadata: Some(&target_metadata),
            ..target_object(&target_metadata)
        };

        assert!(content_matches_by_etag(&source, &target));
        assert!(replication_etags_match(source.etag, target.etag));
    }

    #[test]
    fn target_newer_null_version_skips_existing_object_replication() {
        let source_metadata = HashMap::new();
        let target_metadata = HashMap::new();
        let source = ReplicationSourceObject {
            version_id: None,
            ..source_object(&source_metadata)
        };
        let target = ReplicationTargetObject {
            last_modified: Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(20)),
            ..target_object(&target_metadata)
        };

        assert!(target_is_newer_than_source_null_version(&source, &target));
        assert_eq!(
            replication_action_for_target(&source, &target, ReplicationType::ExistingObject),
            ReplicationAction::None
        );
    }

    #[test]
    fn replication_action_detects_content_and_metadata_differences() {
        let mut source_metadata = HashMap::new();
        source_metadata.insert("Cache-Control".to_string(), "max-age=1".to_string());
        let mut target_metadata = HashMap::new();
        target_metadata.insert("X-Amz-Tagging".to_string(), "a=1".to_string());
        target_metadata.insert("Cache-Control".to_string(), "max-age=1".to_string());

        let source = source_object(&source_metadata);
        let target = target_object(&target_metadata);
        assert_eq!(
            replication_action_for_target(&source, &target, ReplicationType::ExistingObject),
            ReplicationAction::None
        );

        let changed_content = ReplicationTargetObject {
            content_length: 11,
            ..target_object(&target_metadata)
        };
        assert_eq!(
            replication_action_for_target(&source, &changed_content, ReplicationType::ExistingObject),
            ReplicationAction::All
        );

        let mut changed_target_metadata = target_metadata.clone();
        changed_target_metadata.insert("Cache-Control".to_string(), "max-age=2".to_string());
        let changed_metadata = target_object(&changed_target_metadata);
        assert_eq!(
            replication_action_for_target(&source, &changed_metadata, ReplicationType::ExistingObject),
            ReplicationAction::Metadata
        );
    }
}
