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

use std::collections::HashMap;
use std::sync::Arc;

use crate::bucket::bucket_target_sys::{BucketTargetError, BucketTargetSys};
use aws_sdk_s3::types::{ObjectLockLegalHoldStatus, ObjectLockRetentionMode};
use http::HeaderMap;
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_MODE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
    AMZ_OBJECT_TAGGING, AMZ_SERVER_SIDE_ENCRYPTION, AMZ_STORAGE_CLASS, AMZ_TAG_COUNT, CACHE_CONTROL, CONTENT_DISPOSITION,
    CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_TYPE, HeaderExt as _, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP,
    SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE, SUFFIX_REPLICATION_SSEC_CRC,
    SUFFIX_TAGGING_TIMESTAMP, get_str, insert_header_map, is_internal_key,
};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub(crate) use crate::bucket::bucket_target_sys::{
    AdvancedPutOptions, PutObjectOptions, PutObjectPartOptions, RemoveObjectOptions, TargetClient,
};
pub(crate) use crate::bucket::target::BucketTargets;

use super::replication_config_store::ReplicationConfigStore;
use super::replication_error_boundary::{Error, Result};
use super::replication_filemeta_boundary::ReplicationStatusType;
use super::replication_storage_boundary::ObjectInfo;
use super::replication_tagging_boundary::ReplicationTagFilter;

static STANDARD_HEADERS: &[&str] = &[
    CONTENT_TYPE,
    CACHE_CONTROL,
    CONTENT_ENCODING,
    CONTENT_LANGUAGE,
    CONTENT_DISPOSITION,
    AMZ_STORAGE_CLASS,
    AMZ_OBJECT_TAGGING,
    AMZ_BUCKET_REPLICATION_STATUS,
    AMZ_OBJECT_LOCK_MODE,
    AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
    AMZ_OBJECT_LOCK_LEGAL_HOLD,
    AMZ_TAG_COUNT,
    AMZ_SERVER_SIDE_ENCRYPTION,
];

static VALID_SSE_REPLICATION_HEADERS: &[(&str, &str)] = &[
    (
        "X-Rustfs-Internal-Server-Side-Encryption-Sealed-Key",
        "X-Rustfs-Replication-Server-Side-Encryption-Sealed-Key",
    ),
    (
        "X-Rustfs-Internal-Server-Side-Encryption-Seal-Algorithm",
        "X-Rustfs-Replication-Server-Side-Encryption-Seal-Algorithm",
    ),
    (
        "X-Rustfs-Internal-Server-Side-Encryption-Iv",
        "X-Rustfs-Replication-Server-Side-Encryption-Iv",
    ),
    ("X-Rustfs-Internal-Encrypted-Multipart", "X-Rustfs-Replication-Encrypted-Multipart"),
    ("X-Rustfs-Internal-Actual-Object-Size", "X-Rustfs-Replication-Actual-Object-Size"),
];

pub(crate) struct ReplicationTargetStore;

impl ReplicationTargetStore {
    pub(crate) async fn list_bucket_targets(bucket: &str) -> std::result::Result<BucketTargets, BucketTargetError> {
        BucketTargetSys::get().list_bucket_targets(bucket).await
    }

    pub(crate) async fn remote_target_client(bucket: &str, arn: &str) -> Option<Arc<TargetClient>> {
        BucketTargetSys::get().get_remote_target_client(bucket, arn).await
    }

    pub(crate) async fn target_is_offline(target_client: &TargetClient) -> bool {
        BucketTargetSys::get().is_offline(&target_client.to_url()).await
    }
}

pub(crate) fn replication_put_object_options(sc: &str, object_info: &ObjectInfo) -> Result<(PutObjectOptions, bool)> {
    use base64::{Engine, engine::general_purpose::STANDARD as BASE64_STANDARD};
    use rustfs_utils::http::{AMZ_CHECKSUM_TYPE, AMZ_CHECKSUM_TYPE_FULL_OBJECT};

    let mut meta = HashMap::new();
    let is_ssec = rustfs_replication::is_ssec_encrypted(&object_info.user_defined);

    for (key, value) in object_info.user_defined.iter() {
        let has_valid_sse_header = valid_sse_replication_header(key).is_some();

        if !is_ssec || !has_valid_sse_header {
            if is_internal_key(key) || is_standard_header(key) {
                continue;
            }
        }

        if let Some(replication_header) = valid_sse_replication_header(key) {
            meta.insert(replication_header.to_string(), value.to_string());
        } else {
            meta.insert(key.to_string(), value.to_string());
        }
    }

    let mut is_multipart = object_info.is_multipart();

    if let Some(checksum_data) = &object_info.checksum
        && !checksum_data.is_empty()
    {
        if is_ssec {
            let encoded = BASE64_STANDARD.encode(checksum_data);
            insert_header_map(&mut meta, SUFFIX_REPLICATION_SSEC_CRC, encoded);
        } else {
            let (checksum_meta, is_mp) = object_info.decrypt_checksums(0, &HeaderMap::new())?;
            is_multipart = is_mp;

            for (key, value) in checksum_meta.iter() {
                if key != AMZ_CHECKSUM_TYPE {
                    meta.insert(key.clone(), value.clone());
                }
            }

            if !object_info.is_multipart()
                && checksum_meta
                    .get(AMZ_CHECKSUM_TYPE)
                    .is_some_and(|value| value == AMZ_CHECKSUM_TYPE_FULL_OBJECT)
            {
                is_multipart = false;
            }
        }
    }

    let storage_class = if sc.is_empty() {
        let obj_sc = object_info.storage_class.as_deref().unwrap_or_default();
        if obj_sc == ReplicationConfigStore::STANDARD || obj_sc == ReplicationConfigStore::RRS {
            obj_sc.to_string()
        } else {
            sc.to_string()
        }
    } else {
        sc.to_string()
    };

    let mut put_options = PutObjectOptions {
        user_metadata: meta,
        content_type: object_info.content_type.clone().unwrap_or_default(),
        content_encoding: object_info.content_encoding.clone().unwrap_or_default(),
        expires: object_info.expires.unwrap_or(OffsetDateTime::UNIX_EPOCH),
        storage_class,
        internal: AdvancedPutOptions {
            source_version_id: object_info.version_id.map(|value| value.to_string()).unwrap_or_default(),
            source_etag: object_info.etag.clone().unwrap_or_default(),
            source_mtime: object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH),
            replication_status: ReplicationStatusType::Replica,
            replication_request: true,
            ..Default::default()
        },
        ..Default::default()
    };

    if !object_info.user_tags.is_empty() {
        let tags = ReplicationTagFilter::decode_tags_to_map(&object_info.user_tags);

        if !tags.is_empty() {
            put_options.user_tags = tags;
            put_options.internal.tagging_timestamp =
                if let Some(timestamp) = get_str(&object_info.user_defined, SUFFIX_TAGGING_TIMESTAMP) {
                    OffsetDateTime::parse(&timestamp, &Rfc3339)
                        .map_err(|err| Error::other(format!("Failed to parse tagging timestamp: {err}")))?
                } else {
                    object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
                };
        }
    }

    let metadata = &*object_info.user_defined;

    if let Some(language) = metadata.lookup(CONTENT_LANGUAGE) {
        put_options.content_language = language.to_string();
    }

    if let Some(content_disposition) = metadata.lookup(CONTENT_DISPOSITION) {
        put_options.content_disposition = content_disposition.to_string();
    }

    if let Some(cache_control) = metadata.lookup(CACHE_CONTROL) {
        put_options.cache_control = cache_control.to_string();
    }

    if let Some(mode) = metadata.lookup(AMZ_OBJECT_LOCK_MODE) {
        put_options.mode = Some(ObjectLockRetentionMode::from(mode.to_uppercase().as_str()));
    }

    if let Some(retain_until_date) = metadata.lookup(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE) {
        put_options.retain_until_date = OffsetDateTime::parse(retain_until_date, &Rfc3339)
            .map_err(|err| Error::other(format!("Failed to parse retain until date: {err}")))?;
        put_options.internal.retention_timestamp =
            if let Some(timestamp) = get_str(&object_info.user_defined, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP) {
                OffsetDateTime::parse(&timestamp, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH)
            } else {
                object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
            };
    }

    if let Some(legal_hold) = metadata.lookup(AMZ_OBJECT_LOCK_LEGAL_HOLD) {
        put_options.legalhold = Some(ObjectLockLegalHoldStatus::from(legal_hold.to_uppercase().as_str()));
        put_options.internal.legalhold_timestamp =
            if let Some(timestamp) = get_str(&object_info.user_defined, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP) {
                OffsetDateTime::parse(&timestamp, &Rfc3339).unwrap_or(OffsetDateTime::UNIX_EPOCH)
            } else {
                object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
            };
    }

    Ok((put_options, is_multipart))
}

pub(crate) fn replication_put_object_header_size(put_options: &PutObjectOptions) -> usize {
    put_options
        .header()
        .iter()
        .map(|(key, value)| key.as_str().len() + value.as_bytes().len() + 4)
        .sum()
}

pub(crate) fn replication_delete_remove_options(
    delete_marker: bool,
    replication_mtime: Option<OffsetDateTime>,
) -> RemoveObjectOptions {
    RemoveObjectOptions {
        force_delete: false,
        governance_bypass: false,
        replication_delete_marker: delete_marker,
        replication_mtime,
        replication_status: ReplicationStatusType::Replica,
        replication_request: true,
        replication_validity_check: false,
    }
}

pub(crate) fn replication_delete_marker_purge_remove_options(replication_mtime: Option<OffsetDateTime>) -> RemoveObjectOptions {
    RemoveObjectOptions {
        force_delete: false,
        governance_bypass: false,
        replication_delete_marker: false,
        replication_mtime,
        replication_status: ReplicationStatusType::Replica,
        replication_request: true,
        replication_validity_check: false,
    }
}

pub(crate) fn replication_force_delete_remove_options() -> RemoveObjectOptions {
    RemoveObjectOptions {
        force_delete: true,
        governance_bypass: false,
        replication_delete_marker: false,
        replication_mtime: None,
        replication_status: ReplicationStatusType::Replica,
        replication_request: true,
        replication_validity_check: false,
    }
}

pub(crate) fn replication_complete_multipart_options(actual_size: String) -> PutObjectOptions {
    let mut user_metadata = HashMap::new();
    insert_header_map(&mut user_metadata, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE, actual_size);

    PutObjectOptions {
        user_metadata,
        internal: AdvancedPutOptions {
            replication_status: ReplicationStatusType::Replica,
            replication_request: true,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn is_standard_header(key: &str) -> bool {
    STANDARD_HEADERS.iter().any(|header| header.eq_ignore_ascii_case(key))
}

fn valid_sse_replication_header(key: &str) -> Option<&str> {
    VALID_SSE_REPLICATION_HEADERS
        .iter()
        .find(|(internal, _)| key.eq_ignore_ascii_case(internal))
        .map(|(_, replication)| *replication)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_utils::http::{
        SSEC_ALGORITHM_HEADER, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE, SUFFIX_REPLICATION_SSEC_CRC, get_header_map,
    };
    use std::sync::Arc;
    use time::Duration;
    use uuid::Uuid;

    #[test]
    fn replication_remove_options_mark_replication_requests() {
        let mtime = OffsetDateTime::UNIX_EPOCH + Duration::seconds(10);

        let delete = replication_delete_remove_options(true, Some(mtime));
        assert!(!delete.force_delete);
        assert!(delete.replication_delete_marker);
        assert_eq!(delete.replication_mtime, Some(mtime));
        assert_eq!(delete.replication_status, ReplicationStatusType::Replica);
        assert!(delete.replication_request);

        let purge = replication_delete_marker_purge_remove_options(Some(mtime));
        assert!(!purge.force_delete);
        assert!(!purge.replication_delete_marker);
        assert_eq!(purge.replication_mtime, Some(mtime));
        assert_eq!(purge.replication_status, ReplicationStatusType::Replica);
        assert!(purge.replication_request);

        let force = replication_force_delete_remove_options();
        assert!(force.force_delete);
        assert!(!force.replication_delete_marker);
        assert_eq!(force.replication_status, ReplicationStatusType::Replica);
        assert!(force.replication_request);
    }

    #[test]
    fn replication_complete_multipart_options_sets_actual_size() {
        let options = replication_complete_multipart_options("1024".to_string());

        assert_eq!(
            get_header_map(&options.user_metadata, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE).as_deref(),
            Some("1024")
        );
        assert_eq!(options.internal.replication_status, ReplicationStatusType::Replica);
        assert!(options.internal.replication_request);
    }

    #[test]
    fn replication_put_options_filter_and_map_metadata() {
        let mut metadata = HashMap::new();
        metadata.insert(CONTENT_TYPE.to_string(), "text/plain".to_string());
        metadata.insert("x-user-meta".to_string(), "value".to_string());
        metadata.insert(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string());
        metadata.insert("X-Rustfs-Internal-Server-Side-Encryption-Sealed-Key".to_string(), "sealed".to_string());

        let object_info = ObjectInfo {
            user_defined: Arc::new(metadata),
            user_tags: Arc::new("env=prod".to_string()),
            content_type: Some("text/plain".to_string()),
            content_encoding: Some("gzip".to_string()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            version_id: Some(Uuid::nil()),
            etag: Some("0123456789abcdef0123456789abcdef".to_string()),
            ..Default::default()
        };

        let (options, is_multipart) = replication_put_object_options("STANDARD", &object_info).expect("build put options");

        assert!(!is_multipart);
        assert_eq!(options.user_metadata.get("x-user-meta"), Some(&"value".to_string()));
        assert!(!options.user_metadata.contains_key(CONTENT_TYPE));
        assert_eq!(
            options
                .user_metadata
                .get("X-Rustfs-Replication-Server-Side-Encryption-Sealed-Key"),
            Some(&"sealed".to_string())
        );
        assert_eq!(options.content_type, "text/plain");
        assert_eq!(options.content_encoding, "gzip");
        assert_eq!(options.user_tags.get("env"), Some(&"prod".to_string()));
        assert_eq!(options.internal.source_version_id, Uuid::nil().to_string());
        assert_eq!(options.internal.source_etag, "0123456789abcdef0123456789abcdef");
        assert_eq!(options.internal.replication_status, ReplicationStatusType::Replica);
        assert!(options.internal.replication_request);
    }

    #[test]
    fn replication_put_options_adds_ssec_checksum_metadata() {
        let metadata = HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())]);
        let object_info = ObjectInfo {
            user_defined: Arc::new(metadata),
            checksum: Some(bytes::Bytes::from_static(b"checksum")),
            ..Default::default()
        };

        let (options, _) = replication_put_object_options("", &object_info).expect("build put options");

        assert!(get_header_map(&options.user_metadata, SUFFIX_REPLICATION_SSEC_CRC).is_some());
    }
}
