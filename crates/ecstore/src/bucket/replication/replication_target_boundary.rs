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

pub(crate) use crate::bucket::bucket_target_sys::BucketTargetError;
use crate::bucket::bucket_target_sys::BucketTargetSys;
use aws_sdk_s3::operation::head_object::HeadObjectOutput;
use aws_sdk_s3::types::{ObjectLockLegalHoldStatus, ObjectLockRetentionMode};
use http::HeaderMap;
use rustfs_replication::{
    ReplicationSourceObject, ReplicationTargetObject, replication_action_for_target, target_is_newer_than_source_null_version,
};
use rustfs_utils::http::{
    AMZ_BUCKET_REPLICATION_STATUS, AMZ_OBJECT_LOCK_LEGAL_HOLD, AMZ_OBJECT_LOCK_MODE, AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE,
    AMZ_OBJECT_TAGGING, AMZ_SERVER_SIDE_ENCRYPTION, AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT, AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID,
    AMZ_STORAGE_CLASS, AMZ_TAG_COUNT, CACHE_CONTROL, CONTENT_DISPOSITION, CONTENT_ENCODING, CONTENT_LANGUAGE, CONTENT_TYPE,
    HeaderExt as _, SUFFIX_OBJECTLOCK_LEGALHOLD_TIMESTAMP, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP,
    SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE, SUFFIX_REPLICATION_SSEC_CRC, SUFFIX_SOURCE_REPLICATION_LEGALHOLD_TIMESTAMP,
    SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP, SUFFIX_SOURCE_REPLICATION_TAGGING_TIMESTAMP, SUFFIX_TAGGING_TIMESTAMP,
    get_str, insert_header_map, is_internal_key, is_object_encryption_marker, is_replication_stripped_encryption_key,
    ssec_replication_transport_header,
};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

pub(crate) use crate::bucket::bucket_target_sys::{
    AdvancedPutOptions, HeadObjectSdkError, PutObjectOptions, PutObjectPartOptions, RemotePutObjectResponse, RemoveObjectOptions,
    S3ClientError, TargetClient, resolve_read_api_version_id,
};
#[cfg(test)]
pub(crate) use crate::bucket::target::BucketTarget;
pub(crate) use crate::bucket::target::BucketTargets;
pub use rustfs_replication::SsecPassthroughCapability;
pub use rustfs_replication::{ObjectLockIntegrity, object_lock_put_integrity};
pub(crate) use rustfs_replication::{
    SsecPassthroughGate, is_replication_target_offline_error, ssec_passthrough_gate, version_identity_drifted,
};
pub use rustfs_replication::{VersionIdentityCapability, version_identity_capability_from_put};

use super::replication_config_store::ReplicationConfigStore;
use super::replication_error_boundary::{Error, Result};
use super::replication_filemeta_boundary::{ReplicationAction, ReplicationStatusType, ReplicationType};
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

const ERR_REPLICATION_ENCRYPTION_METADATA_UNSUPPORTED: &str = "replication source contains unsupported encryption metadata";
pub(crate) const ERR_REPLICATION_SSEC_PASSTHROUGH_UNSUPPORTED: &str = "replication target does not support SSE-C passthrough: the replica would lose its decryption material \
     (run ?replication-check to re-probe)";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReplicationSourceEncryption {
    Plaintext,
    SseS3,
    SseKms,
    SseC,
    Unsupported,
}

fn metadata_value<'a>(metadata: &'a HashMap<String, String>, name: &str) -> Option<&'a str> {
    metadata
        .iter()
        .find(|(key, _)| key.eq_ignore_ascii_case(name))
        .map(|(_, value)| value.as_str())
}

fn classify_replication_source_encryption(metadata: &HashMap<String, String>) -> ReplicationSourceEncryption {
    let is_ssec = replication_object_is_ssec_encrypted(metadata);
    let sse = metadata_value(metadata, AMZ_SERVER_SIDE_ENCRYPTION);
    let kms_key_id = metadata_value(metadata, AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID);
    let kms_context = metadata_value(metadata, AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT);

    if is_ssec {
        // Stored SSE-C objects always carry x-amz-server-side-encryption=AES256
        // alongside the customer-algorithm key; only KMS evidence marks a
        // mixed, unsupported state.
        let sse_compatible = sse.map(str::trim).is_none_or(|value| value.eq_ignore_ascii_case("AES256"));
        return if sse_compatible && kms_key_id.is_none() && kms_context.is_none() {
            ReplicationSourceEncryption::SseC
        } else {
            ReplicationSourceEncryption::Unsupported
        };
    }

    match sse.map(str::trim) {
        None if kms_key_id.is_none() && kms_context.is_none() => {
            // Sealed material without any recognizable SSE marker (e.g. an
            // object written by MinIO, which does not persist the x-amz SSE
            // intent header) must fail closed: replicating it as plaintext
            // ships ciphertext the target can never decrypt.
            if metadata.keys().any(|key| is_object_encryption_marker(key)) {
                ReplicationSourceEncryption::Unsupported
            } else {
                ReplicationSourceEncryption::Plaintext
            }
        }
        Some(value) if value.eq_ignore_ascii_case("AES256") && kms_key_id.is_none() && kms_context.is_none() => {
            ReplicationSourceEncryption::SseS3
        }
        Some(value) if value.eq_ignore_ascii_case("aws:kms") => ReplicationSourceEncryption::SseKms,
        _ if kms_key_id.is_some() => ReplicationSourceEncryption::SseKms,
        _ => ReplicationSourceEncryption::Unsupported,
    }
}

fn is_legacy_source_replication_timestamp_key(key: &str) -> bool {
    fn has_prefix_and_suffix(key: &str, prefix: &str, suffix: &str) -> bool {
        let key = key.as_bytes();
        key.len() == prefix.len() + suffix.len()
            && key[..prefix.len()].eq_ignore_ascii_case(prefix.as_bytes())
            && key[prefix.len()..].eq_ignore_ascii_case(suffix.as_bytes())
    }

    [
        SUFFIX_SOURCE_REPLICATION_TAGGING_TIMESTAMP,
        SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP,
        SUFFIX_SOURCE_REPLICATION_LEGALHOLD_TIMESTAMP,
    ]
    .iter()
    .any(|suffix| {
        ["x-rustfs-", "x-minio-"]
            .iter()
            .any(|prefix| has_prefix_and_suffix(key, prefix, suffix))
    })
}

pub(crate) fn replication_object_is_ssec_encrypted(user_defined: &HashMap<String, String>) -> bool {
    rustfs_replication::is_ssec_encrypted(user_defined)
}

/// HeadObjectOutput adapter over the pure SSE-C passthrough evidence
/// judgment owned by `rustfs-replication`: extract the echoed
/// customer-algorithm header and let the crate-owned policy decide.
pub(crate) fn ssec_passthrough_evidence_present(head: &HeadObjectOutput) -> bool {
    rustfs_replication::ssec_passthrough_evidence_present(head.sse_customer_algorithm.as_deref())
}

pub(crate) struct ReplicationTargetStore;

impl ReplicationTargetStore {
    pub(crate) async fn list_bucket_targets(bucket: &str) -> std::result::Result<BucketTargets, BucketTargetError> {
        BucketTargetSys::get().list_bucket_targets(bucket).await
    }

    pub(crate) async fn remote_target_client(bucket: &str, arn: &str) -> Option<Arc<TargetClient>> {
        BucketTargetSys::get().get_remote_target_client(bucket, arn).await
    }

    pub(crate) async fn target_is_offline(target_client: &Arc<TargetClient>) -> bool {
        BucketTargetSys::get().is_target_offline(target_client).await
    }

    pub(crate) async fn mark_target_offline(target_client: &Arc<TargetClient>) {
        BucketTargetSys::get().mark_target_offline(target_client).await
    }

    /// Returns the cached verdict and whether it has outlived its TTL.
    pub(crate) async fn ssec_passthrough_capability(arn: &str) -> (SsecPassthroughCapability, bool) {
        BucketTargetSys::get().ssec_passthrough_capability(arn).await
    }

    pub(crate) async fn record_ssec_passthrough_capability(arn: &str, capability: SsecPassthroughCapability) {
        BucketTargetSys::get()
            .record_ssec_passthrough_capability(arn, capability)
            .await
    }

    pub(crate) fn version_identity_capability(arn: &str) -> VersionIdentityCapability {
        BucketTargetSys::get().version_identity_capability(arn)
    }

    pub(crate) fn record_version_identity_capability(arn: &str, capability: VersionIdentityCapability) {
        BucketTargetSys::get().record_version_identity_capability(arn, capability)
    }

    #[cfg(test)]
    pub(crate) async fn register_test_target(target_client: &Arc<TargetClient>) {
        BucketTargetSys::get().arn_remotes_map.write().await.insert(
            target_client.arn.clone(),
            crate::bucket::bucket_target_sys::ArnTarget::with_client(target_client.clone()),
        );
    }
}

pub(crate) fn replication_put_object_options(sc: &str, object_info: &ObjectInfo) -> Result<(PutObjectOptions, bool)> {
    use base64_simd::STANDARD as BASE64_STANDARD;
    use rustfs_utils::http::{AMZ_CHECKSUM_TYPE, AMZ_CHECKSUM_TYPE_FULL_OBJECT};

    let mut meta = HashMap::new();
    let source_encryption = classify_replication_source_encryption(&object_info.user_defined);
    let is_ssec = matches!(source_encryption, ReplicationSourceEncryption::SseC);

    if matches!(source_encryption, ReplicationSourceEncryption::Unsupported) {
        return Err(Error::other(ERR_REPLICATION_ENCRYPTION_METADATA_UNSUPPORTED));
    }

    for (key, value) in object_info.user_defined.iter() {
        if is_ssec && let Some(transport_header) = ssec_replication_transport_header(key) {
            meta.insert(transport_header.to_string(), value.to_string());
            continue;
        }

        // Encryption metadata that is not remapped for SSE-C passthrough must
        // never leave the source site: envelopes and intent headers are only
        // meaningful to the source KMS.
        if is_replication_stripped_encryption_key(key) {
            continue;
        }

        if is_legacy_source_replication_timestamp_key(key) {
            meta.insert(format!("x-amz-meta-{key}"), value.to_string());
            continue;
        }

        if is_internal_key(key) || is_standard_header(key) {
            continue;
        }

        meta.insert(key.to_string(), value.to_string());
    }

    // Managed SSE replicates as plaintext (the replication reader decrypts via
    // the object-encryption resolver) and re-encrypts on the target with the
    // target's own KMS. Send only the encryption intent — never the source
    // key id, whose meaning is local to the source site's KMS.
    if matches!(source_encryption, ReplicationSourceEncryption::SseS3) {
        meta.insert(AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "AES256".to_string());
    } else if matches!(source_encryption, ReplicationSourceEncryption::SseKms) {
        meta.insert(AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "aws:kms".to_string());
    }

    // Older transformed objects can have physical parts without logical part
    // lengths. Keep their existing whole-object transport: physical sizes are
    // not plaintext boundaries for a multipart replication read.
    let legacy_single_put = object_info.etag.as_deref().is_none_or(|etag| etag.len() == 32);
    let base_is_multipart = object_info.is_multipart()
        && !(legacy_single_put
            && object_info.parts.len() > 1
            && (object_info.is_compressed() || object_info.is_encrypted())
            && object_info.parts.iter().any(|part| part.actual_size <= 0));
    let mut is_multipart = base_is_multipart;

    if let Some(checksum_data) = &object_info.checksum
        && !checksum_data.is_empty()
    {
        if is_ssec {
            let encoded = BASE64_STANDARD.encode_to_string(checksum_data);
            insert_header_map(&mut meta, SUFFIX_REPLICATION_SSEC_CRC, encoded);
        } else if object_info.is_encrypted() {
            // Encrypted checksums cannot be exposed as plaintext headers, and
            // decrypt_checksums reports is_multipart=false for them (a value
            // the response path relies on). Keep the transport selected from
            // the object's layout and readable part boundaries.
        } else {
            let (checksum_meta, checksum_record_is_multipart) = object_info.decrypt_checksums(0, &HeaderMap::new())?;
            // The checksum record describes how the *checksum* is composed,
            // not how the object is stored. A full-object checksum carries no
            // MULTIPART flag even on a multipart upload, so trusting it here
            // routed a 768-part object through a single PutObject and the
            // target rejected the 6 GiB body with EntityTooLarge
            // (rustfs#6825). The usable part layout is the authority: the
            // record may only add multipart-ness, never take it away.
            is_multipart = base_is_multipart || checksum_record_is_multipart;

            if !base_is_multipart
                && checksum_meta
                    .get(AMZ_CHECKSUM_TYPE)
                    .is_some_and(|value| value == AMZ_CHECKSUM_TYPE_FULL_OBJECT)
            {
                is_multipart = false;
            }

            // The record keys each checksum by algorithm name ("CRC32"); the
            // target only reads `x-amz-checksum-<algorithm>`. Inserting the bare
            // name here made `PutObjectOptions::header()` send it as user
            // metadata (`x-amz-meta-crc32`), so no replica ever carried the
            // source checksum (rustfs/backlog#2340). The object-level record
            // describes one PUT body: a multipart replica is rebuilt part by
            // part, and its CreateMultipartUpload must not announce a checksum
            // the parts do not carry, so the record is forwarded on the
            // single-PUT route only (MinIO `getCRCMeta` parity).
            if !is_multipart {
                for (key, value) in checksum_meta.iter() {
                    if key == AMZ_CHECKSUM_TYPE {
                        continue;
                    }
                    if let Some(header) = rustfs_rio::ChecksumType::from_string(key).key() {
                        meta.insert(header.to_string(), value.clone());
                    }
                }
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
        }
    }
    // Load the stored tagging timestamp independently of whether any tags
    // remain: DeleteObjectTagging leaves the object tagless but stamps this
    // key, and the deletion's LWW timestamp must still reach the replica.
    // With no stored key, fall back to mod_time only while tags exist
    // (MinIO parity); a tagless object without the key was never tagged and
    // keeps the epoch default (no header).
    put_options.internal.tagging_timestamp = if let Some(timestamp) = get_str(&object_info.user_defined, SUFFIX_TAGGING_TIMESTAMP)
    {
        OffsetDateTime::parse(&timestamp, &Rfc3339)
            .map_err(|err| Error::other(format!("Failed to parse tagging timestamp: {err}")))?
    } else if !put_options.user_tags.is_empty() {
        object_info.mod_time.unwrap_or(OffsetDateTime::UNIX_EPOCH)
    } else {
        OffsetDateTime::UNIX_EPOCH
    };

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

    if let Some(mode) = metadata.lookup(AMZ_OBJECT_LOCK_MODE).filter(|mode| !mode.is_empty()) {
        put_options.mode = Some(ObjectLockRetentionMode::from(mode.to_uppercase().as_str()));
    }

    if let Some(retain_until_date) = metadata.lookup(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE) {
        if !retain_until_date.is_empty() {
            put_options.retain_until_date = OffsetDateTime::parse(retain_until_date, &Rfc3339)
                .map_err(|err| Error::other(format!("Failed to parse retain until date: {err}")))?;
        }
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

fn replication_source_object(object_info: &ObjectInfo) -> ReplicationSourceObject<'_> {
    ReplicationSourceObject {
        mod_time: object_info
            .mod_time
            .map(|mod_time| OffsetDateTime::from_unix_timestamp(mod_time.unix_timestamp()).unwrap_or(mod_time)),
        version_id: object_info.version_id.map(|version_id| version_id.to_string()),
        etag: object_info.etag.as_deref(),
        actual_size: object_info.get_actual_size_or_physical(),
        delete_marker: object_info.delete_marker,
        content_type: object_info.content_type.as_deref(),
        content_encoding: object_info.content_encoding.as_deref(),
        user_tags: object_info.user_tags.as_str(),
        user_defined: object_info.user_defined.as_ref(),
    }
}

fn replication_target_last_modified(target: &HeadObjectOutput) -> Option<OffsetDateTime> {
    target
        .last_modified
        .map(|dt| OffsetDateTime::from_unix_timestamp(dt.secs()).unwrap_or(OffsetDateTime::UNIX_EPOCH))
}

fn replication_target_object(target: &HeadObjectOutput) -> ReplicationTargetObject<'_> {
    ReplicationTargetObject {
        last_modified: replication_target_last_modified(target),
        version_id: target.version_id.as_deref(),
        etag: target.e_tag.as_deref(),
        content_length: target.content_length.unwrap_or_default(),
        delete_marker: target.delete_marker.unwrap_or_default(),
        content_type: target.content_type.as_deref(),
        metadata: target.metadata.as_ref(),
        tag_count: target.tag_count.unwrap_or_default(),
    }
}

pub(crate) fn replication_action_for_target_head(
    object_info: &ObjectInfo,
    target: &HeadObjectOutput,
    op_type: ReplicationType,
) -> ReplicationAction {
    replication_action_for_target(&replication_source_object(object_info), &replication_target_object(target), op_type)
}

pub(crate) fn replication_target_head_is_newer_null_version(object_info: &ObjectInfo, target: &HeadObjectOutput) -> bool {
    target_is_newer_than_source_null_version(&replication_source_object(object_info), &replication_target_object(target))
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

pub(crate) fn replication_complete_multipart_options(
    actual_size: String,
    source_etag: String,
    source_mtime: Option<OffsetDateTime>,
    source_internal: &AdvancedPutOptions,
) -> PutObjectOptions {
    let mut user_metadata = HashMap::new();
    insert_header_map(&mut user_metadata, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE, actual_size);

    PutObjectOptions {
        user_metadata,
        internal: AdvancedPutOptions {
            source_etag,
            // AdvancedPutOptions::default() stamps now_utc(); an absent source
            // mtime must degrade to epoch so header() suppresses the header
            // instead of asserting the replication time as the object's mtime.
            source_mtime: source_mtime.unwrap_or(OffsetDateTime::UNIX_EPOCH),
            // Carry the per-category LWW timestamps on the complete request as
            // well: the receiver's CompleteMultipartUpload options builder
            // parses the same headers, so the multipart transport gets the
            // same receiver-side LWW as the single-PUT transport
            // (rustfs/backlog#1953). Epoch values keep the headers suppressed.
            tagging_timestamp: source_internal.tagging_timestamp,
            retention_timestamp: source_internal.retention_timestamp,
            legalhold_timestamp: source_internal.legalhold_timestamp,
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

#[cfg(test)]
mod tests {
    use super::super::replication_filemeta_boundary::ObjectPartInfo;
    use super::*;
    use aws_smithy_types::DateTime;
    use rustfs_replication::content_matches_by_etag;
    use rustfs_utils::http::{
        SSEC_ALGORITHM_HEADER, SSEC_KEY_MD5_HEADER, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE, SUFFIX_REPLICATION_SSEC_CRC,
        get_header_map,
    };
    use std::sync::Arc;
    use time::Duration;
    use uuid::Uuid;

    /// Serialize an object-level checksum record the way
    /// `complete_multipart_upload` persists it for a **full-object** checksum:
    /// the record carries the plain algorithm type, without the MULTIPART
    /// flags that a composite record gets.
    fn full_object_multipart_checksum_record() -> bytes::Bytes {
        let checksum_type = rustfs_rio::ChecksumType::from_string_with_obj_type("crc32", "FULL_OBJECT");
        assert!(checksum_type.is_set(), "crc32 FULL_OBJECT must be a valid checksum type");
        assert!(checksum_type.full_object_requested());

        let mut combined = Vec::new();
        let mut checksum = rustfs_rio::Checksum {
            checksum_type,
            ..Default::default()
        };
        for part in [b"part-one".as_slice(), b"part-two".as_slice()] {
            let part_checksum = rustfs_rio::Checksum::new_from_data(checksum_type, part).expect("part checksum");
            combined.extend_from_slice(part_checksum.raw.as_slice());
            checksum.add_part(&part_checksum, part.len() as i64).expect("add part");
        }

        checksum.to_bytes(&combined)
    }

    fn replication_route_metadata() -> [(&'static str, Arc<HashMap<String, String>>); 4] {
        let mut compressed = HashMap::new();
        rustfs_utils::http::insert_str(&mut compressed, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
        [
            ("plain", Arc::new(HashMap::new())),
            ("compressed", Arc::new(compressed)),
            (
                "encrypted",
                Arc::new(HashMap::from([(AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "AES256".to_string())])),
            ),
            (
                "ssec",
                Arc::new(HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())])),
            ),
        ]
    }

    fn replication_route_object(
        etag: Option<&str>,
        actual_sizes: [i64; 3],
        metadata: Arc<HashMap<String, String>>,
    ) -> ObjectInfo {
        ObjectInfo {
            etag: etag.map(str::to_string),
            size: 48,
            actual_size: 12,
            user_defined: metadata,
            parts: Arc::new(
                actual_sizes
                    .into_iter()
                    .enumerate()
                    .map(|(index, actual_size)| ObjectPartInfo {
                        number: index + 1,
                        size: 16,
                        actual_size,
                        ..Default::default()
                    })
                    .collect(),
            ),
            ..Default::default()
        }
    }

    #[test]
    fn legacy_transformed_single_put_parts_keep_the_previous_replication_route() {
        let [_, (_, compressed), (_, encrypted), (_, ssec)] = replication_route_metadata();
        let cases = [
            (
                "compressed middle zero",
                compressed.clone(),
                Some("0123456789abcdef0123456789abcdef"),
                [4, 0, 4],
            ),
            ("compressed tail unknown", compressed, None, [4, 4, -1]),
            (
                "encrypted middle unknown",
                encrypted,
                Some("gggggggggggggggggggggggggggggggg"),
                [4, -1, 4],
            ),
            ("ssec tail zero", ssec.clone(), None, [4, 4, 0]),
            ("ssec middle unknown", ssec, Some("gggggggggggggggggggggggggggggggg"), [4, -1, 4]),
        ];
        for (name, metadata, etag, actual_sizes) in cases {
            for checksum in [None, Some(full_object_multipart_checksum_record())] {
                let mut object_info = replication_route_object(etag, actual_sizes, metadata.clone());
                object_info.checksum = checksum;
                assert!(object_info.is_multipart(), "{name}: physical parts remain visible to metadata APIs");
                assert!(object_info.is_compressed() || object_info.is_encrypted());

                let (options, is_multipart) =
                    replication_put_object_options("STANDARD", &object_info).expect("legacy transformed put options");
                assert!(
                    !is_multipart,
                    "{name}: unknown logical part sizes must preserve the old whole-object route"
                );
                assert_eq!(options.internal.source_etag, etag.unwrap_or_default());
                if metadata.contains_key(SSEC_ALGORITHM_HEADER) {
                    assert_eq!(
                        get_header_map(&options.user_metadata, SUFFIX_REPLICATION_SSEC_CRC).is_some(),
                        object_info.checksum.is_some(),
                        "SSE-C checksums retain their raw passthrough transport"
                    );
                }
            }
        }
    }

    #[test]
    fn positive_part_sizes_and_legacy_multipart_etags_keep_the_replication_route() {
        for (name, metadata) in replication_route_metadata() {
            for (etag, actual_sizes) in [
                ("0123456789abcdef0123456789abcdef", [4, 4, 4]),
                ("0123456789abcdef0123456789abcdef-3", [4, 0, -1]),
            ] {
                let mut object_info = replication_route_object(Some(etag), actual_sizes, metadata.clone());
                object_info.checksum = Some(full_object_multipart_checksum_record());
                let (_, is_multipart) = replication_put_object_options("STANDARD", &object_info).expect("multipart put options");
                assert!(is_multipart, "{name}/{etag}: usable sizes and old multipart ETags must retain MPU");
            }
        }
    }

    #[test]
    fn multipart_object_with_full_object_checksum_keeps_the_multipart_route() {
        // rustfs#6825: a 768-part upload was replicated with a single
        // PutObject and rejected by the target with EntityTooLarge. The
        // object's storage shape says multipart; only the checksum record
        // looked single-part, and the checksum record must not decide the
        // transport.
        let object_info = ObjectInfo {
            etag: Some("0123456789abcdef0123456789abcdef-768".to_string()),
            checksum: Some(full_object_multipart_checksum_record()),
            size: 6 * 1024 * 1024 * 1024,
            ..Default::default()
        };

        assert!(object_info.is_multipart(), "the fixture must be a multipart object");
        let (_, checksum_says_multipart) = object_info
            .decrypt_checksums(0, &HeaderMap::new())
            .expect("checksum record must decode");
        assert!(
            !checksum_says_multipart,
            "fixture precondition: a full-object record carries no MULTIPART flag, which is what used to \
             downgrade the transport"
        );

        let (_, is_multipart) = replication_put_object_options("STANDARD", &object_info).expect("build put options");

        assert!(
            is_multipart,
            "a multipart object must replicate over multipart whatever its checksum record looks like"
        );
    }

    #[test]
    fn stored_multipart_parts_keep_the_replication_route_without_a_multipart_etag() {
        for etag in [Some("0123456789abcdef0123456789abcdef"), None] {
            for checksum in [None, Some(full_object_multipart_checksum_record())] {
                let object_info = ObjectInfo {
                    etag: etag.map(str::to_string),
                    checksum,
                    parts: Arc::new(
                        (1..=2)
                            .map(|number| ObjectPartInfo {
                                number,
                                ..Default::default()
                            })
                            .collect(),
                    ),
                    ..Default::default()
                };
                let (options, is_multipart) =
                    replication_put_object_options("STANDARD", &object_info).expect("build put options");

                assert!(
                    is_multipart,
                    "stored parts must retain multipart routing: etag={etag:?}, checksum={:?}",
                    object_info.checksum
                );
                assert_eq!(options.internal.source_etag, etag.unwrap_or_default());
            }
        }
    }

    #[test]
    fn checksum_record_never_changes_the_transport_a_single_part_object_needs() {
        // The mirror of the rustfs#6825 guard: an object stored as one PUT
        // must keep the single-PUT transport, or its replica's ETag would
        // change shape and every ETag-based convergence check would re-copy it.
        let checksum =
            rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::CRC32, b"whole-object").expect("checksum fixture");
        let object_info = ObjectInfo {
            etag: Some("0123456789abcdef0123456789abcdef".to_string()),
            checksum: Some(checksum.to_bytes(&[])),
            parts: Arc::new(vec![ObjectPartInfo {
                number: 1,
                ..Default::default()
            }]),
            ..Default::default()
        };

        assert!(!object_info.is_multipart(), "the fixture must be a single-part object");

        let (_, is_multipart) = replication_put_object_options("STANDARD", &object_info).expect("build put options");

        assert!(!is_multipart, "a single-part object must not be promoted onto the multipart transport");
    }

    #[test]
    fn composite_checksum_multipart_object_keeps_the_multipart_route() {
        // The checksum shape that already worked before rustfs#6825, pinned so
        // the fix cannot regress it.
        let mut checksum_type = rustfs_rio::ChecksumType::from_string("crc32");
        checksum_type
            .merge(rustfs_rio::ChecksumType::MULTIPART)
            .merge(rustfs_rio::ChecksumType::INCLUDES_MULTIPART);

        let mut combined = Vec::new();
        for part in [b"part-one".as_slice(), b"part-two".as_slice()] {
            let part_checksum =
                rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::from_string("crc32"), part).expect("part checksum");
            combined.extend_from_slice(part_checksum.raw.as_slice());
        }
        let checksum = rustfs_rio::Checksum::new_from_data(checksum_type, &combined).expect("composite checksum");

        let object_info = ObjectInfo {
            etag: Some("0123456789abcdef0123456789abcdef-2".to_string()),
            checksum: Some(checksum.to_bytes(&combined)),
            ..Default::default()
        };

        let (_, is_multipart) = replication_put_object_options("STANDARD", &object_info).expect("build put options");

        assert!(is_multipart, "a composite-checksum multipart object must stay on the multipart transport");

        for (name, metadata) in replication_route_metadata() {
            let mut legacy = replication_route_object(Some("0123456789abcdef0123456789abcdef"), [4, 0, 4], metadata);
            legacy.checksum = Some(checksum.to_bytes(&combined));
            let (_, record_is_multipart) = legacy.decrypt_checksums(0, &HeaderMap::new()).expect("decode checksum");
            let (_, is_multipart) = replication_put_object_options("STANDARD", &legacy).expect("legacy checksum put options");
            if legacy.is_encrypted() {
                assert!(!is_multipart, "{name}: encrypted checksum records must not change the old transport");
            } else {
                assert!(record_is_multipart, "the composite checksum must carry its own multipart signal");
                assert!(is_multipart, "{name}: a composite record can still promote the legacy route to MPU");
            }
        }
    }

    #[test]
    fn replication_action_for_target_head_existing_object_source_newer_null_version_requires_replication() {
        let source = ObjectInfo {
            mod_time: Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(20)),
            version_id: None,
            ..Default::default()
        };
        let target = HeadObjectOutput::builder().last_modified(DateTime::from_secs(10)).build();

        assert_eq!(
            replication_action_for_target_head(&source, &target, ReplicationType::ExistingObject),
            ReplicationAction::All,
            "a newer source null version must not be skipped during existing-object replication"
        );
    }

    #[test]
    fn replication_action_for_target_head_existing_object_target_newer_null_version_skips() {
        let source = ObjectInfo {
            mod_time: Some(OffsetDateTime::UNIX_EPOCH + Duration::seconds(10)),
            version_id: None,
            ..Default::default()
        };
        let target = HeadObjectOutput::builder().last_modified(DateTime::from_secs(20)).build();

        assert_eq!(
            replication_action_for_target_head(&source, &target, ReplicationType::ExistingObject),
            ReplicationAction::None,
            "a newer target null-version object should not be overwritten by existing-object replication"
        );
        assert!(replication_target_head_is_newer_null_version(&source, &target));
    }

    #[test]
    fn replication_source_uses_physical_size_for_unknown_compressed_object() {
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(&mut metadata, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
        let source = ObjectInfo {
            size: 128,
            actual_size: -1,
            user_defined: Arc::new(metadata),
            ..Default::default()
        };

        assert_eq!(replication_source_object(&source).actual_size, 128);
    }

    #[test]
    fn replication_target_head_content_matches_compare_etag_only() {
        let source = ObjectInfo {
            etag: Some("\"abc123\"".to_string()),
            ..Default::default()
        };

        let target_match = HeadObjectOutput::builder().e_tag("\"abc123\"").build();
        assert!(
            content_matches_by_etag(&replication_source_object(&source), &replication_target_object(&target_match)),
            "identical ETags must match"
        );

        let target_unquoted_match = HeadObjectOutput::builder().e_tag("abc123").build();
        assert!(
            content_matches_by_etag(&replication_source_object(&source), &replication_target_object(&target_unquoted_match)),
            "quoted and unquoted ETags with identical values must match"
        );

        let target_different_version = HeadObjectOutput::builder()
            .e_tag("\"abc123\"")
            .version_id("aws-alphanumeric-id")
            .build();
        assert!(
            content_matches_by_etag(&replication_source_object(&source), &replication_target_object(&target_different_version)),
            "matching ETags with different version IDs must still match"
        );

        let target_different_content = HeadObjectOutput::builder().e_tag("\"def456\"").build();
        assert!(
            !content_matches_by_etag(&replication_source_object(&source), &replication_target_object(&target_different_content)),
            "different ETags must not match"
        );

        let source_no_etag = ObjectInfo {
            etag: None,
            ..Default::default()
        };
        assert!(
            !content_matches_by_etag(&replication_source_object(&source_no_etag), &replication_target_object(&target_match)),
            "missing source ETag must not match"
        );

        let target_no_etag = HeadObjectOutput::builder().build();
        assert!(
            !content_matches_by_etag(&replication_source_object(&source), &replication_target_object(&target_no_etag)),
            "missing target ETag must not match"
        );
    }

    #[test]
    fn replication_action_for_target_head_compares_http_date_precision() {
        for (source_nanos, target_secs, expected) in [
            (10_123_456_789, 10, ReplicationAction::None),
            (-10_876_543_211, -11, ReplicationAction::None),
            (10_600_000_000, 11, ReplicationAction::All),
        ] {
            let mod_time = OffsetDateTime::from_unix_timestamp_nanos(source_nanos).expect("valid timestamp");
            let object_info = ObjectInfo {
                mod_time: Some(mod_time),
                version_id: Some(Uuid::new_v4()),
                etag: Some("abc123".to_string()),
                size: 10,
                ..Default::default()
            };
            let target = HeadObjectOutput::builder()
                .last_modified(DateTime::from_secs(target_secs))
                .version_id(object_info.version_id.expect("version ID").to_string())
                .e_tag("abc123")
                .content_length(10)
                .build();

            assert_eq!(
                replication_action_for_target_head(&object_info, &target, ReplicationType::Object),
                expected
            );
        }
    }

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
        let source_mtime = OffsetDateTime::from_unix_timestamp(1_716_170_000).expect("valid test timestamp");
        let source_internal = AdvancedPutOptions {
            tagging_timestamp: OffsetDateTime::from_unix_timestamp(1_716_170_100).expect("valid test timestamp"),
            retention_timestamp: OffsetDateTime::from_unix_timestamp(1_716_170_200).expect("valid test timestamp"),
            legalhold_timestamp: OffsetDateTime::from_unix_timestamp(1_716_170_300).expect("valid test timestamp"),
            ..Default::default()
        };
        let options = replication_complete_multipart_options(
            "1024".to_string(),
            "0123456789abcdef0123456789abcdef-3".to_string(),
            Some(source_mtime),
            &source_internal,
        );
        assert_eq!(options.internal.source_etag, "0123456789abcdef0123456789abcdef-3");
        assert_eq!(options.internal.source_mtime, source_mtime);

        // The complete request must carry the same per-category LWW timestamps
        // as the initiate request; the receiver reads them from the complete
        // headers (rustfs/backlog#1953).
        assert_eq!(options.internal.tagging_timestamp, source_internal.tagging_timestamp);
        assert_eq!(options.internal.retention_timestamp, source_internal.retention_timestamp);
        assert_eq!(options.internal.legalhold_timestamp, source_internal.legalhold_timestamp);

        // Absent source mtime must degrade to epoch (header suppressed), not
        // the AdvancedPutOptions default of now_utc() — that default would
        // stamp the replication time as the replica's mtime and break the
        // multipart HEAD convergence. Unset category timestamps stay epoch so
        // header() keeps suppressing them.
        let options_no_mtime =
            replication_complete_multipart_options("1024".to_string(), String::new(), None, &AdvancedPutOptions::default());
        assert_eq!(options_no_mtime.internal.source_mtime.unix_timestamp(), 0);
        assert_eq!(options_no_mtime.internal.tagging_timestamp.unix_timestamp(), 0);
        assert_eq!(options_no_mtime.internal.retention_timestamp.unix_timestamp(), 0);
        assert_eq!(options_no_mtime.internal.legalhold_timestamp.unix_timestamp(), 0);

        assert_eq!(
            get_header_map(&options.user_metadata, SUFFIX_REPLICATION_ACTUAL_OBJECT_SIZE).as_deref(),
            Some("1024")
        );
        assert_eq!(options.internal.replication_status, ReplicationStatusType::Replica);
        assert!(options.internal.replication_request);
    }

    #[test]
    fn replication_put_options_filter_and_map_metadata() {
        use rustfs_utils::http::object_encryption_keys::{
            INTERNAL_ENCRYPTION_IV_HEADER, MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER, MINIO_INTERNAL_ENCRYPTION_IV_HEADER,
            MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER, MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER,
            REPLICATION_ENCRYPTED_MULTIPART_HEADER, REPLICATION_ENCRYPTION_IV_HEADER, REPLICATION_SSE_IV_HEADER,
            REPLICATION_SSE_SEAL_ALGORITHM_HEADER, REPLICATION_SSE_SEALED_KEY_HEADER, REPLICATION_SSEC_ALGORITHM_HEADER,
            REPLICATION_SSEC_KEY_MD5_HEADER, REPLICATION_SSEC_ORIGINAL_SIZE_HEADER, SSEC_ORIGINAL_SIZE_HEADER,
        };

        // The stored shape of a real SSE-C object: SSE marker plus customer
        // material, per encryption_material_to_metadata. Every transport-table
        // source key is present so each mapping is pinned individually.
        let mut metadata = HashMap::new();
        metadata.insert(CONTENT_TYPE.to_string(), "text/plain".to_string());
        metadata.insert("x-user-meta".to_string(), "value".to_string());
        metadata.insert(AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "AES256".to_string());
        metadata.insert(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string());
        metadata.insert(SSEC_KEY_MD5_HEADER.to_string(), "md5-value".to_string());
        metadata.insert(SSEC_ORIGINAL_SIZE_HEADER.to_string(), "1024".to_string());
        metadata.insert(INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "iv-direct".to_string());
        metadata.insert(MINIO_INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "iv-minio".to_string());
        metadata.insert(MINIO_INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(), "DAREv2-HMAC-SHA256".to_string());
        metadata.insert(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER.to_string(), "sealed".to_string());
        metadata.insert(MINIO_INTERNAL_ENCRYPTION_MULTIPART_HEADER.to_string(), "true".to_string());
        rustfs_utils::http::insert_str(
            &mut metadata,
            rustfs_utils::http::SUFFIX_REPLICATION_GENERATION,
            Uuid::from_u128(1).to_string(),
        );

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

        // Every stored SSE-C material key is remapped onto its transport name.
        assert_eq!(options.user_metadata.get(REPLICATION_SSEC_ALGORITHM_HEADER), Some(&"AES256".to_string()));
        assert_eq!(options.user_metadata.get(REPLICATION_SSEC_KEY_MD5_HEADER), Some(&"md5-value".to_string()));
        assert_eq!(
            options.user_metadata.get(REPLICATION_SSEC_ORIGINAL_SIZE_HEADER),
            Some(&"1024".to_string())
        );
        assert_eq!(
            options.user_metadata.get(REPLICATION_ENCRYPTION_IV_HEADER),
            Some(&"iv-direct".to_string())
        );
        assert_eq!(options.user_metadata.get(REPLICATION_SSE_IV_HEADER), Some(&"iv-minio".to_string()));
        assert_eq!(
            options.user_metadata.get(REPLICATION_SSE_SEAL_ALGORITHM_HEADER),
            Some(&"DAREv2-HMAC-SHA256".to_string())
        );
        assert_eq!(options.user_metadata.get(REPLICATION_SSE_SEALED_KEY_HEADER), Some(&"sealed".to_string()));
        assert_eq!(
            options.user_metadata.get(REPLICATION_ENCRYPTED_MULTIPART_HEADER),
            Some(&"true".to_string())
        );

        // The stored keys themselves and the SSE intent header must not leave
        // the source verbatim.
        assert!(!options.user_metadata.contains_key(AMZ_SERVER_SIDE_ENCRYPTION));
        assert!(!options.user_metadata.contains_key(SSEC_ALGORITHM_HEADER));
        assert!(!options.user_metadata.contains_key(INTERNAL_ENCRYPTION_IV_HEADER));
        assert!(
            options
                .user_metadata
                .keys()
                .all(|key| !key.contains(rustfs_utils::http::SUFFIX_REPLICATION_GENERATION)),
            "source-local mutation generation must never cross the replication wire"
        );
        assert!(
            !options
                .user_metadata
                .contains_key(MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER)
        );

        assert_eq!(options.content_type, "text/plain");
        assert_eq!(options.content_encoding, "gzip");
        assert_eq!(options.user_tags.get("env"), Some(&"prod".to_string()));
        assert_eq!(options.internal.source_version_id, Uuid::nil().to_string());
        assert_eq!(options.internal.source_etag, "0123456789abcdef0123456789abcdef");
        assert_eq!(options.internal.replication_status, ReplicationStatusType::Replica);
        assert!(options.internal.replication_request);
    }

    /// DeleteObjectTagging leaves the object tagless but stamps the
    /// tagging-timestamp internal key; the deletion's LWW timestamp must
    /// still be loaded (and therefore sent) so the replica can order the
    /// deletion against concurrent tag edits.
    #[test]
    fn replication_put_options_carry_tagging_timestamp_after_tag_deletion() {
        let mut metadata = std::collections::HashMap::new();
        rustfs_utils::http::insert_str(&mut metadata, SUFFIX_TAGGING_TIMESTAMP, "2026-01-02T03:04:05Z".to_string());

        let object_info = ObjectInfo {
            user_defined: Arc::new(metadata),
            user_tags: Arc::new(String::new()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            version_id: Some(Uuid::nil()),
            ..Default::default()
        };

        let (options, _) = replication_put_object_options("", &object_info).expect("build put options");

        assert!(options.user_tags.is_empty());
        assert_eq!(
            options.internal.tagging_timestamp,
            OffsetDateTime::parse("2026-01-02T03:04:05Z", &Rfc3339).expect("valid timestamp"),
            "the stored tagging timestamp must load independently of remaining tags"
        );

        // A tagless object without the stored key was never tagged: the epoch
        // default keeps the header unsent.
        let untagged = ObjectInfo {
            user_tags: Arc::new(String::new()),
            mod_time: Some(OffsetDateTime::from_unix_timestamp(1_700_000_000).expect("timestamp")),
            version_id: Some(Uuid::nil()),
            ..Default::default()
        };
        let (options, _) = replication_put_object_options("", &untagged).expect("build put options");
        assert_eq!(options.internal.tagging_timestamp, OffsetDateTime::UNIX_EPOCH);
    }

    #[test]
    fn replication_put_options_do_not_promote_legacy_user_timestamp_metadata() {
        let legacy_keys = [
            "x-rustfs-source-replication-tagging-timestamp",
            "x-rustfs-source-replication-retention-timestamp",
            "x-rustfs-source-replication-legalhold-timestamp",
            "x-minio-source-replication-tagging-timestamp",
            "x-minio-source-replication-retention-timestamp",
            "x-minio-source-replication-legalhold-timestamp",
        ];
        let object_info = ObjectInfo {
            user_defined: Arc::new(
                legacy_keys
                    .iter()
                    .map(|key| (key.to_string(), "2099-01-02T03:04:05Z".to_string()))
                    .collect(),
            ),
            ..Default::default()
        };

        let (options, _) = replication_put_object_options("", &object_info).expect("build put options");

        for legacy_key in legacy_keys {
            assert!(!options.user_metadata.contains_key(legacy_key));
            assert_eq!(
                options
                    .user_metadata
                    .get(&format!("x-amz-meta-{legacy_key}"))
                    .map(String::as_str),
                Some("2099-01-02T03:04:05Z")
            );
        }
        assert_eq!(options.internal.tagging_timestamp, OffsetDateTime::UNIX_EPOCH);
        assert_eq!(options.internal.retention_timestamp, OffsetDateTime::UNIX_EPOCH);
        assert_eq!(options.internal.legalhold_timestamp, OffsetDateTime::UNIX_EPOCH);
    }

    #[test]
    fn replication_put_options_carry_retention_timestamp_after_clear() {
        let mut metadata = HashMap::from([
            (AMZ_OBJECT_LOCK_MODE.to_string(), String::new()),
            (AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.to_string(), String::new()),
        ]);
        rustfs_utils::http::insert_str(&mut metadata, SUFFIX_OBJECTLOCK_RETENTION_TIMESTAMP, "2026-01-02T03:04:05Z".to_string());
        let object_info = ObjectInfo {
            user_defined: Arc::new(metadata),
            ..Default::default()
        };

        let (options, _) = replication_put_object_options("", &object_info).expect("retention clear must replicate");

        assert!(options.mode.is_none());
        assert_eq!(options.retain_until_date, OffsetDateTime::UNIX_EPOCH);
        assert_eq!(
            options.internal.retention_timestamp,
            OffsetDateTime::parse("2026-01-02T03:04:05Z", &Rfc3339).expect("valid timestamp")
        );
        let headers = options.header();
        assert!(!headers.contains_key(AMZ_OBJECT_LOCK_MODE));
        assert!(!headers.contains_key(AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE));
        assert_eq!(
            rustfs_utils::http::get_header(&headers, SUFFIX_SOURCE_REPLICATION_RETENTION_TIMESTAMP).as_deref(),
            Some("2026-01-02T03:04:05Z")
        );
    }

    #[test]
    fn replication_put_options_strip_encryption_metadata_from_plaintext_objects() {
        use rustfs_utils::http::object_encryption_keys::{INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER, SSEC_ORIGINAL_SIZE_HEADER};

        // Migration leftovers: original-size metadata is not an encryption
        // marker (older plaintext objects can retain it), so the object still
        // classifies as plaintext — but the keys must be stripped, never
        // forwarded as plain user metadata (backlog#1783 D2). The SSE-C
        // original-size key is also a transport-table source key, so this
        // doubles as the guard for the is_ssec gate: without SSE-C
        // classification it must be stripped, not remapped.
        let metadata = HashMap::from([
            ("x-user-meta".to_string(), "value".to_string()),
            (INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER.to_string(), "1024".to_string()),
            (SSEC_ORIGINAL_SIZE_HEADER.to_string(), "1024".to_string()),
        ]);
        let object_info = ObjectInfo {
            user_defined: Arc::new(metadata),
            ..Default::default()
        };

        let (options, _) = replication_put_object_options("", &object_info).expect("build put options");

        assert_eq!(options.user_metadata.get("x-user-meta"), Some(&"value".to_string()));
        assert!(!options.user_metadata.contains_key(INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER));
        assert!(!options.user_metadata.contains_key(SSEC_ORIGINAL_SIZE_HEADER));
        assert!(
            !options
                .user_metadata
                .keys()
                .any(|key| key.to_ascii_lowercase().starts_with("x-rustfs-replication-")),
            "non-SSE-C objects must never emit SSE replication transport keys"
        );
    }

    #[test]
    fn replication_put_options_fail_closed_on_sealed_material_without_sse_marker() {
        use rustfs_utils::http::object_encryption_keys::{
            INTERNAL_ENCRYPTION_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER,
        };

        // Sealed material without a recognizable SSE marker (MinIO-written
        // objects, or corrupted metadata) must fail closed instead of
        // replicating ciphertext as a plaintext object.
        for sealed_key in [
            INTERNAL_ENCRYPTION_KEY_HEADER,
            MINIO_INTERNAL_ENCRYPTION_SSEC_SEALED_KEY_HEADER,
        ] {
            let object_info = ObjectInfo {
                user_defined: Arc::new(HashMap::from([(sealed_key.to_string(), "sealed-envelope".to_string())])),
                ..Default::default()
            };

            let err = match replication_put_object_options("", &object_info) {
                Ok(_) => panic!("sealed material without an SSE marker must fail closed ({sealed_key})"),
                Err(err) => err,
            };
            assert!(err.to_string().contains(ERR_REPLICATION_ENCRYPTION_METADATA_UNSUPPORTED));
            assert!(!err.to_string().contains("sealed-envelope"));
        }
    }

    /// Pins the HeadObjectOutput field extraction feeding the crate-owned
    /// evidence judgment (the gate/evidence policy matrix itself is pinned in
    /// `rustfs-replication`'s object tests).
    #[test]
    fn ssec_passthrough_evidence_requires_customer_algorithm_echo() {
        let with_evidence = HeadObjectOutput::builder().sse_customer_algorithm("AES256").build();
        assert!(ssec_passthrough_evidence_present(&with_evidence));

        let empty_algorithm = HeadObjectOutput::builder().sse_customer_algorithm("").build();
        assert!(
            !ssec_passthrough_evidence_present(&empty_algorithm),
            "an empty echo is not evidence of preserved SSE-C material"
        );

        let without_evidence = HeadObjectOutput::builder().e_tag("\"abc\"").content_length(8).build();
        assert!(
            !ssec_passthrough_evidence_present(&without_evidence),
            "a plain HEAD response must classify the target as having dropped the material"
        );
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

    #[test]
    fn replication_source_encryption_classification_is_explicit_and_fail_closed() {
        assert_eq!(
            classify_replication_source_encryption(&HashMap::new()),
            ReplicationSourceEncryption::Plaintext
        );
        assert_eq!(
            classify_replication_source_encryption(&HashMap::from([(
                "x-amz-server-side-encryption".to_string(),
                "AES256".to_string()
            )])),
            ReplicationSourceEncryption::SseS3
        );
        assert_eq!(
            classify_replication_source_encryption(&HashMap::from([(
                "x-amz-server-side-encryption".to_string(),
                "AWS:KMS".to_string()
            )])),
            ReplicationSourceEncryption::SseKms
        );
        assert_eq!(
            classify_replication_source_encryption(&HashMap::from([(SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string())])),
            ReplicationSourceEncryption::SseC
        );
        // Real stored SSE-C objects carry the AES256 SSE marker alongside the
        // customer algorithm (encryption_material_to_metadata writes both).
        assert_eq!(
            classify_replication_source_encryption(&HashMap::from([
                (SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string()),
                ("x-amz-server-side-encryption".to_string(), "AES256".to_string()),
            ])),
            ReplicationSourceEncryption::SseC
        );
        // SSE-C material mixed with KMS evidence stays unsupported.
        assert_eq!(
            classify_replication_source_encryption(&HashMap::from([
                (SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string()),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID.to_string(), "key-1".to_string()),
            ])),
            ReplicationSourceEncryption::Unsupported
        );
        assert_eq!(
            classify_replication_source_encryption(&HashMap::from([
                (SSEC_ALGORITHM_HEADER.to_string(), "AES256".to_string()),
                ("x-amz-server-side-encryption".to_string(), "aws:kms".to_string()),
            ])),
            ReplicationSourceEncryption::Unsupported
        );
        assert_eq!(
            classify_replication_source_encryption(&HashMap::from([(
                "x-amz-server-side-encryption".to_string(),
                "unsupported-algorithm".to_string(),
            )])),
            ReplicationSourceEncryption::Unsupported
        );
        assert_eq!(
            classify_replication_source_encryption(&HashMap::from([(
                AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT.to_string(),
                "opaque-context".to_string(),
            )])),
            ReplicationSourceEncryption::Unsupported
        );
    }

    #[test]
    fn replication_put_options_sends_sse_s3_intent_without_source_material() {
        use rustfs_utils::http::object_encryption_keys::{
            INTERNAL_ENCRYPTION_ALGORITHM_HEADER, INTERNAL_ENCRYPTION_IV_HEADER, INTERNAL_ENCRYPTION_KEY_HEADER,
            INTERNAL_ENCRYPTION_KEY_ID_HEADER, INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER,
        };

        // The stored shape of a managed SSE-S3 object per
        // encryption_material_to_metadata: SSE marker plus envelope material.
        let object_info = ObjectInfo {
            user_defined: Arc::new(HashMap::from([
                (AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "AES256".to_string()),
                (INTERNAL_ENCRYPTION_KEY_ID_HEADER.to_string(), "default".to_string()),
                (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "sealed-envelope".to_string()),
                (INTERNAL_ENCRYPTION_IV_HEADER.to_string(), "iv".to_string()),
                (INTERNAL_ENCRYPTION_ALGORITHM_HEADER.to_string(), "AES256-GCM".to_string()),
                (INTERNAL_ENCRYPTION_ORIGINAL_SIZE_HEADER.to_string(), "1024".to_string()),
                ("x-user-meta".to_string(), "value".to_string()),
            ])),
            ..Default::default()
        };

        let (options, _) = replication_put_object_options("", &object_info).expect("managed SSE-S3 must build put options");

        assert_eq!(options.user_metadata.get(AMZ_SERVER_SIDE_ENCRYPTION), Some(&"AES256".to_string()));
        assert_eq!(options.user_metadata.get("x-user-meta"), Some(&"value".to_string()));
        // No envelope material and no key id may leave the source.
        assert!(!options.user_metadata.contains_key(INTERNAL_ENCRYPTION_KEY_HEADER));
        assert!(!options.user_metadata.contains_key(INTERNAL_ENCRYPTION_KEY_ID_HEADER));
        assert!(!options.user_metadata.contains_key(INTERNAL_ENCRYPTION_IV_HEADER));
        assert!(
            !options.user_metadata.values().any(|value| value.contains("sealed-envelope")),
            "source envelope material must never leave the source site"
        );
    }

    #[test]
    fn replication_put_options_sends_sse_kms_intent_without_source_key_id() {
        use rustfs_utils::http::object_encryption_keys::{
            INTERNAL_ENCRYPTION_KEY_HEADER, MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER,
        };

        let object_info = ObjectInfo {
            user_defined: Arc::new(HashMap::from([
                (AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "aws:kms".to_string()),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID.to_string(), "source-key-1".to_string()),
                (INTERNAL_ENCRYPTION_KEY_HEADER.to_string(), "sealed-envelope".to_string()),
                (MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER.to_string(), "ctx".to_string()),
            ])),
            ..Default::default()
        };

        let (options, _) = replication_put_object_options("", &object_info).expect("managed SSE-KMS must build put options");

        // Intent only: the target encrypts with its own default KMS key.
        assert_eq!(options.user_metadata.get(AMZ_SERVER_SIDE_ENCRYPTION), Some(&"aws:kms".to_string()));
        assert!(!options.user_metadata.contains_key(AMZ_SERVER_SIDE_ENCRYPTION_KMS_ID));
        assert!(!options.user_metadata.contains_key(INTERNAL_ENCRYPTION_KEY_HEADER));
        assert!(
            !options
                .user_metadata
                .contains_key(MINIO_INTERNAL_ENCRYPTION_KMS_CONTEXT_HEADER)
        );
        assert!(
            !options
                .user_metadata
                .values()
                .any(|value| value.contains("sealed-envelope") || value.contains("source-key-1")),
            "source KMS identifiers and envelopes must never leave the source site"
        );
    }

    #[test]
    fn replication_put_options_rejects_unknown_encryption_without_echoing_metadata() {
        let secret_like_value = "opaque-context-that-must-not-be-logged";
        let object_info = ObjectInfo {
            user_defined: Arc::new(HashMap::from([
                (AMZ_SERVER_SIDE_ENCRYPTION.to_string(), "unsupported-algorithm".to_string()),
                (AMZ_SERVER_SIDE_ENCRYPTION_KMS_CONTEXT.to_string(), secret_like_value.to_string()),
            ])),
            ..Default::default()
        };

        let err = match replication_put_object_options("", &object_info) {
            Ok(_) => panic!("unknown encryption must fail closed"),
            Err(err) => err,
        };
        assert!(err.to_string().contains(ERR_REPLICATION_ENCRYPTION_METADATA_UNSUPPORTED));
        assert!(!err.to_string().contains(secret_like_value));
    }

    // T3 (#1264): the outbound replication path forwards a stored object checksum into
    // user_metadata via decrypt_checksums, which is algorithm-agnostic. This locks that
    // the AWS 2026-04 additional algorithms (XXHash3/64/128, SHA-512, MD5) are forwarded
    // identically to the classic five — i.e. replication treats the new algorithms
    // consistently, with no new-algorithm-specific gap on the outbound side.
    #[test]
    fn replication_put_object_options_forwards_new_algorithm_checksums_like_classic() {
        use rustfs_rio::{Checksum, ChecksumType};

        let payload = b"replication checksum consistency payload";
        let cases = [
            // classic five (baseline)
            ("CRC32", ChecksumType::CRC32),
            ("SHA256", ChecksumType::SHA256),
            // AWS 2026-04 additional algorithms
            ("XXHASH3", ChecksumType::XXHASH3),
            ("XXHASH64", ChecksumType::XXHASH64),
            ("XXHASH128", ChecksumType::XXHASH128),
            ("SHA512", ChecksumType::SHA512),
            ("MD5", ChecksumType::MD5),
        ];

        for (name, ty) in cases {
            let checksum = Checksum::new_from_data(ty, payload).expect("compute checksum");
            let object_info = ObjectInfo {
                checksum: Some(checksum.to_bytes(&[])),
                ..Default::default()
            };

            let (opts, is_multipart) = replication_put_object_options("", &object_info).expect("build replication put options");

            assert!(!is_multipart, "{name}: a single-part checksum record must keep the single-PUT route");
            let header = ty.key().expect("every forwarded algorithm has an x-amz-checksum header");
            assert_eq!(
                opts.user_metadata.get(header),
                Some(&checksum.encoded),
                "replication must forward the {name} checksum as the {header} header"
            );
            assert!(
                !opts.user_metadata.contains_key(name),
                "{name}: the bare algorithm name would leave as x-amz-meta user metadata"
            );
        }
    }

    /// The object-level record of a multipart upload (composite or full-object)
    /// must not become a PutObject checksum header: the replica is rebuilt
    /// through CreateMultipartUpload/UploadPart, and a checksum announced there
    /// that the parts do not carry would be rejected by the target.
    #[test]
    fn replication_put_object_options_keeps_multipart_checksum_records_off_the_wire() {
        let mut composite_type = rustfs_rio::ChecksumType::from_string("crc32");
        composite_type
            .merge(rustfs_rio::ChecksumType::MULTIPART)
            .merge(rustfs_rio::ChecksumType::INCLUDES_MULTIPART);
        let mut combined = Vec::new();
        for part in [b"part-one".as_slice(), b"part-two".as_slice()] {
            let part_checksum =
                rustfs_rio::Checksum::new_from_data(rustfs_rio::ChecksumType::from_string("crc32"), part).expect("part checksum");
            combined.extend_from_slice(part_checksum.raw.as_slice());
        }
        let composite = rustfs_rio::Checksum::new_from_data(composite_type, &combined)
            .expect("composite checksum")
            .to_bytes(&combined);

        for (label, checksum, etag) in [
            ("composite", composite, "0123456789abcdef0123456789abcdef-2"),
            (
                "full-object",
                full_object_multipart_checksum_record(),
                "0123456789abcdef0123456789abcdef-3",
            ),
        ] {
            let object_info = ObjectInfo {
                etag: Some(etag.to_string()),
                checksum: Some(checksum),
                ..Default::default()
            };
            let (opts, is_multipart) = replication_put_object_options("", &object_info).expect("build replication put options");
            assert!(is_multipart, "{label}: a multipart object must keep the multipart route");
            assert!(
                opts.user_metadata
                    .keys()
                    .all(|key| !key.starts_with("x-amz-checksum-") && key != "CRC32"),
                "{label}: no object-level checksum may reach the target's CreateMultipartUpload: {:?}",
                opts.user_metadata
            );
        }
    }
}
