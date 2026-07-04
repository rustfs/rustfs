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

use super::*;
use crate::storage_api_contracts::{
    list::VersionMarker,
    object::{
        HTTPPreconditions, ObjectLockRetentionOptions, ObjectPreconditionError, ObjectPreconditionPart, ObjectPreconditionState,
    },
};

#[derive(Debug, Default, Clone)]
pub struct ObjectOptions {
    // Use the maximum parity (N/2), used when saving server configuration files
    pub max_parity: bool,
    pub mod_time: Option<OffsetDateTime>,
    pub part_number: Option<usize>,

    pub delete_prefix: bool,
    pub delete_prefix_object: bool,
    pub version_id: Option<String>,
    pub no_lock: bool,
    /// True when an upper layer already holds the object read lock before
    /// forwarding a no_lock read to the set layer.
    pub metadata_cache_safe: bool,

    pub versioned: bool,
    pub version_suspended: bool,
    pub incl_free_versions: bool,

    pub skip_decommissioned: bool,
    pub skip_rebalancing: bool,
    pub skip_free_version: bool,

    pub data_movement: bool,
    pub raw_data_movement_read: bool,
    pub src_pool_idx: usize,
    pub user_defined: HashMap<String, String>,
    pub preserve_etag: Option<String>,
    pub metadata_chg: bool,
    pub http_preconditions: Option<HTTPPreconditions>,

    pub delete_replication: Option<ReplicationState>,
    pub replication_request: bool,
    pub delete_marker: bool,

    pub transition: TransitionOptions,
    pub expiration: ExpirationOptions,
    pub lifecycle_audit_event: LcAuditEvent,

    pub eval_metadata: Option<HashMap<String, String>>,
    pub object_lock_retention: Option<ObjectLockRetentionOptions>,

    pub want_checksum: Option<Checksum>,
    pub skip_verify_bitrot: bool,
    pub capacity_scope_token: Option<Uuid>,
}

impl ObjectOptions {
    pub fn set_delete_replication_state(&mut self, dsc: ReplicateDecision) {
        let mut rs = ReplicationState {
            replicate_decision_str: dsc.to_string(),
            ..Default::default()
        };
        if self.version_id.is_none() {
            rs.replication_status_internal = dsc.pending_status();
            rs.targets = replication_statuses_map(rs.replication_status_internal.as_deref().unwrap_or_default());
        } else {
            rs.version_purge_status_internal = dsc.pending_status();
            rs.purge_targets = version_purge_statuses_map(rs.version_purge_status_internal.as_deref().unwrap_or_default());
        }

        self.delete_replication = Some(rs)
    }

    pub fn set_replica_status(&mut self, status: ReplicationStatusType) {
        if let Some(rs) = self.delete_replication.as_mut() {
            rs.replica_status = status;
            rs.replica_timestamp = Some(OffsetDateTime::now_utc());
        } else {
            self.delete_replication = Some(ReplicationState {
                replica_status: status,
                replica_timestamp: Some(OffsetDateTime::now_utc()),
                ..Default::default()
            });
        }
    }

    pub fn version_purge_status(&self) -> VersionPurgeStatusType {
        self.delete_replication
            .as_ref()
            .map(|v| v.composite_version_purge_status())
            .unwrap_or(VersionPurgeStatusType::Empty)
    }

    pub fn delete_marker_replication_status(&self) -> ReplicationStatusType {
        self.delete_replication
            .as_ref()
            .map(|v| v.composite_replication_status())
            .unwrap_or(ReplicationStatusType::Empty)
    }

    pub fn put_replication_state(&self) -> ReplicationState {
        if self
            .delete_replication
            .as_ref()
            .is_some_and(|state| !state.replica_status.is_empty())
        {
            return self.delete_replication.clone().unwrap_or_default();
        }

        let rs = match rustfs_utils::http::get_str(&self.user_defined, rustfs_utils::http::SUFFIX_REPLICATION_STATUS) {
            Some(v) => v,
            None => return ReplicationState::default(),
        };

        ReplicationState {
            replication_status_internal: Some(rs.to_string()),
            targets: replication_statuses_map(rs.as_str()),
            ..Default::default()
        }
    }

    pub fn precondition_check(&self, obj_info: &ObjectInfo) -> Result<()> {
        let requested_part = self.part_number.and_then(|part_number| {
            if part_number > 1 && !obj_info.parts.is_empty() {
                Some(ObjectPreconditionPart {
                    number: part_number,
                    exists: obj_info.parts.iter().any(|pi| pi.number == part_number),
                })
            } else {
                None
            }
        });
        let state = ObjectPreconditionState {
            etag: obj_info.etag.as_deref(),
            mod_time: obj_info.mod_time,
            requested_part,
        };

        state.check(self.http_preconditions.as_ref()).map_err(|err| match err {
            ObjectPreconditionError::InvalidPartNumber(part_number) => Error::InvalidPartNumber(part_number),
            ObjectPreconditionError::NotModified => Error::NotModified,
            ObjectPreconditionError::PreconditionFailed => Error::PreconditionFailed,
        })
    }
}

#[derive(Debug, Default)]
pub struct ObjectInfo {
    pub bucket: String,
    pub name: String,
    pub storage_class: Option<String>,
    pub mod_time: Option<OffsetDateTime>,
    pub size: i64,
    // Actual size is the real size of the object uploaded by client.
    pub actual_size: i64,
    pub is_dir: bool,
    pub user_defined: Arc<HashMap<String, String>>,
    pub parity_blocks: usize,
    pub data_blocks: usize,
    pub version_id: Option<Uuid>,
    pub delete_marker: bool,
    pub transitioned_object: TransitionedObject,
    pub restore_ongoing: bool,
    pub restore_expires: Option<OffsetDateTime>,
    pub user_tags: Arc<String>,
    pub parts: Arc<Vec<ObjectPartInfo>>,
    pub is_latest: bool,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub expires: Option<OffsetDateTime>,
    pub num_versions: usize,
    pub successor_mod_time: Option<OffsetDateTime>,
    pub put_object_reader: Option<PutObjReader>,
    pub etag: Option<String>,
    pub inlined: bool,
    pub metadata_only: bool,
    pub version_only: bool,
    pub replication_status_internal: Option<String>,
    pub replication_status: ReplicationStatusType,
    pub version_purge_status_internal: Option<String>,
    pub version_purge_status: VersionPurgeStatusType,
    pub replication_decision: String,
    pub checksum: Option<Bytes>,
}

impl Clone for ObjectInfo {
    fn clone(&self) -> Self {
        Self {
            bucket: self.bucket.clone(),
            name: self.name.clone(),
            storage_class: self.storage_class.clone(),
            mod_time: self.mod_time,
            size: self.size,
            actual_size: self.actual_size,
            is_dir: self.is_dir,
            user_defined: self.user_defined.clone(),
            parity_blocks: self.parity_blocks,
            data_blocks: self.data_blocks,
            version_id: self.version_id,
            delete_marker: self.delete_marker,
            transitioned_object: self.transitioned_object.clone(),
            restore_ongoing: self.restore_ongoing,
            restore_expires: self.restore_expires,
            user_tags: self.user_tags.clone(),
            parts: self.parts.clone(),
            is_latest: self.is_latest,
            content_type: self.content_type.clone(),
            content_encoding: self.content_encoding.clone(),
            num_versions: self.num_versions,
            successor_mod_time: self.successor_mod_time,
            put_object_reader: None, // reader can not clone
            etag: self.etag.clone(),
            inlined: self.inlined,
            metadata_only: self.metadata_only,
            version_only: self.version_only,
            replication_status_internal: self.replication_status_internal.clone(),
            replication_status: self.replication_status.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            version_purge_status: self.version_purge_status.clone(),
            replication_decision: self.replication_decision.clone(),
            checksum: self.checksum.clone(),
            expires: self.expires,
        }
    }
}

impl ObjectInfo {
    pub fn is_compressed(&self) -> bool {
        rustfs_utils::http::contains_key_str(&self.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION)
    }

    pub fn is_compressed_ok(&self) -> Result<(CompressionAlgorithm, bool)> {
        let (algorithm, _, compressed) = self.compression_read_plan()?;
        Ok((algorithm, compressed))
    }

    pub fn compression_read_plan(&self) -> Result<(CompressionAlgorithm, crate::io_support::rio::ReadCompressionBackend, bool)> {
        let scheme = rustfs_utils::http::get_str(&self.user_defined, rustfs_utils::http::SUFFIX_COMPRESSION);

        if let Some(scheme) = scheme {
            let (algorithm, backend) = crate::io_support::rio::compression_scheme_to_read_plan(&scheme)?;
            Ok((algorithm, backend, true))
        } else {
            Ok((CompressionAlgorithm::None, crate::io_support::rio::ReadCompressionBackend::Legacy, false))
        }
    }

    pub fn is_multipart(&self) -> bool {
        self.etag.as_ref().is_some_and(|v| v.len() != 32)
    }

    pub fn is_encrypted(&self) -> bool {
        // Corresponding to the logic in rustfs/src/sse.rs/encryption_material_to_metadata function
        use rustfs_utils::http::{SSEC_ALGORITHM_HEADER, SSEC_KEY_HEADER, SSEC_KEY_MD5_HEADER};

        self.user_defined.keys().any(|key| {
            let lower = key.to_ascii_lowercase();
            lower.starts_with("x-minio-encryption-")
                || lower.starts_with("x-minio-internal-server-side-encryption-")
                || matches!(
                    lower.as_str(),
                    "x-minio-internal-encrypted-multipart"
                        | "x-rustfs-encryption-key"
                        | "x-rustfs-encryption-algorithm"
                        | "x-rustfs-encryption-iv"
                        | "x-rustfs-encryption-key-id"
                        | "x-rustfs-encryption-context"
                        | "x-rustfs-encryption-tag"
                        | "x-amz-server-side-encryption-aws-kms-key-id"
                        | SSEC_ALGORITHM_HEADER
                        | SSEC_KEY_HEADER
                        | SSEC_KEY_MD5_HEADER
                        | "x-amz-server-side-encryption"
                )
        })
    }

    /// Maximum inline size for non-versioned objects (128 KiB).
    /// Matches `DEFAULT_INLINE_BLOCK` in `storageclass.rs`.
    pub const INLINE_MAX_SIZE: i64 = 128 * 1024;

    /// Maximum inline size for versioned objects (16 KiB).
    /// Matches `DEFAULT_INLINE_BLOCK / 8` in `storageclass.rs`.
    pub const INLINE_MAX_SIZE_VERSIONED: i64 = 16 * 1024;

    /// Returns `true` when this object qualifies for the inline data fast path.
    ///
    /// The inline fast path decodes erasure-coded data entirely in memory,
    /// bypassing disk I/O, duplex pipes, and the disk-read semaphore.
    ///
    /// The `inlined` flag is the primary signal — it is set during PUT by
    /// `storage_class_should_inline()` which already applies the correct
    /// version-aware threshold (128 KiB non-versioned, 16 KiB versioned).
    /// The size check below is a safety net using the same thresholds.
    ///
    /// Additional conditions:
    /// - Single part
    /// - Not encrypted
    /// - Not compressed
    /// - Not transitioned to remote tier
    pub fn is_inline_fast_path_eligible(&self) -> bool {
        if !self.inlined {
            return false;
        }
        // Apply the same version-aware threshold as PUT (storageclass.rs).
        let max_size = if self.version_id.is_some() {
            Self::INLINE_MAX_SIZE_VERSIONED
        } else {
            Self::INLINE_MAX_SIZE
        };
        self.parts.len() == 1
            && self.size <= max_size
            && !self.is_encrypted()
            && !self.is_compressed()
            && self.transitioned_object.tier.is_empty()
    }

    pub fn encryption_original_size(&self) -> std::io::Result<Option<i64>> {
        let actual_size = rustfs_utils::http::get_str(&self.user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE);
        if let Some(size_str) = self
            .user_defined
            .get("x-rustfs-encryption-original-size")
            .map(String::as_str)
            .or_else(|| {
                self.user_defined
                    .get("x-amz-server-side-encryption-customer-original-size")
                    .map(String::as_str)
            })
            .or(actual_size.as_deref())
            && !size_str.is_empty()
        {
            let size = size_str
                .parse::<i64>()
                .map_err(|e| std::io::Error::other(format!("Failed to parse encryption original size: {e}")))?;
            return Ok(Some(size));
        }

        Ok(None)
    }

    pub fn decrypted_size(&self) -> std::io::Result<i64> {
        Ok(self.encryption_original_size()?.unwrap_or(self.size))
    }

    pub fn get_actual_size(&self) -> std::io::Result<i64> {
        if self.actual_size > 0 {
            return Ok(self.actual_size);
        }

        if self.is_compressed() {
            if let Some(size_str) = rustfs_utils::http::get_str(&self.user_defined, rustfs_utils::http::SUFFIX_ACTUAL_SIZE)
                && !size_str.is_empty()
            {
                // Todo: deal with error
                let size = size_str.parse::<i64>().map_err(|e| std::io::Error::other(e.to_string()))?;
                return Ok(size);
            }
            let mut actual_size = 0;
            self.parts.iter().for_each(|part| {
                actual_size += part.actual_size;
            });
            if actual_size == 0 && actual_size != self.size {
                return Err(std::io::Error::other(format!("invalid decompressed size {} {}", actual_size, self.size)));
            }

            return Ok(actual_size);
        }

        // Check if object is encrypted
        // Managed SSE stores original size in x-rustfs-encryption-original-size metadata
        // SSE-C stores original size in x-amz-server-side-encryption-customer-original-size
        if let Some(size) = self.encryption_original_size()? {
            return Ok(size);
        }

        Ok(self.size)
    }

    pub fn from_file_info(fi: &FileInfo, bucket: &str, object: &str, versioned: bool) -> ObjectInfo {
        let name = decode_dir_object(object);

        let mut version_id = fi.version_id;

        if versioned && version_id.is_none() {
            version_id = Some(Uuid::nil())
        }

        // etag
        let (content_type, content_encoding, etag) = {
            let content_type = fi.metadata.get("content-type").cloned();
            let content_encoding = fi.metadata.get("content-encoding").cloned();
            let etag = fi.metadata.get("etag").cloned();

            (content_type, content_encoding, etag)
        };

        // tags
        let user_tags: Arc<String> = fi
            .metadata
            .get(AMZ_OBJECT_TAGGING)
            .map(|s| Arc::new(s.clone()))
            .unwrap_or_default();

        let inlined = fi.inline_data();

        // Parse expires from metadata (HTTP date format RFC 7231 or ISO 8601)
        let expires = fi.metadata.get("expires").and_then(|s| {
            // Try parsing as ISO 8601 first
            OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT)
                .or_else(|_| {
                    // Try RFC 2822 format
                    OffsetDateTime::parse(s, &time::format_description::well_known::Rfc2822)
                })
                .or_else(|_| {
                    // Try RFC 3339 format
                    OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
                })
                .ok()
        });

        let replication_status_internal = fi
            .replication_state_internal
            .as_ref()
            .and_then(|v| v.replication_status_internal.clone());
        let version_purge_status_internal = fi
            .replication_state_internal
            .as_ref()
            .and_then(|v| v.version_purge_status_internal.clone());
        let replication_decision = fi
            .replication_state_internal
            .as_ref()
            .map(|v| v.replicate_decision_str.clone())
            .unwrap_or_default();

        let mut replication_status = replication_status_from_filemeta(fi.replication_status());
        if replication_status.is_empty()
            && let Some(status) = fi.metadata.get(AMZ_BUCKET_REPLICATION_STATUS).cloned()
            && status == ReplicationStatusType::Replica.as_str()
        {
            replication_status = ReplicationStatusType::Replica;
        }

        let version_purge_status = version_purge_status_from_filemeta(fi.version_purge_status());

        let transitioned_object = TransitionedObject {
            name: fi.transitioned_objname.clone(),
            version_id: if let Some(transition_version_id) = fi.transition_version_id {
                transition_version_id.to_string()
            } else {
                "".to_string()
            },
            status: fi.transition_status.clone(),
            free_version: fi.tier_free_version(),
            tier: fi.transition_tier.clone(),
        };

        let metadata = {
            let mut v = fi.metadata.clone();
            clean_metadata(&mut v);
            v
        };

        // Extract storage class from metadata, default to STANDARD if not found
        let storage_class = if !fi.transition_tier.is_empty() {
            Some(fi.transition_tier.clone())
        } else {
            fi.metadata
                .get(AMZ_STORAGE_CLASS)
                .cloned()
                .or_else(|| Some(storageclass::STANDARD.to_string()))
        };

        let mut restore_ongoing = false;
        let mut restore_expires = None;
        if let Some(restore_status) = fi.metadata.get(AMZ_RESTORE).cloned()
            && let Ok(restore_status) = parse_restore_obj_status(&restore_status)
        {
            restore_ongoing = restore_status.on_going();
            restore_expires = restore_status.expiry();
        }

        // Convert parts from rustfs_filemeta::ObjectPartInfo to object_api::ObjectPartInfo
        let parts = fi
            .parts
            .iter()
            .map(|part| ObjectPartInfo {
                etag: part.etag.clone(),
                index: part.index.clone(),
                size: part.size,
                actual_size: part.actual_size,
                mod_time: part.mod_time,
                checksums: part.checksums.clone(),
                number: part.number,
                error: part.error.clone(),
            })
            .collect::<Vec<_>>();

        // TODO: part checksums

        ObjectInfo {
            bucket: bucket.to_string(),
            name,
            is_dir: object.starts_with('/'),
            parity_blocks: fi.erasure.parity_blocks,
            data_blocks: fi.erasure.data_blocks,
            version_id,
            delete_marker: fi.deleted,
            mod_time: fi.mod_time,
            size: fi.size,
            parts: Arc::new(parts),
            is_latest: fi.is_latest,
            user_tags,
            content_type,
            content_encoding,
            expires,
            num_versions: fi.num_versions,
            successor_mod_time: fi.successor_mod_time,
            etag,
            inlined,
            user_defined: Arc::new(metadata),
            transitioned_object,
            checksum: fi.checksum.clone(),
            storage_class,
            restore_ongoing,
            restore_expires,
            replication_status_internal,
            replication_status,
            version_purge_status_internal,
            version_purge_status,
            replication_decision,
            ..Default::default()
        }
    }

    pub async fn from_meta_cache_entries_sorted_versions(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
        after_version_marker: Option<VersionMarker>,
    ) -> Vec<ObjectInfo> {
        let vcfg = get_versioning_config(bucket).await.ok();
        let mut objects = Vec::with_capacity(entries.entries().len());
        let mut prev_prefix = "";
        let mut after_version_marker = after_version_marker;
        for entry in entries.entries() {
            if entry.is_object() {
                if let Some(delimiter) = &delimiter {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    if let Some(idx) = remaining.find(delimiter.as_str()) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                        continue;
                    }
                }

                let file_infos = match entry.file_info_versions(bucket) {
                    Ok(res) => res,
                    Err(err) => {
                        warn!("file_info_versions err {:?}", err);
                        continue;
                    }
                };

                let versions = if let Some(marker) = after_version_marker.take() {
                    versions_after_marker(&file_infos, marker)
                } else {
                    &file_infos.versions
                };

                for fi in versions.iter() {
                    if !fi.version_purge_status().is_empty() {
                        continue;
                    }

                    let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                    objects.push(ObjectInfo::from_file_info(fi, bucket, &entry.name, versioned));
                }
                continue;
            }

            if entry.is_dir()
                && let Some(delimiter) = &delimiter
                && let Some(idx) = {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    remaining.find(delimiter.as_str())
                }
            {
                let idx = prefix.len() + idx + delimiter.len();
                if let Some(curr_prefix) = entry.name.get(0..idx) {
                    if curr_prefix == prev_prefix {
                        continue;
                    }

                    prev_prefix = curr_prefix;

                    objects.push(ObjectInfo {
                        is_dir: true,
                        bucket: bucket.to_owned(),
                        name: curr_prefix.to_owned(),
                        ..Default::default()
                    });
                }
            }
        }

        objects
    }

    pub async fn from_meta_cache_entries_sorted_infos(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
    ) -> Vec<ObjectInfo> {
        let vcfg = get_versioning_config(bucket).await.ok();
        let mut objects = Vec::with_capacity(entries.entries().len());
        let mut prev_prefix = "";
        for entry in entries.entries() {
            if entry.is_object() {
                if let Some(delimiter) = &delimiter {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    if let Some(idx) = remaining.find(delimiter.as_str()) {
                        let idx = prefix.len() + idx + delimiter.len();
                        if let Some(curr_prefix) = entry.name.get(0..idx) {
                            if curr_prefix == prev_prefix {
                                continue;
                            }

                            prev_prefix = curr_prefix;

                            objects.push(ObjectInfo {
                                is_dir: true,
                                bucket: bucket.to_owned(),
                                name: curr_prefix.to_owned(),
                                ..Default::default()
                            });
                        }
                        continue;
                    }
                }

                let fi = match entry.to_fileinfo(bucket) {
                    Ok(res) => res,
                    Err(err) => {
                        warn!("file_info_versions err {:?}", err);
                        continue;
                    }
                };

                // TODO:VersionPurgeStatus
                let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                objects.push(ObjectInfo::from_file_info(&fi, bucket, &entry.name, versioned));

                continue;
            }

            if entry.is_dir()
                && let Some(delimiter) = &delimiter
                && let Some(idx) = {
                    let remaining = if entry.name.starts_with(prefix) {
                        &entry.name[prefix.len()..]
                    } else {
                        entry.name.as_str()
                    };
                    remaining.find(delimiter.as_str())
                }
            {
                let idx = prefix.len() + idx + delimiter.len();
                if let Some(curr_prefix) = entry.name.get(0..idx) {
                    if curr_prefix == prev_prefix {
                        continue;
                    }

                    prev_prefix = curr_prefix;

                    objects.push(ObjectInfo {
                        is_dir: true,
                        bucket: bucket.to_owned(),
                        name: curr_prefix.to_owned(),
                        ..Default::default()
                    });
                }
            }
        }

        objects
    }

    pub fn replication_state(&self) -> ReplicationState {
        ReplicationState {
            replication_status_internal: self.replication_status_internal.clone(),
            version_purge_status_internal: self.version_purge_status_internal.clone(),
            replicate_decision_str: self.replication_decision.clone(),
            targets: replication_statuses_map(self.replication_status_internal.clone().unwrap_or_default().as_str()),
            purge_targets: version_purge_statuses_map(self.version_purge_status_internal.clone().unwrap_or_default().as_str()),
            reset_statuses_map: self
                .user_defined
                .iter()
                .filter_map(|(k, v)| {
                    rustfs_utils::http::internal_key_strip_suffix_prefix(
                        k,
                        rustfs_utils::http::SUFFIX_REPLICATION_RESET_ARN_PREFIX,
                    )
                    .map(|arn| (arn, v.clone()))
                })
                .collect(),
            ..Default::default()
        }
    }

    pub fn target_replication_status(&self, arn: &str) -> ReplicationStatusType {
        self.replication_status_internal
            .as_deref()
            .unwrap_or_default()
            .split(';')
            .find_map(|entry| {
                let (target_arn, status) = entry.split_once('=')?;
                (!target_arn.is_empty() && target_arn == arn).then(|| ReplicationStatusType::from(status))
            })
            .unwrap_or_default()
    }

    pub fn decrypt_checksums(&self, part: usize, _headers: &HeaderMap) -> Result<(HashMap<String, String>, bool)> {
        if part > 0
            && let Some(checksums) = self.parts.iter().find(|p| p.number == part).and_then(|p| p.checksums.clone())
        {
            return Ok((checksums, true));
        }

        // TODO: decrypt checksums

        if let Some(data) = &self.checksum {
            let (checksums, is_multipart) = rustfs_rio::read_checksums(data.as_ref(), 0);
            return Ok((checksums, is_multipart));
        }

        Ok((HashMap::new(), false))
    }
}

fn versions_after_marker(file_infos: &rustfs_filemeta::FileInfoVersions, marker: VersionMarker) -> &[FileInfo] {
    let marker_idx = match marker {
        VersionMarker::Null => file_infos.versions.iter().position(|version| version.version_id.is_none()),
        VersionMarker::Version(vid) => file_infos.find_version_index(vid),
    };

    marker_idx
        .map(|idx| &file_infos.versions[idx + 1..])
        .unwrap_or(&file_infos.versions)
}
#[cfg(test)]
mod tests {
    use super::*;
    use rustfs_filemeta::{FileInfo, FileMeta, MetaCacheEntry, TRANSITION_COMPLETE};

    #[test]
    fn versions_after_marker_handles_null_version_marker() {
        let first_version = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let last_version = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let file_infos = rustfs_filemeta::FileInfoVersions {
            versions: vec![
                FileInfo {
                    version_id: Some(first_version),
                    ..Default::default()
                },
                FileInfo {
                    version_id: None,
                    ..Default::default()
                },
                FileInfo {
                    version_id: Some(last_version),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let versions = versions_after_marker(&file_infos, VersionMarker::Null);

        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].version_id, Some(last_version));
    }

    #[test]
    fn put_replication_state_preserves_replica_status() {
        let opts = ObjectOptions {
            delete_replication: Some(ReplicationState {
                replica_status: ReplicationStatusType::Replica,
                ..Default::default()
            }),
            ..Default::default()
        };

        let state = opts.put_replication_state();

        assert_eq!(state.composite_replication_status(), ReplicationStatusType::Replica);
    }

    #[test]
    fn object_info_replication_helpers_parse_target_status_and_reset_headers() {
        let reset_key = rustfs_utils::http::internal_key_rustfs("replication-reset-arn:target-a");
        let object = ObjectInfo {
            replication_status_internal: Some("arn:target-a=COMPLETED;arn:target-b=FAILED;".to_string()),
            version_purge_status_internal: Some("arn:target-a=PENDING;".to_string()),
            user_defined: Arc::new(HashMap::from([(reset_key, "reset-id".to_string())])),
            ..Default::default()
        };

        let state = object.replication_state();

        assert_eq!(object.target_replication_status("arn:target-a"), ReplicationStatusType::Completed);
        assert_eq!(object.target_replication_status("arn:target-b"), ReplicationStatusType::Failed);
        assert_eq!(object.target_replication_status("arn:missing"), ReplicationStatusType::Empty);
        assert_eq!(state.targets.get("arn:target-b"), Some(&ReplicationStatusType::Failed));
        assert_eq!(state.purge_targets.get("arn:target-a"), Some(&VersionPurgeStatusType::Pending));
        assert_eq!(state.reset_statuses_map.get("arn:target-a"), Some(&"reset-id".to_string()));
    }

    #[test]
    fn versions_after_marker_handles_uuid_version_marker() {
        let first_version = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();
        let last_version = Uuid::parse_str("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee").unwrap();
        let file_infos = rustfs_filemeta::FileInfoVersions {
            versions: vec![
                FileInfo {
                    version_id: Some(first_version),
                    ..Default::default()
                },
                FileInfo {
                    version_id: None,
                    ..Default::default()
                },
                FileInfo {
                    version_id: Some(last_version),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let versions = versions_after_marker(&file_infos, VersionMarker::Version(first_version));

        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version_id, None);
        assert_eq!(versions[1].version_id, Some(last_version));
    }

    #[tokio::test]
    async fn versions_listing_applies_version_marker_only_to_first_entry() {
        let metadata = rustfs_filemeta::test_data::create_real_xlmeta().expect("test metadata should be valid");
        let entries = MetaCacheEntriesSorted {
            o: rustfs_filemeta::MetaCacheEntries(vec![
                Some(rustfs_filemeta::MetaCacheEntry {
                    name: "obj-a".to_owned(),
                    metadata: metadata.clone(),
                    ..Default::default()
                }),
                Some(rustfs_filemeta::MetaCacheEntry {
                    name: "obj-b".to_owned(),
                    metadata,
                    ..Default::default()
                }),
            ]),
            ..Default::default()
        };
        let marker_version = Uuid::parse_str("11111111-2222-3333-4444-555555555555").unwrap();

        let objects = ObjectInfo::from_meta_cache_entries_sorted_versions(
            &entries,
            "bucket",
            "",
            None,
            Some(VersionMarker::Version(marker_version)),
        )
        .await;

        let obj_a_count = objects.iter().filter(|object| object.name == "obj-a").count();
        let obj_b_count = objects.iter().filter(|object| object.name == "obj-b").count();

        assert_eq!(obj_a_count, 2);
        assert_eq!(obj_b_count, 3);
        assert_eq!(objects.len(), 5);
    }

    #[tokio::test]
    async fn versions_listing_excludes_tier_free_versions_from_delete_marker_count() {
        let object_version_id = Uuid::new_v4();
        let remote_version_id = Uuid::new_v4();
        let free_version_id = Uuid::new_v4();
        let delete_marker_id = Uuid::new_v4();
        let base_time = OffsetDateTime::now_utc();
        let mut fm = FileMeta::new();

        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(object_version_id),
            transition_status: TRANSITION_COMPLETE.to_string(),
            transitioned_objname: "remote/object".to_string(),
            transition_version_id: Some(remote_version_id),
            transition_tier: "WARM".to_string(),
            mod_time: Some(base_time),
            ..Default::default()
        })
        .expect("transitioned object version should be added");

        let mut delete_fi = FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(object_version_id),
            mod_time: Some(base_time),
            ..Default::default()
        };
        delete_fi.set_tier_free_version_id(&free_version_id.to_string());
        fm.delete_version(&delete_fi)
            .expect("transitioned delete should create a free-version record");

        fm.add_version(FileInfo {
            volume: "bucket".to_string(),
            name: "object".to_string(),
            version_id: Some(delete_marker_id),
            deleted: true,
            mod_time: Some(base_time + time::Duration::seconds(1)),
            ..Default::default()
        })
        .expect("delete marker should be added");

        let entries = MetaCacheEntriesSorted {
            o: rustfs_filemeta::MetaCacheEntries(vec![Some(MetaCacheEntry {
                name: "object".to_string(),
                metadata: fm.marshal_msg().expect("metadata should marshal"),
                ..Default::default()
            })]),
            ..Default::default()
        };

        let objects = ObjectInfo::from_meta_cache_entries_sorted_versions(&entries, "bucket", "", None, None).await;

        assert_eq!(objects.len(), 1);
        assert_eq!(objects[0].name, "object");
        assert!(objects[0].delete_marker);
        assert!(objects[0].is_latest);
        assert_eq!(objects[0].num_versions, 1);
    }

    #[test]
    fn get_actual_size_prefers_actual_size_field() {
        let info = ObjectInfo {
            size: 5,
            actual_size: 10,
            ..Default::default()
        };

        assert_eq!(info.get_actual_size().unwrap(), 10);
    }

    #[test]
    fn get_actual_size_uses_compressed_metadata_size() {
        let user_defined = {
            let mut map = HashMap::new();
            rustfs_utils::http::insert_str(&mut map, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
            rustfs_utils::http::insert_str(&mut map, rustfs_utils::http::SUFFIX_ACTUAL_SIZE, "42".to_string());
            map
        };

        let info = ObjectInfo {
            size: 100,
            actual_size: 0,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert_eq!(info.get_actual_size().unwrap(), 42);
    }

    #[test]
    fn get_actual_size_falls_back_to_encrypted_original_size_metadata() {
        let user_defined = {
            let mut map = HashMap::new();
            map.insert("x-amz-server-side-encryption-customer-original-size".to_string(), "77".to_string());
            map
        };

        let info = ObjectInfo {
            size: 100,
            actual_size: 0,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert_eq!(info.get_actual_size().unwrap(), 77);
    }

    #[test]
    fn precondition_check_ignores_empty_etag_conditions() {
        let opts = ObjectOptions {
            http_preconditions: Some(HTTPPreconditions {
                if_match: Some(String::new()),
                if_none_match: Some(" ".to_string()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let info = ObjectInfo {
            mod_time: Some(OffsetDateTime::now_utc()),
            etag: Some("\"abc\"".to_string()),
            ..Default::default()
        };

        assert!(opts.precondition_check(&info).is_ok());
    }

    #[test]
    fn from_file_info_preserves_replication_decision() {
        let fi = FileInfo {
            replication_state_internal: Some(crate::bucket::replication::replication_state_to_filemeta(&ReplicationState {
                replicate_decision_str: "arn=true;false;arn:replication::1:dest;rule-id".to_string(),
                ..Default::default()
            })),
            ..Default::default()
        };

        let info = ObjectInfo::from_file_info(&fi, "bucket", "object", true);

        assert_eq!(info.replication_decision, "arn=true;false;arn:replication::1:dest;rule-id");
    }

    #[test]
    fn get_actual_size_uses_compressed_parts_actual_size_when_metadata_missing() {
        let user_defined = {
            let mut map = HashMap::new();
            rustfs_utils::http::insert_str(&mut map, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
            map
        };

        let info = ObjectInfo {
            size: 12,
            actual_size: 0,
            user_defined: Arc::new(user_defined),
            parts: Arc::new(vec![
                ObjectPartInfo {
                    actual_size: 4,
                    ..Default::default()
                },
                ObjectPartInfo {
                    actual_size: 5,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        assert_eq!(info.get_actual_size().unwrap(), 9);
    }

    #[test]
    fn get_actual_size_returns_error_when_compressed_parts_missing_and_size_mismatch() {
        let user_defined = {
            let mut map = HashMap::new();
            rustfs_utils::http::insert_str(&mut map, rustfs_utils::http::SUFFIX_COMPRESSION, "zstd".to_string());
            map
        };

        let info = ObjectInfo {
            size: 12,
            actual_size: 0,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert!(info.get_actual_size().is_err());
    }

    #[test]
    fn is_encrypted_correct_for_old_version_fileinfo() {
        let mut user_defined: HashMap<String, String> = HashMap::new();

        let metadata = vec![
            ("content-type", "text/plain"),
            ("etag", "e4336b5de4e2180a53fe2e17d03abe4f-4"),
            ("x-minio-internal-actual-size", "67108864"),
            ("x-rustfs-encryption-original-size", "67108864"),
            ("x-rustfs-internal-actual-size", "67108864"),
        ];

        metadata.into_iter().for_each(|(key, value)| {
            user_defined.insert(key.to_string(), value.to_string());
        });

        let info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert!(!info.is_encrypted());
    }

    #[test]
    fn is_encrypted_returns_true_when_encryption_metadata_present() {
        let mut user_defined: HashMap<String, String> = HashMap::new();

        let metadata = vec![
            ("content-type", "text/plain"),
            ("etag", "f1c9645dbc14efddc7d8a322685f26eb"),
            ("x-amz-server-side-encryption", "AES256"),
            ("x-rustfs-encryption-algorithm", "AES256"),
            ("x-rustfs-encryption-iv", "Fb9moBlEBRE0D14F"),
            (
                "x-rustfs-encryption-key",
                "QUFBQUFBQUFBQUFBQUFBQTpZQk5sNnNJdmJHWWl3QmxZbCtsMTJlVlZCeXVoVml4UlV4b3JPbTNoRk5odUlYVnBPdlpXNWVyT0FTcklXMWJr",
            ),
            ("x-rustfs-encryption-key-id", "default"),
            ("x-rustfs-encryption-original-size", "10485760"),
        ];

        metadata.into_iter().for_each(|(key, value)| {
            user_defined.insert(key.to_string(), value.to_string());
        });

        let info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert!(info.is_encrypted());
    }

    #[test]
    fn is_encrypted_handles_case_insensitive_rustfs_metadata_keys() {
        let mut user_defined: HashMap<String, String> = HashMap::new();
        user_defined.insert("X-Rustfs-Encryption-Key".to_string(), "encrypted-key".to_string());

        let info = ObjectInfo {
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };

        assert!(info.is_encrypted());
    }

    #[test]
    fn objectinfo_clone_shares_arc_data_and_is_correct() {
        let mut ud = HashMap::new();
        ud.insert("content-type".to_string(), "application/octet-stream".to_string());
        ud.insert("x-custom-header".to_string(), "custom-value".to_string());

        let original = ObjectInfo {
            bucket: "test-bucket".to_string(),
            name: "test-object".to_string(),
            user_defined: Arc::new(ud),
            user_tags: Arc::new("env=prod&team=storage".to_string()),
            parts: Arc::new(vec![
                ObjectPartInfo {
                    number: 1,
                    size: 1024,
                    actual_size: 1024,
                    ..Default::default()
                },
                ObjectPartInfo {
                    number: 2,
                    size: 512,
                    actual_size: 512,
                    ..Default::default()
                },
            ]),
            size: 1536,
            etag: Some("abc123".to_string()),
            ..Default::default()
        };

        let cloned = original.clone();

        // Verify cloned values are correct
        assert_eq!(cloned.bucket, "test-bucket");
        assert_eq!(cloned.name, "test-object");
        assert_eq!(cloned.size, 1536);
        assert_eq!(cloned.etag, Some("abc123".to_string()));

        // Verify Arc fields share the same allocation
        assert!(Arc::ptr_eq(&original.user_defined, &cloned.user_defined));
        assert!(Arc::ptr_eq(&original.user_tags, &cloned.user_tags));
        assert!(Arc::ptr_eq(&original.parts, &cloned.parts));

        // Verify Arc-wrapped data is accessible through the clone
        assert_eq!(
            cloned.user_defined.get("content-type").map(String::as_str),
            Some("application/octet-stream")
        );
        assert_eq!(cloned.user_tags.as_str(), "env=prod&team=storage");
        assert_eq!(cloned.parts.len(), 2);
        assert_eq!(cloned.parts[0].number, 1);
        assert_eq!(cloned.parts[1].size, 512);

        // Verify default ObjectInfo clone also works
        let default_obj = ObjectInfo::default();
        let default_cloned = default_obj.clone();
        assert!(default_obj.user_defined.is_empty());
        assert!(default_cloned.user_defined.is_empty());
        assert!(default_cloned.user_tags.is_empty());
        assert!(default_cloned.parts.is_empty());
    }
}
