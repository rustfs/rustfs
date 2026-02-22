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

use crate::bucket::metadata_sys::get_versioning_config;
use crate::bucket::versioning::VersioningApi as _;
use crate::config::storageclass;
use crate::disk::DiskStore;
use crate::error::{Error, Result};
use crate::store_utils::clean_metadata;
use crate::{
    bucket::lifecycle::bucket_lifecycle_audit::LcAuditEvent,
    bucket::lifecycle::lifecycle::ExpirationOptions,
    bucket::lifecycle::{bucket_lifecycle_ops::TransitionedObject, lifecycle::TransitionOptions},
};
use bytes::Bytes;
use http::{HeaderMap, HeaderValue};
use rustfs_common::heal_channel::HealOpts;
use rustfs_filemeta::{
    FileInfo, MetaCacheEntriesSorted, ObjectPartInfo, REPLICATION_RESET, REPLICATION_STATUS, ReplicateDecision, ReplicationState,
    ReplicationStatusType, RestoreStatusOps as _, VersionPurgeStatusType, parse_restore_obj_status, replication_statuses_map,
    version_purge_statuses_map,
};
use rustfs_lock::NamespaceLockWrapper;
use rustfs_madmin::heal_commands::HealResultItem;
use rustfs_rio::Checksum;
use rustfs_rio::{DecompressReader, HashReader, LimitReader, WarpReader};
use rustfs_utils::CompressionAlgorithm;
use rustfs_utils::http::headers::{AMZ_OBJECT_TAGGING, RESERVED_METADATA_PREFIX_LOWER};
use rustfs_utils::http::{AMZ_BUCKET_REPLICATION_STATUS, AMZ_RESTORE, AMZ_STORAGE_CLASS};
use rustfs_utils::path::decode_dir_object;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::pin::Pin;
use std::str::FromStr as _;
use std::sync::Arc;
use std::task::{Context, Poll};
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncReadExt, ReadBuf};
use tokio_util::sync::CancellationToken;
use tracing::warn;
use uuid::Uuid;

pub const ERASURE_ALGORITHM: &str = "rs-vandermonde";
pub const BLOCK_SIZE_V2: usize = 1024 * 1024; // 1M

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MakeBucketOptions {
    pub lock_enabled: bool,
    pub versioning_enabled: bool,
    pub force_create: bool,                 // Create buckets even if they are already created.
    pub created_at: Option<OffsetDateTime>, // only for site replication
    pub no_lock: bool,
}

#[derive(Debug, Default, Clone, PartialEq)]
pub enum SRBucketDeleteOp {
    #[default]
    NoOp,
    MarkDelete,
    Purge,
}

#[derive(Debug, Default, Clone)]
pub struct DeleteBucketOptions {
    pub no_lock: bool,
    pub no_recreate: bool,
    pub force: bool, // Force deletion
    pub srdelete_op: SRBucketDeleteOp,
}

pub struct PutObjReader {
    pub stream: HashReader,
}

impl Debug for PutObjReader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PutObjReader").finish()
    }
}

impl PutObjReader {
    pub fn new(stream: HashReader) -> Self {
        PutObjReader { stream }
    }

    pub fn as_hash_reader(&self) -> &HashReader {
        &self.stream
    }

    pub fn from_vec(data: Vec<u8>) -> Self {
        use sha2::{Digest, Sha256};
        let content_length = data.len() as i64;
        let sha256hex = if content_length > 0 {
            Some(hex_simd::encode_to_string(Sha256::digest(&data), hex_simd::AsciiCase::Lower))
        } else {
            None
        };
        PutObjReader {
            stream: HashReader::new(
                Box::new(WarpReader::new(Cursor::new(data))),
                content_length,
                content_length,
                None,
                sha256hex,
                false,
            )
            .unwrap(),
        }
    }

    pub fn size(&self) -> i64 {
        self.stream.size()
    }

    pub fn actual_size(&self) -> i64 {
        self.stream.actual_size()
    }
}

pub struct GetObjectReader {
    pub stream: Box<dyn AsyncRead + Unpin + Send + Sync>,
    pub object_info: ObjectInfo,
}

impl GetObjectReader {
    #[tracing::instrument(level = "debug", skip(reader, rs, opts, _h))]
    pub fn new(
        reader: Box<dyn AsyncRead + Unpin + Send + Sync>,
        rs: Option<HTTPRangeSpec>,
        oi: &ObjectInfo,
        opts: &ObjectOptions,
        _h: &HeaderMap<HeaderValue>,
    ) -> Result<(Self, usize, i64)> {
        let mut rs = rs;

        if let Some(part_number) = opts.part_number
            && rs.is_none()
        {
            rs = HTTPRangeSpec::from_object_info(oi, part_number);
        }

        // TODO:Encrypted

        let (algo, is_compressed) = oi.is_compressed_ok()?;

        // TODO: check TRANSITION

        if is_compressed {
            let actual_size = oi.get_actual_size()?;
            let (off, length, dec_off, dec_length) = if let Some(rs) = rs {
                // Support range requests for compressed objects
                let (dec_off, dec_length) = rs.get_offset_length(actual_size)?;
                (0, oi.size, dec_off, dec_length)
            } else {
                (0, oi.size, 0, actual_size)
            };

            let dec_reader = DecompressReader::new(reader, algo);

            let actual_size_usize = if actual_size > 0 {
                actual_size as usize
            } else {
                return Err(Error::other(format!("invalid decompressed size {actual_size}")));
            };

            let final_reader: Box<dyn AsyncRead + Unpin + Send + Sync> = if dec_off > 0 || dec_length != actual_size {
                // Use RangedDecompressReader for streaming range processing
                // The new implementation supports any offset size by streaming and skipping data
                match RangedDecompressReader::new(dec_reader, dec_off, dec_length, actual_size_usize) {
                    Ok(ranged_reader) => {
                        tracing::debug!(
                            "Successfully created RangedDecompressReader for offset={}, length={}",
                            dec_off,
                            dec_length
                        );
                        Box::new(ranged_reader)
                    }
                    Err(e) => {
                        // Only fail if the range parameters are fundamentally invalid (e.g., offset >= file size)
                        tracing::error!("RangedDecompressReader failed with invalid range parameters: {}", e);
                        return Err(e);
                    }
                }
            } else {
                Box::new(LimitReader::new(dec_reader, actual_size_usize))
            };

            let mut oi = oi.clone();
            oi.size = dec_length;

            return Ok((
                GetObjectReader {
                    stream: final_reader,
                    object_info: oi,
                },
                off,
                length,
            ));
        }

        if let Some(rs) = rs {
            let (off, length) = rs.get_offset_length(oi.size)?;

            Ok((
                GetObjectReader {
                    stream: reader,
                    object_info: oi.clone(),
                },
                off,
                length,
            ))
        } else {
            Ok((
                GetObjectReader {
                    stream: reader,
                    object_info: oi.clone(),
                },
                0,
                oi.size,
            ))
        }
    }
    pub async fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut data = Vec::new();
        self.stream.read_to_end(&mut data).await?;

        // while let Some(x) = self.stream.next().await {
        //     let buf = match x {
        //         Ok(res) => res,
        //         Err(e) => return Err(Error::other(e.to_string())),
        //     };
        //     data.extend_from_slice(buf.as_ref());
        // }

        Ok(data)
    }
}

impl AsyncRead for GetObjectReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.stream).poll_read(cx, buf)
    }
}

#[derive(Debug, Clone)]
pub struct HTTPRangeSpec {
    pub is_suffix_length: bool,
    pub start: i64,
    pub end: i64,
}

impl HTTPRangeSpec {
    pub fn from_object_info(oi: &ObjectInfo, part_number: usize) -> Option<Self> {
        if oi.size == 0 || oi.parts.is_empty() {
            return None;
        }

        if part_number == 0 || part_number > oi.parts.len() {
            return None;
        }

        let mut start = 0_i64;
        let mut end = -1_i64;
        for i in 0..part_number {
            let part = &oi.parts[i];
            start = end + 1;
            end = start + (part.size as i64) - 1;
        }

        Some(HTTPRangeSpec {
            is_suffix_length: false,
            start,
            end,
        })
    }

    pub fn get_offset_length(&self, res_size: i64) -> Result<(usize, i64)> {
        let len = self.get_length(res_size)?;

        let mut start = self.start;
        if self.is_suffix_length {
            let suffix_len = if self.start < 0 {
                self.start
                    .checked_neg()
                    .ok_or_else(|| Error::InvalidRangeSpec("range value invalid: suffix length overflow".to_string()))?
            } else {
                self.start
            };
            start = res_size - suffix_len;
            if start < 0 {
                start = 0;
            }
        }
        Ok((start as usize, len))
    }
    pub fn get_length(&self, res_size: i64) -> Result<i64> {
        if res_size < 0 {
            return Err(Error::InvalidRangeSpec("The requested range is not satisfiable".to_string()));
        }

        if self.is_suffix_length {
            let specified_len = if self.start < 0 {
                self.start
                    .checked_neg()
                    .ok_or_else(|| Error::InvalidRangeSpec("range value invalid: suffix length overflow".to_string()))?
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
            return Err(Error::InvalidRangeSpec("The requested range is not satisfiable".to_string()));
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

        Err(Error::InvalidRangeSpec(format!(
            "range value invalid: start={}, end={}, expected start <= end and end >= -1",
            self.start, self.end
        )))
    }
}

#[derive(Debug, Default, Clone)]
pub struct HTTPPreconditions {
    pub if_match: Option<String>,
    pub if_none_match: Option<String>,
    pub if_modified_since: Option<OffsetDateTime>,
    pub if_unmodified_since: Option<OffsetDateTime>,
}

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

    pub versioned: bool,
    pub version_suspended: bool,

    pub skip_decommissioned: bool,
    pub skip_rebalancing: bool,
    pub skip_free_version: bool,

    pub data_movement: bool,
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

    pub want_checksum: Option<Checksum>,
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
        let rs = match self
            .user_defined
            .get(format!("{RESERVED_METADATA_PREFIX_LOWER}{REPLICATION_STATUS}").as_str())
        {
            Some(v) => v.to_string(),
            None => return ReplicationState::default(),
        };

        ReplicationState {
            replication_status_internal: Some(rs.to_string()),
            targets: replication_statuses_map(rs.as_str()),
            ..Default::default()
        }
    }

    pub fn precondition_check(&self, obj_info: &ObjectInfo) -> Result<()> {
        let has_valid_mod_time = obj_info.mod_time.is_some_and(|t| t != OffsetDateTime::UNIX_EPOCH);

        if let Some(part_number) = self.part_number
            && part_number > 1
            && !obj_info.parts.is_empty()
        {
            let part_found = obj_info.parts.iter().any(|pi| pi.number == part_number);
            if !part_found {
                return Err(Error::InvalidPartNumber(part_number));
            }
        }

        if let Some(pre) = &self.http_preconditions {
            if let Some(if_none_match) = &pre.if_none_match
                && let Some(etag) = &obj_info.etag
                && is_etag_equal(etag, if_none_match)
            {
                return Err(Error::NotModified);
            }

            if has_valid_mod_time
                && let Some(if_modified_since) = &pre.if_modified_since
                && let Some(mod_time) = &obj_info.mod_time
                && !is_modified_since(mod_time, if_modified_since)
            {
                return Err(Error::NotModified);
            }

            if let Some(if_match) = &pre.if_match {
                if let Some(etag) = &obj_info.etag {
                    if !is_etag_equal(etag, if_match) {
                        return Err(Error::PreconditionFailed);
                    }
                } else {
                    return Err(Error::PreconditionFailed);
                }
            }
            if has_valid_mod_time
                && pre.if_match.is_none()
                && let Some(if_unmodified_since) = &pre.if_unmodified_since
                && let Some(mod_time) = &obj_info.mod_time
                && is_modified_since(mod_time, if_unmodified_since)
            {
                return Err(Error::PreconditionFailed);
            }
        }

        Ok(())
    }
}

fn is_etag_equal(etag1: &str, etag2: &str) -> bool {
    let e1 = etag1.trim_matches('"');
    let e2 = etag2.trim_matches('"');
    // Handle wildcard "*" - matches any ETag (per HTTP/1.1 RFC 7232)
    if e2 == "*" {
        return true;
    }
    e1 == e2
}

fn is_modified_since(mod_time: &OffsetDateTime, given_time: &OffsetDateTime) -> bool {
    let mod_secs = mod_time.unix_timestamp();
    let given_secs = given_time.unix_timestamp();
    mod_secs > given_secs
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct BucketOptions {
    pub deleted: bool, // true only when site replication is enabled
    pub cached: bool, // true only when we are requesting a cached response instead of hitting the disk for example ListBuckets() call.
    pub no_metadata: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BucketInfo {
    pub name: String,
    pub created: Option<OffsetDateTime>,
    pub deleted: Option<OffsetDateTime>,
    pub versioning: bool,
    pub object_locking: bool,
}

#[derive(Debug, Default, Clone)]
pub struct MultipartUploadResult {
    pub upload_id: String,
    pub checksum_algo: Option<String>,
    pub checksum_type: Option<String>,
}

#[derive(Debug, Default, Clone)]
pub struct PartInfo {
    pub part_num: usize,
    pub last_mod: Option<OffsetDateTime>,
    pub size: usize,
    pub etag: Option<String>,
    pub actual_size: i64,
}

#[derive(Debug, Clone, Default)]
pub struct CompletePart {
    pub part_num: usize,
    pub etag: Option<String>,
    // pub size: Option<usize>,
    pub checksum_crc32: Option<String>,
    pub checksum_crc32c: Option<String>,
    pub checksum_sha1: Option<String>,
    pub checksum_sha256: Option<String>,
    pub checksum_crc64nvme: Option<String>,
}

impl From<s3s::dto::CompletedPart> for CompletePart {
    fn from(value: s3s::dto::CompletedPart) -> Self {
        Self {
            part_num: value.part_number.unwrap_or_default() as usize,
            etag: value.e_tag.map(|v| v.value().to_owned()),
            checksum_crc32: value.checksum_crc32,
            checksum_crc32c: value.checksum_crc32c,
            checksum_sha1: value.checksum_sha1,
            checksum_sha256: value.checksum_sha256,
            checksum_crc64nvme: value.checksum_crc64nvme,
        }
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
    pub user_defined: HashMap<String, String>,
    pub parity_blocks: usize,
    pub data_blocks: usize,
    pub version_id: Option<Uuid>,
    pub delete_marker: bool,
    pub transitioned_object: TransitionedObject,
    pub restore_ongoing: bool,
    pub restore_expires: Option<OffsetDateTime>,
    pub user_tags: String,
    pub parts: Vec<ObjectPartInfo>,
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
        self.user_defined
            .contains_key(&format!("{RESERVED_METADATA_PREFIX_LOWER}compression"))
    }

    pub fn is_compressed_ok(&self) -> Result<(CompressionAlgorithm, bool)> {
        let scheme = self
            .user_defined
            .get(&format!("{RESERVED_METADATA_PREFIX_LOWER}compression"))
            .cloned();

        if let Some(scheme) = scheme {
            let algorithm = CompressionAlgorithm::from_str(&scheme)?;
            Ok((algorithm, true))
        } else {
            Ok((CompressionAlgorithm::None, false))
        }
    }

    pub fn is_multipart(&self) -> bool {
        self.etag.as_ref().is_some_and(|v| v.len() != 32)
    }

    pub fn get_actual_size(&self) -> std::io::Result<i64> {
        if self.actual_size > 0 {
            return Ok(self.actual_size);
        }

        if self.is_compressed() {
            if let Some(size_str) = self.user_defined.get(&format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size"))
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
        // Encrypted objects store original size in x-rustfs-encryption-original-size metadata
        if let Some(size_str) = self.user_defined.get("x-rustfs-encryption-original-size")
            && !size_str.is_empty()
        {
            let size = size_str
                .parse::<i64>()
                .map_err(|e| std::io::Error::other(format!("Failed to parse encryption original size: {e}")))?;
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
        let user_tags = fi.metadata.get(AMZ_OBJECT_TAGGING).cloned().unwrap_or_default();

        let inlined = fi.inline_data();

        // Parse expires from metadata (HTTP date format RFC 7231 or ISO 8601)
        let expires = fi.metadata.get("expires").and_then(|s| {
            // Try parsing as ISO 8601 first
            time::OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT)
                .or_else(|_| {
                    // Try RFC 2822 format
                    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc2822)
                })
                .or_else(|_| {
                    // Try RFC 3339 format
                    time::OffsetDateTime::parse(s, &time::format_description::well_known::Rfc3339)
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

        let mut replication_status = fi.replication_status();
        if replication_status.is_empty()
            && let Some(status) = fi.metadata.get(AMZ_BUCKET_REPLICATION_STATUS).cloned()
            && status == ReplicationStatusType::Replica.as_str()
        {
            replication_status = ReplicationStatusType::Replica;
        }

        let version_purge_status = fi.version_purge_status();

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

        // Convert parts from rustfs_filemeta::ObjectPartInfo to store_api::ObjectPartInfo
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
            .collect();

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
            parts,
            is_latest: fi.is_latest,
            user_tags,
            content_type,
            content_encoding,
            expires,
            num_versions: fi.num_versions,
            successor_mod_time: fi.successor_mod_time,
            etag,
            inlined,
            user_defined: metadata,
            transitioned_object,
            checksum: fi.checksum.clone(),
            storage_class,
            restore_ongoing,
            restore_expires,
            replication_status_internal,
            replication_status,
            version_purge_status_internal,
            version_purge_status,
            ..Default::default()
        }
    }

    pub async fn from_meta_cache_entries_sorted_versions(
        entries: &MetaCacheEntriesSorted,
        bucket: &str,
        prefix: &str,
        delimiter: Option<String>,
        after_version_id: Option<Uuid>,
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

                let file_infos = match entry.file_info_versions(bucket) {
                    Ok(res) => res,
                    Err(err) => {
                        warn!("file_info_versions err {:?}", err);
                        continue;
                    }
                };

                let versions = if let Some(vid) = after_version_id {
                    if let Some(idx) = file_infos.find_version_index(vid) {
                        &file_infos.versions[idx + 1..]
                    } else {
                        &file_infos.versions
                    }
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
                    if k.starts_with(&format!("{RESERVED_METADATA_PREFIX_LOWER}{REPLICATION_RESET}")) {
                        Some((
                            k.trim_start_matches(&format!("{RESERVED_METADATA_PREFIX_LOWER}{REPLICATION_RESET}-"))
                                .to_string(),
                            v.clone(),
                        ))
                    } else {
                        None
                    }
                })
                .collect(),
            ..Default::default()
        }
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

#[derive(Debug, Default)]
pub struct ListObjectsInfo {
    // Indicates whether the returned list objects response is truncated. A
    // value of true indicates that the list was truncated. The list can be truncated
    // if the number of objects exceeds the limit allowed or specified
    // by max keys.
    pub is_truncated: bool,

    // When response is truncated (the IsTruncated element value in the response
    // is true), you can use the key name in this field as marker in the subsequent
    // request to get next set of objects.
    pub next_marker: Option<String>,

    // List of objects info for this request.
    pub objects: Vec<ObjectInfo>,

    // List of prefixes for this request.
    pub prefixes: Vec<String>,
}

#[derive(Debug, Default)]
pub struct ListObjectsV2Info {
    // Indicates whether the returned list objects response is truncated. A
    // value of true indicates that the list was truncated. The list can be truncated
    // if the number of objects exceeds the limit allowed or specified
    // by max keys.
    pub is_truncated: bool,

    // When response is truncated (the IsTruncated element value in the response
    // is true), you can use the key name in this field as marker in the subsequent
    // request to get next set of objects.
    //
    // NOTE: This element is returned only if you have delimiter request parameter
    // specified.
    pub continuation_token: Option<String>,
    pub next_continuation_token: Option<String>,

    // List of objects info for this request.
    pub objects: Vec<ObjectInfo>,

    // List of prefixes for this request.
    pub prefixes: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct MultipartInfo {
    // Name of the bucket.
    pub bucket: String,

    // Name of the object.
    pub object: String,

    // Upload ID identifying the multipart upload whose parts are being listed.
    pub upload_id: String,

    // Date and time at which the multipart upload was initiated.
    pub initiated: Option<OffsetDateTime>,

    // Any metadata set during InitMultipartUpload, including encryption headers.
    pub user_defined: HashMap<String, String>,
}

// ListMultipartsInfo - represents bucket resources for incomplete multipart uploads.
#[derive(Debug, Clone, Default)]
pub struct ListMultipartsInfo {
    // Together with upload-id-marker, this parameter specifies the multipart upload
    // after which listing should begin.
    pub key_marker: Option<String>,

    // Together with key-marker, specifies the multipart upload after which listing
    // should begin. If key-marker is not specified, the upload-id-marker parameter
    // is ignored.
    pub upload_id_marker: Option<String>,

    // When a list is truncated, this element specifies the value that should be
    // used for the key-marker request parameter in a subsequent request.
    pub next_key_marker: Option<String>,

    // When a list is truncated, this element specifies the value that should be
    // used for the upload-id-marker request parameter in a subsequent request.
    pub next_upload_id_marker: Option<String>,

    // Maximum number of multipart uploads that could have been included in the
    // response.
    pub max_uploads: usize,

    // Indicates whether the returned list of multipart uploads is truncated. A
    // value of true indicates that the list was truncated. The list can be truncated
    // if the number of multipart uploads exceeds the limit allowed or specified
    // by max uploads.
    pub is_truncated: bool,

    // List of all pending uploads.
    pub uploads: Vec<MultipartInfo>,

    // When a prefix is provided in the request, The result contains only keys
    // starting with the specified prefix.
    pub prefix: String,

    // A character used to truncate the object prefixes.
    // NOTE: only supported delimiter is '/'.
    pub delimiter: Option<String>,

    // CommonPrefixes contains all (if there are any) keys between Prefix and the
    // next occurrence of the string specified by delimiter.
    pub common_prefixes: Vec<String>,
    // encoding_type: String, // Not supported yet.
}

/// ListPartsInfo - represents list of all parts.
#[derive(Debug, Clone, Default)]
pub struct ListPartsInfo {
    /// Name of the bucket.
    pub bucket: String,

    /// Name of the object.
    pub object: String,

    /// Upload ID identifying the multipart upload whose parts are being listed.
    pub upload_id: String,

    /// The class of storage used to store the object.
    pub storage_class: String,

    /// Part number after which listing begins.
    pub part_number_marker: usize,

    /// When a list is truncated, this element specifies the last part in the list,
    /// as well as the value to use for the part-number-marker request parameter
    /// in a subsequent request.
    pub next_part_number_marker: usize,

    /// Maximum number of parts that were allowed in the response.
    pub max_parts: usize,

    /// Indicates whether the returned list of parts is truncated.
    pub is_truncated: bool,

    /// List of all parts.
    pub parts: Vec<PartInfo>,

    /// Any metadata set during InitMultipartUpload, including encryption headers.
    pub user_defined: HashMap<String, String>,

    /// ChecksumAlgorithm if set
    pub checksum_algorithm: String,

    /// ChecksumType if set
    pub checksum_type: String,
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
    // MTime of DeleteMarker on source that needs to be propagated to replica
    pub delete_marker_mtime: Option<OffsetDateTime>,
    // to support delete marker replication
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

#[derive(Debug, Default, Clone)]
pub struct ListObjectVersionsInfo {
    pub is_truncated: bool,
    pub next_marker: Option<String>,
    pub next_version_idmarker: Option<String>,
    pub objects: Vec<ObjectInfo>,
    pub prefixes: Vec<String>,
}

type WalkFilter = fn(&FileInfo) -> bool;

#[derive(Clone, Default)]
pub struct WalkOptions {
    pub filter: Option<WalkFilter>,           // return WalkFilter returns 'true/false'
    pub marker: Option<String>,               // set to skip until this object
    pub latest_only: bool,                    // returns only latest versions for all matching objects
    pub ask_disks: String,                    // dictates how many disks are being listed
    pub versions_sort: WalkVersionsSortOrder, // sort order for versions of the same object; default: Ascending order in ModTime
    pub limit: usize,                         // maximum number of items, 0 means no limit
}

#[derive(Clone, Default, PartialEq, Eq)]
pub enum WalkVersionsSortOrder {
    #[default]
    Ascending,
    Descending,
}

#[derive(Debug)]
pub struct ObjectInfoOrErr {
    pub item: Option<ObjectInfo>,
    pub err: Option<Error>,
}

#[async_trait::async_trait]
pub trait ObjectIO: Send + Sync + Debug + 'static {
    // GetObjectNInfo FIXME:
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader>;
    // PutObject
    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo>;
}

#[async_trait::async_trait]
#[allow(clippy::too_many_arguments)]
pub trait StorageAPI: ObjectIO + Debug {
    async fn new_ns_lock(&self, bucket: &str, object: &str) -> Result<NamespaceLockWrapper>;

    async fn backend_info(&self) -> rustfs_madmin::BackendInfo;
    async fn storage_info(&self) -> rustfs_madmin::StorageInfo;
    async fn local_storage_info(&self) -> rustfs_madmin::StorageInfo;

    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()>;
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo>;
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>>;
    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()>;
    // ListObjects TODO: FIXME:
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
    ) -> Result<ListObjectsV2Info>;
    // ListObjectVersions TODO: FIXME:
    async fn list_object_versions(
        self: Arc<Self>,
        bucket: &str,
        prefix: &str,
        marker: Option<String>,
        version_marker: Option<String>,
        delimiter: Option<String>,
        max_keys: i32,
    ) -> Result<ListObjectVersionsInfo>;

    async fn walk(
        self: Arc<Self>,
        rx: CancellationToken,
        bucket: &str,
        prefix: &str,
        result: tokio::sync::mpsc::Sender<ObjectInfoOrErr>,
        opts: WalkOptions,
    ) -> Result<()>;

    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo>;
    async fn verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()>;
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        src_info: &mut ObjectInfo,
        src_opts: &ObjectOptions,
        dst_opts: &ObjectOptions,
    ) -> Result<ObjectInfo>;
    async fn delete_object_version(&self, bucket: &str, object: &str, fi: &FileInfo, force_del_marker: bool) -> Result<()>;
    async fn delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo>;
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>);

    // TransitionObject TODO:
    // RestoreTransitionedObject TODO:

    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        prefix: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
        max_uploads: usize,
    ) -> Result<ListMultipartsInfo>;
    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult>;
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
        src_info: &ObjectInfo,
        src_opts: &ObjectOptions,
        dst_opts: &ObjectOptions,
    ) -> Result<()>;
    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo>;
    async fn get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartInfo>;
    async fn list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: Option<usize>,
        max_parts: usize,
        opts: &ObjectOptions,
    ) -> Result<ListPartsInfo>;
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, opts: &ObjectOptions) -> Result<()>;
    async fn complete_multipart_upload(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo>;
    async fn get_disks(&self, pool_idx: usize, set_idx: usize) -> Result<Vec<Option<DiskStore>>>;
    fn set_drive_counts(&self) -> Vec<usize>;

    // Health TODO:
    async fn put_object_metadata(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo>;
    // DecomTieredObject
    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String>;
    async fn add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()>;
    async fn transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()>;
    async fn restore_transitioned_object(self: Arc<Self>, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()>;
    async fn put_object_tags(&self, bucket: &str, object: &str, tags: &str, opts: &ObjectOptions) -> Result<ObjectInfo>;
    async fn delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo>;

    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)>;
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem>;
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)>;
    // async fn heal_objects(&self, bucket: &str, prefix: &str, opts: &HealOpts, hs: Arc<HealSequence>, is_meta: bool)
    // -> Result<()>;
    async fn get_pool_and_set(&self, id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)>;
    async fn check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()>;
}

/// A streaming decompression reader that supports range requests by skipping data in the decompressed stream.
/// This implementation acknowledges that compressed streams (like LZ4) must be decompressed sequentially
/// from the beginning, so it streams and discards data until reaching the target offset.
#[derive(Debug)]
pub struct RangedDecompressReader<R> {
    inner: R,
    target_offset: usize,
    target_length: usize,
    current_offset: usize,
    bytes_returned: usize,
}

impl<R: AsyncRead + Unpin + Send + Sync> RangedDecompressReader<R> {
    pub fn new(inner: R, offset: usize, length: i64, total_size: usize) -> Result<Self> {
        // Validate the range request
        if offset >= total_size {
            tracing::debug!("Range offset {} exceeds total size {}", offset, total_size);
            return Err(Error::InvalidRangeSpec("Range offset exceeds file size".to_string()));
        }

        // Adjust length if it extends beyond file end
        let actual_length = std::cmp::min(length as usize, total_size - offset);

        tracing::debug!(
            "Creating RangedDecompressReader: offset={}, length={}, total_size={}, actual_length={}",
            offset,
            length,
            total_size,
            actual_length
        );

        Ok(Self {
            inner,
            target_offset: offset,
            target_length: actual_length,
            current_offset: 0,
            bytes_returned: 0,
        })
    }
}

impl<R: AsyncRead + Unpin + Send + Sync> AsyncRead for RangedDecompressReader<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::pin::Pin;
        use std::task::Poll;
        use tokio::io::ReadBuf;

        loop {
            // If we've returned all the bytes we need, return EOF
            if self.bytes_returned >= self.target_length {
                return Poll::Ready(Ok(()));
            }

            // Read from the inner stream
            let buf_capacity = buf.remaining();
            if buf_capacity == 0 {
                return Poll::Ready(Ok(()));
            }

            // Prepare a temporary buffer for reading
            let mut temp_buf = vec![0u8; std::cmp::min(buf_capacity, 8192)];
            let mut temp_read_buf = ReadBuf::new(&mut temp_buf);

            match Pin::new(&mut self.inner).poll_read(cx, &mut temp_read_buf) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Err(e)) => return Poll::Ready(Err(e)),
                Poll::Ready(Ok(())) => {
                    let n = temp_read_buf.filled().len();
                    if n == 0 {
                        // EOF from inner stream
                        if self.current_offset < self.target_offset {
                            // We haven't reached the target offset yet - this is an error
                            return Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::UnexpectedEof,
                                format!(
                                    "Unexpected EOF: only read {} bytes, target offset is {}",
                                    self.current_offset, self.target_offset
                                ),
                            )));
                        }
                        // Normal EOF after reaching target
                        return Poll::Ready(Ok(()));
                    }

                    // Update current position
                    let old_offset = self.current_offset;
                    self.current_offset += n;

                    // Check if we're still in the skip phase
                    if old_offset < self.target_offset {
                        // We're still skipping data
                        let skip_end = std::cmp::min(self.current_offset, self.target_offset);
                        let bytes_to_skip_in_this_read = skip_end - old_offset;

                        if self.current_offset <= self.target_offset {
                            // All data in this read should be skipped
                            tracing::trace!("Skipping {} bytes at offset {}", n, old_offset);
                            // Continue reading in the loop instead of recursive call
                            continue;
                        } else {
                            // Partial skip: some data should be returned
                            let data_start_in_buffer = bytes_to_skip_in_this_read;
                            let available_data = n - data_start_in_buffer;
                            let bytes_to_return = std::cmp::min(
                                available_data,
                                std::cmp::min(buf.remaining(), self.target_length - self.bytes_returned),
                            );

                            if bytes_to_return > 0 {
                                let data_slice =
                                    &temp_read_buf.filled()[data_start_in_buffer..data_start_in_buffer + bytes_to_return];
                                buf.put_slice(data_slice);
                                self.bytes_returned += bytes_to_return;

                                tracing::trace!(
                                    "Skipped {} bytes, returned {} bytes at offset {}",
                                    bytes_to_skip_in_this_read,
                                    bytes_to_return,
                                    old_offset
                                );
                            }
                            return Poll::Ready(Ok(()));
                        }
                    } else {
                        // We're in the data return phase
                        let bytes_to_return =
                            std::cmp::min(n, std::cmp::min(buf.remaining(), self.target_length - self.bytes_returned));

                        if bytes_to_return > 0 {
                            buf.put_slice(&temp_read_buf.filled()[..bytes_to_return]);
                            self.bytes_returned += bytes_to_return;

                            tracing::trace!("Returned {} bytes at offset {}", bytes_to_return, old_offset);
                        }
                        return Poll::Ready(Ok(()));
                    }
                }
            }
        }
    }
}

/// A wrapper that ensures the inner stream is fully consumed even if the outer reader stops early.
/// This prevents broken pipe errors in erasure coding scenarios where the writer expects
/// the full stream to be consumed.
pub struct StreamConsumer<R: AsyncRead + Unpin + Send + 'static> {
    inner: Option<R>,
    consumer_task: Option<tokio::task::JoinHandle<()>>,
}

impl<R: AsyncRead + Unpin + Send + 'static> StreamConsumer<R> {
    pub fn new(inner: R) -> Self {
        Self {
            inner: Some(inner),
            consumer_task: None,
        }
    }

    fn ensure_consumer_started(&mut self) {
        if self.consumer_task.is_none() && self.inner.is_some() {
            let mut inner = self.inner.take().unwrap();
            let task = tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                loop {
                    match inner.read(&mut buf).await {
                        Ok(0) => break,    // EOF
                        Ok(_) => continue, // Keep consuming
                        Err(_) => break,   // Error, stop consuming
                    }
                }
            });
            self.consumer_task = Some(task);
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> AsyncRead for StreamConsumer<R> {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        use std::pin::Pin;
        use std::task::Poll;

        if let Some(ref mut inner) = self.inner {
            Pin::new(inner).poll_read(cx, buf)
        } else {
            Poll::Ready(Ok(())) // EOF
        }
    }
}

impl<R: AsyncRead + Unpin + Send + 'static> Drop for StreamConsumer<R> {
    fn drop(&mut self) {
        if self.consumer_task.is_none() && self.inner.is_some() {
            let mut inner = self.inner.take().unwrap();
            let task = tokio::spawn(async move {
                let mut buf = [0u8; 8192];
                loop {
                    match inner.read(&mut buf).await {
                        Ok(0) => break,    // EOF
                        Ok(_) => continue, // Keep consuming
                        Err(_) => break,   // Error, stop consuming
                    }
                }
            });
            self.consumer_task = Some(task);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn test_ranged_decompress_reader() {
        // Create test data
        let original_data = b"Hello, World! This is a test for range requests on compressed data.";

        // For this test, we'll simulate using the original data directly as "decompressed"
        let cursor = Cursor::new(original_data.to_vec());

        // Test reading a range from the middle
        let mut ranged_reader = RangedDecompressReader::new(cursor, 7, 5, original_data.len()).unwrap();

        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();

        // Should read "World" (5 bytes starting from position 7)
        assert_eq!(result, b"World");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_from_start() {
        let original_data = b"Hello, World! This is a test.";
        let cursor = Cursor::new(original_data.to_vec());

        let mut ranged_reader = RangedDecompressReader::new(cursor, 0, 5, original_data.len()).unwrap();

        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();

        // Should read "Hello" (5 bytes from the start)
        assert_eq!(result, b"Hello");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_to_end() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());

        let mut ranged_reader = RangedDecompressReader::new(cursor, 7, 6, original_data.len()).unwrap();

        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();

        // Should read "World!" (6 bytes starting from position 7)
        assert_eq!(result, b"World!");
    }

    #[tokio::test]
    async fn test_http_range_spec_with_compressed_data() {
        // Test that HTTPRangeSpec::get_offset_length works correctly
        let range_spec = HTTPRangeSpec {
            is_suffix_length: false,
            start: 5,
            end: 14, // inclusive
        };

        let total_size = 100i64;
        let (offset, length) = range_spec.get_offset_length(total_size).unwrap();

        assert_eq!(offset, 5);
        assert_eq!(length, 10); // end - start + 1 = 14 - 5 + 1 = 10
    }

    #[test]
    fn test_http_range_spec_suffix_positive_start() {
        let range_spec = HTTPRangeSpec {
            is_suffix_length: true,
            start: 5,
            end: -1,
        };

        let (offset, length) = range_spec.get_offset_length(20).unwrap();
        assert_eq!(offset, 15);
        assert_eq!(length, 5);
    }

    #[test]
    fn test_http_range_spec_suffix_negative_start() {
        let range_spec = HTTPRangeSpec {
            is_suffix_length: true,
            start: -5,
            end: -1,
        };

        let (offset, length) = range_spec.get_offset_length(20).unwrap();
        assert_eq!(offset, 15);
        assert_eq!(length, 5);
    }

    #[test]
    fn test_http_range_spec_suffix_exceeds_object() {
        let range_spec = HTTPRangeSpec {
            is_suffix_length: true,
            start: 50,
            end: -1,
        };

        let (offset, length) = range_spec.get_offset_length(20).unwrap();
        assert_eq!(offset, 0);
        assert_eq!(length, 20);
    }

    #[test]
    fn test_http_range_spec_from_object_info_valid_and_invalid_parts() {
        let object_info = ObjectInfo {
            size: 300,
            parts: vec![
                ObjectPartInfo {
                    etag: String::new(),
                    number: 1,
                    size: 100,
                    actual_size: 100,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 2,
                    size: 100,
                    actual_size: 100,
                    ..Default::default()
                },
                ObjectPartInfo {
                    etag: String::new(),
                    number: 3,
                    size: 100,
                    actual_size: 100,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };

        let spec = HTTPRangeSpec::from_object_info(&object_info, 2).unwrap();
        assert_eq!(spec.start, 100);
        assert_eq!(spec.end, 199);

        assert!(HTTPRangeSpec::from_object_info(&object_info, 0).is_none());
        assert!(HTTPRangeSpec::from_object_info(&object_info, 4).is_none());
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_zero_length() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());
        let mut ranged_reader = RangedDecompressReader::new(cursor, 5, 0, original_data.len()).unwrap();
        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();
        // Should read nothing
        assert_eq!(result, b"");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_skip_entire_data() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());
        // Skip to end of data with length 0 - this should read nothing
        let mut ranged_reader = RangedDecompressReader::new(cursor, original_data.len() - 1, 0, original_data.len()).unwrap();
        let mut result = Vec::new();
        ranged_reader.read_to_end(&mut result).await.unwrap();
        assert_eq!(result, b"");
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_out_of_bounds_offset() {
        let original_data = b"Hello, World!";
        let cursor = Cursor::new(original_data.to_vec());
        // Offset beyond EOF should return error in constructor
        let result = RangedDecompressReader::new(cursor, original_data.len() + 10, 5, original_data.len());
        assert!(result.is_err());
        // Use pattern matching to avoid requiring Debug on the error type
        if let Err(e) = result {
            assert!(e.to_string().contains("Range offset exceeds file size"));
        }
    }

    #[tokio::test]
    async fn test_ranged_decompress_reader_partial_read() {
        let original_data = b"abcdef";
        let cursor = Cursor::new(original_data.to_vec());
        let mut ranged_reader = RangedDecompressReader::new(cursor, 2, 3, original_data.len()).unwrap();
        let mut buf = [0u8; 2];
        let n = ranged_reader.read(&mut buf).await.unwrap();
        assert_eq!(n, 2);
        assert_eq!(&buf, b"cd");
        let mut buf2 = [0u8; 2];
        let n2 = ranged_reader.read(&mut buf2).await.unwrap();
        assert_eq!(n2, 1);
        assert_eq!(&buf2[..1], b"e");
    }
}
