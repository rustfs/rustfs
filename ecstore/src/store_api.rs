use crate::bucket::metadata_sys::get_versioning_config;
use crate::bucket::versioning::VersioningApi as _;
use crate::cmd::bucket_replication::{ReplicationStatusType, VersionPurgeStatusType};
use crate::error::{Error, Result};
use crate::heal::heal_ops::HealSequence;
use crate::store_utils::clean_metadata;
use crate::{
    bucket::lifecycle::bucket_lifecycle_audit::LcAuditEvent,
    bucket::lifecycle::lifecycle::ExpirationOptions,
    bucket::lifecycle::{bucket_lifecycle_ops::TransitionedObject, lifecycle::TransitionOptions},
};
use crate::{disk::DiskStore, heal::heal_commands::HealOpts};
use http::{HeaderMap, HeaderValue};
use madmin::heal_commands::HealResultItem;
use rustfs_filemeta::headers::RESERVED_METADATA_PREFIX_LOWER;
use rustfs_filemeta::{FileInfo, MetaCacheEntriesSorted, ObjectPartInfo, headers::AMZ_OBJECT_TAGGING};
use rustfs_rio::{DecompressReader, HashReader, LimitReader, WarpReader};
use rustfs_utils::CompressionAlgorithm;
use rustfs_utils::path::decode_dir_object;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::str::FromStr as _;
use std::sync::Arc;
use time::OffsetDateTime;
use tokio::io::{AsyncRead, AsyncReadExt};
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

    pub fn from_vec(data: Vec<u8>) -> Self {
        let content_length = data.len() as i64;
        PutObjReader {
            stream: HashReader::new(Box::new(WarpReader::new(Cursor::new(data))), content_length, content_length, None, false)
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
    #[tracing::instrument(level = "debug", skip(reader))]
    pub fn new(
        reader: Box<dyn AsyncRead + Unpin + Send + Sync>,
        rs: Option<HTTPRangeSpec>,
        oi: &ObjectInfo,
        opts: &ObjectOptions,
        _h: &HeaderMap<HeaderValue>,
    ) -> Result<(Self, usize, i64)> {
        let mut rs = rs;

        if let Some(part_number) = opts.part_number {
            if rs.is_none() {
                rs = HTTPRangeSpec::from_object_info(oi, part_number);
            }
        }

        // TODO:Encrypted

        let (algo, is_compressed) = oi.is_compressed_ok()?;

        // TODO: check TRANSITION

        if is_compressed {
            let actual_size = oi.get_actual_size()?;
            let (off, length) = (0, oi.size);
            let (_dec_off, dec_length) = (0, actual_size);
            if let Some(_rs) = rs {
                // TODO: range spec is not supported for compressed object
                return Err(Error::other("The requested range is not satisfiable"));
                // let (off, length) = rs.get_offset_length(actual_size)?;
            }

            let dec_reader = DecompressReader::new(reader, algo);

            let actual_size = if actual_size > 0 {
                actual_size as usize
            } else {
                return Err(Error::other(format!("invalid decompressed size {actual_size}")));
            };

            let dec_reader = LimitReader::new(dec_reader, actual_size);

            let mut oi = oi.clone();
            oi.size = dec_length;

            return Ok((
                GetObjectReader {
                    stream: Box::new(dec_reader),
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

#[derive(Debug)]
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

        let mut start = 0i64;
        let mut end = -1i64;
        for i in 0..oi.parts.len().min(part_number) {
            start = end + 1;
            end = start + (oi.parts[i].size as i64) - 1
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
            start = res_size + self.start;

            if start < 0 {
                start = 0;
            }
        }
        Ok((start as usize, len))
    }
    pub fn get_length(&self, res_size: i64) -> Result<i64> {
        if res_size < 0 {
            return Err(Error::other("The requested range is not satisfiable"));
        }

        if self.is_suffix_length {
            let specified_len = self.start; // 假设 h.start 是一个 i64 类型
            let mut range_length = specified_len;

            if specified_len > res_size {
                range_length = res_size;
            }

            return Ok(range_length);
        }

        if self.start >= res_size {
            return Err(Error::other("The requested range is not satisfiable"));
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

        Err(Error::other("range value invaild"))
    }
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

    pub data_movement: bool,
    pub src_pool_idx: usize,
    pub user_defined: HashMap<String, String>,
    pub preserve_etag: Option<String>,
    pub metadata_chg: bool,

    pub replication_request: bool,
    pub delete_marker: bool,

    pub transition: TransitionOptions,
    pub expiration: ExpirationOptions,
    pub lifecycle_audit_event: LcAuditEvent,

    pub eval_metadata: Option<HashMap<String, String>>,
}

// impl Default for ObjectOptions {
//     fn default() -> Self {
//         Self {
//             max_parity: Default::default(),
//             mod_time: OffsetDateTime::UNIX_EPOCH,
//             part_number: Default::default(),
//         }
//     }
// }

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
    pub versionning: bool,
    pub object_locking: bool,
}

#[derive(Debug, Default, Clone)]
pub struct MultipartUploadResult {
    pub upload_id: String,
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
}

impl From<s3s::dto::CompletedPart> for CompletePart {
    fn from(value: s3s::dto::CompletedPart) -> Self {
        Self {
            part_num: value.part_number.unwrap_or_default() as usize,
            etag: value.e_tag,
        }
    }
}

#[derive(Debug, Default)]
pub struct ObjectInfo {
    pub bucket: String,
    pub name: String,
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
    pub user_tags: String,
    pub parts: Vec<ObjectPartInfo>,
    pub is_latest: bool,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub num_versions: usize,
    pub successor_mod_time: Option<OffsetDateTime>,
    pub put_object_reader: Option<PutObjReader>,
    pub etag: Option<String>,
    pub inlined: bool,
    pub metadata_only: bool,
    pub version_only: bool,
    pub replication_status_internal: String,
    pub replication_status: ReplicationStatusType,
    pub version_purge_status_internal: String,
    pub version_purge_status: VersionPurgeStatusType,
    pub checksum: Vec<u8>,
}

impl Clone for ObjectInfo {
    fn clone(&self) -> Self {
        Self {
            bucket: self.bucket.clone(),
            name: self.name.clone(),
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
            checksum: Default::default(),
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
            if let Some(size_str) = self.user_defined.get(&format!("{RESERVED_METADATA_PREFIX_LOWER}actual-size")) {
                if !size_str.is_empty() {
                    // Todo: deal with error
                    let size = size_str.parse::<i64>().map_err(|e| std::io::Error::other(e.to_string()))?;
                    return Ok(size);
                }
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

        // TODO: IsEncrypted

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

        // TODO:expires
        // TODO:ReplicationState

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
            })
            .collect();

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
            num_versions: fi.num_versions,
            successor_mod_time: fi.successor_mod_time,
            etag,
            inlined,
            user_defined: metadata,
            transitioned_object,
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
                    if let Some(idx) = entry.name.trim_start_matches(prefix).find(delimiter) {
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
                    // TODO:VersionPurgeStatus
                    let versioned = vcfg.clone().map(|v| v.0.versioned(&entry.name)).unwrap_or_default();
                    objects.push(ObjectInfo::from_file_info(fi, bucket, &entry.name, versioned));
                }
                continue;
            }

            if entry.is_dir() {
                if let Some(delimiter) = &delimiter {
                    if let Some(idx) = entry.name.trim_start_matches(prefix).find(delimiter) {
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
                    if let Some(idx) = entry.name.trim_start_matches(prefix).find(delimiter) {
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

            if entry.is_dir() {
                if let Some(delimiter) = &delimiter {
                    if let Some(idx) = entry.name.trim_start_matches(prefix).find(delimiter) {
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
            }
        }

        objects
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

#[derive(Debug, Default, Clone)]
pub struct ObjectToDelete {
    pub object_name: String,
    pub version_id: Option<Uuid>,
}
#[derive(Debug, Default, Clone)]
pub struct DeletedObject {
    pub delete_marker: bool,
    pub delete_marker_version_id: Option<String>,
    pub object_name: String,
    pub version_id: Option<String>,
    // MTime of DeleteMarker on source that needs to be propagated to replica
    pub delete_marker_mtime: Option<OffsetDateTime>,
    // to support delete marker replication
    // pub replication_state: ReplicationState,
}

#[derive(Debug, Default, Clone)]
pub struct ListObjectVersionsInfo {
    pub is_truncated: bool,
    pub next_marker: Option<String>,
    pub next_version_idmarker: Option<String>,
    pub objects: Vec<ObjectInfo>,
    pub prefixes: Vec<String>,
}

#[async_trait::async_trait]
pub trait ObjectIO: Send + Sync + 'static {
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
pub trait StorageAPI: ObjectIO {
    // NewNSLock TODO:
    // Shutdown TODO:
    // NSScanner TODO:

    async fn backend_info(&self) -> madmin::BackendInfo;
    async fn storage_info(&self) -> madmin::StorageInfo;
    async fn local_storage_info(&self) -> madmin::StorageInfo;

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
    // Walk TODO:

    // GetObjectNInfo ObjectIO
    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo>;
    // PutObject ObjectIO
    // CopyObject
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
    ) -> Result<(Vec<DeletedObject>, Vec<Option<Error>>)>;

    // TransitionObject TODO:
    // RestoreTransitionedObject TODO:

    // ListMultipartUploads
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
    // CopyObjectPart
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
    // GetMultipartInfo
    async fn get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        opts: &ObjectOptions,
    ) -> Result<MultipartInfo>;
    // ListObjectParts
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, opts: &ObjectOptions) -> Result<()>;
    async fn complete_multipart_upload(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo>;
    // GetDisks
    async fn get_disks(&self, pool_idx: usize, set_idx: usize) -> Result<Vec<Option<DiskStore>>>;
    // SetDriveCounts
    fn set_drive_counts(&self) -> Vec<usize>;

    // Health TODO:
    // PutObjectMetadata
    async fn put_object_metadata(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo>;
    // DecomTieredObject
    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String>;
    async fn add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()>;
    async fn transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()>;
    async fn restore_transitioned_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()>;
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
    async fn heal_objects(&self, bucket: &str, prefix: &str, opts: &HealOpts, hs: Arc<HealSequence>, is_meta: bool)
    -> Result<()>;
    async fn get_pool_and_set(&self, id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)>;
    async fn check_abandoned_parts(&self, bucket: &str, object: &str, opts: &HealOpts) -> Result<()>;
}
