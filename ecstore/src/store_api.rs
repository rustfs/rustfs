use crate::heal::heal_ops::HealSequence;
use crate::{
    disk::DiskStore,
    error::{Error, Result},
    heal::heal_commands::{HealOpts, HealResultItem},
    utils::path::decode_dir_object,
    xhttp,
};
use futures::StreamExt;
use http::HeaderMap;
use rmp_serde::Serializer;
use s3s::{dto::StreamingBlob, Body};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use time::OffsetDateTime;
use uuid::Uuid;

pub const ERASURE_ALGORITHM: &str = "rs-vandermonde";
pub const BLOCK_SIZE_V2: usize = 1048576; // 1M
pub const RESERVED_METADATA_PREFIX: &str = "X-Rustfs-Internal-";
pub const RESERVED_METADATA_PREFIX_LOWER: &str = "X-Rustfs-Internal-";
pub const RUSTFS_HEALING: &str = "X-Rustfs-Internal-healing";
pub const RUSTFS_DATA_MOVE: &str = "X-Rustfs-Internal-data-mov";

// #[derive(Debug, Clone)]
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct FileInfo {
    pub volume: String,
    pub name: String,
    pub version_id: Option<Uuid>,
    pub is_latest: bool,
    pub deleted: bool,
    // TransitionStatus
    // TransitionedObjName
    // TransitionTier
    // TransitionVersionID
    // ExpireRestored
    pub data_dir: Option<Uuid>,
    pub mod_time: Option<OffsetDateTime>,
    pub size: usize,
    // Mode
    pub metadata: Option<HashMap<String, String>>,
    pub parts: Vec<ObjectPartInfo>,
    pub erasure: ErasureInfo,
    // MarkDeleted
    // ReplicationState
    pub data: Option<Vec<u8>>,
    pub num_versions: usize,
    pub successor_mod_time: Option<OffsetDateTime>,
    pub fresh: bool,
    pub idx: usize,
    // Checksum
    pub versioned: bool,
}

impl FileInfo {
    pub fn new(object: &str, data_blocks: usize, parity_blocks: usize) -> Self {
        let indexs = {
            let cardinality = data_blocks + parity_blocks;
            let mut nums = vec![0; cardinality];
            let key_crc = crc32fast::hash(object.as_bytes());

            let start = key_crc as usize % cardinality;
            for i in 1..=cardinality {
                nums[i - 1] = 1 + ((start + i) % cardinality);
            }

            nums
        };
        Self {
            erasure: ErasureInfo {
                algorithm: String::from(ERASURE_ALGORITHM),
                data_blocks,
                parity_blocks,
                block_size: BLOCK_SIZE_V2,
                distribution: indexs,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    pub fn is_valid(&self) -> bool {
        if self.deleted {
            return true;
        }

        let data_blocks = self.erasure.data_blocks;
        let parity_blocks = self.erasure.parity_blocks;

        (data_blocks >= parity_blocks)
            && (data_blocks > 0)
            && (self.erasure.index > 0
                && self.erasure.index <= data_blocks + parity_blocks
                && self.erasure.distribution.len() == (data_blocks + parity_blocks))
    }
    pub fn is_remote(&self) -> bool {
        // TODO: when lifecycle
        false
    }

    pub fn get_etag(&self) -> Option<String> {
        if let Some(meta) = &self.metadata {
            meta.get("etag").cloned()
        } else {
            None
        }
    }

    pub fn write_quorum(&self, quorum: usize) -> usize {
        if self.deleted {
            return quorum;
        }

        if self.erasure.data_blocks == self.erasure.parity_blocks {
            return self.erasure.data_blocks + 1;
        }

        self.erasure.data_blocks
    }

    pub fn marshal_msg(&self) -> Result<Vec<u8>> {
        let mut buf = Vec::new();

        self.serialize(&mut Serializer::new(&mut buf))?;

        Ok(buf)
    }

    pub fn unmarshal(buf: &[u8]) -> Result<Self> {
        let t: FileInfo = rmp_serde::from_slice(buf)?;
        Ok(t)
    }

    pub fn add_object_part(
        &mut self,
        num: usize,
        etag: Option<String>,
        part_size: usize,
        mod_time: Option<OffsetDateTime>,
        actual_size: usize,
    ) {
        let part = ObjectPartInfo {
            etag,
            number: num,
            size: part_size,
            mod_time,
            actual_size,
        };

        for p in self.parts.iter_mut() {
            if p.number == num {
                *p = part;
                return;
            }
        }

        self.parts.push(part);

        self.parts.sort_by(|a, b| a.number.cmp(&b.number));
    }

    pub fn to_object_info(&self, bucket: &str, object: &str, versioned: bool) -> ObjectInfo {
        let name = decode_dir_object(object);

        let mut version_id = self.version_id;

        if versioned && version_id.is_none() {
            version_id = Some(Uuid::nil())
        }

        let (content_type, content_encoding, etag) = {
            if let Some(ref meta) = self.metadata {
                let content_type = meta.get("content-type").cloned();
                let content_encoding = meta.get("content-encoding").cloned();
                let etag = meta.get("etag").cloned();

                (content_type, content_encoding, etag)
            } else {
                (None, None, None)
            }
        };
        let user_tags = self
            .metadata
            .as_ref()
            .map(|m| {
                if let Some(tags) = m.get(xhttp::AMZ_OBJECT_TAGGING) {
                    tags.clone()
                } else {
                    "".to_string()
                }
            })
            .unwrap_or_default();

        let inlined = self.inline_data();

        ObjectInfo {
            bucket: bucket.to_string(),
            name,
            is_dir: object.starts_with('/'),
            parity_blocks: self.erasure.parity_blocks,
            data_blocks: self.erasure.data_blocks,
            version_id,
            delete_marker: self.deleted,
            mod_time: self.mod_time,
            size: self.size,
            parts: self.parts.clone(),
            is_latest: self.is_latest,
            user_tags,
            content_type,
            content_encoding,
            num_versions: self.num_versions,
            successor_mod_time: self.successor_mod_time,
            etag,
            inlined,
            ..Default::default()
        }
    }

    // to_part_offset 取offset 所在的part index, 返回part index, offset
    pub fn to_part_offset(&self, offset: i64) -> Result<(usize, i64)> {
        if offset == 0 {
            return Ok((0, 0));
        }

        let mut part_offset = offset;
        for (i, part) in self.parts.iter().enumerate() {
            let part_index = i;
            if part_offset < part.size as i64 {
                return Ok((part_index, part_offset));
            }

            part_offset -= part.size as i64
        }

        Err(Error::msg("part not found"))
    }

    pub fn set_healing(&mut self) {
        if self.metadata.is_none() {
            self.metadata = Some(HashMap::new());
        }

        if let Some(metadata) = self.metadata.as_mut() {
            metadata.insert(RUSTFS_HEALING.to_string(), "true".to_string());
        }
    }

    pub fn set_inline_data(&mut self) {
        if let Some(meta) = self.metadata.as_mut() {
            meta.insert("x-rustfs-inline-data".to_owned(), "true".to_owned());
        } else {
            let mut meta = HashMap::new();
            meta.insert("x-rustfs-inline-data".to_owned(), "true".to_owned());
            self.metadata = Some(meta);
        }
    }
    pub fn inline_data(&self) -> bool {
        if let Some(ref meta) = self.metadata {
            if let Some(val) = meta.get("x-rustfs-inline-data") {
                val.as_str() == "true"
            } else {
                false
            }
        } else {
            false
        }
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct ObjectPartInfo {
    pub etag: Option<String>,
    pub number: usize,
    pub size: usize,
    pub actual_size: usize, // 源数据大小
    pub mod_time: Option<OffsetDateTime>,
    // pub index: Option<Vec<u8>>,
    // pub checksums: Option<std::collections::HashMap<String, String>>,
}

// impl Default for ObjectPartInfo {
//     fn default() -> Self {
//         Self {
//             number: Default::default(),
//             size: Default::default(),
//             mod_time: OffsetDateTime::UNIX_EPOCH,
//             actual_size: Default::default(),
//         }
//     }
// }

#[derive(Default, Serialize, Deserialize)]
pub struct RawFileInfo {
    pub buf: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
// ErasureInfo holds erasure coding and bitrot related information.
pub struct ErasureInfo {
    // Algorithm is the String representation of erasure-coding-algorithm
    pub algorithm: String,
    // DataBlocks is the number of data blocks for erasure-coding
    pub data_blocks: usize,
    // ParityBlocks is the number of parity blocks for erasure-coding
    pub parity_blocks: usize,
    // BlockSize is the size of one erasure-coded block
    pub block_size: usize,
    // Index is the index of the current disk
    pub index: usize,
    // Distribution is the distribution of the data and parity blocks
    pub distribution: Vec<usize>,
    // Checksums holds all bitrot checksums of all erasure encoded blocks
    pub checksums: Vec<ChecksumInfo>,
}

impl ErasureInfo {
    pub fn get_checksum_info(&self, part_number: usize) -> ChecksumInfo {
        for sum in &self.checksums {
            if sum.part_number == part_number {
                return sum.clone();
            }
        }

        ChecksumInfo {
            algorithm: DEFAULT_BITROT_ALGO,
            ..Default::default()
        }
    }

    // 算出每个分片大小
    pub fn shard_size(&self, data_size: usize) -> usize {
        data_size.div_ceil(self.data_blocks)
    }

    // returns final erasure size from original size.
    pub fn shard_file_size(&self, total_size: usize) -> usize {
        if total_size == 0 {
            return 0;
        }

        let num_shards = total_size / self.block_size;
        let last_block_size = total_size % self.block_size;
        let last_shard_size = last_block_size.div_ceil(self.data_blocks);
        num_shards * self.shard_size(self.block_size) + last_shard_size

        // // 因为写入的时候ec需要补全，所以最后一个长度应该也是一样的
        // if last_block_size != 0 {
        //     num_shards += 1
        // }
        // num_shards * self.shard_size(self.block_size)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
// ChecksumInfo - carries checksums of individual scattered parts per disk.
pub struct ChecksumInfo {
    pub part_number: usize,
    pub algorithm: BitrotAlgorithm,
    pub hash: Vec<u8>,
}

pub const DEFAULT_BITROT_ALGO: BitrotAlgorithm = BitrotAlgorithm::HighwayHash256S;

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone, Eq, Hash)]
// BitrotAlgorithm specifies a algorithm used for bitrot protection.
pub enum BitrotAlgorithm {
    // SHA256 represents the SHA-256 hash function
    SHA256,
    // HighwayHash256 represents the HighwayHash-256 hash function
    HighwayHash256,
    // HighwayHash256S represents the Streaming HighwayHash-256 hash function
    #[default]
    HighwayHash256S,
    // BLAKE2b512 represents the BLAKE2b-512 hash function
    BLAKE2b512,
}

#[derive(Debug, Default, Serialize, Deserialize)]
pub struct MakeBucketOptions {
    pub lock_enabled: bool,
    pub versioning_enabled: bool,
    pub force_create: bool,                 // Create buckets even if they are already created.
    pub created_at: Option<OffsetDateTime>, // only for site replication
    pub no_lock: bool,
}

#[derive(Debug, Default, Clone)]
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

#[derive(Debug)]
pub struct PutObjReader {
    pub stream: StreamingBlob,
    pub content_length: usize,
}

impl PutObjReader {
    pub fn new(stream: StreamingBlob, content_length: usize) -> Self {
        PutObjReader { stream, content_length }
    }

    pub fn from_vec(data: Vec<u8>) -> Self {
        let content_length = data.len();
        PutObjReader {
            stream: Body::from(data).into(),
            content_length,
        }
    }
}

pub struct GetObjectReader {
    pub stream: StreamingBlob,
    pub object_info: ObjectInfo,
}

impl GetObjectReader {
    // pub fn new(stream: StreamingBlob, object_info: ObjectInfo) -> Self {
    //     GetObjectReader { stream, object_info }
    // }
    pub async fn read_all(&mut self) -> Result<Vec<u8>> {
        let mut data = Vec::new();

        while let Some(x) = self.stream.next().await {
            let buf = match x {
                Ok(res) => res,
                Err(e) => return Err(Error::msg(e.to_string())),
            };
            data.extend_from_slice(buf.as_ref());
        }

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
    pub fn nil() -> Self {
        Self {
            is_suffix_length: false,
            start: -1,
            end: -1,
        }
    }

    pub fn is_nil(&self) -> bool {
        self.start == -1 && self.end == -1
    }
    pub fn from_object_info(oi: &ObjectInfo, part_number: usize) -> Self {
        let mut l = oi.parts.len();
        if part_number < l {
            l = part_number;
        }

        let mut start = 0;
        let mut end = -1;
        for i in 0..l {
            start = end + 1;
            end = start + oi.parts[i].size as i64 - 1
        }

        HTTPRangeSpec {
            is_suffix_length: false,
            start,
            end,
        }
    }

    pub fn get_offset_length(&self, res_size: i64) -> Result<(i64, i64)> {
        if self.start == 0 && self.end == 0 {
            return Ok((0, res_size));
        }

        let len = self.get_length(res_size)?;
        let mut start = self.start;
        if self.is_suffix_length {
            start = self.start + res_size
        }
        Ok((start, len))
    }
    pub fn get_length(&self, res_size: i64) -> Result<i64> {
        if self.is_nil() {
            return Ok(res_size);
        }

        if self.is_suffix_length {
            let specified_len = -self.start; // 假设 h.start 是一个 i64 类型
            let mut range_length = specified_len;

            if specified_len > res_size {
                range_length = res_size;
            }

            return Ok(range_length);
        }

        if self.start > res_size {
            return Err(Error::msg("The requested range is not satisfiable"));
        }

        if self.end > -1 {
            let mut end = self.end;
            if res_size <= end {
                end = res_size - 1;
            }

            let range_length = end - self.start - 1;
            return Ok(range_length);
        }

        if self.end == -1 {
            let range_length = res_size - self.start;
            return Ok(range_length);
        }

        Err(Error::msg("range value invaild"))
    }
}

#[derive(Debug, Default, Clone)]
pub struct ObjectOptions {
    // Use the maximum parity (N/2), used when saving server configuration files
    pub max_parity: bool,
    pub mod_time: Option<OffsetDateTime>,
    pub part_number: usize,

    pub delete_prefix: bool,
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

#[derive(Debug)]
pub struct MultipartUploadResult {
    pub upload_id: String,
}

#[derive(Debug)]
pub struct PartInfo {
    pub part_num: usize,
    pub last_mod: Option<OffsetDateTime>,
    pub size: usize,
    pub etag: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CompletePart {
    pub part_num: usize,
}

impl From<s3s::dto::CompletedPart> for CompletePart {
    fn from(value: s3s::dto::CompletedPart) -> Self {
        Self {
            part_num: value.part_number.unwrap_or_default() as usize,
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct ObjectInfo {
    pub bucket: String,
    pub name: String,
    pub mod_time: Option<OffsetDateTime>,
    pub size: usize,
    // Actual size is the real size of the object uploaded by client.
    pub actual_size: Option<usize>,
    pub is_dir: bool,
    pub user_defined: HashMap<String, String>,
    pub parity_blocks: usize,
    pub data_blocks: usize,
    pub version_id: Option<Uuid>,
    pub delete_marker: bool,
    pub user_tags: String,
    pub parts: Vec<ObjectPartInfo>,
    pub is_latest: bool,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub num_versions: usize,
    pub successor_mod_time: Option<OffsetDateTime>,
    // pub put_object_reader: Option<PutObjReader>,
    pub etag: Option<String>,
    pub inlined: bool,
}

impl ObjectInfo {
    pub fn is_compressed(&self) -> bool {
        self.user_defined
            .contains_key(&format!("{}compression", RESERVED_METADATA_PREFIX))
    }

    pub fn get_actual_size(&self) -> Result<usize> {
        if let Some(actual_size) = self.actual_size {
            return Ok(actual_size);
        }

        if self.is_compressed() {
            if let Some(size_str) = self.user_defined.get(&format!("{}actual-size", RESERVED_METADATA_PREFIX)) {
                if !size_str.is_empty() {
                    // Todo: deal with error
                    let size = size_str.parse::<usize>()?;
                    return Ok(size);
                }
            }
            let mut actual_size = 0;
            self.parts.iter().for_each(|part| {
                actual_size += part.actual_size;
            });
            if actual_size == 0 && actual_size != self.size {
                return Err(Error::from_string("invalid decompressed size"));
            }
            return Ok(actual_size);
        }

        // TODO: IsEncrypted

        Ok(self.size)
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
    pub next_marker: String,

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
    pub continuation_token: String,
    pub next_continuation_token: String,

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
    pub key_marker: String,

    // Together with key-marker, specifies the multipart upload after which listing
    // should begin. If key-marker is not specified, the upload-id-marker parameter
    // is ignored.
    pub upload_id_marker: String,

    // When a list is truncated, this element specifies the value that should be
    // used for the key-marker request parameter in a subsequent request.
    pub next_key_marker: String,

    // When a list is truncated, this element specifies the value that should be
    // used for the upload-id-marker request parameter in a subsequent request.
    pub next_upload_id_marker: String,

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
    pub delimiter: String,

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

pub struct ListObjectVersionsInfo {
    pub is_truncated: bool,
    pub next_marker: String,
    pub next_version_idmarker: String,
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
        range: HTTPRangeSpec,
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
        &self,
        bucket: &str,
        prefix: &str,
        continuation_token: &str,
        delimiter: &str,
        max_keys: i32,
        fetch_owner: bool,
        start_after: &str,
    ) -> Result<ListObjectsV2Info>;
    // ListObjectVersions TODO: FIXME:
    async fn list_object_versions(
        &self,
        bucket: &str,
        prefix: &str,
        marker: &str,
        version_marker: &str,
        delimiter: &str,
        max_keys: i32,
    ) -> Result<ListObjectVersionsInfo>;
    // Walk TODO:

    // GetObjectNInfo ObjectIO
    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo>;
    // PutObject ObjectIO
    // CopyObject
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
        key_marker: &str,
        upload_id_marker: &str,
        delimiter: &str,
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
        &self,
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
