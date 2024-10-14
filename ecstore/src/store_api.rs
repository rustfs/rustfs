use std::collections::HashMap;

use crate::error::{Error, Result};
use futures::StreamExt;
use http::HeaderMap;
use rmp_serde::Serializer;
use s3s::dto::StreamingBlob;
use serde::{Deserialize, Serialize};
use time::OffsetDateTime;
use uuid::Uuid;

pub const ERASURE_ALGORITHM: &str = "rs-vandermonde";
pub const BLOCK_SIZE_V2: usize = 1048576; // 1M

// #[derive(Debug, Clone)]
#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct FileInfo {
    pub name: String,
    pub volume: String,
    pub version_id: Option<Uuid>,
    pub erasure: ErasureInfo,
    pub deleted: bool,
    // DataDir of the file
    pub data_dir: Option<Uuid>,
    pub mod_time: Option<OffsetDateTime>,
    pub size: usize,
    pub data: Option<Vec<u8>>,
    pub fresh: bool, // indicates this is a first time call to write FileInfo.
    pub parts: Vec<ObjectPartInfo>,
    pub is_latest: bool,
    // #[serde(skip_serializing_if = "Option::is_none", default)]
    pub tags: Option<HashMap<String, String>>,
    pub metadata: Option<HashMap<String, String>>,
    pub num_versions: usize,
}

// impl Default for FileInfo {
//     fn default() -> Self {
//         Self {
//             version_id: Default::default(),
//             erasure: Default::default(),
//             deleted: Default::default(),
//             data_dir: Default::default(),
//             mod_time: None,
//             size: Default::default(),
//             data: Default::default(),
//             fresh: Default::default(),
//             name: Default::default(),
//             volume: Default::default(),
//             parts: Default::default(),
//             is_latest: Default::default(),
//         }
//     }
// }

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

    pub fn add_object_part(&mut self, num: usize, part_size: usize, mod_time: Option<OffsetDateTime>, actual_size: usize) {
        let part = ObjectPartInfo {
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

    pub fn to_object_info(&self, bucket: &str, object: &str, _versioned: bool) -> ObjectInfo {
        ObjectInfo {
            bucket: bucket.to_string(),
            name: object.to_string(),
            is_dir: object.starts_with('/'),
            parity_blocks: self.erasure.parity_blocks,
            data_blocks: self.erasure.data_blocks,
            version_id: self.version_id,
            delete_marker: self.deleted,
            mod_time: self.mod_time,
            size: self.size,
            parts: self.parts.clone(),
            is_latest: self.is_latest,
            tags: self.tags.clone(),
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
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone, Default)]
pub struct ObjectPartInfo {
    // pub etag: Option<String>,
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

#[derive(Serialize, Deserialize)]
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

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
// ChecksumInfo - carries checksums of individual scattered parts per disk.
pub struct ChecksumInfo {
    pub part_number: usize,
    pub algorithm: BitrotAlgorithm,
    pub hash: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Default, Clone)]
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

pub struct DeleteBucketOptions {
    pub force: bool, // Force deletion
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
    pub version_id: String,
    pub no_lock: bool,
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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BucketInfo {
    pub name: String,
    pub created: Option<OffsetDateTime>,
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
}

#[derive(Debug)]
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

#[derive(Debug, Default)]
pub struct ObjectInfo {
    pub bucket: String,
    pub name: String,
    pub mod_time: Option<OffsetDateTime>,
    pub size: usize,
    pub is_dir: bool,
    pub parity_blocks: usize,
    pub data_blocks: usize,
    pub version_id: Option<Uuid>,
    pub delete_marker: bool,
    pub parts: Vec<ObjectPartInfo>,
    pub is_latest: bool,
    pub tags: Option<HashMap<String, String>>,
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

#[async_trait::async_trait]
pub trait ObjectIO: Send + Sync + 'static {
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: HTTPRangeSpec,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader>;
    async fn put_object(&self, bucket: &str, object: &str, data: PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo>;
}

#[async_trait::async_trait]
pub trait StorageAPI: ObjectIO {
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()>;
    async fn delete_bucket(&self, bucket: &str, opts: &DeleteBucketOptions) -> Result<()>;
    async fn list_bucket(&self, opts: &BucketOptions) -> Result<Vec<BucketInfo>>;
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo>;
    async fn delete_object(&self, bucket: &str, object: &str, opts: ObjectOptions) -> Result<ObjectInfo>;
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> Result<(Vec<DeletedObject>, Vec<Option<Error>>)>;
    #[warn(clippy::too_many_arguments)]
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
    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo>;

    async fn put_object_info(&self, bucket: &str, object: &str, info: ObjectInfo, opts: &ObjectOptions) -> Result<()>;

    // async fn get_object_reader(
    //     &self,
    //     bucket: &str,
    //     object: &str,
    //     range: HTTPRangeSpec,
    //     h: HeaderMap,
    //     opts: &ObjectOptions,
    // ) -> Result<GetObjectReader>;
    // async fn put_object(&self, bucket: &str, object: &str, data: PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo>;
    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo>;
    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult>;
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, opts: &ObjectOptions) -> Result<()>;
    async fn complete_multipart_upload(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo>;
}
