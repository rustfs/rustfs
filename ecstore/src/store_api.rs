use std::{default, sync::Arc};

use anyhow::Result;
use bytes::Bytes;
use futures::Stream;
use s3s::{dto::StreamingBlob, Body};
use time::OffsetDateTime;
use tracing::debug;
use uuid::Uuid;

pub const ERASURE_ALGORITHM: &str = "rs-vandermonde";
pub const BLOCK_SIZE_V2: usize = 1048576; // 1M

#[derive(Debug, Clone)]
pub struct FileInfo {
    pub version_id: Uuid,
    pub erasure: ErasureInfo,
    pub deleted: bool,
    // DataDir of the file
    pub data_dir: Uuid,
    pub mod_time: OffsetDateTime,
    pub size: usize,
    pub data: Vec<u8>,
}

impl FileInfo {
    pub fn is_remote(&self) -> bool {
        // TODO: when lifecycle
        false
    }
}

impl Default for FileInfo {
    fn default() -> Self {
        Self {
            version_id: Uuid::nil(),
            erasure: Default::default(),
            deleted: Default::default(),
            data_dir: Uuid::nil(),
            mod_time: OffsetDateTime::UNIX_EPOCH,
            size: Default::default(),
            data: Default::default(),
        }
    }
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
                algorithm: ERASURE_ALGORITHM,
                data_blocks: data_blocks,
                parity_blocks: parity_blocks,
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
}

#[derive(Debug, Default, Clone)]
// ErasureInfo holds erasure coding and bitrot related information.
pub struct ErasureInfo {
    // Algorithm is the String representation of erasure-coding-algorithm
    pub algorithm: &'static str,
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

#[derive(Debug, Default, Clone)]
// ChecksumInfo - carries checksums of individual scattered parts per disk.
pub struct ChecksumInfo {
    pub part_number: usize,
    pub algorithm: BitrotAlgorithm,
    pub hash: Vec<u8>,
}

#[derive(Debug, Default, Clone)]
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

pub struct MakeBucketOptions {
    pub force_create: bool,
}

pub struct PutObjReader {
    pub stream: StreamingBlob,
    pub content_length: usize,
}

impl PutObjReader {
    pub fn new(stream: StreamingBlob, content_length: usize) -> Self {
        PutObjReader { stream, content_length }
    }
}

#[derive(Debug, Default)]
pub struct ObjectOptions {
    // Use the maximum parity (N/2), used when saving server configuration files
    pub max_parity: bool,
}

pub struct BucketOptions {}

#[derive(Debug, Clone)]
pub struct BucketInfo {
    pub name: String,
    pub created: OffsetDateTime,
}

#[async_trait::async_trait]
pub trait StorageAPI {
    async fn make_bucket(&self, bucket: &str, opts: &MakeBucketOptions) -> Result<()>;
    async fn get_bucket_info(&self, bucket: &str, opts: &BucketOptions) -> Result<BucketInfo>;

    async fn put_object(&self, bucket: &str, object: &str, data: PutObjReader, opts: ObjectOptions) -> Result<()>;
}
