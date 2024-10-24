use std::collections::HashMap;

use blake2::{Blake2b, Blake2b512};
use highway::{HighwayHash, HighwayHasher, Key};
use lazy_static::lazy_static;
use sha2::{Digest, Sha256};

use crate::{
    disk::DiskStore,
    erasure::{ReadAt, Write},
    error::Result,
    store_api::BitrotAlgorithm,
};

lazy_static! {
    static ref BITROT_ALGORITHMS: HashMap<BitrotAlgorithm, &'static str> = {
        let mut m = HashMap::new();
        m.insert(BitrotAlgorithm::SHA256, "sha256");
        m.insert(BitrotAlgorithm::BLAKE2b512, "blake2b");
        m.insert(BitrotAlgorithm::HighwayHash256, "highwayhash256");
        m.insert(BitrotAlgorithm::HighwayHash256S, "highwayhash256S");
        m
    };
}

// const MAGIC_HIGHWAY_HASH256_KEY: &[u8] = &[
//     0x4b, 0xe7, 0x34, 0xfa, 0x8e, 0x23, 0x8a, 0xcd, 0x26, 0x3e, 0x83, 0xe6, 0xbb, 0x96, 0x85, 0x52, 0x04, 0x0f, 0x93, 0x5d, 0xa3,
//     0x9f, 0x44, 0x14, 0x97, 0xe0, 0x9d, 0x13, 0x22, 0xde, 0x36, 0xa0,
// ];
const MAGIC_HIGHWAY_HASH256_KEY: &[u64; 4] = &[3, 4, 2, 1];

pub enum Hasher {
    SHA256(Sha256),
    HighwayHash256(HighwayHasher),
    BLAKE2b512(Blake2b512),
}

impl Hasher {
    pub fn update(&mut self, data: impl AsRef<[u8]>) {
        match self {
            Hasher::SHA256(core_wrapper) => {
                core_wrapper.update(data);
            }
            Hasher::HighwayHash256(highway_hasher) => {
                highway_hasher.append(data.as_ref());
            }
            Hasher::BLAKE2b512(core_wrapper) => {
                core_wrapper.update(data);
            }
        }
    }

    pub fn finalize(self) -> Vec<u8> {
        match self {
            Hasher::SHA256(core_wrapper) => core_wrapper.finalize().to_vec(),
            Hasher::HighwayHash256(highway_hasher) => highway_hasher
                .finalize256()
                .iter()
                .flat_map(|&n| n.to_le_bytes()) // 使用小端字节序转换
                .collect(),
            Hasher::BLAKE2b512(core_wrapper) => core_wrapper.finalize().to_vec(),
        }
    }
}

impl BitrotAlgorithm {
    pub fn new(&self) -> Hasher {
        match self {
            BitrotAlgorithm::SHA256 => Hasher::SHA256(Sha256::new()),
            BitrotAlgorithm::HighwayHash256 | BitrotAlgorithm::HighwayHash256S => {
                let key = Key(*MAGIC_HIGHWAY_HASH256_KEY);
                Hasher::HighwayHash256(HighwayHasher::new(key))
            }
            BitrotAlgorithm::BLAKE2b512 => Hasher::BLAKE2b512(Blake2b512::new()),
        }
    }

    pub fn available(&self) -> bool {
        BITROT_ALGORITHMS.get(self).is_some()
    }

    pub fn string(&self) -> String {
        BITROT_ALGORITHMS.get(self).map_or("".to_string(), |s| s.to_string())
    }
}

pub struct BitrotVerifier {
    algorithm: BitrotAlgorithm,
    sum: Vec<u8>,
}

impl BitrotVerifier {
    pub fn new(algorithm: BitrotAlgorithm, checksum: &[u8]) -> BitrotVerifier {
        BitrotVerifier {
            algorithm,
            sum: checksum.to_vec(),
        }
    }
}

pub fn bitrot_algorithm_from_string(s: &str) -> BitrotAlgorithm {
    for (k, v) in BITROT_ALGORITHMS.iter() {
        if *v == s {
            return k.clone();
        }
    }

    BitrotAlgorithm::HighwayHash256S
}

type BitrotWriter = Box<dyn Write>;

pub fn new_bitrot_writer(
    disk: DiskStore,
    orig_volume: &str,
    volume: &str,
    file_path: &str,
    length: usize,
    algo: BitrotAlgorithm,
    shard_size: usize,
) -> BitrotWriter {
    todo!()
}

pub struct WholeBitrotWriter {
    disk: DiskStore,
    volume: String,
    file_path: String,
    shard_size: usize,
    hash: Hasher,
}

impl WholeBitrotWriter {
    pub fn new(disk: DiskStore, volume: &str, file_path: &str, algo: BitrotAlgorithm, shard_size: usize) -> Self {
        WholeBitrotWriter {
            disk,
            volume: volume.to_string(),
            file_path: file_path.to_string(),
            shard_size,
            hash: algo.new(),
        }
    }
}

#[async_trait::async_trait]
impl Write for WholeBitrotWriter {
    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        let mut file = self.disk.append_file(&self.volume, &self.file_path).await?;
        let _ = file.write(buf).await?;
        self.hash.update(buf);

        Ok(())
    }
}

pub struct WholeBitrotReader {
    disk: DiskStore,
    volume: String,
    file_path: String,
}

#[async_trait::async_trait]
impl ReadAt for WholeBitrotReader {
    async fn read_at(&mut self, offset: usize, length: usize) -> Result<(Vec<u8>, usize)> {
        todo!()
    }
}
