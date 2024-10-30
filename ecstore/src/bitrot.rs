use std::{any::Any, collections::HashMap, io::Cursor};

use blake2::Blake2b512;
use highway::{HighwayHash, HighwayHasher, Key};
use lazy_static::lazy_static;
use sha2::{digest::core_api::BlockSizeUser, Digest, Sha256};

use crate::{
    disk::{error::DiskError, DiskStore},
    erasure::{ReadAt, Write},
    error::{Error, Result},
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

#[derive(Clone)]
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

    pub fn size(&self) -> usize {
        match self {
            Hasher::SHA256(_) => Sha256::output_size(),
            Hasher::HighwayHash256(_) => 32,
            Hasher::BLAKE2b512(_) => Blake2b512::output_size(),
        }
    }

    pub fn block_size(&self) -> usize {
        match self {
            Hasher::SHA256(_) => Sha256::block_size(),
            Hasher::HighwayHash256(_) => 64,
            Hasher::BLAKE2b512(_) => Blake2b512::block_size(),
        }
    }

    pub fn reset(&mut self) {
        match self {
            Hasher::SHA256(core_wrapper) => core_wrapper.reset(),
            Hasher::HighwayHash256(highway_hasher) => {
                let key = Key(*MAGIC_HIGHWAY_HASH256_KEY);
                *highway_hasher = HighwayHasher::new(key);
            }
            Hasher::BLAKE2b512(core_wrapper) => core_wrapper.reset(),
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
    _orig_volume: &str,
    volume: &str,
    file_path: &str,
    _length: usize,
    algo: BitrotAlgorithm,
    shard_size: usize,
) -> BitrotWriter {
    Box::new(WholeBitrotWriter::new(disk, volume, file_path, algo, shard_size))
}

type BitrotReader = Box<dyn ReadAt>;

pub fn new_bitrot_reader(
    disk: DiskStore,
    _data: &[u8],
    bucket: &str,
    file_path: &str,
    till_offset: usize,
    algo: BitrotAlgorithm,
    sum: &[u8],
    _shard_size: usize,
) -> BitrotReader {
    Box::new(WholeBitrotReader::new(disk, bucket, file_path, algo, till_offset, sum))
}

pub fn bitrot_writer_sum(w: &BitrotWriter) -> Vec<u8> {
    if let Some(w) = w.as_any().downcast_ref::<WholeBitrotWriter>() {
        return w.hash.clone().finalize();
    }

    Vec::new()
}

pub fn bitrot_shard_file_size(size: i64, _shard_size: i64, algo: BitrotAlgorithm) -> i64 {
    if algo != BitrotAlgorithm::HighwayHash256S {
        return size;
    }
    todo!()
    // ceil_frac(size, shard_size) * algo.new().size() + size
}

pub fn bitrot_verify(
    r: Cursor<Vec<u8>>,
    _want_size: usize,
    _part_size: usize,
    algo: BitrotAlgorithm,
    want: Vec<u8>,
    _shard_size: usize,
) -> Result<()> {
    if algo != BitrotAlgorithm::HighwayHash256S {
        let mut h = algo.new();
        h.update(r.into_inner());
        if h.finalize() != want {
            return Err(Error::new(DiskError::FileCorrupt));
        }
    }
    todo!()
}

pub struct WholeBitrotWriter {
    disk: DiskStore,
    volume: String,
    file_path: String,
    _shard_size: usize,
    pub hash: Hasher,
}

impl WholeBitrotWriter {
    pub fn new(disk: DiskStore, volume: &str, file_path: &str, algo: BitrotAlgorithm, shard_size: usize) -> Self {
        WholeBitrotWriter {
            disk,
            volume: volume.to_string(),
            file_path: file_path.to_string(),
            _shard_size: shard_size,
            hash: algo.new(),
        }
    }
}

#[async_trait::async_trait]
impl Write for WholeBitrotWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

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
    _verifier: BitrotVerifier,
    till_offset: usize,
    buf: Option<Vec<u8>>,
}

impl WholeBitrotReader {
    pub fn new(disk: DiskStore, volume: &str, file_path: &str, algo: BitrotAlgorithm, till_offset: usize, sum: &[u8]) -> Self {
        Self {
            disk,
            volume: volume.to_string(),
            file_path: file_path.to_string(),
            _verifier: BitrotVerifier::new(algo, sum),
            till_offset,
            buf: None,
        }
    }
}

#[async_trait::async_trait]
impl ReadAt for WholeBitrotReader {
    async fn read_at(&mut self, offset: usize, length: usize) -> Result<(Vec<u8>, usize)> {
        if self.buf.is_none() {
            let buf_len = self.till_offset - offset;
            let mut file = self.disk.read_file(&self.volume, &self.file_path).await?;
            let (buf, _) = file.read_at(offset, buf_len).await?;
            self.buf = Some(buf);
        }

        if let Some(buf) = &mut self.buf {
            if buf.len() < length {
                return Err(Error::new(DiskError::LessData));
            }

            return Ok((buf.split_off(length), length));
        }

        Err(Error::new(DiskError::LessData))
    }
}

#[cfg(test)]
mod test {
    use std::{collections::HashMap, fs};

    use hex_simd::decode_to_vec;
    use tempfile::TempDir;

    use crate::{
        bitrot::{new_bitrot_writer, BITROT_ALGORITHMS},
        disk::{endpoint::Endpoint, error::DiskError, new_disk, DiskOption},
        error::{Error, Result},
        store_api::BitrotAlgorithm,
    };

    use super::{bitrot_writer_sum, new_bitrot_reader};

    #[test]
    fn bitrot_self_test() -> Result<()> {
        let mut checksums = HashMap::new();
        checksums.insert(
            BitrotAlgorithm::SHA256,
            "a7677ff19e0182e4d52e3a3db727804abc82a5818749336369552e54b838b004",
        );
        checksums.insert(BitrotAlgorithm::BLAKE2b512, "e519b7d84b1c3c917985f544773a35cf265dcab10948be3550320d156bab612124a5ae2ae5a8c73c0eea360f68b0e28136f26e858756dbfe7375a7389f26c669");
        checksums.insert(
            BitrotAlgorithm::HighwayHash256,
            "c81c2386a1f565e805513d630d4e50ff26d11269b21c221cf50fc6c29d6ff75b",
        );
        checksums.insert(
            BitrotAlgorithm::HighwayHash256S,
            "c81c2386a1f565e805513d630d4e50ff26d11269b21c221cf50fc6c29d6ff75b",
        );

        let iter = [
            BitrotAlgorithm::SHA256,
            BitrotAlgorithm::BLAKE2b512,
            BitrotAlgorithm::HighwayHash256,
        ];

        for algo in iter.iter() {
            if !algo.available() || *algo != BitrotAlgorithm::HighwayHash256 {
                continue;
            }
            let checksum = decode_to_vec(checksums.get(algo).unwrap()).unwrap();

            let mut h = algo.new();
            let mut msg = Vec::with_capacity(h.size() * h.block_size());
            let mut sum = Vec::with_capacity(h.size());

            for _ in (0..h.size() * h.block_size()).step_by(h.size()) {
                h.update(&msg);
                sum = h.finalize();
                msg.extend(sum.clone());
                h = algo.new();
            }

            if checksum != sum {
                println!("failed: {:?}, expect: {:?}, actual: {:?}", algo, checksum, sum);
                return Err(Error::new(DiskError::FileCorrupt));
            }
            println!("success: {:?}", algo);
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_all_bitrot_algorithms() -> Result<()> {
        for algo in BITROT_ALGORITHMS.keys() {
            if *algo == BitrotAlgorithm::HighwayHash256S {
                continue;
            }
            test_bitrot_reader_writer_algo(algo.clone()).await?;
        }

        Ok(())
    }

    async fn test_bitrot_reader_writer_algo(algo: BitrotAlgorithm) -> Result<()> {
        let temp_dir = TempDir::new().unwrap().path().to_string_lossy().to_string();
        fs::create_dir_all(&temp_dir)?;
        let volume = "testvol";
        let file_path = "testfile";

        let ep = Endpoint::try_from(temp_dir.as_str())?;
        let opt = DiskOption::default();
        let disk = new_disk(&ep, &opt).await?;
        let _ = disk.make_volume(volume).await?;
        let mut writer = new_bitrot_writer(disk.clone(), "", volume, file_path, 35, algo.clone(), 10);

        let _ = writer.write(b"aaaaaaaaaa").await?;
        let _ = writer.write(b"aaaaaaaaaa").await?;
        let _ = writer.write(b"aaaaaaaaaa").await?;
        let _ = writer.write(b"aaaaa").await?;

        let mut reader = new_bitrot_reader(disk, b"", volume, file_path, 35, algo, &bitrot_writer_sum(&writer), 10);
        let read_len = 10;
        (_, _) = reader.read_at(0, read_len).await?;
        (_, _) = reader.read_at(0, read_len).await?;
        (_, _) = reader.read_at(0, read_len).await?;
        (_, _) = reader.read_at(0, read_len / 2).await?;

        Ok(())
    }
}
