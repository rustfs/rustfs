use crate::{
    disk::{error::DiskError, DiskStore, FileReader, FileWriter},
    erasure::{ReadAt, Write},
    error::{Error, Result},
    store_api::BitrotAlgorithm,
};
use blake2::Blake2b512;
use blake2::Digest as _;
use bytes::Bytes;
use highway::{HighwayHash, HighwayHasher, Key};
use lazy_static::lazy_static;
use sha2::{digest::core_api::BlockSizeUser, Digest, Sha256};
use std::{
    any::Any,
    collections::HashMap,
    io::{Cursor, Read},
};

use tokio::{
    io::AsyncWriteExt,
    spawn,
    sync::{
        mpsc::{self, Sender},
        RwLock,
    },
    task::JoinHandle,
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

#[derive(Clone, Debug)]
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
            Hasher::BLAKE2b512(_) => 64,
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

#[derive(Debug)]
pub struct BitrotVerifier {
    _algorithm: BitrotAlgorithm,
    _sum: Vec<u8>,
}

impl BitrotVerifier {
    pub fn new(algorithm: BitrotAlgorithm, checksum: &[u8]) -> BitrotVerifier {
        BitrotVerifier {
            _algorithm: algorithm,
            _sum: checksum.to_vec(),
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

pub type BitrotWriter = Box<dyn Write + Send + 'static>;

pub async fn new_bitrot_writer(
    disk: DiskStore,
    orig_volume: &str,
    volume: &str,
    file_path: &str,
    length: usize,
    algo: BitrotAlgorithm,
    shard_size: usize,
) -> Result<BitrotWriter> {
    if algo == BitrotAlgorithm::HighwayHash256S {
        return Ok(Box::new(
            StreamingBitrotWriter::new(disk, orig_volume, volume, file_path, length, algo, shard_size).await?,
        ));
    }
    Ok(Box::new(WholeBitrotWriter::new(disk, volume, file_path, algo, shard_size)))
}

pub type BitrotReader = Box<dyn ReadAt + Send>;

pub fn new_bitrot_reader(
    disk: DiskStore,
    data: &[u8],
    bucket: &str,
    file_path: &str,
    till_offset: usize,
    algo: BitrotAlgorithm,
    sum: &[u8],
    shard_size: usize,
) -> BitrotReader {
    if algo == BitrotAlgorithm::HighwayHash256S {
        return Box::new(StreamingBitrotReader::new(disk, data, bucket, file_path, algo, till_offset, shard_size));
    }
    Box::new(WholeBitrotReader::new(disk, bucket, file_path, algo, till_offset, sum))
}

pub async fn close_bitrot_writers(writers: &mut [Option<BitrotWriter>]) -> Result<()> {
    for w in writers.into_iter() {
        if let Some(w) = w {
            let _ = w.close().await?;
        }
    }

    Ok(())
}

pub fn bitrot_writer_sum(w: &BitrotWriter) -> Vec<u8> {
    if let Some(w) = w.as_any().downcast_ref::<WholeBitrotWriter>() {
        return w.hash.clone().finalize();
    }

    Vec::new()
}

pub fn bitrot_shard_file_size(size: usize, shard_size: usize, algo: BitrotAlgorithm) -> usize {
    if algo != BitrotAlgorithm::HighwayHash256S {
        return size;
    }
    size.div_ceil(shard_size) * algo.new().size() + size
}

pub fn bitrot_verify(
    r: &mut Cursor<Vec<u8>>,
    want_size: usize,
    part_size: usize,
    algo: BitrotAlgorithm,
    want: Vec<u8>,
    mut shard_size: usize,
) -> Result<()> {
    if algo != BitrotAlgorithm::HighwayHash256S {
        let mut h = algo.new();
        h.update(r.get_ref());
        if h.finalize() != want {
            return Err(Error::new(DiskError::FileCorrupt));
        }

        return Ok(());
    }
    let mut h = algo.new();
    let mut hash_buf = vec![0; h.size()];
    let mut left = want_size;

    if left != bitrot_shard_file_size(part_size, shard_size, algo) {
        return Err(Error::new(DiskError::FileCorrupt));
    }

    while left > 0 {
        h.reset();
        let n = r.read(&mut hash_buf)?;
        left -= n;

        if left < shard_size {
            shard_size = left;
        }

        let mut buf = vec![0; shard_size];
        let read = r.read(&mut buf)?;
        h.update(buf);
        left -= read;
        if h.clone().finalize() != hash_buf[0..n] {
            return Err(Error::new(DiskError::FileCorrupt));
        }
    }

    Ok(())
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

#[derive(Debug)]
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

            return Ok((buf.drain(0..length).collect::<Vec<_>>(), length));
        }

        Err(Error::new(DiskError::LessData))
    }
}

struct StreamingBitrotWriter {
    hasher: Hasher,
    tx: Sender<Option<Vec<u8>>>,
    task: Option<JoinHandle<()>>,
}

impl StreamingBitrotWriter {
    pub async fn new(
        disk: DiskStore,
        orig_volume: &str,
        volume: &str,
        file_path: &str,
        length: usize,
        algo: BitrotAlgorithm,
        shard_size: usize,
    ) -> Result<Self> {
        let hasher = algo.new();
        let (tx, mut rx) = mpsc::channel::<Option<Vec<u8>>>(10);

        let total_file_size = length.div_ceil(shard_size) * hasher.size() + length;
        let mut writer = disk.create_file(orig_volume, volume, file_path, total_file_size).await?;

        let task = spawn(async move {
            loop {
                if let Some(Some(buf)) = rx.recv().await {
                    let _ = writer.write(&buf).await.unwrap();
                    continue;
                }

                break;
            }
        });

        Ok(StreamingBitrotWriter {
            hasher,
            tx,
            task: Some(task),
        })
    }
}

#[async_trait::async_trait]
impl Write for StreamingBitrotWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        self.hasher.reset();
        self.hasher.update(&buf);
        let hash_bytes = self.hasher.clone().finalize();
        let _ = self.tx.send(Some(hash_bytes)).await?;
        let _ = self.tx.send(Some(buf.to_vec())).await?;

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        let _ = self.tx.send(None).await?;
        if let Some(task) = self.task.take() {
            let _ = task.await; // 等待任务完成
        }
        Ok(())
    }
}

#[derive(Debug)]
struct StreamingBitrotReader {
    disk: DiskStore,
    _data: Vec<u8>,
    volume: String,
    file_path: String,
    till_offset: usize,
    curr_offset: usize,
    hasher: Hasher,
    shard_size: usize,
    buf: Vec<u8>,
    hash_bytes: Vec<u8>,
}

impl StreamingBitrotReader {
    pub fn new(
        disk: DiskStore,
        data: &[u8],
        volume: &str,
        file_path: &str,
        algo: BitrotAlgorithm,
        till_offset: usize,
        shard_size: usize,
    ) -> Self {
        let hasher = algo.new();
        Self {
            disk,
            _data: data.to_vec(),
            volume: volume.to_string(),
            file_path: file_path.to_string(),
            till_offset: till_offset.div_ceil(shard_size) * hasher.size() + till_offset,
            curr_offset: 0,
            hash_bytes: Vec::with_capacity(hasher.size()),
            hasher,
            shard_size,
            buf: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
impl ReadAt for StreamingBitrotReader {
    async fn read_at(&mut self, offset: usize, length: usize) -> Result<(Vec<u8>, usize)> {
        if offset % self.shard_size != 0 {
            return Err(Error::new(DiskError::Unexpected));
        }
        if self.buf.is_empty() {
            self.curr_offset = offset;
            let stream_offset = (offset / self.shard_size) * self.hasher.size() + offset;
            let buf_len = self.till_offset - stream_offset;
            let mut file = self.disk.read_file(&self.volume, &self.file_path).await?;
            let (buf, _) = file.read_at(stream_offset, buf_len).await?;
            self.buf = buf;
        }
        if offset != self.curr_offset {
            return Err(Error::new(DiskError::Unexpected));
        }

        self.hash_bytes = self.buf.drain(0..self.hash_bytes.capacity()).collect();
        let buf = self.buf.drain(0..length).collect::<Vec<_>>();
        self.hasher.reset();
        self.hasher.update(&buf);
        let actual = self.hasher.clone().finalize();
        if actual != self.hash_bytes {
            return Err(Error::new(DiskError::FileCorrupt));
        }

        let readed_len = buf.len();
        self.curr_offset += readed_len;

        Ok((buf, readed_len))
    }
}

pub struct BitrotFileWriter {
    pub inner: FileWriter,
    hasher: Hasher,
    _shard_size: usize,
}

impl BitrotFileWriter {
    pub fn new(inner: FileWriter, algo: BitrotAlgorithm, _shard_size: usize) -> Self {
        let hasher = algo.new();
        Self {
            inner,
            hasher,
            _shard_size,
        }
    }

    pub fn writer(&self) -> &FileWriter {
        &self.inner
    }
}

#[async_trait::async_trait]
impl Write for BitrotFileWriter {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn write(&mut self, buf: &[u8]) -> Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        self.hasher.reset();
        self.hasher.update(&buf);
        let hash_bytes = self.hasher.clone().finalize();

        let _ = self.inner.write(&hash_bytes).await?;
        let _ = self.inner.write(buf).await?;

        Ok(())
    }
}

pub fn new_bitrot_filewriter(inner: FileWriter, algo: BitrotAlgorithm, shard_size: usize) -> Result<BitrotWriter> {
    Ok(Box::new(BitrotFileWriter::new(inner, algo, shard_size)))
}

#[derive(Debug)]
struct BitrotFileReader {
    pub inner: FileReader,
    till_offset: usize,
    curr_offset: usize,
    hasher: Hasher,
    shard_size: usize,
    buf: Vec<u8>,
    hash_bytes: Vec<u8>,
}

impl BitrotFileReader {
    pub fn new(inner: FileReader, algo: BitrotAlgorithm, till_offset: usize, shard_size: usize) -> Self {
        let hasher = algo.new();
        Self {
            inner,
            till_offset: till_offset.div_ceil(shard_size) * hasher.size() + till_offset,
            curr_offset: 0,
            hash_bytes: Vec::with_capacity(hasher.size()),
            hasher,
            shard_size,
            buf: Vec::new(),
        }
    }
}

#[async_trait::async_trait]
impl ReadAt for BitrotFileReader {
    async fn read_at(&mut self, offset: usize, length: usize) -> Result<(Vec<u8>, usize)> {
        if offset % self.shard_size != 0 {
            return Err(Error::new(DiskError::Unexpected));
        }
        if self.buf.is_empty() {
            self.curr_offset = offset;
            let stream_offset = (offset / self.shard_size) * self.hasher.size() + offset;
            let buf_len = self.till_offset - stream_offset;
            let (buf, _) = self.inner.read_at(stream_offset, buf_len).await?;
            self.buf = buf;
        }
        if offset != self.curr_offset {
            return Err(Error::new(DiskError::Unexpected));
        }

        self.hash_bytes = self.buf.drain(0..self.hash_bytes.capacity()).collect();
        let buf = self.buf.drain(0..length).collect::<Vec<_>>();
        self.hasher.reset();
        self.hasher.update(&buf);
        let actual = self.hasher.clone().finalize();
        if actual != self.hash_bytes {
            return Err(Error::new(DiskError::FileCorrupt));
        }

        let readed_len = buf.len();
        self.curr_offset += readed_len;

        Ok((buf, readed_len))
    }
}

pub fn new_bitrot_filereader(inner: FileReader, till_offset: usize, algo: BitrotAlgorithm, shard_size: usize) -> BitrotReader {
    Box::new(BitrotFileReader::new(inner, algo, till_offset, shard_size))
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
                return Err(Error::new(DiskError::FileCorrupt));
            }
        }

        Ok(())
    }

    #[tokio::test]
    async fn test_all_bitrot_algorithms() -> Result<()> {
        for algo in BITROT_ALGORITHMS.keys() {
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
        let mut writer = new_bitrot_writer(disk.clone(), "", volume, file_path, 35, algo.clone(), 10).await?;

        let _ = writer.write(b"aaaaaaaaaa").await?;
        let _ = writer.write(b"aaaaaaaaaa").await?;
        let _ = writer.write(b"aaaaaaaaaa").await?;
        let _ = writer.write(b"aaaaa").await?;

        let sum = bitrot_writer_sum(&writer);
        let _ = writer.close().await?;

        let mut reader = new_bitrot_reader(disk, b"", volume, file_path, 35, algo, &sum, 10);
        let read_len = 10;
        let mut result: Vec<u8>;
        (result, _) = reader.read_at(0, read_len).await?;
        assert_eq!(result, b"aaaaaaaaaa");
        (result, _) = reader.read_at(10, read_len).await?;
        assert_eq!(result, b"aaaaaaaaaa");
        (result, _) = reader.read_at(20, read_len).await?;
        assert_eq!(result, b"aaaaaaaaaa");
        (result, _) = reader.read_at(30, read_len / 2).await?;
        assert_eq!(result, b"aaaaa");

        Ok(())
    }
}
