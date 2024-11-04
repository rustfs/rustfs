use md5::{Digest as Md5Digest, Md5};
use sha2::{
    digest::{Reset, Update},
    Digest, Sha256 as sha_sha256,
};
pub trait Hasher {
    fn write(&mut self, bytes: &[u8]);
    fn reset(&mut self);
    fn sum(&mut self) -> String;
    fn size(&self) -> usize;
    fn block_size(&self) -> usize;
}

#[derive(Default)]
pub enum HashType {
    #[default]
    Undefined,
    Uuid(Uuid),
    Md5(MD5),
    Sha256(Sha256),
}

impl Hasher for HashType {
    fn write(&mut self, bytes: &[u8]) {
        match self {
            HashType::Md5(md5) => md5.write(bytes),
            HashType::Sha256(sha256) => sha256.write(bytes),
            HashType::Uuid(uuid) => uuid.write(bytes),
            HashType::Undefined => (),
        }
    }

    fn reset(&mut self) {
        match self {
            HashType::Md5(md5) => md5.reset(),
            HashType::Sha256(sha256) => sha256.reset(),
            HashType::Uuid(uuid) => uuid.reset(),
            HashType::Undefined => (),
        }
    }

    fn sum(&mut self) -> String {
        match self {
            HashType::Md5(md5) => md5.sum(),
            HashType::Sha256(sha256) => sha256.sum(),
            HashType::Uuid(uuid) => uuid.sum(),
            HashType::Undefined => "".to_owned(),
        }
    }

    fn size(&self) -> usize {
        match self {
            HashType::Md5(md5) => md5.size(),
            HashType::Sha256(sha256) => sha256.size(),
            HashType::Uuid(uuid) => uuid.size(),
            HashType::Undefined => 0,
        }
    }

    fn block_size(&self) -> usize {
        match self {
            HashType::Md5(md5) => md5.block_size(),
            HashType::Sha256(sha256) => sha256.block_size(),
            HashType::Uuid(uuid) => uuid.block_size(),
            HashType::Undefined => 64,
        }
    }
}

pub struct Sha256 {
    hasher: sha_sha256,
}

impl Sha256 {
    pub fn new() -> Self {
        Self {
            hasher: sha_sha256::new(),
        }
    }
}
impl Default for Sha256 {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher for Sha256 {
    fn write(&mut self, bytes: &[u8]) {
        Update::update(&mut self.hasher, bytes);
    }

    fn reset(&mut self) {
        Reset::reset(&mut self.hasher);
    }

    fn sum(&mut self) -> String {
        hex_simd::encode_to_string(self.hasher.clone().finalize(), hex_simd::AsciiCase::Lower)
    }

    fn size(&self) -> usize {
        32
    }

    fn block_size(&self) -> usize {
        64
    }
}

pub struct MD5 {
    hasher: Md5,
}

impl MD5 {
    pub fn new() -> Self {
        Self { hasher: Md5::new() }
    }
}
impl Default for MD5 {
    fn default() -> Self {
        Self::new()
    }
}

impl Hasher for MD5 {
    fn write(&mut self, bytes: &[u8]) {
        self.hasher.update(bytes);
    }

    fn reset(&mut self) {}

    fn sum(&mut self) -> String {
        hex_simd::encode_to_string(self.hasher.clone().finalize(), hex_simd::AsciiCase::Lower)
    }

    fn size(&self) -> usize {
        32
    }

    fn block_size(&self) -> usize {
        64
    }
}

pub struct Uuid {
    id: String,
}

impl Uuid {
    pub fn new(id: String) -> Self {
        Self { id }
    }
}

impl Hasher for Uuid {
    fn write(&mut self, _bytes: &[u8]) {}

    fn reset(&mut self) {}

    fn sum(&mut self) -> String {
        self.id.clone()
    }

    fn size(&self) -> usize {
        self.id.len()
    }

    fn block_size(&self) -> usize {
        64
    }
}
