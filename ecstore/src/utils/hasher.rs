use sha2::{
    digest::{Reset, Update},
    Digest, Sha256 as sha_sha256,
};
trait Hasher {
    fn write(&mut self, bytes: &[u8]);
    fn reset(&mut self);
    fn sum(&mut self) -> impl AsRef<[u8]>;
    fn size(&self) -> usize;
    fn block_size(&self) -> usize;
}

struct Sha256 {
    hasher: sha_sha256,
}

impl Sha256 {
    pub fn new() -> Self {
        Self {
            hasher: sha_sha256::new(),
        }
    }
}

impl Hasher for Sha256 {
    fn write(&mut self, bytes: &[u8]) {
        Update::update(&mut self.hasher, bytes);
    }

    fn reset(&mut self) {
        Reset::reset(&mut self.hasher);
    }

    fn sum(&mut self) -> impl AsRef<[u8]> {
        self.hasher.clone().finalize()
    }

    fn size(&self) -> usize {
        32
    }

    fn block_size(&self) -> usize {
        64
    }
}
