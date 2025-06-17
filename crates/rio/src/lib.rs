mod limit_reader;

pub use limit_reader::LimitReader;

mod etag_reader;
pub use etag_reader::EtagReader;

mod compress_index;
mod compress_reader;
pub use compress_reader::{CompressReader, DecompressReader};

mod encrypt_reader;
pub use encrypt_reader::{DecryptReader, EncryptReader};

mod hardlimit_reader;
pub use hardlimit_reader::HardLimitReader;

mod hash_reader;
pub use hash_reader::*;

pub mod reader;
pub use reader::WarpReader;

mod writer;
pub use writer::*;

mod http_reader;
pub use http_reader::*;

pub use compress_index::TryGetIndex;

mod etag;

pub trait Reader: tokio::io::AsyncRead + Unpin + Send + Sync + EtagResolvable + HashReaderDetector + TryGetIndex {}

// Trait for types that can be recursively searched for etag capability
pub trait EtagResolvable {
    fn is_etag_reader(&self) -> bool {
        false
    }
    fn try_resolve_etag(&mut self) -> Option<String> {
        None
    }
}

// Generic function that can work with any EtagResolvable type
pub fn resolve_etag_generic<R>(reader: &mut R) -> Option<String>
where
    R: EtagResolvable,
{
    reader.try_resolve_etag()
}

/// Trait to detect and manipulate HashReader instances
pub trait HashReaderDetector {
    fn is_hash_reader(&self) -> bool {
        false
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        None
    }
}

impl Reader for crate::HashReader {}
impl Reader for crate::HardLimitReader {}
impl Reader for crate::EtagReader {}
impl<R> Reader for crate::CompressReader<R> where R: Reader {}
impl<R> Reader for crate::EncryptReader<R> where R: Reader {}
