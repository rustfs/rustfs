mod limit_reader;
use std::io::Cursor;

pub use limit_reader::LimitReader;

mod etag_reader;
pub use etag_reader::EtagReader;

mod compress_reader;
pub use compress_reader::{CompressReader, DecompressReader};

mod encrypt_reader;
pub use encrypt_reader::{DecryptReader, EncryptReader};

mod hardlimit_reader;
pub use hardlimit_reader::HardLimitReader;

mod hash_reader;
pub use hash_reader::*;

pub mod compress;

pub mod reader;
pub use reader::WarpReader;

mod writer;
use tokio::io::{AsyncRead, BufReader};
pub use writer::*;

mod http_reader;
pub use http_reader::*;

mod etag;

pub trait Reader: tokio::io::AsyncRead + Unpin + Send + Sync + EtagResolvable + HashReaderDetector {}

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

impl<T> EtagResolvable for BufReader<T> where T: AsyncRead + Unpin + Send + Sync {}

impl<T> EtagResolvable for Cursor<T> where T: AsRef<[u8]> + Unpin + Send + Sync {}

impl<T> EtagResolvable for Box<T> where T: EtagResolvable {}

/// Trait to detect and manipulate HashReader instances
pub trait HashReaderDetector {
    fn is_hash_reader(&self) -> bool {
        false
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        None
    }
}

impl<T> HashReaderDetector for tokio::io::BufReader<T> where T: AsyncRead + Unpin + Send + Sync {}

impl<T> HashReaderDetector for std::io::Cursor<T> where T: AsRef<[u8]> + Unpin + Send + Sync {}

impl HashReaderDetector for Box<dyn AsyncRead + Unpin + Send + Sync> {}

impl<T> HashReaderDetector for Box<T> where T: HashReaderDetector {}

// Blanket implementations for Reader trait
impl<T> Reader for tokio::io::BufReader<T> where T: AsyncRead + Unpin + Send + Sync {}

impl<T> Reader for std::io::Cursor<T> where T: AsRef<[u8]> + Unpin + Send + Sync {}

impl<T> Reader for Box<T> where T: Reader {}

// Forward declarations for wrapper types that implement all required traits
impl Reader for crate::HashReader {}

impl Reader for HttpReader {}

impl Reader for crate::HardLimitReader {}
impl Reader for crate::EtagReader {}

impl<R> Reader for crate::EncryptReader<R> where R: Reader {}

impl<R> Reader for crate::DecryptReader<R> where R: Reader {}

impl<R> Reader for crate::CompressReader<R> where R: Reader {}

impl<R> Reader for crate::DecompressReader<R> where R: Reader {}

impl Reader for tokio::fs::File {}
impl HashReaderDetector for tokio::fs::File {}
impl EtagResolvable for tokio::fs::File {}

impl Reader for tokio::io::DuplexStream {}
impl HashReaderDetector for tokio::io::DuplexStream {}
impl EtagResolvable for tokio::io::DuplexStream {}
