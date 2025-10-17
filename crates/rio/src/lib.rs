// Copyright 2024 RustFS Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Default encryption block size - aligned with system default read buffer size (1MB)
pub const DEFAULT_ENCRYPTION_BLOCK_SIZE: usize = 1024 * 1024;

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
impl<R> Reader for crate::LimitReader<R> where R: Reader {}
impl<R> Reader for crate::CompressReader<R> where R: Reader {}
impl<R> Reader for crate::EncryptReader<R> where R: Reader {}
impl<R> Reader for crate::DecryptReader<R> where R: Reader {}
impl EtagResolvable for Box<dyn Reader> {
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.as_mut().try_resolve_etag()
    }
}

impl HashReaderDetector for Box<dyn Reader> {
    fn is_hash_reader(&self) -> bool {
        self.as_ref().is_hash_reader()
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        self.as_mut().as_hash_reader_mut()
    }
}

impl TryGetIndex for Box<dyn Reader> {
    fn try_get_index(&self) -> Option<&compress_index::Index> {
        self.as_ref().try_get_index()
    }
}

impl Reader for Box<dyn Reader> {}
