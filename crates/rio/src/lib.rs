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

macro_rules! delegate_reader_capabilities_generic {
    ($name:ident<$inner_ty:ident>, $inner:ident) => {
        impl<$inner_ty> crate::EtagResolvable for $name<$inner_ty>
        where
            $inner_ty: crate::EtagResolvable,
        {
            fn try_resolve_etag(&mut self) -> Option<String> {
                self.$inner.try_resolve_etag()
            }
        }

        impl<$inner_ty> crate::HashReaderDetector for $name<$inner_ty>
        where
            $inner_ty: crate::HashReaderDetector,
        {
            fn is_hash_reader(&self) -> bool {
                self.$inner.is_hash_reader()
            }

            fn as_hash_reader_mut(&mut self) -> Option<&mut dyn crate::HashReaderMut> {
                self.$inner.as_hash_reader_mut()
            }
        }

        impl<$inner_ty> crate::TryGetIndex for $name<$inner_ty>
        where
            $inner_ty: crate::TryGetIndex,
        {
            fn try_get_index(&self) -> Option<&crate::compress_index::Index> {
                self.$inner.try_get_index()
            }
        }
    };
}

macro_rules! delegate_reader_capabilities_generic_no_index {
    ($name:ident<$inner_ty:ident>, $inner:ident) => {
        impl<$inner_ty> crate::EtagResolvable for $name<$inner_ty>
        where
            $inner_ty: crate::EtagResolvable,
        {
            fn try_resolve_etag(&mut self) -> Option<String> {
                self.$inner.try_resolve_etag()
            }
        }

        impl<$inner_ty> crate::HashReaderDetector for $name<$inner_ty>
        where
            $inner_ty: crate::HashReaderDetector,
        {
            fn is_hash_reader(&self) -> bool {
                self.$inner.is_hash_reader()
            }

            fn as_hash_reader_mut(&mut self) -> Option<&mut dyn crate::HashReaderMut> {
                self.$inner.as_hash_reader_mut()
            }
        }
    };
}

mod limit_reader;

pub use limit_reader::LimitReader;

mod etag_reader;
pub use etag_reader::EtagReader;

mod compress_index;
mod compress_reader;
pub use compress_reader::{CompressReader, DecompressReader};

mod encrypt_reader;
pub use encrypt_reader::{DecryptReader, EncryptReader, multipart_part_nonce};

mod hardlimit_reader;
pub use hardlimit_reader::HardLimitReader;

mod hash_reader;
pub use hash_reader::*;
mod checksum;
pub use checksum::*;

mod errors;
pub use errors::*;

pub mod reader;
pub use reader::WarpReader;

mod writer;
pub use writer::*;

mod http_reader;
pub use http_reader::*;

pub use compress_index::{Index, TryGetIndex};

mod etag;

pub trait ReadStream: tokio::io::AsyncRead + Unpin + Send + Sync {}
impl<T> ReadStream for T where T: tokio::io::AsyncRead + Unpin + Send + Sync {}

pub trait ReaderCapabilities: EtagResolvable + HashReaderDetector + TryGetIndex {}
impl<T> ReaderCapabilities for T where T: EtagResolvable + HashReaderDetector + TryGetIndex {}

pub trait Reader: ReadStream + ReaderCapabilities {}
impl<T> Reader for T where T: ReadStream + ReaderCapabilities {}

pub type DynReader = Box<dyn Reader>;

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

pub fn boxed_reader<R>(reader: R) -> DynReader
where
    R: Reader + 'static,
{
    Box::new(reader)
}

pub fn wrap_reader<R>(reader: R) -> DynReader
where
    R: ReadStream + 'static,
{
    boxed_reader(WarpReader::new(reader))
}

impl<T> EtagResolvable for Box<T>
where
    T: EtagResolvable + ?Sized,
{
    fn try_resolve_etag(&mut self) -> Option<String> {
        self.as_mut().try_resolve_etag()
    }
}

impl<T> HashReaderDetector for Box<T>
where
    T: HashReaderDetector + ?Sized,
{
    fn is_hash_reader(&self) -> bool {
        self.as_ref().is_hash_reader()
    }

    fn as_hash_reader_mut(&mut self) -> Option<&mut dyn HashReaderMut> {
        self.as_mut().as_hash_reader_mut()
    }
}

impl<T> TryGetIndex for Box<T>
where
    T: TryGetIndex + ?Sized,
{
    fn try_get_index(&self) -> Option<&compress_index::Index> {
        self.as_ref().try_get_index()
    }
}
