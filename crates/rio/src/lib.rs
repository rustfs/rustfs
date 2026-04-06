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

use std::future::Future;
use std::pin::Pin;
use tokio::io::AsyncReadExt;

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
pub use encrypt_reader::{DecryptReader, EncryptReader};

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

pub type BoxReadBlockFuture<'a> = Pin<Box<dyn Future<Output = std::io::Result<usize>> + Send + 'a>>;

pub trait BlockReadable {
    fn read_block<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxReadBlockFuture<'a>;
}

fn read_block_via_async_read<'a, R>(reader: &'a mut R, buf: &'a mut [u8]) -> BoxReadBlockFuture<'a>
where
    R: tokio::io::AsyncRead + Unpin + Send + Sync + 'a,
{
    Box::pin(async move {
        let mut total = 0;

        while total < buf.len() {
            match reader.read(&mut buf[total..]).await {
                Ok(0) => return Ok(total),
                Ok(n) => total += n,
                Err(err) => return Err(err),
            }
        }

        Ok(total)
    })
}

pub trait ReadStream: tokio::io::AsyncRead + Unpin + Send + Sync {}
impl<T> ReadStream for T where T: tokio::io::AsyncRead + Unpin + Send + Sync {}

pub trait ReaderCapabilities: EtagResolvable + HashReaderDetector + TryGetIndex {}
impl<T> ReaderCapabilities for T where T: EtagResolvable + HashReaderDetector + TryGetIndex {}

pub trait Reader: ReadStream + ReaderCapabilities + BlockReadable {}
impl<T> Reader for T where T: ReadStream + ReaderCapabilities + BlockReadable {}

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

impl<R> BlockReadable for crate::WarpReader<R>
where
    R: tokio::io::AsyncRead + Unpin + Send + Sync,
{
    fn read_block<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxReadBlockFuture<'a> {
        read_block_via_async_read(self, buf)
    }
}

impl<R> BlockReadable for tokio::io::BufReader<R>
where
    R: tokio::io::AsyncRead + Unpin + Send + Sync,
{
    fn read_block<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxReadBlockFuture<'a> {
        read_block_via_async_read(self, buf)
    }
}

impl<R> BlockReadable for crate::LimitReader<R>
where
    R: Reader,
{
    fn read_block<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxReadBlockFuture<'a> {
        read_block_via_async_read(self, buf)
    }
}

impl<R> BlockReadable for crate::DecryptReader<R>
where
    R: Reader,
{
    fn read_block<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxReadBlockFuture<'a> {
        read_block_via_async_read(self, buf)
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

impl<T> BlockReadable for Box<T>
where
    T: BlockReadable + ?Sized,
{
    fn read_block<'a>(&'a mut self, buf: &'a mut [u8]) -> BoxReadBlockFuture<'a> {
        self.as_mut().read_block(buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::io::{self, ErrorKind};
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::{AsyncRead, ReadBuf};

    enum ReadStep {
        Data(Vec<u8>),
        Error(ErrorKind),
        Eof,
    }

    struct StepReader {
        steps: VecDeque<ReadStep>,
    }

    impl StepReader {
        fn new(steps: impl IntoIterator<Item = ReadStep>) -> Self {
            Self {
                steps: steps.into_iter().collect(),
            }
        }
    }

    impl AsyncRead for StepReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<io::Result<()>> {
            match self.steps.pop_front().unwrap_or(ReadStep::Eof) {
                ReadStep::Data(data) => {
                    buf.put_slice(&data);
                    Poll::Ready(Ok(()))
                }
                ReadStep::Error(kind) => Poll::Ready(Err(io::Error::new(kind, "synthetic read failure"))),
                ReadStep::Eof => Poll::Ready(Ok(())),
            }
        }
    }

    #[tokio::test]
    async fn test_read_block_via_async_read_preserves_midstream_error_kind() {
        let reader = StepReader::new([ReadStep::Data(b"ab".to_vec()), ReadStep::Error(ErrorKind::ConnectionReset)]);
        let mut reader = WarpReader::new(reader);
        let mut buf = [0_u8; 4];

        let err = reader.read_block(&mut buf).await.unwrap_err();

        assert_eq!(err.kind(), ErrorKind::ConnectionReset);
    }

    #[tokio::test]
    async fn test_read_block_via_async_read_returns_zero_on_initial_eof() {
        let mut reader = WarpReader::new(StepReader::new([ReadStep::Eof]));
        let mut buf = [0_u8; 4];

        let n = reader.read_block(&mut buf).await.unwrap();

        assert_eq!(n, 0);
    }
}
