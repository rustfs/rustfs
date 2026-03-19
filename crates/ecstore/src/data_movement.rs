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

use crate::error::Result;
use crate::store_api::PutObjReader;
use bytes::Bytes;
use rustfs_rio::{EtagResolvable, HashReader, HashReaderDetector, Index, Reader, TryGetIndex, WarpReader};
use std::io::Cursor;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, ReadBuf};

pub struct IndexedDataMovementReader<R> {
    inner: R,
    index: Option<Index>,
}

impl<R> IndexedDataMovementReader<R> {
    pub fn new(inner: R, index: Option<Index>) -> Self {
        Self { inner, index }
    }
}

impl<R: AsyncRead + Unpin + Send + Sync> AsyncRead for IndexedDataMovementReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl<R: AsyncRead + Unpin + Send + Sync> EtagResolvable for IndexedDataMovementReader<R> {}

impl<R: AsyncRead + Unpin + Send + Sync> HashReaderDetector for IndexedDataMovementReader<R> {}

impl<R: AsyncRead + Unpin + Send + Sync> TryGetIndex for IndexedDataMovementReader<R> {
    fn try_get_index(&self) -> Option<&Index> {
        self.index.as_ref()
    }
}

impl<R: AsyncRead + Unpin + Send + Sync> Reader for IndexedDataMovementReader<R> {}

pub fn decode_part_index(index: Option<&Bytes>) -> Option<Index> {
    let bytes = index?;
    let mut decoded = Index::new();
    if decoded.load(bytes.as_ref()).is_ok() {
        Some(decoded)
    } else {
        None
    }
}

pub fn put_obj_reader_from_chunk(chunk: Vec<u8>, size: i64, actual_size: i64, index: Option<Index>) -> Result<PutObjReader> {
    use sha2::{Digest, Sha256};

    let sha256hex = if !chunk.is_empty() {
        Some(hex_simd::encode_to_string(Sha256::digest(&chunk), hex_simd::AsciiCase::Lower))
    } else {
        None
    };

    let reader = IndexedDataMovementReader::new(WarpReader::new(Cursor::new(chunk)), index);
    let hash_reader = HashReader::new(Box::new(reader), size, actual_size, None, sha256hex, false)?;
    Ok(PutObjReader::new(hash_reader))
}

pub fn new_multipart_abort_flag() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(true))
}

pub fn should_abort_multipart_upload(flag: &Arc<AtomicBool>) -> bool {
    flag.load(Ordering::Relaxed)
}

pub fn mark_multipart_upload_completed(flag: &Arc<AtomicBool>) {
    flag.store(false, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_multipart_abort_flag_defaults_to_abort_enabled() {
        let flag = new_multipart_abort_flag();
        assert!(should_abort_multipart_upload(&flag));
    }

    #[test]
    fn test_mark_multipart_upload_completed_disables_abort_cleanup() {
        let flag = new_multipart_abort_flag();
        mark_multipart_upload_completed(&flag);
        assert!(!should_abort_multipart_upload(&flag));
    }

    #[test]
    fn test_decode_part_index_returns_none_when_absent() {
        assert!(decode_part_index(None).is_none());
    }

    #[test]
    fn test_decode_part_index_returns_none_for_invalid_payload() {
        let invalid = Bytes::from_static(b"not-a-valid-index");
        assert!(decode_part_index(Some(&invalid)).is_none());
    }

    #[test]
    fn test_decode_part_index_returns_some_for_valid_payload() {
        let mut index = Index::new();
        index.add(0, 0).expect("first index entry should be accepted");
        index
            .add(2_097_152, 2_097_152)
            .expect("second index entry should advance totals");

        let encoded = index.into_vec();
        let decoded = decode_part_index(Some(&encoded)).expect("valid index payload should decode");

        assert_eq!(decoded.total_uncompressed, 2_097_152);
        assert_eq!(decoded.total_compressed, 2_097_152);
    }
}
