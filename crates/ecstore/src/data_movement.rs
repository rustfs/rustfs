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

use crate::error::{Error, Result};
use crate::store::ECStore;
use crate::store_api::{CompletePart, GetObjectReader, MultipartOperations, ObjectIO, ObjectInfo, ObjectOptions, PutObjReader};
use bytes::Bytes;
use rustfs_rio::{EtagResolvable, HashReader, HashReaderDetector, Index, TryGetIndex};
use std::io::Cursor;
use std::pin::Pin;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncReadExt, BufReader, ReadBuf};
use tracing::error;

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

    let reader = IndexedDataMovementReader::new(Cursor::new(chunk), index);
    let hash_reader = HashReader::from_stream(reader, size, actual_size, None, sha256hex, false)?;
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

fn data_movement_new_multipart_opts(object_info: &ObjectInfo, src_pool_idx: usize) -> ObjectOptions {
    ObjectOptions {
        versioned: object_info.version_id.is_some(),
        version_id: object_info.version_id.as_ref().map(|v| v.to_string()),
        user_defined: object_info.user_defined.clone(),
        preserve_etag: object_info.etag.clone(),
        src_pool_idx,
        data_movement: true,
        ..Default::default()
    }
}

fn data_movement_complete_multipart_opts(object_info: &ObjectInfo) -> ObjectOptions {
    ObjectOptions {
        versioned: object_info.version_id.is_some(),
        version_id: object_info.version_id.as_ref().map(|v| v.to_string()),
        data_movement: true,
        mod_time: object_info.mod_time,
        preserve_etag: object_info.etag.clone(),
        ..Default::default()
    }
}

fn data_movement_put_object_opts(object_info: &ObjectInfo, src_pool_idx: usize) -> ObjectOptions {
    ObjectOptions {
        versioned: object_info.version_id.is_some(),
        src_pool_idx,
        data_movement: true,
        version_id: object_info.version_id.as_ref().map(|v| v.to_string()),
        mod_time: object_info.mod_time,
        user_defined: object_info.user_defined.clone(),
        preserve_etag: object_info.etag.clone(),
        ..Default::default()
    }
}

fn resolve_data_movement_abort_result(
    op_label: &str,
    bucket: &str,
    object: &str,
    upload_id: &str,
    primary_err: Error,
    abort_err: Error,
) -> Error {
    Error::other(format!(
        "{op_label}: abort_multipart_upload failed for {bucket}/{object} upload {upload_id} after error {primary_err}: {abort_err}"
    ))
}

pub(crate) async fn migrate_object(
    store: Arc<ECStore>,
    pool_idx: usize,
    bucket: String,
    rd: GetObjectReader,
    op_label: &str,
) -> Result<()> {
    let object_info = rd.object_info.clone();

    if object_info.is_multipart() {
        let res = match store
            .new_multipart_upload(&bucket, &object_info.name, &data_movement_new_multipart_opts(&object_info, pool_idx))
            .await
        {
            Ok(res) => res,
            Err(err) => {
                error!("{op_label}: new_multipart_upload err {:?}", &err);
                return Err(err);
            }
        };

        let abort_multipart_flag = new_multipart_abort_flag();
        let multipart_result: Result<()> = async {
            let mut parts = vec![CompletePart::default(); object_info.parts.len()];
            let mut reader = rd.stream;

            for (i, part) in object_info.parts.iter().enumerate() {
                let mut chunk = vec![0u8; part.size];
                reader.read_exact(&mut chunk).await?;

                let part_size = i64::try_from(part.size).map_err(|_| Error::other("part size overflow"))?;
                let part_actual_size = if part.actual_size > 0 { part.actual_size } else { part_size };
                let index = decode_part_index(part.index.as_ref());
                let mut data = put_obj_reader_from_chunk(chunk, part_size, part_actual_size, index)?;

                let pi = match store
                    .put_object_part(
                        &bucket,
                        &object_info.name,
                        &res.upload_id,
                        part.number,
                        &mut data,
                        &ObjectOptions {
                            preserve_etag: Some(part.etag.clone()),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(pi) => pi,
                    Err(err) => {
                        error!("{op_label}: put_object_part {i} err {:?}", &err);
                        return Err(err);
                    }
                };

                parts[i] = CompletePart {
                    part_num: pi.part_num,
                    etag: pi.etag,
                    ..Default::default()
                };
            }

            if let Err(err) = store
                .clone()
                .complete_multipart_upload(
                    &bucket,
                    &object_info.name,
                    &res.upload_id,
                    parts,
                    &data_movement_complete_multipart_opts(&object_info),
                )
                .await
            {
                error!("{op_label}: complete_multipart_upload err {:?}", &err);
                return Err(err);
            }

            mark_multipart_upload_completed(&abort_multipart_flag);
            Ok(())
        }
        .await;

        if let Err(primary_err) = multipart_result {
            if should_abort_multipart_upload(&abort_multipart_flag) {
                return match store
                    .abort_multipart_upload(&bucket, &object_info.name, &res.upload_id, &ObjectOptions::default())
                    .await
                {
                    Ok(()) => Err(primary_err),
                    Err(abort_err) => {
                        error!("{op_label}: abort_multipart_upload err {:?}", &abort_err);
                        Err(resolve_data_movement_abort_result(
                            op_label,
                            bucket.as_str(),
                            object_info.name.as_str(),
                            res.upload_id.as_str(),
                            primary_err,
                            abort_err,
                        ))
                    }
                };
            }
            return Err(primary_err);
        }

        return Ok(());
    }

    let actual_size = object_info.get_actual_size()?;
    let index = object_info
        .parts
        .first()
        .and_then(|part| decode_part_index(part.index.as_ref()));
    let reader = IndexedDataMovementReader::new(BufReader::new(rd.stream), index);
    let hrd = HashReader::from_stream(reader, object_info.size, actual_size, object_info.etag.clone(), None, false)?;
    let mut data = PutObjReader::new(hrd);

    if let Err(err) = store
        .put_object(
            &bucket,
            &object_info.name,
            &mut data,
            &data_movement_put_object_opts(&object_info, pool_idx),
        )
        .await
    {
        error!("{op_label}: put_object err {:?}", &err);
        return Err(err);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::OffsetDateTime;
    use uuid::Uuid;

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
    fn test_resolve_data_movement_abort_result_wraps_abort_context() {
        let err = resolve_data_movement_abort_result(
            "rebalance_object",
            "bucket-a",
            "object-a",
            "upload-1",
            Error::SlowDown,
            Error::OperationCanceled,
        );
        let message = err.to_string();
        assert!(message.contains("rebalance_object: abort_multipart_upload failed"));
        assert!(message.contains("bucket-a/object-a"));
        assert!(message.contains("upload upload-1"));
        assert!(message.contains(Error::SlowDown.to_string().as_str()));
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

    #[test]
    fn test_data_movement_new_multipart_opts_preserves_etag_and_version() {
        let version_id = Uuid::nil();
        let object_info = ObjectInfo {
            version_id: Some(version_id),
            etag: Some("etag-value".to_string()),
            user_defined: std::collections::HashMap::from([("x-amz-meta-key".to_string(), "value".to_string())]),
            ..Default::default()
        };

        let opts = data_movement_new_multipart_opts(&object_info, 7);

        assert!(opts.versioned);
        assert_eq!(opts.version_id.as_deref(), Some(version_id.to_string().as_str()));
        assert_eq!(opts.preserve_etag.as_deref(), Some("etag-value"));
        assert_eq!(opts.user_defined.get("x-amz-meta-key").map(String::as_str), Some("value"));
        assert_eq!(opts.src_pool_idx, 7);
        assert!(opts.data_movement);
    }

    #[test]
    fn test_data_movement_complete_multipart_opts_preserves_mod_time_version_and_etag() {
        let mod_time = OffsetDateTime::now_utc();
        let version_id = Uuid::nil();
        let object_info = ObjectInfo {
            version_id: Some(version_id),
            mod_time: Some(mod_time),
            etag: Some("etag-value".to_string()),
            ..Default::default()
        };

        let opts = data_movement_complete_multipart_opts(&object_info);

        assert!(opts.versioned);
        assert!(opts.data_movement);
        assert_eq!(opts.mod_time, Some(mod_time));
        assert_eq!(opts.version_id.as_deref(), Some(version_id.to_string().as_str()));
        assert_eq!(opts.preserve_etag.as_deref(), Some("etag-value"));
    }

    #[test]
    fn test_data_movement_put_object_opts_preserves_version_and_etag() {
        let version_id = Uuid::nil();
        let object_info = ObjectInfo {
            version_id: Some(version_id),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            etag: Some("etag-value".to_string()),
            user_defined: std::collections::HashMap::from([("x-amz-meta-key".to_string(), "value".to_string())]),
            ..Default::default()
        };

        let opts = data_movement_put_object_opts(&object_info, 9);

        assert!(opts.versioned);
        assert_eq!(opts.version_id.as_deref(), Some(version_id.to_string().as_str()));
        assert_eq!(opts.preserve_etag.as_deref(), Some("etag-value"));
        assert_eq!(opts.user_defined.get("x-amz-meta-key").map(String::as_str), Some("value"));
        assert_eq!(opts.src_pool_idx, 9);
        assert!(opts.data_movement);
        assert_eq!(opts.mod_time, object_info.mod_time);
    }
}
