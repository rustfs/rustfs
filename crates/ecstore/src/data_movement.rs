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

use crate::error::{Error, Result, is_err_data_movement_overwrite, is_err_object_not_found, is_err_version_not_found};
use crate::store::ECStore;
use crate::store_api::{
    CompletePart, GetObjectReader, MultipartOperations, ObjectIO, ObjectInfo, ObjectOperations, ObjectOptions, PutObjReader,
};
use bytes::Bytes;
use rustfs_rio::{EtagResolvable, HashReader, HashReaderDetector, Index, TryGetIndex};
use rustfs_utils::path::encode_dir_object;
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, BufReader, ReadBuf};
use tracing::error;

type SharedDataMovementStream = Arc<Mutex<Box<dyn AsyncRead + Unpin + Send + Sync>>>;

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

struct DataMovementPartReader {
    inner: SharedDataMovementStream,
    remaining: u64,
}

impl DataMovementPartReader {
    fn new(inner: SharedDataMovementStream, size: u64) -> Self {
        Self { inner, remaining: size }
    }
}

impl AsyncRead for DataMovementPartReader {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        if self.remaining == 0 || buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }

        let allowed = buf.remaining().min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
        let target = buf.initialize_unfilled_to(allowed);
        let mut limited_buf = ReadBuf::new(target);

        let poll = {
            let mut inner = self
                .inner
                .lock()
                .map_err(|_| std::io::Error::other("data movement stream lock poisoned"))?;
            Pin::new(&mut **inner).poll_read(cx, &mut limited_buf)
        };

        if let Poll::Ready(Ok(())) = &poll {
            let read = limited_buf.filled().len();
            buf.advance(read);
            self.remaining = self.remaining.saturating_sub(u64::try_from(read).unwrap_or(u64::MAX));
        }

        poll
    }
}

impl EtagResolvable for DataMovementPartReader {}

impl HashReaderDetector for DataMovementPartReader {}

fn put_obj_reader_from_part_stream(
    stream: SharedDataMovementStream,
    size: i64,
    actual_size: i64,
    index: Option<Index>,
) -> Result<PutObjReader> {
    let limit = u64::try_from(size).map_err(|_| Error::other("part size overflow"))?;
    let reader = IndexedDataMovementReader::new(DataMovementPartReader::new(stream, limit), index);
    let hash_reader = HashReader::from_reader(reader, size, actual_size, None, None, false)?;
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
        user_defined: (*object_info.user_defined).clone(),
        preserve_etag: object_info.etag.clone(),
        src_pool_idx,
        data_movement: true,
        ..Default::default()
    }
}

fn data_movement_complete_multipart_opts(object_info: &ObjectInfo, src_pool_idx: usize) -> ObjectOptions {
    ObjectOptions {
        versioned: object_info.version_id.is_some(),
        version_id: object_info.version_id.as_ref().map(|v| v.to_string()),
        data_movement: true,
        mod_time: object_info.mod_time,
        preserve_etag: object_info.etag.clone(),
        src_pool_idx,
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
        user_defined: (*object_info.user_defined).clone(),
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

fn data_movement_stage_error(op_label: &str, stage: &str, bucket: &str, object: &str, err: impl std::fmt::Display) -> Error {
    Error::other(format!("{op_label}: {stage} failed for {bucket}/{object}: {err}"))
}

fn should_check_data_movement_overwrite_resume(err: &Error) -> bool {
    is_err_data_movement_overwrite(err)
}

fn effective_actual_size(info: &ObjectInfo) -> Option<i64> {
    info.get_actual_size().ok()
}

fn is_equivalent_data_movement_object(source: &ObjectInfo, target: &ObjectInfo) -> bool {
    source.version_id == target.version_id
        && source.delete_marker == target.delete_marker
        && source.size == target.size
        && effective_actual_size(source) == effective_actual_size(target)
        && source.etag == target.etag
        && source.checksum == target.checksum
        && source.mod_time == target.mod_time
}

fn should_check_data_movement_resume_target(src_pool_idx: usize, target_pool_idx: usize) -> bool {
    target_pool_idx != src_pool_idx
}

async fn find_data_movement_target_info(
    store: &ECStore,
    target_pool_idx: usize,
    bucket: &str,
    object_info: &ObjectInfo,
) -> Result<Option<ObjectInfo>> {
    let opts = ObjectOptions {
        versioned: object_info.version_id.is_some(),
        version_id: object_info.version_id.as_ref().map(|v| v.to_string()),
        no_lock: true,
        ..Default::default()
    };
    let object = encode_dir_object(object_info.name.as_str());

    let Some(pool) = store.pools.get(target_pool_idx) else {
        return Err(Error::other(format!(
            "data movement resume target pool {target_pool_idx} is out of range for {bucket}/{object}"
        )));
    };

    match pool.get_object_info(bucket, object.as_str(), &opts).await {
        Ok(target_info) => Ok(Some(target_info)),
        Err(err) if is_err_object_not_found(&err) || is_err_version_not_found(&err) => Ok(None),
        Err(err) => Err(err),
    }
}

fn resolve_data_movement_overwrite_resume_result(
    err: &Error,
    target_result: Result<Option<ObjectInfo>>,
    source: &ObjectInfo,
    src_pool_idx: usize,
    target_pool_idx: usize,
) -> Result<bool> {
    if !should_check_data_movement_overwrite_resume(err)
        || !should_check_data_movement_resume_target(src_pool_idx, target_pool_idx)
    {
        return Ok(false);
    }

    let Some(target) = target_result? else {
        return Ok(false);
    };

    Ok(is_equivalent_data_movement_object(source, &target))
}

async fn should_treat_data_movement_overwrite_as_complete(
    store: &ECStore,
    src_pool_idx: usize,
    target_pool_idx: usize,
    bucket: &str,
    object_info: &ObjectInfo,
    err: &Error,
) -> Result<bool> {
    if !should_check_data_movement_overwrite_resume(err) {
        return Ok(false);
    }

    resolve_data_movement_overwrite_resume_result(
        err,
        find_data_movement_target_info(store, target_pool_idx, bucket, object_info).await,
        object_info,
        src_pool_idx,
        target_pool_idx,
    )
}

fn data_movement_part_stage_error(
    op_label: &str,
    stage: &str,
    bucket: &str,
    object: &str,
    part_number: usize,
    err: impl std::fmt::Display,
) -> Error {
    Error::other(format!("{op_label}: {stage} failed for {bucket}/{object} part {part_number}: {err}"))
}

fn is_data_movement_part_read_error(err: &Error) -> bool {
    fn is_unexpected_eof(err: &std::io::Error) -> bool {
        err.kind() == std::io::ErrorKind::UnexpectedEof
            || err
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<std::io::Error>())
                .is_some_and(is_unexpected_eof)
    }

    matches!(err, Error::Io(io_err) if is_unexpected_eof(io_err))
}

fn data_movement_part_upload_failure_stage(err: &Error) -> &'static str {
    if is_data_movement_part_read_error(err) {
        "read_part"
    } else {
        "put_object_part"
    }
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
        let (res, target_pool_idx) = match store
            .handle_new_multipart_upload_with_pool_idx(
                &bucket,
                &object_info.name,
                &data_movement_new_multipart_opts(&object_info, pool_idx),
            )
            .await
        {
            Ok(res) => res,
            Err(err) => {
                error!("{op_label}: new_multipart_upload err {:?}", &err);
                return Err(data_movement_stage_error(
                    op_label,
                    "new_multipart_upload",
                    bucket.as_str(),
                    object_info.name.as_str(),
                    err,
                ));
            }
        };

        let abort_multipart_flag = new_multipart_abort_flag();
        let multipart_result: Result<()> = async {
            let mut parts = vec![CompletePart::default(); object_info.parts.len()];
            let reader = Arc::new(Mutex::new(rd.stream));

            for (i, part) in object_info.parts.iter().enumerate() {
                let part_size = i64::try_from(part.size).map_err(|_| {
                    data_movement_part_stage_error(
                        op_label,
                        "prepare_part",
                        bucket.as_str(),
                        object_info.name.as_str(),
                        part.number,
                        Error::other("part size overflow"),
                    )
                })?;
                let part_actual_size = if part.actual_size > 0 { part.actual_size } else { part_size };
                let index = decode_part_index(part.index.as_ref());
                let mut data =
                    put_obj_reader_from_part_stream(reader.clone(), part_size, part_actual_size, index).map_err(|err| {
                        data_movement_part_stage_error(
                            op_label,
                            "prepare_part",
                            bucket.as_str(),
                            object_info.name.as_str(),
                            part.number,
                            err,
                        )
                    })?;

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
                        let stage = data_movement_part_upload_failure_stage(&err);
                        return Err(data_movement_part_stage_error(
                            op_label,
                            stage,
                            bucket.as_str(),
                            object_info.name.as_str(),
                            part.number,
                            err,
                        ));
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
                    &data_movement_complete_multipart_opts(&object_info, pool_idx),
                )
                .await
            {
                if should_treat_data_movement_overwrite_as_complete(
                    store.as_ref(),
                    pool_idx,
                    target_pool_idx,
                    bucket.as_str(),
                    &object_info,
                    &err,
                )
                .await?
                {
                    mark_multipart_upload_completed(&abort_multipart_flag);
                    return Ok(());
                }

                error!("{op_label}: complete_multipart_upload err {:?}", &err);
                return Err(data_movement_stage_error(
                    op_label,
                    "complete_multipart_upload",
                    bucket.as_str(),
                    object_info.name.as_str(),
                    err,
                ));
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

    let actual_size = object_info.get_actual_size().map_err(|err| {
        data_movement_stage_error(op_label, "prepare_put_object", bucket.as_str(), object_info.name.as_str(), err)
    })?;
    let index = object_info
        .parts
        .first()
        .and_then(|part| decode_part_index(part.index.as_ref()));
    let reader = IndexedDataMovementReader::new(BufReader::new(rd.stream), index);
    let hrd =
        HashReader::from_stream(reader, object_info.size, actual_size, object_info.etag.clone(), None, false).map_err(|err| {
            data_movement_stage_error(op_label, "prepare_put_object", bucket.as_str(), object_info.name.as_str(), err)
        })?;
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
        return Err(data_movement_stage_error(
            op_label,
            "put_object",
            bucket.as_str(),
            object_info.name.as_str(),
            err,
        ));
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::sync::atomic::AtomicUsize;
    use time::OffsetDateTime;
    use tokio::io::AsyncReadExt;
    use uuid::Uuid;

    struct MaxReadRequestReader {
        remaining: u64,
        max_request: usize,
        largest_request: Arc<AtomicUsize>,
    }

    impl MaxReadRequestReader {
        fn new(remaining: u64, max_request: usize, largest_request: Arc<AtomicUsize>) -> Self {
            Self {
                remaining,
                max_request,
                largest_request,
            }
        }
    }

    impl AsyncRead for MaxReadRequestReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            let requested = buf.remaining();
            self.largest_request.fetch_max(requested, Ordering::Relaxed);
            if requested > self.max_request {
                return Poll::Ready(Err(std::io::Error::other(format!("oversized read request: {requested}"))));
            }
            if self.remaining == 0 || requested == 0 {
                return Poll::Ready(Ok(()));
            }

            let read = requested.min(usize::try_from(self.remaining).unwrap_or(usize::MAX));
            let target = buf.initialize_unfilled_to(read);
            target.fill(b'x');
            buf.advance(read);
            self.remaining = self.remaining.saturating_sub(u64::try_from(read).unwrap_or(u64::MAX));
            Poll::Ready(Ok(()))
        }
    }

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
    fn test_data_movement_stage_error_includes_stage_and_object() {
        let err = data_movement_stage_error("rebalance_object", "put_object", "bucket-a", "object-a", Error::SlowDown);
        let message = err.to_string();
        assert!(message.contains("rebalance_object: put_object failed for bucket-a/object-a"));
        assert!(message.contains(Error::SlowDown.to_string().as_str()));
    }

    #[test]
    fn test_data_movement_part_stage_error_includes_stage_object_and_part() {
        let err =
            data_movement_part_stage_error("rebalance_object", "put_object_part", "bucket-a", "object-a", 7, Error::SlowDown);
        let message = err.to_string();
        assert!(message.contains("rebalance_object: put_object_part failed for bucket-a/object-a part 7"));
        assert!(message.contains(Error::SlowDown.to_string().as_str()));
    }

    #[test]
    fn test_data_movement_part_upload_failure_stage_reports_short_read() {
        let err = Error::Io(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "short part"));

        assert_eq!(data_movement_part_upload_failure_stage(&err), "read_part");
    }

    #[test]
    fn test_data_movement_part_upload_failure_stage_keeps_write_errors() {
        assert_eq!(data_movement_part_upload_failure_stage(&Error::SlowDown), "put_object_part");
    }

    #[test]
    fn test_should_check_data_movement_overwrite_resume_only_for_overwrite_error() {
        assert!(should_check_data_movement_overwrite_resume(&Error::DataMovementOverwriteErr(
            "bucket-a".to_string(),
            "object-a".to_string(),
            "version-a".to_string(),
        )));
        assert!(!should_check_data_movement_overwrite_resume(&Error::SlowDown));
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

    #[tokio::test]
    async fn test_multipart_part_stream_preserves_boundaries() {
        let stream: Box<dyn AsyncRead + Unpin + Send + Sync> = Box::new(Cursor::new(b"abcdef".to_vec()));
        let shared = Arc::new(Mutex::new(stream));

        let mut first =
            put_obj_reader_from_part_stream(shared.clone(), 3, 3, None).expect("first bounded part reader should be created");
        let mut first_data = Vec::new();
        first
            .stream
            .read_to_end(&mut first_data)
            .await
            .expect("first part should read only its boundary");

        let mut second =
            put_obj_reader_from_part_stream(shared, 3, 3, None).expect("second bounded part reader should be created");
        let mut second_data = Vec::new();
        second
            .stream
            .read_to_end(&mut second_data)
            .await
            .expect("second part should continue at the next boundary");

        assert_eq!(first_data, b"abc");
        assert_eq!(second_data, b"def");
    }

    #[tokio::test]
    async fn test_multipart_part_stream_does_not_request_full_large_part() {
        let largest_request = Arc::new(AtomicUsize::new(0));
        let stream: Box<dyn AsyncRead + Unpin + Send + Sync> =
            Box::new(MaxReadRequestReader::new(16 * 1024 * 1024, 8 * 1024, largest_request.clone()));
        let shared = Arc::new(Mutex::new(stream));
        let mut data = put_obj_reader_from_part_stream(shared, 16 * 1024 * 1024, 16 * 1024 * 1024, None)
            .expect("large bounded part reader should be created without allocating part size");

        let mut buf = [0u8; 8 * 1024];
        let read = data
            .stream
            .read(&mut buf)
            .await
            .expect("bounded reader should satisfy a small read against a large advertised part");

        assert_eq!(read, buf.len());
        assert!(largest_request.load(Ordering::Relaxed) <= buf.len());
    }

    #[tokio::test]
    async fn test_multipart_part_stream_reports_short_part() {
        let stream: Box<dyn AsyncRead + Unpin + Send + Sync> = Box::new(Cursor::new(b"abc".to_vec()));
        let shared = Arc::new(Mutex::new(stream));
        let mut data = put_obj_reader_from_part_stream(shared, 5, 5, None).expect("short bounded part reader should be created");

        let err = data
            .stream
            .read_to_end(&mut Vec::new())
            .await
            .expect_err("short source stream should fail the part reader");

        assert_eq!(err.kind(), std::io::ErrorKind::UnexpectedEof);
    }

    #[test]
    fn test_multipart_part_stream_preserves_index() {
        let mut index = Index::new();
        index.add(0, 0).expect("index entry should be accepted");

        let stream: Box<dyn AsyncRead + Unpin + Send + Sync> = Box::new(Cursor::new(b"abc".to_vec()));
        let shared = Arc::new(Mutex::new(stream));
        let data = put_obj_reader_from_part_stream(shared, 3, 3, Some(index))
            .expect("bounded part reader should retain compression index");

        assert!(data.stream.try_get_index().is_some());
    }

    #[test]
    fn test_data_movement_new_multipart_opts_preserves_etag_and_version() {
        let version_id = Uuid::nil();
        let object_info = ObjectInfo {
            version_id: Some(version_id),
            etag: Some("etag-value".to_string()),
            user_defined: Arc::new(std::collections::HashMap::from([("x-amz-meta-key".to_string(), "value".to_string())])),
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

        let opts = data_movement_complete_multipart_opts(&object_info, 7);

        assert!(opts.versioned);
        assert!(opts.data_movement);
        assert_eq!(opts.mod_time, Some(mod_time));
        assert_eq!(opts.version_id.as_deref(), Some(version_id.to_string().as_str()));
        assert_eq!(opts.preserve_etag.as_deref(), Some("etag-value"));
        assert_eq!(opts.src_pool_idx, 7);
    }

    #[test]
    fn test_data_movement_put_object_opts_preserves_version_and_etag() {
        let version_id = Uuid::nil();
        let object_info = ObjectInfo {
            version_id: Some(version_id),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            etag: Some("etag-value".to_string()),
            user_defined: Arc::new(std::collections::HashMap::from([("x-amz-meta-key".to_string(), "value".to_string())])),
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

    #[test]
    fn test_is_equivalent_data_movement_object_accepts_matching_metadata() {
        let version_id = Uuid::nil();
        let info = ObjectInfo {
            version_id: Some(version_id),
            size: 128,
            actual_size: 96,
            etag: Some("etag-value".to_string()),
            checksum: Some(Bytes::from_static(b"checksum")),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };

        assert!(is_equivalent_data_movement_object(&info, &info.clone()));
    }

    #[test]
    fn test_is_equivalent_data_movement_object_rejects_content_mismatch() {
        let source = ObjectInfo {
            version_id: Some(Uuid::nil()),
            size: 128,
            actual_size: 96,
            etag: Some("etag-source".to_string()),
            checksum: Some(Bytes::from_static(b"checksum-source")),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };
        let target = ObjectInfo {
            etag: Some("etag-target".to_string()),
            checksum: Some(Bytes::from_static(b"checksum-target")),
            ..source.clone()
        };

        assert!(!is_equivalent_data_movement_object(&source, &target));
    }

    #[test]
    fn test_is_equivalent_data_movement_object_uses_effective_actual_size() {
        let source = ObjectInfo {
            size: 128,
            actual_size: 0,
            etag: Some("etag-value".to_string()),
            ..Default::default()
        };
        let target = ObjectInfo {
            size: 128,
            actual_size: 128,
            etag: Some("etag-value".to_string()),
            ..Default::default()
        };

        assert!(is_equivalent_data_movement_object(&source, &target));
    }

    #[test]
    fn test_resolve_data_movement_overwrite_resume_result_accepts_equivalent_target() {
        let source = ObjectInfo {
            version_id: Some(Uuid::nil()),
            size: 128,
            etag: Some("etag-value".to_string()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };
        let err = Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "version".to_string());

        let should_resume = resolve_data_movement_overwrite_resume_result(&err, Ok(Some(source.clone())), &source, 0, 1)
            .expect("equivalent overwrite target should be evaluated");

        assert!(should_resume);
    }

    #[test]
    fn test_resolve_data_movement_overwrite_resume_result_rejects_source_pool_target() {
        let source = ObjectInfo {
            version_id: Some(Uuid::nil()),
            size: 128,
            etag: Some("etag-value".to_string()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };
        let err = Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "version".to_string());

        let should_resume = resolve_data_movement_overwrite_resume_result(&err, Ok(Some(source.clone())), &source, 0, 0)
            .expect("source-pool target should be rejected before target lookup");

        assert!(!should_resume);
    }

    #[test]
    fn test_resolve_data_movement_overwrite_resume_result_rejects_non_equivalent_target() {
        let source = ObjectInfo {
            version_id: Some(Uuid::nil()),
            size: 128,
            etag: Some("etag-source".to_string()),
            ..Default::default()
        };
        let target = ObjectInfo {
            etag: Some("etag-target".to_string()),
            ..source.clone()
        };
        let err = Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "version".to_string());

        let should_resume = resolve_data_movement_overwrite_resume_result(&err, Ok(Some(target)), &source, 0, 1)
            .expect("non-equivalent overwrite target should be evaluated");

        assert!(!should_resume);
    }

    #[test]
    fn test_resolve_data_movement_overwrite_resume_result_propagates_target_lookup_error() {
        let source = ObjectInfo::default();
        let err = Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "version".to_string());
        let result = resolve_data_movement_overwrite_resume_result(&err, Err(Error::SlowDown), &source, 0, 1);

        assert!(matches!(result, Err(Error::SlowDown)));
    }

    #[test]
    fn test_resolve_data_movement_overwrite_resume_result_ignores_non_overwrite_error() {
        let source = ObjectInfo::default();
        let result = resolve_data_movement_overwrite_resume_result(&Error::SlowDown, Err(Error::FileAccessDenied), &source, 0, 1)
            .expect("non-overwrite errors should not query target equivalence");

        assert!(!result);
    }
}
