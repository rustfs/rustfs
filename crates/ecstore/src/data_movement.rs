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
use crate::set_disk::SetDisks;
use crate::store::ECStore;
use crate::store_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use bytes::Bytes;
use rustfs_filemeta::{FileInfo, FileInfoVersions, ObjectPartInfo};
use rustfs_rio::{ChecksumType, EtagResolvable, HashReader, HashReaderDetector, Index, TryGetIndex};
use rustfs_storage_api::{CompletePart, MultipartOperations as _, ObjectIO as _, ObjectOperations as _};
use rustfs_utils::http::AMZ_OBJECT_TAGGING;
use rustfs_utils::path::encode_dir_object;
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use time::format_description::well_known::Rfc3339;
use tokio::io::{AsyncRead, BufReader, ReadBuf};
use tracing::{error, info};

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

fn data_movement_object_checksum_type(object_info: &ObjectInfo) -> Option<ChecksumType> {
    let checksum = object_info.checksum.as_ref()?;
    let (checksums, _) = rustfs_rio::read_checksums(checksum.as_ref(), 0);
    rustfs_rio::BASE_CHECKSUM_TYPES
        .iter()
        .copied()
        .find(|checksum_type| checksums.contains_key(checksum_type.to_string().as_str()))
}

fn data_movement_multipart_checksum_type(object_info: &ObjectInfo) -> Option<ChecksumType> {
    let checksum = object_info.user_defined.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM)?;
    let checksum_type = object_info
        .user_defined
        .get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE)
        .map(String::as_str)
        .unwrap_or_default();
    let checksum_type = ChecksumType::from_string_with_obj_type(checksum, checksum_type);
    checksum_type.is_set().then_some(checksum_type)
}

fn add_data_movement_calculated_checksum(data: &mut PutObjReader, checksum_type: Option<ChecksumType>) -> Result<()> {
    if let Some(checksum_type) = checksum_type {
        data.stream.add_calculated_checksum(checksum_type).map_err(Error::from)?;
    }
    Ok(())
}

fn data_movement_part_checksum(part: &ObjectPartInfo, checksum_type: ChecksumType) -> Option<String> {
    part.checksums
        .as_ref()
        .and_then(|checksums| checksums.get(checksum_type.to_string().as_str()))
        .cloned()
}

fn data_movement_complete_part(part_num: usize, etag: Option<String>, source_part: &ObjectPartInfo) -> CompletePart {
    CompletePart {
        part_num,
        etag,
        checksum_crc32: data_movement_part_checksum(source_part, ChecksumType::CRC32),
        checksum_crc32c: data_movement_part_checksum(source_part, ChecksumType::CRC32C),
        checksum_sha1: data_movement_part_checksum(source_part, ChecksumType::SHA1),
        checksum_sha256: data_movement_part_checksum(source_part, ChecksumType::SHA256),
        checksum_crc64nvme: data_movement_part_checksum(source_part, ChecksumType::CRC64_NVME),
    }
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
        user_defined: data_movement_user_defined(object_info),
        preserve_etag: object_info.etag.clone(),
        src_pool_idx,
        data_movement: true,
        ..Default::default()
    }
}

fn data_movement_user_defined(object_info: &ObjectInfo) -> HashMap<String, String> {
    let mut user_defined = (*object_info.user_defined).clone();
    if !object_info.user_tags.is_empty() {
        user_defined.insert(AMZ_OBJECT_TAGGING.to_string(), (*object_info.user_tags).clone());
    }
    if let Some(expires) = object_info.expires
        && let Ok(expires) = expires.format(&Rfc3339)
    {
        user_defined.insert("expires".to_string(), expires);
    }
    user_defined
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
        user_defined: data_movement_user_defined(object_info),
        preserve_etag: object_info.etag.clone(),
        ..Default::default()
    }
}

fn data_movement_put_object_reader(
    bucket: &str,
    object_info: &ObjectInfo,
    rd: GetObjectReader,
    op_label: &str,
) -> Result<PutObjReader> {
    let actual_size = object_info
        .get_actual_size()
        .map_err(|err| data_movement_stage_error(op_label, "prepare_put_object", bucket, object_info.name.as_str(), err))?;
    let index = object_info
        .parts
        .first()
        .and_then(|part| decode_part_index(part.index.as_ref()));
    let reader = IndexedDataMovementReader::new(BufReader::new(rd.stream), index);
    let hrd = HashReader::from_stream(reader, object_info.size, actual_size, None, None, false)
        .map_err(|err| data_movement_stage_error(op_label, "prepare_put_object", bucket, object_info.name.as_str(), err))?;
    let mut data = PutObjReader::new(hrd);
    add_data_movement_calculated_checksum(&mut data, data_movement_object_checksum_type(object_info))
        .map_err(|err| data_movement_stage_error(op_label, "prepare_put_object", bucket, object_info.name.as_str(), err))?;
    Ok(data)
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

fn is_equivalent_data_movement_part(source: &ObjectPartInfo, target: &ObjectPartInfo) -> bool {
    source.number == target.number
        && source.etag == target.etag
        && source.size == target.size
        && source.actual_size == target.actual_size
        && source.mod_time == target.mod_time
        && source.index == target.index
        && source.checksums == target.checksums
}

fn data_movement_parts_by_number(parts: &[ObjectPartInfo]) -> Option<BTreeMap<usize, &ObjectPartInfo>> {
    let mut parts_by_number = BTreeMap::new();
    for part in parts {
        if parts_by_number.insert(part.number, part).is_some() {
            return None;
        }
    }

    Some(parts_by_number)
}

fn are_equivalent_data_movement_parts(source: &[ObjectPartInfo], target: &[ObjectPartInfo]) -> bool {
    if source.len() != target.len() {
        return false;
    }

    let Some(source_parts) = data_movement_parts_by_number(source) else {
        return false;
    };
    let Some(target_parts) = data_movement_parts_by_number(target) else {
        return false;
    };

    source_parts.iter().all(|(number, source_part)| {
        target_parts
            .get(number)
            .is_some_and(|target_part| is_equivalent_data_movement_part(source_part, target_part))
    })
}

fn is_equivalent_data_movement_object(source: &ObjectInfo, target: &ObjectInfo) -> bool {
    source.version_id == target.version_id
        && source.delete_marker == target.delete_marker
        && source.size == target.size
        && effective_actual_size(source) == effective_actual_size(target)
        && source.etag == target.etag
        && source.checksum == target.checksum
        && source.mod_time == target.mod_time
        && source.storage_class == target.storage_class
        && source.user_defined == target.user_defined
        && source.user_tags == target.user_tags
        && source.expires == target.expires
        && source.replication_status_internal == target.replication_status_internal
        && source.replication_status == target.replication_status
        && source.version_purge_status_internal == target.version_purge_status_internal
        && source.version_purge_status == target.version_purge_status
        && are_equivalent_data_movement_parts(&source.parts, &target.parts)
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SourceCleanupPartIdentity {
    number: usize,
    etag: String,
    size: usize,
    actual_size: i64,
    mod_time: Option<time::OffsetDateTime>,
    index: Option<Vec<u8>>,
    checksums: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct SourceCleanupVersionIdentity {
    name: String,
    version_id: Option<uuid::Uuid>,
    deleted: bool,
    mod_time: Option<time::OffsetDateTime>,
    size: i64,
    etag: Option<String>,
    checksum: Option<Vec<u8>>,
    data_dir: Option<uuid::Uuid>,
    metadata: BTreeMap<String, String>,
    parts: Vec<SourceCleanupPartIdentity>,
}

fn source_cleanup_part_identity(part: &ObjectPartInfo) -> SourceCleanupPartIdentity {
    SourceCleanupPartIdentity {
        number: part.number,
        etag: part.etag.clone(),
        size: part.size,
        actual_size: part.actual_size,
        mod_time: part.mod_time,
        index: part.index.as_ref().map(|index| index.to_vec()),
        checksums: part
            .checksums
            .as_ref()
            .map(|checksums| checksums.iter().map(|(key, value)| (key.clone(), value.clone())).collect())
            .unwrap_or_default(),
    }
}

pub(crate) fn source_cleanup_version_identity(version: &FileInfo) -> SourceCleanupVersionIdentity {
    let mut parts: Vec<_> = version.parts.iter().map(source_cleanup_part_identity).collect();
    parts.sort();

    SourceCleanupVersionIdentity {
        name: version.name.clone(),
        version_id: version.version_id,
        deleted: version.deleted,
        mod_time: version.mod_time,
        size: version.size,
        etag: version.get_etag(),
        checksum: version.checksum.as_ref().map(|checksum| checksum.to_vec()),
        data_dir: version.data_dir,
        metadata: version
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        parts,
    }
}

fn source_cleanup_version_identities(fivs: &FileInfoVersions) -> Vec<SourceCleanupVersionIdentity> {
    let mut identities: Vec<_> = fivs.versions.iter().map(source_cleanup_version_identity).collect();
    identities.sort();
    identities
}

fn source_cleanup_versions_match(expected: &FileInfoVersions, current: &FileInfoVersions) -> bool {
    source_cleanup_versions_match_with_allowed_missing(expected, current, &[])
}

fn source_cleanup_versions_match_with_allowed_missing(
    expected: &FileInfoVersions,
    current: &FileInfoVersions,
    allowed_missing: &[SourceCleanupVersionIdentity],
) -> bool {
    let mut expected_counts = BTreeMap::new();
    for identity in source_cleanup_version_identities(expected) {
        *expected_counts.entry(identity).or_insert(0usize) += 1;
    }

    for identity in source_cleanup_version_identities(current) {
        let Some(count) = expected_counts.get_mut(&identity) else {
            return false;
        };

        *count = count.saturating_sub(1);
        if *count == 0 {
            expected_counts.remove(&identity);
        }
    }

    let mut allowed_counts = BTreeMap::new();
    for identity in allowed_missing.iter().cloned() {
        *allowed_counts.entry(identity).or_insert(0usize) += 1;
    }

    expected_counts
        .into_iter()
        .all(|(identity, count)| allowed_counts.get(&identity).copied().unwrap_or_default() >= count)
}

fn source_cleanup_preflight_error(op_label: &str, bucket: &str, object: &str, err: impl std::fmt::Display) -> Error {
    Error::other(format!("{op_label}: source cleanup preflight failed for {bucket}/{object}: {err}"))
}

async fn load_source_cleanup_versions(
    set: Arc<SetDisks>,
    bucket: &str,
    object: &str,
    op_label: &str,
) -> Result<Option<FileInfoVersions>> {
    set.load_file_info_versions_exact(bucket, object)
        .await
        .map_err(|err| source_cleanup_preflight_error(op_label, bucket, object, err))
}

pub(crate) async fn ensure_source_cleanup_versions_unchanged(
    set: Arc<SetDisks>,
    bucket: &str,
    object: &str,
    expected: &FileInfoVersions,
    allowed_missing: &[SourceCleanupVersionIdentity],
    op_label: &str,
) -> Result<()> {
    let Some(current) = load_source_cleanup_versions(set, bucket, object, op_label).await? else {
        return Ok(());
    };

    if source_cleanup_versions_match_with_allowed_missing(expected, &current, allowed_missing) {
        return Ok(());
    }

    Err(source_cleanup_preflight_error(
        op_label,
        bucket,
        object,
        "source versions changed after migration started",
    ))
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

async fn should_treat_data_movement_overwrite_as_complete_in_any_target_pool(
    store: &ECStore,
    src_pool_idx: usize,
    bucket: &str,
    object_info: &ObjectInfo,
    err: &Error,
) -> Result<bool> {
    if !should_check_data_movement_overwrite_resume(err) {
        return Ok(false);
    }

    for target_pool_idx in 0..store.pools.len() {
        if target_pool_idx == src_pool_idx {
            continue;
        }

        if should_treat_data_movement_overwrite_as_complete(store, src_pool_idx, target_pool_idx, bucket, object_info, err)
            .await?
        {
            return Ok(true);
        }
    }

    Ok(false)
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
            let multipart_checksum_type = data_movement_multipart_checksum_type(&object_info);

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
                add_data_movement_calculated_checksum(&mut data, multipart_checksum_type).map_err(|err| {
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

                parts[i] = data_movement_complete_part(pi.part_num, pi.etag, part);
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
                    info!(
                        "{op_label}: complete_multipart_upload overwrite resolved by equivalent target for {}/{}",
                        bucket.as_str(),
                        object_info.name.as_str()
                    );
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

    let mut data = data_movement_put_object_reader(bucket.as_str(), &object_info, rd, op_label)?;

    if let Err(err) = store
        .put_object(
            &bucket,
            &object_info.name,
            &mut data,
            &data_movement_put_object_opts(&object_info, pool_idx),
        )
        .await
    {
        if should_treat_data_movement_overwrite_as_complete_in_any_target_pool(
            store.as_ref(),
            pool_idx,
            bucket.as_str(),
            &object_info,
            &err,
        )
        .await?
        {
            info!(
                "{op_label}: put_object overwrite resolved by equivalent target for {}/{}",
                bucket.as_str(),
                object_info.name.as_str()
            );
            return Ok(());
        }

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
    use rustfs_rio::HashReaderMut;
    use s3s::header::{X_AMZ_OBJECT_LOCK_LEGAL_HOLD, X_AMZ_OBJECT_LOCK_MODE, X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE};
    use std::collections::HashMap;
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

    fn assert_data_movement_metadata_equivalent(source: &ObjectInfo, target: &ObjectInfo) {
        assert_eq!(source.version_id, target.version_id);
        assert_eq!(source.etag, target.etag);
        assert_eq!(source.size, target.size);
        assert_eq!(effective_actual_size(source), effective_actual_size(target));
        assert_eq!(source.mod_time, target.mod_time);
        assert_eq!(source.user_defined, target.user_defined);
        assert_eq!(source.storage_class, target.storage_class);
        assert_eq!(source.checksum, target.checksum);
        assert_eq!(source.replication_status_internal, target.replication_status_internal);
        assert_eq!(source.replication_status, target.replication_status);
        assert_eq!(source.version_purge_status_internal, target.version_purge_status_internal);
        assert_eq!(source.version_purge_status, target.version_purge_status);
        assert_eq!(
            source.user_defined.get(X_AMZ_OBJECT_LOCK_MODE.as_str()),
            target.user_defined.get(X_AMZ_OBJECT_LOCK_MODE.as_str())
        );
        assert_eq!(
            source.user_defined.get(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str()),
            target.user_defined.get(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str())
        );
        assert_eq!(
            source.user_defined.get(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str()),
            target.user_defined.get(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str())
        );
        assert_eq!(source.parts.len(), target.parts.len());
        for (source_part, target_part) in source.parts.iter().zip(target.parts.iter()) {
            assert_eq!(source_part.number, target_part.number);
            assert_eq!(source_part.etag, target_part.etag);
            assert_eq!(source_part.size, target_part.size);
            assert_eq!(source_part.actual_size, target_part.actual_size);
            assert_eq!(source_part.checksums, target_part.checksums);
        }
    }

    fn cleanup_test_file_info(name: &str, version_id: Uuid, metadata_value: &str) -> FileInfo {
        FileInfo {
            name: name.to_string(),
            version_id: Some(version_id),
            size: 128,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            data_dir: Some(Uuid::from_u128(100)),
            checksum: Some(Bytes::from_static(b"object-checksum")),
            metadata: HashMap::from([
                ("etag".to_string(), "etag-value".to_string()),
                ("x-amz-meta-key".to_string(), metadata_value.to_string()),
            ]),
            parts: vec![ObjectPartInfo {
                number: 1,
                etag: "part-etag".to_string(),
                size: 128,
                actual_size: 128,
                mod_time: Some(OffsetDateTime::UNIX_EPOCH),
                checksums: Some(HashMap::from([(ChecksumType::CRC32C.to_string(), "part-checksum".to_string())])),
                ..Default::default()
            }],
            ..Default::default()
        }
    }

    fn cleanup_test_versions(versions: Vec<FileInfo>) -> FileInfoVersions {
        FileInfoVersions {
            name: "object.txt".to_string(),
            versions,
            ..Default::default()
        }
    }

    #[test]
    fn test_source_cleanup_version_identities_accept_same_versions_out_of_order() {
        let first = cleanup_test_file_info("object.txt", Uuid::from_u128(1), "first");
        let second = cleanup_test_file_info("object.txt", Uuid::from_u128(2), "second");
        let expected = cleanup_test_versions(vec![first.clone(), second.clone()]);
        let current = cleanup_test_versions(vec![second, first]);

        assert!(source_cleanup_versions_match(&expected, &current));
    }

    #[test]
    fn test_rebalance_entry_cleanup_preflight_rejects_changed_source_metadata() {
        let expected = cleanup_test_versions(vec![cleanup_test_file_info("object.txt", Uuid::from_u128(1), "source")]);
        let current = cleanup_test_versions(vec![cleanup_test_file_info("object.txt", Uuid::from_u128(1), "changed")]);

        assert!(!source_cleanup_versions_match(&expected, &current));
    }

    #[test]
    fn test_decommission_entry_cleanup_preflight_rejects_added_source_version() {
        let expected = cleanup_test_versions(vec![cleanup_test_file_info("object.txt", Uuid::from_u128(1), "source")]);
        let current = cleanup_test_versions(vec![
            cleanup_test_file_info("object.txt", Uuid::from_u128(1), "source"),
            cleanup_test_file_info("object.txt", Uuid::from_u128(2), "new-version"),
        ]);

        assert!(!source_cleanup_versions_match(&expected, &current));
    }

    #[test]
    fn test_decommission_cleanup_preflight_accepts_allowed_expired_missing_version() {
        let migrated = cleanup_test_file_info("object.txt", Uuid::from_u128(1), "migrated");
        let expired = cleanup_test_file_info("object.txt", Uuid::from_u128(2), "expired");
        let expected = cleanup_test_versions(vec![migrated.clone(), expired.clone()]);
        let current = cleanup_test_versions(vec![migrated]);
        let allowed_missing = vec![source_cleanup_version_identity(&expired)];

        assert!(source_cleanup_versions_match_with_allowed_missing(&expected, &current, &allowed_missing));
    }

    #[test]
    fn test_decommission_cleanup_preflight_rejects_unexpected_missing_version() {
        let migrated = cleanup_test_file_info("object.txt", Uuid::from_u128(1), "migrated");
        let protected = cleanup_test_file_info("object.txt", Uuid::from_u128(2), "protected");
        let expected = cleanup_test_versions(vec![migrated.clone(), protected]);
        let current = cleanup_test_versions(vec![migrated]);

        assert!(!source_cleanup_versions_match_with_allowed_missing(&expected, &current, &[]));
    }

    #[test]
    fn test_decommission_cleanup_preflight_rejects_new_version_with_allowed_missing() {
        let migrated = cleanup_test_file_info("object.txt", Uuid::from_u128(1), "migrated");
        let expired = cleanup_test_file_info("object.txt", Uuid::from_u128(2), "expired");
        let new_version = cleanup_test_file_info("object.txt", Uuid::from_u128(3), "new-version");
        let expected = cleanup_test_versions(vec![migrated.clone(), expired.clone()]);
        let current = cleanup_test_versions(vec![migrated, new_version]);
        let allowed_missing = vec![source_cleanup_version_identity(&expired)];

        assert!(!source_cleanup_versions_match_with_allowed_missing(&expected, &current, &allowed_missing));
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
    async fn test_data_movement_single_part_checksum_is_recalculated_from_source_type() {
        let payload = b"checksum-payload";
        let checksum =
            rustfs_rio::Checksum::new_from_data(ChecksumType::CRC32C, payload).expect("source checksum should be created");
        let object_info = ObjectInfo {
            checksum: Some(checksum.to_bytes(&[])),
            ..Default::default()
        };
        let mut data = PutObjReader::from_vec(payload.to_vec());

        add_data_movement_calculated_checksum(&mut data, data_movement_object_checksum_type(&object_info))
            .expect("source checksum type should be enabled on the migrated reader");
        data.stream
            .read_to_end(&mut Vec::new())
            .await
            .expect("reader should consume payload and calculate checksum");

        let migrated = data
            .stream
            .content_hash()
            .as_ref()
            .expect("migrated reader should contain calculated checksum");
        assert_eq!(migrated.to_bytes(&[]), checksum.to_bytes(&[]));
    }

    #[tokio::test]
    async fn test_data_movement_single_part_raw_reader_does_not_validate_source_etag() {
        let raw_payload = b"raw-encrypted-or-compressed-bytes".to_vec();
        let object_info = ObjectInfo {
            name: "object.txt".to_string(),
            size: i64::try_from(raw_payload.len()).expect("test payload size should fit i64"),
            actual_size: 128,
            etag: Some("logical-source-etag".to_string()),
            ..Default::default()
        };
        let rd = GetObjectReader {
            stream: Box::new(Cursor::new(raw_payload.clone())),
            object_info: object_info.clone(),
        };

        let mut data = data_movement_put_object_reader("bucket-a", &object_info, rd, "test_migration")
            .expect("raw data movement reader should ignore source ETag during stream validation");
        let mut migrated = Vec::new();
        data.stream
            .read_to_end(&mut migrated)
            .await
            .expect("raw data movement reader should consume payload without ETag mismatch");

        assert_eq!(migrated, raw_payload);
    }

    #[test]
    fn test_data_movement_single_part_checksum_uses_raw_source_size() {
        let object_info = ObjectInfo {
            size: 32,
            actual_size: 128,
            etag: Some("etag-value".to_string()),
            checksum: Some(Bytes::new()),
            ..Default::default()
        };

        assert_eq!(object_info.size, 32);
        assert_eq!(object_info.get_actual_size().expect("actual size should resolve"), 128);
        assert_eq!(data_movement_object_checksum_type(&object_info), None);
    }

    #[test]
    fn test_data_movement_multipart_checksum_type_uses_source_metadata() {
        let object_info = ObjectInfo {
            user_defined: Arc::new(HashMap::from([
                (rustfs_rio::RUSTFS_MULTIPART_CHECKSUM.to_string(), ChecksumType::CRC64_NVME.to_string()),
                (
                    rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE.to_string(),
                    ChecksumType::CRC64_NVME.obj_type().to_string(),
                ),
            ])),
            ..Default::default()
        };

        assert_eq!(data_movement_multipart_checksum_type(&object_info), Some(ChecksumType::CRC64_NVME));
    }

    #[test]
    fn test_data_movement_complete_part_preserves_source_part_checksums() {
        let source_part = ObjectPartInfo {
            number: 2,
            etag: "etag-2".to_string(),
            checksums: Some(HashMap::from([
                (ChecksumType::CRC32.to_string(), "crc32-value".to_string()),
                (ChecksumType::CRC32C.to_string(), "crc32c-value".to_string()),
                (ChecksumType::SHA1.to_string(), "sha1-value".to_string()),
                (ChecksumType::SHA256.to_string(), "sha256-value".to_string()),
                (ChecksumType::CRC64_NVME.to_string(), "crc64-value".to_string()),
            ])),
            ..Default::default()
        };

        let complete = data_movement_complete_part(2, Some("etag-2".to_string()), &source_part);

        assert_eq!(complete.part_num, 2);
        assert_eq!(complete.etag.as_deref(), Some("etag-2"));
        assert_eq!(complete.checksum_crc32.as_deref(), Some("crc32-value"));
        assert_eq!(complete.checksum_crc32c.as_deref(), Some("crc32c-value"));
        assert_eq!(complete.checksum_sha1.as_deref(), Some("sha1-value"));
        assert_eq!(complete.checksum_sha256.as_deref(), Some("sha256-value"));
        assert_eq!(complete.checksum_crc64nvme.as_deref(), Some("crc64-value"));
    }

    #[test]
    fn test_data_movement_part_reader_uses_stored_part_size_for_raw_stream() {
        let source_part = ObjectPartInfo {
            number: 1,
            size: 32,
            actual_size: 128,
            etag: "etag-1".to_string(),
            ..Default::default()
        };

        let part_size = i64::try_from(source_part.size).expect("part size fits");
        let part_actual_size = if source_part.actual_size > 0 {
            source_part.actual_size
        } else {
            part_size
        };

        assert_eq!(part_size, 32);
        assert_eq!(part_actual_size, 128);
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
    fn test_data_movement_metadata_equivalence_accepts_required_fields() {
        let version_id = Uuid::nil();
        let mod_time = OffsetDateTime::UNIX_EPOCH;
        let metadata = Arc::new(HashMap::from([
            ("x-amz-meta-key".to_string(), "value".to_string()),
            (rustfs_utils::http::AMZ_STORAGE_CLASS.to_string(), "STANDARD_IA".to_string()),
            (X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(), "GOVERNANCE".to_string()),
            (
                X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(),
                "2030-01-01T00:00:00Z".to_string(),
            ),
            (X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str().to_string(), "ON".to_string()),
        ]));
        let part = ObjectPartInfo {
            number: 1,
            etag: "part-etag".to_string(),
            size: 128,
            actual_size: 128,
            checksums: Some(HashMap::from([(ChecksumType::CRC32C.to_string(), "part-checksum".to_string())])),
            ..Default::default()
        };
        let info = ObjectInfo {
            version_id: Some(version_id),
            etag: Some("etag-value".to_string()),
            size: 128,
            actual_size: 128,
            mod_time: Some(mod_time),
            user_defined: metadata,
            storage_class: Some("STANDARD_IA".to_string()),
            checksum: Some(Bytes::from_static(b"object-checksum")),
            replication_status_internal: Some("arn:minio:replication:target=COMPLETED;".to_string()),
            replication_status: rustfs_filemeta::ReplicationStatusType::Completed,
            version_purge_status_internal: Some("arn:minio:replication:target=PENDING;".to_string()),
            version_purge_status: rustfs_filemeta::VersionPurgeStatusType::Pending,
            parts: Arc::new(vec![part]),
            ..Default::default()
        };

        assert_data_movement_metadata_equivalent(&info, &info.clone());
    }

    #[test]
    fn test_data_movement_opts_preserve_replication_and_object_lock_metadata() {
        let version_id = Uuid::nil();
        let object_info = ObjectInfo {
            version_id: Some(version_id),
            user_defined: Arc::new(HashMap::from([
                (
                    rustfs_utils::http::SUFFIX_REPLICATION_STATUS.to_string(),
                    "arn:minio:target=PENDING;".to_string(),
                ),
                (X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(), "COMPLIANCE".to_string()),
                (
                    X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(),
                    "2031-01-01T00:00:00Z".to_string(),
                ),
                (X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str().to_string(), "ON".to_string()),
            ])),
            ..Default::default()
        };

        let put_opts = data_movement_put_object_opts(&object_info, 3);
        let new_multipart_opts = data_movement_new_multipart_opts(&object_info, 3);

        assert_eq!(
            put_opts.user_defined.get(rustfs_utils::http::SUFFIX_REPLICATION_STATUS),
            Some(&"arn:minio:target=PENDING;".to_string())
        );
        assert_eq!(
            new_multipart_opts.user_defined.get(X_AMZ_OBJECT_LOCK_MODE.as_str()),
            Some(&"COMPLIANCE".to_string())
        );
        assert_eq!(
            put_opts.user_defined.get(X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str()),
            Some(&"2031-01-01T00:00:00Z".to_string())
        );
        assert_eq!(
            new_multipart_opts.user_defined.get(X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str()),
            Some(&"ON".to_string())
        );
    }

    #[test]
    fn test_data_movement_opts_preserve_tags_and_expires() {
        let expires = OffsetDateTime::from_unix_timestamp(2_000).expect("valid timestamp");
        let object_info = ObjectInfo {
            user_defined: Arc::new(HashMap::from([("x-amz-meta-key".to_string(), "value".to_string())])),
            user_tags: Arc::new("tag=value".to_string()),
            expires: Some(expires),
            ..Default::default()
        };

        let put_opts = data_movement_put_object_opts(&object_info, 1);
        let multipart_opts = data_movement_new_multipart_opts(&object_info, 1);

        assert_eq!(
            put_opts
                .user_defined
                .get(rustfs_utils::http::AMZ_OBJECT_TAGGING)
                .map(String::as_str),
            Some("tag=value")
        );
        assert_eq!(
            multipart_opts
                .user_defined
                .get(rustfs_utils::http::AMZ_OBJECT_TAGGING)
                .map(String::as_str),
            Some("tag=value")
        );
        assert!(put_opts.user_defined.contains_key("expires"));
        assert!(multipart_opts.user_defined.contains_key("expires"));
        assert_eq!(put_opts.user_defined.get("x-amz-meta-key").map(String::as_str), Some("value"));
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
    fn test_is_equivalent_data_movement_object_rejects_user_metadata_mismatch() {
        let source = ObjectInfo {
            version_id: Some(Uuid::nil()),
            size: 128,
            etag: Some("etag-value".to_string()),
            user_defined: Arc::new(HashMap::from([("x-amz-meta-key".to_string(), "source".to_string())])),
            storage_class: Some("STANDARD_IA".to_string()),
            ..Default::default()
        };
        let target = ObjectInfo {
            user_defined: Arc::new(HashMap::from([("x-amz-meta-key".to_string(), "target".to_string())])),
            ..source.clone()
        };

        assert!(!is_equivalent_data_movement_object(&source, &target));
    }

    #[test]
    fn test_is_equivalent_data_movement_object_rejects_storage_class_mismatch() {
        let source = ObjectInfo {
            version_id: Some(Uuid::nil()),
            size: 128,
            etag: Some("etag-value".to_string()),
            storage_class: Some("STANDARD_IA".to_string()),
            ..Default::default()
        };
        let target = ObjectInfo {
            storage_class: Some("STANDARD".to_string()),
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

    fn overwrite_equivalence_source() -> ObjectInfo {
        let part = ObjectPartInfo {
            number: 1,
            etag: "part-etag".to_string(),
            size: 128,
            actual_size: 128,
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            index: Some(Bytes::from_static(&[1, 2, 3])),
            checksums: Some(HashMap::from([(ChecksumType::CRC32C.to_string(), "part-checksum".to_string())])),
            ..Default::default()
        };

        ObjectInfo {
            version_id: Some(Uuid::from_u128(1)),
            size: 128,
            actual_size: 128,
            etag: Some("etag-value".to_string()),
            checksum: Some(Bytes::from_static(b"object-checksum")),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            user_defined: Arc::new(HashMap::from([("x-amz-meta-key".to_string(), "value".to_string())])),
            user_tags: Arc::new("tag=value".to_string()),
            expires: Some(OffsetDateTime::from_unix_timestamp(2_000).expect("valid expires timestamp")),
            storage_class: Some("STANDARD_IA".to_string()),
            replication_status_internal: Some("arn:minio:replication:target=COMPLETED;".to_string()),
            replication_status: rustfs_filemeta::ReplicationStatusType::Completed,
            version_purge_status_internal: Some("arn:minio:replication:target=PENDING;".to_string()),
            version_purge_status: rustfs_filemeta::VersionPurgeStatusType::Pending,
            parts: Arc::new(vec![part]),
            ..Default::default()
        }
    }

    fn overwrite_resume_for_target(source: &ObjectInfo, target: ObjectInfo) -> bool {
        let err = Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "version".to_string());
        resolve_data_movement_overwrite_resume_result(&err, Ok(Some(target)), source, 0, 1)
            .expect("overwrite target should be evaluated")
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_full_equivalence() {
        let source = overwrite_equivalence_source();

        assert!(overwrite_resume_for_target(&source, source.clone()));
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_missing_part_checksum() {
        let source = overwrite_equivalence_source();
        let mut target = source.clone();
        let mut parts = target.parts.as_ref().clone();
        parts[0].checksums = None;
        target.parts = Arc::new(parts);

        assert!(!overwrite_resume_for_target(&source, target));
    }

    fn overwrite_equivalence_source_with_two_parts() -> ObjectInfo {
        let source = overwrite_equivalence_source();
        let mut parts = source.parts.as_ref().clone();
        let mut second_part = parts[0].clone();
        second_part.number = 2;
        second_part.etag = "part-etag-2".to_string();
        second_part.index = Some(Bytes::from_static(&[4, 5, 6]));
        second_part.checksums = Some(HashMap::from([(ChecksumType::CRC32C.to_string(), "part-checksum-2".to_string())]));
        parts.push(second_part);

        ObjectInfo {
            parts: Arc::new(parts),
            ..source
        }
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_parts_reordered_by_number() {
        let source = overwrite_equivalence_source_with_two_parts();
        let mut target = source.clone();
        let mut parts = target.parts.as_ref().clone();
        parts.reverse();
        target.parts = Arc::new(parts);

        assert!(overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_duplicate_part_number() {
        let source = overwrite_equivalence_source_with_two_parts();
        let mut target = source.clone();
        let mut parts = target.parts.as_ref().clone();
        parts[1].number = parts[0].number;
        target.parts = Arc::new(parts);

        assert!(!overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_version_purge_mismatch() {
        let source = overwrite_equivalence_source();
        let target = ObjectInfo {
            version_purge_status_internal: Some("arn:minio:replication:target=COMPLETE;".to_string()),
            version_purge_status: rustfs_filemeta::VersionPurgeStatusType::Complete,
            ..source.clone()
        };

        assert!(!overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_replication_mismatch() {
        let source = overwrite_equivalence_source();
        let target = ObjectInfo {
            replication_status_internal: Some("arn:minio:replication:target=FAILED;".to_string()),
            replication_status: rustfs_filemeta::ReplicationStatusType::Failed,
            ..source.clone()
        };

        assert!(!overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_tag_mismatch() {
        let source = overwrite_equivalence_source();
        let target = ObjectInfo {
            user_tags: Arc::new(String::new()),
            ..source.clone()
        };

        assert!(!overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_expires_mismatch() {
        let source = overwrite_equivalence_source();
        let target = ObjectInfo {
            expires: None,
            ..source.clone()
        };

        assert!(!overwrite_resume_for_target(&source, target));
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
    fn test_rebalance_overwrite_resume_accepts_equivalent_target_version() {
        let source = ObjectInfo {
            version_id: Some(Uuid::from_u128(1)),
            size: 128,
            etag: Some("etag-value".to_string()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            user_defined: Arc::new(HashMap::from([("x-amz-meta-key".to_string(), "value".to_string())])),
            ..Default::default()
        };
        let err = Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "version".to_string());

        let should_resume = resolve_data_movement_overwrite_resume_result(&err, Ok(Some(source.clone())), &source, 2, 3)
            .expect("rebalance overwrite should converge when the target version is equivalent");

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
    fn test_rebalance_overwrite_resume_rejects_different_target_version() {
        let source = ObjectInfo {
            version_id: Some(Uuid::from_u128(1)),
            size: 128,
            etag: Some("etag-value".to_string()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };
        let target = ObjectInfo {
            version_id: Some(Uuid::from_u128(2)),
            ..source.clone()
        };
        let err = Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "version".to_string());

        let should_resume = resolve_data_movement_overwrite_resume_result(&err, Ok(Some(target)), &source, 2, 3)
            .expect("rebalance overwrite should evaluate a different target version");

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
