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

// #730: data-movement migration keeps staged cleanup helpers until copy paths converge.

pub(crate) mod backpressure;

use crate::error::{
    Error, Result, is_err_data_movement_overwrite, is_err_invalid_upload_id, is_err_object_not_found, is_err_version_not_found,
};
use crate::object_api::{GetObjectReader, ObjectInfo, ObjectOptions, PutObjReader};
use crate::set_disk::{SetDisks, get_lock_acquire_timeout};
use crate::storage_api_contracts::{
    multipart::{CompletePart, MultipartOperations as _},
    namespace::NamespaceLocking as _,
    object::{HTTPPreconditions, ObjectOperations as _},
};
use crate::store::{ECStore, ObjectLockDiagGuard, SourceCleanupMutationFence};
use bytes::Bytes;
use rustfs_filemeta::{FileInfo, FileInfoVersions, ObjectPartInfo};
use rustfs_rio::{EtagResolvable, HashReader, HashReaderDetector, Index, TryGetIndex};
use rustfs_utils::http::{
    AMZ_OBJECT_TAGGING, SUFFIX_ACTUAL_SIZE, SUFFIX_COMPRESSION_SIZE, SUFFIX_CRC, SUFFIX_DATA_MOVED, SUFFIX_DATA_MOVED_TAGS,
    SUFFIX_DATA_MOVEMENT_UPLOAD, SUFFIX_PART_CHECKSUMS, SUFFIX_TRANSITION_STATUS, SUFFIX_TRANSITION_TIER,
    SUFFIX_TRANSITIONED_OBJECTNAME, SUFFIX_TRANSITIONED_VERSION_ID, SUFFIX_TRANSITIONED_VERSION_STATE,
    strip_internal_prefix_preserving_case,
};
use rustfs_utils::path::encode_dir_object;
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::task::{Context, Poll};
use std::time::Duration as StdDuration;
use time::format_description::well_known::Rfc3339;
use tokio::io::{AsyncRead, BufReader, ReadBuf};
use tracing::{error, info};

type SharedDataMovementStream = Arc<Mutex<Box<dyn AsyncRead + Unpin + Send + Sync>>>;
const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_DATA_MOVEMENT: &str = "data_movement";
const EVENT_DATA_MOVEMENT_MULTIPART_ABORT_FAILED: &str = "data_movement_multipart_abort_failed";
const DATA_MOVEMENT_MULTIPART_ABORT_RETRY_ATTEMPTS: usize = 3;
const DATA_MOVEMENT_MULTIPART_ABORT_RETRY_DELAY_SECS: u64 = 60;

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

fn insert_data_movement_checksum(user_defined: &mut HashMap<String, String>, object_info: &ObjectInfo) {
    rustfs_utils::http::remove_header_map(user_defined, rustfs_utils::http::SUFFIX_REPLICATION_SSEC_CRC);
    if let Some(checksum) = object_info.checksum.as_ref().filter(|checksum| !checksum.is_empty()) {
        rustfs_utils::http::insert_header_map(
            user_defined,
            rustfs_utils::http::SUFFIX_REPLICATION_SSEC_CRC,
            base64_simd::STANDARD.encode_to_string(checksum),
        );
    }
}

fn data_movement_upload_identity(object_info: &ObjectInfo) -> String {
    let version_id = object_info
        .version_id
        .map_or_else(|| "none".to_string(), |version_id| version_id.to_string());
    let mod_time = object_info
        .mod_time
        .map_or_else(|| "none".to_string(), |mod_time| mod_time.unix_timestamp_nanos().to_string());
    format!("v1:{version_id}:{mod_time}")
}

fn data_movement_new_multipart_opts(object_info: &ObjectInfo, src_pool_idx: usize) -> ObjectOptions {
    let mut user_defined = data_movement_user_defined(object_info);
    let upload_identity = data_movement_upload_identity(object_info);
    rustfs_utils::http::insert_str(&mut user_defined, SUFFIX_DATA_MOVEMENT_UPLOAD, upload_identity);
    ObjectOptions {
        versioned: object_info.version_id.is_some(),
        version_id: object_info.version_id.as_ref().map(|v| v.to_string()),
        user_defined,
        preserve_etag: object_info.etag.clone(),
        src_pool_idx,
        data_movement: true,
        ..Default::default()
    }
}

fn data_movement_user_defined(object_info: &ObjectInfo) -> HashMap<String, String> {
    let mut user_defined = object_info
        .user_defined
        .iter()
        .filter(|(key, _)| {
            !is_data_movement_internal_metadata(key, SUFFIX_DATA_MOVEMENT_UPLOAD)
                && !is_data_movement_internal_metadata(key, SUFFIX_PART_CHECKSUMS)
        })
        .map(|(key, value)| (key.clone(), value.clone()))
        .collect::<HashMap<_, _>>();
    let remove_canonical = |metadata: &mut HashMap<String, String>, suffix: &str| {
        metadata.remove(&rustfs_utils::http::internal_key_rustfs(suffix));
        metadata.remove(&format!("{}{suffix}", rustfs_utils::http::MINIO_INTERNAL_PREFIX));
    };
    if object_info.checksum.as_ref().is_some_and(|checksum| !checksum.is_empty()) {
        rustfs_utils::http::remove_str(&mut user_defined, SUFFIX_CRC);
    } else {
        remove_canonical(&mut user_defined, SUFFIX_CRC);
    }
    for (suffix, value) in [
        (SUFFIX_TRANSITION_STATUS, object_info.transitioned_object.status.as_str()),
        (SUFFIX_TRANSITIONED_OBJECTNAME, object_info.transitioned_object.name.as_str()),
        (SUFFIX_TRANSITION_TIER, object_info.transitioned_object.tier.as_str()),
    ] {
        if value.is_empty() {
            remove_canonical(&mut user_defined, suffix);
            continue;
        }
        rustfs_utils::http::remove_str(&mut user_defined, suffix);
        rustfs_utils::http::insert_str(&mut user_defined, suffix, value.to_string());
    }
    if object_info.transitioned_object.version_id.is_empty() {
        let version_is_semantically_empty = user_defined
            .iter()
            .filter(|(key, _)| is_data_movement_internal_metadata(key, SUFFIX_TRANSITIONED_VERSION_ID))
            .all(|(_, value)| is_empty_data_movement_transition_version(value));
        if version_is_semantically_empty {
            remove_canonical(&mut user_defined, SUFFIX_TRANSITIONED_VERSION_ID);
        }
    } else {
        rustfs_utils::http::remove_str(&mut user_defined, SUFFIX_TRANSITIONED_VERSION_ID);
        rustfs_utils::http::insert_str(
            &mut user_defined,
            SUFFIX_TRANSITIONED_VERSION_ID,
            object_info.transitioned_object.version_id.clone(),
        );
    }
    let transition_version_state = (object_info.transition_version_state != rustfs_filemeta::TransitionVersionState::Unknown)
        .then(|| object_info.transition_version_state.as_str());
    if let Some(transition_version_state) = transition_version_state {
        rustfs_utils::http::remove_str(&mut user_defined, SUFFIX_TRANSITIONED_VERSION_STATE);
        rustfs_utils::http::insert_str(
            &mut user_defined,
            SUFFIX_TRANSITIONED_VERSION_STATE,
            transition_version_state.to_string(),
        );
    } else {
        remove_canonical(&mut user_defined, SUFFIX_TRANSITIONED_VERSION_STATE);
    }
    user_defined.remove(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM);
    user_defined.remove(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE);
    insert_data_movement_checksum(&mut user_defined, object_info);
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

fn data_movement_part_checksums(parts: &[ObjectPartInfo]) -> Result<Option<String>> {
    let mut part_checksums = BTreeMap::<usize, BTreeMap<String, String>>::new();
    for part in parts {
        let Some(checksums) = part.checksums.as_ref().filter(|checksums| !checksums.is_empty()) else {
            continue;
        };
        let checksums = checksums.iter().map(|(key, value)| (key.clone(), value.clone())).collect();
        if part_checksums.insert(part.number, checksums).is_some() {
            return Err(Error::other("data movement source has duplicate part numbers"));
        }
    }
    if part_checksums.is_empty() {
        return Ok(None);
    }
    let part_checksums = part_checksums
        .into_iter()
        .map(|(part_number, checksums)| (part_number, checksums.into_iter().collect::<Vec<_>>()))
        .collect::<Vec<_>>();
    serde_json::to_string(&part_checksums)
        .map(Some)
        .map_err(|err| Error::other(format!("data movement part checksum metadata encode failed: {err}")))
}

pub(crate) fn prepare_tiered_data_movement_file_info(file_info: &mut rustfs_filemeta::FileInfo) -> Result<()> {
    prepare_tiered_data_movement_file_info_for(file_info, data_movement_part_checksum_writer_enabled())
}

fn prepare_tiered_data_movement_file_info_for(file_info: &mut rustfs_filemeta::FileInfo, writer_enabled: bool) -> Result<()> {
    if !writer_enabled {
        rustfs_utils::http::remove_str(&mut file_info.metadata, SUFFIX_PART_CHECKSUMS);
        for part in &mut file_info.parts {
            part.checksums = None;
        }
        return Ok(());
    }

    SetDisks::hydrate_selected_fileinfo_part_checksums(file_info).map_err(|_| Error::FileCorrupt)?;
    rustfs_utils::http::remove_str(&mut file_info.metadata, SUFFIX_PART_CHECKSUMS);
    if let Some(encoded) = data_movement_part_checksums(&file_info.parts)? {
        rustfs_utils::http::insert_str(&mut file_info.metadata, SUFFIX_PART_CHECKSUMS, encoded);
    }
    Ok(())
}

fn data_movement_part_checksum_writer_enabled_for(requested: bool, fleet_confirmed: bool) -> bool {
    requested && fleet_confirmed
}

fn data_movement_part_checksum_writer_enabled() -> bool {
    data_movement_part_checksum_writer_enabled_for(
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_WRITE,
            rustfs_config::DEFAULT_DATA_MOVEMENT_PART_CHECKSUMS_WRITE,
        ),
        rustfs_utils::get_env_bool(
            rustfs_config::ENV_DATA_MOVEMENT_PART_CHECKSUMS_FLEET_CONFIRMED,
            rustfs_config::DEFAULT_DATA_MOVEMENT_PART_CHECKSUMS_FLEET_CONFIRMED,
        ),
    )
}

fn should_use_multipart_data_movement(object_info: &ObjectInfo, has_part_checksums: bool) -> bool {
    object_info.is_multipart()
        || has_part_checksums
        || object_info.parts.len() > 1
        || object_info.parts.first().is_some_and(|part| part.number != 1)
}

fn data_movement_complete_multipart_opts(
    object_info: &ObjectInfo,
    src_pool_idx: usize,
    preserve_part_checksums: bool,
) -> Result<ObjectOptions> {
    let mut user_defined = HashMap::new();
    insert_data_movement_checksum(&mut user_defined, object_info);
    let actual_size = object_info
        .get_actual_size()
        .map_err(|err| Error::other(format!("data movement source actual size is invalid: {err}")))?;
    if actual_size < 0 {
        return Err(Error::other("data movement source actual size is unknown"));
    }
    rustfs_utils::http::insert_str(&mut user_defined, SUFFIX_ACTUAL_SIZE, actual_size.to_string());
    if preserve_part_checksums && let Some(encoded) = data_movement_part_checksums(&object_info.parts)? {
        rustfs_utils::http::insert_str(&mut user_defined, SUFFIX_PART_CHECKSUMS, encoded);
    }
    Ok(ObjectOptions {
        versioned: object_info.version_id.is_some(),
        version_id: object_info.version_id.as_ref().map(|v| v.to_string()),
        http_preconditions: Some(data_movement_target_precondition()),
        data_movement: true,
        mod_time: object_info.mod_time,
        preserve_etag: object_info.etag.clone(),
        user_defined,
        src_pool_idx,
        ..Default::default()
    })
}

fn data_movement_put_object_opts(object_info: &ObjectInfo, src_pool_idx: usize) -> ObjectOptions {
    ObjectOptions {
        versioned: object_info.version_id.is_some(),
        src_pool_idx,
        data_movement: true,
        version_id: object_info.version_id.as_ref().map(|v| v.to_string()),
        http_preconditions: Some(data_movement_target_precondition()),
        mod_time: object_info.mod_time,
        user_defined: data_movement_user_defined(object_info),
        preserve_etag: object_info.etag.clone(),
        ..Default::default()
    }
}

fn is_unversioned_data_movement_object(object_info: &ObjectInfo) -> bool {
    object_info.version_id.is_none_or(|version_id| version_id.is_nil())
}

pub(crate) fn data_movement_target_precondition() -> HTTPPreconditions {
    HTTPPreconditions {
        if_none_match: Some("*".to_string()),
        ..Default::default()
    }
}

fn is_owned_data_movement_target(target: &ObjectInfo) -> bool {
    let rustfs_marker = rustfs_utils::http::internal_key_rustfs(SUFFIX_DATA_MOVED);
    let minio_marker = format!("{}{SUFFIX_DATA_MOVED}", rustfs_utils::http::MINIO_INTERNAL_PREFIX);
    if rustfs_utils::http::get_consistent_str(&target.user_defined, SUFFIX_DATA_MOVED) != Some("true")
        || target.user_defined.get(&rustfs_marker).map(String::as_str) != Some("true")
        || target.user_defined.get(&minio_marker).map(String::as_str) != Some("true")
    {
        return false;
    }

    let tags_proof = format!("v1:{}", target.user_tags);
    let rustfs_tags_proof = rustfs_utils::http::internal_key_rustfs(SUFFIX_DATA_MOVED_TAGS);
    let minio_tags_proof = format!("{}{SUFFIX_DATA_MOVED_TAGS}", rustfs_utils::http::MINIO_INTERNAL_PREFIX);
    rustfs_utils::http::get_consistent_str(&target.user_defined, SUFFIX_DATA_MOVED_TAGS) == Some(tags_proof.as_str())
        && target.user_defined.get(&rustfs_tags_proof).map(String::as_str) == Some(tags_proof.as_str())
        && target.user_defined.get(&minio_tags_proof).map(String::as_str) == Some(tags_proof.as_str())
}

pub(crate) fn can_replace_stale_data_movement_target(target: &ObjectInfo, opts: &ObjectOptions) -> bool {
    let Some(preconditions) = opts.http_preconditions.as_ref() else {
        return false;
    };
    if !opts.data_movement
        || preconditions.if_none_match_value() != Some("*")
        || preconditions.if_match_value().is_some()
        || target.delete_marker
    {
        return false;
    }

    if !is_owned_data_movement_target(target) {
        return false;
    }

    let version_matches = match (opts.version_id.as_deref(), target.version_id) {
        (None, None) => true,
        (Some(expected), Some(actual)) => uuid::Uuid::parse_str(expected).ok() == Some(actual),
        _ => false,
    };

    version_matches
        && target
            .mod_time
            .zip(opts.mod_time)
            .is_some_and(|(target_time, source_time)| target_time < source_time)
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
    Ok(PutObjReader::new(hrd))
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

/// A data-movement stage failure that keeps the error it wrapped.
///
/// The rendered message is byte-identical to the `format!` this replaced, so
/// logs and any message-matching callers are unaffected. What changes is that
/// the original error stays reachable through `source()`, which is what lets
/// the decommission loop classify by type instead of by substring
/// (backlog#1827 T2).
#[derive(Debug)]
struct DataMovementStageError {
    rendered: String,
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl std::fmt::Display for DataMovementStageError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.rendered)
    }
}

impl std::error::Error for DataMovementStageError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}

fn data_movement_stage_error<E>(op_label: &str, stage: &str, bucket: &str, object: &str, err: E) -> Error
where
    E: std::error::Error + Send + Sync + 'static,
{
    let rendered = format!("{op_label}: {stage} failed for {bucket}/{object}: {err}");
    Error::other(DataMovementStageError {
        rendered,
        source: Box::new(err),
    })
}

#[cfg(test)]
pub(crate) fn data_movement_stage_error_for_test(op_label: &str, stage: &str, bucket: &str, object: &str, err: Error) -> Error {
    data_movement_stage_error(op_label, stage, bucket, object, err)
}

/// Recover the error a [`data_movement_stage_error`] wrapped, if this is one.
///
/// `Error::other` boxes through `std::io::Error`, so the chain is
/// `StorageError::Io` -> `DataMovementStageError` -> the original error.
pub(crate) fn data_movement_stage_source(err: &Error) -> Option<&Error> {
    let Error::Io(io_err) = err else {
        return None;
    };
    io_err
        .get_ref()?
        .downcast_ref::<DataMovementStageError>()?
        .source
        .downcast_ref::<Error>()
}

fn schedule_data_movement_multipart_abort_cleanup(
    store: Arc<ECStore>,
    target_pool_idx: usize,
    bucket: String,
    object: String,
    upload_id: String,
    op_label: &str,
) {
    let op_label = op_label.to_string();
    tokio::spawn(async move {
        for attempt in 1..=DATA_MOVEMENT_MULTIPART_ABORT_RETRY_ATTEMPTS {
            tokio::time::sleep(StdDuration::from_secs(DATA_MOVEMENT_MULTIPART_ABORT_RETRY_DELAY_SECS)).await;

            let Some(pool) = store.pools.get(target_pool_idx).cloned() else {
                error!(
                    "{op_label}: background abort_multipart_upload cleanup skipped for {bucket}/{object} upload {upload_id}: target pool {target_pool_idx} is out of range"
                );
                return;
            };

            match pool
                .abort_multipart_upload(
                    &bucket,
                    &object,
                    &upload_id,
                    &ObjectOptions {
                        data_movement: true,
                        ..Default::default()
                    },
                )
                .await
            {
                Ok(()) => {
                    info!(
                        "{op_label}: background abort_multipart_upload cleanup succeeded for {bucket}/{object} upload {upload_id} on attempt {attempt}"
                    );
                    return;
                }
                Err(err) if is_err_invalid_upload_id(&err) => {
                    info!(
                        "{op_label}: background abort_multipart_upload cleanup found {bucket}/{object} upload {upload_id} already removed"
                    );
                    return;
                }
                Err(err) => {
                    error!(
                        "{op_label}: background abort_multipart_upload cleanup attempt {attempt} failed for {bucket}/{object} upload {upload_id}: {err:?}"
                    );
                }
            }
        }

        crate::bucket::lifecycle::bucket_lifecycle_ops::schedule_stale_multipart_upload_cleanup_once(store);
    });
}

fn should_check_data_movement_overwrite_resume(err: &Error) -> bool {
    is_err_data_movement_overwrite(err) || is_err_invalid_upload_id(err) || matches!(err, Error::PreconditionFailed)
}

fn effective_actual_size(info: &ObjectInfo) -> Option<i64> {
    info.get_actual_size().ok()
}

fn effective_part_actual_size(part: &ObjectPartInfo) -> Option<i64> {
    (part.actual_size != 0)
        .then_some(part.actual_size)
        .or_else(|| i64::try_from(part.size).ok())
}

fn is_equivalent_data_movement_part(source: &ObjectPartInfo, target: &ObjectPartInfo, compare_checksums: bool) -> bool {
    // Multipart migration rewrites part timestamps.
    source.number == target.number
        && source.etag == target.etag
        && source.size == target.size
        && matches!(
            (effective_part_actual_size(source), effective_part_actual_size(target)),
            (Some(source_size), Some(target_size)) if source_size == target_size
        )
        // A missing target compression index selects the safe full-read fallback.
        && (target.index.is_none() || source.index == target.index)
        && (!compare_checksums
            || source.checksums.as_ref().filter(|checksums| !checksums.is_empty())
                == target.checksums.as_ref().filter(|checksums| !checksums.is_empty()))
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

pub(crate) fn are_equivalent_data_movement_parts(source: &[ObjectPartInfo], target: &[ObjectPartInfo]) -> bool {
    are_equivalent_data_movement_parts_for(source, target, true)
}

fn are_equivalent_data_movement_parts_for(source: &[ObjectPartInfo], target: &[ObjectPartInfo], compare_checksums: bool) -> bool {
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
            .is_some_and(|target_part| is_equivalent_data_movement_part(source_part, target_part, compare_checksums))
    })
}

fn is_data_movement_internal_metadata(key: &str, suffix: &str) -> bool {
    strip_internal_prefix_preserving_case(key).is_some_and(|candidate| candidate.eq_ignore_ascii_case(suffix))
}

fn is_canonical_data_movement_internal_metadata(key: &str, suffix: &str) -> bool {
    key.strip_prefix(rustfs_utils::http::RUSTFS_INTERNAL_PREFIX) == Some(suffix)
        || key.strip_prefix(rustfs_utils::http::MINIO_INTERNAL_PREFIX) == Some(suffix)
}

fn data_movement_layout_marker_presence(object_info: &ObjectInfo) -> Option<bool> {
    if !rustfs_utils::http::contains_key_str(&object_info.user_defined, crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX) {
        return Some(false);
    }
    let data_dir = object_info.data_dir.filter(|data_dir| !data_dir.is_nil())?;
    let mut expected_buf = [0_u8; 36];
    let expected = data_dir.hyphenated().encode_lower(&mut expected_buf);
    crate::object_api::has_encrypted_part_layout_marker(
        &object_info.user_defined,
        crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
        expected,
    )
    .then_some(true)
}

fn data_movement_size_marker_presence(object_info: &ObjectInfo, suffix: &str, expected: i64) -> Option<bool> {
    let mut present = false;
    for (key, value) in object_info.user_defined.iter() {
        if !is_data_movement_internal_metadata(key, suffix) {
            continue;
        }
        present = true;
        if value.parse::<i64>().ok() != Some(expected) {
            return None;
        }
    }
    Some(present)
}

fn data_movement_checksum_marker_presence(object_info: &ObjectInfo) -> Option<bool> {
    let mut present = false;
    for (key, value) in object_info.user_defined.iter() {
        if !is_data_movement_internal_metadata(key, SUFFIX_CRC) {
            continue;
        }
        let Some(checksum) = object_info.checksum.as_deref().filter(|checksum| !checksum.is_empty()) else {
            if value.is_empty() {
                continue;
            }
            return None;
        };
        present = true;
        match std::str::from_utf8(checksum) {
            Ok(checksum) if value != checksum => return None,
            Err(_) if !value.is_empty() => return None,
            _ => {}
        }
    }
    Some(present)
}

fn is_compatible_data_movement_marker_presence(source: Option<bool>, target: Option<bool>) -> bool {
    matches!((source, target), (Some(false), Some(_)) | (Some(true), Some(true)))
}

fn is_empty_data_movement_transition_version(value: &str) -> bool {
    value.is_empty()
        || uuid::Uuid::from_slice(value.as_bytes()).is_ok_and(|version_id| version_id.is_nil())
        || uuid::Uuid::parse_str(value).is_ok_and(|version_id| version_id.is_nil())
}

fn is_data_movement_rewritten_transition_metadata(object_info: &ObjectInfo, key: &str) -> bool {
    let state = (object_info.transition_version_state != rustfs_filemeta::TransitionVersionState::Unknown)
        .then(|| object_info.transition_version_state.as_str());
    [
        (SUFFIX_TRANSITION_STATUS, Some(object_info.transitioned_object.status.as_str())),
        (SUFFIX_TRANSITIONED_OBJECTNAME, Some(object_info.transitioned_object.name.as_str())),
        (SUFFIX_TRANSITIONED_VERSION_ID, Some(object_info.transitioned_object.version_id.as_str())),
        (SUFFIX_TRANSITIONED_VERSION_STATE, state),
        (SUFFIX_TRANSITION_TIER, Some(object_info.transitioned_object.tier.as_str())),
    ]
    .iter()
    .any(|(suffix, expected)| {
        let canonical = is_canonical_data_movement_internal_metadata(key, suffix);
        let preserves_unusable_version = *suffix == SUFFIX_TRANSITIONED_VERSION_ID
            && expected == &Some("")
            && object_info
                .user_defined
                .get(key)
                .is_some_and(|value| !is_empty_data_movement_transition_version(value));
        canonical && !preserves_unusable_version
            || expected.is_some_and(|expected| {
                !expected.is_empty()
                    && is_data_movement_internal_metadata(key, suffix)
                    && rustfs_utils::http::get_consistent_str(&object_info.user_defined, suffix) == Some(expected)
            })
    })
}

fn is_data_movement_rewritten_metadata(object_info: &ObjectInfo, key: &str, normalize_compression_size: bool) -> bool {
    [
        SUFFIX_DATA_MOVED,
        SUFFIX_DATA_MOVED_TAGS,
        SUFFIX_DATA_MOVEMENT_UPLOAD,
        SUFFIX_ACTUAL_SIZE,
        SUFFIX_CRC,
        SUFFIX_PART_CHECKSUMS,
        crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
    ]
    .iter()
    .any(|suffix| is_data_movement_internal_metadata(key, suffix))
        || is_data_movement_rewritten_transition_metadata(object_info, key)
        || key == rustfs_rio::RUSTFS_MULTIPART_CHECKSUM
        || key == rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE
        || normalize_compression_size && is_data_movement_internal_metadata(key, SUFFIX_COMPRESSION_SIZE)
}

pub(crate) fn is_equivalent_data_movement_metadata(
    source: &ObjectInfo,
    target: &ObjectInfo,
    source_actual_size: i64,
    target_actual_size: i64,
) -> bool {
    if !is_compatible_data_movement_marker_presence(
        data_movement_size_marker_presence(source, SUFFIX_ACTUAL_SIZE, source_actual_size),
        data_movement_size_marker_presence(target, SUFFIX_ACTUAL_SIZE, target_actual_size),
    ) || !is_compatible_data_movement_marker_presence(
        data_movement_checksum_marker_presence(source),
        data_movement_checksum_marker_presence(target),
    ) {
        return false;
    }

    let normalize_compression_size = source.is_compressed() && target.is_compressed();
    if normalize_compression_size
        && !is_compatible_data_movement_marker_presence(
            data_movement_size_marker_presence(source, SUFFIX_COMPRESSION_SIZE, source.size),
            data_movement_size_marker_presence(target, SUFFIX_COMPRESSION_SIZE, target.size),
        )
    {
        return false;
    }
    let Some(source_layout) = data_movement_layout_marker_presence(source) else {
        return false;
    };
    let Some(target_layout) = data_movement_layout_marker_presence(target) else {
        return false;
    };
    if (source_layout || target_layout)
        && !(source.data_dir.is_some_and(|data_dir| !data_dir.is_nil())
            && target.data_dir.is_some_and(|data_dir| !data_dir.is_nil()))
    {
        return false;
    }

    source
        .user_defined
        .iter()
        .filter(|(key, _)| !is_data_movement_rewritten_metadata(source, key, normalize_compression_size))
        .all(|(key, value)| target.user_defined.get(key) == Some(value))
        && target
            .user_defined
            .iter()
            .filter(|(key, _)| !is_data_movement_rewritten_metadata(target, key, normalize_compression_size))
            .all(|(key, value)| source.user_defined.get(key) == Some(value))
}

fn is_equivalent_data_movement_object_identity(
    source: &ObjectInfo,
    target: &ObjectInfo,
    compare_mod_time: bool,
    compare_part_checksums: bool,
) -> bool {
    let (Some(source_actual_size), Some(target_actual_size)) = (effective_actual_size(source), effective_actual_size(target))
    else {
        return false;
    };

    source.version_id == target.version_id
        && source.delete_marker == target.delete_marker
        && source.size == target.size
        && source_actual_size == target_actual_size
        && source.etag == target.etag
        && source.checksum == target.checksum
        && (!compare_mod_time || source.mod_time == target.mod_time)
        && source.storage_class == target.storage_class
        && is_equivalent_data_movement_metadata(source, target, source_actual_size, target_actual_size)
        && source.user_tags == target.user_tags
        && source.expires == target.expires
        && source.replication_status_internal == target.replication_status_internal
        && source.replication_status == target.replication_status
        && source.version_purge_status_internal == target.version_purge_status_internal
        && source.version_purge_status == target.version_purge_status
        && source.transitioned_object.name == target.transitioned_object.name
        && source.transitioned_object.version_id == target.transitioned_object.version_id
        && source.transitioned_object.tier == target.transitioned_object.tier
        && source.transitioned_object.free_version == target.transitioned_object.free_version
        && source.transitioned_object.status == target.transitioned_object.status
        && source.transition_version_state == target.transition_version_state
        && are_equivalent_data_movement_parts_for(&source.parts, &target.parts, compare_part_checksums)
}

#[cfg(test)]
fn is_equivalent_data_movement_object(source: &ObjectInfo, target: &ObjectInfo) -> bool {
    is_equivalent_data_movement_object_identity(source, target, true, true)
}

fn is_superseding_unversioned_data_movement_object(source: &ObjectInfo, target: &ObjectInfo) -> bool {
    is_unversioned_data_movement_object(source)
        && is_unversioned_data_movement_object(target)
        && source
            .mod_time
            .zip(target.mod_time)
            .is_some_and(|(source_time, target_time)| target_time > source_time)
}

fn is_data_movement_upload_takeover_target(source: &ObjectInfo, target: &ObjectInfo, compare_part_checksums: bool) -> bool {
    let identity = data_movement_upload_identity(source);
    source.mod_time.is_some()
        && rustfs_utils::http::get_consistent_str(&target.user_defined, SUFFIX_DATA_MOVEMENT_UPLOAD) == Some(identity.as_str())
        && is_equivalent_data_movement_object_identity(source, target, false, compare_part_checksums)
}

fn is_legacy_data_movement_checksum_target(source: &ObjectInfo, target: &ObjectInfo) -> bool {
    let has_checksums = |part: &ObjectPartInfo| part.checksums.as_ref().is_some_and(|checksums| !checksums.is_empty());
    source.parts.iter().any(has_checksums)
        && target.parts.iter().all(|part| part.checksums.is_none())
        && !rustfs_utils::http::contains_key_str(&target.user_defined, SUFFIX_PART_CHECKSUMS)
        && is_owned_data_movement_target(target)
        && is_equivalent_data_movement_object_identity(source, target, true, false)
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
struct SourceCleanupErasureIdentity {
    algorithm: String,
    data_blocks: usize,
    parity_blocks: usize,
    block_size: usize,
    distribution: Vec<usize>,
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
    transition_status: String,
    transitioned_objname: String,
    transition_tier: String,
    transition_version_id: Option<uuid::Uuid>,
    transition_version: Option<String>,
    transition_version_state: &'static str,
    expire_restored: bool,
    erasure: SourceCleanupErasureIdentity,
    metadata: BTreeMap<String, String>,
    parts: Vec<SourceCleanupPartIdentity>,
}

fn source_cleanup_erasure_identity(erasure: &rustfs_filemeta::ErasureInfo) -> SourceCleanupErasureIdentity {
    SourceCleanupErasureIdentity {
        algorithm: erasure.algorithm.clone(),
        data_blocks: erasure.data_blocks,
        parity_blocks: erasure.parity_blocks,
        block_size: erasure.block_size,
        distribution: erasure.distribution.clone(),
    }
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
        transition_status: version.transition_status.clone(),
        transitioned_objname: version.transitioned_objname.clone(),
        transition_tier: version.transition_tier.clone(),
        transition_version_id: version.transition_version_id,
        transition_version: version.transition_version.clone(),
        transition_version_state: version.transition_version_state.as_str(),
        expire_restored: version.expire_restored,
        erasure: source_cleanup_erasure_identity(&version.erasure),
        metadata: version
            .metadata
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect(),
        parts,
    }
}

fn source_cleanup_versions_match_with_allowed_missing(
    expected: &FileInfoVersions,
    current: &FileInfoVersions,
    allowed_missing: &[SourceCleanupVersionIdentity],
) -> bool {
    let mut expected_free_versions: Vec<_> = expected.free_versions.iter().map(source_cleanup_version_identity).collect();
    let mut current_free_versions: Vec<_> = current.free_versions.iter().map(source_cleanup_version_identity).collect();
    expected_free_versions.sort();
    current_free_versions.sort();
    if expected_free_versions != current_free_versions {
        return false;
    }

    let mut expected_counts = BTreeMap::new();
    for identity in expected.versions.iter().map(source_cleanup_version_identity) {
        *expected_counts.entry(identity).or_insert(0usize) += 1;
    }

    for identity in current.versions.iter().map(source_cleanup_version_identity) {
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

#[derive(Debug, thiserror::Error)]
pub(crate) enum SourceCleanupError {
    #[error("source versions changed after migration started")]
    SourceChanged,
    #[error(transparent)]
    Storage(#[from] Error),
}

#[derive(Clone, Copy, Default)]
pub(crate) struct SourceCleanupBucketFence<'a> {
    pub(crate) expected_incarnation_id: Option<uuid::Uuid>,
    pub(crate) lifecycle_guard: Option<&'a rustfs_lock::NamespaceLockGuard>,
    pub(crate) object_mutation_fence: Option<&'a SourceCleanupMutationFence>,
}

fn ensure_source_cleanup_versions_match(
    expected: &FileInfoVersions,
    current: &FileInfoVersions,
    allowed_missing: &[SourceCleanupVersionIdentity],
) -> std::result::Result<(), SourceCleanupError> {
    if source_cleanup_versions_match_with_allowed_missing(expected, current, allowed_missing) {
        Ok(())
    } else {
        Err(SourceCleanupError::SourceChanged)
    }
}

pub(crate) async fn ensure_source_cleanup_versions_unchanged(
    set: Arc<SetDisks>,
    bucket: &str,
    object: &str,
    expected: &FileInfoVersions,
    allowed_missing: &[SourceCleanupVersionIdentity],
    op_label: &str,
) -> std::result::Result<(), SourceCleanupError> {
    let Some(current) = set
        .load_file_info_versions_exact(bucket, object)
        .await
        .map_err(|err| Error::other(format!("{op_label}: source cleanup preflight failed for {bucket}/{object}: {err}")))?
    else {
        return Ok(());
    };

    ensure_source_cleanup_versions_match(expected, &current, allowed_missing)
}

#[cfg(test)]
struct SourceCleanupDeleteBarrierState {
    bucket: String,
    object: String,
    fence_pending: tokio::sync::Notify,
    arrived: tokio::sync::Notify,
    is_paused: AtomicBool,
    release: tokio::sync::Notify,
}

#[cfg(test)]
#[allow(
    dead_code,
    reason = "installed by set_disk object tests behind `--features test-util` (backlog#1823)"
)]
pub(crate) struct SourceCleanupDeleteBarrier {
    state: Arc<SourceCleanupDeleteBarrierState>,
}

#[cfg(test)]
static SOURCE_CLEANUP_DELETE_BARRIERS: std::sync::OnceLock<std::sync::Mutex<Vec<Arc<SourceCleanupDeleteBarrierState>>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
#[allow(
    dead_code,
    reason = "installed by set_disk object tests behind `--features test-util` (backlog#1823)"
)]
impl SourceCleanupDeleteBarrier {
    pub(crate) fn install(bucket: &str, object: &str) -> Self {
        let state = Arc::new(SourceCleanupDeleteBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            fence_pending: tokio::sync::Notify::new(),
            arrived: tokio::sync::Notify::new(),
            is_paused: AtomicBool::new(false),
            release: tokio::sync::Notify::new(),
        });
        let mut barriers = SOURCE_CLEANUP_DELETE_BARRIERS
            .get_or_init(|| std::sync::Mutex::new(Vec::new()))
            .lock()
            .expect("source cleanup delete barrier mutex should not poison");
        assert!(
            !barriers
                .iter()
                .any(|barrier| barrier.bucket == bucket && barrier.object == object),
            "source cleanup delete barrier must be unique per object"
        );
        barriers.push(Arc::clone(&state));
        Self { state }
    }

    pub(crate) async fn wait_until_paused(&self) {
        tokio::time::timeout(StdDuration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("source cleanup should reach the pre-delete barrier");
    }

    pub(crate) async fn wait_until_fence_pending(&self) {
        tokio::time::timeout(StdDuration::from_secs(30), self.state.fence_pending.notified())
            .await
            .expect("source cleanup should attempt the fixed mutation fence");
    }

    pub(crate) fn is_paused(&self) -> bool {
        self.state.is_paused.load(Ordering::Acquire)
    }

    pub(crate) fn release(&self) {
        self.state.release.notify_one();
    }
}

#[cfg(test)]
pub(crate) fn notify_source_cleanup_mutation_fence_pending(bucket: &str, object: &str) {
    let barrier = SOURCE_CLEANUP_DELETE_BARRIERS
        .get_or_init(|| std::sync::Mutex::new(Vec::new()))
        .lock()
        .expect("source cleanup delete barrier mutex should not poison")
        .iter()
        .find(|barrier| barrier.bucket == bucket && barrier.object == object)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.fence_pending.notify_one();
    }
}

#[cfg(test)]
impl Drop for SourceCleanupDeleteBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut barriers = SOURCE_CLEANUP_DELETE_BARRIERS
            .get_or_init(|| std::sync::Mutex::new(Vec::new()))
            .lock()
            .expect("source cleanup delete barrier mutex should not poison");
        barriers.retain(|state| !Arc::ptr_eq(state, &self.state));
    }
}

#[cfg(test)]
async fn pause_source_cleanup_before_delete(bucket: &str, object: &str) {
    let barrier = SOURCE_CLEANUP_DELETE_BARRIERS
        .get_or_init(|| std::sync::Mutex::new(Vec::new()))
        .lock()
        .expect("source cleanup delete barrier mutex should not poison")
        .iter()
        .find(|barrier| barrier.bucket == bucket && barrier.object == object)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.is_paused.store(true, Ordering::Release);
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

pub(crate) async fn cleanup_source_entry_if_unchanged(
    set: Arc<SetDisks>,
    bucket: &str,
    object: &str,
    expected: &FileInfoVersions,
    allowed_missing: &[SourceCleanupVersionIdentity],
    bucket_fence: SourceCleanupBucketFence<'_>,
    op_label: &str,
) -> std::result::Result<ObjectInfo, SourceCleanupError> {
    let cleanup_key = encode_dir_object(object);
    let source_guard = if bucket_fence
        .object_mutation_fence
        .is_some_and(SourceCleanupMutationFence::source_lock_covered)
    {
        None
    } else {
        let ns_lock = set.new_ns_lock(bucket, cleanup_key.as_str()).await?;
        Some(
            ns_lock
                .get_write_lock(get_lock_acquire_timeout())
                .await
                .map_err(Error::from)?,
        )
    };

    if bucket_fence
        .lifecycle_guard
        .is_some_and(rustfs_lock::NamespaceLockGuard::is_lock_lost)
    {
        return Err(SourceCleanupError::Storage(Error::other(format!(
            "{op_label}: bucket incarnation fence was lost before source cleanup"
        ))));
    }
    if bucket_fence
        .object_mutation_fence
        .is_some_and(SourceCleanupMutationFence::is_lock_lost)
    {
        return Err(SourceCleanupError::Storage(Error::other(format!(
            "{op_label}: object mutation fence was lost before source cleanup"
        ))));
    }

    ensure_source_cleanup_versions_unchanged(set.clone(), bucket, object, expected, allowed_missing, op_label).await?;

    #[cfg(test)]
    pause_source_cleanup_before_delete(bucket, object).await;

    let mut opts = ObjectOptions {
        delete_prefix: true,
        delete_prefix_object: true,
        data_movement: true,
        no_lock: true,
        expected_bucket_incarnation_id: bucket_fence.expected_incarnation_id,
        ..Default::default()
    };
    if let Some(source_guard) = source_guard.as_ref() {
        opts.add_namespace_lock_guard(source_guard);
    }
    if let Some(object_mutation_fence) = bucket_fence.object_mutation_fence {
        object_mutation_fence.add_namespace_lock_fence(&mut opts);
    }
    if let Some(bucket_lifecycle_guard) = bucket_fence.lifecycle_guard {
        opts.add_bucket_lifecycle_lock_guard(bucket_lifecycle_guard);
    }
    let result = set.delete_object(bucket, cleanup_key.as_str(), opts).await;
    if result.is_ok() {
        crate::store::list_objects::observe_scanner_namespace_mutations(bucket, 1);
    }
    result.map_err(SourceCleanupError::from)
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
        include_part_checksums: true,
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

#[allow(dead_code, reason = "resume adjudication asserted by this file's tests (backlog#1823)")]
fn resolve_data_movement_overwrite_resume_result(
    err: &Error,
    target_result: Result<Option<ObjectInfo>>,
    source: &ObjectInfo,
    src_pool_idx: usize,
    target_pool_idx: usize,
) -> Result<bool> {
    resolve_data_movement_overwrite_resume_result_for(
        err,
        target_result,
        source,
        src_pool_idx,
        target_pool_idx,
        data_movement_part_checksum_writer_enabled(),
    )
}

fn resolve_data_movement_overwrite_resume_result_for(
    err: &Error,
    target_result: Result<Option<ObjectInfo>>,
    source: &ObjectInfo,
    src_pool_idx: usize,
    target_pool_idx: usize,
    compare_part_checksums: bool,
) -> Result<bool> {
    if !should_check_data_movement_overwrite_resume(err)
        || !should_check_data_movement_resume_target(src_pool_idx, target_pool_idx)
    {
        return Ok(false);
    }

    let Some(target) = target_result? else {
        return Ok(false);
    };

    if is_equivalent_data_movement_object_identity(source, &target, true, compare_part_checksums) {
        return Ok(true);
    }

    if compare_part_checksums && is_legacy_data_movement_checksum_target(source, &target) {
        return Ok(true);
    }

    if is_data_movement_upload_takeover_target(source, &target, compare_part_checksums) {
        return Ok(true);
    }

    Ok(matches!(err, Error::PreconditionFailed) && is_superseding_unversioned_data_movement_object(source, &target))
}

async fn should_treat_data_movement_overwrite_as_complete(
    store: &ECStore,
    src_pool_idx: usize,
    target_pool_idx: usize,
    bucket: &str,
    object_info: &ObjectInfo,
    err: &Error,
    compare_part_checksums: bool,
) -> Result<bool> {
    if !should_check_data_movement_overwrite_resume(err) {
        return Ok(false);
    }

    resolve_data_movement_overwrite_resume_result_for(
        err,
        find_data_movement_target_info(store, target_pool_idx, bucket, object_info).await,
        object_info,
        src_pool_idx,
        target_pool_idx,
        compare_part_checksums,
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

pub(crate) async fn migrate_decommission_object(
    store: Arc<ECStore>,
    pool_idx: usize,
    bucket: String,
    rd: GetObjectReader,
    source_bucket_incarnation_id: Option<uuid::Uuid>,
    op_label: &str,
) -> Result<()> {
    let source = rd.object_info.clone();
    let _mutation_fence = store
        .acquire_decommission_object_mutation_fence(&bucket, &source.name)
        .await?;
    let current = find_data_movement_target_info(store.as_ref(), pool_idx, &bucket, &source)
        .await?
        .ok_or(Error::FileNotFound)?;
    if !is_equivalent_data_movement_object_identity(&source, &current, true, false) {
        return Err(Error::FileNotFound);
    }

    migrate_object_inner(
        store,
        pool_idx,
        bucket,
        rd,
        source_bucket_incarnation_id,
        op_label,
        Some(&_mutation_fence),
    )
    .await
}

pub(crate) async fn migrate_object(
    store: Arc<ECStore>,
    pool_idx: usize,
    bucket: String,
    rd: GetObjectReader,
    source_bucket_incarnation_id: Option<uuid::Uuid>,
    op_label: &str,
) -> Result<()> {
    migrate_object_inner(store, pool_idx, bucket, rd, source_bucket_incarnation_id, op_label, None).await
}

async fn migrate_object_inner(
    store: Arc<ECStore>,
    pool_idx: usize,
    bucket: String,
    rd: GetObjectReader,
    source_bucket_incarnation_id: Option<uuid::Uuid>,
    op_label: &str,
    mutation_fence: Option<&ObjectLockDiagGuard>,
) -> Result<()> {
    let object_info = rd.object_info.clone();
    let has_part_checksums = object_info
        .parts
        .iter()
        .any(|part| part.checksums.as_ref().is_some_and(|checksums| !checksums.is_empty()));

    let preserve_part_checksums = data_movement_part_checksum_writer_enabled();

    if should_use_multipart_data_movement(&object_info, has_part_checksums) {
        let mut new_multipart_opts = data_movement_new_multipart_opts(&object_info, pool_idx);
        new_multipart_opts.expected_bucket_incarnation_id = source_bucket_incarnation_id;
        let (res, target_pool_idx, expected_bucket_incarnation_id) = match store
            .handle_new_multipart_upload_with_pool_idx(&bucket, &object_info.name, &new_multipart_opts, mutation_fence)
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
                let part_actual_size = if part.actual_size == 0 { part_size } else { part.actual_size };
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
                let part_opts = ObjectOptions {
                    part_number: Some(part.number),
                    preserve_etag: Some(part.etag.clone()),
                    data_movement: true,
                    src_pool_idx: pool_idx,
                    expected_bucket_incarnation_id,
                    ..Default::default()
                };
                let pi = match store
                    .put_object_part_for_data_movement(
                        target_pool_idx,
                        &bucket,
                        &object_info.name,
                        &res.upload_id,
                        &mut data,
                        &part_opts,
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

            let mut complete_multipart_opts =
                data_movement_complete_multipart_opts(&object_info, pool_idx, preserve_part_checksums).map_err(|err| {
                    data_movement_stage_error(
                        op_label,
                        "prepare_complete_multipart",
                        bucket.as_str(),
                        object_info.name.as_str(),
                        err,
                    )
                })?;
            complete_multipart_opts.expected_bucket_incarnation_id = expected_bucket_incarnation_id;
            if let Err(err) = store
                .clone()
                .complete_multipart_upload_for_data_movement(
                    (target_pool_idx, mutation_fence),
                    &bucket,
                    &object_info.name,
                    &res.upload_id,
                    parts,
                    &complete_multipart_opts,
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
                    preserve_part_checksums,
                )
                .await?
                {
                    info!(
                        "{op_label}: complete_multipart_upload overwrite resolved by equivalent target for {}/{}",
                        bucket.as_str(),
                        object_info.name.as_str()
                    );
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

        if multipart_result.is_ok() && should_abort_multipart_upload(&abort_multipart_flag) {
            let abort_result = store
                .abort_multipart_upload_for_data_movement(
                    target_pool_idx,
                    &bucket,
                    &object_info.name,
                    &res.upload_id,
                    &ObjectOptions {
                        data_movement: true,
                        src_pool_idx: pool_idx,
                        expected_bucket_incarnation_id,
                        ..Default::default()
                    },
                )
                .await;
            match abort_result {
                Ok(()) => return Ok(()),
                Err(abort_err) if is_err_invalid_upload_id(&abort_err) => {
                    if should_treat_data_movement_overwrite_as_complete(
                        store.as_ref(),
                        pool_idx,
                        target_pool_idx,
                        bucket.as_str(),
                        &object_info,
                        &abort_err,
                        preserve_part_checksums,
                    )
                    .await?
                    {
                        return Ok(());
                    }
                    return Err(data_movement_stage_error(
                        op_label,
                        "verify_superseded_multipart",
                        bucket.as_str(),
                        object_info.name.as_str(),
                        abort_err,
                    ));
                }
                Err(abort_err) => {
                    error!(
                        event = EVENT_DATA_MOVEMENT_MULTIPART_ABORT_FAILED,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_DATA_MOVEMENT,
                        result = "error",
                        operation = op_label,
                        error = ?abort_err,
                        "data movement multipart abort failed"
                    );
                    schedule_data_movement_multipart_abort_cleanup(
                        store.clone(),
                        target_pool_idx,
                        bucket.clone(),
                        object_info.name.clone(),
                        res.upload_id.clone(),
                        op_label,
                    );
                    return Err(data_movement_stage_error(
                        op_label,
                        "abort_superseded_multipart",
                        bucket.as_str(),
                        object_info.name.as_str(),
                        abort_err,
                    ));
                }
            }
        }

        if let Err(primary_err) = multipart_result {
            if should_abort_multipart_upload(&abort_multipart_flag) {
                return match store
                    .abort_multipart_upload_for_data_movement(
                        target_pool_idx,
                        &bucket,
                        &object_info.name,
                        &res.upload_id,
                        &ObjectOptions {
                            data_movement: true,
                            src_pool_idx: pool_idx,
                            expected_bucket_incarnation_id,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(()) => Err(primary_err),
                    Err(abort_err) => {
                        error!("{op_label}: abort_multipart_upload err {:?}", &abort_err);
                        schedule_data_movement_multipart_abort_cleanup(
                            store.clone(),
                            target_pool_idx,
                            bucket.clone(),
                            object_info.name.clone(),
                            res.upload_id.clone(),
                            op_label,
                        );
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

    let mut put_opts = data_movement_put_object_opts(&object_info, pool_idx);
    put_opts.expected_bucket_incarnation_id = source_bucket_incarnation_id;
    let (target_pool_idx, put_result) = store
        .put_object_for_data_movement(&bucket, &object_info.name, &mut data, &put_opts, mutation_fence)
        .await
        .map_err(|err| data_movement_stage_error(op_label, "prepare_put_object", &bucket, &object_info.name, err))?;
    if let Err(err) = put_result {
        if should_treat_data_movement_overwrite_as_complete(
            store.as_ref(),
            pool_idx,
            target_pool_idx,
            bucket.as_str(),
            &object_info,
            &err,
            preserve_part_checksums,
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
    use crate::bucket::replication::{ReplicationStatusType, VersionPurgeStatusType};
    use rustfs_rio::{Checksum, ChecksumType};
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

        assert!(source_cleanup_versions_match_with_allowed_missing(&expected, &current, &[]));
    }

    #[test]
    fn test_rebalance_entry_cleanup_preflight_rejects_changed_source_metadata() {
        let expected = cleanup_test_versions(vec![cleanup_test_file_info("object.txt", Uuid::from_u128(1), "source")]);
        let current = cleanup_test_versions(vec![cleanup_test_file_info("object.txt", Uuid::from_u128(1), "changed")]);

        let err = ensure_source_cleanup_versions_match(&expected, &current, &[])
            .expect_err("changed source metadata must defer cleanup");
        assert!(matches!(err, SourceCleanupError::SourceChanged));
    }

    #[test]
    fn test_source_cleanup_preflight_rejects_changed_or_missing_free_version() {
        let mut expected = cleanup_test_versions(vec![cleanup_test_file_info("object.txt", Uuid::from_u128(1), "source")]);
        expected.free_versions = vec![cleanup_test_file_info("object.txt", Uuid::from_u128(2), "tier-cleanup")];

        let mut changed = expected.clone();
        changed.free_versions[0]
            .metadata
            .insert("x-amz-meta-key".to_string(), "changed".to_string());
        assert!(!source_cleanup_versions_match_with_allowed_missing(&expected, &changed, &[]));

        let mut missing = expected.clone();
        missing.free_versions.clear();
        assert!(!source_cleanup_versions_match_with_allowed_missing(&expected, &missing, &[]));
    }

    #[test]
    fn test_source_cleanup_preflight_rejects_changed_transition_or_erasure() {
        let expected = cleanup_test_versions(vec![cleanup_test_file_info("object.txt", Uuid::from_u128(1), "source")]);
        let mut current = expected.clone();
        current.versions[0].transition_tier = "COLD".to_string();
        let err = ensure_source_cleanup_versions_match(&expected, &current, &[])
            .expect_err("transition metadata changes must defer cleanup");
        assert!(matches!(err, SourceCleanupError::SourceChanged));

        let mut current = expected.clone();
        current.versions[0].erasure.algorithm = "changed".to_string();
        let err = ensure_source_cleanup_versions_match(&expected, &current, &[])
            .expect_err("erasure metadata changes must defer cleanup");
        assert!(matches!(err, SourceCleanupError::SourceChanged));
    }

    #[test]
    fn test_source_cleanup_preflight_ignores_per_disk_erasure_fields() {
        let mut expected = cleanup_test_versions(vec![cleanup_test_file_info("object.txt", Uuid::from_u128(1), "source")]);
        expected.versions[0].erasure.checksums = vec![rustfs_filemeta::ChecksumInfo {
            part_number: 1,
            hash: Bytes::from_static(b"disk-a-checksum"),
            ..Default::default()
        }];
        let mut current = expected.clone();
        current.versions[0].erasure.index = 7;
        current.versions[0].erasure.checksums[0].hash = Bytes::from_static(b"disk-b-checksum");

        assert!(source_cleanup_versions_match_with_allowed_missing(&expected, &current, &[]));
    }

    #[test]
    fn test_decommission_entry_cleanup_preflight_rejects_added_source_version() {
        let expected = cleanup_test_versions(vec![cleanup_test_file_info("object.txt", Uuid::from_u128(1), "source")]);
        let current = cleanup_test_versions(vec![
            cleanup_test_file_info("object.txt", Uuid::from_u128(1), "source"),
            cleanup_test_file_info("object.txt", Uuid::from_u128(2), "new-version"),
        ]);

        let err = ensure_source_cleanup_versions_match(&expected, &current, &[])
            .expect_err("an added source version must defer cleanup");
        assert!(matches!(err, SourceCleanupError::SourceChanged));
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

        let err = ensure_source_cleanup_versions_match(&expected, &current, &[])
            .expect_err("an unexpected missing version must defer cleanup");
        assert!(matches!(err, SourceCleanupError::SourceChanged));
    }

    #[test]
    fn test_decommission_cleanup_preflight_rejects_new_version_with_allowed_missing() {
        let migrated = cleanup_test_file_info("object.txt", Uuid::from_u128(1), "migrated");
        let expired = cleanup_test_file_info("object.txt", Uuid::from_u128(2), "expired");
        let new_version = cleanup_test_file_info("object.txt", Uuid::from_u128(3), "new-version");
        let expected = cleanup_test_versions(vec![migrated.clone(), expired.clone()]);
        let current = cleanup_test_versions(vec![migrated, new_version]);
        let allowed_missing = vec![source_cleanup_version_identity(&expired)];

        let err = ensure_source_cleanup_versions_match(&expected, &current, &allowed_missing)
            .expect_err("a new source version must defer cleanup even when an expired version may be missing");
        assert!(matches!(err, SourceCleanupError::SourceChanged));
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
    fn stage_error_renders_exactly_as_the_format_it_replaced() {
        // The wrapper gained a source; its message must not have moved, or log
        // scrapers and any message-matching caller would break (backlog#1827 T2).
        // `Error::other` renders through `StorageError::Io`, which prefixes
        // "Io error: " — that was true of the `format!` this replaced too, so
        // the full string is what must stay stable.
        let err = data_movement_stage_error("rebalance_object", "put_object", "bucket-a", "object-a", Error::SlowDown);
        assert_eq!(
            err.to_string(),
            format!("Io error: rebalance_object: put_object failed for bucket-a/object-a: {}", Error::SlowDown)
        );
        assert_eq!(
            err.to_string(),
            Error::other(format!("rebalance_object: put_object failed for bucket-a/object-a: {}", Error::SlowDown)).to_string()
        );
    }

    #[test]
    fn stage_error_keeps_the_wrapped_error_recoverable() {
        for original in [Error::DiskFull, Error::StorageFull, Error::FileNotFound, Error::SlowDown] {
            let wrapped =
                data_movement_stage_error("decommission_object", "put_object", "bucket-a", "object-a", original.clone());
            let recovered = data_movement_stage_source(&wrapped).expect("the wrapped error must be recoverable");
            assert_eq!(recovered.to_string(), original.to_string());
        }
    }

    #[test]
    fn stage_source_ignores_errors_it_did_not_wrap() {
        assert!(data_movement_stage_source(&Error::DiskFull).is_none());
        assert!(data_movement_stage_source(&Error::other("plain io error")).is_none());
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
    fn test_should_check_data_movement_overwrite_resume_accepts_conflict_errors() {
        assert!(should_check_data_movement_overwrite_resume(&Error::DataMovementOverwriteErr(
            "bucket-a".to_string(),
            "object-a".to_string(),
            "version-a".to_string(),
        )));
        assert!(should_check_data_movement_overwrite_resume(&Error::PreconditionFailed));
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

    #[test]
    fn test_data_movement_checksum_is_preserved_opaque() {
        let checksum = Bytes::from_static(b"sealed-or-plaintext-checksum");
        let object_info = ObjectInfo {
            checksum: Some(checksum.clone()),
            ..Default::default()
        };
        let opts = data_movement_put_object_opts(&object_info, 0);
        let encoded = rustfs_utils::http::get_header_map(&opts.user_defined, rustfs_utils::http::SUFFIX_REPLICATION_SSEC_CRC)
            .expect("data movement must carry the persisted checksum out of band");

        assert_eq!(
            base64_simd::STANDARD
                .decode_to_vec(&encoded)
                .expect("checksum marker should decode"),
            checksum
        );
        assert!(!rustfs_utils::http::contains_key_str(&opts.user_defined, SUFFIX_CRC));
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
            buffered_body: None,
            body_source: Default::default(),
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
    fn test_data_movement_empty_checksum_adds_no_passthrough_marker() {
        let mut object_info = ObjectInfo {
            size: 32,
            actual_size: 128,
            etag: Some("etag-value".to_string()),
            checksum: Some(Bytes::new()),
            ..Default::default()
        };
        rustfs_utils::http::insert_header_map(
            Arc::make_mut(&mut object_info.user_defined),
            rustfs_utils::http::SUFFIX_REPLICATION_SSEC_CRC,
            "stale-checksum",
        );

        let opts = data_movement_put_object_opts(&object_info, 0);

        assert!(
            rustfs_utils::http::get_header_map(&opts.user_defined, rustfs_utils::http::SUFFIX_REPLICATION_SSEC_CRC,).is_none()
        );
    }

    #[test]
    fn test_data_movement_multipart_opts_strip_upload_checksum_contract() {
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

        let opts = data_movement_new_multipart_opts(&object_info, 0);

        assert!(!opts.user_defined.contains_key(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM));
        assert!(!opts.user_defined.contains_key(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE));
    }

    #[test]
    fn test_data_movement_multipart_opts_defer_part_checksums_until_completion() {
        let object_info = ObjectInfo {
            parts: Arc::new(vec![ObjectPartInfo {
                number: 2,
                checksums: Some(HashMap::from([(ChecksumType::CRC32C.to_string(), "crc32c-value".to_string())])),
                ..Default::default()
            }]),
            ..Default::default()
        };

        let new_opts = data_movement_new_multipart_opts(&object_info, 0);
        let compatible_opts =
            data_movement_complete_multipart_opts(&object_info, 0, false).expect("compatible opts should be created");
        let complete_opts =
            data_movement_complete_multipart_opts(&object_info, 0, true).expect("complete opts should be created");

        assert!(!rustfs_utils::http::contains_key_str(&new_opts.user_defined, SUFFIX_PART_CHECKSUMS));
        assert!(rustfs_utils::http::contains_key_str(&new_opts.user_defined, SUFFIX_DATA_MOVEMENT_UPLOAD));
        assert!(!rustfs_utils::http::contains_key_str(
            &compatible_opts.user_defined,
            SUFFIX_PART_CHECKSUMS
        ));
        assert_eq!(
            rustfs_utils::http::get_consistent_str(&complete_opts.user_defined, SUFFIX_PART_CHECKSUMS),
            Some(r#"[[2,[["CRC32C","crc32c-value"]]]]"#)
        );
    }

    #[test]
    fn test_data_movement_part_checksum_writer_requires_fleet_confirmation() {
        let object_info = ObjectInfo {
            parts: Arc::new(vec![ObjectPartInfo {
                checksums: Some(HashMap::from([("CRC32C".to_string(), "AAAAAA==".to_string())])),
                ..Default::default()
            }]),
            ..Default::default()
        };

        assert!(!data_movement_part_checksum_writer_enabled_for(false, false));
        assert!(!data_movement_part_checksum_writer_enabled_for(true, false));
        assert!(!data_movement_part_checksum_writer_enabled_for(false, true));
        assert!(data_movement_part_checksum_writer_enabled_for(true, true));
        let mut compatible = FileInfo {
            parts: object_info.parts.as_ref().clone(),
            ..Default::default()
        };
        prepare_tiered_data_movement_file_info_for(&mut compatible, false)
            .expect("disabled sidecar writer should preserve data movement compatibility");
        assert!(compatible.parts.iter().all(|part| part.checksums.is_none()));
        assert!(!rustfs_utils::http::contains_key_str(&compatible.metadata, SUFFIX_PART_CHECKSUMS));

        let empty = ObjectInfo {
            parts: Arc::new(vec![ObjectPartInfo {
                checksums: Some(HashMap::new()),
                ..Default::default()
            }]),
            ..Default::default()
        };
        assert_eq!(
            data_movement_part_checksums(&empty.parts).expect("empty checksum maps should normalize"),
            None
        );
        assert!(
            !empty
                .parts
                .iter()
                .any(|part| part.checksums.as_ref().is_some_and(|checksums| !checksums.is_empty()))
        );
    }

    #[test]
    fn test_tiered_data_movement_prepares_and_validates_part_checksum_sidecar() {
        let valid_checksums = HashMap::from([("CRC32C".to_string(), "AAAAAA==".to_string())]);
        let mut valid = FileInfo {
            parts: vec![ObjectPartInfo {
                number: 1,
                checksums: Some(valid_checksums),
                ..Default::default()
            }],
            ..Default::default()
        };
        let mut compatible = valid.clone();
        prepare_tiered_data_movement_file_info_for(&mut compatible, false)
            .expect("disabled sidecar writer should omit optional part checksums");
        assert!(compatible.parts.iter().all(|part| part.checksums.is_none()));
        assert!(!rustfs_utils::http::contains_key_str(&compatible.metadata, SUFFIX_PART_CHECKSUMS));
        prepare_tiered_data_movement_file_info_for(&mut valid, true).expect("valid legacy part checksums should be encoded");
        assert_eq!(
            rustfs_utils::http::get_consistent_str(&valid.metadata, SUFFIX_PART_CHECKSUMS),
            Some(r#"[[1,[["CRC32C","AAAAAA=="]]]]"#)
        );

        let mut invalid = FileInfo {
            parts: vec![ObjectPartInfo {
                number: 1,
                checksums: Some(HashMap::from([("CRC32C".to_string(), "not-base64".to_string())])),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert!(matches!(
            prepare_tiered_data_movement_file_info_for(&mut invalid, true),
            Err(Error::FileCorrupt)
        ));
    }

    #[test]
    fn test_data_movement_preserves_multipart_topology_with_opaque_etag() {
        let object_info = ObjectInfo {
            etag: Some("0123456789abcdef0123456789abcdef".to_string()),
            parts: Arc::new(vec![
                ObjectPartInfo {
                    number: 2,
                    ..Default::default()
                },
                ObjectPartInfo {
                    number: 7,
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };
        assert!(!object_info.is_multipart());
        assert!(should_use_multipart_data_movement(&object_info, false));

        let single_nonstandard_part = ObjectInfo {
            parts: Arc::new(vec![ObjectPartInfo {
                number: 7,
                ..Default::default()
            }]),
            ..object_info
        };
        assert!(should_use_multipart_data_movement(&single_nonstandard_part, false));
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
            replication_status: ReplicationStatusType::Completed,
            version_purge_status_internal: Some("arn:minio:replication:target=PENDING;".to_string()),
            version_purge_status: VersionPurgeStatusType::Pending,
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
    fn test_data_movement_opts_canonicalize_derived_binary_metadata() {
        let transition_version = Uuid::from_u128(400).to_string();
        let checksum = Checksum::new_from_data(ChecksumType::CRC32C, b"checksum-payload")
            .expect("checksum should be created")
            .to_bytes(&[]);
        let mut object_info = ObjectInfo {
            checksum: Some(checksum),
            transitioned_object: crate::storage_api_contracts::lifecycle::TransitionedObject {
                name: "remote/object".to_string(),
                version_id: transition_version.clone(),
                tier: "WARM".to_string(),
                status: rustfs_filemeta::TRANSITION_COMPLETE.to_string(),
                ..Default::default()
            },
            transition_version_state: rustfs_filemeta::TransitionVersionState::Exact,
            ..Default::default()
        };
        Arc::make_mut(&mut object_info.user_defined)
            .insert(format!("{}{SUFFIX_CRC}", rustfs_utils::http::MINIO_INTERNAL_PREFIX), String::new());
        Arc::make_mut(&mut object_info.user_defined).insert(
            format!("{}{SUFFIX_TRANSITIONED_VERSION_ID}", rustfs_utils::http::MINIO_INTERNAL_PREFIX),
            String::new(),
        );

        let metadata = data_movement_new_multipart_opts(&object_info, 1).user_defined;

        assert!(!rustfs_utils::http::contains_key_str(&metadata, SUFFIX_CRC));
        assert_eq!(
            rustfs_utils::http::get_consistent_str(&metadata, SUFFIX_TRANSITIONED_VERSION_ID),
            Some(transition_version.as_str())
        );
        assert_eq!(
            rustfs_utils::http::get_consistent_str(&metadata, SUFFIX_TRANSITIONED_VERSION_STATE),
            Some("exact")
        );
    }

    #[test]
    fn test_data_movement_opts_preserve_unusable_transition_versions() {
        for value in ["opaque\0version".to_string(), "x".repeat(1_025)] {
            let key = rustfs_utils::http::internal_key_rustfs(SUFFIX_TRANSITIONED_VERSION_ID);
            let mut source = overwrite_equivalence_source();
            source.user_defined = Arc::new(HashMap::from([(key.clone(), value.clone())]));

            let opts = data_movement_new_multipart_opts(&source, 0);
            assert_eq!(opts.user_defined.get(&key), Some(&value));

            let mut target = source.clone();
            let different_value = if value.contains('\0') {
                "different\0version".to_string()
            } else {
                "y".repeat(1_025)
            };
            Arc::make_mut(&mut target.user_defined).insert(key, different_value);
            assert!(!overwrite_resume_for_target(&source, target));
        }
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
        let version_id = Uuid::from_u128(7);
        let object_info = ObjectInfo {
            version_id: Some(version_id),
            mod_time: Some(mod_time),
            etag: Some("etag-value".to_string()),
            ..Default::default()
        };

        let opts = data_movement_complete_multipart_opts(&object_info, 7, false).expect("complete opts should encode metadata");

        assert!(opts.versioned);
        assert!(opts.data_movement);
        assert_eq!(opts.mod_time, Some(mod_time));
        assert_eq!(opts.version_id.as_deref(), Some(version_id.to_string().as_str()));
        assert_eq!(opts.preserve_etag.as_deref(), Some("etag-value"));
        assert_eq!(opts.src_pool_idx, 7);
        assert_eq!(
            opts.http_preconditions
                .as_ref()
                .and_then(HTTPPreconditions::if_none_match_value),
            Some("*")
        );
    }

    #[test]
    fn test_data_movement_put_object_opts_preserves_version_and_etag() {
        let version_id = Uuid::from_u128(9);
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
        assert_eq!(
            opts.http_preconditions
                .as_ref()
                .and_then(HTTPPreconditions::if_none_match_value),
            Some("*")
        );
    }

    #[test]
    fn test_data_movement_put_opts_do_not_persist_multipart_part_checksums() {
        let object_info = ObjectInfo {
            etag: Some("0123456789abcdef0123456789abcdef".to_string()),
            parts: Arc::new(vec![
                ObjectPartInfo {
                    number: 1,
                    checksums: Some(HashMap::from([("CRC32C".to_string(), "AAAAAA==".to_string())])),
                    ..Default::default()
                },
                ObjectPartInfo {
                    number: 2,
                    checksums: Some(HashMap::from([("CRC32C".to_string(), "BBBBBB==".to_string())])),
                    ..Default::default()
                },
            ]),
            ..Default::default()
        };

        assert!(!object_info.is_multipart());
        assert!(object_info.parts.iter().any(|part| part.checksums.is_some()));
        let opts = data_movement_put_object_opts(&object_info, 0);
        assert!(!rustfs_utils::http::contains_key_str(&opts.user_defined, SUFFIX_PART_CHECKSUMS));
    }

    #[test]
    fn test_data_movement_put_and_complete_require_absent_target() {
        for version_id in [None, Some(Uuid::nil()), Some(Uuid::from_u128(1))] {
            let object_info = ObjectInfo {
                version_id,
                ..Default::default()
            };

            let put_opts = data_movement_put_object_opts(&object_info, 9);
            let complete_opts =
                data_movement_complete_multipart_opts(&object_info, 9, false).expect("complete opts should encode metadata");

            assert_eq!(
                put_opts
                    .http_preconditions
                    .as_ref()
                    .and_then(HTTPPreconditions::if_none_match_value),
                Some("*")
            );
            assert_eq!(
                complete_opts
                    .http_preconditions
                    .as_ref()
                    .and_then(HTTPPreconditions::if_none_match_value),
                Some("*")
            );
        }
    }

    #[test]
    fn test_stale_data_movement_target_replacement_requires_exact_owned_generation() {
        let version_id = Uuid::from_u128(41);
        let source_time = OffsetDateTime::UNIX_EPOCH + time::Duration::seconds(2);
        let opts = ObjectOptions {
            data_movement: true,
            versioned: true,
            version_id: Some(version_id.to_string()),
            mod_time: Some(source_time),
            http_preconditions: Some(data_movement_target_precondition()),
            ..Default::default()
        };
        let mut metadata = HashMap::new();
        rustfs_utils::http::insert_str(&mut metadata, SUFFIX_DATA_MOVED, "true".to_string());
        rustfs_utils::http::insert_str(&mut metadata, SUFFIX_DATA_MOVED_TAGS, "v1:".to_string());
        let target = ObjectInfo {
            version_id: Some(version_id),
            mod_time: Some(source_time - time::Duration::SECOND),
            user_defined: Arc::new(metadata),
            ..Default::default()
        };

        assert!(can_replace_stale_data_movement_target(&target, &opts));

        let mut client_target = target.clone();
        client_target.user_defined = Arc::new(HashMap::new());
        assert!(!can_replace_stale_data_movement_target(&client_target, &opts));

        let mut single_marker = target.clone();
        Arc::make_mut(&mut single_marker.user_defined)
            .remove(&format!("{}{SUFFIX_DATA_MOVED}", rustfs_utils::http::MINIO_INTERNAL_PREFIX));
        assert!(!can_replace_stale_data_movement_target(&single_marker, &opts));

        let mut conflicting_marker = target.clone();
        Arc::make_mut(&mut conflicting_marker.user_defined).insert(
            format!("{}{SUFFIX_DATA_MOVED}", rustfs_utils::http::MINIO_INTERNAL_PREFIX),
            "false".to_string(),
        );
        assert!(!can_replace_stale_data_movement_target(&conflicting_marker, &opts));

        let mut retagged_target = target.clone();
        retagged_target.user_tags = Arc::new("acknowledged=true".to_string());
        assert!(!can_replace_stale_data_movement_target(&retagged_target, &opts));

        let mut retagged_owned_target = retagged_target;
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut retagged_owned_target.user_defined),
            SUFFIX_DATA_MOVED_TAGS,
            "v1:acknowledged=true".to_string(),
        );
        assert!(can_replace_stale_data_movement_target(&retagged_owned_target, &opts));

        let mut different_version = target.clone();
        different_version.version_id = Some(Uuid::from_u128(42));
        assert!(!can_replace_stale_data_movement_target(&different_version, &opts));

        let mut same_generation = target.clone();
        same_generation.mod_time = Some(source_time);
        assert!(!can_replace_stale_data_movement_target(&same_generation, &opts));

        let mut delete_marker = target;
        delete_marker.delete_marker = true;
        assert!(!can_replace_stale_data_movement_target(&delete_marker, &opts));
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
            replication_status: ReplicationStatusType::Completed,
            version_purge_status_internal: Some("arn:minio:replication:target=PENDING;".to_string()),
            version_purge_status: VersionPurgeStatusType::Pending,
            parts: Arc::new(vec![part]),
            ..Default::default()
        }
    }

    fn overwrite_resume_for_target(source: &ObjectInfo, target: ObjectInfo) -> bool {
        overwrite_resume_for_target_with_checksums(source, target, data_movement_part_checksum_writer_enabled())
    }

    fn overwrite_resume_for_target_with_checksums(source: &ObjectInfo, target: ObjectInfo, compare_part_checksums: bool) -> bool {
        let err = Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "version".to_string());
        resolve_data_movement_overwrite_resume_result_for(&err, Ok(Some(target)), source, 0, 1, compare_part_checksums)
            .expect("overwrite target should be evaluated")
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_full_equivalence() {
        let source = overwrite_equivalence_source();

        assert!(overwrite_resume_for_target(&source, source.clone()));
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_part_mod_time_drift() {
        let source = overwrite_equivalence_source();
        let mut target = source.clone();
        let mut parts = target.parts.as_ref().clone();
        parts[0].mod_time = Some(OffsetDateTime::UNIX_EPOCH + time::Duration::SECOND);
        target.parts = Arc::new(parts);

        assert!(overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_legacy_part_actual_size_fallback() {
        let mut source = overwrite_equivalence_source();
        let mut source_parts = source.parts.as_ref().clone();
        source_parts[0].actual_size = 0;
        source.parts = Arc::new(source_parts);
        let mut target = source.clone();
        let mut target_parts = target.parts.as_ref().clone();
        target_parts[0].actual_size = i64::try_from(target_parts[0].size).expect("part size should fit i64");
        target.parts = Arc::new(target_parts);

        assert!(overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_preserves_negative_part_actual_size() {
        let mut source = overwrite_equivalence_source();
        let mut source_parts = source.parts.as_ref().clone();
        source_parts[0].actual_size = -1;
        source.parts = Arc::new(source_parts);

        let mut target = source.clone();
        let mut target_parts = target.parts.as_ref().clone();
        target_parts[0].actual_size = i64::try_from(target_parts[0].size).expect("part size should fit i64");
        target.parts = Arc::new(target_parts);
        assert!(!overwrite_resume_for_target(&source, target));

        assert!(overwrite_resume_for_target(&source, source.clone()));
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_destination_marker() {
        let source = overwrite_equivalence_source();
        let mut target = source.clone();
        let mut metadata = target.user_defined.as_ref().clone();
        rustfs_utils::http::insert_str(&mut metadata, SUFFIX_DATA_MOVED, "true".to_string());
        target.user_defined = Arc::new(metadata);

        assert!(overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_compatible_internal_aliases() {
        for suffix in [SUFFIX_ACTUAL_SIZE, SUFFIX_COMPRESSION_SIZE] {
            let mut source = overwrite_equivalence_source();
            let mut source_metadata = source.user_defined.as_ref().clone();
            if suffix == SUFFIX_COMPRESSION_SIZE {
                rustfs_utils::http::insert_str(
                    &mut source_metadata,
                    rustfs_utils::http::SUFFIX_COMPRESSION,
                    "klauspost/compress/s2".to_string(),
                );
            }
            source_metadata.insert(format!("X-Minio-Internal-{suffix}"), "000128".to_string());
            source.user_defined = Arc::new(source_metadata);

            let mut target = source.clone();
            let mut target_metadata = target.user_defined.as_ref().clone();
            rustfs_utils::http::insert_str(&mut target_metadata, suffix, "128".to_string());
            target.user_defined = Arc::new(target_metadata);

            assert!(
                overwrite_resume_for_target(&source, target),
                "compatible aliases for {suffix} should match"
            );

            let mut source_without_marker = source.clone();
            rustfs_utils::http::remove_str(Arc::make_mut(&mut source_without_marker.user_defined), suffix);
            let mut target_with_generated_marker = source_without_marker.clone();
            rustfs_utils::http::insert_str(
                Arc::make_mut(&mut target_with_generated_marker.user_defined),
                suffix,
                "128".to_string(),
            );
            assert!(
                overwrite_resume_for_target(&source_without_marker, target_with_generated_marker),
                "a generated target marker for {suffix} should match"
            );
        }
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_conflicting_internal_aliases() {
        for suffix in [SUFFIX_ACTUAL_SIZE, SUFFIX_COMPRESSION_SIZE] {
            let mut source = overwrite_equivalence_source();
            if suffix == SUFFIX_COMPRESSION_SIZE {
                rustfs_utils::http::insert_str(
                    Arc::make_mut(&mut source.user_defined),
                    rustfs_utils::http::SUFFIX_COMPRESSION,
                    "klauspost/compress/s2".to_string(),
                );
            }
            let mut target = source.clone();
            let mut metadata = target.user_defined.as_ref().clone();
            rustfs_utils::http::insert_str(&mut metadata, suffix, "128".to_string());
            metadata.insert(rustfs_utils::http::internal_key_rustfs(suffix), "64".to_string());
            target.user_defined = Arc::new(metadata);

            assert!(
                !overwrite_resume_for_target(&source, target),
                "conflicting aliases for {suffix} must fail closed"
            );

            rustfs_utils::http::insert_str(Arc::make_mut(&mut source.user_defined), suffix, "128".to_string());
            let mut target_without_marker = source.clone();
            rustfs_utils::http::remove_str(Arc::make_mut(&mut target_without_marker.user_defined), suffix);
            assert!(
                !overwrite_resume_for_target(&source, target_without_marker),
                "a missing target marker for {suffix} must fail closed"
            );
        }
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_unreadable_actual_size() {
        let mut source = overwrite_equivalence_source();
        source.actual_size = 0;
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut source.user_defined),
            rustfs_utils::http::SUFFIX_COMPRESSION,
            "klauspost/compress/s2".to_string(),
        );
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut source.user_defined),
            SUFFIX_ACTUAL_SIZE,
            "invalid-source-size".to_string(),
        );
        let mut target = source.clone();
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut target.user_defined),
            SUFFIX_ACTUAL_SIZE,
            "invalid-target-size".to_string(),
        );

        assert!(!overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_missing_unreadable_actual_size() {
        let mut source = overwrite_equivalence_source();
        source.actual_size = 0;
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut source.user_defined),
            rustfs_utils::http::SUFFIX_COMPRESSION,
            "klauspost/compress/s2".to_string(),
        );
        let mut parts = source.parts.as_ref().clone();
        parts[0].actual_size = 0;
        source.parts = Arc::new(parts);

        assert!(!overwrite_resume_for_target(&source, source.clone()));
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_checksum_alias_expansion() {
        let mut source = overwrite_equivalence_source();
        let checksum = rustfs_rio::Checksum::new_from_data(ChecksumType::CRC32C, b"checksum-payload")
            .expect("checksum should be created")
            .to_bytes(&[]);
        assert!(
            std::str::from_utf8(&checksum).is_err(),
            "wire checksum should exercise non-UTF-8 metadata"
        );
        source.checksum = Some(checksum);
        Arc::make_mut(&mut source.user_defined)
            .insert(format!("{}{SUFFIX_CRC}", rustfs_utils::http::MINIO_INTERNAL_PREFIX), String::new());

        let mut target = source.clone();
        rustfs_utils::http::insert_str(Arc::make_mut(&mut target.user_defined), SUFFIX_CRC, String::new());

        assert!(overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_empty_checksum_normalization() {
        let mut source = overwrite_equivalence_source();
        source.checksum = None;
        source.user_defined = Arc::new(HashMap::from([(
            format!("{}{SUFFIX_CRC}", rustfs_utils::http::MINIO_INTERNAL_PREFIX),
            String::new(),
        )]));
        let mut target = source.clone();
        target.user_defined = Arc::new(HashMap::new());

        assert!(is_equivalent_data_movement_object(&source, &target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_checksum_alias_conflict() {
        let mut source = overwrite_equivalence_source();
        rustfs_utils::http::insert_str(Arc::make_mut(&mut source.user_defined), SUFFIX_CRC, "object-checksum".to_string());
        let mut target_without_marker = source.clone();
        rustfs_utils::http::remove_str(Arc::make_mut(&mut target_without_marker.user_defined), SUFFIX_CRC);
        assert!(!overwrite_resume_for_target(&source, target_without_marker));

        let mut target = source.clone();
        rustfs_utils::http::insert_str(Arc::make_mut(&mut target.user_defined), SUFFIX_CRC, "different".to_string());

        assert!(!overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_transition_alias_expansion() {
        let mut source = overwrite_equivalence_source();
        source.transitioned_object.name = "remote/object".to_string();
        source.transitioned_object.version_id = Uuid::from_u128(300).to_string();
        source.transitioned_object.tier = "WARM".to_string();
        source.transitioned_object.status = rustfs_filemeta::TRANSITION_COMPLETE.to_string();
        source.transition_version_state = rustfs_filemeta::TransitionVersionState::Exact;
        let markers = [
            (SUFFIX_TRANSITION_STATUS, source.transitioned_object.status.as_str()),
            (SUFFIX_TRANSITIONED_OBJECTNAME, source.transitioned_object.name.as_str()),
            (SUFFIX_TRANSITIONED_VERSION_ID, source.transitioned_object.version_id.as_str()),
            (SUFFIX_TRANSITIONED_VERSION_STATE, "exact"),
            (SUFFIX_TRANSITION_TIER, source.transitioned_object.tier.as_str()),
        ];
        for (suffix, value) in markers {
            Arc::make_mut(&mut source.user_defined)
                .insert(format!("{}{suffix}", rustfs_utils::http::MINIO_INTERNAL_PREFIX), value.to_string());
        }

        let mut target = source.clone();
        for (suffix, value) in markers {
            rustfs_utils::http::insert_str(Arc::make_mut(&mut target.user_defined), suffix, value.to_string());
        }
        assert!(overwrite_resume_for_target(&source, target.clone()));

        target.transitioned_object.tier = "OTHER".to_string();
        assert!(!overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_empty_transition_marker_normalization() {
        for value in [
            String::new(),
            "\0".repeat(16),
            Uuid::nil().to_string(),
            Uuid::nil().simple().to_string(),
        ] {
            let mut source = overwrite_equivalence_source();
            Arc::make_mut(&mut source.user_defined).insert(
                format!("{}{SUFFIX_TRANSITIONED_VERSION_ID}", rustfs_utils::http::MINIO_INTERNAL_PREFIX),
                value,
            );
            Arc::make_mut(&mut source.user_defined).insert(
                format!("{}{SUFFIX_TRANSITIONED_VERSION_STATE}", rustfs_utils::http::MINIO_INTERNAL_PREFIX),
                "unknown".to_string(),
            );
            let mut target = source.clone();
            rustfs_utils::http::remove_str(Arc::make_mut(&mut target.user_defined), SUFFIX_TRANSITIONED_VERSION_ID);
            rustfs_utils::http::remove_str(Arc::make_mut(&mut target.user_defined), SUFFIX_TRANSITIONED_VERSION_STATE);

            assert!(overwrite_resume_for_target(&source, target));
        }
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_unparsed_transition_case_alias_mismatch() {
        let mut source = overwrite_equivalence_source();
        Arc::make_mut(&mut source.user_defined).insert("X-Minio-Internal-transition-tier".to_string(), "source-tier".to_string());
        let mut target = source.clone();
        Arc::make_mut(&mut target.user_defined).insert("X-Minio-Internal-transition-tier".to_string(), "target-tier".to_string());

        assert!(!overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_rebuilt_transition_case_alias() {
        let mut source = overwrite_equivalence_source();
        source.transitioned_object.tier = "WARM".to_string();
        rustfs_utils::http::insert_str(Arc::make_mut(&mut source.user_defined), SUFFIX_TRANSITION_TIER, "WARM".to_string());
        Arc::make_mut(&mut source.user_defined).insert("X-Minio-Internal-transition-tier".to_string(), "WARM".to_string());
        let mut target = source.clone();
        Arc::make_mut(&mut target.user_defined).remove("X-Minio-Internal-transition-tier");

        assert!(overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_target_local_layout_marker() {
        let source_data_dir = Uuid::from_u128(100);
        let mut source = overwrite_equivalence_source();
        source.data_dir = Some(source_data_dir);
        let mut source_metadata = source.user_defined.as_ref().clone();
        rustfs_utils::http::insert_str(
            &mut source_metadata,
            crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
            source_data_dir.to_string(),
        );
        source.user_defined = Arc::new(source_metadata);

        let target_data_dir = Uuid::from_u128(200);
        let mut target = source.clone();
        target.data_dir = Some(target_data_dir);
        let mut target_metadata = target.user_defined.as_ref().clone();
        rustfs_utils::http::insert_str(
            &mut target_metadata,
            crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
            target_data_dir.to_string(),
        );
        target.user_defined = Arc::new(target_metadata);
        assert!(overwrite_resume_for_target(&source, target.clone()));

        let mut target_without_marker = target.clone();
        rustfs_utils::http::remove_str(
            Arc::make_mut(&mut target_without_marker.user_defined),
            crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
        );
        assert!(overwrite_resume_for_target(&source, target_without_marker.clone()));
        target_without_marker.data_dir = None;
        assert!(!overwrite_resume_for_target(&source, target_without_marker));

        let mut source_without_marker = source.clone();
        rustfs_utils::http::remove_str(
            Arc::make_mut(&mut source_without_marker.user_defined),
            crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
        );
        assert!(overwrite_resume_for_target(&source_without_marker, target.clone()));
        source_without_marker.data_dir = Some(Uuid::nil());
        assert!(!overwrite_resume_for_target(&source_without_marker, target.clone()));

        let mut invalid_target_metadata = target.user_defined.as_ref().clone();
        rustfs_utils::http::insert_str(
            &mut invalid_target_metadata,
            crate::object_api::ENCRYPTED_PART_LAYOUT_QUORUM_SUFFIX,
            source_data_dir.to_string(),
        );
        target.user_defined = Arc::new(invalid_target_metadata);
        assert!(!overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_missing_part_checksum() {
        let source = overwrite_equivalence_source();
        let mut target = source.clone();
        let mut parts = target.parts.as_ref().clone();
        parts[0].checksums = None;
        target.parts = Arc::new(parts);

        assert!(!overwrite_resume_for_target_with_checksums(&source, target, true));
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_dropped_part_index() {
        let source = overwrite_equivalence_source();
        let mut target = source.clone();
        let mut parts = target.parts.as_ref().clone();
        parts[0].index = None;
        target.parts = Arc::new(parts);

        assert!(overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_target_only_part_index() {
        let mut source = overwrite_equivalence_source();
        let mut source_parts = source.parts.as_ref().clone();
        source_parts[0].index = None;
        source.parts = Arc::new(source_parts);
        let mut target = source.clone();
        let mut target_parts = target.parts.as_ref().clone();
        target_parts[0].index = Some(Bytes::from_static(&[9]));
        target.parts = Arc::new(target_parts);

        assert!(!overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_accepts_generated_part_checksum_marker() {
        let source = overwrite_equivalence_source();
        let mut target = source.clone();
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut target.user_defined),
            SUFFIX_PART_CHECKSUMS,
            r#"[[1,[["CRC32C","part-checksum"]]]]"#.to_string(),
        );

        assert!(overwrite_resume_for_target(&source, target));
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
            version_purge_status: VersionPurgeStatusType::Complete,
            ..source.clone()
        };

        assert!(!overwrite_resume_for_target(&source, target));
    }

    #[test]
    fn test_data_movement_overwrite_resume_rejects_replication_mismatch() {
        let source = overwrite_equivalence_source();
        let target = ObjectInfo {
            replication_status_internal: Some("arn:minio:replication:target=FAILED;".to_string()),
            replication_status: ReplicationStatusType::Failed,
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
    fn test_overwrite_resume_accepts_owned_target_without_legacy_checksum_sidecar() {
        let mut source = overwrite_equivalence_source();
        let mut source_parts = source.parts.as_ref().clone();
        source_parts.push(ObjectPartInfo {
            number: 2,
            etag: "second-part-etag".to_string(),
            size: 64,
            actual_size: 64,
            checksums: Some(HashMap::from([(ChecksumType::CRC32C.to_string(), "second-part-checksum".to_string())])),
            ..Default::default()
        });
        source.parts = Arc::new(source_parts);
        let mut target = source.clone();
        let mut target_parts = target.parts.as_ref().clone();
        for part in &mut target_parts {
            part.checksums = None;
        }
        target.parts = Arc::new(target_parts);
        let err = Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "version".to_string());

        assert!(
            resolve_data_movement_overwrite_resume_result_for(&err, Ok(Some(target.clone())), &source, 0, 1, false)
                .expect("compatible migration should accept an omitted optional checksum sidecar")
        );
        assert!(
            !resolve_data_movement_overwrite_resume_result_for(&err, Ok(Some(target)), &source, 0, 1, true)
                .expect("an unowned target must not bypass fleet-confirmed checksum comparison")
        );

        let mut owned_target = source.clone();
        let mut owned_target_parts = owned_target.parts.as_ref().clone();
        for part in &mut owned_target_parts {
            part.checksums = None;
        }
        owned_target.parts = Arc::new(owned_target_parts);
        rustfs_utils::http::insert_str(Arc::make_mut(&mut owned_target.user_defined), SUFFIX_DATA_MOVED, "true".to_string());
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut owned_target.user_defined),
            SUFFIX_DATA_MOVED_TAGS,
            "v1:tag=value".to_string(),
        );
        assert!(
            resolve_data_movement_overwrite_resume_result_for(&err, Ok(Some(owned_target.clone())), &source, 0, 1, true,)
                .expect("an owned pre-gate target should remain compatible after enabling checksum persistence")
        );

        let mut partial_target = owned_target.clone();
        let mut partial_target_parts = partial_target.parts.as_ref().clone();
        partial_target_parts[0].checksums.clone_from(&source.parts[0].checksums);
        partial_target.parts = Arc::new(partial_target_parts);
        assert!(
            !resolve_data_movement_overwrite_resume_result_for(&err, Ok(Some(partial_target)), &source, 0, 1, true)
                .expect("a partially missing checksum sidecar must fail closed")
        );

        let mut corrupt_target = owned_target.clone();
        rustfs_utils::http::insert_str(Arc::make_mut(&mut corrupt_target.user_defined), SUFFIX_PART_CHECKSUMS, String::new());
        assert!(
            !resolve_data_movement_overwrite_resume_result_for(&err, Ok(Some(corrupt_target)), &source, 0, 1, true)
                .expect("a present but empty checksum sidecar must fail closed")
        );

        let mut conflicting_target = owned_target;
        let mut conflicting_target_parts = conflicting_target.parts.as_ref().clone();
        conflicting_target_parts[0].checksums = Some(HashMap::from([(
            ChecksumType::CRC32C.to_string(),
            "conflicting-part-checksum".to_string(),
        )]));
        conflicting_target.parts = Arc::new(conflicting_target_parts);
        assert!(
            !resolve_data_movement_overwrite_resume_result_for(&err, Ok(Some(conflicting_target)), &source, 0, 1, true)
                .expect("a conflicting checksum sidecar must fail closed")
        );
    }

    #[test]
    fn test_invalid_upload_accepts_versioned_target_taken_over_by_old_node() {
        let mut source = overwrite_equivalence_source();
        let mut source_parts = source.parts.as_ref().clone();
        source_parts[0].checksums = None;
        source.parts = Arc::new(source_parts);
        let mut target = ObjectInfo {
            mod_time: OffsetDateTime::UNIX_EPOCH.checked_add(time::Duration::SECOND),
            ..source.clone()
        };
        let err = Error::InvalidUploadID("bucket".to_string(), "object".to_string(), "upload-id".to_string());
        assert!(
            !resolve_data_movement_overwrite_resume_result(&err, Ok(Some(target.clone())), &source, 0, 1)
                .expect("an unrelated invalid upload must not accept a different target")
        );
        let upload_identity = data_movement_upload_identity(&source);
        rustfs_utils::http::insert_str(Arc::make_mut(&mut target.user_defined), SUFFIX_DATA_MOVEMENT_UPLOAD, upload_identity);
        let should_resume = resolve_data_movement_overwrite_resume_result(&err, Ok(Some(target.clone())), &source, 0, 1)
            .expect("an old-node completion must make its target version authoritative");

        assert!(should_resume);

        target.etag = Some("etag-client-write".to_string());
        assert!(
            !resolve_data_movement_overwrite_resume_result(&err, Ok(Some(target.clone())), &source, 0, 1)
                .expect("a takeover target with changed content must be rejected")
        );

        target.etag.clone_from(&source.etag);
        target.parts = Arc::new(Vec::new());
        assert!(
            !resolve_data_movement_overwrite_resume_result(&err, Ok(Some(target.clone())), &source, 0, 1)
                .expect("a partial old-node completion must not replace the source")
        );

        target.parts.clone_from(&source.parts);
        let mut newer_source = source.clone();
        newer_source.mod_time = OffsetDateTime::UNIX_EPOCH.checked_add(time::Duration::SECOND * 2);
        assert!(
            !resolve_data_movement_overwrite_resume_result(&err, Ok(Some(target)), &newer_source, 0, 1)
                .expect("a stale takeover marker must not accept a newer source generation")
        );

        let mut missing_time_source = source.clone();
        missing_time_source.mod_time = None;
        let mut missing_time_target = missing_time_source.clone();
        missing_time_target.mod_time = OffsetDateTime::UNIX_EPOCH.checked_add(time::Duration::SECOND);
        let upload_identity = data_movement_upload_identity(&missing_time_source);
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut missing_time_target.user_defined),
            SUFFIX_DATA_MOVEMENT_UPLOAD,
            upload_identity,
        );
        assert!(
            !resolve_data_movement_overwrite_resume_result(&err, Ok(Some(missing_time_target)), &missing_time_source, 0, 1)
                .expect("a takeover target must not replace a source with no generation timestamp")
        );

        let legacy_source = overwrite_equivalence_source();
        let mut legacy_target = legacy_source.clone();
        legacy_target.mod_time = OffsetDateTime::UNIX_EPOCH.checked_add(time::Duration::SECOND);
        let mut legacy_target_parts = legacy_target.parts.as_ref().clone();
        legacy_target_parts[0].checksums = None;
        legacy_target.parts = Arc::new(legacy_target_parts);
        let upload_identity = data_movement_upload_identity(&legacy_source);
        rustfs_utils::http::insert_str(
            Arc::make_mut(&mut legacy_target.user_defined),
            SUFFIX_DATA_MOVEMENT_UPLOAD,
            upload_identity,
        );
        assert!(
            !resolve_data_movement_overwrite_resume_result_for(&err, Ok(Some(legacy_target)), &legacy_source, 0, 1, true)
                .expect("an old-node takeover must not discard legacy part checksums")
        );
    }

    #[test]
    fn test_precondition_conflict_accepts_newer_unversioned_target() {
        for version_id in [None, Some(Uuid::nil())] {
            let source = ObjectInfo {
                version_id,
                size: 128,
                etag: Some("etag-source".to_string()),
                mod_time: Some(OffsetDateTime::UNIX_EPOCH),
                ..Default::default()
            };
            let target = ObjectInfo {
                etag: Some("etag-client-write".to_string()),
                mod_time: OffsetDateTime::UNIX_EPOCH.checked_add(time::Duration::SECOND),
                ..source.clone()
            };

            let should_resume =
                resolve_data_movement_overwrite_resume_result(&Error::PreconditionFailed, Ok(Some(target)), &source, 0, 1)
                    .expect("precondition conflict target should be evaluated");

            assert!(should_resume);
        }
    }

    #[test]
    fn test_precondition_conflict_accepts_equivalent_target() {
        let source = ObjectInfo {
            size: 128,
            etag: Some("etag-source".to_string()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };

        let should_resume =
            resolve_data_movement_overwrite_resume_result(&Error::PreconditionFailed, Ok(Some(source.clone())), &source, 0, 1)
                .expect("equivalent precondition target should be evaluated");

        assert!(should_resume);
    }

    #[test]
    fn test_precondition_conflict_rejects_non_newer_unversioned_target() {
        let source = ObjectInfo {
            size: 128,
            etag: Some("etag-source".to_string()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };
        let target = ObjectInfo {
            etag: Some("etag-conflict".to_string()),
            ..source.clone()
        };

        let should_resume =
            resolve_data_movement_overwrite_resume_result(&Error::PreconditionFailed, Ok(Some(target)), &source, 0, 1)
                .expect("precondition conflict target should be evaluated");

        assert!(!should_resume);
    }

    #[test]
    fn test_precondition_conflict_accepts_only_newer_null_delete_marker() {
        for version_id in [None, Some(Uuid::nil())] {
            let source = ObjectInfo {
                version_id,
                size: 128,
                etag: Some("etag-source".to_string()),
                mod_time: Some(OffsetDateTime::UNIX_EPOCH),
                ..Default::default()
            };
            let target = ObjectInfo {
                delete_marker: true,
                etag: None,
                mod_time: OffsetDateTime::UNIX_EPOCH.checked_add(time::Duration::SECOND),
                ..source.clone()
            };

            assert!(
                resolve_data_movement_overwrite_resume_result(
                    &Error::PreconditionFailed,
                    Ok(Some(target.clone())),
                    &source,
                    0,
                    1,
                )
                .expect("newer null delete marker should be evaluated")
            );

            let mut same_time = target.clone();
            same_time.mod_time = source.mod_time;
            assert!(
                !resolve_data_movement_overwrite_resume_result(&Error::PreconditionFailed, Ok(Some(same_time)), &source, 0, 1,)
                    .expect("same-generation null delete marker should be rejected")
            );

            let mut versioned = target;
            versioned.version_id = Some(Uuid::new_v4());
            assert!(
                !resolve_data_movement_overwrite_resume_result(&Error::PreconditionFailed, Ok(Some(versioned)), &source, 0, 1,)
                    .expect("a UUID delete marker must not erase a null source version")
            );
        }
    }

    #[test]
    fn test_overwrite_error_rejects_newer_unversioned_target() {
        let source = ObjectInfo {
            size: 128,
            etag: Some("etag-source".to_string()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };
        let target = ObjectInfo {
            etag: Some("etag-client-write".to_string()),
            mod_time: OffsetDateTime::UNIX_EPOCH.checked_add(time::Duration::SECOND),
            ..source.clone()
        };
        let err = Error::DataMovementOverwriteErr("bucket".to_string(), "object".to_string(), "version".to_string());

        let should_resume = resolve_data_movement_overwrite_resume_result(&err, Ok(Some(target)), &source, 0, 1)
            .expect("pool-selection overwrite must require target equivalence");

        assert!(!should_resume);
    }

    #[test]
    fn test_precondition_conflict_rejects_newer_versioned_target() {
        let source = ObjectInfo {
            size: 128,
            etag: Some("etag-source".to_string()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };
        let target = ObjectInfo {
            version_id: Some(Uuid::from_u128(2)),
            etag: Some("etag-conflict".to_string()),
            mod_time: OffsetDateTime::UNIX_EPOCH.checked_add(time::Duration::SECOND),
            ..source.clone()
        };

        let should_resume =
            resolve_data_movement_overwrite_resume_result(&Error::PreconditionFailed, Ok(Some(target)), &source, 0, 1)
                .expect("versioned conflict target should be evaluated");

        assert!(!should_resume);
    }

    #[test]
    fn test_precondition_conflict_rejects_versioned_source_with_unversioned_target() {
        let source = ObjectInfo {
            version_id: Some(Uuid::from_u128(1)),
            size: 128,
            etag: Some("etag-source".to_string()),
            mod_time: Some(OffsetDateTime::UNIX_EPOCH),
            ..Default::default()
        };
        let target = ObjectInfo {
            version_id: None,
            etag: Some("etag-conflict".to_string()),
            mod_time: OffsetDateTime::UNIX_EPOCH.checked_add(time::Duration::SECOND),
            ..source.clone()
        };

        let should_resume =
            resolve_data_movement_overwrite_resume_result(&Error::PreconditionFailed, Ok(Some(target)), &source, 0, 1)
                .expect("versioned source conflict should be evaluated");

        assert!(!should_resume);
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
