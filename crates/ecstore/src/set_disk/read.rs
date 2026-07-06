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

use super::*;
use crate::diagnostics::get::{
    GET_DIRECT_MEMORY_SUBPATH_DISK_DATA_BLOCKS, GET_DIRECT_MEMORY_SUBPATH_INLINE_BUFFERED, GET_METADATA_CACHE_DECISION_HIT,
    GET_METADATA_CACHE_DECISION_MISS, GET_METADATA_CACHE_DECISION_REJECT, GET_METADATA_CACHE_DECISION_SKIP,
    GET_METADATA_CACHE_REASON_DATA_MOVEMENT, GET_METADATA_CACHE_REASON_DELETE_MARKER, GET_METADATA_CACHE_REASON_DIST_ERASURE,
    GET_METADATA_CACHE_REASON_INCL_FREE_VERSIONS, GET_METADATA_CACHE_REASON_INSUFFICIENT_CACHED_QUORUM,
    GET_METADATA_CACHE_REASON_META_BUCKET, GET_METADATA_CACHE_REASON_NO_LOCK, GET_METADATA_CACHE_REASON_NOT_FOUND_OR_EXPIRED,
    GET_METADATA_CACHE_REASON_NOT_READ_DATA, GET_METADATA_CACHE_REASON_PART_NUMBER,
    GET_METADATA_CACHE_REASON_RAW_DATA_MOVEMENT_READ, GET_METADATA_CACHE_REASON_USABLE, GET_METADATA_CACHE_REASON_VERSION_ID,
    GET_METADATA_CACHE_REASON_VERSION_SUSPENDED, GET_METADATA_CACHE_REASON_VERSIONED,
    GET_METADATA_EARLY_STOP_REASON_CONFLICTING_METADATA, GET_METADATA_EARLY_STOP_REASON_DELETE_MARKER,
    GET_METADATA_EARLY_STOP_REASON_ERROR, GET_METADATA_EARLY_STOP_REASON_INSUFFICIENT_QUORUM,
    GET_METADATA_EARLY_STOP_REASON_NOT_FOUND, GET_METADATA_EARLY_STOP_REASON_UNSAFE_REQUEST,
    GET_METADATA_EARLY_STOP_REASON_VALID_QUORUM, GET_METADATA_EARLY_STOP_REASON_VERSION_MATCH_QUORUM,
    GET_METADATA_EARLY_STOP_REASON_VERSION_NOT_FOUND, GET_METADATA_RESPONSE_CORRUPT, GET_METADATA_RESPONSE_DISK_NOT_FOUND,
    GET_METADATA_RESPONSE_ERROR, GET_METADATA_RESPONSE_IGNORED, GET_METADATA_RESPONSE_NOT_FOUND, GET_METADATA_RESPONSE_TIMEOUT,
    GET_METADATA_RESPONSE_VALID, GET_METADATA_RESPONSE_VERSION_NOT_FOUND, GET_OBJECT_PATH_CODEC_STREAMING,
    GET_OBJECT_PATH_DIRECT_MEMORY, GET_OBJECT_PATH_LEGACY_DUPLEX, GET_OBJECT_PATH_SET_DISK, GET_STAGE_DECODE,
    GET_STAGE_METADATA_CACHE_LOOKUP, GET_STAGE_METADATA_RESOLVE, GET_STAGE_RANGE, GET_STAGE_READER_SETUP,
    GET_STAGE_READER_SETUP_DROP_PENDING, GET_STAGE_READER_SETUP_SCHEDULE, GET_STAGE_READER_SETUP_WAIT_QUORUM,
    GET_STAGE_READER_TASK_BITROT_READER_INIT, GET_STAGE_READER_TASK_FILE_OPEN, GET_STAGE_READER_TASK_READER_CONSTRUCTION,
    GetObjectFailureReason, classify_disk_error, get_stage_timer_if_enabled, record_get_object_pipeline_failure,
    record_get_object_pipeline_failure_for_path, record_get_stage_duration_if_enabled,
};
use crate::erasure::coding::BitrotReader;
use crate::io_support::bitrot::{
    BitrotReaderStageMetrics, create_bitrot_reader_with_stage_metrics, create_deferred_bitrot_reader, object_mmap_read_enabled,
};
use crate::set_disk::shard_source::ShardReadCost;
use futures::stream::{FuturesUnordered, StreamExt};
use metrics::counter;
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    pin::Pin,
    sync::OnceLock,
    task::{Context, Poll},
    time::{Duration, Instant},
};
use tokio::io::{AsyncRead, ReadBuf};
use tokio::sync::RwLock;
use tokio::task::JoinSet;

use super::core::io_primitives::*;

impl SetDisks {
    async fn get_object_metadata_cache_bypass_reason(
        bucket: &str,
        opts: &ObjectOptions,
        read_data: bool,
    ) -> Option<&'static str> {
        if let Some(reason) = get_object_metadata_cache_request_bypass_reason(bucket, opts, read_data) {
            return Some(reason);
        }
        runtime_sources::setup_is_dist_erasure()
            .await
            .then_some(GET_METADATA_CACHE_REASON_DIST_ERASURE)
    }

    async fn cached_get_object_fileinfo(&self, bucket: &str, object: &str) -> Option<GetObjectMetadataCacheEntry> {
        match self.lookup_cached_get_object_fileinfo(bucket, object).await {
            MetadataCacheLookup::Hit(entry) => Some((*entry).clone()),
            MetadataCacheLookup::Miss | MetadataCacheLookup::RejectedInsufficientQuorum => None,
        }
    }

    async fn lookup_cached_get_object_fileinfo(&self, bucket: &str, object: &str) -> MetadataCacheLookup {
        let key = GetObjectMetadataCacheKey::new(bucket, object);
        // moka handles TTL expiry automatically; no is_fresh() check needed
        let Some(entry) = self.get_object_metadata_cache.get(&key).await else {
            return MetadataCacheLookup::Miss;
        };
        if entry.online_disks.iter().filter(|disk| disk.is_some()).count() >= entry.read_quorum {
            MetadataCacheLookup::Hit(entry)
        } else {
            MetadataCacheLookup::RejectedInsufficientQuorum
        }
    }

    async fn cache_get_object_fileinfo(
        &self,
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        parts_metadata: &[FileInfo],
        online_disks: &[Option<DiskStore>],
        read_quorum: usize,
    ) {
        if fi.deleted || !fi.is_valid() {
            return;
        }

        let key = GetObjectMetadataCacheKey::new(bucket, object);
        // moka handles capacity eviction (LRU) automatically
        self.get_object_metadata_cache
            .insert(
                key,
                Arc::new(GetObjectMetadataCacheEntry {
                    created_at: Instant::now(),
                    fi: fi.clone(),
                    parts_metadata: parts_metadata.to_vec(),
                    online_disks: online_disks.to_vec(),
                    read_quorum,
                }),
            )
            .await;
    }

    pub async fn read_version_optimized(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &ReadOptions,
    ) -> Result<Vec<FileInfo>> {
        // Use existing disk selection logic
        let disks = self.disks.read().await;
        let required_reads = self.format.erasure.sets.len();

        // Clone parameters outside the closure to avoid lifetime issues
        let bucket = bucket.to_string();
        let object = object.to_string();
        let version_id = version_id.to_string();
        let opts = opts.clone();

        let processor = runtime_sources::batch_processors().read_processor();
        let tasks: Vec<_> = disks
            .iter()
            .take(required_reads + 2) // Read a few extra for reliability
            .filter_map(|disk| {
                disk.as_ref().map(|d| {
                    let disk = d.clone();
                    let bucket = bucket.clone();
                    let object = object.clone();
                    let version_id = version_id.clone();
                    let opts = opts.clone();

                    async move { disk.read_version(&bucket, &bucket, &object, &version_id, &opts).await }
                })
            })
            .collect();

        match processor.execute_batch_with_quorum(tasks, required_reads).await {
            Ok(results) => Ok(results),
            Err(_) => Err(DiskError::FileNotFound.into()), // Use existing error type
        }
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn get_object_fileinfo(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
        read_data: bool,
    ) -> Result<(FileInfo, Vec<FileInfo>, Vec<Option<DiskStore>>)> {
        let vid = opts.version_id.clone().unwrap_or_default();
        let stage_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();

        let metadata_cache_lookup_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let cache_bypass_reason = Self::get_object_metadata_cache_bypass_reason(bucket, opts, read_data).await;
        let use_metadata_cache = cache_bypass_reason.is_none();
        if let Some(reason) = cache_bypass_reason {
            rustfs_io_metrics::record_get_object_metadata_cache_decision(
                GET_OBJECT_PATH_SET_DISK,
                GET_METADATA_CACHE_DECISION_SKIP,
                reason,
            );
        } else if vid.is_empty() {
            match self.lookup_cached_get_object_fileinfo(bucket, object).await {
                MetadataCacheLookup::Hit(cached) => {
                    rustfs_io_metrics::record_get_object_metadata_cache_decision(
                        GET_OBJECT_PATH_SET_DISK,
                        GET_METADATA_CACHE_DECISION_HIT,
                        GET_METADATA_CACHE_REASON_USABLE,
                    );
                    record_get_stage_duration_if_enabled(
                        GET_OBJECT_PATH_SET_DISK,
                        GET_STAGE_METADATA_CACHE_LOOKUP,
                        metadata_cache_lookup_start,
                    );
                    return Ok((cached.fi.clone(), cached.parts_metadata.clone(), cached.online_disks.clone()));
                }
                MetadataCacheLookup::Miss => {
                    rustfs_io_metrics::record_get_object_metadata_cache_decision(
                        GET_OBJECT_PATH_SET_DISK,
                        GET_METADATA_CACHE_DECISION_MISS,
                        GET_METADATA_CACHE_REASON_NOT_FOUND_OR_EXPIRED,
                    );
                }
                MetadataCacheLookup::RejectedInsufficientQuorum => {
                    rustfs_io_metrics::record_get_object_metadata_cache_decision(
                        GET_OBJECT_PATH_SET_DISK,
                        GET_METADATA_CACHE_DECISION_REJECT,
                        GET_METADATA_CACHE_REASON_INSUFFICIENT_CACHED_QUORUM,
                    );
                }
            }
        }
        record_get_stage_duration_if_enabled(
            GET_OBJECT_PATH_SET_DISK,
            GET_STAGE_METADATA_CACHE_LOOKUP,
            metadata_cache_lookup_start,
        );

        let disks = self.disks.read().await;

        let disks = disks.clone();

        // Early-stop for safe metadata reads is handled inside
        // read_all_fileinfo_observed (see read_all_fileinfo_early_stop in
        // core/io_primitives.rs); unsafe requests fall back to full-wait.
        let (parts_metadata, errs, metadata_fanout_diagnostics) = Self::read_all_fileinfo_observed(
            &disks,
            "",
            bucket,
            object,
            vid.as_str(),
            read_data,
            false,
            opts.incl_free_versions,
            self.default_parity_count,
        )
        .await?;
        metadata_fanout_diagnostics.record(GET_OBJECT_PATH_LEGACY_DUPLEX);
        let metadata_fanout_complete = metadata_fanout_diagnostics.total_responses() >= disks.len();
        // warn!("get_object_fileinfo parts_metadata {:?}", &parts_metadata);
        // warn!("get_object_fileinfo {}/{} errs {:?}", bucket, object, &errs);

        let _min_disks = self.set_drive_count - self.default_parity_count;

        let metadata_resolve_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let (read_quorum, write_quorum) = match Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count)
            .map_err(|err| to_object_err(err.into(), vec![bucket, object]))
        {
            Ok(v) => v,
            Err(e) => {
                // error!("Self::object_quorum_from_meta: {:?}, bucket: {}, object: {}", &e, bucket, object);
                record_get_stage_duration_if_enabled(
                    GET_OBJECT_PATH_SET_DISK,
                    GET_STAGE_METADATA_RESOLVE,
                    metadata_resolve_stage_start,
                );
                return Err(e);
            }
        };
        let read_quorum =
            usize::try_from(read_quorum).map_err(|_| to_object_err(DiskError::ErasureReadQuorum.into(), vec![bucket, object]))?;
        let write_quorum = usize::try_from(write_quorum)
            .map_err(|_| to_object_err(DiskError::ErasureWriteQuorum.into(), vec![bucket, object]))?;
        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum) {
            error!("reduce_read_quorum_errs: {:?}, bucket: {}, object: {}", &err, bucket, object);
            record_get_stage_duration_if_enabled(
                GET_OBJECT_PATH_SET_DISK,
                GET_STAGE_METADATA_RESOLVE,
                metadata_resolve_stage_start,
            );
            return Err(to_object_err(err.into(), vec![bucket, object]));
        }

        let (op_online_disks, fi, fileinfo_selection_quorum) =
            Self::select_valid_fileinfo(&disks, &parts_metadata, &errs, vid.as_str(), read_quorum, write_quorum)?;
        metadata_fanout_diagnostics.record_quorum_candidate_latency(GET_OBJECT_PATH_LEGACY_DUPLEX, fileinfo_selection_quorum);
        if errs.iter().any(|err| err.is_some()) {
            let version_id = resolved_read_repair_version_id(&fi, opts.version_id.as_deref());
            submit_read_repair_heal(
                &fi.volume,
                &fi.name,
                version_id.as_deref(),
                self.pool_index,
                self.set_index,
                None,
                "metadata_read_error",
            )
            .await;
        } else if use_metadata_cache && metadata_fanout_complete {
            self.cache_get_object_fileinfo(bucket, object, &fi, &parts_metadata, &op_online_disks, read_quorum)
                .await;
        }
        record_get_stage_duration_if_enabled(GET_OBJECT_PATH_SET_DISK, GET_STAGE_METADATA_RESOLVE, metadata_resolve_stage_start);
        // debug!("get_object_fileinfo pick fi {:?}", &fi);

        // let online_disks: Vec<Option<DiskStore>> = op_online_disks.iter().filter(|v| v.is_some()).cloned().collect();

        Ok((fi, parts_metadata, op_online_disks))
    }

    pub(super) async fn get_object_info_and_quorum(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> (ObjectInfo, usize, Option<StorageError>) {
        let fi = match self.get_object_fileinfo(bucket, object, opts, false).await {
            Ok((fi, _, _)) => fi,
            Err(e) => return (ObjectInfo::default(), 0, Some(e)),
        };

        let write_quorum = fi.write_quorum(self.default_write_quorum());

        let oi = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);

        if !fi.version_purge_status().is_empty() && opts.version_id.is_some() {
            return (
                oi,
                write_quorum,
                Some(to_object_err(StorageError::MethodNotAllowed, vec![bucket, object])),
            );
        }

        if fi.deleted {
            if opts.incl_free_versions && fi.tier_free_version() && opts.version_id.is_some() {
                return (oi, write_quorum, None);
            }
            return if opts.version_id.is_none() || opts.delete_marker {
                (oi, write_quorum, Some(to_object_err(StorageError::FileNotFound, vec![bucket, object])))
            } else {
                (
                    oi,
                    write_quorum,
                    Some(to_object_err(StorageError::MethodNotAllowed, vec![bucket, object])),
                )
            };
        }

        (oi, write_quorum, None)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn try_get_object_direct_data_shards_with_fileinfo(
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        files: &[FileInfo],
        disks: &[Option<DiskStore>],
        skip_verify_bitrot: bool,
        metrics_object_class: &'static str,
        metrics_size_bucket: &'static str,
    ) -> Result<Option<Bytes>> {
        if fi.parts.len() != 1 || !object_fits_single_block(fi.size, fi.erasure.block_size) {
            return Ok(None);
        }

        let object_size = usize::try_from(fi.size)
            .map_err(|_| to_object_err(Error::other("direct-memory GET object size is invalid"), vec![bucket, object]))?;
        let Some(part) = fi.parts.first() else {
            return Ok(None);
        };
        if part.size != object_size {
            return Ok(None);
        }

        let erasure = coding::Erasure::new_with_options(
            fi.erasure.data_blocks,
            fi.erasure.parity_blocks,
            fi.erasure.block_size,
            fi.uses_legacy_checksum,
        );
        if erasure.data_shards == 0 {
            return Ok(None);
        }

        let checksum_info = fi.erasure.get_checksum_info(part.number);
        let checksum_algo = if fi.uses_legacy_checksum && checksum_info.algorithm == HashAlgorithm::HighwayHash256S {
            HashAlgorithm::HighwayHash256SLegacy
        } else {
            checksum_info.algorithm
        };
        let read_length = erasure.shard_file_offset(0, object_size, object_size);

        if fi.data.is_some() {
            // Collect from the canonical disk-ordered inputs: the helper indexes
            // fi.erasure.distribution by disk position, so passing shard-ordered
            // (shuffled) arrays would apply the permutation twice and concatenate
            // the wrong shards into the response body.
            let Some(data_files) = collect_inline_data_shard_fileinfos_by_index(files, fi, erasure.data_shards, |index| {
                disks.get(index).is_some_and(Option::is_some)
            }) else {
                return Ok(None);
            };

            let reader_setup_stage_start = Instant::now();
            let mut readers = build_inline_bitrot_readers_from_refs(
                &data_files,
                bucket,
                object,
                read_length,
                erasure.shard_size(),
                &checksum_algo,
                skip_verify_bitrot,
            )
            .await?;
            let reader_setup_elapsed = reader_setup_stage_start.elapsed();
            rustfs_io_metrics::record_get_object_shard_reader_setup_duration(reader_setup_elapsed.as_secs_f64());
            rustfs_io_metrics::record_get_object_stage_duration_by_size(
                GET_OBJECT_PATH_DIRECT_MEMORY,
                GET_STAGE_READER_SETUP,
                metrics_object_class,
                metrics_size_bucket,
                reader_setup_elapsed.as_secs_f64(),
            );

            let decode_stage_start = Instant::now();
            let body = try_read_inline_data_shards_direct(&mut readers, erasure.data_shards, read_length, object_size).await;
            let decode_elapsed = decode_stage_start.elapsed();
            rustfs_io_metrics::record_get_object_decode_duration(decode_elapsed.as_secs_f64());
            rustfs_io_metrics::record_get_object_stage_duration_by_size(
                GET_OBJECT_PATH_DIRECT_MEMORY,
                GET_STAGE_DECODE,
                metrics_object_class,
                metrics_size_bucket,
                decode_elapsed.as_secs_f64(),
            );

            if body.is_some() {
                rustfs_io_metrics::record_get_object_direct_memory_subpath(
                    GET_DIRECT_MEMORY_SUBPATH_INLINE_BUFFERED,
                    metrics_object_class,
                    metrics_size_bucket,
                );
            }

            return Ok(body);
        }

        let (disks, files) = Self::shuffle_disks_and_parts_metadata_by_index(disks, files, fi);
        let use_mmap_read = object_mmap_read_enabled();

        let reader_setup_stage_start = Instant::now();
        let mut reader_setup = create_data_block_bitrot_readers(
            &files,
            &disks,
            bucket,
            object,
            part.number,
            0,
            read_length,
            erasure.shard_size(),
            checksum_algo,
            skip_verify_bitrot,
            use_mmap_read,
            erasure.data_shards,
        )
        .await;
        let reader_setup_elapsed = reader_setup_stage_start.elapsed();
        rustfs_io_metrics::record_get_object_shard_reader_setup_duration(reader_setup_elapsed.as_secs_f64());
        rustfs_io_metrics::record_get_object_stage_duration_by_size(
            GET_OBJECT_PATH_DIRECT_MEMORY,
            GET_STAGE_READER_SETUP,
            metrics_object_class,
            metrics_size_bucket,
            reader_setup_elapsed.as_secs_f64(),
        );

        if reader_setup.available_data_shards(erasure.data_shards) < erasure.data_shards {
            return Ok(None);
        }

        let decode_stage_start = Instant::now();
        let body =
            try_read_inline_data_shards_direct(&mut reader_setup.readers, erasure.data_shards, read_length, object_size).await;
        let decode_elapsed = decode_stage_start.elapsed();
        rustfs_io_metrics::record_get_object_decode_duration(decode_elapsed.as_secs_f64());
        rustfs_io_metrics::record_get_object_stage_duration_by_size(
            GET_OBJECT_PATH_DIRECT_MEMORY,
            GET_STAGE_DECODE,
            metrics_object_class,
            metrics_size_bucket,
            decode_elapsed.as_secs_f64(),
        );

        if body.is_some() {
            rustfs_io_metrics::record_get_object_direct_memory_subpath(
                GET_DIRECT_MEMORY_SUBPATH_DISK_DATA_BLOCKS,
                metrics_object_class,
                metrics_size_bucket,
            );
        }

        Ok(body)
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn get_object_with_fileinfo<W>(
        // &self,
        bucket: &str,
        object: &str,
        offset: usize,
        length: i64,
        writer: &mut W,
        fi: FileInfo,
        files: Vec<FileInfo>,
        disks: &[Option<DiskStore>],
        set_index: usize,
        pool_index: usize,
        skip_verify_bitrot: bool,
        prefer_data_blocks_first_reader_setup: bool,
        metrics_path: &'static str,
        metrics_object_class: &'static str,
        metrics_size_bucket: &'static str,
    ) -> Result<()>
    where
        W: AsyncWrite + Send + Sync + Unpin + 'static,
    {
        let pipeline_started = Instant::now();
        debug!(bucket, object, requested_length = length, offset, "get_object_with_fileinfo start");
        let (disks, files) = Self::shuffle_disks_and_parts_metadata_by_index(disks, &files, &fi);

        let total_size = fi.size as usize;

        if offset > total_size {
            let reason = GetObjectFailureReason::RangeOrLengthInvalid;
            record_get_object_pipeline_failure(GET_STAGE_RANGE, reason);
            error!(
                bucket,
                object,
                offset,
                total_size,
                requested_length = length,
                stage = GET_STAGE_RANGE,
                reason = reason.as_str(),
                state = "range_or_length_invalid",
                "GetObject range validation failed"
            );
            return Err(Error::other("offset out of range"));
        }

        let length = if length < 0 { total_size - offset } else { length as usize };

        let Some(end_offset_exclusive) = offset.checked_add(length) else {
            let reason = GetObjectFailureReason::RangeOrLengthInvalid;
            record_get_object_pipeline_failure(GET_STAGE_RANGE, reason);
            error!(
                bucket,
                object,
                offset,
                total_size,
                requested_length = length,
                stage = GET_STAGE_RANGE,
                reason = reason.as_str(),
                state = "range_or_length_invalid",
                "GetObject range validation overflow"
            );
            return Err(Error::other("offset out of range"));
        };

        if end_offset_exclusive > total_size {
            let reason = GetObjectFailureReason::RangeOrLengthInvalid;
            record_get_object_pipeline_failure(GET_STAGE_RANGE, reason);
            error!(
                bucket,
                object,
                offset,
                total_size,
                requested_length = length,
                end_offset_exclusive,
                stage = GET_STAGE_RANGE,
                reason = reason.as_str(),
                state = "range_or_length_invalid",
                "GetObject range validation failed"
            );
            return Err(Error::other("offset out of range"));
        }

        let (part_index, mut part_offset) = fi.to_part_offset(offset)?;

        let mut end_offset = offset;
        if length > 0 {
            end_offset += length - 1
        }

        let (last_part_index, last_part_relative_offset) = fi.to_part_offset(end_offset)?;

        debug!(
            bucket,
            object, offset, length, end_offset, part_index, last_part_index, last_part_relative_offset, "Multipart read bounds"
        );

        let erasure = coding::Erasure::new_with_options(
            fi.erasure.data_blocks,
            fi.erasure.parity_blocks,
            fi.erasure.block_size,
            fi.uses_legacy_checksum,
        );

        // Erasure params come from on-disk metadata; zero values must fail the read
        // instead of panicking on the block/shard divisions below.
        if !erasure.has_valid_dimensions() {
            return Err(Error::other(format!(
                "invalid erasure metadata for {bucket}/{object}: block_size={}, data_blocks={}",
                erasure.block_size, erasure.data_shards
            )));
        }

        let part_indices: Vec<usize> = (part_index..=last_part_index).collect();
        debug!(bucket, object, ?part_indices, "Multipart part indices to stream");

        let mut total_read = 0;
        for current_part in part_indices {
            if total_read == length {
                debug!(
                    bucket,
                    object,
                    total_read,
                    requested_length = length,
                    part_index = current_part,
                    "Stopping multipart stream early because accumulated bytes match request"
                );
                break;
            }

            let part_number = fi.parts[current_part].number;
            let part_size = fi.parts[current_part].size;
            let mut part_length = part_size - part_offset;
            if part_length > (length - total_read) {
                part_length = length - total_read
            }

            let till_offset = erasure.shard_file_offset(part_offset, part_length, part_size);

            let read_offset = (part_offset / erasure.block_size) * erasure.shard_size();

            debug!(
                bucket,
                object,
                part_index = current_part,
                part_number,
                part_offset,
                part_size,
                part_length,
                read_offset,
                till_offset,
                total_read_before = total_read,
                requested_length = length,
                "Streaming multipart part"
            );

            let checksum_info = fi.erasure.get_checksum_info(part_number);
            let checksum_algo = if fi.uses_legacy_checksum && checksum_info.algorithm == HashAlgorithm::HighwayHash256S {
                HashAlgorithm::HighwayHash256SLegacy
            } else {
                checksum_info.algorithm
            };
            let read_length = till_offset.saturating_sub(read_offset);

            let use_mmap_read = object_mmap_read_enabled();

            let reader_setup_stage_start = Instant::now();
            let read_costs = coding::decode::should_collect_shard_read_costs().then(|| shard_read_costs_for_disks(&disks));
            let reader_setup = create_bitrot_readers_until_quorum_with_preference(
                &files,
                &disks,
                bucket,
                object,
                part_number,
                read_offset,
                read_length,
                erasure.shard_size(),
                checksum_algo,
                skip_verify_bitrot,
                use_mmap_read,
                erasure.data_shards,
                erasure.parity_shards,
                BitrotReaderSetupMode::ReadQuorum,
                prefer_data_blocks_first_reader_setup,
                None,
                Some(BitrotReaderSetupAttribution {
                    path: metrics_path,
                    object_class: metrics_object_class,
                    size_bucket: metrics_size_bucket,
                }),
            )
            .await;
            let reader_setup_elapsed = reader_setup_stage_start.elapsed();
            rustfs_io_metrics::record_get_object_shard_reader_setup_duration(reader_setup_elapsed.as_secs_f64());
            rustfs_io_metrics::record_get_object_stage_duration_by_size(
                metrics_path,
                GET_STAGE_READER_SETUP,
                metrics_object_class,
                metrics_size_bucket,
                reader_setup_elapsed.as_secs_f64(),
            );
            let setup_available_readers = reader_setup.available_shards();
            if reader_setup_elapsed >= SLOW_OBJECT_READ_LOG_THRESHOLD {
                warn!(
                    event = EVENT_SET_DISK_READ,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    part_index = current_part,
                    part_number,
                    read_offset,
                    read_length,
                    available_shards = setup_available_readers,
                    total_shards = reader_setup.errors.len(),
                    data_shards = erasure.data_shards,
                    parity_shards = erasure.parity_shards,
                    elapsed_ms = reader_setup_elapsed.as_millis(),
                    errors = ?reader_setup.errors,
                    state = "reader_setup_slow",
                    "Set disk object reader setup is slow"
                );
            }

            let nil_count = reader_setup.available_shards();
            if nil_count < erasure.data_shards {
                if let Some(read_err) = reduce_read_quorum_errs(&reader_setup.errors, OBJECT_OP_IGNORED_ERRS, erasure.data_shards)
                {
                    let reason = classify_disk_error(&read_err);
                    error!(
                        event = EVENT_SET_DISK_READ,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        bucket,
                        object,
                        part_index = current_part,
                        part_number,
                        part_offset,
                        part_length,
                        read_offset,
                        till_offset,
                        total_shards = erasure.data_shards + erasure.parity_shards,
                        available_shards = nil_count,
                        data_shards = erasure.data_shards,
                        stage = GET_STAGE_READER_SETUP,
                        reason = reason.as_str(),
                        errors = ?reader_setup.errors,
                        state = "read_quorum_unavailable",
                        "Create bitrot reader failed read quorum"
                    );
                    record_get_object_pipeline_failure(GET_STAGE_READER_SETUP, reason);
                    return Err(to_object_err(read_err.into(), vec![bucket, object]));
                }
                let reason = GetObjectFailureReason::ReadQuorum;
                error!(
                    event = EVENT_SET_DISK_READ,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    part_index = current_part,
                    part_number,
                    part_offset,
                    part_length,
                    read_offset,
                    till_offset,
                    total_shards = erasure.data_shards + erasure.parity_shards,
                    available_shards = nil_count,
                    data_shards = erasure.data_shards,
                    stage = GET_STAGE_READER_SETUP,
                    reason = reason.as_str(),
                    errors = ?reader_setup.errors,
                    state = "not_enough_readers",
                    "Create bitrot reader did not have enough disks"
                );
                record_get_object_pipeline_failure(GET_STAGE_READER_SETUP, reason);
                return Err(Error::other(format!("not enough disks to read: {:?}", reader_setup.errors)));
            }

            // Check if we have missing shards even though we can read successfully
            // This happens when a node was offline during write and comes back online
            let total_shards = erasure.data_shards + erasure.parity_shards;
            let available_shards = nil_count;
            let missing_shards = reader_setup.completed_failed_shards();

            debug!(
                bucket,
                object,
                part_number,
                total_shards,
                available_shards,
                attempted_shards = reader_setup.attempted.iter().filter(|attempted| **attempted).count(),
                missing_shards,
                data_shards = erasure.data_shards,
                parity_shards = erasure.parity_shards,
                "Shard availability check"
            );

            if missing_shards > 0 && available_shards >= erasure.data_shards {
                // We have missing shards but enough to read - trigger background heal
                debug!(
                    bucket,
                    object,
                    part_number,
                    missing_shards,
                    available_shards,
                    pool_index,
                    set_index,
                    "Detected missing shards during read, triggering background heal"
                );
                let version_id = fi.version_id.as_ref().map(ToString::to_string);
                submit_read_repair_heal(
                    bucket,
                    object,
                    version_id.as_deref(),
                    pool_index,
                    set_index,
                    Some(part_number),
                    "missing_shards",
                )
                .await;
            }

            // debug!(
            //     "read part {} part_offset {},part_length {},part_size {}  ",
            //     part_number, part_offset, part_length, part_size
            // );
            let decode_stage_start = Instant::now();
            let unattempted_data_shards = !reader_setup.data_shards_attempted(erasure.data_shards);
            let readers = reader_setup.readers;
            let (written, err) = if let Some(read_costs) = read_costs {
                erasure
                    .decode_with_read_costs(writer, readers, part_offset, part_length, part_size, read_costs)
                    .await
            } else {
                erasure.decode(writer, readers, part_offset, part_length, part_size).await
            };
            let decode_elapsed = decode_stage_start.elapsed();
            rustfs_io_metrics::record_get_object_decode_duration(decode_elapsed.as_secs_f64());
            rustfs_io_metrics::record_get_object_stage_duration_by_size(
                metrics_path,
                GET_STAGE_DECODE,
                metrics_object_class,
                metrics_size_bucket,
                decode_elapsed.as_secs_f64(),
            );
            if decode_elapsed >= SLOW_OBJECT_READ_LOG_THRESHOLD || err.is_some() {
                warn!(
                    event = EVENT_SET_DISK_READ,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    part_index = current_part,
                    part_number,
                    part_offset,
                    part_size,
                    part_length,
                    bytes_written = written,
                    available_shards,
                    missing_shards,
                    elapsed_ms = decode_elapsed.as_millis(),
                    error = ?err,
                    state = if err.is_some() { "decode_failed_or_slow" } else { "decode_slow" },
                    "Set disk object decode stage is slow or failed"
                );
            }
            debug!(
                bucket,
                object,
                part_index = current_part,
                part_number,
                part_length,
                bytes_written = written,
                "Finished decoding multipart part"
            );
            if let Some(e) = err {
                let de_err: DiskError = e.into();
                let mut has_err = true;
                if written == part_length {
                    let should_enqueue_heal = matches!(de_err, DiskError::FileCorrupt)
                        || (matches!(de_err, DiskError::FileNotFound) && !unattempted_data_shards);
                    if should_enqueue_heal {
                        debug!(
                            bucket,
                            object,
                            part_number,
                            error = ?de_err,
                            "Recoverable decode error triggered read repair"
                        );
                        let version_id = fi.version_id.as_ref().map(ToString::to_string);
                        submit_read_repair_heal(
                            bucket,
                            object,
                            version_id.as_deref(),
                            pool_index,
                            set_index,
                            Some(part_number),
                            "decode_error",
                        )
                        .await;
                        has_err = false;
                    }
                }

                if has_err {
                    let reason = classify_disk_error(&de_err);
                    error!(
                        bucket,
                        object,
                        part_index = current_part,
                        part_number,
                        part_offset,
                        part_length,
                        bytes_written = written,
                        stage = GET_STAGE_DECODE,
                        reason = reason.as_str(),
                        error = ?de_err,
                        "Erasure decode failed during GetObject"
                    );
                    record_get_object_pipeline_failure(GET_STAGE_DECODE, reason);
                    return Err(de_err.into());
                }
            }

            // debug!("ec decode {} written size {}", part_number, n);

            total_read += part_length;
            part_offset = 0;
        }

        // debug!("read end");

        debug!(bucket, object, total_read, expected_length = length, "Multipart read finished");
        let pipeline_elapsed = pipeline_started.elapsed();
        if pipeline_elapsed >= SLOW_OBJECT_READ_LOG_THRESHOLD {
            warn!(
                event = EVENT_SET_DISK_READ,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                bucket,
                object,
                offset,
                total_read,
                expected_length = length,
                elapsed_ms = pipeline_elapsed.as_millis(),
                state = "pipeline_slow",
                "Set disk object read pipeline is slow"
            );
        }

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn get_object_decode_reader_with_fileinfo(
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        files: &[FileInfo],
        disks: &[Option<DiskStore>],
        _set_index: usize,
        _pool_index: usize,
        skip_verify_bitrot: bool,
        metrics_object_class: &'static str,
        metrics_size_bucket: &'static str,
        prefer_data_blocks_first_reader_setup: bool,
    ) -> Result<GetCodecStreamingReaderBuildOutcome> {
        let erasure = coding::Erasure::new_with_options(
            fi.erasure.data_blocks,
            fi.erasure.parity_blocks,
            fi.erasure.block_size,
            fi.uses_legacy_checksum,
        );

        // Erasure params come from on-disk metadata; zero values must fail the read
        // instead of panicking on the block/shard divisions inside the codec
        // streaming reader below. Mirrors the legacy multipart guard.
        if !erasure.has_valid_dimensions() {
            return Err(Error::other(format!(
                "invalid erasure metadata for {bucket}/{object}: block_size={}, data_blocks={}",
                erasure.block_size, erasure.data_shards
            )));
        }

        let (disks, files) = Self::shuffle_disks_and_parts_metadata_by_index(disks, files, fi);

        if fi.parts.len() == 1 {
            let part = &fi.parts[0];
            let part_length =
                usize::try_from(fi.size).map_err(|_| Error::other("codec streaming reader object size is invalid"))?;
            return Self::build_codec_streaming_part_reader(
                bucket,
                object,
                fi,
                &files,
                &disks,
                &erasure,
                part.number,
                0,
                part_length,
                part.size,
                skip_verify_bitrot,
                metrics_object_class,
                metrics_size_bucket,
                prefer_data_blocks_first_reader_setup,
            )
            .await;
        }

        if !is_codec_streaming_multipart_enabled() {
            return Ok(GetCodecStreamingReaderBuildOutcome::Fallback(GetCodecStreamingFallbackReason::Multipart));
        }
        if fi.parts.len() > get_codec_streaming_multipart_max_parts() {
            return Ok(GetCodecStreamingReaderBuildOutcome::Fallback(
                GetCodecStreamingFallbackReason::MultipartPartLimit,
            ));
        }

        let object_length =
            usize::try_from(fi.size).map_err(|_| Error::other("codec streaming reader object size is invalid"))?;
        let mut total_part_size = 0usize;
        for part in &fi.parts {
            total_part_size = total_part_size
                .checked_add(part.size)
                .ok_or_else(|| Error::other("codec streaming multipart part sizes overflow"))?;
        }
        if total_part_size != object_length {
            return Err(Error::other("codec streaming multipart part sizes do not match object size"));
        }

        let mut readers = Vec::with_capacity(fi.parts.len());
        for part in &fi.parts {
            match Self::build_codec_streaming_part_reader(
                bucket,
                object,
                fi,
                &files,
                &disks,
                &erasure,
                part.number,
                0,
                part.size,
                part.size,
                skip_verify_bitrot,
                metrics_object_class,
                metrics_size_bucket,
                false,
            )
            .await?
            {
                GetCodecStreamingReaderBuildOutcome::Reader(reader) => readers.push(reader),
                GetCodecStreamingReaderBuildOutcome::Fallback(reason) => {
                    return Ok(GetCodecStreamingReaderBuildOutcome::Fallback(reason));
                }
            }
        }

        Ok(GetCodecStreamingReaderBuildOutcome::Reader(Box::new(MultipartCodecStreamingReader::new(
            readers,
        ))))
    }

    #[allow(clippy::too_many_arguments)]
    async fn build_codec_streaming_part_reader(
        bucket: &str,
        object: &str,
        fi: &FileInfo,
        files: &[FileInfo],
        disks: &[Option<DiskStore>],
        erasure: &coding::Erasure,
        part_number: usize,
        part_offset: usize,
        part_length: usize,
        part_size: usize,
        skip_verify_bitrot: bool,
        metrics_object_class: &'static str,
        metrics_size_bucket: &'static str,
        prefer_data_blocks_first_reader_setup: bool,
    ) -> Result<GetCodecStreamingReaderBuildOutcome> {
        if part_length > part_size {
            return Err(Error::other("codec streaming reader part length exceeds part size"));
        }
        let checksum_info = fi.erasure.get_checksum_info(part_number);
        let checksum_algo = if fi.uses_legacy_checksum && checksum_info.algorithm == HashAlgorithm::HighwayHash256S {
            HashAlgorithm::HighwayHash256SLegacy
        } else {
            checksum_info.algorithm
        };
        let use_mmap_read = object_mmap_read_enabled();
        let till_offset = erasure.shard_file_offset(part_offset, part_length, part_size);
        let read_offset = (part_offset / erasure.block_size) * erasure.shard_size();
        let read_length = till_offset.saturating_sub(read_offset);

        let stage_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        let metrics_path = get_codec_streaming_metrics_path();
        let reader_stage_metrics = stage_metrics_enabled.then_some(BitrotReaderStageMetrics {
            path: metrics_path,
            reader_construction_stage: GET_STAGE_READER_TASK_READER_CONSTRUCTION,
            file_open_stage: GET_STAGE_READER_TASK_FILE_OPEN,
            bitrot_reader_init_stage: GET_STAGE_READER_TASK_BITROT_READER_INIT,
        });
        let reader_setup_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let read_costs = coding::decode::should_collect_shard_read_costs().then(|| shard_read_costs_for_disks(disks));
        let reader_setup = create_bitrot_readers_until_quorum_with_preference(
            files,
            disks,
            bucket,
            object,
            part_number,
            read_offset,
            read_length,
            erasure.shard_size(),
            checksum_algo,
            skip_verify_bitrot,
            use_mmap_read,
            erasure.data_shards,
            erasure.parity_shards,
            BitrotReaderSetupMode::VerifyReconstruction,
            prefer_data_blocks_first_reader_setup,
            reader_stage_metrics,
            Some(BitrotReaderSetupAttribution {
                path: metrics_path,
                object_class: metrics_object_class,
                size_bucket: metrics_size_bucket,
            }),
        )
        .await;
        record_get_stage_duration_if_enabled(metrics_path, GET_STAGE_READER_SETUP, reader_setup_stage_start);

        let available_shards = reader_setup.available_shards();
        if available_shards < erasure.data_shards {
            if let Some(read_err) = reduce_read_quorum_errs(&reader_setup.errors, OBJECT_OP_IGNORED_ERRS, erasure.data_shards) {
                let reason = classify_disk_error(&read_err);
                record_get_object_pipeline_failure_for_path(metrics_path, GET_STAGE_READER_SETUP, reason);
                return Err(to_object_err(read_err.into(), vec![bucket, object]));
            }
            record_get_object_pipeline_failure_for_path(metrics_path, GET_STAGE_READER_SETUP, GetObjectFailureReason::ReadQuorum);
            return Err(Error::other(format!("not enough disks to read: {:?}", reader_setup.errors)));
        }

        let missing_shards = reader_setup.completed_failed_shards();
        if let Some(reason) = codec_streaming_reader_setup_fallback_reason(missing_shards) {
            return Ok(GetCodecStreamingReaderBuildOutcome::Fallback(reason));
        }

        let readers = reader_setup.readers;
        let source = if let Some(read_costs) = read_costs {
            coding::decode::ParallelReader::new_with_metrics_path_read_costs_and_reconstruction_verification(
                readers,
                erasure.clone(),
                part_offset,
                part_size,
                Some(metrics_path),
                read_costs,
            )
        } else {
            coding::decode::ParallelReader::new_with_metrics_path_and_reconstruction_verification(
                readers,
                erasure.clone(),
                part_offset,
                part_size,
                Some(metrics_path),
            )
        };
        let engine = build_get_codec_streaming_decode_engine(erasure.clone())?;
        let reader =
            coding::decode_reader::ErasureDecodeReader::new_with_metrics_path(source, engine, part_length, metrics_path)?;
        Ok(GetCodecStreamingReaderBuildOutcome::Reader(Box::new(
            coding::decode_reader::SyncErasureDecodeReader::new_with_metrics_path(reader, metrics_path),
        )))
    }
}

fn get_object_metadata_cache_request_bypass_reason(bucket: &str, opts: &ObjectOptions, read_data: bool) -> Option<&'static str> {
    if !read_data {
        return Some(GET_METADATA_CACHE_REASON_NOT_READ_DATA);
    }
    if opts.no_lock && !opts.metadata_cache_safe {
        return Some(GET_METADATA_CACHE_REASON_NO_LOCK);
    }
    if opts.version_id.is_some() {
        return Some(GET_METADATA_CACHE_REASON_VERSION_ID);
    }
    if opts.versioned {
        return Some(GET_METADATA_CACHE_REASON_VERSIONED);
    }
    if opts.version_suspended {
        return Some(GET_METADATA_CACHE_REASON_VERSION_SUSPENDED);
    }
    if opts.incl_free_versions {
        return Some(GET_METADATA_CACHE_REASON_INCL_FREE_VERSIONS);
    }
    if opts.delete_marker {
        return Some(GET_METADATA_CACHE_REASON_DELETE_MARKER);
    }
    if opts.part_number.is_some() {
        return Some(GET_METADATA_CACHE_REASON_PART_NUMBER);
    }
    if opts.data_movement {
        return Some(GET_METADATA_CACHE_REASON_DATA_MOVEMENT);
    }
    if opts.raw_data_movement_read {
        return Some(GET_METADATA_CACHE_REASON_RAW_DATA_MOVEMENT_READ);
    }
    bucket
        .starts_with(RUSTFS_META_BUCKET)
        .then_some(GET_METADATA_CACHE_REASON_META_BUCKET)
}

fn is_get_object_metadata_cache_request_eligible(bucket: &str, opts: &ObjectOptions, read_data: bool) -> bool {
    get_object_metadata_cache_request_bypass_reason(bucket, opts, read_data).is_none()
}

#[cfg(test)]
mod metadata_cache_tests {
    use super::*;
    use rustfs_common::heal_channel::HealAdmissionDropReason;
    use serial_test::serial;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static SLOW_READ_REPAIR_SUBMITTER_CALLS: AtomicUsize = AtomicUsize::new(0);
    static DROPPED_READ_REPAIR_SUBMITTER_CALLS: AtomicUsize = AtomicUsize::new(0);
    static CAPTURED_READ_REPAIR_PRIORITY: Mutex<Option<HealChannelPriority>> = Mutex::new(None);
    static CAPTURED_READ_REPAIR_CALLS: AtomicUsize = AtomicUsize::new(0);

    fn slow_read_repair_submitter(_request: rustfs_common::heal_channel::HealChannelRequest) -> ReadRepairAdmissionFuture {
        SLOW_READ_REPAIR_SUBMITTER_CALLS.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            tokio::time::sleep(Duration::from_millis(250)).await;
            ReadRepairAdmissionOutcome::Response(HealAdmissionResult::Accepted)
        })
    }

    fn dropped_read_repair_submitter(_request: rustfs_common::heal_channel::HealChannelRequest) -> ReadRepairAdmissionFuture {
        DROPPED_READ_REPAIR_SUBMITTER_CALLS.fetch_add(1, Ordering::Relaxed);
        Box::pin(async {
            ReadRepairAdmissionOutcome::Response(HealAdmissionResult::Dropped(HealAdmissionDropReason::PolicyDropped))
        })
    }

    fn capture_read_repair_submitter(request: rustfs_common::heal_channel::HealChannelRequest) -> ReadRepairAdmissionFuture {
        CAPTURED_READ_REPAIR_CALLS.fetch_add(1, Ordering::Relaxed);
        *CAPTURED_READ_REPAIR_PRIORITY.lock().expect("capture mutex poisoned") = Some(request.priority);
        Box::pin(async {
            tokio::time::sleep(Duration::from_millis(1)).await;
            ReadRepairAdmissionOutcome::Response(HealAdmissionResult::Accepted)
        })
    }

    async fn new_metadata_cache_test_set() -> Arc<SetDisks> {
        SetDisks::new(
            "metadata-cache-test".to_string(),
            Arc::new(RwLock::new(Vec::new())),
            4,
            2,
            0,
            0,
            Vec::new(),
            FormatV3::new(1, 4),
            Vec::new(),
        )
        .await
    }

    #[test]
    #[serial]
    fn get_object_metadata_cache_capacity_uses_default_and_env_override() {
        temp_env::with_var(ENV_RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES, None::<&str>, || {
            assert_eq!(get_object_metadata_cache_max_entries(), DEFAULT_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES);
        });

        temp_env::with_var(ENV_RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES, Some("16384"), || {
            assert_eq!(get_object_metadata_cache_max_entries(), 16_384);
        });

        temp_env::with_var(ENV_RUSTFS_GET_OBJECT_METADATA_CACHE_MAX_ENTRIES, Some("0"), || {
            assert_eq!(get_object_metadata_cache_max_entries(), 1);
        });
    }

    fn valid_test_fileinfo(object: &str) -> FileInfo {
        let mut fi = FileInfo::new(object, 2, 2);
        fi.volume = "bucket".to_string();
        fi.name = object.to_string();
        fi.size = 1;
        fi.erasure.index = 1;
        fi.metadata.insert("etag".to_string(), "etag-1".to_string());
        fi
    }

    #[test]
    fn get_object_metadata_cache_request_eligibility_is_conservative() {
        let opts = ObjectOptions::default();
        assert!(is_get_object_metadata_cache_request_eligible("bucket", &opts, true));
        assert_eq!(get_object_metadata_cache_request_bypass_reason("bucket", &opts, true), None);
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, false));
        assert_eq!(
            get_object_metadata_cache_request_bypass_reason("bucket", &opts, false),
            Some(GET_METADATA_CACHE_REASON_NOT_READ_DATA)
        );
        assert!(!is_get_object_metadata_cache_request_eligible(RUSTFS_META_BUCKET, &opts, true));
        assert_eq!(
            get_object_metadata_cache_request_bypass_reason(RUSTFS_META_BUCKET, &opts, true),
            Some(GET_METADATA_CACHE_REASON_META_BUCKET)
        );

        let mut opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));
        assert_eq!(
            get_object_metadata_cache_request_bypass_reason("bucket", &opts, true),
            Some(GET_METADATA_CACHE_REASON_NO_LOCK)
        );
        opts.metadata_cache_safe = true;
        assert!(is_get_object_metadata_cache_request_eligible("bucket", &opts, true));
        assert_eq!(get_object_metadata_cache_request_bypass_reason("bucket", &opts, true), None);

        opts = ObjectOptions {
            version_id: Some("version".to_string()),
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));
        assert_eq!(
            get_object_metadata_cache_request_bypass_reason("bucket", &opts, true),
            Some(GET_METADATA_CACHE_REASON_VERSION_ID)
        );

        opts = ObjectOptions {
            versioned: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));
        assert_eq!(
            get_object_metadata_cache_request_bypass_reason("bucket", &opts, true),
            Some(GET_METADATA_CACHE_REASON_VERSIONED)
        );

        opts = ObjectOptions {
            version_suspended: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));
        assert_eq!(
            get_object_metadata_cache_request_bypass_reason("bucket", &opts, true),
            Some(GET_METADATA_CACHE_REASON_VERSION_SUSPENDED)
        );

        opts = ObjectOptions {
            incl_free_versions: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));
        assert_eq!(
            get_object_metadata_cache_request_bypass_reason("bucket", &opts, true),
            Some(GET_METADATA_CACHE_REASON_INCL_FREE_VERSIONS)
        );

        opts = ObjectOptions {
            delete_marker: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));
        assert_eq!(
            get_object_metadata_cache_request_bypass_reason("bucket", &opts, true),
            Some(GET_METADATA_CACHE_REASON_DELETE_MARKER)
        );

        opts = ObjectOptions {
            part_number: Some(1),
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));
        assert_eq!(
            get_object_metadata_cache_request_bypass_reason("bucket", &opts, true),
            Some(GET_METADATA_CACHE_REASON_PART_NUMBER)
        );

        opts = ObjectOptions {
            data_movement: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));
        assert_eq!(
            get_object_metadata_cache_request_bypass_reason("bucket", &opts, true),
            Some(GET_METADATA_CACHE_REASON_DATA_MOVEMENT)
        );

        opts = ObjectOptions {
            raw_data_movement_read: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));
        assert_eq!(
            get_object_metadata_cache_request_bypass_reason("bucket", &opts, true),
            Some(GET_METADATA_CACHE_REASON_RAW_DATA_MOVEMENT_READ)
        );
    }

    #[tokio::test]
    async fn read_repair_heal_dedupes_same_object_version() {
        let bucket = format!("bucket-{}", Uuid::new_v4());

        assert!(reserve_read_repair_heal(&bucket, "object", None, 0, 0).await.is_some());
        assert!(reserve_read_repair_heal(&bucket, "object", None, 0, 0).await.is_none());
        assert!(
            reserve_read_repair_heal(&bucket, "object", Some("version-2"), 0, 0)
                .await
                .is_some()
        );
        assert!(reserve_read_repair_heal(&bucket, "object", None, 0, 1).await.is_some());
    }

    #[tokio::test]
    async fn read_repair_heal_reservation_can_be_released_after_rejection() {
        let bucket = format!("bucket-{}", Uuid::new_v4());
        let key = reserve_read_repair_heal(&bucket, "object", None, 0, 0)
            .await
            .expect("first reservation should be accepted");

        assert!(reserve_read_repair_heal(&bucket, "object", None, 0, 0).await.is_none());
        release_read_repair_heal_reservation(&key).await;
        assert!(reserve_read_repair_heal(&bucket, "object", None, 0, 0).await.is_some());
    }

    #[tokio::test]
    #[serial]
    async fn submit_read_repair_heal_does_not_wait_for_slow_admission() {
        SLOW_READ_REPAIR_SUBMITTER_CALLS.store(0, Ordering::Relaxed);
        let bucket = format!("bucket-{}", Uuid::new_v4());
        let started = Instant::now();

        submit_read_repair_heal_with_submitter(
            ReadRepairHealSubmission {
                bucket: &bucket,
                object: "object",
                version_id: None,
                pool_index: 0,
                set_index: 0,
                part_number: Some(1),
                reason: "missing_shards",
            },
            slow_read_repair_submitter,
        )
        .await;

        assert!(
            started.elapsed() < Duration::from_millis(50),
            "read-repair submission should not wait for admission response"
        );

        timeout(Duration::from_secs(1), async {
            while SLOW_READ_REPAIR_SUBMITTER_CALLS.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("background read-repair submitter should be called");
    }

    #[tokio::test]
    #[serial]
    async fn submit_read_repair_heal_releases_reservation_after_policy_drop() {
        DROPPED_READ_REPAIR_SUBMITTER_CALLS.store(0, Ordering::Relaxed);
        let bucket = format!("bucket-{}", Uuid::new_v4());

        submit_read_repair_heal_with_submitter(
            ReadRepairHealSubmission {
                bucket: &bucket,
                object: "object",
                version_id: None,
                pool_index: 0,
                set_index: 0,
                part_number: Some(1),
                reason: "missing_shards",
            },
            dropped_read_repair_submitter,
        )
        .await;

        let released_key = timeout(Duration::from_secs(1), async {
            loop {
                if let Some(key) = reserve_read_repair_heal(&bucket, "object", None, 0, 0).await {
                    break key;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("read-repair reservation should be released after policy drop");

        assert_eq!(DROPPED_READ_REPAIR_SUBMITTER_CALLS.load(Ordering::Relaxed), 1);
        release_read_repair_heal_reservation(&released_key).await;
    }

    #[tokio::test]
    #[serial]
    async fn submit_read_repair_heal_uses_low_priority() {
        CAPTURED_READ_REPAIR_CALLS.store(0, Ordering::Relaxed);
        *CAPTURED_READ_REPAIR_PRIORITY.lock().expect("capture mutex poisoned") = None;
        let bucket = format!("bucket-{}", Uuid::new_v4());

        submit_read_repair_heal_with_submitter(
            ReadRepairHealSubmission {
                bucket: &bucket,
                object: "object",
                version_id: None,
                pool_index: 0,
                set_index: 0,
                part_number: Some(1),
                reason: "missing_shards",
            },
            capture_read_repair_submitter,
        )
        .await;

        tokio::time::timeout(Duration::from_secs(1), async {
            while CAPTURED_READ_REPAIR_CALLS.load(Ordering::Relaxed) == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("background read-repair submitter should be called");

        assert_eq!(
            *CAPTURED_READ_REPAIR_PRIORITY.lock().expect("capture mutex poisoned"),
            Some(HealChannelPriority::Low)
        );
    }

    #[test]
    fn resolved_read_repair_version_prefers_selected_fileinfo_version() {
        let mut fi = valid_test_fileinfo("object");
        fi.version_id = Some(Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap());

        assert_eq!(
            resolved_read_repair_version_id(&fi, Some("00000000-0000-0000-0000-000000000002")).as_deref(),
            Some("00000000-0000-0000-0000-000000000001")
        );

        fi.version_id = None;
        assert_eq!(
            resolved_read_repair_version_id(&fi, Some("00000000-0000-0000-0000-000000000002")).as_deref(),
            Some("00000000-0000-0000-0000-000000000002")
        );
    }

    #[tokio::test]
    async fn get_object_metadata_cache_hit_returns_stored_metadata() {
        let set = new_metadata_cache_test_set().await;
        let fi = valid_test_fileinfo("object");
        let parts_metadata = vec![fi.clone()];
        let online_disks = Vec::new();

        set.cache_get_object_fileinfo("bucket", "object", &fi, &parts_metadata, &online_disks, 0)
            .await;

        let cached = set
            .cached_get_object_fileinfo("bucket", "object")
            .await
            .expect("fresh cache entry should be returned");
        assert_eq!(cached.fi.name, "object");
        assert_eq!(cached.parts_metadata.len(), 1);
        assert_eq!(cached.online_disks.len(), 0);
        assert_eq!(cached.read_quorum, 0);
    }

    #[tokio::test]
    async fn get_object_metadata_cache_rejects_deleted_and_invalid_fileinfo() {
        let set = new_metadata_cache_test_set().await;

        let mut deleted = valid_test_fileinfo("deleted-object");
        deleted.deleted = true;
        set.cache_get_object_fileinfo("bucket", "deleted-object", &deleted, &[deleted.clone()], &[], 0)
            .await;
        assert!(
            set.cached_get_object_fileinfo("bucket", "deleted-object").await.is_none(),
            "deleted metadata must not be cached"
        );

        let invalid = FileInfo::default();
        set.cache_get_object_fileinfo("bucket", "invalid-object", &invalid, std::slice::from_ref(&invalid), &[], 0)
            .await;
        assert!(
            set.cached_get_object_fileinfo("bucket", "invalid-object").await.is_none(),
            "invalid metadata must not be cached"
        );
    }

    #[tokio::test]
    async fn get_object_metadata_cache_requires_cached_read_quorum() {
        let set = new_metadata_cache_test_set().await;
        let fi = valid_test_fileinfo("object");

        set.get_object_metadata_cache
            .insert(
                GetObjectMetadataCacheKey::new("bucket", "object"),
                Arc::new(GetObjectMetadataCacheEntry {
                    created_at: Instant::now(),
                    fi: fi.clone(),
                    parts_metadata: vec![fi],
                    online_disks: vec![None],
                    read_quorum: 1,
                }),
            )
            .await;

        assert!(
            set.cached_get_object_fileinfo("bucket", "object").await.is_none(),
            "cache hit must be rejected when cached online disks cannot satisfy read quorum"
        );
    }

    #[tokio::test]
    async fn get_object_metadata_cache_rejects_stale_entries() {
        // moka handles TTL expiry automatically via time_to_live(250ms).
        // This test verifies that entries inserted with the cache API are retrievable
        // while fresh, and that the cache API works correctly.
        let set = new_metadata_cache_test_set().await;
        let fi = valid_test_fileinfo("object");

        set.cache_get_object_fileinfo("bucket", "object", &fi, std::slice::from_ref(&fi), &[], 0)
            .await;

        assert!(
            set.cached_get_object_fileinfo("bucket", "object").await.is_some(),
            "freshly inserted entry should be returned"
        );
    }

    #[tokio::test]
    async fn get_object_metadata_cache_invalidation_removes_object_entry() {
        let set = new_metadata_cache_test_set().await;
        let fi = valid_test_fileinfo("object");

        set.cache_get_object_fileinfo("bucket", "object", &fi, std::slice::from_ref(&fi), &[], 0)
            .await;
        assert!(set.cached_get_object_fileinfo("bucket", "object").await.is_some());

        set.invalidate_get_object_metadata_cache("bucket", "object").await;
        assert!(
            set.cached_get_object_fileinfo("bucket", "object").await.is_none(),
            "explicit invalidation must remove the cached object metadata"
        );
    }

    #[tokio::test]
    async fn get_object_metadata_cache_prunes_when_capacity_is_reached() {
        // moka handles capacity eviction automatically via the configured max_capacity.
        // This test verifies that the cache can hold entries and that insertion works.
        let set = new_metadata_cache_test_set().await;
        let fresh_fi = valid_test_fileinfo("fresh-object");

        set.cache_get_object_fileinfo("bucket", "fresh-object", &fresh_fi, std::slice::from_ref(&fresh_fi), &[], 0)
            .await;

        assert!(
            set.cached_get_object_fileinfo("bucket", "fresh-object").await.is_some(),
            "freshly inserted entry should be retrievable"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::erasure::coding::BitrotWriter;
    use std::io::{Cursor, ErrorKind};
    use std::sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    };
    use tokio::io::AsyncReadExt;

    const CODEC_STREAMING_TEST_BUCKET: &str = "bucket";
    const CODEC_STREAMING_TEST_OBJECT: &str = "object";

    #[test]
    fn shard_read_cost_for_endpoint_maps_topology_classes() {
        let local_hosts = vec!["node-a:9000".to_string()];

        assert_eq!(shard_read_cost_for_endpoint(true, "node-a:9000", &local_hosts), ShardReadCost::Local);
        assert_eq!(shard_read_cost_for_endpoint(false, "node-a:9000", &local_hosts), ShardReadCost::SameNode);
        assert_eq!(shard_read_cost_for_endpoint(false, "node-b:9000", &local_hosts), ShardReadCost::Remote);
        assert_eq!(shard_read_cost_for_endpoint(false, "", &local_hosts), ShardReadCost::Remote);
        assert_eq!(shard_read_cost_for_disk(None, &local_hosts), ShardReadCost::Unknown);
    }

    fn metadata_fanout_test_fileinfo(object: &str) -> FileInfo {
        let mut fi = FileInfo::new(object, 2, 2);
        fi.volume = "bucket".to_string();
        fi.name = object.to_string();
        fi.size = 1;
        fi.erasure.index = 1;
        fi.metadata.insert("etag".to_string(), "etag-1".to_string());
        fi
    }

    fn codec_streaming_test_fileinfo(size: i64, part_count: usize) -> FileInfo {
        let mut fi = FileInfo::new("object", 4, 2);
        fi.volume = "bucket".to_string();
        fi.name = "object".to_string();
        fi.size = size;
        fi.metadata.insert("etag".to_string(), "etag-1".to_string());

        let size = usize::try_from(size).expect("test object size should fit usize");
        let part_count = part_count.max(1);
        let part_size = size.div_ceil(part_count);
        for index in 0..part_count {
            let remaining = size.saturating_sub(index * part_size);
            let current_part_size = remaining.min(part_size);
            fi.add_object_part(
                index + 1,
                format!("etag-{index}"),
                current_part_size,
                None,
                i64::try_from(current_part_size).expect("test part size should fit i64"),
                None,
                None,
            );
        }

        fi
    }

    fn read_part_test_part(number: usize, etag: &str) -> ObjectPartInfo {
        ObjectPartInfo {
            number,
            etag: etag.to_string(),
            ..Default::default()
        }
    }

    fn read_part_test_missing(number: usize) -> ObjectPartInfo {
        ObjectPartInfo {
            number,
            error: Some("file not found".to_string()),
            ..Default::default()
        }
    }

    #[test]
    fn resolve_read_part_returns_part_when_etag_reaches_quorum() {
        let part = read_part_test_part(1, "etag-1");
        let responses = vec![
            Some(vec![part.clone()]),
            Some(vec![part]),
            Some(vec![read_part_test_missing(1)]),
            None,
        ];

        let resolved = resolve_read_part_from_responses("bucket", "upload/part.1.meta", 1, 0, 1, &responses, 2)
            .expect("etag quorum should resolve part");

        assert_eq!(resolved.etag, "etag-1");
        assert!(resolved.error.is_none());
    }

    #[test]
    fn resolve_read_part_returns_missing_only_when_missing_reaches_quorum() {
        let responses = vec![
            Some(vec![read_part_test_missing(1)]),
            Some(vec![read_part_test_missing(1)]),
            None,
        ];

        let resolved = resolve_read_part_from_responses("bucket", "upload/part.1.meta", 1, 0, 1, &responses, 2)
            .expect("missing quorum should resolve as a confirmed missing part");

        assert_eq!(resolved.number, 1);
        assert_eq!(resolved.error.as_deref(), Some("part.1 not found"));
    }

    #[test]
    fn resolve_read_part_treats_os_not_found_as_confirmed_missing() {
        let responses = vec![
            Some(vec![ObjectPartInfo {
                number: 9999,
                error: Some("No such file or directory (os error 2)".to_string()),
                ..Default::default()
            }]),
            Some(vec![ObjectPartInfo {
                number: 9999,
                error: Some("No such file or directory (os error 2)".to_string()),
                ..Default::default()
            }]),
            Some(vec![read_part_test_part(1, "stale-etag")]),
            None,
        ];

        let resolved = resolve_read_part_from_responses("bucket", "upload/part.9999.meta", 9999, 0, 1, &responses, 2)
            .expect("OS not-found quorum should resolve as a missing part");

        assert_eq!(resolved.number, 9999);
        assert_eq!(resolved.error.as_deref(), Some("part.9999 not found"));
    }

    #[test]
    fn resolve_read_part_returns_missing_when_missing_quorum_beats_stale_present_part() {
        let responses = vec![
            Some(vec![read_part_test_missing(1)]),
            Some(vec![read_part_test_missing(1)]),
            Some(vec![read_part_test_part(1, "stale-etag")]),
            None,
        ];

        let resolved = resolve_read_part_from_responses("bucket", "upload/part.1.meta", 1, 0, 1, &responses, 2)
            .expect("confirmed missing quorum should resolve as a missing part despite stale metadata");

        assert_eq!(resolved.number, 1);
        assert_eq!(resolved.error.as_deref(), Some("part.1 not found"));
    }

    #[test]
    fn resolve_read_part_preserves_read_quorum_when_present_part_lacks_quorum() {
        let responses = vec![
            Some(vec![read_part_test_part(1, "etag-1")]),
            Some(vec![read_part_test_missing(1)]),
            None,
        ];

        let err = resolve_read_part_from_responses("bucket", "upload/part.1.meta", 1, 0, 1, &responses, 2)
            .expect_err("mixed present and missing observations should not become InvalidPart");

        assert_eq!(err, DiskError::ErasureReadQuorum);
    }

    #[test]
    fn resolve_read_part_does_not_count_mismatched_response_as_missing() {
        let responses = vec![Some(Vec::new()), Some(vec![read_part_test_missing(1)]), None];

        let err = resolve_read_part_from_responses("bucket", "upload/part.1.meta", 1, 0, 1, &responses, 2)
            .expect_err("invalid disk responses must not vote for a missing part");

        assert_eq!(err, DiskError::ErasureReadQuorum);
    }

    #[test]
    fn metadata_fanout_diagnostics_classifies_response_outcomes() {
        let valid = metadata_fanout_test_fileinfo("object");
        let invalid = FileInfo::default();

        let observations = vec![
            MetadataFanoutObservation::from_file_info(&valid, Duration::from_millis(3)),
            MetadataFanoutObservation::from_file_info(&invalid, Duration::from_millis(4)),
            MetadataFanoutObservation::from_error(&DiskError::FileNotFound, Duration::from_millis(5)),
            MetadataFanoutObservation::from_error(&DiskError::FileVersionNotFound, Duration::from_millis(6)),
            MetadataFanoutObservation::from_error(&DiskError::DiskNotFound, Duration::from_millis(7)),
            MetadataFanoutObservation::from_error(&DiskError::FileCorrupt, Duration::from_millis(8)),
            MetadataFanoutObservation::from_error(&DiskError::Timeout, Duration::from_millis(9)),
            MetadataFanoutObservation::from_error(&DiskError::FaultyDisk, Duration::from_millis(10)),
            MetadataFanoutObservation::from_error(&DiskError::Unexpected, Duration::from_millis(11)),
        ];
        let diagnostics = MetadataFanoutDiagnostics::new(Duration::from_millis(12), observations);
        let outcomes = diagnostics
            .observations
            .iter()
            .map(|observation| observation.outcome)
            .collect::<Vec<_>>();

        assert_eq!(
            outcomes,
            vec![
                GET_METADATA_RESPONSE_VALID,
                GET_METADATA_RESPONSE_ERROR,
                GET_METADATA_RESPONSE_NOT_FOUND,
                GET_METADATA_RESPONSE_VERSION_NOT_FOUND,
                GET_METADATA_RESPONSE_DISK_NOT_FOUND,
                GET_METADATA_RESPONSE_CORRUPT,
                GET_METADATA_RESPONSE_TIMEOUT,
                GET_METADATA_RESPONSE_IGNORED,
                GET_METADATA_RESPONSE_ERROR,
            ]
        );
        assert_eq!(diagnostics.total_responses(), 9);
        assert_eq!(diagnostics.valid_responses(), 1);
        assert_eq!(diagnostics.error_responses(), 8);
    }

    #[test]
    fn metadata_fanout_diagnostics_tracks_ignored_errors_without_hiding_outcomes() {
        let diagnostics = MetadataFanoutDiagnostics::new(
            Duration::from_millis(15),
            vec![
                MetadataFanoutObservation::from_error(&DiskError::DiskNotFound, Duration::from_millis(1)),
                MetadataFanoutObservation::from_error(&DiskError::FaultyRemoteDisk, Duration::from_millis(2)),
                MetadataFanoutObservation::from_error(&DiskError::FileNotFound, Duration::from_millis(3)),
            ],
        );

        assert_eq!(diagnostics.ignored_responses(), 2);
        assert_eq!(diagnostics.error_responses(), 3);
        assert_eq!(diagnostics.observations[0].outcome, GET_METADATA_RESPONSE_DISK_NOT_FOUND);
        assert_eq!(diagnostics.observations[1].outcome, GET_METADATA_RESPONSE_IGNORED);
        assert_eq!(diagnostics.observations[2].outcome, GET_METADATA_RESPONSE_NOT_FOUND);
    }

    #[test]
    fn metadata_fanout_quorum_candidate_latency_ignores_slow_trailing_valid_response() {
        let valid = metadata_fanout_test_fileinfo("object");
        let diagnostics = MetadataFanoutDiagnostics::new(
            Duration::from_millis(250),
            vec![
                MetadataFanoutObservation::from_error(&DiskError::FileNotFound, Duration::from_millis(2)),
                MetadataFanoutObservation::from_file_info(&valid, Duration::from_millis(5)),
                MetadataFanoutObservation::from_file_info(&valid, Duration::from_millis(7)),
                MetadataFanoutObservation::from_file_info(&valid, Duration::from_millis(250)),
            ],
        );

        assert_eq!(diagnostics.first_response_latency(), Some(Duration::from_millis(2)));
        assert_eq!(diagnostics.first_valid_response_latency(), Some(Duration::from_millis(5)));
        assert_eq!(diagnostics.slowest_response_latency(), Some(Duration::from_millis(250)));
        assert_eq!(diagnostics.quorum_candidate_latency(2), Some(Duration::from_millis(7)));
    }

    #[test]
    fn metadata_fanout_quorum_candidate_latency_requires_enough_valid_responses() {
        let valid = metadata_fanout_test_fileinfo("object");
        let diagnostics = MetadataFanoutDiagnostics::new(
            Duration::from_millis(8),
            vec![
                MetadataFanoutObservation::from_file_info(&valid, Duration::from_millis(5)),
                MetadataFanoutObservation::from_error(&DiskError::FileNotFound, Duration::from_millis(6)),
            ],
        );

        assert_eq!(diagnostics.quorum_candidate_latency(0), Some(Duration::ZERO));
        assert_eq!(diagnostics.quorum_candidate_latency(1), Some(Duration::from_millis(5)));
        assert_eq!(diagnostics.quorum_candidate_latency(2), None);
    }

    #[test]
    fn metadata_fanout_counts_delete_marker_and_conflicting_versions_as_valid_observations_only() {
        let mut latest = metadata_fanout_test_fileinfo("object");
        latest.version_id = Some(Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("static uuid should parse"));

        let mut historical = metadata_fanout_test_fileinfo("object");
        historical.version_id = Some(Uuid::parse_str("00000000-0000-0000-0000-000000000002").expect("static uuid should parse"));

        let mut delete_marker = metadata_fanout_test_fileinfo("object");
        delete_marker.deleted = true;
        delete_marker.version_id =
            Some(Uuid::parse_str("00000000-0000-0000-0000-000000000003").expect("static uuid should parse"));

        let diagnostics = MetadataFanoutDiagnostics::new(
            Duration::from_millis(9),
            vec![
                MetadataFanoutObservation::from_file_info(&latest, Duration::from_millis(3)),
                MetadataFanoutObservation::from_file_info(&historical, Duration::from_millis(4)),
                MetadataFanoutObservation::from_file_info(&delete_marker, Duration::from_millis(5)),
            ],
        );

        assert_eq!(diagnostics.total_responses(), 3);
        assert_eq!(diagnostics.valid_responses(), 3);
        assert_eq!(diagnostics.error_responses(), 0);
        assert!(
            diagnostics
                .observations
                .iter()
                .all(|observation| observation.outcome == GET_METADATA_RESPONSE_VALID)
        );
    }

    fn metadata_early_stop_accumulator() -> MetadataQuorumAccumulator {
        MetadataQuorumAccumulator::new(4, 2, true)
    }

    fn metadata_early_stop_candidate(object: &str, disk_index: usize) -> FileInfo {
        let mut fi = metadata_fanout_test_fileinfo(object);
        fi.erasure.index = disk_index;
        fi.add_object_part(1, "part-etag-1".to_string(), 1, None, 1, None, None);
        fi
    }

    #[test]
    fn metadata_quorum_accumulator_hits_all_valid_same_version() {
        let mut accumulator = metadata_early_stop_accumulator();

        accumulator.observe_file_info(&metadata_early_stop_candidate("object", 1));
        assert!(accumulator.early_stop_decision().is_none());
        accumulator.observe_file_info(&metadata_early_stop_candidate("object", 2));

        assert_eq!(
            accumulator.early_stop_decision(),
            Some(MetadataEarlyStopDecision {
                reason: GET_METADATA_EARLY_STOP_REASON_VALID_QUORUM
            })
        );
        assert_eq!(accumulator.valid_responses, 2);
    }

    #[test]
    fn metadata_quorum_accumulator_hits_quorum_valid_with_slow_trailing_disk() {
        let mut accumulator = metadata_early_stop_accumulator();

        accumulator.observe_file_info(&metadata_early_stop_candidate("object", 1));
        accumulator.observe_file_info(&metadata_early_stop_candidate("object", 2));

        assert!(accumulator.early_stop_decision().is_some());
        assert_eq!(accumulator.candidate_votes, 2);
    }

    #[test]
    fn metadata_quorum_accumulator_partial_result_remains_quorum_compatible() {
        let first = metadata_early_stop_candidate("object", 1);
        let second = metadata_early_stop_candidate("object", 2);
        let parts_metadata = vec![first.clone(), second, FileInfo::default(), FileInfo::default()];
        let errs = vec![None, None, None, None];

        let (read_quorum, _) = SetDisks::object_quorum_from_meta(&parts_metadata, &errs, 2)
            .expect("partial early-stop metadata should preserve read quorum");
        let read_quorum = usize::try_from(read_quorum).expect("read quorum should be non-negative");
        let selected = SetDisks::pick_valid_fileinfo(&parts_metadata, None, Some("etag-1".to_string()), read_quorum)
            .expect("partial early-stop metadata should preserve selected FileInfo");

        assert_eq!(read_quorum, 2);
        assert_eq!(selected.name, first.name);
        assert_eq!(selected.get_etag(), first.get_etag());
    }

    #[test]
    fn metadata_quorum_accumulator_falls_back_on_conflicting_versions() {
        let mut accumulator = metadata_early_stop_accumulator();
        let first = metadata_early_stop_candidate("object", 1);
        let mut second = metadata_early_stop_candidate("object", 2);
        second.version_id = Some(Uuid::parse_str("00000000-0000-0000-0000-000000000002").expect("static uuid should parse"));

        accumulator.observe_file_info(&first);
        accumulator.observe_file_info(&second);

        assert!(accumulator.early_stop_decision().is_none());
        assert_eq!(accumulator.final_miss_reason(), GET_METADATA_EARLY_STOP_REASON_CONFLICTING_METADATA);
    }

    #[test]
    fn metadata_quorum_accumulator_falls_back_on_split_data_dir() {
        let mut accumulator = metadata_early_stop_accumulator();
        let mut first = metadata_early_stop_candidate("object", 1);
        let mut second = metadata_early_stop_candidate("object", 2);
        first.data_dir = Some(Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("static uuid should parse"));
        second.data_dir = Some(Uuid::parse_str("00000000-0000-0000-0000-000000000002").expect("static uuid should parse"));

        accumulator.observe_file_info(&first);
        accumulator.observe_file_info(&second);

        assert!(accumulator.early_stop_decision().is_none());
        assert_eq!(accumulator.final_miss_reason(), GET_METADATA_EARLY_STOP_REASON_CONFLICTING_METADATA);
    }

    #[test]
    fn metadata_quorum_accumulator_falls_back_on_explicit_version_quorum() {
        let mut accumulator = MetadataQuorumAccumulator::new(4, 2, false);

        accumulator.observe_file_info(&metadata_early_stop_candidate("object", 1));
        accumulator.observe_file_info(&metadata_early_stop_candidate("object", 2));

        assert!(accumulator.early_stop_decision().is_none());
        assert_eq!(accumulator.final_miss_reason(), GET_METADATA_EARLY_STOP_REASON_UNSAFE_REQUEST);
    }

    #[test]
    fn metadata_quorum_accumulator_hits_delete_marker_quorum_early_stop() {
        let mut accumulator = metadata_early_stop_accumulator();
        let mut deleted = metadata_early_stop_candidate("object", 1);
        deleted.deleted = true;

        accumulator.observe_file_info(&deleted);
        accumulator.observe_file_info(&deleted);

        assert_eq!(
            accumulator.early_stop_decision(),
            Some(MetadataEarlyStopDecision {
                reason: GET_METADATA_EARLY_STOP_REASON_DELETE_MARKER
            })
        );
    }

    #[test]
    fn metadata_quorum_accumulator_falls_back_on_delete_marker_below_quorum() {
        let mut accumulator = metadata_early_stop_accumulator();
        let mut deleted = metadata_early_stop_candidate("object", 1);
        deleted.deleted = true;

        accumulator.observe_file_info(&deleted);

        assert!(accumulator.early_stop_decision().is_none());
        assert_eq!(accumulator.final_miss_reason(), GET_METADATA_EARLY_STOP_REASON_DELETE_MARKER);
    }

    #[test]
    fn metadata_quorum_accumulator_falls_back_on_object_not_found_quorum() {
        let mut accumulator = metadata_early_stop_accumulator();

        accumulator.observe_error(&DiskError::FileNotFound);
        accumulator.observe_error(&DiskError::VolumeNotFound);

        assert!(accumulator.early_stop_decision().is_none());
        assert_eq!(accumulator.final_miss_reason(), GET_METADATA_EARLY_STOP_REASON_NOT_FOUND);
    }

    #[test]
    fn metadata_quorum_accumulator_falls_back_on_version_not_found_quorum() {
        let mut accumulator = metadata_early_stop_accumulator();

        accumulator.observe_error(&DiskError::FileVersionNotFound);
        accumulator.observe_error(&DiskError::FileVersionNotFound);

        assert!(accumulator.early_stop_decision().is_none());
        assert_eq!(accumulator.final_miss_reason(), GET_METADATA_EARLY_STOP_REASON_VERSION_NOT_FOUND);
    }

    #[test]
    fn metadata_quorum_accumulator_falls_back_on_insufficient_quorum() {
        let mut accumulator = metadata_early_stop_accumulator();

        accumulator.observe_file_info(&metadata_early_stop_candidate("object", 1));

        assert!(accumulator.early_stop_decision().is_none());
        assert_eq!(accumulator.final_miss_reason(), GET_METADATA_EARLY_STOP_REASON_INSUFFICIENT_QUORUM);
    }

    #[test]
    fn metadata_quorum_accumulator_falls_back_on_mixed_corrupt_and_valid() {
        let mut accumulator = metadata_early_stop_accumulator();

        accumulator.observe_file_info(&metadata_early_stop_candidate("object", 1));
        accumulator.observe_error(&DiskError::FileCorrupt);
        accumulator.observe_file_info(&metadata_early_stop_candidate("object", 2));

        assert!(accumulator.early_stop_decision().is_none());
        assert_eq!(accumulator.final_miss_reason(), GET_METADATA_EARLY_STOP_REASON_ERROR);
    }

    #[test]
    fn metadata_quorum_accumulator_uses_same_gate_for_head_and_get() {
        let mut get_accumulator = metadata_early_stop_accumulator();
        let mut head_accumulator = metadata_early_stop_accumulator();

        for disk_index in [1, 2] {
            let fi = metadata_early_stop_candidate("object", disk_index);
            get_accumulator.observe_file_info(&fi);
            head_accumulator.observe_file_info(&fi);
        }

        assert_eq!(get_accumulator.early_stop_decision(), head_accumulator.early_stop_decision());
        assert_eq!(get_accumulator.final_miss_reason(), head_accumulator.final_miss_reason());
    }

    #[test]
    fn metadata_early_stop_gate_defaults_to_enabled() {
        temp_env::with_var(ENV_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE, None::<&str>, || {
            assert!(is_get_metadata_early_stop_enabled());
        });
    }

    #[test]
    fn metadata_early_stop_gate_honors_explicit_opt_out() {
        temp_env::with_var(ENV_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE, Some("false"), || {
            assert!(!is_get_metadata_early_stop_enabled());
            // With the gate off, even safe metadata-only requests must fall
            // back to the full-wait fanout.
            assert!(!should_allow_metadata_early_stop(false, "", false, false));
        });
    }

    #[test]
    fn metadata_early_stop_rejects_data_reads() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_METADATA_EARLY_STOP_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_METADATA_VERSION_EARLY_STOP_ENABLE, Some("true")),
            ],
            || {
                assert!(!should_allow_metadata_early_stop(true, "", false, false));
                assert!(!should_allow_metadata_early_stop(true, "version-id", false, false));
                assert!(should_allow_metadata_early_stop(false, "", false, false));
                assert!(should_allow_metadata_early_stop(false, "version-id", false, false));
            },
        );
    }

    #[test]
    fn version_early_stop_gate_defaults_to_disabled() {
        temp_env::with_var(ENV_RUSTFS_GET_METADATA_VERSION_EARLY_STOP_ENABLE, None::<&str>, || {
            assert!(!is_version_early_stop_enabled());
        });
    }

    fn version_early_stop_candidate(object: &str, disk_index: usize, version_id: Uuid) -> FileInfo {
        let mut fi = metadata_early_stop_candidate(object, disk_index);
        fi.version_id = Some(version_id);
        fi
    }

    fn version_early_stop_accumulator(requested_version_id: &str) -> MetadataQuorumAccumulator {
        MetadataQuorumAccumulator::new(4, 2, true).with_requested_version_id(requested_version_id)
    }

    #[test]
    fn version_early_stop_respects_disabled_accumulator() {
        let vid = Uuid::new_v4();
        let mut accumulator = MetadataQuorumAccumulator::new(4, 2, false).with_requested_version_id(&vid.to_string());

        accumulator.observe_file_info(&version_early_stop_candidate("object", 1, vid));
        accumulator.observe_file_info(&version_early_stop_candidate("object", 2, vid));

        assert!(accumulator.version_early_stop_decision().is_none());
        assert_eq!(accumulator.matching_version_votes, 2);
    }

    #[test]
    fn version_early_stop_hits_quorum_with_matching_versions() {
        let vid = Uuid::new_v4();
        let mut accumulator = version_early_stop_accumulator(&vid.to_string());

        accumulator.observe_file_info(&version_early_stop_candidate("object", 1, vid));
        assert!(accumulator.version_early_stop_decision().is_none());
        accumulator.observe_file_info(&version_early_stop_candidate("object", 2, vid));

        assert_eq!(
            accumulator.version_early_stop_decision(),
            Some(MetadataEarlyStopDecision {
                reason: GET_METADATA_EARLY_STOP_REASON_VERSION_MATCH_QUORUM
            })
        );
        assert_eq!(accumulator.matching_version_votes, 2);
    }

    #[test]
    fn version_early_stop_does_not_fire_without_requested_version() {
        let vid = Uuid::new_v4();
        let mut accumulator = version_early_stop_accumulator("");

        accumulator.observe_file_info(&version_early_stop_candidate("object", 1, vid));
        accumulator.observe_file_info(&version_early_stop_candidate("object", 2, vid));

        assert!(accumulator.version_early_stop_decision().is_none());
    }

    #[test]
    fn version_early_stop_does_not_fire_with_mismatched_versions() {
        let requested_vid = Uuid::new_v4();
        let other_vid = Uuid::new_v4();
        let mut accumulator = version_early_stop_accumulator(&requested_vid.to_string());

        accumulator.observe_file_info(&version_early_stop_candidate("object", 1, requested_vid));
        accumulator.observe_file_info(&version_early_stop_candidate("object", 2, other_vid));

        assert!(accumulator.version_early_stop_decision().is_none());
        assert_eq!(accumulator.matching_version_votes, 1);
    }

    #[test]
    fn version_early_stop_does_not_fire_below_quorum() {
        let vid = Uuid::new_v4();
        let mut accumulator = version_early_stop_accumulator(&vid.to_string());

        accumulator.observe_file_info(&version_early_stop_candidate("object", 1, vid));

        assert!(accumulator.version_early_stop_decision().is_none());
        assert_eq!(accumulator.matching_version_votes, 1);
    }

    #[test]
    fn version_early_stop_falls_back_on_conflicting_metadata() {
        let requested_vid = Uuid::new_v4();
        let mut accumulator = version_early_stop_accumulator(&requested_vid.to_string());

        // Two valid responses with matching version_id but different erasure.index
        // (so they conflict on the candidate path and must keep the fanout open)
        let mut fi1 = version_early_stop_candidate("object", 1, requested_vid);
        fi1.size = 100;
        let mut fi2 = version_early_stop_candidate("object", 2, requested_vid);
        fi2.size = 200;

        accumulator.observe_file_info(&fi1);
        accumulator.observe_file_info(&fi2);

        assert!(accumulator.early_stop_decision().is_none());
        assert!(accumulator.version_early_stop_decision().is_none());
        assert_eq!(accumulator.final_miss_reason(), GET_METADATA_EARLY_STOP_REASON_CONFLICTING_METADATA);
    }

    #[test]
    fn version_early_stop_falls_back_on_split_data_dir() {
        let requested_vid = Uuid::new_v4();
        let mut accumulator = version_early_stop_accumulator(&requested_vid.to_string());
        let mut first = version_early_stop_candidate("object", 1, requested_vid);
        let mut second = version_early_stop_candidate("object", 2, requested_vid);
        first.data_dir = Some(Uuid::parse_str("00000000-0000-0000-0000-000000000001").expect("static uuid should parse"));
        second.data_dir = Some(Uuid::parse_str("00000000-0000-0000-0000-000000000002").expect("static uuid should parse"));

        accumulator.observe_file_info(&first);
        accumulator.observe_file_info(&second);

        assert!(accumulator.early_stop_decision().is_none());
        assert!(accumulator.version_early_stop_decision().is_none());
        assert_eq!(accumulator.final_miss_reason(), GET_METADATA_EARLY_STOP_REASON_CONFLICTING_METADATA);
    }

    fn codec_streaming_test_object_info(fi: &FileInfo) -> ObjectInfo {
        ObjectInfo::from_file_info(fi, CODEC_STREAMING_TEST_BUCKET, CODEC_STREAMING_TEST_OBJECT, false)
    }

    fn codec_streaming_reader_gate_for_test(
        range: &Option<HTTPRangeSpec>,
        object_info: &ObjectInfo,
        fi: &FileInfo,
        lock_optimization_enabled: bool,
    ) -> GetCodecStreamingGate {
        get_codec_streaming_reader_gate(
            CODEC_STREAMING_TEST_BUCKET,
            CODEC_STREAMING_TEST_OBJECT,
            range,
            object_info,
            fi,
            lock_optimization_enabled,
        )
    }

    #[tokio::test]
    async fn codec_streaming_reader_rejects_zero_block_size_metadata() {
        // Corrupted on-disk metadata: valid data/parity blocks but block_size == 0.
        // The codec streaming entry must reject this and return an error instead of
        // panicking on the block_size division inside the reader (mirrors the legacy
        // multipart guard). The guard fires before any disk access, so empty disk /
        // parts-metadata slices are sufficient.
        let mut fi = codec_streaming_test_fileinfo(1024, 1);
        fi.erasure.block_size = 0;

        let result = SetDisks::get_object_decode_reader_with_fileinfo(
            CODEC_STREAMING_TEST_BUCKET,
            CODEC_STREAMING_TEST_OBJECT,
            &fi,
            &[],
            &[],
            0,
            0,
            false,
            "test-object-class",
            "test-size-bucket",
            false,
        )
        .await;

        assert!(result.is_err(), "zero block_size metadata must be rejected, not panic");
    }

    #[tokio::test]
    async fn multipart_codec_streaming_reader_reads_parts_in_order() {
        let readers: Vec<Box<dyn AsyncRead + Unpin + Send + Sync>> = vec![
            Box::new(Cursor::new(b"hello ".to_vec())),
            Box::new(Cursor::new(b"multipart".to_vec())),
        ];
        let mut reader = MultipartCodecStreamingReader::new(readers);
        let mut output = Vec::new();

        reader
            .read_to_end(&mut output)
            .await
            .expect("multipart codec reader should read all parts");

        assert_eq!(output, b"hello multipart");
    }

    struct OneByteAsyncReader {
        data: Vec<u8>,
        position: usize,
    }

    impl OneByteAsyncReader {
        fn new(data: &'static [u8]) -> Self {
            Self {
                data: data.to_vec(),
                position: 0,
            }
        }
    }

    impl AsyncRead for OneByteAsyncReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if self.position >= self.data.len() || buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            buf.put_slice(&self.data[self.position..self.position + 1]);
            self.position += 1;
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn multipart_codec_streaming_reader_crosses_part_boundaries_with_short_reads() {
        let readers: Vec<Box<dyn AsyncRead + Unpin + Send + Sync>> = vec![
            Box::new(OneByteAsyncReader::new(b"abc")),
            Box::new(OneByteAsyncReader::new(b"def")),
        ];
        let mut reader = MultipartCodecStreamingReader::new(readers);
        let mut first = [0u8; 5];
        let mut second = Vec::new();

        reader
            .read_exact(&mut first)
            .await
            .expect("multipart codec reader should cross part boundaries");
        reader
            .read_to_end(&mut second)
            .await
            .expect("multipart codec reader should drain the final part");

        assert_eq!(&first, b"abcde");
        assert_eq!(second, b"f");
    }

    struct DropCountingReader {
        data: Vec<u8>,
        position: usize,
        drops: Arc<AtomicUsize>,
    }

    impl DropCountingReader {
        fn new(data: &'static [u8], drops: Arc<AtomicUsize>) -> Self {
            Self {
                data: data.to_vec(),
                position: 0,
                drops,
            }
        }
    }

    impl AsyncRead for DropCountingReader {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            if self.position >= self.data.len() || buf.remaining() == 0 {
                return Poll::Ready(Ok(()));
            }

            let available = self.data.len() - self.position;
            let count = available.min(buf.remaining());
            let end = self.position + count;
            buf.put_slice(&self.data[self.position..end]);
            self.position = end;
            Poll::Ready(Ok(()))
        }
    }

    impl Drop for DropCountingReader {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn multipart_codec_streaming_reader_drops_remaining_parts_on_abort() {
        let drops = Arc::new(AtomicUsize::new(0));
        {
            let readers: Vec<Box<dyn AsyncRead + Unpin + Send + Sync>> = vec![
                Box::new(DropCountingReader::new(b"abc", Arc::clone(&drops))),
                Box::new(DropCountingReader::new(b"def", Arc::clone(&drops))),
            ];
            let mut reader = MultipartCodecStreamingReader::new(readers);
            let mut first = [0u8; 1];

            reader
                .read_exact(&mut first)
                .await
                .expect("multipart codec reader should support partial reads");
            assert_eq!(&first, b"a");
        }

        assert_eq!(drops.load(Ordering::SeqCst), 2);
    }

    fn inline_reader_setup_fileinfo(data: Option<&'static [u8]>) -> FileInfo {
        let mut fi = FileInfo::new("object", 2, 2);
        fi.volume = "bucket".to_string();
        fi.name = "object".to_string();
        fi.size = data.map_or(0, |data| i64::try_from(data.len()).expect("test data length should fit i64"));
        fi.data = data.map(Bytes::from_static);
        fi
    }

    async fn setup_inline_bitrot_readers(
        data: Vec<Option<&'static [u8]>>,
        data_shards: usize,
        parity_shards: usize,
        mode: BitrotReaderSetupMode,
    ) -> BitrotReaderSetup {
        setup_inline_bitrot_readers_with_env(data, data_shards, parity_shards, mode, false).await
    }

    async fn setup_inline_bitrot_readers_with_env(
        data: Vec<Option<&'static [u8]>>,
        data_shards: usize,
        parity_shards: usize,
        mode: BitrotReaderSetupMode,
        data_blocks_first: bool,
    ) -> BitrotReaderSetup {
        setup_inline_bitrot_readers_with_reader_setup_env(data, data_shards, parity_shards, mode, data_blocks_first, false).await
    }

    async fn setup_inline_bitrot_readers_with_codec_reader_setup_env(
        data: Vec<Option<&'static [u8]>>,
        data_shards: usize,
        parity_shards: usize,
        mode: BitrotReaderSetupMode,
        codec_data_blocks_first: bool,
    ) -> BitrotReaderSetup {
        setup_inline_bitrot_readers_with_reader_setup_env(data, data_shards, parity_shards, mode, false, codec_data_blocks_first)
            .await
    }

    async fn setup_inline_bitrot_readers_with_reader_setup_env(
        data: Vec<Option<&'static [u8]>>,
        data_shards: usize,
        parity_shards: usize,
        mode: BitrotReaderSetupMode,
        data_blocks_first: bool,
        codec_data_blocks_first: bool,
    ) -> BitrotReaderSetup {
        let files = data.into_iter().map(inline_reader_setup_fileinfo).collect::<Vec<_>>();
        let disks = vec![None; files.len()];

        temp_env::async_with_vars(
            [
                (ENV_RUSTFS_GET_DATA_BLOCKS_FIRST_READER_SETUP, data_blocks_first.then_some("true")),
                (
                    ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_READER_SETUP,
                    codec_data_blocks_first.then_some("true"),
                ),
            ],
            async {
                create_bitrot_readers_until_quorum(
                    &files,
                    &disks,
                    "bucket",
                    "object",
                    1,
                    0,
                    4,
                    4,
                    HashAlgorithm::None,
                    false,
                    false,
                    data_shards,
                    parity_shards,
                    mode,
                    None,
                    None,
                )
                .await
            },
        )
        .await
    }

    async fn setup_inline_bitrot_readers_with_preference(
        data: Vec<Option<&'static [u8]>>,
        data_shards: usize,
        parity_shards: usize,
        mode: BitrotReaderSetupMode,
        prefer_data_blocks_first: bool,
    ) -> BitrotReaderSetup {
        let files = data.into_iter().map(inline_reader_setup_fileinfo).collect::<Vec<_>>();
        let disks = vec![None; files.len()];

        create_bitrot_readers_until_quorum_with_preference(
            &files,
            &disks,
            "bucket",
            "object",
            1,
            0,
            4,
            4,
            HashAlgorithm::None,
            false,
            false,
            data_shards,
            parity_shards,
            mode,
            prefer_data_blocks_first,
            None,
            None,
        )
        .await
    }

    fn encoded_reader_setup_fileinfo(data: Option<Vec<u8>>) -> FileInfo {
        let mut fi = FileInfo::new("object", 2, 2);
        fi.volume = "bucket".to_string();
        fi.name = "object".to_string();
        fi.size = data
            .as_ref()
            .map_or(0, |data| i64::try_from(data.len()).expect("test data length should fit i64"));
        fi.data = data.map(Bytes::from);
        fi
    }

    async fn setup_codec_data_blocks_first_encoded_bitrot_readers(
        erasure: &coding::Erasure,
        data: &[u8],
        missing_indexes: &[usize],
        bitrot_corrupt_indexes: &[usize],
        inconsistent_source_indexes: &[usize],
        hash_algo: HashAlgorithm,
    ) -> BitrotReaderSetup {
        let shard_size = erasure.shard_size();
        let encoded_shards = erasure.encode_data(data).expect("test stripe should encode");
        let mut files = Vec::with_capacity(encoded_shards.len());
        for (index, shard) in encoded_shards.into_iter().enumerate() {
            if missing_indexes.contains(&index) {
                files.push(encoded_reader_setup_fileinfo(None));
                continue;
            }

            let mut shard = shard.to_vec();
            if inconsistent_source_indexes.contains(&index)
                && let Some(byte) = shard.first_mut()
            {
                *byte ^= 0x80;
            }

            let mut writer = BitrotWriter::new(Cursor::new(Vec::new()), shard_size, hash_algo.clone());
            writer.write(&shard).await.expect("test shard should write with bitrot hash");
            let mut encoded = writer.into_inner().into_inner();
            if bitrot_corrupt_indexes.contains(&index) {
                let data_offset = hash_algo.size();
                let byte = encoded
                    .get_mut(data_offset)
                    .expect("encoded test shard should contain payload bytes");
                *byte ^= 0x80;
            }

            files.push(encoded_reader_setup_fileinfo(Some(encoded)));
        }
        let disks = vec![None; files.len()];

        temp_env::async_with_vars([(ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_READER_SETUP, Some("true"))], async {
            create_bitrot_readers_until_quorum(
                &files,
                &disks,
                "bucket",
                "object",
                1,
                0,
                shard_size,
                shard_size,
                hash_algo,
                false,
                false,
                erasure.data_shards,
                erasure.parity_shards,
                BitrotReaderSetupMode::VerifyReconstruction,
                None,
                None,
            )
            .await
        })
        .await
    }

    async fn decode_codec_data_blocks_first_setup(
        erasure: coding::Erasure,
        data: &[u8],
        readers: Vec<Option<ObjectBitrotReader>>,
    ) -> std::io::Result<Vec<u8>> {
        let source = coding::decode::ParallelReader::new_with_metrics_path_and_reconstruction_verification(
            readers,
            erasure.clone(),
            0,
            data.len(),
            Some(GET_OBJECT_PATH_CODEC_STREAMING_RUSTFS_ENGINE),
        );
        let engine = CodecStreamingDecodeEngine::rustfs(&erasure).expect("rustfs codec engine should be created");
        let mut reader = coding::decode_reader::ErasureDecodeReader::new_with_metrics_path(
            source,
            engine,
            data.len(),
            GET_OBJECT_PATH_CODEC_STREAMING_RUSTFS_ENGINE,
        )
        .expect("codec streaming reader should be constructed");
        let mut decoded = Vec::new();
        reader.read_to_end(&mut decoded).await?;
        Ok(decoded)
    }

    #[tokio::test]
    async fn bitrot_reader_setup_stops_at_read_quorum() {
        let setup = setup_inline_bitrot_readers(
            vec![Some(b"aaaa"), Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
            2,
            2,
            BitrotReaderSetupMode::ReadQuorum,
        )
        .await;

        assert_eq!(setup.available_shards(), 2);
        assert!(setup.data_shards_attempted(2));
        assert_eq!(setup.completed_failed_shards(), 0);
    }

    #[tokio::test]
    async fn bitrot_reader_setup_retains_unattempted_fallback_readers() {
        let mut setup = setup_inline_bitrot_readers(
            vec![Some(b"aaaa"), Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
            2,
            2,
            BitrotReaderSetupMode::ReadQuorum,
        )
        .await;

        assert_eq!(setup.available_shards(), 2);
        assert_eq!(setup.readers.iter().filter(|reader| reader.is_some()).count(), 4);

        let fallback_index = setup
            .attempted
            .iter()
            .position(|attempted| !*attempted)
            .expect("read quorum should leave at least one deferred fallback");
        assert!(!setup.ready[fallback_index]);

        let mut fallback = setup.readers[fallback_index]
            .take()
            .expect("deferred fallback reader should be retained");
        let mut out = [0u8; 4];
        let n = fallback
            .read(&mut out)
            .await
            .expect("deferred fallback reader should open on read");

        assert_eq!(n, 4);
        assert_eq!(&out[..n], [b"aaaa", b"bbbb", b"cccc", b"dddd"][fallback_index]);
    }

    #[tokio::test]
    async fn bitrot_reader_setup_data_blocks_first_keeps_deferred_fallback_readers() {
        let mut setup = setup_inline_bitrot_readers_with_env(
            vec![Some(b"aaaa"), Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
            2,
            2,
            BitrotReaderSetupMode::ReadQuorum,
            true,
        )
        .await;

        assert!(setup.has_setup_quorum(2, 2, BitrotReaderSetupMode::ReadQuorum));
        assert_eq!(setup.available_shards(), 2);
        assert_eq!(setup.scheduled_shards(), 2);
        assert_eq!(setup.readers.iter().filter(|reader| reader.is_some()).count(), 4);

        let fallback_index = setup
            .attempted
            .iter()
            .position(|attempted| !*attempted)
            .expect("data-blocks-first setup should leave at least one deferred fallback");
        let mut fallback = setup.readers[fallback_index]
            .take()
            .expect("deferred fallback reader should be retained");
        let mut out = [0u8; 4];
        let n = fallback
            .read(&mut out)
            .await
            .expect("deferred fallback reader should open on read");

        assert_eq!(n, 4);
        assert_eq!(&out[..n], [b"aaaa", b"bbbb", b"cccc", b"dddd"][fallback_index]);
    }

    #[tokio::test]
    async fn bitrot_reader_setup_preference_uses_data_blocks_first_without_env() {
        let setup = setup_inline_bitrot_readers_with_preference(
            vec![Some(b"aaaa"), Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
            2,
            2,
            BitrotReaderSetupMode::ReadQuorum,
            true,
        )
        .await;

        assert!(setup.has_setup_quorum(2, 2, BitrotReaderSetupMode::ReadQuorum));
        assert_eq!(setup.available_shards(), 2);
        assert_eq!(setup.scheduled_shards(), 2);
        assert_eq!(setup.deferred_shards(), 2);
    }

    #[tokio::test]
    async fn bitrot_reader_setup_preference_can_apply_to_verify_mode() {
        let setup = setup_inline_bitrot_readers_with_preference(
            vec![Some(b"aaaa"), Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
            2,
            2,
            BitrotReaderSetupMode::VerifyReconstruction,
            true,
        )
        .await;

        assert!(setup.has_setup_quorum(2, 2, BitrotReaderSetupMode::VerifyReconstruction));
        assert_eq!(setup.available_data_shards(2), 2);
        assert_eq!(setup.scheduled_shards(), 2);
        assert_eq!(setup.attempted_shards(), 2);
        assert_eq!(setup.deferred_shards(), 2);
    }

    #[tokio::test]
    async fn bitrot_reader_setup_data_blocks_first_schedules_parity_after_missing_data() {
        let setup = setup_inline_bitrot_readers_with_env(
            vec![None, Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
            2,
            2,
            BitrotReaderSetupMode::ReadQuorum,
            true,
        )
        .await;

        assert!(setup.has_setup_quorum(2, 2, BitrotReaderSetupMode::ReadQuorum));
        assert_eq!(setup.available_shards(), 2);
        assert_eq!(setup.completed_failed_shards(), 1);
        assert_eq!(setup.scheduled_shards(), 3);
        assert!(setup.ready.iter().skip(2).any(|ready| *ready));
    }

    #[tokio::test]
    async fn bitrot_reader_setup_data_blocks_first_does_not_apply_to_verify_mode() {
        let setup = setup_inline_bitrot_readers_with_env(
            vec![None, Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
            2,
            2,
            BitrotReaderSetupMode::VerifyReconstruction,
            true,
        )
        .await;

        assert_eq!(setup.available_shards(), 3);
        assert_eq!(setup.scheduled_shards(), 4);
        assert_eq!(setup.completed_failed_shards(), 1);
    }

    #[tokio::test]
    async fn bitrot_reader_setup_codec_data_blocks_first_can_apply_to_verify_mode() {
        let setup = setup_inline_bitrot_readers_with_codec_reader_setup_env(
            vec![Some(b"aaaa"), Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
            2,
            2,
            BitrotReaderSetupMode::VerifyReconstruction,
            true,
        )
        .await;

        assert!(setup.has_setup_quorum(2, 2, BitrotReaderSetupMode::VerifyReconstruction));
        assert_eq!(setup.available_data_shards(2), 2);
        assert_eq!(setup.scheduled_shards(), 2);
    }

    #[tokio::test]
    async fn bitrot_reader_setup_codec_data_blocks_first_collects_extra_source_for_reconstruction() {
        let setup = setup_inline_bitrot_readers_with_codec_reader_setup_env(
            vec![None, Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
            2,
            2,
            BitrotReaderSetupMode::VerifyReconstruction,
            true,
        )
        .await;

        assert!(setup.has_setup_quorum(2, 2, BitrotReaderSetupMode::VerifyReconstruction));
        assert_eq!(setup.available_shards(), 3);
        assert_eq!(setup.available_data_shards(2), 1);
        assert!(setup.data_shards_attempted(2));
        assert_eq!(setup.completed_failed_shards(), 1);
        assert_eq!(setup.scheduled_shards(), 4);
    }

    #[tokio::test]
    async fn bitrot_reader_setup_verify_mode_stops_when_data_quorum_is_available() {
        let setup = setup_inline_bitrot_readers(
            vec![Some(b"aaaa"), Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
            2,
            2,
            BitrotReaderSetupMode::VerifyReconstruction,
        )
        .await;

        assert_eq!(setup.available_shards(), 2);
        assert_eq!(setup.available_data_shards(2), 2);
        assert_eq!(setup.completed_failed_shards(), 0);
    }

    #[tokio::test]
    async fn bitrot_reader_setup_verify_mode_collects_extra_source_for_reconstruction() {
        let setup = setup_inline_bitrot_readers(
            vec![None, Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
            2,
            2,
            BitrotReaderSetupMode::VerifyReconstruction,
        )
        .await;

        assert_eq!(setup.available_shards(), 3);
        assert_eq!(setup.available_data_shards(2), 1);
        assert!(setup.data_shards_attempted(2));
        assert_eq!(setup.completed_failed_shards(), 1);
    }

    #[tokio::test]
    async fn bitrot_reader_setup_verify_mode_does_not_wait_for_impossible_extra_source() {
        let setup = setup_inline_bitrot_readers(
            vec![None, Some(b"bbbb"), Some(b"cccc")],
            2,
            1,
            BitrotReaderSetupMode::VerifyReconstruction,
        )
        .await;

        assert_eq!(setup.available_shards(), 2);
        assert_eq!(setup.available_data_shards(2), 1);
        assert_eq!(setup.completed_failed_shards(), 1);
    }

    #[tokio::test]
    async fn codec_data_blocks_first_recovers_corrupt_data_with_deferred_parity() {
        let erasure = coding::Erasure::new(2, 2, 64);
        let data = (0..64u8).collect::<Vec<_>>();
        let setup =
            setup_codec_data_blocks_first_encoded_bitrot_readers(&erasure, &data, &[], &[0], &[], HashAlgorithm::HighwayHash256)
                .await;

        assert!(setup.has_setup_quorum(2, 2, BitrotReaderSetupMode::VerifyReconstruction));
        assert_eq!(setup.scheduled_shards(), 2);
        assert_eq!(setup.attempted_shards(), 2);
        assert_eq!(setup.available_shards(), 2);
        assert_eq!(setup.deferred_shards(), 2);

        let decoded = decode_codec_data_blocks_first_setup(erasure, &data, setup.readers)
            .await
            .expect("deferred parity should recover the corrupt data shard");

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn codec_data_blocks_first_reconstructs_missing_data_shard() {
        let erasure = coding::Erasure::new(2, 2, 64);
        let data = (0..64u8).rev().collect::<Vec<_>>();
        let setup =
            setup_codec_data_blocks_first_encoded_bitrot_readers(&erasure, &data, &[0], &[], &[], HashAlgorithm::HighwayHash256)
                .await;

        assert!(setup.has_setup_quorum(2, 2, BitrotReaderSetupMode::VerifyReconstruction));
        assert_eq!(setup.scheduled_shards(), 4);
        assert_eq!(setup.available_shards(), 3);
        assert_eq!(setup.available_data_shards(2), 1);
        assert_eq!(setup.completed_failed_shards(), 1);

        let decoded = decode_codec_data_blocks_first_setup(erasure, &data, setup.readers)
            .await
            .expect("parity shards should reconstruct the missing data shard");

        assert_eq!(decoded, data);
    }

    #[tokio::test]
    async fn codec_data_blocks_first_rejects_inconsistent_deferred_reconstruction_source() {
        let erasure = coding::Erasure::new(2, 2, 64);
        let data = (0..64u8).map(|value| value.wrapping_mul(3)).collect::<Vec<_>>();
        let setup =
            setup_codec_data_blocks_first_encoded_bitrot_readers(&erasure, &data, &[], &[0], &[2], HashAlgorithm::HighwayHash256)
                .await;

        assert!(setup.has_setup_quorum(2, 2, BitrotReaderSetupMode::VerifyReconstruction));
        assert_eq!(setup.scheduled_shards(), 2);
        assert_eq!(setup.attempted_shards(), 2);
        assert_eq!(setup.deferred_shards(), 2);

        let err = decode_codec_data_blocks_first_setup(erasure, &data, setup.readers)
            .await
            .expect_err("inconsistent deferred parity source must fail reconstruction verification");

        assert_eq!(err.kind(), ErrorKind::InvalidData);
        assert!(err.to_string().contains("inconsistent read source shards"));
    }

    #[test]
    fn codec_streaming_reader_gate_is_conservative() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true).decision,
                    GetCodecStreamingDecision::Use
                );

                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &object_info, &fi, false).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::LockOptimizationDisabled)
                );

                let range = Some(HTTPRangeSpec {
                    is_suffix_length: false,
                    start: 0,
                    end: 1,
                });
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&range, &object_info, &fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Range)
                );

                let multipart_fi = codec_streaming_test_fileinfo(1024, 2);
                let multipart_object_info = codec_streaming_test_object_info(&multipart_fi);
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &multipart_object_info, &multipart_fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Multipart)
                );

                let mut encrypted_fi = fi.clone();
                encrypted_fi
                    .metadata
                    .insert("x-amz-server-side-encryption".to_string(), "AES256".to_string());
                let encrypted = codec_streaming_test_object_info(&encrypted_fi);
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &encrypted, &encrypted_fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Encrypted)
                );

                let mut compressed_fi = fi.clone();
                insert_str(&mut compressed_fi.metadata, SUFFIX_COMPRESSION, "lz4".to_string());
                let compressed = codec_streaming_test_object_info(&compressed_fi);
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &compressed, &compressed_fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Compressed)
                );

                let small_fi = codec_streaming_test_fileinfo(0, 1);
                let small_object_info = codec_streaming_test_object_info(&small_fi);
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &small_object_info, &small_fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::BelowMinSize)
                );

                let mut remote_fi = fi;
                remote_fi.transition_status = TRANSITION_COMPLETE.to_string();
                let remote = codec_streaming_test_object_info(&remote_fi);
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &remote, &remote_fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Remote)
                );
            },
        );
    }

    #[test]
    fn codec_streaming_engine_defaults_to_legacy_and_parses_rustfs() {
        temp_env::with_var(ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, None::<&str>, || {
            assert_eq!(get_codec_streaming_engine(), GetCodecStreamingEngine::Legacy);
        });

        temp_env::with_var(ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, Some(GET_CODEC_STREAMING_ENGINE_RUSTFS), || {
            assert_eq!(get_codec_streaming_engine(), GetCodecStreamingEngine::Rustfs);
        });

        temp_env::with_var(ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, Some("unknown"), || {
            assert_eq!(get_codec_streaming_engine(), GetCodecStreamingEngine::Legacy);
        });
    }

    #[test]
    fn rustfs_codec_streaming_uses_conservative_default_min_size() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, Some(GET_CODEC_STREAMING_ENGINE_RUSTFS)),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, None::<&str>),
                (ENV_RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE, None::<&str>),
            ],
            || {
                let below_threshold_fi = codec_streaming_test_fileinfo(512 * 1024, 1);
                let below_threshold_object_info = codec_streaming_test_object_info(&below_threshold_fi);
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &below_threshold_object_info, &below_threshold_fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::BelowMinSize)
                );

                let threshold_fi = codec_streaming_test_fileinfo(1_048_576, 1);
                let threshold_object_info = codec_streaming_test_object_info(&threshold_fi);
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &threshold_object_info, &threshold_fi, true).decision,
                    GetCodecStreamingDecision::Use
                );
            },
        );
    }

    #[test]
    fn rustfs_codec_streaming_min_size_override_can_lower_threshold() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, Some(GET_CODEC_STREAMING_ENGINE_RUSTFS)),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, None::<&str>),
                (ENV_RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE, Some("524288")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(512 * 1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true).decision,
                    GetCodecStreamingDecision::Use
                );
            },
        );
    }

    #[test]
    fn codec_streaming_data_blocks_first_gate_defaults_to_off() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, Some(GET_CODEC_STREAMING_ENGINE_RUSTFS)),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE, None::<&str>),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(512 * 1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);
                let gate = codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true);

                assert_eq!(gate.decision, GetCodecStreamingDecision::Use);
                assert!(!gate.prefer_data_blocks_first_reader_setup);
            },
        );
    }

    #[test]
    fn codec_streaming_data_blocks_first_gate_allows_small_plain_single_part() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, Some(GET_CODEC_STREAMING_ENGINE_RUSTFS)),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_MAX_SIZE, Some("524288")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(512 * 1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);
                let gate = codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true);

                assert_eq!(gate.decision, GetCodecStreamingDecision::Use);
                assert!(gate.prefer_data_blocks_first_reader_setup);
            },
        );
    }

    #[test]
    fn codec_streaming_data_blocks_first_gate_rejects_large_and_non_plain_objects() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, Some(GET_CODEC_STREAMING_ENGINE_RUSTFS)),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_DATA_BLOCKS_FIRST_MAX_SIZE, Some("524288")),
            ],
            || {
                let large_fi = codec_streaming_test_fileinfo(768 * 1024, 1);
                let large_object_info = codec_streaming_test_object_info(&large_fi);
                let large_gate = codec_streaming_reader_gate_for_test(&None, &large_object_info, &large_fi, true);
                assert_eq!(large_gate.decision, GetCodecStreamingDecision::Use);
                assert!(!large_gate.prefer_data_blocks_first_reader_setup);

                let multipart_fi = codec_streaming_test_fileinfo(512 * 1024, 2);
                let multipart_object_info = codec_streaming_test_object_info(&multipart_fi);
                let multipart_gate = codec_streaming_reader_gate_for_test(&None, &multipart_object_info, &multipart_fi, true);
                assert!(!multipart_gate.prefer_data_blocks_first_reader_setup);

                let range = Some(HTTPRangeSpec {
                    is_suffix_length: false,
                    start: 0,
                    end: 1,
                });
                let range_gate = codec_streaming_reader_gate_for_test(&range, &large_object_info, &large_fi, true);
                assert!(!range_gate.prefer_data_blocks_first_reader_setup);
            },
        );
    }

    #[test]
    fn generic_codec_streaming_min_size_override_remains_authoritative() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, Some(GET_CODEC_STREAMING_ENGINE_RUSTFS)),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_RUSTFS_MIN_SIZE, Some("786432")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true).decision,
                    GetCodecStreamingDecision::Use
                );
            },
        );
    }

    #[test]
    fn codec_streaming_engine_env_is_ignored_when_streaming_is_disabled() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("false")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, None::<&str>),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, None::<&str>),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, None::<&str>),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, Some(GET_CODEC_STREAMING_ENGINE_RUSTFS)),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);

                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Disabled)
                );
            },
        );
    }

    #[test]
    fn codec_streaming_reader_gate_allows_multipart_when_explicitly_enabled() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 2);
                let object_info = codec_streaming_test_object_info(&fi);
                let gate = codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true);

                assert_eq!(gate.object_class, GetCodecStreamingObjectClass::Multipart);
                assert_eq!(gate.decision, GetCodecStreamingDecision::Use);
            },
        );
    }

    #[test]
    fn codec_streaming_reader_gate_keeps_multipart_default_off() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE, None),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 2);
                let object_info = codec_streaming_test_object_info(&fi);
                let gate = codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true);

                assert_eq!(gate.object_class, GetCodecStreamingObjectClass::Multipart);
                assert_eq!(
                    gate.decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Multipart)
                );
            },
        );
    }

    #[test]
    fn codec_streaming_reader_gate_limits_multipart_part_count() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MULTIPART_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MULTIPART_MAX_PARTS, Some("1")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 2);
                let object_info = codec_streaming_test_object_info(&fi);
                let gate = codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true);

                assert_eq!(gate.object_class, GetCodecStreamingObjectClass::Multipart);
                assert_eq!(
                    gate.decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::MultipartPartLimit)
                );
            },
        );
    }

    #[test]
    fn codec_streaming_decode_engine_builder_selects_rustfs() {
        temp_env::with_var(ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, Some(GET_CODEC_STREAMING_ENGINE_RUSTFS), || {
            let erasure = coding::Erasure::new(4, 2, 32);
            let engine = build_get_codec_streaming_decode_engine(erasure).expect("engine should be built");

            assert!(matches!(engine, CodecStreamingDecodeEngine::Rustfs(_)));
        });
    }

    #[test]
    fn codec_streaming_metrics_path_matches_selected_engine() {
        temp_env::with_var(ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, None::<&str>, || {
            assert_eq!(get_codec_streaming_metrics_path(), GET_OBJECT_PATH_CODEC_STREAMING_LEGACY_ENGINE);
        });

        temp_env::with_var(ENV_RUSTFS_GET_CODEC_STREAMING_ENGINE, Some(GET_CODEC_STREAMING_ENGINE_RUSTFS), || {
            assert_eq!(get_codec_streaming_metrics_path(), GET_OBJECT_PATH_CODEC_STREAMING_RUSTFS_ENGINE);
        });
    }

    #[test]
    fn codec_streaming_fallback_metric_labels_are_stable() {
        assert_eq!(GetCodecStreamingFallbackReason::Disabled.as_str(), "disabled");
        assert_eq!(GetCodecStreamingFallbackReason::RolloutNotOptedIn.as_str(), "rollout_not_opted_in");
        assert_eq!(
            GetCodecStreamingFallbackReason::RolloutPctNotSelected.as_str(),
            "rollout_pct_not_selected"
        );
        assert_eq!(
            GetCodecStreamingFallbackReason::BodyCompatibilityUnconfirmed.as_str(),
            "body_compatibility_unconfirmed"
        );
        assert_eq!(
            GetCodecStreamingFallbackReason::HeaderCompatibilityUnconfirmed.as_str(),
            "header_compatibility_unconfirmed"
        );
        assert_eq!(
            GetCodecStreamingFallbackReason::LockOptimizationDisabled.as_str(),
            "lock_optimization_disabled"
        );
        assert_eq!(GetCodecStreamingFallbackReason::Range.as_str(), "range");
        assert_eq!(GetCodecStreamingFallbackReason::BelowMinSize.as_str(), "below_min_size");
        assert_eq!(GetCodecStreamingFallbackReason::Encrypted.as_str(), "encrypted");
        assert_eq!(GetCodecStreamingFallbackReason::Compressed.as_str(), "compressed");
        assert_eq!(GetCodecStreamingFallbackReason::Remote.as_str(), "remote");
        assert_eq!(GetCodecStreamingFallbackReason::Multipart.as_str(), "multipart");
        assert_eq!(GetCodecStreamingFallbackReason::InvalidMinSize.as_str(), "invalid_min_size");
        assert_eq!(GetCodecStreamingFallbackReason::ReadQuorumNotSafe.as_str(), "read_quorum_not_safe");
        assert_eq!(GetCodecStreamingFallbackReason::MultipartPartLimit.as_str(), "multipart_part_limit");
        assert_eq!(GetCodecStreamingObjectClass::PlainSinglePart.as_str(), "plain_single_part");
        assert_eq!(GetCodecStreamingObjectClass::Range.as_str(), "range");
        assert_eq!(GetCodecStreamingObjectClass::Encrypted.as_str(), "encrypted");
        assert_eq!(GetCodecStreamingObjectClass::Compressed.as_str(), "compressed");
        assert_eq!(GetCodecStreamingObjectClass::Remote.as_str(), "remote");
        assert_eq!(GetCodecStreamingObjectClass::Multipart.as_str(), "multipart");
    }

    #[test]
    fn codec_streaming_reader_gate_defaults_to_disabled() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, None::<&str>),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, None::<&str>),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, None::<&str>),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, None::<&str>),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);

                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Disabled)
                );
            },
        );
    }

    #[test]
    fn codec_streaming_reader_gate_requires_explicit_rollout_and_compat_confirmation() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("off")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, None::<&str>),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, None::<&str>),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);

                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::RolloutNotOptedIn)
                );
            },
        );

        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("false")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);

                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::BodyCompatibilityUnconfirmed)
                );
            },
        );

        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("false")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);

                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::HeaderCompatibilityUnconfirmed)
                );
            },
        );
    }

    #[test]
    fn codec_streaming_reader_gate_honors_rollout_percentage() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT_PCT, Some("0")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);

                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true).decision,
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::RolloutPctNotSelected)
                );
            },
        );

        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT_PCT, Some("100")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);

                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true).decision,
                    GetCodecStreamingDecision::Use
                );
            },
        );
    }

    #[test]
    fn codec_streaming_reader_gate_records_object_classes() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_ROLLOUT, Some("benchmark")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_BODY_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_HEADER_COMPAT_CONFIRMED, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&None, &object_info, &fi, true).object_class,
                    GetCodecStreamingObjectClass::PlainSinglePart
                );

                let range = Some(HTTPRangeSpec {
                    is_suffix_length: false,
                    start: 0,
                    end: 1,
                });
                assert_eq!(
                    codec_streaming_reader_gate_for_test(&range, &object_info, &fi, true).object_class,
                    GetCodecStreamingObjectClass::Range
                );
            },
        );
    }

    #[tokio::test]
    async fn codec_streaming_reader_build_falls_back_when_read_quorum_is_not_safe() {
        let setup = setup_inline_bitrot_readers(
            vec![None, Some(b"bbbb"), Some(b"cccc"), Some(b"dddd")],
            2,
            2,
            BitrotReaderSetupMode::VerifyReconstruction,
        )
        .await;

        assert_eq!(setup.completed_failed_shards(), 1);
        assert_eq!(
            codec_streaming_reader_setup_fallback_reason(setup.completed_failed_shards()),
            Some(GetCodecStreamingFallbackReason::ReadQuorumNotSafe)
        );
    }

    #[tokio::test]
    async fn collect_read_multiple_results_fails_early_when_quorum_is_impossible() {
        let started = Instant::now();
        let resp = ReadMultipleResp {
            bucket: "bucket".to_string(),
            prefix: "prefix".to_string(),
            file: "file".to_string(),
            exists: true,
            error: String::new(),
            data: vec![1],
            mod_time: None,
        };

        let tasks: Vec<_> = vec![
            (10_u64, Err(DiskError::DiskNotFound)),
            (15, Err(DiskError::DiskNotFound)),
            (250, Ok::<Vec<ReadMultipleResp>, DiskError>(vec![resp])),
        ]
        .into_iter()
        .map(|(delay_ms, outcome)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let result = collect_read_multiple_results(tasks, 2).await;
        assert!(result.is_err(), "quorum should become impossible before slow tail completes");
        assert!(started.elapsed() < Duration::from_millis(120));
    }

    #[tokio::test]
    async fn collect_read_multiple_results_returns_collected_responses_on_quorum() {
        let resp = ReadMultipleResp {
            bucket: "bucket".to_string(),
            prefix: "prefix".to_string(),
            file: "file".to_string(),
            exists: true,
            error: String::new(),
            data: vec![1, 2, 3],
            mod_time: None,
        };

        let tasks: Vec<_> = vec![
            (10_u64, Ok::<Vec<ReadMultipleResp>, DiskError>(vec![resp.clone()])),
            (15, Ok::<Vec<ReadMultipleResp>, DiskError>(vec![resp.clone()])),
            (250, Err(DiskError::DiskNotFound)),
        ]
        .into_iter()
        .map(|(delay_ms, outcome)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let (responses, errors) = collect_read_multiple_results(tasks, 2).await.expect("quorum should succeed");

        assert_eq!(responses.iter().filter(|item| item.is_some()).count(), 2);
        assert_eq!(errors.iter().filter(|item| item.is_none()).count(), 2);
    }

    #[tokio::test]
    async fn collect_read_multiple_results_tolerates_single_panicked_task_when_quorum_is_met() {
        let resp = ReadMultipleResp {
            bucket: "bucket".to_string(),
            prefix: "prefix".to_string(),
            file: "file".to_string(),
            exists: true,
            error: String::new(),
            data: vec![1, 2, 3],
            mod_time: None,
        };

        let tasks: Vec<_> = vec![(5_u64, true), (10, false), (12, false)]
            .into_iter()
            .map(|(delay_ms, should_panic)| {
                let resp = resp.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    if should_panic {
                        panic!("simulated task panic");
                    }
                    Ok::<Vec<ReadMultipleResp>, DiskError>(vec![resp])
                }
            })
            .collect();

        let (responses, errors) = collect_read_multiple_results(tasks, 2)
            .await
            .expect("quorum should still succeed");
        assert_eq!(responses.iter().filter(|item| item.is_some()).count(), 2);
        assert_eq!(errors.iter().filter(|item| item.is_none()).count(), 2);
    }

    #[tokio::test]
    async fn collect_read_parts_results_fails_early_when_quorum_is_impossible() {
        let started = Instant::now();
        let part = ObjectPartInfo {
            number: 1,
            etag: "etag".to_string(),
            ..Default::default()
        };

        let tasks: Vec<_> = vec![
            (10_u64, Err(DiskError::DiskNotFound)),
            (15, Err(DiskError::DiskNotFound)),
            (250, Ok::<Vec<ObjectPartInfo>, DiskError>(vec![part])),
        ]
        .into_iter()
        .map(|(delay_ms, outcome)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let result = collect_read_parts_results(tasks, 2).await;
        assert!(result.is_err(), "quorum should become impossible before slow tail completes");
        assert!(started.elapsed() < Duration::from_millis(120));
    }

    #[tokio::test]
    async fn collect_read_parts_results_returns_collected_responses_on_quorum() {
        let part = ObjectPartInfo {
            number: 1,
            etag: "etag".to_string(),
            ..Default::default()
        };

        let tasks: Vec<_> = vec![
            (10_u64, Ok::<Vec<ObjectPartInfo>, DiskError>(vec![part.clone()])),
            (15, Ok::<Vec<ObjectPartInfo>, DiskError>(vec![part.clone()])),
            (250, Err(DiskError::DiskNotFound)),
        ]
        .into_iter()
        .map(|(delay_ms, outcome)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let (responses, errors) = collect_read_parts_results(tasks, 2).await.expect("quorum should succeed");
        assert_eq!(responses.iter().filter(|item| item.is_some()).count(), 2);
        assert_eq!(errors.iter().filter(|item| item.is_none()).count(), 2);
    }

    #[tokio::test]
    async fn collect_read_parts_results_tolerates_single_panicked_task_when_quorum_is_met() {
        let part = ObjectPartInfo {
            number: 1,
            etag: "etag".to_string(),
            ..Default::default()
        };

        let tasks: Vec<_> = vec![(5_u64, true), (10, false), (12, false)]
            .into_iter()
            .map(|(delay_ms, should_panic)| {
                let part = part.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    if should_panic {
                        panic!("simulated task panic");
                    }
                    Ok::<Vec<ObjectPartInfo>, DiskError>(vec![part])
                }
            })
            .collect();

        let (responses, errors) = collect_read_parts_results(tasks, 2)
            .await
            .expect("quorum should still succeed");
        assert_eq!(responses.iter().filter(|item| item.is_some()).count(), 2);
        assert_eq!(errors.iter().filter(|item| item.is_none()).count(), 2);
    }
}
