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

//! `ObjectIO` and `ObjectOperations` storage-api contract impls for `SetDisks`
//! — the core object read/write hot path (P6 of the God-Object split, tracking
//! backlog#815, issue backlog#821). Relocated verbatim from set_disk/mod.rs;
//! the contracts stay implemented `for SetDisks`, so their associated-type
//! bounds are unchanged, and the impls reach shared primitives through the
//! SetDisks core (io_primitives) via inherent calls.

use super::super::*;

use crate::bucket::lifecycle::tier_sweeper::{RemoteTierDeleteOutcome, delete_object_from_remote_tier_with_lease_idempotent};
use crate::disk::OldCurrentSize;
use crate::object_api::{GetObjectBodySource, get_object_body_cache_hook_suppressed};
use crate::services::tier::tier::{TierConfigMgr, TierOperationLease};
use futures::FutureExt as _;
use std::future::Future;

fn erasure_from_file_info(fi: &FileInfo, uses_legacy: bool) -> Result<coding::Erasure> {
    coding::Erasure::try_new_with_options(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size, uses_legacy)
        .map_err(Error::from)
}

/// Length of the full plaintext body when — and only when — this read's output
/// is exactly the object's complete plaintext, so the app-layer body cache may
/// serve it in place of the erasure read.
///
/// A hook hit bypasses `ReadPlan`/`ReadTransform` entirely, so it is sound only
/// where the normal read path would produce that same plaintext byte-for-byte
/// AND expose the same `object_info.size`. This is a fail-closed allow-list, not
/// a deny-list: every read whose `ReadPlan` applies some other transform returns
/// `None`, so a newly added `ReadPlan` branch bypasses the cache by default
/// instead of silently serving bytes in the wrong representation.
///
/// Refused reads, each mapping to a `ReadPlan::build` branch:
/// - ranged / part-number reads — the cache only holds whole objects;
/// - `raw_data_movement_read` / `data_movement` — yields the STORED
///   representation, e.g. compressed bytes (backlog#1108);
/// - restore reads — `restore_request_active` forces the `Plain` branch, so a
///   compressed object yields STORED bytes under its compressed `size`;
/// - encrypted objects — `ReadTransform::Encrypted` rewrites size and the cache
///   must never hold their plaintext;
/// - remote (transitioned) objects — served from the warm tier.
///
/// Compressed objects ARE eligible: `ReadTransform::Compressed` returns the full
/// plaintext and rewrites `object_info.size` to the decompressed length, which
/// the caller must replicate with the returned length (backlog#1109).
///
/// The fail-closed shape here is enforced by `scripts/check_body_cache_whitelist.sh`
/// (backlog#1146): the guard requires every exclusion predicate and a `return
/// None` to precede the first `Some(..)`, so a refactor to a deny-list fails CI.
/// If you rename or move this function, update that script.
fn full_object_plaintext_len(range: &Option<HTTPRangeSpec>, opts: &ObjectOptions, object_info: &ObjectInfo) -> Option<i64> {
    if range.is_some()
        || opts.part_number.is_some()
        || opts.raw_data_movement_read
        || opts.data_movement
        || crate::object_api::restore_request_active(opts)
        || object_info.is_encrypted()
        || object_info.is_remote()
        || object_info.delete_marker
        || object_info.size == 0
        || object_info.version_only
        || object_info.metadata_only
        || object_info.is_inline_fast_path_eligible()
    {
        return None;
    }

    if object_info.is_compressed() {
        return object_info.get_actual_size().ok();
    }

    Some(object_info.size)
}

pub(crate) fn body_cache_plaintext_len(
    range: &Option<HTTPRangeSpec>,
    opts: &ObjectOptions,
    object_info: &ObjectInfo,
) -> Option<i64> {
    full_object_plaintext_len(range, opts, object_info)
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::object::ObjectIO for SetDisks {
    type Error = Error;
    type RangeSpec = HTTPRangeSpec;
    type HeaderMap = HeaderMap;
    type ObjectOptions = ObjectOptions;
    type ObjectInfo = ObjectInfo;
    type GetObjectReader = GetObjectReader;
    type PutObjectReader = PutObjReader;

    #[tracing::instrument(level = "debug", skip(self))]
    async fn get_object_reader(
        &self,
        bucket: &str,
        object: &str,
        range: Option<HTTPRangeSpec>,
        h: HeaderMap,
        opts: &ObjectOptions,
    ) -> Result<GetObjectReader> {
        crate::hp_guard!("SetDisks::get_object_reader");
        let stage_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        // Check if lock optimization is enabled for reads that are fully materialized in memory.
        let lock_optimization_enabled = is_lock_optimization_enabled();

        // Acquire a shared read-lock early to protect read consistency
        let mut read_lock_guard = if !opts.no_lock {
            let acquire_start = stage_metrics_enabled.then(Instant::now);
            let lock_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);

            // Record lock wait for deadlock detection
            if is_deadlock_detection_enabled() {
                debug!(
                    lock_id = format!("{}:{}", bucket, object),
                    lock_type = "read",
                    resource = format!("{}/{}", bucket, object),
                    "Waiting for read lock"
                );
            }

            let guard = self.acquire_read_lock_diag("get_object", bucket, object).await?;

            // Record lock acquisition for deadlock detection
            let _lock_id = record_lock_acquire(bucket, object, "read");

            // Record lock statistics only when GET stage metrics are enabled,
            // matching the adjacent stage timer. Avoids a per-GET clock read and
            // two global-recorder lookups when observability/stage metrics are off.
            if let Some(acquire_start) = acquire_start {
                metrics::counter!("rustfs.lock.acquire.total", "type" => "read").increment(1);
                metrics::histogram!("rustfs.lock.acquire.duration.seconds").record(acquire_start.elapsed().as_secs_f64());
            }
            record_get_stage_duration_if_enabled(GET_OBJECT_PATH_SET_DISK, GET_STAGE_LOCK_ACQUIRE, lock_stage_start);

            Some(guard)
        } else {
            None
        };

        let metadata_stage_start = Instant::now();
        let (fi, files, disks, prepared_object_info) = if let Some(prepared) = take_prepared_get_object_metadata() {
            (prepared.fi, prepared.files, prepared.disks, prepared.object_info)
        } else {
            match self.get_object_fileinfo(bucket, object, opts, true, true).await {
                Ok((fi, files, disks)) => (fi, files, disks, None),
                Err(err) => {
                    rustfs_io_metrics::record_get_object_metadata_phase_duration(metadata_stage_start.elapsed().as_secs_f64());
                    record_get_object_pipeline_failure(GET_STAGE_METADATA, classify_storage_error(&err));
                    return Err(to_object_err(err, vec![bucket, object]));
                }
            }
        };
        let object_info_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let object_info = prepared_object_info
            .unwrap_or_else(|| build_get_object_info(&fi, bucket, object, opts.versioned || opts.version_suspended));
        let object_class = classify_get_codec_streaming_object_class(&range, &object_info, &fi);
        let size_bucket = rustfs_io_metrics::get_object_size_bucket(object_info.size);
        record_get_stage_duration_if_enabled(GET_OBJECT_PATH_SET_DISK, GET_STAGE_OBJECT_INFO, object_info_stage_start);
        let metadata_elapsed = metadata_stage_start.elapsed().as_secs_f64();
        rustfs_io_metrics::record_get_object_metadata_phase_duration(metadata_elapsed);
        rustfs_io_metrics::record_get_object_stage_duration_by_size(
            GET_OBJECT_PATH_SET_DISK,
            GET_STAGE_METADATA,
            object_class.as_str(),
            size_bucket,
            metadata_elapsed,
        );

        if object_info.delete_marker {
            if opts.version_id.is_none() {
                return Err(to_object_err(Error::FileNotFound, vec![bucket, object]));
            }
            return Err(to_object_err(Error::MethodNotAllowed, vec![bucket, object]));
        }

        // if object_info.size == 0 {
        //     let empty_rd: Box<dyn AsyncRead> = Box::new(Bytes::new());

        //     return Ok(GetObjectReader {
        //         stream: empty_rd,
        //         object_info,
        //     });
        // }

        if object_info.size == 0 {
            record_get_object_reader_path_observation(GET_OBJECT_PATH_EMPTY, object_class, size_bucket);
            // if let Some(rs) = range {
            //     let _ = rs.get_offset_length(object_info.size)?;
            // }

            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(Vec::new())),
                object_info,
                buffered_body: Some(Bytes::new()),
                body_source: GetObjectBodySource::Unprobed,
            };
            return Ok(reader);
        }

        // Inline data fast path: skip duplex pipe for small inline objects.
        // Uses the shared predicate from ObjectInfo; additionally checks that
        // inline data is actually present and neither range nor partNumber is
        // in flight.
        if should_use_inline_fast_path(&range, &object_info, &fi, opts) {
            let mut inline_prepare_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
            let data_shards = fi.erasure.data_blocks;

            let object_size = usize::try_from(fi.size)
                .map_err(|_| to_object_err(Error::other("inline fast path object size is invalid"), vec![bucket, object]))?;

            let checksum_info = fi.erasure.get_checksum_info(fi.parts[0].number);
            let checksum_algo =
                if fi.uses_legacy_checksum && checksum_info.algorithm == rustfs_utils::HashAlgorithm::HighwayHash256S {
                    rustfs_utils::HashAlgorithm::HighwayHash256SLegacy
                } else {
                    checksum_info.algorithm
                };

            if can_try_inline_data_shards_direct(object_size, fi.erasure.block_size)
                && let Some(data_files) = collect_inline_data_shard_fileinfos_by_index(&files, &fi, data_shards, |index| {
                    disks.get(index).is_some_and(Option::is_some)
                })
            {
                let read_length = inline_erasure_shard_file_offset(
                    0,
                    object_size,
                    object_size,
                    fi.erasure.block_size,
                    data_shards,
                    fi.uses_legacy_checksum,
                );
                let shard_size = inline_erasure_shard_size(fi.erasure.block_size, data_shards, fi.uses_legacy_checksum);
                if let Some(inline_prepare_stage_start) = inline_prepare_stage_start.take() {
                    rustfs_io_metrics::record_get_object_stage_duration_by_size(
                        GET_OBJECT_PATH_INLINE_DIRECT,
                        GET_STAGE_INLINE_PREPARE,
                        object_class.as_str(),
                        size_bucket,
                        inline_prepare_stage_start.elapsed().as_secs_f64(),
                    );
                }
                let reader_setup_stage_start = rustfs_io_metrics::get_stage_metrics_enabled().then(Instant::now);
                let mut readers = build_inline_bitrot_readers_from_refs(
                    &data_files,
                    bucket,
                    object,
                    read_length,
                    shard_size,
                    &checksum_algo,
                    opts.skip_verify_bitrot,
                )
                .await?;
                if let Some(reader_setup_stage_start) = reader_setup_stage_start {
                    rustfs_io_metrics::record_get_object_stage_duration_by_size(
                        GET_OBJECT_PATH_INLINE_DIRECT,
                        GET_STAGE_READER_SETUP,
                        object_class.as_str(),
                        size_bucket,
                        reader_setup_stage_start.elapsed().as_secs_f64(),
                    );
                }

                // Decode directly
                let decode_stage_start = rustfs_io_metrics::get_stage_metrics_enabled().then(Instant::now);
                if let Some(body) = try_read_inline_data_shards_direct(&mut readers, data_shards, read_length, object_size).await
                {
                    if let Some(decode_stage_start) = decode_stage_start {
                        rustfs_io_metrics::record_get_object_stage_duration_by_size(
                            GET_OBJECT_PATH_INLINE_DIRECT,
                            GET_STAGE_DECODE,
                            object_class.as_str(),
                            size_bucket,
                            decode_stage_start.elapsed().as_secs_f64(),
                        );
                    }

                    record_get_object_reader_path_observation(GET_OBJECT_PATH_INLINE_DIRECT, object_class, size_bucket);
                    let reader = GetObjectReader {
                        stream: Box::new(Cursor::new(body.clone())),
                        object_info,
                        buffered_body: Some(body),
                        body_source: GetObjectBodySource::Unprobed,
                    };
                    return Ok(reader);
                }
            }

            let erasure = erasure_from_file_info(&fi, fi.uses_legacy_checksum)?;
            let read_length = erasure.shard_file_offset(0, object_size, object_size);
            let total_shards = data_shards + fi.erasure.parity_blocks;
            let (_disks, files) = Self::shuffle_disks_and_parts_metadata_by_index(&disks, &files, &fi);

            // Check if we have enough inline data shards
            let inline_count = files
                .iter()
                .take(data_shards)
                .filter(|f| f.data.as_ref().is_some_and(|d| !d.is_empty()))
                .count();

            if inline_count >= data_shards {
                if let Some(inline_prepare_stage_start) = inline_prepare_stage_start.take() {
                    rustfs_io_metrics::record_get_object_stage_duration_by_size(
                        GET_OBJECT_PATH_INLINE_DIRECT,
                        GET_STAGE_INLINE_PREPARE,
                        object_class.as_str(),
                        size_bucket,
                        inline_prepare_stage_start.elapsed().as_secs_f64(),
                    );
                }
                let reader_setup_stage_start = rustfs_io_metrics::get_stage_metrics_enabled().then(Instant::now);
                let readers = build_inline_bitrot_readers(
                    &files,
                    total_shards,
                    bucket,
                    object,
                    read_length,
                    erasure.shard_size(),
                    &checksum_algo,
                    opts.skip_verify_bitrot,
                )
                .await?;
                if let Some(reader_setup_stage_start) = reader_setup_stage_start {
                    rustfs_io_metrics::record_get_object_stage_duration_by_size(
                        GET_OBJECT_PATH_INLINE_DIRECT,
                        GET_STAGE_READER_SETUP,
                        object_class.as_str(),
                        size_bucket,
                        reader_setup_stage_start.elapsed().as_secs_f64(),
                    );
                }

                let decode_stage_start = rustfs_io_metrics::get_stage_metrics_enabled().then(Instant::now);
                let mut output = Cursor::new(Vec::with_capacity(object_size));
                let (written, err) = erasure.decode(&mut output, readers, 0, object_size, object_size).await;
                if let Some(e) = err {
                    return Err(to_object_err(e.into(), vec![bucket, object]));
                }
                if written == 0 && fi.size > 0 {
                    return Err(to_object_err(
                        Error::other("inline fast path: erasure decode returned 0 bytes"),
                        vec![bucket, object],
                    ));
                }
                let body = Bytes::from(output.into_inner());
                if let Some(decode_stage_start) = decode_stage_start {
                    rustfs_io_metrics::record_get_object_stage_duration_by_size(
                        GET_OBJECT_PATH_INLINE_DIRECT,
                        GET_STAGE_DECODE,
                        object_class.as_str(),
                        size_bucket,
                        decode_stage_start.elapsed().as_secs_f64(),
                    );
                }

                record_get_object_reader_path_observation(GET_OBJECT_PATH_INLINE_DIRECT, object_class, size_bucket);
                let reader = GetObjectReader {
                    stream: Box::new(Cursor::new(body.clone())),
                    object_info,
                    buffered_body: Some(body),
                    body_source: GetObjectBodySource::Unprobed,
                };
                return Ok(reader);
            }
        }

        let path_decision_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let codec_streaming_gate = get_codec_streaming_reader_gate(
            bucket,
            object,
            &range,
            opts.part_number,
            &object_info,
            &fi,
            lock_optimization_enabled,
        );
        record_get_stage_duration_if_enabled(GET_OBJECT_PATH_SET_DISK, GET_STAGE_PATH_DECISION, path_decision_stage_start);

        if object_info.is_remote() {
            if let GetCodecStreamingDecision::Fallback(reason) = codec_streaming_gate.decision {
                record_get_codec_streaming_gate_decision(
                    codec_streaming_gate.object_class,
                    codec_streaming_gate.decision,
                    size_bucket,
                );
                rustfs_io_metrics::record_get_object_codec_streaming_fallback(reason.as_str());
            }
            record_get_object_reader_path_observation(GET_OBJECT_PATH_REMOTE_TRANSITION, object_class, size_bucket);
            let mut opts = opts.clone();
            if object_info.parts.len() == 1 {
                opts.part_number = Some(1);
            }
            let gr = get_transitioned_object_reader_with_tier_manager(
                bucket,
                object,
                &range,
                &h,
                &object_info,
                &opts,
                &self.ctx.tier_config_mgr(),
            )
            .await?;
            return Ok(finish_set_disk_read_lock(
                gr,
                read_lock_guard.take(),
                lock_optimization_enabled,
                bucket,
                object,
            ));
        }

        // App-layer object data cache probe: metadata (etag/size) is resolved
        // but no data shards have been read yet, so a hit skips the erasure
        // read, bitrot verify and decode entirely. The hook validates object
        // identity and rejects anything it cannot serve byte-identically.
        //
        // `plaintext_len` carries the size the normal `ReadPlan` would have
        // published, which the reader below must reproduce: for a compressed
        // object `ReadTransform::Compressed` sets `object_info.size` to the
        // decompressed length, and consumers such as UploadPartCopy read the
        // copy length straight off that field (backlog#1109).
        // Records whether the app-layer cache probe ran for this read, so the
        // app layer does not repeat the lookup it already performed after fresh
        // metadata resolution (backlog#1121 / ODC-16). It stays `Unprobed` when
        // the read is ineligible under the allow-list or the hook is not
        // registered; the direct-memory and streaming readers built below carry
        // it forward.
        let mut body_source = GetObjectBodySource::Unprobed;
        if let Some(plaintext_len) = full_object_plaintext_len(&range, opts, &object_info)
            && !get_object_body_cache_hook_suppressed()
            && let Some(hook) = get_object_body_cache_hook()
        {
            match hook.lookup(bucket, object, &object_info).await {
                Some(body) if i64::try_from(body.len()).is_ok_and(|len| len == plaintext_len) => {
                    record_get_object_reader_path_observation(GET_OBJECT_PATH_BODY_CACHE, object_class, size_bucket);
                    let mut object_info = object_info;
                    object_info.size = plaintext_len;
                    let reader = GetObjectReader {
                        stream: Box::new(Cursor::new(body.clone())),
                        object_info,
                        buffered_body: Some(body),
                        body_source: GetObjectBodySource::HookServed,
                    };
                    if lock_optimization_enabled {
                        release_materialized_read_lock(bucket, object, read_lock_guard.take());
                    }
                    return Ok(reader);
                }
                // Probed after fresh metadata resolution but no usable body: a
                // genuine miss, or a length-defensive rejection. The miss is
                // authoritative, so the app layer must not look up again.
                _ => {
                    body_source = GetObjectBodySource::HookMissed;
                }
            }
        }

        let direct_memory_decision = get_small_object_direct_memory_decision(&range, &object_info, &fi, opts);
        record_get_direct_memory_decision(object_class, direct_memory_decision, size_bucket);
        if let GetDirectMemoryDecision::Use { object_size } = direct_memory_decision {
            if let Some(body) = Self::try_get_object_direct_data_shards_with_fileinfo(
                bucket,
                object,
                &fi,
                &files,
                &disks,
                opts.skip_verify_bitrot,
                object_class.as_str(),
                size_bucket,
            )
            .await?
            {
                if body.len() != object_size {
                    return Err(to_object_err(
                        Error::other("direct-memory GET decoded length mismatch"),
                        vec![bucket, object],
                    ));
                }

                record_get_object_reader_path_observation(GET_OBJECT_PATH_DIRECT_MEMORY, object_class, size_bucket);
                let reader = GetObjectReader {
                    stream: Box::new(Cursor::new(body.clone())),
                    object_info,
                    buffered_body: Some(body),
                    body_source,
                };
                if lock_optimization_enabled {
                    release_materialized_read_lock(bucket, object, read_lock_guard.take());
                    debug!(bucket, object, "Lock optimization: released read lock after direct-memory read");
                }
                return Ok(reader);
            }

            let mut output = Vec::with_capacity(object_size);
            Self::get_object_with_fileinfo(
                bucket,
                object,
                0,
                object_info.size,
                &mut output,
                fi,
                files,
                &disks,
                self.set_index,
                self.pool_index,
                opts.skip_verify_bitrot,
                true,
                GET_OBJECT_PATH_DIRECT_MEMORY,
                object_class.as_str(),
                size_bucket,
            )
            .await?;

            if output.len() != object_size {
                return Err(to_object_err(
                    Error::other("direct-memory GET decoded length mismatch"),
                    vec![bucket, object],
                ));
            }

            record_get_object_reader_path_observation(GET_OBJECT_PATH_DIRECT_MEMORY, object_class, size_bucket);
            let body = Bytes::from(output);
            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(body.clone())),
                object_info,
                buffered_body: Some(body),
                body_source,
            };
            if lock_optimization_enabled {
                release_materialized_read_lock(bucket, object, read_lock_guard.take());
                debug!(bucket, object, "Lock optimization: released read lock after direct-memory read");
            }
            return Ok(reader);
        }

        match codec_streaming_gate.decision {
            GetCodecStreamingDecision::Use => {
                match Self::get_object_decode_reader_with_fileinfo(
                    bucket,
                    object,
                    &fi,
                    &files,
                    &disks,
                    self.set_index,
                    self.pool_index,
                    opts.skip_verify_bitrot,
                    object_class.as_str(),
                    size_bucket,
                    codec_streaming_gate.prefer_data_blocks_first_reader_setup,
                )
                .await?
                {
                    core::io_primitives::GetCodecStreamingReaderBuildOutcome::Reader(stream) => {
                        record_get_codec_streaming_gate_decision(
                            codec_streaming_gate.object_class,
                            GetCodecStreamingDecision::Use,
                            size_bucket,
                        );
                        record_get_object_reader_path_observation(GET_OBJECT_PATH_CODEC_STREAMING, object_class, size_bucket);
                        let (mut reader, _offset, _length) = GetObjectReader::new(stream, range, &object_info, opts, &h).await?;
                        // Carry the hook probe result so the app layer skips its
                        // now-redundant lookup on the streaming miss path (ODC-16).
                        reader.body_source = body_source;
                        return Ok(finish_set_disk_read_lock(
                            reader,
                            read_lock_guard.take(),
                            lock_optimization_enabled,
                            bucket,
                            object,
                        ));
                    }
                    core::io_primitives::GetCodecStreamingReaderBuildOutcome::Fallback(reason) => {
                        record_get_codec_streaming_gate_decision(
                            codec_streaming_gate.object_class,
                            GetCodecStreamingDecision::Fallback(reason),
                            size_bucket,
                        );
                        rustfs_io_metrics::record_get_object_codec_streaming_fallback(reason.as_str());
                    }
                }
            }
            GetCodecStreamingDecision::Fallback(reason) => {
                record_get_codec_streaming_gate_decision(
                    codec_streaming_gate.object_class,
                    codec_streaming_gate.decision,
                    size_bucket,
                );
                rustfs_io_metrics::record_get_object_codec_streaming_fallback(reason.as_str());
            }
        }

        record_get_object_reader_path_observation(GET_OBJECT_PATH_LEGACY_DUPLEX, object_class, size_bucket);

        let duplex_buffer_size = adaptive_duplex_buffer_size(object_info.size);
        let (rd, wd) = tokio::io::duplex(duplex_buffer_size);
        debug!(bucket, object, duplex_buffer_size, "Created duplex pipe for object data transfer");

        let (mut reader, offset, length) = GetObjectReader::new(Box::new(rd), range, &object_info, opts, &h).await?;
        // Carry the hook probe result so the app layer skips its now-redundant
        // lookup on the streaming miss path (ODC-16).
        reader.body_source = body_source;

        // let disks = disks.clone();
        let bucket = bucket.to_owned();
        let object = object.to_owned();
        let set_index = self.set_index;
        let pool_index = self.pool_index;
        let skip_verify = opts.skip_verify_bitrot;
        if lock_optimization_enabled {
            release_materialized_read_lock(&bucket, &object, read_lock_guard.take());
            debug!(bucket, object, "Lock optimization: released read lock before streaming read");
        }

        // When lock optimization is disabled, keep the read-lock guard in the
        // task so it lives for the duration of the streaming read.
        tokio::spawn(async move {
            let _guard = read_lock_guard;
            let mut writer = wd;
            // Do not wrap the entire read+write pipeline in `disk_read_timeout`.
            // `get_object_with_fileinfo` also waits on `writer`, so an outer timeout
            // would incorrectly treat downstream backpressure as disk-read latency.
            // Disk read timeouts must be enforced at the actual disk I/O operations.
            if let Err(e) = Self::get_object_with_fileinfo(
                &bucket,
                &object,
                offset,
                length,
                &mut writer,
                fi,
                files,
                &disks,
                set_index,
                pool_index,
                skip_verify,
                false,
                GET_OBJECT_PATH_LEGACY_DUPLEX,
                object_class.as_str(),
                size_bucket,
            )
            .await
            {
                let reason = classify_storage_error(&e);
                record_get_object_pipeline_failure(GET_STAGE_EMIT, reason);
                error!(
                    event = EVENT_SET_DISK_WRITE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    pool_index,
                    set_index,
                    offset,
                    requested_length = length,
                    skip_verify_bitrot = skip_verify,
                    state = "read_pipeline_failed",
                    stage = GET_STAGE_EMIT,
                    reason = reason.as_str(),
                    error = ?e,
                    "Set disk object read pipeline failed"
                );
            };
        });

        Ok(reader)
    }

    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.put_object_with_old_current_size(bucket, object, data, opts)
            .await
            .map(|(object_info, _)| object_info)
    }
}

impl SetDisks {
    /// `put_object` plus the destination key's previous current-version size,
    /// quorum-reduced from the dst `xl.meta` copies `rename_data` reads while
    /// committing (rustfs/backlog#1009). `None` means unknown (mixed-version
    /// peers, unparsable metadata, or sub-quorum divergence) — callers must
    /// fall back to degraded accounting, never assume "absent". The extra
    /// value is deliberately *not* part of `ObjectInfo`, which feeds S3
    /// responses, event payloads, replication, and ILM verbatim.
    #[tracing::instrument(skip(self, data,))]
    pub async fn put_object_with_old_current_size(
        &self,
        bucket: &str,
        object: &str,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<(ObjectInfo, Option<OldCurrentSize>)> {
        crate::hp_guard!("SetDisks::put_object");
        let storage_class_config = self.storage_class_config_snapshot();
        self.invalidate_get_object_metadata_cache(bucket, object).await;

        let disks = self.get_disks_internal().await;

        let mut object_lock_guard = None;

        if opts.http_preconditions.is_some() {
            if !opts.no_lock {
                object_lock_guard = Some(
                    self.acquire_write_lock_diag("put_object_precondition", bucket, object)
                        .await?,
                );
            }

            if let Some(err) = self.check_write_precondition(bucket, object, opts).await {
                return Err(err);
            }
        }

        let expected_restore_operation_id = restore_commit_operation_id_from_metadata(&opts.user_defined)?;
        let mut user_defined = opts.user_defined.clone();
        if let Some(eval_metadata) = &opts.eval_metadata {
            for (key, value) in eval_metadata {
                user_defined.insert(key.clone(), value.clone());
            }
        }
        if expected_restore_operation_id.is_some() {
            rustfs_utils::http::metadata_compat::remove_str(&mut user_defined, SUFFIX_RESTORE_OPERATION_ID);
        }
        let WriteLayout {
            data_drives,
            parity_drives,
            write_quorum,
        } = resolve_write_layout(
            &storage_class_config,
            self.pool_index,
            disks.len(),
            self.default_parity_count,
            user_defined.get(AMZ_STORAGE_CLASS).map(String::as_str),
            opts.max_parity,
        )?;

        // if filtered_online < write_quorum {
        //     warn!(
        //         "online disk snapshot {} below write quorum {} for {}/{}; returning erasure write quorum error",
        //         filtered_online, write_quorum, bucket, object
        //     );
        //     return Err(to_object_err(Error::ErasureWriteQuorum, vec![bucket, object]));
        // }

        let mut fi = FileInfo::new([bucket, object].join("/").as_str(), data_drives, parity_drives);

        fi.version_id = {
            if let Some(ref vid) = opts.version_id {
                Some(Uuid::parse_str(vid.as_str()).map_err(Error::other)?)
            } else {
                None
            }
        };

        if opts.versioned && fi.version_id.is_none() {
            fi.version_id = Some(Uuid::new_v4());
        }

        fi.data_dir = Some(Uuid::new_v4());

        let parts_metadata = vec![fi.clone(); disks.len()];

        let (mut shuffle_disks, mut parts_metadatas) = Self::shuffle_disks_and_parts_metadata(&disks, &parts_metadata, &fi);

        let tmp_dir = Uuid::new_v4().to_string();

        let tmp_object = format!("{}/{}/part.1", tmp_dir, fi.data_dir.unwrap());

        let result: Result<(ObjectInfo, Option<OldCurrentSize>)> = async {
            let erasure = erasure_from_file_info(&fi, false)?;

            let put_object_size = known_put_object_storage_size(data.size());
            let is_inline_buffer = storage_class_config.should_inline(erasure.shard_file_size(put_object_size), opts.versioned);

            let shard_file_size = erasure.shard_file_size(put_object_size);
            let shard_size = erasure.shard_size();
            let writer_setup_stage_start = Instant::now();
            let writer_futs: Vec<_> = shuffle_disks
                .iter()
                .map(|disk_op| {
                    let tmp_obj = tmp_object.clone();
                    async move {
                        if let Some(disk) = disk_op
                            && disk.is_online().await
                        {
                            match create_bitrot_writer(
                                is_inline_buffer,
                                Some(disk),
                                RUSTFS_META_TMP_BUCKET,
                                &tmp_obj,
                                shard_file_size,
                                shard_size,
                                HashAlgorithm::HighwayHash256S,
                            )
                            .await
                            {
                                Ok(writer) => (Some(writer), None),
                                Err(err) => {
                                    warn!(
                                        event = EVENT_SET_DISK_WRITE,
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                                        disk = ?disk,
                                        state = "bitrot_writer_skipped",
                                        error = ?err,
                                        "Set disk bitrot writer skipped"
                                    );
                                    (None, Some(err))
                                }
                            }
                        } else {
                            (None, Some(DiskError::DiskNotFound))
                        }
                    }
                })
                .collect();
            let writer_results = join_all(writer_futs).await;
            let mut writers = Vec::with_capacity(writer_results.len());
            let mut errors = Vec::with_capacity(writer_results.len());
            for (w, e) in writer_results {
                writers.push(w);
                errors.push(e);
            }
            let writer_setup_ms = writer_setup_stage_start.elapsed().as_millis() as u64;
            rustfs_io_metrics::record_put_object_stage_duration("set_disk_writer_setup", writer_setup_ms as f64);

            let nil_count = errors.iter().filter(|&e| e.is_none()).count();
            if nil_count < write_quorum {
                error!(
                    event = EVENT_SET_DISK_WRITE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    write_quorum,
                    available_writers = nil_count,
                    state = "write_quorum_unavailable",
                    error = ?errors,
                    "Set disk write quorum unavailable"
                );
                if let Some(write_err) = reduce_write_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, write_quorum) {
                    return Err(to_object_err(write_err.into(), vec![bucket, object]));
                }

                return Err(Error::other(format!("not enough disks to write: {errors:?}")));
            }

            let stream = mem::replace(
                &mut data.stream,
                HashReader::from_stream(Cursor::new(Vec::new()), 0, 0, None, None, false)?,
            );

            let write_path = classify_put_write_path(is_inline_buffer, put_object_size, fi.erasure.block_size);
            rustfs_io_metrics::record_put_object_path(write_path.metric_label());

            let encode_stage_start = Instant::now();
            let (reader, w_size) = match write_path {
                SmallWritePath::Inline => match Arc::new(erasure)
                    .encode_inline_small(stream, &mut writers, write_quorum)
                    .await
                {
                    Ok((r, w)) => (r, w),
                    Err(e) => {
                        error!("encode_inline_small err {:?}", e);
                        return Err(e.into());
                    }
                },
                SmallWritePath::SingleBlockNonInline => match Arc::new(erasure)
                    .encode_single_block_non_inline(stream, &mut writers, write_quorum)
                    .await
                {
                    Ok((r, w)) => (r, w),
                    Err(e) => {
                        error!("encode_single_block_non_inline err {:?}", e);
                        return Err(e.into());
                    }
                },
                SmallWritePath::PipelineBatchedLarge => {
                    match Arc::new(erasure).encode_batched(stream, &mut writers, write_quorum).await {
                        Ok((r, w)) => (r, w),
                        Err(e) => {
                            error!("encode_batched err {:?}", e);
                            return Err(e.into());
                        }
                    }
                }
                SmallWritePath::Pipeline => match Arc::new(erasure).encode(stream, &mut writers, write_quorum).await {
                    Ok((r, w)) => (r, w),
                    Err(e) => {
                        error!("encode err {:?}", e);
                        return Err(e.into());
                    }
                },
            };
            let encode_ms = encode_stage_start.elapsed().as_millis() as u64;
            rustfs_io_metrics::record_put_object_stage_duration("set_disk_encode", encode_ms as f64);

            let _ = mem::replace(&mut data.stream, reader);
            // if let Err(err) = close_bitrot_writers(&mut writers).await {
            //     error!("close_bitrot_writers err {:?}", err);
            // }

            if (w_size as i64) < data.size() {
                warn!(
                    event = EVENT_SET_DISK_WRITE,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket,
                    object,
                    written_size = w_size,
                    expected_size = data.size(),
                    state = "short_write",
                    "Set disk write produced fewer bytes than expected"
                );
                return Err(Error::other(format!(
                    "put_object write size < data.size(), w_size={}, data.size={}",
                    w_size,
                    data.size()
                )));
            }

            if contains_key_str(&user_defined, SUFFIX_COMPRESSION) {
                insert_str(&mut user_defined, SUFFIX_COMPRESSION_SIZE, w_size.to_string());
            }

            let index_op = data
                .stream
                .try_get_index()
                .map(crate::io_support::rio::compression_index_storage_bytes);

            let mut etag = data.stream.try_resolve_etag().unwrap_or_default();
            if let Some(ref tag) = opts.preserve_etag {
                etag = tag.clone();
            }

            user_defined.insert("etag".to_owned(), etag.clone());

            if !user_defined.contains_key("content-type") {
                //  get content-type
            }

            let mut actual_size = data.actual_size();
            if actual_size < 0 {
                let is_compressed = fi.is_compressed();
                if !is_compressed {
                    actual_size = w_size as i64;
                }
            }

            if fi.checksum.is_none()
                && let Some(content_hash) = data.as_hash_reader().content_hash()
            {
                fi.checksum = Some(content_hash.to_bytes(&[]));
            }

            if let Some(sc) = user_defined.get(AMZ_STORAGE_CLASS)
                && sc == storageclass::STANDARD
            {
                let _ = user_defined.remove(AMZ_STORAGE_CLASS);
            }

            let mod_time = if let Some(mod_time) = opts.mod_time {
                Some(mod_time)
            } else {
                Some(OffsetDateTime::now_utc())
            };

            // Drop any disk whose shard did not fully commit (offline at writer
            // setup, short write, or a write/shutdown error) so its truncated or
            // absent shard is not renamed into place and counted toward write
            // quorum. Otherwise redundancy is inflated: the object claims N good
            // shards but one is short/corrupt, so a single later disk failure can
            // drop it below reconstructable quorum (backlog#852 / #799 B3).
            // `rename_data` re-checks write quorum over the surviving disks and
            // rolls back if too few remain.
            let committed_shards = drop_failed_writer_disks(&mut shuffle_disks, &writers);
            if committed_shards < write_quorum {
                return Err(Error::other(format!(
                    "put_object write quorum unavailable after encode: {committed_shards} shard(s) committed, need {write_quorum}"
                )));
            }

            for (i, pfi) in parts_metadatas.iter_mut().enumerate() {
                pfi.metadata = user_defined.clone();
                if is_inline_buffer {
                    if let Some(writer) = writers[i].take() {
                        pfi.data = Some(writer.into_inline_data().map(Bytes::from).unwrap_or_default());
                    }

                    pfi.set_inline_data();
                }

                pfi.mod_time = mod_time;
                pfi.size = w_size as i64;
                pfi.versioned = opts.versioned || opts.version_suspended;
                pfi.add_object_part(1, etag.clone(), w_size, mod_time, actual_size, index_op.clone(), None);
                pfi.checksum = fi.checksum.clone();

                if opts.data_movement {
                    pfi.set_data_moved();
                }
            }

            drop(writers); // drop writers to close all files, this is to prevent FileAccessDenied errors when renaming data

            if !opts.no_lock && object_lock_guard.is_none() {
                object_lock_guard = Some(self.acquire_write_lock_diag("put_object_commit", bucket, object).await?);
            }

            // Phase 2 (backlog#899): fence the commit on lock loss. If the refresh
            // heartbeat has observed a refresh-quorum loss, another writer may have
            // re-acquired this object's lock; committing now would race a double-write.
            // Abort with a retryable error *before* rename_data makes the new version
            // durable — once rename_data returns Ok the write is committed and must
            // never be aborted (that would violate "a committed write is not lost").
            if object_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
                return Err(StorageError::NamespaceLockQuorumUnavailable {
                    mode: "put_object_commit",
                    bucket: bucket.to_string(),
                    object: object.to_string(),
                    required: 1,
                    achieved: 0,
                });
            }

            self.require_current_restore_operation_id(bucket, object, opts, expected_restore_operation_id, "put_object_commit")
                .await?;

            let rename_stage_start = Instant::now();
            let (online_disks, _, op_old_dir, cleanup_disks, old_current_size) = Self::rename_data(
                &shuffle_disks,
                RUSTFS_META_TMP_BUCKET,
                tmp_dir.as_str(),
                &parts_metadatas,
                bucket,
                object,
                write_quorum,
            )
            .await?;
            let rename_stage_ms = rename_stage_start.elapsed().as_millis() as u64;
            rustfs_io_metrics::record_put_object_stage_duration("set_disk_rename", rename_stage_ms as f64);
            if (rename_stage_ms as u128) >= SET_DISK_COMMIT_TAIL_WARN_THRESHOLD_MS {
                warn!(
                    event = EVENT_SET_DISK_COMMIT_TAIL_SLOW,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    stage = "rename_data",
                    bucket = %bucket,
                    object = %object,
                    tmp_dir = %tmp_dir,
                    duration_ms = { rename_stage_ms },
                    write_quorum,
                    state = "slow",
                    "SetDisk commit tail stage is slow"
                );
            }

            let mut cleanup_stage_ms: Option<u64> = None;
            if let Some(old_dir) = op_old_dir {
                let committed_dir = fi.data_dir.unwrap_or_default().to_string();
                let cleanup_stage_start = Instant::now();
                // backlog#898: reclaiming the dereferenced old data dir is
                // best-effort and returns a receipt (never `Err`). A failed GC
                // here must not negate an already-committed, durable write, so we
                // deliberately do NOT `?`-propagate it into a 503. On residue the
                // report path emits the leak metric and enqueues a heal.
                let cleanup = self
                    .commit_rename_data_dir(&cleanup_disks, bucket, object, &old_dir.to_string(), &committed_dir, write_quorum)
                    .await;
                let cleanup_ms = cleanup_stage_start.elapsed().as_millis() as u64;
                cleanup_stage_ms = Some(cleanup_ms);
                rustfs_io_metrics::record_put_object_stage_duration("set_disk_old_data_cleanup", cleanup_ms as f64);
                self.report_old_data_dir_cleanup(bucket, object, &old_dir.to_string(), &cleanup)
                    .await;
                if (cleanup_ms as u128) >= SET_DISK_COMMIT_TAIL_WARN_THRESHOLD_MS {
                    warn!(
                        event = EVENT_SET_DISK_COMMIT_TAIL_SLOW,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        stage = "commit_rename_data_dir",
                        bucket = %bucket,
                        object = %object,
                        tmp_dir = %tmp_dir,
                        old_dir = %old_dir,
                        duration_ms = cleanup_ms,
                        write_quorum,
                        state = "slow",
                        "SetDisk commit tail stage is slow"
                    );
                }
            }

            drop(object_lock_guard); // drop object lock guard to release the lock

            for (i, op_disk) in online_disks.iter().enumerate() {
                if let Some(disk) = op_disk
                    && disk.is_online().await
                {
                    fi = parts_metadatas[i].clone();
                    break;
                }
            }

            if fi.is_compressed() {
                record_compression_total_memory(actual_size as u64, w_size as u64).await;
            }
            self.record_capacity_scope_if_needed(opts.capacity_scope_token, &online_disks);

            fi.replication_state_internal = Some(replication_state_to_filemeta(&opts.put_replication_state()));

            fi.is_latest = true;

            if issue3031_diag_enabled() {
                let online_success_count = online_disks.iter().filter(|disk| disk.is_some()).count();
                warn!(
                    target: "rustfs_ecstore::set_disk",
                    bucket = %bucket,
                    object = %object,
                    tmp_dir = %tmp_dir,
                    data_dir = ?fi.data_dir,
                    write_quorum,
                    online_success_count,
                    op_old_dir = ?op_old_dir,
                    "issue3031_put_object_commit_succeeded"
                );
            }

            let total_commit_tail_ms = rename_stage_start.elapsed().as_millis();
            if total_commit_tail_ms >= SET_DISK_COMMIT_TAIL_WARN_THRESHOLD_MS {
                warn!(
                    event = EVENT_SET_DISK_COMMIT_TAIL_SLOW,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    stage = "put_object_commit_tail",
                    bucket = %bucket,
                    object = %object,
                    tmp_dir = %tmp_dir,
                    duration_ms = total_commit_tail_ms as u64,
                    write_quorum,
                    state = "slow",
                    "SetDisk commit tail is slow"
                );
            }

            if issue3031_diag_enabled() {
                warn!(
                    event = EVENT_SET_DISK_PUT_OBJECT_STAGE_SUMMARY,
                    component = LOG_COMPONENT_ECSTORE,
                    subsystem = LOG_SUBSYSTEM_SET_DISK,
                    bucket = %bucket,
                    object = %object,
                    write_quorum,
                    write_path = write_path.metric_label(),
                    writer_setup_ms,
                    encode_ms,
                    rename_ms = rename_stage_ms,
                    cleanup_ms = cleanup_stage_ms.unwrap_or_default(),
                    cleanup_present = cleanup_stage_ms.is_some(),
                    commit_tail_ms = total_commit_tail_ms as u64,
                    result = "success",
                    "SetDisk put_object stage summary"
                );
            }

            Ok((
                ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended),
                old_current_size,
            ))
        }
        .await;

        if issue3031_diag_enabled()
            && let Err(err) = &result
        {
            let stage_hint = if err.to_string().contains("not enough disks to write") {
                "writer_setup_or_quorum"
            } else {
                "unknown"
            };
            warn!(
                event = EVENT_SET_DISK_PUT_OBJECT_STAGE_SUMMARY,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                bucket = %bucket,
                object = %object,
                result = "error",
                stage_hint,
                error = %err,
                "SetDisk put_object stage summary"
            );
        }

        if result.is_ok() {
            self.invalidate_get_object_metadata_cache(bucket, object).await;
        }

        if issue3031_diag_enabled() {
            warn!(
                target: "rustfs_ecstore::set_disk",
                bucket = %bucket,
                object = %object,
                tmp_dir = %tmp_dir,
                result = ?result.as_ref().map(|_| ()).map_err(|err| err.to_string()),
                "issue3031_put_object_tmp_cleanup_start"
            );
        }

        if result.is_ok() {
            // Success path: `rename_data` has already moved the data dir out of
            // the tmp workspace and removed the (empty) tmp dir where it could,
            // so this delete_all is a speculative safety net that normally hits
            // a missing path. It still must run — `rename_data`'s `remove_std`
            // only removes empty directories and silently ignores failures —
            // but it has no reason to run on the response path: under
            // fsync-heavy load the same-disk queueing behind it was measured to
            // add ~9ms average (p99 77ms) to PUT latency (backlog#924 / HP-3).
            // If the process dies before the spawned task runs, the stale tmp
            // entry is reclaimed by cleanup_stale_tmp_objects (24h expiry,
            // 5-minute background loop).
            let set_disks = self.clone();
            tokio::spawn(async move {
                if let Err(err) = set_disks.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_dir).await {
                    warn!(tmp_dir = %tmp_dir, error = ?err, "failed to cleanup put_object temporary data");
                } else if issue3031_diag_enabled() {
                    warn!(
                        target: "rustfs_ecstore::set_disk",
                        tmp_dir = %tmp_dir,
                        "issue3031_put_object_tmp_cleanup_done"
                    );
                }
            });
        } else {
            // Failure path (quorum loss / rollback): keep the cleanup inline so
            // a failed PUT never returns while its tmp shards are still on disk
            // (state-residue hardening tracked by backlog#864 / backlog#898).
            if let Err(err) = self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_dir).await {
                warn!(tmp_dir = %tmp_dir, error = ?err, "failed to cleanup put_object temporary data");
            } else if issue3031_diag_enabled() {
                warn!(
                    target: "rustfs_ecstore::set_disk",
                    bucket = %bucket,
                    object = %object,
                    tmp_dir = %tmp_dir,
                    "issue3031_put_object_tmp_cleanup_done"
                );
            }
        }

        result
    }
}

struct TransitionUploadReader<R> {
    inner: R,
    consumed: Arc<AtomicU64>,
}

impl<R> TransitionUploadReader<R> {
    fn new(inner: R, consumed: Arc<AtomicU64>) -> Self {
        Self { inner, consumed }
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for TransitionUploadReader<R> {
    fn poll_read(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let read = buf.filled().len() - before;
                let read =
                    u64::try_from(read).map_err(|_| std::io::Error::other("transition upload read count exceeds u64::MAX"))?;
                self.consumed
                    .fetch_update(Ordering::Release, Ordering::Relaxed, |consumed| consumed.checked_add(read))
                    .map_err(|_| std::io::Error::other("transition upload read count overflow"))?;
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

struct TransitionUploadWriter<W> {
    inner: W,
    produced: u64,
}

impl<W> TransitionUploadWriter<W> {
    fn new(inner: W) -> Self {
        Self { inner, produced: 0 }
    }

    fn produced(&self) -> u64 {
        self.produced
    }
}

impl<W: AsyncWrite + Unpin> AsyncWrite for TransitionUploadWriter<W> {
    fn poll_write(mut self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<std::io::Result<usize>> {
        match Pin::new(&mut self.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                let written_u64 = u64::try_from(written)
                    .map_err(|_| std::io::Error::other("transition upload write count exceeds u64::MAX"))?;
                self.produced = self
                    .produced
                    .checked_add(written_u64)
                    .ok_or_else(|| std::io::Error::other("transition upload write count overflow"))?;
                Poll::Ready(Ok(written))
            }
            other => other,
        }
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[derive(Debug)]
pub(crate) struct TransitionUploadFailure {
    pub(crate) error: StorageError,
    pub(crate) candidate: Option<TransitionUploadCandidate>,
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) struct TransitionUploadCompletion {
    pub(crate) candidate: TransitionUploadCandidate,
    pub(crate) produced: u64,
    pub(crate) consumed: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TransitionUploadCandidate {
    remote_version: TransitionUploadRemoteVersion,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TransitionUploadRemoteVersion {
    KnownExact(String),
    KnownUnversioned(String),
}

impl TransitionUploadCandidate {
    pub(crate) fn from_put_response(remote_version: String) -> Self {
        let remote_version =
            if remote_version.is_empty() || Uuid::parse_str(&remote_version).is_ok_and(|version_id| version_id.is_nil()) {
                TransitionUploadRemoteVersion::KnownUnversioned(remote_version)
            } else {
                TransitionUploadRemoteVersion::KnownExact(remote_version)
            };
        Self { remote_version }
    }

    pub(crate) fn remote_version(&self) -> &str {
        match &self.remote_version {
            TransitionUploadRemoteVersion::KnownExact(remote_version)
            | TransitionUploadRemoteVersion::KnownUnversioned(remote_version) => remote_version,
        }
    }

    pub(crate) fn cleanup_version(&self) -> &str {
        match &self.remote_version {
            TransitionUploadRemoteVersion::KnownExact(remote_version) => remote_version,
            TransitionUploadRemoteVersion::KnownUnversioned(_) => "",
        }
    }
}

pub(crate) async fn complete_transition_upload<Remote, Producer>(
    remote_upload: Remote,
    producer: Producer,
    expected_size: u64,
    consumed: Arc<AtomicU64>,
) -> std::result::Result<TransitionUploadCompletion, TransitionUploadFailure>
where
    Remote: Future<Output = std::result::Result<String, std::io::Error>>,
    Producer: Future<Output = Result<u64>>,
{
    let producer = std::panic::AssertUnwindSafe(producer).catch_unwind();
    let (remote_result, producer_result) = tokio::join!(remote_upload, producer);
    let remote_version = match remote_result {
        Ok(remote_version) => remote_version,
        Err(remote_error) => {
            let error = match producer_result {
                Ok(Err(StorageError::Io(producer_error))) if producer_error.kind() == std::io::ErrorKind::BrokenPipe => {
                    StorageError::Io(remote_error)
                }
                Ok(Err(producer_error)) => producer_error,
                Err(_) => StorageError::Unexpected,
                Ok(Ok(_)) => StorageError::Io(remote_error),
            };
            return Err(TransitionUploadFailure { error, candidate: None });
        }
    };
    let candidate = TransitionUploadCandidate::from_put_response(remote_version);
    let produced = match producer_result {
        Ok(Ok(produced)) => produced,
        Ok(Err(error)) => {
            return Err(TransitionUploadFailure {
                error,
                candidate: Some(candidate),
            });
        }
        Err(_) => {
            return Err(TransitionUploadFailure {
                error: StorageError::Unexpected,
                candidate: Some(candidate),
            });
        }
    };
    let consumed = consumed.load(Ordering::Acquire);
    if produced != expected_size || consumed != expected_size {
        let error = if produced < expected_size || consumed < expected_size {
            StorageError::LessData
        } else {
            StorageError::MoreData
        };
        return Err(TransitionUploadFailure {
            error,
            candidate: Some(candidate),
        });
    }
    Ok(TransitionUploadCompletion {
        candidate,
        produced,
        consumed,
    })
}

pub(crate) async fn cleanup_uncommitted_transition_upload(
    lease: &TierOperationLease,
    object: &str,
    candidate: &TransitionUploadCandidate,
) -> std::io::Result<RemoteTierDeleteOutcome> {
    delete_object_from_remote_tier_with_lease_idempotent(object, candidate.cleanup_version(), lease).await
}

fn log_transition_upload_cleanup_failure(
    lease: &TierOperationLease,
    object: &str,
    candidate: &TransitionUploadCandidate,
    err: &std::io::Error,
) {
    warn!(
        tier = lease.tier_name(),
        tier_generation = lease.generation(),
        object,
        remote_version = candidate.cleanup_version(),
        error = ?err,
        "failed to clean uncommitted transition upload"
    );
}

pub(crate) struct TransitionUploadCleanup {
    lease: TierOperationLease,
    object: String,
    candidate: TransitionUploadCandidate,
    armed: bool,
}

impl TransitionUploadCleanup {
    pub(crate) fn new(lease: TierOperationLease, object: &str, candidate: TransitionUploadCandidate) -> Self {
        Self {
            lease,
            object: object.to_string(),
            candidate,
            armed: true,
        }
    }

    pub(crate) async fn cleanup(&mut self) -> std::io::Result<RemoteTierDeleteOutcome> {
        match cleanup_uncommitted_transition_upload(&self.lease, &self.object, &self.candidate).await {
            Ok(outcome) => {
                self.armed = false;
                Ok(outcome)
            }
            Err(err) => {
                log_transition_upload_cleanup_failure(&self.lease, &self.object, &self.candidate, &err);
                Err(err)
            }
        }
    }

    pub(crate) fn disarm(&mut self) {
        self.armed = false;
    }
}

impl Drop for TransitionUploadCleanup {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let lease = match self.lease.try_clone() {
            Ok(lease) => lease,
            Err(err) => {
                warn!(
                    tier = self.lease.tier_name(),
                    tier_generation = self.lease.generation(),
                    object = self.object,
                    error = ?err,
                    "unable to retain tier lease for cancelled transition cleanup"
                );
                return;
            }
        };
        let object = self.object.clone();
        let candidate = self.candidate.clone();
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            handle.spawn(async move {
                if let Err(err) = cleanup_uncommitted_transition_upload(&lease, &object, &candidate).await {
                    log_transition_upload_cleanup_failure(&lease, &object, &candidate, &err);
                }
            });
        }
    }
}

#[cfg(test)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum TransitionCommitPause {
    BeforeLockLost,
    BeforeLeaseValidation,
    AfterLeaseValidation,
}

#[cfg(test)]
struct TransitionCommitBarrierState {
    bucket: String,
    object: String,
    pause: TransitionCommitPause,
    arrived: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
struct TransitionCommitBarrier {
    state: Arc<TransitionCommitBarrierState>,
}

#[cfg(test)]
static TRANSITION_COMMIT_BARRIER: std::sync::OnceLock<std::sync::Mutex<Option<Arc<TransitionCommitBarrierState>>>> =
    std::sync::OnceLock::new();

#[cfg(test)]
impl TransitionCommitBarrier {
    fn install_before_lock_lost_check(bucket: &str, object: &str) -> Self {
        Self::install_at(bucket, object, TransitionCommitPause::BeforeLockLost)
    }

    fn install(bucket: &str, object: &str) -> Self {
        Self::install_at(bucket, object, TransitionCommitPause::BeforeLeaseValidation)
    }

    fn install_after_lease_check(bucket: &str, object: &str) -> Self {
        Self::install_at(bucket, object, TransitionCommitPause::AfterLeaseValidation)
    }

    fn install_at(bucket: &str, object: &str, pause: TransitionCommitPause) -> Self {
        let state = Arc::new(TransitionCommitBarrierState {
            bucket: bucket.to_string(),
            object: object.to_string(),
            pause,
            arrived: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
        });
        let mut slot = TRANSITION_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition commit barrier mutex should not poison");
        assert!(slot.is_none(), "transition commit barrier must be installed by one test at a time");
        *slot = Some(Arc::clone(&state));
        drop(slot);
        Self { state }
    }

    async fn wait_until_paused(&self) {
        tokio::time::timeout(Duration::from_secs(30), self.state.arrived.notified())
            .await
            .expect("transition should reach the deterministic commit barrier");
    }

    fn release(&self) {
        self.state.release.notify_one();
    }
}

#[cfg(test)]
impl Drop for TransitionCommitBarrier {
    fn drop(&mut self) {
        self.state.release.notify_one();
        let mut slot = TRANSITION_COMMIT_BARRIER
            .get_or_init(|| std::sync::Mutex::new(None))
            .lock()
            .expect("transition commit barrier mutex should not poison");
        if slot.as_ref().is_some_and(|state| Arc::ptr_eq(state, &self.state)) {
            *slot = None;
        }
    }
}

#[cfg(test)]
async fn pause_transition_commit(bucket: &str, object: &str, pause: TransitionCommitPause) {
    let barrier = TRANSITION_COMMIT_BARRIER
        .get_or_init(|| std::sync::Mutex::new(None))
        .lock()
        .expect("transition commit barrier mutex should not poison")
        .as_ref()
        .filter(|barrier| barrier.bucket == bucket && barrier.object == object && barrier.pause == pause)
        .cloned();
    if let Some(barrier) = barrier {
        barrier.arrived.notify_one();
        barrier.release.notified().await;
    }
}

fn parse_transition_version_id(remote_version: &str) -> std::result::Result<Option<Uuid>, uuid::Error> {
    if remote_version.is_empty() {
        return Ok(None);
    }
    Uuid::parse_str(remote_version).map(|version_id| (!version_id.is_nil()).then_some(version_id))
}

#[cfg(test)]
mod transition_upload_completion_tests {
    use super::*;

    fn consumed(bytes: u64) -> Arc<AtomicU64> {
        Arc::new(AtomicU64::new(bytes))
    }

    #[tokio::test]
    async fn rejects_source_errors_at_first_middle_and_last_chunk() {
        let remote_version = Uuid::nil().to_string();
        for consumed_bytes in [0, 512, 1023] {
            let result = complete_transition_upload(
                std::future::ready(Ok(remote_version.clone())),
                std::future::ready(Err(StorageError::FileCorrupt)),
                1024,
                consumed(consumed_bytes),
            )
            .await;
            let failure = result.expect_err("a source read error must fail the upload completion protocol");
            assert!(matches!(failure.error, StorageError::FileCorrupt));
            assert_eq!(
                failure.candidate.as_ref().map(TransitionUploadCandidate::remote_version),
                Some(remote_version.as_str())
            );
        }
    }

    #[tokio::test]
    async fn rejects_partial_body_accepted_by_remote() {
        let failure = complete_transition_upload(
            std::future::ready(Ok(Uuid::new_v4().to_string())),
            std::future::ready(Ok(1024)),
            1024,
            consumed(511),
        )
        .await
        .expect_err("remote success must not hide a partially consumed body");
        assert!(matches!(failure.error, StorageError::LessData));
        assert!(failure.candidate.is_some());
    }

    #[tokio::test]
    async fn maps_source_panic_cancel_and_early_close_to_failures() {
        let panic_failure = complete_transition_upload(
            std::future::ready(Ok(Uuid::new_v4().to_string())),
            async {
                panic!("injected transition producer panic");
                #[allow(unreachable_code)]
                Ok(0)
            },
            1,
            consumed(0),
        )
        .await
        .expect_err("a producer panic must not enter local commit");
        assert!(matches!(panic_failure.error, StorageError::Unexpected));

        let cancelled = complete_transition_upload(
            std::future::ready(Ok(Uuid::new_v4().to_string())),
            std::future::ready(Err(StorageError::OperationCanceled)),
            1,
            consumed(0),
        )
        .await
        .expect_err("a cancelled producer must not enter local commit");
        assert!(matches!(cancelled.error, StorageError::OperationCanceled));

        let early_close = complete_transition_upload(
            std::future::ready(Ok(Uuid::new_v4().to_string())),
            std::future::ready(Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "remote reader closed early",
            )))),
            1024,
            consumed(16),
        )
        .await
        .expect_err("an early remote close must not enter local commit");
        assert!(matches!(early_close.error, StorageError::Io(ref err) if err.kind() == std::io::ErrorKind::BrokenPipe));
    }

    #[tokio::test]
    async fn rejects_declared_size_mismatches_and_accepts_zero_size() {
        let shorter = complete_transition_upload(
            std::future::ready(Ok(Uuid::new_v4().to_string())),
            std::future::ready(Ok(511)),
            512,
            consumed(511),
        )
        .await
        .expect_err("a short source must fail the declared-size check");
        assert!(matches!(shorter.error, StorageError::LessData));

        let longer = complete_transition_upload(
            std::future::ready(Ok(Uuid::new_v4().to_string())),
            std::future::ready(Ok(513)),
            512,
            consumed(513),
        )
        .await
        .expect_err("an oversized source must fail the declared-size check");
        assert!(matches!(longer.error, StorageError::MoreData));

        let exact_remote_version = Uuid::new_v4().to_string();
        let exact = complete_transition_upload(
            std::future::ready(Ok(exact_remote_version.clone())),
            std::future::ready(Ok(512)),
            512,
            consumed(512),
        )
        .await
        .expect("an exact producer and consumer byte count must complete");
        assert_eq!(exact.candidate.remote_version(), exact_remote_version);
        assert_eq!((exact.produced, exact.consumed), (512, 512));

        let remote_version = Uuid::nil().to_string();
        let result =
            complete_transition_upload(std::future::ready(Ok(remote_version.clone())), std::future::ready(Ok(0)), 0, consumed(0))
                .await
                .expect("an empty source and empty remote body must complete");
        assert_eq!(result.candidate.remote_version(), remote_version);
        assert_eq!((result.produced, result.consumed), (0, 0));
    }

    #[tokio::test]
    async fn preserves_remote_error_when_commit_status_is_unknown() {
        let failure = complete_transition_upload(
            std::future::ready(Err(std::io::Error::new(std::io::ErrorKind::ConnectionReset, "remote response was lost"))),
            std::future::ready(Err(StorageError::Io(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "consumer disappeared",
            )))),
            1024,
            consumed(0),
        )
        .await
        .expect_err("an unknown remote commit result must fail closed");
        assert!(matches!(failure.error, StorageError::Io(ref err) if err.kind() == std::io::ErrorKind::ConnectionReset));
        assert!(failure.candidate.is_none(), "unknown remote versions must never enter precise cleanup");

        let source_failure = complete_transition_upload(
            std::future::ready(Err(std::io::Error::new(
                std::io::ErrorKind::ConnectionReset,
                "remote rejected the truncated body",
            ))),
            std::future::ready(Err(StorageError::FileCorrupt)),
            1024,
            consumed(128),
        )
        .await
        .expect_err("a source integrity error must survive a concurrent remote failure");
        assert!(matches!(source_failure.error, StorageError::FileCorrupt));
        assert!(source_failure.candidate.is_none());
    }
}

#[cfg(test)]
mod transition_version_id_tests {
    use super::{TransitionUploadCandidate, parse_transition_version_id};
    use uuid::Uuid;

    #[test]
    fn normalizes_unversioned_remote_ids() {
        assert_eq!(parse_transition_version_id("").expect("empty remote version should be valid"), None);
        assert_eq!(
            parse_transition_version_id(&Uuid::nil().to_string()).expect("nil remote version should be valid"),
            None
        );
        assert_eq!(
            TransitionUploadCandidate::from_put_response(Uuid::nil().to_string()).cleanup_version(),
            ""
        );
        assert_eq!(TransitionUploadCandidate::from_put_response(String::new()).cleanup_version(), "");
    }

    #[test]
    fn preserves_valid_remote_id_and_rejects_invalid_text() {
        let version_id = Uuid::new_v4();
        assert_eq!(
            parse_transition_version_id(&version_id.to_string()).expect("UUID remote version should be valid"),
            Some(version_id)
        );
        assert_eq!(
            TransitionUploadCandidate::from_put_response(version_id.to_string()).cleanup_version(),
            version_id.to_string()
        );
        assert_eq!(
            TransitionUploadCandidate::from_put_response("opaque-version-token".to_string()).cleanup_version(),
            "opaque-version-token"
        );
        assert!(parse_transition_version_id("not-a-uuid").is_err());
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::object::ObjectOperations for SetDisks {
    type Error = Error;
    type ObjectInfo = ObjectInfo;
    type ObjectOptions = ObjectOptions;
    type FileInfo = FileInfo;
    type ObjectToDelete = ObjectToDelete;
    type DeletedObject = DeletedObject;

    #[tracing::instrument(skip(self))]
    async fn copy_object(
        &self,
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        src_info: &mut ObjectInfo,
        src_opts: &ObjectOptions,
        dst_opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        if !src_info.metadata_only {
            if path_join_buf(&[src_bucket, src_object]) != path_join_buf(&[dst_bucket, dst_object]) {
                return Err(StorageError::NotImplemented);
            }
            // Self-copy with a data reader: write tier data back locally (de-tiering).
            // Handles `mc cp --storage-class STANDARD obj obj` on a transitioned object.
            if let Some(mut put_reader) = src_info.put_object_reader.take() {
                return self.put_object(dst_bucket, dst_object, &mut put_reader, dst_opts).await;
            }
            // Same-key tiered copy without a pre-fetched reader: fall through to the metadata
            // path so the caller gets a disk/quorum error rather than NotImplemented.
        }

        if path_join_buf(&[src_bucket, src_object]) != path_join_buf(&[dst_bucket, dst_object]) {
            return Err(StorageError::NotImplemented);
        }

        let _lock_guard = if dst_opts.no_lock {
            None
        } else {
            Some(
                self.acquire_write_lock_diag("copy_object_metadata", dst_bucket, dst_object)
                    .await?,
            )
        };

        self.invalidate_get_object_metadata_cache(dst_bucket, dst_object).await;

        if dst_opts.http_preconditions.is_some()
            && let Some(err) = self.check_write_precondition(dst_bucket, dst_object, dst_opts).await
        {
            return Err(err);
        }

        let disks = self.get_disks_internal().await;

        let (mut metas, errs) = {
            if let Some(vid) = &src_opts.version_id {
                Self::read_all_fileinfo(&disks, "", src_bucket, src_object, vid, true, false, false).await?
            } else {
                Self::read_all_xl(&disks, src_bucket, src_object, true, false).await
            }
        };

        let (read_quorum, write_quorum) = match Self::object_quorum_from_meta(&metas, &errs, self.default_parity_count) {
            Ok((r, w)) => (
                usize::try_from(r)
                    .map_err(|_| to_object_err(DiskError::ErasureReadQuorum.into(), vec![src_bucket, src_object]))?,
                usize::try_from(w)
                    .map_err(|_| to_object_err(DiskError::ErasureWriteQuorum.into(), vec![src_bucket, src_object]))?,
            ),
            Err(mut err) => {
                if err == DiskError::ErasureReadQuorum
                    && !src_bucket.starts_with(RUSTFS_META_BUCKET)
                    && self
                        .delete_if_dangling(src_bucket, src_object, &metas, &errs, &HashMap::new(), src_opts.clone())
                        .await
                        .is_ok()
                {
                    if src_opts.version_id.is_some() {
                        err = DiskError::FileVersionNotFound
                    } else {
                        err = DiskError::FileNotFound
                    }
                }
                return Err(to_object_err(err.into(), vec![src_bucket, src_object]));
            }
        };

        let src_version_id = src_opts.version_id.as_deref().unwrap_or_default();
        let (online_disks, mut fi, _) =
            Self::select_valid_fileinfo(&disks, &metas, &errs, src_version_id, read_quorum, write_quorum)
                .map_err(|e| to_object_err(e.into(), vec![src_bucket, src_object]))?;

        if fi.deleted {
            if src_opts.version_id.is_none() {
                return Err(to_object_err(Error::FileNotFound, vec![src_bucket, src_object]));
            }
            return Err(to_object_err(Error::MethodNotAllowed, vec![src_bucket, src_object]));
        }

        let version_id = {
            if src_info.version_only {
                if let Some(vid) = &dst_opts.version_id {
                    Some(Uuid::parse_str(vid)?)
                } else {
                    Some(Uuid::new_v4())
                }
            } else {
                src_info.version_id
            }
        };

        fi.metadata = (*src_info.user_defined).clone();

        if let Some(etag) = &src_info.etag {
            fi.metadata.insert("etag".to_owned(), etag.clone());
        }

        let mod_time = OffsetDateTime::now_utc();
        fi.mod_time = Some(mod_time);
        fi.version_id = version_id;
        fi.versioned = src_opts.versioned || src_opts.version_suspended;

        if src_info.version_only {
            let inline_data = fi.inline_data();

            for fi in metas.iter_mut() {
                if fi.has_valid_erasure_geometry() {
                    fi.metadata = (*src_info.user_defined).clone();
                    if let Some(etag) = &src_info.etag {
                        fi.metadata.insert("etag".to_owned(), etag.clone());
                    }
                    fi.mod_time = Some(mod_time);
                    fi.version_id = version_id;
                    fi.versioned = src_opts.versioned || src_opts.version_suspended;

                    if !fi.inline_data() {
                        fi.data = None;
                    }

                    if inline_data {
                        fi.set_inline_data();
                    }
                }
            }

            Self::write_unique_file_info(&online_disks, "", src_bucket, src_object, &metas, write_quorum)
                .await
                .map_err(|e| to_object_err(e.into(), vec![src_bucket, src_object]))?;
        } else {
            self.update_object_meta_with_opts(
                src_bucket,
                src_object,
                fi.clone(),
                &online_disks,
                &UpdateMetadataOpts {
                    replace_user_metadata: true,
                    ..Default::default()
                },
            )
            .await
            .map_err(|e| to_object_err(e.into(), vec![src_bucket, src_object]))?;
        }

        self.invalidate_get_object_metadata_cache(src_bucket, src_object).await;

        Ok(ObjectInfo::from_file_info(
            &fi,
            src_bucket,
            src_object,
            src_opts.versioned || src_opts.version_suspended,
        ))
    }
    #[tracing::instrument(skip(self))]
    async fn delete_object_version(&self, bucket: &str, object: &str, fi: &FileInfo, force_del_marker: bool) -> Result<()> {
        let disks = self.disk_inventory().await;
        let write_quorum = disks.len() / 2 + 1;
        let rollback_dir = Uuid::new_v4();

        let mut futures = Vec::with_capacity(disks.len());
        let mut errs = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            futures.push(async move {
                if let Some(disk) = disk {
                    match disk
                        .delete_version(
                            bucket,
                            object,
                            fi.clone(),
                            force_del_marker,
                            DeleteOptions {
                                old_data_dir: Some(rollback_dir),
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(r) => Ok(r),
                        Err(e) => Err(e),
                    }
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(_) => {
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e));
                }
            }
        }

        let quorum_result = resolve_tiered_decommission_write_quorum_result(&errs, write_quorum, bucket, object);
        let should_rollback = quorum_result.is_err();
        let mut rollback_futures = Vec::new();
        for (index, err) in errs.iter().enumerate() {
            // backlog#1158: when rolling back, fan the idempotent undo out to every
            // online disk (each self-decides from its staged backup: restore if the
            // rollback dir is present, no-op otherwise). This covers a disk that
            // staged + applied the delete and *then* errored, which the plain
            // `err.is_some()` skip would leave deleted while its peers were restored.
            // On success only the successful disks' backup dirs need cleaning; errored
            // disks' residue is reclaimed by heal/scanner.
            if !should_rollback && err.is_some() {
                continue;
            }

            let Some(disk) = disks[index].as_ref() else {
                continue;
            };

            let disk = disk.clone();
            let bucket = bucket.to_string();
            let object = object.to_string();
            let fi = fi.clone();
            rollback_futures.push(async move {
                if should_rollback {
                    if let Err(err) = disk
                        .delete_version(
                            &bucket,
                            &object,
                            fi,
                            force_del_marker,
                            DeleteOptions {
                                undo_write: true,
                                undo_delete: true,
                                old_data_dir: Some(rollback_dir),
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        warn!(
                            bucket = %bucket,
                            object = %object,
                            rollback_dir = %rollback_dir,
                            error = ?err,
                            "failed to roll back delete after write quorum failure"
                        );
                    }
                } else {
                    let rollback_path = format!("{object}/{rollback_dir}");
                    if let Err(err) = disk
                        .delete(
                            &bucket,
                            &rollback_path,
                            DeleteOptions {
                                recursive: true,
                                immediate: true,
                                ..Default::default()
                            },
                        )
                        .await
                        && err != DiskError::FileNotFound
                        && err != DiskError::VolumeNotFound
                    {
                        warn!(
                            bucket = %bucket,
                            object = %object,
                            rollback_dir = %rollback_dir,
                            error = ?err,
                            "failed to clean delete rollback state after quorum success"
                        );
                    }
                }
            });
        }

        join_all(rollback_futures).await;
        quorum_result
    }

    #[tracing::instrument(skip(self))]
    async fn delete_objects(
        &self,
        bucket: &str,
        objects: Vec<ObjectToDelete>,
        opts: ObjectOptions,
    ) -> (Vec<DeletedObject>, Vec<Option<Error>>) {
        for object in &objects {
            self.invalidate_get_object_metadata_cache(bucket, &object.object_name).await;
        }

        // Default return value
        let mut del_objects = vec![DeletedObject::default(); objects.len()];

        let mut del_errs = Vec::with_capacity(objects.len());

        for _ in 0..objects.len() {
            del_errs.push(None)
        }

        // Acquire locks in batch mode (best effort, matching previous behavior)
        let mut batch = rustfs_lock::BatchLockRequest::new(self.locker_owner.as_str()).with_all_or_nothing(false);
        let mut unique_objects: HashSet<String> = HashSet::new();
        for dobj in &objects {
            if unique_objects.insert(dobj.object_name.clone()) {
                batch = batch.add_write_lock(ObjectKey::new(bucket, dobj.object_name.clone()));
            }
        }
        let unique_lock_count = batch.requests.len();

        let mut failed_map = HashMap::new();
        let mut _local_batch_guards: Vec<FastLockGuard> = Vec::with_capacity(batch.requests.len());
        let mut locked_objects = HashSet::new();

        let dist_erasure = runtime_sources::setup_is_dist_erasure().await;
        let mut dist_batch_lock_ids = vec![Vec::new(); self.lockers.len()];

        if opts.no_lock {
            locked_objects = unique_objects;
        } else if dist_erasure {
            (failed_map, locked_objects, dist_batch_lock_ids) = self.acquire_dist_delete_object_locks_batch(&batch).await;
        } else {
            let batch_result = self.local_lock_manager.acquire_locks_batch(batch).await;
            _local_batch_guards = batch_result.guards;

            for key in batch_result.successful_locks {
                locked_objects.insert(key.object.as_ref().to_string());
            }

            for (key, err) in batch_result.failed_locks {
                failed_map.insert((key.bucket.as_ref().to_string(), key.object.as_ref().to_string()), format!("{err:?}"));
            }
        }

        if issue3031_diag_enabled() {
            let failed_lock_count = failed_map.len();
            let locked_object_count = locked_objects.len();
            let dist_lock_id_count = dist_batch_lock_ids.iter().map(Vec::len).sum::<usize>();
            warn!(
                target: "rustfs_ecstore::set_disk",
                bucket = %bucket,
                requested_object_count = objects.len(),
                unique_lock_count,
                locked_object_count,
                failed_lock_count,
                dist_erasure,
                dist_lock_id_count,
                failed_objects = ?failed_map.keys().collect::<Vec<_>>(),
                "issue3031_delete_objects_lock_batch_context"
            );
        }

        // Mark failures for objects that could not be locked
        for (i, dobj) in objects.iter().enumerate() {
            if let Some(err) = failed_map.get(&(bucket.to_string(), dobj.object_name.clone())) {
                del_errs[i] = Some(Error::other(err.to_string()));
            }
        }

        let ver_cfg = BucketVersioningSys::get(bucket).await.unwrap_or_default();

        // backlog#929 (HP-8): the per-object stat below exists solely to feed
        // check_object_lock_delete (#4297). Resolve the bucket lock
        // configuration once (in-memory cache) and skip the whole stat fanout
        // for buckets without Object Lock; unknown metadata fails closed and
        // keeps the stat, so the #4297 protection is preserved verbatim for
        // every object-lock-enabled bucket.
        let object_lock_checks_required = object_lock_delete_check_required(metadata_sys::get(bucket).await.ok().as_deref());

        let mut vers_map: HashMap<&String, FileInfoVersions> = HashMap::new();

        for (i, dobj) in objects.iter().enumerate() {
            if del_errs[i].is_some() {
                continue;
            }

            let explicit_null_version = is_explicit_null_version(dobj.version_id);
            let version_id = delete_file_info_version_id(dobj.version_id);
            if object_lock_checks_required {
                let check_opts = ObjectOptions {
                    version_id: version_id.map(|version_id| version_id.to_string()),
                    versioned: ver_cfg.prefix_enabled(dobj.object_name.as_str()),
                    version_suspended: ver_cfg.suspended(),
                    object_lock_delete: opts.object_lock_delete.clone(),
                    no_lock: true,
                    ..Default::default()
                };
                let (goi, _write_quorum, gerr) = self.get_object_info_and_quorum(bucket, &dobj.object_name, &check_opts).await;
                if gerr.is_none()
                    && let Err(err) = check_object_lock_delete(bucket, &dobj.object_name, &goi, &check_opts).await
                {
                    del_errs[i] = Some(err);
                    continue;
                }
            }

            let mut vr = FileInfo {
                name: dobj.object_name.clone(),
                version_id,
                idx: i,
                replication_state_internal: Some(dobj.replication_state()),
                ..Default::default()
            };

            vr.set_tier_free_version_id(&Uuid::new_v4().to_string());

            // Delete
            // del_objects[i].object_name.clone_from(&vr.name);
            // del_objects[i].version_id = vr.version_id.map(|v| v.to_string());

            if dobj.version_id.is_none() {
                let (suspended, versioned) = (ver_cfg.suspended(), ver_cfg.prefix_enabled(dobj.object_name.as_str()));
                if suspended || versioned {
                    vr.mod_time = Some(OffsetDateTime::now_utc());
                    vr.deleted = true;
                    if versioned {
                        vr.version_id = Some(Uuid::new_v4());
                    }
                }
            }

            let v = {
                if vers_map.contains_key(&dobj.object_name) {
                    let val = vers_map.get_mut(&dobj.object_name).unwrap();
                    val.versions.push(vr.clone());
                    val.clone()
                } else {
                    FileInfoVersions {
                        name: vr.name.clone(),
                        versions: vec![vr.clone()],
                        ..Default::default()
                    }
                }
            };

            if vr.deleted {
                del_objects[i] = DeletedObject {
                    delete_marker: vr.deleted,
                    delete_marker_version_id: vr.version_id,
                    delete_marker_mtime: vr.mod_time,
                    object_name: vr.name.clone(),
                    replication_state: vr.replication_state_internal.clone(),
                    ..Default::default()
                }
            } else {
                del_objects[i] = DeletedObject {
                    object_name: vr.name.clone(),
                    version_id: if explicit_null_version {
                        Some(Uuid::nil())
                    } else {
                        vr.version_id
                    },
                    replication_state: vr.replication_state_internal.clone(),
                    ..Default::default()
                }
            }

            // Only add to vers_map if we hold the lock
            if locked_objects.contains(&dobj.object_name) {
                vers_map.insert(&dobj.object_name, v);
            }
        }

        let mut vers = Vec::with_capacity(vers_map.len());

        for (_, mut fi_vers) in vers_map {
            fi_vers.versions.sort_by_key(|a| a.deleted);

            if let Some(index) = fi_vers.versions.iter().position(|fi| fi.deleted) {
                fi_vers.versions.truncate(index + 1);
            }

            vers.push(fi_vers);
        }

        let rollback_dir = Uuid::new_v4();

        let disks = self.disks.read().await;

        let disks = disks.clone();

        let mut futures = Vec::with_capacity(disks.len());

        // let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let vers = vers.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.delete_versions(
                        bucket,
                        vers,
                        DeleteOptions {
                            old_data_dir: Some(rollback_dir),
                            ..Default::default()
                        },
                    )
                    .await
                } else {
                    let mut errs = Vec::with_capacity(vers.len());
                    for _ in 0..vers.len() {
                        errs.push(Some(DiskError::DiskNotFound));
                    }
                    errs
                }
            });
        }

        let results = join_all(futures).await;

        let mut del_obj_errs: Vec<Vec<Option<DiskError>>> = vec![vec![None; objects.len()]; disks.len()];

        // For each disk delete all objects
        for (disk_idx, errors) in results.into_iter().enumerate() {
            // Deletion results for all objects
            for idx in 0..vers.len() {
                if errors[idx].is_some() {
                    for fi in vers[idx].versions.iter() {
                        del_obj_errs[disk_idx][fi.idx] = errors[idx].clone();
                    }
                }
            }
        }

        for obj_idx in 0..objects.len() {
            let mut disk_err = vec![None; disks.len()];

            for disk_idx in 0..disks.len() {
                if del_obj_errs[disk_idx][obj_idx].is_some() {
                    disk_err[disk_idx] = del_obj_errs[disk_idx][obj_idx].clone();
                }
            }

            let mut has_err = reduce_write_quorum_errs(&disk_err, OBJECT_OP_IGNORED_ERRS, disks.len() / 2 + 1);
            if let Some(err) = has_err.clone() {
                let er = err.into();
                if (is_err_object_not_found(&er) || is_err_version_not_found(&er)) && !del_objects[obj_idx].delete_marker {
                    has_err = None;
                }
            } else {
                del_objects[obj_idx].found = true;
            }

            if let Some(err) = has_err {
                if del_objects[obj_idx].version_id.is_some() {
                    del_errs[obj_idx] = Some(to_object_err(
                        err.into(),
                        vec![
                            bucket,
                            &objects[obj_idx].object_name.clone(),
                            &objects[obj_idx].version_id.unwrap_or_default().to_string(),
                        ],
                    ));
                } else {
                    del_errs[obj_idx] = Some(to_object_err(err.into(), vec![bucket, &objects[obj_idx].object_name.clone()]));
                }
            }
        }

        self.record_capacity_scope_if_needed(opts.capacity_scope_token, &disks);

        let mut rollback_futures = Vec::new();
        for fi_vers in &vers {
            // delete_versions commits one xl.meta per object group, so rollback must use the same boundary.
            let should_rollback = fi_vers.versions.iter().any(|fi| del_errs[fi.idx].is_some());
            for (disk_idx, disk) in disks.iter().enumerate() {
                // backlog#1158: on rollback, include every online disk so a disk that
                // staged + applied the delete and then errored is still restored (the
                // disk-side undo is idempotent, no-op when nothing was staged). On
                // success, skip the errored disks and only clean up successful ones.
                if !should_rollback && fi_vers.versions.iter().any(|fi| del_obj_errs[disk_idx][fi.idx].is_some()) {
                    continue;
                }

                let Some(disk) = disk.as_ref() else {
                    continue;
                };

                let disk = disk.clone();
                let bucket = bucket.to_string();
                let object = fi_vers.name.clone();
                let versions = fi_vers.clone();
                rollback_futures.push(async move {
                    if should_rollback {
                        let errs = disk
                            .delete_versions(
                                &bucket,
                                vec![versions],
                                DeleteOptions {
                                    undo_write: true,
                                    undo_delete: true,
                                    old_data_dir: Some(rollback_dir),
                                    ..Default::default()
                                },
                            )
                            .await;
                        if let Some(err) = errs.into_iter().flatten().next() {
                            warn!(
                                bucket = %bucket,
                                object = %object,
                                rollback_dir = %rollback_dir,
                                error = ?err,
                                "failed to roll back batch delete after write quorum failure"
                            );
                        }
                    } else {
                        let rollback_path = format!("{object}/{rollback_dir}");
                        if let Err(err) = disk
                            .delete(
                                &bucket,
                                &rollback_path,
                                DeleteOptions {
                                    recursive: true,
                                    immediate: true,
                                    ..Default::default()
                                },
                            )
                            .await
                            && err != DiskError::FileNotFound
                            && err != DiskError::VolumeNotFound
                        {
                            warn!(
                                bucket = %bucket,
                                object = %object,
                                rollback_dir = %rollback_dir,
                                error = ?err,
                                "failed to clean batch delete rollback state after quorum success"
                            );
                        }
                    }
                });
            }
        }

        join_all(rollback_futures).await;

        // TODO: add_partial

        if dist_erasure {
            self.release_dist_delete_object_locks_batch(dist_batch_lock_ids).await;
        }

        for (object, err) in objects.iter().zip(del_errs.iter()) {
            if err.is_none() {
                self.invalidate_get_object_metadata_cache(bucket, &object.object_name).await;
            }
        }

        (del_objects, del_errs)
    }

    #[tracing::instrument(skip(self))]
    async fn delete_object(&self, bucket: &str, object: &str, mut opts: ObjectOptions) -> Result<ObjectInfo> {
        self.invalidate_get_object_metadata_cache(bucket, object).await;

        // Guard lock for single object delete
        let _lock_guard = if (!opts.delete_prefix || opts.delete_prefix_object) && !opts.no_lock {
            Some(self.acquire_write_lock_diag("delete_object", bucket, object).await?)
        } else {
            None
        };
        if opts.delete_prefix {
            self.delete_prefix(bucket, object)
                .await
                .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;

            self.invalidate_all_get_object_metadata_cache();
            return Ok(ObjectInfo::default());
        }

        // TODO: Lifecycle

        let mut version_found = true;
        let (mut goi, write_quorum, gerr) = self.get_object_info_and_quorum(bucket, object, &opts).await;
        if let Some(err) = &gerr
            && goi.name.is_empty()
        {
            if should_force_delete_marker_for_missing_version(&opts) {
                version_found = false;
            } else {
                return Err(err.clone());
            }
        }

        if version_found {
            check_object_lock_delete(bucket, object, &goi, &opts).await?;
        }

        if opts.transition.expire_restored {
            // Restore-expiry (DeleteRestoredAction / DeleteRestoredVersionAction)
            // must only drop the local restored copy and strip the x-amz-restore
            // headers; the version itself stays transitioned (status=complete)
            // and keeps serving GETs from the tier. Route it before delete-marker
            // resolution and replication dispatch: a delete marker would hide the
            // version, a replicated delete would remove it on the target, and a
            // free-version record would schedule remote tier cleanup.
            if !version_found {
                return Err(gerr.unwrap_or_else(|| StorageError::ObjectNotFound(bucket.to_string(), object.to_string())));
            }
            let dfi = FileInfo {
                name: object.to_string(),
                version_id: goi.version_id,
                mod_time: Some(opts.mod_time.unwrap_or_else(OffsetDateTime::now_utc)),
                expire_restored: true,
                ..Default::default()
            };
            self.delete_object_version(bucket, object, &dfi, false)
                .await
                .map_err(|e| to_object_err(e, vec![bucket, object]))?;
            self.invalidate_get_object_metadata_cache(bucket, object).await;
            return Ok(ObjectInfo::from_file_info(&dfi, bucket, object, opts.versioned || opts.version_suspended));
        }

        let otd = ObjectToDelete {
            object_name: object.to_string(),
            version_id: opts
                .version_id
                .clone()
                .map(|v| Uuid::parse_str(v.as_str()).ok().unwrap_or_default()),
            ..Default::default()
        };

        let dsc = if should_preserve_delete_replication_state(&opts) {
            ReplicateDecision::default()
        } else {
            ReplicationObjectBridge::check_delete(bucket, &otd, &goi, &opts, gerr.map(|e| e.to_string())).await
        };

        if dsc.replicate_any() {
            opts.set_delete_replication_state(dsc);
            goi.replication_decision = opts
                .delete_replication
                .as_ref()
                .map(|v| v.replicate_decision_str.clone())
                .unwrap_or_default();
        }

        let (mark_delete, mut delete_marker) = resolve_delete_version_state(&opts, &goi, version_found);

        let mod_time = if let Some(mt) = opts.mod_time {
            mt
        } else {
            OffsetDateTime::now_utc()
        };

        let find_vid = Uuid::new_v4();

        if mark_delete && (opts.versioned || opts.version_suspended) {
            if !delete_marker {
                delete_marker = opts.version_suspended && opts.version_id.is_none();
            }

            let mut fi = FileInfo {
                name: object.to_string(),
                deleted: delete_marker,
                mark_deleted: mark_delete,
                mod_time: Some(mod_time),
                replication_state_internal: opts.delete_replication.as_ref().map(replication_state_to_filemeta),
                ..Default::default() // TODO: Transition
            };

            fi.set_tier_free_version_id(&find_vid.to_string());

            if opts.skip_free_version {
                fi.set_skip_tier_free_version();
            }

            fi.version_id = if let Some(vid) = opts.version_id.as_ref() {
                Some(Uuid::parse_str(vid.as_str())?)
            } else if opts.versioned {
                Some(Uuid::new_v4())
            } else {
                None
            };

            self.delete_object_version(bucket, object, &fi, should_force_delete_marker_for_missing_version(&opts))
                .await
                .map_err(|e| to_object_err(e, vec![bucket, object]))?;

            let disks = self.disk_inventory().await;
            self.record_capacity_scope_if_needed(opts.capacity_scope_token, &disks);

            let mut oi = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);
            oi.replication_decision = goi.replication_decision;
            self.invalidate_get_object_metadata_cache(bucket, object).await;
            return Ok(oi);
        }

        // Create a single object deletion request
        let mut dfi = FileInfo {
            name: object.to_string(),
            version_id: opts.version_id.as_ref().and_then(|v| Uuid::parse_str(v).ok()),
            mark_deleted: mark_delete,
            deleted: delete_marker,
            mod_time: Some(mod_time),
            replication_state_internal: opts.delete_replication.as_ref().map(replication_state_to_filemeta),
            ..Default::default()
        };

        dfi.set_tier_free_version_id(&find_vid.to_string());

        if opts.skip_free_version {
            dfi.set_skip_tier_free_version();
        }

        self.delete_object_version(bucket, object, &dfi, opts.delete_marker)
            .await
            .map_err(|e| to_object_err(e, vec![bucket, object]))?;

        let disks = self.disk_inventory().await;
        self.record_capacity_scope_if_needed(opts.capacity_scope_token, &disks);

        let mut obj_info = ObjectInfo::from_file_info(&dfi, bucket, object, opts.versioned || opts.version_suspended);
        obj_info.size = goi.size;
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        Ok(obj_info)
    }

    #[tracing::instrument(skip(self))]
    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        crate::hp_guard!("SetDisks::get_object_info");
        // Acquire a shared read-lock to protect consistency during info fetch
        let _read_lock_guard = if !opts.no_lock {
            Some(self.acquire_read_lock_diag("get_object_info", bucket, object).await?)
        } else {
            None
        };

        // Use the same full xl.meta read path as GetObject metadata resolution.
        // This avoids HEAD/GetObject metadata visibility skew immediately after
        // PutObject/CompleteMultipartUpload.
        let (fi, _, _) = self
            .get_object_fileinfo(bucket, object, opts, true, false)
            .await
            .map_err(|e| to_object_err(e, vec![bucket, object]))?;

        let oi = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);

        Ok(oi)
    }

    #[tracing::instrument(skip(self))]
    async fn add_partial(&self, bucket: &str, object: &str, version_id: &str) -> Result<()> {
        if let Err(e) =
            rustfs_common::heal_channel::send_heal_request(rustfs_common::heal_channel::create_heal_request_with_options(
                bucket.to_string(),
                Some(object.to_string()),
                false,
                Some(HealChannelPriority::Normal),
                Some(self.pool_index),
                Some(self.set_index),
            ))
            .await
        {
            warn!(
                bucket,
                object,
                version_id,
                error = %e,
                "Failed to enqueue heal request for partial object"
            );
        }
        Ok(())
    }

    #[tracing::instrument(skip(self))]
    async fn put_object_metadata(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.invalidate_get_object_metadata_cache(bucket, object).await;

        // Guard lock for metadata update
        let _lock_guard = if !opts.no_lock {
            Some(self.acquire_write_lock_diag("put_object_metadata", bucket, object).await?)
        } else {
            None
        };

        let disks = self.get_disks_internal().await;

        let (metas, errs) = {
            if let Some(version_id) = &opts.version_id {
                Self::read_all_fileinfo(&disks, "", bucket, object, version_id.to_string().as_str(), false, false, false).await?
            } else {
                Self::read_all_xl(&disks, bucket, object, false, false).await
            }
        };

        let (read_quorum, write_quorum) = match Self::object_quorum_from_meta(&metas, &errs, self.default_parity_count) {
            Ok((read_quorum, write_quorum)) => (read_quorum, write_quorum),
            Err(mut err) => {
                if err == DiskError::ErasureReadQuorum
                    && !bucket.starts_with(RUSTFS_META_BUCKET)
                    && self
                        .delete_if_dangling(bucket, object, &metas, &errs, &HashMap::new(), opts.clone())
                        .await
                        .is_ok()
                {
                    if opts.version_id.is_some() {
                        err = DiskError::FileVersionNotFound
                    } else {
                        err = DiskError::FileNotFound
                    }
                }
                return Err(to_object_err(err.into(), vec![bucket, object]));
            }
        };

        let read_quorum =
            usize::try_from(read_quorum).map_err(|_| to_object_err(DiskError::ErasureReadQuorum.into(), vec![bucket, object]))?;
        let write_quorum = usize::try_from(write_quorum)
            .map_err(|_| to_object_err(DiskError::ErasureWriteQuorum.into(), vec![bucket, object]))?;

        let version_id = opts.version_id.as_deref().unwrap_or_default();
        let (online_disks, mut fi, _) = Self::select_valid_fileinfo(&disks, &metas, &errs, version_id, read_quorum, write_quorum)
            .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;

        if fi.deleted {
            return Err(to_object_err(Error::MethodNotAllowed, vec![bucket, object]));
        }

        let obj_info = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);

        check_object_lock_retention_update(bucket, object, &obj_info, opts)?;

        for (k, v) in obj_info.user_defined.iter() {
            fi.metadata.insert(k.clone(), v.clone());
        }

        if let Some(mt) = &opts.eval_metadata {
            for (k, v) in mt {
                fi.metadata.insert(k.clone(), v.clone());
            }
        }

        if opts.mod_time.is_some() {
            fi.mod_time = opts.mod_time;
        }
        if let Some(ref version_id) = opts.version_id {
            fi.version_id = Uuid::parse_str(version_id).ok();
        }

        self.update_object_meta(bucket, object, fi.clone(), &online_disks)
            .await
            .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;

        self.invalidate_get_object_metadata_cache(bucket, object).await;

        Ok(ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended))
    }

    #[tracing::instrument(skip(self))]
    async fn get_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<String> {
        let oi = self.get_object_info(bucket, object, opts).await?;
        Ok((*oi.user_tags).clone())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn transition_object(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let tier_config_mgr = self.ctx.tier_config_mgr();
        let tgt_client = match TierConfigMgr::acquire_operation_lease(&tier_config_mgr, &opts.transition.tier).await {
            Ok(client) => client,
            Err(err) => {
                return Err(Error::other(format!("remote tier error: {err}")));
            }
        };

        // Acquire write-lock early; hold for the whole transition operation scope
        // if !opts.no_lock {
        //     let guard_opt = self
        //         .namespace_lock
        //         .lock_guard(object, &self.locker_owner, Duration::from_secs(5), Duration::from_secs(10))
        //         .await?;
        //     if guard_opt.is_none() {
        //         return Err(Error::other("can not get lock. please retry".to_string()));
        //     }
        //     _lock_guard = guard_opt;
        // }

        let (mut fi, meta_arr, online_disks) = self.get_object_fileinfo(bucket, object, opts, true, false).await?;
        /*if err != nil {
            return Err(to_object_err(err, vec![bucket, object]));
        }*/
        /*if fi.deleted {
            if opts.version_id.is_none() {
                return Err(to_object_err(DiskError::FileNotFound, vec![bucket, object]));
            }
            return Err(to_object_err(ERR_METHOD_NOT_ALLOWED, vec![bucket, object]));
        }*/
        // Normalize ETags by removing quotes before comparison (PR #592 compatibility)
        let transition_etag = rustfs_utils::path::trim_etag(&opts.transition.etag);
        let stored_etag = rustfs_utils::path::trim_etag(&get_raw_etag(&fi.metadata));
        if let Some(mod_time1) = opts.mod_time {
            if let Some(mod_time2) = fi.mod_time.as_ref() {
                if mod_time1.unix_timestamp() != mod_time2.unix_timestamp()
                    || (!transition_etag.is_empty() && transition_etag != stored_etag)
                {
                    return Err(to_object_err(Error::other(DiskError::FileNotFound), vec![bucket, object]));
                }
            } else {
                return Err(Error::other("mod_time 2 error.".to_string()));
            }
        } else {
            return Err(Error::other("mod_time 1 error.".to_string()));
        }
        if fi.transition_status == TRANSITION_COMPLETE {
            return Ok(());
        }

        /*if fi.xlv1 {
            if let Err(err) = self.heal_object(bucket, object, "", &HealOpts {no_lock: true, ..Default::default()}) {
                return err.expect("err");
            }
            (fi, meta_arr, online_disks) = self.get_object_fileinfo(&bucket, &object, &opts, true, false);
            if err != nil {
                return to_object_err(err, vec![bucket, object]);
            }
        }*/

        let dest_obj = gen_transition_objname(bucket);
        if let Err(err) = dest_obj {
            return Err(to_object_err(err, vec![]));
        }
        let dest_obj = dest_obj?;

        let oi = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);
        let mut transition_meta = (*oi.user_defined).clone();
        transition_meta.insert("name".to_string(), object.to_string());

        if let Some(content_type) = oi.content_type.as_ref().filter(|value| !value.is_empty()) {
            transition_meta.insert(CONTENT_TYPE.to_ascii_lowercase(), content_type.clone());
        }

        for header in [
            CONTENT_ENCODING,
            CONTENT_LANGUAGE,
            CONTENT_DISPOSITION,
            CACHE_CONTROL,
            EXPIRES,
            X_AMZ_OBJECT_LOCK_MODE.as_str(),
            X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str(),
            X_AMZ_OBJECT_LOCK_LEGAL_HOLD.as_str(),
        ] {
            if let Some(value) = fi.metadata.lookup(header).filter(|value| !value.is_empty()) {
                transition_meta.insert(header.to_ascii_lowercase(), value.to_string());
            }
        }

        let expected_size = u64::try_from(fi.size).map_err(|_| StorageError::FileCorrupt)?;
        let (pr, pw) = tokio::io::duplex(fi.erasure.block_size);
        let consumed = Arc::new(AtomicU64::new(0));
        let reader = ReaderImpl::ObjectBody(GetObjectReader {
            stream: Box::new(TransitionUploadReader::new(pr, Arc::clone(&consumed))),
            object_info: oi,
            buffered_body: None,
            body_source: GetObjectBodySource::Unprobed,
        });

        let cloned_bucket = bucket.to_string();
        let cloned_object = object.to_string();
        let cloned_fi = fi.clone();
        let set_index = self.set_index;
        let pool_index = self.pool_index;
        let skip_verify = opts.skip_verify_bitrot;
        let metrics_size_bucket = rustfs_io_metrics::get_object_size_bucket(cloned_fi.size);
        let producer = async move {
            let mut writer = TransitionUploadWriter::new(pw);
            Self::get_object_with_fileinfo(
                &cloned_bucket,
                &cloned_object,
                0,
                cloned_fi.size,
                &mut writer,
                cloned_fi,
                meta_arr,
                &online_disks,
                set_index,
                pool_index,
                skip_verify,
                false,
                GET_OBJECT_PATH_LEGACY_DUPLEX,
                GET_CODEC_STREAMING_OBJECT_CLASS_PLAIN_SINGLE_PART,
                metrics_size_bucket,
            )
            .await?;
            writer.shutdown().await?;
            Ok(writer.produced())
        };

        let rv = complete_transition_upload(
            tgt_client.put_with_meta(&dest_obj, reader, fi.size, transition_meta),
            producer,
            expected_size,
            consumed,
        )
        .await;
        let candidate = match rv {
            Ok(completion) => completion.candidate,
            Err(failure) => {
                if let Some(candidate) = failure.candidate {
                    let mut upload_cleanup = TransitionUploadCleanup::new(tgt_client, &dest_obj, candidate);
                    let _cleanup_result = upload_cleanup.cleanup().await;
                }
                return Err(failure.error);
            }
        };

        let transition_version_id = match parse_transition_version_id(candidate.remote_version()) {
            Ok(version_id) => version_id,
            Err(err) => {
                let mut upload_cleanup = TransitionUploadCleanup::new(tgt_client, &dest_obj, candidate);
                let _cleanup_result = upload_cleanup.cleanup().await;
                return Err(err.into());
            }
        };
        let mut upload_cleanup = TransitionUploadCleanup::new(tgt_client, &dest_obj, candidate);

        let mut commit_opts = opts.clone();
        commit_opts.no_lock = true;
        commit_opts.metadata_cache_safe = false;
        let transition_lock_guard = if opts.no_lock {
            None
        } else {
            match self.acquire_write_lock_diag("transition_object_commit", bucket, object).await {
                Ok(guard) => Some(guard),
                Err(err) => {
                    let _cleanup_result = upload_cleanup.cleanup().await;
                    return Err(err);
                }
            }
        };
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        let current = self.get_object_fileinfo(bucket, object, &commit_opts, true, false).await;
        let (mut current_fi, _, _) = match current {
            Ok(current) => current,
            Err(err) => {
                drop(transition_lock_guard);
                let _cleanup_result = upload_cleanup.cleanup().await;
                return Err(err);
            }
        };
        let source_matches = current_fi.version_id == fi.version_id
            && current_fi.data_dir == fi.data_dir
            && current_fi.mod_time == fi.mod_time
            && current_fi.size == fi.size
            && rustfs_utils::path::trim_etag(&get_raw_etag(&current_fi.metadata)) == stored_etag;
        if current_fi.transition_status == TRANSITION_COMPLETE || !source_matches {
            let already_transitioned = current_fi.transition_status == TRANSITION_COMPLETE;
            drop(transition_lock_guard);
            let _cleanup_result = upload_cleanup.cleanup().await;
            if already_transitioned {
                return Ok(());
            }
            return Err(to_object_err(Error::other(DiskError::FileNotFound), vec![bucket, object]));
        }

        current_fi.transition_status = TRANSITION_COMPLETE.to_string();
        current_fi.transitioned_objname = dest_obj;
        current_fi.transition_tier = opts.transition.tier.clone();
        current_fi.transition_version_id = transition_version_id;
        rustfs_utils::http::metadata_compat::insert_str(
            &mut current_fi.metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            rustfs_utils::crypto::hex(upload_cleanup.lease.backend_identity()),
        );
        fi = current_fi;
        let event_name = EventName::LifecycleTransition.as_str();

        #[cfg(test)]
        pause_transition_commit(bucket, object, TransitionCommitPause::BeforeLockLost).await;
        if transition_lock_guard.as_ref().is_some_and(|guard| guard.is_lock_lost()) {
            drop(transition_lock_guard);
            let _cleanup_result = upload_cleanup.cleanup().await;
            return Err(StorageError::NamespaceLockQuorumUnavailable {
                mode: "transition_object_commit",
                bucket: bucket.to_string(),
                object: object.to_string(),
                required: 1,
                achieved: 0,
            });
        }
        #[cfg(test)]
        pause_transition_commit(bucket, object, TransitionCommitPause::BeforeLeaseValidation).await;
        if !upload_cleanup.lease.is_current_generation() {
            drop(transition_lock_guard);
            let _cleanup_result = upload_cleanup.cleanup().await;
            return Err(Error::other("remote tier configuration changed during transition"));
        }
        #[cfg(test)]
        pause_transition_commit(bucket, object, TransitionCommitPause::AfterLeaseValidation).await;
        upload_cleanup.disarm();
        if let Err(err) = self.delete_object_version(bucket, object, &fi, false).await {
            warn!(
                bucket = bucket,
                object = object,
                error = ?err,
                "transition remote upload completed but local commit failed"
            );
            self.invalidate_get_object_metadata_cache(bucket, object).await;
            drop(transition_lock_guard);
            return Err(err);
        }

        // delete_object_version persisted transition_status=complete and freed the
        // local data, but does not touch the GET metadata cache. Drop any cached
        // pre-transition entry so a late duplicate transition task (or a plain GET)
        // re-reads the fresh state; a stale hit here defeats the TRANSITION_COMPLETE
        // early-return above and streams the already-deleted local data to the
        // remote tier again (rustfs/rustfs#4827).
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        drop(transition_lock_guard);
        let disks = self.disk_inventory().await;
        self.record_capacity_scope_if_needed(opts.capacity_scope_token, &disks);

        for disk in disks.iter() {
            if let Some(disk) = disk {
                continue;
            }
            let _ = self
                .add_partial(bucket, object, opts.version_id.as_deref().unwrap_or_default())
                .await;
            break;
        }

        let obj_info = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);
        send_event(EventArgs {
            event_name: event_name.to_string(),
            bucket_name: bucket.to_string(),
            object: obj_info,
            user_agent: "Internal: [ILM-Transition]".to_string(),
            host: runtime_sources::default_local_node_name(),
            ..Default::default()
        });
        //let tags = opts.lifecycle_audit_event.tags();
        //auditLogLifecycle(ctx, objInfo, ILMTransition, tags, traceFn)
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn restore_transitioned_object(self: Arc<Self>, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        // Acquire write-lock early for the restore operation
        // if !opts.no_lock {
        //     let guard_opt = self
        //         .namespace_lock
        //         .lock_guard(object, &self.locker_owner, Duration::from_secs(5), Duration::from_secs(10))
        //         .await?;
        //     if guard_opt.is_none() {
        //         return Err(Error::other("can not get lock. please retry".to_string()));
        //     }
        //     _lock_guard = guard_opt;
        // }
        let self_ = self.clone();
        let set_restore_header_fn = async move |oi: &mut ObjectInfo, rerr: Option<Error>| -> Result<()> {
            if rerr.is_none() {
                return Ok(());
            }
            self.update_restore_metadata(bucket, object, oi, opts).await?;
            Err(rerr.unwrap())
        };
        let mut oi = ObjectInfo::default();
        let fi = self_.clone().get_object_fileinfo(bucket, object, opts, true, false).await;
        if let Err(err) = fi {
            return set_restore_header_fn(&mut oi, Some(to_object_err(err, vec![bucket, object]))).await;
        }
        let (actual_fi, _, _) = fi?;

        oi = ObjectInfo::from_file_info(&actual_fi, bucket, object, opts.versioned || opts.version_suspended);
        let expected_operation_id = restore_operation_id_from_metadata(&opts.user_defined)?;
        if let Some(expected_operation_id) = expected_operation_id {
            require_restore_operation_id(oi.user_defined.as_ref(), expected_operation_id)?;
        }
        let mut ropts = put_restore_opts(bucket, object, &opts.transition.restore_request, &oi).await?;
        if let Some(expected_operation_id) = expected_operation_id {
            rustfs_utils::http::metadata_compat::insert_str(
                &mut ropts.user_defined,
                SUFFIX_RESTORE_OPERATION_ID,
                expected_operation_id.to_string(),
            );
        }
        let restore_commit_metadata = if let Some(expected_operation_id) = expected_operation_id {
            let mut metadata = HashMap::new();
            metadata.insert(X_AMZ_RESTORE.as_str().to_string(), "ongoing-request=\"false\"".to_string());
            rustfs_utils::http::metadata_compat::insert_str(
                &mut metadata,
                SUFFIX_RESTORE_OPERATION_ID,
                expected_operation_id.to_string(),
            );
            metadata
        } else {
            HashMap::new()
        };
        // The restore copy-back re-writes this same object via put_object /
        // new_multipart_upload / complete_multipart_upload, each of which takes
        // the object write lock in its commit phase. The caller
        // (handle_restore_transitioned_object, #4877) already holds that write
        // lock for the whole restore and forwards no_lock=true, so the inner
        // writes must inherit it or they self-deadlock on the lock we already
        // hold and time out. put_restore_opts builds fresh options that default
        // no_lock=false, so propagate it explicitly here.
        ropts.no_lock = opts.no_lock;
        if oi.parts.len() == 1 {
            let mut opts = opts.clone();
            opts.part_number = Some(1);
            let rs: Option<HTTPRangeSpec> = None;
            let gr = get_transitioned_object_reader_with_tier_manager(
                bucket,
                object,
                &rs,
                &HeaderMap::new(),
                &oi,
                &opts,
                &self_.ctx.tier_config_mgr(),
            )
            .await;
            if let Err(err) = gr {
                return set_restore_header_fn(&mut oi, Some(to_object_err(err.into(), vec![bucket, object]))).await;
            }
            let gr = gr?;
            let reader = BufReader::new(gr.stream);
            let hash_reader = HashReader::from_stream(reader, gr.object_info.size, gr.object_info.size, None, None, false)?;
            let mut p_reader = PutObjReader::new(hash_reader);
            return match self_.clone().put_object(bucket, object, &mut p_reader, &ropts).await {
                Ok(restored_info) => {
                    send_event(EventArgs {
                        event_name: EventName::ObjectRestoreCompleted.as_str().to_string(),
                        bucket_name: bucket.to_string(),
                        object: restored_info,
                        user_agent: "Internal: [Restore-Completed]".to_string(),
                        host: runtime_sources::default_local_node_name(),
                        ..Default::default()
                    });
                    Ok(())
                }
                Err(err) => set_restore_header_fn(&mut oi, Some(to_object_err(err, vec![bucket, object]))).await,
            };
        }

        let res = self_.clone().new_multipart_upload(bucket, object, &ropts).await?;
        //if err != nil {
        //    return set_restore_header_fn(&mut oi, err).await;
        //}

        let mut uploaded_parts: Vec<CompletePart> = vec![];
        let parts = Arc::clone(&oi.parts);
        let mut part_offset: i64 = 0;
        for part_info in parts.iter() {
            let mut part_opts = opts.clone();
            part_opts.part_number = Some(part_info.number);
            if part_info.actual_size <= 0 {
                return set_restore_header_fn(
                    &mut oi,
                    Some(Error::other(format!("invalid multipart restore part size {}", part_info.actual_size))),
                )
                .await;
            }
            let part_end = match part_offset.checked_add(part_info.actual_size - 1) {
                Some(end) => end,
                None => {
                    return set_restore_header_fn(
                        &mut oi,
                        Some(Error::other("multipart restore part range overflow".to_string())),
                    )
                    .await;
                }
            };
            let rs = Some(HTTPRangeSpec {
                is_suffix_length: false,
                start: part_offset,
                end: part_end,
            });
            part_offset = match part_end.checked_add(1) {
                Some(next) => next,
                None => {
                    return set_restore_header_fn(
                        &mut oi,
                        Some(Error::other("multipart restore part offset overflow".to_string())),
                    )
                    .await;
                }
            };
            let gr = match get_transitioned_object_reader_with_tier_manager(
                bucket,
                object,
                &rs,
                &HeaderMap::new(),
                &oi,
                &part_opts,
                &self_.ctx.tier_config_mgr(),
            )
            .await
            {
                Ok(reader) => reader,
                Err(err) => {
                    return set_restore_header_fn(&mut oi, Some(StorageError::Io(err))).await;
                }
            };
            let reader = BufReader::new(gr.stream);
            let hash_reader = HashReader::from_stream(reader, part_info.actual_size, part_info.actual_size, None, None, false)?;
            let mut p_reader = PutObjReader::new(hash_reader);
            let p_info = self_
                .clone()
                .put_object_part(bucket, object, &res.upload_id, part_info.number, &mut p_reader, &ObjectOptions::default())
                .await?;
            //if let Err(err) = p_info {
            //    return set_restore_header_fn(&mut oi, err).await;
            //}
            if p_info.size as i64 != part_info.actual_size {
                return set_restore_header_fn(
                    &mut oi,
                    Some(Error::other(ObjectApiError::InvalidObjectState(GenericError {
                        bucket: bucket.to_string(),
                        object: object.to_string(),
                        ..Default::default()
                    }))),
                )
                .await;
            }
            uploaded_parts.push(CompletePart {
                part_num: p_info.part_num,
                etag: p_info.etag,
                checksum_crc32: None,
                checksum_crc32c: None,
                checksum_sha1: None,
                checksum_sha256: None,
                checksum_crc64nvme: None,
            });
        }
        let restored_info = match self_
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &res.upload_id,
                uploaded_parts,
                &ObjectOptions {
                    mod_time: oi.mod_time,
                    version_id: oi.version_id.map(|version| version.to_string()),
                    user_defined: restore_commit_metadata,
                    // Inherit the restore write lock (see ropts.no_lock above):
                    // the commit phase re-acquires this object's write lock.
                    no_lock: opts.no_lock,
                    ..Default::default()
                },
            )
            .await
        {
            Ok(info) => info,
            Err(err) => return set_restore_header_fn(&mut oi, Some(err)).await,
        };
        send_event(EventArgs {
            event_name: EventName::ObjectRestoreCompleted.as_str().to_string(),
            bucket_name: bucket.to_string(),
            object: restored_info,
            user_agent: "Internal: [Restore-Completed]".to_string(),
            host: runtime_sources::default_local_node_name(),
            ..Default::default()
        });
        Ok(())
    }

    #[tracing::instrument(level = "debug", skip(self))]
    async fn put_object_tags(&self, bucket: &str, object: &str, tags: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        // Acquire write-lock for tag update (metadata write)
        // if !opts.no_lock {
        //     let guard_opt = self
        //         .namespace_lock
        //         .lock_guard(object, &self.locker_owner, Duration::from_secs(5), Duration::from_secs(10))
        //         .await?;
        //     if guard_opt.is_none() {
        //         return Err(Error::other("can not get lock. please retry".to_string()));
        //     }
        //     _lock_guard = guard_opt;
        // }
        // Force the full quorum fanout (allow_early_stop=false): `disks` is the
        // write target below, and an early-stop subset would only carry read
        // quorum, failing write quorum on update_object_meta (backlog#872).
        let (mut fi, _, disks) = self.get_object_fileinfo_gated(bucket, object, opts, false, false).await?;

        fi.metadata.insert(AMZ_OBJECT_TAGGING.to_owned(), tags.to_owned());

        self.update_object_meta(bucket, object, fi.clone(), disks.as_slice()).await?;

        Ok(ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended))
    }

    #[tracing::instrument(skip(self))]
    async fn delete_object_tags(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
        self.put_object_tags(bucket, object, "", opts).await
    }

    #[tracing::instrument(skip(self))]
    async fn verify_object_integrity(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<()> {
        let get_object_reader = <Self as crate::storage_api_contracts::object::ObjectIO>::get_object_reader(
            self,
            bucket,
            object,
            None,
            HeaderMap::new(),
            opts,
        )
        .await?;
        // Stream to sink to avoid loading entire object into memory during verification
        let mut reader = get_object_reader.stream;
        tokio::io::copy(&mut reader, &mut tokio::io::sink()).await?;
        Ok(())
    }
}

/// Null out any disk whose shard writer failed (or was never created) so its
/// truncated/absent shard is not committed by `rename_data`, and return the
/// number of disks that still carry a fully-written shard. Extracted for unit
/// testing the write-quorum accounting (backlog#852 / #799 B3).
fn drop_failed_writer_disks<D, W>(disks: &mut [Option<D>], writers: &[Option<W>]) -> usize {
    let mut committed = 0usize;
    for (slot, writer) in disks.iter_mut().zip(writers.iter()) {
        if writer.is_none() {
            *slot = None;
        } else if slot.is_some() {
            committed += 1;
        }
    }
    committed
}

#[cfg(test)]
mod erasure_construction_tests {
    use super::*;
    use crate::erasure::coding::ErasureConstructionError;
    use std::error::Error as _;

    #[test]
    fn object_file_info_mapping_preserves_construction_error() {
        let mut fi = FileInfo::new("object", 2, 2);
        fi.erasure.block_size = 0;

        let error = match erasure_from_file_info(&fi, false) {
            Ok(_) => panic!("invalid object erasure metadata must be rejected"),
            Err(error) => error,
        };
        assert!(error.to_string().contains("block_size must be greater than zero"));
        let io_source = error.source().expect("StorageError::Io must expose its io::Error source");
        let construction_source = io_source
            .source()
            .expect("io::Error must expose the erasure construction error");
        assert!(construction_source.is::<ErasureConstructionError>());
    }
}

#[cfg(test)]
mod b3_write_quorum_tests {
    use super::drop_failed_writer_disks;

    #[test]
    fn excludes_failed_writers_and_counts_committed() {
        // Writer 2 failed (short write / error -> nulled). Its disk must be
        // dropped so the truncated shard is not renamed or counted.
        let mut disks = vec![Some(0u8), Some(1), Some(2), Some(3)];
        let writers = vec![Some(()), Some(()), None, Some(())];
        assert_eq!(drop_failed_writer_disks(&mut disks, &writers), 3);
        assert_eq!(disks, vec![Some(0), Some(1), None, Some(3)]);
    }

    #[test]
    fn offline_disk_stays_excluded_and_uncounted() {
        // Disk 1 was offline at setup (disk None, writer None): stays None, not counted.
        let mut disks = vec![Some(0u8), None, Some(2)];
        let writers = vec![Some(()), None, Some(())];
        assert_eq!(drop_failed_writer_disks(&mut disks, &writers), 2);
        assert_eq!(disks, vec![Some(0), None, Some(2)]);
    }

    #[test]
    fn all_writers_ok_keeps_every_disk() {
        let mut disks = vec![Some(0u8), Some(1), Some(2)];
        let writers = vec![Some(()), Some(()), Some(())];
        assert_eq!(drop_failed_writer_disks(&mut disks, &writers), 3);
        assert_eq!(disks, vec![Some(0), Some(1), Some(2)]);
    }
}

#[cfg(test)]
pub(in crate::set_disk::ops) mod hermetic_set_disks_support {
    //! Shared hermetic `SetDisks` construction for the ops tests below: the
    //! `SetDisks` under test is built directly on formatted local disks (same
    //! pattern as the `ops/locking.rs` tests) so the tests stay hermetic — no
    //! global local-disk registry, lock clients, or ECStore instance is
    //! touched, and each test owns its temp workspace.

    use super::*;
    use crate::store::init_format::save_format_file;
    use rustfs_lock::client::LockClient;
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    async fn make_formatted_local_disk_for_pool(
        disk_idx: usize,
        pool_index: usize,
        format: &FormatV3,
    ) -> (TempDir, Endpoint, DiskStore) {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let mut endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        endpoint.set_pool_index(pool_index);
        endpoint.set_set_index(0);
        endpoint.set_disk_index(disk_idx);

        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("local disk should be created");

        let mut disk_format = format.clone();
        disk_format.erasure.this = format.erasure.sets[0][disk_idx];
        save_format_file(&Some(disk.clone()), &Some(disk_format))
            .await
            .expect("format should be saved");

        (dir, endpoint, disk)
    }

    pub(in crate::set_disk::ops) async fn hermetic_set_disks(disk_count: usize) -> (Vec<TempDir>, Vec<DiskStore>, Arc<SetDisks>) {
        hermetic_set_disks_for_pool_with_default_parity(disk_count, 0, disk_count / 2).await
    }

    pub(in crate::set_disk::ops) async fn hermetic_set_disks_for_pool_with_default_parity(
        disk_count: usize,
        pool_index: usize,
        default_parity_count: usize,
    ) -> (Vec<TempDir>, Vec<DiskStore>, Arc<SetDisks>) {
        hermetic_set_disks_with_lockers(disk_count, pool_index, default_parity_count, Vec::new()).await
    }

    pub(in crate::set_disk::ops) async fn hermetic_set_disks_with_lockers(
        disk_count: usize,
        pool_index: usize,
        default_parity_count: usize,
        lockers: Vec<Arc<dyn LockClient>>,
    ) -> (Vec<TempDir>, Vec<DiskStore>, Arc<SetDisks>) {
        let format = FormatV3::new(1, disk_count);

        let mut temp_dirs = Vec::with_capacity(disk_count);
        let mut endpoints = Vec::with_capacity(disk_count);
        let mut disk_stores = Vec::with_capacity(disk_count);
        let mut disks = Vec::with_capacity(disk_count);

        for disk_idx in 0..disk_count {
            let (temp_dir, endpoint, disk) = make_formatted_local_disk_for_pool(disk_idx, pool_index, &format).await;
            temp_dirs.push(temp_dir);
            endpoints.push(endpoint);
            disk_stores.push(disk.clone());
            disks.push(Some(disk));
        }

        let set_disks = SetDisks::new(
            "hermetic-ops-test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            disk_count,
            default_parity_count,
            0,
            pool_index,
            endpoints,
            format,
            lockers,
        )
        .await;

        (temp_dirs, disk_stores, set_disks)
    }
}

#[cfg(test)]
mod metadata_mutation_generation_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks;
    use super::*;
    use crate::disk::DiskAPI as _;
    use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};

    async fn put_and_prime(
        set_disks: &Arc<SetDisks>,
        bucket: &str,
        object: &str,
        payload: &[u8],
    ) -> (ObjectInfo, GetObjectMetadataCacheKey) {
        let mut reader = PutObjReader::from_vec(payload.to_vec());
        let info = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("test object should be written");
        set_disks
            .get_object_fileinfo(bucket, object, &ObjectOptions::default(), true, false)
            .await
            .expect("test object metadata should resolve");
        let generation = set_disks
            .get_object_metadata_cache_generation(bucket, object)
            .expect("metadata cache generation should be active");
        let key = GetObjectMetadataCacheKey::new(bucket, object, generation);
        assert!(
            set_disks.get_object_metadata_cache.get(&key).await.is_some(),
            "metadata priming should publish the current generation"
        );
        (info, key)
    }

    async fn assert_retired(set_disks: &SetDisks, key: &GetObjectMetadataCacheKey) {
        set_disks.get_object_metadata_cache.run_pending_tasks().await;
        assert!(
            set_disks.get_object_metadata_cache.get(key).await.is_none(),
            "the mutation must physically retire the prior metadata generation"
        );
    }

    #[tokio::test]
    #[serial_test::serial(metadata_cache_invalidation_probe)]
    async fn metadata_semantic_mutation_generation_matrix_retires_cached_snapshot() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "metadata-mutation-generation-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let put_object = "put-object";
        let (_, put_key) = put_and_prime(&set_disks, bucket, put_object, b"initial PUT body").await;
        let put_probe = MetadataCacheInvalidationProbe::install(bucket, put_object);
        let mut replacement = PutObjReader::from_vec(b"replacement PUT body".to_vec());
        set_disks
            .put_object(bucket, put_object, &mut replacement, &ObjectOptions::default())
            .await
            .expect("replacement PUT should succeed");
        assert_eq!(put_probe.count(), 2, "PUT must invalidate before mutation and after commit");
        assert_retired(&set_disks, &put_key).await;
        drop(put_probe);

        let delete_object = "delete-object";
        let (_, delete_key) = put_and_prime(&set_disks, bucket, delete_object, b"DELETE body").await;
        let delete_probe = MetadataCacheInvalidationProbe::install(bucket, delete_object);
        set_disks
            .delete_object(bucket, delete_object, ObjectOptions::default())
            .await
            .expect("DELETE should succeed");
        assert_eq!(delete_probe.count(), 2, "DELETE must invalidate before mutation and after commit");
        assert_retired(&set_disks, &delete_key).await;
        drop(delete_probe);

        let copy_object = "copy-object";
        let (mut copy_info, copy_key) = put_and_prime(&set_disks, bucket, copy_object, b"COPY body").await;
        copy_info.metadata_only = true;
        Arc::make_mut(&mut copy_info.user_defined).insert("x-amz-meta-copy".to_string(), "updated".to_string());
        let copy_probe = MetadataCacheInvalidationProbe::install(bucket, copy_object);
        set_disks
            .copy_object(
                bucket,
                copy_object,
                bucket,
                copy_object,
                &mut copy_info,
                &ObjectOptions::default(),
                &ObjectOptions::default(),
            )
            .await
            .expect("metadata COPY should succeed");
        assert_eq!(
            copy_probe.count(),
            4,
            "metadata COPY must retain both outer and update-object-meta fences"
        );
        assert_retired(&set_disks, &copy_key).await;
        drop(copy_probe);

        let metadata_object = "metadata-object";
        let (_, metadata_key) = put_and_prime(&set_disks, bucket, metadata_object, b"metadata body").await;
        let mut metadata = HashMap::new();
        metadata.insert("x-amz-meta-updated".to_string(), "true".to_string());
        let metadata_opts = ObjectOptions {
            eval_metadata: Some(metadata),
            ..Default::default()
        };
        let metadata_probe = MetadataCacheInvalidationProbe::install(bucket, metadata_object);
        set_disks
            .put_object_metadata(bucket, metadata_object, &metadata_opts)
            .await
            .expect("metadata PUT should succeed");
        assert_eq!(
            metadata_probe.count(),
            4,
            "metadata PUT must retain both outer and update-object-meta fences"
        );
        assert_retired(&set_disks, &metadata_key).await;
    }
}

#[cfg(all(test, feature = "test-util"))]
mod transition_commit_failure_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks;
    use super::*;
    use crate::bucket::lifecycle::lifecycle::{TRANSITION_COMPLETE, TRANSITION_PENDING, TransitionOptions};
    use crate::disk::DiskAPI as _;
    use crate::services::tier::test_util::{MockWarmBackend, register_mock_tier};
    use crate::services::tier::tier::TierConfigMgr;
    use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};
    use http::HeaderMap;
    use s3s::dto::RestoreRequest;
    use tokio::io::AsyncReadExt;

    fn restore_operation_id_metadata(operation_id: Uuid) -> HashMap<String, String> {
        let mut metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_id.to_string(),
        );
        metadata
    }

    fn restore_metadata(operation_id: Uuid, ongoing: bool) -> HashMap<String, String> {
        let mut metadata = restore_operation_id_metadata(operation_id);
        metadata.insert(s3s::header::X_AMZ_RESTORE.as_str().to_string(), format!("ongoing-request=\"{ongoing}\""));
        metadata
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn local_commit_failure_returns_error_and_preserves_remote_candidate() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-commit-failure-bucket";
        let object = "object.bin";
        let payload = b"transition commit failure must preserve the uploaded candidate".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(payload.clone());
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let (fi, parts_metadata, online_disks) = set_disks
            .get_object_fileinfo(bucket, object, &ObjectOptions::default(), true, false)
            .await
            .expect("source metadata should resolve");
        let generation = set_disks
            .get_object_metadata_cache_generation(bucket, object)
            .expect("metadata cache generation should be active");
        let cache_key = GetObjectMetadataCacheKey::new(bucket, object, generation);
        set_disks
            .get_object_metadata_cache
            .insert(
                cache_key.clone(),
                Arc::new(GetObjectMetadataCacheEntry {
                    created_at: Instant::now(),
                    fi: fi.clone(),
                    parts_metadata,
                    online_disks,
                    read_quorum: 2,
                }),
            )
            .await;
        assert!(set_disks.get_object_metadata_cache.get(&cache_key).await.is_some());

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name,
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let barrier = TransitionCommitBarrier::install(bucket, object);
        let invalidations = MetadataCacheInvalidationProbe::install(bucket, object);
        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move { transition_set.transition_object(bucket, object, &opts).await });
        barrier.wait_until_paused().await;
        assert_eq!(invalidations.count(), 1, "precommit revalidation must fence the old metadata once");
        let saved_disks = {
            let mut disks = set_disks.disks.write().await;
            let saved = std::mem::take(&mut *disks);
            *disks = vec![None; saved.len()];
            saved
        };
        barrier.release();
        let result = transition.await.expect("transition task should not panic");
        *set_disks.disks.write().await = saved_disks;

        result.expect_err("local write quorum failure must be returned to the transition worker");
        assert_eq!(
            invalidations.count(),
            2,
            "local commit failure must fence any partially committed metadata again"
        );
        assert_eq!(backend.put_count().await, 1);
        assert_eq!(
            backend.remove_count().await,
            0,
            "ambiguous local commit failure must retain the remote candidate"
        );
        assert_eq!(backend.object_count().await, 1);
        assert!(
            set_disks.get_object_metadata_cache.get(&cache_key).await.is_none(),
            "local commit failure must invalidate pre-transition metadata"
        );

        let mut restored = Vec::new();
        set_disks
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the local source must remain readable after a fail-before-commit result")
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("the local source body should drain");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn partial_local_commit_failure_rolls_back_applied_disks_and_preserves_remote_candidate() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-partial-rollback-bucket";
        let object = "object.bin";
        let payload = b"partial local commit failure must roll back applied disks".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(payload.clone());
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name,
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let barrier = TransitionCommitBarrier::install_after_lease_check(bucket, object);
        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move { transition_set.transition_object(bucket, object, &opts).await });
        barrier.wait_until_paused().await;
        assert_eq!(backend.put_count().await, 1, "remote candidate must exist before local commit");

        let saved_disks = {
            let mut disks = set_disks.disks.write().await;
            let saved = disks.clone();
            for disk in disks.iter_mut().skip(2) {
                *disk = None;
            }
            saved
        };
        barrier.release();
        let result = transition.await.expect("transition task should not panic");

        result.expect_err("partial local commit must fail when only two of four disks are writable");
        let fi = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("rollback should keep the source metadata readable on applied disks")
            .0;
        assert_ne!(
            fi.transition_status, TRANSITION_COMPLETE,
            "rollback must not leave the applied disks marked as transitioned"
        );
        let mut restored = Vec::new();
        set_disks
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("rollback should keep the source object readable on applied disks")
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("the local source body should drain after rollback");
        assert_eq!(restored, payload);

        *set_disks.disks.write().await = saved_disks;
        assert_eq!(
            backend.remove_count().await,
            0,
            "ambiguous local commit failure must retain the remote candidate for scanner reconciliation"
        );
        assert_eq!(backend.object_count().await, 1);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn local_commit_post_apply_error_rolls_back_the_errored_disk() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-post-apply-rollback-bucket";
        let object = "object.bin";
        let payload = b"post-apply delete errors must still roll back the changed disk".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(payload.clone());
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name,
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let barrier = TransitionCommitBarrier::install_after_lease_check(bucket, object);
        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move { transition_set.transition_object(bucket, object, &opts).await });
        barrier.wait_until_paused().await;
        assert_eq!(backend.put_count().await, 1, "remote candidate must exist before local commit");

        crate::disk::local::set_delete_version_fail_after_commit(disk_stores[0].path().as_path(), object);
        let saved_disk = {
            let mut disks = set_disks.disks.write().await;
            let saved = disks[1].take();
            assert!(saved.is_some(), "test setup should start with the second disk online");
            saved
        };
        barrier.release();
        let result = transition.await.expect("transition task should not panic");

        result.expect_err("post-apply disk error plus one offline disk must fail write quorum");
        let errored_disk_fi = disk_stores[0]
            .read_version("", bucket, object, "", &ReadOptions::default())
            .await
            .expect("rollback must restore metadata on the disk that returned the post-apply error");
        assert_ne!(
            errored_disk_fi.transition_status, TRANSITION_COMPLETE,
            "rollback must not skip the disk that applied delete_version but returned an error"
        );
        disk_stores[0]
            .check_parts(bucket, object, &errored_disk_fi)
            .await
            .expect("rollback must restore the errored disk's staged data directory");

        set_disks.disks.write().await[1] = saved_disk;
        assert_eq!(
            backend.remove_count().await,
            0,
            "ambiguous local commit failure must retain the remote candidate for reconciliation"
        );
        assert_eq!(backend.object_count().await, 1);

        let mut restored = Vec::new();
        set_disks
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("source must remain readable after rolling back the post-apply error")
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("the local source body should drain");
        assert_eq!(restored, payload);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn production_transition_replacement_revokes_commit_and_cleans_with_old_driver() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-generation-fence-bucket";
        let object = "object.bin";
        let payload = b"generation replacement must fence the local transition commit".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut reader = PutObjReader::from_vec(payload);
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let manager = runtime_sources::global_tier_config_mgr();
        let old_backend = register_mock_tier(&manager, &tier_name).await;
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name.clone(),
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let barrier = TransitionCommitBarrier::install(bucket, object);
        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move { transition_set.transition_object(bucket, object, &opts).await });
        barrier.wait_until_paused().await;
        assert_eq!(old_backend.put_count().await, 1, "remote candidate must exist before replacement");

        let replacement_backend = MockWarmBackend::new();
        let replacement_manager = Arc::new(RwLock::new(TierConfigMgr {
            driver_cache: HashMap::new(),
            tiers: manager.read().await.tiers.clone(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        }));
        {
            let mut replacement = replacement_manager.write().await;
            replacement
                .tiers
                .get_mut(&tier_name)
                .and_then(|tier| tier.minio.as_mut())
                .expect("replacement tier should exist")
                .prefix = "replacement/".to_string();
            replacement
                .install_test_driver(&tier_name, Box::new(replacement_backend))
                .expect("replacement driver should install");
        }
        let replacement = match Arc::try_unwrap(replacement_manager) {
            Ok(manager) => manager.into_inner(),
            Err(_) => panic!("replacement manager should have one owner"),
        };
        let publish_handle = manager.clone();
        let publish_tier = tier_name.clone();
        let publish =
            tokio::spawn(
                async move { TierConfigMgr::publish_candidate(&publish_handle, replacement, Some(&publish_tier)).await },
            );

        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match TierConfigMgr::acquire_operation_lease(&manager, &tier_name).await {
                    Err(err) if err.message.contains("being replaced") => break,
                    Ok(lease) => drop(lease),
                    Err(err) => panic!("unexpected lease error while replacement drains: {err}"),
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("replacement should revoke the in-flight generation");
        assert!(!publish.is_finished(), "replacement must wait for the production transition lease");

        barrier.release();
        transition
            .await
            .expect("transition task should join")
            .expect_err("revoked generation must not commit tier-name metadata");
        publish
            .await
            .expect("replacement task should join")
            .expect("replacement should finish after old cleanup releases its lease");
        assert_eq!(
            old_backend.remove_count().await,
            1,
            "cancelled transition must clean up with the old driver"
        );
        assert_eq!(old_backend.object_count().await, 0);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn failed_restore_cleanup_does_not_overwrite_concurrent_unversioned_put() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-cleanup-cas-bucket";
        let object = "object.bin";
        let original_payload = b"old transitioned body".repeat(1024);
        let replacement_payload = b"new visible unversioned body".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(original_payload.clone());
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        set_disks
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name,
                        etag: original.etag.clone().unwrap_or_default(),
                        ..Default::default()
                    },
                    version_id: original.version_id.map(|version| version.to_string()),
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("source object should transition before restore");

        let get_barrier = backend.arm_failing_get_barrier().await;
        let restore_set = Arc::clone(&set_disks);
        let restore = tokio::spawn(async move {
            let mut opts = ObjectOptions::default();
            opts.transition.restore_request.days = Some(1);
            restore_set.restore_transitioned_object(bucket, object, &opts).await
        });
        get_barrier.wait_until_paused().await;

        let mut replacement_reader = PutObjReader::from_vec(replacement_payload.clone());
        let replacement = set_disks
            .put_object(bucket, object, &mut replacement_reader, &ObjectOptions::default())
            .await
            .expect("concurrent unversioned PUT should commit while restore GET is paused");
        get_barrier.release();
        restore
            .await
            .expect("restore task should join")
            .expect_err("injected tier GET failure should surface");

        let visible = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("replacement metadata should remain visible");
        assert_eq!(visible.etag, replacement.etag, "stale restore cleanup must not republish the old ETag");
        assert_ne!(
            visible.transitioned_object.status, TRANSITION_COMPLETE,
            "replacement object must not regain stale transition metadata"
        );
        let mut body = Vec::new();
        set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("replacement object should be readable")
            .stream
            .read_to_end(&mut body)
            .await
            .expect("replacement body should drain");
        assert_eq!(
            body, replacement_payload,
            "stale restore cleanup must not make the old remote body current again"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn failed_restore_cleanup_ignores_replaced_operation_id_with_same_identity() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-cleanup-operation-id-bucket";
        let object = "object.bin";
        let payload = b"same identity restore cleanup must respect operation id".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let operation_a = Uuid::new_v4();
        let operation_b = Uuid::new_v4();
        let metadata = restore_metadata(operation_a, true);

        let mut reader = PutObjReader::from_vec(payload);
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(metadata),
                    ..Default::default()
                },
            )
            .await
            .expect("restore operation A metadata should be installed");
        let stale_operation_a = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("operation A metadata should resolve");

        let mut operation_b_metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut operation_b_metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_b.to_string(),
        );
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(operation_b_metadata),
                    ..Default::default()
                },
            )
            .await
            .expect("same-identity restore operation B should replace A");

        let mut expected_operation_a = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut expected_operation_a,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_a.to_string(),
        );
        set_disks
            .update_restore_metadata(
                bucket,
                object,
                &stale_operation_a,
                &ObjectOptions {
                    user_defined: expected_operation_a,
                    ..Default::default()
                },
            )
            .await
            .expect("stale operation A cleanup should no-op, not fail");
        let current_operation_b = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("operation B metadata should remain readable");
        assert!(
            current_operation_b
                .user_defined
                .contains_key(s3s::header::X_AMZ_RESTORE.as_str()),
            "stale cleanup for operation A must not remove operation B's restore header"
        );
        assert_eq!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                current_operation_b.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            ),
            Some(operation_b.to_string().as_str()),
            "stale cleanup for operation A must not remove or rewrite operation B"
        );

        let mut expected_operation_b = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut expected_operation_b,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_b.to_string(),
        );
        set_disks
            .update_restore_metadata(
                bucket,
                object,
                &current_operation_b,
                &ObjectOptions {
                    user_defined: expected_operation_b,
                    ..Default::default()
                },
            )
            .await
            .expect("matching operation B cleanup should remove restore markers");
        let cleaned = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("cleaned object metadata should remain readable");
        assert!(
            !cleaned.user_defined.contains_key(s3s::header::X_AMZ_RESTORE.as_str()),
            "matching cleanup must remove the restore header"
        );
        assert!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                cleaned.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            )
            .is_none(),
            "matching cleanup must remove the restore operation id"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn restore_worker_propagates_operation_id_to_final_put_commit() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-worker-commit-operation-id-bucket";
        let object = "object.bin";
        let payload = b"restore worker must carry operation id to final commit".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(payload.clone());
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        set_disks
            .transition_object(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    transition: TransitionOptions {
                        status: TRANSITION_PENDING.to_string(),
                        tier: tier_name,
                        etag: original.etag.clone().unwrap_or_default(),
                        ..Default::default()
                    },
                    version_id: original.version_id.map(|version| version.to_string()),
                    mod_time: original.mod_time,
                    ..Default::default()
                },
            )
            .await
            .expect("source object should transition before restore");

        let operation_a = Uuid::new_v4();
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(restore_metadata(operation_a, true)),
                    ..Default::default()
                },
            )
            .await
            .expect("restore operation A metadata should be installed");

        let get_barrier = backend.arm_get_barrier().await;
        let restore_set = Arc::clone(&set_disks);
        let restore = tokio::spawn(async move {
            restore_set
                .restore_transitioned_object(
                    bucket,
                    object,
                    &ObjectOptions {
                        transition: TransitionOptions {
                            restore_request: RestoreRequest {
                                days: Some(1),
                                ..Default::default()
                            },
                            ..Default::default()
                        },
                        user_defined: restore_operation_id_metadata(operation_a),
                        ..Default::default()
                    },
                )
                .await
        });
        get_barrier.wait_until_paused().await;

        let operation_b = Uuid::new_v4();
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(restore_operation_id_metadata(operation_b)),
                    ..Default::default()
                },
            )
            .await
            .expect("operation B should replace operation A after worker starts tier GET");
        get_barrier.release();
        restore
            .await
            .expect("restore task should join")
            .expect_err("stale operation A must fail at final PUT commit");

        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("operation B metadata should remain visible after stale worker is rejected");
        assert_eq!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                current.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            ),
            Some(operation_b.to_string().as_str()),
            "stale worker must not remove or replace operation B"
        );
        assert_eq!(
            current.transitioned_object.status, TRANSITION_COMPLETE,
            "stale worker must not publish its restored local body after operation id replacement"
        );
        let mut body = Vec::new();
        set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("original transitioned object should remain readable")
            .stream
            .read_to_end(&mut body)
            .await
            .expect("original remote body should drain");
        assert_eq!(body, payload);
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn restore_put_commit_rechecks_operation_id_and_strips_internal_marker() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-put-commit-operation-id-bucket";
        let object = "object.bin";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let operation_a = Uuid::new_v4();
        let operation_a_metadata = restore_metadata(operation_a, true);
        let mut reader = PutObjReader::from_vec(b"restore source body".repeat(1024));
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(operation_a_metadata.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("ongoing restore operation A should be installed");

        let operation_b = Uuid::new_v4();
        let mut operation_b_metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut operation_b_metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_b.to_string(),
        );
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(operation_b_metadata),
                    ..Default::default()
                },
            )
            .await
            .expect("operation B should replace operation A before final commit");

        let mut stale_restore_reader = PutObjReader::from_vec(b"stale A restored body".repeat(1024));
        let result = set_disks
            .put_object(
                bucket,
                object,
                &mut stale_restore_reader,
                &ObjectOptions {
                    user_defined: operation_a_metadata,
                    ..Default::default()
                },
            )
            .await;
        result.expect_err("stale operation A must not commit after operation B replaces it");

        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("operation B metadata should remain current after stale commit is rejected");
        assert_eq!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                current.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            ),
            Some(operation_b.to_string().as_str()),
            "stale commit must not remove or replace operation B"
        );

        let mut matching_restore_reader = PutObjReader::from_vec(b"matching B restored body".repeat(1024));
        let operation_b_restore_metadata = restore_metadata(operation_b, false);
        set_disks
            .put_object(
                bucket,
                object,
                &mut matching_restore_reader,
                &ObjectOptions {
                    user_defined: operation_b_restore_metadata,
                    ..Default::default()
                },
            )
            .await
            .expect("matching operation B should be allowed to commit");
        let restored = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("restored object should remain readable");
        assert!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                restored.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            )
            .is_none(),
            "completed restore PUT must not persist the internal operation id"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn restore_multipart_complete_rechecks_operation_id_and_strips_internal_marker() {
        use crate::storage_api_contracts::multipart::MultipartOperations as _;

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "restore-multipart-commit-operation-id-bucket";
        let object = "object.bin";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let operation_a = Uuid::new_v4();
        let operation_a_metadata = restore_metadata(operation_a, true);
        let mut initial_reader = PutObjReader::from_vec(b"multipart restore source body".repeat(1024));
        set_disks
            .put_object(bucket, object, &mut initial_reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(operation_a_metadata.clone()),
                    ..Default::default()
                },
            )
            .await
            .expect("ongoing multipart restore operation A should be installed");

        let upload = set_disks
            .new_multipart_upload(
                bucket,
                object,
                &ObjectOptions {
                    user_defined: operation_a_metadata.clone(),
                    ..Default::default()
                },
            )
            .await
            .expect("restore multipart upload should be created");
        let mut part_reader = PutObjReader::from_vec(b"stale multipart A restored body".repeat(1024));
        let part = set_disks
            .put_object_part(bucket, object, &upload.upload_id, 1, &mut part_reader, &ObjectOptions::default())
            .await
            .expect("restore multipart part should be written");

        let operation_b = Uuid::new_v4();
        let mut operation_b_metadata = HashMap::new();
        rustfs_utils::http::metadata_compat::insert_str(
            &mut operation_b_metadata,
            rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            operation_b.to_string(),
        );
        set_disks
            .put_object_metadata(
                bucket,
                object,
                &ObjectOptions {
                    eval_metadata: Some(operation_b_metadata),
                    ..Default::default()
                },
            )
            .await
            .expect("operation B should replace operation A before multipart complete");

        let complete_result = set_disks
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &upload.upload_id,
                vec![CompletePart {
                    part_num: part.part_num,
                    etag: part.etag.clone(),
                    ..Default::default()
                }],
                &ObjectOptions {
                    user_defined: operation_a_metadata,
                    ..Default::default()
                },
            )
            .await;
        complete_result.expect_err("stale operation A must not complete after operation B replaces it");

        let current = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("operation B metadata should remain current after stale multipart completion");
        assert_eq!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                current.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            ),
            Some(operation_b.to_string().as_str()),
            "stale multipart completion must not remove or replace operation B"
        );

        let operation_b_restore_metadata = restore_metadata(operation_b, false);
        let upload = set_disks
            .new_multipart_upload(
                bucket,
                object,
                &ObjectOptions {
                    user_defined: operation_b_restore_metadata.clone(),
                    ..Default::default()
                },
            )
            .await
            .expect("matching operation B multipart upload should be created");
        let mut part_reader = PutObjReader::from_vec(b"matching multipart B restored body".repeat(1024));
        let part = set_disks
            .put_object_part(bucket, object, &upload.upload_id, 1, &mut part_reader, &ObjectOptions::default())
            .await
            .expect("matching restore multipart part should be written");
        set_disks
            .clone()
            .complete_multipart_upload(
                bucket,
                object,
                &upload.upload_id,
                vec![CompletePart {
                    part_num: part.part_num,
                    etag: part.etag,
                    ..Default::default()
                }],
                &ObjectOptions {
                    user_defined: operation_b_restore_metadata,
                    ..Default::default()
                },
            )
            .await
            .expect("matching operation B should complete");
        let restored = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("completed multipart restore should remain readable");
        assert!(
            rustfs_utils::http::metadata_compat::get_consistent_str(
                restored.user_defined.as_ref(),
                rustfs_utils::http::metadata_compat::SUFFIX_RESTORE_OPERATION_ID,
            )
            .is_none(),
            "completed multipart restore must not persist the internal operation id"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn legacy_reload_rejects_route_change_after_local_transition_commit() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-post-check-fence-bucket";
        let object = "object.bin";
        let payload = b"the operation lease must cover the local transition commit".repeat(1024);
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut reader = PutObjReader::from_vec(payload.clone());
        let original = set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written");

        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let manager = runtime_sources::global_tier_config_mgr();
        let old_backend = register_mock_tier(&manager, &tier_name).await;
        let old_prefix = manager.read().await.tiers[&tier_name]
            .minio
            .as_ref()
            .expect("old tier route should exist")
            .prefix
            .clone();
        let old_identity = TierConfigMgr::acquire_operation_lease(&manager, &tier_name)
            .await
            .expect("old tier identity should be available")
            .backend_identity();
        let opts = ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name.clone(),
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        };

        let barrier = TransitionCommitBarrier::install_after_lease_check(bucket, object);
        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move { transition_set.transition_object(bucket, object, &opts).await });
        barrier.wait_until_paused().await;
        assert_eq!(old_backend.put_count().await, 1, "remote candidate must exist before replacement");

        let replacement_manager = Arc::new(RwLock::new(TierConfigMgr {
            driver_cache: HashMap::new(),
            tiers: manager.read().await.tiers.clone(),
            last_refreshed_at: OffsetDateTime::now_utc(),
        }));
        {
            let mut replacement = replacement_manager.write().await;
            replacement
                .tiers
                .get_mut(&tier_name)
                .and_then(|tier| tier.minio.as_mut())
                .expect("replacement tier should exist")
                .prefix = "replacement/".to_string();
        }
        let replacement = match Arc::try_unwrap(replacement_manager) {
            Ok(manager) => manager.into_inner(),
            Err(_) => panic!("replacement manager should have one owner"),
        };
        let publish_handle = manager.clone();
        let publish = tokio::spawn(async move { publish_handle.write().await.publish_legacy_reload(replacement).await });
        tokio::time::timeout(Duration::from_secs(5), async {
            while manager.try_read().is_ok() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("legacy reload should hold the manager guard while the checked generation drains");
        assert!(
            !publish.is_finished(),
            "replacement must wait until the local transition commit releases its lease"
        );

        barrier.release();
        transition
            .await
            .expect("transition task should join")
            .expect("a transition linearized before replacement should commit");
        publish
            .await
            .expect("replacement task should join")
            .expect_err("replacement must not rebind a tier name referenced by the committed transition");
        assert_eq!(old_backend.remove_count().await, 0, "committed transition data must not be cleaned up");
        assert_eq!(old_backend.object_count().await, 1);
        let transitioned = set_disks
            .get_object_info(bucket, object, &ObjectOptions::default())
            .await
            .expect("transitioned metadata should resolve");
        let expected_identity = rustfs_utils::crypto::hex(old_identity);
        assert_eq!(
            rustfs_utils::http::metadata_compat::get_str(
                &transitioned.user_defined,
                rustfs_utils::http::metadata_compat::SUFFIX_TRANSITION_TIER_DESTINATION_ID,
            )
            .as_deref(),
            Some(expected_identity.as_str())
        );
        assert_eq!(
            manager
                .read()
                .await
                .tiers
                .get(&tier_name)
                .and_then(|tier| tier.minio.as_ref())
                .expect("old tier route should remain configured")
                .prefix,
            old_prefix
        );

        let mut restored = Vec::new();
        set_disks
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("the transitioned object must remain routed through the old generation")
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("the transitioned object body should drain");
        assert_eq!(restored, payload);
    }
}

#[cfg(all(test, feature = "test-util"))]
mod transition_upload_integrity_tests {
    use super::hermetic_set_disks_support::{hermetic_set_disks, hermetic_set_disks_with_lockers};
    use super::*;
    use crate::bucket::lifecycle::lifecycle::{TRANSITION_PENDING, TransitionOptions};
    use crate::disk::DiskAPI as _;
    use crate::layout::endpoints::SetupType;
    use crate::services::tier::test_util::register_mock_tier;
    use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};
    use http::HeaderMap;
    use rustfs_lock::client::local::LocalClient;
    use rustfs_lock::{LockClient, LockError, LockId, LockInfo, LockRequest, LockResponse, LockStats};
    use std::collections::HashSet;
    use std::path::PathBuf;
    use std::sync::atomic::AtomicUsize;

    struct SetupTypeGuard {
        previous: SetupType,
    }

    impl SetupTypeGuard {
        async fn switch_to(next: SetupType) -> Self {
            let previous = runtime_sources::current_setup_type().await;
            runtime_sources::set_setup_type(next).await;
            Self { previous }
        }
    }

    impl Drop for SetupTypeGuard {
        fn drop(&mut self) {
            let previous = self.previous.clone();
            let handle = tokio::runtime::Handle::current();
            tokio::task::block_in_place(|| {
                handle.block_on(async move {
                    runtime_sources::set_setup_type(previous).await;
                });
            });
        }
    }

    #[derive(Debug)]
    struct FailingLockClient;

    #[async_trait::async_trait]
    impl LockClient for FailingLockClient {
        async fn acquire_lock(&self, _request: &rustfs_lock::LockRequest) -> rustfs_lock::Result<LockResponse> {
            Err(LockError::internal("simulated transition commit lock client failure"))
        }

        async fn release(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            Ok(false)
        }

        async fn refresh(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            Ok(false)
        }

        async fn force_release(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<bool> {
            Ok(false)
        }

        async fn check_status(&self, _lock_id: &rustfs_lock::LockId) -> rustfs_lock::Result<Option<LockInfo>> {
            Ok(None)
        }

        async fn get_stats(&self) -> rustfs_lock::Result<LockStats> {
            Ok(LockStats::default())
        }

        async fn close(&self) -> rustfs_lock::Result<()> {
            Ok(())
        }

        async fn is_online(&self) -> bool {
            false
        }

        async fn is_local(&self) -> bool {
            false
        }
    }

    #[derive(Debug)]
    struct LockLostRefreshClient {
        refresh_calls: Arc<AtomicUsize>,
        active: tokio::sync::Mutex<HashSet<LockId>>,
    }

    impl LockLostRefreshClient {
        fn new(refresh_calls: Arc<AtomicUsize>) -> Self {
            Self {
                refresh_calls,
                active: tokio::sync::Mutex::new(HashSet::new()),
            }
        }
    }

    #[async_trait::async_trait]
    impl LockClient for LockLostRefreshClient {
        async fn acquire_lock(&self, request: &LockRequest) -> rustfs_lock::Result<LockResponse> {
            self.active.lock().await.insert(request.lock_id.clone());
            Ok(LockResponse::success(
                LockInfo {
                    id: request.lock_id.clone(),
                    resource: request.resource.clone(),
                    lock_type: request.lock_type,
                    status: rustfs_lock::LockStatus::Acquired,
                    owner: request.owner.clone(),
                    acquired_at: std::time::SystemTime::now(),
                    expires_at: std::time::SystemTime::now() + request.ttl,
                    last_refreshed: std::time::SystemTime::now(),
                    metadata: request.metadata.clone(),
                    priority: request.priority,
                    wait_start_time: None,
                },
                Duration::ZERO,
            ))
        }

        async fn release(&self, lock_id: &LockId) -> rustfs_lock::Result<bool> {
            Ok(self.active.lock().await.remove(lock_id))
        }

        async fn refresh(&self, _lock_id: &LockId) -> rustfs_lock::Result<bool> {
            self.refresh_calls.fetch_add(1, Ordering::SeqCst);
            Ok(false)
        }

        async fn force_release(&self, lock_id: &LockId) -> rustfs_lock::Result<bool> {
            self.release(lock_id).await
        }

        async fn check_status(&self, _lock_id: &LockId) -> rustfs_lock::Result<Option<LockInfo>> {
            Ok(None)
        }

        async fn get_stats(&self) -> rustfs_lock::Result<LockStats> {
            Ok(LockStats::default())
        }

        async fn close(&self) -> rustfs_lock::Result<()> {
            Ok(())
        }

        async fn is_online(&self) -> bool {
            true
        }

        async fn is_local(&self) -> bool {
            false
        }
    }

    async fn assert_local_source_intact(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, payload: &[u8]) {
        let mut restored = Vec::new();
        set_disks
            .get_object_reader(
                bucket,
                object,
                None,
                HeaderMap::new(),
                &ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            )
            .await
            .expect("failed transition must leave the local source readable")
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("local source should drain after failed transition");
        assert_eq!(restored, payload);
        let (fi, _, _) = set_disks
            .get_object_fileinfo(
                bucket,
                object,
                &ObjectOptions {
                    no_lock: true,
                    metadata_cache_safe: false,
                    ..Default::default()
                },
                true,
                false,
            )
            .await
            .expect("local source metadata should remain available");
        assert_ne!(fi.transition_status, TRANSITION_COMPLETE);
    }

    async fn write_source(
        set_disks: &Arc<SetDisks>,
        disk_stores: &[DiskStore],
        bucket: &str,
        object: &str,
        payload: &[u8],
    ) -> ObjectInfo {
        for disk in disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let mut reader = PutObjReader::from_vec(payload.to_vec());
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("source object should be written")
    }

    fn transition_options(original: &ObjectInfo, tier_name: String) -> ObjectOptions {
        ObjectOptions {
            no_lock: true,
            transition: TransitionOptions {
                status: TRANSITION_PENDING.to_string(),
                tier: tier_name,
                etag: original.etag.clone().unwrap_or_default(),
                ..Default::default()
            },
            version_id: original.version_id.map(|version| version.to_string()),
            mod_time: original.mod_time,
            ..Default::default()
        }
    }

    async fn corrupt_shard_at(path: PathBuf, position: ShardCorruptionPosition) {
        let mut bytes = tokio::fs::read(&path).await.expect("committed shard should be readable");
        assert!(!bytes.is_empty(), "committed shard should not be empty");
        let offset = match position {
            ShardCorruptionPosition::First => 0,
            ShardCorruptionPosition::Middle => bytes.len() / 2,
            ShardCorruptionPosition::Last => bytes.len() - 1,
        };
        bytes[offset] ^= 0xff;
        tokio::fs::write(&path, bytes)
            .await
            .expect("committed shard corruption should be written");
    }

    #[derive(Clone, Copy, Debug)]
    enum ShardCorruptionPosition {
        First,
        Middle,
        Last,
    }

    impl ShardCorruptionPosition {
        fn label(self) -> &'static str {
            match self {
                ShardCorruptionPosition::First => "first",
                ShardCorruptionPosition::Middle => "middle",
                ShardCorruptionPosition::Last => "last",
            }
        }
    }

    async fn corrupt_beyond_read_quorum(
        temp_dirs: &[tempfile::TempDir],
        bucket: &str,
        object: &str,
        data_dir: Uuid,
        parity_blocks: usize,
        position: ShardCorruptionPosition,
    ) {
        for temp_dir in temp_dirs.iter().take(parity_blocks + 1) {
            let shard = temp_dir
                .path()
                .join(bucket)
                .join(object)
                .join(data_dir.to_string())
                .join("part.1");
            corrupt_shard_at(shard, position).await;
        }
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn partial_remote_acceptance_cleans_exact_candidate_and_preserves_source() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-partial-accept-bucket";
        let object = "object.bin";
        let payload = vec![0x5a; 2 * 1024 * 1024];
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let remote_version = Uuid::nil().to_string();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        backend.set_put_read_limit(Some(4096)).await;
        backend.set_put_remote_version(Some(remote_version.clone())).await;

        let error = set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect_err("accepting only a prefix must fail transition completion");
        assert!(matches!(error, StorageError::Io(_) | StorageError::LessData));
        let removed_versions = backend.remove_versions().await;
        assert_eq!(removed_versions.len(), 1);
        assert_eq!(
            removed_versions[0].1, "",
            "the nil UUID response is the backend's unversioned sentinel and must not become an S3 versionId"
        );
        assert_eq!(backend.object_count().await, 0);
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial_test::serial]
    async fn commit_lock_acquire_failure_cleans_remote_candidate_and_preserves_source() {
        let manager = Arc::new(rustfs_lock::GlobalLockManager::new());
        let lockers: Vec<Arc<dyn LockClient>> = vec![Arc::new(LocalClient::with_manager(manager)), Arc::new(FailingLockClient)];
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "transition-lock-acquire-failure-bucket";
        let object = "object.bin";
        let payload = b"transition commit lock failure must clean the remote candidate".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let _setup_type_guard = SetupTypeGuard::switch_to(SetupType::DistErasure).await;
        let mut opts = transition_options(&original, tier_name);
        opts.no_lock = false;

        let error = set_disks
            .transition_object(bucket, object, &opts)
            .await
            .expect_err("transition must fail when the commit namespace write lock cannot reach quorum");
        assert!(
            matches!(
                error,
                StorageError::NamespaceLockQuorumUnavailable { .. }
                    | StorageError::Lock(LockError::QuorumNotReached { .. })
                    | StorageError::Lock(LockError::Internal { .. })
            ),
            "unexpected transition lock-acquire error: {error:?}"
        );
        assert_eq!(
            backend.put_count().await,
            1,
            "remote candidate must be uploaded before commit lock failure"
        );
        assert_eq!(
            backend.remove_count().await,
            1,
            "known remote candidate must be removed when commit lock acquisition fails"
        );
        assert_eq!(
            backend.remove_versions().await,
            backend.put_versions().await,
            "lock-acquire cleanup must target the exact remote version returned by PUT"
        );
        assert_eq!(
            backend.object_count().await,
            0,
            "commit lock failure must not leave an orphan remote candidate"
        );
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test(flavor = "current_thread", start_paused = true)]
    #[serial_test::serial]
    async fn commit_lock_lost_after_upload_cleans_remote_candidate_and_preserves_source() {
        let refresh_calls = Arc::new(AtomicUsize::new(0));
        let lockers: Vec<Arc<dyn LockClient>> = (0..4)
            .map(|_| Arc::new(LockLostRefreshClient::new(Arc::clone(&refresh_calls))) as Arc<dyn LockClient>)
            .collect();
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_with_lockers(4, 0, 2, lockers).await;
        let bucket = "transition-lock-lost-bucket";
        let object = "object.bin";
        let payload = b"lost transition commit lock must clean the remote candidate".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let previous_setup_type = runtime_sources::current_setup_type().await;
        runtime_sources::set_setup_type(SetupType::DistErasure).await;
        let mut opts = transition_options(&original, tier_name);
        opts.no_lock = false;
        let barrier = TransitionCommitBarrier::install_before_lock_lost_check(bucket, object);

        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move { transition_set.transition_object(bucket, object, &opts).await });
        barrier.wait_until_paused().await;
        tokio::time::advance(Duration::from_secs(11)).await;
        tokio::task::yield_now().await;
        assert!(
            refresh_calls.load(Ordering::SeqCst) > 0,
            "test must drive the real distributed-lock heartbeat before the commit fence"
        );
        barrier.release();

        let error = transition
            .await
            .expect("transition task should not panic")
            .expect_err("transition must fail after the commit namespace write lock loses refresh quorum");
        assert!(
            matches!(error, StorageError::NamespaceLockQuorumUnavailable { .. }),
            "unexpected transition lock-lost error: {error:?}"
        );
        runtime_sources::set_setup_type(previous_setup_type).await;
        assert_eq!(backend.put_count().await, 1);
        assert_eq!(
            backend.remove_count().await,
            1,
            "lock-lost commit fence must remove the uploaded remote candidate"
        );
        assert_eq!(
            backend.remove_versions().await,
            backend.put_versions().await,
            "lock-lost cleanup must target the exact remote version returned by PUT"
        );
        assert_eq!(
            backend.object_count().await,
            0,
            "lock-lost commit fence must not leave an orphan remote candidate"
        );
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn cancelled_after_remote_upload_cleans_candidate_and_preserves_source() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-cancel-after-upload-bucket";
        let object = "object.bin";
        let payload = b"cancelled transition after upload must clean remote candidate".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let barrier = TransitionCommitBarrier::install(bucket, object);

        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move {
            transition_set
                .transition_object(bucket, object, &transition_options(&original, tier_name))
                .await
        });
        barrier.wait_until_paused().await;
        assert_eq!(
            backend.put_count().await,
            1,
            "transition must upload a remote candidate before the cancellation point"
        );
        let put_versions = backend.put_versions().await;
        assert_eq!(put_versions.len(), 1, "transition should expose one remote candidate/version");
        assert_eq!(backend.object_count().await, 1, "remote candidate should be visible before cancellation");

        transition.abort();
        let join = transition
            .await
            .expect_err("aborted transition task should report cancellation");
        assert!(join.is_cancelled(), "transition task should be cancelled, not panic");
        drop(barrier);

        assert!(
            backend
                .wait_for_remote_absence(&put_versions[0].0, Duration::from_secs(5))
                .await,
            "cancellation cleanup must remove the uploaded remote candidate"
        );
        assert_eq!(
            backend.remove_versions().await,
            put_versions,
            "cancellation cleanup must target the exact remote version returned by PUT"
        );
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn real_bitrot_producer_failures_do_not_commit_transition() {
        let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;

        for position in [
            ShardCorruptionPosition::First,
            ShardCorruptionPosition::Middle,
            ShardCorruptionPosition::Last,
        ] {
            let bucket = format!("transition-real-bitrot-{}", position.label());
            let object = format!("{}-corrupt.bin", position.label());
            let payload = vec![0x41; 2 * 1024 * 1024];
            let original = write_source(&set_disks, &disk_stores, &bucket, &object, &payload).await;
            let (source, _, _) = set_disks
                .get_object_fileinfo(
                    &bucket,
                    &object,
                    &ObjectOptions {
                        no_lock: true,
                        metadata_cache_safe: false,
                        ..Default::default()
                    },
                    true,
                    false,
                )
                .await
                .expect("source metadata should be available before shard corruption");
            let data_dir = source.data_dir.expect("source object should have a data directory");

            corrupt_beyond_read_quorum(&temp_dirs, &bucket, &object, data_dir, source.erasure.parity_blocks, position).await;

            let error = set_disks
                .transition_object(&bucket, &object, &transition_options(&original, tier_name.clone()))
                .await
                .expect_err("producer bitrot failure must not commit transition metadata");
            assert!(
                matches!(
                    error,
                    StorageError::FileCorrupt
                        | StorageError::ErasureReadQuorum
                        | StorageError::InsufficientReadQuorum(_, _)
                        | StorageError::LessData
                        | StorageError::Io(_)
                ),
                "{position:?}: unexpected transition producer error: {error:?}"
            );
            let (after, _, _) = set_disks
                .get_object_fileinfo(
                    &bucket,
                    &object,
                    &ObjectOptions {
                        no_lock: true,
                        metadata_cache_safe: false,
                        ..Default::default()
                    },
                    true,
                    false,
                )
                .await
                .expect("failed transition must leave metadata readable");
            assert_eq!(
                after.data_dir,
                Some(data_dir),
                "{position:?}: transition must not release the source data dir"
            );
            assert_ne!(
                after.transition_status, TRANSITION_COMPLETE,
                "{position:?}: transition status must remain incomplete after producer bitrot failure"
            );
        }

        assert_eq!(
            backend.put_count().await,
            3,
            "each bitrot case should create one remote cleanup candidate"
        );
        assert_eq!(backend.remove_count().await, 3, "each bitrot candidate must be removed before returning");
        assert_eq!(
            backend.remove_versions().await,
            backend.put_versions().await,
            "bitrot cleanup must target the exact remote version returned by PUT"
        );
        assert_eq!(
            backend.object_count().await,
            0,
            "bitrot producer failures must not leave remote candidates"
        );
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn opaque_remote_version_is_cleaned_before_parse_failure() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-unknown-version-bucket";
        let object = "object.bin";
        let payload = b"unknown remote version must retain local data".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        backend.set_put_remote_version(Some("opaque-version-token".to_string())).await;

        set_disks
            .transition_object(bucket, object, &transition_options(&original, tier_name))
            .await
            .expect_err("an unparseable remote version must fail closed");
        let removed_versions = backend.remove_versions().await;
        assert_eq!(removed_versions.len(), 1);
        assert_eq!(removed_versions[0].1, "opaque-version-token");
        assert_eq!(backend.object_count().await, 0);
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn authoritative_read_failure_after_upload_cleans_exact_candidate_and_preserves_source() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-authoritative-read-failure-bucket";
        let object = "object.bin";
        let payload = b"authoritative metadata read failure must clean remote candidate".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        let put_barrier = backend.arm_put_barrier().await;

        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move {
            transition_set
                .transition_object(bucket, object, &transition_options(&original, tier_name))
                .await
        });
        put_barrier.wait_until_paused().await;
        let saved_disks = {
            let mut disks = set_disks.disks.write().await;
            let saved = std::mem::take(&mut *disks);
            *disks = vec![None; saved.len()];
            saved
        };
        put_barrier.release();

        let err = transition
            .await
            .expect("transition task should not panic")
            .expect_err("authoritative metadata read failure must fail the transition");
        *set_disks.disks.write().await = saved_disks;
        assert!(
            matches!(
                err,
                StorageError::ErasureReadQuorum | StorageError::InsufficientReadQuorum(_, _) | StorageError::Io(_)
            ),
            "unexpected authoritative read failure: {err:?}"
        );
        assert_eq!(backend.put_count().await, 1);
        assert_eq!(backend.remove_count().await, 1);
        assert_eq!(
            backend.remove_versions().await,
            backend.put_versions().await,
            "authoritative read failure must remove the exact uploaded remote version"
        );
        assert_eq!(backend.object_count().await, 0);
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }

    #[tokio::test]
    #[serial_test::serial]
    async fn remote_cleanup_failure_after_version_rejection_preserves_source_and_candidate() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-cleanup-failure-bucket";
        let object = "object.bin";
        let payload = b"cleanup failure must keep source and orphan evidence".repeat(1024);
        let original = write_source(&set_disks, &disk_stores, bucket, object, &payload).await;
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;
        backend.set_put_remote_version(Some("opaque-version-token".to_string())).await;
        let put_barrier = backend.arm_put_barrier().await;

        let transition_set = Arc::clone(&set_disks);
        let transition = tokio::spawn(async move {
            transition_set
                .transition_object(bucket, object, &transition_options(&original, tier_name))
                .await
        });
        put_barrier.wait_until_paused().await;
        backend.set_server_error(true).await;
        put_barrier.release();

        transition
            .await
            .expect("transition task should not panic")
            .expect_err("opaque remote version must fail closed even if cleanup also fails");
        assert_eq!(backend.put_count().await, 1);
        assert_eq!(
            backend.remove_count().await,
            0,
            "cleanup failure before backend remove must not be reported as a successful exact delete"
        );
        assert_eq!(
            backend.object_count().await,
            1,
            "failed cleanup must leave the remote candidate visible for durable reconciliation"
        );
        assert_local_source_intact(&set_disks, bucket, object, &payload).await;
    }
}

#[cfg(all(test, feature = "test-util"))]
mod transition_source_identity_matrix_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks;
    use super::*;
    use crate::bucket::lifecycle::lifecycle::{TRANSITION_PENDING, TransitionOptions};
    use crate::disk::DiskAPI as _;
    use crate::services::tier::test_util::register_mock_tier;
    use crate::storage_api_contracts::object::{ObjectIO as _, ObjectOperations as _};

    #[tokio::test]
    #[serial_test::serial]
    async fn transition_source_identity_field_matrix_rejects_single_field_drift() {
        #[derive(Clone, Copy, Debug)]
        enum IdentityField {
            VersionId,
            DataDir,
            ModTime,
            Size,
            Etag,
        }

        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "transition-identity-matrix-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        let tier_name = format!("COLDTIER{}", &Uuid::new_v4().simple().to_string()[..8]).to_uppercase();
        let backend = register_mock_tier(&runtime_sources::global_tier_config_mgr(), &tier_name).await;

        for (index, field) in [
            IdentityField::VersionId,
            IdentityField::DataDir,
            IdentityField::ModTime,
            IdentityField::Size,
            IdentityField::Etag,
        ]
        .into_iter()
        .enumerate()
        {
            let object = format!("identity-{index}.bin");
            let payload = vec![u8::try_from(index + 1).expect("matrix index should fit u8"); 1024 * 1024];
            let mut reader = PutObjReader::from_vec(payload);
            let original = set_disks
                .put_object(bucket, &object, &mut reader, &ObjectOptions::default())
                .await
                .expect("source object should be written");
            let (source, _, _) = set_disks
                .get_object_fileinfo(bucket, &object, &ObjectOptions::default(), true, false)
                .await
                .expect("source metadata should resolve");
            let opts = ObjectOptions {
                no_lock: true,
                transition: TransitionOptions {
                    status: TRANSITION_PENDING.to_string(),
                    tier: tier_name.clone(),
                    etag: original.etag.clone().unwrap_or_default(),
                    ..Default::default()
                },
                version_id: original.version_id.map(|version| version.to_string()),
                mod_time: original.mod_time,
                ..Default::default()
            };
            let put_barrier = backend.arm_put_barrier().await;
            let transition_set = Arc::clone(&set_disks);
            let transition_object = object.clone();
            let transition =
                tokio::spawn(async move { transition_set.transition_object(bucket, &transition_object, &opts).await });
            put_barrier.wait_until_paused().await;

            let mut changed = source.clone();
            match field {
                IdentityField::VersionId => changed.version_id = Some(Uuid::new_v4()),
                IdentityField::DataDir => changed.data_dir = Some(Uuid::new_v4()),
                IdentityField::ModTime => {
                    changed.mod_time = changed.mod_time.map(|value| value + time::Duration::nanoseconds(1));
                }
                IdentityField::Size => changed.size += 1,
                IdentityField::Etag => {
                    changed.metadata.insert("etag".to_string(), format!("changed-{index}"));
                }
            }
            for disk in &disk_stores {
                disk.write_metadata("", bucket, &object, changed.clone())
                    .await
                    .expect("single-field metadata drift should be written");
            }
            put_barrier.release();

            transition
                .await
                .expect("transition task should not panic")
                .expect_err("transition must reject a source whose identity changed after upload");
            let expected_attempts = index + 1;
            assert_eq!(backend.put_count().await, expected_attempts);
            assert_eq!(backend.remove_count().await, expected_attempts);
            assert_eq!(
                backend.remove_versions().await,
                backend.put_versions().await,
                "rejected identity drift must remove the exact uploaded version"
            );

            match field {
                IdentityField::VersionId => assert_ne!(source.version_id, changed.version_id),
                IdentityField::DataDir => assert_ne!(source.data_dir, changed.data_dir),
                IdentityField::ModTime => assert_ne!(source.mod_time, changed.mod_time),
                IdentityField::Size => assert_ne!(source.size, changed.size),
                IdentityField::Etag => assert_ne!(get_raw_etag(&source.metadata), get_raw_etag(&changed.metadata)),
            }
            if !matches!(field, IdentityField::VersionId) {
                assert_eq!(source.version_id, changed.version_id);
            }
            if !matches!(field, IdentityField::DataDir) {
                assert_eq!(source.data_dir, changed.data_dir);
            }
            if !matches!(field, IdentityField::ModTime) {
                assert_eq!(source.mod_time, changed.mod_time);
            }
            if !matches!(field, IdentityField::Size) {
                assert_eq!(source.size, changed.size);
            }
            if !matches!(field, IdentityField::Etag) {
                assert_eq!(get_raw_etag(&source.metadata), get_raw_etag(&changed.metadata));
            }
        }
    }
}

#[cfg(test)]
mod heterogeneous_pool_put_tests {
    use super::hermetic_set_disks_support::hermetic_set_disks_for_pool_with_default_parity;
    use super::*;
    use crate::config::storageclass::lookup_config_for_pools_without_env;
    use crate::disk::{DiskAPI as _, ReadOptions};
    use rustfs_config::server_config::KVS;
    use tokio::io::AsyncReadExt;

    #[tokio::test]
    async fn second_pool_regular_put_uses_its_own_layout_and_round_trips() {
        // Deliberately inject the first pool's invalid scalar fallback. The
        // test can pass only if the production PUT uses the held [4, 2]
        // storage-class snapshot and resolves pool 1 to parity 1.
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks_for_pool_with_default_parity(2, 1, 2).await;
        set_disks.set_test_storage_class_config(
            lookup_config_for_pools_without_env(&KVS::new(), &[4, 2]).expect("heterogeneous pool storage class should resolve"),
        );

        let bucket = "regular-put-second-pool-bucket";
        let object = "object";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let payload = vec![0x3c; 4096];
        let mut reader = PutObjReader::from_vec(payload.clone());
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("second-pool regular PUT should encode without zero data shards");

        for (disk_index, disk) in disk_stores.iter().enumerate() {
            let file_info = disk
                .read_version("", bucket, object, "", &ReadOptions::default())
                .await
                .unwrap_or_else(|err| panic!("disk {disk_index} should persist valid second-pool metadata: {err}"));
            assert_eq!(file_info.erasure.data_blocks, 1);
            assert_eq!(file_info.erasure.parity_blocks, 1);
        }

        let mut object_reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &ObjectOptions::default())
            .await
            .expect("second-pool regular PUT should be readable");
        let mut restored = Vec::new();
        object_reader
            .stream
            .read_to_end(&mut restored)
            .await
            .expect("second-pool regular PUT should stream");
        assert_eq!(restored, payload);
    }
}

#[cfg(test)]
mod put_object_tmp_cleanup_tests {
    //! Regression coverage for backlog#924 (HP-3): the speculative tmp-dir
    //! cleanup at the end of a successful PUT runs on a spawned task (off the
    //! response path), while a failed PUT must still clean its tmp shards
    //! inline before returning.

    use super::hermetic_set_disks_support::hermetic_set_disks;
    use super::*;
    use crate::disk::DiskAPI as _;
    use std::time::Duration;
    use tempfile::TempDir;

    /// Large enough that the erasure shards are written as real tmp files
    /// (never inlined into xl.meta), so both tests exercise actual cleanup.
    const TEST_OBJECT_SIZE: usize = 1 << 20;

    /// Entries under `.rustfs.sys/tmp` on every disk, excluding the `.trash`
    /// staging directory (trash reclamation is a background concern).
    async fn non_trash_tmp_entries(temp_dirs: &[TempDir]) -> Vec<String> {
        let mut leftovers = Vec::new();
        for temp_dir in temp_dirs {
            let tmp_path = temp_dir.path().join(RUSTFS_META_TMP_BUCKET);
            let mut read_dir = match tokio::fs::read_dir(&tmp_path).await {
                Ok(read_dir) => read_dir,
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
                Err(err) => panic!("tmp dir {tmp_path:?} should be listable: {err}"),
            };
            while let Some(entry) = read_dir.next_entry().await.expect("tmp dir entry should be readable") {
                let name = entry.file_name().to_string_lossy().to_string();
                if name != ".trash" {
                    leftovers.push(format!("{}/{name}", tmp_path.display()));
                }
            }
        }
        leftovers
    }

    #[tokio::test]
    async fn put_object_success_eventually_cleans_tmp_workspace() {
        let (temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;

        let bucket = "tmp-clean-ok-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        let mut reader = PutObjReader::from_vec(vec![7u8; TEST_OBJECT_SIZE]);
        set_disks
            .put_object(bucket, "hot-path-object", &mut reader, &ObjectOptions::default())
            .await
            .expect("put_object should succeed");

        // The speculative cleanup runs on a spawned task off the PUT response
        // path, so poll for the tmp workspace to drain instead of asserting
        // immediately.
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let leftovers = non_trash_tmp_entries(&temp_dirs).await;
            if leftovers.is_empty() {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "tmp workspace should drain after a successful PUT, leftovers: {leftovers:?}"
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }

        drop(temp_dirs);
    }

    #[tokio::test]
    async fn put_object_failure_cleans_tmp_workspace_inline() {
        let (temp_dirs, _disk_stores, set_disks) = hermetic_set_disks(4).await;

        // The bucket volume is never created, so the shards are written into
        // the tmp workspace and the commit fails at rename_data with a quorum
        // error — exercising the failure-path cleanup.
        let mut reader = PutObjReader::from_vec(vec![9u8; TEST_OBJECT_SIZE]);
        let err = set_disks
            .put_object("tmp-clean-missing-bucket", "orphan-object", &mut reader, &ObjectOptions::default())
            .await
            .expect_err("put_object into a missing bucket volume must fail");

        // No polling: the failure path must clean the tmp workspace inline,
        // before put_object returns (backlog#864 / backlog#898 hardening).
        let leftovers = non_trash_tmp_entries(&temp_dirs).await;
        assert!(
            leftovers.is_empty(),
            "failed PUT must not leave tmp shards behind, leftovers: {leftovers:?}, err: {err}"
        );

        drop(temp_dirs);
    }
}

#[cfg(test)]
mod put_object_tags_early_stop_regression_tests {
    //! Regression coverage for backlog#881: read-before-write tagging under GET
    //! metadata early-stop. backlog#872 enabled metadata early-stop by default;
    //! an early-stopped metadata fanout returns only a read-quorum subset of
    //! disks, and `put_object_tags` reuses that disk set as its write target, so
    //! it shrank the write set and failed write quorum with SlowDown. The fix
    //! forces a full-quorum fanout (allow_early_stop=false) inside
    //! `put_object_tags`. This multi-disk integration test pins that end to end:
    //! with early-stop enabled, the tag must land on EVERY online disk's xl.meta,
    //! not a read-quorum subset.

    use super::hermetic_set_disks_support::hermetic_set_disks;
    use super::*;
    use crate::disk::{DiskAPI as _, ReadOptions};

    #[tokio::test]
    async fn put_object_tags_writes_all_online_disks_under_early_stop() {
        // Early-stop defaults to on; pin it explicitly (enabled + full rollout)
        // so the regression scenario holds regardless of default/rollout drift.
        temp_env::async_with_vars(
            [
                ("RUSTFS_GET_METADATA_EARLY_STOP_ENABLE", Some("true")),
                ("RUSTFS_GET_METADATA_EARLY_STOP_ROLLOUT_PCT", Some("100")),
            ],
            async {
                let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
                let bucket = "backlog881-early-stop-bucket";
                let object = "read-before-write-tagged-object";
                for disk in &disk_stores {
                    disk.make_volume(bucket).await.expect("bucket volume should be created");
                }

                let mut reader = PutObjReader::from_vec(vec![5u8; 1024]);
                set_disks
                    .put_object(bucket, object, &mut reader, &ObjectOptions::default())
                    .await
                    .expect("put_object should succeed");

                let tags = "unit=backlog881&stage=regression";
                set_disks
                    .put_object_tags(bucket, object, tags, &ObjectOptions::default())
                    .await
                    .expect("put_object_tags must not fail write quorum (SlowDown) under early-stop");

                // Core assertion: the tag must be present on EVERY online disk,
                // proving the write target was the full set and not an
                // early-stopped read-quorum subset.
                for (idx, disk) in disk_stores.iter().enumerate() {
                    let fi = disk
                        .read_version("", bucket, object, "", &ReadOptions::default())
                        .await
                        .unwrap_or_else(|e| panic!("disk {idx} must hold xl.meta for the tagged object: {e}"));
                    assert_eq!(
                        fi.metadata.get(AMZ_OBJECT_TAGGING).map(String::as_str),
                        Some(tags),
                        "disk {idx} must carry the tag written under early-stop (write set not shrunk to a read-quorum subset)"
                    );
                }
            },
        )
        .await;
    }
}

#[cfg(test)]
mod delete_objects_lock_gating_tests {
    //! Regression coverage for backlog#929 (HP-8): the batch-delete per-object
    //! stat is gated on the bucket object-lock configuration. For buckets whose
    //! metadata is unknown the gate fails closed, so these hermetic tests (their
    //! buckets are never registered with the metadata sys) exercise the
    //! locked-stat path and prove the #4297 delete protection is intact end to
    //! end, while per-key result mapping of mixed batches stays stable.

    use super::hermetic_set_disks_support::hermetic_set_disks;
    use super::*;
    use crate::disk::DiskAPI as _;

    async fn put_plain_object(set_disks: &Arc<SetDisks>, bucket: &str, object: &str) {
        let mut reader = PutObjReader::from_vec(vec![3u8; 1024]);
        set_disks
            .put_object(bucket, object, &mut reader, &ObjectOptions::default())
            .await
            .expect("plain object should be written");
    }

    #[tokio::test]
    async fn delete_objects_reports_mixed_results_per_key() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "hp8-mixed-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        put_plain_object(&set_disks, bucket, "obj-a").await;
        put_plain_object(&set_disks, bucket, "obj-c").await;

        let objects = vec![
            ObjectToDelete {
                object_name: "obj-a".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "missing-b".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "obj-c".to_string(),
                ..Default::default()
            },
        ];

        let (deleted, errs) = set_disks.delete_objects(bucket, objects, ObjectOptions::default()).await;

        assert_eq!(deleted.len(), 3);
        assert_eq!(errs.len(), 3);
        assert!(
            errs.iter().all(Option::is_none),
            "S3 batch delete reports missing keys as deleted, not as errors: {errs:?}"
        );
        assert_eq!(deleted[0].object_name, "obj-a");
        assert_eq!(deleted[1].object_name, "missing-b");
        assert_eq!(deleted[2].object_name, "obj-c");
        assert!(deleted[0].found, "existing key must be reported as found");
        assert!(!deleted[1].found, "missing key must be reported as not found");
        assert!(deleted[2].found, "existing key must be reported as found");

        for object in ["obj-a", "obj-c"] {
            set_disks
                .get_object_info(bucket, object, &ObjectOptions::default())
                .await
                .expect_err("deleted object must be gone");
        }
    }

    #[tokio::test]
    async fn delete_objects_blocks_locked_object_and_deletes_the_rest() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "hp8-locked-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }

        // COMPLIANCE retention metadata on the object; this bucket is unknown
        // to the metadata sys, so the fail-closed gate must keep the held-lock
        // stat and the #4297 rejection.
        let retain_until = OffsetDateTime::now_utc() + Duration::from_secs(60 * 60 * 24 * 30);
        let mut user_defined = HashMap::new();
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_MODE.as_str().to_string(),
            s3s::dto::ObjectLockRetentionMode::COMPLIANCE.to_string(),
        );
        user_defined.insert(
            X_AMZ_OBJECT_LOCK_RETAIN_UNTIL_DATE.as_str().to_string(),
            retain_until
                .format(&time::format_description::well_known::Rfc3339)
                .expect("retain-until date should format"),
        );
        let mut reader = PutObjReader::from_vec(vec![7u8; 512]);
        set_disks
            .put_object(
                bucket,
                "locked",
                &mut reader,
                &ObjectOptions {
                    user_defined,
                    ..Default::default()
                },
            )
            .await
            .expect("locked object should be written");

        put_plain_object(&set_disks, bucket, "plain").await;

        let objects = vec![
            ObjectToDelete {
                object_name: "locked".to_string(),
                ..Default::default()
            },
            ObjectToDelete {
                object_name: "plain".to_string(),
                ..Default::default()
            },
        ];

        let (_deleted, errs) = set_disks.delete_objects(bucket, objects, ObjectOptions::default()).await;

        let lock_err = errs[0]
            .as_ref()
            .expect("COMPLIANCE retention must block the batch delete entry");
        assert!(
            matches!(lock_err, Error::PrefixAccessDenied(_, _)),
            "locked entry must fail with access denied, got: {lock_err:?}"
        );
        assert!(errs[1].is_none(), "unlocked entry must still be deleted: {:?}", errs[1]);

        set_disks
            .get_object_info(bucket, "locked", &ObjectOptions::default())
            .await
            .expect("locked object must survive the batch delete");
        set_disks
            .get_object_info(bucket, "plain", &ObjectOptions::default())
            .await
            .expect_err("plain object must be deleted");
    }

    #[tokio::test]
    async fn delete_objects_honors_no_lock_when_outer_write_lock_is_held() {
        let (_temp_dirs, disk_stores, set_disks) = hermetic_set_disks(4).await;
        let bucket = "batch-no-lock-bucket";
        for disk in &disk_stores {
            disk.make_volume(bucket).await.expect("bucket volume should be created");
        }
        put_plain_object(&set_disks, bucket, "obj-a").await;

        let _outer_guard = set_disks
            .new_ns_lock(bucket, "obj-a")
            .await
            .expect("namespace lock should be created")
            .get_write_lock(Duration::from_secs(1))
            .await
            .expect("outer write lock should be acquired");

        let objects = vec![ObjectToDelete {
            object_name: "obj-a".to_string(),
            ..Default::default()
        }];

        let (deleted, errs) = tokio::time::timeout(
            Duration::from_secs(1),
            set_disks.delete_objects(
                bucket,
                objects,
                ObjectOptions {
                    no_lock: true,
                    ..Default::default()
                },
            ),
        )
        .await
        .expect("no_lock batch delete path must not wait for the outer lock");

        assert!(errs[0].is_none(), "no_lock batch delete should not fail with a lock error: {:?}", errs[0]);
        assert!(deleted[0].found, "existing object must still be deleted");
    }
}

#[cfg(test)]
mod body_cache_hook_gate_tests {
    //! Regression coverage for backlog#1108 (raw data-movement reads) and
    //! backlog#1109 (compressed objects): the app-layer body-cache hook serves
    //! cached full-object plaintext directly, bypassing `ReadPlan`. It must not
    //! be probed when the normal read path would return a different byte stream.

    use super::full_object_plaintext_len;
    use crate::object_api::{ObjectInfo, ObjectOptions};
    use std::collections::HashMap;
    use std::sync::Arc;

    const STORED_SIZE: i64 = 1024;
    const PLAINTEXT_SIZE: i64 = 4096;

    fn plain_object_info() -> ObjectInfo {
        ObjectInfo {
            size: STORED_SIZE,
            ..Default::default()
        }
    }

    fn compressed_object_info() -> ObjectInfo {
        let mut user_defined = HashMap::new();
        // is_compressed() checks for the internal compression suffix key.
        user_defined.insert("x-rustfs-internal-compression".to_string(), "klauspost/compress/s2".to_string());
        user_defined.insert("x-rustfs-internal-actual-size".to_string(), PLAINTEXT_SIZE.to_string());
        ObjectInfo {
            size: STORED_SIZE,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        }
    }

    fn encrypted_object_info() -> ObjectInfo {
        let mut user_defined = HashMap::new();
        user_defined.insert("x-minio-encryption-key".to_string(), "opaque".to_string());
        ObjectInfo {
            size: STORED_SIZE,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        }
    }

    fn restore_opts() -> ObjectOptions {
        let mut opts = ObjectOptions::default();
        opts.transition.restore_request.days = Some(1);
        opts
    }

    #[test]
    fn plain_full_object_read_yields_its_stored_size() {
        let len = full_object_plaintext_len(&None, &ObjectOptions::default(), &plain_object_info());
        assert_eq!(len, Some(STORED_SIZE));
    }

    #[test]
    fn compressed_object_yields_decompressed_size() {
        // ReadTransform::Compressed publishes the decompressed length as
        // object_info.size; the hit site must reproduce exactly this value or
        // UploadPartCopy truncates the copy (backlog#1109).
        let len = full_object_plaintext_len(&None, &ObjectOptions::default(), &compressed_object_info());
        assert_eq!(len, Some(PLAINTEXT_SIZE));
    }

    #[test]
    fn raw_data_movement_read_is_refused() {
        // Decommission reads set raw_data_movement_read: ReadPlan returns the
        // STORED bytes, so the cached decompressed body must not be served
        // (backlog#1108 — silent data corruption on the destination pool).
        let opts = ObjectOptions {
            raw_data_movement_read: true,
            ..Default::default()
        };
        assert_eq!(full_object_plaintext_len(&None, &opts, &plain_object_info()), None);
    }

    #[test]
    fn data_movement_read_is_refused() {
        let opts = ObjectOptions {
            data_movement: true,
            ..Default::default()
        };
        assert_eq!(full_object_plaintext_len(&None, &opts, &plain_object_info()), None);
    }

    #[test]
    fn restore_read_of_compressed_object_is_refused() {
        // restore_request_active forces ReadPlan down the Plain branch, so the
        // read yields STORED (compressed) bytes under the compressed size.
        assert_eq!(full_object_plaintext_len(&None, &restore_opts(), &compressed_object_info()), None);
    }

    #[test]
    fn restore_read_of_plain_object_is_refused() {
        assert_eq!(full_object_plaintext_len(&None, &restore_opts(), &plain_object_info()), None);
    }

    #[test]
    fn encrypted_object_is_refused() {
        assert_eq!(
            full_object_plaintext_len(&None, &ObjectOptions::default(), &encrypted_object_info()),
            None
        );
    }

    #[test]
    fn compressed_object_without_actual_size_is_refused() {
        // A compressed object whose actual size cannot be resolved must not be
        // served from cache: there is no length to publish as object_info.size.
        let mut user_defined = HashMap::new();
        user_defined.insert("x-rustfs-internal-compression".to_string(), "klauspost/compress/s2".to_string());
        let info = ObjectInfo {
            size: STORED_SIZE,
            user_defined: Arc::new(user_defined),
            ..Default::default()
        };
        assert_eq!(full_object_plaintext_len(&None, &ObjectOptions::default(), &info), None);
    }
}

#[cfg(test)]
mod body_cache_hook_e2e_tests {
    //! End-to-end regression coverage for the two merged P0 data-corruption
    //! fixes, driving the *real* `get_object_reader` gate + probe + reader
    //! construction rather than the `full_object_plaintext_len` predicate in
    //! isolation. The predicate-only tests above cannot catch a caller that
    //! opens a new shortcut serving the cached body directly — which was the
    //! original form of both P0s (backlog#1108 / #1109) and the reason ODC-21
    //! (backlog#1126) made the hook re-registrable so these tests can install a
    //! deterministic hook.
    //!
    //! A stand-in `GetObjectBodyCacheHook` plays the app-layer cache: the
    //! adapter itself lives above ecstore and cannot be reached from here, but
    //! the hook trait is exactly the production injection point, so exercising
    //! it through `get_object_reader` is a true end-to-end test of the ecstore
    //! side (gate decision -> probe -> served bytes and published size).
    //!
    //! The tests share one process-global hook slot, so they serialize under a
    //! single key and clear the slot on the way out; each uses a unique
    //! bucket/object and the hook returns `None` for anything else, so a stray
    //! concurrent GET in another test is unaffected.

    use crate::ecstore_validation_blackbox::make_local_set_disks;
    use crate::io_support::rio::{HashReader, compression_metadata_value, compression_reader};
    use crate::object_api::{
        GetObjectBodyCacheHook, GetObjectBodySource, ObjectInfo, ObjectOptions, PutObjReader, clear_get_object_body_cache_hook,
        register_get_object_body_cache_hook,
    };
    use crate::set_disk::SetDisks;
    use crate::storage_api_contracts::bucket::{BucketOperations as _, MakeBucketOptions};
    use crate::storage_api_contracts::object::ObjectIO as _;
    use bytes::Bytes;
    use http::HeaderMap;
    use rustfs_utils::CompressionAlgorithm;
    use rustfs_utils::http::{SUFFIX_ACTUAL_SIZE, SUFFIX_COMPRESSION, insert_str};
    use std::collections::HashMap;
    use std::io::Cursor;
    use std::sync::Arc;
    use tokio::io::AsyncReadExt as _;

    /// A stand-in for the app-layer object-data cache: returns `body` only for
    /// the one primed key, mirroring how the real adapter answers a probe.
    struct PrimedBodyHook {
        bucket: String,
        object: String,
        body: Bytes,
    }

    #[async_trait::async_trait]
    impl GetObjectBodyCacheHook for PrimedBodyHook {
        async fn lookup(&self, bucket: &str, object: &str, _info: &ObjectInfo) -> Option<Bytes> {
            (bucket == self.bucket && object == self.object).then(|| self.body.clone())
        }
    }

    /// Registers a primed hook and clears it on drop so no hook leaks into an
    /// unrelated test sharing the process-global slot.
    struct HookGuard;
    impl HookGuard {
        fn install(bucket: &str, object: &str, body: Bytes) -> Self {
            register_get_object_body_cache_hook(Arc::new(PrimedBodyHook {
                bucket: bucket.to_string(),
                object: object.to_string(),
                body,
            }));
            HookGuard
        }
    }
    impl Drop for HookGuard {
        fn drop(&mut self) {
            clear_get_object_body_cache_hook();
        }
    }

    /// Compresses `plaintext` with the same codec the real PUT path uses, so
    /// the stored bytes round-trip through `get_object_reader`'s decompressor.
    async fn compress(plaintext: &[u8]) -> Vec<u8> {
        let mut reader = compression_reader(Cursor::new(plaintext.to_vec()), CompressionAlgorithm::default(), false);
        let mut compressed = Vec::new();
        reader.read_to_end(&mut compressed).await.expect("compress plaintext");
        compressed
    }

    /// Writes a genuinely compressed object: the stored data is `compressed`
    /// and the metadata marks it compressed with `plaintext.len()` as the
    /// decompressed length, exactly as the app-layer compress path records it.
    async fn put_compressed_object(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, plaintext: &[u8], compressed: &[u8]) {
        let mut user_defined = HashMap::new();
        insert_str(
            &mut user_defined,
            SUFFIX_COMPRESSION,
            compression_metadata_value(CompressionAlgorithm::default()),
        );
        insert_str(&mut user_defined, SUFFIX_ACTUAL_SIZE, plaintext.len().to_string());

        let opts = ObjectOptions {
            no_lock: true,
            user_defined,
            ..Default::default()
        };
        let stream = HashReader::from_stream(
            Cursor::new(compressed.to_vec()),
            compressed.len() as i64, // stored (compressed) size
            plaintext.len() as i64,  // actual (decompressed) size
            None,
            None,
            false,
        )
        .expect("hash reader over compressed bytes");
        let mut reader = PutObjReader::new(stream);
        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("compressed object should be written");
    }

    async fn read_to_vec(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, opts: &ObjectOptions) -> (Vec<u8>, i64) {
        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), opts)
            .await
            .expect("object reader should open");
        let published_size = reader.object_info.size;
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("object should stream");
        (body, published_size)
    }

    /// A compressible payload large enough to guarantee stored != plaintext and
    /// to keep the object off the inline / direct-memory fast paths.
    fn compressible_plaintext() -> Vec<u8> {
        b"rustfs-body-cache-e2e-regression-".repeat(20_000)
    }

    /// Writes a plain (uncompressed, unencrypted) object of `data`, large enough
    /// to keep it off the inline fast path so the cache hook is actually probed.
    async fn put_plain_object(set_disks: &Arc<SetDisks>, bucket: &str, object: &str, data: &[u8]) {
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let stream = HashReader::from_stream(Cursor::new(data.to_vec()), data.len() as i64, data.len() as i64, None, None, false)
            .expect("hash reader over plain bytes");
        let mut reader = PutObjReader::new(stream);
        set_disks
            .make_bucket(bucket, &MakeBucketOptions::default())
            .await
            .expect("bucket should be created");
        set_disks
            .put_object(bucket, object, &mut reader, &opts)
            .await
            .expect("plain object should be written");
    }

    /// ODC-16: a cache-hook hit must mark the reader `HookServed` so the app
    /// layer serves the buffered body without a second lookup. A large plain
    /// object stays off the inline fast path, so the hook is genuinely probed.
    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn plain_cache_hit_marks_reader_hook_served() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "e2e-body-cache-hook-served";
        let object = "plain.bin";
        let payload = b"rustfs-hook-served-payload-".repeat(40_000);
        put_plain_object(&set_disks, bucket, object, &payload).await;

        let _guard = HookGuard::install(bucket, object, Bytes::from(payload.clone()));
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let mut reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("object reader should open");

        assert_eq!(
            reader.body_source,
            GetObjectBodySource::HookServed,
            "a hook hit must mark the reader HookServed"
        );
        assert!(reader.buffered_body.is_some(), "a hook-served reader carries the cache body");
        let mut body = Vec::new();
        reader.stream.read_to_end(&mut body).await.expect("object should stream");
        assert_eq!(body, payload, "the hook-served body must be the primed plaintext");
    }

    /// ODC-16: when the hook is registered but misses this object, the reader
    /// must be marked `HookMissed` so the app layer skips its now-redundant
    /// lookup (the hook's miss ran after fresh metadata resolution).
    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn plain_cache_miss_marks_reader_hook_missed() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "e2e-body-cache-hook-missed";
        let object = "plain.bin";
        let payload = b"rustfs-hook-missed-payload-".repeat(40_000);
        put_plain_object(&set_disks, bucket, object, &payload).await;

        // Register a hook primed for a DIFFERENT object, so this read is probed
        // (hook registered + eligible) but the probe misses.
        let _guard = HookGuard::install(bucket, "other-object", Bytes::from_static(b"unrelated"));
        let opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        let reader = set_disks
            .get_object_reader(bucket, object, None, HeaderMap::new(), &opts)
            .await
            .expect("object reader should open");

        assert_eq!(
            reader.body_source,
            GetObjectBodySource::HookMissed,
            "a probed miss must mark the reader HookMissed"
        );
    }

    /// backlog#1108: a raw data-movement read (decommission/rebalance copy) must
    /// yield the STORED (compressed) representation, never the cached plaintext.
    /// Serving the cache here writes decompressed bytes into the destination
    /// pool under the compressed object's metadata — silent corruption.
    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn raw_data_movement_read_serves_stored_bytes_not_cached_plaintext() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "e2e-body-cache-raw-move";
        let object = "compressed.bin";
        let plaintext = compressible_plaintext();
        let compressed = compress(&plaintext).await;
        assert_ne!(compressed, plaintext, "fixture must be genuinely compressed");
        put_compressed_object(&set_disks, bucket, object, &plaintext, &compressed).await;

        // Prime the cache with the DECOMPRESSED plaintext.
        let _guard = HookGuard::install(bucket, object, Bytes::from(plaintext.clone()));

        // decommission_object_migration_read_opts sets both raw_data_movement_read
        // and data_movement; ReadPlan keys the stored-bytes path on the former.
        let opts = ObjectOptions {
            no_lock: true,
            raw_data_movement_read: true,
            ..Default::default()
        };
        let (body, published_size) = read_to_vec(&set_disks, bucket, object, &opts).await;

        assert_eq!(body, compressed, "raw data-movement read must return the stored compressed bytes");
        assert_ne!(body, plaintext, "raw data-movement read must NOT return the cached plaintext");
        assert_eq!(published_size, compressed.len() as i64, "raw read must publish the stored size");
    }

    /// backlog#1109: on a cache hit for a compressed object the served
    /// `object_info.size` must be the DECOMPRESSED length (what
    /// ReadTransform::Compressed publishes), and the streamed body length must
    /// match it. UploadPartCopy reads the copy length straight off this field.
    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn compressed_cache_hit_publishes_decompressed_size() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "e2e-body-cache-compressed";
        let object = "compressed.bin";
        let plaintext = compressible_plaintext();
        let compressed = compress(&plaintext).await;
        assert_ne!(compressed, plaintext, "fixture must be genuinely compressed");
        put_compressed_object(&set_disks, bucket, object, &plaintext, &compressed).await;

        let normal_opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };

        // Control: with no hook the real decode path must decompress to the
        // plaintext and publish the decompressed length. This also proves the
        // fixture round-trips (stored compressed bytes -> plaintext).
        clear_get_object_body_cache_hook();
        let (control_body, control_size) = read_to_vec(&set_disks, bucket, object, &normal_opts).await;
        assert_eq!(control_body, plaintext, "real read must decompress to the plaintext");
        assert_eq!(control_size, plaintext.len() as i64, "real read must publish the decompressed size");

        // Hit: prime with the plaintext and read normally. The probe must serve
        // it AND republish the decompressed length, not the stored size.
        let _guard = HookGuard::install(bucket, object, Bytes::from(plaintext.clone()));
        let (hit_body, hit_size) = read_to_vec(&set_disks, bucket, object, &normal_opts).await;
        assert_eq!(
            hit_size,
            plaintext.len() as i64,
            "cache hit must publish the decompressed size (backlog#1109)"
        );
        assert_ne!(hit_size, compressed.len() as i64, "cache hit must not publish the stored compressed size");
        assert_eq!(hit_body.len() as i64, hit_size, "streamed length must equal the published size");
        assert_eq!(hit_body, plaintext, "cache hit must stream the plaintext");
    }

    /// backlog#1146: a restore read forces ReadPlan down the Plain branch, so a
    /// compressed object yields its STORED bytes under the compressed size. The
    /// cache (holding plaintext) must not be served.
    #[tokio::test]
    #[serial_test::serial(body_cache_hook)]
    async fn restore_read_serves_stored_bytes_not_cached_plaintext() {
        let (_dirs, set_disks) = make_local_set_disks(4, 2).await;
        let bucket = "e2e-body-cache-restore";
        let object = "compressed.bin";
        let plaintext = compressible_plaintext();
        let compressed = compress(&plaintext).await;
        assert_ne!(compressed, plaintext, "fixture must be genuinely compressed");
        put_compressed_object(&set_disks, bucket, object, &plaintext, &compressed).await;

        let _guard = HookGuard::install(bucket, object, Bytes::from(plaintext.clone()));

        let mut opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        opts.transition.restore_request.days = Some(1);
        let (body, published_size) = read_to_vec(&set_disks, bucket, object, &opts).await;

        assert_eq!(body, compressed, "restore read must return the stored compressed bytes");
        assert_ne!(body, plaintext, "restore read must NOT return the cached plaintext");
        assert_eq!(
            published_size,
            compressed.len() as i64,
            "restore read must publish the stored compressed size"
        );
    }
}
