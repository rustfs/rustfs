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
        let stage_metrics_enabled = rustfs_io_metrics::get_stage_metrics_enabled();
        // Check if lock optimization is enabled for reads that are fully materialized in memory.
        let lock_optimization_enabled = is_lock_optimization_enabled();

        // Acquire a shared read-lock early to protect read consistency
        let mut read_lock_guard = if !opts.no_lock {
            let acquire_start = Instant::now();
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

            // Record lock statistics
            metrics::counter!("rustfs.lock.acquire.total", "type" => "read").increment(1);
            metrics::histogram!("rustfs.lock.acquire.duration.seconds").record(acquire_start.elapsed().as_secs_f64());
            record_get_stage_duration_if_enabled(GET_OBJECT_PATH_SET_DISK, GET_STAGE_LOCK_ACQUIRE, lock_stage_start);

            Some(guard)
        } else {
            None
        };

        let metadata_stage_start = Instant::now();
        let (fi, files, disks) = match self.get_object_fileinfo(bucket, object, opts, true).await {
            Ok(result) => result,
            Err(err) => {
                rustfs_io_metrics::record_get_object_metadata_phase_duration(metadata_stage_start.elapsed().as_secs_f64());
                record_get_object_pipeline_failure(GET_STAGE_METADATA, classify_storage_error(&err));
                return Err(to_object_err(err, vec![bucket, object]));
            }
        };
        let object_info_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let object_info = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);
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
                    };
                    return Ok(reader);
                }
            }

            let erasure = coding::Erasure::new_with_options(
                fi.erasure.data_blocks,
                fi.erasure.parity_blocks,
                fi.erasure.block_size,
                fi.uses_legacy_checksum,
            );
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
                };
                return Ok(reader);
            }
        }

        let path_decision_stage_start = get_stage_timer_if_enabled(stage_metrics_enabled);
        let codec_streaming_gate =
            get_codec_streaming_reader_gate(bucket, object, &range, &object_info, &fi, lock_optimization_enabled);
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
            let gr = get_transitioned_object_reader(bucket, object, &range, &h, &object_info, &opts).await?;
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
        if range.is_none()
            && opts.part_number.is_none()
            && let Some(hook) = get_object_body_cache_hook()
            && let Some(body) = hook.lookup(bucket, object, &object_info).await
        {
            record_get_object_reader_path_observation(GET_OBJECT_PATH_BODY_CACHE, object_class, size_bucket);
            let reader = GetObjectReader {
                stream: Box::new(Cursor::new(body.clone())),
                object_info,
                buffered_body: Some(body),
            };
            if lock_optimization_enabled {
                release_materialized_read_lock(bucket, object, read_lock_guard.take());
            }
            return Ok(reader);
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
                        let (reader, _offset, _length) = GetObjectReader::new(stream, range, &object_info, opts, &h).await?;
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

        let (reader, offset, length) = GetObjectReader::new(Box::new(rd), range, &object_info, opts, &h).await?;

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

    #[tracing::instrument(skip(self, data,))]
    async fn put_object(&self, bucket: &str, object: &str, data: &mut PutObjReader, opts: &ObjectOptions) -> Result<ObjectInfo> {
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

        let mut user_defined = opts.user_defined.clone();
        if let Some(eval_metadata) = &opts.eval_metadata {
            for (key, value) in eval_metadata {
                user_defined.insert(key.clone(), value.clone());
            }
        }
        let sc_parity_drives = runtime_sources::storage_class_parity(user_defined.get(AMZ_STORAGE_CLASS).map(String::as_str));

        let mut parity_drives = sc_parity_drives.unwrap_or(self.default_parity_count);
        if opts.max_parity {
            parity_drives = disks.len() / 2;
        }

        let data_drives = disks.len() - parity_drives;
        let mut write_quorum = data_drives;
        if data_drives == parity_drives {
            write_quorum += 1
        }

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

        let result: Result<ObjectInfo> = async {
            let erasure = coding::Erasure::new(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size);

            let put_object_size = known_put_object_storage_size(data.size());
            let is_inline_buffer =
                runtime_sources::storage_class_should_inline(erasure.shard_file_size(put_object_size), opts.versioned);

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

            let rename_stage_start = Instant::now();
            let (online_disks, _, op_old_dir, cleanup_disks) = Self::rename_data(
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
            record_capacity_scope_if_needed(opts.capacity_scope_token, &online_disks);

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

            Ok(ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended))
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

        result
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
                if fi.is_valid() {
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

        let mut futures = Vec::with_capacity(disks.len());
        let mut errs = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            futures.push(async move {
                if let Some(disk) = disk {
                    match disk
                        .delete_version(bucket, object, fi.clone(), force_del_marker, DeleteOptions::default())
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

        resolve_tiered_decommission_write_quorum_result(&errs, write_quorum, bucket, object)
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

        if dist_erasure {
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

        let mut vers_map: HashMap<&String, FileInfoVersions> = HashMap::new();

        for (i, dobj) in objects.iter().enumerate() {
            if del_errs[i].is_some() {
                continue;
            }

            let explicit_null_version = is_explicit_null_version(dobj.version_id);
            let version_id = delete_file_info_version_id(dobj.version_id);
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

        let disks = self.disks.read().await;

        let disks = disks.clone();

        let mut futures = Vec::with_capacity(disks.len());

        // let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let vers = vers.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.delete_versions(bucket, vers, DeleteOptions::default()).await
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

        record_capacity_scope_if_needed(opts.capacity_scope_token, &disks);

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

            self.get_object_metadata_cache.invalidate_all();
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
            record_capacity_scope_if_needed(opts.capacity_scope_token, &disks);

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
        record_capacity_scope_if_needed(opts.capacity_scope_token, &disks);

        let mut obj_info = ObjectInfo::from_file_info(&dfi, bucket, object, opts.versioned || opts.version_suspended);
        obj_info.size = goi.size;
        self.invalidate_get_object_metadata_cache(bucket, object).await;
        Ok(obj_info)
    }

    #[tracing::instrument(skip(self))]
    async fn get_object_info(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<ObjectInfo> {
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
            .get_object_fileinfo(bucket, object, opts, true)
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
        let tier_config_mgr = runtime_sources::tier_config_mgr_handle();
        let mut tier_config_mgr = tier_config_mgr.write().await;
        let tgt_client = match tier_config_mgr.get_driver(&opts.transition.tier).await {
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

        let (mut fi, meta_arr, online_disks) = self.get_object_fileinfo(bucket, object, opts, true).await?;
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
                /*|| transition_etag != stored_etag*/
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
            (fi, meta_arr, online_disks) = self.get_object_fileinfo(&bucket, &object, &opts, true);
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

        let (pr, mut pw) = tokio::io::duplex(fi.erasure.block_size);
        let reader = ReaderImpl::ObjectBody(GetObjectReader {
            stream: Box::new(pr),
            object_info: oi,
            buffered_body: None,
        });

        let cloned_bucket = bucket.to_string();
        let cloned_object = object.to_string();
        let cloned_fi = fi.clone();
        let set_index = self.set_index;
        let pool_index = self.pool_index;
        let skip_verify = opts.skip_verify_bitrot;
        let metrics_size_bucket = rustfs_io_metrics::get_object_size_bucket(cloned_fi.size);
        tokio::spawn(async move {
            if let Err(e) = Self::get_object_with_fileinfo(
                &cloned_bucket,
                &cloned_object,
                0,
                cloned_fi.size,
                &mut pw,
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
            .await
            {
                error!("get_object_with_fileinfo err {:?}", e);
            };
        });

        let rv = tgt_client.put_with_meta(&dest_obj, reader, fi.size, transition_meta).await;
        if let Err(err) = rv {
            return Err(StorageError::Io(err));
        }
        let rv = rv?;
        fi.transition_status = TRANSITION_COMPLETE.to_string();
        fi.transitioned_objname = dest_obj;
        fi.transition_tier = opts.transition.tier.clone();
        fi.transition_version_id = if rv.is_empty() { None } else { Some(Uuid::parse_str(&rv)?) };
        let event_name = EventName::LifecycleTransition.as_str();
        let mut should_notify_transition = true;

        let disks = self.disk_inventory().await;

        if let Err(err) = self.delete_object_version(bucket, object, &fi, false).await {
            should_notify_transition = false;
            warn!(
                bucket = bucket,
                object = object,
                error = ?err,
                "transition completed on remote tier but source cleanup failed; skipping external lifecycle transition notification"
            );
        } else {
            record_capacity_scope_if_needed(opts.capacity_scope_token, &disks);
        }

        for disk in disks.iter() {
            if let Some(disk) = disk {
                continue;
            }
            let _ = self
                .add_partial(bucket, object, opts.version_id.as_deref().unwrap_or_default())
                .await;
            break;
        }

        if should_notify_transition {
            let obj_info = ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended);
            send_event(EventArgs {
                event_name: event_name.to_string(),
                bucket_name: bucket.to_string(),
                object: obj_info,
                user_agent: "Internal: [ILM-Transition]".to_string(),
                host: runtime_sources::default_local_node_name(),
                ..Default::default()
            });
        }
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
        let fi = self_.clone().get_object_fileinfo(bucket, object, opts, true).await;
        if let Err(err) = fi {
            return set_restore_header_fn(&mut oi, Some(to_object_err(err, vec![bucket, object]))).await;
        }
        let (actual_fi, _, _) = fi?;

        oi = ObjectInfo::from_file_info(&actual_fi, bucket, object, opts.versioned || opts.version_suspended);
        let ropts = put_restore_opts(bucket, object, &opts.transition.restore_request, &oi).await?;
        if oi.parts.len() == 1 {
            let mut opts = opts.clone();
            opts.part_number = Some(1);
            let rs: Option<HTTPRangeSpec> = None;
            let gr = get_transitioned_object_reader(bucket, object, &rs, &HeaderMap::new(), &oi, &opts).await;
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
            let gr = match get_transitioned_object_reader(bucket, object, &rs, &HeaderMap::new(), &oi, &part_opts).await {
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
