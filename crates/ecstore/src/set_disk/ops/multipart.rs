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

//! `MultipartOperations` for `SetDisks` plus its multipart helpers.
//!
//! P2 of the SetDisks God-Object split (tracking backlog#815, issue #818).
//! The storage-api contract impl and the multipart list/quorum helpers are
//! relocated here from `set_disk/mod.rs` and `set_disk/multipart.rs`. The
//! contract stays implemented `for SetDisks`, so its associated-type bounds are
//! unchanged; method bodies are moved verbatim and runtime behavior is the same.

use super::super::*;
use std::future::Future;
use std::time::Duration;
use tokio::task::JoinSet;

fn map_upload_id_metadata_error(bucket: &str, object: &str, upload_id: &str, err: DiskError) -> Error {
    if err == DiskError::FileNotFound {
        return StorageError::InvalidUploadID(bucket.to_owned(), object.to_owned(), upload_id.to_owned());
    }
    err.into()
}

fn empty_upload_fallback_possible(successful_responses: usize, errs: &[Option<DiskError>]) -> bool {
    successful_responses == 0
        && errs.iter().any(|err| matches!(err, Some(DiskError::FileNotFound)))
        && errs.iter().all(|err| match err {
            Some(DiskError::FileNotFound) => true,
            Some(err) => OBJECT_OP_IGNORED_ERRS.contains(err),
            None => false,
        })
}

async fn collect_list_parts_results<F>(
    tasks: Vec<F>,
    read_quorum: usize,
) -> disk::error::Result<(Vec<Option<DiskError>>, Vec<Vec<String>>)>
where
    F: Future<Output = disk::error::Result<Vec<String>>> + Send + 'static,
{
    let mut errs = vec![Some(DiskError::DiskNotFound); tasks.len()];
    let mut object_parts = vec![Vec::new(); tasks.len()];
    let mut successful_responses = 0usize;
    let mut pending = tasks.len();
    let mut join_set = JoinSet::new();

    for (index, task) in tasks.into_iter().enumerate() {
        join_set.spawn(async move { (index, task.await) });
    }

    while let Some(join_result) = join_set.join_next().await {
        pending = pending.saturating_sub(1);

        match join_result {
            Ok((index, Ok(parts))) => {
                errs[index] = None;
                object_parts[index] = parts;
                successful_responses += 1;
            }
            Ok((index, Err(err))) => {
                errs[index] = Some(err);
            }
            Err(_) => {}
        }

        if successful_responses + pending < read_quorum && !empty_upload_fallback_possible(successful_responses, &errs) {
            return Err(DiskError::ErasureReadQuorum);
        }
    }

    if successful_responses < read_quorum {
        if empty_upload_fallback_possible(successful_responses, &errs) {
            return Err(DiskError::FileNotFound);
        }

        return Err(DiskError::ErasureReadQuorum);
    }

    Ok((errs, object_parts))
}

fn reduce_quorum_part_numbers(object_parts: Vec<Vec<String>>, read_quorum: usize) -> Vec<usize> {
    let mut part_quorum_map: HashMap<usize, usize> = HashMap::new();

    for drive_parts in object_parts {
        let mut parts_with_meta_count: HashMap<usize, usize> = HashMap::new();

        // part files can be either part.N or part.N.meta
        for part_path in drive_parts {
            if let Some(num_str) = part_path.strip_prefix("part.") {
                if let Some(meta_idx) = num_str.find(".meta") {
                    if let Ok(part_num) = num_str[..meta_idx].parse::<usize>() {
                        *parts_with_meta_count.entry(part_num).or_insert(0) += 1;
                    }
                } else if let Ok(part_num) = num_str.parse::<usize>() {
                    *parts_with_meta_count.entry(part_num).or_insert(0) += 1;
                }
            }
        }

        // Include only part.N.meta files with corresponding part.N
        for (&part_num, &cnt) in &parts_with_meta_count {
            if cnt >= 2 {
                *part_quorum_map.entry(part_num).or_insert(0) += 1;
            }
        }
    }

    let mut part_numbers = Vec::with_capacity(part_quorum_map.len());
    for (part_num, count) in part_quorum_map {
        if count >= read_quorum {
            part_numbers.push(part_num);
        }
    }

    part_numbers.sort();
    part_numbers
}

impl SetDisks {
    pub(super) async fn list_parts(
        disks: &[Option<DiskStore>],
        part_path: &str,
        read_quorum: usize,
    ) -> disk::error::Result<Vec<usize>> {
        let mut futures = Vec::with_capacity(disks.len());
        let part_path = part_path.to_string();
        for disk in disks.iter() {
            let disk = disk.clone();
            let part_path = part_path.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.list_dir(RUSTFS_META_MULTIPART_BUCKET, RUSTFS_META_MULTIPART_BUCKET, part_path.as_str(), -1)
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let mut errs = Vec::with_capacity(disks.len());
        let mut object_parts = Vec::with_capacity(disks.len());

        let (collected_errs, collected_parts) = collect_list_parts_results(futures, read_quorum).await?;
        errs.extend(collected_errs);
        object_parts.extend(collected_parts);

        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum) {
            return Err(err);
        }

        Ok(reduce_quorum_part_numbers(object_parts, read_quorum))
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn check_upload_id_exists(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        write: bool,
    ) -> Result<(FileInfo, Vec<FileInfo>)> {
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);
        let disks = self.disks.read().await;

        let disks = disks.clone();

        let (parts_metadata, errs) =
            Self::read_all_fileinfo(&disks, bucket, RUSTFS_META_MULTIPART_BUCKET, &upload_id_path, "", false, false, false)
                .await?;

        let (read_quorum, write_quorum) = Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count)
            .map_err(|err| map_upload_id_metadata_error(bucket, object, upload_id, err))?;

        if read_quorum < 0 {
            error!("check_upload_id_exists: read_quorum < 0, errs={:?}", errs);
            return Err(Error::ErasureReadQuorum);
        }

        if write_quorum < 0 {
            return Err(Error::ErasureWriteQuorum);
        }

        let mut quorum = read_quorum as usize;
        if write {
            quorum = write_quorum as usize;

            if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, quorum) {
                log_multipart_write_quorum_failure(
                    MultipartWriteQuorumContext {
                        stage: MULTIPART_WRITE_QUORUM_UPLOAD_METADATA,
                        bucket,
                        object,
                        upload_id,
                        part_number: None,
                    },
                    &errs,
                    quorum,
                    &err,
                );
                return Err(map_upload_id_metadata_error(bucket, object, upload_id, err));
            }
        } else if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, quorum) {
            return Err(map_upload_id_metadata_error(bucket, object, upload_id, err));
        }

        let (_, mod_time, etag) = Self::list_online_disks(&disks, &parts_metadata, &errs, quorum);

        let fi = Self::pick_valid_fileinfo(&parts_metadata, mod_time, etag, quorum)?;

        Ok((fi, parts_metadata))
    }
}

#[async_trait::async_trait]
impl crate::storage_api_contracts::multipart::MultipartOperations for SetDisks {
    type Error = Error;
    type ObjectInfo = ObjectInfo;
    type ObjectOptions = ObjectOptions;
    type PutObjectReader = PutObjReader;
    type CompletePart = CompletePart;
    type ListMultipartsInfo = ListMultipartsInfo;
    type MultipartUploadResult = MultipartUploadResult;
    type PartInfo = PartInfo;
    type MultipartInfo = MultipartInfo;
    type ListPartsInfo = ListPartsInfo;

    #[tracing::instrument(skip(self))]
    async fn copy_object_part(
        &self,
        _src_bucket: &str,
        _src_object: &str,
        _dst_bucket: &str,
        _dst_object: &str,
        _upload_id: &str,
        _part_id: usize,
        _start_offset: i64,
        _length: i64,
        _src_info: &ObjectInfo,
        _src_opts: &ObjectOptions,
        _dst_opts: &ObjectOptions,
    ) -> Result<()> {
        Err(StorageError::NotImplemented)
    }

    #[tracing::instrument(level = "debug", skip(self, data, opts))]
    async fn put_object_part(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_id: usize,
        data: &mut PutObjReader,
        opts: &ObjectOptions,
    ) -> Result<PartInfo> {
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);

        let (fi, _) = self.check_upload_id_exists(bucket, object, upload_id, true).await?;

        let write_quorum = fi.write_quorum(self.default_write_quorum());

        if let Some(checksum) = fi.metadata.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM)
            && !checksum.is_empty()
            && data
                .as_hash_reader()
                .content_crc_type()
                .is_none_or(|v| v.to_string() != *checksum)
        {
            return Err(Error::other(format!("checksum mismatch: {checksum}")));
        }

        let disks = self.get_disks_internal().await;
        // let (disks, filtered_online) = self.filter_online_disks(disks_snapshot).await;

        // if filtered_online < write_quorum {
        //     warn!(
        //         "online disk snapshot {} below write quorum {} for multipart {}/{}; returning erasure write quorum error",
        //         filtered_online, write_quorum, bucket, object
        //     );
        //     return Err(to_object_err(Error::ErasureWriteQuorum, vec![bucket, object]));
        // }

        let shuffle_disks = Self::shuffle_disks(&disks, &fi.erasure.distribution);

        let part_suffix = format!("part.{part_id}");
        let tmp_part = format!("{}x{}", Uuid::new_v4(), OffsetDateTime::now_utc().unix_timestamp());
        let tmp_part_path = Arc::new(format!("{tmp_part}/{part_suffix}"));

        let erasure = coding::Erasure::new(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size);
        let writer_setup_stage_start = rustfs_io_metrics::put_stage_metrics_enabled().then(Instant::now);

        let mut writers = Vec::with_capacity(shuffle_disks.len());
        let mut errors = Vec::with_capacity(shuffle_disks.len());
        for disk_op in shuffle_disks.iter() {
            if let Some(disk) = disk_op {
                let writer = match create_bitrot_writer(
                    false,
                    Some(disk),
                    RUSTFS_META_TMP_BUCKET,
                    &tmp_part_path,
                    erasure.shard_file_size(data.size()),
                    erasure.shard_size(),
                    HashAlgorithm::HighwayHash256S,
                )
                .await
                {
                    Ok(writer) => writer,
                    Err(err) => {
                        warn!(
                            event = EVENT_SET_DISK_MULTIPART,
                            component = LOG_COMPONENT_ECSTORE,
                            subsystem = LOG_SUBSYSTEM_SET_DISK,
                            disk = ?disk,
                            state = "bitrot_writer_skipped",
                            error = ?err,
                            "Set disk multipart bitrot writer skipped"
                        );
                        errors.push(Some(err));
                        writers.push(None);
                        continue;
                    }
                };

                writers.push(Some(writer));
                errors.push(None);
            } else {
                errors.push(Some(DiskError::DiskNotFound));
                writers.push(None);
            }
        }

        if let Some(stage_start) = writer_setup_stage_start {
            rustfs_io_metrics::record_put_object_stage_duration(
                "multipart_set_disk_writer_setup",
                stage_start.elapsed().as_secs_f64() * 1000.0,
            );
        }

        let nil_count = errors.iter().filter(|&e| e.is_none()).count();
        if nil_count < write_quorum {
            if let Some(write_err) = reduce_write_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, write_quorum) {
                log_multipart_write_quorum_failure(
                    MultipartWriteQuorumContext {
                        stage: MULTIPART_WRITE_QUORUM_WRITER_SETUP,
                        bucket,
                        object,
                        upload_id,
                        part_number: Some(part_id),
                    },
                    &errors,
                    write_quorum,
                    &write_err,
                );
                return Err(to_object_err(write_err.into(), vec![bucket, object]));
            }

            return Err(Error::other(format!("not enough disks to write: {errors:?}")));
        }

        // Capture the original part size before swapping the stream out for encoding.
        let multipart_part_size = data.size();
        let stream = mem::replace(
            &mut data.stream,
            HashReader::from_stream(Cursor::new(Vec::new()), 0, 0, None, None, false)?,
        );

        let write_path = classify_multipart_part_write_path(multipart_part_size, fi.erasure.block_size);
        rustfs_io_metrics::record_put_object_path(write_path.multipart_metric_label());
        let encode_stage_start = rustfs_io_metrics::put_stage_metrics_enabled().then(Instant::now);

        let (reader, w_size) = match write_path {
            SmallWritePath::SingleBlockNonInline => {
                Arc::new(erasure)
                    .encode_single_block_non_inline(stream, &mut writers, write_quorum)
                    .await?
            }
            SmallWritePath::PipelineBatchedLarge => Arc::new(erasure).encode_batched(stream, &mut writers, write_quorum).await?,
            SmallWritePath::Inline | SmallWritePath::Pipeline => {
                Arc::new(erasure).encode(stream, &mut writers, write_quorum).await?
            }
        }; // TODO: delete temporary directory on error

        if let Some(stage_start) = encode_stage_start {
            rustfs_io_metrics::record_put_object_stage_duration(
                "multipart_set_disk_encode",
                stage_start.elapsed().as_secs_f64() * 1000.0,
            );
        }

        let _ = mem::replace(&mut data.stream, reader);

        if (w_size as i64) < data.size() {
            warn!(
                event = EVENT_SET_DISK_MULTIPART,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                bucket,
                object,
                part_number = part_id,
                written_size = w_size,
                expected_size = data.size(),
                state = "short_write",
                "Set disk multipart write produced fewer bytes than expected"
            );
            return Err(Error::other(format!(
                "put_object_part write size < data.size(), w_size={}, data.size={}",
                w_size,
                data.size()
            )));
        }

        let index_op = data
            .stream
            .try_get_index()
            .map(crate::io_support::rio::compression_index_storage_bytes);

        let mut etag = data.stream.try_resolve_etag().unwrap_or_default();

        if let Some(ref tag) = opts.preserve_etag {
            etag = tag.clone();
        }

        let mut actual_size = data.actual_size();
        if actual_size < 0 {
            let is_compressed = fi.is_compressed();
            if !is_compressed {
                actual_size = w_size as i64;
            }
        }

        if fi.is_compressed() {
            record_compression_total_memory(actual_size as u64, w_size as u64).await;
        }
        let checksums = data.as_hash_reader().content_crc();

        let part_info = ObjectPartInfo {
            etag: etag.clone(),
            number: part_id,
            size: w_size,
            mod_time: Some(OffsetDateTime::now_utc()),
            actual_size,
            index: index_op,
            checksums: if checksums.is_empty() { None } else { Some(checksums) },
            ..Default::default()
        };

        let part_info_buff = part_info.marshal_msg()?;

        drop(writers); // drop writers to close all files

        let part_path = format!("{}/{}/{}", upload_id_path, fi.data_dir.unwrap_or_default(), part_suffix);
        let _ = self
            .rename_part(
                &disks,
                RUSTFS_META_TMP_BUCKET,
                &tmp_part_path,
                RUSTFS_META_MULTIPART_BUCKET,
                &part_path,
                part_info_buff.into(),
                write_quorum,
                Some(MultipartWriteQuorumContext {
                    stage: MULTIPART_WRITE_QUORUM_RENAME_PART,
                    bucket,
                    object,
                    upload_id,
                    part_number: Some(part_id),
                }),
            )
            .await?;

        let ret: PartInfo = PartInfo {
            etag: Some(etag.clone()),
            part_num: part_id,
            last_mod: Some(OffsetDateTime::now_utc()),
            size: w_size,
            actual_size,
        };

        // error!("put_object_part ret {:?}", &ret);

        Ok(ret)
    }

    #[tracing::instrument(skip(self))]
    async fn list_object_parts(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        part_number_marker: Option<usize>,
        mut max_parts: usize,
        opts: &ObjectOptions,
    ) -> Result<ListPartsInfo> {
        let (fi, _) = self.check_upload_id_exists(bucket, object, upload_id, false).await?;

        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);

        if max_parts > MAX_PARTS_COUNT {
            max_parts = MAX_PARTS_COUNT;
        }

        let part_number_marker = part_number_marker.unwrap_or_default();

        // Extract storage class from metadata, default to STANDARD if not found
        let storage_class = fi
            .metadata
            .get(AMZ_STORAGE_CLASS)
            .cloned()
            .unwrap_or_else(|| storageclass::STANDARD.to_string());

        let mut ret = ListPartsInfo {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            upload_id: upload_id.to_owned(),
            storage_class,
            max_parts,
            part_number_marker,
            user_defined: {
                let mut metadata = fi.metadata.clone();
                strip_internal_multipart_metadata(&mut metadata);
                metadata
            },
            ..Default::default()
        };

        if max_parts == 0 {
            return Ok(ret);
        }

        let online_disks = self.get_disks_internal().await;

        let read_quorum = fi.read_quorum(self.default_read_quorum());

        let part_path = format!(
            "{}{}",
            path_join_buf(&[
                &upload_id_path,
                fi.data_dir.map(|v| v.to_string()).unwrap_or_default().as_str(),
            ]),
            SLASH_SEPARATOR
        );

        let mut part_numbers = match Self::list_parts(&online_disks, &part_path, read_quorum).await {
            Ok(parts) => parts,
            Err(err) => {
                if err == DiskError::FileNotFound {
                    return Ok(ret);
                }

                return Err(to_object_err(err.into(), vec![bucket, object]));
            }
        };

        if part_numbers.is_empty() {
            return Ok(ret);
        }
        let Some(remaining_part_numbers) = parts_after_marker(&part_numbers, part_number_marker) else {
            return Ok(ret);
        };
        part_numbers = remaining_part_numbers.to_vec();

        let mut parts = Vec::with_capacity(part_numbers.len());

        let part_meta_paths = part_numbers
            .iter()
            .map(|v| format!("{part_path}part.{v}.meta"))
            .collect::<Vec<String>>();

        let object_parts =
            Self::read_parts(&online_disks, RUSTFS_META_MULTIPART_BUCKET, &part_meta_paths, &part_numbers, read_quorum)
                .await
                .map_err(|e| to_object_err(e.into(), vec![bucket, object, upload_id]))?;

        let mut count = max_parts;

        for (i, part) in object_parts.iter().enumerate() {
            if let Some(err) = &part.error {
                warn!("list_object_parts part error: {:?}", &err);
            }

            parts.push(PartInfo {
                etag: Some(part.etag.clone()),
                part_num: part.number,
                last_mod: part.mod_time,
                size: part.size,
                actual_size: part.actual_size,
            });

            count -= 1;
            if count == 0 {
                break;
            }
        }

        ret.parts = parts;

        if object_parts.len() > ret.parts.len() {
            ret.is_truncated = true;
            ret.next_part_number_marker = ret.parts.last().map(|v| v.part_num).unwrap_or_default();
        }

        Ok(ret)
    }

    #[tracing::instrument(skip(self))]
    async fn list_multipart_uploads(
        &self,
        bucket: &str,
        object: &str,
        key_marker: Option<String>,
        upload_id_marker: Option<String>,
        delimiter: Option<String>,
        max_uploads: usize,
    ) -> Result<ListMultipartsInfo> {
        let disks = {
            let disks = self.get_online_local_disks().await;
            if disks.is_empty() {
                // TODO: getOnlineDisksWithHealing
                self.get_online_disks().await
            } else {
                disks
            }
        };

        let mut upload_ids: Vec<String> = Vec::new();

        for disk in disks.iter().flatten() {
            if !disk.is_online().await {
                continue;
            }

            let has_uoload_ids = match disk
                .list_dir(
                    bucket,
                    RUSTFS_META_MULTIPART_BUCKET,
                    Self::get_multipart_sha_dir(bucket, object).as_str(),
                    -1,
                )
                .await
            {
                Ok(res) => Some(res),
                Err(err) => {
                    if err == DiskError::DiskNotFound {
                        None
                    } else if err == DiskError::FileNotFound {
                        return Ok(ListMultipartsInfo {
                            key_marker: key_marker.to_owned(),
                            max_uploads,
                            prefix: object.to_owned(),
                            delimiter: delimiter.to_owned(),
                            ..Default::default()
                        });
                    } else {
                        return Err(to_object_err(err.into(), vec![bucket, object]));
                    }
                }
            };

            if let Some(ids) = has_uoload_ids {
                upload_ids = ids;
                break;
            }
        }

        let mut uploads = Vec::new();

        let mut populated_upload_ids = HashSet::new();

        for upload_id in upload_ids.iter() {
            let upload_id = upload_id.trim_end_matches(SLASH_SEPARATOR).to_string();
            if populated_upload_ids.contains(&upload_id) {
                continue;
            }

            let start_time = {
                let now = OffsetDateTime::now_utc();

                let splits: Vec<&str> = upload_id.split("x").collect();
                if splits.len() == 2 {
                    if let Ok(unix) = splits[1].parse::<i128>() {
                        OffsetDateTime::from_unix_timestamp_nanos(unix)?
                    } else {
                        now
                    }
                } else {
                    now
                }
            };

            uploads.push(MultipartInfo {
                bucket: bucket.to_owned(),
                object: object.to_owned(),
                upload_id: runtime_sources::deployment_upload_id(&upload_id),
                initiated: Some(start_time),
                ..Default::default()
            });

            populated_upload_ids.insert(upload_id);
        }

        uploads.sort_by_key(|a| a.initiated);

        let mut upload_idx = 0;
        if let Some(upload_id_marker) = &upload_id_marker {
            while upload_idx < uploads.len() {
                if &uploads[upload_idx].upload_id != upload_id_marker {
                    upload_idx += 1;
                    continue;
                }

                if &uploads[upload_idx].upload_id == upload_id_marker {
                    upload_idx += 1;
                    break;
                }

                upload_idx += 1;
            }
        }

        let mut ret_uploads = Vec::new();
        let mut next_upload_id_marker = None;
        while upload_idx < uploads.len() {
            ret_uploads.push(uploads[upload_idx].clone());
            next_upload_id_marker = Some(uploads[upload_idx].upload_id.clone());
            upload_idx += 1;

            if ret_uploads.len() > max_uploads {
                break;
            }
        }

        let is_truncated = ret_uploads.len() < uploads.len();

        if !is_truncated {
            next_upload_id_marker = None;
        }

        Ok(ListMultipartsInfo {
            key_marker: key_marker.to_owned(),
            next_upload_id_marker,
            max_uploads,
            is_truncated,
            uploads: ret_uploads,
            prefix: object.to_owned(),
            delimiter: delimiter.to_owned(),
            ..Default::default()
        })
    }

    #[tracing::instrument(skip(self))]
    async fn new_multipart_upload(&self, bucket: &str, object: &str, opts: &ObjectOptions) -> Result<MultipartUploadResult> {
        let mut _object_lock_guard = None;

        if opts.http_preconditions.is_some() {
            if !opts.no_lock {
                _object_lock_guard = Some(
                    self.acquire_write_lock_diag("new_multipart_upload_precondition", bucket, object)
                        .await?,
                );
            }

            if let Some(err) = self.check_write_precondition(bucket, object, opts).await {
                return Err(err);
            }
        }

        let disks = self.disks.read().await;

        let disks = disks.clone();

        let mut user_defined = opts.user_defined.clone();

        if let Some(ref etag) = opts.preserve_etag {
            user_defined.insert("etag".to_owned(), etag.clone());
        }

        if let Some(sc) = user_defined.get(AMZ_STORAGE_CLASS)
            && sc == storageclass::STANDARD
        {
            let _ = user_defined.remove(AMZ_STORAGE_CLASS);
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

        let mut fi = FileInfo::new([bucket, object].join("/").as_str(), data_drives, parity_drives);

        fi.version_id = if let Some(vid) = &opts.version_id {
            Some(Uuid::parse_str(vid)?)
        } else {
            None
        };

        if opts.versioned && opts.version_id.is_none() {
            fi.version_id = Some(Uuid::new_v4());
        }

        fi.data_dir = Some(Uuid::new_v4());

        if let Some(cssum) = get_header_map(&user_defined, SUFFIX_REPLICATION_SSEC_CRC)
            && !cssum.is_empty()
        {
            fi.checksum = base64_simd::STANDARD.decode_to_vec(&cssum).ok().map(Bytes::from);
            remove_header_map(&mut user_defined, SUFFIX_REPLICATION_SSEC_CRC);
        }

        let parts_metadata = vec![fi.clone(); disks.len()];

        if !user_defined.contains_key("content-type") {
            // TODO: get content-type
        }

        if let Some(sc) = user_defined.get(AMZ_STORAGE_CLASS)
            && sc == storageclass::STANDARD
        {
            let _ = user_defined.remove(AMZ_STORAGE_CLASS);
        }

        if let Some(checksum) = &opts.want_checksum {
            user_defined.insert(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM.to_string(), checksum.checksum_type.to_string());
            user_defined.insert(
                rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE.to_string(),
                checksum.checksum_type.obj_type().to_string(),
            );
        }

        user_defined.insert(RUSTFS_MULTIPART_BUCKET_KEY.to_string(), bucket.to_string());
        user_defined.insert(RUSTFS_MULTIPART_OBJECT_KEY.to_string(), object.to_string());

        let (shuffle_disks, mut parts_metadatas) = Self::shuffle_disks_and_parts_metadata(&disks, &parts_metadata, &fi);

        let mod_time = opts.mod_time.unwrap_or_else(OffsetDateTime::now_utc);

        for f in parts_metadatas.iter_mut() {
            f.metadata = user_defined.clone();
            f.mod_time = Some(mod_time);
            f.fresh = true;
        }

        // fi.mod_time = Some(now);

        let upload_uuid = format!("{}x{}", Uuid::new_v4(), mod_time.unix_timestamp_nanos());

        let upload_id = runtime_sources::deployment_upload_id(&upload_uuid);

        let upload_path = Self::get_upload_id_dir(bucket, object, upload_uuid.as_str());

        Self::write_unique_file_info(
            &shuffle_disks,
            bucket,
            RUSTFS_META_MULTIPART_BUCKET,
            upload_path.as_str(),
            &parts_metadatas,
            write_quorum,
        )
        .await
        .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;

        // evalDisks

        Ok(MultipartUploadResult {
            upload_id,
            checksum_algo: user_defined.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM).cloned(),
            checksum_type: user_defined.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE).cloned(),
        })
    }

    #[tracing::instrument(skip(self))]
    async fn get_multipart_info(
        &self,
        bucket: &str,
        object: &str,
        upload_id: &str,
        _opts: &ObjectOptions,
    ) -> Result<MultipartInfo> {
        // TODO: nslock
        let (mut fi, _) = self
            .check_upload_id_exists(bucket, object, upload_id, false)
            .await
            .map_err(|e| to_object_err(e, vec![bucket, object, upload_id]))?;

        Ok(MultipartInfo {
            bucket: bucket.to_owned(),
            object: object.to_owned(),
            upload_id: upload_id.to_owned(),
            user_defined: {
                strip_internal_multipart_metadata(&mut fi.metadata);
                fi.metadata.clone()
            },
            ..Default::default()
        })
    }

    #[tracing::instrument(skip(self))]
    async fn abort_multipart_upload(&self, bucket: &str, object: &str, upload_id: &str, _opts: &ObjectOptions) -> Result<()> {
        self.check_upload_id_exists(bucket, object, upload_id, false).await?;
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);

        self.delete_all(RUSTFS_META_MULTIPART_BUCKET, &upload_id_path).await
    }
    // complete_multipart_upload finished
    #[tracing::instrument(skip(self))]
    async fn complete_multipart_upload(
        self: Arc<Self>,
        bucket: &str,
        object: &str,
        upload_id: &str,
        uploaded_parts: Vec<CompletePart>,
        opts: &ObjectOptions,
    ) -> Result<ObjectInfo> {
        self.invalidate_get_object_metadata_cache(bucket, object).await;

        let mut object_lock_guard = None;

        if opts.http_preconditions.is_some() {
            if !opts.no_lock {
                object_lock_guard = Some(
                    self.acquire_write_lock_diag("complete_multipart_upload_precondition", bucket, object)
                        .await?,
                );
            }

            if let Some(err) = self.check_write_precondition(bucket, object, opts).await {
                return Err(err);
            }
        }

        let (mut fi, files_metas) = self.check_upload_id_exists(bucket, object, upload_id, true).await?;
        let upload_id_path = Self::get_upload_id_dir(bucket, object, upload_id);

        let write_quorum = fi.write_quorum(self.default_write_quorum());
        let read_quorum = fi.read_quorum(self.default_read_quorum());

        let disks = self.disks.read().await;

        let disks = disks.clone();
        // let disks = Self::shuffle_disks(&disks, &fi.erasure.distribution);

        let part_path = format!("{}/{}/", upload_id_path, fi.data_dir.unwrap_or(Uuid::nil()));

        let part_meta_paths = uploaded_parts
            .iter()
            .map(|v| format!("{part_path}part.{0}.meta", v.part_num))
            .collect::<Vec<String>>();

        let part_numbers = uploaded_parts.iter().map(|v| v.part_num).collect::<Vec<usize>>();

        let object_parts = Self::read_parts(&disks, RUSTFS_META_MULTIPART_BUCKET, &part_meta_paths, &part_numbers, read_quorum)
            .await
            .map_err(|err| to_object_err(err.into(), vec![bucket, object]))?;

        if object_parts.len() != uploaded_parts.len() {
            return Err(Error::other("part result number err"));
        }

        let mut checksum_type = rustfs_rio::ChecksumType::NONE;

        if let Some(cs) = fi.metadata.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM) {
            let Some(ct) = fi.metadata.get(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE) else {
                return Err(Error::other("checksum type not found"));
            };

            checksum_type = rustfs_rio::ChecksumType::from_string_with_obj_type(cs, ct);
            if let Some(want) = opts.want_checksum.as_ref()
                && !want.checksum_type.is(checksum_type)
            {
                return Err(Error::other(format!("checksum type mismatch, got {:?}, want {:?}", want, checksum_type)));
            }
        }

        for (i, part) in object_parts.iter().enumerate() {
            if let Some(err) = &part.error {
                let mapped_err = complete_multipart_part_error(uploaded_parts[i].part_num, err, bucket, object);
                let result = complete_multipart_part_error_result(&mapped_err);
                if matches!(mapped_err, Error::InvalidPart(_, _, _)) {
                    debug!(
                        target: "rustfs_ecstore::set_disk",
                        event = EVENT_SET_DISK_MULTIPART,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        op = "complete_multipart_upload",
                        result = result,
                        bucket = %bucket,
                        object = %object,
                        upload_id = %upload_id,
                        uploaded_part_num = uploaded_parts[i].part_num,
                        observed_part_num = part.number,
                        read_quorum = read_quorum,
                        write_quorum = write_quorum,
                        error = %err,
                        "Set disk multipart part missing"
                    );
                } else {
                    warn!(
                        target: "rustfs_ecstore::set_disk",
                        event = EVENT_SET_DISK_MULTIPART,
                        component = LOG_COMPONENT_ECSTORE,
                        subsystem = LOG_SUBSYSTEM_SET_DISK,
                        op = "complete_multipart_upload",
                        result = result,
                        bucket = %bucket,
                        object = %object,
                        upload_id = %upload_id,
                        uploaded_part_num = uploaded_parts[i].part_num,
                        observed_part_num = part.number,
                        read_quorum = read_quorum,
                        write_quorum = write_quorum,
                        error = %err,
                        "Set disk multipart part resolution failed"
                    );
                }
                return Err(mapped_err);
            }

            if uploaded_parts[i].part_num != part.number {
                error!(
                    "complete_multipart_upload part_id err part_id != part_num {} != {}",
                    uploaded_parts[i].part_num, part.number
                );
                return Err(Error::InvalidPart(uploaded_parts[i].part_num, bucket.to_owned(), object.to_owned()));
            }

            fi.add_object_part(
                part.number,
                part.etag.clone(),
                part.size,
                part.mod_time,
                part.actual_size,
                part.index.clone(),
                part.checksums.clone(),
            );
        }

        let (shuffle_disks, mut parts_metadatas) = Self::shuffle_disks_and_parts_metadata_by_index(&disks, &files_metas, &fi);

        let curr_fi = fi.clone();

        fi.parts = Vec::with_capacity(uploaded_parts.len());

        let mut object_size: usize = 0;
        let mut object_actual_size: i64 = 0;

        let mut checksum_combined = bytes::BytesMut::new();
        let mut checksum = rustfs_rio::Checksum {
            checksum_type,
            ..Default::default()
        };

        // Build a lookup map for O(1) part resolution instead of O(n) find() in the loop
        // This optimizes from O(n^2) to O(n) when processing many parts
        use std::collections::HashMap;
        let part_lookup: HashMap<usize, &ObjectPartInfo> = curr_fi.parts.iter().map(|part| (part.number, part)).collect();

        for (i, p) in uploaded_parts.iter().enumerate() {
            let Some(ext_part) = part_lookup.get(&p.part_num) else {
                error!(
                    "complete_multipart_upload part not found: part_id={}, bucket={}, object={}",
                    p.part_num, bucket, object
                );
                return Err(Error::InvalidPart(p.part_num, "".to_owned(), p.etag.clone().unwrap_or_default()));
            };
            debug!(
                target:"rustfs_ecstore::set_disk",
                event = EVENT_SET_DISK_MULTIPART,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                part_number = p.part_num,
                part_size = ext_part.size,
                part_actual_size = ext_part.actual_size,
                state = "part_validated",
                "Set disk multipart part validated"
            );

            // Normalize ETags by removing quotes before comparison (PR #592 compatibility)
            let client_etag = p.etag.as_ref().map(|e| rustfs_utils::path::trim_etag(e));
            let stored_etag = Some(rustfs_utils::path::trim_etag(&ext_part.etag));
            if client_etag != stored_etag {
                error!(
                    "complete_multipart_upload etag err client={:?}, stored={:?}, part_id={}, bucket={}, object={}",
                    p.etag, ext_part.etag, p.part_num, bucket, object
                );
                return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
            }

            // TODO: crypto

            if (i < uploaded_parts.len() - 1) && !is_min_allowed_part_size(ext_part.actual_size) {
                error!(
                    "complete_multipart_upload part size too small: part {} size {} is less than minimum {}",
                    p.part_num,
                    ext_part.actual_size,
                    GLOBAL_MIN_PART_SIZE.as_u64()
                );
                return Err(Error::EntityTooSmall(
                    p.part_num,
                    ext_part.actual_size,
                    GLOBAL_MIN_PART_SIZE.as_u64() as i64,
                ));
            }

            if checksum_type.is_set() {
                let Some(crc) = ext_part
                    .checksums
                    .as_ref()
                    .and_then(|f| f.get(checksum_type.to_string().as_str()))
                    .cloned()
                else {
                    error!(
                        "complete_multipart_upload fi.checksum not found type={checksum_type}, part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                };

                let Some(part_crc) = complete_part_checksum(p, checksum_type) else {
                    error!(
                        "complete_multipart_upload checksum type={checksum_type}, part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                };

                if let Some(part_crc) = part_crc
                    && part_crc != crc
                {
                    error!("complete_multipart_upload checksum_type={checksum_type:?}, part_crc={part_crc:?}, crc={crc:?}");
                    error!(
                        "complete_multipart_upload checksum mismatch part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                }

                let Some(cs) = rustfs_rio::Checksum::new_with_type(checksum_type, &crc) else {
                    error!(
                        "complete_multipart_upload checksum new_with_type failed part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                };

                if !cs.valid() {
                    error!(
                        "complete_multipart_upload checksum valid failed part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                }

                if checksum_type.full_object_requested()
                    && let Err(err) = checksum.add_part(&cs, ext_part.actual_size)
                {
                    error!(
                        "complete_multipart_upload checksum add_part failed part_id={}, bucket={}, object={}",
                        p.part_num, bucket, object
                    );
                    return Err(Error::InvalidPart(p.part_num, ext_part.etag.clone(), p.etag.clone().unwrap_or_default()));
                }

                checksum_combined.extend_from_slice(cs.raw.as_slice());
            }

            // TODO: check min part size

            object_size += ext_part.size;
            object_actual_size += ext_part.actual_size;

            fi.parts.push(completed_multipart_object_part(p.part_num, ext_part));
        }

        if let Some(wtcs) = opts.want_checksum.as_ref() {
            if checksum_type.full_object_requested() {
                if wtcs.encoded != checksum.encoded {
                    error!(
                        "complete_multipart_upload checksum mismatch want={}, got={}",
                        wtcs.encoded, checksum.encoded
                    );
                    return Err(Error::other(format!(
                        "complete_multipart_upload checksum mismatch want={}, got={}",
                        wtcs.encoded, checksum.encoded
                    )));
                }
            } else if let Err(err) = wtcs.matches(&checksum_combined, uploaded_parts.len() as i32) {
                error!(
                    "complete_multipart_upload checksum matches failed want={}, got={}",
                    wtcs.encoded, checksum.encoded
                );
                return Err(Error::other(format!(
                    "complete_multipart_upload checksum matches failed want={}, got={}",
                    wtcs.encoded, checksum.encoded
                )));
            }
        }

        if let Some(rc_crc) = get_header_map(&opts.user_defined, SUFFIX_REPLICATION_SSEC_CRC) {
            if let Ok(rc_crc_bytes) = base64_simd::STANDARD.decode_to_vec(&rc_crc) {
                fi.checksum = Some(Bytes::from(rc_crc_bytes));
            } else {
                error!("complete_multipart_upload decode rc_crc failed rc_crc={}", rc_crc);
            }
        }

        if checksum_type.is_set() {
            checksum_type
                .merge(rustfs_rio::ChecksumType::MULTIPART)
                .merge(rustfs_rio::ChecksumType::INCLUDES_MULTIPART);
            if !checksum_type.full_object_requested() {
                checksum = rustfs_rio::Checksum::new_from_data(checksum_type, &checksum_combined)
                    .ok_or_else(|| Error::other("checksum new_from_data failed"))?;
            }
            fi.checksum = Some(checksum.to_bytes(&checksum_combined));
        }

        fi.metadata.remove(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM);
        fi.metadata.remove(rustfs_rio::RUSTFS_MULTIPART_CHECKSUM_TYPE);
        strip_internal_multipart_metadata(&mut fi.metadata);

        fi.size = object_size as i64;
        fi.mod_time = opts.mod_time;
        if fi.mod_time.is_none() {
            fi.mod_time = Some(OffsetDateTime::now_utc());
        }

        // etag
        let etag = {
            if let Some(etag) = opts.user_defined.get("etag") {
                etag.clone()
            } else {
                get_complete_multipart_md5(&uploaded_parts)
            }
        };

        fi.metadata.insert("etag".to_owned(), etag);

        let persist_encryption_original_size = should_persist_encryption_original_size(&fi.metadata);

        if opts.replication_request {
            if let Some(actual_size) = get_str(&opts.user_defined, SUFFIX_ACTUAL_OBJECT_SIZE_CAP) {
                insert_str(&mut fi.metadata, SUFFIX_ACTUAL_SIZE, actual_size.clone());
                if persist_encryption_original_size {
                    fi.metadata
                        .insert("x-rustfs-encryption-original-size".to_string(), actual_size);
                }
            }
        } else {
            insert_str(&mut fi.metadata, SUFFIX_ACTUAL_SIZE, object_actual_size.to_string());
            if persist_encryption_original_size {
                fi.metadata
                    .insert("x-rustfs-encryption-original-size".to_string(), object_actual_size.to_string());
            }
        }

        if fi.is_compressed() {
            insert_str(&mut fi.metadata, SUFFIX_COMPRESSION_SIZE, object_size.to_string());
        }

        if opts.data_movement {
            fi.set_data_moved();
        }

        for meta in parts_metadatas.iter_mut() {
            if meta.is_valid() {
                meta.size = fi.size;
                meta.mod_time = fi.mod_time;
                meta.parts.clone_from(&fi.parts);
                meta.metadata = fi.metadata.clone();
                meta.versioned = opts.versioned || opts.version_suspended;
                meta.checksum = fi.checksum.clone();
            }
        }

        let mut parts = Vec::with_capacity(curr_fi.parts.len());

        for p in curr_fi.parts.iter() {
            parts.push(path_join_buf(&[
                &upload_id_path,
                curr_fi.data_dir.unwrap_or(Uuid::nil()).to_string().as_str(),
                format!("part.{}.meta", p.number).as_str(),
            ]));

            if !fi.parts.iter().any(|v| v.number == p.number) {
                parts.push(path_join_buf(&[
                    &upload_id_path,
                    curr_fi.data_dir.unwrap_or(Uuid::nil()).to_string().as_str(),
                    format!("part.{}", p.number).as_str(),
                ]));
            }
        }

        if !opts.no_lock && object_lock_guard.is_none() {
            object_lock_guard = Some(
                self.acquire_write_lock_diag("complete_multipart_upload_commit", bucket, object)
                    .await?,
            );
        }

        let complete_tail_stage_start = rustfs_io_metrics::put_stage_metrics_enabled().then(Instant::now);
        self.cleanup_multipart_path(&parts).await;

        let (online_disks, versions, op_old_dir, cleanup_disks) = Self::rename_data(
            &shuffle_disks,
            RUSTFS_META_MULTIPART_BUCKET,
            &upload_id_path,
            &parts_metadatas,
            bucket,
            object,
            write_quorum,
        )
        .await?;

        if let Some(old_dir) = op_old_dir {
            self.commit_rename_data_dir(&cleanup_disks, bucket, object, &old_dir.to_string(), write_quorum)
                .await?;
        }

        if let Some(stage_start) = complete_tail_stage_start {
            rustfs_io_metrics::record_put_object_stage_duration(
                "multipart_complete_tail",
                stage_start.elapsed().as_secs_f64() * 1000.0,
            );
        }

        drop(object_lock_guard); // drop object lock guard to release the lock

        if let Some(versions) = versions {
            let _ =
                rustfs_common::heal_channel::send_heal_request(rustfs_common::heal_channel::create_heal_request_with_options(
                    bucket.to_string(),
                    Some(object.to_string()),
                    false,
                    Some(HealChannelPriority::Normal),
                    Some(self.pool_index),
                    Some(self.set_index),
                ))
                .await;
        }

        let upload_id_path = upload_id_path.clone();
        let store = self.clone();
        let _cleanup_handle = tokio::spawn(async move {
            let _ = store.delete_all(RUSTFS_META_MULTIPART_BUCKET, &upload_id_path).await;
        });

        for (i, op_disk) in online_disks.iter().enumerate() {
            if let Some(disk) = op_disk
                && disk.is_online().await
            {
                fi = parts_metadatas[i].clone();
                break;
            }
        }

        record_capacity_scope_if_needed(opts.capacity_scope_token, &online_disks);

        fi.is_latest = true;

        self.invalidate_get_object_metadata_cache(bucket, object).await;

        Ok(ObjectInfo::from_file_info(&fi, bucket, object, opts.versioned || opts.version_suspended))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn collect_list_parts_results_fails_early_when_quorum_is_impossible() {
        let started = Instant::now();
        let tasks: Vec<_> = vec![
            (10_u64, Err(DiskError::DiskNotFound)),
            (15, Err(DiskError::DiskNotFound)),
            (250, Ok::<Vec<String>, DiskError>(vec!["part.1".to_string(), "part.1.meta".to_string()])),
        ]
        .into_iter()
        .map(|(delay_ms, outcome)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let err = collect_list_parts_results(tasks, 2)
            .await
            .expect_err("quorum should become impossible before slow tail completes");

        assert_eq!(err, DiskError::ErasureReadQuorum);
        assert!(started.elapsed() < Duration::from_millis(120));
    }

    #[tokio::test]
    async fn collect_list_parts_results_tolerates_single_panicked_task_when_quorum_is_met() {
        let tasks: Vec<_> = vec![(5_u64, true), (10, false), (12, false)]
            .into_iter()
            .map(|(delay_ms, should_panic)| async move {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                if should_panic {
                    panic!("simulated task panic");
                }
                Ok::<Vec<String>, DiskError>(vec!["part.1".to_string(), "part.1.meta".to_string()])
            })
            .collect();

        let (errs, object_parts) = collect_list_parts_results(tasks, 2)
            .await
            .expect("quorum should still succeed");
        assert_eq!(errs.iter().filter(|err| err.is_none()).count(), 2);
        assert_eq!(object_parts.iter().filter(|parts| !parts.is_empty()).count(), 2);
    }

    #[tokio::test]
    async fn collect_list_parts_results_returns_file_not_found_for_empty_upload_dirs() {
        let tasks: Vec<_> = vec![
            (5_u64, Err(DiskError::FileNotFound)),
            (10, Err(DiskError::DiskNotFound)),
            (12, Err(DiskError::DiskNotFound)),
        ]
        .into_iter()
        .map(|(delay_ms, outcome)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let err = collect_list_parts_results(tasks, 2)
            .await
            .expect_err("missing multipart directories should be treated as empty uploads");

        assert_eq!(err, DiskError::FileNotFound);
    }

    #[tokio::test]
    async fn collect_list_parts_results_fails_early_when_file_not_found_fallback_is_impossible() {
        let started = Instant::now();
        let tasks: Vec<_> = vec![
            (5_u64, Err(DiskError::FileNotFound)),
            (10, Err(DiskError::FileCorrupt)),
            (250, Err(DiskError::DiskNotFound)),
        ]
        .into_iter()
        .map(|(delay_ms, outcome)| async move {
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let err = collect_list_parts_results(tasks, 2)
            .await
            .expect_err("non-ignored errors should preserve early quorum failure");

        assert_eq!(err, DiskError::ErasureReadQuorum);
        assert!(started.elapsed() < Duration::from_millis(120));
    }

    #[test]
    fn reduce_quorum_part_numbers_only_keeps_parts_present_on_quorum_of_drives() {
        let object_parts = vec![
            vec![
                "part.1".to_string(),
                "part.1.meta".to_string(),
                "part.2".to_string(),
                "part.2.meta".to_string(),
            ],
            vec![
                "part.1".to_string(),
                "part.1.meta".to_string(),
                "part.3".to_string(),
                "part.3.meta".to_string(),
            ],
            vec![
                "part.1".to_string(),
                "part.1.meta".to_string(),
                "part.2".to_string(),
                "part.2.meta".to_string(),
            ],
        ];

        let parts = reduce_quorum_part_numbers(object_parts, 2);
        assert_eq!(parts, vec![1, 2]);
    }

    fn test_multipart_fileinfo(object: &str, data_blocks: usize, parity_blocks: usize, index: usize) -> FileInfo {
        let mut file_info = FileInfo::new(object, data_blocks, parity_blocks);
        file_info.erasure.index = index;
        file_info.data_dir = Some(Uuid::new_v4());
        file_info
    }

    #[test]
    fn upload_id_write_quorum_fails_when_only_read_quorum_metadata_is_visible() {
        let parts_metadata = vec![
            test_multipart_fileinfo("bucket/object", 2, 2, 1),
            test_multipart_fileinfo("bucket/object", 2, 2, 2),
            FileInfo::default(),
            FileInfo::default(),
        ];
        let errs = vec![None, None, Some(DiskError::DiskNotFound), Some(DiskError::DiskNotFound)];

        let (read_quorum, write_quorum) =
            SetDisks::object_quorum_from_meta(&parts_metadata, &errs, 2).expect("read quorum should resolve metadata geometry");

        assert_eq!(read_quorum, 2);
        assert_eq!(write_quorum, 3);
        assert!(reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum as usize).is_none());

        let err = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum as usize)
            .expect("write quorum should fail with only two writable metadata copies");

        assert_eq!(err, DiskError::ErasureWriteQuorum);
    }

    #[test]
    fn upload_id_all_not_found_maps_to_invalid_upload_id() {
        let parts_metadata = vec![
            FileInfo::default(),
            FileInfo::default(),
            FileInfo::default(),
            FileInfo::default(),
        ];
        let errs = vec![
            Some(DiskError::FileNotFound),
            Some(DiskError::FileNotFound),
            Some(DiskError::FileNotFound),
            Some(DiskError::FileNotFound),
        ];

        let err = SetDisks::object_quorum_from_meta(&parts_metadata, &errs, 2)
            .map(|_| ())
            .map_err(|err| map_upload_id_metadata_error("bucket", "object", "upload-id", err))
            .expect_err("all missing upload metadata should remain an invalid upload id");

        assert!(matches!(err, StorageError::InvalidUploadID(bucket, object, upload_id)
            if bucket == "bucket" && object == "object" && upload_id == "upload-id"));
    }
}
