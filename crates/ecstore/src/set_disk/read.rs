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

impl SetDisks {
    pub(super) async fn read_parts(
        disks: &[Option<DiskStore>],
        bucket: &str,
        part_meta_paths: &[String],
        part_numbers: &[usize],
        read_quorum: usize,
    ) -> disk::error::Result<Vec<ObjectPartInfo>> {
        let mut errs = Vec::with_capacity(disks.len());
        let mut object_parts = Vec::with_capacity(disks.len());

        // Use batch processor for better performance
        let processor = get_global_processors().read_processor();
        let bucket = bucket.to_string();
        let part_meta_paths = part_meta_paths.to_vec();

        let tasks: Vec<_> = disks
            .iter()
            .map(|disk| {
                let disk = disk.clone();
                let bucket = bucket.clone();
                let part_meta_paths = part_meta_paths.clone();

                async move {
                    if let Some(disk) = disk {
                        disk.read_parts(&bucket, &part_meta_paths).await
                    } else {
                        Err(DiskError::DiskNotFound)
                    }
                }
            })
            .collect();

        let results = processor.execute_batch(tasks).await;
        for result in results {
            match result {
                Ok(res) => {
                    errs.push(None);
                    object_parts.push(res);
                }
                Err(e) => {
                    errs.push(Some(e));
                    object_parts.push(vec![]);
                }
            }
        }

        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum) {
            return Err(err);
        }

        let mut ret = vec![ObjectPartInfo::default(); part_meta_paths.len()];

        for (part_idx, part_info) in part_meta_paths.iter().enumerate() {
            let mut part_meta_quorum = HashMap::new();
            let mut part_infos = Vec::new();
            for (j, parts) in object_parts.iter().enumerate() {
                if parts.len() != part_meta_paths.len() {
                    *part_meta_quorum.entry(part_info.clone()).or_insert(0) += 1;
                    continue;
                }

                if !parts[part_idx].etag.is_empty() {
                    *part_meta_quorum.entry(parts[part_idx].etag.clone()).or_insert(0) += 1;
                    part_infos.push(parts[part_idx].clone());
                    continue;
                }

                *part_meta_quorum.entry(part_info.clone()).or_insert(0) += 1;
            }

            let mut max_quorum = 0;
            let mut max_etag = None;
            let mut max_part_meta = None;
            for (etag, quorum) in part_meta_quorum.iter() {
                if quorum > &max_quorum {
                    max_quorum = *quorum;
                    max_etag = Some(etag);
                    max_part_meta = Some(etag);
                }
            }

            let mut found = None;
            for info in part_infos.iter() {
                if let Some(etag) = max_etag
                    && info.etag == *etag
                {
                    found = Some(info.clone());
                    break;
                }

                if let Some(part_meta) = max_part_meta
                    && info.etag.is_empty()
                    && part_meta.ends_with(format!("part.{0}.meta", info.number).as_str())
                {
                    found = Some(info.clone());
                    break;
                }
            }

            if let (Some(found), Some(max_etag)) = (found, max_etag)
                && !found.etag.is_empty()
                && part_meta_quorum.get(max_etag).unwrap_or(&0) >= &read_quorum
            {
                ret[part_idx] = found.clone();
            } else {
                ret[part_idx] = ObjectPartInfo {
                    number: part_numbers[part_idx],
                    error: Some(format!("part.{} not found", part_numbers[part_idx])),
                    ..Default::default()
                };
            }
        }

        Ok(ret)
    }

    #[tracing::instrument(level = "debug", skip(disks))]
    pub(super) async fn read_all_fileinfo(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        healing: bool,
    ) -> disk::error::Result<(Vec<FileInfo>, Vec<Option<DiskError>>)> {
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());
        let opts = Arc::new(ReadOptions {
            read_data,
            healing,
            ..Default::default()
        });
        let org_bucket = Arc::new(org_bucket.to_string());
        let bucket = Arc::new(bucket.to_string());
        let object = Arc::new(object.to_string());
        let version_id = Arc::new(version_id.to_string());
        let futures = disks.iter().map(|disk| {
            let disk = disk.clone();
            let opts = opts.clone();
            let org_bucket = org_bucket.clone();
            let bucket = bucket.clone();
            let object = object.clone();
            let version_id = version_id.clone();
            tokio::spawn(async move {
                if let Some(disk) = disk {
                    disk.read_version(&org_bucket, &bucket, &object, &version_id, &opts).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            })
        });

        // Wait for all tasks to complete
        let results = join_all(futures).await;

        for result in results {
            match result {
                Ok(res) => match res {
                    Ok(file_info) => {
                        ress.push(file_info);
                        errors.push(None);
                    }
                    Err(e) => {
                        ress.push(FileInfo::default());
                        errors.push(Some(e));
                    }
                },
                Err(_) => {
                    ress.push(FileInfo::default());
                    errors.push(Some(DiskError::Unexpected));
                }
            }
        }
        Ok((ress, errors))
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

        let processor = get_global_processors().read_processor();
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

    pub(super) async fn read_all_xl(
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        read_data: bool,
        incl_free_vers: bool,
    ) -> (Vec<FileInfo>, Vec<Option<DiskError>>) {
        let (fileinfos, errs) = Self::read_all_raw_file_info(disks, bucket, object, read_data).await;

        Self::pick_latest_quorum_files_info(fileinfos, errs, bucket, object, read_data, incl_free_vers).await
    }

    pub(super) async fn read_all_raw_file_info(
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        read_data: bool,
    ) -> (Vec<Option<RawFileInfo>>, Vec<Option<DiskError>>) {
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        let mut futures = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.read_xl(bucket, object, read_data).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(res) => {
                    ress.push(Some(res));
                    errors.push(None);
                }
                Err(e) => {
                    ress.push(None);
                    errors.push(Some(e));
                }
            }
        }

        (ress, errors)
    }

    pub(super) async fn pick_latest_quorum_files_info(
        fileinfos: Vec<Option<RawFileInfo>>,
        errs: Vec<Option<DiskError>>,
        bucket: &str,
        object: &str,
        read_data: bool,
        incl_free_vers: bool,
    ) -> (Vec<FileInfo>, Vec<Option<DiskError>>) {
        let mut metadata_array = vec![None; fileinfos.len()];
        let mut meta_file_infos = vec![FileInfo::default(); fileinfos.len()];
        let mut metadata_shallow_versions = vec![None; fileinfos.len()];

        let mut v2_bufs = {
            if !read_data {
                vec![Vec::new(); fileinfos.len()]
            } else {
                Vec::new()
            }
        };

        let mut errs = errs;

        for (idx, info_op) in fileinfos.iter().enumerate() {
            if let Some(info) = info_op {
                if !read_data {
                    v2_bufs[idx] = info.buf.clone();
                }

                let xlmeta = match FileMeta::load(&info.buf) {
                    Ok(res) => res,
                    Err(err) => {
                        errs[idx] = Some(err.into());
                        continue;
                    }
                };

                metadata_array[idx] = Some(xlmeta);
                meta_file_infos[idx] = FileInfo::default();
            }
        }

        for (idx, info_op) in metadata_array.iter().enumerate() {
            if let Some(info) = info_op {
                metadata_shallow_versions[idx] = Some(info.versions.clone());
            }
        }

        let shallow_versions: Vec<Vec<FileMetaShallowVersion>> = metadata_shallow_versions.iter().flatten().cloned().collect();

        let read_quorum = fileinfos.len().div_ceil(2);
        let versions = merge_file_meta_versions(read_quorum, false, 1, &shallow_versions);
        let meta = FileMeta {
            versions,
            ..Default::default()
        };

        let finfo = match meta.into_fileinfo(bucket, object, "", true, incl_free_vers, true) {
            Ok(res) => res,
            Err(err) => {
                for item in errs.iter_mut() {
                    if item.is_none() {
                        *item = Some(err.clone().into());
                    }
                }

                return (meta_file_infos, errs);
            }
        };

        if !finfo.is_valid() {
            for item in errs.iter_mut() {
                if item.is_none() {
                    *item = Some(DiskError::FileCorrupt);
                }
            }

            return (meta_file_infos, errs);
        }

        let vid = finfo.version_id.unwrap_or(Uuid::nil());

        for (idx, meta_op) in metadata_array.iter().enumerate() {
            if let Some(meta) = meta_op {
                match meta.into_fileinfo(bucket, object, vid.to_string().as_str(), read_data, incl_free_vers, true) {
                    Ok(res) => meta_file_infos[idx] = res,
                    Err(err) => errs[idx] = Some(err.into()),
                }
            }
        }

        (meta_file_infos, errs)
    }

    pub(super) async fn read_multiple_files(
        disks: &[Option<DiskStore>],
        req: ReadMultipleReq,
        read_quorum: usize,
    ) -> Vec<ReadMultipleResp> {
        let mut futures = Vec::with_capacity(disks.len());
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let req = req.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.read_multiple(req).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let results = join_all(futures).await;
        for result in results {
            match result {
                Ok(res) => {
                    ress.push(Some(res));
                    errors.push(None);
                }
                Err(e) => {
                    ress.push(None);
                    errors.push(Some(e));
                }
            }
        }

        // debug!("ReadMultipleResp ress {:?}", ress);
        // debug!("ReadMultipleResp errors {:?}", errors);

        let mut ret = Vec::with_capacity(req.files.len());

        for want in req.files.iter() {
            let mut quorum = 0;

            let mut get_res = ReadMultipleResp::default();

            for res in ress.iter() {
                if res.is_none() {
                    continue;
                }

                let disk_res = res.as_ref().unwrap();

                for resp in disk_res.iter() {
                    if !resp.error.is_empty() || !resp.exists {
                        continue;
                    }

                    if &resp.file != want || resp.bucket != req.bucket || resp.prefix != req.prefix {
                        continue;
                    }
                    quorum += 1;

                    if get_res.mod_time > resp.mod_time || get_res.data.len() > resp.data.len() {
                        continue;
                    }

                    get_res = resp.clone();
                }
            }

            if quorum < read_quorum {
                // debug!("quorum < read_quorum: {} < {}", quorum, read_quorum);
                get_res.exists = false;
                get_res.error = Error::ErasureReadQuorum.to_string();
                get_res.data = Vec::new();
            }

            ret.push(get_res);
        }

        // log err

        ret
    }

    #[tracing::instrument(level = "debug", skip(self))]
    pub(super) async fn get_object_fileinfo(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
        read_data: bool,
    ) -> Result<(FileInfo, Vec<FileInfo>, Vec<Option<DiskStore>>)> {
        let disks = self.disks.read().await;

        let disks = disks.clone();

        let vid = opts.version_id.clone().unwrap_or_default();

        // TODO: optimize concurrency and break once enough slots are available
        let (parts_metadata, errs) = Self::read_all_fileinfo(&disks, "", bucket, object, vid.as_str(), read_data, false).await?;
        // warn!("get_object_fileinfo parts_metadata {:?}", &parts_metadata);
        // warn!("get_object_fileinfo {}/{} errs {:?}", bucket, object, &errs);

        let _min_disks = self.set_drive_count - self.default_parity_count;

        let (read_quorum, _) = match Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count)
            .map_err(|err| to_object_err(err.into(), vec![bucket, object]))
        {
            Ok(v) => v,
            Err(e) => {
                // error!("Self::object_quorum_from_meta: {:?}, bucket: {}, object: {}", &e, bucket, object);
                return Err(e);
            }
        };

        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum as usize) {
            error!("reduce_read_quorum_errs: {:?}, bucket: {}, object: {}", &err, bucket, object);
            return Err(to_object_err(err.into(), vec![bucket, object]));
        }

        let (op_online_disks, mot_time, etag) = Self::list_online_disks(&disks, &parts_metadata, &errs, read_quorum as usize);

        let fi = Self::pick_valid_fileinfo(&parts_metadata, mot_time, etag, read_quorum as usize)?;
        if errs.iter().any(|err| err.is_some()) {
            let _ =
                rustfs_common::heal_channel::send_heal_request(rustfs_common::heal_channel::create_heal_request_with_options(
                    fi.volume.to_string(),             // bucket
                    Some(fi.name.to_string()),         // object_prefix
                    false,                             // force_start
                    Some(HealChannelPriority::Normal), // priority
                    Some(self.pool_index),             // pool_index
                    Some(self.set_index),              // set_index
                ))
                .await;
        }
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
    ) -> Result<()>
    where
        W: AsyncWrite + Send + Sync + Unpin + 'static,
    {
        debug!(bucket, object, requested_length = length, offset, "get_object_with_fileinfo start");
        let (disks, files) = Self::shuffle_disks_and_parts_metadata_by_index(disks, &files, &fi);

        let total_size = fi.size as usize;

        let length = if length < 0 {
            fi.size as usize - offset
        } else {
            length as usize
        };

        if offset > total_size || offset + length > total_size {
            error!("get_object_with_fileinfo offset out of range: {}, total_size: {}", offset, total_size);
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

        let erasure = erasure_coding::Erasure::new(fi.erasure.data_blocks, fi.erasure.parity_blocks, fi.erasure.block_size);

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

            let mut readers = Vec::with_capacity(disks.len());
            let mut errors = Vec::with_capacity(disks.len());
            for (idx, disk_op) in disks.iter().enumerate() {
                match create_bitrot_reader(
                    files[idx].data.as_deref(),
                    disk_op.as_ref(),
                    bucket,
                    &format!("{}/{}/part.{}", object, files[idx].data_dir.unwrap_or_default(), part_number),
                    read_offset,
                    till_offset,
                    erasure.shard_size(),
                    HashAlgorithm::HighwayHash256,
                )
                .await
                {
                    Ok(Some(reader)) => {
                        readers.push(Some(reader));
                        errors.push(None);
                    }
                    Ok(None) => {
                        readers.push(None);
                        errors.push(Some(DiskError::DiskNotFound));
                    }
                    Err(e) => {
                        readers.push(None);
                        errors.push(Some(e));
                    }
                }
            }

            let nil_count = errors.iter().filter(|&e| e.is_none()).count();
            if nil_count < erasure.data_shards {
                if let Some(read_err) = reduce_read_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, erasure.data_shards) {
                    error!("create_bitrot_reader reduce_read_quorum_errs {:?}", &errors);
                    return Err(to_object_err(read_err.into(), vec![bucket, object]));
                }
                error!("create_bitrot_reader not enough disks to read: {:?}", &errors);
                return Err(Error::other(format!("not enough disks to read: {errors:?}")));
            }

            // Check if we have missing shards even though we can read successfully
            // This happens when a node was offline during write and comes back online
            let total_shards = erasure.data_shards + erasure.parity_shards;
            let available_shards = nil_count;
            let missing_shards = total_shards - available_shards;

            info!(
                bucket,
                object,
                part_number,
                total_shards,
                available_shards,
                missing_shards,
                data_shards = erasure.data_shards,
                parity_shards = erasure.parity_shards,
                "Shard availability check"
            );

            if missing_shards > 0 && available_shards >= erasure.data_shards {
                // We have missing shards but enough to read - trigger background heal
                info!(
                    bucket,
                    object,
                    part_number,
                    missing_shards,
                    available_shards,
                    pool_index,
                    set_index,
                    "Detected missing shards during read, triggering background heal"
                );
                if let Err(e) =
                    rustfs_common::heal_channel::send_heal_request(rustfs_common::heal_channel::create_heal_request_with_options(
                        bucket.to_string(),
                        Some(object.to_string()),
                        false,
                        Some(HealChannelPriority::Normal),
                        Some(pool_index),
                        Some(set_index),
                    ))
                    .await
                {
                    warn!(
                        bucket,
                        object,
                        part_number,
                        error = %e,
                        "Failed to enqueue heal request for missing shards"
                    );
                } else {
                    warn!(bucket, object, part_number, "Successfully enqueued heal request for missing shards");
                }
            }

            // debug!(
            //     "read part {} part_offset {},part_length {},part_size {}  ",
            //     part_number, part_offset, part_length, part_size
            // );
            let (written, err) = erasure.decode(writer, readers, part_offset, part_length, part_size).await;
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
                    match de_err {
                        DiskError::FileNotFound | DiskError::FileCorrupt => {
                            error!("erasure.decode err 111 {:?}", &de_err);
                            if let Err(e) = rustfs_common::heal_channel::send_heal_request(
                                rustfs_common::heal_channel::create_heal_request_with_options(
                                    bucket.to_string(),
                                    Some(object.to_string()),
                                    false,
                                    Some(HealChannelPriority::Normal),
                                    Some(pool_index),
                                    Some(set_index),
                                ),
                            )
                            .await
                            {
                                warn!(
                                    bucket,
                                    object,
                                    part_number,
                                    error = %e,
                                    "Failed to enqueue heal request after decode error"
                                );
                            }
                            has_err = false;
                        }
                        _ => {}
                    }
                }

                if has_err {
                    error!("erasure.decode err {} {:?}", written, &de_err);
                    return Err(de_err.into());
                }
            }

            // debug!("ec decode {} written size {}", part_number, n);

            total_read += part_length;
            part_offset = 0;
        }

        // debug!("read end");

        debug!(bucket, object, total_read, expected_length = length, "Multipart read finished");

        Ok(())
    }
}
