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
use crate::get_diagnostics::{
    GET_OBJECT_PATH_CODEC_STREAMING, GET_STAGE_DECODE, GET_STAGE_RANGE, GET_STAGE_READER_SETUP, GetObjectFailureReason,
    classify_disk_error, record_get_object_pipeline_failure, record_get_object_pipeline_failure_for_path,
};
use rustfs_config::{DEFAULT_OBJECT_ZERO_COPY_ENABLE, ENV_OBJECT_ZERO_COPY_ENABLE};
use std::future::Future;
use tokio::io::AsyncRead;
use tokio::task::JoinSet;

async fn collect_read_multiple_results<F>(
    tasks: Vec<F>,
    read_quorum: usize,
) -> std::result::Result<(Vec<Option<Vec<ReadMultipleResp>>>, Vec<Option<DiskError>>), ()>
where
    F: Future<Output = disk::error::Result<Vec<ReadMultipleResp>>> + Send + 'static,
{
    let mut responses = vec![None; tasks.len()];
    let mut errors = vec![Some(DiskError::DiskNotFound); tasks.len()];
    let mut successful_responses = 0usize;
    let mut pending = tasks.len();
    let mut join_set = JoinSet::new();

    for (index, task) in tasks.into_iter().enumerate() {
        join_set.spawn(async move { (index, task.await) });
    }

    while let Some(join_result) = join_set.join_next().await {
        pending = pending.saturating_sub(1);

        match join_result {
            Ok((index, Ok(resp))) => {
                responses[index] = Some(resp);
                errors[index] = None;
                successful_responses += 1;
            }
            Ok((index, Err(err))) => {
                errors[index] = Some(err);
            }
            Err(_) => {}
        }

        if successful_responses + pending < read_quorum {
            return Err(());
        }
    }

    Ok((responses, errors))
}

async fn collect_read_parts_results<F>(
    tasks: Vec<F>,
    read_quorum: usize,
) -> std::result::Result<(Vec<Option<Vec<ObjectPartInfo>>>, Vec<Option<DiskError>>), ()>
where
    F: Future<Output = disk::error::Result<Vec<ObjectPartInfo>>> + Send + 'static,
{
    let mut responses = vec![None; tasks.len()];
    let mut errors = vec![Some(DiskError::DiskNotFound); tasks.len()];
    let mut successful_responses = 0usize;
    let mut pending = tasks.len();
    let mut join_set = JoinSet::new();

    for (index, task) in tasks.into_iter().enumerate() {
        join_set.spawn(async move { (index, task.await) });
    }

    while let Some(join_result) = join_set.join_next().await {
        pending = pending.saturating_sub(1);

        match join_result {
            Ok((index, Ok(resp))) => {
                responses[index] = Some(resp);
                errors[index] = None;
                successful_responses += 1;
            }
            Ok((index, Err(err))) => {
                errors[index] = Some(err);
            }
            Err(_) => {}
        }

        if successful_responses + pending < read_quorum {
            return Err(());
        }
    }

    Ok((responses, errors))
}

impl SetDisks {
    async fn is_get_object_metadata_cache_enabled(&self, bucket: &str, opts: &ObjectOptions, read_data: bool) -> bool {
        is_get_object_metadata_cache_request_eligible(bucket, opts, read_data) && !is_dist_erasure().await
    }

    async fn cached_get_object_fileinfo(&self, bucket: &str, object: &str) -> Option<GetObjectMetadataCacheEntry> {
        let key = GetObjectMetadataCacheKey::new(bucket, object);
        let cache = self.get_object_metadata_cache.read().await;
        cache
            .get(&key)
            .filter(|entry| {
                entry.is_fresh() && entry.online_disks.iter().filter(|disk| disk.is_some()).count() >= entry.read_quorum
            })
            .cloned()
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
        let mut cache = self.get_object_metadata_cache.write().await;
        if cache.len() >= GET_OBJECT_METADATA_CACHE_MAX_ENTRIES {
            cache.retain(|_, entry| entry.is_fresh());
            if cache.len() >= GET_OBJECT_METADATA_CACHE_MAX_ENTRIES {
                cache.clear();
            }
        }

        cache.insert(
            key,
            GetObjectMetadataCacheEntry {
                created_at: Instant::now(),
                fi: fi.clone(),
                parts_metadata: parts_metadata.to_vec(),
                online_disks: online_disks.to_vec(),
                read_quorum,
            },
        );
    }

    pub(super) async fn read_parts(
        disks: &[Option<DiskStore>],
        bucket: &str,
        part_meta_paths: &[String],
        part_numbers: &[usize],
        read_quorum: usize,
    ) -> disk::error::Result<Vec<ObjectPartInfo>> {
        let mut errs = Vec::with_capacity(disks.len());
        let mut object_parts = Vec::with_capacity(disks.len());
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

        let (responses, collected_errors) = match collect_read_parts_results(tasks, read_quorum).await {
            Ok(collected) => collected,
            Err(()) => return Err(DiskError::ErasureReadQuorum),
        };

        errs.extend(collected_errors);
        object_parts.extend(responses.into_iter().map(|resp| resp.unwrap_or_default()));

        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum) {
            return Err(err);
        }

        let mut ret = vec![ObjectPartInfo::default(); part_meta_paths.len()];

        for (part_idx, part_info) in part_meta_paths.iter().enumerate() {
            let mut part_meta_quorum = HashMap::new();
            let mut part_infos = Vec::new();
            let mut present_count = 0usize;
            let mut missing_or_empty_count = 0usize;
            let mut mismatched_response_count = 0usize;
            for parts in object_parts.iter() {
                if parts.len() != part_meta_paths.len() {
                    mismatched_response_count += 1;
                    *part_meta_quorum.entry(part_info.clone()).or_insert(0) += 1;
                    continue;
                }

                if !parts[part_idx].etag.is_empty() {
                    present_count += 1;
                    *part_meta_quorum.entry(parts[part_idx].etag.clone()).or_insert(0) += 1;
                    part_infos.push(parts[part_idx].clone());
                    continue;
                }

                missing_or_empty_count += 1;
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
                if issue3031_diag_enabled() {
                    warn!(
                        target: "rustfs_ecstore::set_disk",
                        bucket = %bucket,
                        part_meta_path = %part_info,
                        part_id = part_numbers[part_idx],
                        read_quorum = read_quorum,
                        max_quorum = max_quorum,
                        disk_response_count = object_parts.len(),
                        present_count = present_count,
                        missing_or_empty_count = missing_or_empty_count,
                        mismatched_response_count = mismatched_response_count,
                        max_vote_is_missing_marker = max_etag.map(|etag| etag == part_info).unwrap_or(false),
                        "issue3031_read_parts_part_quorum"
                    );
                }
                ret[part_idx] = ObjectPartInfo {
                    number: part_numbers[part_idx],
                    error: Some(format!("part.{} not found", part_numbers[part_idx])),
                    ..Default::default()
                };
            }
        }

        Ok(ret)
    }

    #[allow(clippy::too_many_arguments)]
    #[tracing::instrument(level = "debug", skip(disks))]
    pub(super) async fn read_all_fileinfo(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        object: &str,
        version_id: &str,
        read_data: bool,
        healing: bool,
        incl_free_versions: bool,
    ) -> disk::error::Result<(Vec<FileInfo>, Vec<Option<DiskError>>)> {
        let mut ress = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());
        let opts = Arc::new(ReadOptions {
            incl_free_versions,
            read_data,
            healing,
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

    pub(crate) async fn load_file_info_versions_exact(
        &self,
        bucket: &str,
        object: &str,
    ) -> Result<Option<rustfs_filemeta::FileInfoVersions>> {
        let disks = self.get_disks_internal().await;
        if disks.is_empty() {
            return Err(to_object_err(StorageError::ErasureReadQuorum, vec![bucket, object]));
        }

        let read_quorum = disks.len().div_ceil(2).max(1);
        let (raw_fileinfos, errs) = Self::read_all_raw_file_info(&disks, bucket, object, false).await;

        if let Some(err) = reduce_read_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, read_quorum) {
            let object_err = to_object_err(err.into(), vec![bucket, object]);
            if is_err_object_not_found(&object_err) || is_err_version_not_found(&object_err) {
                return Ok(None);
            }
            return Err(object_err);
        }

        let mut shallow_versions = Vec::with_capacity(raw_fileinfos.len());
        for raw_fileinfo in raw_fileinfos.into_iter().flatten() {
            let meta = FileMeta::load(&raw_fileinfo.buf)
                .map_err(|err| Error::other(format!("exact object metadata decode failed for {bucket}/{object}: {err}")))?;
            shallow_versions.push(meta.versions);
        }

        if shallow_versions.len() < read_quorum {
            return Err(to_object_err(StorageError::ErasureReadQuorum, vec![bucket, object]));
        }

        let versions = merge_file_meta_versions(read_quorum, true, 0, &shallow_versions);
        if versions.is_empty() {
            return Err(Error::other(format!(
                "exact object metadata read returned no quorum versions for {bucket}/{object}"
            )));
        }

        FileMeta {
            versions,
            ..Default::default()
        }
        .get_all_file_info_versions(bucket, object, true)
        .map(Some)
        .map_err(|err| Error::other(format!("exact object versions decode failed for {bucket}/{object}: {err}")))
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
        let empty_quorum_result = || {
            req.files
                .iter()
                .map(|want| ReadMultipleResp {
                    bucket: req.bucket.clone(),
                    prefix: req.prefix.clone(),
                    file: want.clone(),
                    exists: false,
                    error: Error::ErasureReadQuorum.to_string(),
                    data: Vec::new(),
                    mod_time: None,
                })
                .collect::<Vec<_>>()
        };

        for disk in disks.iter() {
            let disk = disk.clone();
            let req = req.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.read_multiple(req).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let (ress, errors) = match collect_read_multiple_results(futures, read_quorum).await {
            Ok(collected) => collected,
            Err(()) => return empty_quorum_result(),
        };

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
        let use_metadata_cache = self.is_get_object_metadata_cache_enabled(bucket, opts, read_data).await;
        if use_metadata_cache && let Some(cached) = self.cached_get_object_fileinfo(bucket, object).await {
            return Ok((cached.fi, cached.parts_metadata, cached.online_disks));
        }

        let disks = self.disks.read().await;

        let disks = disks.clone();

        let vid = opts.version_id.clone().unwrap_or_default();

        // TODO: optimize concurrency and break once enough slots are available
        let (parts_metadata, errs) =
            Self::read_all_fileinfo(&disks, "", bucket, object, vid.as_str(), read_data, false, opts.incl_free_versions).await?;
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
        } else if use_metadata_cache {
            self.cache_get_object_fileinfo(bucket, object, &fi, &parts_metadata, &op_online_disks, read_quorum as usize)
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
    ) -> Result<()>
    where
        W: AsyncWrite + Send + Sync + Unpin + 'static,
    {
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

        let erasure = erasure_coding::Erasure::new_with_options(
            fi.erasure.data_blocks,
            fi.erasure.parity_blocks,
            fi.erasure.block_size,
            fi.uses_legacy_checksum,
        );

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
            let checksum_algo =
                if fi.uses_legacy_checksum && checksum_info.algorithm == rustfs_utils::HashAlgorithm::HighwayHash256S {
                    rustfs_utils::HashAlgorithm::HighwayHash256SLegacy
                } else {
                    checksum_info.algorithm
                };

            // Read zero-copy configuration from environment variable
            // Default: enabled (true) for performance
            let use_zero_copy = rustfs_utils::get_env_bool(ENV_OBJECT_ZERO_COPY_ENABLE, DEFAULT_OBJECT_ZERO_COPY_ENABLE);

            let reader_setup_stage_start = Instant::now();
            let mut readers = Vec::with_capacity(disks.len());
            let mut errors = Vec::with_capacity(disks.len());
            for (idx, disk_op) in disks.iter().enumerate() {
                match create_bitrot_reader(
                    files[idx].data.as_deref(),
                    disk_op.as_ref(),
                    bucket,
                    &format!("{}/{}/part.{}", object, files[idx].data_dir.unwrap_or_default(), part_number),
                    read_offset,
                    till_offset.saturating_sub(read_offset),
                    erasure.shard_size(),
                    checksum_algo.clone(),
                    skip_verify_bitrot,
                    use_zero_copy,
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
            rustfs_io_metrics::record_get_object_shard_reader_setup_duration(reader_setup_stage_start.elapsed().as_secs_f64());

            let nil_count = errors.iter().filter(|&e| e.is_none()).count();
            if nil_count < erasure.data_shards {
                if let Some(read_err) = reduce_read_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, erasure.data_shards) {
                    let reason = classify_disk_error(&read_err);
                    error!(
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
                        errors = ?errors,
                        "Create bitrot reader failed read quorum"
                    );
                    record_get_object_pipeline_failure(GET_STAGE_READER_SETUP, reason);
                    return Err(to_object_err(read_err.into(), vec![bucket, object]));
                }
                let reason = GetObjectFailureReason::ReadQuorum;
                error!(
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
                    errors = ?errors,
                    "Create bitrot reader did not have enough disks"
                );
                record_get_object_pipeline_failure(GET_STAGE_READER_SETUP, reason);
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
            let decode_stage_start = Instant::now();
            let (written, err) = erasure.decode(writer, readers, part_offset, part_length, part_size).await;
            rustfs_io_metrics::record_get_object_decode_duration(decode_stage_start.elapsed().as_secs_f64());
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

        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn get_object_decode_reader_with_fileinfo(
        bucket: &str,
        object: &str,
        fi: FileInfo,
        files: Vec<FileInfo>,
        disks: &[Option<DiskStore>],
        set_index: usize,
        pool_index: usize,
        skip_verify_bitrot: bool,
    ) -> Result<Box<dyn AsyncRead + Unpin + Send + Sync>> {
        let (disks, files) = Self::shuffle_disks_and_parts_metadata_by_index(disks, &files, &fi);
        if fi.parts.len() != 1 {
            return Err(Error::other("codec streaming reader only supports single-part plain objects"));
        }

        let erasure = erasure_coding::Erasure::new_with_options(
            fi.erasure.data_blocks,
            fi.erasure.parity_blocks,
            fi.erasure.block_size,
            fi.uses_legacy_checksum,
        );
        let part = &fi.parts[0];
        let part_number = part.number;
        let part_size = part.size;
        let part_length = usize::try_from(fi.size).map_err(|_| Error::other("codec streaming reader object size is invalid"))?;
        if part_length > part_size {
            return Err(Error::other("codec streaming reader part length exceeds part size"));
        }

        let checksum_info = fi.erasure.get_checksum_info(part_number);
        let checksum_algo = if fi.uses_legacy_checksum && checksum_info.algorithm == rustfs_utils::HashAlgorithm::HighwayHash256S
        {
            rustfs_utils::HashAlgorithm::HighwayHash256SLegacy
        } else {
            checksum_info.algorithm
        };
        let use_zero_copy = rustfs_utils::get_env_bool(ENV_OBJECT_ZERO_COPY_ENABLE, DEFAULT_OBJECT_ZERO_COPY_ENABLE);
        let till_offset = erasure.shard_file_offset(0, part_length, part_size);

        let reader_setup_stage_start = Instant::now();
        let mut readers = Vec::with_capacity(disks.len());
        let mut errors = Vec::with_capacity(disks.len());
        for (idx, disk_op) in disks.iter().enumerate() {
            match create_bitrot_reader(
                files[idx].data.as_deref(),
                disk_op.as_ref(),
                bucket,
                &format!("{}/{}/part.{}", object, files[idx].data_dir.clone().unwrap_or_default(), part_number),
                0,
                till_offset,
                erasure.shard_size(),
                checksum_algo.clone(),
                skip_verify_bitrot,
                use_zero_copy,
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
        rustfs_io_metrics::record_get_object_stage_duration(
            GET_OBJECT_PATH_CODEC_STREAMING,
            GET_STAGE_READER_SETUP,
            reader_setup_stage_start.elapsed().as_secs_f64(),
        );

        let available_shards = errors.iter().filter(|err| err.is_none()).count();
        if available_shards < erasure.data_shards {
            if let Some(read_err) = reduce_read_quorum_errs(&errors, OBJECT_OP_IGNORED_ERRS, erasure.data_shards) {
                let reason = classify_disk_error(&read_err);
                record_get_object_pipeline_failure_for_path(GET_OBJECT_PATH_CODEC_STREAMING, GET_STAGE_READER_SETUP, reason);
                return Err(to_object_err(read_err.into(), vec![bucket, object]));
            }
            record_get_object_pipeline_failure_for_path(
                GET_OBJECT_PATH_CODEC_STREAMING,
                GET_STAGE_READER_SETUP,
                GetObjectFailureReason::ReadQuorum,
            );
            return Err(Error::other(format!("not enough disks to read: {errors:?}")));
        }

        let total_shards = erasure.data_shards + erasure.parity_shards;
        let missing_shards = total_shards.saturating_sub(available_shards);
        if missing_shards > 0 {
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
            }
        }

        let source = erasure_coding::decode::ParallelReader::new(readers, erasure.clone(), 0, part_size);
        let engine = crate::erasure_codec::bridge::LegacyEcDecodeEngine::new(erasure);
        let reader = erasure_coding::decode_reader::ErasureDecodeReader::new(source, engine, part_length)?;
        Ok(Box::new(erasure_coding::decode_reader::SyncErasureDecodeReader::new(reader)))
    }
}

fn is_get_object_metadata_cache_request_eligible(bucket: &str, opts: &ObjectOptions, read_data: bool) -> bool {
    read_data
        && !opts.no_lock
        && opts.version_id.is_none()
        && !opts.versioned
        && !opts.version_suspended
        && !opts.incl_free_versions
        && !opts.delete_marker
        && opts.part_number.is_none()
        && !opts.data_movement
        && !opts.raw_data_movement_read
        && !bucket.starts_with(RUSTFS_META_BUCKET)
}

#[cfg(test)]
mod metadata_cache_tests {
    use super::*;

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
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, false));
        assert!(!is_get_object_metadata_cache_request_eligible(RUSTFS_META_BUCKET, &opts, true));

        let mut opts = ObjectOptions {
            no_lock: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));

        opts = ObjectOptions {
            version_id: Some("version".to_string()),
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));

        opts = ObjectOptions {
            versioned: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));

        opts = ObjectOptions {
            version_suspended: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));

        opts = ObjectOptions {
            incl_free_versions: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));

        opts = ObjectOptions {
            delete_marker: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));

        opts = ObjectOptions {
            part_number: Some(1),
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));

        opts = ObjectOptions {
            data_movement: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));

        opts = ObjectOptions {
            raw_data_movement_read: true,
            ..Default::default()
        };
        assert!(!is_get_object_metadata_cache_request_eligible("bucket", &opts, true));
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

        set.get_object_metadata_cache.write().await.insert(
            GetObjectMetadataCacheKey::new("bucket", "object"),
            GetObjectMetadataCacheEntry {
                created_at: Instant::now(),
                fi: fi.clone(),
                parts_metadata: vec![fi],
                online_disks: vec![None],
                read_quorum: 1,
            },
        );

        assert!(
            set.cached_get_object_fileinfo("bucket", "object").await.is_none(),
            "cache hit must be rejected when cached online disks cannot satisfy read quorum"
        );
    }

    #[tokio::test]
    async fn get_object_metadata_cache_rejects_stale_entries() {
        let set = new_metadata_cache_test_set().await;
        let fi = valid_test_fileinfo("object");

        set.get_object_metadata_cache.write().await.insert(
            GetObjectMetadataCacheKey::new("bucket", "object"),
            GetObjectMetadataCacheEntry {
                created_at: Instant::now() - GET_OBJECT_METADATA_CACHE_TTL - Duration::from_millis(1),
                fi: fi.clone(),
                parts_metadata: vec![fi],
                online_disks: Vec::new(),
                read_quorum: 0,
            },
        );

        assert!(
            set.cached_get_object_fileinfo("bucket", "object").await.is_none(),
            "stale cache entry must not be returned"
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
        let set = new_metadata_cache_test_set().await;
        let stale_fi = valid_test_fileinfo("stale-object");
        let fresh_fi = valid_test_fileinfo("fresh-object");
        {
            let mut cache = set.get_object_metadata_cache.write().await;
            for idx in 0..GET_OBJECT_METADATA_CACHE_MAX_ENTRIES {
                cache.insert(
                    GetObjectMetadataCacheKey::new("bucket", &format!("stale-object-{idx}")),
                    GetObjectMetadataCacheEntry {
                        created_at: Instant::now() - GET_OBJECT_METADATA_CACHE_TTL - Duration::from_millis(1),
                        fi: stale_fi.clone(),
                        parts_metadata: vec![stale_fi.clone()],
                        online_disks: Vec::new(),
                        read_quorum: 0,
                    },
                );
            }
        }

        set.cache_get_object_fileinfo("bucket", "fresh-object", &fresh_fi, std::slice::from_ref(&fresh_fi), &[], 0)
            .await;

        let cache = set.get_object_metadata_cache.read().await;
        assert_eq!(cache.len(), 1);
        assert!(cache.contains_key(&GetObjectMetadataCacheKey::new("bucket", "fresh-object")));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    fn codec_streaming_test_object_info(fi: &FileInfo) -> ObjectInfo {
        ObjectInfo::from_file_info(fi, "bucket", "object", false)
    }

    #[test]
    fn codec_streaming_reader_gate_is_conservative() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, Some("true")),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);
                assert_eq!(
                    get_codec_streaming_reader_decision(&None, &object_info, &fi, true),
                    GetCodecStreamingDecision::Use
                );

                assert_eq!(
                    get_codec_streaming_reader_decision(&None, &object_info, &fi, false),
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::LockOptimizationDisabled)
                );

                let range = Some(HTTPRangeSpec {
                    is_suffix_length: false,
                    start: 0,
                    end: 1,
                });
                assert_eq!(
                    get_codec_streaming_reader_decision(&range, &object_info, &fi, true),
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Range)
                );

                let multipart_fi = codec_streaming_test_fileinfo(1024, 2);
                let multipart_object_info = codec_streaming_test_object_info(&multipart_fi);
                assert_eq!(
                    get_codec_streaming_reader_decision(&None, &multipart_object_info, &multipart_fi, true),
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Multipart)
                );

                let mut encrypted_fi = fi.clone();
                encrypted_fi
                    .metadata
                    .insert("x-amz-server-side-encryption".to_string(), "AES256".to_string());
                let encrypted = codec_streaming_test_object_info(&encrypted_fi);
                assert_eq!(
                    get_codec_streaming_reader_decision(&None, &encrypted, &fi, true),
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Encrypted)
                );

                let mut compressed_fi = fi.clone();
                insert_str(&mut compressed_fi.metadata, SUFFIX_COMPRESSION, "lz4".to_string());
                let compressed = codec_streaming_test_object_info(&compressed_fi);
                assert_eq!(
                    get_codec_streaming_reader_decision(&None, &compressed, &fi, true),
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Compressed)
                );

                let small_fi = codec_streaming_test_fileinfo(0, 1);
                let small_object_info = codec_streaming_test_object_info(&small_fi);
                assert_eq!(
                    get_codec_streaming_reader_decision(&None, &small_object_info, &small_fi, true),
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::BelowMinSize)
                );

                let mut remote_fi = fi.clone();
                remote_fi.transition_status = crate::bucket::lifecycle::lifecycle::TRANSITION_COMPLETE.to_string();
                let remote = codec_streaming_test_object_info(&remote_fi);
                assert_eq!(
                    get_codec_streaming_reader_decision(&None, &remote, &remote_fi, true),
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Remote)
                );
            },
        );
    }

    #[test]
    fn codec_streaming_fallback_metric_labels_are_stable() {
        assert_eq!(GetCodecStreamingFallbackReason::Disabled.as_str(), "disabled");
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
    }

    #[test]
    fn codec_streaming_reader_gate_defaults_to_disabled() {
        temp_env::with_vars(
            [
                (ENV_RUSTFS_GET_CODEC_STREAMING_ENABLE, None::<&str>),
                (ENV_RUSTFS_GET_CODEC_STREAMING_MIN_SIZE, Some("1")),
            ],
            || {
                let fi = codec_streaming_test_fileinfo(1024, 1);
                let object_info = codec_streaming_test_object_info(&fi);

                assert_eq!(
                    get_codec_streaming_reader_decision(&None, &object_info, &fi, true),
                    GetCodecStreamingDecision::Fallback(GetCodecStreamingFallbackReason::Disabled)
                );
            },
        );
    }

    #[tokio::test]
    async fn collect_read_multiple_results_fails_early_when_quorum_is_impossible() {
        let started = std::time::Instant::now();
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
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let result = collect_read_multiple_results(tasks, 2).await;
        assert!(result.is_err(), "quorum should become impossible before slow tail completes");
        assert!(started.elapsed() < std::time::Duration::from_millis(120));
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
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
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
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
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
        let started = std::time::Instant::now();
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
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
            outcome
        })
        .collect();

        let result = collect_read_parts_results(tasks, 2).await;
        assert!(result.is_err(), "quorum should become impossible before slow tail completes");
        assert!(started.elapsed() < std::time::Duration::from_millis(120));
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
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
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
                    tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
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
