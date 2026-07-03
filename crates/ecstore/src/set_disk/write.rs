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

/// Grace window during which a recently modified object is never deleted as
/// dangling. 0 disables the grace window.
const ENV_HEAL_DANGLING_DELETE_GRACE_SECS: &str = "RUSTFS_HEAL_DANGLING_DELETE_GRACE_SECS";
const DEFAULT_HEAL_DANGLING_DELETE_GRACE_SECS: u64 = 3600;

fn dangling_delete_grace() -> time::Duration {
    let secs = rustfs_utils::get_env_u64(ENV_HEAL_DANGLING_DELETE_GRACE_SECS, DEFAULT_HEAL_DANGLING_DELETE_GRACE_SECS);
    time::Duration::seconds(i64::try_from(secs).unwrap_or(i64::MAX))
}

/// Result of scanning one disk's copy of a directory prefix while deciding
/// whether an orphan (metadata-less) directory tree can be safely purged.
enum OrphanDirScan {
    /// The subtree holds at least one regular file (object metadata or data), so
    /// it is a real object and must not be purged.
    HasData,
    /// The prefix exists on this disk and contains only nested empty directories.
    /// Carries every directory path in pre-order (parents before children).
    Empty(Vec<String>),
    /// The prefix does not exist on this disk.
    Missing,
}

impl SetDisks {
    pub(super) fn default_read_quorum(&self) -> usize {
        self.set_drive_count - self.default_parity_count
    }

    pub(super) fn default_write_quorum(&self) -> usize {
        let mut data_count = self.set_drive_count - self.default_parity_count;
        if data_count == self.default_parity_count {
            data_count += 1
        }

        data_count
    }

    #[tracing::instrument(level = "debug", skip(disks, file_infos))]
    #[allow(clippy::type_complexity)]
    pub(super) async fn rename_data(
        disks: &[Option<DiskStore>],
        src_bucket: &str,
        src_object: &str,
        file_infos: &[FileInfo],
        dst_bucket: &str,
        dst_object: &str,
        write_quorum: usize,
    ) -> disk::error::Result<(Vec<Option<DiskStore>>, Option<Vec<u8>>, Option<Uuid>, Vec<Option<DiskStore>>)> {
        let mut futures = Vec::with_capacity(disks.len());

        let mut errs = Vec::with_capacity(disks.len());

        let src_bucket = Arc::new(src_bucket.to_string());
        let src_object = Arc::new(src_object.to_string());
        let dst_bucket = Arc::new(dst_bucket.to_string());
        let dst_object = Arc::new(dst_object.to_string());

        for (i, (disk, file_info)) in disks.iter().zip(file_infos.iter()).enumerate() {
            let mut file_info = file_info.clone();
            let disk = disk.clone();
            let src_bucket = src_bucket.clone();
            let src_object = src_object.clone();
            let dst_object = dst_object.clone();
            let dst_bucket = dst_bucket.clone();

            futures.push(tokio::spawn(async move {
                if file_info.erasure.index == 0 {
                    file_info.erasure.index = i + 1;
                }

                if !file_info.is_valid() {
                    return Err(DiskError::FileCorrupt);
                }

                if let Some(disk) = disk {
                    disk.rename_data(&src_bucket, &src_object, file_info, &dst_bucket, &dst_object)
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            }));
        }

        let mut disk_versions = vec![None; disks.len()];
        let mut data_dirs = vec![None; disks.len()];

        let results = join_all(futures).await;

        for (idx, result) in results.iter().enumerate() {
            match result.as_ref().map_err(|_| DiskError::Unexpected)? {
                Ok(res) => {
                    data_dirs[idx] = res.old_data_dir;
                    disk_versions[idx].clone_from(&res.sign);
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e.clone()));
                }
            }
        }

        if issue3031_diag_enabled() {
            let success_count = errs.iter().filter(|err| err.is_none()).count();
            let failure_count = errs.len().saturating_sub(success_count);
            let ignored_failure_count = errs
                .iter()
                .filter(|err| err.as_ref().is_some_and(|err| OBJECT_OP_IGNORED_ERRS.contains(err)))
                .count();
            let data_dir_vote_count = data_dirs.iter().filter(|data_dir| data_dir.is_some()).count();
            let reduced_data_dir = Self::reduce_common_data_dir(&data_dirs, write_quorum);
            warn!(
                target: "rustfs_ecstore::set_disk",
                src_bucket = %src_bucket,
                src_object = %src_object,
                dst_bucket = %dst_bucket,
                dst_object = %dst_object,
                write_quorum,
                disk_count = errs.len(),
                success_count,
                failure_count,
                ignored_failure_count,
                data_dir_vote_count,
                reduced_data_dir = ?reduced_data_dir,
                errs = ?errs,
                data_dirs = ?data_dirs,
                "issue3031_rename_data_quorum_context"
            );
        }

        let mut futures = Vec::with_capacity(disks.len());
        if let Some(ret_err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            // TODO: add concurrency
            for (i, err) in errs.iter().enumerate() {
                if err.is_some() {
                    continue;
                }

                if let Some(disk) = disks[i].as_ref() {
                    let fi = file_infos[i].clone();
                    let old_data_dir = data_dirs[i];
                    let disk = disk.clone();
                    let dst_bucket = dst_bucket.clone();
                    let dst_object = dst_object.clone();
                    futures.push(tokio::spawn(async move {
                        disk.delete_version(
                            &dst_bucket,
                            &dst_object,
                            fi,
                            false,
                            DeleteOptions {
                                undo_write: true,
                                old_data_dir,
                                ..Default::default()
                            },
                        )
                        .await
                    }));
                }
            }

            if issue3031_diag_enabled() {
                warn!(
                    target: "rustfs_ecstore::set_disk",
                    src_bucket = %src_bucket,
                    src_object = %src_object,
                    dst_bucket = %dst_bucket,
                    dst_object = %dst_object,
                    write_quorum,
                    ret_err = %ret_err,
                    errs = ?errs,
                    data_dirs = ?data_dirs,
                    "issue3031_rename_data_quorum_failed"
                );
            }

            let undo_results = join_all(futures).await;
            let undo_error_count = undo_results
                .iter()
                .filter(|result| match result {
                    Err(_) | Ok(Err(_)) => true,
                    Ok(Ok(_)) => false,
                })
                .count();
            if undo_error_count > 0 {
                warn!(
                    target: "rustfs_ecstore::set_disk",
                    dst_bucket = %dst_bucket,
                    dst_object = %dst_object,
                    undo_error_count,
                    "rename_data quorum rollback reported errors"
                );
            }
            return Err(ret_err);
        }

        let versions = None;
        // TODO: reduceCommonVersions

        let data_dir = Self::reduce_common_data_dir(&data_dirs, write_quorum);
        let online_disks = Self::eval_disks(disks, &errs);
        let cleanup_disks = if let Some(data_dir) = data_dir {
            disks
                .iter()
                .zip(errs.iter())
                .zip(data_dirs.iter())
                .map(|((disk, err), old_data_dir)| {
                    if err.is_none() && *old_data_dir == Some(data_dir) {
                        disk.clone()
                    } else {
                        None
                    }
                })
                .collect()
        } else {
            vec![None; disks.len()]
        };

        // // TODO: reduce_common_data_dir
        // if let Some(old_dir) = rename_ress
        //     .iter()
        //     .filter_map(|v| if v.is_some() { v.as_ref().unwrap().old_data_dir } else { None })
        //     .map(|v| v.to_string())
        //     .next()
        // {
        //     let cm_errs = self.commit_rename_data_dir(&shuffle_disks, &bucket, &object, &old_dir).await;
        //     warn!("put_object commit_rename_data_dir:{:?}", &cm_errs);
        // }

        // self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_dir).await?;

        Ok((online_disks, versions, data_dir, cleanup_disks))
    }

    #[allow(dead_code)]
    #[tracing::instrument(level = "debug", skip(self, disks))]
    pub(super) async fn commit_rename_data_dir(
        &self,
        disks: &[Option<DiskStore>],
        bucket: &str,
        object: &str,
        data_dir: &str,
        write_quorum: usize,
    ) -> disk::error::Result<()> {
        let file_path = Arc::new(format!("{object}/{data_dir}"));
        let bucket = Arc::new(bucket.to_string());
        let futures = disks.iter().map(|disk| {
            let file_path = file_path.clone();
            let bucket = bucket.clone();
            let disk = disk.clone();
            tokio::spawn(async move {
                if let Some(disk) = disk {
                    (disk
                        .delete(
                            &bucket,
                            &file_path,
                            DeleteOptions {
                                recursive: true,
                                ..Default::default()
                            },
                        )
                        .await)
                        .err()
                } else {
                    Some(DiskError::DiskNotFound)
                }
            })
        });
        let errs: Vec<Option<DiskError>> = join_all(futures)
            .await
            .into_iter()
            .map(|e| e.unwrap_or(Some(DiskError::Unexpected)))
            .collect();

        if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            return Err(err);
        }

        Ok(())
    }

    #[tracing::instrument(skip(self))]
    pub(super) async fn cleanup_multipart_path(&self, paths: &[String]) {
        if paths.is_empty() {
            return;
        }
        let disks = self.get_disks_internal().await;

        let mut errs = Vec::with_capacity(disks.len());

        // Use improved simple batch processor instead of join_all for better performance
        let processor = runtime_sources::batch_processors().write_processor();

        let tasks: Vec<_> = disks
            .iter()
            .map(|disk| {
                let disk = disk.clone();
                let paths = paths.to_vec();

                async move {
                    if let Some(disk) = disk {
                        disk.delete_paths(RUSTFS_META_MULTIPART_BUCKET, &paths).await
                    } else {
                        Err(DiskError::DiskNotFound)
                    }
                }
            })
            .collect();

        let results = processor.execute_batch(tasks).await;
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

        if errs.iter().any(|e| e.is_some()) {
            warn!("cleanup_multipart_path errs {:?}", &errs);
        }
    }

    #[tracing::instrument(skip(disks, meta))]
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn rename_part(
        &self,
        disks: &[Option<DiskStore>],
        src_bucket: &str,
        src_object: &str,
        dst_bucket: &str,
        dst_object: &str,
        meta: Bytes,
        write_quorum: usize,
        quorum_context: Option<MultipartWriteQuorumContext<'_>>,
    ) -> disk::error::Result<Vec<Option<DiskStore>>> {
        let src_bucket = Arc::new(src_bucket.to_string());
        let src_object = Arc::new(src_object.to_string());
        let dst_bucket = Arc::new(dst_bucket.to_string());
        let dst_object = Arc::new(dst_object.to_string());

        // Match MinIO's multipart overwrite semantics: clear any stale destination
        // part payload and metadata before the new per-disk rename fan-out begins.
        self.cleanup_multipart_path(&[dst_object.to_string(), format!("{dst_object}.meta")])
            .await;

        let mut errs = Vec::with_capacity(disks.len());

        let futures = disks.iter().map(|disk| {
            let disk = disk.clone();
            let meta = meta.clone();
            let src_bucket = src_bucket.clone();
            let src_object = src_object.clone();
            let dst_bucket = dst_bucket.clone();
            let dst_object = dst_object.clone();
            tokio::spawn(async move {
                if let Some(disk) = disk {
                    disk.rename_part(&src_bucket, &src_object, &dst_bucket, &dst_object, meta)
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            })
        });

        let results = join_all(futures).await;
        for result in results {
            match result? {
                Ok(_) => {
                    errs.push(None);
                }
                Err(e) => {
                    errs.push(Some(e));
                }
            }
        }

        if issue3031_diag_enabled() {
            let success_count = errs.iter().filter(|err| err.is_none()).count();
            let error_count = errs.len().saturating_sub(success_count);
            let disk_not_found_count = errs.iter().filter(|err| matches!(err, Some(DiskError::DiskNotFound))).count();
            let file_not_found_count = errs.iter().filter(|err| matches!(err, Some(DiskError::FileNotFound))).count();
            warn!(
                target: "rustfs_ecstore::set_disk",
                src_bucket = %src_bucket,
                src_object = %src_object,
                dst_bucket = %dst_bucket,
                dst_object = %dst_object,
                write_quorum = write_quorum,
                disk_count = errs.len(),
                success_count = success_count,
                error_count = error_count,
                disk_not_found_count = disk_not_found_count,
                file_not_found_count = file_not_found_count,
                "issue3031_rename_part_context"
            );
        }

        if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            if let Some(context) = quorum_context {
                log_multipart_write_quorum_failure(context, &errs, write_quorum, &err);
            } else {
                warn!("rename_part errs {:?}", &errs);
            }
            self.cleanup_multipart_path(&[dst_object.to_string(), format!("{dst_object}.meta")])
                .await;
            return Err(err);
        }

        let disks = Self::eval_disks(disks, &errs);
        Ok(disks)
    }

    pub(super) fn eval_disks(disks: &[Option<DiskStore>], errs: &[Option<DiskError>]) -> Vec<Option<DiskStore>> {
        if disks.len() != errs.len() {
            return Vec::new();
        }

        let mut online_disks = vec![None; disks.len()];

        for (i, err_op) in errs.iter().enumerate() {
            if err_op.is_none() {
                online_disks[i].clone_from(&disks[i]);
            }
        }

        online_disks
    }

    #[tracing::instrument(skip(disks, files))]
    pub(super) async fn write_unique_file_info(
        disks: &[Option<DiskStore>],
        org_bucket: &str,
        bucket: &str,
        prefix: &str,
        files: &[FileInfo],
        write_quorum: usize,
    ) -> disk::error::Result<()> {
        let mut futures = Vec::with_capacity(disks.len());
        let mut errs = Vec::with_capacity(disks.len());

        for (i, disk) in disks.iter().enumerate() {
            let mut file_info = files[i].clone();
            file_info.erasure.index = i + 1;
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.write_metadata(org_bucket, bucket, prefix, file_info).await
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

        if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            let mut revert_futures = Vec::with_capacity(disks.len());
            for (i, err) in errs.iter().enumerate() {
                if err.is_some() {
                    continue;
                }

                if let Some(disk) = disks[i].as_ref() {
                    let disk = disk.clone();
                    let bucket = bucket.to_string();
                    let path = path_join_buf(&[prefix, STORAGE_FORMAT_FILE]);
                    revert_futures.push(async move {
                        if let Err(err) = disk
                            .delete(
                                &bucket,
                                &path,
                                DeleteOptions {
                                    recursive: true,
                                    ..Default::default()
                                },
                            )
                            .await
                        {
                            warn!("write meta revert err {:?}", err);
                        }
                    });
                }
            }

            join_all(revert_futures).await;
            return Err(err);
        }
        Ok(())
    }

    pub(super) async fn update_object_meta(
        &self,
        bucket: &str,
        object: &str,
        fi: FileInfo,
        disks: &[Option<DiskStore>],
    ) -> disk::error::Result<()> {
        self.update_object_meta_with_opts(bucket, object, fi, disks, &UpdateMetadataOpts::default())
            .await
    }

    pub(super) async fn update_object_meta_with_opts(
        &self,
        bucket: &str,
        object: &str,
        fi: FileInfo,
        disks: &[Option<DiskStore>],
        opts: &UpdateMetadataOpts,
    ) -> disk::error::Result<()> {
        if fi.metadata.is_empty() && !opts.replace_user_metadata {
            return Ok(());
        }

        self.invalidate_get_object_metadata_cache(bucket, object).await;

        let mut futures = Vec::with_capacity(disks.len());

        let mut errs = Vec::with_capacity(disks.len());

        for disk in disks.iter() {
            let fi = fi.clone();
            futures.push(async move {
                if let Some(disk) = disk {
                    disk.update_metadata(bucket, object, fi, opts).await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            })
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

        if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, fi.write_quorum(self.default_write_quorum())) {
            return Err(err);
        }

        self.invalidate_get_object_metadata_cache(bucket, object).await;

        Ok(())
    }

    pub(super) async fn delete_if_dangling(
        &self,
        bucket: &str,
        object: &str,
        meta_arr: &[FileInfo],
        errs: &[Option<DiskError>],
        data_errs_by_part: &HashMap<usize, Vec<usize>>,
        opts: ObjectOptions,
    ) -> disk::error::Result<FileInfo> {
        let (m, can_heal) = is_object_dangling(meta_arr, errs, data_errs_by_part);

        if !can_heal {
            return Err(DiskError::ErasureReadQuorum);
        }

        // Recently written objects get a grace window before dangling cleanup: after
        // an unclean shutdown some disks may still be catching up (or carry writes
        // that were never made durable), and deleting the surviving shards right away
        // turns a partial loss into a total one. Skip deletion and leave the object
        // for a later heal/scanner pass to re-evaluate.
        if m.is_valid()
            && let Some(mod_time) = m.mod_time
        {
            let grace = dangling_delete_grace();
            if !grace.is_zero() && OffsetDateTime::now_utc() - mod_time < grace {
                info!(
                    bucket = bucket,
                    object = object,
                    mod_time = %mod_time,
                    grace_secs = grace.whole_seconds(),
                    "skipping dangling-object deletion within grace window"
                );
                return Err(DiskError::ErasureReadQuorum);
            }
        }

        let mut tags: HashMap<String, String> = HashMap::new();
        tags.insert("set".to_string(), self.set_index.to_string());
        tags.insert("pool".to_string(), self.pool_index.to_string());
        tags.insert("merrs".to_string(), join_errs(errs));
        tags.insert("derrs".to_string(), format!("{data_errs_by_part:?}"));
        if m.is_valid() {
            tags.insert("sz".to_string(), m.size.to_string());
            tags.insert(
                "mt".to_string(),
                m.mod_time
                    .as_ref()
                    .map_or(String::new(), |mod_time| mod_time.unix_timestamp().to_string()),
            );
            tags.insert("d:p".to_string(), format!("{}:{}", m.erasure.data_blocks, m.erasure.parity_blocks));
        } else {
            tags.insert("invalid".to_string(), "1".to_string());
            tags.insert(
                "d:p".to_string(),
                format!("{}:{}", self.set_drive_count - self.default_parity_count, self.default_parity_count),
            );
        }
        let mut offline = 0;
        for (i, err) in errs.iter().enumerate() {
            let mut found = false;
            if let Some(err) = err
                && err == &DiskError::DiskNotFound
            {
                found = true;
            }
            for p in data_errs_by_part {
                if let Some(v) = p.1.get(i)
                    && *v == CHECK_PART_DISK_NOT_FOUND
                {
                    found = true;
                    break;
                }
            }

            if found {
                offline += 1;
            }
        }

        if offline > 0 {
            tags.insert("offline".to_string(), offline.to_string());
        }

        let mut fi = FileInfo::default();
        if let Some(ref version_id) = opts.version_id {
            fi.version_id = Uuid::parse_str(version_id).ok();
        }

        fi.set_tier_free_version_id(&Uuid::new_v4().to_string());

        let disks = self.get_disks_internal().await;

        let mut futures = Vec::with_capacity(disks.len());
        for disk_op in disks.iter() {
            let bucket = bucket.to_string();
            let object = object.to_string();
            let fi = fi.clone();
            futures.push(async move {
                if let Some(disk) = disk_op {
                    disk.delete_version(&bucket, &object, fi, false, DeleteOptions::default())
                        .await
                } else {
                    Err(DiskError::DiskNotFound)
                }
            });
        }

        let results = join_all(futures).await;
        let mut delete_errs = Vec::with_capacity(results.len());
        for (index, result) in results.into_iter().enumerate() {
            let key = format!("ddisk-{index}");
            let already_absent = matches!(
                errs.get(index).and_then(Option::as_ref),
                Some(DiskError::FileNotFound | DiskError::FileVersionNotFound)
            );
            match result {
                Ok(_) => {
                    tags.insert(key, "<nil>".to_string());
                    delete_errs.push(None);
                }
                Err(e) => {
                    tags.insert(key, e.to_string());
                    if already_absent || matches!(&e, DiskError::FileNotFound | DiskError::FileVersionNotFound) {
                        delete_errs.push(None);
                    } else {
                        delete_errs.push(Some(e));
                    }
                }
            }
        }

        let write_quorum = if m.is_valid() {
            m.write_quorum(self.default_write_quorum())
        } else {
            self.default_write_quorum()
        };
        if let Some(err) = reduce_write_quorum_errs(&delete_errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            return Err(err);
        }

        Ok(m)
    }

    pub(super) async fn delete_prefix(&self, bucket: &str, prefix: &str) -> disk::error::Result<()> {
        let disks = self.get_disks_internal().await;
        let write_quorum = disks.len() / 2 + 1;

        let mut futures = Vec::with_capacity(disks.len());

        for disk_op in disks.iter() {
            let bucket = bucket.to_string();
            let prefix = prefix.to_string();
            futures.push(async move {
                if let Some(disk) = disk_op {
                    disk.delete(
                        &bucket,
                        &prefix,
                        DeleteOptions {
                            recursive: true,
                            immediate: true,
                            ..Default::default()
                        },
                    )
                    .await
                } else {
                    Ok(())
                }
            });
        }

        let errs = join_all(futures).await.into_iter().map(|v| v.err()).collect::<Vec<_>>();

        if let Some(err) = reduce_write_quorum_errs(&errs, OBJECT_OP_IGNORED_ERRS, write_quorum) {
            return Err(err);
        }

        Ok(())
    }

    /// Scan a single disk's copy of `prefix` and decide whether it is an orphan
    /// (metadata-less) directory subtree. Walks the tree iteratively and returns
    /// [`OrphanDirScan::HasData`] as soon as any regular file is found.
    async fn scan_orphan_dir(disk: &DiskStore, bucket: &str, prefix: &str) -> OrphanDirScan {
        let root = prefix.trim_end_matches(SLASH_SEPARATOR).to_string();
        let mut stack = vec![root.clone()];
        // Pre-order list of directories (a parent always precedes its descendants),
        // so reversing it yields a safe children-first removal order.
        let mut dirs: Vec<String> = Vec::new();
        let mut existed = false;

        while let Some(dir) = stack.pop() {
            let entries = match disk.list_dir("", bucket, &dir, 0).await {
                Ok(entries) => entries,
                Err(_) => {
                    // The root missing (or never existing) means there is nothing to
                    // purge on this disk. A nested directory vanishing mid-scan is a
                    // benign race, so skip it and keep walking.
                    if dir == root {
                        return OrphanDirScan::Missing;
                    }
                    continue;
                }
            };

            existed = true;
            dirs.push(dir.clone());

            for entry in entries {
                match entry.strip_suffix(SLASH_SEPARATOR) {
                    // `read_dir` marks directories with a trailing slash; anything else
                    // is a regular file, which means real object data lives here.
                    Some(child) => stack.push(format!("{dir}{SLASH_SEPARATOR}{child}")),
                    None => return OrphanDirScan::HasData,
                }
            }
        }

        if existed {
            OrphanDirScan::Empty(dirs)
        } else {
            OrphanDirScan::Missing
        }
    }

    /// Purge an orphan directory prefix — a trailing-slash key that exists on disk
    /// as an empty directory tree with no object metadata on any disk of this set.
    /// Such prefixes are listable (see `scan_dir`) yet are not real objects, so the
    /// normal delete path returns NotFound and leaves them stranded (issue #4189).
    ///
    /// Callers pass the *decoded* directory name (`prefix/`), not the `__XLDIR__`
    /// encoded object key — the orphan tree on disk uses the plain path.
    ///
    /// Returns `Ok(true)` when the prefix was an orphan tree on this set and was
    /// removed, `Ok(false)` when it holds real data or does not exist on any disk
    /// of this set (the caller should surface the original NotFound), and `Err` on
    /// a hard disk failure.
    pub(crate) async fn purge_orphan_dir_object(&self, bucket: &str, object: &str) -> disk::error::Result<bool> {
        let disks = self.get_disks_internal().await;

        // Phase 1: classify every online disk. Refuse to purge if ANY disk holds
        // object data under the prefix, so a degraded/healable object is never
        // destroyed.
        let mut per_disk_dirs: Vec<(usize, Vec<String>)> = Vec::new();
        let mut existed = false;
        for (i, disk) in disks.iter().enumerate() {
            let Some(disk) = disk else { continue };
            match Self::scan_orphan_dir(disk, bucket, object).await {
                OrphanDirScan::HasData => return Ok(false),
                OrphanDirScan::Empty(dirs) => {
                    existed = true;
                    per_disk_dirs.push((i, dirs));
                }
                OrphanDirScan::Missing => {}
            }
        }

        if !existed {
            return Ok(false);
        }

        // Phase 2: remove the empty directories children-first on each disk. A
        // non-recursive delete performs an empty-only `rmdir`, so a directory that
        // concurrently gained an object fails with DirectoryNotEmpty and is skipped —
        // a racing PutObject is never clobbered.
        for (i, mut dirs) in per_disk_dirs {
            let Some(disk) = disks[i].as_ref() else { continue };
            dirs.reverse();
            for dir in dirs {
                if let Err(err) = disk
                    .delete(
                        bucket,
                        &dir,
                        DeleteOptions {
                            recursive: false,
                            immediate: true,
                            ..Default::default()
                        },
                    )
                    .await
                {
                    // Best effort: a sibling removal may have already cleared a shared
                    // parent, or a concurrent writer repopulated the directory. Neither
                    // is fatal to purging the orphan tree.
                    debug!(bucket, object, dir, error = ?err, "purge_orphan_dir_object: skipped non-empty/absent directory");
                }
            }
        }

        Ok(true)
    }

    pub(super) async fn check_write_precondition(
        &self,
        bucket: &str,
        object: &str,
        opts: &ObjectOptions,
    ) -> Option<StorageError> {
        let mut opts = opts.clone();

        let http_preconditions = opts.http_preconditions?;
        opts.http_preconditions = None;

        // Never claim a lock here, to avoid deadlock
        // - If no_lock is false, we must have obtained the lock out side of this function
        // - If no_lock is true, we should not obtain locks
        opts.no_lock = true;
        let oi = self.get_object_info(bucket, object, &opts).await;

        match oi {
            Ok(oi) => {
                // If top level is a delete marker proceed to upload.
                if oi.delete_marker {
                    return None;
                }
                let if_none_match = http_preconditions.if_none_match_value().map(str::to_owned);
                let if_match = http_preconditions.if_match_value().map(str::to_owned);
                if should_prevent_write(&oi, if_none_match, if_match) {
                    return Some(StorageError::PreconditionFailed);
                }
            }

            Err(StorageError::VersionNotFound(_, _, _))
            | Err(StorageError::ObjectNotFound(_, _))
            | Err(StorageError::ErasureReadQuorum) => {
                // When the object is not found,
                // - if If-Match is set, we should return 404 NotFound
                // - if If-None-Match is set, we should be able to proceed with the request
                if http_preconditions.if_match_value().is_some() {
                    return Some(StorageError::ObjectNotFound(bucket.to_string(), object.to_string()));
                }
            }

            Err(e) => {
                return Some(e);
            }
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dangling_delete_grace_defaults_to_one_hour() {
        temp_env::with_var(ENV_HEAL_DANGLING_DELETE_GRACE_SECS, None::<&str>, || {
            assert_eq!(dangling_delete_grace(), time::Duration::seconds(3600));
        });
    }

    #[test]
    fn dangling_delete_grace_env_override_and_disable() {
        temp_env::with_var(ENV_HEAL_DANGLING_DELETE_GRACE_SECS, Some("120"), || {
            assert_eq!(dangling_delete_grace(), time::Duration::seconds(120));
        });
        temp_env::with_var(ENV_HEAL_DANGLING_DELETE_GRACE_SECS, Some("0"), || {
            assert!(dangling_delete_grace().is_zero());
        });
    }
}
