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

use super::super::*;
use crate::io_support::bitrot::object_mmap_read_enabled;
use crate::storage_api_contracts::namespace::NamespaceLocking as _;

const LOG_COMPONENT_ECSTORE: &str = "ecstore";
const LOG_SUBSYSTEM_HEAL: &str = "heal";
const EVENT_HEAL_OBJECT_RENAME: &str = "heal_object_rename";

impl SetDisks {
    #[tracing::instrument(skip(self, opts), fields(bucket = %bucket, object = %object, version_id = %version_id))]
    pub(in crate::set_disk) async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> disk::error::Result<(HealResultItem, Option<DiskError>)> {
        // `allow_meta_regen` is true on the first pass: a version whose data shards
        // physically survive (>= data_blocks) but whose xl.meta fell below
        // read-quorum is RESCUED (missing xl.meta regenerated) rather than
        // dangling-deleted. The re-drive after a rescue sets it false so the
        // regeneration can happen at most once (no unbounded recursion).
        Box::pin(self.heal_object_with_regen(bucket, object, version_id, opts, true)).await
    }

    /// Best-effort orphan-data-dir reclaim for an object that is healthy on this
    /// set. Wraps [`Self::reclaim_orphan_data_dirs`] with the shared logging so
    /// both `heal_object` exits — the already-healthy early return and the
    /// post-heal tail — reclaim identically. Never fails the heal: delete errors
    /// are logged and swallowed. Callers must gate this on `!opts.dry_run`.
    async fn reclaim_orphan_data_dirs_best_effort(&self, bucket: &str, object: &str) {
        match self.reclaim_orphan_data_dirs(bucket, object).await {
            Ok(removed) if removed > 0 => {
                info!(bucket, object, removed, "heal_object: reclaimed orphaned data directories");
            }
            Ok(_) => {}
            Err(e) => {
                warn!(bucket, object, error = %e, "heal_object: orphan data-dir reclaim failed");
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn heal_object_with_regen(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
        allow_meta_regen: bool,
    ) -> disk::error::Result<(HealResultItem, Option<DiskError>)> {
        info!(?opts, "Starting heal_object");

        let disks = self.get_disks_internal().await;

        let mut result = HealResultItem {
            heal_item_type: HealItemType::Object.to_string(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            version_id: version_id.to_string(),
            disk_count: disks.len(),
            ..Default::default()
        };

        let write_lock_guard = if !opts.no_lock {
            let ns_lock = self.new_ns_lock(bucket, object).await?;
            Some(
                ns_lock
                    .get_write_lock(get_lock_acquire_timeout())
                    .await
                    .map_err(|e| self.map_namespace_lock_error(bucket, object, "write", e))?,
            )
        } else {
            None
        };

        let version_id_op = {
            if version_id.is_empty() {
                None
            } else {
                Some(version_id.to_string())
            }
        };

        let (mut parts_metadata, errs) =
            Self::read_all_fileinfo(&disks, "", bucket, object, version_id, true, true, false).await?;

        info!(
            parts_count = parts_metadata.len(),
            bucket = bucket,
            object = object,
            version_id = version_id,
            ?errs,
            "File info read complete"
        );
        if DiskError::is_all_not_found(&errs) {
            debug!(bucket, object, version_id, "heal_object skipped missing object");
            let err = if !version_id.is_empty() {
                DiskError::FileVersionNotFound
            } else {
                DiskError::FileNotFound
            };
            // Nothing to do, file is already gone.
            return Ok((
                self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                    .await,
                Some(err),
            ));
        }

        info!(parts_count = parts_metadata.len(), "heal_object Initiating quorum check");
        match Self::object_quorum_from_meta(&parts_metadata, &errs, self.default_parity_count) {
            Ok((read_quorum, _)) => {
                result.parity_blocks = result.disk_count - read_quorum as usize;
                result.data_blocks = read_quorum as usize;

                let ((mut online_disks, quorum_mod_time, quorum_etag), disk_len) = {
                    let disks = self.disks.read().await;
                    let disk_len = disks.len();
                    (Self::list_online_disks(&disks, &parts_metadata, &errs, read_quorum as usize), disk_len)
                };

                info!(?parts_metadata, ?errs, ?read_quorum, ?disk_len, "heal_object List disks metadata");

                info!(?online_disks, ?quorum_mod_time, ?quorum_etag, "heal_object List online disks");

                let filter_by_etag = quorum_etag.is_some();
                match Self::pick_valid_fileinfo(&parts_metadata, quorum_mod_time, quorum_etag.clone(), read_quorum as usize) {
                    Ok(latest_meta) => {
                        info!("heal_object latest_meta: {:?}", latest_meta);

                        let (data_errs_by_disk, data_errs_by_part) = disks_with_all_parts(
                            &mut online_disks,
                            &mut parts_metadata,
                            &errs,
                            &latest_meta,
                            filter_by_etag,
                            bucket,
                            object,
                            opts.scan_mode,
                        )
                        .await?;

                        info!(
                            "disks_with_all_parts heal_object results: available_disks count={}, total_disks={}",
                            online_disks.iter().filter(|d| d.is_some()).count(),
                            online_disks.len()
                        );

                        let erasure = if !latest_meta.deleted && !latest_meta.is_remote() {
                            // Initialize erasure coding; use legacy mode for old-version files
                            coding::Erasure::try_new_with_options(
                                latest_meta.erasure.data_blocks,
                                latest_meta.erasure.parity_blocks,
                                latest_meta.erasure.block_size,
                                latest_meta.uses_legacy_checksum,
                            )
                            .map_err(DiskError::from)?
                        } else {
                            coding::Erasure::default()
                        };

                        result.object_size =
                            ObjectInfo::from_file_info(&latest_meta, bucket, object, true).get_actual_size()? as usize;
                        // Loop to find number of disks with valid data, per-drive
                        // data state and a list of outdated disks on which data needs
                        // to be healed.
                        let mut out_dated_disks = vec![None; disk_len];
                        let mut disks_to_heal_count = 0;
                        let mut meta_to_heal_count = 0;

                        for index in 0..online_disks.len() {
                            let (yes, is_meta, reason) = should_heal_object_on_disk(
                                &errs[index],
                                &data_errs_by_disk[&index],
                                &parts_metadata[index],
                                &latest_meta,
                            );

                            if yes {
                                out_dated_disks[index] = disks[index].clone();
                                disks_to_heal_count += 1;
                                if is_meta {
                                    meta_to_heal_count += 1;
                                }
                                debug!("heal_object Disk {} marked for healing (endpoint={})", index, self.set_endpoints[index]);
                            }

                            let drive_state = match reason {
                                Some(err) => match err {
                                    DiskError::DiskNotFound => DriveState::Offline.to_string(),
                                    DiskError::FileNotFound
                                    | DiskError::FileVersionNotFound
                                    | DiskError::VolumeNotFound
                                    | DiskError::PartMissingOrCorrupt
                                    | DiskError::OutdatedXLMeta => DriveState::Missing.to_string(),
                                    DiskError::FileCorrupt => DriveState::Corrupt.to_string(),
                                    _ => DriveState::Unknown(err.to_string()).to_string(),
                                },
                                None => DriveState::Ok.to_string(),
                            };
                            result.before.drives.push(HealDriveInfo {
                                uuid: "".to_string(),
                                endpoint: self.set_endpoints[index].to_string(),
                                state: drive_state.to_string(),
                            });

                            result.after.drives.push(HealDriveInfo {
                                uuid: "".to_string(),
                                endpoint: self.set_endpoints[index].to_string(),
                                state: drive_state.to_string(),
                            });
                        }

                        if disks_to_heal_count == 0 {
                            // The object is already healthy: no disk needs healing.
                            // This is the common case for the very objects PR #4356
                            // targets — a valid `xl.meta` plus a leaked pre-#3510
                            // data dir needs no shard healing, so it would otherwise
                            // return here and never reach the post-heal reclaim tail
                            // below. Sweep the strays on this path too (issues #3231,
                            // #3191). Skipped on dry-run, like every mutating step.
                            if !opts.dry_run {
                                self.reclaim_orphan_data_dirs_best_effort(bucket, object).await;
                            }
                            return Ok((result, None));
                        }

                        if opts.dry_run {
                            return Ok((result, None));
                        }

                        let mut cannot_heal = !latest_meta.deleted && meta_to_heal_count > latest_meta.erasure.parity_blocks;
                        if cannot_heal && quorum_etag.is_some() {
                            cannot_heal = false;
                        }

                        if !latest_meta.deleted && !latest_meta.is_remote() {
                            for part_errs in data_errs_by_part.values() {
                                if count_part_not_success(part_errs) > latest_meta.erasure.parity_blocks {
                                    cannot_heal = true;
                                    break;
                                }
                            }
                        }

                        // DATA-SAFETY GUARD (backlog#920, decision 1): before any
                        // dangling delete, if the version's DATA shards physically
                        // survive on >= data_blocks disks it is RECONSTRUCTABLE.
                        // Regenerate the missing xl.meta from a surviving valid
                        // FileInfo and re-drive the heal instead of destroying a
                        // recoverable version. Torn writes (< data_blocks data
                        // shards) fall through to the existing dangling behavior.
                        if cannot_heal
                            && allow_meta_regen
                            && self
                                .try_regenerate_recoverable_meta(bucket, object, &parts_metadata, &errs, &disks)
                                .await?
                        {
                            return Box::pin(self.heal_object_with_regen(bucket, object, version_id, opts, false)).await;
                        }

                        if cannot_heal {
                            let total_disks = parts_metadata.len();
                            let healthy_count = total_disks.saturating_sub(disks_to_heal_count);
                            let required_data = total_disks.saturating_sub(latest_meta.erasure.parity_blocks);

                            error!(
                                bucket,
                                object,
                                version_id,
                                required_data_shards = required_data,
                                healthy_shards = healthy_count,
                                missing_or_corrupt_shards = disks_to_heal_count,
                                parity_shards = latest_meta.erasure.parity_blocks,
                                "Heal object cannot reconstruct with available shards"
                            );

                            // Allow for dangling deletes, on versions that have DataDir missing etc.
                            // this would end up restoring the correct readable versions.
                            return match self
                                .delete_if_dangling(
                                    bucket,
                                    object,
                                    &parts_metadata,
                                    &errs,
                                    &data_errs_by_part,
                                    ObjectOptions {
                                        version_id: version_id_op.clone(),
                                        ..Default::default()
                                    },
                                )
                                .await
                            {
                                Ok(m) => {
                                    let derr = if !version_id.is_empty() {
                                        DiskError::FileVersionNotFound
                                    } else {
                                        DiskError::FileNotFound
                                    };
                                    let mut t_errs = Vec::with_capacity(errs.len());
                                    for _ in 0..errs.len() {
                                        t_errs.push(None);
                                    }
                                    Ok((self.default_heal_result(m, &t_errs, bucket, object, version_id).await, Some(derr)))
                                }
                                Err(err) => {
                                    error!(
                                        bucket,
                                        object,
                                        version_id,
                                        error = %err,
                                        "Heal object dangling cleanup could not prove object deletion"
                                    );
                                    let quorum_err = DiskError::ErasureReadQuorum;
                                    let mut t_errs = Vec::with_capacity(errs.len());
                                    for _ in 0..errs.len() {
                                        t_errs.push(Some(quorum_err.clone()));
                                    }

                                    Ok((
                                        self.default_heal_result(FileInfo::default(), &t_errs, bucket, object, version_id)
                                            .await,
                                        Some(quorum_err),
                                    ))
                                }
                            };
                        }

                        if !latest_meta.deleted && latest_meta.erasure.distribution.len() != online_disks.len() {
                            let err_str = format!(
                                "unexpected file distribution ({:?}) from available disks ({:?}), looks like backend disks have been manually modified refusing to heal {}/{}({})",
                                latest_meta.erasure.distribution, online_disks, bucket, object, version_id
                            );
                            warn!(err_str);
                            let err = DiskError::other(err_str);
                            return Ok((
                                self.default_heal_result(latest_meta, &errs, bucket, object, version_id).await,
                                Some(err),
                            ));
                        }

                        let latest_disks = Self::shuffle_disks(&online_disks, &latest_meta.erasure.distribution);
                        if !latest_meta.deleted && latest_meta.erasure.distribution.len() != out_dated_disks.len() {
                            let err_str = format!(
                                "unexpected file distribution ({:?}) from outdated disks ({:?}), looks like backend disks have been manually modified refusing to heal {}/{}({})",
                                latest_meta.erasure.distribution, out_dated_disks, bucket, object, version_id
                            );
                            warn!(err_str);
                            let err = DiskError::other(err_str);
                            return Ok((
                                self.default_heal_result(latest_meta, &errs, bucket, object, version_id).await,
                                Some(err),
                            ));
                        }

                        if !latest_meta.deleted && latest_meta.erasure.distribution.len() != parts_metadata.len() {
                            let err_str = format!(
                                "unexpected file distribution ({:?}) from metadata entries ({:?}), looks like backend disks have been manually modified refusing to heal {}/{}({})",
                                latest_meta.erasure.distribution,
                                parts_metadata.len(),
                                bucket,
                                object,
                                version_id
                            );
                            warn!(err_str);
                            let err = DiskError::other(err_str);
                            return Ok((
                                self.default_heal_result(latest_meta, &errs, bucket, object, version_id).await,
                                Some(err),
                            ));
                        }

                        out_dated_disks = Self::shuffle_disks(&out_dated_disks, &latest_meta.erasure.distribution);
                        let mut parts_metadata = Self::shuffle_parts_metadata(&parts_metadata, &latest_meta.erasure.distribution);
                        let mut copy_parts_metadata = vec![None; parts_metadata.len()];
                        for (index, disk) in latest_disks.iter().enumerate() {
                            if disk.is_some() {
                                copy_parts_metadata[index] = Some(parts_metadata[index].clone());
                            }
                        }

                        let clean_file_info = |fi: &FileInfo| -> FileInfo {
                            let mut nfi = fi.clone();
                            if !nfi.is_remote() {
                                nfi.data = None;
                                nfi.erasure.index = 0;
                                nfi.erasure.checksums = Vec::new();
                            }
                            nfi
                        };
                        for (index, disk) in out_dated_disks.iter().enumerate() {
                            if disk.is_some() {
                                // Make sure to write the FileInfo information
                                // that is expected to be in quorum.
                                parts_metadata[index] = clean_file_info(&latest_meta);
                            }
                        }

                        // We write at temporary location and then rename to final location.
                        let tmp_id = Uuid::new_v4().to_string();
                        // Delete markers and remote (transitioned) objects carry no data_dir and
                        // skip the data-heal block below, so a nil placeholder is safe for them.
                        // For a regular object a missing data_dir means the latest metadata is
                        // corrupt; fail this object's heal with a clear error instead of building
                        // part paths under a nil UUID directory.
                        let data_dir = match latest_meta.data_dir {
                            Some(data_dir) => data_dir,
                            None => {
                                if !latest_meta.deleted && !latest_meta.is_remote() {
                                    error!(
                                        "heal: latest metadata for {}/{} has no data_dir, cannot heal object data",
                                        bucket, object
                                    );
                                    return Err(DiskError::FileCorrupt);
                                }
                                Uuid::nil()
                            }
                        };
                        let src_data_dir = data_dir.to_string();
                        let dst_data_dir = data_dir;

                        if !latest_meta.deleted && !latest_meta.is_remote() {
                            let erasure_info = latest_meta.erasure.clone();

                            for (part_index, part) in latest_meta.parts.iter().enumerate() {
                                let till_offset = erasure.shard_file_offset(0, part.size, part.size);
                                let use_mmap_read = object_mmap_read_enabled();

                                let mut readers = Vec::with_capacity(latest_disks.len());
                                let mut writers = Vec::with_capacity(out_dated_disks.len());
                                // let mut errors = Vec::with_capacity(out_dated_disks.len());

                                let mut prefer = vec![false; latest_disks.len()];
                                for (index, disk) in latest_disks.iter().enumerate() {
                                    let this_part_errs =
                                        Self::shuffle_check_parts(&data_errs_by_part[&part_index], &erasure_info.distribution);
                                    if this_part_errs[index] != CHECK_PART_SUCCESS {
                                        info!(
                                            "reading part {}: index={}, part_errs={:?}, skipping",
                                            part.number, index, this_part_errs[index]
                                        );
                                        readers.push(None);
                                        continue;
                                    }

                                    if let (Some(disk), Some(metadata)) = (disk, &copy_parts_metadata[index]) {
                                        let checksum_info = metadata.erasure.get_checksum_info(part.number);
                                        let checksum_algo = if metadata.uses_legacy_checksum
                                            && checksum_info.algorithm == HashAlgorithm::HighwayHash256S
                                        {
                                            HashAlgorithm::HighwayHash256SLegacy
                                        } else {
                                            checksum_info.algorithm
                                        };

                                        match create_bitrot_reader(
                                            metadata.data.as_deref(),
                                            Some(disk),
                                            bucket,
                                            &path_join_buf(&[object, &src_data_dir, &format!("part.{}", part.number)]),
                                            0,
                                            till_offset,
                                            erasure.shard_size(),
                                            checksum_algo.clone(),
                                            false,
                                            use_mmap_read,
                                        )
                                        .await
                                        {
                                            Ok(Some(reader)) => {
                                                readers.push(Some(reader));
                                            }
                                            Ok(None) => {
                                                readers.push(None);
                                                continue;
                                            }
                                            Err(e) => {
                                                readers.push(None);
                                                continue;
                                            }
                                        }

                                        prefer[index] = disk.host_name().is_empty();
                                    } else {
                                        readers.push(None);
                                        // errors.push(Some(DiskError::DiskNotFound));
                                    }
                                }

                                // Preserve the committed layout: recomputing inline-ness here
                                // (with a hardcoded unversioned threshold) makes healed replicas
                                // diverge from healthy ones in quorum identity, so heal would
                                // flag them forever.
                                let is_inline_buffer = latest_meta.inline_data();
                                // create writers for all disk positions, but only for outdated disks
                                for (index, disk_op) in out_dated_disks.iter().enumerate() {
                                    if let Some(outdated_disk) = disk_op {
                                        let writer = match create_bitrot_writer(
                                            is_inline_buffer,
                                            Some(outdated_disk),
                                            RUSTFS_META_TMP_BUCKET,
                                            &path_join_buf(&[
                                                &tmp_id.to_string(),
                                                &dst_data_dir.to_string(),
                                                &format!("part.{}", part.number),
                                            ]),
                                            erasure.shard_file_size(part.size as i64),
                                            erasure.shard_size(),
                                            HashAlgorithm::HighwayHash256S,
                                        )
                                        .await
                                        {
                                            Ok(writer) => writer,
                                            Err(err) => {
                                                info!(
                                                    "create_bitrot_writer  disk {}, err {:?}, skipping operation",
                                                    outdated_disk.to_string(),
                                                    err
                                                );
                                                writers.push(None);
                                                continue;
                                            }
                                        };
                                        writers.push(Some(writer));
                                    } else {
                                        writers.push(None);
                                    }
                                }

                                // Heal each part. erasure.Heal() will write the healed
                                // part to .rustfs/tmp/uuid/ which needs to be renamed
                                // later to the final location.
                                if let Err(e) = erasure.heal(&mut writers, readers, part.size, &prefer).await {
                                    // Don't leak the partially-written healed shards in
                                    // .rustfs/tmp when heal fails midway (backlog#799 B20).
                                    let _ = self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_id).await;
                                    return Err(e);
                                }
                                // close_bitrot_writers(&mut writers).await?;

                                for (index, disk_op) in out_dated_disks.iter_mut().enumerate() {
                                    if disk_op.is_none() {
                                        continue;
                                    }

                                    if writers[index].is_none() {
                                        *disk_op = None;
                                        disks_to_heal_count -= 1;
                                        continue;
                                    }

                                    parts_metadata[index].data_dir = Some(dst_data_dir);
                                    parts_metadata[index].add_object_part(
                                        part.number,
                                        part.etag.clone(),
                                        part.size,
                                        part.mod_time,
                                        part.actual_size,
                                        part.index.clone(),
                                        part.checksums.clone(),
                                    );
                                    if is_inline_buffer {
                                        if let Some(writer) = writers[index].take() {
                                            // if let Some(w) = writer.as_any().downcast_ref::<BitrotFileWriter>() {
                                            //     parts_metadata[index].data = Some(w.inline_data().to_vec());
                                            // }
                                            parts_metadata[index].data =
                                                Some(writer.into_inline_data().map(Bytes::from).unwrap_or_default());
                                        }
                                        parts_metadata[index].set_inline_data();
                                    } else {
                                        parts_metadata[index].data = None;
                                    }
                                }

                                if disks_to_heal_count == 0 {
                                    // Clean up healed shards written to .rustfs/tmp before bailing (B20).
                                    let _ = self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_id).await;
                                    return Ok((
                                        result,
                                        Some(DiskError::other(format!(
                                            "all drives had write errors, unable to heal {bucket}/{object}"
                                        ))),
                                    ));
                                }
                            }
                        }
                        // Rename from tmp location to the actual location.
                        let mut rename_attempts = 0usize;
                        let mut rename_successes = 0usize;
                        for (index, outdated_disk) in out_dated_disks.iter().enumerate() {
                            if let Some(disk) = outdated_disk {
                                rename_attempts += 1;
                                // record the index of the updated disks
                                parts_metadata[index].erasure.index = index + 1;
                                // Attempt a rename now from healed data to final location.
                                parts_metadata[index].set_healing();

                                let rename_result = disk
                                    .rename_data(RUSTFS_META_TMP_BUCKET, &tmp_id, parts_metadata[index].clone(), bucket, object)
                                    .await;

                                if let Err(err) = &rename_result {
                                    warn!(
                                        event = EVENT_HEAL_OBJECT_RENAME,
                                        component = LOG_COMPONENT_ECSTORE,
                                        subsystem = LOG_SUBSYSTEM_HEAL,
                                        bucket,
                                        object,
                                        version_id,
                                        disk_index = index,
                                        endpoint = %disk.endpoint(),
                                        tmp_id,
                                        result = "failed",
                                        error = %err,
                                        "Heal object rename failed"
                                    );
                                } else {
                                    rename_successes += 1;
                                    if parts_metadata[index].is_remote() {
                                        let rm_data_dir =
                                            parts_metadata[index].data_dir.expect("operation should succeed").to_string();

                                        let d_path = Path::new(&encode_dir_object(object)).join(rm_data_dir);

                                        if let Err(e) = disk
                                            .delete(
                                                bucket,
                                                d_path.to_str().expect("operation should succeed"),
                                                DeleteOptions {
                                                    immediate: true,
                                                    recursive: true,
                                                    ..Default::default()
                                                },
                                            )
                                            .await
                                        {
                                            // The healed shard has already been renamed into place; a
                                            // failure cleaning up the old remote data dir must not abort
                                            // the heal and leak the tmp shards (backlog#799 B20).
                                            warn!(
                                                component = LOG_COMPONENT_ECSTORE,
                                                subsystem = LOG_SUBSYSTEM_HEAL,
                                                bucket,
                                                object,
                                                error = %e,
                                                "Heal remote data-dir cleanup failed"
                                            );
                                        }
                                    }

                                    for (i, v) in result.before.drives.iter().enumerate() {
                                        if v.endpoint == disk.endpoint().to_string() {
                                            result.after.drives[i].state = DriveState::Ok.to_string();
                                        }
                                    }
                                }
                            }
                        }
                        self.delete_all(RUSTFS_META_TMP_BUCKET, &tmp_id)
                            .await
                            .map_err(DiskError::other)?;

                        if rename_attempts > 0 && rename_successes == 0 {
                            return Ok((
                                result,
                                Some(DiskError::other(format!("all healed data rename attempts failed for {bucket}/{object}"))),
                            ));
                        }

                        self.record_healed_capacity_scope(&out_dated_disks);

                        // The object is healthy here; sweep any data dirs left behind
                        // by pre-#3510 unversioned overwrites, which the dangling paths
                        // above never touch (issues #3231, #3191). Best effort — a
                        // failure must not fail the heal.
                        self.reclaim_orphan_data_dirs_best_effort(bucket, object).await;

                        Ok((result, None))
                    }
                    Err(err) => Ok((result, Some(err))),
                }
            }
            Err(err) => {
                // DATA-SAFETY GUARD (backlog#920, decision 1): meta quorum failed,
                // but the version's DATA may still physically survive on enough
                // disks (xl.meta lost on > parity disks while part files remain).
                // Rescue it by regenerating the missing xl.meta and re-driving heal
                // instead of dangling-deleting a reconstructable version.
                if allow_meta_regen
                    && self
                        .try_regenerate_recoverable_meta(bucket, object, &parts_metadata, &errs, &disks)
                        .await?
                {
                    return Box::pin(self.heal_object_with_regen(bucket, object, version_id, opts, false)).await;
                }

                let data_errs_by_part = HashMap::new();
                match self
                    .delete_if_dangling(
                        bucket,
                        object,
                        &parts_metadata,
                        &errs,
                        &data_errs_by_part,
                        ObjectOptions {
                            version_id: version_id_op.clone(),
                            ..Default::default()
                        },
                    )
                    .await
                {
                    Ok(m) => {
                        let err = if !version_id.is_empty() {
                            DiskError::FileVersionNotFound
                        } else {
                            DiskError::FileNotFound
                        };
                        Ok((self.default_heal_result(m, &errs, bucket, object, version_id).await, Some(err)))
                    }
                    Err(_) => Ok((
                        self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                            .await,
                        Some(err),
                    )),
                }
            }
        }
    }

    /// backlog#920 (decision 1): rescue a version that meta-quorum logic would
    /// otherwise dangling-DELETE, when its DATA is still reconstructable.
    ///
    /// Returns `Ok(true)` if the version was rescued (missing xl.meta regenerated
    /// on at least one disk, so a re-driven heal can reconstruct it), `Ok(false)`
    /// to fall through to the existing dangling-delete behavior.
    ///
    /// Recoverability is computed by physically probing part files across ALL
    /// disks in the set with `check_parts` — including disks whose xl.meta is
    /// absent (a lost xl.meta does not lose the sibling `part.*` data). If at
    /// least `data_blocks` disks hold every part of a surviving valid FileInfo,
    /// the object is EC-reconstructable, so we regenerate that FileInfo's xl.meta
    /// on every disk whose metadata is absent (via `write_metadata`, which merges
    /// into any existing xl.meta). Delete markers, remote/transitioned versions,
    /// and genuine torn writes (< `data_blocks` surviving data shards) are NOT
    /// rescued — they keep the current dangling-delete-after-grace behavior, so no
    /// regression on those paths.
    async fn try_regenerate_recoverable_meta(
        &self,
        bucket: &str,
        object: &str,
        parts_metadata: &[FileInfo],
        errs: &[Option<DiskError>],
        disks: &[Option<DiskStore>],
    ) -> disk::error::Result<bool> {
        // A surviving valid, non-deleted, non-remote data FileInfo to rebuild from.
        let Some(surviving) = parts_metadata
            .iter()
            .find(|fi| fi.has_valid_erasure_geometry() && !fi.deleted && !fi.is_remote())
            .cloned()
        else {
            return Ok(false);
        };

        // Without a data_dir + parts there is no data to prove recoverable.
        if surviving.data_dir.is_none() || surviving.parts.is_empty() {
            return Ok(false);
        }
        let data_blocks = surviving.erasure.data_blocks;
        if data_blocks == 0 {
            return Ok(false);
        }

        // Physically probe part presence on EVERY online disk using the surviving
        // FileInfo's data_dir/parts. `check_parts` stats `object/<data_dir>/part.N`
        // directly, so it counts disks that still hold the data even if their
        // xl.meta was deleted.
        let mut available = 0usize;
        for disk in disks.iter().flatten() {
            if let Ok(resp) = disk.check_parts(bucket, object, &surviving).await
                && !resp.results.is_empty()
                && resp.results.iter().all(|r| *r == CHECK_PART_SUCCESS)
            {
                available += 1;
            }
        }

        // Torn write: fewer than data_blocks surviving data shards is genuinely
        // unrecoverable — preserve the current dangling behavior (no resurrection).
        if available < data_blocks {
            debug!(
                bucket,
                object,
                available,
                data_blocks,
                "heal_object: version not reconstructable (torn write), keeping dangling behavior"
            );
            return Ok(false);
        }

        // Reconstructable: regenerate the surviving xl.meta on every disk whose
        // metadata is absent so the version regains read-quorum. Each disk gets its
        // OWN shard index: the disk at physical position `index` holds shard
        // `distribution[index]` (mirrors `shuffle_disks` + the write path's
        // `erasure.index = shuffled_pos + 1`). Copying the surviving disk's index
        // verbatim would write an inconsistent xl.meta that the re-heal then treats
        // as corrupt.
        let distribution = &surviving.erasure.distribution;
        let mut wrote = 0usize;
        for (index, disk) in disks.iter().enumerate() {
            let Some(disk) = disk else { continue };
            let meta_absent = matches!(
                errs.get(index).and_then(Option::as_ref),
                Some(DiskError::FileNotFound | DiskError::FileVersionNotFound)
            ) || !parts_metadata.get(index).map(FileInfo::is_valid).unwrap_or(false);
            if !meta_absent {
                continue;
            }
            // Without a known shard index for this position we cannot write a
            // consistent xl.meta; leave it for the normal heal to reconstruct.
            let Some(&shard_index) = distribution.get(index) else {
                continue;
            };
            let mut regen = surviving.clone();
            regen.fresh = false; // merge into any existing xl.meta on the disk
            regen.erasure.index = shard_index;
            match disk.write_metadata("", bucket, object, regen).await {
                Ok(()) => wrote += 1,
                Err(e) => {
                    warn!(
                        bucket,
                        object,
                        disk_index = index,
                        error = %e,
                        "heal_object: failed to regenerate recoverable xl.meta on disk"
                    );
                }
            }
        }

        if wrote == 0 {
            return Ok(false);
        }

        info!(
            bucket,
            object,
            available,
            data_blocks,
            regenerated_meta_disks = wrote,
            "heal_object: rescued reconstructable sub-quorum version by regenerating xl.meta"
        );
        Ok(true)
    }

    pub(in crate::set_disk) async fn heal_object_dir_locked(
        &self,
        bucket: &str,
        object: &str,
        dry_run: bool,
        remove: bool,
    ) -> Result<(HealResultItem, Option<DiskError>)> {
        let disks = {
            let disks = self.disks.read().await;
            disks.clone()
        };
        let mut result = HealResultItem {
            heal_item_type: HealItemType::Object.to_string(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            disk_count: self.disks.read().await.len(),
            parity_blocks: self.default_parity_count,
            data_blocks: disks.len() - self.default_parity_count,
            object_size: 0,
            ..Default::default()
        };

        // Filled below by pushing one entry per disk while zipping the (index-aligned) `errs`.
        // Pre-filling here would double the reported drive list once the push loop runs.
        result.before.drives = Vec::with_capacity(disks.len());
        result.after.drives = Vec::with_capacity(disks.len());

        let errs = stat_all_dirs(&disks, bucket, object).await;
        let dangling_object = is_object_dir_dangling(&errs);
        if dangling_object && !dry_run && remove {
            let mut futures = Vec::with_capacity(disks.len());
            for disk in disks.iter().flatten() {
                let disk = disk.clone();
                let bucket = bucket.to_string();
                let object = object.to_string();
                futures.push(tokio::spawn(async move {
                    let _ = disk
                        .delete(
                            &bucket,
                            &object,
                            DeleteOptions {
                                recursive: false,
                                immediate: false,
                                ..Default::default()
                            },
                        )
                        .await;
                }));
            }

            // ignore errors
            let _ = join_all(futures).await;
        }

        for (err, drive) in errs.iter().zip(self.set_endpoints.iter()) {
            let endpoint = drive.to_string();
            let drive_state = match err {
                Some(err) => match err {
                    DiskError::DiskNotFound => DriveState::Offline.to_string(),
                    DiskError::FileNotFound | DiskError::VolumeNotFound => DriveState::Missing.to_string(),
                    _ => DriveState::Corrupt.to_string(),
                },
                None => DriveState::Ok.to_string(),
            };
            result.before.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint: endpoint.clone(),
                state: drive_state.to_string(),
            });

            result.after.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint,
                state: drive_state.to_string(),
            });
        }

        if dangling_object || DiskError::is_all_not_found(&errs) {
            return Ok((result, Some(DiskError::FileNotFound)));
        }

        if dry_run {
            // Quit without try to heal the object dir
            return Ok((result, None));
        }
        for (index, (err, disk)) in errs.iter().zip(disks.iter()).enumerate() {
            if let (Some(DiskError::VolumeNotFound | DiskError::FileNotFound), Some(disk)) = (err, disk) {
                let vol_path = Path::new(bucket).join(object);
                let drive_state = match disk.make_volume(vol_path.to_str().expect("operation should succeed")).await {
                    Ok(_) => DriveState::Ok.to_string(),
                    Err(merr) => match merr {
                        DiskError::VolumeExists => DriveState::Ok.to_string(),
                        DiskError::DiskNotFound => DriveState::Offline.to_string(),
                        _ => DriveState::Corrupt.to_string(),
                    },
                };
                result.after.drives[index].state = drive_state.to_string();
            }
        }

        Ok((result, None))
    }

    #[tracing::instrument(skip(self))]
    pub(in crate::set_disk) async fn heal_object_dir(
        &self,
        bucket: &str,
        object: &str,
        dry_run: bool,
        remove: bool,
    ) -> Result<(HealResultItem, Option<DiskError>)> {
        let _write_lock_guard = self
            .new_ns_lock(bucket, object)
            .await?
            .get_write_lock(get_lock_acquire_timeout())
            .await
            .map_err(|e| DiskError::other(self.map_namespace_lock_error(bucket, object, "write", e).to_string()))?;

        self.heal_object_dir_locked(bucket, object, dry_run, remove).await
    }

    pub(in crate::set_disk) async fn default_heal_result(
        &self,
        lfi: FileInfo,
        errs: &[Option<DiskError>],
        bucket: &str,
        object: &str,
        version_id: &str,
    ) -> HealResultItem {
        // Take a single snapshot of the disk vector and drive both `disk_len` and
        // the per-drive loop below from it, so the reported `disk_count` and the
        // pushed drive records always agree (previously two independent
        // `self.disks.read()` calls could observe different lengths).
        let disks = self.disks.read().await;
        let disk_len = disks.len();
        let mut result = HealResultItem {
            heal_item_type: HealItemType::Object.to_string(),
            bucket: bucket.to_string(),
            object: object.to_string(),
            object_size: lfi.size as usize,
            version_id: version_id.to_string(),
            disk_count: disk_len,
            ..Default::default()
        };

        // Report the object's own parity only when it actually carries erasure
        // geometry; delete markers and geometry-less versions fall back to the
        // pool default. Uses `has_valid_erasure_geometry()` (not `is_valid()`)
        // to stay in step with the rest of the metadata-predicate migration —
        // `is_valid()` now requires full payload validation and returns `false`
        // for delete markers, which would misreport their parity here.
        if lfi.has_valid_erasure_geometry() {
            result.parity_blocks = lfi.erasure.parity_blocks;
        } else {
            result.parity_blocks = self.default_parity_count;
        }

        result.data_blocks = disk_len - result.parity_blocks;

        // `errs` is index-aligned with the disk vector; only the online path below
        // indexes into it (the offline branch `continue`s before touching it).
        debug_assert_eq!(errs.len(), disk_len, "errs length must match the disk count");

        for (index, disk) in disks.iter().enumerate() {
            if disk.is_none() {
                result.before.drives.push(HealDriveInfo {
                    uuid: "".to_string(),
                    endpoint: self.set_endpoints[index].to_string(),
                    state: DriveState::Offline.to_string(),
                });

                result.after.drives.push(HealDriveInfo {
                    uuid: "".to_string(),
                    endpoint: self.set_endpoints[index].to_string(),
                    state: DriveState::Offline.to_string(),
                });
                // Offline disks contribute exactly one record; without this the
                // control flow fell through and pushed a second (Corrupt) record
                // for the same disk, doubling the list and breaking index alignment.
                continue;
            }

            let mut drive_state = DriveState::Corrupt;
            if let Some(err) = &errs[index] {
                if err == &DiskError::FileNotFound || err == &DiskError::VolumeNotFound {
                    drive_state = DriveState::Missing;
                }
            } else {
                drive_state = DriveState::Ok;
            }

            result.before.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint: self.set_endpoints[index].to_string(),
                state: drive_state.to_string(),
            });
            result.after.drives.push(HealDriveInfo {
                uuid: "".to_string(),
                endpoint: self.set_endpoints[index].to_string(),
                state: drive_state.to_string(),
            });
        }
        result
    }
}

// Heal operation family: the storage-api `HealOperations` contract stays
// implemented `for SetDisks` (contract bounds unchanged) but now lives beside
// its inherent helpers in the `set_disk::ops::heal` module. Bodies are moved
// unchanged; `get_pool_and_set` reads the core through `SetDisksCtx` to keep
// the Heal family aligned with the borrow pattern from #816.
#[async_trait::async_trait]
impl crate::storage_api_contracts::heal::HealOperations for SetDisks {
    type Error = Error;
    type HealResultItem = HealResultItem;
    type HealOptions = HealOpts;

    #[tracing::instrument(skip(self))]
    async fn heal_format(&self, dry_run: bool) -> Result<(HealResultItem, Option<Error>)> {
        let disks = self.disks.read().await.clone();
        let (formats, errs) = load_format_erasure_all(&disks, true).await;
        let ref_format = match get_format_erasure_in_quorum(&formats) {
            Ok(format) => format,
            Err(err) => {
                let can_use_cached_layout = count_errs(&errs, &DiskError::UnformattedDisk) > 0
                    && formats.iter().flatten().all(|format| self.format.check_other(format).is_ok())
                    && errs
                        .iter()
                        .all(|err| err.is_none() || matches!(err, Some(DiskError::UnformattedDisk)));
                if can_use_cached_layout {
                    self.format.clone()
                } else {
                    return Ok((HealResultItem::default(), Some(err)));
                }
            }
        };

        let endpoints = crate::layout::endpoints::Endpoints::from(self.set_endpoints.clone());
        let before_drives = crate::layout::set_heal::formats_to_drives_info(&endpoints, &formats, &errs);
        let mut result = HealResultItem {
            heal_item_type: HealItemType::Metadata.to_string(),
            detail: "disk-format".to_string(),
            disk_count: self.set_drive_count,
            set_count: 1,
            before: Infos {
                drives: before_drives.clone(),
            },
            after: Infos { drives: before_drives },
            ..Default::default()
        };

        if count_errs(&errs, &DiskError::UnformattedDisk) == 0 {
            info!("set disk formats success, NoHealRequired, errs: {:?}", errs);
            return Ok((result, Some(StorageError::NoHealRequired)));
        }

        if !dry_run {
            for (disk_idx, err) in errs.iter().enumerate() {
                if !matches!(err, Some(DiskError::UnformattedDisk)) {
                    continue;
                }

                let mut new_format = ref_format.clone();
                new_format.erasure.this = ref_format.erasure.sets[self.set_index][disk_idx];
                if save_format_file(&disks[disk_idx], &Some(new_format.clone())).await.is_ok() {
                    result.after.drives[disk_idx].uuid = new_format.erasure.this.to_string();
                    result.after.drives[disk_idx].state = DriveState::Ok.to_string();
                }
            }
        }

        Ok((result, None))
    }

    #[tracing::instrument(skip(self))]
    async fn heal_bucket(&self, bucket: &str, opts: &HealOpts) -> Result<HealResultItem> {
        let mut result = heal_bucket_local_on_disks(bucket, opts, self.disk_inventory().await).await?;
        result.set_count = 1;
        Ok(result)
    }

    #[tracing::instrument(skip(self))]
    async fn heal_object(
        &self,
        bucket: &str,
        object: &str,
        version_id: &str,
        opts: &HealOpts,
    ) -> Result<(HealResultItem, Option<Error>)> {
        let _write_lock_guard = if !opts.no_lock {
            let ns_lock = self.new_ns_lock(bucket, object).await?;
            Some(
                ns_lock
                    .get_write_lock(get_lock_acquire_timeout())
                    .await
                    .map_err(|e| self.map_namespace_lock_error(bucket, object, "write", e))?,
            )
        } else {
            None
        };

        if has_suffix(object, SLASH_SEPARATOR) {
            let (result, err) = self.heal_object_dir_locked(bucket, object, opts.dry_run, opts.remove).await?;
            return Ok((result, err.map(|e| e.into())));
        }

        let disks = self.disks.read().await;

        let disks = disks.clone();
        let (_, errs) = Self::read_all_fileinfo(&disks, "", bucket, object, version_id, false, false, false)
            .await
            .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;
        if DiskError::is_all_not_found(&errs) {
            debug!(
                event = EVENT_SET_DISK_HEAL,
                component = LOG_COMPONENT_ECSTORE,
                subsystem = LOG_SUBSYSTEM_SET_DISK,
                bucket,
                object,
                version_id,
                state = "missing_object_skipped",
                "Set disk heal skipped missing object"
            );
            let err = if !version_id.is_empty() {
                Error::FileVersionNotFound
            } else {
                Error::FileNotFound
            };
            return Ok((
                self.default_heal_result(FileInfo::default(), &errs, bucket, object, version_id)
                    .await,
                Some(err),
            ));
        }

        // Heal the object.
        // Pass no_lock=true since we already obtained write lock (or are already called with no_lock=true)
        let mut inner_opts = *opts;
        inner_opts.no_lock = true;
        let (result, err) = self
            .heal_object(bucket, object, version_id, &inner_opts)
            .await
            .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;
        if let Some(err) = err.as_ref() {
            match err {
                &DiskError::FileCorrupt if opts.scan_mode != HealScanMode::Deep => {
                    // Instead of returning an error when a bitrot error is detected
                    // during a normal heal scan, heal again with bitrot flag enabled.
                    inner_opts.scan_mode = HealScanMode::Deep;
                    let (result, err) = self
                        .heal_object(bucket, object, version_id, &inner_opts)
                        .await
                        .map_err(|e| to_object_err(e.into(), vec![bucket, object]))?;
                    return Ok((result, err.map(|e| e.into())));
                }
                _ => {}
            }
        }
        Ok((result, err.map(|e| e.into())))
    }

    #[tracing::instrument(skip(self))]
    async fn get_pool_and_set(&self, id: &str) -> Result<(Option<usize>, Option<usize>, Option<usize>)> {
        let ctx = self.ctx();
        for (set_idx, set) in ctx.format().erasure.sets.iter().enumerate() {
            for (disk_idx, disk_id) in set.iter().enumerate() {
                if disk_id.to_string() == id {
                    return Ok((Some(ctx.pool_index()), Some(set_idx), Some(disk_idx)));
                }
            }
        }

        Err(Error::DiskNotFound)
    }

    #[tracing::instrument(skip(self))]
    async fn check_abandoned_parts(&self, _bucket: &str, _object: &str, _opts: &HealOpts) -> Result<()> {
        // Multipart orphan reconciliation is intentionally retained above the set layer
        // until there is a concrete caller and a stable lower-level contract to implement.
        Err(StorageError::NotImplemented)
    }
}

#[cfg(test)]
mod heal_result_report_tests {
    use super::SetDisks;
    use crate::disk::endpoint::Endpoint;
    use crate::disk::error::DiskError;
    use crate::disk::format::FormatV3;
    use crate::disk::{DiskOption, DiskStore, new_disk};
    use rustfs_common::heal_channel::DriveState;
    use rustfs_filemeta::FileInfo;
    use std::sync::Arc;
    use tempfile::TempDir;
    use tokio::sync::RwLock;

    async fn real_disk() -> (TempDir, Endpoint, DiskStore) {
        let dir = tempfile::tempdir().expect("tempdir should be created");
        let endpoint =
            Endpoint::try_from(dir.path().to_str().expect("tempdir path should be utf8")).expect("endpoint should parse");
        let disk = new_disk(
            &endpoint,
            &DiskOption {
                cleanup: false,
                health_check: false,
            },
        )
        .await
        .expect("disk should be created");
        (dir, endpoint, disk)
    }

    async fn set_disks_with(
        disks: Vec<Option<DiskStore>>,
        endpoints: Vec<Endpoint>,
        default_parity_count: usize,
    ) -> Arc<SetDisks> {
        let set_drive_count = disks.len();
        SetDisks::new(
            "test-owner".to_string(),
            Arc::new(RwLock::new(disks)),
            set_drive_count,
            default_parity_count,
            0,
            0,
            endpoints,
            FormatV3::new(1, set_drive_count),
            vec![],
        )
        .await
    }

    // Regression for #955: an offline disk must contribute exactly one drive
    // record. Before the fix the offline branch fell through and pushed a second
    // (Corrupt) record for the same disk, so `before/after.drives` grew to
    // `disk_count + offline_count` and every entry after the first offline slot
    // was misaligned relative to its disk index.
    #[tokio::test]
    async fn default_heal_result_reports_one_record_per_disk_and_stays_aligned() {
        // index 0: online, no error       -> Ok
        // index 1: offline (None)         -> Offline (single record)
        // index 2: online, FileNotFound   -> Missing
        // index 3: online, DiskAccessDenied-> Corrupt
        let (_d0, ep0, disk0) = real_disk().await;
        let (_d2, ep2, disk2) = real_disk().await;
        let (_d3, ep3, disk3) = real_disk().await;
        let ep1 = Endpoint::try_from("http://127.0.0.1:9001/data").expect("endpoint should parse");

        let disks = vec![Some(disk0), None, Some(disk2), Some(disk3)];
        let endpoints = vec![ep0, ep1, ep2, ep3];
        let set = set_disks_with(disks, endpoints, 1).await;

        let errs = vec![
            None,
            Some(DiskError::DiskNotFound),
            Some(DiskError::FileNotFound),
            Some(DiskError::DiskAccessDenied),
        ];

        let result = set
            .default_heal_result(FileInfo::default(), &errs, "bucket", "object", "")
            .await;

        // Exactly one record per disk (not disk_count + offline_count).
        assert_eq!(result.disk_count, 4);
        assert_eq!(result.before.drives.len(), 4, "one before record per disk");
        assert_eq!(result.after.drives.len(), 4, "one after record per disk");

        // Records stay index-aligned with the disk vector and set_endpoints.
        let expected_states = [
            DriveState::Ok.to_string(),
            DriveState::Offline.to_string(),
            DriveState::Missing.to_string(),
            DriveState::Corrupt.to_string(),
        ];
        for (i, expected) in expected_states.iter().enumerate() {
            assert_eq!(&result.before.drives[i].state, expected, "before state at {i}");
            assert_eq!(&result.after.drives[i].state, expected, "after state at {i}");
            assert_eq!(
                result.before.drives[i].endpoint,
                set.set_endpoints[i].to_string(),
                "before endpoint aligned at {i}"
            );
            assert_eq!(
                result.after.drives[i].endpoint,
                set.set_endpoints[i].to_string(),
                "after endpoint aligned at {i}"
            );
        }

        // The offline endpoint appears exactly once, never as a second Corrupt row.
        let offline_ep = set.set_endpoints[1].to_string();
        assert_eq!(
            result.before.drives.iter().filter(|d| d.endpoint == offline_ep).count(),
            1,
            "offline disk must not produce a duplicate record"
        );
    }

    // Two interleaved offline disks: assert every record still maps to its own
    // set_endpoints[index] (no cumulative drift after the first offline slot).
    #[tokio::test]
    async fn default_heal_result_alignment_with_multiple_offline_disks() {
        let (_d1, ep1, disk1) = real_disk().await;
        let (_d3, ep3, disk3) = real_disk().await;
        let ep0 = Endpoint::try_from("http://127.0.0.1:9000/data").expect("endpoint should parse");
        let ep2 = Endpoint::try_from("http://127.0.0.1:9002/data").expect("endpoint should parse");

        // index 0 offline, 1 online, 2 offline, 3 online.
        let disks = vec![None, Some(disk1), None, Some(disk3)];
        let endpoints = vec![ep0, ep1, ep2, ep3];
        let set = set_disks_with(disks, endpoints, 1).await;

        let errs = vec![Some(DiskError::DiskNotFound), None, Some(DiskError::DiskNotFound), None];

        let result = set
            .default_heal_result(FileInfo::default(), &errs, "bucket", "object", "")
            .await;

        assert_eq!(result.before.drives.len(), 4);
        assert_eq!(result.after.drives.len(), 4);
        for i in 0..4 {
            assert_eq!(result.before.drives[i].endpoint, set.set_endpoints[i].to_string(), "aligned at {i}");
        }
        assert_eq!(result.before.drives[0].state, DriveState::Offline.to_string());
        assert_eq!(result.before.drives[1].state, DriveState::Ok.to_string());
        assert_eq!(result.before.drives[2].state, DriveState::Offline.to_string());
        assert_eq!(result.before.drives[3].state, DriveState::Ok.to_string());
    }
}
